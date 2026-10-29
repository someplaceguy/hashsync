#!/usr/bin/env bash
#
# Copyright 2026 Ricardo Correia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Unlike the rest of hashsync, which is under the GPL, this script is under
# the license that the FSF recommends for small programs, so that it can be
# adapted freely. LICENSE-Apache-2.0 next to it has the full text.

set -euo pipefail

# Copy the closures of store paths from one host to another, like
# nix-copy-closure. The source sends only the paths that the destination does
# not have, and hashsync sends only the blocks that the destination does not
# have in its Nix store.
#
# Without --from, this host pushes the paths to the destination with
# hashsync; without --to, it pulls them from the source. With both, the
# destination pulls the paths, and logs in to the source with the SSH agent
# that this host forwards to it. That login has no terminal to prompt on, so
# the source must accept a key in the agent, and the destination must already
# know the host key of the source.
#
# Both hosts need Nix and hashsync, and commands run over SSH must find
# nix-store and hashsync in the PATH. On the destination, the Nix daemon must
# trust the user (root, or a user in trusted-users in nix.conf), since the
# paths come without signatures, as with nix-copy-closure.
#
# This script uses a separate hashsync profile named nix, whose roots on the
# destination should include /nix/store. The source needs no profile. For
# example, in ~/.config/hashsync/nix.toml:
#
#     [[roots]]
#     path = "/nix/store/*"
#     immutable = true
#     exclude = [".links"]
#     may_vanish = true
#
# Build the index once with "hashsync update --profile nix": otherwise the
# first copy builds it, and deduplicates little until it is done. Each copy
# then keeps the index up to date.

usage() {
	echo "usage: ${0##*/} [--from [USER@]HOST] [--to [USER@]HOST] PATH..." >&2
	echo "The default of --from and --to is this host. Give one of them or both." >&2
	exit 2
}

# Print the sum of the NAR sizes on stdin, from nix-store --query --size, in
# the format of the hashsync summary. hashsync usually sends less: it does not
# send the blocks that the destination has, and it compresses the other
# blocks.
sum_sizes() {
	awk '
		{ size += $1 }
		END {
			n = split("B KiB MiB GiB TiB PiB", units)
			for (i = 1; size >= 1024 && i < n; i++)
				size /= 1024
			printf(i == 1 ? "%d %s" : "%.2f %s", size, units[i])
		}'
}

# Make the closures of the store PATHS valid on this host. hashsync copies
# the paths that this host does not have from SOURCE, which must have them.
pull_closure() {
	local source="$1"
	shift
	local -a missing
	local size

	if nix-store --check-validity "$@" 2>/dev/null; then
		return
	fi

	# The commands must not read stdin. On a remote destination, stdin
	# contains the remainder of the commands that this script sends.
	mapfile -t missing < <(
		ssh -n "$source" nix-store --query --requisites "$@" |
			nix-store --check-validity --print-invalid --stdin
	)
	if ! wait "$!"; then
		echo "error: cannot get the closure from $source" >&2
		return 1
	fi
	if ! size="$(
		printf '%s\n' "${missing[@]}" |
			ssh "$source" nix-store --query --size --stdin |
			sum_sizes
	)"; then
		echo "error: cannot get the size of the paths from $source" >&2
		return 1
	fi
	echo "Copying ${#missing[@]} paths ($size)..." >&2
	# The paths go through stdin, which hashsync gives to the command. A
	# closure can have more paths than the kernel permits in the arguments
	# of a command (ARG_MAX).
	#
	# Batch delivery sends the paths and the export only from full buffers
	# and at the end of each stream, not each time that their source waits.
	# Then hashsync keeps more block references, and the compression is
	# better. Batch delivery is not safe when the producer of the input
	# waits for the output (e.g. rsync -e), but printf does not wait.
	#
	# If GC on this host removes a path that the import needs, the import
	# fails. Then, do the copy again. nix-copy-closure has the same problem.
	printf '%s\n' "${missing[@]}" |
		hashsync ssh --profile nix \
			--no-shell --batch-stdin --batch-stdout \
			"$source" -- nix-store --export --stdin |
		nix-store --import
	# Check that this host has the paths now, only as a safety net: if the
	# export did not get the paths from hashsync, the export and the import
	# would succeed without them.
	if ! nix-store --check-validity "$@"; then
		echo "error: the copy from $source did not make the paths valid" >&2
		return 1
	fi
}

# Make the closures of the store PATHS valid on DEST. hashsync copies the
# paths that DEST does not have from this host, which must have them.
push_closure() {
	local dest="$1"
	shift
	local -a missing
	local size

	# One SSH call both checks the closures on DEST and finds the paths to
	# copy.
	mapfile -t missing < <(
		nix-store --query --requisites "$@" |
			ssh "$dest" nix-store --check-validity --print-invalid --stdin
	)
	if ! wait "$!"; then
		echo "error: cannot find the paths to copy to $dest" >&2
		return 1
	fi
	if [[ ${#missing[@]} -eq 0 ]]; then
		return
	fi
	size="$(
		printf '%s\n' "${missing[@]}" |
			nix-store --query --size --stdin |
			sum_sizes
	)"
	echo "Copying ${#missing[@]} paths ($size)..." >&2
	# hashsync deduplicates the export against the index of DEST. Batch
	# delivery sends the export and the output of the import only from full
	# buffers and at the end of each stream, as in a pull. The export does
	# not wait for the output, so batch delivery is safe.
	#
	# If GC on DEST removes a path that the import needs, the import fails.
	# Then, do the copy again. nix-copy-closure has the same problem.
	printf '%s\n' "${missing[@]}" |
		nix-store --export --stdin |
		hashsync ssh --server-profile nix \
			--no-shell --dedup-stdin --batch-stdin --batch-stdout \
			"$dest" -- nix-store --import
	# Check that DEST has the copied paths now, only as a safety net: the
	# export gets the paths from printf, and the import fails on an empty
	# stdin. PATHS can name symlinks on this host, such as ./result, which
	# DEST could not check.
	if ! printf '%s\n' "${missing[@]}" |
			ssh "$dest" nix-store --check-validity --stdin; then
		echo "error: the copy to $dest did not make the paths valid" >&2
		return 1
	fi
}

from=
to=
while [[ $# -gt 0 ]]; do
	case "$1" in
	--from)
		[[ $# -ge 2 ]] || usage
		from="$2"
		shift 2
		;;
	--to)
		[[ $# -ge 2 ]] || usage
		to="$2"
		shift 2
		;;
	--)
		shift
		break
		;;
	-*)
		usage
		;;
	*)
		break
		;;
	esac
done

if [[ $# -eq 0 || ( -z "$from" && -z "$to" ) ]]; then
	usage
fi

if [[ -z "$to" ]]; then
	pull_closure "$from" "$@"
elif [[ -z "$from" ]]; then
	push_closure "$to" "$@"
else
	# The destination pulls the paths. Its login shell (e.g. fish) only
	# starts bash, which reads the functions and the call from stdin.
	ssh -A "$to" bash -s <<-EOF
		set -euo pipefail
		$(declare -f sum_sizes pull_closure)
		pull_closure ${from@Q} ${*@Q}
	EOF
fi
