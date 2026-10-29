#!/usr/bin/env bash
#
# Copyright 2026 Ricardo Correia
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Check an installed hashsync package, as `default.nix` does after it
# installs one:
#
#     bash install-check.sh [--static] [--mimalloc] PREFIX
#
# The tests run the binary before the Nix fixup changes it, and never as a
# user installs it, so this check covers what they cannot see: the binary
# after the fixup, its man page, the libraries that it links, and a server
# that the client finds through PATH. In order:
#
# - RocksDB, LZ4, libzstd, and with the `mimalloc` feature mimalloc, are
#   inside the binary (DESIGN.md §9), which links only the C and C++ runtime
#   libraries, and nothing at all with --static. mimalloc reports its start
#   exactly when --mimalloc is given, which `default.nix` does for that
#   feature, so a binary with another allocator than the build asked for
#   fails here.
# - The man page is installed, and mandoc finds nothing wrong in it.
# - Every command works. An update builds an index whose data blocks
#   RocksDB compresses with LZ4, and the runs compress with zstd worker
#   threads, so a binary that lacks either fails here.
# - `hashsync ssh` finds the server through PATH in a pull, and through
#   --hashsync-path in a push. Both runs send every block of their stream
#   as a reference, the pull verified with BLAKE3 and the push with SHA-256.
#
# The runs go through a fake ssh, which runs the remote command with a local
# shell, as sshd would. Every file of the check goes to a temporary
# directory, with homes of its own for the client and the server, so the
# check never touches the files of the user who runs it.

set -euo pipefail

static=
mimalloc=
while :; do
	case ${1-} in
	--static) static=1 ;;
	--mimalloc) mimalloc=1 ;;
	*) break ;;
	esac
	shift
done
if [ $# -ne 1 ]; then
	echo "usage: $0 [--static] [--mimalloc] PREFIX" >&2
	exit 2
fi
# The fake ssh below starts the server in its own home directory, so the PATH
# entry and the --hashsync-path of the runs must not be relative.
prefix=$(realpath -- "$1")
hashsync=$prefix/bin/hashsync

fail() {
	echo "install-check: $*" >&2
	exit 1
}

step() {
	echo "install-check: $*" >&2
}

# Run a command, keep its standard error in the file $1 for a later look,
# and show it in the log too.
logged() {
	local log=$1
	shift
	local status=0
	"$@" 2>"$log" || status=$?
	cat "$log" >&2
	return "$status"
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
# The paths of the check go into TOML literal strings and into --ssh as they
# are, which only works without quotes, backslashes, and white space.
case $work in
*[!A-Za-z0-9/._-]*) fail "the temporary directory $work needs quoting" ;;
esac

step "linked libraries"
readelf=${READELF:-readelf}
needed=$("$readelf" --dynamic "$hashsync" | sed -n 's/.*(NEEDED).*\[\(.*\)\]$/\1/p')
for library in $needed; do
	case $library in
	ld-linux*.so.* | ld-musl-*.so.* | libc.so | libc.so.* | libm.so.* | libgcc_s.so.* | \
		libstdc++.so.* | libpthread.so.* | libdl.so.* | librt.so.*) ;;
	*) fail "$hashsync links $library" ;;
	esac
done
# grep reads all of readelf's output: with -q, it could exit at a match before
# readelf has written the rest, and readelf, killed by SIGPIPE, would fail the
# pipeline and pass the check.
if [ "$static" ] && { [ "$needed" ] || "$readelf" --program-headers "$hashsync" | grep INTERP >/dev/null; }; then
	fail "$hashsync is not linked statically"
fi

# With MIMALLOC_VERBOSE set, mimalloc writes lines that start with its name,
# the first one when it starts. Such lines must appear exactly when --mimalloc
# is given.
step "allocator"
verbose=$(MIMALLOC_VERBOSE=1 "$hashsync" --version 2>&1)
if grep -q '^mimalloc: ' <<<"$verbose"; then
	[ "$mimalloc" ] || fail "$hashsync allocates with mimalloc, but --mimalloc was not given"
elif [ "$mimalloc" ]; then
	fail "$hashsync does not allocate with mimalloc"
fi

step "man page"
man=$(echo "$prefix"/share/man/man1/hashsync.1*)
[ -f "$man" ] || fail "no man page in $prefix/share/man/man1"
mandoc -T lint -W warning "$man"

export HOME=$work/client
export XDG_CONFIG_HOME=$HOME/.config XDG_CACHE_HOME=$HOME/.cache
unset XDG_RUNTIME_DIR
server=$work/server
mkdir -p "$XDG_CONFIG_HOME/hashsync" "$server/.config/hashsync" "$server/run" \
	"$work/client-tree" "$work/server-tree" "$work/bin"

# One MiB of random data in a root of each host. Each stream carries it, so
# that every block of the stream is a block of the receiver's index. Each
# host reads only its own copy, as on two machines.
client_data=$work/client-tree/data
server_data=$work/server-tree/data
head -c 1048576 /dev/urandom >"$client_data"
cp "$client_data" "$server_data"
cat >"$XDG_CONFIG_HOME/hashsync/default.toml" <<EOF
block_size = "4KiB"

[[roots]]
path = '$work/client-tree'
EOF
cat >"$server/.config/hashsync/default.toml" <<EOF
block_size = "8KiB"
strong_hash = "sha256"

[[roots]]
path = '$work/server-tree'
EOF

# The remote command is the last argument of ssh, which the loop leaves in
# `command`. sshd runs it with the user's shell, in the home directory.
cat >"$work/bin/ssh" <<EOF
#!/bin/sh
for command; do :; done
export HOME='$server' XDG_CONFIG_HOME='$server/.config' \\
	XDG_CACHE_HOME='$server/.cache' XDG_RUNTIME_DIR='$server/run'
cd "\$HOME"
exec /bin/sh -c "\$command"
EOF
chmod +x "$work/bin/ssh"

step "local commands"
"$hashsync" --version
"$hashsync" update
"$hashsync" stats
"$hashsync" prune
"$hashsync" compact
"$hashsync" bench --block-size 64KiB --probe-threads 2

# The whole hash list before the first byte, and batch delivery, so that
# every block is a reference whatever the timing (DESIGN.md §6.3).
run=(--no-human-readable --wait --compression-threads 2 --ssh "$work/bin/ssh")

step "pull, with hashsync in PATH on both hosts"
PATH=$prefix/bin:$PATH logged "$work/pull.log" \
	hashsync ssh -n "${run[@]}" --batch-stdout host cat "$server_data" >"$work/pulled"
cmp "$server_data" "$work/pulled"
grep -qF 'stdout -- 1048576 B (0 B literal, 1048576 B -> ' "$work/pull.log" ||
	fail "the pull sent literal bytes"

step "push, with --hashsync-path"
logged "$work/push.log" \
	"$hashsync" ssh --dedup-stdin "${run[@]}" --batch-stdin --hashsync-path "$hashsync" \
	host 'cat >pushed' <"$client_data"
cmp "$client_data" "$server/pushed"
grep -qF 'stdin -- 1048576 B (0 B literal, 1048576 B -> ' "$work/push.log" ||
	fail "the push sent literal bytes"

step "passed"
