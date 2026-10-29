#!/usr/bin/env bash
#
# Copyright 2026 Ricardo Correia
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Regenerate THIRD-PARTY-NOTICES, the licenses of the code that the hashsync
# binary bundles, with cargo-about, from `scripts/about.toml` and
# `scripts/about.hbs`:
#
#     scripts/notices.sh [--check]
#
# Run it after changing the dependencies or either of those files, and
# commit the regenerated THIRD-PARTY-NOTICES with the change. With --check,
# the script leaves the file as it is, and fails if it is out of date;
# `scripts/release.sh` runs that check before it makes a release.

set -euo pipefail

fail() {
	echo "notices: $*" >&2
	exit 1
}

check=false
if [ "${1-}" = --check ]; then
	check=true
	shift
fi
if [ $# -ne 0 ]; then
	echo "usage: $0 [--check]" >&2
	exit 2
fi

cd "$(dirname "$0")/.."

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# cargo-about reads the license files of the crates from Cargo's cache.
./shell.sh cargo fetch --locked
# When a clarification in `scripts/about.toml` no longer matches its crate,
# cargo-about only warns, and falls back to the license files that it finds
# on its own, which leave out the C and C++ libraries that the crate
# bundles. So the script fails on any message from it.
if ! ./shell.sh sh -c 'exec cargo about generate --frozen --fail -c scripts/about.toml -o "$1" scripts/about.hbs 2>"$2"' \
	sh "$tmp/notices" "$tmp/about.log" ||
	[ -s "$tmp/about.log" ]; then
	cat "$tmp/about.log" >&2
	fail "cargo about failed or warned; see its messages above"
fi

if $check; then
	cmp -s "$tmp/notices" THIRD-PARTY-NOTICES ||
		fail "THIRD-PARTY-NOTICES is out of date; run $0 to update it"
else
	mv "$tmp/notices" THIRD-PARTY-NOTICES
fi
