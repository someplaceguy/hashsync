#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")"

# Sandboxed agents often have no usable <nixpkgs> in NIX_PATH.
if ! nix-instantiate --eval -E '<nixpkgs>' >/dev/null 2>&1; then
	export NIX_PATH="nixpkgs=$HOME/src-nobackup/nixpkgs"
fi

if [ $# -eq 0 ]; then
	exec nix-shell --arg shell true --command fish default.nix
fi

exec nix-shell --arg shell true --run "$(printf '%q ' "$@")" default.nix
