#!/usr/bin/env bash
#
# Copyright 2026 Ricardo Correia
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Make a release of hashsync, for review before anything leaves this machine:
#
#     scripts/release.sh --licenses-ok VERSION
#
# VERSION is like 1.2.0 or 1.2.0-rc.1, without a leading v. Without
# --licenses-ok, the script only asks you to check the licenses that
# `scripts/about.hbs` adds by hand, for the Rust standard library and musl,
# since cargo-about doesn't update them. With it, in order, the script:
#
# - sets VERSION as the version of the crate in Cargo.toml and Cargo.lock,
#   which is also the version that `hashsync --version` prints;
# - commits both with jj, as "Release VERSION";
# - points the tag vVERSION at that commit;
# - writes the source tarball hashsync-VERSION.tar.zst of that commit to
#   the directory hashsync-releases next to the repository;
# - prints what is left to do by hand: pushing, and the GitHub release.
#
# The working copy must have no changes and no description, so that the
# release commit holds only the changes of the script. The commits after
# master must have descriptions, since `git push` publishes them as they are.
# THIRD-PARTY-NOTICES must be up to date, which the script checks with
# `scripts/notices.sh --check`.
#
# The tarball is reproducible: it depends only on the release commit and on
# the versions of git and zstd, which come from nixpkgs through the
# development shell. The script overrides the git settings that change what
# `git archive` writes, and zstd compresses on a single thread, so that its
# output doesn't depend on ZSTD_NBTHREADS.
#
# To fix a release before publishing it, amend the release commit, which
# the tag makes immutable in jj, and run the script again with the same
# VERSION. Since the version is already set, the script commits nothing; it
# moves the tag to the amended commit and replaces the tarball.

set -euo pipefail

# The Git remote that releases are pushed to, and its GitHub repository.
remote=someplaceguy
github=someplaceguy/hashsync

fail() {
	echo "release: $*" >&2
	exit 1
}

step() {
	echo "release: $*" >&2
}

licenses_ok=false
if [ "${1-}" = --licenses-ok ]; then
	licenses_ok=true
	shift
fi
if [ $# -ne 1 ]; then
	echo "usage: $0 [--licenses-ok] VERSION" >&2
	exit 2
fi
version=$1
# A SemVer version, as Cargo requires, but without build metadata (+...),
# which has no place in a tag or a file name. Numbers have no leading zeros,
# and each dot-separated identifier of the pre-release is a number or
# contains a letter or a hyphen.
number='(0|[1-9][0-9]*)'
identifier="($number|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
[[ $version =~ ^$number\.$number\.$number(-$identifier(\.$identifier)*)?$ ]] ||
	fail "'$version' is not a version like 1.2.0 or 1.2.0-rc.1"
tag=v$version

if ! $licenses_ok; then
	cat >&2 <<EOF
release: before making a new release, check whether the licenses that
scripts/about.hbs adds by hand, for the Rust standard library and musl, need
to be updated; the comment above them says which versions they come from.
Then run "$0 --licenses-ok $version" to make the release.
EOF
	exit 1
fi

cd "$(dirname "$0")/.."

[ "$(jj log -r @ --no-graph -T 'if(empty && !description, "clean")')" = clean ] ||
	fail "the working copy has changes or a description; commit or restore them first"
parents=$(jj log -r @- --no-graph -T 'commit_id ++ "\n"')
[ "$(wc -l <<<"$parents")" -eq 1 ] ||
	fail "the working copy has more than one parent"
undescribed=$(jj log -r 'master..@- & description(exact:"")' --no-graph -T 'change_id.short() ++ " "')
[ -z "$undescribed" ] ||
	fail "commits after master have no description: $undescribed"
# `git push` updates master@$remote, so a published release is in it, and
# its tag and commit must stay as they are.
[ -z "$(jj log -r "present($tag) & ::remote_bookmarks(remote=exact:\"$remote\")" --no-graph -T commit_id)" ] ||
	fail "$tag is already on $remote; make a new release instead"

git_dir=$(jj git root)
# `git archive` also takes attributes from this file, which no setting turns
# off.
[ ! -s "$git_dir/info/attributes" ] ||
	fail "$git_dir/info/attributes could change the tarball; empty it first"

step "checking THIRD-PARTY-NOTICES"
scripts/notices.sh --check

release_dir=$(realpath -m ../hashsync-releases)
tarball=$release_dir/hashsync-$version.tar.zst
trap 'rm -f "$tarball.part"' EXIT

step "setting the version to $version"
# The cargo commands below run offline, and only see the crates in Cargo's
# cache.
./shell.sh cargo fetch --locked
# The first version line of Cargo.toml is that of [package].
sed -i "0,/^version = \".*\"\$/s//version = \"$version\"/" Cargo.toml
./shell.sh cargo update --workspace --offline
# With --locked, this also fails if Cargo.lock doesn't match Cargo.toml.
pkgid=$(./shell.sh cargo pkgid --locked --offline)
[ "${pkgid##*[#@]}" = "$version" ] ||
	fail "the crate has the package ID '$pkgid' instead of version $version"

paths=(root-file:Cargo.toml root-file:Cargo.lock)
if [ -n "$(jj diff --name-only -r @ "${paths[@]}")" ]; then
	step "committing the release"
	jj commit -m "Release $version" "${paths[@]}"
fi

release=$(jj log -r @- --no-graph -T commit_id)

step "tagging the release as $tag"
jj tag set --allow-move -r "$release" "$tag"

step "writing $tarball"
mkdir -p "$release_dir"
# The file modes in the archive come from tar.umask, and line endings and
# filters from core.autocrlf and attributes, so the script sets them all.
./shell.sh bash -c '
	set -euo pipefail
	git -c tar.umask=0022 -c core.autocrlf=false -c core.attributesFile=/dev/null \
		--git-dir="$1" archive --format=tar --prefix="$2" "$3" |
		zstd -19 --single-thread --no-progress -q >"$4"
' bash "$git_dir" "hashsync-$version/" "$release" "$tarball.part"
mv -f "$tarball.part" "$tarball"

# GitHub marks the release as a pre-release with this query parameter.
prerelease=
[[ $version != *-* ]] || prerelease='&prerelease=1'

cat <<EOF

Release $version is ready for review:

    commit   $(jj log -r "$release" --no-graph -T 'change_id.shortest(8) ++ " " ++ commit_id.short() ++ " " ++ description.first_line()')
    tag      $tag
    tarball  $tarball
    sha256   $(sha256sum <"$tarball" | cut -d' ' -f1)

Review the release with "jj show --git $tag" and "tar --zstd -tvf $tarball".
To fix something, amend the release commit, which the tag makes immutable
(for example, "jj squash --ignore-immutable" for a fix in the working copy),
and run "$0 --licenses-ok $version" again.

Then publish it:

1. Move master to the release, and push both to GitHub:

       jj bookmark set master -r $tag
       git push $remote master tag $tag

2. Create the GitHub release of $tag, and attach the tarball to it:

       https://github.com/$github/releases/new?tag=$tag&title=hashsync+$version$prerelease
EOF
