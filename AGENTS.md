docs/DESIGN.md contains the purpose of this project and the design decisions.

docs/PLAN.md contains the implementation plan for the project.

`default.nix` builds the package (`nix-build default.nix`). `shell.sh` enters the
development shell, or runs one command in it: `./shell.sh cargo test`.

`docs/hashsync.1` is the man page, in mdoc. Change it and `README.md` along
with the commands, options, and configuration keys that they describe; tests
check that each of them has an entry in the man page, and that no other entry
does.

`docs/hashsync.adoc` is generated from the man page; don't edit it. After a
change to the man page, a test rewrites it and fails once: run the tests again,
and keep the new file with the change.

`THIRD-PARTY-NOTICES` holds the licenses of the code that the binary bundles.
Don't edit it: `scripts/notices.sh` regenerates it with cargo-about, from
`scripts/about.toml` and `scripts/about.hbs`. Run the script after a change to
either of them, and keep the regenerated file with the change.
`scripts/release.sh` checks that the file is up to date, with
`scripts/notices.sh --check`.

When updating dependencies:

- Run `./shell.sh cargo update --verbose`. Inside the development shell,
  Cargo only picks versions that work with the shell's Rust, which is the same
  one the Nix build uses. With `--verbose`, it also lists the newer versions
  that need a change to `Cargo.toml` (the `Unchanged` lines). Upgrade those
  too, and fix whatever they break.
- Look at what changed in the crates that end up in the binary, and in `cc`
  and the `-sys` crates, which build the bundled C and C++ code. Some of the
  behavior that DESIGN.md describes comes from these crates, such as
  `gix-glob`'s pattern matching.
- Run `./shell.sh cargo clippy --all-targets`, again with
  `--no-default-features`, and `./shell.sh cargo test`.
- Build the package natively, statically and for aarch64, with the commands at
  the top of `default.nix`. The native and static builds also run the tests
  and `install-check.sh`. The static build links the C++ runtime statically,
  which has caused link failures before.
- Run `scripts/notices.sh`, and keep the regenerated `THIRD-PARTY-NOTICES`
  with the update. If cargo-about complains, a license file that
  `scripts/about.toml` pins by checksum has probably changed: check that the
  license is still the same, update the checksum, and run the script again.

The development shell is the package's derivation, so its output path changes
with `Cargo.lock`. nixpkgs' bindgen hook puts a `-frandom-seed` derived from
that path into `BINDGEN_EXTRA_CLANG_ARGS`, and Cargo rebuilds bindgen when
that variable changes. So after an update, the first build in each Cargo
profile compiles RocksDB's C++ code again, which takes several minutes and a
lot of memory. Don't run it alongside other heavy jobs, such as a `nix-build`.
