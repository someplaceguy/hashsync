{
  pkgs ? import <nixpkgs> {},
  shell ? false,
}:

with pkgs;

let
  cargoToml = lib.importTOML ./Cargo.toml;

  # A development shell needs no source; a build takes the checkout without
  # ignored files and version-control directories.
  src =
    if shell then
      builtins.filterSource (path: type: false) ./.
    else
      nix-gitignore.gitignoreSourcePure [ "/.git/\n/.jj/\n" ./.gitignore ] ./.;
in

rustPlatform.buildRustPackage {
  pname = cargoToml.package.name;
  version = cargoToml.package.version;

  inherit src;

  # Dependencies come from the committed lockfile; no separate hash.
  cargoLock.lockFile = ./Cargo.lock;

  # SQLite, libzstd, and BLAKE3's SIMD code are C, compiled by the crates'
  # build scripts with the target C compiler that rustPlatform provides.

  nativeBuildInputs = lib.optionals shell [ clippy rustfmt ];

  doCheck = stdenv.buildPlatform.canExecute stdenv.hostPlatform;

  meta = {
    description = "Copy a remote command's output, replacing indexed local blocks";
    mainProgram = "hashsync";
    platforms = lib.platforms.linux;
  };
}
