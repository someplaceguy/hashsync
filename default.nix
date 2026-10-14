{
  pkgs ? import <nixpkgs> {},
  shell ? false,
  # The Cargo feature `mimalloc`: allocate through mimalloc.
  mimalloc ? true,
}:

with pkgs;

let
  cargoToml = lib.importTOML ./Cargo.toml;

  # Cargo's default features, without `mimalloc` when the argument is false.
  features =
    if mimalloc then
      cargoToml.features.default
    else
      lib.remove "mimalloc" cargoToml.features.default;

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

  buildNoDefaultFeatures = true;
  buildFeatures = features;

  # RocksDB and, with the `mimalloc` feature, `src/cxx_allocation.cc` are
  # C++; LZ4, libzstd, and BLAKE3's SIMD code are C. The build scripts
  # compile them with the target compilers that rustPlatform provides.
  # bindgen generates RocksDB's bindings through libclang.

  nativeBuildInputs = [ rustPlatform.bindgenHook ] ++ lib.optionals shell [ clippy rustfmt ];

  doCheck = stdenv.buildPlatform.canExecute stdenv.hostPlatform;

  # The transfer tests run rsync with `hashsync ssh` as its remote shell.
  # The development shell gets it too, since it builds with checks.
  nativeCheckInputs = [ rsync ];

  meta = {
    description = "Run a command through SSH, deduplicating its stdout or its stdin against indexed blocks";
    mainProgram = "hashsync";
    platforms = lib.platforms.linux;
  };
}
