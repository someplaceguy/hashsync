# The hashsync package (DESIGN.md §11):
#
#     nix-build
#
# `pkgs` selects the platform. A cross build, here for 64-bit ARM:
#
#     nix-build --arg pkgs 'import <nixpkgs> { crossSystem = "aarch64-linux"; }'
#
# A static build, linked with musl, which runs on any Linux system of its
# architecture, such as a server without Nix, where `hashsync ssh
# --hashsync-path` names it:
#
#     nix-build --arg pkgs '(import <nixpkgs> {}).pkgsStatic'
#
# `shell.sh` enters the development shell of this expression.
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

  # The tests and the install check run the binary, which a cross build
  # cannot do.
  canRun = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
in

rustPlatform.buildRustPackage {
  pname = cargoToml.package.name;
  version = cargoToml.package.version;

  inherit src;

  # Dependencies come from the committed lockfile; no separate hash. With
  # `--locked`, a lockfile that no longer matches Cargo.toml fails the
  # build instead of being updated in it.
  cargoLock.lockFile = ./Cargo.lock;
  cargoBuildFlags = [ "--locked" ];
  cargoTestFlags = [ "--locked" ];

  buildNoDefaultFeatures = true;
  buildFeatures = features;

  # Rust links a static binary for musl as a static PIE, but nixpkgs builds
  # the static libraries of a static build without -fPIE, and the link then
  # fails on those of libstdc++. So a static binary is not position
  # independent. Cargo adds the flags of this variable to those that the
  # Cargo configuration of nixpkgs gives the target, such as
  # -Cforce-frame-pointers=yes; RUSTFLAGS would replace them.
  env = lib.optionalAttrs stdenv.hostPlatform.isStatic {
    "CARGO_TARGET_${stdenv.hostPlatform.rust.cargoEnvVarTarget}_RUSTFLAGS" =
      "-C relocation-model=static";
  };

  # RocksDB and, with the `mimalloc` feature, `src/cxx_allocation.cc` are
  # C++; LZ4, libzstd, and BLAKE3's SIMD code are C. The build scripts
  # compile them with the target compilers that rustPlatform provides.
  # bindgen generates RocksDB's bindings through libclang.

  nativeBuildInputs = [
    installShellFiles
    rustPlatform.bindgenHook
  ]
  ++ lib.optionals shell [
    clippy
    rustfmt
  ];

  doCheck = canRun;

  # The transfer tests run rsync with `hashsync ssh` as its remote shell,
  # and a test of the man page converts it to AsciiDoc with pandoc. The
  # development shell gets both too, since it builds with checks.
  nativeCheckInputs = [
    pandoc
    rsync
  ];

  postInstall = ''
    installManPage docs/hashsync.1
  '';

  # `install-check.sh` checks the binary as installed, after the fixup that
  # strips its debug information and shrinks its RPATH, and its man page,
  # which it lints with mandoc. The development shell gets mandoc too.
  doInstallCheck = canRun;
  nativeInstallCheckInputs = [ mandoc ];
  installCheckPhase = ''
    runHook preInstallCheck
    bash install-check.sh \
      ${lib.optionalString stdenv.hostPlatform.isStatic "--static"} \
      ${lib.optionalString mimalloc "--mimalloc"} \
      "$out"
    runHook postInstallCheck
  '';

  meta = {
    description = "Run a command through SSH, deduplicating its stdout or its stdin against indexed blocks";
    mainProgram = "hashsync";
    platforms = lib.platforms.linux;
  };
}
