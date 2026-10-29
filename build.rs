// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! With the Cargo feature `mimalloc`, compile `src/cxx_allocation.cc`,
//! which replaces the C++ allocation functions of the process with
//! mimalloc's (DESIGN.md §9).

fn main() {
    println!("cargo::rerun-if-changed=src/cxx_allocation.cc");
    if std::env::var_os("CARGO_FEATURE_MIMALLOC").is_none() {
        return;
    }
    // `libmimalloc-sys` gives the directory of the headers of the mimalloc
    // that it builds.
    let include = std::env::var_os("DEP_MIMALLOC_INCLUDE_DIR")
        .expect("libmimalloc-sys gives no include directory");
    // Optimized also in development builds, as RocksDB (`Cargo.toml`): the
    // functions are on the allocation path of all C++ code, and the
    // `_FORTIFY_SOURCE` of Nix's compiler wrapper warns without it.
    //
    // A static library that only the binary crate links (`src/sys.rs`), so
    // it goes to the binary and its unit tests, which link mimalloc, and not
    // to the integration tests, which do not. cc's metadata would link it
    // into all targets.
    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .opt_level(3)
        .include(include)
        .file("src/cxx_allocation.cc")
        .cargo_metadata(false)
        .compile("cxx_allocation");
    let out_dir = std::env::var("OUT_DIR").expect("Cargo gives no OUT_DIR");
    println!("cargo::rustc-link-search=native={out_dir}");
}
