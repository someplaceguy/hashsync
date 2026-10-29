// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later
//
// The replaceable global allocation functions of C++ (`operator new` and
// `operator delete` in <new>), defined through mimalloc by its header
// `mimalloc-new-delete.h`. `build.rs` compiles this file into the
// executable with the Cargo feature `mimalloc` (DESIGN.md §9).
//
// Only C++ code calls these functions: RocksDB and libstdc++. The
// definitions in the executable replace the ones in libstdc++ for the whole
// process, as C++ requires: the linker exports them, and libstdc++ binds its
// own calls to them. So memory that RocksDB allocates and libstdc++ frees,
// or the reverse, stays in mimalloc. libc `malloc` is not replaced (see the
// test `mimalloc_does_not_interpose_libc_malloc` in `src/sys.rs`).
//
// When mimalloc cannot allocate, `operator new` ends the process with
// `abort` (mimalloc's C build cannot throw `std::bad_alloc`); the `nothrow`
// forms return null.
//
// RocksDB still calls libc `malloc` directly in a few places, and frees
// that memory with libc `free`: the handles of an LRU block cache (the
// hyper clock cache uses `new`), the compression buffers of the table
// builder, the shard arrays of block caches, and the copies that its C API
// returns to the caller, such as error messages and property values, which
// the `rocksdb` crate frees with `rocksdb_free`. Its gets through
// `rocksdb_get_pinned_cf`, iterators, and write batches use `new`.
// `librocksdb-sys` builds RocksDB without `ROCKSDB_MALLOC_USABLE_SIZE`: with
// it, RocksDB would ask libc `malloc_usable_size` about memory from `new`.

#include <mimalloc-new-delete.h>
