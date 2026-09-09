# hashsync

This file is the design specification of `hashsync`. Contributors and AI agents
follow it and update it when the design changes. Section 10 logs rejected and
reverted decisions; section 12 lists the open questions.

Status (2026-09-10): design stage. No code exists yet.

## 1. Purpose

`hashsync` copies the output of a command that runs on a remote host to the
local host. It does not send bytes that already exist on the local disk.
Instead, the remote sends a short reference to a local file block. The local
program reads that block from disk, verifies it, and splices it into the output.

This pays off when the output of the remote command repeats data that the local
host already holds in indexed files, in any order and at any alignment: for
example a package tree or an archive stream produced after a rebuild in which
most files did not change.

The program is written in Go. One binary contains both roles: the local side
and the remote side. The local side starts the remote side through `ssh`.

## 2. Terminology

- **Local**: the host that runs `hashsync run` and receives the output.
- **Remote**: the host reached through SSH. It runs the user command and a copy
  of `hashsync`.
- **Block**: `block_size` bytes (default 32 KiB) of a local file, at an offset
  that is a multiple of `block_size`. Only complete blocks are indexed; the
  shorter tail of a file is not a block.
- **Weak hash**: the 64-bit value `mix64(P)`, where `P` is the polynomial hash
  of a block or window (section 5.1). It selects candidates. It does not prove
  equality.
- **Index**: an SQLite database on the local host. It has one row per block with
  the block's weak hash, its Blake3 hash, and the inode and block number that
  hold it.
- **Root**: a configured path that the indexer scans recursively.
- **Immutable root**: a root whose content never changes while the root exists.
- **Filesystem id**: the persistent identity of a filesystem (section 4.3).
- **Inode handle**: the persistent identity of an inode inside its filesystem,
  as returned by `name_to_handle_at` (section 4.3).
- **Hash list**: the sorted, compressed list of the weak hashes of all indexed
  blocks (section 5.4). The local sends it to the remote at the start
  of each run.
- **Hash table**: the in-memory fingerprint table that the remote builds from
  the hash list (section 5.5).
- **Window**: the `block_size` bytes of command output that start at a given
  offset. The remote computes the weak hash of the window at every offset.
- **Literal**: output bytes that the remote sends as they are.
- **Reference**: a message that tells the local to insert one indexed block
  instead of literal bytes.
- **Run**: a stretch of at least `block_size` identical bytes in the output.
  The remote sends it as one RLE frame (byte value and length) instead of
  literals or references, and the local synthesizes it.
- **Query**: a message from the remote that asks: "do you have a block with this
  weak hash and this Blake3 hash?"

## 3. One run, step by step

1. The local loads `config.toml`, opens the index, and updates it (section 4).
   `--no-update` skips the update.
2. The local exports the hash list from the index, or loads the cached one
   (section 5.4).
3. The local opens two SSH sessions to the remote host: a **data session** and a
   **control session** (section 6.1).
4. The local sends the hash list over the data session. The remote decodes it
   and builds its hash table.
5. The remote starts the user command and reads its stdout.
6. The remote computes the weak hash at every offset of the output and looks it
   up in its hash table.
   - Table miss: the byte at this offset is a literal. Continue at the next
     offset.
   - Table hit: the remote computes the Blake3 hash of the window and sends a
     query over the control session. It continues at the offset after the
     window (section 6.3).
   - Window of one repeated byte: the remote sends one RLE frame for the whole
     run and continues after it, without a query (section 6.3).
7. The local answers each query. It looks the weak hash up in the index, keeps
   the candidates whose stored Blake3 equals the queried one, reads a candidate
   block from disk, verifies its Blake3 hash, and keeps the verified bytes in
   memory. It answers "yes" if a block verified and "no" otherwise
   (section 6.4).
8. The remote sends the resolved output over the data session, in order: literal
   runs, and one reference for each window that got a "yes". A window that got a
   "no" is sent as literal bytes.
9. The local writes literal runs, pinned block bytes, and synthesized runs to
   its stdout, in order.
10. When the command exits, the remote sends the exit status. The local exits
    with the same status.

Design principle: the index, the hash list, and the hash table are hints. The
local verifies every spliced block with Blake3 against bytes it read from local
disk, in memory, before it uses them. Stale index entries, files changed after
indexing, and weak hash collisions cost efficiency, never correctness.

## 4. The local index

### 4.1 What gets indexed

- Only regular files. Symbolic links, devices, sockets, and FIFOs are ignored.
  The indexer never follows symbolic links, neither to directories nor to files
  (`O_NOFOLLOW`, `AT_SYMLINK_NOFOLLOW`). This rules out infinite recursion
  through symlink loops.
- Each directory once per scan. The walker keeps the set of visited directories,
  keyed by `(filesystem id, inode number)`, for the duration of the scan. This
  stops the cycles that bind mounts can create.
- Only complete blocks. A file of size S has `floor(S / block_size)` blocks.
  A file smaller than `block_size` has no blocks and is skipped entirely: it
  gets no inode row and no path row. Such files are stat'ed again at every scan
  of a mutable root, which is cheap. The remote scan works with windows of
  exactly `block_size` bytes, so a shorter tail could never be matched
  (section 10). An indexed inode that shrinks below `block_size` is deleted at
  the next scan.
- Each inode once per scan. When a scan meets an inode a second time (a hard
  link), it only records the new path.
- Constant blocks, whose bytes are all equal, are not stored. The remote sends
  a run of one byte value as one RLE frame (section 6.3), so such blocks never
  need a lookup. This also removes the many rows that zero padding would
  produce.
- Holes in sparse files are not read. A block that lies entirely inside a hole
  is neither read nor stored (section 4.5); it would be a constant block anyway.

### 4.2 Roots and change detection

Roots come from `config.toml`. A root path can be a glob. Each match is its own
root. The indexer expands globs at every update with `filepath.Glob`, so the syntax
is that of Go's `filepath.Match`. Note that `*` also matches names that start
with a dot: `/nix/store/*` matches `/nix/store/.links`. Use `exclude` for such
entries. A root that is a regular file is indexed as that single file.

Roots are opened by path at every update. Together with the SQLite file and the
`ssh` processes, this is the only path-based filesystem access of the program
(section 4.5). Roots are scanned in parallel.

Mutable root (`immutable = false`, the default):

- Every update traverses the root recursively.
- For each regular file, the indexer compares `(size, ctime)` with the stored
  inode row. If both are equal, the file is not read again. Otherwise the
  indexer hashes the whole file and replaces all its block rows. This is the
  only criterion for hashing a file again.
- Path rows under the root that the traversal did not see are deleted.

Immutable root (`immutable = true`):

- The indexer traverses the root once, when it first sees it.
- While the root exists, the indexer never traverses it again and never stats
  the files below it. It only checks that the root still exists (one open).
- When the root disappears, its path rows are deleted.
- A file below an immutable root can also be reachable through a mutable root
  (for example through a hard link). It is then checked like any other file of
  that mutable root. If it changed, its block rows are replaced, and every path
  of the inode sees the new rows.

In both cases, an inode with no remaining path rows is deleted together with its
block rows. Pruning is immediate: a root that vanished, or that is temporarily
unavailable (for example an unmounted filesystem), loses its rows at the next
update and is indexed again when it returns. The index is a cache, so this costs
one extra scan and nothing else.

Use `ctime`, not `mtime`. Programs can set `mtime`. Only the kernel changes
`ctime`.

An immutable root that is scanned while it is still being written is indexed in
an incomplete state and never scanned again. This costs efficiency only (see the
design principle in section 3). Choose immutable roots accordingly.

Per-root options:

- `exclude`: glob patterns in the same syntax. A pattern that starts with `/`
  is matched against the absolute path of an entry, including the glob matches
  of `path` themselves; any other pattern is matched against the base name.
  Matching entries and everything below them are skipped.
- `one_filesystem`: do not cross mount points. An entry whose `stx_mnt_id`
  differs from its parent's is skipped, together with everything below it.

### 4.3 Identity of filesystems and inodes

`(st_dev, st_ino)` is not a usable identity. `st_dev` can change between boots
(XFS, btrfs subvolumes, and other filesystems with anonymous device numbers),
which would make every indexed file look new. Inode numbers are reused after
deletion. The index therefore identifies:

- A **filesystem** by the UUID that the `FS_IOC_GETFSUUID` ioctl returns
  (Linux 6.9 and newer). The request is `_IOR(0x15, 0, struct fsuuid2)` with
  `struct fsuuid2 { u8 len; u8 uuid[16]; }`, which is `0x80111500` on amd64
  and arm64. `golang.org/x/sys/unix` (v0.48.0) has no constant for it; define
  the number and the 17-byte struct in the project and call it through
  `unix.Syscall(unix.SYS_IOCTL, ...)`. A filesystem that does not implement the
  ioctl (`ENOTTY`) falls back to its mount id from `statx` (`stx_mnt_id`; the
  program requests `STATX_MNT_ID_UNIQUE`, and a kernel before 6.8 returns the
  non-unique `STATX_MNT_ID` value and leaves the unique bit clear in
  `stx_mask`), stored with a tag so that it cannot collide with a UUID. A
  mount id is not stable across boots, so such a filesystem is indexed again
  after a reboot.
- An **inode** by the file handle that `name_to_handle_at` returns
  (`unix.NameToHandleAt`). The handle is unique within its filesystem and
  persistent, and it includes the inode generation, so a reused inode number
  gets a different handle. Handles are opaque byte strings of up to 128 bytes,
  usually 8 to 40. If the filesystem does not support handles (`EOPNOTSUPP`),
  fall back to the inode number from `statx`, stored with a tag.

The index assigns a 64-bit integer id to every filesystem and to every inode and
uses these ids as foreign keys. The pair `(fs_id, handle)` is unique.

Cost control: the ioctl runs once per mount seen in a scan, not once per inode.
`statx`, which the walker needs anyway for size and ctime, returns `stx_mnt_id`
for every entry. Only when it differs from the parent's does the walker resolve
the mount to a filesystem id (or skip the entry, with `one_filesystem`). The
walker caches `mount id -> fs_id` for the scan. `name_to_handle_at` runs once
per regular file; it is a metadata-only call with no disk read.

### 4.4 Storage

The index is one SQLite file, by default
`$XDG_STATE_HOME/hashsync/index.sqlite`. Suggested schema:

```sql
CREATE TABLE meta        (key TEXT PRIMARY KEY, value BLOB NOT NULL);
  -- keys: schema_version, block_size, weak_hash_id, generation
CREATE TABLE filesystems (id INTEGER PRIMARY KEY, fsid BLOB NOT NULL UNIQUE);
CREATE TABLE roots       (id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE,
                          immutable INTEGER NOT NULL);
CREATE TABLE inodes      (id INTEGER PRIMARY KEY, fs_id INTEGER NOT NULL,
                          handle BLOB NOT NULL, size INTEGER NOT NULL,
                          ctime_ns INTEGER NOT NULL, UNIQUE (fs_id, handle));
CREATE TABLE paths       (root_id INTEGER NOT NULL, rel_path TEXT NOT NULL,
                          inode_id INTEGER NOT NULL,
                          PRIMARY KEY (root_id, rel_path)) WITHOUT ROWID;
CREATE INDEX paths_by_inode ON paths (inode_id);
CREATE TABLE blocks      (inode_id INTEGER NOT NULL, block_no INTEGER NOT NULL,
                          weak INTEGER NOT NULL, blake3 BLOB NOT NULL,
                          PRIMARY KEY (inode_id, block_no)) WITHOUT ROWID;
CREATE INDEX blocks_by_weak ON blocks (weak);
```

Rules:

- `meta` stores `block_size` and `weak_hash_id` (algorithm name, multiplier,
  and mixer, section 5.1). If either differs from the configuration or from the
  binary, the program drops all tables and rebuilds the index.
- Use WAL mode with `synchronous=NORMAL`. The database stays consistent after a
  crash or a power loss. The last transactions can be lost, which is acceptable
  for an index that can be rebuilt.
- A transaction contains whole inodes only: the inode row, all its block rows,
  and its path rows. Put many inodes into one transaction (commit every few
  thousand files or every 30 seconds, whichever comes first) to amortise the commit
  cost. A crash then leaves the index at a consistent earlier state.
- Index the weak hash alone. The polynomial hash has no systematic collisions,
  so rows that share a weak value hold identical content, apart from stale
  entries. Identical blocks (for example zero blocks) legitimately share one
  weak value; the lookup uses `LIMIT` and stops at the first candidate that
  verifies, so their number does not matter.
- Store `weak` as the int64 bit pattern of the uint64 value.
- Only inodes with at least one stored block have rows (sections 4.1 and 4.5).
- Keep the Blake3 hash in the row, not in an index. It rejects stale candidates
  without disk reads. Option: store only the first 16 bytes; the verification
  still uses the full 32-byte hash from the query.
- Expected size: about 55 bytes per block row plus about 20 bytes per index
  entry, about 75 bytes per block, or 0.23 % of the indexed data at 32 KiB
  blocks. A 256 GiB tree (8M blocks) needs about 600 MB.
- `generation` increments after every update that changed something. The cached
  hash list file records the generation it was built from.

### 4.5 Traversal and hashing

Follow the design of gdu (`pkg/analyze/parallel.go`, version 5.36.1): one
goroutine per directory, limited by a semaphore of `2 * GOMAXPROCS`. Roots are
scanned in parallel like any other directories.

Descriptor-relative operations: apart from opening the roots, the SQLite file,
and the `ssh` processes, every filesystem operation is relative to an open
descriptor of a directory or a file. This removes the races of path-based
traversal (renames, symlink swaps) and the cost of long path lookups.

- Directory: `openat(dirfd, name, O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)`,
  then `getdents64` on the descriptor (`(*os.File).ReadDir` on `os.NewFile(fd)`,
  or `unix.Getdents`).
- Every entry: `statx(dirfd, name, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW,
  STATX_TYPE | STATX_SIZE | STATX_BLOCKS | STATX_CTIME | STATX_INO |
  STATX_MNT_ID_UNIQUE)`.
  Every `statx` call of the program uses these flags and this mask: with a name
  the call does not follow a final symlink, with an empty name it describes the
  descriptor itself, and `stx_mask` tells whether the kernel returned the unique
  mount id. Skip everything that is not a directory or a regular file, regular
  files smaller than `block_size`, and whatever `exclude` and `one_filesystem`
  rule out.
- Unchanged check for a regular file: `name_to_handle_at(dirfd, name, 0)` gives
  the handle; look up `(fs_id, handle)` in the index; if `(size, ctime)` match,
  record the path and stop. Optional shortcut: look the path up first and skip
  the handle call when the path's inode row already matches.
- Changed or new file: `openat(dirfd, name, O_RDONLY|O_NOFOLLOW|O_CLOEXEC)`, then
  `statx(fd, "", ...)` with the same flags and mask, and
  `name_to_handle_at(fd, "", AT_EMPTY_PATH)` on the open descriptor, so that size, ctime, and handle
  describe the object that is hashed. Read the file in `block_size` chunks with
  `pread` into one reusable buffer per worker and compute the weak hash and
  Blake3 for each block. A block whose bytes are all equal is skipped before
  hashing (section 4.1); the check is a word-wise comparison against the first
  byte.
- Sparse files: when `stx_blocks * 512 < size`, the file may have holes. Find
  them with `lseek(SEEK_HOLE)` and `lseek(SEEK_DATA)` before reading. A hole
  `[h0, h1)` makes the indexer skip every block `k` with `k * B >= h0` and
  `(k + 1) * B <= h1`; these blocks get no rows. A block that overlaps a hole
  only partially is read in full, and the kernel returns zeros for the hole
  part. Example with `B` = 32 KiB: a hole from 10 KiB to 200 KiB skips blocks 1
  to 5 (32 KiB to 192 KiB); blocks 0 and 6 are read. When the allocated size
  covers the file size there are no holes and no seek calls are made (a
  compressing filesystem can report a smaller allocation; that only costs the
  seek calls). A filesystem without hole support reports one data extent, so
  the procedure is safe everywhere.
- Filesystem id: when an entry's `stx_mnt_id` differs from its parent's, call
  `FS_IOC_GETFSUUID` on the entry's descriptor (section 4.3).
- Call `posix_fadvise(SEQUENTIAL)` before a file is read and
  `POSIX_FADV_DONTNEED` after it is hashed. A full scan must not evict the page
  cache of the whole machine.

Pipeline:

- Directory walkers emit the regular files that need hashing.
- Hashing workers (2 to 4 times `GOMAXPROCS`, bounded by `RLIMIT_NOFILE`
  together with the walkers' directory descriptors) hash the files. SSDs profit
  from this read parallelism.
- One writer goroutine receives finished inodes and writes them in batched
  transactions. SQLite allows one writer at a time; this design never contends
  for the write lock.
- Two in-memory sets per scan, both keyed by `(fs_id, inode number)`: visited
  directories (cycle protection) and seen regular files (hard links).

## 5. Hashes, hash list, and hash table

### 5.1 Weak hash: mixed polynomial rolling hash modulo 2^64

Both sides must compute identical values, so the definition is part of the
protocol. All arithmetic is unsigned 64-bit, modulo 2^64.

- `a` is a fixed odd multiplier compiled into the binary, with `a ≡ 5 (mod 8)`.
  Example: `0x9E3779B97F4A7C15`. This class has the maximal multiplicative
  order 2^62 and the smallest entropy loss on constant blocks.
- Polynomial hash of a window `w[0..B)`, with `B = block_size`:
  `P = sum over i of w[i] * a^(B-1-i)`. Horner's rule computes it: start with
  `P = 0`; for each byte `P = P*a + b`.
- Weak hash: `mix64(P)` (section 5.2). The index, the queries, and the hash list
  all carry this mixed value.
- Roll one byte (`out` leaves the window, `in` enters it):
  `P' = a*P - Tout[out] + in`, with the 256-entry table `Tout[b] = b * a^B`,
  computed at start-up for the configured `B`. Rolling operates on
  `P`; `mix64` is applied to `P` at every offset before the table probe.

`meta.weak_hash_id` records the algorithm, `a`, and the mixer.

The dependency chain per byte is one multiply and one add, about 4 to 5 cycles,
so about 1 GB/s per core. This is not the bottleneck of the remote scan
(section 6.3). If it ever is, run k interleaved streams. Stream j handles the
offsets congruent to j modulo k and advances k bytes per step:

```
P(o+k) = a^k * P(o) - sum(m=0..k-1) Tout_m[w[o+m]] + sum(m=0..k-1) Tin_m[w[o+B+m]]
Tout_m[b] = b * a^(B+k-1-m)        Tin_m[b] = b * a^(k-1-m)
```

Only the multiply and one add stay on the chain; the table sums are independent.

Block hash in the indexer (no rolling): split the block into k parts of
`B/k` bytes, run Horner's rule on each part, and combine with
`P = sum over j of P(part_j) * a^((k-1-j)*B/k)`. This gives k independent
chains.

Collision behaviour on structured, non-adversarial data: `mix64` is a bijection,
so the collisions of the weak hash are exactly those of `P`. A constant block
`c` has `P` equal to `c` times an odd multiple of `B`, so all 256 constants are
distinct. A block with period `p` (a power of
two) loses only `log2(B) - log2(p)` high bits of entropy and is otherwise
random-like. Swapping bytes changes `P` unless the distance is a multiple of the
multiplicative order of `a`, which never happens. The only known structural
collision (Thue-Morse strings of 2 KiB) does not occur in real data.

Rejected alternatives (see also section 10): buzhash (cyclic polynomial) has
algebraic collisions; the Rabin fingerprint in GF(2^64) has provable uniformity,
but its per-byte table lookup sits on the dependency chain (about 7 cycles per
byte) and is not needed.

### 5.2 `mix64`: the bit mixer

`mix64` is a fixed function that scrambles one 64-bit word so that every output
bit depends on every input bit. Such a function is also called a hash finaliser.
It is the last step of the weak hash (section 5.1) and a protocol constant, like
the multiplier `a`. The chosen function is the splitmix64 finaliser (Stafford's
variant 13 of the MurmurHash3 `fmix64`):

```go
func mix64(z uint64) uint64 {
	z ^= z >> 30
	z *= 0xBF58476D1CE4E5B9
	z ^= z >> 27
	z *= 0x94D049BB133111EB
	z ^= z >> 31
	return z
}
```

Why it exists: the bits of the polynomial hash `P` are not uniform. The low `j`
bits of `P` depend only on the low `j` bits of the input bytes; bit 0 is the
parity of all bytes. A constant block has `P` equal to a multiple of `B`, so
its low `log2(B)` bits are zero; a block with period 2^m has `log2(B) - m` zero
low bits;
and such blocks form arithmetic progressions in the hash space. Three parts of
the design assume uniformly random keys: the slot selection in the remote table
(probe lengths), the false positive rate of the fingerprints, and the truncation
to `W` bits with Golomb-Rice coding of the gaps. `mix64` makes these assumptions
hold for any input.

Properties:

- Bijective. Every step is invertible: an xor-shift can be undone, and
  multiplication by an odd constant is invertible modulo 2^64. It never merges
  two distinct values of `P`.
- Avalanche. Flipping one input bit flips each output bit with probability close
  to one half.
- Cheap. Two multiplies and three xor-shifts, outside the rolling hash's
  dependency chain.

It never affects correctness. Without it, only efficiency would depend on the
data. Rolling must operate on the unmixed `P`, because `mix64` is not
rolling-compatible; the remote therefore applies it once per offset, after the
roll and before the probe.

### 5.3 Blake3

The 32-byte Blake3 hash identifies the content of a block.
The local always recomputes it on the bytes it read from disk before it uses a
block. Treat Blake3 equality as byte equality.

### 5.4 The hash list

The local exports the set of weak hashes of all indexed blocks as a sorted,
compressed list:

1. `SELECT DISTINCT weak FROM blocks` through the `blocks_by_weak` index.
   Constant blocks have no rows (section 4.1), so their hashes are absent.
2. Keep the top `W = 48` bits, sort, and remove duplicates.
3. Encode the gaps between consecutive values with Golomb-Rice coding. Use the
   Rice parameter `k = floor(log2(2^W / N))`, so the average gap is between
   2^k and 2^(k+1). Each value then costs about `k + 2` bits.
4. Prefix a header: format version, `W`, `N`, `k`.

Why this shape: sorted uniform values have gaps with about `W - log2(N) + 1.4`
bits of entropy, so the sorted list compresses well while random fingerprints
(as in a filter) do not. The remote only needs `log2(slots)` index bits plus a
fingerprint; 48 bits leave an effective fingerprint of about 25 bits for 8M
keys (section 5.5). `W = 64` removes the truncation at about 2 more bytes per
key; the 32-bit fingerprints of the table still produce a few false hits. Sizes
for a 256 GiB tree (8M blocks of 32 KiB):

| Transfer format | Size | Cache lines per lookup | False hits per GiB scanned |
|---|---|---|---|
| Sorted 48-bit prefixes, Golomb-Rice gaps (chosen) | 27 MiB | ~1 | ~400 |
| Sorted 64-bit values, Golomb-Rice gaps | 43 MiB | ~1 | ~3 |
| Raw sorted 64-bit values | 64 MiB | ~1 | ~3 |
| Binary fuse filter, 32-bit fingerprints (rejected) | 36 MiB | 3 | 0.25 |

A false hit costs one Blake3 of the window and one query of about 60 bytes, so
even thousands per GiB would not matter. Decoding 8M values takes tens of
milliseconds. Cache the encoded list next to the database together with
`meta.generation` and rebuild it only when the generation changed. The sort of
8M values costs well under a second.

### 5.5 The remote hash table

The remote decodes the hash list into a custom open-addressing table of 32-bit
fingerprint slots. It is a flat `[]uint32` obtained from `mmap`, not a Go
`map`: a `map[uint64]struct{}` needs about 10 to 12 bytes per key, hashes the
key a second time, touches control bytes and slots, and gives no control over
alignment, huge pages, or batching.

- Layout: buckets of 16 slots, one bucket per 64-byte cache line.
  `buckets = ceil(N / (16 * 0.7))`; map a value to a bucket with multiply-shift
  range reduction on its top bits (`(v * buckets) >> W`).
- Fingerprint = the low 32 bits of the value; replace 0 by 1, because 0 marks an
  empty slot. Insert into the first empty slot of the bucket; when the bucket is
  full, continue in the next bucket. A lookup scans the bucket until it finds
  the fingerprint or an empty slot, and continues in the next bucket only when
  the bucket is full.
- At load factor 0.7 about 10 % of the buckets are full, so about 90 % of the
  lookups touch exactly one cache line. Load factor 0.6 brings that to about
  97 % for a sixth more memory.
- False positives: the bucket index and the fingerprint overlap in about 7 bits
  of the 48-bit value, so a fingerprint comparison inside a bucket has about
  `W - log2(16 * buckets)` independent bits, 24.6 for 8M keys. A negative
  lookup compares against about 11 occupied slots and false-positives with
  probability about 4e-7.
- Memory: about 5.7 bytes per key at load factor 0.7: 46 MiB for 8M keys
  (256 GiB), 183 MiB for 32M keys (1 TiB).
- Allocate the table with `mmap` and `madvise(MADV_HUGEPAGE)`. With 4 KiB pages
  every probe also misses the TLB.
- Probe in batches. Go has no prefetch intrinsic. Compute the weak hashes and
  bucket addresses for 32 consecutive offsets, load the first word of each
  bucket into a small array in one simple loop (the loads are independent, so
  the CPU overlaps the cache misses), then scan the buckets in a second loop.

Why a table and not a filter: when the structure is larger than the last-level
cache, throughput is bounded by memory-level parallelism, so it scales with the
number of cache lines touched per lookup. A binary fuse filter touches 3
independent lines per lookup; this table touches about 1. Expect 2 to 3 times
the scan rate. Measure both with a microbenchmark before the first release; each
is about 100 lines.

## 6. The transfer protocol

### 6.1 Transport: two SSH sessions

Why two channels: the data stream can saturate the link with literal bytes. A
query that waits behind queued data waits behind the sshd channel window (about
2 MiB in OpenSSH) plus the kernel socket buffers (several MiB). On a 20 Mbit/s
link this adds seconds of latency to every query. The remote can only scan
`lookahead` bytes ahead while queries are outstanding (section 6.3), so the
throughput is capped at about `lookahead / query_latency`. On a stream with a
high deduplication ratio this cap dominates. A separate TCP connection keeps the
query latency near the network round-trip time.

SSH connection multiplexing (`ControlMaster`), port forwarding (`-L`, `-R`), and
stderr all share one TCP connection with the data, so they do not solve this.
Two independent `ssh` processes do.

The remote shell command comes from the configuration (`ssh`, default `ssh`),
from a per-host override, or from `--ssh` on the command line, like rsync's
`--rsh`. It is split like a shell would split it, so a port or an identity file
goes there: `ssh -p 2222 -i ~/.ssh/key`. The program then appends
`-o Compression=no` for both sessions, because the data session is already
zstd-compressed (section 6.2) and the control session is incompressible; this
overrides a `Compression yes` from the user's `ssh_config`. `ssh` uses the
first value it obtains for an option, so an explicit `-o Compression=yes` in
the configured command still wins. The `HOST` argument of `hashsync run` is
appended last and unchanged, so `user@host` and `ssh_config` aliases work.

Rendezvous:

1. The local generates a random token `T` and starts two processes at the same
   time (`SSH` stands for the configured command):
   - data session: `SSH HOST hashsync serve --token T`
   - control session: `SSH HOST hashsync attach --token T`
2. `serve` creates a Unix socket `$XDG_RUNTIME_DIR/hashsync/T.sock` (fallback: a
   private directory under `/tmp`), mode 0600, and listens on it. It writes a
   HELLO frame to stdout, waits for the `attach` connection (timeout 60 s), reads
   the hash list from stdin, builds the hash table, and then starts the user
   command. It may start the command earlier and buffer its output, up to the
   look-ahead size.
3. `attach` connects to the socket. It retries until the socket exists or the
   timeout expires. Then it copies its stdin to the socket and the socket to its
   stdout. It has no other logic.
4. The user command gets `/dev/null` as stdin. It inherits stderr, so `ssh`
   forwards it to the local terminal.
5. After the END frame, the local closes both sessions. `serve` kills the child
   if it still runs and removes the socket.

Notes:

- If the user's ssh configuration enables `ControlMaster`, both sessions share
  one TCP connection. The program still works; only the latency benefit shrinks.
  Document this for users.
- Provide `--single-connection` as a fallback: one session, both logical streams
  multiplexed in the frame layer, and a remote writer that drains control frames
  before data frames. `github.com/hashicorp/yamux` can also do this, but the
  frame layer below is enough.
- Run the remote shell as a subprocess. Do not use `golang.org/x/crypto/ssh`.
  The subprocess honours the user's configuration, agent, `ProxyJump`, and host
  key checks.
- The remote host needs the same `hashsync` version in `PATH`, or the path given
  by `remote_command` in the configuration. Both sides check versions in HELLO.

### 6.2 Frames

Every frame is `type u8, length u32 (little endian), payload`. Suggested types:

| Session | Direction | Types |
|---|---|---|
| data | remote to local | HELLO, LIT (bytes), REF (qid), RLE (byte u8, length u64), END (exit status, error text) |
| data | local to remote | HELLO, HASHLIST (encoded hash list, can span several frames) |
| control | remote to local | HELLO, QUERY (qid u32, offset u64, weak u64, blake3 [32]byte) |
| control | local to remote | HELLO, ANSWER (qid u32, found u8) |

HELLO carries the protocol version, the block size, `weak_hash_id`, the hash
list width `W`, and the program version. Any mismatch is a fatal error. The
local's HELLO also carries the compression level and thread count it wants the
remote to use.

Compression. After the HELLO exchange, the remote-to-local direction of the
data session is one zstd stream, and all later frames (LIT, REF, END) travel
inside it. Compression is done by libzstd (section 9). Rules:

- The remote flushes the compressor (`ZSTD_e_flush`) whenever its outgoing
  queue runs empty, so a pause in the command output or a wait for answers
  never leaves frames stuck in the compressor. It ends the stream after END.
  Flushes cost a little ratio and happen only when the stream is idle anyway.
- The level comes from `compression_level` in the local configuration (zstd
  scale 1 to 19, 0 disables). Default 9. Levels 6 to 9 keep most of the ratio
  gain over the zstd default of 3 at roughly 100 MB/s per thread on current x86
  cores; above 12 the ratio grows by a few percent for several times the CPU.
  `compression_threads` sets libzstd's worker threads (default 0: half the
  remote's cores, at least one), so the compressor keeps up with a gigabit link
  at level 9 on a few cores. Lower the level when the remote's CPU, not the
  network, is the bottleneck; raise it on slow links.
- Only literal bytes cost compression time. REF and RLE frames are a few bytes
  each. Runs shorter than `block_size` stay in the literals, where zstd encodes
  them cheaply.
- Nothing else is compressed: the control session carries random hashes and
  one-byte answers, and the hash list is already entropy-coded.
- The local decompresses with the streaming reader; it yields output block by
  block, so frame parsing is not delayed by more than one zstd block.

### 6.3 Remote scanner

State: a ring buffer of command output with at least `lookahead + block_size`
bytes (default `lookahead = 256 MiB`), a scan position, and an emit position.
Everything between the emit position and the scan position is literal bytes or a
pending window.

The scanner also tracks the start of the current run of identical bytes, with
one comparison per byte as bytes enter the window.

Scan loop, at position `p` with window `[p, p + B)` available:

- Constant window (the run that ends at the newest byte of the window started
  at or before `p`): no hash and no probe. Record a resolved RLE item that
  covers the run from `p` to its end and continue at the end of the run. The
  run may grow as more output arrives; a run longer than the look-ahead is sent
  as several RLE frames. After the run, compute the hash of the next window
  from scratch (Horner's rule over `B` bytes) and resume rolling.
- Table miss: `p += 1`.
- Table hit: compute the Blake3 hash of the window, send a QUERY, record the
  pending window, and continue at `p + B`. This is optimistic: a hit is almost
  always a real match, so the scanner does not evaluate the offsets inside the
  window.
- Stop when `scan - emit >= lookahead`, and wait for answers.

Emit loop, in stream order:

- Send LIT for the literal bytes before the next pending item.
- RLE item: send RLE(byte, length) and advance the emit position by the length.
  It needs no answer and no pinned entry.
- Answer "yes": send REF(qid) and advance the emit position by `B`.
- Answer "no": send the window as LIT. Optional refinement: rescan from `p + 1`
  instead. If you implement this, add a small LRU cache of recent negative
  Blake3 answers. Without it, a run of constant bytes (all windows share one
  Blake3 value) produces one query per offset.

Answers can arrive out of order; keep the pending list ordered by offset. The
last bytes of the stream (fewer than `B`) are literal unless they belong to a
run.

CPU cost per byte: the rolling hash (4 to 5 cycles on the dependency chain),
`mix64` (section 5.2) and the slot computation (independent of the chain), and
one table probe. The probe dominates when the table is larger than the
last-level cache: about one cache line per byte, which gives roughly 120 to
180 MB/s per core from DRAM and several hundred MB/s per core when the table
fits in L3. Batch the probes as described in section 5.5, split the stream into
segments with `B` bytes of overlap, and scan the segments on several cores.
Compression (section 6.2) runs in libzstd's own threads and touches literal
bytes only, so on a stream with a high deduplication ratio it costs little.
Inside a run the scanner neither hashes nor probes, so a long run of one byte
costs a memory scan only.

### 6.4 Local answering and reconstruction

For each QUERY:

1. `SELECT inode_id, block_no FROM blocks WHERE weak = ? AND blake3 = ? LIMIT 128`.
   SQLite uses the `blocks_by_weak` index and filters the rows by Blake3. The
   candidates are checked in order until one verifies. A candidate costs one
   `block_size` read and one Blake3, mostly from the page cache; a "no" costs
   `block_size` bytes of network. The high limit skips over stale rows (deleted
   or changed files).
2. For each candidate: take one `(root_id, rel_path)` of the inode and open it
   relative to the root's descriptor, which the run opens once and caches:
   `openat2(rootfd, rel_path, RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)`
   (Linux 5.6 and newer; fall back to component-wise `openat` with
   `O_NOFOLLOW`). Then `pread` `B` bytes at `block_no * B` and compute Blake3.
3. On the first match: pin the buffer, keyed by qid and tagged with the query
   offset, and answer "yes". If no candidate verifies, answer "no".

A pinned buffer makes a "yes" binding: the local serves the later REF from
memory, so a concurrent change to the file cannot corrupt the output.

Pinned entries are per query, not per content. A query id names exactly one
entry, and the entry records the query offset. Several in-flight queries for the
same Blake3 (a run of identical blocks, for example) produce several entries.
They share one buffer through a reference count, so identical content is held
once. An entry is released only by its own REF, or when the emitted stream
position passes its offset without a REF (the remote resolved that region in
another way). A buffer is freed when its last entry is released. A REF can
therefore never find its bytes gone, whatever the order of the answers or the
distance between identical blocks in the stream.

Keep verified buffers in a small LRU cache keyed by Blake3 after their last
entry is released. Blake3 equality is byte equality, so a later query for the
same content is answered from the cache without a disk read or a hash
computation; it only pins the cached buffer again.

The pinned set is bounded by `lookahead` plus the answers in flight, so the local
holds at most about `lookahead` bytes of pinned buffers (256 MiB by default)
when all pending blocks are distinct. When the bound is reached, answer "no". A
"no" is always safe.

A small pool of lookup workers (4 to 8) keeps disk latency off the critical
path. Answers may be sent out of order.

An RLE frame makes the local write `length` copies of `byte` to its stdout from
a static buffer and advance the stream position. It involves no query and no
pinned entry. The advance releases pinned entries whose offsets it passes, like
any other frame. The local accepts any length of at least 1; the remote only
emits runs of at least `block_size`.

### 6.5 Failures

- Version or parameter mismatch in HELLO: exit with an error before the command
  starts.
- The user command exits with a non-zero status: END carries the status. The
  local exits with the same status after it has written all data.
- The local gets EOF without END: exit with a non-zero status. Never report
  success for a truncated stream.
- SIGINT on the local: close both sessions. `serve` kills the child command when
  its stdin closes.
- Security: the hash list reveals the block hashes of all indexed files to the
  remote host. Use `hashsync` only with hosts the user trusts.

## 7. Configuration

`$XDG_CONFIG_HOME/hashsync/config.toml`, or the path given by `--config`:

```toml
block_size     = "32KiB"   # 4 KiB or more; a change discards and rebuilds the index
index_path     = "~/.local/state/hashsync/index.sqlite"
lookahead      = "256MiB"  # remote scan-ahead; also bounds the pinned memory on the local
compression_level   = 9    # zstd level for the data session, 1 to 19; 0 disables
compression_threads = 0    # libzstd worker threads on the remote; 0 = half the cores
ssh            = "ssh"     # remote shell command, split like a shell would (rsync's --rsh);
                           # the program appends -o Compression=no
remote_command = "hashsync"  # path of hashsync on the remote host

[hosts.builder]            # optional overrides, keyed by the HOST argument of `hashsync run`
ssh            = "ssh -p 2222 -i ~/.ssh/builder"
remote_command = "/run/current-system/sw/bin/hashsync"

[[roots]]
path = "/nix/store/*"      # glob: each match is its own root; also matches dot names
immutable = true
exclude = ["/nix/store/.links"]

[[roots]]
path = "/srv/data"
exclude = ["*.tmp", ".cache"]
one_filesystem = true
```

The hash list width `W` (48), the multiplier `a`, and `mix64` are protocol
constants, not configuration.

## 8. Command line

- `hashsync update [--rebuild]`: update the index, or discard and rebuild it.
- `hashsync run [--no-update] [--single-connection] [--ssh CMD] [USER@]HOST -- COMMAND ARGS...`:
  run COMMAND on HOST, write its stdout to the local stdout, and exit with the
  status of COMMAND. HOST is appended to the remote shell command unchanged, so
  `user@host` and `ssh_config` aliases work. `--ssh` overrides the configured
  remote shell command for this run. At the end it prints one summary line to
  stderr: output bytes, literal bytes, referenced bytes, run bytes, and bytes on
  the wire.
- `hashsync serve --token T` and `hashsync attach --token T`: internal. The
  local runs them on the remote through the remote shell.
- `hashsync stats`: show index statistics.
- `hashsync prune`: remove path rows whose files no longer exist (also under
  immutable roots) and the orphaned inodes.

Example:

```sh
hashsync run backup@nas -- tar -C /srv/data -cf - . | tar -C /srv/data -xf -
```

## 9. Libraries

Versions checked against the Go module proxy on 2026-09-09.

| Need | Library | Version | Notes |
|---|---|---|---|
| Weak hash | own code | | Section 5.1 and 5.2, about 50 lines. |
| Hash list coding | own code | | Golomb-Rice encoder and decoder, about 60 lines. |
| Hash table | own code | | Section 5.5, about 100 lines. |
| Blake3 | `lukechampine.com/blake3` | v1.4.1 (2025-05) | `blake3.Sum256`. AVX-512 and AVX2 assembly on amd64 (about 4 GB/s for 64 KiB inputs). Pure Go on other architectures. |
| Blake3 (alternative) | `github.com/zeebo/blake3` | v0.2.4 (2024-08) | The tagged release has AVX2 and SSE4.1 on amd64. The main branch (2026-09) adds AVX-512 and arm64 NEON/SVE2 but has no tag yet. Pick it if arm64 speed matters and a pseudo-version is acceptable. |
| SQLite | `zombiezen.com/go/sqlite` | v1.4.2 (2025-05) | Low-level API. See below. |
| SQLite engine | `modernc.org/sqlite` | v1.58.0 (2026-09) | Pulled in by zombiezen. Bump it with `go get`; zombiezen pins v1.37.1. |
| TOML | `github.com/BurntSushi/toml` | v1.6.0 (2025-12) | Alternative: `github.com/pelletier/go-toml/v2` v2.4.3. |
| Syscalls | `golang.org/x/sys/unix` | v0.48.0 | `Statx` (with `Mnt_id`), `NameToHandleAt`, `Openat2` with `RESOLVE_BENEATH` and `RESOLVE_NO_SYMLINKS`, `Getdents`, `Fadvise`, `Mmap`, `Madvise`. It has no constant for `FS_IOC_GETFSUUID`; define it in the project (section 4.3). |
| zstd | `github.com/DataDog/zstd` | v1.5.7 (2025-03) | cgo binding of libzstd 1.5.7 (embedded, or the system library with the `external_libzstd` build tag). Streaming writer with `Flush`, `Close`, `SetNbWorkers`; levels 1 to 22. |
| zstd (fallback, `!cgo` builds) | `github.com/klauspost/compress/zstd` | v1.20.0 (2026-09) | Pure Go, same wire format. Maps zstd levels 1-2, 3-5, 6-9, 10+ onto four internal tiers (about zstd 1, 3, 7, 11). Slower and less flexible than libzstd; keep it behind a build tag so a static binary is still possible. |
| Multiplexing (fallback mode only) | `github.com/hashicorp/yamux` | v0.1.2 (2024-09) | Optional. |

Everything the design needs exists in Go or is reachable through cgo. The one
missing constant, the `FS_IOC_GETFSUUID` request number and its 17-byte struct,
is a few lines. Raw syscalls are available through `unix.Syscall` for anything
else that turns up. cgo is used for libzstd; a cross-compiled remote binary
therefore needs a C cross toolchain (Nix provides one through `pkgsCross`), or
the `!cgo` fallback. No filter library is needed. `github.com/FastFilter/xorfilter`
was the candidate for the rejected binary fuse filter.

Compression alternatives, in order of preference if the cgo binding is not
usable: pipe the data session through the `zstd` command (`zstd -T0 -<level>`
on the remote, `zstd -d` on the local). It gives the real libzstd with
multithreading and even `--adapt`, but the command has no idle flush, so frames
can sit in its job buffer until the job fills or the stream ends, which delays
output and holds pinned blocks longer; it also needs the command on both hosts.
Last, the pure-Go library above.

### SQLite driver: use `zombiezen.com/go/sqlite`

It is a pure-Go port of the crawshaw API on top of the `modernc.org/sqlite`
engine: `sqlitex.Pool`, transactions and savepoints, cached prepared statements
through `conn.Prep`, and no `database/sql` layer.

Reasons:

- No cgo needed. The database engine is not a bottleneck (next point), so a
  cgo engine would buy nothing, and one pure-Go dependency less keeps the build
  and cross-compilation simple.
- Fastest queries of all measured drivers. `cvilsmeier/go-sqlite-bench`, run of
  2026-03-23, minimum of two runs, milliseconds:

  | Driver | cgo | API | insert 1M rows | query 1M rows | 8 goroutines query |
  |---|---|---|---|---|---|
  | zombiezen.com/go/sqlite | no | low-level | 1746 | 263 | 1033 |
  | modernc.org/sqlite | no | database/sql | 2419 | 758 | 2061 |
  | github.com/ncruces/go-sqlite3 | no | database/sql | 2719 | 850 | 2355 |
  | github.com/mattn/go-sqlite3 | yes | database/sql | 1480 | 871 | 2395 |
  | crawshaw.io/sqlite (last commit 2022) | yes | low-level | 1035 | 429 | 1398 |
  | github.com/bvinc/go-sqlite-lite (last commit 2019) | yes | low-level | 1021 | 438 | 1526 |

  The insert column measures engine compute, where native C is about 1.7 times
  faster than transpiled Go. The query column measures per-call overhead, where
  `database/sql` costs about 2 times and each cgo transition costs more.
- The database is not the bottleneck under any driver. A full index of 8M
  blocks costs a few seconds of insert time against minutes of reading and
  hashing 256 GiB. Copying 10 GiB of matched data costs about 320k point
  lookups, each followed by a 32 KiB read and a Blake3 computation that cost
  more than the lookup.
- The maintained cgo option, `mattn/go-sqlite3`, is slower than zombiezen on
  queries. The faster cgo wrappers are unmaintained. A separate cgo binary for
  the local side would buy nothing.

Usage rules:

- Open with WAL mode, `synchronous=NORMAL`, and a `busy_timeout` of several
  seconds.
- Give the writer goroutine its own connection and start its transactions with
  `BEGIN IMMEDIATE` (`sqlitex.ImmediateTransaction`), so a write transaction
  never fails with `SQLITE_BUSY` halfway through. Readers use the pool.
- Bind and step prepared statements directly; do not build SQL strings per row.

## 10. Alternatives considered

- **Binary fuse filter (32-bit fingerprints)** for the remote membership test.
  Rejected on 2026-09-09. It touches 3 cache lines per lookup against about 1
  for the fingerprint table, and its random fingerprints cannot be compressed,
  while the sorted hash list shrinks to 3.5 bytes per key.
- **buzhash64 (cyclic polynomial)** as the weak hash. Rejected on 2026-09-09.
  Its only mixing operation is a 1-bit rotation, which has order 64, so bytes at
  positions congruent modulo 64 contribute identically and equal bytes at
  distance 64 cancel. Every window with a period that divides 64 collapses to a
  handful of values, and to 0 when the window length is a multiple of 128. No
  table choice fixes this. The polynomial hash of section 5.1 has none of it at
  about twice the per-byte cost, which is irrelevant next to the table probe.
- **Composite index `(weak, blake3)`**. Rejected on 2026-09-09. It was only a
  workaround for the buzhash collisions and adds about 33 bytes per block.
- **cgo SQLite drivers**. Rejected on 2026-09-09; see section 9.
- **Unmixed weak hash in the index** with mixing only at export and probe time.
  Reverted on 2026-09-10 in favour of storing the mixed value everywhere; one
  quantity is simpler.
- **Marking inodes as immutable** when they are found under an immutable root.
  Reverted on 2026-09-10. The `(size, ctime)` check is cheap and is now the
  single criterion for hashing a file again. Immutable roots alone give the
  saving: their files are never stat'ed unless a mutable root reaches them.
- **`(st_dev, st_ino)` as the inode identity**. Rejected on 2026-09-10.
  `st_dev` changes between boots on XFS and on btrfs subvolumes, which would
  rebuild the whole index; inode numbers are reused. Section 4.3 uses the
  filesystem UUID and the file handle instead.
- **Indexing incomplete blocks** (the shorter tail of each file). Considered
  and dropped on 2026-09-10. The remote scan uses windows of exactly
  `block_size` bytes and could never find them, so they would only cost index
  rows. The default block size went from 64 KiB to 32 KiB instead, which halves
  the unmatched tail of every file.
- **Shell-style dot handling in globs** (`*` not matching a leading dot).
  Considered and dropped on 2026-09-10. The program uses Go's `filepath.Glob`
  as it is, and `exclude` handles entries such as `/nix/store/.links`.
- **Content-defined chunking** (as in casync, restic, and borg) on both sides
  would reduce the remote's lookups from one per byte to one per chunk. It was
  rejected: inside an archive stream, the chunk at the start and the chunk at
  the end of every file span the archive's framing, so about two chunks per file
  are lost. Trees with many files between 10 KiB and 1 MiB lose a lot. Fixed
  local blocks with a rolling remote scan lose only the incomplete tail of each
  file.
- **Pure-Go zstd as the default compressor**. Rejected on 2026-09-10 in favour
  of libzstd through cgo: fewer levels, no worker threads, and a lower ratio at
  the same CPU cost. Kept as the `!cgo` fallback.
- **SSH transport compression**. Disabled on 2026-09-10 with
  `-o Compression=no`: zlib on top of zstd wastes CPU on both hosts and cannot
  compress the control session.
- **Anchor sampling**: look up only at content-defined positions (for example
  where a small rolling hash has 12 low zero bits) and let the index record the
  anchor's offset inside its block. It would remove the per-byte probe, but a
  64-byte anchor context is shared by many distinct blocks (zero padding, common
  file headers), so each hit would need several candidates resolved. Not
  pursued. Revisit if the remote scan rate turns out to be the bottleneck.

## 11. Repository conventions

- One Go module, one binary: `go build ./cmd/hashsync`. cgo is used for
  libzstd. A `CGO_ENABLED=0` build is not required, but the `!cgo` zstd fallback
  keeps it possible at little cost; do not break it without a reason.
- If `go` is not in `PATH`, use a Nix shell (`nix develop` once a flake exists,
  or `nix shell nixpkgs#go`).
- Suggested layout:
  - `cmd/hashsync`: command-line entry point.
  - `internal/weakhash`: multiplier, `Tout` tables, block hash, rolling hash,
    `mix64`.
  - `internal/hashset`: Golomb-Rice hash list encoder and decoder, fingerprint
    table.
  - `internal/scan`: descriptor-relative traversal, filesystem and inode
    identities, mount tracking, hashing workers.
  - `internal/index`: schema, migrations, writer, pruning, hash list export and
    cache.
  - `internal/proto`: frame types, encoding, HELLO handshake.
  - `internal/remote`: `serve` (scanner) and `attach` (relay).
  - `internal/local`: `run` (remote shell processes, answering, reconstruction).
  - `internal/config`: `config.toml` and XDG paths.
- Run `go vet ./...` and `go test -race ./...` before you finish a change.
- Tests to keep: rolling hash equals direct hash (property test); the
  interleaved k-stream hash and the split block hash equal the serial ones;
  Golomb-Rice round trip on random and adversarial gap patterns; the hash table
  never gives a false negative; traversal tests on a temporary tree (hard links,
  symlink loops, dot names and `exclude`, immutable roots, glob
  roots, ctime change, pruning, identity fallbacks when the ioctl or handles are
  unsupported, sparse files with holes that start and end off block boundaries);
  a fuzz test for the frame decoder; runs of one byte at arbitrary offsets,
  crossing block boundaries and longer than the look-ahead, reproduced
  byte-exact through RLE frames, and constant blocks absent from the index; a
  compression round trip with flushes at arbitrary frame boundaries; the ssh argument order (`-o Compression=no` after
  the configured command, HOST last); an end-to-end test that connects `serve`
  and `attach` through in-process pipes without ssh.
- Protocol constants (frame layout, multiplier `a`, `mix64`, hash list width,
  HELLO fields) are shared by both roles. A change to any of them needs a
  protocol version bump.
- Nix: package with `buildGoModule` and keep `vendorHash` current.

## 12. Open questions

- **Stored Blake3 length.** 32 bytes as specified, or 16 bytes to save about
  20 % of the index (section 4.4).
- **Adaptive compression level.** libzstd allows changing the level during a
  multithreaded stream (that is how `zstd --adapt` works). A small controller
  could raise the level while the network is the bottleneck and lower it while
  the compressor is. Needs a parameter setter that the current binding lacks.
