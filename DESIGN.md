# hashsync

This file is the design specification of `hashsync`. Contributors and AI agents
follow it and update it when the design changes. Section 10 logs rejected and
reverted decisions; section 12 lists the open questions.

Status (2026-09-17): design stage. No code exists yet. The implementation
language is Rust. An earlier version of this document, from 2026-09-10,
targeted Go (section 10).

## 1. Purpose

`hashsync` copies the output of a command that runs on one host, the sender,
to another host, the receiver. It does not send bytes that already exist on
the receiver's disk. Instead, the sender sends a short reference to a block of
a file on the receiver. The receiver reads that block from its disk, verifies
it, and splices it into the output.

This pays off when the output of the command repeats data that the receiver
already holds in indexed files, in any order and at any alignment: for
example a package tree or an archive stream produced after a rebuild in which
most files did not change.

One binary contains both roles. The receiver starts the sender through `ssh`.
The program uses threads and blocking system calls, with no asynchronous
runtime or event loop (section 4.6).

## 2. Terminology

- **Receiver**: the host that runs `hashsync run`, and the program on it. It
  holds the index and receives the output of the command.
- **Sender**: the host reached through SSH, and the program on it. It runs the
  user command and a copy of `hashsync`, and it sends the output.
- **Block**: `block_size` bytes (default 32 KiB) of a file on the receiver, at
  an offset that is a multiple of `block_size`. Only complete blocks are
  indexed; the shorter tail of a file is not a block.
- **Weak hash**: the 64-bit value `mix64(P)`, where `P` is the polynomial hash
  of a block or window (section 5.1). It selects candidates. It does not prove
  equality.
- **Index**: an SQLite database on the receiver. It has one row per block with
  the block's weak hash, the first 16 bytes of its strong hash, and the inode
  and block number that hold it.
- **Root**: a configured path that the indexer scans recursively.
- **Immutable root**: a root whose content never changes while the root exists.
- **Identifier**: the string that names an immutable root in the index
  (section 4.2). By default it is the root's path. Immutable roots with equal
  identifiers are copies of one root, and only one copy is scanned.
- **Filesystem id**: the persistent identity of a filesystem (section 4.3).
- **Inode handle**: the persistent identity of an inode inside its filesystem,
  as returned by `name_to_handle_at` (section 4.3).
- **Hash list**: the sorted, compressed list of the weak hashes of all indexed
  blocks (section 5.4). The sender keeps it between runs. At the start of each
  run the receiver sends it in full, or sends only its changes (section 5.6).
- **Strong hash**: the configured cryptographic hash, with a fixed output
  length of at least 16 bytes (section 5.3). Full digests verify content; the
  index stores only 16-byte prefixes.
- **List ID**: a strong-hash algorithm name and the full digest of a canonical
  encoded hash list. It identifies content independently of an index or peer.
- **Delta**: the removed and added weak hashes between two full hash lists
  (section 5.6).
- **Hash table**: the in-memory fingerprint table that the sender builds from
  the hash list (section 5.5).
- **Offset**: a position in the command's output, counted in bytes from 0.
  Sections 6.3 and 6.4 use offsets of the output only, never positions inside
  network messages.
- **Window**: the `block_size` bytes of command output that start at a given
  offset. The sender computes the weak hash of the window at every offset it
  scans.
- **Literal**: output bytes that the sender sends as they are, in LIT frames.
- **Reference**: a REF frame. It tells the receiver to insert the block that the
  receiver pinned for the query at its current output offset (section 6.4).
- **Run**: a stretch of at least `block_size` identical bytes in the output.
  The sender sends it as RLE frames (byte value and length) instead of
  literals or references, and the receiver synthesizes it.
- **Query**: a QUERY frame. It names the offset of a window and its full
  strong hash, and asks: "do you have a block with this strong hash?"
  The sender sends at most one query per offset during a run.
- **Answer**: an ANSWER frame: the receiver's "yes" or "no" to the query at an
  offset.
- **Pin**: verified block bytes that the receiver keeps in memory for one query
  offset, from its "yes" until its output passes that offset (section 6.4).
- **Record**: an item of the sender's record list. It describes a piece of the
  output that is scanned but not yet sent: a Literal, a Run, a Query, or a Ref
  (section 6.3).

## 3. One run, step by step

1. The receiver loads `config.toml`, resolves roots and immutable groups,
   acquires access to the index, and updates it (section 4). `--no-update`
   acquires read-only access and skips the update. The connections keep
   other processes from updating the index until this run finishes using it.
2. The receiver exports and encodes the full hash list from one database
   snapshot and computes its list ID (sections 5.4 and 5.6).
3. The receiver opens two SSH sessions to the sender host: a **data session**
   and a **control session** (section 6.1).
4. The two sides exchange HELLO frames. The sender names its cached hash
   list; the receiver sends the full list, its changes, or nothing when it
   is current. Before sending list data, the receiver durably publishes
   its full list in its local cache for this peer. The sender verifies the
   result, builds its hash table, and reports whether it durably stored the
   list. Both sides clean up their caches (section 5.6).
5. The sender starts the user command and reads its stdout.
6. One scanner thread scans the output (section 6.3). A window of one
   repeated byte becomes a run. Otherwise, a miss in the hash table records
   one literal byte. A hit computes the strong hash: a cached "no" records one
   literal byte; otherwise the sender queries over the control session.
   A literal advances the scan one byte; a run or query advances it one window.
7. The receiver looks up each queried strong hash, reads and verifies a
   candidate block, and pins its bytes under the query offset before
   answering "yes". If no block verifies, it answers "no" (section 6.4).
8. The sender resolves records in offset order. A "yes" turns the Query
   into a reference. A "no" deletes it and every later record, and restarts
   the scan one byte after the rejected window's start to find matches
   inside it. Records before the first unanswered Query go over the data
   session as literals, references, or RLE frames (section 6.3).
9. The receiver writes literal bytes, pinned block bytes, and synthesized runs
   to its stdout, in order. A reference names the pin at the receiver's current
   output offset. The receiver releases every pin that its output passes.
10. When the command exits, the sender sends the exit status after the last
    data frame. The receiver exits with the same status.

Design principle: the index, the hash list, its cached copies, and the hash
table are hints. The receiver verifies every spliced block with the full
strong hash of bytes it read from its own disk, in memory, before using them.
Stale index entries, files changed after indexing, stale cached lists, and weak hash
collisions cost efficiency, never correctness.

Design principle: no timeouts. A slow disk, a slow link, or a loaded host must
not turn into a failure because an operation exceeded a fixed time. When
something is broken, the program hangs and can be inspected with a debugger.
A wait ends only when the awaited event happens or its peer, descriptor, or
process goes away (an error, an end of file, a closed channel). A fatal error
ends the process and thereby reaches every thread (section 6.7). Poll
intervals and cache expiry ages are not timeouts: they fail nothing.

## 4. The receiver's index

### 4.1 What gets indexed

- Only regular files. Symbolic links, devices, sockets, and FIFOs are ignored.
  The indexer never follows symbolic links, neither to directories nor to files
  (`O_NOFOLLOW`, `AT_SYMLINK_NOFOLLOW`). This rules out infinite recursion
  through symlink loops.
- Each directory once per scan. The walker keeps the set of visited directories,
  keyed by `(filesystem id, inode handle)` (section 4.3), for the duration of
  the scan. This stops the cycles that bind mounts can create.
- Only complete blocks. A file of size S has `floor(S / block_size)` blocks.
  A file smaller than `block_size` has no blocks and is skipped entirely: it
  gets no inode row and no path row, but is opened and stat'ed again at
  every scan of a mutable root. The sender's fixed-size windows cannot match
  a shorter tail (section 10). An inode that shrinks below `block_size` is deleted
  at the next scan.
- Each inode once per scan. When a scan meets an inode a second time (a hard
  link), it only records the new path.
- Constant blocks, whose bytes are all equal, are not stored. The sender sends
  a run of one byte value as RLE frames (section 6.3), so such blocks never
  need a lookup. This also removes the many rows that zero padding would
  produce.
- Blocks with a short period are not stored either. With `B = block_size`,
  a block has period `p` when `block[i] == block[i - p]` for every
  `p <= i < B`: equivalently, `block[p..B] == block[0..B-p]`, comparing two
  slices of length `B-p` with exclusive upper bounds. For example,
  `abcabcab` has period 3; the final repetition may be partial. Skip blocks
  with any period from 1 to 16; period 1 is the constant block above. zstd
  encodes such a window in a few bytes,
  about the cost of a reference and without a query, so matching it gains
  nothing. It also keeps periodic windows out of the sender's strong hash
  computation: without such rows they miss the table (section 6.3). The bound
  of 16 covers the usual fill patterns; it is an efficiency choice, not a
  protocol constant.
- Holes in sparse files are not read. A block that lies entirely inside a hole
  is neither read nor stored (section 4.5); it would be a constant block anyway.

### 4.2 Roots and change detection

Roots come from `config.toml`. A root path can be a glob; a match can be a
directory or a single regular file. Expand globs with `glob::glob_with`
(section 9), with `require_literal_separator = true` and the default
`require_literal_leading_dot = false`. Thus `/nix/store/*` also matches
`/nix/store/.links`; use `exclude` for such entries. `**` matches any number
of path components.

Resolve the configuration for every update, run (including `--no-update`),
and prune. After exclusions, matched paths must be distinct and none may
lie below another: `/a` cannot occur twice, and `/a` and `/a/b` cannot both
occur. Reject violations before changing the index. This is a lexical check
on absolute paths, using a set lookup per match and ancestor; it does not
resolve symlinks or bind mounts. Different paths can still reach one inode,
which the handle-based visited sets detect (sections 4.1 and 4.6).

Mutable roots (`immutable = false`, the default) are traversed on every
update. For each regular file, compare `(size, ctime)` from its open
descriptor with its inode row. Equal values reuse the complete block rows;
otherwise hash the file again. Use `ctime`, not `mtime`: programs can set
`mtime`, while the kernel maintains `ctime`. Every update reconstructs the
paths under mutable roots, including paths of unchanged files.

Immutable matches (`immutable = true`) are grouped by their configured
identifier. One group has one root row and one set of relative paths, from
one scanned copy. A non-NULL `roots.identifier` means that the group was
completely indexed. While that identifier still has a configured match,
the update reuses its rows without traversing or stat'ing files below it.
NULL means a mutable root or an unfinished immutable traversal; these roots
are reconstructed at the next update. File and directory errors leave an
immutable traversal unfinished (section 4.5).

No separate root-kind flag is stored. The configuration supplies each scan
task's intended identifier: absent for a mutable root, present for an
immutable group. Only successful immutable completion publishes that value.
Recovery treats both kinds of NULL-identifier row identically.

All locations of each immutable group are retained in memory only. Earlier
`[[roots]]` entries have priority; the order within one glob is unspecified.
The index stores one representative `roots.path`, normally the scanned
copy. Candidate reads enumerate the group's current locations from memory
(section 6.4). No table of copies is needed. For `--no-update`, map complete
immutable rows to the current groups by identifier; map NULL rows by their
stored path and current configuration. Rows without a current match have
no usable location. A group that now exists at a different path can thus
serve candidates without modifying the index. A single-file root uses an
empty `rel_path`, meaning the root file itself.

Per-root options:

- `exclude`: glob patterns with the same syntax and match options
  (`glob::Pattern::matches_with`). A pattern starting with `/` matches the
  absolute path, including root matches themselves; other patterns match
  the base name. Matching entries and their descendants are skipped.
- `one_filesystem`: skip an entry and its descendants when its `stx_mnt_id`
  differs from its parent's.
- `identifier`: allowed only on immutable roots. Forms:
  - `"path"` (default): the absolute path of the match.
  - `"name"`: its last path component. `/nix/store/*` and
    `/mnt/disk/nix/store/*` can then share identifiers.
  - `{ strip_prefix = "/mnt/disk" }`: remove this prefix from the absolute
    match, so `/mnt/disk/nix/store/x` becomes `/nix/store/x`. A match without
    the prefix is a configuration error.
  - `{ literal = "photos-2019" }`: use this byte string, preserving the
    group's identity when the tree moves.

Identifiers are nonempty byte strings, compared identically regardless of
which form or configuration entry produced them. The intended identifier
of an unfinished traversal lives in memory until it can be committed.

An update reconciles roots before starting the pool, in one transaction:

1. Find all incomplete inodes (`size IS NULL`). Before removing any of their
   paths, set `identifier = NULL` on every root referencing them. Mutable
   roots already have NULL; completed immutable roots now require rescanning.
2. Delete the paths of all roots with NULL identifiers and all roots no
   longer kept by the configuration. Delete those root rows afterwards.
   Delete any remaining paths of incomplete inodes, then their blocks and
   inode rows. Keep complete inodes and blocks, including orphans, for reuse.
3. Keep completed immutable groups still present, with their path rows.
   Plan their final representative paths in memory: retain a path still in
   its group, otherwise choose the first current location. Plan new rows
   for all other groups and mutable matches, with NULL identifiers.
4. Before assigning final paths or inserting new rows, stage changing retained
   rows at distinct NUL-prefixed BLOBs derived from their root IDs. Then apply
   all planned paths and insertions. This respects `UNIQUE(path)` even when
   roots exchange paths. A real path cannot contain NUL; no temporary value
   may be committed or used for I/O. New scan tasks carry the resulting root
   IDs and intended immutable identifiers.

The pool scans one chosen location per unfinished immutable group and every
mutable root. It opens each root when its task starts, with the same
descriptor lifetime rules as other entries (section 4.5). A failure at that
location leaves the group unfinished for the next update; other copies
remain available for candidate reads. `RootDone` publishes an immutable
identifier only after all work and dependent inode results have succeeded
and been committed (section 4.6). Mutable identifiers remain NULL.

This recovery runs at every update, including after a crash. A changed
identifier, rename, or discarded unfinished root reuses complete inodes by
`(fs_id, handle)` and unchanged `(size, ctime)`: metadata calls and new paths,
without rereading contents. After the pool and writer finish, remove all
unreferenced inodes and their blocks (section 4.6).

A file below an immutable root can also be reached through a mutable root.
The mutable traversal checks it normally, and replacing its block rows
affects every path. If that work is interrupted or abandoned, its immutable
roots become unfinished before any dependent paths are removed. A complete
immutable root scanned while content was still being added can still omit
later files; the immutability promise remains the user's responsibility.

Copies are assumed to have equal content, but may have different filesystem
and inode identities. Full strong-hash verification makes an unequal copy
cost a failed candidate read rather than incorrect output. `prune` checks
all current locations before removing a path as absent; an inaccessible
location is not proof of absence. It performs no file hashing.

### 4.3 Identity of filesystems and inodes

`(st_dev, st_ino)` is not a usable identity. `st_dev` can change between boots
(XFS, btrfs subvolumes, and other filesystems with anonymous device numbers),
which would make every indexed file look new. Inode numbers are reused after
deletion. The index therefore identifies:

- A **filesystem** by the UUID that the `FS_IOC_GETFSUUID` ioctl returns
  (Linux 6.9 and newer). The request is `_IOR(0x15, 0, struct fsuuid2)` with
  `struct fsuuid2 { u8 len; u8 uuid[16]; }`, which is `0x80111500` on amd64
  and arm64. No crate defines the request number (checked: `rustix` 1.1.4,
  `libc` 0.2.189, `linux-raw-sys` 0.12.1; the last one defines the struct).
  Build the opcode in the project with
  `rustix::ioctl::opcode::read::<fsuuid2>(0x15, 0)` and call it through
  `rustix::ioctl::ioctl` with a `Getter` (section 9). A filesystem that does
  not implement the ioctl (`ENOTTY`; btrfs does not, as of Linux 6.18) falls
  back to the `f_fsid` value that `fstatfs` returns (`rustix::fs::fstatfs`),
  stored with a tag so that it cannot collide with a UUID. Most filesystems
  derive `f_fsid` from an on-disk identifier, so it is stable across boots;
  btrfs also folds the id of the subvolume into it, so each subvolume gets its
  own filesystem id. A filesystem whose `f_fsid` is not stable across boots is
  indexed again after a reboot. The mount id (`stx_mnt_id`) is never an
  identity: it is not stable across boots, and every subvolume of a btrfs
  filesystem that is reached through one mount has the same mount id.
- An **inode** by the file handle that `name_to_handle_at` returns
  (`libc::name_to_handle_at`; `rustix` has no wrapper). The handle is unique
  within its filesystem and persistent, and it includes the inode generation,
  so a reused inode number gets a different handle. On btrfs the handle also
  names the subvolume, so two subvolumes that reuse one inode number get
  different handles. Handles are opaque byte strings of up to 128 bytes
  (`MAX_HANDLE_SZ`), usually 8 to 40. If the filesystem does not support
  handles (`EOPNOTSUPP`), fall back to the inode number from `statx`, stored
  with a tag.

The index assigns 64-bit integer ids to filesystems and inodes for foreign
keys. The unique pair `(fs_id, handle)` is the inode's only identity, in
memory as in the index. The walker's sets (section 4.6) keep those same
handle bytes, never inode numbers alone.

Cost control: the ioctl or `fstatfs` runs once per filesystem seen in a scan,
not once per inode. `statx`, which the walker needs anyway for size and ctime,
returns `stx_mnt_id`, `stx_dev_major`, and `stx_dev_minor` for every entry.
Only when the triple `(stx_mnt_id, stx_dev_major, stx_dev_minor)` differs
from the parent's does the walker resolve the entry to a filesystem id (or
skip the entry, with `one_filesystem`, when the mount id differs). The walker
caches `triple -> fs_id` for the scan. The mount id alone is not enough as the
trigger: a btrfs subvolume below a mount keeps the mount id of that mount, but
it has its own anonymous device number and its own `f_fsid`, so it must be
resolved again. `name_to_handle_at` runs once per regular file and once per
directory; it is a metadata-only call with no disk read.

### 4.4 Storage

The index is one SQLite file, by default
`$XDG_STATE_HOME/hashsync/index.sqlite`. Suggested schema:

```sql
CREATE TABLE meta        (key TEXT PRIMARY KEY, value BLOB NOT NULL);
  -- keys: schema_version, block_size, weak_hash_id, strong_hash_id, index_id
CREATE TABLE filesystems (id INTEGER PRIMARY KEY, fsid BLOB NOT NULL UNIQUE);
CREATE TABLE roots       (id INTEGER PRIMARY KEY, identifier BLOB UNIQUE,
                          path BLOB NOT NULL UNIQUE);
  -- identifier: non-NULL only for a completely indexed immutable group
  -- path: one representative location; other copies live only in memory
CREATE TABLE inodes      (id INTEGER PRIMARY KEY,
                          fs_id INTEGER NOT NULL REFERENCES filesystems(id)
                            ON DELETE RESTRICT,
                          handle BLOB NOT NULL, size INTEGER,
                          ctime_ns INTEGER NOT NULL, UNIQUE (fs_id, handle));
  -- size: NULL while block rows are incomplete; otherwise stx_size bits
CREATE TABLE paths       (root_id INTEGER NOT NULL REFERENCES roots(id)
                            ON DELETE RESTRICT,
                          rel_path BLOB NOT NULL,
                          inode_id INTEGER NOT NULL REFERENCES inodes(id)
                            ON DELETE RESTRICT,
                          PRIMARY KEY (root_id, rel_path)) WITHOUT ROWID;
CREATE INDEX paths_by_inode ON paths (inode_id);
CREATE TABLE blocks      (inode_id INTEGER NOT NULL REFERENCES inodes(id)
                            ON DELETE RESTRICT,
                          block_no INTEGER NOT NULL, weak INTEGER NOT NULL,
                          strong BLOB NOT NULL CHECK (length(strong) = 16),
                          PRIMARY KEY (inode_id, block_no)) WITHOUT ROWID;
CREATE INDEX blocks_by_weak   ON blocks (weak);
CREATE INDEX blocks_by_strong ON blocks (strong);
```

Connections and process exclusion:

- Every connection executes `PRAGMA foreign_keys = ON` immediately after
  opening, before any transaction. Use foreign keys with `ON DELETE RESTRICT`
  everywhere: delete referencing rows before their parent, in the same
  transaction. Install the unlimited busy handler of section 9.
- A process that may modify the index (`update`, `prune`, or a normal `run`)
  starts with no other connections to that database. Open its writer with
  `file:<escaped-path>?mode=rw&vfs=unix-excl`; use `mode=rwc` when creation is
  allowed. Enable URI processing. Execute and finish a real read such as
  `SELECT count(*) FROM sqlite_schema` before opening any other connection.
  Opening alone does not acquire the process lock. The successful read
  establishes `unix-excl`'s exclusive process lock; on lock contention wait
  and retry, and on any other initialization failure open no worker readers.
- Only after that read succeeds, open same-process readers with
  `file:<escaped-path>?mode=ro` through the normal Unix VFS. Use normal
  locking mode, not `PRAGMA locking_mode=EXCLUSIVE`, and never `immutable=1`
  or `nolock=1`. All connections use the same SQLite library instance.
  `unix-excl` coordinates those connections through its shared in-process
  inode state and WAL index, so readers and the writer retain normal WAL
  snapshot isolation while other processes cannot access the database.
- Keep the writer connection alive after indexing and throughout a normal
  run's use of the index. Close worker readers before the writer, or let
  process exit close everything. There is no interval between update and
  use during which another process can update or rebuild the index.
- A read-only process (`run --no-update` or `stats`) opens only normal
  `mode=ro` connections. Before using the index, a keeper connection begins
  a read transaction and executes the real read above; keep it and its
  transaction alive until all index use ends. Several such processes can
  coexist. A `unix-excl` writer waits for them to release access, and they
  wait for an existing writer. `--no-update` never rebuilds or repairs a
  missing or incompatible index: report that an update is required.

This uses SQLite's Unix VFS locks, not a separate lock file. A real read is
the initialization barrier on either path. Use fresh connections for a new
access mode rather than upgrading a process with existing readers. SQLite's
[VFS documentation](https://sqlite.org/vfs.html) describes `unix-excl`;
the initialization order also ensures that the first WAL-index attachment
uses the process-exclusive state.

Storage and recovery rules:

- Store `block_size`, `weak_hash_id` (algorithm, multiplier, mixer), and
  `strong_hash_id` (algorithm with its fixed output length). An updating
  process rebuilds when any parameter or the schema version is incompatible.
- Paths and identifiers are `BLOB`s, preserving arbitrary non-UTF-8 bytes
  from `OsStr` through the database and back. SQLite permits multiple NULL
  values under `UNIQUE(identifier)`; completed immutable groups are unique.
- Store `weak` as the `i64` bit pattern of the `u64` value. Likewise,
  `inodes.size` holds `stx_size` as the signed integer with the same bits;
  compare sizes for equality only, never order or calculate with them.
  NULL marks an incomplete inode, so no integer is reserved as a sentinel.
- Use WAL mode with `synchronous=NORMAL`. A crash leaves a consistent
  database, though power loss may discard recent commits. This is acceptable
  for the index. Hash lists identify their own content and are independently
  persisted (section 5.6); exporting requires no forced FULL checkpoint.
- The writer commits every few thousand completed inode operations or every
  30 seconds, whichever comes first. A transaction can cut through block
  batches of any number of interleaved files. Each remains `size = NULL`
  until its terminal success is stored with its last block rows.
- Before replacing an inode's blocks, set `size = NULL` and delete the old
  blocks in the same transaction. Successful completion stores the real
  size and ctime. Recovery first invalidates immutable roots referencing
  incomplete inodes, then discards incomplete inodes and unfinished roots
  in child-first order (section 4.2). Complete inodes survive for reuse.
- An immutable root's identifier stays NULL until the whole traversal and
  every pending inode dependency succeeded. Publish it in the transaction
  that finishes that work. There is no separate `complete` column.
- `blocks.strong` and `blocks_by_strong` contain only the first 16 bytes of
  the strong digest. Lookup uses
  `SELECT inode_id, block_no FROM blocks WHERE strong = ?`, binding that
  prefix. `WITHOUT ROWID` secondary indexes include the primary-key
  columns, so this index supplies candidates without reading the block
  table. Stream candidates and their paths under the overall attempt limit
  of section 6.4. Full digest comparison, never prefix equality, verifies a
  block. `blocks_by_weak` serves distinct weak-hash export (section 5.4).
- Only complete inodes with stored blocks need persistent rows; temporary
  incomplete rows are allowed while streaming. A successful scan finding
  no useful blocks deletes the old inode's paths, blocks, and inode row.
- Give the writer about 256 MiB of SQLite page cache; readers use small
  caches and share the OS page cache. The secondary keys are random, so
  their B-tree working sets dominate insert cost.
- Rebuild in place under process-exclusive access. Drop child tables before
  parents and recreate the schema and metadata in one transaction with
  foreign keys enabled. Never replace the database file by rename. Populate
  it in normal batches, then create both secondary block indexes. Before
  every update, create either missing index left by an interrupted build;
  a read-only run requiring missing indexes reports that an update is needed.
  A crash after the reset leaves a partial index recoverable by the same
  rules, rather than retaining the previous complete index.
- Expected storage is roughly 90 bytes per block across the table and its
  two secondary indexes, about 0.27 % of indexed data at 32 KiB blocks:
  around 750 MB for 8M blocks. The 16-byte prefix saves about 32 bytes per
  block compared with storing a 32-byte digest in both places.
- `index_id` is a random 128-bit value from `getrandom`, created with the
  schema. A rebuild creates a new one. It partitions peer cache directories
  together with account IDs (section 5.6); it is not the identity of a list.
  There is no index generation counter.

### 4.5 Traversal and hashing

Apart from configured root paths, SQLite and cache files, and `ssh`, every
filesystem access is relative to an open descriptor. A task opens its entry
before retrieving metadata or identity and keeps that descriptor throughout
processing. It never checks an object by name and later opens that name for
hashing. Each encounter opens once; a hard link or directory loop is detected
after retrieving the identity of the newly opened descriptor.

- Open: for an expected directory use
  `openat(parentfd, name, O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)`.
  `O_DIRECTORY` rejects FIFOs and sockets without waiting for their I/O.
  For a regular-file hint, an unknown entry type, or a root whose type is
  unknown, use `O_RDONLY|O_NOFOLLOW|O_NONBLOCK|O_NOCTTY|O_CLOEXEC`, then inspect
  the descriptor. `O_NONBLOCK` prevents waiting for a FIFO writer and
  `O_NOCTTY` avoids acquiring a controlling terminal. Skip every opened
  object that is neither a regular file nor a directory.
- Metadata: `statx(fd, "", AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW,
  STATX_TYPE | STATX_SIZE | STATX_CTIME | STATX_INO | STATX_MNT_ID_UNIQUE)`.
  These calls describe the open object, not a directory entry. Check
  `stx_mask` for returned fields. Apply `one_filesystem` using this metadata;
  apply name-based exclusions before queueing/opening when possible.
- Filesystem identity: when `(stx_mnt_id, stx_dev_major, stx_dev_minor)`
  differs from the parent's, resolve the filesystem using the open fd's
  ioctl or `fstatfs` and the cache of section 4.3.
- Inode identity: `name_to_handle_at(fd, "", AT_EMPTY_PATH)` on every
  directory and regular file, with the fallback of section 4.3. Atomically
  claim `(fs_id, handle)` in the corresponding visited set. A repeated
  directory is closed without descending; a repeated file records its
  additional path without rehashing. Its first task owns the result.
- Directory: after metadata and identity checks, use `getdents64`
  (`rustix::fs::RawDir`, with a reusable 64 KiB buffer). `d_type` is a hint
  for filtering and selecting open flags, never proof of the opened type.
  Queue directories, regular files, and `DT_UNKNOWN` entries as tasks; skip
  symlink and other known special-file hints. Children carry the parent's
  `Arc<OwnedFd>` until they open their entries. The directory is not reopened.
- Regular file: if smaller than `block_size`, report that it has no indexed
  blocks. Otherwise look up `(fs_id, handle)` through the worker's reader.
  Equal stored `(size, ctime)` reports an unchanged inode. A changed or new
  file streams `Begin`, `Blocks`, and its terminal result to the writer
  (section 4.6), while reading this same fd with `pread` in chunks up to
  1 MiB. Compute the weak and full strong hash of every useful complete
  block, storing only the strong prefix. Skip short-period blocks first:
  compare `block[p..B]` with `block[0..B-p]` for `p` from 1 to 16, testing
  the first 80 bytes before a full vectorised comparison (section 4.1).
  Accumulate short reads until the requested bytes are filled or EOF is
  reached. Unexpected EOF within the initial full-block range abandons the
  file. Recheck size and ctime on the same fd after hashing; a change also
  abandons it for this update. An open descriptor fixes identity and type,
  not contents, size, or the entries of a directory.
- Holes: for every file being hashed, attempt `lseek(SEEK_DATA)` and
  `lseek(SEEK_HOLE)` to enumerate data and holes before reading their blocks.
  Do not infer absence of holes from `stx_blocks`: allocation rounding,
  metadata, and ZFS `copies=2` or `copies=3` can mask sparseness. A hole
  `[h0, h1)` skips blocks `k` with `k*B >= h0` and `(k+1)*B <= h1`.
  Read partially overlapping blocks in full; the hole portion reads as
  zeros. With `B = 32 KiB`, a hole from 10 KiB to 200 KiB skips blocks 1
  through 5. `SEEK_DATA` returning `ENXIO` means no more data before EOF;
  account for concurrent truncation with the final metadata check. When
  hole seeking is unsupported (`EINVAL` or `EOPNOTSUPP`), read normally.
  A filesystem may also conservatively report all bytes as data. Other
  seek errors abandon this file, like read errors.
- Cache advice: before reading, make separate calls
  `posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL)` and
  `posix_fadvise(fd, 0, 0, POSIX_FADV_NOREUSE)` (or the corresponding
  `rustix::fs::fadvise` wrappers). The advice values are not a bitmask.
  NOREUSE influences Linux page replacement starting with 6.3 and is a
  no-op on several older versions. These are hints, not a guarantee against
  eviction of useful cache. Unsupported advice does not fail the scan.

Opening, metadata, directory-listing, and reading errors are local to the
entry: log the error, stop processing that entry, and continue the update.
A directory error also prevents visiting its unread descendants. Mark its
root's traversal unsuccessful so an immutable root is retried next time.
If the worker already began an inode, send `Abandon` so partial block rows
are removed without leaving paths attached to an incomplete result. The
supported identity and hole-seeking fallbacks above are not failures.

An entry may disappear before open. After a local file is open, unlink or
rename does not invalidate the descriptor; reading can continue. Truncation,
concurrent modification, or an actual I/O error can still abandon the file.
Database failures and programming errors are fatal, unlike these per-entry
filesystem failures (section 6.7).

### 4.6 I/O architecture

This section states how the indexer is parallelised and why. Constraints: no
asynchronous runtime (no `async` functions, no executor, no futures); threads
for parallelism. Asynchronous calls in the C sense (a function that runs on
another thread and reports back, or a submission with a completion) are fine.

#### Where the time goes

Three kinds of work:

1. **Metadata.** One open per directory and regular file, one
   `getdents64` per directory, and one initial `statx` and handle retrieval
   per opened entry. Regular files large enough to index also get an inode
   lookup; hashed files get a final `statx`. With warm dentry and
   inode caches this is 1 to 3 µs of kernel CPU per entry. With cold caches it
   is disk latency: a directory costs a few reads of directory blocks, and a
   group of neighbouring inodes costs one read of an inode-table block (ext4
   and XFS read inodes in clusters). A read costs 50 to 200 µs on NVMe, so the
   cold rate is the number of reads in flight divided by the latency.
2. **Data.** For a new or changed file: hole discovery, reads, hashing,
   final metadata check, and close on the existing descriptor. Per core,
   BLAKE3 runs at 3 to 7 GB/s on 32 KiB inputs, the weak hash
   at 1 to 4 GB/s (k streams, section 5.1), the constant-block check near
   memory speed, and the copy out of the page cache at about 10 GB/s. All
   together about 1 GB/s per core, up to 2 GB/s with AVX-512. An NVMe device
   delivers 3 to 7 GB/s when it has 32 or more large requests in flight and
   about 1 GB/s with one request in flight. From a warm page cache the machine
   is CPU-bound; from disk it is disk-bound once 4 to 8 cores hash.
3. **Rows.** The writer inserts one inode row and one path row per file and one
   block row per 32 KiB. SQLite costs 1 to 2 µs per B-tree insert with cached
   prepared statements when the B-trees are in memory. A block row is one
   insert on a rebuild, where the secondary indexes are created at the end,
   and three inserts during an update, where the two secondary indexes are in
   place (section 4.4). Ten million rows of a rebuild cost 10 to 20 s of one
   core plus about 20 s for the two indexes: less than hashing 256 GiB from
   disk, about the same as hashing it from the page cache on a machine with
   many cores. An update replaces block rows only for changed files, but
   rewrites paths for all traversed roots. Section 4.4 gives the cache and
   deferred-index rules. The strong prefix is already 16 bytes; if the
   writer becomes the bottleneck, measure multi-row `INSERT` statements.

In every case the unit of work is a file or a directory, and the work per unit
is a few blocking system calls plus CPU time. A thread that blocks in the
kernel costs nothing while it waits. This is why a pool of threads with plain
blocking calls is the right tool; the rest of this section shapes that pool.

#### The pool

One pool walks and hashes, following ripgrep and fd (`ignore::WalkParallel`,
in `crates/ignore/src/walk.rs` of ripgrep 15.1.0). ripgrep does per-file work
inside the walk callback; fd batches results to one output thread over a
bounded channel. hashsync uses the same division:

- A fixed number of threads. Each thread owns a LIFO deque of tasks
  (`crossbeam_deque::Worker::new_lifo` in `ignore`) and steals the oldest task
  of another thread when its own deque is empty. LIFO order makes each thread go
  depth-first, so the number of pending tasks stays near `depth × fan-out`
  instead of growing to the width of the tree. Stealing the oldest task
  hands over a whole subtree, which keeps the threads balanced.
- The walk ends when every thread is idle at the same time (an active-thread
  counter), not when a queue is empty, because a running task can still push
  more tasks.
- A task is an entry to open and inspect. A directory task lists entries
  and pushes one task for each potentially relevant child, including files
  that may prove unchanged. The child task owns all metadata and hashing.
  Depth-first order hashes the files of a directory right after listing it,
  while its inodes are hot in the cache.
  Threads that wait for the disk and threads that hash overlap without any
  scheduling logic.
- A task carries the `Arc<OwnedFd>` of its parent directory and its name, and
  opens its own descriptor when it runs. Queued tasks hold no descriptor
  for their entry, but retain the shared parent descriptor until open.
  With depth-first order, the queued tasks of a thread are the unvisited
  siblings along its current path, so the open descriptors are bounded by
  `threads × depth`, not by the queue length or the size of the tree. Each
  entry is opened once per encounter and stays open throughout processing.
  The roots are the initial tasks, one per traversed root (section 4.2), with no
  parent: a root task opens its path when a thread takes it, so the number of
  roots adds no descriptors. ripgrep and fd bound descriptors by `threads`
  by queueing paths, but pay a full path lookup per directory and file;
  hashsync's parent descriptor avoids that cost below a root. The program
  raises the soft `RLIMIT_NOFILE` to the hard limit at start.
- Thread count. ripgrep uses `available_parallelism` capped at 12 and fd caps
  it at 64; both assume warm caches and CPU-bound work. hashsync's threads also
  wait for the disk, and every waiting thread adds one request to the device
  queue. Default: `4 × available_parallelism`, clamped to the range 16 to 64
  (`threads` in the configuration, `-j` on the command line). More threads do
  not help: a warm walk is CPU-bound, and an NVMe device reaches its rate at a
  queue depth of 32 to 64.
- Per-thread state, created once: a 64 KiB `getdents64` buffer, a 1 MiB read
  buffer, the configured strong hasher, and an SQLite read connection with
  cached prepared statements (section 9). Readers do not block ordinary
  writer commits in WAL mode.
- Two shared sets, keyed by `(fs_id, handle)` (section 4.3): visited
  directories (cycle protection) and seen regular files (hard links). The
  key holds the handle bytes as they are; the set of seen files therefore
  costs about 64 bytes per regular file of the update, which is not a concern.
  Shard each set into 64 mutexes by a hash of the key; there is no contention
  to speak of. The handle of a directory costs one extra metadata call per
  directory, next to its `openat`, `getdents64`, and `statx`.
- Implementation: a dedicated `rayon` pool (`ThreadPoolBuilder::num_threads`)
  and `pool.scope` with one `spawn` per task. rayon's local deques are LIFO
  (depth-first is its default; `breadth_first` is deprecated) and its stealers
  take the oldest task, so it has exactly the schedule above, and it parks idle
  threads on a futex where `ignore` polls every millisecond. Blocking system
  calls inside the tasks are fine in a pool that runs nothing else. If rayon
  ever gets in the way, the `Stack` of `ignore` on `crossbeam-deque` is about
  150 lines.

#### The writer

One thread owns the write connection and receives work over a bounded
channel. It uses `BEGIN IMMEDIATE` transactions and the commit schedule of
section 4.4. No other thread writes. After indexing, keep that connection
alive until the run stops using the index, preserving process exclusion.

Every message is at most 64 KiB and the channel holds at most 4096 messages,
bounding queued data to 256 MiB. A slow writer blocks worker sends. Stream
large files: a 1 TiB file has 32M blocks even with 16-byte stored digests.
Every file message carries `key = (fs_id, handle)`; the writer maps it to an
inode ID and per-file state. Messages from different workers may interleave.
Messages from the one owner of a key arrive in that owner's send order.

- `Begin { key }`: insert an incomplete inode or set its size to NULL, and
  delete its old blocks, in the same transaction. Initialize required ctime
  storage for a new incomplete row; only terminal success makes it reusable.
- `Blocks { key, rows }`: up to 1024 rows, each containing block number,
  weak hash, and 16-byte strong prefix (32 bytes before message overhead).
  Insert them for this key into the current transaction.
- `End { key, size, ctime, root_id, rel_path }`: complete a successful hash,
  store size and ctime with its last rows, and record the owner's path. If
  no blocks survived filtering, remove its paths, blocks, and inode instead.
- `Unchanged { key, root_id, rel_path }`: the owner reused complete rows;
  record its path and mark the key successful without changing its blocks.
- `Omit { key }`: the owner's regular file is too small to index. Remove
  any old paths, blocks, and inode for this key; it has no useful content.
- `Path { key, root_id, rel_path }`: a second encounter with this inode.
  Hold the path until its owner's terminal result is known, even if an old
  complete inode row exists. Insert it only for a successful, indexed result.
- `Abandon { key }`: the owner failed after claiming the key. Before
  deleting any existing paths, set identifiers of all referencing roots
  to NULL. Then remove its paths, partial blocks, and inode row in
  child-first order, and discard pending paths for the key. Those roots and
  roots of pending paths cannot be marked complete by this update.
- `RootDone { root_id, success }`: this root's task and descendants have
  finished sending work. `success` is false if any entry failed. The writer
  also waits for terminal results of pending hardlink paths from other roots
  before publishing this root's intended immutable identifier.

Every claimed regular file produces exactly one terminal result: `End`,
`Unchanged`, `Omit`, or `Abandon`. Only hashed files send `Begin` and
`Blocks`. A `Path` may arrive before its owner's `Begin`; buffer it by key.
A later path consults the remembered terminal result; a failed result also
marks that path's root unsuccessful. An abandoned key is not retried
through another hardlink in the same update. Unexpected missing
terminal results are bugs, not files that vanished while open.

The writer remembers failed root IDs even after their `RootDone`, so a
later abandonment cannot be overwritten by a completion marker. Root
completion is stored after all its dependent inode and path work, in the
same transaction or a later one. An interrupted transaction leaves a NULL
identifier or NULL inode size for recovery (sections 4.2 and 4.4).

Paths under traversed roots were deleted at reconciliation and are now
reinserted; there are no seen-path hash sets. After every root and inode
result has been processed, delete blocks of all inodes with no path rows,
then delete those inodes, in bounded transactions. Search all orphans, not
only IDs noticed during this update, so an earlier interrupted cleanup is
also completed. A crash can leave harmless unreferenced rows for next time.

#### The reader

The pool reads with `pread` (section 4.5). A blocking reader keeps about two
requests in flight per thread: `fadvise(SEQUENTIAL)` doubles the readahead
window, and readahead is asynchronous. So 32 to 64 threads put a queue depth
of 64 or more of 128 to 256 KiB requests on the device for files of a few
hundred KiB and larger, which is where an NVMe device reaches its rate
(above). Smaller files are bounded by their per-file metadata work, not by
their reads. The hashing stage keeps a narrow interface, a function that
takes an already-open regular file and streams its rows and result, so a
different reader (below, spinning disks) can replace its body without a
change to the pool or the writer.

#### Event loops and asynchronous runtimes

Readiness notification (`epoll` through `mio` or `polling`, libevent-style)
tells when a socket or a pipe can be read or written. Regular files are always
ready, so it cannot express file I/O at all. Each side of a run owns two or
three pipes and sockets; one blocking thread per direction serves them (section
6.6). An asynchronous runtime is excluded by the constraints and would add
nothing: its file I/O is a thread pool underneath.

#### Refinements, not in the first version

- Files larger than a few hundred MiB can be split into ranges that several
  tasks hash in parallel; rows are per block, so their order does not
  matter. One coordinator streams batches under the same key and sends the
  terminal result only after all ranges finish. This removes the tail where
  one thread hashes a disk image while the others are idle.
- Spinning disks want one reader per device with the files in inode order. A
  per-root reader limit would be the knob (section 12).

## 5. Hashes, hash list, and hash table

### 5.1 Weak hash: mixed polynomial rolling hash modulo 2^64

Both sides must compute identical values, so the definition is part of the
protocol. All arithmetic is unsigned 64-bit, modulo 2^64 (`wrapping_mul`,
`wrapping_add`, `wrapping_sub`).

- `a` is a fixed odd multiplier compiled into the binary, with `a ≡ 5 (mod 8)`.
  Example: `0x9E3779B97F4A7C15`. This class has the maximal multiplicative
  order 2^62 and the smallest entropy loss on constant blocks.
- Polynomial hash of a window `w[0..B)`, with `B = block_size`:
  `P = sum over i of w[i] * a^(B-1-i)`. Horner's rule computes it: start with
  `P = 0`; for each byte `P = P*a + b`.
- Weak hash: `mix64(P)` (section 5.2). The index and the hash list carry this
  mixed value, and the sender probes its table with it.
- Roll one byte (`out` leaves the window, `in` enters it):
  `P' = a*P - Tout[out] + in`, with the 256-entry table `Tout[b] = b * a^B`,
  computed at start-up for the configured `B`. Rolling operates on
  `P`; `mix64` is applied to `P` at every offset before the table probe.

`meta.weak_hash_id` records the algorithm, `a`, and the mixer.

The dependency chain per byte is one multiply and one add, about 4 to 5 cycles,
so about 1 GB/s per core. This is not the bottleneck of the sender's scan
(section 6.3). If it ever is, run k interleaved streams. Stream j handles the
offsets congruent to j modulo k and advances k bytes per step:

```
P(o+k) = a^k * P(o) - sum(m=0..k-1) Tout_m[w[o+m]] + sum(m=0..k-1) Tin_m[w[o+B+m]]
Tout_m[b] = b * a^(B+k-1-m)        Tin_m[b] = b * a^(k-1-m)
```

Only the multiply and one add stay on the chain; the table sums are independent.
With k = 8 the multiplies pipeline at one per cycle. On x86_64 with AVX-512
the eight streams fit one vector (`_mm512_mullo_epi64`; the AVX-512 intrinsics
are stable since Rust 1.89), but the scalar version already reaches a few GB/s.

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

```rust
fn mix64(mut z: u64) -> u64 {
    z ^= z >> 30;
    z = z.wrapping_mul(0xBF58476D1CE4E5B9);
    z ^= z >> 27;
    z = z.wrapping_mul(0x94D049BB133111EB);
    z ^= z >> 31;
    z
}
```

Why it exists: the bits of the polynomial hash `P` are not uniform. The low `j`
bits of `P` depend only on the low `j` bits of the input bytes; bit 0 is the
parity of all bytes. A constant block has `P` equal to a multiple of `B`, so its
low `log2(B)` bits are zero; a block with period 2^m has `log2(B) - m` zero low
bits; and such blocks form arithmetic progressions in the hash space. Three
parts of the design assume uniformly random keys: the slot selection in the
sender's table (probe lengths), the false positive rate of the fingerprints, and
the Golomb-Rice coding of the gaps of the sorted list. `mix64` makes these
assumptions hold for any input.

Properties:

- Bijective. Every step is invertible: an xor-shift can be undone, and
  multiplication by an odd constant is invertible modulo 2^64. It never merges
  two distinct values of `P`.
- Avalanche. Flipping one input bit flips each output bit with probability close
  to one half.
- Cheap. Two multiplies and three xor-shifts, outside the rolling hash's
  dependency chain.

It affects efficiency only. Rolling operates on the unmixed `P` as specified
in section 5.1; `mix64` is not rolling-compatible.

### 5.3 Strong hashes

`strong_hash` selects a supported cryptographic hash algorithm. Initially
support `blake3` (the default, its standard 32-byte output) and `sha256`
(32 bytes). Every supported algorithm has one fixed output length of at
least 128 bits (16 bytes); configuration cannot select a shorter digest or
an arbitrary BLAKE3 XOF length. The algorithm ID determines the full length
`S`, and HELLO requires both sides to use that algorithm.

The full digest identifies block content. The receiver hashes the exact
bytes read into its candidate buffer and compares all `S` bytes with the
query before pinning. Treat full strong-hash equality as byte equality.
Only the index uses 16-byte prefixes; queries, the negative cache, the
verified-buffer cache, and list IDs use full digests. Changing the strong
algorithm rebuilds the index. Section 9 gives the implementations.

### 5.4 The hash list

The receiver exports the set of weak hashes of all indexed blocks as a sorted,
compressed list:

1. `SELECT DISTINCT weak FROM blocks` through the `blocks_by_weak` index.
   Constant and short-period blocks have no rows (section 4.1), so their
   hashes are absent.
2. Sort the values (`sort_unstable`) and remove duplicates. The list holds the
   full 64-bit values: `W = 64`, and no bits are dropped.
3. Encode the gaps with Golomb-Rice coding: `g_0 = v_0`, and
   `g_i = v_i - v_(i-1) - 1` for `i >= 1` (the values are distinct, so the
   `- 1` loses nothing). A gap `g` is coded as `g >> k` one bits, a zero bit,
   and the low `k` bits of `g`. The bits are packed least significant first
   into bytes, and the last byte is padded with zero bits. The Rice parameter
   is `k = floor(log2(2^W / N)) = W - ceil(log2(N))`, so the average gap is
   between 2^k and 2^(k+1) and each value costs about `k + 2` bits. For
   `N = 0`, `k = 0` and no gap codes follow the header: the empty list is the
   header alone. It occurs for an empty index and for a delta whose removed
   or added list is empty (section 5.6). For `N = 1`, `k = 64`: the code is
   one zero bit followed by the 64 bits of the value, and the implementation
   must not shift a `u64` by 64 (`checked_shr`).
4. Prefix a header: `format_version u8`, `W u8`, `k u8`, `N u64`.

Why this shape: sorted uniform values have gaps with about `W - log2(N) + 1.4`
bits of entropy, while a filter's random fingerprints do not compress. Full
64-bit values cost about 2 bytes per key more than 48-bit prefixes, but keep
the table's fingerprints independent of its bucket index (section 5.5).
Sizes and false-hit rates for a 256 GiB tree (8M blocks of 32 KiB):

| Transfer format | Size | Cache lines per lookup | False hits per GiB scanned |
|---|---|---|---|
| Sorted 64-bit values, Golomb-Rice gaps (chosen) | 43 MiB | ~1 | ~3 |
| Sorted 48-bit prefixes, Golomb-Rice gaps (rejected) | 27 MiB | ~1 | ~400 |
| Raw sorted 64-bit values | 64 MiB | ~1 | ~3 |
| Binary fuse filter, 32-bit fingerprints (rejected) | 36 MiB | 3 | 0.25 |

A false hit costs one strong hash, a query of `13 + S` bytes (45 bytes
with the initial algorithms), and a restart that repeats one round trip
of scanning (section 6.3). This cost ruled out 48-bit
prefixes (section 10). Decoding 8M values takes tens of milliseconds;
sorting them takes well under a second.

The encoding is canonical: the values are sorted and deduplicated, and `k` is
a function of `N`, so equal sets give equal bytes. The strong hash of the
encoded list, together with the hash algorithm ID, identifies the set.
Every run exports the current list; there is no generation-based shortcut.
Section 5.6 describes its independent persistence, caches, and direct deltas.

### 5.5 The sender's hash table

The sender decodes the hash list into a custom open-addressing table of 32-bit
fingerprint slots. It is a flat array of `u32` obtained from `mmap`, not a
`HashSet<u64>`: hashbrown (the table behind `std::collections::HashSet`) needs
about 9 to 12 bytes per key, hashes the key a second time, touches control
bytes and slots, and gives no control over alignment, huge pages, or batching.

- Layout: buckets of 16 slots, one bucket per 64-byte cache line.
  `buckets = ceil(N / (16 * 0.7))`; map a value to a bucket with multiply-shift
  range reduction on its top bits: the high 64 bits of the 128-bit product
  `v * buckets` (`((v as u128 * buckets as u128) >> 64) as usize`).
- Fingerprint = the low 32 bits of the value; replace 0 by 1, because 0 marks an
  empty slot. Insert into the first empty slot of the bucket; when the bucket is
  full, continue in the next bucket. A lookup scans the bucket until it finds
  the fingerprint or an empty slot, and continues in the next bucket only when
  the bucket is full.
- At load factor 0.7 about 10 % of the buckets are full, so about 90 % of the
  lookups touch exactly one cache line. Load factor 0.6 brings that to about
  97 % for a sixth more memory.
- False positives: the bucket index comes from the high bits of the 64-bit
  value and the fingerprint from its low 32 bits, so the two are independent
  for any table below 2^32 buckets. A negative lookup compares against about
  11 occupied slots and false-positives with probability about 3e-9: about 3
  false hits per GiB scanned. The 48-bit design overlapped the two in about
  7 bits, leaving only about 25 independent bits (section 10).
- Memory: about 5.7 bytes per key at load factor 0.7: 46 MiB for 8M keys
  (256 GiB), 183 MiB for 32M keys (1 TiB).
- Allocate the table with `rustix::mm::mmap_anonymous` and
  `madvise(Advice::LinuxHugepage)`. With 4 KiB pages every probe also misses
  the TLB.
- Fill: the list is sorted and the bucket index is monotonic in the value, so
  the fill writes the table sequentially.
- Probe in batches. Compute the weak hashes and bucket addresses for 32
  consecutive offsets, load the first word of each bucket into a small array in
  one simple loop (the loads are independent, so the CPU overlaps the cache
  misses), then scan the buckets in a second loop. On x86_64,
  `core::arch::x86_64::_mm_prefetch` can issue the prefetches explicitly; on
  other architectures the batched loads do the same job.
- Scan a bucket with SIMD: two AVX2 compares (`_mm256_cmpeq_epi32` and
  `_mm256_movemask_ps`) or four SSE2 compares cover the 16 slots; four
  `vceqq_u32` on aarch64. Select the AVX2 version at run time with
  `is_x86_feature_detected!` and `#[target_feature]`, or use the `wide`
  crate's `u32x8` for a portable version.

Why a table and not a filter: when the structure is larger than the last-level
cache, throughput is bounded by memory-level parallelism, so it scales with the
number of cache lines touched per lookup. A binary fuse filter touches 3
independent lines per lookup; this table touches about 1. Expect 2 to 3 times
the scan rate. Measure both with a microbenchmark before the first release; each
is about 100 lines (`xorf` provides the filter, section 9).

### 5.6 The hash list cache and direct deltas

Each full list is the canonical encoding of section 5.4, identified by
`(strong_hash_id, full_digest)`. The index and account IDs organize cache
directories but do not identify list content. There is no generation,
delta log, or segment chain. Compute a direct delta from any available base.

Account and directory identities:

- Each UNIX account creates one random 128-bit `account_id`, stored as hex
  in `$XDG_CONFIG_HOME/hashsync/account-id` (fallback
  `~/.config/hashsync/account-id`), independently of `--config`. Publish it
  once from a complete, fsynced temporary file using an atomic no-replace
  operation, then fsync the directory. Concurrent initializers use the
  winning file's ID; never overwrite an existing ID. Missing directories
  are created privately and their publication is also made durable.
- HELLO exchanges account IDs; the receiver also supplies its `index_id`.
  `serve` needs an account ID but does not open or create its own index.
- Let `A` be the receiver account ID, `B` the sender account ID, and `I` the
  receiver index ID, all encoded as fixed-width lowercase hex. Under
  `$XDG_CACHE_HOME/hashsync/` (fallback `~/.cache/hashsync/`), the receiver
  stores `local-hashlists/B-I/<algorithm>-<digest>.cache`, and the sender
  stores `remote-hashlists/A-I/<algorithm>-<digest>.cache`. Thus neither
  hostnames nor SSH aliases are identities, and indexes of one account are
  separate. Both sides use the receiver's index ID; no sender index exists.
- Directories have mode 0700 and files mode 0600. A file has a versioned
  cache header with its list ID, `block_size`, `weak_hash_id`, and encoded
  length, followed by the encoded full list. Hash only the canonical list
  bytes, not account IDs, index IDs, timestamps, or the cache wrapper.

Exchange:

1. The receiver exports, encodes, and hashes its current list in one stable
   database snapshot. After HELLO supplies the sender's account ID, it
   durably publishes this target in `local-hashlists/B-I/` before sending
   any list or delta. Retain the encoded bytes or an open descriptor for
   the exchange. Failure to persist on the receiver ends setup with an error.
2. The sender examines its receiver directory and selects the compatible,
   valid full list with greatest `ctime`; ties use the filename's byte
   order. Open and verify candidates, skipping unreadable, missing, or
   damaged files. Compatibility requires a supported encoding and matching
   block size and weak-hash parameters; the base's strong algorithm may differ.
   Advertise the selected list ID or an explicit absence,
   and retain its fd or decoded values until no longer needed. Selection
   need not represent a globally newest snapshot; any valid base works.
3. If base and target IDs match, send HASHKEEP with no list data. Otherwise
   search all `local-hashlists/*/*.cache` on the receiver for the advertised
   ID, including other peers and indexes. Open and verify any candidate
   before using it. A missing or damaged base causes a full HASHLIST.
   With a base, merge the sorted lists to compute removed and added values,
   encode each as in section 5.4, and send HASHDELTA only if smaller than
   the full-list transfer. Lists from unrelated histories may have large
   deltas. The base and target algorithms may differ; each ID identifies
   how its own bytes are verified.
4. The sender decodes a full list or applies the delta to its retained base,
   re-encodes canonically, and verifies the full target digest. A failed
   delta verification requests a full list once with HASHFULL; it never
   publishes the failed result. A full-list digest mismatch is a protocol
   error. HASHKEEP also uses the verified retained base.
5. Build the hash table and durably publish the verified target in the
   sender's receiver directory. Send `CACHE_RESULT { target, stored: 1 }`
   only after publication succeeds. Then remove the advertised old file
   from that directory if its ID differs from the target's. An inability
   to store gives `stored: 0`, logs a warning, and still permits the command
   transfer; retain the old cache. `sender_cache = false` advertises no
   base, stores nothing, and removes earlier files of this receiver/index
   directory; it also reports `stored: 0`.
6. On `stored: 1`, the receiver can remove that exchange's old list from
   `local-hashlists/B-I/`, if present and different from the target. It does
   not delete the same ID from another peer or index directory. With
   `stored: 0` or a lost acknowledgement, retain the old file until expiry.
   Both sides then expire old caches as described below.

Publication and concurrency:

- Write a unique hidden temporary file in the destination directory, fsync
  the file, rename it over its final content-derived filename, then fsync
  the parent directory. Also fsync parent directories when newly creating
  the directory hierarchy. Publish a complete file even when the target ID
  already exists; this avoids trusting a stale, deleted, or corrupt copy.
  HASHKEEP may use the bytes of its retained descriptor for this publication.
- No cache locks are needed. Different IDs have different filenames; equal
  IDs designate equal verified list bytes. Published files are never edited
  in place. A concurrent replacement or unlink cannot change bytes held by
  an open fd or in memory. A directory scan is not a snapshot: tolerate
  disappearance between listing and open, and fall back to a full transfer.
- Cleanup or expiry can race with a store or touch and remove a recently
  used filename. This is allowed: it can lose a future delta opportunity,
  not corrupt a current transfer. An acknowledgement reports completed
  durable publication, not a promise against later deletion. There is no
  mutable "current list" pointer or generation ordering to protect.
- Touch verified files after successful use (`futimens` with `UTIME_NOW`),
  on the open descriptor. A successful store also renews `ctime`. The
  sender's current target is used once verified and accepted; the receiver
  renews its target and any base used after successful CACHE_RESULT receipt.
  `ctime` is the cache's use/renewal marker, not a creation timestamp.
  It also advances on other metadata changes, which can delay expiry.
- After the exchange, each side scans both
  `local-hashlists/*/*.cache` and `remote-hashlists/*/*.cache`, deleting
  entries whose `ctime` is older than `hashlist_max_age` (default 90 days
  since last successful use/renewal). The receiver sends this setting to
  `serve` in HELLO. Missing files and failed cleanup are nonfatal. Old
  hidden temporary files from interrupted publication can be removed using
  the same age bound; newly written temporaries are not normal cache entries.
  Expiry may trigger a later full transfer. Never delete the target merely
  because it equals the advertised old ID.

Cache storage grows with peers and retained versions; there is no two-list
bound per index. Lost acknowledgements leave harmless extra files. Restoring
an older database or rebuilding it needs no cache repair: each list still
names its own content. The database need not be checkpointed before export.

Sizes for the 8M-block example of section 5.4. A sparse list value costs
about `W - log2(n) + 2` bits: 46 bits at 0.8M values.

| Sent | Size | At 20 Mbit/s |
|---|---|---|
| Full list | 43 MiB | 18 s |
| Delta, 0.8M removed and 0.8M added (10 % churn) | ~9 MiB | 3.7 s |
| Delta, 80k removed and 80k added (1 % churn) | ~0.9 MiB | 0.4 s |

Each exchange costs export and encoding of the target, and optionally one
linear merge for the delta; section 12 considers encoding removals as ranks.
`sender_cache` (default true, with per-host overrides) controls whether the
sender may retain a list. The weak hashes reveal indexed content hints, so
use trusted peers (section 6.5).

## 6. The transfer protocol

### 6.1 Transport: two SSH sessions

Why two channels: a query behind literal data waits for the sshd channel
window (about 2 MiB in OpenSSH) and kernel socket buffers (several MiB).
At 20 Mbit/s this adds seconds of latency. Outstanding queries cap throughput
at about `lookahead / query_latency` (section 6.3), which dominates on a
stream with a high deduplication ratio. A separate TCP connection keeps query
latency near the network round-trip time.

SSH connection multiplexing (`ControlMaster`), port forwarding (`-L`, `-R`), and
stderr all share one TCP connection with the data, so they do not solve this.
Two independent `ssh` processes do.

The SSH command comes from the configuration (`ssh`, default `ssh`), from a
per-host override, or from `--ssh` on the command line, like rsync's `--rsh`.
It is split like a shell would split it, so a port or an identity file goes
there: `ssh -p 2222 -i ~/.ssh/key`. The program then appends
`-o Compression=no` for both sessions, because the data session is already
zstd-compressed (section 6.2) and the control session is incompressible; this
overrides a `Compression yes` from the user's `ssh_config`. `ssh` uses the
first value it obtains for an option, so an explicit `-o Compression=yes` in
the configured command still wins. The `HOST` argument of `hashsync run` is
appended last and unchanged, so `user@host` and `ssh_config` aliases work.

Rendezvous:

1. The receiver generates a random token `T` and starts two processes at the
   same time with `std::process::Command` (stdin and stdout piped, stderr
   inherited; `SSH` stands for the configured command):
   - data session: `SSH HOST hashsync serve --token T`
   - control session: `SSH HOST hashsync attach --token T`
   The user command and its arguments are not on the `ssh` command line; the
   receiver's HELLO carries them (section 6.2).
2. `serve` creates a Unix socket `$XDG_RUNTIME_DIR/hashsync/T.sock` (fallback: a
   private directory under `/tmp`), mode 0600, and listens on it before it
   reads anything. It reads the receiver's HELLO frame from stdin, looks up its
   cached hash list for the receiver's index (section 5.6), writes its own HELLO
   frame to stdout, accepts the `attach` connection, reads the hash list or
   the delta from stdin, verifies it and builds the hash table, reports
   CACHE_RESULT after attempting cache publication, and then starts the user
   command. HASHFULL can request a full list after a failed delta (section
   6.2). Flush each handshake response before waiting or starting the command.
   None of these waits has a timeout (section 3): when the
   control session never arrives, `serve` waits until its stdin reaches end
   of file, which happens when the receiver process ends (section 6.7).
3. `attach` connects to the socket. When the socket does not exist yet
   (`ENOENT`, or `ECONNREFUSED` before `listen`), it sleeps 10 ms and tries
   again, for as long as its stdin has a writer: between two attempts it
   polls stdin for `POLLHUP` and exits when the receiver's side of the session
   is gone. Then it copies its stdin to the socket and the socket to its
   stdout, and shuts down the socket's write side when stdin reaches end of
   file. It has no other logic.
4. Start the user command with `std::process::Command`: stdin `/dev/null`,
   stdout piped, stderr inherited so that `ssh` forwards it to the receiver's
   terminal. Give it its own process group (`CommandExt::process_group(0)`)
   so `serve` can kill its descendants too (section 6.7).
5. After END, the receiver closes both sessions and waits for their processes
   as specified in section 6.7.

Notes:

- If the user's ssh configuration enables `ControlMaster`, both sessions share
  one TCP connection. The program still works; only the latency benefit shrinks.
  Document this for users.
- Provide `--single-connection` as a fallback: one session, both logical streams
  multiplexed in the frame layer, and a writer on the sender that drains control
  frames before data frames. No multiplexing library: the frame layer is enough,
  and the Rust ones (`yamux`) are asynchronous.
- Run the SSH command as a subprocess. Do not use an SSH library (`russh`,
  `ssh2`). The subprocess honours the user's configuration, agent, `ProxyJump`,
  and host key checks.
- The sender host needs the same `hashsync` version in `PATH`, or the path given
  by `sender_command` in the configuration. Both sides check versions in HELLO.

### 6.2 Frames

Every frame is `type u8, length u32 (little endian), payload`. Integers are
little endian. A string or byte string is `length u32` followed by its bytes;
a list is `count u32` followed by its items. Output offsets and lengths are
`u64`, with checked arithmetic; overflow is a protocol error.

`S` is the fixed full digest length of the negotiated strong algorithm
(section 5.3). A `list_id` encodes `algorithm string` followed by that
algorithm's fixed-length full digest. A base list may use a different
supported algorithm from the target. An optional list ID has a `present u8`
(0 or 1), followed by the ID only when present; no digest value is a sentinel.

| Session | Direction | Types |
|---|---|---|
| data | receiver to sender | HELLO, HASHLIST, HASHDELTA, HASHKEEP, LISTDATA |
| data | sender to receiver | HELLO, HASHFULL, CACHE_RESULT, LIT, REF, RLE, END |
| control | sender to receiver | HELLO, QUERY |
| control | receiver to sender | HELLO, ANSWER |

Payloads:

- HELLO, receiver to sender (data): `protocol_version u32`,
  `program_version string`, `block_size u32`, `weak_hash_id string`,
  `strong_hash_id string`, `W u8`, `account_id [u8; 16]`,
  `index_id [u8; 16]`, `target list_id`, `sender_cache u8`,
  `hashlist_max_age u64` (seconds), `lookahead u64`,
  `compression_level i32`, `compression_threads u32`, `negative_cache u64`
  (bytes), and `command`, a list of byte strings containing argv. Run argv
  directly without a shell, finding argv[0] in PATH as `execvp` does. To
  request a shell, use `hashsync run host -- sh -c '...'`.
- HELLO, sender to receiver (data): `protocol_version u32`,
  `program_version string`, `block_size u32`, `weak_hash_id string`,
  `strong_hash_id string`, `W u8`, `account_id [u8; 16]`,
  `base optional_list_id`. The base is selected by ctime and retained as
  described in section 5.6. `serve` has no index ID of its own.
- HELLO, both directions (control): `protocol_version u32`,
  `program_version string`. Both sides send it at once.
- HASHLIST: `target list_id`, `length u64`, followed by exactly that many
  encoded full-list bytes in LISTDATA frames.
- HASHDELTA: `base list_id`, `target list_id`, `removed_length u64`,
  `added_length u64`, followed by the encoded removed list and encoded added
  list in LISTDATA frames. The base must equal the sender's advertised ID;
  the target must equal the receiver's HELLO target. No segments follow.
- HASHKEEP: `target list_id`. It must equal both the advertised base and
  target; no LISTDATA follows. Verify and use the already-open full list.
- LISTDATA: at most 1 MiB of the announced bytes. Count exact lengths;
  excess bytes or an unexpected frame before completion are protocol errors.
  Full lists and reconstructed delta results must match the target's full
  digest. Enforce the canonical encoding, including padding and ordering.
- HASHFULL: `target list_id`. Sent once if delta reconstruction fails digest
  verification, before any command output. The receiver answers with
  HASHLIST and its bytes for that target. A full-list mismatch is a protocol
  error; a failed delta is never published or used to build the final table.
- CACHE_RESULT: `target list_id`, `stored u8`. A single response after
  successful target verification: 1 means durable cache publication
  succeeded; 0 means disabled or failed sender caching. Both allow command
  transfer. A wrong target or repeated result is a protocol error. Flush
  this frame before starting the command; it must precede all output frames.
- LIT: literal bytes. RLE: `byte u8`, `length u64`. REF: empty payload,
  naming the pin at the receiver's current output offset (section 6.4).
- END: `status i32`, `error string`. Status is the command's exit status,
  `128 + signal`, or 127 with an error when the command cannot start.
  Nothing follows END.
- QUERY: `offset u64`, `strong [u8; S]`, the full digest of that window.
  There is at most one QUERY per offset during a run. Offsets need not
  increase, because a restart can reach an earlier window.
- ANSWER: `offset u64`, `found u8` (0 or 1). Answers can arrive in any order.
  The sender discards answers below its emit position; every other answer
  must name a waiting query. Its query-state map holds the full digest.

Protocol and program versions must be compatible, and `block_size`,
`weak_hash_id`, `strong_hash_id`, and `W` must agree. Unsupported algorithms,
wrong digest lengths, or parameter mismatches fail before command startup.
The receiver's target uses its negotiated strong algorithm. These changed
frames require a protocol version bump; generations and SEGMENT are absent.

The data handshake is receiver HELLO, sender HELLO, durable receiver cache
publication, then HASHLIST/HASHDELTA/HASHKEEP and any announced bytes. The
sender either requests the full-list fallback or replies CACHE_RESULT. The
receiver processes that result and cache cleanup before entering its output
loop. After the successful exchange it sends no further data-session frames
and leaves stdin open until END, as required by the shutdown protocol.

Compression. After the HELLO exchange, the sender-to-receiver direction of the
data session is one zstd stream. All later sender-to-receiver frames,
including HASHFULL and CACHE_RESULT before LIT, REF, RLE, and END, travel
inside it. Setup responses are explicitly flushed before either side waits.
Compression is done by libzstd through the `zstd` crate (section 9). The
main thread hands its encoder to the data writer after the handshake, so
setup and command output share one stream. Rules:

- The sender's data writer flushes the compressor (`ZSTD_e_flush`,
  `Encoder::flush`) when its queue runs empty after the scanner asked for a
  flush, which the scanner does before every wait (section 6.3), so a pause
  in the command output or a wait for answers never leaves frames stuck in
  the compressor. It ends the stream after END (`Encoder::finish`). Flushes
  cost a little ratio and happen only when the stream is idle anyway.
- The level comes from `compression_level` in the receiver's configuration (zstd
  scale 1 to 19, 0 disables). Default 9. Levels 6 to 9 keep most of the ratio
  gain over the zstd default of 3 at roughly 100 MB/s per thread on current x86
  cores; above 12 the ratio grows by a few percent for several times the CPU.
  `compression_threads` sets libzstd's worker threads (`Encoder::multithread`;
  default 0: half the sender's cores, at least one), so the compressor keeps up
  with a gigabit link at level 9 on a few cores. Lower the level when the
  sender's CPU, not the network, is the bottleneck; raise it on slow links.
- Only literal bytes cost compression time. REF and RLE frames are a few bytes
  each. Runs shorter than `block_size` stay in the literals, where zstd encodes
  them cheaply.
- Nothing else is compressed: the control session carries random hashes and
  one-byte answers, and the hash list is already entropy-coded.
- The receiver decompresses with the streaming reader
  (`zstd::stream::read::Decoder`); it yields output block by block, so frame
  parsing is not delayed by more than one zstd block.

### 6.3 The sender's scanner

`B` means `block_size`. Offsets count bytes of command output from 0, as
defined in section 2.

One scanner thread makes every decision about the output and owns the record
list and maps below. The other threads handle I/O and compression (section
6.6); receiver workers answer queries in any order (section 6.4).

#### Data structures on the sender

**The lookahead buffer.** It holds the output bytes that are not emitted yet.
Once read, these bytes never change. Three positions describe it:

- `e`: the emit position, the offset of the next byte to emit over the data
  session.
- `p`: the scan position, the offset of the next byte that the scanner
  processes.
- `r`: the offset after the last byte read from the command.

At the start `e = p = r = 0`. Always `e <= p <= r`. The buffer holds
`[e, r)`: all bytes from `e` up to, but not including, `r`. The bytes
`[p, r)` are not processed yet. The bytes before `p` can be needed again
after a "no" (Q3 below).

The buffer has room for `lookahead + B` bytes (`lookahead` from the
configuration, default 256 MiB, at least `B`). The reader pauses when the
buffer is full. A scanning step that needs a full window starts only when
`p - e < lookahead`. Such a step can advance `p` by `B`, so `p` can end a
little beyond `e + lookahead`; the extra `B` bytes of room allow this. The
buffer is a `memfd` mapped twice back to back (`memfd_create`, then two
`mmap` calls with `MAP_FIXED` into one reserved range), so every window is
one contiguous slice and the wrap-around costs nothing.

Two flags belong to the buffer: whether the command's output has reached end
of file, and whether the command's exit status is available. They are
separate conditions (section 6.6).

**The record list.** A list of output records in offset order. Together the
records describe exactly `[e, p)`, with no gaps and no overlaps. The record
types are:

- `Literal { offset, length }`: bytes to copy from the lookahead buffer.
- `Run { offset, length, byte }`: `length` copies of `byte`.
- `Query { offset }`: the window at `offset`, whose answer is not resolved in
  this list.
- `Ref { offset }`: the window at `offset`, confirmed by a "yes" for this
  exact offset.

Rules:

- A Query blocks emission. Neither it nor any record after it is emitted
  while the Query is in the list.
- A "no" deletes the Query and every record after it (Q3), also records that
  are already a Ref, a Run, or a Literal.
- A Literal or a Run can grow while it is the last record of the list. Its
  existing bytes can be emitted before it stops growing; it does not wait
  for end of file. The emission rules (E1 to E4) control the size of the
  outgoing frames.

**The query-state map.** A map indexed by offset. An entry holds the full
strong hash of the window at that offset and one of four states:

- `Not_sent`: the hash was computed, but no QUERY was queued or sent.
- `Waiting`: exactly one QUERY was queued or sent, and no answer has arrived.
- `Yes`: that QUERY got a "yes".
- `No`: that QUERY got a "no".

The map also caches computed strong hashes, with entries only at offsets
where one was computed. Entries stay until their offsets are below `e`
(E4), including when a "no" deletes their records.

A `Waiting` entry stays `Waiting` when its Query record is deleted. When the
scanner reaches that offset again, it appends a new Query record for the
same request; it does not send another QUERY. A queued QUERY is not
cancelled when its record is deleted.

**The negative cache.** An LRU set of strong hashes with a byte limit
(`negative_cache` in the configuration, default 100 MiB, about 1.4M hashes;
the receiver's HELLO carries the size). A "no" inserts its hash, and a "yes"
removes it. Answers for offsets below `e` do not change it (A1). The cache
avoids a query for content that got a "no" at another offset. Its key is the
strong hash, not the weak hash: two windows with the same weak hash and
different content are possible, and a "no" for one must not suppress the
query for the other. The cache therefore saves the query but not the strong hash
computation. Periodic content never reaches this path: the index holds no
short-period blocks (section 4.1), so its windows miss the table without a
strong hash. The size limit of the cache has no effect on the lifetime of the
entries of the query-state map.

**The queues.** Three queues connect the scanner to the other threads
(section 6.6): incoming ANSWER messages, outgoing QUERY messages, and
outgoing data frames. The incoming answers need no order. Only the scanner
changes the query-state map and the record list.

- The outgoing QUERY queue is unbounded, and queuing a QUERY never waits for
  room. The control writer sends the messages in queue order, also when the
  query offsets go backwards. The lookahead and the queue order bound the
  backlog indirectly: a query queued later cannot get its answer before the
  older queued queries are sent, and an unanswered Query record stops
  emission. Deleted records can leave obsolete requests in the queue, so the
  bound is not simply the number of windows in the lookahead buffer.
- The outgoing data-frame queue is bounded. The scanner checks whether the
  queue can accept a frame instead of blocking in a send (E4). A full data
  queue must not stop the scanner from processing answers. The bound gives
  backpressure: a frame handed to the data writer advances `e` and frees
  room in the lookahead buffer, so a slow link pauses the scanner, then the
  reader, then the command.
- The reader, the control reader, and the data writer notify the scanner
  when input, answers, or room in the data queue become available.

To emit a record means to hand its frame to the data writer, in list order.
A LIT frame owns a copy of its bytes before `e` advances, so the lookahead
buffer can discard those bytes. The data writer sends the frames in queue
order and applies the zstd compression (section 6.2).

#### Appending records

- To record literal bytes: extend the last record if it is a Literal;
  otherwise append a new Literal. The bytes must begin at `p`. Advance `p` by
  their length.
- To record a run: extend the last record if it is a Run with the same byte;
  otherwise append a new Run at `p`. Advance `p` by the length of the run.
- To append `Query { p }` or `Ref { p }`: advance `p` by `B`.
- Never append an empty record. Once a record, or a prefix of it, has been
  emitted, later changes apply only to the bytes that remain in the list.

#### The main loop

The scanner repeats these steps:

1. Process every ANSWER in the incoming queue (A1 to A3). Answers have
   priority over more input. Then resolve the first Query records (Q1 to
   Q3).

2. Emit every record, or prefix of a record, that is ready (E1 to E4).

3. If end of file has been reached, `p = r`, and the list holds no Query
   record, finish: emit every remaining record, including a short final
   Literal. If the data queue is full, continue to process answers and wait
   for room. When the list is empty, `e = p = r`. Wait for the command's
   exit status if it is not available yet, then queue END after all data
   frames. The data writer then ends the zstd stream, and the shutdown of
   section 6.7 follows.

   Deleted Query records can still have unanswered requests on the network.
   They do not delay the end: their bytes were emitted in another way, and
   their late answers are not needed.

4. If `p - e >= lookahead`, pause scanning: wait for answers or for room in
   the data queue, the events that let `e` advance. Otherwise check whether
   at least `B` bytes are available from `p`, that is, whether `r - p >= B`:
   - Yes: go to step 6.
   - End of file reached and `0 < r - p < B`: go to step 5.
   - End of file reached and `p = r`: there is nothing to process. Wait for
     answers or for room in the data queue; do not repeat step 5 on an
     empty tail.
   - Otherwise: wait for more input or for an answer.

   Before any wait, emit a ready short Literal (E3) and ask the data writer
   to flush the compressor when its queue is empty. Process answers before
   the next scanning step. If the emission of records makes the next step
   possible, return to step 1 without a wait. The same applies when the
   data queue is full. Wait for an event that can allow progress; do not
   retry an unchanged full queue or an unchanged input buffer.

5. Fewer than `B` unprocessed bytes remain at end of file. They are exactly
   `[p, r)`; they do not include bytes held for earlier Query records. If
   the last record is a Run, extend it by as many of these bytes as equal
   its byte, and record the remaining bytes as literal. Otherwise record all
   these bytes as literal. Now `p = r`. A Run that was already emitted is no
   longer in the list, so it cannot be extended; its short continuation is
   then literal. Return to step 1. An earlier Query can still get a "no" and
   make the scanner process this tail again.

6. If the window at `p` consists of one repeated byte, record those `B`
   bytes as a run, which advances `p` by `B`, and return to step 1. Later
   iterations process the further pieces of the run.

7. Look `p` up in the query-state map. If an entry exists, go to step 9: its
   hash and its answer still describe the same bytes.

8. Probe the hash table with the weak hash of the window at `p`:
   - Miss: record the one byte at `p` as literal and return to step 1.
   - Hit: compute the strong hash of the window, store it in the map at `p`
     with state `Not_sent`, and continue with step 9.

9. Use the map entry at `p`:
   - `Yes`: append `Ref { p }` and return to step 1.
   - `Waiting`: append `Query { p }` and return to step 1. Do not send
     another QUERY.
   - `No`: record the one byte at `p` as literal and return to step 1.
   - `Not_sent`: look the stored hash up in the negative cache. On a hit,
     record the one byte at `p` as literal and return to step 1; the entry
     keeps its hash and its state `Not_sent`. Otherwise queue
     `QUERY { offset: p, strong: hash }`, set the entry to `Waiting`, append
     `Query { p }`, and return to step 1. Complete these three changes
     before the scanner processes any answer.

An entry at one offset never justifies a Ref at another offset, also when the
hashes are equal. Every Ref needs a "yes" for its own offset (section 10).

#### Processing an answer

- **A1.** For `ANSWER { offset: x, found }`: if `x < e`, discard the answer and
  leave the negative cache unchanged, whatever `found` is. The bytes at `x` were
  emitted, and the scan cannot return there. This is a normal late answer to a
  query that was not used. The receiver can answer "no" for such an offset
  without a lookup (L1 and L3 in section 6.4), so the answer does not establish
  a miss that the cache could record.

- **A2.** Otherwise find the map entry at `x`. It must exist and be `Waiting`. A
  missing entry or another state is a protocol error (section 6.5).

- **A3.** Set the state to `Yes` or `No` according to `found`. Use the hash of
  the entry to remove it from the negative cache (`Yes`) or to insert it (`No`).
  Do not change `p` or the record list here. Continue with the other available
  answers, then resolve records in offset order (Q1 to Q3).

#### Resolving Query records

- **Q1.** Find the first Query record of the list. If there is none, return to
  the main loop. Let its offset be `x`, and look at its map entry.

- **Q2.** If the state is `Waiting`, stop resolving for now. If it is `Yes`,
  replace the Query with `Ref { x }` at the same position and repeat Q1.

- **Q3.** If the state is `No`, delete this Query and every record after it.
  Keep every earlier record, the bytes of the lookahead buffer, and the
  query-state map. Set `p = x`. Record the one byte at `x` as literal, which
  advances `p` to `x + 1`. Return to step 1 of the main loop. The answer
  established that the window at `x` was rejected, so that window is not hashed
  or queried again. The scan from `x + 1` examines the possible matches inside
  the rejected window, including matches that cross later records of the old
  list.

#### Emitting records

- **E1.** Look at the first record only. If there is none, return. If it is a
  Query, stop: neither it nor any record after it can be emitted yet. The first
  record always starts at `e`.

- **E2.** For a Ref, emit REF, with no payload, and advance `e` by `B`. For a
  Run, emit `RLE { byte, length }` for its whole known length and advance `e` by
  that length. Remove the emitted record. A new Run begins with `B` bytes, so it
  can be emitted at once when no earlier Query blocks it.

- **E3.** For a Literal, emit its first `B` bytes as one LIT frame whenever it
  holds at least `B` bytes. A shorter Literal can be emitted too: when another
  record follows it, when the scanner would otherwise wait, or when finishing at
  end of file. Advance `e` by the emitted length. Remove those bytes from the
  Literal by moving its offset forward, and remove the record when it is empty.
  Otherwise leave the short last Literal in the list, where it can grow.

- **E4.** After `e` has advanced, discard the buffer bytes and the map entries
  whose offsets are less than `e`. Repeat E1 while another frame can be emitted
  and the data queue has room. If a frame cannot be queued, leave its record and
  `e` unchanged. The reader continues as buffer room becomes free.

#### Cost

**Rolling.** The weak hash and the test for a constant window are updated as `p`
advances one byte: the rolling hash of section 5.1, and one comparison per
byte for the run check. The run check tracks the start of the run of equal
bytes that ends at the newest byte of the window; the window at `p` is
constant when that run starts at or before `p`. The scanner never examines
all `B` bytes again only to make either test at the next offset. After a
jump of `B` bytes (a Query, a Ref, or a Run) or a restart (Q3), it
initializes both for the new position: Horner's rule over the window for the
hash and one pass over the window for the run check. It never uses a hash or
a run start left over from another window.

**Strong hashing.** The map avoids a second hash computation at the same offset, also
when the negative cache prevented the query, and it avoids a duplicate query
after a restart. Entries exist only for the buffered part of the stream, but
in a bad case there is one entry per byte, not one per block.

**Rescans.** A "no" discards the whole later list, for simplicity. The rescan
can then repeat the weak-hash probes and the record construction over a long
suffix: all the scanning work of one round trip. This version accepts that
cost instead of extra records that say where a rescan could rejoin the old
list (section 12).

**Dense hits.** On dense positive matches the scanner hashes and queries one
window per `B` bytes, and the probe rate no longer bounds the throughput.
Dense false weak hits at distinct offsets can still cost a strong hash
computation per byte; the map only avoids a second computation at the same
offset. A stale hash list can produce such false hits even when accidental
weak collisions are rare. Neither case changes the reconstructed bytes.

CPU cost per byte, outside runs and skipped windows: the rolling hash (4 to
5 cycles on the dependency chain), `mix64` (section 5.2) and the slot
computation (independent of the chain), and one table probe. The probe
dominates when the table is larger than the last-level cache: about one
cache line per byte, which gives roughly 120 to 180 MB/s from DRAM and
several hundred MB/s when the table fits in L3. The probes can be batched as
in section 5.5 while the scanner advances one byte at a time; the results of
a batch after a hit or a run are discarded. Compression (section 6.2) runs in
libzstd's own threads and touches literal bytes only, so on a stream with a
high deduplication ratio it costs little. Inside a run the scanner neither
hashes nor probes, so a long run of one byte costs a memory scan only.

### 6.4 The receiver's answering and reconstruction

#### Data structures on the receiver

- **The output offset `c`**: the offset of the next byte that the receiver
  writes to its stdout. It starts at 0.
- **The pin map**: a map from query offsets to pinned, verified block bytes.
  A pin holds the `B` bytes of a block that verified against the strong hash
  of the query at that offset. Pins for identical content can share one
  immutable buffer (`Arc`), but each query offset has its own pin. A pin is
  released when `c` passes its offset (D3).
- **One lock** protects `c`, the pin map, and the verified-buffer cache
  below. Every access holds it, twice per query and up to twice per frame.
  Slow disk reads and output writes do not hold it.
- **The verified-buffer cache**: an LRU cache of 256 MiB of verified block
  bytes, keyed by the full strong hash. A buffer enters it when its last pin
  is released. Full strong-hash equality is byte equality, so a later query for the same
  content is answered from the cache without a disk read or a hash
  computation; it only creates a new pin on the cached buffer.
- **The lookup workers**: a pool of 4 to 8 threads, each with its own read
  connection (section 6.6). They keep disk latency off the critical path and
  may answer out of order.

The pin map has no limit. It grows with the queries in flight, which the
sender's lookahead and the in-flight stream bound in practice, and the receiver
never answers "no" for lack of memory. Memory is not a concern of this
design, and the peer is trusted (section 6.5).

#### Answering a QUERY

- **L1.** Receive `QUERY { offset: x, strong: h }`. Read `c` under the lock. If
  `x < c`, the bytes at `x` were already reconstructed: send
  `ANSWER { offset: x, found: 0 }` and create no pin. The sender discards this
  obsolete answer (A1 in section 6.3).

- **L2.** Otherwise look for bytes with full hash `h` in the verified-buffer
  cache. If absent, select block rows whose stored strong prefix equals
  `h[0..16]` through `blocks_by_strong` (section 4.4). For each block,
  enumerate all indexed `(root_id, rel_path)` paths and all current root
  locations from the in-memory configuration map (section 4.2). Prefer the
  configured location order. Identical location/offset pairs need only one
  attempt; an alternate copy need not have the scanned inode's identity.

  Stop on success, exhaustion, or `candidate_attempts` attempts (default
  128, configurable, positive). The budget is per query across all blocks,
  paths, and copies, not per block. Failed opens and reads count; a block
  with no usable path/location also consumes an attempt. Never exhaust an
  unbounded collection of stale rows before charging the budget.

  For each attempted location:

  1. If `rel_path` is empty, this is a single-file root: open the root file
     itself with `O_RDONLY|O_NOFOLLOW|O_NONBLOCK|O_NOCTTY|O_CLOEXEC`.
     Do not require `O_DIRECTORY`. Otherwise open the root with
     `O_PATH|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`, then open the relative path
     with `openat2` using `O_RDONLY|O_NONBLOCK|O_NOCTTY|O_CLOEXEC` and
     `RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS`. On older kernels, use
     component-wise `openat` with `O_NOFOLLOW`, directory flags on ancestors,
     and the file flags on the final component. Close ancestor descriptors
     once the next descriptor has opened; nothing stays open between attempts.
  2. `statx(fd, "", ...)` on the opened candidate must identify a regular
     file. Otherwise close it and continue. A stale path may now name a
     FIFO, socket, directory, or device; its type is never trusted from the
     index. The flags and checks have the same purpose as in section 4.5.
  3. Read exactly `B` bytes at `block_no * B` with `pread`, accumulating
     short reads as needed. Hash those bytes with the negotiated algorithm
     and compare the entire digest with `h`. Error, EOF before `B` bytes,
     or mismatch continues to the next attempt. The stored prefix alone
     never justifies a "yes". Successful bytes stay in their immutable buffer.

  A successful disk candidate costs a block read and one strong hash. All
  filesystem failures are candidate misses; they do not terminate the run.
  The full hash, rather than a path or inode identity check, authorizes the
  bytes. Already-verified cached buffers likewise use the full digest key.

- **L3.** When the lookup has ended, take the lock and compare `x` with `c`
  again. If `x < c`, create no pin and answer "no" as in L1. Otherwise, if
  verified bytes were found, put a pin for `x` into the map; then release the
  lock and send `ANSWER { offset: x, found: 1 }`. If no candidate verified,
  release the lock and send `found: 0`.

  An advance of `c` can release the pin before its answer is sent. Such a late
  answer is safe: the sender has emitted past `x` and ignores it (A1).

A pin makes a "yes" binding: the receiver serves the later REF from memory, so a
concurrent change to the file cannot corrupt the output. A REF can never find
its bytes gone, whatever the order of the answers or the distance between
identical blocks in the stream.

#### Processing data frames

- **D1.** Process the data frames in the order received. The next frame begins
  at offset `c`; LIT, RLE, and REF frames carry no offset.

- **D2.** For LIT, write its bytes to stdout. For RLE, write `length` copies of
  `byte` from a static buffer; the receiver accepts any length of at least 1,
  and the sender only emits runs of at least `B` bytes. For REF, take the lock,
  obtain the pin at `c`, and release the lock; keep the bytes while writing
  them. A missing pin is a protocol error (section 6.5). A REF has an empty
  payload and writes exactly `B` bytes.

- **D3.** After the frame's output has been written, take the lock, advance `c`
  by the number of output bytes of the frame, and release every pin whose offset
  is less than `c`. This includes the pin that a REF used and the pins that a
  LIT, an RLE, or another REF covered. Release the lock before further output
  work.

  Do this as soon as the current frame's output has been written, also when no
  further data frame arrives, so that no pin is kept during a pause or after the
  last data frame. A pin is obsolete once the output passes its start offset,
  also when its window extends beyond `c`. For a lookup whose offset the frame
  passes, the shared lock allows only two orders: L3 creates its pin first and
  D3 releases it, or D3 advances `c` first and L3 rejects the obsolete lookup
  without a pin.

- **D4.** On END, finish the output and the shutdown of section 6.7, and exit
  with the exit status that END carries. No data frame may follow END. End of
  file without END is a failure (section 6.5).

### 6.5 Failures

- Version or parameter mismatch in HELLO: exit with an error before the command
  starts.
- The user command exits with a non-zero status: END carries the status. The
  receiver exits with the same status after it has written all data.
- The receiver gets EOF without END: exit with a non-zero status. Never report
  success for a truncated stream.
- A broken session, a broken stdout on the receiver, or SIGINT or SIGTERM on the
  receiver: the side that notices ends its process without a core dump
  (section 6.7), the receiver with a non-zero status after it killed its `ssh`
  processes, `serve` after it killed the command's process group.
- A protocol error (a frame that does not parse, an ANSWER for an offset
  that waits for no answer, a REF without a pin at the output offset, an
  offset or a length that overflows): a bug on one side, since SSH
  guarantees the integrity of the stream. It is a panic and leaves a core
  dump (section 6.7).
- Security: the sender learns the block hashes of all indexed files and can
  retain them in its cache (section 5.6). Use `hashsync` only with trusted
  hosts. For a host that must not keep the list, set `sender_cache = false`;
  the next run also deletes any earlier cached list.

### 6.6 Threads at run time

Both roles use blocking threads and channels, with no event loop (section
4.6). Every hand-off is a channel. Threads are never joined or cancelled;
normal and fatal exits end the process (section 6.7).

Sender, `serve`:

- The main thread does the handshake of section 6.1, starts the command and
  the threads below, and then blocks in `read` on its stdin until end of
  file. The data writer's finished flag distinguishes normal shutdown from
  a lost receiver (section 6.7).
- One reader thread reads the command's stdout pipe into the lookahead
  buffer until end of file, and notifies the scanner after every read. It
  waits while the buffer is full. The pipe is enlarged to 1 MiB with
  `F_SETPIPE_SZ` (`rustix::pipe::fcntl_setpipe_size`), so the reads are
  large. At end of file it sets the end-of-file flag, waits for the child
  (`Child::wait`), and publishes the exit status (section 6.3). The child
  can exit before its stdout reaches end of file (a descendant still holds
  the pipe) or after it (it closed stdout and went on); the run has ended
  only when both have happened, and END is queued only then.
- One scanner thread runs section 6.3, owns its records and maps, queues
  QUERY messages and data frames, and waits for notifications from the
  other threads.
- One data writer thread takes the frames from the bounded data queue in
  order, writes them through the zstd encoder to stdout, and notifies the
  scanner when the queue has room. It follows the flush rules of section
  6.2 and finishes the stream after END (section 6.7).
- One control writer thread writes the QUERY frames of the unbounded query
  queue to the Unix socket, in queue order and promptly: it never waits for
  more command output or for data frames. One control reader thread reads
  ANSWER frames from the socket, puts them on the incoming answer queue, and
  notifies the scanner.

Receiver, `run`:

- The main thread starts both `ssh` processes and the threads below, then
  reads through the streaming decoder and runs the data loop of section
  6.4. Blocking stdout writes give backpressure to the whole chain. After
  END it follows the shutdown sequence of section 6.7.
- One control reader thread reads QUERY frames and dispatches them to the
  lookup workers over a channel (section 6.4); one control writer thread
  writes the ANSWER frames that the workers put on another channel, promptly
  and in the order the workers produced them.
- One signal thread blocks in `signal_hook::iterator::Signals` for SIGINT and
  SIGTERM and dies (section 6.7) when one arrives, with status 128 plus the
  signal number.

`attach` copies with two threads and `std::io::copy`, which uses `splice`
between a pipe and a socket on Linux. The thread that copies stdin to the
socket shuts down the socket's write side when stdin reaches end of file; the
other thread ends when the socket reaches end of file. When either copy fails
(`EPIPE`, a reset), `attach` exits.

### 6.7 Shutdown

There is no in-process cancellation. Another thread's `close` does not
interrupt a blocked `read` or `write`; `pthread_cancel` is unusable from
Rust; and `pthread_kill` with an empty handler races with entry into the
call, while helpers that retry `EINTR` swallow the interruption (section
10). Persistent state needs no orderly stop: the index uses WAL, and both
roles publish complete cache files under temporary names and rename them
(section 5.6). Interrupted files are ignored until expiry. Every fatal
condition therefore ends the process. The kernel closes its descriptors,
ending peers through end of file and `EPIPE`.

Two exits exist, chosen by the root cause. A condition that is not a fault
of the program (a network failure, a consumer that closed the receiver's
stdout, a signal) ends the process without a core dump. A fault of the
program or a failure of its database (a bug, an invalid frame, a database
error) aborts and leaves a core dump. Per-entry filesystem failures are
handled locally, as specified in section 4.5.

`die(status, message)` is the first kind, one function per role, callable
from any thread. It prints the message to stderr, does the cleanup that the
kernel would not do, and exits with `status` without unwinding
(`std::process::exit`; no destructor needs to run). A second caller blocks
until the process is gone. The cleanup is:

- sender: kill the command's process group (`kill(-pgid, SIGKILL)`) and remove
  the socket file. A temporary cache file (section 5.6) is left to the expiry
  pass of the next `serve`.
- receiver: kill both `ssh` processes with SIGKILL. The kernel then closes their
  descriptors, the sender's `serve` gets end of file on stdin and dies in
  turn, and `attach` ends with its socket.

Everything else panics. The crate builds with `panic = "abort"` (section
11); a panic hook runs the same cleanup before leaving a core dump. Invalid
frames are bugs because SSH guarantees stream integrity. Database errors
and indexing invariant violations panic too; the index recovers at the next
update (section 4.4). Filesystem errors opening or reading an indexed entry
only abandon that entry and leave its root eligible for retry (section 4.5).
Cache store failures on the sender report `stored: 0`; inability to persist
the receiver's target ends setup with an error, without a core dump.

Who calls `die()`:

| Event | Noticed by | Role |
|---|---|---|
| stdout is broken (`EPIPE`) or fails | main thread | receiver |
| The data session reaches end of file before END | main thread | receiver |
| The control session reaches end of file | control reader | receiver |
| SIGINT or SIGTERM | signal thread | receiver |
| stdin reaches end of file before the data writer has finished | main thread | sender |
| stdout is broken (`EPIPE`) | data writer | sender |
| The control socket reaches end of file before the data writer has finished | control reader | sender |

A command that cannot be started is not a failure of the run: `serve` sends
END with status 127 and the error text, and the normal end follows.

Normal end, in order:

1. The reader thread of `serve` gets end of file on the command's stdout and
   the exit status from `Child::wait`.
2. The scanner emits its last records and queues END with the status
   (step 3 of section 6.3). The data writer sends them, ends the zstd
   stream, flushes and closes stdout, and sets the finished flag. `serve`
   writes nothing more.
3. The receiver's main thread receives END, writes the last bytes to stdout and
   flushes it, and closes the stdin pipes of both sessions.
4. The main thread of `serve` gets end of file on stdin, sees the finished
   flag, removes the socket file, and exits 0. The kernel closes the socket;
   `attach` gets end of file on stdin and on the socket and exits.
5. Both `ssh` processes exit; the receiver waits for them and exits with the
   status from END.

Every step waits for a definite event, and none has a deadline (section 3). A
descendant of the command that keeps its stdout open keeps the run going, as
it would under plain `ssh`. A descendant that left the process group
(`setsid`) survives `die()`, as it would under plain `ssh`, and gets `EPIPE`
at its next write.

## 7. Configuration

`$XDG_CONFIG_HOME/hashsync/config.toml`, or the path given by `--config`:

```toml
block_size     = "32KiB"   # 4 KiB or more; a change rebuilds the index
strong_hash    = "blake3"  # blake3 or sha256; fixed full output >= 16 bytes
candidate_attempts = 128  # total block/path/copy attempts per query, at least 1
hashlist_max_age = "90d"  # expire caches after this much time without renewal
index_path     = "~/.local/state/hashsync/index.sqlite"
threads        = 0         # indexer threads; 0 = 4 × cores, at least 16, at most 64
lookahead      = "256MiB"  # how far the sender's scanner runs ahead of the output
negative_cache = "100MiB"  # the sender's LRU of strong hashes answered "no" (6.3)
compression_level   = 9    # zstd level for the data session, 1 to 19; 0 disables
compression_threads = 0    # libzstd worker threads on the sender; 0 = half the cores
ssh            = "ssh"     # SSH command, split like a shell would (rsync's --rsh);
                           # the program appends -o Compression=no
sender_command = "hashsync"  # path of hashsync on the sender host
sender_cache   = true      # the sender may keep the hash list between runs (section 5.6)

[hosts.builder]            # optional overrides, keyed by the HOST argument of `hashsync run`
ssh            = "ssh -p 2222 -i ~/.ssh/builder"
sender_command = "/run/current-system/sw/bin/hashsync"
sender_cache   = false     # shared machine: do not leave the hash list on its disk

[[roots]]
path = "/nix/store/*"      # glob: each match is its own root; also matches dot names
immutable = true
identifier = "name"        # the store path's name (section 4.2); default: the full path
exclude = ["/nix/store/.links"]

[[roots]]                  # a second Nix store, on a disk mounted at /mnt/disk
path = "/mnt/disk/nix/store/*"
immutable = true
identifier = "name"        # a store path whose name is indexed already is not scanned;
                           # { strip_prefix = "/mnt/disk" } with the default on the
                           # first store would do the same
exclude = ["/mnt/disk/nix/store/.links"]

[[roots]]
path = "/srv/data"
exclude = ["*.tmp", ".cache"]
one_filesystem = true
```

The hash list width `W` (64), multiplier `a`, and `mix64` are protocol
constants. A change to `strong_hash` rebuilds the index; the stored prefix
length remains 16 bytes for every algorithm. Account IDs are persistent
state under the XDG configuration directory (section 5.6), not TOML options.

## 8. Command line

- `hashsync update [--rebuild] [-j N]`: update the index, or discard and
  rebuild it. `-j` overrides `threads`.
- `hashsync run [--no-update] [--single-connection] [--ssh CMD] [USER@]HOST -- COMMAND ARGS...`:
  run COMMAND on HOST, write its stdout to the receiver's stdout, and exit
  with its status. Normal runs hold process-exclusive index access through
  update and transfer. `--no-update` shares read-only access with other such
  runs, still loads all configured root locations, and waits for any writer.
  HOST is appended to the SSH command unchanged, so
  `user@host` and `ssh_config` aliases work. COMMAND and ARGS travel in the
  HELLO frame (section 6.2) and run on the sender without a shell. `--ssh`
  overrides the configured SSH command for this run. At the end it prints one
  summary line to stderr: output bytes, literal bytes, referenced bytes, run
  bytes, bytes on the wire, and the bytes of the hash list or delta that was
  sent.
- `hashsync serve --token T` and `hashsync attach --token T`: internal. The
  receiver runs them on the sender through the SSH command.
- `hashsync stats`: show index statistics.
- `hashsync prune`: under process-exclusive access, remove paths absent
  from every configured copy of their root, and all orphaned inodes and
  blocks, in foreign-key-safe order. Inaccessible locations are not proof
  of absence. It also loads the root configuration without storing copies.

Example:

```sh
hashsync run backup@nas -- tar -C /srv/data -cf - . | tar -C /srv/data -xf -
```

## 9. Libraries

Versions checked against crates.io on 2026-09-10; SHA-256 support added
and its crate checked on 2026-09-17. The toolchain is stable Rust
(1.98.1, released 2026-09-03), edition 2024. Three dependencies compile C
sources through the `cc` crate: SQLite, libzstd, and the Blake3 SIMD kernels.
The target's C compiler is needed at build time; there is no pure-Rust build
and no need for one.

| Need | Crate | Version | Notes |
|---|---|---|---|
| Weak hash | own code | | Sections 5.1 and 5.2, about 50 lines. |
| Hash list coding | own code | | Golomb-Rice encoder and decoder, about 60 lines. |
| Hash list cache and deltas | own code | | Section 5.6: canonical full lists, direct diff/merge, atomic publication, acknowledgements, and expiry. |
| Sender's hash table | own code | | Section 5.5, about 100 lines plus the SIMD bucket scan. |
| Strong hash: BLAKE3 | `blake3` | 1.8.7 (2026-08) | `blake3::hash`. Assembly for SSE4.1, AVX2, and AVX-512 on x86_64 with run-time detection; NEON on aarch64 by default. See below. |
| Strong hash: SHA-256 | `sha2` | 0.11.0 | `Sha256` with the `Digest` trait; fixed 32-byte output. |
| SQLite | `rusqlite` | 0.40.2 (2026-08) | Feature `bundled`: SQLite 3.53.2 through `libsqlite3-sys` 0.38.2. `prepare_cached`, `transaction_with_behavior`, `busy_handler`, `pragma_update`. See below. |
| System calls | `rustix` | 1.1.4 (2026-02) | `fs::{openat, openat2, statx, RawDir, fadvise, seek, memfd_create, futimens}`, `io::{pread, preadv2}`, `mm::{mmap, mmap_anonymous, madvise}`, `pipe::{splice, fcntl_setpipe_size}`, `process::setrlimit`, `rand::getrandom`, `ioctl::{Getter, opcode}`. Raw system calls on Linux (`linux_raw` backend). |
| System calls, gaps | `libc` | 0.2.189 (2026-08) | `name_to_handle_at`, `file_handle`, `MAX_HANDLE_SZ`. |
| Kernel constants | `linux-raw-sys` | 0.12.1 (2025-12) | `general::{fsuuid2, STATX_MNT_ID_UNIQUE}`. Already a dependency of `rustix`. |
| Thread pool | `rayon` | 1.12.0 (2026-04) | A dedicated `ThreadPool`, `scope` and `spawn`. LIFO local deques, oldest-first stealing (section 4.6). |
| Work stealing, alternative | `crossbeam-deque` | 0.8.8 (2026-09) | `Worker::new_lifo` and `Stealer`, as in `ignore`. Only if rayon is dropped. |
| Channels | `crossbeam-channel` | 0.5.17 (2026-09) | Bounded channels to the writer and between the run-time threads; `select!`. `std::sync::mpsc::sync_channel` is enough where no `select!` is needed. |
| zstd | `zstd` | 0.14.0 (2026-09) | libzstd 1.5.7 through `zstd-sys` 2.1 (bundled; the `pkg-config` feature links the system library). See below. |
| TOML | `toml` | 1.1.5 (2026-09) | With `serde` 1.0.229 derive. |
| Command line | `clap` | 4.6.6 (2026-08) | Derive API. |
| Globs | `glob` | 0.3.4 (2026-07) | `glob_with` for roots, `Pattern::matches_with` for `exclude`, options of section 4.2. |
| Hash maps | `foldhash` | 0.2.0 (2025-08) | `foldhash::fast::RandomState` for `std::collections::HashMap`. Alternative: `rustc-hash` 2.1.3. See below. |
| Identity hasher | `nohash-hasher` | 0.2.0 (2020-02) | For maps keyed by full strong hashes or the weak hash. Or 10 lines. |
| LRU cache | `lru` | 0.18.4 (2026-09) | The verified-buffer cache of section 6.4. Or about 60 lines. |
| Signals | `signal-hook` | 0.4.4 (2026-04) | SIGINT and SIGTERM flag. `ctrlc` 3.5.2 is the smaller alternative. |
| Errors | `anyhow`, `thiserror` | 1.0.104, 2.0.20 | |
| Logging | `log`, `env_logger` | 0.4.34, 0.11.11 | |
| Property tests | `proptest` | 1.11.0 (2026-03) | |
| Benchmarks | `criterion` or `divan` | 0.8.2, 0.1.21 | Table against filter; `xorf` 0.13.0 provides the binary fuse filter. |
| Temporary trees in tests | `tempfile` | 3.27.0 (2026-03) | |
| Portable SIMD, optional | `wide` | 1.7.0 (2026-08) | `u32x8` for the bucket scan if `core::arch` intrinsics are not wanted. |

### SQLite: `rusqlite`

`rusqlite` is the binding to the C library; nothing else is in question. The
`bundled` feature compiles SQLite into the binary, which keeps the build
independent of the system and makes static builds simple; without it,
`libsqlite3-sys` links the system library through `pkg-config`. A point lookup
through a cached prepared statement costs 1 to 3 µs, about one `statx`. At run
time, 10 GiB of matched data cost about 320k such lookups, each followed by a
32 KiB read and a strong-hash computation that cost more than the lookup; the
indexer's budget is in section 4.6.

Usage rules:

- Follow the connection ordering and lifetime protocol of section 4.4.
  `Connection::open_with_flags` needs `SQLITE_OPEN_URI` for URI filenames;
  percent-encode the filesystem path rather than interpolating URI syntax.
  Alternatively, `open_with_flags_and_vfs` selects `unix-excl` directly.
  Use `SQLITE_OPEN_NO_MUTEX`: each connection belongs to one thread;
  `Connection` is Send but not Sync. Keep SQLite's library-wide mutexes.
- Enable `foreign_keys` immediately on every connection. Install
  `Connection::busy_handler` with a callback that sleeps a millisecond and
  returns true indefinitely, replacing rusqlite's default busy timeout.
  If a lock acquisition still returns `SQLITE_BUSY` without invoking the
  handler, retry initialization with fresh connections before opening any
  worker readers. Non-contention failures do not count as acquiring access.
- Acquire the process lock by a real schema read before opening readers or
  changing the schema. Configure WAL and `synchronous=NORMAL` on the writer
  after exclusive initialization and before spawning reader connections.
  Read-only connections do not attempt to set the journal mode. There is
  no publication checkpoint or index generation counter.

- The writer thread owns its connection and starts transactions with
  `transaction_with_behavior(TransactionBehavior::Immediate)`, so a write
  transaction never fails with `SQLITE_BUSY` halfway through. Pool threads and
  lookup workers own read connections.
- `prepare_cached` for every statement; bind positionally; never build SQL
  strings per row. Step the rows; do not collect them.
- Paths and root identifiers are `BLOB` columns (section 4.4). Bind them as
  `&[u8]` (`OsStrExt::as_bytes`) and read them as `Vec<u8>`
  (`OsStringExt::from_vec`); never as `String`, which fails on bytes that are
  not UTF-8.
- `inodes.size` crosses the boundary as `Option<i64>`: `Some(size as i64)`
  when binding, `as u64` when reading, `None` for the NULL marker
  (section 4.4).
- The pragmas of section 4.4: a large `cache_size` for the writer, a small one
  for readers, and no secondary indexes on `blocks` during a rebuild.

### System calls: `rustix` and `libc`

`rustix` wraps the calls with `OwnedFd` and `BorrowedFd` and returns
`io::Result`; on Linux its default `linux_raw` backend makes the system calls
itself, without libc. It covers everything in section 4.5 except three items,
which the `sys` module supplies (section 11):

- `name_to_handle_at`: `libc::name_to_handle_at` with a `MAX_HANDLE_SZ` buffer
  behind a `libc::file_handle` header.
- `FS_IOC_GETFSUUID`: the opcode from
  `rustix::ioctl::opcode::read::<fsuuid2>(0x15, 0)`, the struct from
  `linux_raw_sys::general::fsuuid2`, the call through `rustix::ioctl::Getter`.
- `STATX_MNT_ID_UNIQUE`:
  `StatxFlags::from_bits_retain(linux_raw_sys::general::STATX_MNT_ID_UNIQUE)`;
  `rustix` 1.1 only names `MNT_ID`.

### Threads: `rayon` and `crossbeam-channel`

Section 4.6 gives the reasons for rayon. `std::thread::scope` and
`std::sync::mpsc::sync_channel` are enough for the run-time threads of section
6.6; `crossbeam-channel` adds `select!` and is faster under contention.

### Compression: the `zstd` crate

Binds libzstd 1.5.7. `stream::write::Encoder::new(stdout, level)`,
`multithread(n)` for `ZSTD_c_nbWorkers`, `flush()` for `ZSTD_e_flush`,
`finish()` for `ZSTD_e_end`, and `set_parameter(CParameter::CompressionLevel)`
during the stream (libzstd applies a new level to the next job of a
multithreaded stream). `stream::read::Decoder` for the receiver. The pure-Rust
`ruzstd` 0.9 has a complete decoder but only an uncompressed and a fastest
level in its encoder, so it is not an option for the sender.

### Strong hashes: `blake3` and `sha2`

`blake3::hash(&block)` returns 32 bytes. The crate carries assembly for SSE4.1,
AVX2, and AVX-512 on x86_64 with run-time detection, and NEON C code that its
build script enables by default on little-endian aarch64. 32 KiB inputs are 32
chunks, enough to fill the widest vectors. Do not enable the `rayon` or `mmap`
features: blocks are hashed in parallel by the pool, not inside one hash.

For SHA-256, use `sha2::{Digest, Sha256}` and `Sha256::digest(&block)`,
which also produces 32 bytes ([crate documentation](https://docs.rs/sha2/0.11.0/sha2/)).
Select the implementation through one strong-hash abstraction shared by
indexing, queries, candidate verification, and hash-list persistence. Its
algorithm ID fixes the full length; index truncation is a storage decision.

### Hash maps and caches

`std::collections::HashMap` is hashbrown; only its default hasher (SipHash-1-3)
is slow. Use `foldhash::fast::RandomState` (hashbrown's own default) or
`rustc_hash::FxHashMap` for the sets keyed by `(fs_id, handle)`, the
filesystem cache of the walker, the query-state map (section 6.3), and the
pin map (section 6.4). Keys that are already hashes (full strong hashes in
the negative and verified-buffer caches) get an identity hasher. The
verified-buffer cache (section 6.4) is `lru::LruCache` or about 60 lines.
The sender's table stays custom (section 5.5).

## 10. Alternatives considered

- **BLAKE3-only hashing and full digests in the index.** Replaced on
  2026-09-17 (user decision) by configured BLAKE3 or SHA-256, each with a
  fixed full output of at least 16 bytes, and 16-byte indexed prefixes.
  Verification and cache keys retain full digests (sections 4.4 and 5.3).
- **Rebuild by replacing the database file, and concurrent external reads
  during updates.** Replaced on 2026-09-17 (user decision) by in-place schema
  reset and the SQLite `unix-excl` connection protocol. Independent read-only
  runs can coexist while no updating process holds the index (section 4.4).
- **Path-based metadata checks before opening a file.** Replaced on
  2026-09-17 by opening first and using that fd for type, identity, metadata,
  and contents. Directory-entry replacement cannot switch the object midway.
- **Allocated-size gate for sparse-file discovery.** Removed on 2026-09-17:
  allocation counts, notably with ZFS copies, cannot prove absence of holes.
- **`cachestat` plus `fadvise(DONTNEED)`.** Removed on 2026-09-17 (user
  decision). Separate SEQUENTIAL and NOREUSE advice simplifies the policy.
- **Root completion flag and seen-path hash sets.** Replaced on 2026-09-17
  by a nullable immutable identifier, deletion/reconstruction of scanned
  paths, and cleanup of all orphaned inodes. Recovery invalidates immutable
  roots referencing incomplete inodes before deleting those inodes.

- **Go as the implementation language.** Replaced by Rust on 2026-09-10 (user
  decision). Rust gives direct access to SIMD and prefetch intrinsics, control
  over memory layout, and threads without a scheduler in between; C libraries
  (SQLite, libzstd) link without a cgo/pure split, so the build has one
  configuration. The Go-era entries below are kept for the record.
- **Asynchronous runtime (`tokio` and the like).** Rejected on 2026-09-10. A
  project constraint: it complicates control flow and adds no file-I/O
  benefit over the pool (section 4.6).
- **Readiness event loop (`mio`, `polling`, libevent-style).** Rejected on
  2026-09-10. Regular files are always ready to `epoll`, and the program has
  two or three pipes and sockets per side (section 4.6).
- **`io_uring`.** Rejected on 2026-09-13 (user decision). The directory walk
  was rejected on 2026-09-10: no `getdents64`, no `name_to_handle_at`, no
  `ioctl`; `statx` and `openat2` run on io-wq, a thread pool inside the
  kernel. The file reads were deferred the same day as an optional backend of
  the hashing stage, to be adopted after a measurement, and are now rejected
  too: `io_uring` is often unavailable (the seccomp profiles of container
  runtimes, `kernel.io_uring_disabled`, older kernels), so the `pread` reader
  is needed in any case, and no gain in this architecture is established: the
  blocking pool already keeps a queue depth of 64 or more on the device, and
  small files are bounded by metadata work (section 4.6).
- **`O_DIRECT` reads in the indexer.** Rejected on 2026-09-10. It avoids the
  page cache copy, but files that are already cached (the common case after a
  rebuild) would be read from the device again, buffers need alignment, and
  btrfs falls back to buffered I/O for compressed extents.
- **`mmap` for hashing.** Rejected on 2026-09-10. Page faults and TLB work
  cost about as much as the copy of a `pread`, and a file truncated during the
  scan raises `SIGBUS` in the thread that touches the mapping.
- **`open_by_handle_at` for candidate reads (section 6.4).** Rejected on
  2026-09-10. It would open the exact indexed inode without a path, but it
  needs `CAP_DAC_READ_SEARCH`.
- **Path-first lookup shortcut in the walker.** Dropped on 2026-09-10. It would
  skip `name_to_handle_at` when the path's inode row already matches, but a
  handle call costs about as much as the extra index lookup it saves.
- **Sending a rejected window as literal bytes**, with the rescan from `p + 1`
  as an optional refinement. Reverted on 2026-09-10: a false hit just before
  a matched file would lose its first block. Rescanning from `p + 1` is
  mandatory (section 6.3), as is the negative cache. The intermediate
  two-pass scanner is recorded below.
- **Negative cache keyed by the weak hash.** Rejected on 2026-09-10. It would
  suppress queries for different content with the same weak hash. The
  Blake3 key saves no hashing (section 6.3), so short-period blocks were
  removed from the index the same day to avoid that cost (section 4.1).
- **Binary fuse filter (32-bit fingerprints)** for the sender's membership test.
  Rejected on 2026-09-09. It touches 3 cache lines per lookup against about 1
  for the fingerprint table, its random fingerprints cannot be compressed, and
  a filter cannot be directly patched with the set deltas of section 5.6. Its size,
  between the old 48-bit list and the current 64-bit list (section 5.4),
  was never decisive.
- **48-bit prefixes in the hash list** (`W = 48`). Adopted on 2026-09-09 and
  replaced on 2026-09-13 (user decision) by the full 64-bit values. The
  truncation saved 16 MiB for 8M blocks, but increased false hits from about
  3 to 400 per GiB (sections 5.4 and 5.5). Each queried false hit now costs
  a rescan (section 6.3).
- **buzhash64 (cyclic polynomial)** as the weak hash. Rejected on 2026-09-09.
  Its only mixing operation is a 1-bit rotation, which has order 64, so bytes at
  positions congruent modulo 64 contribute identically and equal bytes at
  distance 64 cancel. Every window with a period that divides 64 collapses to a
  handful of values, and to 0 when the window length is a multiple of 128. No
  table choice fixes this. The polynomial hash of section 5.1 has none of it at
  about twice the per-byte cost, which is irrelevant next to the table probe.
- **Composite index `(weak, blake3)`**. Rejected on 2026-09-09. It was only a
  workaround for the buzhash collisions and adds about 33 bytes per block.
  The index on the Blake3 hash alone, added on 2026-09-13, serves another
  purpose: a query carries no weak hash any more (below).
- **cgo SQLite drivers** (Go era). Rejected on 2026-09-09 in favour of the
  pure-Go `zombiezen.com/go/sqlite`. Moot since the switch to Rust: `rusqlite`
  binds the C library, and the per-call overhead that made the pure-Go driver
  competitive does not exist.
- **Unmixed weak hash in the index** with mixing only at export and probe time.
  Reverted on 2026-09-10 in favour of storing the mixed value everywhere; one
  quantity is simpler.
- **Marking inodes as immutable** when they are found under an immutable root.
  Reverted on 2026-09-10. Immutable roots already save the stats; files
  reached through mutable roots use the cheap `(size, ctime)` check
  (section 4.2).
- **One root row per copy of an immutable tree**, with the path rows copied
  to every copy. Rejected on 2026-09-10. It multiplies the path rows of a
  Nix store by the number of copies and needs a re-parenting of the rows
  when the scanned copy disappears. One identifier avoids both (section 4.2).
- **Identifier from the wildcard part of the glob** (the components that the
  wildcards matched). Considered and dropped on 2026-09-10. It is ambiguous
  with several wildcards and undefined for a literal path; `name` and
  `strip_prefix` say the same thing explicitly.
- **Renaming a root when its `identifier` option changes**, by finding the
  stored path of an old root among the paths of a new identifier, so that the
  rows survive without a rescan. Dropped on 2026-09-10 (user decision): not a
  normal use, and it adds logic to the update. A change of the option
  rescans the root, at one `statx` per file (section 4.2).
- **Roots at equal or nested paths.** Rejected on 2026-09-11 (user decision).
  Duplicate matches, mutable and immutable roots at one path, and nested
  roots duplicated path rows. A mutable ancestor also stat'ed an immutable
  tree at every update. Rejecting these pairs permits reconciliation before
  traversal (section 4.2), replacing insertion by the writer on first use.
- **Relying on reconciliation alone for root-path uniqueness.** Replaced on
  2026-09-17 by `UNIQUE(path)` (user decision). The earlier rejection of
  2026-09-11 avoided transient path-swap conflicts. Reconciliation now
  stages changing paths inside its transaction before assigning final paths
  (section 4.2); alternate immutable locations remain only in memory.

- **`TEXT` columns for paths and root identifiers.** Replaced on 2026-09-13
  (user decision) by `BLOB` to preserve non-UTF-8 paths (section 4.4).
- **`(st_dev, st_ino)` as the inode identity**. Rejected on 2026-09-10.
  Device numbers can change across boots and inode numbers are reused;
  section 4.3 defines the persistent identity.
- **`(fs_id, inode number)` as the key of the walker's in-memory sets.**
  Replaced on 2026-09-10 by `(fs_id, handle)`, the same identity that the
  index uses. Reuse within a scan and equal numbers across btrfs subvolumes
  have the same problems (section 4.3). An index-assigned integer was also
  considered, but would require a round trip to the writer for every file;
  handle bytes do not.
- **The mount id as the filesystem identity when the ioctl is unsupported.**
  Replaced on 2026-09-10 by `f_fsid` from `fstatfs`: mount ids change at
  boot and are shared across btrfs subvolumes. The mount/device triple also
  replaced the mount id alone as the resolution trigger (section 4.3).
- **Indexing incomplete blocks** (the shorter tail of each file). Considered
  and dropped on 2026-09-10: fixed-size windows cannot match them (section
  4.1). The default block size went from 64 KiB to 32 KiB instead, halving
  the unmatched tail of every file.
- **Shell-style dot handling in globs** (`*` not matching a leading dot).
  Considered and dropped on 2026-09-10. Keep the crate's default and use
  `exclude` (section 4.2). Go's `filepath.Glob` behaved the same.
- **Content-defined chunking** (as in casync, restic, and borg) on both sides
  would reduce the sender's lookups from one per byte to one per chunk. It was
  rejected: inside an archive stream, the chunk at the start and the chunk at
  the end of every file span the archive's framing, so about two chunks per file
  are lost. Trees with many files between 10 KiB and 1 MiB lose a lot. Fixed
  blocks on the receiver with a rolling scan on the sender lose only the
  incomplete tail of each file.
- **Pure-Go zstd as the default compressor** (Go era). Rejected on 2026-09-10
  in favour of libzstd: fewer levels, no worker threads, and a lower ratio at
  the same CPU cost. The Go design kept it as a `!cgo` fallback and the `zstd`
  command as a second fallback; both are gone. The binary links libzstd, and
  the pure-Rust `ruzstd` has no usable encoder (section 9).
- **SSH transport compression**. Disabled on 2026-09-10 with
  `-o Compression=no`: zlib on top of zstd wastes CPU on both hosts and cannot
  compress the control session.
- **Anchor sampling**: look up only at content-defined positions (for example
  where a small rolling hash has 12 low zero bits) and let the index record the
  anchor's offset inside its block. It would remove the per-byte probe, but a
  64-byte anchor context is shared by many distinct blocks (zero padding, common
  file headers), so each hit would need several candidates resolved. Not
  pursued. Revisit if the sender's scan rate turns out to be the bottleneck.
- **Per-peer full hash lists on the receiver.** Rejected on 2026-09-10,
  adopted on 2026-09-17 (user decision). The extra storage is acceptable.
  Content-derived IDs, account/index directories, and store acknowledgements
  replace the delta log and its generation/history bookkeeping (section 5.6).

- **Set reconciliation sketches** (IBLT, rateless IBLT, minisketch), to find the
  difference without history on the receiver. Rejected on 2026-09-10. An IBLT
  costs at least 13 bytes per cell (a count, a 64-bit key sum, and a 32-bit
  check sum) and about 1.3 to 1.5 cells per differing key: 26 to 37 MiB for the
  1.6M differing keys of the 10 % case, against 43 MiB for the full list and
  about 9 MiB for the delta. At any churn it costs three to four times the
  delta, and it needs an estimate of the difference (or a rateless scheme) and
  a fallback for a failed decode. Minisketch is optimal in size but decodes in
  O(d²), for thousands of differences, not millions. Direct list deltas
  cost about 6 bytes per changed key; a missing receiver-side base simply
  causes a full-list transfer.
- **Range fingerprint reconciliation** (Merkle-style recursion over the sorted
  key space, as in negentropy). Rejected on 2026-09-10. The weak hashes are
  uniform, so 10 % churn touches every range at every level, and the recursion
  sends about as much as the full list.
- **Bloom filter exchange** (each side sends a filter of its set). Rejected on
  2026-09-10. Two filters at 1 % false positives cost about 10 MiB each for 8M
  keys, and a false positive drops a new hash from the sender's table.
- **Deltas with additions only**, never removals. Rejected on 2026-09-10. The
  sender's table would grow by the churn at every run, and a stale hash whose
  content still appears in the output costs one query per occurrence until the
  negative cache learns it. Removals cost half of the delta as values and much
  less as positions (section 12).
- **Delta-log segments and generation counters.** Replaced on 2026-09-17
  (user decision) by canonical full lists identified by their strong hash
  and one directly computed delta. The former design rejected composing
  segment chains on 2026-09-10; there are now no chains to compose.

- **Expiry of the sender's cache after 30 days of no use, by `mtime`.**
  Replaced on 2026-09-10 (user decision) by 3 months, measured on `ctime`
  so restored timestamps do not cause expiry (section 5.6). `atime` is out:
  `noatime` mounts never update it.
- **One mutable cache file per index, with generation ordering and locks.**
  Replaced on 2026-09-17 (user decision) by immutable content-named files.
  Concurrent stores cannot replace different content under one name; a
  cleanup race may lose a future cache hit and needs no lock (section 5.6).

- **Optimistic skip after a query, with a rescan of the window after a
  "no".** Replaced on 2026-09-10 (user decision) by a two-pass scanner that
  probed every offset. The rescan stopped at the next pending window and
  could lose a match spanning both windows when both candidates were
  rejected. An unbounded rescan needed a record of probed offsets. The
  scanner of 2026-09-13 supplies it with the query-state map and discards
  all later records on a "no" (section 6.3).
- **Two-pass scanner that probes every offset.** Adopted on 2026-09-10 and
  replaced on 2026-09-13 by the single scanner of section 6.3. Pass 1
  recorded a candidate at every table hit and never skipped a queried
  window; pass 2 chose among overlapping candidates in stream order. It cost
  one probe per byte on matched streams, about 150 MB/s per thread from
  DRAM. Skipping queried windows removes that cost.
- **Several scanner threads over independent segments of the stream**
  (`scan_threads`). Rejected on 2026-09-13 with the single scanner. A
  segment's skipped window can overlap the reference that an earlier segment
  chose, which leaves the offsets inside the skipped window unexamined. One
  scanner makes every decision about offsets; the reader, the writers, the
  compressor, and the receiver's lookup workers stay separate threads
  (section 6.6).
- **Query ids.** Replaced on 2026-09-13 by the query offset. The sender sends
  at most one QUERY per offset, so a separate id is redundant and REF needs
  no payload (section 6.4). QUERY also lost its weak hash (below).
- **Lookup by weak hash with a Blake3 filter** (`WHERE weak = ? AND
  blake3 = ?` through `blocks_by_weak`). Replaced on 2026-09-13 by the lookup
  through `blocks_by_blake3`, so a QUERY carried only the strong digest.
  On 2026-09-17 the index became `blocks_by_strong`, storing a 16-byte prefix
  while queries retain the full configured hash. `blocks_by_weak` stays for
  hash-list export (section 4.4).
- **A memory limit on the pinned set**, with "no" answers once it was
  reached. Removed on 2026-09-10 (user decision). Memory is not a concern of
  the project, and in-flight queries bound the set in practice (section
  6.4). The negative cache retains a limit, now 100 MiB instead of the
  earlier 2 MiB.
- **Timeouts**: 60 s for the `attach` connection, a deadline on the retries
  of `attach`, and `busy_timeout` in SQLite. Removed on 2026-09-10 (user
  decision) in favour of the design principle of section 3 and the
  end-of-file chain of section 6.7. Fixed deadlines fail on slower systems.
- **`synchronous=FULL`** for the index, so that every commit is durable.
  Adopted on 2026-09-10 to prevent reuse of published generations after
  power loss. Replaced on 2026-09-11 (user decision) by `NORMAL` with a
  FULL checkpoint at publication. The checkpoint was removed on
  2026-09-17: full lists are independently durable and identify their own
  content, so database rollback cannot reuse an external list identity.
- **A positive cache** of the Blake3 hashes answered "yes", with a skip over
  the window at a cache hit. Added on 2026-09-10 and removed on 2026-09-11
  (user decision). An unpinned block is not guaranteed, so the skip still
  needed a query and saved probes only, while leaving the window's inside
  unprobed. The scanner of 2026-09-13 reuses a "yes" only at its pinned
  offset (section 6.3). The negative cache grew from 50 MiB to 100 MiB with
  the removal.
- **An LRU of open root descriptors at run time** (1024 entries). Added on
  2026-09-10 and rejected on 2026-09-11 (user decision). A descriptor that is
  reopened by path can name another inode. The cache is unnecessary:
  candidate reads open roots individually (section 6.4), as do pool tasks
  during indexing (section 4.6).
- **In-process cancellation with per-thread wake-ups** (`pthread_kill` with
  a handler that does nothing, `shutdown` on sockets, notified condition
  variables, and joins). Designed on 2026-09-10 and replaced on 2026-09-11
  (user decision) by ending the process. Wake-ups are unreliable, and all
  persistent state is crash-safe (section 6.7). Network errors exit without
  a core dump (user decision); bugs, protocol errors, and indexing errors
  panic and leave one. Since 2026-09-17, per-entry filesystem failures
  abandon that entry without aborting the update; database errors remain
  fatal (section 6.7).
- **One generation per update**, incremented after the last commit. Replaced
  on 2026-09-10 by an increment inside every transaction that changes block
  rows. A crash before the increment made a stale hash list look current.
  All index generations were removed on 2026-09-17 with content-identified
  full lists (section 5.6).
- **Whole inodes per transaction and per channel message.** Replaced on
  2026-09-10 by streamed block rows with a byte budget (section 4.6). One
  large inode defeats a message-count bound. The NULL `size` marker makes
  partial commits recoverable (section 4.4).
- **`size = -1` as the marker of an inode whose block rows are being
  written.** Replaced on 2026-09-13 (user decision) by NULL in a nullable
  `size` column. The full unsigned range is usable: `-1` represents
  `2^64 - 1`, so no integer is free for a marker (section 4.4).
- **Waiting for the child after END.** Reversed on 2026-09-10: END carries
  the exit status, so the child is waited for first (section 6.6).

## 11. Repository conventions

- One crate, one binary: `cargo build --release` produces
  `target/release/hashsync`. `Cargo.lock` is committed. Edition 2024, current
  stable toolchain. Every profile sets `panic = "abort"`, and `main` installs
  the panic hook of section 6.7 before it starts any thread.
- If `cargo` is not in `PATH`, use a Nix shell (`nix develop` once a flake
  exists, or `nix shell nixpkgs#cargo nixpkgs#rustc nixpkgs#gcc`; the C
  compiler is needed for the bundled C sources).
- Suggested layout of `src/`:
  - `main.rs`: command-line entry point (`clap`).
  - `weakhash.rs`: multiplier, `Tout` tables, block hash, rolling hash,
    `mix64`.
  - `hashset/`: Golomb-Rice hash list encoder and decoder, diff and merge of
    sorted lists, fingerprint table, SIMD bucket scan.
  - `scan/`: pool, directory and file tasks, filesystem and inode identities,
    mount tracking, hashing.
  - `strong.rs`: supported algorithms, fixed output lengths, full hashing.
  - `index/`: schema, process access, writer, recovery, pruning, and export.
  - `hashlists/`: content-named caches, diff/merge, publication, and expiry.
  - `proto/`: frame types, encoding, HELLO handshake.
  - `sender/`: `serve` (hash list cache, lookahead buffer, scanner, data
    writer, shutdown) and `attach` (relay).
  - `receiver/`: `run` (SSH processes, answering, reconstruction).
  - `config.rs`: `config.toml` and XDG paths.
  - `sys/`: every `unsafe` FFI call (handles, ioctl, the `statx` bit). The
    crate root has `#![deny(unsafe_code)]`; `sys` and the SIMD
    bucket scan carry `#![allow(unsafe_code)]`.
- Run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test` before you
  finish a change.
- Tests to keep: rolling hash equals direct hash (`proptest`); the
  interleaved k-stream hash and the split block hash equal the serial ones;
  Golomb-Rice round trip on random and adversarial gap patterns; direct
  deltas (section 5.6): applying a diff produces its target, choosing a full
  list when smaller, HASHKEEP for equal IDs, fallback after a bad delta, and
  content identity after database restore; hash-list caches: separate account
  and index directories, concurrent account-ID initialization, newest-ctime
  base selection, receiver lookup across peer directories, verified target
  publication before acknowledgements, lost acknowledgements, disabled and
  unwritable sender caches, same-ID cleanup, simultaneous stores and deletion
  with retained fds, and expiry with an injected clock (ctime cannot be set);
  the hash table never gives a false negative; traversal tests on a temporary
  tree (hard links,
  symlink loops, dot names and `exclude`, immutable roots and their
  identifiers (two copies under `name`, `strip_prefix`, and `literal`, as
  directories and as single files, scanned once; the stored path moves when
  the scanned copy disappears; two roots that exchange their paths between
  updates keep their rows; a changed `identifier` and a renamed root record
  paths only, with no read; `identifier` rejected on a mutable root), a path
  matched twice and a root below another root rejected, glob roots, ctime
  change, pruning, identity fallbacks when the ioctl or handles are
  unsupported, sparse files with holes that start and end off block boundaries,
  descriptor count under a low `RLIMIT_NOFILE`); a fuzz target (`cargo fuzz`)
  for the frame decoder; runs of one byte at arbitrary offsets, crossing block
  boundaries and longer than the lookahead, reproduced byte-exact through RLE
  frames, and constant blocks absent from the index; the scanner
  (section 6.3): a rejected query just before a sequence of matched blocks
  keeps their alignment (the restart at `x + 1` finds them), a "no" deletes
  the Ref, Run, and Literal records after it and the rescan reproduces the
  bytes exactly, a `Waiting` entry reached again after a restart sends no
  second QUERY, a late answer for an offset below `e` is discarded and
  leaves the negative cache unchanged, a `Yes` at one offset gives no Ref at
  another offset with the same hash, a run that continues across a rejected
  window, a short tail at end of file that a later "no" makes the scanner
  process again, the end conditions with the exit status available before
  and after the last byte, a full data queue that does not stop the
  processing of answers, and query offsets that go backwards; the receiver
  (section 6.4): a lookup that ends after `c` passed its offset creates no
  pin and answers "no", a pin released by a LIT or RLE frame that covers its
  offset, a REF without a pin at `c` rejected as a protocol error, pins
  released after the last data frame, and identical content queried at
  several offsets that shares one buffer with one pin per offset; a negative
  cache hit needs a full strong-hash match, and a different window with the
  same weak hash is still queried; the empty list
  (`N = 0`) and a delta with an empty removed or added list round-trip; a
  list sent over several LISTDATA frames, and a transfer whose bytes do not
  match the announced length or hash is rejected; an interrupted update
  (section 4.4): an immutable root left incomplete is traversed again and
  then assigned its identifier; NULL-sized inodes invalidate referring
  immutable roots before recovery removes them; complete inodes survive path
  reconstruction; interleaved block batches span transactions safely; paths
  arriving before Begin await End, Unchanged, Omit, or Abandon; RootDone
  cannot precede a pending inode result; per-file read/open failures leave
  other files indexed; stale orphans are removed after any successful update;
  and an in-place rebuild interrupted before secondary-index creation recovers.
  SQLite process access: the schema read acquires exclusion, same-process
  normal readers retain WAL snapshots, external readers/writers wait, and
  independent read-only processes coexist while excluding a writer. Test
  with the bundled SQLite build, separate processes, and fresh connections; the
  database boundary (section 4.4): a size of `2^63` or more and a path that
  is not valid UTF-8 round-trip through the index, and the run opens that
  path; FIFO/socket rejection by O_DIRECTORY, descriptor-based identity
  after pathname replacement or unlink, and nonblocking rejection of special
  file candidates; single-file roots; alternate hardlink paths and immutable
  copies; one configurable attempt budget across all locations; a forced
  16-byte-prefix collision rejected by full hash verification; both strong
  algorithms and a rebuild on algorithm change; sparse files whose allocated
  size exceeds logical size, unsupported hole seeking, and trailing holes;
  every event of the table in section 6.7 ends both roles, including a
  command whose descendant keeps stdout open, and the command's process group
  is gone afterwards;
  blocks with periods 1 to 16 absent from the index and a block with period 17
  present; a compression round trip
  with flushes at arbitrary frame boundaries; the ssh argument order
  (`-o Compression=no` after the configured command, HOST last); an end-to-end
  test that connects `serve` and `attach` through in-process pipes without ssh.
- Protocol constants (frame layout, multiplier `a`, `mix64`, hash list width,
  HELLO fields) are shared by both roles. A change to any of them needs a
  protocol version bump.
- Nix: package with `rustPlatform.buildRustPackage` and keep `cargoHash`
  current. Cross builds for a sender of another architecture go through
  `pkgsCross`; static builds through `pkgsStatic` (musl). The bundled C sources
  build in both.

## 12. Open questions

- **Adaptive compression level.** The streaming encoder of the `zstd` crate
  exposes `set_parameter(CParameter::CompressionLevel)`, and libzstd applies a
  new level to the next job of a multithreaded stream, so the setter exists.
  What remains is the controller: raise the level while the network is the
  bottleneck, lower it while the compressor is.
- **Thread count default.** `4 × cores`, clamped to 16 to 64, assumes NVMe.
  Measure a cold and a warm full build and an incremental update with 8, 16,
  32, and 64 threads.
- **Spinning disks.** A per-root limit on concurrent readers and reading in
  inode order; not in the first version.
- **Removals as positions in the delta.** The delta encodes removed hashes as
  64-bit values, about 46 bits each. The sender holds the old list, so a
  removal could be its rank in that list: the gaps between ranks cost about
  `log2(N / n_removed) + 2` bits, 5 to 6 bits at 10 % churn. This cuts the
  10 % delta of section 5.6 from about 9 MiB to about 5 MiB for about 20
  lines on each side. Decide after the first measurements on a real tree.
- **Rescan cost after a "no".** A "no" deletes every record after the
  rejected Query, and the scan repeats the probes and the record
  construction over that suffix: the scanning work of one round trip
  (section 6.3). False weak hits are rare with 64-bit values (section 5.4),
  so a "no" comes mostly from stale hits: content that the table names but
  the receiver's disk no longer holds, once per distinct content because of the
  negative cache. Records that say where a rescan can rejoin the old list
  would remove the repeated work. Measure the share of "no" answers and the
  length of the rescans on a real tree and a slow link before adding them.
- **Dense false weak hits.** A stale hash list, or content that the table
  still names but the receiver no longer holds, can hit the table at many
  distinct offsets, and each hit costs a strong hash computation; the query-state
  map avoids only a second computation at the same offset (section 6.3).
  Measure whether this happens on real streams.
- **Throughput on matched streams.** The scanner skips every queried window,
  so a stream whose content is all indexed costs one strong hash and one query
  per block on the sender, and the throughput is bounded by
  `lookahead / query_latency` (section 6.1) and by the receiver's disk. Measure
  such a stream against the receiver's disk rate and the link.
