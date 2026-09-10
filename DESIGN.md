# hashsync

This file is the design specification of `hashsync`. Contributors and AI agents
follow it and update it when the design changes. Section 10 logs rejected and
reverted decisions; section 12 lists the open questions.

Status (2026-09-10): design stage. No code exists yet. The implementation
language is Rust. An earlier version of this document, from the same day,
targeted Go (section 10).

## 1. Purpose

`hashsync` copies the output of a command that runs on a remote host to the
local host. It does not send bytes that already exist on the local disk.
Instead, the remote sends a short reference to a local file block. The local
program reads that block from disk, verifies it, and splices it into the output.

This pays off when the output of the remote command repeats data that the local
host already holds in indexed files, in any order and at any alignment: for
example a package tree or an archive stream produced after a rebuild in which
most files did not change.

The program is written in Rust. One binary contains both roles: the local side
and the remote side. The local side starts the remote side through `ssh`. The
program uses threads and blocking system calls for parallelism. It has no
asynchronous runtime and no event loop (section 4.6).

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
- **Identifier**: the string that names an immutable root in the index
  (section 4.2). By default it is the root's path. Immutable roots with equal
  identifiers are copies of one root, and only one copy is scanned.
- **Filesystem id**: the persistent identity of a filesystem (section 4.3).
- **Inode handle**: the persistent identity of an inode inside its filesystem,
  as returned by `name_to_handle_at` (section 4.3).
- **Hash list**: the sorted, compressed list of the weak hashes of all indexed
  blocks (section 5.4). The remote keeps it between runs. At the start of each
  run the local sends it in full, or sends only its changes (section 5.6).
- **Delta log**: the local's record of the changes of the hash list from one
  export to the next, as segments. A segment lists the removed and the added
  hashes (section 5.6).
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
   (section 5.4). When the index changed since the last export, it first
   checkpoints the database, so that the generation it publishes is durable
   (section 4.4), and it records the changes in the delta log (section 5.6).
3. The local opens two SSH sessions to the remote host: a **data session** and a
   **control session** (section 6.1).
4. The two sides exchange HELLO frames over the data session. The remote's
   HELLO names the hash list it kept from an earlier run with this index, if
   any. The local sends the whole hash list, only the changes since that
   list, or nothing when that list is current (section 5.6). The remote
   applies the changes to its cached list, stores the result for the next
   run, and builds its hash table.
5. The remote starts the user command and reads its stdout.
6. The remote scans the output (section 6.3). At every offset it computes the
   weak hash of the window and looks it up in its hash table.
   - Run of one byte value, at least `block_size` long: the remote records
     it and continues after it, without a query.
   - Table miss: continue at the next offset.
   - Table hit: the remote computes the Blake3 hash of the window. When the
     local said "no" to that hash earlier in this run, continue at the next
     offset. Otherwise send a query over the control session, record the
     window as a candidate, and continue at the next offset.
7. The local answers each query. It looks the weak hash up in the index, keeps
   the candidates whose stored Blake3 equals the queried one, reads a candidate
   block from disk, verifies its Blake3 hash, and keeps the verified bytes in
   memory. It answers "yes" if a block verified and "no" otherwise
   (section 6.4).
8. In a second pass, the remote resolves the recorded runs and candidates in
   stream order (section 6.3): it waits for the answer of each candidate,
   takes every candidate that got a "yes" and does not overlap an item taken
   before it, and sends the output over the data session: literal bytes, one
   reference per taken candidate, and one RLE frame per run.
9. The local writes literal runs, pinned block bytes, and synthesized runs to
   its stdout, in order.
10. When the command exits, the remote sends the exit status. The local exits
    with the same status.

Design principle: the index, the hash list, its cached copies, and the hash
table are hints. The local verifies every spliced block with Blake3 against
bytes it read from local disk, in memory, before it uses them. Stale index
entries, files changed after indexing, stale cached lists, and weak hash
collisions cost efficiency, never correctness.

Design principle: no timeouts. The program never abandons an operation because
it took longer than a fixed time. A slow disk, a slow link, or a loaded host
must not turn into a failure, and a deadline chosen by a programmer is
arbitrary for some system. When something is broken, the program hangs, and a
debugger can be attached to it; it does not give up. A wait ends only when the
awaited event happens or when the peer, descriptor, or process it waits for
goes away (an error, an end of file, a closed channel). A fatal error ends the
process, which is what makes every such end reach every thread
(section 6.7). Poll intervals and cache expiry ages are not timeouts: they
fail nothing.

## 4. The local index

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
- Blocks with a short period are not stored either: a block whose bytes repeat
  with a period `p` of 1 to 16 bytes (`block[p..] == block[..B - p]`; period 1
  is the constant block above). zstd encodes such a window in a few bytes,
  about the cost of a reference and without a query, so matching it gains
  nothing. It also keeps periodic windows out of the remote's Blake3
  computation: without such rows they miss the table (section 6.3). The bound
  of 16 covers the usual fill patterns; it is an efficiency choice, not a
  protocol constant.
- Holes in sparse files are not read. A block that lies entirely inside a hole
  is neither read nor stored (section 4.5); it would be a constant block anyway.

### 4.2 Roots and change detection

Roots come from `config.toml`. A root path can be a glob. Each match is its own
root, except that immutable matches with equal identifiers are one root
(below). The indexer expands globs at every update with the `glob` crate
(`glob::glob_with`, section 9). The match options are
`require_literal_separator = true`, so that `*` and `?` never match a `/`, and
the crate's default `require_literal_leading_dot = false`, so that `*` also
matches names that start with a dot: `/nix/store/*` matches
`/nix/store/.links`. Use `exclude` for such entries. `**` matches any number of
path components. A root that is a regular file is indexed as that single file.

Roots are opened by path: at every update, and at run time for every
candidate read (section 6.4). Together with the SQLite file, the cache files,
and the `ssh` processes, these are the only path-based filesystem accesses of
the program (section 4.5). Roots are scanned in parallel, as tasks of the
pool (section 4.6); they are not opened up front.

Mutable root (`immutable = false`, the default):

- Every update traverses the root recursively.
- For each regular file, the indexer compares `(size, ctime)` with the stored
  inode row. If both are equal, the file is not read again. Otherwise the
  indexer hashes the whole file and replaces all its block rows. This is the
  only criterion for hashing a file again.
- Path rows under the root that the traversal did not see are deleted
  (section 4.6).

Immutable root (`immutable = true`):

- The index knows the root by its identifier: a string that the `identifier`
  option (below) derives from the path of the match or takes from the
  configuration. By default it is the path itself.
- The indexer traverses the root once, when it first sees its identifier.
  The root row is marked complete when the whole traversal has been
  committed (section 4.4). A root whose traversal was interrupted (a crash,
  a signal) stays marked incomplete, and the next update traverses it again,
  in the way of a mutable root: files whose `(size, ctime)` match their rows
  are not read again, so the second traversal costs one `statx` per file.
- While a match with that identifier exists and the root is complete, the
  indexer never traverses the root again and never stats the files below it.
  It only checks that the match still exists (one open).
- Matches with equal identifiers are copies of one root. Only the first is
  scanned; its rows serve all of them (below).
- When no match of the update produces the identifier, the root's path rows
  are deleted.
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

- `exclude`: glob patterns in the same syntax and with the same match options
  (`glob::Pattern::matches_with`). A pattern that starts with `/` is matched
  against the absolute path of an entry, including the glob matches of `path`
  themselves; any other pattern is matched against the base name. Matching
  entries and everything below them are skipped.
- `one_filesystem`: do not cross mount points. An entry whose `stx_mnt_id`
  differs from its parent's is skipped, together with everything below it.
- `identifier`: how the index names an immutable root. It is a configuration
  error on a mutable root, which the index names by its path. The match can
  be a directory or a single file. Forms:
  - `"path"` (default): the absolute path of the match.
  - `"name"`: the last component of the path of the match. With it,
    `/nix/store/*` and `/mnt/disk/nix/store/*` give a store path the same
    identifier on both disks.
  - `{ strip_prefix = "/mnt/disk" }`: the absolute path of the match without
    the leading `/mnt/disk`: `/mnt/disk/nix/store/x` gives `/nix/store/x`,
    the default identifier of the same tree on the root filesystem. This
    suits copies of trees whose names are not unique. A match that does not
    start with the prefix is a configuration error.
  - `{ literal = "photos-2019" }`: the string as given. The root keeps its
    rows when the tree moves to another path.

  Identifiers are compared as strings, whatever form and whatever `[[roots]]`
  entry produced them. An empty identifier is a configuration error.

Copies of an immutable root: matches of one update with equal identifiers are
copies of one root, whether one glob or several entries produced them. The
update handles them in this order:

1. It expands every glob, derives the identifier of every match, and groups
   the matches by identifier. Inside a group, the matches of an earlier
   `[[roots]]` entry come first; the order of the matches of one glob is not
   specified. Put the entry of the preferred copy first.
2. A group whose identifier names an immutable root of the index keeps that
   root and its rows. When the root's stored path is not one of the group's
   paths, the first path of the group replaces it: the tree moved, or the
   scanned copy is gone. When the root is marked incomplete, the indexer
   traverses the stored path once more (above).
3. Every other group gets a new root, with its first path as the stored path,
   and the indexer scans that path. The other paths of the group are never
   read. A change of the `identifier` option therefore rescans the root
   under its new identifier.
4. Immutable roots that no group kept lose their path rows.

The rows of a root describe the copy that was scanned. Candidate reads
(section 6.4) and `hashsync prune` open files below the root's stored path,
which can be another copy. The design assumes that copies are equal. The
Blake3 check of every spliced block (section 3) makes that assumption safe: a
copy that differs costs "no" answers, nothing else. With
`identifier = "name"` on two Nix stores, the second store costs one directory
listing and one index lookup per store path instead of a read of every file,
because two store paths with the same name almost always hold the same
content.

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

The index assigns a 64-bit integer id to every filesystem and to every inode and
uses these ids as foreign keys. The pair `(fs_id, handle)` is unique.

The pair `(fs_id, handle)` is the only identity of an inode in the program,
in memory as well as in the index. The inode number alone is never a key: a
filesystem reuses inode numbers after a deletion, and two btrfs subvolumes of
one filesystem can hold different inodes with equal numbers. The in-memory
sets of the walker (section 4.6) therefore hold the same `(fs_id, handle)`
bytes that the index stores, and an inode that is met twice in one scan is
found by them whatever happened to inode numbers in between.

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
  -- keys: schema_version, block_size, weak_hash_id, generation, index_id
CREATE TABLE filesystems (id INTEGER PRIMARY KEY, fsid BLOB NOT NULL UNIQUE);
CREATE TABLE roots       (id INTEGER PRIMARY KEY, ident TEXT NOT NULL,
                          immutable INTEGER NOT NULL, path TEXT NOT NULL,
                          complete INTEGER NOT NULL, UNIQUE (ident, immutable));
  -- ident: the path of a mutable root, the identifier of an immutable one;
  -- path: where the root was last found (section 4.2);
  -- complete: 1 once a whole traversal of the root has been committed
CREATE TABLE inodes      (id INTEGER PRIMARY KEY, fs_id INTEGER NOT NULL,
                          handle BLOB NOT NULL, size INTEGER NOT NULL,
                          ctime_ns INTEGER NOT NULL, UNIQUE (fs_id, handle));
  -- size = -1 while the block rows of the inode are being written
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
- Use WAL mode with `synchronous=NORMAL`. The database stays consistent after
  a crash or a power loss, a commit costs no `fsync`, and the last
  transactions can be lost, which is acceptable for an index that is a cache.
  The publication contract below makes the transactions durable that
  something outside the database depends on.
- The writer commits every few thousand inodes or every 30 seconds, whichever
  comes first, to amortise the commit cost. A transaction may cut through the
  rows of a large file (below); every other inode is either wholly in a
  transaction or wholly outside it. A crash leaves the index at a consistent
  earlier state.
- Recovery contract. The index never needs a repair step. Every committed
  state is a valid index, because every row of it is a hint (section 3) and
  because two markers tell the next update what was interrupted:
  - `inodes.size = -1` marks an inode whose block rows are being written.
    The writer sets it in the transaction that deletes the inode's old block
    rows, and it stores the real size and ctime in the transaction that holds
    the last block row. An inode left at `-1` by a crash matches no
    `(size, ctime)` and is hashed again at the next update.
  - `roots.complete = 0` marks a root whose traversal was not committed to
    the end. The writer inserts the row with `0` in the first transaction that
    references the root and sets `1` in the transaction that follows the end
    of the root's walk. An immutable root left at `0` is traversed again at
    the next update (section 4.2); a mutable root is traversed at every
    update anyway.
- Publication contract. `meta.generation` names the content of the `blocks`
  table: the writer increments it inside every transaction that inserts or
  deletes block rows, so two committed states with different block rows never
  share a generation, and a crash can never leave changed rows behind an
  unchanged generation. Transactions that touch only path or root rows leave
  it alone, because the hash list does not depend on them. The generation
  therefore moves several times during one update. That is intended: any
  committed state may be exported as a hash list, by this process or by a
  concurrent `hashsync run`, and the export (section 5.6) reads the generation
  and the block rows in one read transaction, so the list it publishes
  describes exactly the generation it is labelled with. Before a generation
  is named outside the database, the export makes it durable: after it has
  read the generation and the rows and has ended that read transaction, it
  runs `PRAGMA main.wal_checkpoint(FULL)` in a loop until the first column of
  the result (`busy`) is 0. A completed FULL checkpoint waits for the current
  writer, syncs the WAL, copies its frames into the database file, and syncs
  that too, so every transaction committed before it survives a power loss,
  including the one that produced the exported generation. Only then does the
  export write the segment and the cache file, and only then does the run
  send the list or the delta. A generation that no export has published can
  be lost with its transactions; nothing outside the database names it, so
  nothing can reuse it. The read transaction must be over before the
  checkpoint: FULL waits until every reader is on the newest snapshot, and a
  reader that waits for its own checkpoint would wait forever.
- Index the weak hash alone. The polynomial hash has no systematic collisions,
  so rows that share a weak value hold identical content, apart from stale
  entries. Identical blocks (for example zero blocks) legitimately share one
  weak value; the lookup uses `LIMIT` and stops at the first candidate that
  verifies, so their number does not matter.
- Store `weak` as the `i64` bit pattern of the `u64` value (`weak as i64`).
- Only inodes with at least one stored block have rows (sections 4.1 and 4.5).
- Keep the Blake3 hash in the row, not in an index. It rejects stale candidates
  without disk reads. Option: store only the first 16 bytes; the verification
  still uses the full 32-byte hash from the query.
- Give the writer's connection a large page cache (`PRAGMA cache_size`, about
  256 MiB). The keys of `blocks_by_weak` are random, so every insert touches a
  random page of that B-tree; once the tree outgrows the cache, every insert
  costs a page read. Reader connections (section 4.6) keep a small cache; the
  OS page cache holds the file for them.
- `--rebuild` and the first build of an index insert the block rows without
  `blocks_by_weak` and create the index at the end. An index built over the
  finished table costs about a second per million rows; random inserts into a
  B-tree that has outgrown the cache cost several microseconds each. A rebuild
  writes a new database file next to the old one and renames it into place
  after its last commit, so a crash during the rebuild leaves the old index
  in use and a stray file that the next rebuild deletes. Readers of the old
  file (a concurrent `run`) keep their open descriptor and finish on the old
  index.
- Expected size: about 55 bytes per block row plus about 20 bytes per index
  entry, about 75 bytes per block, or 0.23 % of the indexed data at 32 KiB
  blocks. A 256 GiB tree (8M blocks) needs about 600 MB.
- `generation` is a 64-bit counter that only grows (the publication contract
  above). It starts at 1 when the tables are created; 0 means "no list" in
  the protocol (section 6.2). The cached hash list file and the segments of
  the delta log record the generations they were built from (section 5.6).
- `index_id` is a random 128-bit value (`rustix::rand::getrandom`), written
  when the tables are created. `--rebuild`, and the rebuild after a parameter
  mismatch above, create a new one. It identifies the index towards remote
  hosts, which key their cached hash lists by it (section 5.6). An index
  restored from a backup keeps its id and reuses generation numbers; the list
  hash of section 5.6 covers that case.

### 4.5 Traversal and hashing

This section lists the system calls of the indexer. Section 4.6 says which
threads run them.

Descriptor-relative operations: apart from opening the roots, the SQLite file,
and the `ssh` processes, every filesystem operation is relative to an open
descriptor of a directory or a file. This removes the races of path-based
traversal (renames, symlink swaps) and the cost of long path lookups. The
wrappers come from `rustix` and, for the few calls it lacks, from `libc`
(section 9); all of them live in the `sys` module (section 11).

- Directory: `openat(dirfd, name, O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)`,
  then `getdents64` on the descriptor (`rustix::fs::RawDir` over a 64 KiB
  buffer that the thread reuses). The entry type that `getdents64` returns
  (`d_type`) decides without a `statx`: directories and regular files continue
  below; symbolic links and everything else are skipped. A filesystem that
  reports `DT_UNKNOWN` gets a `statx` for the type.
- Every directory and every regular file:
  `statx(dirfd, name, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW,
  STATX_TYPE | STATX_SIZE | STATX_BLOCKS | STATX_CTIME | STATX_INO |
  STATX_MNT_ID_UNIQUE)`.
  Every `statx` call of the program uses these flags and this mask: with a name
  the call does not follow a final symlink, with an empty name it describes the
  descriptor itself, and `stx_mask` tells whether the kernel returned the unique
  mount id. Skip regular files smaller than `block_size`, and whatever
  `exclude` and `one_filesystem` rule out.
- Unchanged check for a regular file: `name_to_handle_at(dirfd, name, 0)` gives
  the handle; look up `(fs_id, handle)` in the index through the thread's read
  connection; if `(size, ctime)` match, send the path to the writer and stop.
- Changed or new file:
  `openat(dirfd, name, O_RDONLY|O_NOFOLLOW|O_NONBLOCK|O_NOCTTY|O_CLOEXEC)`,
  then `statx(fd, "", ...)` with the same flags and mask, and
  `name_to_handle_at(fd, "", AT_EMPTY_PATH)` on the open descriptor, so that
  size, ctime, and handle describe the object that is hashed. The entry can
  have been replaced since `getdents64` returned it: when `stx_mode` says that
  the descriptor is not a regular file, close it and skip the entry.
  `O_NONBLOCK` keeps the open of a FIFO from waiting for a writer, and
  `O_NOCTTY` keeps a terminal from becoming the controlling terminal
  (section 6.4). Read the file
  with `pread` in chunks of up to 1 MiB into the thread's buffer (one system
  call per 32 blocks instead of one per block) and compute the weak hash and
  Blake3 of every complete block of the chunk. A block with a period of 1 to
  16 bytes is skipped before hashing (section 4.1). The check for period `p`
  is `block[p..] == block[..B - p]`, one vectorised `memcmp`; compare the
  first 80 bytes first, so that the full comparisons run only on the rare
  blocks whose prefix is periodic.
- Sparse files: when `stx_blocks * 512 < size`, the file may have holes. Find
  them with `lseek(SEEK_HOLE)` and `lseek(SEEK_DATA)` (`rustix::fs::seek` with
  `SeekFrom::Hole` and `SeekFrom::Data`) before reading. A hole `[h0, h1)`
  makes the indexer skip every block `k` with `k * B >= h0` and
  `(k + 1) * B <= h1`; these blocks get no rows. A block that overlaps a hole
  only partially is read in full, and the kernel returns zeros for the hole
  part. Example with `B` = 32 KiB: a hole from 10 KiB to 200 KiB skips blocks 1
  to 5 (32 KiB to 192 KiB); blocks 0 and 6 are read. When the allocated size
  covers the file size there are no holes and no seek calls are made (a
  compressing filesystem can report a smaller allocation; that only costs the
  seek calls). A filesystem without hole support reports one data extent, so
  the procedure is safe everywhere.
- Filesystem id: when an entry's `(stx_mnt_id, stx_dev_major, stx_dev_minor)`
  differs from its parent's, call `FS_IOC_GETFSUUID` on the entry's descriptor,
  or `fstatfs` when the ioctl fails with `ENOTTY` (section 4.3).
- Directory identity: `name_to_handle_at(dirfd, "", AT_EMPTY_PATH)` on the
  open directory descriptor gives the key of the visited-directories set
  (section 4.6).
- Page cache: call `fadvise(SEQUENTIAL)` before a file is read. After a file is
  hashed, call `fadvise(DONTNEED)` on it, unless the file was already fully in
  the page cache before the read. A full scan must not evict the page cache of
  the whole machine, but the files that a `run` will splice from (freshly built
  outputs, for example) must not be evicted either. `cachestat` (Linux 6.5;
  `libc::syscall(SYS_cachestat)`, no wrapper in `rustix` 1.1) counts the cached
  pages of the file in one call; a file with all pages cached keeps them. When
  the call fails with `ENOSYS`, always evict.

### 4.6 I/O architecture

This section states how the indexer is parallelised and why. Constraints: no
asynchronous runtime (no `async` functions, no executor, no futures); threads
for parallelism; `io_uring` where it makes the program measurably faster.
Asynchronous calls in the C sense (a function that runs on another thread and
reports back, or a submission with a completion) are fine.

#### Where the time goes

Three kinds of work:

1. **Metadata.** One `openat` and one `getdents64` per directory; one `statx`
   per directory and per regular file; one `name_to_handle_at` and one index
   lookup per regular file of at least `block_size`. With warm dentry and
   inode caches this is 1 to 3 µs of kernel CPU per entry. With cold caches it
   is disk latency: a directory costs a few reads of directory blocks, and a
   group of neighbouring inodes costs one read of an inode-table block (ext4
   and XFS read inodes in clusters). A read costs 50 to 200 µs on NVMe, so the
   cold rate is the number of reads in flight divided by the latency.
2. **Data.** For a new or changed file: open, `statx`, handle, reads, hashing,
   close. Per core, Blake3 runs at 3 to 7 GB/s on 32 KiB inputs, the weak hash
   at 1 to 4 GB/s (k streams, section 5.1), the constant-block check near
   memory speed, and the copy out of the page cache at about 10 GB/s. All
   together about 1 GB/s per core, up to 2 GB/s with AVX-512. An NVMe device
   delivers 3 to 7 GB/s when it has 32 or more large requests in flight and
   about 1 GB/s with one request in flight. From a warm page cache the machine
   is CPU-bound; from disk it is disk-bound once 4 to 8 cores hash.
3. **Rows.** The writer inserts one inode row and one path row per file and one
   block row per 32 KiB. SQLite costs 1 to 2 µs per row with cached prepared
   statements when the B-trees are in memory. Ten million rows cost 10 to 20 s
   of one core: less than hashing 256 GiB from disk, about the same as hashing
   it from the page cache on a machine with many cores. Section 4.4 gives the
   rules that keep it there (large cache, deferred index). If the writer ever
   is the bottleneck, the first lever is the 16-byte Blake3 (section 12), the
   second is multi-row `INSERT` statements.

In every case the unit of work is a file or a directory, and the work per unit
is a few blocking system calls plus CPU time. A thread that blocks in the
kernel costs nothing while it waits. This is why a pool of threads with plain
blocking calls is the right tool; the rest of this section shapes that pool.

#### The pool

Follow the parallel walker of ripgrep and fd (`ignore::WalkParallel`, in
`crates/ignore/src/walk.rs` of ripgrep 15.1.0):

- A fixed number of threads. Each thread owns a LIFO deque of tasks
  (`crossbeam_deque::Worker::new_lifo`) and steals the oldest task of another
  thread when its own deque is empty. LIFO order makes each thread go
  depth-first, so the number of pending tasks stays near `depth × fan-out`
  instead of growing to the width of the tree; the comment in `ignore` calls a
  breadth-first walk of a wide tree "disastrous". Stealing the oldest task
  hands over a whole subtree, which keeps the threads balanced.
- The per-file work runs on the same threads: ripgrep searches each file inside
  the walk callback. fd sends its results to one output thread in batches over
  a bounded `crossbeam-channel`.
- The walk ends when every thread is idle at the same time (an active-thread
  counter), not when a queue is empty, because a running task can still push
  more tasks.

hashsync applies this as follows:

- One pool for walking and hashing. A task is either a directory or a file. A
  directory task lists its entries and pushes one task per subdirectory and one
  per file that needs hashing. Depth-first order hashes the files of a
  directory right after listing it, while its inodes are hot in the cache.
  Threads that wait for the disk and threads that hash overlap without any
  scheduling logic.
- A task carries the `Arc<OwnedFd>` of its parent directory and its name, and
  opens its own descriptor when it runs. Queued tasks hold no descriptors; a
  parent's descriptor lives until its last child task has opened its own.
  With depth-first order, the queued tasks of a thread are the unvisited
  siblings along its current path, so the open descriptors are bounded by
  `threads × depth`, not by the queue length or the size of the tree. Each
  directory is opened once and never opened again. The roots are the initial
  tasks, one per match, with no parent: a root task opens its path when a
  thread takes it, so the number of roots (one per store path in a Nix store)
  does not add open descriptors either. ripgrep and fd get a bound of
  `threads` by queueing paths and opening a directory only while they read
  it, at the price of one full path lookup per directory and per file;
  hashsync keeps the parent's descriptor in the task instead and pays no
  path lookup below a root. The program raises the soft `RLIMIT_NOFILE` to
  the hard limit at start anyway.
- Thread count. ripgrep uses `available_parallelism` capped at 12 and fd caps
  it at 64; both assume warm caches and CPU-bound work. hashsync's threads also
  wait for the disk, and every waiting thread adds one request to the device
  queue. Default: `4 × available_parallelism`, clamped to the range 16 to 64
  (`threads` in the configuration, `-j` on the command line). More threads do
  not help: a warm walk is CPU-bound, and an NVMe device reaches its rate at a
  queue depth of 32 to 64.
- Per-thread state, created once: a 64 KiB `getdents64` buffer, a 1 MiB read
  buffer, a Blake3 hasher, and an SQLite read connection with cached prepared
  statements (section 9). Readers never block the writer in WAL mode.
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

One thread owns the write connection. It receives its work over a bounded
channel and writes it in `BEGIN IMMEDIATE` transactions, committed every few
thousand inodes or every 30 seconds, whichever comes first. Nothing else
writes, so the write lock is never contended. Section 4.4 lists its pragmas.

Messages and their byte budget. Every message is at most 64 KiB, and the
channel holds at most 4096 messages, so the queue never holds more than
256 MiB; when the writer lags, the pool blocks in `send`, which is the
intended backpressure. The block rows of a file are not one message: a 1 TiB
file has 32M block rows, more than 1 GiB of hashes. The hashing thread
streams them instead:

- `Begin { fs_id, handle }`: the writer looks the inode up, inserts its row
  with `size = -1` or sets `size = -1` on the existing row, and deletes its
  old block rows.
- `Blocks { rows }`: up to 1024 block rows (48 bytes each), sent as soon as
  they are complete, while the file is still being read. The writer inserts
  them into the current transaction.
- `End { size, ctime, root_id, rel_path }`: the writer stores the real size
  and ctime. The inode is now complete (section 4.4).
- `Path { fs_id, handle, root_id, rel_path }`: a path of an inode that this
  update did not hash (unchanged, or a hard link met a second time). The
  writer resolves the handle to the inode id.
- `RootDone { root_id }` after the walk of a root has ended (below).

The messages of one file come from one thread, so the writer sees them in
order, and a transaction that commits between two `Blocks` messages cuts
through a file that is marked `size = -1` at that moment (section 4.4). A
`Path` message can arrive before the `Begin` of its inode, when a thread met a
hard link whose first path another thread is still hashing. The writer keeps
such paths in a map keyed by `(fs_id, handle)` and inserts them when the
inode's `End` arrives; entries whose inode never arrives (the file vanished
while it was being read) are dropped at the end of the update.

Deleted paths: for each mutable root of the current update, and for each
immutable root that is traversed again because it was incomplete, the writer
keeps a set of 64-bit hashes of the relative paths it was told about. On the
root's `RootDone`, it streams the stored paths of the root and deletes the ones
whose hash is not in the set, then sets `roots.complete = 1`, all in the same
transaction. This costs 8 bytes per path and no write for an unchanged file. A
hash collision keeps a stale path row, which costs efficiency only (section
3); `hashsync prune` removes it.

#### io_uring: where it helps and where it does not

Facts, checked against the Linux 6.18 headers and the `io-uring` crate 0.7.15:

- `io_uring` has `openat2`, `statx`, `read` and `readv` (also with registered
  buffers), `fadvise`, `madvise`, and `close`. It has no `getdents64`, no
  `name_to_handle_at`, no `lseek` (`SEEK_HOLE`), no `ioctl` (only `uring_cmd`
  for specific drivers), and no `cachestat`. The walk therefore cannot run on
  `io_uring`; every directory needs a thread that blocks in `getdents64`.
- `statx` always runs on io-wq, the kernel's own pool of worker threads
  (bounded to the smaller of the queue depth and 4 × the CPU count), one
  blocking call per worker; `openat2` completes inline only when the whole
  lookup is cached and runs on io-wq otherwise. Submitting metadata calls
  through a ring replaces a user thread pool by a kernel thread pool with fewer
  knobs.
- Buffered reads on ext4, XFS, btrfs, and the other filesystems with
  asynchronous buffered reads are the real thing: cached pages complete inline
  at submission; missing pages start readahead and complete later without
  occupying any thread. This is the one place where a ring gives queue depth
  without threads.
- `io_uring` is often unavailable: the default seccomp profiles of container
  runtimes block it, `kernel.io_uring_disabled` (Linux 6.6) turns it off, and
  older kernels lack operations. A `pread` path must exist in any case.

Against that, the blocking reader of this design keeps about two requests in
flight per thread (`fadvise(SEQUENTIAL)` doubles the readahead window, and
readahead is asynchronous), so 32 to 64 threads put a queue depth of 64 or more
of 128 to 256 KiB requests on the device for files of a few hundred KiB and
larger. Smaller files are bounded by their per-file metadata work, which
`io_uring` cannot reduce. The expected gain of a ring-based reader is therefore
small and confined to cold full rebuilds, and it costs a second code path.

Decision: the first version reads with `pread` from the pool. The hashing stage
keeps a narrow interface (a function that takes a directory descriptor and a
name and returns the inode's rows), so a ring-based reader can replace its body
later. Adopt one only if a measurement on a cold NVMe shows the blocking pool
below about 70 % of the device's sequential rate (measure the device with `fio`
at the same request size). The reader would look like this:

- One ring per thread (`IoUring::builder()` with `setup_single_issuer`,
  `setup_defer_taskrun`, and `setup_coop_taskrun`), 16 files in flight per
  thread, 16 registered 1 MiB buffers (`register_buffers`).
- Per file: `OpenAt2` with `RESOLVE_CACHED` (the walk just touched the dentry,
  so it completes inline); then `statx`, `name_to_handle_at`, and the hole map
  with plain calls on the returned descriptor (metadata, no disk read); then
  `ReadFixed` chunks. On each completion the thread hashes the chunk and
  submits the next one; at the end `Fadvise(DONTNEED)` and `Close`, linked.
- Control flow: a loop over completions and a small state per file. This is
  ordinary C-style asynchronous I/O, not an executor.

#### Event loops and asynchronous runtimes

Readiness notification (`epoll` through `mio` or `polling`, libevent-style)
tells when a socket or a pipe can be read or written. Regular files are always
ready, so it cannot express file I/O at all. Each side of a run owns two or
three pipes and sockets; one blocking thread per direction serves them (section
6.6). An asynchronous runtime is excluded by the constraints and would add
nothing: its file I/O is a thread pool underneath.

#### Refinements, not in the first version

- Files larger than a few hundred MiB can be split into ranges that several
  tasks hash in parallel; rows are per block, so the order does not matter, and
  the inode is sent as one message after a join. This removes the tail where
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

The 32-byte Blake3 hash identifies the content of a block (`blake3::hash`,
section 9). The local always recomputes it on the bytes it read from disk
before it uses a block. Treat Blake3 equality as byte equality.

### 5.4 The hash list

The local exports the set of weak hashes of all indexed blocks as a sorted,
compressed list:

1. `SELECT DISTINCT weak FROM blocks` through the `blocks_by_weak` index.
   Constant and short-period blocks have no rows (section 4.1), so their
   hashes are absent.
2. Keep the top `W = 48` bits, sort (`sort_unstable`), and remove duplicates.
3. Encode the gaps with Golomb-Rice coding: `g_0 = v_0`, and
   `g_i = v_i - v_(i-1) - 1` for `i >= 1` (the values are distinct, so the
   `- 1` loses nothing). A gap `g` is coded as `g >> k` one bits, a zero bit,
   and the low `k` bits of `g`. The bits are packed least significant first
   into bytes, and the last byte is padded with zero bits. The Rice parameter
   is `k = floor(log2(2^W / N)) = W - ceil(log2(N))`, so the average gap is
   between 2^k and 2^(k+1) and each value costs about `k + 2` bits. For
   `N = 0`, `k = 0` and no gap codes follow the header: the empty list is the
   header alone. It occurs for an empty index and for a segment whose removed
   or added list is empty (section 5.6).
4. Prefix a header: `format_version u8`, `W u8`, `k u8`, `N u64`.

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

A false hit costs one Blake3 of the window and one query of about 60 bytes;
the scanner continues at the next offset in any case (section 6.3), so even
thousands per GiB would not matter. Decoding 8M values takes tens of
milliseconds. The sort of 8M values costs well under a second.

The encoding is canonical: the values are sorted and deduplicated, and `k` is
a function of `N`, so equal sets give equal bytes. The Blake3 hash of the
encoded list therefore identifies the set; section 5.6 uses it as the list
hash. Cache the encoded list next to the database together with
`meta.generation`, `meta.index_id`, and the list hash, and rebuild it only when
the generation changed. Section 5.6 describes the cache file and the delta log
that every rebuild extends.

### 5.5 The remote hash table

The remote decodes the hash list into a custom open-addressing table of 32-bit
fingerprint slots. It is a flat array of `u32` obtained from `mmap`, not a
`HashSet<u64>`: hashbrown (the table behind `std::collections::HashSet`) needs
about 9 to 12 bytes per key, hashes the key a second time, touches control
bytes and slots, and gives no control over alignment, huge pages, or batching.

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

### 5.6 The hash list cache and the delta log

The hash list costs about 27 MiB for 8M blocks: about 11 s on a 20 Mbit/s
link, at every run. Between two runs most of the index stays the same; the
working assumption is that about 90 % of the hashes are shared. So the remote
keeps the list between runs, and the local sends only the changes.

What keeps this simple: the list on the remote is always an earlier export of
the local's own list. The two sides never reconcile two independent sets
(section 10). The remote names the list it holds; the local replays the
changes from its own history. The local keeps no state per peer, and the
remote is the only authority on what it holds, so an interrupted run needs no
repair.

Three values identify a list:

- `meta.index_id` (section 4.4). The remote keys its cache by it.
- The generation (`meta.generation`) the list was exported from.
- The list hash: the Blake3 hash of the encoded list (section 5.4). It guards
  against an index restored from a backup, which reuses generation numbers
  for different content, and against a damaged cache file.

Local side: the cache and the delta log live next to the database.

- `hashlist`: the encoded list, behind a header with `index_id`, the
  generation, and the list hash. This is the cache file of section 5.4.
- `deltas/<from_gen>-<to_gen>`: one segment per export that found a new
  generation. A segment is a header (`index_id`, `from_gen`, `to_gen`,
  `from_hash`, `to_hash`) followed by two lists in the format of section 5.4:
  the removed values and the added values, each sorted, each with its own `k`.
  A segment is exactly one HASHDELTA payload; the export writes it once, and
  every later run sends it unchanged.
- The export (step 2 of section 3) takes an exclusive `flock` on
  `hashlist.lock` and compares `meta.generation` with the cached generation.
  When they differ, it exports the new sorted list, decodes the old cache,
  computes the removed and the added values in one merge pass over the two
  sorted arrays, writes the segment, and replaces the cache through a rename.
  It reads `meta.generation` and the block rows in one read transaction (a
  WAL snapshot), so a transaction that the writer commits meanwhile is either
  wholly in the list, with its generation, or wholly out of it. Then it ends
  the read transaction and runs the checkpoint loop of section 4.4, so that
  the generation is durable before the segment and the cache file name it.
  The `flock` is a blocking call without a timeout (section 3).
  Then it prunes the log: it keeps the newest segments while their total size
  stays below the size of the new full list, and deletes the older ones. A
  missing cache (first export, or deleted by hand) gives no segment. A cache
  or a segment whose `index_id` differs from `meta.index_id` (after
  `--rebuild`) is deleted. The lock is released after the pruning, so two
  concurrent runs from one index write one chain of segments.
- Cost: the merge pass over two lists of 8M values and the encoding of the
  changes take tens of milliseconds, on top of the export of section 5.4.

Storage: the log is smaller than the full list, so the local holds at most two
lists per index, whatever the number of peers. In a fleet of N hosts with one
index each that all reach each other, the whole fleet stores N × (N − 1)
cached lists on the remote sides plus two files per host; a second user of a
host has a second index, which adds its own lists. The remote-side copies are
the cache itself; copies per peer on the local side are not needed
(section 10).

Choice at run time: the remote's HELLO (section 6.2) carries the generation
and the list hash of the list it holds for `index_id`; an all-zero hash means
that it holds none. The local looks for the chain of segments that starts at
that generation and ends at the current one, and checks that the `from_hash`
of the first segment equals the remote's hash. If the chain exists, the local
sends its segments, one after the other, as HASHDELTA; the pruning rule
guarantees that they are smaller than the full list. Otherwise it sends
HASHLIST. A remote whose generation and hash equal the local's gets a
HASHDELTA without segments: the chain is empty, and the remote's list is
current. Every list a remote can hold was the local's cache at some time, so
its generation is a segment boundary. The chain is missing when the remote's
list is older than the log or when the local's cache was deleted by hand, and
the hash check fails after a restore from a backup; in each case the remote
gets the full list and its cache is current again after the run.

Remote side, in `serve` (section 6.1):

- Cache directory: `$XDG_CACHE_HOME/hashsync/lists/` (fallback
  `~/.cache/hashsync/lists/`) of the account that runs `serve`, mode 0700.
  One file per `index_id`, named by the id in hexadecimal, mode 0600: a
  header (generation, list hash, `block_size`, `weak_hash_id`, `W`) and the
  encoded list. The file holds the newest list that a run of that index
  delivered. When the directory cannot be written, `serve` keeps the list in
  memory only and reports none at the next run.
- One list per index. An `index_id` names one index file (section 4.4), and
  each user of each local host has an index of their own (`index_path`,
  section 7). A remote that serves two local hosts, or two users of one local
  host, therefore keeps two lists, and the runs of one index never touch the
  list of another. Two indexes with the same roots cost two files. A rebuilt
  index has a new id; its old list stays until the expiry below removes it.
- On the local's HELLO: open the file for `index_id`, read its header, and
  report the generation and the hash when the header's parameters equal
  those of the HELLO; otherwise report none. Keep the descriptor until the
  list is no longer needed: a rename or a deletion by another `serve` (below)
  then cannot take the list away. When the HELLO's `remote_cache` flag is
  false, report none, store nothing, and delete the file of that index if
  there is one.
- On HASHLIST: build the table and store the payload as received.
- On HASHDELTA: decode the cached list, apply the segments in order (one
  linear merge each: drop the removed values, insert the added ones), encode
  the result, and compare its Blake3 hash with the `to_hash` of the last
  segment. Equal: store the new list. Different: log a warning, delete the
  cache file, and continue with the table. This can only cost efficiency
  (section 3); the next run gets a full list. A HASHDELTA without segments
  (the remote's list is current) builds the table from the cached list and
  sets the file's timestamps to now (`futimens` on the open descriptor with
  `UTIME_NOW`), so that the file records the use.
- Stores, deletions, and the expiry below run under an exclusive `flock` on
  the directory's descriptor. The lock is held for a header read and a
  rename, never for a transfer. A store writes the new list under a
  temporary name in the directory, takes the lock, and reads the header of
  the current file. Then it renames the new file into place, unless the
  current file changed since the HELLO and holds a generation that is not
  lower: a concurrent run of the same index stored a newer list first, and
  the new file is discarded. The deletion after a failed merge happens only
  when the file did not change since the HELLO. The rule keeps the file at
  the newest generation whatever the order of concurrent stores. An index
  restored from a backup, whose generation went backwards, still replaces
  the list that the HELLO reported, because that file did not change.
- Expiry: at start-up, before it reads the HELLO, `serve` deletes every file
  of the directory whose `ctime` is older than 3 months (90 days). Every use
  renews the `ctime`: a store renames a new inode into place, and a run that
  changes nothing touches the file. `ctime` and not `mtime`: only the kernel
  sets `ctime`, so it always dates an event on this host, while `cp -p` or
  `rsync -a` keeps an old `mtime`, which would make a restored list look
  unused. The touch sets `mtime` too, so `ls -l` shows the last use. An
  expired list costs the next run of its index one full transfer.
- Cost: one decode, one merge per segment, one encode, and one Blake3 of the
  encoded list; well under a second. The expiry costs one `statx` per file.

Sizes for the 8M-block example of section 5.4. A value of a sparse list costs
about `W - log2(n) + 2` bits: 30 bits at 0.8M values.

| Sent | Size | At 20 Mbit/s |
|---|---|---|
| Full list | 27 MiB | 11 s |
| Delta, 0.8M removed and 0.8M added (10 % churn) | ~6 MiB | 2.5 s |
| Delta, 80k removed and 80k added (1 % churn) | ~0.7 MiB | 0.3 s |

Removals cost as much as additions in this format; section 12 lists the
cheaper encoding as positions in the old list.

`remote_cache` in the configuration (default `true`, per-host override,
section 7) tells the remote through HELLO whether it may keep the list on
disk. The list reveals the hashes of every indexed block (section 6.5).

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
   time with `std::process::Command` (stdin and stdout piped, stderr
   inherited; `SSH` stands for the configured command):
   - data session: `SSH HOST hashsync serve --token T`
   - control session: `SSH HOST hashsync attach --token T`
   The user command and its arguments are not on the `ssh` command line; the
   local's HELLO carries them (section 6.2).
2. `serve` creates a Unix socket `$XDG_RUNTIME_DIR/hashsync/T.sock` (fallback: a
   private directory under `/tmp`), mode 0600, and listens on it before it
   reads anything. It reads the local's HELLO frame from stdin, looks up its
   cached hash list for the local's index (section 5.6), writes its own HELLO
   frame to stdout, accepts the `attach` connection, reads the hash list or
   the delta from stdin, builds the hash table, and then starts the user
   command. It may start the command earlier and buffer its output, up to the
   look-ahead size. None of these waits has a timeout (section 3): when the
   control session never arrives, `serve` waits until its stdin reaches end
   of file, which happens when the local process ends (section 6.7).
3. `attach` connects to the socket. When the socket does not exist yet
   (`ENOENT`, or `ECONNREFUSED` before `listen`), it sleeps 10 ms and tries
   again, for as long as its stdin has a writer: between two attempts it
   polls stdin for `POLLHUP` and exits when the local's side of the session
   is gone. Then it copies its stdin to the socket and the socket to its
   stdout, and shuts down the socket's write side when stdin reaches end of
   file. It has no other logic.
4. The user command gets `/dev/null` as stdin. It inherits stderr, so `ssh`
   forwards it to the local terminal. It runs in a process group of its own
   (`CommandExt::process_group(0)`), so that `serve` can terminate it
   together with its descendants (section 6.7).
5. After the END frame, the local closes its stdin pipes of both sessions and
   waits for the two `ssh` processes to exit. `serve` exits when its stdin
   reaches end of file and removes the socket; `attach` exits when the socket
   closes (section 6.7).

Notes:

- If the user's ssh configuration enables `ControlMaster`, both sessions share
  one TCP connection. The program still works; only the latency benefit shrinks.
  Document this for users.
- Provide `--single-connection` as a fallback: one session, both logical streams
  multiplexed in the frame layer, and a remote writer that drains control frames
  before data frames. No multiplexing library: the frame layer is enough, and
  the Rust ones (`yamux`) are asynchronous.
- Run the remote shell as a subprocess. Do not use an SSH library (`russh`,
  `ssh2`). The subprocess honours the user's configuration, agent, `ProxyJump`,
  and host key checks.
- The remote host needs the same `hashsync` version in `PATH`, or the path given
  by `remote_command` in the configuration. Both sides check versions in HELLO.

### 6.2 Frames

Every frame is `type u8, length u32 (little endian), payload`. All integers
are little endian; a string or a byte string is `length u32` followed by the
bytes; a list is `count u32` followed by the items. Frame types:

| Session | Direction | Types |
|---|---|---|
| data | local to remote | HELLO, HASHLIST, HASHDELTA, SEGMENT, LISTDATA |
| data | remote to local | HELLO, LIT, REF, RLE, END |
| control | remote to local | HELLO, QUERY |
| control | local to remote | HELLO, ANSWER |

Payloads:

- HELLO, local to remote (data session): `protocol_version u32`,
  `program_version string`, `block_size u32`, `weak_hash_id string`, `W u8`,
  `index_id [u8; 16]`, `remote_cache u8`, `lookahead u64`, `scan_threads u32`,
  `compression_level i32`, `compression_threads u32`, `negative_cache u64`
  (bytes), and `command`, a list of byte strings: the argument vector of the
  user command, `argv[0]` first. The
  remote runs it directly, without a shell, and finds `argv[0]` in `PATH` as
  `execvp` does. A user who wants a shell names one:
  `hashsync run host -- sh -c '...'`.
- HELLO, remote to local (data session): `protocol_version u32`,
  `program_version string`, `block_size u32`, `weak_hash_id string`, `W u8`,
  `generation u64`, `list_hash [u8; 32]`: the cached hash list that the
  remote holds for `index_id`, or generation 0 and an all-zero hash when it
  holds none (section 5.6).
- HELLO, both directions (control session): `protocol_version u32`,
  `program_version string`. Both sides send it at once.
- HASHLIST: `generation u64`, `list_hash [u8; 32]`, `length u64`. It announces
  a full list (section 5.4) of `length` encoded bytes, which follow in
  LISTDATA frames. The remote stores the list under `generation` and
  `list_hash` (section 5.6).
- HASHDELTA: `from_generation u64`, `to_generation u64`, `segments u32`. It
  announces a chain of `segments` segments (section 5.6) from the remote's
  list to the local's current one. `segments = 0` means that the remote's
  list is current; then the two generations are equal and nothing follows.
- SEGMENT: `from_generation u64`, `to_generation u64`, `from_hash [u8; 32]`,
  `to_hash [u8; 32]`, `removed_length u64`, `added_length u64`. It announces
  one segment: the encoded list of the removed values, then the encoded list
  of the added values, `removed_length + added_length` bytes in LISTDATA
  frames. The first SEGMENT's `from_generation` and `from_hash` equal what
  the remote's HELLO reported, and each SEGMENT starts where the previous one
  ended. The last one's `to_generation` equals the HASHDELTA's.
- LISTDATA: a chunk of the announced bytes, at most 1 MiB per frame. The
  remote counts the bytes: the transfer of a list or of a segment is complete
  when exactly the announced length has arrived. A LISTDATA frame beyond that
  length, or a frame of another type before it, is a protocol error. After a
  full list, the remote checks that the Blake3 hash of the received bytes
  equals `list_hash`; a mismatch is a protocol error.
- LIT: the literal bytes. REF: `qid u32`. RLE: `byte u8`, `length u64`.
- END: `status i32`, `error string`. `status` is the exit status of the user
  command, `128 + signal` when a signal ended it, or 127 with an error text
  when the command could not be started. Nothing follows END.
- QUERY: `qid u32`, `offset u64`, `weak u64`, `blake3 [u8; 32]`. Query ids
  start at 0 and grow by one per QUERY; `offset` is the stream offset of the
  window.
- ANSWER: `qid u32`, `found u8`. An ANSWER for an unknown qid is a protocol
  error. Answers may arrive in any order.

`protocol_version`, `block_size`, `weak_hash_id`, and `W` must be equal on
both sides, and the program versions must be compatible; any mismatch is a
fatal error before the command starts. On the data session the local sends
its HELLO first. The remote reads it, looks up its cache for that index, and
answers with its HELLO. The local then sends HASHLIST or HASHDELTA with the
LISTDATA that they announce, after which it sends nothing more on this
session; it closes its end after END (section 6.1). The remote starts the
command only when the whole list or chain has arrived.

Compression. After the HELLO exchange, the remote-to-local direction of the
data session is one zstd stream, and all later frames (LIT, REF, RLE, END)
travel inside it. Compression is done by libzstd through the `zstd` crate
(section 9). Rules:

- The remote flushes the compressor (`ZSTD_e_flush`, `Encoder::flush`)
  whenever its outgoing queue runs empty, so a pause in the command output or a
  wait for answers never leaves frames stuck in the compressor. It ends the
  stream after END (`Encoder::finish`). Flushes cost a little ratio and happen
  only when the stream is idle anyway.
- The level comes from `compression_level` in the local configuration (zstd
  scale 1 to 19, 0 disables). Default 9. Levels 6 to 9 keep most of the ratio
  gain over the zstd default of 3 at roughly 100 MB/s per thread on current x86
  cores; above 12 the ratio grows by a few percent for several times the CPU.
  `compression_threads` sets libzstd's worker threads (`Encoder::multithread`;
  default 0: half the remote's cores, at least one), so the compressor keeps up
  with a gigabit link at level 9 on a few cores. Lower the level when the
  remote's CPU, not the network, is the bottleneck; raise it on slow links.
- Only literal bytes cost compression time. REF and RLE frames are a few bytes
  each. Runs shorter than `block_size` stay in the literals, where zstd encodes
  them cheaply.
- Nothing else is compressed: the control session carries random hashes and
  one-byte answers, and the hash list is already entropy-coded.
- The local decompresses with the streaming reader
  (`zstd::stream::read::Decoder`); it yields output block by block, so frame
  parsing is not delayed by more than one zstd block.

### 6.3 Remote scanner

State: a ring buffer of command output with at least `lookahead + block_size`
bytes (default `lookahead = 256 MiB`), a scan position (pass 1, below), and an
emit position (pass 2). Everything between the emit position and the scan
position is in the buffer: literal bytes and the bytes of the recorded items.
The ring buffer is a `memfd` mapped twice back to back (`memfd_create`, then
two `mmap` calls with `MAP_FIXED` into one reserved range), so every window is
one contiguous slice and the wrap-around costs nothing.

The scanner works in two passes that run at the same time on different
threads. Pass 1 finds candidates and records them; it never waits for an
answer. Pass 2 walks the records in stream order, waits for the answers it
needs, and decides what to emit. The split keeps pass 1 a plain loop over the
bytes, and it turns the choice among overlapping candidates into one rule of
pass 2 instead of a rescan protocol between the two.

Records: pass 1 appends items to an ordered list (a bounded queue between the
passes, in increasing order of offset):

- `Run { offset, len, byte }`: a run of at least `B` identical bytes. Pass 1
  skips over it. While pass 1 is still inside the run, the item is open and
  its length grows as bytes arrive; pass 2 can emit the known part before the
  item closes.
- `Query { offset, qid }`: a window whose Blake3 hash was sent as a QUERY.

Pass 1, at position `p` with the window `[p, p + B)` in the buffer:

1. Run check. Pass 1 tracks the start of the current run of identical bytes
   with one comparison per byte as bytes enter the window. When the run that
   ends at the newest byte of the window started at or before `p`, the whole
   window is one byte value: record a `Run` from `p` and continue at the end
   of the run, which may still grow. After the run, compute the hash of the
   next window from scratch (Horner's rule over `B` bytes) and resume rolling.
2. Table probe. Miss: `p += 1`.
3. Table hit: compute the Blake3 hash of the window. In the negative cache
   (below): a false hit, `p += 1`. Otherwise send a QUERY, record a `Query`,
   and `p += 1` as well. Pass 1 never skips a queried window: the offsets
   inside it are probed like any other, so a match that starts inside a
   rejected window is never missed, and no rescan exists.
4. Back to step 1.

Pass 1 stops when `scan - emit >= lookahead` and continues when pass 2 has
advanced. It ends when fewer than `B` bytes remain after `p` at the end of the
stream; those bytes are literal unless they belong to a run.

Cost: every offset outside a run costs one probe, whatever the answers. A
stream whose content is all indexed is therefore scanned at the probe rate
(below), about 150 MB/s per scanner thread from DRAM, not at the Blake3 rate.
This is the price of a scanner that never skips a possible match; section 12
records the measurement that decides whether the price matters. A window is
not skipped on the strength of an earlier "yes" for the same content either:
a REF can only name the block that the local pinned for that query, and an
earlier answer guarantees nothing about a block that is not pinned
(section 10).

Negative cache: an LRU set of the Blake3 hashes whose queries got a "no",
with a byte limit (`negative_cache` in the configuration, default 100 MiB,
about 1.4M hashes; the local's HELLO carries the size). The thread that reads
ANSWER frames maintains it: a "no" inserts the hash and a "yes" removes it.
The key is the Blake3 hash, not the weak hash: two windows with the same weak
hash and different content are possible, and a "no" for one must not suppress
the query for the other. The cache therefore saves the query but not the
Blake3 computation: content that repeats in the stream and is gone from the
local disk costs one Blake3 per occurrence, like matched content. Periodic
content never reaches this path: the index holds no short-period blocks
(section 4.1), so its windows miss the table without a Blake3.

Pass 2, with the emit position `e` (everything before `e` has been sent):

- `Run`: send LIT for `[e, offset)` when `offset > e`, then RLE for
  `[max(e, offset), offset + len)`, and set `e` to the run's end. A run is
  never dropped, only clipped: a window can end inside a run (an indexed
  block that ends in padding), but no window starts inside one, so the clipped
  part is shorter than `B`. A run longer than the look-ahead, or one that is
  still open, is sent as several RLE frames as its known length grows.
- `Query` with `offset < e`: it overlaps something already sent. Drop it. Its
  answer is ignored when it arrives, and the local releases the pinned buffer
  when the stream passes the offset (section 6.4).
- `Query` with `offset >= e`: wait for its answer. "yes": send LIT for
  `[e, offset)`, then REF(qid), and set `e = offset + B`. "no": drop it; `e`
  does not move, so the bytes from `e` on stay literal until the next item.
- No item: when pass 1 has ended, send LIT for the rest, then END
  (section 6.7); otherwise wait for the next item.

This is greedy in stream order, and it is optimal here: all windows have the
same length, so among overlapping windows the one that starts first also ends
first, and taking it leaves the most room for the next one (interval
scheduling by earliest end). Waiting for the earliest pending answer, instead
of taking a later window whose "yes" arrived first, keeps the alignment: a
false hit just before a sequence of matched blocks gets its "no", and the
matched blocks after it are taken at their own offsets.

Answers arrive in any order; pass 2 waits on a table keyed by qid. Its wait
for one answer stalls the output, not pass 1, which continues until the
look-ahead is full.

Several scanner threads (`scan_threads`): split the stream into segments and
let each thread run pass 1 over its own segment, with a hash computed from
scratch at the segment start; a window that starts in a segment may extend
into the next one. Pass 2 consumes the item lists of consecutive segments in
order. Items of two adjacent segments can overlap by less than `B` bytes, and
the greedy rule handles them like any other overlap; a run that crosses a
segment boundary becomes two items and two RLE frames.

CPU cost per byte: the rolling hash (4 to 5 cycles on the dependency chain),
`mix64` (section 5.2) and the slot computation (independent of the chain), and
one table probe. The probe dominates when the table is larger than the
last-level cache: about one cache line per byte, which gives roughly 120 to
180 MB/s per core from DRAM and several hundred MB/s per core when the table
fits in L3. Batch the probes as described in section 5.5. Compression
(section 6.2) runs in libzstd's own threads and touches literal bytes only, so
on a stream with a high deduplication ratio it costs little. Inside a run the
scanner neither hashes nor probes, so a long run of one byte costs a memory
scan only.

### 6.4 Local answering and reconstruction

For each QUERY:

1. `SELECT inode_id, block_no FROM blocks WHERE weak = ? AND blake3 = ? LIMIT 128`.
   SQLite uses the `blocks_by_weak` index and filters the rows by Blake3. The
   candidates are checked in order until one verifies. A candidate costs one
   `block_size` read and one Blake3, mostly from the page cache; a "no" leaves
   the window's bytes literal unless a later candidate covers them
   (section 6.3). The high limit skips over stale rows (deleted or changed
   files).
2. For each candidate: take one `(root_id, rel_path)` of the inode. Open the
   root's stored path (section 4.2) with
   `open(path, O_PATH | O_DIRECTORY | O_CLOEXEC)`, then the candidate with
   `openat2(rootfd, rel_path, O_RDONLY | O_NONBLOCK | O_NOCTTY | O_CLOEXEC,
   RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)` (`rustix::fs::openat2`, Linux 5.6
   and newer; fall back to component-wise `openat` with `O_NOFOLLOW`, with the
   same flags on the last component), and close the root descriptor. Then
   `statx(fd, "", ...)` with the flags of section 4.5, and check `stx_mode`:
   skip the candidate unless the descriptor is a regular file. Then `pread`
   `B` bytes at `block_no * B` and compute Blake3.
   Nothing is kept open between candidates, so the run holds at most one root
   descriptor per lookup worker, whatever the number of roots (a Nix store
   has one root per store path). A root descriptor kept across candidates
   would be no more bound to the indexed tree than one opened now: both
   resolve the stored path at some time, and the Blake3 check decides in
   either case. The two opens cost a few microseconds next to the read and
   the hash.
   The path is stale whenever the file changed after the indexing, and it can
   now name a FIFO, a device, or a socket. `O_NONBLOCK` makes the open of a
   FIFO without a writer return at once instead of waiting for one,
   `O_NOCTTY` keeps a terminal device from becoming the controlling terminal,
   and the type check keeps the `pread` off anything that is not a regular
   file. On a regular file `O_NONBLOCK` has no effect. The indexer opens
   files with the same flags and the same check (section 4.5).
3. On the first match: pin the buffer, keyed by qid and tagged with the query
   offset, and answer "yes". If no candidate verifies, answer "no".

A pinned buffer makes a "yes" binding: the local serves the later REF from
memory, so a concurrent change to the file cannot corrupt the output.

Pinned entries are per query, not per content. A query id names exactly one
entry, and the entry records the query offset. Several in-flight queries for the
same Blake3 (a run of identical blocks, for example) produce several entries.
They share one buffer through a reference count (`Arc`), so identical content
is held once. An entry is released only by its own REF, or when the emitted
stream position passes its offset without a REF (the remote resolved that
region in another way). A buffer is freed when its last entry is released. A
REF can therefore never find its bytes gone, whatever the order of the answers
or the distance between identical blocks in the stream.

Keep verified buffers in an LRU cache of 256 MiB keyed by Blake3 after their
last entry is released. Blake3 equality is byte equality, so a later query for
the same content is answered from the cache without a disk read or a hash
computation; it only pins the cached buffer again.

The pinned set has no limit. It grows with the queries in flight, which the
remote's look-ahead and the in-flight command stream bound in practice, and
the local never answers "no" for lack of memory. Memory is not a concern of
this design, and the peer is trusted (section 6.5).

A small pool of lookup workers (4 to 8 threads, each with its own read
connection) keeps disk latency off the critical path. Answers may be sent out
of order.

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
- A broken session, a broken local stdout, or SIGINT or SIGTERM on the
  local: the side that notices ends its process without a core dump
  (section 6.7), the local with a non-zero status after it killed its `ssh`
  processes, `serve` after it killed the command's process group.
- A protocol error (a frame that does not parse, an unknown qid): a bug on
  one side, since SSH guarantees the integrity of the stream. It is a panic
  and leaves a core dump (section 6.7).
- Security: the hash list reveals the block hashes of all indexed files to the
  remote host, and with the cache of section 5.6 it stays on the remote's disk
  between runs and for 3 months after the last run (mode 0600, under the
  remote user's cache directory). Use `hashsync` only with hosts the user
  trusts. Set `remote_cache = false` for a host that must not keep the list
  (section 7); the next run to that host deletes the list that an earlier run
  left there.

### 6.6 Threads at run time

Both roles use a few blocking threads and bounded channels, no event loop
(section 4.6). Every hand-off is a channel. Threads are never joined and never
cancelled: the process ends when the run has ended or when a fatal error
occurs (section 6.7).

Remote, `serve`:

- The main thread does the handshake of section 6.1, starts the command and
  the threads below, and then blocks in `read` on its stdin until end of
  file. That is how it learns that the local has closed the data session.
  When the resolver has finished by then (the normal end), it removes the
  socket file and exits 0; otherwise the local went away early, and it dies
  (section 6.7).
- One reader thread reads the command's stdout pipe into the ring buffer
  until end of file. The pipe is enlarged to 1 MiB with `F_SETPIPE_SZ`
  (`rustix::pipe::fcntl_setpipe_size`), so the reads are large. At end of
  file it waits for the child (`Child::wait`) and hands the exit status to
  the resolver. The child can exit before its stdout reaches end of file (a
  descendant still holds the pipe) or after it (it closed stdout and went
  on); the run has ended only when both have happened, and END is sent only
  then.
- Scanner threads (`scan_threads` from the local's HELLO) run pass 1 of
  section 6.3 over segments of the stream and hand queries to the control
  writer.
- One resolver thread runs pass 2. It owns the zstd encoder and stdout, sends
  the resolved items in stream order, and flushes when it has nothing to
  send. After the last item and the exit status it sends END, ends the zstd
  stream (`Encoder::finish`), flushes and closes stdout, and sets the
  finished flag that the main thread reads.
- One control thread writes QUERY frames to the Unix socket; one reads ANSWER
  frames from it, updates the negative cache, and posts each answer to the
  table that the resolver waits on (section 6.3).
- The child command is a `std::process::Command` with stdin `/dev/null`,
  stdout piped, stderr inherited, and a process group of its own
  (section 6.1).

Local, `run`:

- The main thread starts the two `ssh` processes and the threads below, then
  runs the data loop: it reads the data session through the streaming
  decoder, parses frames, and writes to stdout: literals as they come,
  references from the pinned buffers, runs from a static buffer. Blocking
  writes to stdout give backpressure to the whole chain. After END it
  flushes stdout, closes the stdin pipes of both sessions, waits for the two
  `ssh` processes, and exits with the status from END.
- One control thread reads QUERY frames and dispatches them to the lookup
  workers over a channel (section 6.4); one control thread writes the ANSWER
  frames that the workers put on another channel.
- The pinned table and the LRU cache sit behind one mutex; they are touched
  once per query and once per REF.
- One signal thread blocks in `signal_hook::iterator::Signals` for SIGINT and
  SIGTERM and dies (section 6.7) when one arrives, with status 128 plus the
  signal number.

`attach` copies with two threads and `std::io::copy`, which uses `splice`
between a pipe and a socket on Linux. The thread that copies stdin to the
socket shuts down the socket's write side when stdin reaches end of file; the
other thread ends when the socket reaches end of file. When either copy fails
(`EPIPE`, a reset), `attach` exits.

### 6.7 Shutdown

There is no in-process cancellation. A thread that is blocked in a system
call cannot be woken reliably by another thread: `close` from another thread
does not interrupt a `read` or `write` that is blocked on that descriptor,
`pthread_cancel` is not usable from Rust, and a wake-up signal (`pthread_kill`
with a handler that does nothing, so that the call returns `EINTR`) races
with the entry into the call and is swallowed by every helper that retries
`EINTR` (section 10). The program needs none of it. Everything it keeps
outside memory is crash-safe by construction: the index is an SQLite database
in WAL mode, the cache files and the segments are written under a temporary
name and renamed, and the remote stores its list the same way. A run that
fails has nothing to hand over. So every fatal condition ends the process,
and the kernel closes its descriptors, which ends every peer through end of
file and `EPIPE`.

Two exits exist, chosen by the root cause. A condition that is not a fault
of the program (a network failure, a consumer that closed the local's
stdout, a signal) ends the process without a core dump. A fault of the
program or of the machine (a bug, an invalid frame, a database error, an
I/O error) aborts and leaves a core dump.

`die(status, message)` is the first kind, one function per role, callable
from any thread. It prints the message to stderr, does the cleanup that the
kernel would not do, and exits with `status` without unwinding
(`std::process::exit`; no destructor needs to run) and without a core dump:
a network error is not a fault of the program, and its core dump would show
nothing. A second caller blocks until the process is gone. The cleanup is:

- remote: kill the command's process group (`kill(-pgid, SIGKILL)`) and remove
  the socket file. A temporary cache file (section 5.6) is left to the expiry
  pass of the next `serve`.
- local: kill both `ssh` processes with SIGKILL. The kernel then closes their
  descriptors, the remote's `serve` gets end of file on stdin and dies in
  turn, and `attach` ends with its socket.

Everything else is the second kind, a panic: the crate builds with
`panic = "abort"` (section 11), and a panic hook installed at start runs the
same cleanup before the abort, so a bug leaves neither a running command nor
a stray socket, and the abort produces a core dump. An invalid frame on any
session is a panic, not a `die()`: SSH guarantees the integrity of the
stream, so a frame that does not parse is a bug on one side. Errors during
indexing (a database error, a read error other than a vanished file) are
panics too: the index is a cache and recovers at the next update
(section 4.4).

Who calls `die()`:

| Event | Noticed by | Role |
|---|---|---|
| stdout is broken (`EPIPE`) or fails | main thread | local |
| The data session reaches end of file before END | main thread | local |
| The control session reaches end of file | control reader | local |
| SIGINT or SIGTERM | signal thread | local |
| stdin reaches end of file before the resolver has finished | main thread | remote |
| stdout is broken (`EPIPE`) | resolver | remote |
| The control socket reaches end of file before the resolver has finished | control reader | remote |

A command that cannot be started is not a failure of the run: `serve` sends
END with status 127 and the error text, and the normal end follows.

Normal end, in order:

1. The reader thread of `serve` gets end of file on the command's stdout and
   the exit status from `Child::wait`.
2. The resolver sends the last literal bytes, then END with the status, ends
   the zstd stream, flushes and closes stdout, and sets the finished flag.
   `serve` writes nothing more.
3. The local's main thread receives END, writes the last bytes to stdout and
   flushes it, and closes the stdin pipes of both sessions.
4. The main thread of `serve` gets end of file on stdin, sees the finished
   flag, removes the socket file, and exits 0. The kernel closes the socket;
   `attach` gets end of file on stdin and on the socket and exits.
5. Both `ssh` processes exit; the local waits for them and exits with the
   status from END.

Every step waits for a definite event, and none has a deadline (section 3). A
descendant of the command that keeps its stdout open keeps the run going, as
it would under plain `ssh`. A descendant that left the process group
(`setsid`) survives `die()`, as it would under plain `ssh`, and gets `EPIPE`
at its next write.

## 7. Configuration

`$XDG_CONFIG_HOME/hashsync/config.toml`, or the path given by `--config`:

```toml
block_size     = "32KiB"   # 4 KiB or more; a change discards and rebuilds the index
index_path     = "~/.local/state/hashsync/index.sqlite"
threads        = 0         # indexer threads; 0 = 4 × cores, at least 16, at most 64
lookahead      = "256MiB"  # how far the remote scanner runs ahead of the output
scan_threads   = 0         # remote scanner threads; 0 = half the cores
negative_cache = "100MiB"  # remote LRU of Blake3 hashes answered "no" (6.3)
compression_level   = 9    # zstd level for the data session, 1 to 19; 0 disables
compression_threads = 0    # libzstd worker threads on the remote; 0 = half the cores
ssh            = "ssh"     # remote shell command, split like a shell would (rsync's --rsh);
                           # the program appends -o Compression=no
remote_command = "hashsync"  # path of hashsync on the remote host
remote_cache   = true      # the remote may keep the hash list between runs (section 5.6)

[hosts.builder]            # optional overrides, keyed by the HOST argument of `hashsync run`
ssh            = "ssh -p 2222 -i ~/.ssh/builder"
remote_command = "/run/current-system/sw/bin/hashsync"
remote_cache   = false     # shared machine: do not leave the hash list on its disk

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

The hash list width `W` (48), the multiplier `a`, and `mix64` are protocol
constants, not configuration.

## 8. Command line

- `hashsync update [--rebuild] [-j N]`: update the index, or discard and
  rebuild it. `-j` overrides `threads`.
- `hashsync run [--no-update] [--single-connection] [--ssh CMD] [USER@]HOST -- COMMAND ARGS...`:
  run COMMAND on HOST, write its stdout to the local stdout, and exit with the
  status of COMMAND. HOST is appended to the remote shell command unchanged, so
  `user@host` and `ssh_config` aliases work. COMMAND and ARGS travel in the
  HELLO frame (section 6.2) and run on the remote without a shell. `--ssh`
  overrides the configured remote shell command for this run. At the end it
  prints one summary line to stderr: output bytes, literal bytes, referenced
  bytes, run bytes, bytes on the wire, and the bytes of the hash list or delta
  that was sent.
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

Versions checked against crates.io on 2026-09-10. The toolchain is stable Rust
(1.98.1, released 2026-09-03), edition 2024. Three dependencies compile C
sources through the `cc` crate: SQLite, libzstd, and the Blake3 SIMD kernels.
The target's C compiler is needed at build time; there is no pure-Rust build
and no need for one.

| Need | Crate | Version | Notes |
|---|---|---|---|
| Weak hash | own code | | Sections 5.1 and 5.2, about 50 lines. |
| Hash list coding | own code | | Golomb-Rice encoder and decoder, about 60 lines. |
| Hash list cache and delta log | own code | | Section 5.6: diff of two sorted lists, segment files, pruning, the remote's cache, merge, and expiry; about 150 lines over both roles. |
| Remote hash table | own code | | Section 5.5, about 100 lines plus the SIMD bucket scan. |
| Blake3 | `blake3` | 1.8.7 (2026-08) | `blake3::hash`. Assembly for SSE4.1, AVX2, and AVX-512 on x86_64 with run-time detection; NEON on aarch64 by default. See below. |
| SQLite | `rusqlite` | 0.40.2 (2026-08) | Feature `bundled`: SQLite 3.53.2 through `libsqlite3-sys` 0.38.2. `prepare_cached`, `transaction_with_behavior`, `busy_handler`, `pragma_update`. See below. |
| System calls | `rustix` | 1.1.4 (2026-02) | `fs::{openat, openat2, statx, RawDir, fadvise, seek, memfd_create, flock, futimens}`, `io::{pread, preadv2}`, `mm::{mmap, mmap_anonymous, madvise}`, `pipe::{splice, fcntl_setpipe_size}`, `process::setrlimit`, `rand::getrandom`, `ioctl::{Getter, opcode}`. Raw system calls on Linux (`linux_raw` backend). |
| System calls, gaps | `libc` | 0.2.189 (2026-08) | `name_to_handle_at`, `file_handle`, `MAX_HANDLE_SZ`, `syscall(SYS_cachestat, ...)`. |
| Kernel constants | `linux-raw-sys` | 0.12.1 (2025-12) | `general::{fsuuid2, cachestat, STATX_MNT_ID_UNIQUE}`. Already a dependency of `rustix`. |
| Thread pool | `rayon` | 1.12.0 (2026-04) | A dedicated `ThreadPool`, `scope` and `spawn`. LIFO local deques, oldest-first stealing (section 4.6). |
| Work stealing, alternative | `crossbeam-deque` | 0.8.8 (2026-09) | `Worker::new_lifo` and `Stealer`, as in `ignore`. Only if rayon is dropped. |
| Channels | `crossbeam-channel` | 0.5.17 (2026-09) | Bounded channels to the writer and between the run-time threads; `select!`. `std::sync::mpsc::sync_channel` is enough where no `select!` is needed. |
| io_uring, optional reader | `io-uring` | 0.7.15 (2026-09) | `IoUring`, `opcode::{OpenAt2, ReadFixed, Fadvise, Close}`, `submit_and_wait`, `register_buffers`, `setup_single_issuer`, `setup_defer_taskrun`, `setup_coop_taskrun`. Synchronous API, no runtime, no liburing. |
| zstd | `zstd` | 0.14.0 (2026-09) | libzstd 1.5.7 through `zstd-sys` 2.1 (bundled; the `pkg-config` feature links the system library). See below. |
| TOML | `toml` | 1.1.5 (2026-09) | With `serde` 1.0.229 derive. |
| Command line | `clap` | 4.6.6 (2026-08) | Derive API. |
| Globs | `glob` | 0.3.4 (2026-07) | `glob_with` for roots, `Pattern::matches_with` for `exclude`, options of section 4.2. |
| Hash maps | `foldhash` | 0.2.0 (2025-08) | `foldhash::fast::RandomState` for `std::collections::HashMap`. Alternative: `rustc-hash` 2.1.3. See below. |
| Identity hasher | `nohash-hasher` | 0.2.0 (2020-02) | For maps keyed by Blake3 or by the weak hash. Or 10 lines. |
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
32 KiB read and a Blake3 computation that cost more than the lookup; the
indexer's budget is in section 4.6.

Usage rules:

- Open with WAL mode and `synchronous=NORMAL`. The export runs
  `PRAGMA main.wal_checkpoint(FULL)` through `query_row` in a loop until the
  first column is 0, before it publishes a generation (section 4.4). Install
  a busy handler that retries without limit (`Connection::busy_handler` with
  a callback that sleeps for a millisecond and returns `true`); never
  `busy_timeout`, which is a deadline (section 3). A second process on the
  same index (`hashsync update` next to `hashsync run`) then waits for the
  write lock as long as it takes. Open with `SQLITE_OPEN_NO_MUTEX`: a
  `Connection` is `Send` and not `Sync`, and every connection belongs to one
  thread.
- The writer thread owns its connection and starts transactions with
  `transaction_with_behavior(TransactionBehavior::Immediate)`, so a write
  transaction never fails with `SQLITE_BUSY` halfway through. Pool threads and
  lookup workers own read connections.
- `prepare_cached` for every statement; bind positionally; never build SQL
  strings per row. Step the rows; do not collect them.
- The pragmas of section 4.4: a large `cache_size` for the writer, a small one
  for readers, and no `blocks_by_weak` during a rebuild.

### System calls: `rustix` and `libc`

`rustix` wraps the calls with `OwnedFd` and `BorrowedFd` and returns
`io::Result`; on Linux its default `linux_raw` backend makes the system calls
itself, without libc. It covers everything in section 4.5 except four items,
which the `sys` module supplies (section 11):

- `name_to_handle_at`: `libc::name_to_handle_at` with a `MAX_HANDLE_SZ` buffer
  behind a `libc::file_handle` header.
- `FS_IOC_GETFSUUID`: the opcode from
  `rustix::ioctl::opcode::read::<fsuuid2>(0x15, 0)`, the struct from
  `linux_raw_sys::general::fsuuid2`, the call through `rustix::ioctl::Getter`.
- `STATX_MNT_ID_UNIQUE`:
  `StatxFlags::from_bits_retain(linux_raw_sys::general::STATX_MNT_ID_UNIQUE)`;
  `rustix` 1.1 only names `MNT_ID`.
- `cachestat`: `libc::syscall(libc::SYS_cachestat, fd, &range, &result, 0)`
  with the struct from `linux_raw_sys::general::cachestat`; `rustix` 1.1 lists
  it as not implemented.

### Threads: `rayon` and `crossbeam-channel`

Section 4.6 gives the reasons for rayon. `std::thread::scope` and
`std::sync::mpsc::sync_channel` are enough for the run-time threads of section
6.6; `crossbeam-channel` adds `select!` and is faster under contention.

### io_uring: the `io-uring` crate

Only for the optional reader of section 4.6. It is a direct binding to the ring
interface, needs no liburing, and has no executor: the caller pushes entries,
calls `submit_and_wait`, and drains completions. `rustix::io_uring` exposes the
raw `io_uring_setup` and `io_uring_enter` calls too, but the typed opcodes of
the `io-uring` crate are worth the dependency.

### Compression: the `zstd` crate

Binds libzstd 1.5.7. `stream::write::Encoder::new(stdout, level)`,
`multithread(n)` for `ZSTD_c_nbWorkers`, `flush()` for `ZSTD_e_flush`,
`finish()` for `ZSTD_e_end`, and `set_parameter(CParameter::CompressionLevel)`
during the stream (libzstd applies a new level to the next job of a
multithreaded stream). `stream::read::Decoder` for the local. The pure-Rust
`ruzstd` 0.9 has a complete decoder but only an uncompressed and a fastest
level in its encoder, so it is not an option for the remote.

### Blake3: the `blake3` crate

`blake3::hash(&block)` returns 32 bytes. The crate carries assembly for SSE4.1,
AVX2, and AVX-512 on x86_64 with run-time detection, and NEON C code that its
build script enables by default on little-endian aarch64. 32 KiB inputs are 32
chunks, enough to fill the widest vectors. Do not enable the `rayon` or `mmap`
features: blocks are hashed in parallel by the pool, not inside one hash.

### Hash maps and caches

`std::collections::HashMap` is hashbrown; only its default hasher (SipHash-1-3)
is slow. Use `foldhash::fast::RandomState` (hashbrown's own default) or
`rustc_hash::FxHashMap` for the sets keyed by `(fs_id, handle)`, the
filesystem cache of the walker, and the pinned entries by query id. Keys that
are already hashes (Blake3 in the LRU cache) get an identity hasher. The LRU
cache of verified buffers (section 6.4) is `lru::LruCache` or about 60 lines.
The remote table stays custom (section 5.5).

## 10. Alternatives considered

- **Go as the implementation language.** Replaced by Rust on 2026-09-10 (user
  decision). Rust gives direct access to SIMD and prefetch intrinsics, control
  over memory layout, and threads without a scheduler in between; C libraries
  (SQLite, libzstd) link without a cgo/pure split, so the build has one
  configuration. The Go-era entries below are kept for the record.
- **Asynchronous runtime (`tokio` and the like).** Rejected on 2026-09-10. A
  constraint of the project, and no benefit: the file I/O of such runtimes is
  a thread pool underneath, and the control flow of the program becomes hard
  to follow.
- **Readiness event loop (`mio`, `polling`, libevent-style).** Rejected on
  2026-09-10. Regular files are always ready to `epoll`, and the program has
  two or three pipes and sockets per side (section 4.6).
- **`io_uring` for the directory walk.** Rejected on 2026-09-10. No
  `getdents64`, no `name_to_handle_at`, no `ioctl`; `statx` and `openat2` run
  on io-wq, which is a thread pool inside the kernel.
- **`io_uring` for the file reads.** Deferred on 2026-09-10. Optional backend
  of the hashing stage, adopted only after a measurement (section 4.6).
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
  as an optional refinement. Reverted on 2026-09-10. A "no" means that the
  probe at `p` was wrong, so the scanner must continue at `p + 1` as after a
  miss; otherwise a false hit just before a matched file loses that file's
  first block. The rescan itself was replaced later the same day by the
  two-pass scanner (below), which probes every offset once; the negative
  cache stays mandatory.
- **Negative cache keyed by the weak hash.** Rejected on 2026-09-10. It would
  suppress the query for a different window with the same weak hash, a false
  negative that the weak hash must never cause on its own. The key is the
  Blake3 hash (section 6.3). Since the cache then no longer avoids the Blake3
  computation, short-period blocks were removed from the index the same day
  (section 4.1), so that periodic content misses the table instead.
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
- **cgo SQLite drivers** (Go era). Rejected on 2026-09-09 in favour of the
  pure-Go `zombiezen.com/go/sqlite`. Moot since the switch to Rust: `rusqlite`
  binds the C library, and the per-call overhead that made the pure-Go driver
  competitive does not exist.
- **Unmixed weak hash in the index** with mixing only at export and probe time.
  Reverted on 2026-09-10 in favour of storing the mixed value everywhere; one
  quantity is simpler.
- **Marking inodes as immutable** when they are found under an immutable root.
  Reverted on 2026-09-10. The `(size, ctime)` check is cheap and is now the
  single criterion for hashing a file again. Immutable roots alone give the
  saving: their files are never stat'ed unless a mutable root reaches them.
- **One root row per copy of an immutable tree**, with the path rows copied
  to every copy. Rejected on 2026-09-10. It multiplies the path rows of a
  Nix store by the number of copies and needs a re-parenting of the rows
  when the scanned copy disappears. The identifier of section 4.2 keeps one
  root with one stored path instead.
- **Identifier from the wildcard part of the glob** (the components that the
  wildcards matched). Considered and dropped on 2026-09-10. It is ambiguous
  with several wildcards and undefined for a literal path; `name` and
  `strip_prefix` say the same thing explicitly.
- **Renaming a root when its `identifier` option changes**, by finding the
  stored path of an old root among the paths of a new identifier, so that the
  rows survive without a rescan. Dropped on 2026-09-10 (user decision): not a
  normal use, and it adds logic to the update. A change of the option
  rescans the root (section 4.2).
- **`(st_dev, st_ino)` as the inode identity**. Rejected on 2026-09-10.
  `st_dev` changes between boots on XFS and on btrfs subvolumes, which would
  rebuild the whole index; inode numbers are reused. Section 4.3 uses the
  filesystem UUID and the file handle instead.
- **`(fs_id, inode number)` as the key of the walker's in-memory sets.**
  Replaced on 2026-09-10 by `(fs_id, handle)`, the same identity that the
  index uses. Inode numbers are reused inside one scan when files are
  deleted and created, and two btrfs subvolumes of one filesystem can hold
  different inodes with one number. An integer id assigned by the index was
  considered too: the walker's threads would have to ask the writer for it
  before they can test a file, which puts the writer on the path of every
  file. The handle bytes need no round trip.
- **The mount id as the filesystem identity when the ioctl is unsupported.**
  Replaced on 2026-09-10 by `f_fsid` from `fstatfs`. The mount id changes at
  every boot and is shared by all btrfs subvolumes below one mount; `f_fsid`
  is stable on most filesystems and differs per subvolume on btrfs. The
  triple `(stx_mnt_id, stx_dev_major, stx_dev_minor)` replaced the mount id
  alone as the trigger for resolving the filesystem id for the same reason.
- **Indexing incomplete blocks** (the shorter tail of each file). Considered
  and dropped on 2026-09-10. The remote scan uses windows of exactly
  `block_size` bytes and could never find them, so they would only cost index
  rows. The default block size went from 64 KiB to 32 KiB instead, which halves
  the unmatched tail of every file.
- **Shell-style dot handling in globs** (`*` not matching a leading dot).
  Considered and dropped on 2026-09-10. The program uses the `glob` crate with
  its default `require_literal_leading_dot = false` (Go's `filepath.Glob`
  behaved the same), and `exclude` handles entries such as `/nix/store/.links`.
- **Content-defined chunking** (as in casync, restic, and borg) on both sides
  would reduce the remote's lookups from one per byte to one per chunk. It was
  rejected: inside an archive stream, the chunk at the start and the chunk at
  the end of every file span the archive's framing, so about two chunks per file
  are lost. Trees with many files between 10 KiB and 1 MiB lose a lot. Fixed
  local blocks with a rolling remote scan lose only the incomplete tail of each
  file.
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
  pursued. Revisit if the remote scan rate turns out to be the bottleneck.
- **Per-peer copies of the hash list on the local**, so that the local can
  compute the delta for each remote itself. Rejected on 2026-09-10. It costs
  one list per peer on every host (about 2 × N² lists in a fleet of N hosts
  instead of N × (N − 1)), needs a peer identity and a cleanup of stale
  copies, and leaves the local unsure of what the remote holds after an
  interrupted transfer. The remote reports the generation and hash of the
  list it holds, and the local replays its own delta log (section 5.6). The
  local keeps no state per peer.
- **Set reconciliation sketches** (IBLT, rateless IBLT, minisketch), to find
  the difference without history on the local. Rejected on 2026-09-10. An
  IBLT costs at least 11 bytes per cell (a count, a 48-bit key sum, and a
  32-bit check sum) and about 1.3 to 1.5 cells per differing key: 22 to 38 MiB
  for the 1.6M differing keys of the 10 % case, against 27 MiB for the full
  list and about 6 MiB for the delta. At any churn it costs about four times
  the delta, and it needs an estimate of the difference (or a rateless scheme)
  and a fallback for a failed decode. Minisketch is optimal in size but decodes
  in O(d²), for thousands of differences, not millions. The delta log costs
  about 4 bytes per changed key, and the local already holds the list it diffs
  against.
- **Range fingerprint reconciliation** (Merkle-style recursion over the sorted
  key space, as in negentropy). Rejected on 2026-09-10. The weak hashes are
  uniform, so 10 % churn touches every range at every level, and the recursion
  sends about as much as the full list.
- **Bloom filter exchange** (each side sends a filter of its set). Rejected on
  2026-09-10. Two filters at 1 % false positives cost about 10 MiB each for 8M
  keys, and a false positive drops a new hash from the remote's table.
- **Deltas with additions only**, never removals. Rejected on 2026-09-10. The
  remote's table would grow by the churn at every run, and a stale hash whose
  content still appears in the output costs one query per occurrence until the
  negative cache learns it. Removals cost half of the delta as values and much
  less as positions (section 12).
- **Composing a chain of segments into one net delta** before sending it.
  Considered and dropped on 2026-09-10. It only saves bytes when a hash leaves
  and returns within the chain, which is rare for build outputs, and the
  pruning rule already caps a chain below the size of the full list. The local
  sends the segments as they are on disk, and the remote applies them in order
  (section 5.6).
- **Expiry of the remote cache after 30 days of no use, by `mtime`.**
  Replaced on 2026-09-10 (user decision) by 3 months, measured on `ctime`
  (section 5.6). Any program can set `mtime`, and `cp -p` or `rsync -a` keeps
  an old one, so a restored list would look unused; only the kernel sets
  `ctime`. `atime` is out: `noatime` mounts never update it.
- **Last writer wins for the remote cache file.** Rejected on 2026-09-10. Two
  concurrent runs of one index store in either order, so the older list could
  replace the newer one, and the next run would send the same segments again.
  The store reads the header under the lock and keeps the newer file
  (section 5.6).

- **Optimistic skip after a query, with a rescan of the window after a
  "no".** Replaced on 2026-09-10 (user decision) by the two-pass scanner of
  section 6.3. The rescan was bounded by the next pending window, so a real
  match that started inside a rejected window and ended after the next
  candidate was lost when that candidate was rejected too, and an unbounded
  rescan would have needed a record of which offsets were probed. Pass 1 now
  probes every offset once and continues at the next offset after a query.
  The cost is one probe per byte outside runs (section 12).
- **A memory limit on the pinned set**, with "no" answers once it was
  reached. Removed on 2026-09-10 (user decision). Memory is not a concern of
  the project; the look-ahead and the in-flight stream bound the set in
  practice, and the peer is trusted. The negative cache of the remote keeps
  a limit because it is a cache, at 100 MiB instead of the earlier 2 MiB.
- **Timeouts**: 60 s for the `attach` connection, a deadline on the retries
  of `attach`, and `busy_timeout` in SQLite. Removed on 2026-09-10 (user
  decision) in favour of the design principle of section 3 and the
  end-of-file chain of section 6.7. A program that gives up after a fixed
  time fails on a
  system that is slower than the programmer expected; a program that hangs
  can be inspected with a debugger.
- **`synchronous=FULL`** for the index, so that every commit is durable.
  Adopted on 2026-09-10 because cache files and segments record generations,
  and a generation that a power loss takes back would be used again for
  different rows. Replaced on 2026-09-11 (user decision) by `NORMAL` with a
  FULL checkpoint at publication (section 4.4): the `fsync` per commit was
  paid at every batch for a guarantee that only the export needs, and a
  generation that no export has published can be lost without harm.
- **A positive cache** of the Blake3 hashes answered "yes", with a skip over
  the window at a cache hit. Added on 2026-09-10 and removed on 2026-09-11
  (user decision). An unpinned block is not guaranteed, so the skip still
  needed a query and saved probes only, and it brought back a window whose
  inside is not probed. Pass 1 now advances by one at every offset outside a
  run (section 6.3). The negative cache grew from 50 MiB to 100 MiB with the
  removal.
- **An LRU of open root descriptors at run time** (1024 entries). Added on
  2026-09-10 and rejected on 2026-09-11 (user decision). A descriptor that is
  closed and opened again by path can name another inode, and a cache of
  descriptors is not needed at all: the run opens the root for each candidate
  read and closes it afterwards (section 6.4). During indexing, the roots are
  pool tasks, opened when a thread takes them, so they never hold descriptors
  all at once (section 4.6).
- **In-process cancellation with per-thread wake-ups** (`pthread_kill` with
  a handler that does nothing, `shutdown` on sockets, notified condition
  variables, and joins). Designed on 2026-09-10 and replaced on 2026-09-11
  (user decision) by ending the process (section 6.7). The wake-up signal
  races with the entry into the system call and is swallowed by every helper
  that retries `EINTR`, `pthread_cancel` was never an option in Rust, and
  nothing in the program needs an orderly stop, because all persistent state
  is crash-safe. Network errors end the process the same way, without a core
  dump (user decision); bugs, protocol errors, and errors during indexing are
  panics and leave one.
- **One generation per update**, incremented after the last commit. Replaced
  on 2026-09-10 by an increment inside every transaction that changes block
  rows. A crash between the last commit and the increment left changed rows
  under an unchanged generation, so the cached hash list looked current and
  was stale (section 4.4).
- **Whole inodes per transaction and per channel message.** Replaced on
  2026-09-10 by streamed block rows with a byte budget (section 4.6). One
  1 TiB file has more than 1 GiB of hashes, which no message-count bound
  contains. The `size = -1` marker keeps a cut inode safe (section 4.4).
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
    mount tracking, hashing; the optional `io_uring` reader.
  - `index/`: schema, migrations, writer, pruning, hash list export, cache, and
    delta log.
  - `proto/`: frame types, encoding, HELLO handshake.
  - `remote/`: `serve` (hash list cache, ring buffer, scanner, resolver,
    shutdown) and `attach` (relay).
  - `local/`: `run` (remote shell processes, answering, reconstruction).
  - `config.rs`: `config.toml` and XDG paths.
  - `sys/`: every `unsafe` FFI call (handles, ioctl, `cachestat`, the `statx`
    bit, ring setup). The crate root has `#![deny(unsafe_code)]`; `sys`, the
    SIMD bucket scan, and the `io_uring` reader carry `#![allow(unsafe_code)]`.
- Run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test` before you
  finish a change.
- Tests to keep: rolling hash equals direct hash (`proptest`); the
  interleaved k-stream hash and the split block hash equal the serial ones;
  Golomb-Rice round trip on random and adversarial gap patterns; the delta log
  (section 5.6): the diff of two lists applied to the first gives the second,
  a chain of several segments, the pruning rule, the choice between full list
  and chain from the remote's HELLO, a list hash mismatch on the remote that
  gets a full list at the next run, and an index restored from a backup (same
  generation, different content) that the hash detects; the remote cache
  (section 5.6): two indexes served from one directory, each with its own
  file, a HASHDELTA without segments that touches the file, the expiry with
  the current time as a parameter (tests cannot set `ctime`), and a store
  that does not replace a newer generation; the hash table
  never gives a false negative; traversal tests on a temporary tree (hard links,
  symlink loops, dot names and `exclude`, immutable roots and their
  identifiers (two copies under `name`, `strip_prefix`, and `literal`, as
  directories and as single files, scanned once; the stored path moves when
  the scanned copy disappears; `identifier` rejected on a mutable root), glob
  roots, ctime change, pruning, identity fallbacks when the ioctl or handles are
  unsupported, sparse files with holes that start and end off block boundaries,
  descriptor count under a low `RLIMIT_NOFILE`); a fuzz target (`cargo fuzz`)
  for the frame decoder; runs of one byte at arbitrary offsets, crossing block
  boundaries and longer than the look-ahead, reproduced byte-exact through RLE
  frames, and constant blocks absent from the index; the two-pass scanner
  (section 6.3): a rejected candidate just before a sequence of matched
  blocks keeps their alignment, overlapping candidates with "yes" answers
  resolve to the earliest, and a run clipped by a preceding window is sent
  in full from the clip on; a negative cache hit needs a Blake3 match, and a
  different window with the same weak hash is still queried; the empty list
  (`N = 0`) and a segment with an empty removed or added list round-trip; a
  list sent over several LISTDATA frames, and a transfer whose bytes do not
  match the announced length or hash is rejected; an interrupted update
  (section 4.4): an immutable root left incomplete is traversed again and
  then marked complete, an inode left at `size = -1` is hashed again, block
  rows of one file spread over several transactions, a `Path` message that
  arrives before the `Begin` of its inode, the generation read in one
  snapshot with the rows it describes, and the checkpoint before publication
  (after an export, `wal_checkpoint` reports no frames left in the WAL); an
  indexed file replaced by a FIFO or
  a directory gets a "no" without blocking, at run time and in the indexer;
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
  current. Cross builds for a remote of another architecture go through
  `pkgsCross`; static builds through `pkgsStatic` (musl). The bundled C sources
  build in both.

## 12. Open questions

- **Stored Blake3 length.** 32 bytes as specified, or 16 bytes to save about
  20 % of the index and a fifth of the writer's work (section 4.4).
- **Adaptive compression level.** The streaming encoder of the `zstd` crate
  exposes `set_parameter(CParameter::CompressionLevel)`, and libzstd applies a
  new level to the next job of a multithreaded stream, so the setter exists.
  What remains is the controller: raise the level while the network is the
  bottleneck, lower it while the compressor is.
- **`io_uring` reader.** Adopt or drop after the measurement of section 4.6,
  before the first release.
- **Thread count default.** `4 × cores`, clamped to 16 to 64, assumes NVMe.
  Measure a cold and a warm full build and an incremental update with 8, 16,
  32, and 64 threads.
- **Spinning disks.** A per-root limit on concurrent readers and reading in
  inode order; not in the first version.
- **Removals as positions in the delta.** A segment encodes removed hashes as
  48-bit values, about 30 bits each. The remote holds the old list, so a
  removal could be its rank in that list: the gaps between ranks cost about
  `log2(N / n_removed) + 2` bits, 5 to 6 bits at 10 % churn. This cuts the
  10 % delta of section 5.6 from about 6 MiB to about 3.5 MiB for about 20
  lines on each side. Decide after the first measurements on a real tree.
- **Scan rate on matched streams.** Pass 1 probes every offset outside a
  run, even inside windows that the local will confirm (section 6.3), so a
  stream whose content is all indexed is scanned at about 150 MB/s per
  scanner thread from DRAM. Measure a stream whose content is all indexed
  against the local's disk rate and the link. Only if the remote's scanner
  bounds the throughput is a skip after a query worth its complexity: it
  needs a record of the offsets that were not probed and a rescan of them
  after a "no", which is what the two-pass design removed (section 10).
