# hashsync

This file is the design specification of `hashsync`. Contributors and AI agents
follow it and update it when the design changes. Section 10 logs rejected and
reverted decisions; section 12 lists the open questions.

Status (2026-09-10): design stage. No code exists yet. The implementation
language is Rust. An earlier version of this document, from the same day,
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

The program is written in Rust. One binary contains both roles: the receiver
and the sender. The receiver starts the sender through `ssh`. The program uses
threads and blocking system calls for parallelism. It has no asynchronous
runtime and no event loop (section 4.6).

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
  blocks (section 5.4). The sender keeps it between runs. At the start of each
  run the receiver sends it in full, or sends only its changes (section 5.6).
- **Delta log**: the receiver's record of the changes of the hash list from one
  export to the next, as segments. A segment lists the removed and the added
  hashes (section 5.6).
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
- **Query**: a QUERY frame. It names the offset of a window and the Blake3
  hash of the window, and asks: "do you have a block with this Blake3 hash?"
  The sender sends at most one query per offset during a run.
- **Answer**: an ANSWER frame: the receiver's "yes" or "no" to the query at an
  offset.
- **Pin**: verified block bytes that the receiver keeps in memory for one query
  offset, from its "yes" until its output passes that offset (section 6.4).
- **Record**: an item of the sender's record list. It describes a piece of the
  output that is scanned but not yet sent: a Literal, a Run, a Query, or a Ref
  (section 6.3).

## 3. One run, step by step

1. The receiver loads `config.toml`, opens the index, and updates it (section
   4). `--no-update` skips the update.
2. The receiver exports the hash list from the index, or loads the cached one
   (section 5.4). When the index changed since the last export, it first
   checkpoints the database, so that the generation it publishes is durable
   (section 4.4), and it records the changes in the delta log (section 5.6).
3. The receiver opens two SSH sessions to the sender host: a **data session**
   and a **control session** (section 6.1).
4. The two sides exchange HELLO frames over the data session. The sender's
   HELLO names the hash list it kept from an earlier run with this index, if
   any. The receiver sends the whole hash list, only the changes since that
   list, or nothing when that list is current (section 5.6). The sender
   applies the changes to its cached list, stores the result for the next
   run, and builds its hash table.
5. The sender starts the user command and reads its stdout.
6. One scanner thread on the sender scans the output (section 6.3). At the
   scan position it tests whether the window is one repeated byte, and
   otherwise looks the weak hash of the window up in its hash table.
   - One repeated byte: the sender records a run and continues after the
     window, without a query.
   - Table miss: it records one literal byte and continues at the next
     offset.
   - Table hit: it computes the Blake3 hash of the window. When the receiver
     said "no" to that hash earlier in this run, it records one literal byte
     and continues at the next offset. Otherwise it sends a query with the
     offset and the hash over the control session, records a Query at that
     offset, and continues after the window.
7. The receiver answers each query. It looks the Blake3 hash up in the index,
   reads a candidate block from disk, verifies its Blake3 hash, and keeps the
   verified bytes in memory, pinned under the offset of the query. It answers
   "yes" if a block verified and "no" otherwise (section 6.4).
8. The sender resolves its records in offset order (section 6.3). A "yes"
   turns the Query into a reference. A "no" deletes the Query and every
   record after it, and the scanner continues at the offset after the start
   of the rejected window, so that a match inside that window is found. The
   records before the first unanswered Query go over the data session:
   literal bytes, one reference per confirmed window, and RLE frames for
   runs.
9. The receiver writes literal bytes, pinned block bytes, and synthesized runs
   to its stdout, in order. A reference names the pin at the receiver's current
   output offset. The receiver releases every pin that its output passes.
10. When the command exits, the sender sends the exit status after the last
    data frame. The receiver exits with the same status.

Design principle: the index, the hash list, its cached copies, and the hash
table are hints. The receiver verifies every spliced block with Blake3 against
bytes it read from its own disk, in memory, before it uses them. Stale index
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
  gets no inode row and no path row. Such files are stat'ed again at every scan
  of a mutable root, which is cheap. The sender's scan works with windows of
  exactly `block_size` bytes, so a shorter tail could never be matched
  (section 10). An indexed inode that shrinks below `block_size` is deleted at
  the next scan.
- Each inode once per scan. When a scan meets an inode a second time (a hard
  link), it only records the new path.
- Constant blocks, whose bytes are all equal, are not stored. The sender sends
  a run of one byte value as RLE frames (section 6.3), so such blocks never
  need a lookup. This also removes the many rows that zero padding would
  produce.
- Blocks with a short period are not stored either: a block whose bytes repeat
  with a period `p` of 1 to 16 bytes (`block[p..] == block[..B - p]`; period 1
  is the constant block above). zstd encodes such a window in a few bytes,
  about the cost of a reference and without a query, so matching it gains
  nothing. It also keeps periodic windows out of the sender's Blake3
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

The matches of one update that `exclude` (below) does not skip are distinct
paths, and no match lies below another: `/a` cannot be a match twice, and
`/a` and `/a/b` cannot both be matches, whatever entries or globs produced
them (`/data/**` produces such pairs). The update rejects them as a
configuration error before it touches the index. The check is lexical, one
set lookup per match and per ancestor directory of a match. It does not
resolve symbolic links or bind mounts, so two roots can still reach one
inode; the hard link rule of section 4.1 handles that. Every path is
therefore the path of at most one root, in the configuration and, because
the index stores only matches of the last update (below), in the index.

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
- When no match of the update produces the identifier, the root is deleted
  (below).
- A file below an immutable root can also be reachable through a mutable root
  (through a hard link or a bind mount). It is then checked like any other
  file of that mutable root. If it changed, its block rows are replaced, and
  every path of the inode sees the new rows.

In both cases, a root that the update does not keep (an immutable root whose
identifier no match produced, a mutable root whose path is not a mutable
match) is deleted with its path rows when the update starts (below), and an
inode with no remaining path rows is deleted together with its block rows
when the update ends (section 4.6). Until then the inode is a hint for the
rest of the update: a tree that the update traverses under another root (a
changed `identifier`, a renamed root, a mutable root that became immutable)
finds its inodes by `(fs_id, handle)` with `(size, ctime)` unchanged and
records paths only, at one `statx` per file and no read. Pruning is
otherwise immediate: a root that vanished, or that is temporarily
unavailable (for example an unmounted filesystem), loses its rows at the
next update and is indexed again when it returns. The index is a cache, so
this costs one extra scan and nothing else.

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

1. It expands every glob, drops the matches that `exclude` skips, checks the
   paths (above), derives the identifier of every immutable match, and groups
   the immutable matches by identifier. Inside a group, the matches of an
   earlier `[[roots]]` entry come first; the order of the matches of one glob
   is not specified. Put the entry of the preferred copy first.
2. It reconciles the `roots` table with the result in one transaction on the
   write connection, before the pool starts (section 4.6):
   - A root that the update does not keep is deleted with its path rows: an
     immutable root whose identifier no group produced, or a mutable root
     whose path is not a mutable match.
   - A group whose identifier names an immutable root of the index keeps that
     root and its rows. When the root's stored path is not one of the group's
     paths, the tree moved or the scanned copy is gone, and the first path of
     the group replaces the stored path.
   - Every other group gets a new row, with its first path as the stored
     path, and so does every mutable match without a row; both with
     `complete = 0`.

   After the transaction every stored path is a match of the update, so the
   stored paths are distinct and none lies below another (above). The root
   tasks of the pool carry the ids of this transaction.
3. The pool traverses every mutable root, every new immutable root, and every
   immutable root that is marked incomplete (above), at its stored path. The
   other paths of a group are never read. A change of the `identifier` option
   therefore rescans the root under its new identifier: step 2 deletes the
   root with the old identifier, and the traversal of the new one finds the
   inodes of the old rows and records paths only (above).

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
  -- path: where the root was last found; distinct across rows, because
  --   the update stores only its matches (section 4.2)
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
CREATE INDEX blocks_by_weak   ON blocks (weak);
CREATE INDEX blocks_by_blake3 ON blocks (blake3);
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
    the end. The reconciliation transaction of the update inserts the row
    with `0` (section 4.2), and the writer sets `1` in the transaction that
    follows the end of the root's walk. An immutable root left at `0` is
    traversed again at the next update (section 4.2); a mutable root is
    traversed at every update anyway.
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
- Two secondary indexes. `blocks_by_weak` serves the export of the distinct
  weak hashes (section 5.4). `blocks_by_blake3` serves the lookup of a query,
  which carries the Blake3 hash only (section 6.4):
  `SELECT inode_id, block_no FROM blocks WHERE blake3 = ? LIMIT 128`. The
  table is `WITHOUT ROWID` with the primary key `(inode_id, block_no)`, and a
  secondary index of such a table holds the primary-key columns, so this
  query reads its candidates from `blocks_by_blake3` alone. Identical blocks
  (for example many copies of one file) legitimately share one hash; the
  lookup uses `LIMIT` and stops at the first candidate that verifies, so
  their number does not matter. The verification of every candidate is
  mandatory (section 6.4): a stored hash alone never justifies a "yes".
- Store `weak` as the `i64` bit pattern of the `u64` value (`weak as i64`).
- Only inodes with at least one stored block have rows (sections 4.1 and 4.5).
- Store the full 32-byte Blake3 hash in the row and in `blocks_by_blake3`.
  Option: store and index only the first 16 bytes; the lookup then uses the
  first 16 bytes of the queried hash, and the verification of every candidate
  still uses the full 32-byte hash from the query (section 12).
- Give the writer's connection a large page cache (`PRAGMA cache_size`, about
  256 MiB). The keys of both secondary indexes are random, so every insert
  touches a random page of each B-tree; once a tree outgrows the cache, every
  insert costs a page read. Reader connections (section 4.6) keep a small
  cache; the OS page cache holds the file for them.
- `--rebuild` and the first build of an index insert the block rows without
  the two secondary indexes and create both after the last block row. An
  index built over the finished table costs about a second per million rows;
  random inserts into a B-tree that has outgrown the cache cost several
  microseconds each. A rebuild writes a new database file next to the old one
  and renames it into place after its last commit, so a crash during the
  rebuild leaves the old index in use and a stray file that the next rebuild
  deletes. Readers of the old file (a concurrent `run`) keep their open
  descriptor and finish on the old index.
- Expected size: about 55 bytes per block row, about 20 bytes per entry of
  `blocks_by_weak`, and about 45 bytes per entry of `blocks_by_blake3`: about
  120 bytes per block, or 0.37 % of the indexed data at 32 KiB blocks. A
  256 GiB tree (8M blocks) needs about 1 GB. The 16-byte option above saves
  about 30 bytes per block.
- `generation` is a 64-bit counter that only grows (the publication contract
  above). It starts at 1 when the tables are created; 0 means "no list" in
  the protocol (section 6.2). The cached hash list file and the segments of
  the delta log record the generations they were built from (section 5.6).
- `index_id` is a random 128-bit value (`rustix::rand::getrandom`), written
  when the tables are created. `--rebuild`, and the rebuild after a parameter
  mismatch above, create a new one. It identifies the index towards sender
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
   block row per 32 KiB. SQLite costs 1 to 2 µs per B-tree insert with cached
   prepared statements when the B-trees are in memory. A block row is one
   insert on a rebuild, where the secondary indexes are created at the end,
   and three inserts during an update, where the two secondary indexes are in
   place (section 4.4). Ten million rows of a rebuild cost 10 to 20 s of one
   core plus about 20 s for the two indexes: less than hashing 256 GiB from
   disk, about the same as hashing it from the page cache on a machine with
   many cores. An update inserts rows only for changed files. Section 4.4
   gives the rules that keep it there (large cache, deferred indexes). If the
   writer ever is the bottleneck, the first lever is the 16-byte Blake3
   (section 12), the second is multi-row `INSERT` statements.

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
  tasks, one per root that the update traverses (section 4.2), with no
  parent: a root task opens its path when a thread takes it, so the number of
  roots (one per store path in a Nix store) does not add open descriptors
  either. ripgrep and fd get a bound of `threads` by queueing paths and
  opening a directory only while they read it, at the price of one full path
  lookup per directory and per file; hashsync keeps the parent's descriptor
  in the task instead and pays no path lookup below a root. The program
  raises the soft `RLIMIT_NOFILE` to the hard limit at start anyway.
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

Deleted inodes: the writer remembers the inode id of every path row it
deletes, in the reconciliation transaction (section 4.2) and at `RootDone`.
After the last `RootDone` it deletes those among them that have no path row
left, with their block rows, in transactions of the usual size; this is the
end of the update. An update that dies before that leaves inodes without a
path; they serve no candidate read (section 6.4), and `hashsync prune`
removes them.

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

It never affects correctness. Without it, only efficiency would depend on the
data. Rolling must operate on the unmixed `P`, because `mix64` is not
rolling-compatible; the sender therefore applies it once per offset, after the
roll and before the probe.

### 5.3 Blake3

The 32-byte Blake3 hash identifies the content of a block (`blake3::hash`,
section 9). The receiver always recomputes it on the bytes it read from disk
before it uses a block. Treat Blake3 equality as byte equality.

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
   header alone. It occurs for an empty index and for a segment whose removed
   or added list is empty (section 5.6). For `N = 1`, `k = 64`: the code is
   one zero bit followed by the 64 bits of the value, and the implementation
   must not shift a `u64` by 64 (`checked_shr`).
4. Prefix a header: `format_version u8`, `W u8`, `k u8`, `N u64`.

Why this shape: sorted uniform values have gaps with about `W - log2(N) + 1.4`
bits of entropy, so the sorted list compresses well while random fingerprints
(as in a filter) do not. The full 64-bit values cost about 2 bytes per key
more than 48-bit prefixes would, and they make false hits rare: the sender's
table compares 32-bit fingerprints that are independent of the bucket index
(section 5.5), which leaves about 3 false hits per GiB scanned against about
400 with 48-bit prefixes (section 10). Sizes for a 256 GiB tree (8M blocks of
32 KiB):

| Transfer format | Size | Cache lines per lookup | False hits per GiB scanned |
|---|---|---|---|
| Sorted 64-bit values, Golomb-Rice gaps (chosen) | 43 MiB | ~1 | ~3 |
| Sorted 48-bit prefixes, Golomb-Rice gaps (rejected) | 27 MiB | ~1 | ~400 |
| Raw sorted 64-bit values | 64 MiB | ~1 | ~3 |
| Binary fuse filter, 32-bit fingerprints (rejected) | 36 MiB | 3 | 0.25 |

A false hit costs one Blake3 of the window, one query of about 45 bytes, and,
when the "no" arrives, a restart of the scan at the next offset, which repeats
the scanning work of one round trip (section 6.3). At about 3 per GiB scanned
this cost is negligible; at about 400 per GiB, with 48-bit prefixes, it was
not (section 10). Decoding 8M values takes tens of milliseconds. The sort of
8M values costs well under a second.

The encoding is canonical: the values are sorted and deduplicated, and `k` is
a function of `N`, so equal sets give equal bytes. The Blake3 hash of the
encoded list therefore identifies the set; section 5.6 uses it as the list
hash. Cache the encoded list next to the database together with
`meta.generation`, `meta.index_id`, and the list hash, and rebuild it only when
the generation changed. Section 5.6 describes the cache file and the delta log
that every rebuild extends.

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
  false hits per GiB scanned. With 48-bit prefixes the two overlapped in about
  7 bits, which left about 25 independent bits and about 400 false hits per
  GiB (section 10).
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

The hash list costs about 43 MiB for 8M blocks: about 18 s on a 20 Mbit/s
link, at every run. Between two runs most of the index stays the same; the
working assumption is that about 90 % of the hashes are shared. So the sender
keeps the list between runs, and the receiver sends only the changes.

What keeps this simple: the list on the sender is always an earlier export of
the receiver's own list. The two sides never reconcile two independent sets
(section 10). The sender names the list it holds; the receiver replays the
changes from its own history. The receiver keeps no state per peer, and the
sender is the only authority on what it holds, so an interrupted run needs no
repair.

Three values identify a list:

- `meta.index_id` (section 4.4). The sender keys its cache by it.
- The generation (`meta.generation`) the list was exported from.
- The list hash: the Blake3 hash of the encoded list (section 5.4). It guards
  against an index restored from a backup, which reuses generation numbers
  for different content, and against a damaged cache file.

Receiver side: the cache and the delta log live next to the database.

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

Storage: the log is smaller than the full list, so the receiver holds at most
two lists per index, whatever the number of peers. In a fleet of N hosts with
one index each that all reach each other, the whole fleet stores N × (N − 1)
cached lists on the senders plus two files per host; a second user of a host has
a second index, which adds its own lists. The copies on the senders are the
cache itself; copies per peer on the receiver are not needed (section 10).

Choice at run time: the sender's HELLO (section 6.2) carries the generation
and the list hash of the list it holds for `index_id`; an all-zero hash means
that it holds none. The receiver looks for the chain of segments that starts at
that generation and ends at the current one, and checks that the `from_hash`
of the first segment equals the sender's hash. If the chain exists, the receiver
sends its segments, one after the other, as HASHDELTA; the pruning rule
guarantees that they are smaller than the full list. Otherwise it sends
HASHLIST. A sender whose generation and hash equal the receiver's gets a
HASHDELTA without segments: the chain is empty, and the sender's list is
current. Every list a sender can hold was the receiver's cache at some time, so
its generation is a segment boundary. The chain is missing when the sender's
list is older than the log or when the receiver's cache was deleted by hand, and
the hash check fails after a restore from a backup; in each case the sender
gets the full list and its cache is current again after the run.

Sender side, in `serve` (section 6.1):

- Cache directory: `$XDG_CACHE_HOME/hashsync/lists/` (fallback
  `~/.cache/hashsync/lists/`) of the account that runs `serve`, mode 0700.
  One file per `index_id`, named by the id in hexadecimal, mode 0600: a
  header (generation, list hash, `block_size`, `weak_hash_id`, `W`) and the
  encoded list. The file holds the newest list that a run of that index
  delivered. When the directory cannot be written, `serve` keeps the list in
  memory only and reports none at the next run.
- One list per index. An `index_id` names one index file (section 4.4), and each
  user of each receiver host has an index of their own (`index_path`, section
  7). A sender that serves two receiver hosts, or two users of one receiver
  host, therefore keeps two lists, and the runs of one index never touch the
  list of another. Two indexes with the same roots cost two files. A rebuilt
  index has a new id; its old list stays until the expiry below removes it.
- On the receiver's HELLO: open the file for `index_id`, read its header, and
  report the generation and the hash when the header's parameters equal
  those of the HELLO; otherwise report none. Keep the descriptor until the
  list is no longer needed: a rename or a deletion by another `serve` (below)
  then cannot take the list away. When the HELLO's `sender_cache` flag is
  false, report none, store nothing, and delete the file of that index if
  there is one.
- On HASHLIST: build the table and store the payload as received.
- On HASHDELTA: decode the cached list, apply the segments in order (one
  linear merge each: drop the removed values, insert the added ones), encode
  the result, and compare its Blake3 hash with the `to_hash` of the last
  segment. Equal: store the new list. Different: log a warning, delete the
  cache file, and continue with the table. This can only cost efficiency
  (section 3); the next run gets a full list. A HASHDELTA without segments
  (the sender's list is current) builds the table from the cached list and
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
about `W - log2(n) + 2` bits: 46 bits at 0.8M values.

| Sent | Size | At 20 Mbit/s |
|---|---|---|
| Full list | 43 MiB | 18 s |
| Delta, 0.8M removed and 0.8M added (10 % churn) | ~9 MiB | 3.7 s |
| Delta, 80k removed and 80k added (1 % churn) | ~0.9 MiB | 0.4 s |

Removals cost as much as additions in this format; section 12 lists the
cheaper encoding as positions in the old list.

`sender_cache` in the configuration (default `true`, per-host override,
section 7) tells the sender through HELLO whether it may keep the list on
disk. The list reveals the hashes of every indexed block (section 6.5).

## 6. The transfer protocol

### 6.1 Transport: two SSH sessions

Why two channels: the data stream can saturate the link with literal bytes. A
query that waits behind queued data waits behind the sshd channel window (about
2 MiB in OpenSSH) plus the kernel socket buffers (several MiB). On a 20 Mbit/s
link this adds seconds of latency to every query. The sender can only scan
`lookahead` bytes ahead while queries are outstanding (section 6.3), so the
throughput is capped at about `lookahead / query_latency`. On a stream with a
high deduplication ratio this cap dominates. A separate TCP connection keeps the
query latency near the network round-trip time.

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
   the delta from stdin, builds the hash table, and then starts the user
   command. It may start the command earlier and buffer its output, up to the
   lookahead size. None of these waits has a timeout (section 3): when the
   control session never arrives, `serve` waits until its stdin reaches end
   of file, which happens when the receiver process ends (section 6.7).
3. `attach` connects to the socket. When the socket does not exist yet
   (`ENOENT`, or `ECONNREFUSED` before `listen`), it sleeps 10 ms and tries
   again, for as long as its stdin has a writer: between two attempts it
   polls stdin for `POLLHUP` and exits when the receiver's side of the session
   is gone. Then it copies its stdin to the socket and the socket to its
   stdout, and shuts down the socket's write side when stdin reaches end of
   file. It has no other logic.
4. The user command gets `/dev/null` as stdin. It inherits stderr, so `ssh`
   forwards it to the receiver's terminal. It runs in a process group of its own
   (`CommandExt::process_group(0)`), so that `serve` can terminate it
   together with its descendants (section 6.7).
5. After the END frame, the receiver closes its stdin pipes of both sessions and
   waits for the two `ssh` processes to exit. `serve` exits when its stdin
   reaches end of file and removes the socket; `attach` exits when the socket
   closes (section 6.7).

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

Every frame is `type u8, length u32 (little endian), payload`. All integers
are little endian; a string or a byte string is `length u32` followed by the
bytes; a list is `count u32` followed by the items. Offsets and lengths of
the output are `u64`; every sum of an offset and a length is checked, and an
overflow is a protocol error (section 6.5). Frame types:

| Session | Direction | Types |
|---|---|---|
| data | receiver to sender | HELLO, HASHLIST, HASHDELTA, SEGMENT, LISTDATA |
| data | sender to receiver | HELLO, LIT, REF, RLE, END |
| control | sender to receiver | HELLO, QUERY |
| control | receiver to sender | HELLO, ANSWER |

Payloads:

- HELLO, receiver to sender (data session): `protocol_version u32`,
  `program_version string`, `block_size u32`, `weak_hash_id string`, `W u8`,
  `index_id [u8; 16]`, `sender_cache u8`, `lookahead u64`,
  `compression_level i32`, `compression_threads u32`, `negative_cache u64`
  (bytes), and `command`, a list of byte strings: the argument vector of the
  user command, `argv[0]` first. The
  sender runs it directly, without a shell, and finds `argv[0]` in `PATH` as
  `execvp` does. A user who wants a shell names one:
  `hashsync run host -- sh -c '...'`.
- HELLO, sender to receiver (data session): `protocol_version u32`,
  `program_version string`, `block_size u32`, `weak_hash_id string`, `W u8`,
  `generation u64`, `list_hash [u8; 32]`: the cached hash list that the
  sender holds for `index_id`, or generation 0 and an all-zero hash when it
  holds none (section 5.6).
- HELLO, both directions (control session): `protocol_version u32`,
  `program_version string`. Both sides send it at once.
- HASHLIST: `generation u64`, `list_hash [u8; 32]`, `length u64`. It announces
  a full list (section 5.4) of `length` encoded bytes, which follow in
  LISTDATA frames. The sender stores the list under `generation` and
  `list_hash` (section 5.6).
- HASHDELTA: `from_generation u64`, `to_generation u64`, `segments u32`. It
  announces a chain of `segments` segments (section 5.6) from the sender's
  list to the receiver's current one. `segments = 0` means that the sender's
  list is current; then the two generations are equal and nothing follows.
- SEGMENT: `from_generation u64`, `to_generation u64`, `from_hash [u8; 32]`,
  `to_hash [u8; 32]`, `removed_length u64`, `added_length u64`. It announces
  one segment: the encoded list of the removed values, then the encoded list
  of the added values, `removed_length + added_length` bytes in LISTDATA
  frames. The first SEGMENT's `from_generation` and `from_hash` equal what
  the sender's HELLO reported, and each SEGMENT starts where the previous one
  ended. The last one's `to_generation` equals the HASHDELTA's.
- LISTDATA: a chunk of the announced bytes, at most 1 MiB per frame. The
  sender counts the bytes: the transfer of a list or of a segment is complete
  when exactly the announced length has arrived. A LISTDATA frame beyond that
  length, or a frame of another type before it, is a protocol error. After a
  full list, the sender checks that the Blake3 hash of the received bytes
  equals `list_hash`; a mismatch is a protocol error.
- LIT: the literal bytes. RLE: `byte u8`, `length u64`. REF: an empty
  payload (`length = 0`). It names the pin at the receiver's current output
  offset (section 6.4).
- END: `status i32`, `error string`. `status` is the exit status of the user
  command, `128 + signal` when a signal ended it, or 127 with an error text
  when the command could not be started. Nothing follows END.
- QUERY: `offset u64`, `blake3 [u8; 32]`. `offset` is the offset of the
  window in the command's output (section 6.3). The sender sends at most one
  QUERY per offset during a run, so the offset identifies the query on both
  sides. Query offsets need not increase: a restart after a "no" can reach an
  earlier offset after a later query was sent.
- ANSWER: `offset u64`, `found u8`, 0 or 1. It carries no hash; the sender
  keeps the hash of every query in its query-state map (section 6.3). Answers
  may arrive in any order. An ANSWER for an offset below the sender's emit
  position is a normal late answer, and the sender discards it. Every other
  ANSWER must name a query that waits for an answer; otherwise it is a
  protocol error.

`protocol_version`, `block_size`, `weak_hash_id`, and `W` must be equal on
both sides, and the program versions must be compatible; any mismatch is a
fatal error before the command starts. On the data session the receiver sends
its HELLO first. The sender reads it, looks up its cache for that index, and
answers with its HELLO. The receiver then sends HASHLIST or HASHDELTA with the
LISTDATA that they announce, after which it sends nothing more on this
session; it closes its end after END (section 6.1). The sender starts the
command only when the whole list or chain has arrived.

Compression. After the HELLO exchange, the sender-to-receiver direction of the
data session is one zstd stream, and all later frames (LIT, REF, RLE, END)
travel inside it. Compression is done by libzstd through the `zstd` crate
(section 9). Rules:

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

The sender runs the command and scans its output. The receiver holds the index
and reconstructs the output (section 6.4). In this section and in section
6.4, an offset is a position in the command's output, counted in bytes from
0; it is never a position inside a network message. `B` means `block_size`.

One scanner thread scans the whole output and makes every decision about it.
It owns the record list and the maps described below. Other threads read the
command's output, send and receive messages, and compress data
(section 6.6); on the receiver, lookup workers answer the queries (section 6.4).
Answers can arrive in any order.

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

**The query-state map.** A map indexed by offset. An entry holds the Blake3
hash of the window at that offset and one of four states:

- `Not_sent`: the hash was computed, but no QUERY was queued or sent.
- `Waiting`: exactly one QUERY was queued or sent, and no answer has arrived.
- `Yes`: that QUERY got a "yes".
- `No`: that QUERY got a "no".

The map is the query-state map and the cache of computed Blake3 hashes at the
same time. It has entries only at offsets where a Blake3 hash was computed.
An entry stays in the map until its offset is less than `e` (E4). The
deletion of records after a "no" does not delete map entries.

A `Waiting` entry stays `Waiting` when its Query record is deleted. When the
scanner reaches that offset again, it appends a new Query record for the
same request; it does not send another QUERY. A queued QUERY is not
cancelled when its record is deleted.

**The negative cache.** An LRU set of Blake3 hashes with a byte limit
(`negative_cache` in the configuration, default 100 MiB, about 1.4M hashes;
the receiver's HELLO carries the size). A "no" inserts its hash, and a "yes"
removes it. Answers for offsets below `e` do not change it (A1). The cache
avoids a query for content that got a "no" at another offset. Its key is the
Blake3 hash, not the weak hash: two windows with the same weak hash and
different content are possible, and a "no" for one must not suppress the
query for the other. The cache therefore saves the query but not the Blake3
computation. Periodic content never reaches this path: the index holds no
short-period blocks (section 4.1), so its windows miss the table without a
Blake3. The size limit of the cache has no effect on the lifetime of the
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
   - Hit: compute the Blake3 hash of the window, store it in the map at `p`
     with state `Not_sent`, and continue with step 9.

9. Use the map entry at `p`:
   - `Yes`: append `Ref { p }` and return to step 1.
   - `Waiting`: append `Query { p }` and return to step 1. Do not send
     another QUERY.
   - `No`: record the one byte at `p` as literal and return to step 1.
   - `Not_sent`: look the stored hash up in the negative cache. On a hit,
     record the one byte at `p` as literal and return to step 1; the entry
     keeps its hash and its state `Not_sent`. Otherwise queue
     `QUERY { offset: p, blake3: hash }`, set the entry to `Waiting`, append
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

**Blake3.** The map avoids a second Blake3 computation at the same offset, also
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
Dense false weak hits at distinct offsets can still cost a Blake3
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

The receiver answers the queries and reconstructs the output. Offsets are
positions in the command's output (section 6.3).

#### Data structures on the receiver

- **The output offset `c`**: the offset of the next byte that the receiver
  writes to its stdout. It starts at 0.
- **The pin map**: a map from query offsets to pinned, verified block bytes.
  A pin holds the `B` bytes of a block that verified against the Blake3 hash
  of the query at that offset. Pins for identical content can share one
  immutable buffer (`Arc`), but each query offset has its own pin. A pin is
  released when `c` passes its offset (D3).
- **One lock** protects `c` and the pin map. Every read and every change of
  either happens under it. Slow disk reads and output writes do not hold it.
- **The verified-buffer cache**: an LRU cache of 256 MiB of verified block
  bytes, keyed by Blake3 hash. A buffer enters it when its last pin is
  released. Blake3 equality is byte equality, so a later query for the same
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

- **L1.** Receive `QUERY { offset: x, blake3: h }`. Read `c` under the lock. If
  `x < c`, the bytes at `x` were already reconstructed: send
  `ANSWER { offset: x, found: 0 }` and create no pin. The sender discards this
  obsolete answer (A1 in section 6.3).

- **L2.** Otherwise look for verified bytes with hash `h` in the verified-buffer
  cache. If there are none, select candidate blocks from the index with
  `SELECT inode_id, block_no FROM blocks WHERE blake3 = ? LIMIT 128` (section
  4.4). The high limit skips over stale rows (deleted or changed files). Check
  the candidates in order until one verifies. For each candidate:
  1. Take one `(root_id, rel_path)` of the inode; skip the candidate when the
     inode has none. Open the root's stored path (section 4.2) with
     `open(path, O_PATH | O_DIRECTORY | O_CLOEXEC)`, then the candidate with
     `openat2(rootfd, rel_path, O_RDONLY | O_NONBLOCK | O_NOCTTY | O_CLOEXEC,
     RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)` (`rustix::fs::openat2`, Linux 5.6
     and newer; fall back to component-wise `openat` with `O_NOFOLLOW`, with
     the same flags on the last component), and close the root descriptor.
  2. Call `statx(fd, "", ...)` with the flags of section 4.5 and check
     `stx_mode`: skip the candidate unless the descriptor is a regular file.
  3. `pread` `B` bytes at `block_no * B` and compute their Blake3 hash. The
     candidate verifies when the read returned exactly `B` bytes and the hash
     equals `h`. A stored hash alone never justifies a "yes".

  A candidate costs one read of `B` bytes and one Blake3 computation, mostly
  from the page cache. Nothing is kept open between candidates, so the run holds
  at most one root descriptor per lookup worker, whatever the number of roots (a
  Nix store has one root per store path). A root descriptor kept across
  candidates would be no more bound to the indexed tree than one opened now:
  both resolve the stored path at some time, and the Blake3 check decides in
  either case. The two opens cost a few microseconds next to the read and the
  hash.

  The path is stale whenever the file changed after the indexing, and it can now
  name a FIFO, a device, or a socket. `O_NONBLOCK` makes the open of a FIFO
  without a writer return at once instead of waiting for one, `O_NOCTTY` keeps a
  terminal device from becoming the controlling terminal, and the type check
  keeps the `pread` off anything that is not a regular file. On a regular file
  `O_NONBLOCK` has no effect. The indexer opens files with the same flags and
  the same check (section 4.5).

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
- Security: the hash list reveals the block hashes of all indexed files to the
  sender host, and with the cache of section 5.6 it stays on the sender's disk
  between runs and for 3 months after the last run (mode 0600, under the cache
  directory of the account that runs `serve`). Use `hashsync` only with hosts
  the user trusts. Set `sender_cache = false` for a host that must not keep the
  list (section 7); the next run to that host deletes the list that an earlier
  run left there.

### 6.6 Threads at run time

Both roles use a few blocking threads and bounded channels, no event loop
(section 4.6). Every hand-off is a channel. Threads are never joined and never
cancelled: the process ends when the run has ended or when a fatal error
occurs (section 6.7).

Sender, `serve`:

- The main thread does the handshake of section 6.1, starts the command and
  the threads below, and then blocks in `read` on its stdin until end of
  file. That is how it learns that the receiver has closed the data session.
  When the data writer has finished by then (the normal end), it removes the
  socket file and exits 0; otherwise the receiver went away early, and it dies
  (section 6.7).
- One reader thread reads the command's stdout pipe into the lookahead
  buffer until end of file, and notifies the scanner after every read. It
  waits while the buffer is full. The pipe is enlarged to 1 MiB with
  `F_SETPIPE_SZ` (`rustix::pipe::fcntl_setpipe_size`), so the reads are
  large. At end of file it sets the end-of-file flag, waits for the child
  (`Child::wait`), and publishes the exit status (section 6.3). The child
  can exit before its stdout reaches end of file (a descendant still holds
  the pipe) or after it (it closed stdout and went on); the run has ended
  only when both have happened, and END is queued only then.
- One scanner thread runs the algorithm of section 6.3. It owns the record
  list, the query-state map, and the negative cache. It queues QUERY messages
  for the control writer and data frames for the data writer, and it waits
  for the notifications of the other threads.
- One data writer thread takes the frames from the bounded data queue in
  order, writes them through the zstd encoder to stdout, and notifies the
  scanner when the queue has room. When the queue runs empty after the
  scanner asked for a flush, it flushes the encoder (section 6.2). After END
  it ends the zstd stream (`Encoder::finish`), flushes and closes stdout,
  and sets the finished flag that the main thread reads.
- One control writer thread writes the QUERY frames of the unbounded query
  queue to the Unix socket, in queue order and promptly: it never waits for
  more command output or for data frames. One control reader thread reads
  ANSWER frames from the socket, puts them on the incoming answer queue, and
  notifies the scanner.
- The child command is a `std::process::Command` with stdin `/dev/null`,
  stdout piped, stderr inherited, and a process group of its own
  (section 6.1).

Receiver, `run`:

- The main thread starts the two `ssh` processes and the threads below, then
  runs the data loop of section 6.4: it reads the data session through the
  streaming decoder, parses frames, and writes to stdout: literals as they
  come, references from the pins, runs from a static buffer. After every
  frame it advances `c` and releases the pins that `c` passed. Blocking
  writes to stdout give backpressure to the whole chain. After END it
  flushes stdout, closes the stdin pipes of both sessions, waits for the two
  `ssh` processes, and exits with the status from END.
- One control reader thread reads QUERY frames and dispatches them to the
  lookup workers over a channel (section 6.4); one control writer thread
  writes the ANSWER frames that the workers put on another channel, promptly
  and in the order the workers produced them.
- The output offset `c`, the pin map, and the verified-buffer cache sit
  behind one mutex; they are touched twice per query and up to twice per
  frame.
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
name and renamed, and the sender stores its list the same way. A run that
fails has nothing to hand over. So every fatal condition ends the process,
and the kernel closes its descriptors, which ends every peer through end of
file and `EPIPE`.

Two exits exist, chosen by the root cause. A condition that is not a fault
of the program (a network failure, a consumer that closed the receiver's
stdout, a signal) ends the process without a core dump. A fault of the
program or of the machine (a bug, an invalid frame, a database error, an
I/O error) aborts and leaves a core dump.

`die(status, message)` is the first kind, one function per role, callable
from any thread. It prints the message to stderr, does the cleanup that the
kernel would not do, and exits with `status` without unwinding
(`std::process::exit`; no destructor needs to run) and without a core dump:
a network error is not a fault of the program, and its core dump would show
nothing. A second caller blocks until the process is gone. The cleanup is:

- sender: kill the command's process group (`kill(-pgid, SIGKILL)`) and remove
  the socket file. A temporary cache file (section 5.6) is left to the expiry
  pass of the next `serve`.
- receiver: kill both `ssh` processes with SIGKILL. The kernel then closes their
  descriptors, the sender's `serve` gets end of file on stdin and dies in
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
block_size     = "32KiB"   # 4 KiB or more; a change discards and rebuilds the index
index_path     = "~/.local/state/hashsync/index.sqlite"
threads        = 0         # indexer threads; 0 = 4 × cores, at least 16, at most 64
lookahead      = "256MiB"  # how far the sender's scanner runs ahead of the output
negative_cache = "100MiB"  # the sender's LRU of Blake3 hashes answered "no" (6.3)
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

The hash list width `W` (64), the multiplier `a`, and `mix64` are protocol
constants, not configuration.

## 8. Command line

- `hashsync update [--rebuild] [-j N]`: update the index, or discard and
  rebuild it. `-j` overrides `threads`.
- `hashsync run [--no-update] [--single-connection] [--ssh CMD] [USER@]HOST -- COMMAND ARGS...`:
  run COMMAND on HOST, write its stdout to the receiver's stdout, and exit with
  the status of COMMAND. HOST is appended to the SSH command unchanged, so
  `user@host` and `ssh_config` aliases work. COMMAND and ARGS travel in the
  HELLO frame (section 6.2) and run on the sender without a shell. `--ssh`
  overrides the configured SSH command for this run. At the end it prints one
  summary line to stderr: output bytes, literal bytes, referenced bytes, run
  bytes, bytes on the wire, and the bytes of the hash list or delta that was
  sent.
- `hashsync serve --token T` and `hashsync attach --token T`: internal. The
  receiver runs them on the sender through the SSH command.
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
| Hash list cache and delta log | own code | | Section 5.6: diff of two sorted lists, segment files, pruning, the sender's cache, merge, and expiry; about 150 lines over both roles. |
| Sender's hash table | own code | | Section 5.5, about 100 lines plus the SIMD bucket scan. |
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
  for readers, and no secondary indexes on `blocks` during a rebuild.

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
multithreaded stream). `stream::read::Decoder` for the receiver. The pure-Rust
`ruzstd` 0.9 has a complete decoder but only an uncompressed and a fastest
level in its encoder, so it is not an option for the sender.

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
filesystem cache of the walker, the query-state map (section 6.3), and the
pin map (section 6.4). Keys that are already hashes (Blake3 in the negative
cache and in the verified-buffer cache) get an identity hasher. The
verified-buffer cache (section 6.4) is `lru::LruCache` or about 60 lines.
The sender's table stays custom (section 5.5).

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
  first block. The two-pass scanner of the same day probed every offset once
  instead; the scanner of 2026-09-13 restarts at `p + 1` after a "no" and
  keeps the hashes and answers of the offsets it visited (section 6.3). The
  negative cache stays mandatory.
- **Negative cache keyed by the weak hash.** Rejected on 2026-09-10. It would
  suppress the query for a different window with the same weak hash, a false
  negative that the weak hash must never cause on its own. The key is the
  Blake3 hash (section 6.3). Since the cache then no longer avoids the Blake3
  computation, short-period blocks were removed from the index the same day
  (section 4.1), so that periodic content misses the table instead.
- **Binary fuse filter (32-bit fingerprints)** for the sender's membership test.
  Rejected on 2026-09-09. It touches 3 cache lines per lookup against about 1
  for the fingerprint table, its random fingerprints cannot be compressed, and
  a filter cannot be patched with the segments of section 5.6. At 36 MiB for
  8M keys it lies between the 48-bit list of the time (27 MiB) and the 64-bit
  list adopted on 2026-09-13 (43 MiB); its size was never the decisive
  argument.
- **48-bit prefixes in the hash list** (`W = 48`). Adopted on 2026-09-09 and
  replaced on 2026-09-13 (user decision) by the full 64-bit values. The
  truncation saved 16 MiB of the 43 MiB list for 8M blocks, but the sender's
  table then had only about 25 independent fingerprint bits and gave about
  400 false hits per GiB scanned. With the scanner of 2026-09-13 every queried
  false hit costs a restart of the scan, not only a query (section 6.3). Full
  values leave about 3 false hits per GiB (section 5.5).
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
  rescans the root, at one `statx` per file (section 4.2).
- **Roots at equal or nested paths.** Rejected on 2026-09-11 (user decision).
  The earlier design let one path be a mutable root and an immutable root at
  the same time, or a match of two entries, and let a root lie below another
  root. Each case indexed a tree under two roots, with two sets of path rows,
  and a mutable root above an immutable root stat'ed the immutable tree at
  every update, which is what immutable roots exist to avoid. The update now
  rejects every such pair of matches (section 4.2). With the rule, the root
  rows are reconciled in one transaction before the traversal, instead of
  being inserted by the writer on first use.
- **`UNIQUE (path)` on the `roots` table**, to enforce the rule above in the
  index. Considered and dropped on 2026-09-11. The reconciliation of
  section 4.2 stores only matches of the update, which are
  distinct, so the constraint would check nothing new, and it would force a
  detour when two immutable roots exchange their paths between updates (a
  nullable `path` set to NULL first, or a delete and a reinsert of the row):
  SQLite checks a UNIQUE constraint at every row it writes and cannot defer
  it to the commit.
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
  and dropped on 2026-09-10. The sender's scan uses windows of exactly
  `block_size` bytes and could never find them, so they would only cost index
  rows. The default block size went from 64 KiB to 32 KiB instead, which halves
  the unmatched tail of every file.
- **Shell-style dot handling in globs** (`*` not matching a leading dot).
  Considered and dropped on 2026-09-10. The program uses the `glob` crate with
  its default `require_literal_leading_dot = false` (Go's `filepath.Glob`
  behaved the same), and `exclude` handles entries such as `/nix/store/.links`.
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
- **Per-peer copies of the hash list on the receiver**, so that the receiver can
  compute the delta for each sender itself. Rejected on 2026-09-10. It costs
  one list per peer on every host (about 2 × N² lists in a fleet of N hosts
  instead of N × (N − 1)), needs a peer identity and a cleanup of stale
  copies, and leaves the receiver unsure of what the sender holds after an
  interrupted transfer. The sender reports the generation and hash of the
  list it holds, and the receiver replays its own delta log (section 5.6). The
  receiver keeps no state per peer.
- **Set reconciliation sketches** (IBLT, rateless IBLT, minisketch), to find the
  difference without history on the receiver. Rejected on 2026-09-10. An IBLT
  costs at least 13 bytes per cell (a count, a 64-bit key sum, and a 32-bit
  check sum) and about 1.3 to 1.5 cells per differing key: 26 to 37 MiB for the
  1.6M differing keys of the 10 % case, against 43 MiB for the full list and
  about 9 MiB for the delta. At any churn it costs three to four times the
  delta, and it needs an estimate of the difference (or a rateless scheme) and
  a fallback for a failed decode. Minisketch is optimal in size but decodes in
  O(d²), for thousands of differences, not millions. The delta log costs about
  6 bytes per changed key, and the receiver already holds the list it diffs
  against.
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
- **Composing a chain of segments into one net delta** before sending it.
  Considered and dropped on 2026-09-10. It only saves bytes when a hash leaves
  and returns within the chain, which is rare for build outputs, and the pruning
  rule already caps a chain below the size of the full list. The receiver sends
  the segments as they are on disk, and the sender applies them in order
  (section 5.6).
- **Expiry of the sender's cache after 30 days of no use, by `mtime`.**
  Replaced on 2026-09-10 (user decision) by 3 months, measured on `ctime`
  (section 5.6). Any program can set `mtime`, and `cp -p` or `rsync -a` keeps
  an old one, so a restored list would look unused; only the kernel sets
  `ctime`. `atime` is out: `noatime` mounts never update it.
- **Last writer wins for the sender's cache file.** Rejected on 2026-09-10. Two
  concurrent runs of one index store in either order, so the older list could
  replace the newer one, and the next run would send the same segments again.
  The store reads the header under the lock and keeps the newer file
  (section 5.6).

- **Optimistic skip after a query, with a rescan of the window after a
  "no".** Replaced on 2026-09-10 (user decision) by a two-pass scanner that
  probed every offset. The rescan was bounded by the next pending window, so
  a real match that started inside a rejected window and ended after the
  next candidate was lost when that candidate was rejected too, and an
  unbounded rescan would have needed a record of which offsets were probed.
  The scanner of 2026-09-13 (section 6.3) skips a queried window again, with
  that record: the query-state map keeps the hash and the answer of every
  offset it visited, a "no" deletes every later record, and the scan
  continues from the byte after the rejected window's start without a stop
  at an old record boundary. No offset stays unexamined, and no window is
  queried twice.
- **Two-pass scanner that probes every offset.** Adopted on 2026-09-10 and
  replaced on 2026-09-13 by the single scanner of section 6.3. Pass 1
  recorded a candidate at every table hit and never skipped a queried
  window; pass 2 chose among overlapping candidates in stream order. It cost
  one probe per byte on matched streams, about 150 MB/s per thread from
  DRAM. The new scanner skips a queried window, so a matched stream costs
  one Blake3 and one query per block, and it restarts after a "no" with the
  query-state map (above).
- **Several scanner threads over independent segments of the stream**
  (`scan_threads`). Rejected on 2026-09-13 with the single scanner. A
  segment's skipped window can overlap the reference that an earlier segment
  chose, which leaves the offsets inside the skipped window unexamined. One
  scanner makes every decision about offsets; the reader, the writers, the
  compressor, and the receiver's lookup workers stay separate threads
  (section 6.6).
- **Query ids.** Replaced on 2026-09-13 by the query offset. The sender sends
  at most one QUERY per offset, so the offset identifies the query, its map
  entry on the sender, and its pin on the receiver, and a REF carries nothing:
  the receiver's output offset names the pin (section 6.4). The QUERY also lost
  its weak hash, which the receiver no longer needs (below).
- **Lookup by weak hash with a Blake3 filter** (`WHERE weak = ? AND
  blake3 = ?` through `blocks_by_weak`). Replaced on 2026-09-13 by the lookup
  through `blocks_by_blake3` (section 4.4), so that a QUERY carries the
  Blake3 hash only. The second index costs about 45 bytes per block;
  `blocks_by_weak` stays for the export of the hash list.
- **A memory limit on the pinned set**, with "no" answers once it was
  reached. Removed on 2026-09-10 (user decision). Memory is not a concern of
  the project; the lookahead and the in-flight stream bound the set in
  practice, and the peer is trusted. The negative cache of the sender keeps
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
  inside is not probed. The scanner of 2026-09-13 reuses a "yes" only at the
  same offset, whose bytes stay pinned until the receiver's output passes it
  (section 6.3). The negative cache grew from 50 MiB to 100 MiB with the
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
  - `sender/`: `serve` (hash list cache, lookahead buffer, scanner, data
    writer, shutdown) and `attach` (relay).
  - `receiver/`: `run` (SSH processes, answering, reconstruction).
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
  and chain from the sender's HELLO, a list hash mismatch on the sender that
  gets a full list at the next run, and an index restored from a backup (same
  generation, different content) that the hash detects; the sender's cache
  (section 5.6): two indexes served from one directory, each with its own
  file, a HASHDELTA without segments that touches the file, the expiry with
  the current time as a parameter (tests cannot set `ctime`), and a store
  that does not replace a newer generation; the hash table
  never gives a false negative; traversal tests on a temporary tree (hard links,
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
  cache hit needs a Blake3 match, and a
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
  current. Cross builds for a sender of another architecture go through
  `pkgsCross`; static builds through `pkgsStatic` (musl). The bundled C sources
  build in both.

## 12. Open questions

- **Stored Blake3 length.** 32 bytes as specified, or 16 bytes in the row and
  in `blocks_by_blake3`, to save about a quarter of the index and of the
  writer's work (section 4.4).
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
  distinct offsets, and each hit costs a Blake3 computation; the query-state
  map avoids only a second computation at the same offset (section 6.3).
  Measure whether this happens on real streams.
- **Throughput on matched streams.** The scanner skips every queried window,
  so a stream whose content is all indexed costs one Blake3 and one query
  per block on the sender, and the throughput is bounded by
  `lookahead / query_latency` (section 6.1) and by the receiver's disk. Measure
  such a stream against the receiver's disk rate and the link.
