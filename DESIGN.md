# hashsync

This document specifies the purpose and design of `hashsync`. Update it when
the design changes. `PLAN.md` orders the implementation.

## 1. Purpose

`hashsync` copies a remote command's stdout to the local host, replacing
data already held in local files with block references. The receiver reads
and verifies those blocks before inserting them into the output. This is
useful for package trees and archive streams that repeat existing content
at different positions and alignments.

One Rust binary contains both roles. The receiver starts the sender through
SSH. Parallelism uses threads and blocking system calls, with no
asynchronous runtime or event loop.

## 2. Terminology

- **Receiver**: the host running `hashsync run`; it holds the index and
  reconstructs the command's output.
- **Sender**: the remote host; it runs the command and scans its output.
- **Block**: `block_size` bytes (default 16 KiB) at an aligned offset in an
  indexed file. Only complete blocks are indexed.
- **Window**: `block_size` bytes at any offset in the command's output.
  All protocol offsets count output bytes from zero.
- **Root**: a configured file or directory to index. An **immutable root**
  promises unchanging content; copies sharing an **identifier** are indexed
  as one group.
- **Index**: the receiver's RocksDB database of block hashes and locations.
- **Weak hash**: a 64-bit rolling hash used to select possible matches.
  A **strong hash** is the configured cryptographic digest used to verify them.
- **Hash list**: the canonical encoded set of indexed weak hashes.
  Its **list ID** is its strong-hash algorithm and full digest.
  A **delta** contains the removed and added values between two lists.
- **Hash table**: the sender's in-memory membership table built from the list.
- **Literal / reference / run**: output sent as bytes, a verified block
  reference, or a repeated byte value and length.
- **Query / answer**: a request for a window's full strong hash at an output
  offset, and the receiver's yes/no response. Each offset is queried at most
  once per run.
- **Pin**: a query offset's verified block, retained by the receiver in
  its own **extent**: a segment and the block's offset in it.
- **Segment**: an open, unlinked file in the **tmp_data directory** of a
  host. It holds the extents of pins, or the bytes of a spool.
- **Spool**: a pipe whose bytes wait in segments, so that the producer of
  the bytes waits for their consumer only at the limit of the spool. The
  sender spools the command's stdout, and the receiver spools the data
  session.
- **Record**: a sender-side description of scanned but un-emitted output:
  a Literal, Run, Query, or Ref.

## 3. One run, step by step

1. The receiver resolves configured roots and updates the index.
   `--no-update` skips the update and acquires read-only access. Index access
   is held throughout the run.
2. It exports the hash list from one database snapshot, opens its
   tmp_data directory and makes sure that it can create segments in it,
   and starts data and control SSH sessions to the sender. It reads the
   data session through a spool.
3. After both sides exchange protocol versions, HELLO negotiates parameters
   and cached lists. The receiver durably publishes its target list, then
   sends a full list, a delta, or confirmation that the sender's list is
   current. The sender verifies it, builds its table, durably publishes it
   when caching is enabled, and acknowledges the accepted target.
4. The sender starts the command, whose stdout goes through a spool: the
   command waits for the transfer only when `sender_spool_limit` bytes
   wait in the spool. The scanner sends literals for misses, runs for
   repeated bytes, and queries for possible block matches.
5. The receiver reads and verifies candidates, appending their bytes to a
   segment before answering yes. The sender emits references for
   yes answers and rescans rejected windows from the next byte. Output is
   always emitted in order; when it has passed the pins of a segment that
   receives no more blocks, the receiver closes that segment, which frees
   its storage.
6. The receiver reconstructs stdout from its spool of the data session.
   While stdout is blocked, the receiver keeps reading the session until
   `receiver_spool_limit` bytes wait in the spool, so the transfer goes
   on. After all command stdout is processed, and the command's exit
   status is available, the sender sends END. The receiver validates
   stream completion and flushes stdout before sending END_ACK. When the
   sender needs neither session any more, it sends DONE, and the receiver
   closes both session stdin pipes. Both SSH sessions must exit
   successfully before the receiver returns the command's status.

**Verification is authoritative.** The index, hash lists, caches, and weak
hash table are hints. Verify candidates before writing pins and verify the
exact buffer read back from a pin before writing it to stdout. Both checks
use the query's full strong hash. Stale files and collisions can reduce
deduplication, never change the reconstructed output.

**No timeouts.** Waits end when the expected event occurs or a peer,
descriptor, or process goes away. Slow systems must not fail because of a
deadline; a stalled process can be inspected with a debugger. Fatal errors
end the process (section 6.8). Retry intervals and cache age bounds are not
operation deadlines.

**No artificial size limits.** Valid input never fails because of an
arbitrary size constant. Only the width of the field that carries a length
or count, such as a frame's `u32` length, and the resources of the host
bound it. Readers grow buffers only as bytes arrive, so a corrupt length
costs no more memory than the bytes that really follow it. Chunk sizes and
budgets that tune performance, without making valid input fail, are not
limits.

**Retry interrupted calls.** Every system call that fails with `EINTR` is
retried indefinitely; an interruption is never a failure. The exception is
`close`: Linux releases the descriptor even when `close` reports `EINTR`,
so a retry could close an unrelated, reused descriptor. Close descriptors
once and ignore the result. Retrying `connect` is safe for the Unix
sockets this program uses: an interrupted `connect` leaves them unconnected.
A TCP connection attempt instead continues after `EINTR` and a retried
`connect` reports `EALREADY`, so TCP would need another exception; this
program opens no TCP sockets (section 6.1).

## 4. The receiver's index

### 4.1 What gets indexed

- Only regular files, without following symlinks. Directory identities
  repeated during traversal, including bind-mount cycles and aliases, end
  the update with `die`, regardless of root or immutable grouping. Root
  overlap detection is best-effort (section 4.5).
- Hash each inode once per scan; additional hard links add paths, including
  links across different roots. Sharing a regular-file inode is supported
  and is not itself a traversal conflict.
- Index only complete blocks. Files smaller than `block_size` have no inode
  or path rows, but mutable scans check them again. Remove indexed inodes
  that shrink below that size.
- Skip blocks with any period from 1 to 16, including constant blocks:
  period `p` means `block[i] == block[i-p]` for all `p <= i < block_size`.
  Partial final repetitions count. RLE handles constant runs and zstd
  handles short patterns without needing references. The bound is an
  indexing policy, not a protocol constant.
- Skip reads of blocks entirely inside sparse-file holes. Read blocks
  partially overlapping holes normally.
- For files at least `block_size`, retain inode and path rows and mark the
  inode complete after successful processing, even if all complete blocks
  are skipped as repetitive or holes, with or without a partial tail.
  This avoids rehashing unchanged files on every update.

### 4.2 Roots and change detection

Roots come from `config.toml`; a glob match can be a directory or a single
regular file. Root and exclusion globs use `gix-glob`'s raw `wildmatch`
interface in pathname mode, with case-sensitive matching and leading dots
included. `*`, `?`, and bracket classes cannot match `/`; `**` spans path
components. Matching operates on bytes, including `?` and bracket ranges.
Both patterns and filenames may contain non-UTF-8 bytes; never reject them
for that reason or convert them lossily. Section 7 defines how to spell
those bytes in configuration. Thus `/nix/store/*` also matches
`/nix/store/.links` and non-UTF-8 names.

`gix-glob` supplies matching, not filesystem expansion. Expand root patterns
with descriptor-relative directory walking using the no-symlink rules of
section 4.5. Detect directory cycles during recursive expansion, before
index traversal begins.

Resolve roots for every update, run (including `--no-update`), and prune.
After exclusions, reject duplicate or nested absolute paths before changing
the index. This check is lexical; mount-alias overlap is detected when
directory identities repeat during actual traversal (section 4.5). Hard
links across otherwise separate roots are allowed.

Mutable roots are traversed on every update. Reuse complete inode rows when
`(size, ctime)` from the open file matches; otherwise rehash. Use kernel
`ctime`, since programs can restore `mtime`. Reconstruct all paths under
traversed roots, including paths of unchanged files.

Immutable roots are grouped by identifier, with one root row and one set of
relative paths from one scanned copy. A root row with an identifier means
the group was completely indexed. Reuse it without traversal while the
configuration still names that identifier. A root row without an identifier
is a mutable root or an unfinished traversal; configuration supplies the
intended kind and identifier.

Copies sharing an identifier promise equivalent content. Directory copies
must be distinct directories. A change to a copy's root-directory filesystem
identity or inode handle does not invalidate the completed group or trigger
a rescan.

All current locations of an immutable group live in memory. The database
stores one representative path, while candidate reads search every current
copy (section 6.4). With `--no-update`, map completed rows by identifier and
rows without an identifier by stored path and current configuration;
unmatched rows have no usable location. A single-file root has an empty
relative path.

Per-root options:

| Option | Meaning |
|---|---|
| `exclude` | Globs using the same match options as roots. Patterns beginning with `/` match absolute paths, including root matches; others match basenames. Excluded entries and descendants are skipped. |
| `one_filesystem` | Skip an entry and descendants when its mount ID differs from its parent's. |
| `identifier = "path"` | Default for immutable roots: the absolute matched path. |
| `identifier = "name"` | Use the last path component, allowing copies under different parent directories. |
| `identifier = { strip_prefix = "/mnt/disk" }` | Remove that prefix from the absolute match; a missing prefix is a configuration error. |
| `identifier = { literal = "photos-2019" }` | A fixed identity that survives moving a tree. |

Identifiers are nonempty byte strings with the same comparison rules for all
forms. They are allowed only on immutable roots.

All configured copies sharing an identifier must have the same indexing
policy: identical `exclude` pattern sets, ignoring order and duplicates,
and equal `one_filesystem` values after applying defaults. Compare the
patterns after byte-escape decoding and glob quoting, without attempting
to prove glob equivalence.
Conflicting policies are a configuration error, including for completed
groups and `--no-update` runs; reject them before changing the index.
The user is responsible for making absolute exclusions select corresponding
content in every copy; pattern equality does not establish that equivalence.

Policy changes do not invalidate completed immutable groups automatically.
After changing exclusions or `one_filesystem`, the user must run
`hashsync update --rebuild` to apply the new policy to those groups.

Before scanning, reconcile roots. Paths and blocks to remove can be many,
so their removal commits in bounded transactions; an interruption leaves
work that the next reconciliation finishes. The root and inode rows then
change in one transaction:

1. In one transaction, invalidate every completed immutable root that is
   no longer configured or that references an incomplete inode: remove its
   identifier.
2. In bounded transactions, on one thread per core, remove the paths and
   blocks of incomplete inodes. If a root keeps its identifier, also
   remove the paths of every root without an identifier. Then end those
   threads and join them.
3. In one transaction, remove the rows of those roots and of incomplete
   inodes. If no root keeps its identifier, the same transaction removes
   every path, with one range deletion in `paths` and one in
   `inode_paths`. Preserve complete inodes, including orphans, for reuse.
   Retain completed immutable groups still configured. Keep their
   representative path if still present, otherwise choose any current
   location. Create rows without identifiers for roots that need
   scanning. Two root rows never share a path, also when roots exchange
   paths.

A range deletion removes all entries of a key range with one change, and
later scans skip the range quickly. The key of a `paths` row starts with
the root ID, so the rows of the roots without an identifier between two
roots with an identifier form one key range. The key of an `inode_paths`
entry starts with the inode ID instead: the entries of one root are
spread among the entries of all other roots, and a range deletion cannot
remove them. So in step 2, a scan of the `paths` rows finds their
entries. Each bounded transaction removes the entries that it found one
by one, and their rows with range deletions; one range deletion can cover
the rows of many roots.

Schedule scan tasks for every mutable root and one chosen location per
unfinished immutable group. Completed groups and unchosen copies have no
scan tasks. After all scan tasks finish, publish immutable identifiers only
for groups whose traversals and dependent inode and path results succeeded
and committed. A failed chosen copy leaves the traversal unfinished; other
copies remain available for candidate reads. After processing all results,
remove every orphaned inode and its blocks.

The same reconciliation recovers interrupted updates. Renames and identifier
changes can reuse complete inodes with unchanged identity, size, and ctime.
An inode reached through both mutable and immutable roots is checked by the
mutable scan; interrupted or abandoned rehashing invalidates its immutable
roots before deleting dependent paths.

The user is responsible for immutability: a completed root can omit files
added later. Copies may have different inode identities; unequal content
fails verification. `prune` hashes no files and removes a path only when it
is absent from every current copy. Inaccessibility does not prove absence.
When a root is no longer configured or is confirmed absent from every copy,
remove its immutable completion marker, then its paths, then its root row.
The marker must not survive the removal of any path, so a root that later
returns is scanned again. An existing root with no indexed paths is still
valid.

### 4.3 Identity of filesystems and inodes

Device numbers can change across boots and inode numbers can be reused.
Use persistent identities:

- Filesystem: UUID from `FS_IOC_GETFSUUID`, falling back on unsupported
  filesystems to tagged `f_fsid` from `fstatfs`. An unstable fallback
  identity causes scanned files to be rehashed after reboot.
- Inode: the opaque handle from `name_to_handle_at`, which distinguishes
  inode generations and btrfs subvolumes. On `EOPNOTSUPP`, fall back to a
  tagged inode number.

`(filesystem ID, handle)` is the identity used both by the index and
scan-time visited sets. The index gives each inode an integer ID, which the
entries of its paths and blocks use.

Distinct filesystems retaining identical filesystem IDs and inode handles,
such as block-level clones mounted together, are unsupported. They may be
mistaken for aliases or hard links. Full content verification still protects
reconstructed output.

Cache filesystem resolution per scan by
`(stx_mnt_id, stx_dev_major, stx_dev_minor)`, resolving again when this
triple differs from the parent's. A mount ID alone misses btrfs subvolume
changes and is not a persistent identity. Obtain metadata and handles from
the open descriptor (section 4.5).

`statx`, `fstatfs`, and `name_to_handle_at` work with the initial `O_PATH`
descriptor. `FS_IOC_GETFSUUID` requires a readable descriptor: reuse one for
the same filesystem, or reopen the verified object as in section 4.5 when
resolving a filesystem for the first time. Do not issue ioctls on `O_PATH`
descriptors.

### 4.4 Storage

The index is one RocksDB database: a directory, defaulting to
`$XDG_STATE_HOME/hashsync/default.db`. Each table below is a column
family, which holds entries (keys with values) in key order. A **row** is
one entry of `roots`, `inodes`, `paths`, or `blocks`. `identities`,
`inode_paths`, and `strong` hold one more entry for each row of `inodes`,
`paths`, and `blocks`, keyed for another lookup; the transaction that
changes a row changes that entry too.

| Column family | Key | Value |
|---|---|---|
| `default` | metadata name | metadata value |
| `roots` | root ID | identifier length (`u32`), identifier, path |
| `inodes` | inode ID | state (`u8`), size, ctime, identity |
| `identities` | identity | inode ID |
| `paths` | root ID, relative path | inode ID |
| `inode_paths` | inode ID, root ID, relative path | empty |
| `blocks` | inode ID, block number | weak hash, strong-hash prefix |
| `strong` | strong-hash prefix, inode ID, block number | empty |

- Integers are unsigned 64-bit and big endian, so the byte order of keys
  is their numeric order. The ctime is the bit pattern of a signed 64-bit
  value, in nanoseconds. A strong-hash prefix is the first 16 bytes of the
  digest.
- An **identity** is the length of the filesystem ID as one byte, the
  filesystem ID, and the inode handle (section 4.3).
- Metadata names are `schema_version`, `block_size`, `weak_hash_id`,
  `strong_hash_id`, and `index_id`.
- A root row has an identifier only for a completely indexed immutable
  group; length 0 means no identifier, because identifiers are never
  empty. The path is one representative location; other copies live only
  in memory.
- An inode's state is 0 while processing is incomplete, and its size is
  then 0. Otherwise the state is 1 and the size is `stx_size`.
- IDs start at 1. A new row gets an ID larger than all IDs in use.
  Removal takes every entry that refers to a root or inode away before its
  row, so an ID used again finds no old entries.

A **transaction** is one atomic RocksDB write batch. It goes to RocksDB's
write-ahead log without sync. Power loss may discard recent transactions;
recovery then restores the state after an earlier one, which is acceptable
for an index of hints. Hash lists identify their own content and are
persisted independently (section 5.6). Reads see committed transactions
only, with one exception. The threads of an update commit at the same
time, and RocksDB inserts each transaction into its memtables on its own
(unordered writes); only the write-ahead log keeps their order. A read can
then see part of a transaction that another thread is still inserting, so
no thread of an update reads rows that another thread writes
(section 4.6). The end of an update writes the memtables to the database
files, so that read-only processes have no write-ahead log to replay. It
writes one column family after the other, so that the last write also
removes the old log. Then it waits for the compactions that RocksDB has
scheduled: closing the database would cancel them (section 10).

All column families use LZ4 compression and share one 256 MiB block cache,
a hyper clock cache. `identities` and `inodes` have bloom filters for point
lookups. `strong` has a 16-byte prefix extractor and prefix bloom filters,
so the lookup of an absent prefix skips most files. A column family
flushes its memtable at 64 MiB, RocksDB's default. When the memtables of
all column families together use 256 MiB, RocksDB also flushes the oldest
one. One flush and one compaction run at a time. A compaction from level 0
splits into subcompactions on up to half the cores, one thread each.
RocksDB keeps all files of the database open, so a process raises its soft
descriptor limit to the hard limit before it opens the index.

**Process access.** A lock on the file `hashsync.lock` in the index
directory excludes other processes:

- A modifying process (`update`, `prune`, or normal `run`) creates the
  directory and the lock file if necessary, opens the lock file
  read/write, and takes an exclusive lock before it opens the database
  read/write.
- A read-only process (`run --no-update`, `stats`) opens the lock file
  read-only and takes a shared lock, then opens the database read-only. It
  sees the database as it was when it opened it. Read-only processes can
  coexist but exclude a modifying process. Without the lock file, there is
  no index.
- The lock is an open file description lock (`F_OFD_SETLKW`) on the whole
  file. Unlike a classic POSIX lock, which belongs to the process, it stays
  when another descriptor of the file closes. The descriptor is
  close-on-exec, so no child keeps the lock.
- Wait indefinitely on lock contention, and retry interrupted calls.
- All threads of a process share one database handle. Reads of a
  modifying process see its latest committed transaction.
- Keep the lock until the database handle has closed, and keep both until
  index use ends. A normal `run` keeps the update's database open through
  the transfer, so there is no gap in exclusion between update and use.
- RocksDB's own `LOCK` file keeps only two read/write opens apart, and the
  second open fails at once instead of waiting. The lock file comes first,
  and its lock goes after RocksDB's classic lock of `LOCK`, also when a
  process exits with the database open: the kernel releases classic locks
  while it closes the descriptors, and an open file description lock only
  when it releases the description, after that. So that failure cannot
  occur.

**Representation and recovery.**

- Store schema version, block size, weak-hash parameters, and strong-hash
  algorithm. Incompatibility requires an in-place rebuild. Read-only runs
  report that an update is required for a missing or incompatible index.
- Paths and identifiers are byte strings, preserving non-UTF-8 bytes.
  Stored sizes are compared only for equality. The state, not a size
  sentinel, marks an incomplete inode.
- Before replacing blocks, mark the inode incomplete and store the ctime
  observed before hashing; this may commit before any old blocks are
  deleted. Remove old blocks in bounded transactions before inserting new
  ones. Keep the inode incomplete while streaming new blocks across bounded
  batches and periodic commits; only successful completion stores the size,
  together with any final block rows. Recovery follows section 4.2.
- Successful processing of a file at least `block_size` retains the inode
  and path rows even when no block rows were inserted. Store the size
  normally so the inode is complete and reusable.
- `strong` supplies candidates using 16-byte prefixes; only a full digest
  comparison verifies a candidate. No column family orders blocks by weak
  hash; the export sorts the weak hashes in memory (section 5.4).
- Rebuild under process-exclusive access: one transaction removes every
  entry of every column family, with one range deletion each, and stores
  new metadata. Then populate in batches. Never replace the database
  directory or its files.
- `index_id` is a random 128-bit value created with the schema and renewed
  on rebuild. It partitions caches, not list content; there is no index
  generation counter.

### 4.5 Traversal and hashing

Resolve filesystem entries relative to open directory descriptors, without
following symlinks in any pathname component. Initially open each entry with
`O_PATH|O_NOFOLLOW|O_CLOEXEC`. Obtain its type and metadata with `statx` using
the descriptor, an empty pathname, and `AT_EMPTY_PATH`, checking which fields
were returned. Skip symlinks and other types besides regular files and
directories without marking the root unsuccessful or opening them for I/O.
Directory-entry types are only hints. Obtain regular-file and directory
handles with `name_to_handle_at` on that descriptor, also using an empty
pathname and `AT_EMPTY_PATH`.

When a regular file needs hashing or a candidate read, reopen
`/proc/self/fd/%d`, where `%d` is its still-open `O_PATH` descriptor, with
`O_RDONLY|O_CLOEXEC`. For directory listing, reopen the same way with
`O_RDONLY|O_DIRECTORY|O_CLOEXEC`. Both reopen the verified object even if it
has been renamed or unlinked; never reopen its original pathname or the text
returned by `readlink`. Keep the `O_PATH` descriptor alive until reopening
finishes, then retain the readable descriptor through processing. An `O_PATH`
directory descriptor can itself serve as the parent for relative opens.

Reopening deliberately follows the process's own procfs magic link; do not
apply `O_NOFOLLOW` or `RESOLVE_NO_SYMLINKS` to that step. It requires access
to `/proc/self/fd`. All descriptors are close-on-exec. Path replacement cannot
switch the object between checking and reading it.

Open and identify a root when its scan task starts, and use that open object
for traversal under the descriptor rules above. There is no separate root
registration pass or close-and-reopen step to compare identities. An identity
observed during root expansion imposes no continuity requirement on the scan.

Before traversing any directory, including a root, atomically insert its
`(fs_id, handle)` into the shared directory visited set. A duplicate calls
`die`, reporting both paths, regardless of root or immutable grouping.
Retain entries until all scan tasks finish. A directory task then queues
relevant children with its shared parent descriptor, each name once: a
listing can return a name twice when the directory changes meanwhile, and
the index keeps one inode per path.

Claim regular-file identities in a separate visited set. Repeated files add
paths to the first task's result regardless of root, preserving all dependent
roots and hard-link paths. Claims include small files and files without
useful blocks. Single-file roots use the same file handling; no separate
directory-entry bookkeeping is required to detect overlap beyond the lexical
root-path checks.

Overlap detection is best-effort. Root expansion has its own cycle detection
and does not populate the scan's visited sets. Completed immutable groups
and unused alternate copies are neither opened nor scheduled solely for
overlap detection. Mount aliases involving skipped roots or copies can go
undetected. `--no-update` runs perform no index traversal.

Every detected directory conflict calls `die` without a core dump; do not
skip the conflicting branch or publish new immutable completion markers.
Interrupted-update recovery handles any work committed before detection.

For a new or changed regular file, obtain the readable descriptor as above
and read with `pread`, accumulating short reads, and hash useful complete
blocks until the file ends, even past the size observed before hashing. A
file that changes while it is hashed is not abandoned: the stored size and
ctime are the ones observed before hashing, so the next update rehashes it.
Unlink or rename alone does not invalidate an open descriptor.

Attempt `SEEK_DATA`/`SEEK_HOLE` for every file being hashed. Allocation
counts cannot prove absence of holes. Skip only blocks wholly inside a hole,
read partial overlaps in full, and treat `SEEK_DATA` returning `ENXIO` as
no further data before EOF. Unsupported seeking (`EINVAL` or `EOPNOTSUPP`)
falls back to reading without hole discovery. Use separate SEQUENTIAL and
NOREUSE cache-advice calls as optional hints.

Batch reads to reduce system calls. From the block size `B`, compute with
integer division a batch size `B × max(1, 1 MiB / B)` and a hole window
`B × max(1, 128 KiB / B)`. Both are whole numbers of blocks; a block larger
than 1 MiB is read in one `pread`. Plan each read up to the batch size,
stopping at the next hole boundary rounded up to a block boundary. When the
planned read is shorter than the hole window, look ahead with `SEEK_DATA`
at the hole's start. If the next data starts within one hole window of the
read's start, extend the read to the hole window and do not inspect the
holes inside it; hole blocks read this way count as read and are filtered
as periodic. Otherwise keep the shorter read, reuse the lookahead answer
for the following seek, and skip the hole. After a read that crosses holes,
resume hole discovery at the next unread block; do not reuse extent
information for positions already read. This policy is a heuristic: an
extended window can contain a large hole that was not examined. Accept this
bounded read amplification instead of inspecting every extent.

If `SEEK_HOLE` returns `ENXIO` or an offset no greater than the preceding
data offset or the current unread offset, discard the stale extent and
lookahead, perform one batch-size read at the current unread offset, and
resume seeking at the next unread block. This makes progress after
concurrent truncation, hole punching, or inconsistent seek results while
keeping seeking enabled. Other seek errors abandon the file.

Opening, metadata, directory-listing, and reading errors are local: log
them, abandon the entry, and continue. An entry that vanishes before it is
opened is skipped silently. Entries that are not indexed are normal and do
not change the exit status. A directory failure also skips unread
descendants. Mark the root unsuccessful so an immutable traversal is retried.
If inode work has begun, its owner removes the partial result (section 4.6).
Supported identity and hole-seeking fallbacks are not failures. Database
failures and invariant violations are fatal. The traversal conflicts above
also end the process instead of abandoning a single entry.

### 4.6 I/O architecture

One dedicated work-stealing `rayon` pool walks and hashes. Tasks are files
or directories; local depth-first scheduling keeps pending work and open
parent descriptors small, while stealing distributes subtrees. Completion
means all tasks have finished, including work spawned by running tasks.

Tasks retain a shared parent fd and open their entry only when scheduled.
Roots are opened when their tasks start. Raise the soft descriptor limit to
the hard limit at startup. Workers own reusable buffers; the database
handle and the directory and file visited sets are shared. The default
thread count is `4 × cores`, clamped to 16–64, with a configuration
override.

The hashing stage takes an open regular file and produces block rows and
a result. The scan tasks write their index changes themselves. Each thread
of the pool collects the changes of its tasks in a transaction of its own,
which commits when it reaches the bound of a transaction, before the
thread publishes an inode row that other links depend on, and after all
tasks have finished. A large file never requires a whole-file transaction.
Only the owner of a file changes its inode, so the owner reads committed
rows of its inode without a commit first, except when Abandon removes the
rows that its own transaction wrote. No thread reads rows that another
thread writes during the scan, as unordered writes require (section 4.4).

The file visited set holds the state of each claimed file, keyed by
`(filesystem ID, handle)`:

| State | Meaning |
|---|---|
| Pending | The owner has no result yet. The paths of other links wait in the entry. |
| Indexed | The owner's inode row has committed. Other links add their paths to it. |
| Dropped | The file has no rows: it is too small to index, or its processing failed. Other links add nothing. |

The first claim of a file makes its task the owner, with a Dropped entry
for a file too small to index and a Pending entry otherwise. The owner
looks up its identity once and uses the row for one of these results:

| Result | Changes |
|---|---|
| Unchanged | Reuse the complete row, add the owner's path, and publish Indexed. |
| Hashed | Mark the inode incomplete with the observed ctime, remove old blocks in bounded transactions, and add bounded batches of block number, weak hash, and strong prefix rows. A new identity gets a new row, with an ID from a counter that all threads share. Store the successful size and the owner's path, marking the inode complete even if no block rows were added, then commit and publish Indexed. |
| Small | Remove the inode's existing rows with its paths and blocks. |
| Abandon | Commit, invalidate referring immutable roots, remove the paths and partial inode data, and publish Dropped. |

An index without inode rows, such as a new or rebuilt one, has no
identities, so its owners look up none. When the owner publishes Indexed,
it adds the paths that waited; a later link adds its own path. A path
therefore commits only after its inode row. A link that finds Pending
waits even if an old complete row exists. Every claimed file ends Indexed
or Dropped. A failure makes the roots of all waiting and later paths
unsuccessful. Do not retry an abandoned inode through another hard link
during the same update.

Tasks mark failed roots, which stay remembered until the scan ends. Once
all scan tasks finish and every thread's transaction has committed,
publish immutable identifiers for successful groups whose dependent inode
and path results have committed. This prevents a group from becoming
reusable before a later task detects a directory overlap. Then remove all
orphaned inodes and blocks in bounded transactions, including orphans left
by earlier interrupted cleanup. The threads of the pool share this work by
ranges of inode IDs. A thread reads only entries whose keys start with an
inode ID in its ranges, so no thread reads rows that another thread
writes. Then end the threads of the pool and join them.

## 5. Hashes, hash list, and hash table

### 5.1 Weak hash: mixed polynomial rolling hash modulo 2^64

The definition is shared by both roles. All arithmetic is unsigned 64-bit
modulo 2^64.

- `a` is a fixed odd multiplier with `a ≡ 5 (mod 8)`, for example
  `0x9E3779B97F4A7C15`.
- For a window `w[0..B)`, `P = sum(w[i] * a^(B-1-i))`.
  Compute directly, as dot products of the bytes with the powers of `a`,
  in steps that Horner's rule combines.
- The weak hash is `mix64(P)`.
- Roll one byte with `P' = a*P - Tout[out] + in`, where
  `Tout[b] = b * a^B` is precomputed for the block size.

Rolling uses the unmixed `P`; indexing, list encoding, and table lookup use
the mixed value. `weak_hash_id` names the algorithm, multiplier, and mixer
in the index, in cached lists, and in HELLO. A change to any of them makes
a new weak hash with a new ID, not a new protocol version (section 11).

### 5.2 `mix64`: the bit mixer

The splitmix64 finaliser distributes the polynomial hash's structured bits
for bucket selection, fingerprints, and gap coding. It is a bijection, so it
does not introduce or remove polynomial-hash collisions.

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

This function is part of the weak hash that `weak_hash_id` names, and it
is not rolling-compatible.

### 5.3 Strong hashes

Initially support `blake3` (default) and `sha256`, both with 32-byte outputs.
Each supported algorithm has one fixed output length `S >= 16` bytes.
Configuration cannot truncate digests or choose an arbitrary XOF length.
HELLO names the receiver's algorithm, and the sender must support it;
changing it rebuilds the index.

Treat full strong-hash equality as byte equality. Only the index stores
16-byte prefixes. Queries, content caches, and list IDs use full digests.
Candidate verification hashes the exact buffer written to the pin's extent.
Reconstruction verifies the buffer read from that extent before using it
for output.

### 5.4 The hash list

Read the weak hashes of all block rows from one database snapshot. The first
keys of the files of `blocks` split it into parts, which one thread per core
reads in parallel. Sort the values in memory as unsigned 64-bit values, with
the same threads. Then end those threads and join them. Remove duplicates
and encode canonically:

1. Width `W = 64`; no hash bits are dropped.
2. For sorted distinct values, gaps are `g_0 = v_0` and
   `g_i = v_i - v_(i-1) - 1`.
3. Encode each gap using Golomb-Rice coding: `g >> k` one bits, a zero,
   then the low `k` bits. Pack least-significant-bit first and pad the last
   byte with zeros.
4. Prefix `N u64`. The encoder and the decoder derive
   `k = W - ceil(log2(N))` from it. For `N = 0`, use `k = 0` and the
   header alone. For `N = 1`, `k = 64`: a zero bit followed by all
   64 value bits, without performing an invalid 64-bit shift.

Equal sets must produce equal bytes. The list has no format version of its
own: the protocol version covers it on the wire, and the cache wrapper's
format covers it on disk (section 5.6). The list ID is the strong algorithm
and full digest of those bytes. Export on every run; there is no
generation-based shortcut.

### 5.5 The sender's hash table

Use a custom open-addressing table of 32-bit fingerprints in a flat array:

- For `N = 0`, allocate no buckets and return a miss for every lookup,
  without computing a bucket address. The remaining rules apply to `N > 0`.
- A bucket contains 16 slots, occupying one cache line.
  `buckets = ceil(N / (16 * 0.7))`.
- Map `v` to `high64(v * buckets)` using a 128-bit product.
- The fingerprint is `v`'s low 32 bits, replacing zero with one.
  Zero slots are empty.
- Insert in the first empty slot, continuing to the next bucket when full.
  Lookup stops at the fingerprint or an empty slot and continues only
  through full buckets. It may return false positives, never false negatives.

Full 64-bit list values keep bucket selection and fingerprints sufficiently
independent. The table favours one cache-line access per probe; SIMD bucket
scans and batched independent probes can accelerate it without changing
membership semantics.

### 5.6 The hash list cache and direct deltas

Full lists are immutable and named by content. Account and index IDs
partition cache directories; they do not identify list content. Compute a
direct delta from any available base, without generations, delta logs, or
segment chains.

**Identity and storage.**

Each UNIX account has one random 128-bit `account_id` in
`$XDG_CONFIG_HOME/hashsync/account-id` (fallback
`~/.config/hashsync/account-id`), independent of `--config`. Initialize it
durably with atomic no-replace publication; concurrent initializers use the
winner's ID. Never overwrite an existing ID. HELLO exchanges account IDs
and the receiver's index ID; `serve` does not open its own index.

Let `A` be the receiver account, `B` the sender account, and `I` the
receiver index, encoded as fixed-width lowercase hex. Under
`$XDG_CACHE_HOME/hashsync/` (fallback `~/.cache/hashsync/`):

| Role | Cache path |
|---|---|
| Receiver | `local-hashlists/B-I/<algorithm>-<digest>.cache` |
| Sender | `remote-hashlists/A-I/<algorithm>-<digest>.cache` |

Directories are private (0700), files 0600. A file is a versioned wrapper,
a magic string and a format byte, followed by the list bytes; the file name
holds the list ID. The lists in one peer/index directory all come from one
index, so they share its block size, weak hash, and strong hash: a change
to any of them rebuilds the index with a new index ID (section 4.4). Files
therefore store none of these parameters. Hash only the canonical list
bytes, excluding the wrapper and peer identities.

**Cache failures and validation.**

For both roles, cache directory and file operation failures end the run with
`die` without a core dump. This includes creation, directory enumeration,
opening, metadata reads, reading, writing, fsync, rename, renewal, and
removal. Apply the same rule to publication, invalid-file removal, cleanup
for `sender_cache = false`, and expiry, including abandoned temporaries.
Retry interrupted calls as specified in section 3.

An absent cached list is a cache miss. Missing cache directories during
lookup or cleanup, and files that disappear during lookup or are already
gone when removed, are normal absence. Failures to create or publish a
required file remain fatal.

Unexpected cached file contents are also a cache miss: remove the file and
continue without it. This includes malformed or truncated wrappers or list
encodings, unsupported formats, and list bytes whose digest does not match
the list ID in the file name. Failure to remove the file is fatal unless it
is already gone. An I/O error while reading a file is fatal; successfully
reading invalid contents is not. Validation failures in lists received over
the protocol follow the exchange rules below.

**Exchange.**

1. Export and hash the target from one database snapshot. Once the sender's
   account is known, durably publish the target in its receiver-side peer
   directory before sending list data. Retain bytes or an open fd.
   Failure to persist the receiver's target ends setup.
2. The sender advertises its valid list with greatest `ctime`, breaking
   ties by filename byte order, or explicit absence. Verify files before use
   and retain the selected base.
3. Equal IDs use HASHKEEP. Otherwise the receiver searches all its
   `local-hashlists/*/*.cache` directories for the advertised base and
   verifies it. A missing or damaged base uses HASHLIST. With a base, merge
   sorted sets and encode removed and added values as lists; use HASHDELTA
   only when smaller than the full transfer.
4. The sender decodes or applies the delta, re-encodes canonically, and
   verifies the full target digest. A bad delta requests HASHLIST once via
   HASHFULL; a bad full list is a protocol error. HASHKEEP uses the retained,
   verified base.
5. Build the table and durably publish the verified target on the sender.
   Reply CACHE_RESULT only after publication.
   Then remove the advertised old file from that peer/index directory if
   its ID differs.
6. On CACHE_RESULT, the receiver may remove that exchange's old list from
   its peer/index directory, unless it equals the target. On lost
   acknowledgement, retain it until expiry. Never remove matching files
   from other peer/index directories as part of this cleanup.

`sender_cache = false` advertises no base, stores nothing, removes earlier
files in that receiver/index directory, and sends the same CACHE_RESULT
after verification, table construction, and removal.
The receiver still durably publishes its target. The negotiated setting
determines whether the sender publishes; no storage-success flag is needed.

**Publication, concurrency, and expiry.**

Publish complete files using a unique hidden temporary, file fsync, rename
to the content-derived name, and directory fsync. Make new directory
hierarchies durable too. Republish even an existing target ID rather than
trusting its filename; HASHKEEP can copy its retained verified bytes.
Published files are never edited in place.

No cache locks or mutable current-list pointer are needed. Retained fds or
bytes survive replacement and unlink. Tolerate disappearance during directory
scans and fall back to a full transfer. Cleanup racing with replacement or
removal of a published file may lose a future cache hit; it cannot invalidate
retained bytes. A concurrent cleanup can remove a temporary before its rename.
Treat this as normal absence: the effect is the same as removal of the
published file just after publication. Other failures to publish a required
target remain fatal. With sender caching enabled, acknowledgement promises
completed publication, not immunity from later deletion.

Touch verified files through their open descriptors after successful use.
The sender renews its published target; the receiver renews its target and any
base used after receiving CACHE_RESULT. Use `ctime` as the renewal marker.
After the exchange, both sides expire files in both local and remote cache
trees older than `hashlist_max_age` (default 90 days, sent in HELLO).
Apply the same age bound to abandoned hidden temporaries. The cache failure
rules above apply to renewal and expiry too; equal base and target IDs never
justify deleting the current target.

Retained versions and lost acknowledgements may leave extra files. Database
restore or rebuild needs no cache repair or publication checkpoint because
lists identify their own content.

## 6. The transfer protocol

### 6.1 Transport: two SSH sessions

Separate data and control SSH connections keep queries from waiting behind
literal output. SSH multiplexing or forwarding over one TCP connection
cannot provide that separation. If the user's ControlMaster configuration
shares a connection, the program still works but loses the latency benefit.

Use the configured SSH subprocess, preserving its agent, host configuration,
ProxyJump, and host-key handling. Shell-split `ssh` from configuration,
a host override, or `--ssh`; the first word is the executable and the rest
are its options. For both sessions, insert `-o ClearAllForwardings=yes` and
`-o RemoteCommand=none` before the configured options, then append
`-T -x -o Compression=no`, the unchanged HOST, and the remote command string.
The leading options take precedence under SSH's first-value rule; the final
`-T` and `-x` disable PTYs and X11 forwarding even if configured options
requested them. Local, remote, and dynamic port forwardings are cleared.
An explicit compression option in the configured command still takes
precedence. SSH options must keep the sessions in the foreground, preserve
their stdin and stdout, and execute the specified remote command.

SSH executes its remote command through the remote login shell, which must
support POSIX shell quoting for this bootstrap. Treat `sender_command` as
one executable name or path, not a shell fragment. Construct a remote
`exec` command by single-quoting each argument, replacing embedded single
quotes with `'\''`, and preserving all other bytes, including non-UTF-8
bytes. Passing separate local argv elements to SSH does not quote them for
the remote shell. The sender needs a compatible `hashsync` in PATH or at
`sender_command`.

The receiver generates token `T` and starts both sessions together, with
the signal mask that the receiver started with (section 6.8):

- Data: `SSH HOST hashsync serve --token T`.
- Control: `SSH HOST hashsync attach --token T`.

`serve` first listens on `$XDG_RUNTIME_DIR/hashsync/T.sock` (fallback: a
private directory under `/tmp`), mode 0600. It exchanges protocol versions
and data HELLOs, accepts `attach`, and completes the list exchange and
CACHE_RESULT before starting the command. Before it waits for a reply, it
flushes the frames that the receiver must read to reply.

`attach` retries a not-yet-listening socket while its stdin remains open,
with no deadline. It relays stdin to the socket and the socket to stdout,
half-closing the socket on stdin EOF. Socket EOF or a peer-closed socket
error (`EPIPE` or `ECONNRESET`) on a read ends the relay from the socket.
`attach` then waits until stdin EOF ends the relay from stdin, joins that
thread, and exits zero. After a peer-closed error on a write, the relay
from stdin writes nothing more to the socket, but reads stdin to EOF and
discards the bytes. This prevents a deadlock: the receiver closes the
control session's stdin only after its control writer has ended
(section 6.8), and that writer could otherwise block on a full pipe.
Either direction may observe closure first. The receiver decides from its
completion state whether a closure was expected; other relay failures end
the relay unsuccessfully.
Rendezvous waits must also observe receiver disappearance (section 6.8).

The command's argv travels in HELLO, not on the SSH command line. Start it
without a shell, with stdin from `/dev/null`, piped stdout and stderr, its
own process group for cleanup, and the signal mask that `serve` started
with (section 6.8).

A dedicated sender thread copies command stderr to `serve`'s stderr with
bounded buffering and prompt writes. SSH forwards these bytes through its
existing stderr channel; both local SSH subprocesses inherit the receiver's
stderr. The relay finishes only after command stderr reaches
EOF and all copied bytes have been written and flushed to SSH. It runs
independently of stdout scanning and control traffic. Stderr is outside the
hashsync protocol and its compression streams. Successful sender exit requires
relay completion; its coordination with stdout completion is an implementation
decision (section 6.8). The receiver waits for both SSH exits before returning.

### 6.2 Protocol version and frames

Each side sends `protocol_version u32`, little endian, as the first bytes of
each session. It does not wait for the peer's version before it sends its
own. It reads the peer's version before it reads any frame, and it ends the
run with `die` if the versions are different; the message names both
versions. The sender starts the command only after it has checked the
receiver's version on both sessions. The remote commands of section 6.1 and
this version exchange never change. Everything after the exchange belongs to
one protocol version (section 11).

Every frame is `type u8, length u32, payload`. Integers are little endian.
Strings and byte strings have a `u32` byte length; lists have a `u32` item
count. Output offsets and lengths are `u64`; overflow is a protocol error.
Readers grow their buffers only as bytes arrive, so no length has a bound
other than the size of its field (section 3).

`S` is the negotiated strong algorithm's full digest length. Every digest of
the run uses that algorithm, including the list IDs of the target and the
base, so a list ID travels as its digest alone, `[u8; S]`. An optional
digest begins with a 0/1 presence byte. A run has one target and at most
one base; the HELLOs name them, and later frames refer to them without
repeating them.

| Session | Direction | Types |
|---|---|---|
| data | receiver to sender | HELLO, HASHLIST, HASHDELTA, HASHKEEP, END_ACK |
| data | sender to receiver | HELLO, HASHFULL, CACHE_RESULT, LIT, REF, RLE, END, DONE |
| control | sender to receiver | QUERY |
| control | receiver to sender | ANSWER |

Payloads:

- HELLO, receiver to sender: `block_size u32`, `weak_hash_id string`,
  `strong_hash_id string`, `target [u8; S]`, `account_id [u8; 16]`,
  `index_id [u8; 16]`, `sender_cache u8`, `hashlist_max_age u64` (seconds),
  `compression_level i32`, `compression_long u8`, `compression_threads u32`,
  `probe_threads u32`, `lookahead u64`, `negative_cache u64` (bytes),
  `sender_spool_limit u64` (bytes; `2^64 − 1` for `unlimited`), and
  `command`, a list of argv byte strings.
  Find argv[0] in PATH and run directly; a shell requires explicit
  `sh -c`.
- HELLO, sender to receiver: `account_id [u8; 16]` and an optional
  `base [u8; S]`.
- HASHLIST: `length u64`. That many encoded bytes of the target follow the
  frame directly.
- HASHDELTA: `removed_length u64, added_length u64`. The removed and added
  encoded lists follow the frame directly. They change the advertised base
  into the target.
- HASHKEEP: empty. The advertised base is the target; no data follows.
- List bytes after HASHLIST and HASHDELTA are not frames. Enforce exact
  lengths and canonical encoding, including ordering and padding.
- HASHFULL: empty. Request a full list once after a delta digest mismatch,
  before command output. The response is HASHLIST. Failed results are
  never published or used for the final table.
- CACHE_RESULT: empty. A single acknowledgement after target verification,
  table construction, and durable sender publication when caching is
  enabled. With `sender_cache = false`, send it after removing earlier
  files for this receiver/index. Cache operation failures are fatal as
  specified in section 5.6. Repeated acknowledgements are protocol errors.
- LIT: literal bytes.
- REF: empty; use the pin at the current output offset.
- RLE: `byte u8, length u64`.
- END: `status i32, error string`. Status is the command's exit status,
  `128 + signal`, or 127 plus error text if startup failed. Of the
  sender-to-receiver data frames, only DONE follows.
- END_ACK: empty. The receiver has validated END and compression completion
  and flushed reconstructed stdout. Send exactly once on the data session;
  it is the receiver's last data-session frame. Section 6.8 defines shutdown.
- DONE: empty. The completion conditions of section 6.8 hold, and the
  sender needs neither session any more, so the receiver can close both
  session stdin pipes. The sender sends it once, after it has received
  END_ACK. It directly follows the compression stream, outside it. Nothing
  follows DONE: a byte after it is a protocol error.
- QUERY: `offset u64, strong [u8; S]`. At most one per offset; offsets may
  go backwards after rescanning.
- ANSWER: `offset u64, found u8` (0 or 1). Answers may arrive out of order.

The receiver's HELLO sets the block size, weak hash, and strong hash of the
run. The sender uses them, or it ends the run with `die` before command
startup if it does not support them. It checks the strong hash before it
reads the target, whose length depends on it. Its HELLO does not repeat
them.

The data handshake is the version exchange, receiver HELLO, sender HELLO,
durable receiver publication, then HASHLIST/HASHDELTA/HASHKEEP and its data.
The sender replies with CACHE_RESULT, possibly after one HASHFULL fallback.
The receiver handles the result and cache cleanup before entering its output
loop. Its only remaining data-session frame is END_ACK; it keeps both session
stdin pipes open until DONE (section 6.8).

After HELLO, all sender-to-receiver data-session frames through END share one
zstd stream, unless compression is disabled. This includes HASHFULL and
CACHE_RESULT. DONE follows the stream, outside it.
Output latency does not matter, only bandwidth. So the sender flushes the
stream only before it waits for a reply, after HASHFULL, and finishes the
stream after END. Other frames can stay in the compressor and the transport
buffer until more output fills them or the stream ends. No wait depends on
them: queries and answers use the control session. Compression settings are
in section 7.

After the protocol version, each direction of the control session is also
one zstd stream at the same level, unless compression is disabled. The
frames are small, so the stream uses no long-distance matching and no
worker threads. A control writer flushes its stream each time its queue
becomes empty, because the scanner waits for answers. When a slow link
makes the writer wait, more frames share each flush and compress better.
No frame ends the stream: the closure of the session ends it (section 6.8).

The receiver-to-sender direction of the data session is not compressed.
Almost all its bytes are the hash list, and zstd cannot make the list
smaller (section 10).

### 6.3 The sender's scanner

One scanner owns all output decisions, records, and query states. Other
threads perform I/O and compression, and probe threads probe the weak table
for the scanner. Let `B = block_size`.

**State and invariants.**

- `e`: next output byte to emit; `p`: next byte to scan; `r`: end of
  command bytes read. Initially all are zero, with `e <= p <= r`.
- The lookahead buffer retains immutable bytes `[e, r)`, including scanned
  bytes that a rejection may require again. Its capacity is `lookahead + B`;
  the output reader, which takes command stdout from its spool
  (section 6.5), pauses when the buffer is full. Scanning starts a step
  only while `p - e < lookahead`; a full-window step may advance `p` by
  `B`.
- Ordered records cover exactly `[e, p)`, without gaps or overlaps:
  `Literal { offset, length }`, `Run { offset, length, byte }`,
  `Query { offset }`, and `Ref { offset }`. Queries and references each
  cover `B` bytes.
- The query-state map is keyed by offset and stores the full digest with
  state Not_sent, Waiting, Yes, or No. Keep entries until their offsets
  fall below `e`, even when their records are discarded.
- A byte-bounded negative LRU cache holds full strong hashes answered no
  (default 128 MiB). It suppresses queries for the same content at other
  offsets, but does not avoid hashing a new window.
- Command stdout EOF and availability of the command's exit status are
  separate conditions; both are required to finish scanning and send END.

A Query blocks emission of itself and every following record. References
require a yes for their exact offset; equal hashes at different offsets do
not share permission to emit REF. A last Literal or Run can grow, and its
ready prefix can be emitted before it stops growing.

**Scanning.**

Each iteration processes available answers, resolves Query records in
offset order, and emits ready records before scanning more input.

With a full window at `p`:

1. If all `B` bytes are equal, append or extend a Run and advance by `B`.
2. Otherwise reuse the query-state entry at `p`, if present. If absent,
   probe the weak table. A miss records one literal byte; a hit computes
   the full strong hash and creates a Not_sent entry.
3. Apply the entry's state:

   | State | Action |
   |---|---|
   | Yes | Append Ref and advance by `B`. |
   | Waiting | Append Query and advance by `B`, without another request. |
   | No | Record one literal byte. |
   | Not_sent | On a negative-cache hit, record one literal byte and retain the entry. Otherwise queue QUERY, set Waiting, append Query, and advance by `B` before processing another answer. |

Recording a literal advances `p` by its length. Coalesce adjacent Literals
and same-byte Runs; never create empty records.
Update the rolling hash and constant-window check incrementally on one-byte
advances. Reinitialize both after a jump or restart so neither describes an
old window.

At EOF, a tail shorter than `B` extends the last Run for as long as bytes
match; the remainder is literal. If that Run was already emitted, its short
continuation is literal. Earlier unresolved queries can still force this
tail to be scanned again.

**Probe threads.**

A weak-table probe depends only on the bytes of its window. When the
scanner looks for the next hit or constant window, it can split a long
stretch of windows into consecutive parts and probe them on
`probe_threads` threads at once. Every part but the first initializes the
rolling hash and the constant-window check from the bytes of its first
window. The first hit or constant window in offset order ends the stretch,
and parts after it may stop early, so the result equals checking each
window in order. After each weak hit, new or known, the scanner scans at
most a short stretch on its own thread before it processes answers again.
With more than one probe thread, each following stretch without a hit or
a constant window doubles the next one, up to a bound per thread. A
constant window neither doubles nor shortens the next stretch: the stretch
that it ends did not use its full length, and the window makes no query.
Short gaps between matches thus stay on one thread, and a long run of
misses soon uses all probe threads. HELLO's `probe_threads` sets the
thread count; 0 selects half the sender's cores, at least one.

**Answers and rescanning.**

Discard answers below `e` without changing the negative cache: they may
describe obsolete requests rejected without a lookup. Every other answer
must name a Waiting entry. Set it to Yes or No, removing or adding its hash
in the negative cache respectively. Do not mutate records while ingesting
answers.

Then resolve the first Query in record order:

- Waiting stops resolution.
- Yes replaces that Query with Ref; continue to the next Query.
- No deletes that Query and every later record, including confirmed Refs,
  Runs, and Literals. Retain buffer bytes and query states, record the byte
  at the rejected offset `x` as literal, and resume scanning at `x + 1`.

Discarding the entire suffix allows matches crossing former record
boundaries. Requests belonging to discarded records remain live; encountering
a Waiting offset again reuses its request. A late answer can update the map
even while no record currently refers to it.

**Emission and waiting.**

Emit from the front until blocked by a Query or a full data queue:

- Ref becomes an empty REF and advances `e` by `B`.
- Run becomes RLE for its known length and advances `e` by that length.
  A new Run starts at `B` bytes and need not wait to grow.
- Literal emits up to `B` bytes per LIT. Emit a short Literal when another
  record follows or at EOF; otherwise let it grow.

Advance `e` only after the frame is accepted by the data writer. LIT owns
a copy before those buffer bytes are released. Remove emitted record portions
and map entries below `e`.

Outgoing data is bounded. Queueing it must not block the scanner from
processing answers. Outgoing QUERY messages use an unbounded, nonblocking
queue and remain ordered even when offsets go backwards. Discarded records
can leave obsolete requests; lookahead is not a bound on all outstanding
queries or receiver pins.

When progress requires input, answers, or data-queue space, wait on
notifications for those events, continuing to prioritize answers.
Backpressure propagates from the data writer through the lookahead buffer
to the spool of command stdout. It reaches the command only when
`sender_spool_limit` bytes wait in the spool (section 6.5). Pin
allocation and release are entirely receiver-local.

Finish only after stdout EOF, `p = r`, and no Query records remain. Drain all
records so `e = p = r`. END can be queued once the exit status is available;
coordinate shutdown according to section 6.8. Flush queued data before waiting
for further completion conditions. Obsolete unanswered requests do not delay
completion.

### 6.4 The receiver's answering and reconstruction

The receiver keeps pin payloads in extents of segments: open, unlinked
files in its tmp_data directory (section 6.5). Its map from query offsets
contains only extents and full strong hashes. Extents are independent of
command-output offsets. Each pin has its own extent, including pins with
identical content.

**Pin storage.**

Let `B = block_size`. The pins are one user of segments, with extents of
`B` bytes (section 6.5). The receiver writes the verified block of each
pin to its own extent. It releases the extent when nothing uses it any
more: the output has passed its pin, or its lookup worker has found its
write obsolete, or has finished it in Completing.

Setup, before the SSH sessions start: open the tmp_data directory, which
makes sure that segments can be created in it (section 6.5). An unusable
tmp_data directory thus ends the run before command startup. The QUERY
procedure below allocates extents with steps A1 to A3 of section 6.5. The
QUERY procedure and the advance of `c` release them with steps R1 to R3.
At the end, closing the pins closes all their open segments (section
6.8).

The receiver tracks output offset `c`, its pin map, and the open segments
of the pins.
A bounded 256 MiB LRU cache of verified buffers, keyed by full strong
hash, can serve candidate lookups. Populate it on verified reads; its
eviction is independent of pin lifetimes. Pins never retain those buffers.
One lock protects this state and the completion state. Source reads,
segment I/O, and stdout writes occur outside the lock; the file creation
of step A1 is the one exception, and it occurs once per segment. In-memory
payload storage is limited to the cache and worker/reconstruction buffers;
pin metadata and queued requests still scale with outstanding work. The
filesystem manages reclaimable page-cache pages and backing storage for
pin payloads.

A pool of 4–8 lookup workers, which share the database handle, answers
queries out of order. The receiver does not reject a match merely because
many pins exist, and pin release requires no protocol message.
Queue all answers nonblockingly under the shared lock, checking the
completion state before publication. Completing suppresses new work and
answers, including no answers from obsolete lookups.

For `QUERY { offset: x, strong: h }`:

1. Under the lock, check `c`. If `x < c`, answer no without a pin.
   Otherwise try the verified-buffer cache.
2. If uncached, select blocks matching `h[0..16]` through `strong`. Each
   path of a block's inode gives one candidate; a
   block whose inode has no path is one candidate too. Try candidates in
   any order. Repeated location/offset pairs need only one read.
3. Try at most `candidate_attempts` candidates (default 128, positive).
   Try each candidate at every current location of its root until one
   verifies: all copies of an immutable group are searched, and together
   they use one attempt. A candidate without a usable location, or whose
   reads all fail, also uses one; stale rows must not evade the budget.
4. Open a single-file root directly. Otherwise open its root directory and
   resolve the relative path beneath it without symlinks (`openat2` with
   `RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS`). Use the `O_PATH` classification
   and procfs reopening rules of section 4.5: check that the opened object
   is a regular file before obtaining its readable descriptor. Close
   descriptors before trying the next location.
5. Read exactly `B` bytes at `block_no * B`, accumulating short reads.
   Verify the full digest of that buffer. Errors, early EOF, and mismatches
   try the next location or candidate; filesystem failures are misses, not
   fatal. Alternate copies need not share the indexed inode identity.
6. If nothing verified, answer no. Otherwise recheck `c` under the lock. If
   `x < c`, answer no without an extent. If still needed, allocate an
   extent under the lock (section 6.5, steps A1 to A3).
7. Outside the lock, write the verified buffer's `B` bytes to the extent
   with `pwrite`, accumulating short writes. Segments are open without
   `O_APPEND`; concurrent workers write only their assigned,
   nonoverlapping extents. Finish every write before publishing the pin or
   answering yes.
8. Reacquire the lock and recheck `x < c`. If now obsolete, leave the extent
   unpublished, release it (section 6.5, steps R1 to R3), and answer no.
   Otherwise publish `{ extent, strong: h }` at `x` and queue yes
   nonblockingly under the lock. Release the worker's buffer. The
   Completing state prevents new extents or pin publication; late workers
   discard their results, and release the extents that they wrote.

A yes is binding because the receiver owns an independent copy of the
verified bytes in a segment. A later source-file change cannot affect
REF. Output may pass `x` and release its extent before the answer is sent;
the sender then discards that obsolete answer.

The receiver reads the data session through a spool whose limit is
`receiver_spool_limit` (section 6.5). The thread of the spool reads the
session as its bytes arrive, so until that many bytes wait in the spool,
a blocked stdout stops only reconstruction. The sender keeps sending,
scanning, and querying, the lookups keep writing pins, and the spool and
the pins hold the output until stdout takes it. The limit counts bytes of
the data session, which is compressed; it does not bound the pins.

Process data frames in order at `c`, as reconstruction reads them from the
spool:

- LIT writes its payload; RLE writes the repeated byte. Accept any positive
  RLE length, though the sender emits runs of at least `B`.
- REF obtains the pin at `c` under the lock. Outside it, read exactly `B`
  bytes from the pin's extent with `pread`, accumulating short reads,
  and verify the full digest against the pin's stored hash before writing
  that buffer to stdout. Keep the extent in use until the write completes
  and `c` advances. A missing pin is a protocol error; segment EOF, read
  errors, or a digest mismatch end the run unsuccessfully without emitting
  that block or attempting to reread the source file.
- After each frame's output is written, advance `c` under the lock. In
  bounded batches, detach pins whose query offsets are below it, including
  pins covered by literals or runs, and release their extents (section
  6.5, steps R1 to R3). After each batch, release the lock and close the
  files of the segments that the batch removed from the open segments. An
  unpublished write that becomes obsolete is released by its lookup worker
  after that write finishes.
- END carries the command status into shutdown. Validate compression
  completion and flush stdout before END_ACK (section 6.8). EOF without
  END or before the compressed stream is complete is failure.

The shared lock makes pin publication and output advancement atomic relative
to each other: a lookup either publishes before the offset passes and has its
extent released, or observes the obsolete offset and releases its extent.
No extent is released while its write or REF read is still using it, so no
segment is closed while one of its extents is in use.

### 6.5 Segments and spools

Both roles keep temporary data in **segments**: open, unlinked files in
the **tmp_data directory** of their host. The receiver keeps its pins in
segments (section 6.4), and both roles keep the bytes of their spools in
segments. Closing an unlinked file frees its storage and writes nothing
to disk. There is no hole punching: any filesystem that can hold files
works as a tmp_data directory.

**Segments.**

Each **user** of segments, the pins or one spool, has its own open
segments and its own extent size: `B` for the pins, 1 MiB for a spool.
One segment of a user at a time receives the new extents, until it is
full. The user closes a segment that receives no more extents as soon as
nothing uses its extents. The more segments of a user are open, the more
bytes a segment holds before it is full, so the open files stay few.

Data structures:

- **tmp_data directory**: on the receiver, `tmp_data_dir`, defaulting to
  `$XDG_CACHE_HOME/hashsync/tmp-data/` (fallback
  `~/.cache/hashsync/tmp-data/`). The sender always uses that default, in
  its own environment. To move it on a sender host, set the
  `sender_command` of that host to a script that runs
  `export XDG_CACHE_HOME=DIR; exec hashsync "$@"`; the sender's list cache
  (section 5.6) moves with it. Use a private directory (0700). A role
  creates it if it does not exist, opens it, and keeps it open through
  the run, to create the segments in it. When it opens the directory, it
  creates a segment and closes it at once, to make sure that segments can
  be created in it. The receiver opens the directory before the SSH
  sessions start (section 6.4), and the sender opens it when it has
  accepted HELLO, before the list exchange. So an unusable directory ends
  the run before command startup. Choose a disk-backed directory to move
  the data out of RAM; a tmpfs directory would store it in memory or swap
  instead. Avoid NFS: its client writes the data of a file to the server
  before it closes the file, so every byte of the segments would go to
  the server (section 10).
- **Segment**: one open, unlinked file in the tmp_data directory, with its
  size and its count of extents in use. The size is the bytes of the
  extents appended to the segment. The file is read/write (0600) and
  close-on-exec. Prefer `O_TMPFILE`; otherwise create a unique file
  securely and unlink it before writing any data.
- **Open segments**: each user keeps its open segments in creation order.
  At most one of them, the newest, is the **current segment**: it
  receives the new extents. The others are **sealed**: they receive no
  more extents.
- **Extent**: a range of bytes in one segment, as long as the extent size
  of its user. A segment and an offset in it identify the extent. The
  extents of a segment follow each other without gaps, alignment, or
  padding, in allocation order, and are never used again.
- **Count of extents in use**: the extents of a segment that are allocated
  and not yet **released**. The user releases an extent when nothing uses
  it any more.

The current segment is **full** when it holds at least `16 MiB × 2^n`
bytes, where `n` is the number of sealed segments of its user that are
open at that moment. The limit is not fixed when the segment is created:
`n` decreases when sealed segments close. Seal the current segment as
soon as it is full: evaluate the rule after each extent allocation, and
after each release that closes a sealed segment. Thus the current segment
is never full, and after a seal there is no current segment until the
next allocation. The 16 MiB are a constant, not a configuration key.

A user starts without open segments: its first allocation creates its
first segment (step A1). Allocate an extent, under the lock of the user:

- **A1.** If there is no current segment, create one.
- **A2.** The extent starts at the size of the current segment. Check the
  file-offset arithmetic first: `pwrite` takes a signed offset. Add the
  extent size to the size of the segment and 1 to its count of extents in
  use.
- **A3.** If the current segment is now full, seal it. It stays open: the
  new extent is in use.

Release an extent, under the lock of the user:

- **R1.** Subtract 1 from the count of extents in use of the extent's
  segment.
- **R2.** If the count is 0 and the segment is sealed, remove it from the
  open segments, and close its file after the lock is released, on the
  thread that released the extent. The current segment stays open when
  its count is 0: it is not full.
- **R3.** If step R2 removed a segment, `n` is now smaller. If the current
  segment is now full, seal it. If its count of extents in use is 0, also
  remove it from the open segments, and close its file as in step R2.

End: closing a user closes all its open segments (section 6.8). Process
exit closes them too. No fsync or crash recovery is required for these
transient files; closing a segment frees all its storage.

Failures to create the tmp_data directory or a segment, and failures of
segment writes or reads, including exhaustion of space, descriptors, or
file-size limits, end the run with `die` without a core dump.

Properties, for each user:

- **Descriptors**, for any release order. A segment is sealed at a size of
  at least `16 MiB × 2^m`, where `m` is the number of sealed segments that
  are open at that moment. All of them are older, and afterwards they only
  close. Thus the i-th oldest sealed open segment holds at least
  `16 MiB × 2^(i−1)`, and `k` sealed open segments hold at least
  `16 MiB × (2^k − 1)`. So there are at most `log2(R / 16 MiB + 1) + 1`
  open segments, where `R` is the bytes that they hold: 13 for 64 GiB and
  17 for 1 TiB. There are never more than 40: a segment holds less than
  2^63 bytes, because `pwrite` takes a signed offset. The tmp_data
  directory uses one more descriptor.
- **Retention**, when releases follow the allocation order, as in a spool.
  No segment gets an extent after its size passes the bytes of the extents
  in use at that moment plus 32 MiB. Only the oldest open segment holds
  released extents. If it is sealed, their bytes stay allocated until its
  last extent is released. If it is the current segment, no sealed segment
  is open, and it is not full, so it holds less than 16 MiB.
- **File creations**: a sealed segment holds at least 16 MiB, so there is
  at most one file creation per 16 MiB of extents.
- **Accepted cost**: when the extents in use pile up, the last sealed
  segment holds about half of them, and its released bytes stay allocated
  until its last extent is released.

**Spools.**

A **spool** is a pipe whose bytes wait in segments, so that the producer
of the bytes waits for their consumer only at the **limit** of the spool.
The thread of the spool reads its **source** as the bytes arrive and
appends them; it waits for the reader only at the limit. The reader takes
the bytes in order, and waits only while no byte waits for it.

| Role | Source | Reader | Limit |
|---|---|---|---|
| Sender | Command stdout | The output reader, for the lookahead buffer (section 6.3) | `sender_spool_limit`, from HELLO |
| Receiver | Data-session stdout | Reconstruction (section 6.4) | `receiver_spool_limit` |

Each limit is 2 GiB by default, and `unlimited` removes it. Without a
limit, a spool can grow to almost the whole output when its reader stays
slower than its source (section 10).

Data structures:

- **Spool extents**: the extents of 1 MiB of the spool's segments, which
  the thread allocates in stream order. The reader is in the first extent
  that is not released, and the thread appends to the last one.
- **Written** and **read**: the bytes that the thread has appended, and
  that the reader has taken. The bytes between them **wait**.
- **End**: the source reached EOF, and the thread has appended all its
  bytes.
- **Limit**: the bytes that can wait before the thread stops reading its
  source. `unlimited` is the largest value, `2^64 − 1`, which no spool
  reaches.

The thread of the spool:

- **W1.** Wait while bytes wait and at least `limit` bytes wait. The
  reader wakes the thread when it takes bytes. With a limit of 0, the
  thread reads when no byte waits.
- **W2.** Read the source. At EOF, mark the end, wake the reader, and
  stop. A read error ends the run with `die` without a core dump.
- **W3.** Append the bytes, one extent at a time. If there is no extent,
  or the last extent is full, allocate an extent (steps A1 to A3) under
  the lock. Write the bytes that fit in the last extent with `pwrite`
  outside the lock. Then add them to written under the lock, and wake the
  reader. Repeat until all bytes are appended, then go to step W1.

The reader:

- **T1.** Under the lock, wait until bytes wait or the end is marked. If
  no byte waits and the end is marked, release the remaining extent, if
  one remains (steps R1 to R3), and report EOF. That extent is the last
  one, which is not full, so step T3 did not release it.
- **T2.** Read the waiting bytes of the first extent with `pread`, outside
  the lock.
- **T3.** Under the lock, add them to read, and wake the thread. If the
  reader has passed the first extent, release it (steps R1 to R3). Close
  the files of the segments that this removes after the lock is released.

The waits of the thread and of the reader have no timeout. The reader
releases the extents in allocation order, so the retention property above
holds for a spool: its files hold its waiting bytes and, in its oldest
open segment, bytes that the reader has passed. With a limit, at most the
limit plus one read of the source (256 KiB) wait, so the files hold less
than twice the limit plus 40 MiB. In a simulation of the default
limit of 2 GiB, with readers 2 to 10 times slower than the source and
with stalls, the files held at most 2.5 GiB, in at most 8 files.

Cost: on XFS, a spool passed 1 GiB in 0.36 s, with 0.27 s of CPU in the
thread and 0.16 s in the reader. That is about the cost of two copies in
memory (0.18–0.22 s each), as a channel of chunks makes. While the reader
keeps up, a segment can fill and close before the kernel writes back its
dirty pages, by default after 30 s. Then the close discards the pages,
and nothing reaches the disk. A slow source keeps its current segment
open longer, and the kernel writes those pages to the disk. The data
session can be such a source: it took 6.3 MB for 3.29 GB of output
(section 10).

### 6.6 Failures and trust

- Different protocol versions and unsupported HELLO parameters fail before
  command startup.
- Command failure still transfers all output and returns its status in END.
  Startup failure sends status 127 and error text.
- Truncated streams, broken sessions or receiver stdout, and receiver
  SIGINT/SIGTERM end the run unsuccessfully without a core dump.
- Stderr relay read, write, or flush failures also end the run with `die`
  without a core dump.
- Invalid frames, a compressed stream that ends before END or on the
  control session, unexpected answers, missing pins, protocol arithmetic
  overflow, database errors, and invariant violations are bugs: panic and
  abort.
- Conflicting immutable-copy policies, duplicate or nested root paths, and
  repeated directory identities detected during expansion or traversal call
  `die` without a core dump.
- Filesystem failures during indexing abandon entries; during lookup they
  reject source candidates. Failures of the tmp_data directory setup,
  segment creation, segment I/O, or verification end the run with `die`
  without a core dump; a yes answer cannot be retracted.
- Cache directory and file operation failures on either role end the run
  with `die` without a core dump, subject to the absence and invalid-content
  rules in section 5.6. Invalid cached files are removed and treated as
  misses; failures to remove them are fatal unless they are already gone.

Use trusted peers: the sender learns hashes of all indexed content.
`sender_cache = false` disables retention and removes earlier caches for
that receiver/index.

### 6.7 Threads at run time

Both roles use blocking threads and channels, with the threads of the table
below. At the normal end, a thread ends when its work ends: its input
reaches EOF or END, its queue closes because no thread can send to it any
more, or a write fails at an expected closure (section 6.8). Then the main
thread joins it, in the order of section 6.8; the reader of a spool joins
the thread of the spool when it closes the spool. One thread is the
exception: the receiver's signal thread stays until the process exits,
and no thread joins it, so that SIGINT and SIGTERM during the end still
give `128 + signal`.

Fatal shutdown ends the process rather than cancelling or joining blocked
threads.

| Role | Responsibilities |
|---|---|
| Sender main | Handshake and command startup, then obtain the command's exit status without reaping, wait for the completion conditions of section 6.8, and send DONE. |
| Sender data reader | Read the data session for the whole run: the protocol version, HELLO, the list, END_ACK, and EOF. EOF before DONE means that the receiver disappeared. |
| Sender spool thread | Copy command stdout into its spool until EOF, waiting at the limit of the spool (section 6.5). |
| Sender output reader | Fill lookahead from the spool of command stdout until its EOF, then close the spool. |
| Sender stderr relay | Copy command stderr to sender stderr through EOF and flush. |
| Sender scanner | Own records and query states; queue queries and output. |
| Sender probe threads | Probe the parts of long stretches of windows for the scanner (section 6.3). |
| Sender data writer | Drain output in order, compress, and finish and flush the stream after END. |
| Sender control reader/writer | Receive answers and send queries promptly, independently of data output. |
| Receiver main | Start SSH sessions, reconstruct stdout from the spool of the data session, validate completion, send END_ACK, and close both session stdin pipes after DONE. |
| Receiver spool thread | Copy data-session stdout into its spool until EOF, waiting at the limit of the spool (section 6.5). |
| Receiver control reader/writer | Dispatch queries to lookup workers and send answers promptly. |
| Receiver lookup workers | Answer queries out of order (section 6.4). |
| Receiver signal thread | Handle SIGINT/SIGTERM with status `128 + signal`. |
| Attach relay | Copy both directions; half-close on stdin EOF. Once the socket is closed, discard stdin; at socket EOF, wait for stdin EOF, then exit zero. |

Blocking output provides backpressure, which the spools absorb up to their
limits (section 6.5); it must not prevent independent control traffic.
Completion requirements are in section 6.8.

Keep successfully spawned children unreaped until their owning `hashsync`
process exits. Obtain command and SSH exit statuses with
`waitid(P_PID, pid, WEXITED | WNOWAIT)`; do not use reaping waits such as
`Child::wait` or `Child::try_wait`. Before spawning, set `SIGCHLD` to
`SIG_DFL` with `SA_NOCLDWAIT` clear. Retaining the command's group leader
reserves its process-group ID, and retaining the SSH children reserves their
PIDs, so late cleanup cannot signal an unrelated process after ID reuse.

### 6.8 Shutdown

Fatal errors end the process rather than attempting to interrupt arbitrary
blocked threads. Persistent state is recoverable: RocksDB recovers from its
write-ahead log, and cache publication is atomic. Kernel descriptor closure
propagates EOF or errors to peers.

Each role has a `die(status, message)` callable from any thread. It reports
the error, performs cleanup once, and exits without unwinding. Concurrent
callers wait for that cleanup rather than exiting ahead of it:

- Sender: SIGKILL the command's process group and remove the rendezvous socket.
- Receiver: SIGKILL both SSH processes, causing sender stdin EOF and cleanup.
- Process exit closes the unlinked segments of both roles, which frees all
  their storage without a separate cleanup pass.
- Abandoned cache temporaries remain for expiry.

Build with `panic = "abort"`; install a panic hook that performs the same
cleanup before aborting. External failures use `die` without a core dump;
bugs and database errors abort. Per-entry failures remain local.

Both roles block SIGXFSZ in every thread. Then a write past the file-size
limit fails with `EFBIG`, and the run ends through `die`, with its
cleanup; the default action of the signal would end the process without
it. The receiver also blocks SIGINT and SIGTERM in every thread, for its
signal thread (section 6.7). A blocked signal stays blocked across
`exec`, so each role starts its children with the signal mask that it
started with.

Only the normal end runs the C library's exit handlers. One of them, a C++
static destructor of RocksDB (`JoinThreadsOnExit`), joins RocksDB's
background threads, so the end waits for a running flush or compaction.
`update`, `stats`, and `run` close the database first, which stops its
background jobs. A transfer starts only after its update has waited for
compactions, so none runs at its end. A fatal end, through `die` or a
termination signal, calls `_exit` instead, and an abort runs no handlers
either: many threads can be inside RocksDB then, and the process must
neither wait for a background job nor destroy RocksDB's static objects
under them. The index recovers from its write-ahead log, as after a crash.

The receiver monitors both sessions and termination signals. The sender
monitors stdin and the control socket. Unexpected EOF or session loss is
fatal. Monitoring also applies during rendezvous and setup, when no command
output is flowing. A broken stdout is detected when a write to it fails;
neither role watches an idle stdout. As under plain SSH, a consumer that
exits after it reads all output does not fail the run. Normal shutdown uses
END_ACK, not stdin EOF, to establish receiver completion, and DONE, not
stdout EOF, to establish sender completion.

Successful sender completion requires:

- Command stdout has reached EOF and all its bytes have been represented by
  data frames followed by END with the command's exit status. The compression
  stream, if enabled, is finished and data-session output is flushed.
- Command stderr has reached EOF and the relay has forwarded and flushed
  all its bytes to SSH.
- One valid END_ACK has been received in response to END. An unsolicited or
  duplicate acknowledgement is a protocol error.

The implementation chooses the ordering and synchronization of these
conditions to make the code simple and reliable. It may serialize waits or
coordinate concurrent completion; no particular event ordering or state
machine is required. Startup failure has no command streams to drain.

Keep sender stdout open until process exit. Join the threads whose work
ends with the command's streams or with END: the output reader after the
EOF of the spool of command stdout, which it closes, so that it joins the
spool's thread; the stderr relay for its completion condition; and the
data writer after END. Once the completion conditions hold, perform normal
cleanup once:

1. Shut down the control socket. `attach` then relays the rest of the
   socket to its stdout, and discards its stdin until EOF (section 6.1).
   Join the control reader, which then reads EOF, and the control writer,
   which ends when its queue is closed and empty or a write fails. Socket
   EOF and a failed write are expected closure then.
2. End the probe threads and join them.
3. Send DONE and flush the data session: the sender needs neither session
   any more.
4. Join the data reader. It reads stdin to EOF, which the receiver causes
   after DONE. Before DONE, EOF or a read error remains the receiver's
   disappearance and is fatal. A read error is fatal also after DONE. A
   byte after END_ACK is a protocol error.
5. Remove the rendezvous socket and exit zero.

Receiver completion:

1. After END, the receiver consumes the rest of that zstd frame, checking its
   trailer and rejecting any further decoded bytes. Stop at frame completion
   without waiting for transport EOF; when compression is disabled END is
   sufficient framing. Flush reconstructed stdout.
2. The receiver enters Completing before sending and flushing END_ACK on
   the data session. It stops admitting lookups and publishing control
   replies, and closes the queue of replies. Late control traffic can be
   discarded. It keeps both session stdin pipes open until DONE. Workers
   returning later discard their results, including results of unfinished
   segment writes, and release the extents of those writes.
   Control EOF, broken pipes, and a partial final control frame or
   compressed block in Completing are expected closure, not failures.
3. The receiver reads DONE, which directly follows the zstd frame. EOF or
   another frame before DONE fails the run.
4. The receiver closes both session stdin pipes, in this order:
   1. Close the data session's stdin.
   2. Join the control writer. It ends when its queue is closed and empty,
      after it has sent the queued replies, or when a write fails.
   3. Close the control session's stdin.

   Neither close waits for the EOF of a stdout pipe: through an SSH
   multiplexing client, or through a wrapper that waits for SSH, that EOF
   comes only when SSH exits, and SSH exits only after the sender has read
   stdin EOF (section 10).
5. The receiver drains the spool of the data session to its EOF, which
   follows transport EOF, rejecting any bytes after DONE. It closes the
   spool, which joins the spool's thread. Then it ends its other threads,
   in this order:
   1. Join the control reader. It reads control-session stdout to EOF, also
      in Completing, so it does not close the pipe while SSH can still
      write to it: SSH would fail with a broken pipe. Its end closes the
      query queue.
   2. Join the lookup workers. In Completing, a worker skips the queries
      that are still in the queue and reads no candidates for them; a
      lookup in progress finishes first.
6. The receiver observes both SSH exits without reaping. Both must exit
   zero; otherwise the transfer failed even if END was received. Then it
   closes what the threads used, in this order:
   1. Close the pins: close all their open segments, which frees all their
      storage. The tmp_data directory closes with its last user, the
      pins.
   2. Close the index: first the database, then its lock (section 4.4).
7. The receiver prints the summary (section 8) and exits with END's command
   status.

The receiver's Completing transition uses the same lock as pin allocation
and response publication. This prevents late lookup completion from trying
to prolong the exchange. Sender stdin EOF before DONE remains a failure,
and so does control EOF before the sender shuts down the control socket:
the receiver closes those pipes only after DONE. Failures during stream
validation, stdout flushing, sending END_ACK, or reading DONE take the
fatal cleanup path.

No step has a deadline. A descendant retaining command stdout or stderr
keeps the run open, as under plain SSH; this is expected behavior. In the
same way, a process that inherited a session's stdout pipe from SSH delays
the receiver's end until it closes the pipe. It does not delay the sender:
the receiver closes both stdin pipes after DONE, before it waits for the
EOF of either stdout. A lookup or segment I/O that stalls, for example on
an unresponsive NFS mount, delays the receiver's end until the I/O
completes. After fatal cleanup, a descendant that left the process group can
survive and will get a broken pipe on its next write to either closed output
pipe.

## 7. Configuration

Use `$XDG_CONFIG_HOME/hashsync/config.toml`, or `--config PATH`:

```toml
block_size     = "16KiB"   # at least 512 bytes; changes rebuild the index
strong_hash    = "blake3"  # blake3 or sha256
candidate_attempts = 128  # candidates tried per query, at least 1
hashlist_max_age = "90d"
index_path     = "~/.local/state/hashsync/default.db"  # a directory
threads        = 0         # 0 = 4 × cores, clamped to 16–64
lookahead      = "256MiB"  # at least block_size
sender_spool_limit   = "2GiB"  # command stdout that can wait on the sender, or "unlimited"
receiver_spool_limit = "2GiB"  # data session that can wait on the receiver, or "unlimited"
tmp_data_dir   = "~/.cache/hashsync/tmp-data"  # receiver segments; any filesystem
negative_cache = "128MiB"
probe_threads  = 0         # 0 = half the sender's cores, at least one
compression_level   = 9    # zstd 1–19; 0 disables
compression_long    = true # zstd long-distance matching, 128 MiB window
compression_threads = 0    # 0 = half the sender's cores, at least one
ssh            = "ssh"     # shell-split command; appends -o Compression=no
sender_command = "hashsync"
sender_cache   = true

[hosts.builder]            # overrides keyed by the run command's HOST argument
ssh            = "ssh -p 2222 -i ~/.ssh/builder"
sender_command = "/run/current-system/sw/bin/hashsync"
sender_cache   = false

[[roots]]
path = "/nix/store/*"      # each match is a root; includes dot names
immutable = true
identifier = "name"
exclude = [".links"]

[[roots]]                 # copies of the first store share identifiers
path = "/mnt/disk/nix/store/*"
immutable = true
identifier = "name"
exclude = [".links"]

[[roots]]
path = "/srv/data"
exclude = ["*.tmp", ".cache"]
one_filesystem = true
```

The list width is a protocol constant, and `weak_hash_id` fixes the
weak-hash multiplier and mixer. Account IDs are persistent state. None of
these are TOML options.

**Byte strings.** TOML strings contain Unicode characters, not arbitrary
bytes. Even TOML 1.1's `"\xFF"` denotes U+00FF, whose UTF-8 bytes are
`C3 BF`, not the single byte `FF`. After TOML parsing, `hashsync` interprets
the following syntax in `index_path`, `tmp_data_dir`, `sender_command`,
root `path` and `exclude` values, and identifier `literal` and `strip_prefix`
values. For `ssh`, shell-split first, then decode each argv word.

| Text after TOML parsing | Meaning |
|---|---|
| `\xHH` | One byte; exactly two hexadecimal digits, with either letter case. |
| `\\` | One literal backslash byte. |
| Ordinary Unicode text | Its UTF-8 bytes, without normalization. |

Decode once, from left to right; never interpret decoded bytes as another
escape. A trailing backslash, malformed hex escape, or unsupported escape
is a configuration error. Octal escapes are not supported. NUL is invalid
in paths, path patterns, and process arguments; literal identifiers may
contain it. Non-UTF-8 bytes are valid in all these byte-string fields.
Other configuration fields retain their ordinary TOML meanings. Do not
apply configuration escapes to names read from the filesystem or to CLI
arguments.

In globs, byte escapes produce literal bytes, not glob operators:
`\x2A` matches a literal `*`, while an unescaped `*` is a wildcard.
Also allow `\*`, `\?`, `\[`, `\]`, `\-`, `\!`, and `\^` to quote those
characters in glob patterns. Preserve this quoting when producing the raw
byte pattern for `gix-glob`; a decoded backslash must not introduce a second
glob escape. A decoded `/` remains a path separator. Use the raw matcher
without Git ignore-file negation or repository-relative path rewriting.

Prefer single-quoted TOML literal strings so TOML preserves the backslashes.
In double-quoted TOML strings, double each backslash for the TOML layer:
`'/srv/\xFF/*'` and `"/srv/\\xFF/*"` specify the same pattern.

```toml
[[roots]]
path = '/srv/archive-\xFF'  # byte FF in the root directory name
immutable = true
identifier = { literal = 'archive-\xFF' }
exclude = ['*.tmp', 'literal\x2Aname', 'back\\slash']
# The last two exclusions match the literal names "literal*name" and "back\slash".
```

## 8. Command line

- `hashsync update [--rebuild] [-j N]`: update or rebuild the index;
  `-j` overrides the configured thread count. Use `--rebuild` after changing
  the indexing policy of completed immutable roots.
- `hashsync run [--no-update] [--ssh CMD] [USER@]HOST -- COMMAND ARGS...`:
  run the remote command, reproduce stdout, and return its status.
  Normal runs hold exclusive index access through update and transfer.
  `--no-update` shares read-only access and still resolves current roots.
  Pass HOST unchanged, preserving SSH aliases. Run argv without a shell.
  After a successful transfer, also when the command fails, print a
  summary of two lines to stderr:
  1. The output bytes, as literal, reference, and run bytes; the wire
     bytes, which the receiver read from the data session; and the
     hash-list and delta bytes.
  2. As rsync does, the bytes that the receiver sent and received on both
     sessions, the elapsed time of the run, including its index update,
     the rate of the sent and received bytes in that time, and the
     speedup: the output bytes divided by the sent and received bytes.
     These counts do not include the bytes that SSH adds or the command's
     stderr. They are exact: the receiver prints the summary after the
     threads that count the bytes have ended (section 6.8).
- `hashsync serve --token T` and `hashsync attach --token T`: internal
  sender entry points.
- `hashsync stats`: report index statistics.
- `hashsync prune`: acquire exclusive index access, resolve roots, and
  remove paths absent from every configured copy. Remove vanished or
  unconfigured roots together with their paths, then orphaned inodes and
  blocks. Inaccessible locations are not proof of absence (section 4.2).

Every command also accepts these options, besides `--config PATH`
(section 7):

- `-q` (`--quiet`): do not print progress messages and summaries to
  stderr. Warnings and errors stay.
- `--no-human-readable`: show all amounts of bytes as whole bytes.

Summaries and statistics show amounts of bytes in binary units. An amount
below 1 KiB shows as whole bytes, for example `512 B`. A larger amount
shows with two decimal digits, in the first of KiB, MiB, GiB, TiB, and PiB
in which its rounded value is less than 1024, else in PiB. For example,
1536 bytes show as `1.50 KiB`, and 1048575 bytes as `1.00 MiB`. Settings,
limits, and messages that help to debug show exact byte counts, because
rounding could make them unclear.

```sh
hashsync run backup@nas -- tar -C /srv/data -cf - . | tar -C /srv/data -xf -
```

## 9. Libraries

Use stable Rust, edition 2024. Bundle RocksDB, LZ4, and libzstd. RocksDB
requires a target C++ compiler, and libclang to generate its bindings; LZ4,
libzstd, and BLAKE3's SIMD support require a target C compiler. There is no
separate pure-Rust build.

| Need | Choice |
|---|---|
| Index database | `rocksdb` with bundled RocksDB and LZ4 |
| Strong hashes | `blake3`, `sha2`, behind one shared strong-hash interface |
| SIMD | `fearless_simd` and its `#[simd]` attribute (`fearless_simd_macros`) on x86-64, with the level chosen at run time |
| System calls | `rustix`; `libc` and `linux-raw-sys` for missing wrappers/constants |
| Thread pools and channels | `rayon` for indexing, the hash-list sort, and sender probes, `crossbeam-channel`; blocking standard threads for transfer |
| Compression | `zstd-sys` binding libzstd with worker-thread support |
| Memory allocation | `mimalloc` for Rust code, RocksDB, and libzstd, with the `mimalloc` feature (on by default): the global allocator, replacements of the C++ allocation functions from mimalloc's `mimalloc-new-delete.h`, and libzstd's allocator interface; never its `override`, so other C code keeps libc `malloc` |
| Unlinked segment files | `O_TMPFILE` through `rustix` |
| Configuration and CLI | `serde`, `toml`, `clap`; `gix-glob` for byte patterns and paths |
| Maps and caches | Standard maps with a fast hasher; `lru` for bounded caches |
| Signals | Blocked signals and a polled `signalfd` through `libc`; no signal handlers |
| Tests | `proptest`, `tempfile`, and frame-decoder fuzzing |

The weak hash, canonical list encoding, direct deltas, and fingerprint table
are project code. Library versions and API-level details belong in the
implementation and lockfile.

## 10. Alternatives considered

Keep the rationale for choices that are likely to be revisited; detailed
decision history is in version control.

- **Content-defined chunking:** archive framing disrupts chunks at file
  boundaries. Fixed receiver blocks and a rolling sender scan lose only
  incomplete file tails.
- **Buzhash:** periodic inputs have structural collisions. The mixed
  polynomial hash avoids that specific weakness.
- **Truncated weak-hash lists and membership filters:** truncation raises
  false-hit costs; binary fuse filters need several cache-line accesses per
  probe. The full list also supports direct set deltas.
- **Reconciliation sketches or range recursion:** add decoding/fallback
  complexity or transfer poorly when changes are spread throughout the
  uniform hash space. Cached full lists allow straightforward direct deltas.
- **Independent scanner segments:** skipped windows can cross a reference
  chosen by another segment and leave possible matches unexamined. One
  scanner owns all offset decisions.
- **Content-defined anchors for weak probes:** probing only the windows
  that a short-context rolling hash selects makes the scanner's cost per
  byte independent of the list size. It changes the list, the index, and
  the scanner, and very repetitive blocks without an anchor cannot be
  found. Probe threads keep one table probe per window.
- **SQLite for the index:** its B-tree indexes by strong hash (and, at
  first, by weak hash) have random keys. When such an index is larger than
  the writer's page cache, almost every block insert changes a different
  page, and SQLite writes each changed page twice: to its write-ahead log
  and at the checkpoint. At 21M rows, an update that added 262k block rows
  took 110 s with both indexes and 18–23 s with the strong one only.
  RocksDB applies the same changes in 2.4–4 s. In a benchmark of the
  database work alone, at 20M rows, a new index took SQLite 66–68 s, with
  its index created after the rows, and RocksDB 32 s; a warm candidate
  lookup took SQLite 14–15 µs and RocksDB 19 µs. Through the code of
  hashsync, the one writer thread of that time stored 245k rows/s in
  RocksDB, so a new index of 20M rows took about 90 s, and a warm
  candidate lookup takes 26 µs; the file reads of a transfer cost more.
  The export took SQLite 4.2–5.5 s and takes RocksDB 1.6 s with parallel
  reads (12.7 s with one thread). The index took 2.1 GB and takes 1.3 GB.
- **One writer thread:** SQLite allows one writer, so one thread made
  every index change, and the scan tasks sent it messages. With RocksDB
  that thread limited updates. In an update of a new index of 77.6 GB at
  4 KiB blocks (18.7M block rows), it was busy 98% of the time, 52 of 64
  scan threads waited for it, and the disk was 20–45% busy. The update
  took 121–125 s with it and 65–68 s with the scan tasks writing; an
  update without changes took 26–29 s and 8–13 s. Eight writer threads
  were as fast, but kept the messages, a second lookup of each identity,
  and the state of each file in two places. RocksDB inserts the
  transactions of several threads in parallel only with unordered writes:
  its ordered write path runs one write group at a time, and 64 scan
  threads with ordered writes took 98–113 s.
- **Other embedded stores:** LMDB read fastest, but inserted 16k rows/s
  and wrote 35 KB per row. LevelDB has one compaction thread, no bulk load,
  and lets one process open a database, even to read it; fjall has the
  same limit.
- **Compaction only at the end of an update:** turning automatic
  compactions off during an update and compacting at its end does not make
  updates faster. The one writer thread of that time set the pace, and
  compactions on the other cores never stalled it: a new index of 20.5M
  rows took 83 s of writing either way. A full compaction at the end of
  each update added 3–6 s to small updates, because it rewrote the whole
  index (1.3 GB).
  Waiting at the end only for the compactions that RocksDB has scheduled
  costs little. Without that wait, closing the database cancels them: over
  short updates they pile up, and lookups slowed from 26 µs to 41 µs.
- **One memtable limit for all column families:** with a limit of 256 MiB
  for each column family, so that only the shared limit applies, the one
  writer thread of that time stored a new index of 20.5M rows 10% more
  slowly, and the end of an update that added 262k rows took 5.8 s instead
  of 0.4 s.
- **More background jobs:** 12 background jobs run up to 9 compactions
  at a time. Without subcompactions, the end of a new index of 20.5M rows
  took 8.5 s instead of 3 s: its last compaction, from level 0, ran in one
  thread. With 12 subcompactions as well, it took 3 s, but compactions
  from level 0 in several column families could then use 12 threads each
  at the same time. 4 background jobs with 3 subcompactions took
  3.4–4.5 s. At 20.5M rows, one compaction at a time kept up with the one
  writer thread of that time; compactions below level 0 do not split.
  With the scan tasks writing, 8 background jobs saved about 1 s at the end
  of a new index of 18.7M block rows, and nothing overall (65–68 s either
  way).
- **Parallel flushes at the end of an update:** with 3 flush threads, the
  end of an update that added 262k rows took 0.24 s instead of 0.4 s. But
  after 40 of 40 updates of a new small index, the old write-ahead log
  stayed on disk until the next modifying process opened the index. Each
  flush records the oldest log that the other column families still need,
  and flushes that end together see the old log of the others. Read-only
  processes skip that log: each column family also records the log after
  its own flush.
- **Point deletions of all paths in reconciliation:** in an index of
  `/nix/store` at 4 KiB blocks (360k inodes, 625k paths), the removal of
  each `paths` row and `inode_paths` entry made the reconciliation of an
  update without changes take 0.62–0.83 s. The orphan pass after the scan
  then stepped over the removed `inode_paths` entries and took 0.67–2.2 s.
  With one range deletion in each column family, they took 0.24–0.40 s
  and 0.27–0.40 s. The whole update, with the orphan pass below, took
  6.2–12.1 s instead of 7.9–12.6 s (medians 6.5 s and 11.0 s).
- **One thread for the orphan pass:** the merge of `inodes` and
  `inode_paths` took 0.27–0.40 s after that update without changes, and
  0.54–0.67 s after a new index. On the threads of the scan pool it takes
  0.022–0.030 s and 0.065–0.085 s. Completed immutable roots skip the
  scan, but this pass reads the whole index.
- **Other locks:** RocksDB's own `LOCK` file keeps only read/write opens
  apart, and a second open fails at once. A `flock` on the index directory
  waits, and lets read-only processes exclude a modifying one, without
  another file, but it fails on NFS: Linux emulates `flock` there with
  byte-range locks, and an exclusive one needs a file open for writing,
  which a directory cannot be. A classic POSIX lock on the lock file
  belongs to the process: closing any other descriptor of the file releases
  it. At exit, the kernel releases such locks in descriptor order, so the
  lock file, opened before `LOCK`, would be free while `LOCK` is still
  held.
- **An index by weak hash:** a column family keyed by weak hash gives the
  export its order without a sort, but adds one entry per block. In the
  benchmark of the database work alone, at 20M rows in RocksDB, inserts
  without it were 2 times faster (126k to 260k rows/s), removals 1.8
  times faster, and a new index took 32 s instead of 52 s. The export, a
  scan of `blocks` with a parallel in-memory sort, was not slower.
- **Async runtimes and `io_uring`:** the blocking pool already serves the
  file-oriented workload, and `io_uring` availability would require a
  fallback. Keep one I/O model.
- **Direct I/O or mmap for file hashing:** direct I/O bypasses useful cached
  data; mmap exposes the process to SIGBUS on concurrent truncation.
  Use buffered `pread`.
- **Opening candidates by inode handle:** requires privileges. Open indexed
  paths and verify the resulting bytes instead.
- **Hole punching in one pin file:** one open, unlinked file held all
  extents, and a reclaimer thread punched the released extents out of it
  with `fallocate(FALLOC_FL_PUNCH_HOLE)`, from the start of the file,
  128 MiB at a time. XFS and btrfs write a range back before they punch
  it, so punching forces every pin byte to disk, and it holds the inode
  lock that pin writes and reads need. On XFS, for 2 GiB of pins written
  in 16 KiB `pwrite` calls, one punch per 128 MiB took 425–441 ms in
  `fallocate` and wrote 2068 MiB to disk; closing one unlinked file per
  128 MiB instead took 71 ms in `close` and wrote nothing. Smaller punches
  cost more: with a punch for each filesystem block of released extents,
  one transfer spent 14.9 s in 36.5k punches. Punching also needs a
  filesystem that supports it, the allocation granularity of that
  filesystem, and a probe at setup. Segments need none of these, and no
  thread: creating an unlinked file takes 40–180 µs, and closing one takes
  about 35 µs per MiB written in 16 KiB writes.
- **Segments of one fixed size:** descriptors are scarce: a common soft
  limit is 1024, and RocksDB uses many. Segments of 128 MiB keep 512 files
  open when 64 GiB of pins pile up, and 128 when one pin in each 16 MiB of
  16 GiB of pins is never released. In a simulation with 16 KiB extents,
  released almost in allocation order, the full rule of section 6.5 kept
  13 and 11 files open. Its cost is the retention: while the 64 GiB of
  pins drained, its last sealed segment kept up to 32 GiB of released
  bytes allocated, 10.7 GiB on average.
- **One open segment per size:** segments of 16 MiB, 32 MiB, 64 MiB, and
  so on, where a new segment takes the smallest size that no open segment
  has. The open segments stay few, but a segment keeps the size that it
  got at its creation, so a large segment stays the current segment after
  the pins that made it large are released. In the same simulation, with
  3 GiB of pins in use at all times, 9 files were open, and up to 4 GiB of
  released bytes stayed allocated, 1.1 GiB on average. The full rule of
  section 6.5 kept 8 files open, and up to 1.0 GiB allocated, 254 MiB on
  average. After 8 GiB of pins piled up and drained, the output kept up
  for 16 GiB of pins. The segment of 8 GiB stayed the current segment
  until it was full: up to 8 GiB of released bytes stayed allocated, 2 GiB
  on average. With the full rule, at most 16 MiB stayed allocated.
- **Spooling the reconstructed output:** a spool between reconstruction
  and stdout would let reconstruction go on while stdout is blocked. But
  it writes every referenced block a second time, from its pin to the
  spool, and every run in full. In a transfer of 3.29 GB of `/nix/store`
  files, 97% references at 4 KiB blocks, the data session took 6.3 MB,
  and such a spool would take 3.29 GB; a run of 10 GiB of zeros is one
  RLE frame of 14 bytes. The spool of the data session keeps the transfer
  going too; only reconstruction, which is local, waits for stdout.
- **Spools without a limit:** the sender does not keep the command's
  output, so with a slow link its spool would grow to almost the whole
  output on the sender's disk. For `tar` of 2 TB at 200 MB/s over a link
  of 10 MB/s, the spool would hold about 1.9 TB when `tar` ends, and a
  full disk ends the run. Only the command would end sooner, not the
  transfer. On the receiver, a consumer that stops for a long time would
  let almost the whole data session wait on the receiver's disk, which
  the consumer can be writing to as well. At its limit, the source of a
  spool waits instead.
- **A lookahead mapped from the spool:** the scanner could map the
  segments of the spool of command stdout into a reserved address range,
  in place of the copy into its lookahead buffer. The buffer would then
  use pages of the page cache, and `lookahead` could be much larger. But a
  rejection makes the scanner scan again up to `lookahead` bytes
  (section 6.3): at 2.7–19.3 ns per byte (12 to 1 probe threads, 10M list
  values), up to 0.7–5.2 s for 256 MiB, and 6–44 min for 128 GiB. The
  query states of the sender, and the queued queries and the pins of the
  receiver, grow with the lookahead too. The spool already lets the
  command run ahead of the scanner. In a benchmark of the copies, the
  mapping would save about 0.15–0.35 s per GiB on the scanner thread. It
  would cost unsafe slices over mappings, SIGBUS in place of `die` when a
  page cannot be read, and a second interface on the spool.
- **The scanner reading the spool of command stdout itself:** without the
  output reader, its thread, and its queue of chunks, the scanner would
  read the spool when its buffer has room, and wait for the spool in the
  same wait as for answers. But the read from the segment would move to
  the scanner thread, which limits the sender on new output: about
  0.15–0.35 s per GiB, as in the benchmark above, up to 12% of a scan at
  2.7 ns per byte. A read straight into the lookahead buffer would avoid
  that copy, but the buffer would have to keep initialized room, and the
  spool would need a second interface that does not wait.
- **Truncating a segment before it closes:** on NFS, a close writes the
  dirty pages of a file to the server, also for an unlinked file. A
  truncation to size 0 before the close does not prevent that: the NFS
  client writes all dirty data to the server before it sends the
  truncation (`nfs_setattr`, Linux 6.18), and so does the SMB client. On
  local filesystems, the close of an unlinked file already discards its
  pages, so a truncation would only add a metadata transaction per
  segment. Use a tmp_data directory that is not on NFS instead (section
  6.5).
- **Flushing the compression stream when the scanner waits:** kept output
  responsive when the command pauses, but each flush ends a zstd block or
  job early. With a command paced at 2 MB/s, level 9 sent 5.7% more bytes
  and used 53% more compression CPU. Output latency does not matter here.
- **Compressing the hash list:** the weak hashes are uniform 64-bit
  values. For all of `/nix/store` at 4 KiB blocks, 14.0M values in
  73.3 MB, the Rice code of section 5.4 is only 0.16% larger than the
  smallest encoding of a random set of that size, and zstd at levels 1 to
  19 added 1.7 KB. For random lists of 1M and 4M values, the sorted values
  as `u64`, compressed with zstd at levels 3 to 19, were 33–40% larger
  than the Rice code. Only fewer values make the list smaller: cached
  lists and deltas (section 5.6).
- **An uncompressed control session:** in a transfer of 3.29 GB of
  `/nix/store` files, 97% references at 4 KiB blocks, the queries took
  35.0 MB, the answers 10.9 MB, and the data session 6.3 MB. The 32-byte
  digest of a query does not compress, but the rest of a query and almost
  all of an answer do. In one zstd frame at level 9, queries are 25%
  smaller and answers 92% smaller. But each flush adds a block header:
  with a flush after each frame, they are 0.6% and 21% larger. Between
  two processes on one host, with a flush each time the writer's queue
  becomes empty, the transfer sent 18% fewer query bytes and 9% fewer
  answer bytes, in the same time. A link that limits a direction makes
  its writer wait, so more frames share each flush. In a transfer of
  0.98 GB, with the queries limited to 400 KiB/s, the queries were 24%
  smaller, and the transfer took 22.5 s instead of 28.6 s. With the
  answers limited to 50 KiB/s, the answers were 60% smaller, and the
  transfer took 28.1 s instead of 65.3 s. From level 9 to 19, the control
  bytes shrink by about 1%, but the cost per frame in a control writer
  grows from at most 4 µs to 33 µs.
- **Flushing the control streams less often:** a writer that kept queries
  until the scanner waits would hold a query in the compressor while the
  scanner fills its lookahead with literal bytes, and the output would
  wait for that query's answer. Answers kept until the lookup workers are
  idle would stop the scanner in the same way. Small flushes happen only
  while a link keeps up with the writer; a slower link makes the writer
  wait, and its flushes grow.
- **Stdout EOF as the end of the sender:** `serve` could close its stdout
  when it has finished, and the receiver could close each session's stdin
  at the EOF of that session's stdout. With SSH multiplexing, this is a
  deadlock: the OpenSSH mux client (ControlMaster) gives its descriptors 0
  to 2 to the master, and keeps its own stdout open until the session ends
  (`mux.c`, `mux_client_request_session`). So the receiver sees no stdout
  EOF before `serve` exits, and `serve` waits for stdin EOF before it
  exits. A wrapper script that waits for SSH holds the pipe in the same
  way. Without multiplexing, `ssh` replaces its stdout with `/dev/null`
  after it opens the session, so the EOF arrives, but the design cannot
  depend on that. DONE needs no stdout EOF, and stdin EOF always arrives:
  the receiver is the only writer of each stdin pipe.

## 11. Repository conventions

- One crate, one binary, committed `Cargo.lock`. All profiles use
  `panic = "abort"`; install cleanup hooks before starting threads.
- Isolate unsafe syscall wrappers and SIMD code; deny unsafe code elsewhere.
- Share protocol definitions between both roles. The protocol version is 1
  until the first release. After that, any change that a peer can observe
  after the version exchange requires a protocol version bump, for example a
  change to frame layouts, frame order or meaning, the list encoding, or the
  frames that the zstd streams carry. New weak and strong hashes are the
  exception: HELLO names both, and a sender that does not support them
  ends the run before command startup. The remote commands and the version
  exchange never change (sections 6.1 and 6.2).
- Run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test` for
  implementation changes.
- Validate hashing/encoding properties, empty-table misses, and absence of
  table false negatives;
  byte-exact reconstruction under rescans, late answers, backpressure,
  and any split of a stretch among probe threads;
  segment appends and closure with slow stdout, concurrent lookups,
  rejected-query rescans, and late answers; obsolete writes, unaligned block
  sizes, the full rule after each allocation and each closure of a sealed
  segment, the bound on open segments for any release order, sealed
  segments held open by extents in use, pin storage failures, corruption
  detection, and cleanup on exit; spool order and EOF, the limit of a
  spool, segments that close behind the reader of a spool, a command that
  ends while the consumer of stdout reads nothing, a command that waits
  when both spools are full, and a receiver that stops reading the data
  session at its own limit; filesystem races and sparse files;
  reopening regular files and directories after rename or unlink;
  skipping symlinks and special files without opening them for I/O;
  configuration byte escapes, TOML quoting, literal glob metacharacters,
  and non-UTF-8 root, exclusion, and identifier values; symlinks and cycles
  during root expansion; fatal directory cycles and aliases in either
  traversal order; root replacement between expansion and scanning;
  immutable reuse after root-directory identity changes; no tasks or opens
  solely for overlap checks on cached groups or unused copies; completion
  withheld after a late directory conflict; links that the scan finds
  before and after their owner's row commits; hard links within and across
  roots (including links to a single-file root); conflicting copy policies;
  pruning a vanished immutable root and indexing it again when it returns;
  SSH option precedence and bootstrap quoting of executable paths containing
  whitespace, quotes, shell metacharacters, and non-UTF-8 bytes;
  a different protocol version on either side, detected before any frame;
  crash recovery, process exclusion through the index's lock file, and
  reads of later commits; cache publication and cleanup races, fatal
  cache I/O failures, and removal of invalid cached lists with fallback to
  a full list;
  completion without truncation or premature exit under the chosen
  synchronization scheme;
  complete stderr forwarding with backpressure and delayed stderr EOF from
  descendants; truncated compression trailers, compressed control
  traffic in both directions that the closure ends, late lookups, relay
  closure with control writes in flight, shutdown on session loss, and fatal
  cleanup after a child's exit status has been collected without reaping. Use
  separate processes for locking tests and fuzz protocol decoding.
- Tests wait for events, also to check that something does not happen.
  Only when no event can show it can a test wait a fixed time, and then
  the end of that wait must be the success of the test: a slow machine
  can make such a test miss a fault, but never fail a correct build.
- Package with Nix `rustPlatform.buildRustPackage`, including support for
  cross and static builds.

## 12. Deferred work

Measure real workloads before adding adaptive compression, different
indexing concurrency for spinning disks or very large files, or deltas that
encode removals by rank in the base list.

Measure rejected-query rescans and dense stale hits before adding scanner
state to reduce repeated work. The current design deliberately accepts
rescanning a suffix for simpler state transitions.
