# hashsync

This document specifies the purpose and design of `hashsync`. Update it when
the design changes. `PLAN.md` orders the implementation.

## 1. Purpose

`hashsync` copies a remote command's stdout to the local host, replacing
data already held in local files with block references. The receiver reads
and verifies those blocks before inserting them into the output. This is
useful for package trees and archive streams that repeat existing content
at different positions and alignments. With `--stdin`, the command reads
the receiver's stdin.

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
  offset, and the receiver's yes/no response.
- **Pin**: a query offset's verified block, retained by the receiver in its
  own **extent**: a range of bytes in a segment.
- **Segment**: an open, unlinked file in the **tmp_data directory** of a
  host, which holds the extents of pins or the bytes of a spool
  (section 6.5).
- **Spool**: a pipe whose bytes wait in segments, so that the producer of
  the bytes waits for their consumer only at the limit of the spool.
- **Input**: the bytes that the command reads as its stdin: the receiver's
  stdin when the run has `--stdin` or `--batch-stdin`, else none. The
  **input stream** carries them from the receiver to the sender,
  compressed (section 6.6).
- **Idle point**: a moment when the source of the output, command stdout,
  or the source of the input, the stdin that the stdin reader reads, has no
  byte ready.
- **Delivery**: when the bytes of the output, or of the input, go on from
  the buffers on their way. **Eager** delivery, the default, sends them on
  at each idle point of their source, once the writer of their session is
  idle, and when a buffer is full; **batch** delivery only when a buffer
  is full and at the end of the stream (section 6.2).

## 3. One run, step by step

1. The receiver resolves configured roots and updates the index.
   `--no-update` skips the update and acquires read-only access. Index access
   is held throughout the run.
2. It exports the hash list from one database snapshot, opens its
   tmp_data directory (section 6.5), and starts data and control SSH
   sessions to the sender. It reads the data session through a spool.
3. After both sides exchange protocol versions, HELLO negotiates parameters
   and cached lists. With sender caching enabled, the receiver durably
   publishes its target list. It sends a full list, a delta, or
   confirmation that the sender's list is current. The sender verifies it,
   builds its table, and durably publishes it when caching is enabled.
   Then it removes its other cached lists for this peer and index,
   expires old cache files, and acknowledges the accepted target. On the
   acknowledgement, the receiver does the same cleanup (section 5.6).
4. The sender starts the command, whose stdout goes through a spool: the
   command waits for the transfer only when `sender_output_spool_limit`
   bytes wait in the spool. The scanner sends literals for misses, runs for
   repeated bytes, and queries for possible block matches. From the
   acknowledgement on, the receiver sends the input to the command's
   stdin, until the sender stops the input (section 6.6). In eager
   delivery, the bytes of the output and of the input reach their
   destinations at each idle point of their sources, once the link can
   carry them (section 6.2).
5. The receiver reads and verifies candidates, and writes each verified
   block to a pin before it answers yes. The sender emits references for
   yes answers and rescans rejected windows from the next byte. Output is
   always emitted in order.
6. The receiver reconstructs stdout from its spool of the data session.
   Until `receiver_output_spool_limit` bytes wait in the spool, a blocked
   stdout stops only the reconstruction. After all command stdout is
   processed, and the command's exit status is available, the sender sends
   END. The receiver validates stream completion and flushes stdout before
   sending END_ACK. When the sender needs neither session any more, it
   sends DONE, and the receiver closes both session stdin pipes. Both SSH
   sessions must exit successfully before the receiver returns the
   command's status.

**Verification is authoritative.** The index, hash lists, caches, and weak
hash table are hints. Verify the exact buffer of a candidate before writing
it to a pin, and the exact buffer read back from a pin before writing it to
stdout. Both checks use the query's full strong hash. Stale files and
collisions can reduce deduplication, never change the reconstructed output.

**No timeouts.** Waits end when the expected event occurs or a peer,
descriptor, or process goes away. Slow systems must not fail because of a
deadline; a stalled process can be inspected with a debugger. Fatal errors
end the process (section 6.9). Retry intervals and cache age bounds are not
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
once and ignore the result. Retrying `connect` is safe for Unix sockets, the
only sockets that this program opens: an interrupted `connect` leaves them
unconnected, while a TCP connection attempt would go on.

**Complete short transfers.** A read or write of a given number of bytes,
such as a block, continues when a call moves fewer bytes, until all the
bytes have moved or a read returns 0 at EOF. One call on Linux moves less
than 2 GiB, and a block can be larger.

**Wait on non-blocking standard descriptors.** A standard descriptor
shares its open file description with other processes, and one of them
can make it non-blocking. Then a read or a write that would wait fails
with `EAGAIN` instead. Each role reads and writes its standard
descriptors as blocking ones: after `EAGAIN`, it waits with `poll` until
the descriptor is ready, and tries again. A wait that also watches other
descriptors, such as that of the receiver's stdin reader (section 6.6),
goes back to its own `poll`.

**Join threads at the normal end.** At a normal end, every thread that
`hashsync` starts has ended and has been joined, except the receiver's
signal thread (section 6.8). A fatal end joins nothing (section 6.9).

## 4. The receiver's index

### 4.1 What gets indexed

- Only regular files, without following symlinks.
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
- Skip reads of blocks entirely inside sparse-file holes (section 4.5).
- For files at least `block_size`, retain inode and path rows and mark the
  inode complete after successful processing, even if all complete blocks
  are skipped as repetitive or holes, with or without a partial tail. This
  avoids rehashing unchanged files on every update.

### 4.2 Roots and change detection

Roots come from `config.toml`; a glob match can be a directory or a single
regular file. Root and exclusion globs use `gix-glob`'s raw `wildmatch`
interface in pathname mode, with case-sensitive matching on bytes and
leading dots included. `*`, `?`, and bracket classes cannot match `/`; `**`
spans path components. Patterns and filenames may contain non-UTF-8 bytes,
which section 7 spells in configuration; never reject them for that reason
or convert them lossily. Thus `/nix/store/*` also matches
`/nix/store/.links` and non-UTF-8 names.

`gix-glob` supplies matching, not filesystem expansion. Expand root
patterns by walking directories relative to open descriptors, with the
no-symlink rules of section 4.5, and detect directory cycles during the
expansion.

Resolve roots for every update, run (including `--no-update`), and prune.
After exclusions, reject duplicate or nested absolute paths before changing
the index. This check is lexical; mount-alias overlap is detected when
directory identities repeat during actual traversal (section 4.5).

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
to prove glob equivalence. Conflicting policies are a configuration error,
including for completed groups and `--no-update` runs; reject them before
changing the index. The user is responsible for making absolute exclusions
select corresponding content in every copy.

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
   remove the paths of every root without an identifier: the `paths` rows
   of those roots form key ranges between the roots that keep theirs, and
   each transaction removes the rows that it scans with one range
   deletion. The `inode_paths` entries of those rows, whose keys start
   with the inode ID, go one by one (section 10).
3. In one transaction, remove the rows of those roots and of incomplete
   inodes. If no root keeps its identifier, the same transaction removes
   every path, with one range deletion in `paths` and one in
   `inode_paths`. Preserve complete inodes, including orphans, for reuse.
   Retain completed immutable groups still configured. Keep their
   representative path if still present, otherwise choose any current
   location. Create rows without identifiers for roots that need
   scanning. Two root rows never share a path, also when roots exchange
   paths.

Schedule scan tasks for every mutable root and one chosen location per
unfinished immutable group. Completed groups and unchosen copies have no
scan tasks. After all scan tasks finish, publish immutable identifiers only
for groups whose traversals and dependent inode and path results succeeded
and committed (section 4.6). A failed chosen copy leaves the traversal
unfinished; other copies remain available for candidate reads. After
processing all results, remove every orphaned inode and its blocks.

The same reconciliation recovers interrupted updates. Renames and identifier
changes can reuse complete inodes with unchanged identity, size, and ctime.
An inode reached through both mutable and immutable roots is checked by the
mutable scan; interrupted or abandoned rehashing invalidates its immutable
roots before deleting dependent paths.

The user is responsible for immutability: a completed root can omit files
added later. Copies may have different inode identities; unequal content
fails verification. `prune` takes exclusive index access, hashes no files,
and removes a path only when it is absent from every current copy.
Inaccessibility does not prove absence. When a root is no longer configured
or is confirmed absent from every copy, remove its immutable completion
marker, then its paths, then its root row, and then the orphaned inodes and
their blocks. The marker must not survive the removal of any path, so a
root that later returns is scanned again. An existing root with no indexed
paths is still valid.

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

Obtain metadata and handles from the initial `O_PATH` descriptor
(section 4.5). `FS_IOC_GETFSUUID` requires a readable descriptor instead:
reopen the verified object as in section 4.5, and never issue ioctls on
`O_PATH` descriptors. A process resolves the filesystem of each
`(stx_mnt_id, stx_dev_major, stx_dev_minor)` once, and caches it for the
entries with that triple. A mount ID alone misses btrfs subvolume changes
and is not a persistent identity.

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
  digest. Paths and identifiers are byte strings, preserving non-UTF-8
  bytes.
- An **identity** is the length of the filesystem ID as one byte, the
  filesystem ID, and the inode handle (section 4.3).
- Metadata names are `schema_version`, `block_size`, `weak_hash_id`,
  `strong_hash_id`, and `index_id`. An index whose metadata does not match
  requires an in-place rebuild. Read-only runs report that an update is
  required for a missing or incompatible index.
- `index_id` is a random 128-bit value created with the schema and renewed
  on rebuild. It partitions caches, not list content; there is no index
  generation counter.
- An identifier length of 0 means no identifier (section 4.2):
  identifiers are never empty.
- An inode's state is 0 while processing is incomplete, and its size is
  then 0. Otherwise the state is 1 and the size is `stx_size`. Stored sizes
  are compared only for equality.
- No column family orders blocks by weak hash; the export sorts the weak
  hashes in memory (section 5.4).
- IDs start at 1. A new row gets an ID larger than all IDs in use.
  Removal takes every entry that refers to a root or inode away before its
  row, so an ID used again finds no old entries.
- A rebuild, under process-exclusive access, removes every entry of every
  column family in one transaction, with one range deletion each, and
  stores new metadata. Never replace the database directory or its files.

A **transaction** is one atomic RocksDB write batch. It goes to RocksDB's
write-ahead log without sync. Power loss may discard recent transactions;
recovery then restores the state after an earlier one, which is acceptable
for an index of hints. Reads see committed transactions only, with one
exception. The threads of an update commit at the same time, and RocksDB
inserts each transaction into its memtables on its own (unordered writes);
only the write-ahead log keeps their order. A read can then see part of a
transaction that another thread is still inserting, so no thread of an
update reads rows that another thread writes (section 4.6). The end of an
update writes the memtables to the database files, so that read-only
processes have no write-ahead log to replay. It writes one column family
after the other, so that the last write also removes the old log.
Parallel writes end sooner, but can leave that log on disk until the next
modifying process opens the index. Then it waits for the compactions that
RocksDB has scheduled: closing the database would cancel them
(section 10). RocksDB keeps all files of the database open, so a process
raises its soft descriptor limit to the hard limit before it opens the
index.

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
  file, on a close-on-exec descriptor. Unlike a classic POSIX lock, it
  stays when another descriptor of the file closes, and at exit it goes
  only after RocksDB's own lock of its `LOCK` file, so a second read/write
  open never fails on `LOCK` (section 10). Wait indefinitely on contention.
- All threads of a process share one database handle. Reads of a
  modifying process see its latest committed transaction.
- Keep the lock until the database handle has closed, and keep both until
  index use ends. A normal `run` keeps the update's database open through
  the transfer, so there is no gap in exclusion between update and use.

### 4.5 Traversal and hashing

Resolve filesystem entries relative to open directory descriptors, without
following symlinks in any pathname component. Initially open each entry with
`O_PATH|O_NOFOLLOW|O_CLOEXEC`. Obtain its type and metadata with `statx` on
that descriptor, an empty pathname, and `AT_EMPTY_PATH`, checking which
fields were returned. Directory-entry types are only hints. Skip symlinks
and other types besides regular files and directories without marking the
root unsuccessful or opening them for I/O. Obtain the handles of regular
files and directories with `name_to_handle_at` on the same descriptor.

To hash or read a regular file, reopen `/proc/self/fd/%d`, where `%d` is
its still-open `O_PATH` descriptor, with `O_RDONLY|O_CLOEXEC`; to list a
directory, reopen it with `O_RDONLY|O_DIRECTORY|O_CLOEXEC`. This reopens
the verified object even if it has been renamed or unlinked, so path
replacement cannot switch the object between checking and reading it.
Never reopen its original pathname or the text returned by `readlink`.
Reopening deliberately follows the process's own procfs magic link: do not
apply `O_NOFOLLOW` or `RESOLVE_NO_SYMLINKS` to that step. It requires access
to `/proc/self/fd`. An `O_PATH` directory descriptor can itself serve as the
parent for relative opens. All descriptors are close-on-exec.

A root's scan task opens and identifies the root, and traverses that open
object. There is no separate root registration pass or close-and-reopen
step to compare identities. An identity observed during root expansion
imposes no continuity requirement on the scan.

Before traversing any directory, including a root, atomically insert its
`(filesystem ID, handle)` into the shared directory visited set. A
duplicate, such as a bind-mount cycle or alias, calls `die`, reporting both
paths, regardless of root or immutable grouping. Do not skip the
conflicting branch or publish new immutable completion markers;
interrupted-update recovery handles any work committed before the
detection. Retain entries until all scan tasks finish. A directory task
queues relevant children with its shared parent descriptor, each name once:
a listing can return a name twice when the directory changes meanwhile, and
the index keeps one inode per path.

Claim regular-file identities in a separate visited set (section 4.6).
Repeated files add paths to the first task's result regardless of root,
preserving all dependent roots and hard-link paths. Claims include small
files and files without useful blocks. Single-file roots use the same file
handling; no separate directory-entry bookkeeping is required to detect
overlap beyond the lexical root-path checks.

Overlap detection is best-effort. Root expansion has its own cycle detection
and does not populate the scan's visited sets. Completed immutable groups
and unused alternate copies are neither opened nor scheduled solely for
overlap detection, so mount aliases involving them can go undetected.

Read a new or changed regular file with `pread`, and hash useful complete
blocks until the file ends, even past the size observed before hashing. A
file that changes while it is hashed is not abandoned: the stored size and
ctime are the ones observed before hashing, so the next update rehashes it.

Attempt `SEEK_DATA`/`SEEK_HOLE` for every file being hashed. Allocation
counts cannot prove absence of holes. Skip only blocks wholly inside a hole,
read partial overlaps in full, and treat `SEEK_DATA` returning `ENXIO` as
no further data before EOF. Unsupported seeking (`EINVAL` or `EOPNOTSUPP`)
falls back to reading without hole discovery. When `SEEK_HOLE` contradicts
the preceding `SEEK_DATA`, after concurrent truncation or hole punching,
read one batch at the current unread offset and resume seeking after it:
hashing makes progress and keeps seeking enabled. Other seek errors abandon
the file. Reads come in batches of up to 1 MiB, rounded down to whole
blocks, at least one block. The **hole window** is 128 KiB, rounded down
to whole blocks, at least one block. A read that stops at a hole and is
shorter than the hole window extends to the hole window when more data
starts within it, instead of seeking around the small holes; accept this
bounded read amplification instead of examining every hole. Use separate
SEQUENTIAL and NOREUSE cache-advice calls as optional hints.

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

One dedicated work-stealing `rayon` pool walks and hashes, by default with
`4 × cores` threads, clamped to 16–64 (section 7). Tasks are files or
directories; local depth-first scheduling keeps pending work and open
parent descriptors small, while stealing distributes subtrees. Tasks retain
a shared parent descriptor and open their entry only when scheduled. Raise
the soft descriptor limit to the hard limit at startup. The database
handle and the directory and file visited sets are shared.

The scan tasks write their index changes themselves. Each thread of the
pool collects the changes of its tasks in a transaction of its own, which
commits when it reaches the bound of a transaction, before the thread
publishes an inode row that other links depend on, and after all tasks have
finished. A large file never requires a whole-file transaction. Only the
owner of a file changes its inode, so the owner reads committed rows of its
inode without a commit first, except when Abandon removes the rows that its
own transaction wrote. No thread reads rows that another thread writes
during the scan, as unordered writes require (section 4.4).

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
| Hashed | Mark the inode incomplete with the ctime observed before hashing, which may commit before old blocks are removed. Remove old blocks in bounded transactions, then add bounded batches of block number, weak hash, and strong prefix rows. A new identity gets a new row, with an ID from a counter that all threads share. Only success stores the size, with the last block rows and the owner's path, which marks the inode complete even if no block rows were added; then commit and publish Indexed. |
| Small | Remove the inode's existing rows with its paths and blocks. |
| Abandon | Commit, invalidate referring immutable roots, remove the paths and partial inode data, and publish Dropped. |

When the owner publishes Indexed, it adds the paths that waited; a later
link adds its own path. A path therefore commits only after its inode row.
A link that finds Pending waits even if an old complete row exists. Every
claimed file ends Indexed or Dropped. A failure makes the roots of all
waiting and later paths unsuccessful. Do not retry an abandoned inode
through another hard link during the same update.

Tasks mark failed roots, which stay remembered until the scan ends. Once
all scan tasks finish and every thread's transaction has committed,
publish the identifiers of the successful immutable groups (section 4.2).
This prevents a group from becoming reusable before a later task detects a
directory overlap. Then the threads of the pool remove all orphaned inodes
and their blocks in bounded transactions, including orphans left by earlier
interrupted cleanup. They share this work by ranges of inode IDs: a thread
reads only entries whose keys start with an inode ID in its ranges, so no
thread reads rows that another thread writes.

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
in the index and in HELLO. A change to any of them makes a new weak hash
with a new ID, not a new protocol version (section 11).

### 5.2 `mix64`: the bit mixer

The splitmix64 finaliser distributes the polynomial hash's structured bits
for bucket selection, fingerprints, and gap coding. It is a bijection, so it
does not introduce or remove polynomial-hash collisions. It does not roll.

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

### 5.3 Strong hashes

Initially support `blake3` (default) and `sha256`, both with 32-byte outputs.
Each supported algorithm has one fixed output length `S >= 16` bytes.
Configuration cannot truncate digests or choose an arbitrary XOF length.
Changing the algorithm rebuilds the index.

Treat full strong-hash equality as byte equality. Only the index stores
16-byte prefixes. Queries, content caches, and list IDs use full digests.

### 5.4 The hash list

Read the weak hashes of all block rows from one database snapshot, with
one thread per core, and sort them in memory as unsigned 64-bit values,
with the same threads. Remove duplicates and encode canonically:

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
format covers it on disk (section 5.6). The list ID is the strong
algorithm and full digest of those bytes. Export on every run; there is no
generation-based shortcut.

### 5.5 The sender's hash table

Use a custom open-addressing table of 32-bit fingerprints in a flat array:

- For `N = 0`, allocate no buckets and return a miss for every lookup,
  without computing a bucket address. The remaining rules apply to `N > 0`.
- A bucket contains 16 slots, occupying one cache line.
  `buckets = ceil(N / (16 * 0.7))`.
- Map `v` to `high64(v * buckets)` using a 128-bit product. Full 64-bit
  list values keep bucket selection and fingerprints sufficiently
  independent.
- The fingerprint is `v`'s low 32 bits, replacing zero with one.
  Zero slots are empty.
- Insert in the first empty slot, continuing to the next bucket when full.
  Lookup stops at the fingerprint or an empty slot and continues only
  through full buckets. It may return false positives, never false negatives.
- A second flat array holds the high 32 bits of the value of each slot, at
  the same bucket and slot. Lookups do not read it. The check of a value
  stops at a slot that has both the fingerprint and the high bits of the
  value, or at an empty slot. Like a lookup, it continues only through full
  buckets. Values whose low 32 bits are 0 and 1, with equal high bits,
  share a fingerprint, so the check does not tell them apart.

A lookup usually reads one cache line and compares about 13 fingerprints,
so about 3 windows per GiB of new output are false positives. Before it
computes a strong hash, the scanner checks each hit against the whole
value, which reads one more cache line (section 6.3): a false positive
costs no strong hash, no query, and no rescan.

### 5.6 The hash list cache and direct deltas

Full lists are immutable and named by content. Account and index IDs
partition cache directories; they do not identify list content. Compute a
direct delta from any available base, without generations, delta logs, or
delta chains.

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

For both roles, a failed cache directory or file operation ends the run
with `die`. This covers every operation, including publication, renewal,
the removal of listed and invalid files, and expiry, with three
exceptions:

- Absence is normal. An absent cached list is a cache miss. Missing cache
  directories during lookup or cleanup, and files that disappear during
  lookup or are already gone when removed, are not failures. Failures to
  create or publish a required file remain fatal.
- Unexpected cached file contents are a cache miss: remove the file and
  continue without it. This includes malformed or truncated wrappers or
  list encodings, unsupported formats, and list bytes whose digest does not
  match the list ID in the file name. An I/O error while reading a file is
  fatal; successfully reading invalid contents is not.
- A failure to remove an empty peer/index directory only prints a
  warning, and the directory stays. A directory that is no longer empty,
  or that is already gone, is normal and gets no warning.

Validation failures in lists received over the protocol follow the
exchange rules below.

**Exchange.**

A side's *listing* is the names of the lists and hidden temporaries in its
peer/index directory when it lists the directory, with an open file for
each list. The name of a hidden temporary starts with `.hashsync-`
(section 11). Other hidden names are not part of the cache layout, and
only expiry removes them. For example, an NFS client gives such a name to
a removed file that is still open, and the removal of that name fails
while the file is open.

1. Export and hash the target from one database snapshot. Once the sender's
   account is known, the receiver lists its peer/index directory. With
   sender caching enabled, it then durably publishes the target there
   before sending list data, and retains bytes or an open fd. Failure to
   persist the receiver's target ends setup.
2. Before it sends HELLO, the sender lists its peer/index directory. With
   sender caching enabled, it advertises the IDs of all listed lists of
   the run's strong hash, usually one; otherwise none.
3. If the target is one of the advertised lists, the receiver uses
   HASHKEEP. Otherwise it searches all its `local-hashlists/*/*.cache`
   directories for the advertised lists, in their order, and verifies the
   first one that it finds. A missing or damaged base uses HASHLIST. With a
   base, merge sorted sets and encode removed and added values as lists;
   use HASHDELTA, which names the base, only when smaller than the full
   transfer.
4. The sender reads the list that HASHDELTA or HASHKEEP names through the
   open file of its listing, and verifies it. It decodes or applies the
   delta, re-encodes canonically, and verifies the full target digest. An
   invalid listed list or a bad delta requests HASHLIST once via HASHFULL;
   the result of a bad delta is never published or used for the table. A
   bad full list is a protocol error.
5. The sender builds the table, durably publishes the verified target
   when sender caching is enabled, ends its exchange, and only then
   replies CACHE_RESULT.
6. On CACHE_RESULT, the receiver ends its exchange. Without CACHE_RESULT,
   it removes nothing.

To end its exchange, a side renews its published target, and the receiver
also the base that it used. It removes the files of its listing except
its published target, and then expires the cache (below). The removal of
listed files touches no other peer/index directory.

`sender_cache = false` advertises no list, and neither side publishes one,
so the end of the exchange removes every listed file. The negotiated
setting determines whether each side publishes; no storage-success flag is
needed.

**Publication, concurrency, and expiry.**

Publish complete files using a unique hidden temporary, file fsync, rename
to the content-derived name, and directory fsync. Make new directory
hierarchies durable too. Republish even an existing target ID rather than
trusting its filename; HASHKEEP can copy its retained verified bytes.
Published files are never edited in place. When the temporary cannot be
created because its directory is missing, create the directory and try
again: expiry can remove an empty directory at any time, also just after
its creation. Once the temporary exists, the directory is not empty, and
expiry cannot remove it. A directory that is gone at the fsync after the
rename means that a concurrent cleanup removed the file: this is normal
absence.

No cache locks or mutable current-list pointer are needed. Retained fds or
bytes survive replacement and unlink. Tolerate disappearance during directory
scans and fall back to a full transfer. Cleanup racing with replacement or
removal of a published file may lose a future cache hit; it cannot invalidate
retained bytes. A concurrent cleanup can remove a temporary before its
rename: treat this as normal absence, with the same effect as removal of
the published file just after publication. With sender caching enabled,
acknowledgement promises completed publication, not immunity from later
deletion.

A run with another target starts only after this run's receiver has ended
(section 4.4). The sender lists its directory before it sends HELLO, and
ends its exchange only after HASHLIST, HASHDELTA, or HASHKEEP, which its
receiver sends only after it received that HELLO. So no exchange that ends
has a listing that holds the target of a later run, also when a sender
outlives its failed receiver. A later run can still publish a listed list
again under the same name, when its index has returned to an earlier
state. So a side removes a listed list only if its name still names the
listed file: it moves the name to a new temporary name, compares the file
there with its open file, removes it if they are the same, and moves any
other file back. A listed temporary belongs to an earlier run, or to a
concurrent run with the same target, which then publishes nothing: normal
absence.

The end of an exchange and expiry can remove the base of a concurrent
exchange at any time. So each side reads its base once, through one open
file, verifies the bytes that it read, and uses only these bytes, or that
file, until the end of the exchange: the sender through the file of its
listing. Neither side opens a list by name a second time.

Touch verified files through their open descriptors after successful use.
Use `ctime` as the renewal marker. To expire the cache, a side removes
every file in every peer/index directory of both cache trees whose `ctime`
is older than `hashlist_max_age` (default 30 days, sent in HELLO): lists
and abandoned hidden temporaries. Then it removes every peer/index
directory that is empty.

A lost acknowledgement or a failed run can leave extra files until the
next exchange of that peer and index, or until expiry. Database restore or
rebuild needs no cache repair or publication checkpoint because lists
identify their own content.

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
requested them. An explicit compression option in the configured command
still takes precedence. SSH options must keep the sessions in the
foreground, preserve their stdin and stdout, and execute the specified
remote command.

SSH executes its remote command through the remote login shell, which must
support POSIX shell quoting for this bootstrap. Treat `sender_command` as
one executable name or path, not a shell fragment. Construct a remote
`exec` command by single-quoting each argument, replacing embedded single
quotes with `'\''`, and preserving all other bytes, including non-UTF-8
bytes. Passing separate local argv elements to SSH does not quote them for
the remote shell. The sender needs a compatible `hashsync` in PATH or at
`sender_command`.

The receiver generates token `T` and starts both sessions together, with
the signal mask that the receiver started with (section 6.9):

- Data: `SSH HOST hashsync serve --token T`.
- Control: `SSH HOST hashsync attach --token T`.

`serve` first listens on `$XDG_RUNTIME_DIR/hashsync/T.sock` (fallback: a
private directory under `/tmp`), mode 0600. It exchanges protocol versions
and data HELLOs, accepts `attach`, and completes the list exchange and
CACHE_RESULT before starting the command.

`attach` retries a not-yet-listening socket while its stdin remains open,
with no deadline. It relays stdin to the socket and the socket to stdout,
half-closing the socket on stdin EOF. Socket EOF or a peer-closed socket
error (`EPIPE` or `ECONNRESET`) on a read ends the relay from the socket.
`attach` then waits until stdin EOF ends the relay from stdin, joins that
thread, and exits zero. After a peer-closed error on a write, the relay
from stdin writes nothing more to the socket, but reads stdin to EOF and
discards the bytes. This prevents a deadlock: the receiver closes the
control session's stdin only after its control writer has ended (section
6.9), and that writer could otherwise block on a full pipe. Either
direction may observe closure first. The receiver decides from its
completion state whether a closure was expected; other relay failures end
the relay unsuccessfully. Rendezvous waits must also observe receiver
disappearance (section 6.9).

The command's argv travels in HELLO, not on the SSH command line. Find
`argv[0]` in PATH and start the command without a shell, so a shell
requires an explicit `sh -c`. Start it with piped stdin, stdout, and
stderr, its own process group for cleanup, and the signal mask that
`serve` started with (section 6.9).

The **stderr relay**, a sender thread, copies command stderr to `serve`'s
stderr with bounded buffering and prompt writes, independently of stdout
scanning and control traffic. SSH forwards these bytes through its stderr
channel; both local SSH subprocesses inherit the receiver's stderr. Stderr
is outside the hashsync protocol and its compression streams. The relay
finishes only after command stderr reaches EOF and all copied bytes have
been written and flushed to SSH; successful sender exit requires that
(section 6.9).

### 6.2 Protocol version, features, and frames

Each side sends `protocol_version u32`, little endian, as the first bytes of
each session. It does not wait for the peer's version before it sends its
own. It reads the peer's version before anything else, and it ends the run
with `die` if the versions are different; the message names both versions.
The sender starts the command only after it has checked the receiver's
version on both sessions. The remote commands of section 6.1 and this
version exchange never change. Everything after the exchange belongs to one
protocol version (section 11).

Directly after its version, each side sends `features u64`, little endian,
also without waiting for the peer's. It has one bit for each optional
feature of the protocol that the side supports. A side uses a feature only
when both sides have its bit, and it ignores the bits that it does not
know. When a run needs a feature that the peer does not have, for example
for an option that the user gave, the side that needs it ends the run with
`die`. No features exist yet, so both sides send 0.

Every frame is `type u8, length u32, payload`. Integers are little endian.
Strings and byte strings have a `u32` byte length; lists have a `u32` item
count. Output offsets and lengths are `u64`; overflow is a protocol error.

`S` is the negotiated strong algorithm's full digest length. Every digest of
the run uses that algorithm, including all list IDs, so a list ID travels
as its digest alone, `[u8; S]`. A run has one target and at most one base:
the receiver's HELLO names the target, and HASHDELTA names the base, one of
the lists that the sender's HELLO advertises.

| Session | Direction | Types |
|---|---|---|
| data | receiver to sender | HELLO, HASHLIST, HASHDELTA, HASHKEEP, INPUT, INPUT_END, END_ACK |
| data | sender to receiver | HELLO, HASHFULL, CACHE_RESULT, LIT, REF, RLE, END, DONE |
| control | sender to receiver | QUERY, INPUT_STOP |
| control | receiver to sender | ANSWER |

Payloads:

- HELLO, receiver to sender: `block_size u32`, `weak_hash_id string`,
  `strong_hash_id string`, `target [u8; S]`, `account_id [u8; 16]`,
  `index_id [u8; 16]`, `sender_cache u8`, `hashlist_max_age u64` (seconds),
  `compression_level i32`, `compression_long u8`, `compression_threads u32`,
  `probe_threads u32`, `lookahead u64`, `negative_cache u64` (bytes),
  `sender_output_spool_limit u64` and `sender_input_spool_limit u64` (bytes;
  `2^64 − 1` for `unlimited`), `output_delivery u8` (0 for eager, 1 for
  batch), and `command`, a list of argv byte strings.
- HELLO, sender to receiver: `account_id [u8; 16]`, and `lists`, a list of
  the IDs `[u8; S]` of the advertised lists (section 5.6).
- HASHLIST: `length u64`. That many encoded bytes of the target follow the
  frame directly.
- HASHDELTA: `base [u8; S], removed_length u64, added_length u64`. The
  removed and added encoded lists follow the frame directly. They change
  the base, one of the advertised lists, into the target.
- HASHKEEP: empty. The target is one of the advertised lists; no data
  follows.
- List bytes after HASHLIST and HASHDELTA are not frames. Enforce exact
  lengths and canonical encoding, including ordering and padding.
- HASHFULL: empty. Request a full list once when the listed list that
  HASHDELTA or HASHKEEP names is invalid, or after a delta digest
  mismatch, before command output (section 5.6). The response is HASHLIST.
- CACHE_RESULT: empty. The sender's single acknowledgement of the target
  (section 5.6). A repeated one is a protocol error. The receiver sends the
  input only after it.
- LIT: literal bytes.
- REF: empty; use the pin at the current output offset.
- RLE: `byte u8, length u64`.
- END: `status i32, error string`. Status is the command's exit status,
  `128 + signal`, or 127 plus error text if startup failed. Of the
  sender-to-receiver data frames, only DONE follows.
- INPUT: the next bytes of the input stream, at least one (section 6.6).
- INPUT_END: empty. The input ended, at the EOF of what the stdin reader
  reads (section 6.6), and the input stream ended with the last INPUT:
  with compression, its zstd frame ended there. At most once; no INPUT
  follows it.
- END_ACK: empty. The receiver has validated END and compression completion
  and flushed reconstructed stdout. Sent exactly once; it is the receiver's
  last data-session frame.
- DONE: empty. The completion conditions of section 6.9 hold, and the
  sender needs neither session any more, so the receiver can close both
  session stdin pipes. The sender sends it once, after it has received
  END_ACK. It directly follows the compression stream, outside it. Nothing
  follows DONE: a byte after it is a protocol error.
- QUERY: `offset u64, strong [u8; S]`. At most one per offset; offsets may
  go backwards after rescanning.
- INPUT_STOP: empty. The sender stopped the input, and the receiver stops
  it too when it has not stopped it at END yet (section 6.6). At most
  once, after CACHE_RESULT; the receiver can read it before it reads
  CACHE_RESULT on the data session, and also in Completing.
- ANSWER: `offset u64, found u8` (0 or 1). Answers may arrive out of order.

The receiver's HELLO sets the block size, weak hash, and strong hash of the
run. The sender uses them, or it ends the run with `die` before command
startup if it does not support them. It checks the strong hash before it
reads the target, whose length depends on it. Its HELLO does not repeat
them.

The data handshake is the version exchange, the features, receiver HELLO,
sender HELLO, durable receiver publication with sender caching, then
HASHLIST/HASHDELTA/HASHKEEP and its data. The sender replies with
CACHE_RESULT, possibly after one HASHFULL fallback. The receiver handles the
result and cache cleanup before entering its output loop. Its remaining
data-session frames are the input, in INPUT frames and INPUT_END, until the
stop of the input, and then END_ACK.

After HELLO, all sender-to-receiver data-session frames through END share one
zstd stream, unless compression is disabled. This includes HASHFULL and
CACHE_RESULT. DONE follows the stream, outside it. The sender flushes the
stream before it waits for a reply, after HASHFULL, and after
CACHE_RESULT, which the input waits for: the command can wait for its
input before it writes output. In eager delivery of the output, the data
writer also flushes the stream after each idle point of command stdout,
once it has written the frames of the bytes before the idle point and
has nothing left to write (section 6.3): before that, the bytes could not
leave sooner, and the flush would only cost compression. It finishes the
stream after END.
Other frames can stay in the compressor and the transport buffer until
more output fills them or the stream ends. No wait of `hashsync` depends
on them: queries and answers use the control session. Compression
settings are in section 7.

**Delivery.** The run gives the output and the input each a delivery
(section 8). Eager delivery, the default, works as SSH does: a command
that waits for its next input before it writes more output, and a
consumer of the output that writes the next input only after it has read
that output, such as the two ends of `rsync -e`, never wait for bytes that
a buffer of `hashsync` holds. It sends the bytes of an idle point on only
when they can leave at once: when the writer of their session has nothing
left to write. On a slow link, that writer is busy, so the bytes wait,
and the bytes that follow join them: the output keeps its matches, and the
streams compress better. So the link decides how much eager delivery
costs. Batch delivery keeps the bytes until a buffer is full or the
stream ends: it costs fewer bytes and less CPU also on a fast link, and
keeps the matches that eager delivery loses at idle points there (section
10). The receiver's HELLO names the delivery of the output, whose idle
points are those of command stdout (sections 6.3 and 6.5). The receiver
applies the delivery of the input alone, at the idle points of its stdin
(section 6.6).

After the features, each direction of the control session is also one
zstd stream at the same level, unless compression is disabled. The
frames are small, so the stream uses no long-distance matching and no
worker threads. A control writer flushes its stream each time its queue
becomes empty, because the scanner waits for answers, and the receiver
must stop sending the input as soon as the sender has stopped it. When a
slow link makes the writer wait, more frames share each flush and
compress better. No frame ends the stream: the closure of the session ends
it (section 6.9).

The frames of the receiver-to-sender direction of the data session are not
compressed: zstd cannot make the hash list smaller (section 10), and the
input stream has a zstd stream of its own, in the payloads of INPUT frames,
so the sender can find END_ACK after input that it discards without
decompressing it (section 6.6). Each frame goes to SSH in one write: SSH
sends what one read of its stdin returns in packets of its own, so a header
that arrives alone, before its payload, can cost a packet.

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
- `f`: in eager delivery of the output (section 6.2), the end of the
  bytes before the last idle point of command stdout, initially zero, with
  `f <= r`. The output reader passes each idle point that the spool
  reports after the bytes before it, and the scanner then sets `f = r`.
  The bytes before `f` leave without waiting for more output, once they
  can leave at once.
- The data writer is **idle** when it waits for frames, after it has
  written every frame that the scanner queued, and made the flush that an
  idle point asked for. It tells the scanner when it becomes idle.
- **Records** describe scanned output that is not emitted yet. Ordered
  records cover exactly `[e, p)`, without gaps or overlaps:
  `Literal { offset, length }`, `Run { offset, length, byte }`,
  `Query { offset }`, and `Ref { offset }`. Queries and references each
  cover `B` bytes.
- The query-state map is keyed by offset and stores the full digest with
  state Not_sent, Waiting, Yes, or No. Keep entries until their offsets
  fall below `e`, even when their records are discarded.
- A byte-bounded negative LRU cache (`negative_cache`) holds full strong
  hashes answered no. It suppresses queries for the same content at other
  offsets, but does not avoid hashing a new window.

A Query blocks emission of itself and every following record. References
require a yes for their exact offset; equal hashes at different offsets do
not share permission to emit REF. A last Literal or Run can grow, and its
ready prefix can be emitted before it stops growing.

**Scanning.**

Each iteration processes available answers, resolves Query records in
offset order, and emits ready records before scanning more bytes.

With a full window at `p`:

1. If all `B` bytes are equal, append or extend a Run and advance by `B`.
2. Otherwise reuse the query-state entry at `p`, if present. If absent,
   probe the weak table, and check a hit against the whole weak hash
   (section 5.5). A miss of either records one literal byte. A hit of both
   is a weak hit: it computes the full strong hash and creates a Not_sent
   entry.
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

Before EOF, when `p < f` and fewer than `B` bytes follow `p`, the bytes
from `p` to `f` become such a tail, so that they leave at once, when two
conditions hold: every record before them has left, except a last Literal
shorter than `B`, and the data writer is idle. Until then, they wait for
more output, as the bytes from `f` on do. While the data writer is busy,
as on a slow link, output that arrives meanwhile can fill their windows.

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
constant window, or a hit that the check rejects, neither doubles nor
shortens the next stretch. Short gaps between matches thus stay on one
thread, and a long run of misses soon uses all probe threads. HELLO's
`probe_threads` sets the thread count; 0 selects half the sender's cores,
at least one.

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
  record follows, at EOF, or, once `p >= f`, for its bytes before `f`;
  otherwise let it grow.
- After a change of `f`, once `e >= f`, queue a flush of the data stream
  for the data writer, behind the frames before `f`. The data writer makes
  it once it has nothing left to write (section 6.2).

Advance `e` only after the data writer accepts the frame. LIT owns a copy
before those buffer bytes are released. Remove emitted record portions and
map entries below `e`.

Outgoing data is bounded, but queueing it must not block the scanner from
processing answers. Outgoing QUERY messages use an unbounded, nonblocking
queue and remain ordered even when offsets go backwards. Discarded records
can leave obsolete requests; lookahead is not a bound on all outstanding
queries or receiver pins.

When progress requires command output, answers, data-queue space, or, while
a tail before `f` waits, an idle data writer, wait on notifications for
those events, continuing to prioritize answers.
Backpressure propagates from the data writer through the lookahead buffer
to the spool of command stdout (section 6.5).

Finish only after stdout EOF, `p = r`, and no Query records remain. Drain all
records so `e = p = r`, and queue the flush of the last idle point, which
can come after the last frame: END can come much later than stdout EOF,
and the bytes before that idle point must not wait for it. END can be
queued once the command's exit status is also available (section 6.9).
Flush queued data before waiting for further completion conditions.
Obsolete unanswered requests do not delay completion.

### 6.4 The receiver's answering and reconstruction

Let `B = block_size`. The pins are one user of segments, with extents of
`B` bytes (section 6.5). The receiver keeps:

- `c`: the output offset of reconstruction.
- The pin map, from the query offset of each pin to its extent and the
  full strong hash of its block. Each pin has its own extent, including
  pins with identical content; extents are independent of output offsets.
- The open segments of the pins.
- A bounded 256 MiB LRU cache of verified buffers, keyed by full strong
  hash, which can serve candidate lookups. It holds at least one buffer,
  also when `B` is larger than 256 MiB. Verified reads populate it; its
  eviction is independent of pin lifetimes, and pins never retain its
  buffers.

One lock protects this state and the completion state. Source reads,
segment I/O, and stdout writes occur outside the lock; the file creation
of step A1 is the one exception, and it occurs once per segment. In-memory
payload storage is limited to the cache and worker/reconstruction buffers;
pin metadata and queued requests still scale with outstanding work. The
receiver releases the extent of a pin when nothing uses it any more: the
output has passed the pin, or its lookup worker has found its write
obsolete or has finished it in Completing. The receiver does not reject a
match merely because many pins exist, and pin release requires no
protocol message.

A pool of 4–8 lookup workers, which share the database handle, answers
queries out of order. Queue all answers nonblockingly under the lock,
checking the completion state before publication. Completing (section 6.9)
suppresses new lookups, extents, pin publication, and answers, including no
answers from obsolete lookups.

For `QUERY { offset: x, strong: h }`:

1. Under the lock, check `c`. If `x < c`, answer no without a pin.
   Otherwise try the verified-buffer cache.
2. If uncached, select blocks matching `h[0..16]` through `strong`. Each
   path of a block's inode gives one candidate; a block whose inode has no
   path is one candidate too. Try candidates in any order. Repeated
   location/offset pairs need only one read.
3. Try at most `candidate_attempts` candidates (default 128, positive). Try
   each candidate at every current location of its root until one verifies:
   all copies of an immutable group are searched, and together they use one
   attempt. A candidate without a usable location, or whose reads all fail,
   also uses one; stale rows must not evade the budget.
4. Open a single-file root directly. Otherwise open its root directory and
   resolve the relative path beneath it without symlinks (`openat2` with
   `RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS`). Use the `O_PATH` classification
   and procfs reopening rules of section 4.5: check that the opened object
   is a regular file before obtaining its readable descriptor. Close
   descriptors before trying the next location.
5. Read exactly `B` bytes at `block_no * B`, and verify the full digest of
   that buffer. Errors, early EOF, and mismatches try the next location or
   candidate; filesystem failures are misses, not fatal. Alternate copies
   need not share the indexed inode identity.
6. If nothing verified, answer no. Otherwise recheck `c` under the lock. If
   `x < c`, answer no without an extent. If still needed, allocate an
   extent under the lock (section 6.5, steps A1 to A3).
7. Outside the lock, write the `B` bytes of the verified buffer to the
   extent with `pwrite`. Segments are open without `O_APPEND`; concurrent
   workers write only their assigned, nonoverlapping extents.
8. Reacquire the lock and recheck `x < c`. If now obsolete, leave the extent
   unpublished, release it (section 6.5, steps R1 to R3), and answer no.
   In Completing, release the extent without an answer. Otherwise publish
   `{ extent, strong: h }` at `x` and queue yes.

A yes is binding because the receiver owns an independent copy of the
verified bytes in a segment. A later source-file change cannot affect
REF. Output may pass `x` and release its extent before the answer is sent;
the sender then discards that obsolete answer.

Reconstruction reads the data session from its spool (section 6.5). Until
`receiver_output_spool_limit` bytes of the compressed session wait there,
a blocked stdout stops only the reconstruction: the sender keeps scanning
and querying, and the spool and the pins hold the output until stdout
takes it. The limit does not bound the pins. Reconstruction processes the
data frames in order at `c`:

- LIT writes its payload; RLE writes the repeated byte. Accept any positive
  RLE length, though the sender emits runs of at least `B`.
- REF obtains the pin at `c` under the lock. Outside it, read exactly `B`
  bytes from the pin's extent with `pread`, and verify the full digest
  against the pin's stored hash before writing that buffer to stdout. Keep
  the extent in use until the write completes and `c` advances. A missing
  pin is a protocol error; segment EOF, read errors, or a digest mismatch
  end the run with `die` without emitting that block or attempting to
  reread the source file.
- After each frame's output is written, advance `c` under the lock. In
  bounded batches, detach pins whose query offsets are below it, including
  pins covered by literals or runs, and release their extents (section
  6.5, steps R1 to R3). After each batch, release the lock and close the
  files of the segments that the batch removed from the open segments.
- END carries the command status into shutdown (section 6.9). EOF without
  END or before the compressed stream is complete is failure.

The shared lock makes pin publication and output advancement atomic relative
to each other: a lookup either publishes before the offset passes and has its
extent released, or observes the obsolete offset and releases its extent.
No extent is released while its write or REF read is still using it, so no
segment is closed while one of its extents is in use.

### 6.5 Segments and spools

Both roles keep temporary data in segments: the receiver its pins
(section 6.4), and both roles the bytes of their spools. Closing an
unlinked file frees its storage without hole punching, so any filesystem
that can hold files can hold the tmp_data directory.

**Segments.**

Each **user** of segments, the pins or one spool, has its own open
segments and its own extent size: `B` for the pins, 1 MiB for a spool.
One segment of a user at a time receives the new extents, until it is
full. The user closes a segment that receives no more extents as soon as
nothing uses its extents. The more segments of a user are open, the more
bytes a segment holds before it is full, so the open files stay few.

Data structures:

- **tmp_data directory**: `tmp_data_dir` on the receiver. The sender
  always uses the default, `$XDG_CACHE_HOME/hashsync/tmp-data/` (fallback
  `~/.cache/hashsync/tmp-data/`) in its own environment; a
  `sender_command` script that runs
  `export XDG_CACHE_HOME=DIR; exec hashsync "$@"` moves it, with the
  sender's list cache (section 5.6). Use a private directory (0700). A
  role creates it if necessary and keeps it open through the run. When it
  opens it, it creates a segment and closes it at once, to make sure that
  segments can be created: the receiver before the SSH sessions start, and
  the sender when it has accepted HELLO, before the list exchange. So an
  unusable directory ends the run before command startup. A disk-backed
  directory moves the data out of RAM, where tmpfs would keep it in memory
  or swap. Avoid NFS: its client writes the data of a file to the server
  before it closes the file (section 10).
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

Closing a user closes all its open segments (section 6.9), and so does
process exit; these files need no fsync or crash recovery. Failures to
create the tmp_data directory or a segment, and failures of segment I/O,
including exhaustion of space, descriptors, or file-size limits, end the
run with `die`.

Properties, for each user:

- **Descriptors**: a sealed segment holds at least `16 MiB × 2^m`, where
  `m` sealed segments, all older, were open when it was sealed. So for
  any release order, at most `log2(R / 16 MiB + 1) + 1` segments are open,
  where `R` is the bytes that they hold: 13 for 64 GiB, 17 for 1 TiB, and
  never more than 40, because `pwrite` offsets are below 2^63. The
  tmp_data directory uses one more descriptor.
- **File creations**: at most one per 16 MiB of extents.
- **Retention**, when releases follow the allocation order, as in a
  spool: only the oldest open segment holds released extents, and no
  segment gets an extent after its size passes the bytes of the extents in
  use plus 32 MiB. If the oldest open segment is the current segment, it
  holds less than 16 MiB.
- **Accepted cost**: when the extents in use pile up, the last sealed
  segment holds about half of them, and its released bytes stay allocated
  until its last extent is released.

**Spools.**

The **writer** of a spool takes the bytes from the **source** of the spool
as they arrive, and appends them; it waits for the reader only at the
**limit** of the spool. The reader takes the bytes in order, and waits
only while no byte waits for it. For command stdout and the data session,
the writer is a thread of its own, the **thread of the spool**, which
reads the source; for the input spools, it is a thread of the input
(section 6.6).

| Role | Source | Writer | Reader | Limit |
|---|---|---|---|---|
| Sender | Command stdout | The thread of the spool | The output reader, for the lookahead buffer (section 6.3) | `sender_output_spool_limit`, from HELLO |
| Receiver | Data-session stdout | The thread of the spool | Reconstruction (section 6.4) | `receiver_output_spool_limit` |
| Receiver | The input stream, from stdin | The stdin reader | The input writer | `receiver_input_spool_limit` |
| Sender | The input stream, from INPUT frames | The data reader | The stdin writer | `sender_input_spool_limit`, from HELLO |

Each limit is 2 GiB by default. `unlimited` (`2^64 − 1`, which no spool
reaches) removes it; a spool can then grow to almost the whole output, or
the whole input, when its reader stays slower than its source
(section 10).

Data structures:

- **Spool extents**: the extents of 1 MiB of the spool's segments, which
  the writer allocates in stream order. The reader is in the first extent
  that is not released, and the writer appends to the last one.
- **Written** and **read**: the bytes that the writer has appended, and
  that the reader has taken. The bytes between them **wait**.
- **End**: the source ended, and the writer has appended all its bytes.
- **Stopped**: a role stopped the spool, before or after its end, because
  its bytes have no use any more. Only the input spools stop
  (section 6.6).
- **Idle point**: the thread of the spool of command stdout, in eager
  delivery of the output (section 6.2), marks one when its source has no
  byte ready. The end of the source is the last idle point. The next
  append removes it, so the reader reports an idle point only when no byte
  follows it.

The writer:

- **W1.** Wait while bytes wait and at least `limit` bytes wait, unless
  the spool is stopped. The reader wakes the writer when it takes bytes.
  With a limit of 0, the writer takes bytes from its source when no byte
  waits.
- **W2.** Take the next bytes from the source. For command stdout in
  eager delivery, when the source has no byte ready, first mark an idle
  point and wake the reader under the lock, then wait for the source. Do
  this also at the end of the source, unless the last read found no byte
  ready: the bytes before the end must not wait for the end of the
  command, which can come much later. At the end of the source, mark the
  end, wake the reader, and stop. A thread of a spool reads its source; a
  read error ends the run with `die`.
- **W3.** Append the bytes, one extent at a time. If there is no extent,
  or the last extent is full, allocate an extent (steps A1 to A3) under
  the lock. Write the bytes that fit in the last extent with `pwrite`
  outside the lock. Then add them to written under the lock, remove the
  idle point, if one is marked, and wake the reader. Repeat until all
  bytes are appended, then go to step W1. When the spool is stopped,
  discard the bytes that are not appended.

The reader:

- **T1.** Under the lock, wait until bytes wait, the end or an idle point
  is marked, or the spool is stopped. If it is stopped, report EOF. If no
  byte waits and an idle point is marked, remove it, and report the idle
  point, also when the end follows it. Else, if no byte waits and the end
  is marked, release the remaining extent, if one remains (steps R1 to
  R3), and report EOF. That extent is the last one, which is not full, so
  step T3 did not release it.
- **T2.** Read the waiting bytes of the first extent with `pread`, outside
  the lock.
- **T3.** Under the lock, add them to read, and wake the writer. If the
  reader has passed the first extent, release it (steps R1 to R3). Close
  the files of the segments that this removes after the lock is released.
  If the spool was stopped meanwhile, report EOF instead of the bytes.

A role stops a spool under its lock: it marks the spool stopped, and
wakes the writer and the reader. A stop is final, so the spool needs its
extents no more, nor the full rule: the stop takes all its extents and
open segments at once, without steps R1 to R3, which discards the bytes
that wait. Their files close after the lock is released, or after the
I/O of the writer or the reader that still uses one of them. The reader
can tell the EOF of a stopped spool from the end.

The reader releases the extents in allocation order, so the retention
property holds: with a limit, at most the limit plus one append (256 KiB)
wait, and the files hold less than twice the limit plus 40 MiB.

Cost: on XFS, a spool passed 1 GiB in 0.36 s, about the cost of the two
copies in memory of a channel of chunks. While the reader keeps up, a
segment can fill and close before the kernel writes back its dirty pages,
by default after 30 s, and nothing reaches the disk. A slow source, such
as the data session, keeps its current segment open longer, and the
kernel then writes its dirty pages to the disk.

### 6.6 The input

The receiver sends the input in INPUT frames on the data session, from
CACHE_RESULT until the stop of the input. Each role keeps it in an
**input spool** on the way (section 6.5). So the producer of the
receiver's stdin waits for a command that reads slowly, or late, only when
both input spools hold their limits.

The input stream is one zstd frame at the level and long-distance matching
of the data stream, or the input itself when compression is disabled. The
input spools hold the input stream, and their limits count its bytes. On
its way to the input spool, the stream passes a buffer of 256 KiB, which
goes on when the next bytes do not fit in it, or at a flush. In eager
delivery (section 6.2), the stdin reader flushes the stream at each idle
point of stdin, once the input writer is idle: the command can wait for
its input before it writes output. In batch delivery, it flushes
nothing. In both, it compresses with `compression_threads` worker threads
(section 7), as the sender's data stream does; in eager delivery, each
flush then ends a zstd job, which costs more bytes than the end of a
block (section 10). The EOF of stdin ends the zstd frame. The frames that
carry the stream are not compressed (section 6.2): a role can stop the
input after any frame, and the zstd frame then stays incomplete.

Data structures:

- **Input spool**: on the receiver, with the limit
  `receiver_input_spool_limit`; on the sender, with the limit
  `sender_input_spool_limit` from HELLO. The receiver's exists before the
  sessions start, so that INPUT_STOP can stop it at any time; the
  sender's exists from the sender's HELLO, before the command starts.
- **Sent**: the bytes of the receiver's input spool that the input writer
  has sent. The input writer is **idle** when it has sent every byte that
  the stdin reader appended to the input spool.
- **Wake-up**: an eventfd of the receiver, which the input writer signals
  after each INPUT frame, so that the stdin reader can wait for an idle
  input writer.
- **Stop pipe**: a pipe of each role. At the stop of the input, the role
  closes its write end, and the EOF that follows ends a wait of `poll`:
  that of the receiver's stdin reader for stdin, and that of the sender's
  session watcher for the hangup of the data session.
- **Stopped**: the input of the role stopped. Each role stops its input
  once; a later stop does nothing.

The receiver's **stdin reader**, from CACHE_RESULT, reads stdin when the
run has `--stdin` or `--batch-stdin`, else `/dev/null` (section 8). As
`ssh` does, it makes a stdin that is not a terminal non-blocking while it
reads it, unless it is non-blocking already, so that no read waits. A
terminal stays as it is, because the shell of the user shares it. Other
processes can share stdin too, so the stdin reader restores the flags of
stdin at the EOF of stdin and at the stop of the input, and every end of
the process restores them, also a fatal one (section 6.9).

- **I1.** In eager delivery, if neither stdin nor the stop pipe is ready,
  which `poll` shows without a wait, an idle point waits. While one waits
  and the input writer is idle, flush the stream, and append the buffer to
  the input spool: the idle point waits no more. Wait until stdin or the
  stop pipe is ready, or, while an idle point waits, until the wake-up is
  signaled; after the wake-up alone, do this step again. At the EOF of the
  stop pipe, end. So in eager delivery, every wait for stdin follows a
  flush, or waits for the input writer, also when another process that
  shares stdin took the bytes that `poll` reported.
- **I2.** Read stdin. If the read finds no bytes, because another process
  that shares stdin took them first (`EAGAIN`, section 3), go to step I1.
  At the EOF of stdin, end the zstd frame, append the buffer to the input
  spool, mark the end of the spool, and end. A read error ends the run
  with `die`.
- **I3.** Compress the bytes into the buffer. When the next bytes do not
  fit in the buffer, the buffer goes to the input spool first, which waits
  while the spool holds its limit (section 6.5, steps W1 and W3). An idle
  point that waits gives way to the next one. Go to step I1.

The receiver's **input writer**, from CACHE_RESULT:

- **S1.** Take the next bytes of the input spool, at most 256 KiB, and
  send them in one INPUT frame. Add them to sent, and signal the wake-up.
  Go to step S1.
- **S2.** At the end of the spool, send INPUT_END, and end. At the EOF of
  a stopped spool, end without INPUT_END.

The sender's data reader appends the payload of each INPUT frame to the
input spool, and waits while the spool holds its limit (section 6.5,
steps W1 and W3). INPUT_END marks the end of the spool. While the data
reader waits, it reads nothing from the data session, so it cannot see
the EOF that the loss of the session causes (section 6.9).

The sender's **session watcher**, from the sender's HELLO:

- **M1.** Wait until the data session hangs up, or the stop pipe reaches
  EOF. At the EOF of the stop pipe, end. A hangup ends the run with `die`:
  the receiver disappeared. A pipe tells its hangup with `POLLHUP`, and a
  socket whose peer shut down its writes with `POLLRDHUP`; bytes that
  arrive do not end the wait.

The sender's **stdin writer**, from command startup:

- **C1.** Take the next bytes of the input spool. At the end of the spool,
  require that the zstd frame ended with the last byte, if the stream is
  compressed. Then close the command's stdin, and end. At the EOF of a
  stopped spool, close the command's stdin, and end.
- **C2.** Decompress the bytes, and write the output to the command's
  stdin, at most 256 KiB in each write. A failed write with `EPIPE` means
  that no process reads the command's stdin any more: stop the input
  (below), close the command's stdin, and end. Other write failures end the
  run with `die`. When the input is stopped after a write, close the
  command's stdin, and end: a few bytes of the input stream can decompress
  to gigabytes, which have no use any more. Else go to step C1 once all the
  output is written.

The sender's **exit watcher**, from command startup:

- **E1.** Wait until the command exits, without reaping it (section 6.8).
  Then stop the input (below), and end. The exit can come long before the
  EOF of the command's stdout, which a descendant can hold. A descendant
  that reads the command's stdin then reads EOF, as under SSH, where
  `sshd` closes the stdin of a command that exited.

The input has no use any more once no process reads the command's stdin,
or once the command has exited. Then each role stops it, once:

1. The sender stops the input at `EPIPE` (step C2), at the exit of the
   command (step E1), or after a startup failure, whichever comes first.
   It stops its input spool (section 6.5), closes the write end of its
   stop pipe, and sends INPUT_STOP on the control session, which the
   control writer flushes at once (section 6.2). Before it sends END, the
   sender stops the input too. A stop waits until a stop in progress on
   another thread is done, so END always follows the stop. From then on,
   the data reader discards the payloads of INPUT frames, until END_ACK.
2. The receiver stops the input at INPUT_STOP, or at END when END comes
   first: the two sessions are independent. It stops its input spool, and
   closes the write end of its stop pipe. The input spool stops first, so
   the input writer reads the EOF of a stopped spool, and sends no
   INPUT_END. At END, before END_ACK, it joins the stdin reader and the
   input writer.

Between the two stops, the sender discards the input that arrives: about
what the link carries in one round trip. INPUT_STOP travels on the
control session, so it does not wait behind the output that the receiver
must write before it reads END (section 10). As with SSH, the input that
the receiver has read and not sent is lost, a command that neither reads
nor closes its stdin makes a run with `--stdin` or `--batch-stdin` send
up to the sender's limit of input for nothing, and such a run in the
background of an interactive shell stops at its first read of the
terminal.

Three waits can delay the end. When another program that reads the same
terminal, such as a pager in the same pipeline, takes the bytes that
`poll` reported, the stdin reader's read of the terminal waits for its
next bytes or its EOF, and so does the receiver's join of the stdin
reader at END, as `ssh` waits in its read; until then, the stream holds
the input compressed after the last flush. A descendant of the command
that holds the command's stdin without reading it keeps the stdin writer
in a write, which otherwise fails with `EPIPE` at once after the
command's exit. The sender joins the stdin writer after END_ACK, as it
joins the stderr relay, so such a descendant delays DONE but not END
(section 6.9). And zstd cannot cancel a job of its worker threads: after
a stop before the EOF of stdin, the stdin reader ends only once they have
compressed the jobs that zstd holds, about one for each worker thread,
and the receiver's join of the stdin reader at END waits until then. For
a producer that never pauses, with 12 worker threads, this took less
than 0.1 s at level 9, 2.4 s at level 15, and about a minute at level 19.

### 6.7 Failures and trust

- Non-fatal failures: filesystem failures during indexing abandon entries
  (section 4.5); during lookup they reject source candidates (section 6.4).
  Invalid cached lists are removed and treated as misses (section 5.6). A
  failed command still transfers all its output and returns its status in
  END; a startup failure returns status 127 and error text.
- External failures end the run without a core dump (section 6.9).
  Receiver SIGINT/SIGTERM give status `128 + signal` (section 6.8). These
  end the run with `die`: different protocol versions, features that the
  run needs and the peer does not have, and unsupported HELLO parameters,
  before command startup; truncated streams; broken sessions or receiver
  stdout; stderr relay read, write, or flush failures; failed reads of the
  receiver's stdin, and failed writes to the command's stdin other than
  `EPIPE` (section 6.6); configuration errors, including conflicting
  immutable-copy policies and duplicate or nested root paths; repeated
  directory identities detected during expansion or traversal; failures of
  the tmp_data directory setup, segment creation, segment I/O, or pin
  verification, because a yes answer cannot be retracted; and cache
  operation failures (section 5.6).
- Bugs panic and abort: invalid frames, a compressed stream that ends
  before END or on the control session, invalid compressed input, an
  input stream that does not end with its zstd frame, unexpected answers,
  missing pins, protocol arithmetic overflow, database errors, and
  invariant violations.

Use trusted peers: the sender learns hashes of all indexed content.
`sender_cache = false` disables their retention and removes earlier caches
for that receiver/index (section 5.6).

### 6.8 Threads at run time

Both roles use blocking threads and channels, with the threads of the table
below. At the normal end, a thread ends when its work ends: what it reads
reaches EOF or END, its queue closes because no thread can send to it any
more, a write fails at an expected closure (section 6.9), the command
exits, or a role stops the input (section 6.6). Then the main thread
joins it, in the order of section 6.9; the reader of a spool joins the
thread of the spool when it closes the spool. One thread is the
exception: the receiver's signal thread stays until the process exits,
and no thread joins it, so that SIGINT and SIGTERM during the end still
give `128 + signal`.

| Role | Responsibilities |
|---|---|
| Sender main | Handshake and command startup, then obtain the command's exit status without reaping, wait for the completion conditions of section 6.9, and send DONE. |
| Sender data reader | Read the data session for the whole run: the protocol version, the features, HELLO, the list, the input, END_ACK, and EOF. EOF before DONE means that the receiver disappeared. Append the input to the input spool (section 6.6). |
| Sender spool thread | Copy command stdout into its spool until EOF, and in eager delivery mark its idle points (section 6.5). |
| Sender output reader | Fill lookahead from the spool of command stdout, and pass its idle points to the scanner, until its EOF; then close the spool. |
| Sender stderr relay | Copy command stderr to sender stderr through EOF and flush (section 6.1). |
| Sender session watcher | Until the stop of the input, end the run at the hangup of the data session (section 6.6). |
| Sender stdin writer | Decompress the input spool into the command's stdin (section 6.6). |
| Sender exit watcher | Stop the input when the command exits (section 6.6). |
| Sender scanner | Own records and query states; queue queries and output (section 6.3). |
| Sender probe threads | Probe the parts of long stretches of windows for the scanner (section 6.3). |
| Sender data writer | Drain output in order and compress; make the flush that the scanner queues once nothing is left to write, then tell the scanner that it is idle; finish and flush the stream after END. |
| Sender control reader/writer | Receive answers, and send queries and INPUT_STOP promptly, independently of data output. |
| Receiver main | Start SSH sessions, reconstruct stdout from the spool of the data session, validate completion, send END_ACK, and close both session stdin pipes after DONE (section 6.9). |
| Receiver spool thread | Copy data-session stdout into its spool until EOF (section 6.5). |
| Receiver stdin reader | Compress stdin into the input spool (section 6.6). |
| Receiver input writer | Send the input spool in INPUT frames, and signal each one to the stdin reader (section 6.6). |
| Receiver control reader/writer | Dispatch queries to lookup workers, stop the input at INPUT_STOP, and send answers promptly. |
| Receiver lookup workers | Answer queries out of order (section 6.4). |
| Receiver signal thread | Handle SIGINT/SIGTERM with status `128 + signal`. |
| Attach relay | Copy both directions (section 6.1). |

Blocking output provides backpressure, which the spools absorb up to their
limits (section 6.5); it must not prevent independent control traffic.

Keep successfully spawned children unreaped until their owning `hashsync`
process exits: the retained group leader of the command reserves its
process-group ID, and the retained SSH children reserve their PIDs, so late
cleanup cannot signal an unrelated process after ID reuse. Obtain command
and SSH exit statuses with `waitid(P_PID, pid, WEXITED | WNOWAIT)`; do not
use reaping waits such as `Child::wait` or `Child::try_wait`. Before
spawning, set `SIGCHLD` to `SIG_DFL` with `SA_NOCLDWAIT` clear.

### 6.9 Shutdown

Fatal errors end the process rather than attempting to interrupt, cancel,
or join blocked threads. Persistent state is recoverable: RocksDB recovers
from its write-ahead log, and cache publication is atomic. Kernel
descriptor closure propagates EOF or errors to peers.

Each role has a `die(message)` callable from any thread. It reports the
error, performs cleanup once, and exits without unwinding and without a
core dump, with status 1, or `128 + signal` when the receiver has received
SIGINT or SIGTERM (section 6.8). Concurrent callers wait for that cleanup
rather than exiting ahead of it:

- Sender: SIGKILL the command's process group and remove the rendezvous
  socket.
- Receiver: SIGKILL both SSH processes, causing sender stdin EOF and
  cleanup, and restore the flags of stdin, when the stdin reader made it
  non-blocking (section 6.6). Unlike `ssh`, which leaves them changed at
  a fatal exit, every end restores them, also the normal end.
- Process exit closes the unlinked segments of both roles, which frees all
  their storage.
- Abandoned cache temporaries remain until the next exchange of that peer
  and index, or until expiry (section 5.6).

Build with `panic = "abort"`; install a panic hook that performs the same
cleanup before aborting. External failures use `die`; bugs and database
errors abort with a core dump (section 6.7).

Both roles block SIGXFSZ in every thread, so that a write past the
file-size limit fails with `EFBIG` and the run ends through `die`, with its
cleanup; the default action of the signal would end the process without
it. The receiver also blocks SIGINT and SIGTERM in every thread, for its
signal thread (section 6.8). A blocked signal stays blocked across
`exec`, so each role starts its children with the signal mask that it
started with.

Only the normal end runs the C library's exit handlers, after the
database has closed, which stops its background jobs. A fatal end, through
`die` or a termination signal, calls `_exit` instead, and an abort runs no
handlers either: many threads can be inside RocksDB then, and the process
must neither wait for a background job nor destroy RocksDB's static
objects under them. The index recovers from its write-ahead log, as after
a crash.

The receiver monitors both sessions and termination signals. The sender
monitors stdin and the control socket. The data reader monitors stdin
while it reads it, and until the stop of the input, the session watcher
monitors it too, because the data reader can wait at the limit of the
input spool (section 6.6).
Unexpected EOF or session loss is fatal, also during rendezvous and setup,
when no command output is flowing. A broken stdout is detected when a write
to it fails; neither role watches an idle stdout. As under plain SSH, a
consumer that exits after it reads all output does not fail the run. Normal
shutdown uses END_ACK, not stdin EOF, to establish receiver completion, and
DONE, not stdout EOF, to establish sender completion.

Successful sender completion requires:

- Command stdout has reached EOF and all its bytes have been represented by
  data frames followed by END with the command's exit status. The compression
  stream, if enabled, is finished and data-session output is flushed.
- Command stderr has reached EOF and the relay has forwarded and flushed
  all its bytes to SSH.
- One valid END_ACK has been received in response to END. An unsolicited or
  duplicate acknowledgement is a protocol error.

The implementation chooses the ordering and synchronization of these
conditions; no particular event ordering or state machine is required.
Startup failure has no command streams to drain.

Keep sender stdout open until process exit. Join the threads whose work
ends with the command's streams or with END: the output reader after the
EOF of the spool of command stdout, which it closes, so that it joins the
spool's thread; the data writer after END; and after END_ACK, the session
watcher, which ends at the stop of the input, the exit watcher, which
ends at the exit of the command, the stdin writer, which can wait for a
descendant that holds the command's stdin (section 6.6), and the stderr
relay for its completion condition. Once the completion conditions hold,
perform normal cleanup once:

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

1. After END, the receiver stops the input, unless INPUT_STOP has stopped
   it, and joins the stdin reader and the input writer (section 6.6). It
   consumes the rest of that zstd frame, checking its trailer and
   rejecting any further decoded bytes. Stop at frame completion without
   waiting for transport EOF; when compression is disabled END is
   sufficient framing. Flush reconstructed stdout.
2. The receiver enters Completing before sending and flushing END_ACK on
   the data session. It stops admitting lookups and publishing control
   replies, and closes the queue of replies. Late control traffic can be
   discarded. Workers returning later discard their results, including
   results of unfinished segment writes, and release the extents of those
   writes.
   Control EOF, broken pipes, and a partial final control frame or
   compressed block in Completing are expected closure, not failures.
3. The receiver reads DONE, which directly follows the zstd frame. EOF or
   another frame before DONE fails the run.
4. The receiver closes both session stdin pipes, in this order:
   1. Close the data session's stdin.
   2. Join the control writer. It ends when its queue is closed and empty,
      after it has sent the queued replies, or when a write fails.
   3. Close the control session's stdin.

   Neither close waits for the EOF of a stdout pipe (section 10).
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
and answer publication, so that a late lookup cannot prolong the exchange.
Sender stdin EOF before DONE remains a failure, and so does control EOF
before the sender shuts down the control socket: the receiver closes those
pipes only after DONE. Failures during stream validation, stdout flushing,
sending END_ACK, or reading DONE take the fatal cleanup path.

A descendant retaining command stdout or stderr keeps the run open, as
under plain SSH; this is expected behavior. A descendant that holds the
command's stdin, and does not read it, delays DONE too, but not END
(section 6.6). In the same way, a process that inherited a session's stdout
pipe from SSH delays the receiver's end until it closes the pipe, but not
the sender's. A lookup or segment I/O that stalls, for example on an
unresponsive NFS mount, delays the receiver's end until the I/O completes.
After fatal cleanup, a descendant that left the process group can survive
and will get a broken pipe on its next write to either closed output pipe.

## 7. Configuration

Use `$XDG_CONFIG_HOME/hashsync/config.toml`, or `--config PATH`:

```toml
block_size     = "16KiB"   # at least 512 bytes; changes rebuild the index
strong_hash    = "blake3"  # blake3 or sha256
candidate_attempts = 128  # candidates tried per query, at least 1
hashlist_max_age = "30d"
index_path     = "~/.local/state/hashsync/default.db"  # a directory
threads        = 0         # 0 = 4 × cores, clamped to 16–64
lookahead      = "256MiB"  # at least block_size; default max(256 MiB, 3 × block_size), for the probe threads
sender_output_spool_limit   = "2GiB"  # command stdout that can wait on the sender, or "unlimited"
receiver_output_spool_limit = "2GiB"  # data session that can wait on the receiver, or "unlimited"
receiver_input_spool_limit  = "2GiB"  # input stream that can wait on the receiver, or "unlimited"
sender_input_spool_limit    = "2GiB"  # input stream that can wait on the sender, or "unlimited"
tmp_data_dir   = "~/.cache/hashsync/tmp-data"  # receiver segments; any filesystem
negative_cache = "128MiB"
probe_threads  = 0         # 0 = half the sender's cores, at least one
compression_level   = 9    # zstd 1–19; 0 disables
compression_long    = true # zstd long-distance matching, 128 MiB window
compression_threads = 0    # data and input streams; 0 = half the cores, at least one
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
escape. A trailing backslash, malformed hex escape, or unsupported escape,
such as an octal one, is a configuration error. NUL is invalid in paths,
path patterns, and process arguments; literal identifiers may contain it.
Non-UTF-8 bytes are valid in all these byte-string fields. Do not apply
configuration escapes to other fields, to names read from the filesystem,
or to CLI arguments.

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
  `-j` overrides the configured thread count.
- `hashsync run [--stdin] [--batch-stdin] [--batch-stdout] [--no-update]
  [--ssh CMD] [USER@]HOST -- COMMAND ARGS...`: run the remote command,
  reproduce stdout, and return its status. The command reads EOF from its
  stdin at once, as with `ssh -n`: the run reads `/dev/null` in place of
  its stdin, and nothing from its stdin. `--stdin` gives the command this
  process's stdin instead: as `ssh` does, the run then reads its stdin
  while the command runs (section 6.6). The output and the input have
  eager delivery (section 6.2). `--batch-stdout` gives the output batch
  delivery, and `--batch-stdin` gives the input batch delivery, and also
  gives the command this process's stdin, as `--stdin` does.
  `--no-update` skips the index update, takes read-only index access
  (section 4.4), and still resolves current roots (section 4.2). After a
  successful transfer, also when the command fails, print a summary of two
  lines to stderr:
  1. The output bytes, as literal, reference, and run bytes; the wire
     bytes, which the receiver read from the data session; and the
     hash-list and delta bytes.
  2. As rsync does, the bytes that the receiver sent and received on both
     sessions, the elapsed time of the run, including its index update,
     the rate of the sent and received bytes in that time, and the
     speedup: the output bytes divided by the sent and received bytes.
     These counts do not include the bytes that SSH adds or the command's
     stderr. They are exact: the receiver prints the summary after the
     threads that count the bytes have ended (section 6.9).
- `hashsync serve --token T` and `hashsync attach --token T`: internal
  sender entry points.
- `hashsync stats`: report index statistics.
- `hashsync prune`: remove the paths and roots that no configured copy
  holds any more (section 4.2).

Every command also accepts these options, besides `--config PATH`
(section 7):

- `-q` (`--quiet`): do not print progress messages and summaries to
  stderr. Warnings and errors stay.
- `--no-human-readable`: show all amounts of bytes as whole bytes.

Summaries and statistics show an amount of bytes below 1 KiB as whole
bytes, such as `512 B`, and a larger one with two decimal digits, rounded
half up, such as `1.50 KiB`, in the first of KiB, MiB, GiB, TiB, and PiB
in which its rounded value is less than 1024, else in PiB. So 1048575
bytes, which round to 1024.00 KiB, show as `1.00 MiB`. Settings, limits,
and messages that help to debug show exact byte counts, because rounding
could make them unclear.

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
| Memory allocation | `mimalloc` for Rust code, RocksDB, and libzstd, with the `mimalloc` Cargo feature (on by default): the global allocator, replacements of the C++ allocation functions from mimalloc's `mimalloc-new-delete.h`, and libzstd's allocator interface; never its `override`, so other C code keeps libc `malloc` |
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
decision history, including fuller measurements, is in version control.

- **Content-defined chunking:** archive framing disrupts chunks at file
  boundaries. Fixed receiver blocks and a rolling sender scan lose only
  incomplete file tails.
- **Buzhash:** periodic inputs have structural collisions. The mixed
  polynomial hash avoids that specific weakness.
- **Truncated weak-hash lists and membership filters:** truncation raises
  false-hit costs; binary fuse filters need several cache-line accesses per
  probe. The full list also supports direct set deltas.
- **64-bit values in the weak table:** a probe runs for each byte of new
  output and is most of the cost of a scan. 64-bit slots made the scan
  19–137% slower, and a check of the high bits inside the probe loop up to
  25%. The second array of section 5.5 is read only for hits; it takes 5.7
  bytes per value and one cache line per check, where a sorted copy of the
  list would take 8 bytes per value and a binary search.
- **Reconciliation sketches or range recursion:** add decoding/fallback
  complexity or transfer poorly when changes are spread throughout the
  uniform hash space. Cached full lists allow straightforward direct deltas.
- **Removing only the advertised base after an exchange:** leaves the lists
  of failed exchanges until expiry. Removing every other list found after
  publication can remove the target of a later run, when a sender
  outlives its failed receiver. A listing made before the sender's HELLO
  needs no clock and holds no later target (section 5.6).
- **Advertising only the sender's newest list, by `ctime`:** depends on the
  clock, and a sender that outlives its failed receiver can publish a
  newer list that the receiver no longer has; the next run then transfers
  the full list. The sender advertises every listed list instead.
- **Independent scanners for parts of the output:** skipped windows can
  cross a reference chosen by another scanner and leave possible matches
  unexamined. One scanner owns all offset decisions.
- **Content-defined anchors for weak probes:** probing only the windows
  that a short-context rolling hash selects makes the cost of a scan
  independent of the list size, but it changes the list, the index, and
  the scanner, and it cannot find very repetitive blocks without an
  anchor. Probe threads keep one table probe per window.
- **SQLite for the index:** its B-tree indexes by strong hash have random
  keys, so once an index outgrows the writer's SQLite page cache, almost
  every block insert writes a different page, twice: to the write-ahead log
  and at the checkpoint. At 21M rows, an update that added 262k block rows
  took 18–23 s, and takes 2.4–4 s with RocksDB. At 20M rows, a new index
  took 66–68 s against 32 s, and a warm candidate lookup 14–15 µs against
  19 µs, less than the file reads of a transfer cost.
- **One writer thread:** SQLite allows one writer, so the scan tasks sent
  their changes to one thread. With RocksDB, that thread limited updates:
  a new index of 18.7M block rows took 121–125 s with it and 65–68 s with
  the scan tasks writing. Eight writer threads were as fast, but kept the
  messages, and the state of each file in two places. RocksDB inserts the
  transactions of several threads in parallel only with unordered writes:
  with ordered writes, 64 scan threads took 98–113 s.
- **Other embedded stores:** LMDB read fastest, but inserted 16k rows/s
  and wrote 35 KB per row. LevelDB has one compaction thread, no bulk load,
  and lets one process open a database, even to read it; fjall has the
  same limit.
- **Compaction only at the end of an update:** with the one writer thread
  of that time, which set the pace, turning automatic compactions off
  during an update did not make it faster, and a full compaction at its
  end added 3–6 s to small updates. Without the wait for
  the compactions that RocksDB has scheduled, closing the database cancels
  them: over short updates they pile up, and lookups slowed from 26 µs to
  41 µs.
- **One memtable limit for all column families:** each column family
  flushes its memtable at 64 MiB, RocksDB's default, and RocksDB also
  flushes the oldest one when all of them together use 256 MiB. With
  256 MiB for each, so that only the shared limit applies, the end of an
  update that added 262k rows took 5.8 s instead of 0.4 s.
- **More background jobs:** two jobs run one flush and one compaction at
  a time, and a compaction from level 0 splits into subcompactions on up
  to half the cores. With the scan tasks writing, 8 jobs saved nothing
  overall: a new index of 18.7M block rows took 65–68 s either way.
  Without subcompactions, the end of a new index of 20.5M rows took 8.5 s
  instead of 3 s: its last compaction, from level 0, ran in one thread.
  12 jobs with 12 subcompactions also took 3 s, but compactions from
  level 0 in several column families could then use 12 threads each at
  the same time.
- **Point deletions of all paths in reconciliation:** a scan steps over
  each removed entry until a compaction drops it, but skips a range
  deletion quickly. In an index of `/nix/store` at 4 KiB blocks (625k
  paths), point deletions made the reconciliation of an update without
  changes take 0.62–0.83 s, and the removal of orphaned inodes after it
  0.67–2.2 s; range deletions made them take 0.24–0.40 s and
  0.27–0.40 s.
- **One thread for the removal of orphaned inodes:** it merges all of
  `inodes` and `inode_paths`, also when completed immutable roots skip
  the scan. After an update without changes, it took 0.27–0.40 s on one
  thread, and takes 0.022–0.030 s on the threads of the scan pool.
- **Other locks:** RocksDB's own `LOCK` file keeps only read/write opens
  apart, and a second open fails at once. A `flock` on the index directory
  fails on NFS, where Linux emulates it with byte-range locks, and an
  exclusive one needs a file open for writing, which a directory cannot
  be. A classic POSIX lock on the lock file belongs to the process:
  closing any other descriptor of the file releases it, and at exit, the
  kernel releases it before RocksDB's lock of `LOCK`, which was opened
  later.
- **An index by weak hash:** it gives the export its order without a
  sort, but adds one entry per block: at 20M rows, a new index took 52 s
  instead of 32 s. The export, with a parallel in-memory sort, is not
  slower.
- **Async runtimes and `io_uring`:** the blocking pool already serves the
  file-oriented workload, and `io_uring` availability would require a
  fallback. Keep one I/O model.
- **Direct I/O or mmap for file hashing:** direct I/O bypasses useful cached
  data; mmap exposes the process to SIGBUS on concurrent truncation.
  Use buffered `pread`.
- **Opening candidates by inode handle:** requires privileges. Open indexed
  paths and verify the resulting bytes instead.
- **Hole punching in one pin file:** XFS and btrfs write a range back
  before they punch it, under the inode lock that pin writes and reads
  need. On XFS, punching 2 GiB of released pins took 425–441 ms and wrote
  2068 MiB to disk; closing unlinked files of 128 MiB took 71 ms and wrote
  nothing. Punching also needs filesystem support and a reclaimer thread.
- **Segments of one fixed size:** descriptors are scarce: a common soft
  limit is 1024, and RocksDB uses many. Segments of 128 MiB keep 512 files
  open when 64 GiB of pins pile up; the full rule of section 6.5 keeps 13,
  at its accepted cost.
- **One open segment per size:** segments of 16 MiB, 32 MiB, 64 MiB, and
  so on, where a new segment takes the smallest size that no open segment
  has. A segment would keep the size that it got at its creation, so a
  large segment would stay the current segment after the pins that made
  it large are released. In a simulation, after 8 GiB of pins piled up
  and drained, up to 8 GiB of released bytes stayed allocated, against
  16 MiB with the full rule.
- **Spooling the reconstructed output:** it would write every referenced
  block a second time, and every run in full. For 3.29 GB of `/nix/store`
  files, 97% references at 4 KiB blocks, the data session took 6.3 MB, and
  such a spool would take 3.29 GB. The spool of the data session keeps the
  transfer going instead; only the local reconstruction waits for stdout.
- **Spools without a limit:** with a slow link, the sender's spool would
  grow to almost the whole output: about 1.9 TB for `tar` of 2 TB at
  200 MB/s over 10 MB/s. Only the command would end sooner, not the
  transfer. On the receiver, a stopped consumer would let almost the whole
  data session wait on the disk that the consumer may be writing to.
- **A lookahead mapped from the spool:** it would allow a much larger
  `lookahead`, but a rejection makes the scanner scan again up to
  `lookahead` bytes (section 6.3), 6–44 min for 128 GiB, and the query
  states and pins grow with the lookahead too. The spool already lets the
  command run ahead of the scanner. It would save 0.15–0.35 s per GiB of
  copies, at the cost of unsafe slices over mappings and SIGBUS in place
  of `die`.
- **The scanner reading the spool of command stdout itself:** the read
  from the segments would move to the scanner thread, which limits the
  sender on new output: up to 12% of a scan.
- **Truncating a segment before it closes:** the NFS and SMB clients write
  all dirty data to the server before they send a truncation
  (`nfs_setattr`, Linux 6.18), and on local filesystems, the close of an
  unlinked file already discards its pages.
- **Batch delivery only:** the scanner keeps the last `B − 1` bytes and a
  short last literal until EOF, and the data stream keeps frames until
  END, so a command that writes its next output only after the consumer
  has answered the last, such as the remote end of `rsync -e`, waits
  forever. Eager delivery costs bytes and CPU instead, where the link has
  room for them: each flush ends a zstd block or job early, and a block or
  a run whose window spans an idle point becomes literal. For a 72 MB
  text tar without matches, at level 9, paced at 2 MB/s and 20 MB/s,
  eager delivery sent 4.0% and 1.0% more bytes than batch delivery, with
  40% and 5% more sender CPU; over a link of 200 KB/s, 0.4% more. For a
  tar of 631 MB of indexed files at 16 KiB blocks, paced at 20 MB/s and
  100 MB/s, it made 300 and 52 blocks literal, and sent 3.04 MB and
  1.55 MB against 1.22 MB. Over links of 200 KB/s and 50 KB/s, it made
  312 and 106 blocks literal, and ended after 31 s and 36 s, where batch
  delivery took 32 s and 49 s: batch delivery sends its last buffers only
  at the end.
- **Sending the bytes of every idle point at once:** the first eager
  design recorded the tail and flushed at each idle point that the
  scanner saw. The scanner scans up to `lookahead` ahead of emission, so
  it still saw idle points while the data queue was full behind a slow
  link, and the literal tails then waited in the queue. For the tar of
  indexed files above, it made 2,281 and 1,515 blocks literal over links
  of 200 KB/s and 50 KB/s, and took 63 s and 180 s; on a local link, 1,753
  blocks at 20 MB/s. For the input, the stdin reader flushed while the
  input writer waited on a link of 200 KB/s, and sent 0.2–0.3% more bytes.
- **No worker threads for the data stream in eager delivery:** each flush
  then ends only a zstd block. For the paced text above, eager delivery
  then sent 0.4–0.6% fewer bytes than batch delivery with worker threads,
  at about the same CPU. But a command that is faster than the scanner
  gives no idle points, and its output compressed 2.0–3.7 times slower:
  the unpaced text took 3.0–3.8 s instead of 1.0–1.5 s.
- **No worker threads for the input stream in eager delivery:** each
  flush would then end only a zstd block. For 72 MB of text paced at
  2 MB/s, eager delivery with 12 worker threads sent 5.4% more bytes than
  without them, and 8.3% more in writes of 4 KiB, with about 50% more CPU;
  over an input link of 200 KB/s, which holds most flushes back, 0.6%
  more. But without worker threads, a producer that never pauses waits for
  one compressing thread: 72 MB of text took 2.3–3.0 s instead of
  1.2–1.7 s, and 631 MB of binaries 21.9–22.8 s instead of 8.4–9.9 s.
  Batch delivery, which flushes nothing, took 0.9–1.1 s and 3.4–3.5 s: in
  eager delivery, the stdin reader empties the pipe sooner than the
  producer fills it, so idle points still end zstd jobs.
- **One zstd stream for the frames from the receiver to the sender:**
  END_ACK would follow all the compressed input, so at END, the receiver
  would send its whole input spool for a command that has exited, and the
  sender would decompress all of it to find END_ACK.
- **Discarding the input after `EPIPE`:** `sshd` discards the data of a
  channel whose stdin is closed, and goes on reading it, so the receiver
  would read and send all its stdin while the command runs. A first design
  stopped reading the sender's input spool instead, which hid the loss of
  the data session from the data reader. INPUT_STOP stops the receiver at
  once; the cost is that the producer of stdin waits until the run ends,
  where with SSH it could finish.
- **Stopping the receiver's input only at END:** END can wait behind
  `receiver_output_spool_limit` bytes for a slow consumer of stdout, and
  the receiver would send its stdin for nothing until then: 3.54 GB in
  6.4 s over local pipes, in a test.
- **Joining the stdin writer before END:** END would wait for a descendant
  that holds the command's stdin without reading it. Before INPUT_STOP
  existed, the receiver also sent its stdin for nothing all that time:
  2.73 GB in 4 s over local pipes.
- **Credits for the input:** the receiver could send only the input that
  the sender has room for, as an SSH channel window does, so that the data
  reader never waits at its limit. That needs a report on the control
  session for each part of the input that the stdin writer takes; the
  session watcher only watches for the hangup.
- **Compressing the hash list:** the weak hashes are uniform 64-bit
  values. For all of `/nix/store` at 4 KiB blocks, 14.0M values, the Rice
  code of section 5.4 is only 0.16% larger than the smallest encoding of a
  random set of that size, and zstd added 1.7 KB. For random lists of 1M
  and 4M values, the sorted values as `u64`, compressed with zstd, were
  33–40% larger than the Rice code. Only cached lists and deltas make the
  list smaller (section 5.6).
- **An uncompressed control session:** for 3.29 GB of `/nix/store` files,
  97% references at 4 KiB blocks, the queries took 35.0 MB and the answers
  10.9 MB, against 6.3 MB for the data session. Each flush adds a block
  header: with a flush after each frame, they were 0.6% and 21% larger
  than without compression. With a flush each time the writer's queue
  becomes empty, they were 18% and 9% smaller between two processes on
  one host, and 24% and 60% smaller over links that limited them, where
  the transfer took 22.5 s instead of 28.6 s and 28.1 s instead of
  65.3 s. Level 19 saves about 1% more than level 9, at up to 33 µs per
  frame instead of 4 µs.
- **Flushing the control streams less often:** a query kept in the
  compressor while the scanner fills its lookahead with literal bytes
  would stop the output until its answer; answers kept until the lookup
  workers are idle would stop the scanner in the same way.
- **Stdout EOF as the end of the sender:** the receiver could close each
  session's stdin at the EOF of that session's stdout, after `serve`
  closes it. With SSH multiplexing, this is a deadlock: the OpenSSH mux
  client (ControlMaster) keeps its stdout open until the session ends
  (`mux.c`, `mux_client_request_session`), and `serve` waits for stdin EOF
  before it exits. A wrapper script that waits
  for SSH holds the pipe in the same way. DONE needs no stdout EOF, and
  stdin EOF always arrives: the receiver is the only writer of each stdin
  pipe.

## 11. Repository conventions

- One crate, one binary, committed `Cargo.lock`. All profiles use
  `panic = "abort"`; install cleanup hooks before starting threads.
- Isolate unsafe syscall wrappers and SIMD code; deny unsafe code elsewhere.
- Share protocol definitions between both roles. Until the first release,
  the protocol version stays 1, and no change adds a feature. After that,
  any change that a peer can observe after the version exchange requires a
  protocol version bump or a new feature (section 6.2), for example a
  change to frame layouts, frame order or meaning, the list encoding, or
  the frames that the zstd streams carry. New weak and strong hashes are the
  exception: HELLO names both, and a sender that does not support them
  ends the run before command startup (section 6.2).
- The name of every temporary file of this program is `.hashsync-` and
  16 random hexadecimal digits: the cache temporaries, the temporary of
  the account ID, and the segment files without `O_TMPFILE`. Then a file
  that a failed run leaves shows its owner, and the cache can tell its
  temporaries apart from other hidden names (section 5.6).
- Run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test` for
  implementation changes.
- Test every rule of this document. Use separate processes for locking
  tests, and fuzz protocol decoding. Cover in particular:
  - hashing and encoding properties, empty-table misses, and no table
    false negatives;
  - byte-exact reconstruction under rescans, late answers, backpressure,
    and any split of a stretch among probe threads;
  - pins: concurrent lookups, obsolete writes, unaligned block sizes, the
    full rule after each allocation and each closure of a sealed segment,
    the bound on open segments for any release order, sealed segments held
    open by extents in use, storage failures, corruption detection, and
    cleanup on exit;
  - spools: limits, segments that close behind the reader, a stop that
    closes all segments and wakes both ends, a consumer of stdout that
    reads nothing, and a producer that waits when both spools of its
    stream are full;
  - delivery: in eager delivery, idle points that the reader of a spool
    reports only when no byte follows them, the end of command stdout as
    the last one, also when the end follows at once, the tail and the
    short literal before an idle point, one flush after their frames, also
    when the data queue is full at the last frame, a tail and flushes that
    wait while the data writer or the input writer is busy and leave once
    it is idle, a block that spans an idle point while the data session is
    full, and a command and a consumer that answer each other through both
    sessions, also after the command closed its stdout; in batch delivery,
    a full buffer and the end of the stream as the only flushes;
  - the input: with and without compression, `--stdin`, and
    `--batch-stdin`; a run that ends with its command while stdin stays
    open; a command that reads a part of its input, closes its stdin, or
    exits while a descendant holds stdout or reads its stdin;
    INPUT_STOP before CACHE_RESULT; END that does not wait for a
    descendant that holds the command's stdin; the loss of the data
    session while the data reader waits at its limit; a stop inside the
    output of one read of the input spool; input streams that do not end
    with their zstd frame; and a non-blocking stdin, unless it is a
    terminal, with its flags restored at its EOF and at a fatal end;
  - one write call per frame, and non-blocking standard descriptors;
  - filesystem races and sparse files; reopening files and directories
    after rename or unlink; symlinks and special files, never opened for
    I/O; symlinks and cycles during root expansion; directory cycles and
    aliases in either traversal order; root replacement between expansion
    and scanning; links found before and after their owner's row commits;
    hard links within and across roots, also to a single-file root;
  - immutable reuse after root-directory identity changes; no tasks or
    opens solely for overlap checks; completion withheld after a late
    directory conflict; conflicting copy policies; a pruned immutable root
    that returns;
  - configuration byte escapes, TOML quoting, literal glob metacharacters,
    and non-UTF-8 values;
  - SSH option precedence; bootstrap quoting of executable paths with
    whitespace, quotes, shell metacharacters, and non-UTF-8 bytes; a
    different protocol version on either side, detected before the
    features; feature bits that a side does not know, on either side;
  - crash recovery, process exclusion, and reads of later commits; cache
    races, also a publication whose directory is gone and a listed name
    that a later run published again; fatal cache I/O failures, and a
    warning for an empty directory that stays; the advertisement and
    removal of listed files, hidden names that are not temporaries, and
    the removal of empty directories; and invalid cached lists;
  - completion without truncation or premature exit; complete stderr
    forwarding with backpressure and late stderr EOF from descendants;
    truncated compression trailers; compressed control traffic that the
    closure ends; late lookups; relay closure with control writes in
    flight; shutdown on session loss; and fatal cleanup after a child's
    exit status was collected without reaping.
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
