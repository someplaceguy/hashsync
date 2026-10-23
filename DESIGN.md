# hashsync

This document specifies the purpose and design of `hashsync`. Update it when
the design changes. `PLAN.md` orders the implementation.

## 1. Purpose

`hashsync ssh` runs a remote command as ssh does: the command reads the
client's stdin, its stdout comes back to the client, and the client
returns its exit status. One or both of the two streams are
deduplicated: data that the receiving host already holds in its files
travels as block references, which that host reads and verifies before it
inserts them into the stream. By default, this is the command's stdout,
against the client's files; with `--dedup-stdin`, the client's stdin,
against the server's; and with `--dedup-stdin --dedup-stdout`, both.
This is useful for package trees and archive streams that repeat existing
content at different positions and alignments.

One Rust binary contains both the client and the server. The client
starts the server through SSH. Parallelism uses threads and blocking
system calls, with no asynchronous runtime or event loop.

## 2. Terminology

- **Client**: the host that runs `hashsync ssh`. It starts both SSH
  sessions, and has the user's stdin, stdout, and exit status.
- **Server**: the host where SSH starts `hashsync serve` and `hashsync
  attach`. It runs the command.
- **Forward stream** and **backward stream**: the two streams of a run
  (section 6.6). The forward stream carries the client's stdin to the
  command's stdin, and the backward stream the command's stdout to the
  client's stdout.
- **Sending end** and **receiving end**: the parts of a stream on the host
  that reads its source and on the host that writes its destination.
- **Deduplicated stream** and **plain stream**: a deduplicated stream
  replaces the blocks that its receiving host holds with references, and a
  plain stream carries all its bytes. A run deduplicates one stream or
  both.
- **Pull** and **push**: a run whose backward stream alone is
  deduplicated, the default, and a run whose forward stream alone is, with
  `--dedup-stdin`. A run with `--dedup-stdin --dedup-stdout` deduplicates
  both streams, and is neither.
- **Receiver** and **sender**: the two roles in a deduplicated stream. The
  receiver has the index: it sends the hash list, answers queries, keeps
  the pins, and decodes the stream. The sender scans the stream against
  the list. In a pull, the client is the receiver and the server the
  sender; in a push, the other way around. With both streams
  deduplicated, each host is the receiver of the stream that it receives,
  and the sender of the other.
- **Session frames**, **stream frames**, and **control frames**: the levels
  of frames of the sessions (section 6.2). The stream frames, LIT, REF and
  RLE, carry the bytes of a stream inside its zstd frame. The control
  frames, QUERY, ANSWER and STOP, form the **control stream** of each
  direction of the control session. The session frames carry everything
  else, among them the pieces of those zstd frames and control streams.
- **Block**: `block_size` bytes (default 16 KiB) at an aligned offset in an
  indexed file. Only complete blocks are indexed.
- **Window**: `block_size` bytes at any offset of a deduplicated stream.
  All protocol offsets count the bytes of the stream from zero.
- **Root**: a configured file or directory to index. An **immutable root**
  promises unchanging content; copies sharing an **identifier** are indexed
  as one group.
- **Index**: a host's RocksDB database of block hashes and locations,
  which the receiver of a deduplicated stream uses.
- **Weak hash**: a 64-bit rolling hash used to select possible matches.
  A **strong hash** is the configured cryptographic digest used to verify them.
- **Hash list**: the canonical encoded set of indexed weak hashes.
  Its **list ID** is its strong-hash algorithm and full digest.
  A **delta** contains the removed and added values between two lists.
- **Hash table**: the sender's in-memory membership table built from the
  list, which fills while the list arrives (section 5.5).
- **Literal / reference / run**: output sent as bytes, a verified block
  reference, or a repeated byte value and length.
- **Query / answer**: a request for a window's full strong hash at an output
  offset, and the receiver's yes/no response.
- **Pin**: a query offset's verified block, retained by the receiver in an
  **extent**: a range of bytes in a segment. Pins with identical content
  share one extent (section 6.4).
- **Segment**: an open, unlinked file in the **tmp_data directory** of a
  host, which holds the extents of pins or the bytes of a spool
  (section 6.5).
- **Spool**: a pipe whose bytes wait in segments, so that the producer of
  the bytes waits for their consumer only at the limit of the spool.
- **Stop**: the end of a stream before the end of its source, once no
  process reads its destination any more, or for the forward stream, once
  the command has exited (section 6.6).
- **Idle point**: a moment when the source of a stream has no byte ready:
  command stdout for the backward stream, and the stdin that the client
  reads for the forward stream.
- **Delivery**: when the bytes of a stream go on from the buffers on their
  way. **Eager** delivery, the default, sends them on at each idle point of
  the stream's source, once the data writer of the stream is idle, and
  when a buffer is full; **batch** delivery only when a buffer is full and
  at the end of the stream (section 6.2). In both, a deduplicated stream
  also goes on after every `lookahead` bytes of it (section 6.3).

## 3. One run, step by step

1. The client opens its tmp_data directory (section 6.5), and starts data
   and control SSH sessions to the server. Each side sends its protocol
   version at once, and the client follows its own with REQUEST, with the
   command and the settings of the run, without waiting for the server's.
   When the client receives a deduplicated stream, its HELLO follows at
   once: the block size and the hashes of that stream, from its
   configuration. The server reads the configuration file that REQUEST
   names, by default its own file of the profile `default`: it needs one
   when it receives a deduplicated stream (section 7). It opens its
   tmp_data directory, and sends its own HELLO at once when it receives a
   deduplicated stream.
2. The server starts the command, in a pull once it has read the client's
   HELLO, and both streams start (section 6.6). The source of each stream,
   the command's stdout or the client's stdin, goes through a spool: it
   waits for the transfer only when `--spool-limit` bytes wait in the
   spool. The sender of a deduplicated stream starts its scanner at the
   receiver's HELLO, with an empty hash table, which fills while the list
   exchange of step 3 goes on: the scanner sends literals for misses, runs
   for repeated bytes, and queries for possible block matches, and the more
   of the list it has, the more blocks it finds. With `--wait`, the scanner
   scans only once its table is whole, and the stream waits in its spool
   until then (section 6.3). The client sends its stdin to the command's
   stdin, until the forward stream ends, or the server stops it, and the
   server sends the command's stdout to the client's stdout, until the
   backward stream ends, or the client stops it once no process reads its
   stdout. In eager delivery, the bytes of both streams reach their
   destinations at each idle point of their sources, once the link can
   carry them (section 6.2).
3. Meanwhile, the receiver of each deduplicated stream resolves its
   configured roots, updates its index, and then holds it read-only
   (section 4.4). `--no-update` skips the update and opens the index
   read-only. A failed update ends the run, and the command with it. With
   both streams deduplicated, both hosts update their index at the same
   time, and each one releases its index after its update, and opens it
   again only once the other's update is done (section 4.4). Then each
   deduplicated stream has a list exchange, on the control session
   (section 5.6). INDEX names the receiver's account and index, and the
   sender advertises the lists that its cache holds for them. The receiver
   exports its hash list, the target, from one database snapshot, and
   starts its lookup workers, while the sender prepares its cached list.
   With sender caching enabled, the receiver durably publishes its target
   list. It sends a full list, a delta, or confirmation that the sender's
   list is current. The sender verifies it, fills its table, and durably
   publishes it when caching is enabled. Then it removes its other cached
   lists for this peer and index, expires old cache files, and acknowledges
   the accepted target. On the acknowledgement, the receiver does the same
   cleanup. A receiver whose stream has ended or stopped before it sends
   its target sends no list: HASHNONE ends the exchange.
4. The receiver reads and verifies candidates, and writes each verified
   block to a pin before it answers yes; a block that a pin already holds
   needs no lookup. The sender emits references for yes answers and
   rescans rejected windows from the next byte. The stream is always
   emitted in order. Once a deduplicated stream has ended or stopped, its
   receiver's lookups end, and its index closes (section 6.4).
5. Each receiving end decodes its stream from its spool: the client to
   its stdout, the server to the command's stdin. Until `--spool-limit`
   bytes wait in the spool, a blocked destination stops only the
   decoding. Once the command has exited, both streams have ended or
   stopped, command stderr has reached its end, and the server's sides of
   the list exchanges have ended, the server sends END with the command's
   status. The client then closes both session stdin pipes, writes the
   rest of the backward stream, checks its end, and flushes stdout. Both
   SSH sessions must exit successfully before the client returns the
   command's status (section 6.9).

**Verification is authoritative.** The index, hash lists, caches, and weak
hash table are hints. Verify the exact buffer of a candidate before writing
it to a pin, and the exact buffer read back from a pin before writing it to
the stream's destination or to a copy (section 6.4). Both checks use the
query's full strong hash. Stale files and collisions can reduce
deduplication, never change the reconstructed output.

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
with `EAGAIN` instead. Each host reads and writes its standard
descriptors as blocking ones: after `EAGAIN`, it waits with `poll` until
the descriptor is ready, and tries again. A wait that also watches other
descriptors, such as that of the client's stdin reader (section 6.6),
goes back to its own `poll`.

**Join threads at the normal end.** At a normal end, every thread that
`hashsync` starts has ended and has been joined, except the client's
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

Roots come from the configuration file (section 7); a glob match can be a
directory or a single regular file. Root and exclusion globs use
`gix-glob`'s raw `wildmatch` interface in pathname mode, with
case-sensitive matching on bytes and leading dots included. `*`, `?`, and
bracket classes cannot match `/`; `**` spans path components. Patterns and
filenames may contain non-UTF-8 bytes, which section 7 spells in
configuration, and section 8 in messages; never reject them for that
reason or convert them lossily.
Thus `/nix/store/*` also matches `/nix/store/.links` and non-UTF-8 names.

`gix-glob` supplies matching, not filesystem expansion. Expand root
patterns by walking directories relative to open descriptors, with the
no-symlink rules of section 4.5, and detect directory cycles during the
expansion. A pattern matches only entries that exist, also a pattern
without wildcards: names come from directory listings, or a literal name
is looked up, for existence only, and the scan task decides what the
entry is (section 4.5). A root that is not there therefore has no root
row, and a group whose copies are all missing is no longer configured.
An entry whose lookup fails is reported and is no match either, like a
directory on its path that cannot be opened: no scan or candidate read
could reach it. A pattern without matches gets a warning; when the root
of a pattern without wildcards is all that is missing, the warning says
that the root does not exist.

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
configuration still names that identifier, unless `--scan-immutable` asks
the update to scan every group again (section 8). A root row without an
identifier is a mutable root or an unfinished traversal; configuration
supplies the intended kind and identifier.

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
| `exclude` | Globs using the same match options as roots. Patterns beginning with `/` match absolute paths, including root matches; others match basenames. Normalization drops empty components and a trailing `/` from each pattern; a `.` or `..` component, or a `/` that remains in a basename pattern, is a configuration error, since matched paths have no such components and basenames have no `/`. Excluded entries and descendants are skipped. |
| `one_filesystem` | Skip an entry and descendants when its mount ID differs from its parent's. |
| `may_vanish` | The root need not exist: neither a pattern without matches nor a missing root gets a warning, and a copy that vanishes before its scan task opens it gives its place to the next one (below). Copies of a group can differ in it. |
| `identifier = "path"` | Default for immutable roots: the absolute matched path. |
| `identifier = "name"` | Use the last path component, allowing copies under different parent directories. |
| `identifier = { strip_prefix = "/mnt/disk" }` | Remove that prefix from the absolute match; a missing prefix is a configuration error. |
| `identifier = { literal = "photos-2019" }` | A fixed identity that survives moving a tree. |

Identifiers are nonempty byte strings with the same comparison rules for all
forms. They are allowed only on immutable roots.

All configured copies sharing an identifier must have the same indexing
policy: identical `exclude` pattern sets, ignoring order and duplicates,
and equal `one_filesystem` values after applying defaults. Compare the
patterns after byte-escape decoding, glob quoting, and normalization,
without attempting to prove glob equivalence. Conflicting policies are a
configuration error, including for completed groups and `--no-update`
runs; reject them before changing the index. The user is responsible for
making absolute exclusions select corresponding content in every copy.

Policy changes do not invalidate completed immutable groups automatically.
After changing exclusions or `one_filesystem`, the user must run
`hashsync update --scan-immutable`, or `--rebuild`, to apply the new policy
to those groups. `prune --scan-immutable` only removes what the new policy
leaves out.

Before scanning, reconcile roots. Paths and blocks to remove can be many,
so their removal commits in bounded transactions; an interruption leaves
work that the next reconciliation finishes. The root and inode rows then
change in one transaction:

1. In one transaction, invalidate every completed immutable root that is
   no longer configured or that references an incomplete inode, and with
   `--scan-immutable` every completed immutable root: remove its
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
scan tasks. The chosen location is the first copy in the order of the
configuration. A copy can vanish after root expansion: when the scan task
does not find it and it may vanish, the task takes the next copy, until
one is there or a copy that may not vanish is missing. A group without a
copy left stays unfinished without a warning. After all scan tasks
finish, publish immutable identifiers only for groups whose traversals
and dependent inode and path results succeeded and committed (section
4.6), with the path of the copy that the scan traversed. A group that
stays unfinished gets that path too, by which a run with `--no-update`
finds its copies. A failed chosen copy leaves the traversal unfinished; a
missing copy that may not vanish is such a failure. Other copies remain
available for candidate reads.
After processing all results, remove every orphaned inode and its blocks.

The same reconciliation recovers interrupted updates. Renames and identifier
changes can reuse complete inodes with unchanged identity, size, and ctime.
An inode reached through both mutable and immutable roots is checked by the
mutable scan; interrupted or abandoned rehashing invalidates its immutable
roots before deleting dependent paths.

The user is responsible for immutability: a completed root can omit files
added later. Copies may have different inode identities; unequal content
fails verification. `prune` takes exclusive index access, hashes no files,
and removes what no current copy holds. A root goes when it has no current
location. Prune looks into the other mutable roots, and with
`--scan-immutable` into the immutable ones too: such a root goes when none
of its copies opens as a directory or a regular file, and otherwise prune
looks up each of its paths in its copies, as a candidate read does
(section 6.4), and removes the path unless a copy holds a regular file
there that a scan of the copy would reach. So a path also goes when an
`exclude` pattern of the copy matches it or a directory above it, or with
`one_filesystem`, when its lookup crosses a mount point (`RESOLVE_NO_XDEV`):
prune removes what a narrower policy leaves out, but only a scan adds what
a wider one takes in. A path whose lookup fails goes too, as an update drops what it
cannot reach (section 4.5): no candidate read could reach it. Such a
failure also makes a completed group unfinished, so that the next update
scans it again, and its row takes the path of the first copy that prune
opened, by which a run with `--no-update` finds its copies: the copy that
the row named can have vanished after root resolution. A permission error
below the root of the copy does not make the group unfinished, since it does
not fail a scan of the copy either (section 4.5); a lookup that the root
itself denies, because the root cannot be searched, does. Prune warns once
for each directory in which lookups fail: the deepest directory above a
failed path that still opens. So a directory that cannot be searched gets
one warning, as in an update, and not one for each indexed path below it.
The completion markers of the roots that go and of the groups that become
unfinished go first, then the paths, then the root rows, and then the
orphaned inodes and their blocks. So the marker of a root that goes never
survives the removal of one of its paths, and a root that later returns
is scanned again. An existing root with no indexed paths is still valid.

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

The index is one RocksDB database: a directory, by default named after the
configuration file (section 7), such as
`$XDG_CACHE_HOME/hashsync/default.db` for the profile `default`. Each table
below is a column family, which holds entries (keys with values) in key
order. A **row** is one entry of `roots`, `inodes`, `paths`, or `blocks`.
`identities`, `inode_paths`, and `strong` hold one more entry for each row
of `inodes`, `paths`, and `blocks`, keyed for another lookup; the
transaction that changes a row changes that entry too.

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
  requires an in-place rebuild. Read-only runs, prune, and compact report
  that an update is required for a missing or incompatible index.
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
(section 10). A prune ends the same way, after it has compacted every
column family into its last level, so that the entries that it and the
updates before it removed leave the database files. An update with
`--compact` ends with the same compaction, also in a run with
`ssh --compact`, and `hashsync compact` does nothing else (section 8).
RocksDB keeps all files of the database open, so a process raises its
soft descriptor limit to the hard limit before it opens the index. Its
children, such as the command of a run whose forward stream is
deduplicated, start with the limit from before the raise, as they would
under SSH alone.

**Process access.** A lock on the file `hashsync.lock` in the index
directory excludes other processes:

- A modifying process (`update`, `prune`, `compact`, or the receiver of a
  run without `--no-update`, for its update) opens the lock file
  read/write and takes an exclusive lock before it opens the database
  read/write. An update first creates the directory and the lock file if
  necessary, so without the lock file, there is no index. Prune and
  compact create no directory, lock file, or database, so that a wrong
  index path leaves no files behind.
- A read-only process (the receiver of a run with `--no-update`, `stats`)
  opens the lock file read-only and takes a shared lock, then opens the
  database read-only. It sees the database as it was when it opened it.
  Read-only processes can coexist but exclude a modifying process.
- After its update, a run closes the read/write database, turns its
  exclusive lock into a shared one on the same open file description,
  which never leaves the index unlocked, and opens the database read-only.
  So no modifying process can come between the update and the use of the
  index, and read-only processes can use the index during the rest of the
  run. A shared lock needs the lock file open for reading, so a modifying
  process opens it read/write, not write-only. A run with both streams
  deduplicated is the exception: each host releases its lock after its
  update (steps L1 to L3 below).
- The lock is an open file description lock (`F_OFD_SETLKW`) on the whole
  file, on a close-on-exec descriptor. Unlike a classic POSIX lock, it
  stays when another descriptor of the file closes, and at exit it goes
  only after RocksDB's own lock of its `LOCK` file, so a second read/write
  open never fails on `LOCK` (section 10). Wait indefinitely on
  contention. A process first tries the lock without a wait
  (`F_OFD_SETLK`), and when another process holds it, says in a note that
  it waits for the lock of the index, which it names, before it waits: a
  long wait must not look like a hang. When it gets the lock, a second
  note says so and names the index again, so that the work that follows,
  such as an update, does not look like more waiting. An update's time
  counts from the moment it holds the lock.
- All threads of a process share one database handle. Reads of a
  modifying process see its latest committed transaction.
- Keep the lock until the database handle has closed. A run uses an index
  until the lookups of the stream that it deduplicates end, once that
  stream has ended or stopped (section 6.4): then the database and the
  lock close, while the run can go on. A process never opens an index a
  second time while it has it open: the locks of two open file
  descriptions conflict also within one process, so the second open would
  wait forever.

**Lock waits.** A lock wait has no timeout, so no run may take part in a
cycle of waits. An update waits for nothing else while it holds its
exclusive lock, and a run waits for an exclusive lock only while it holds
no shared lock. Then the locks that runs take for themselves form no
cycle. A wait for a lock that an update holds lasts only until that
update ends, whatever else the update's run waits for. Any other lock
wait is that of an exclusive lock for a shared holder, and its run holds
no shared lock that another wait could be stuck on. A run with one
deduplicated stream follows the rule as it is. With both streams
deduplicated, both hosts update their index at the start of the run, at
the same time. A host that kept its lock after its update could hold it
while the other host still waits for the exclusive lock of its own
update, so each host releases its lock after its update, and takes it
again only once the other's update is done, which UPDATED frames on the
control session pass:

- **L1.** Each host updates its index, then closes it, which releases
  the lock, and sends UPDATED.
- **L2.** At the other host's UPDATED, each host opens its index again,
  read-only with a shared lock, and checks that it still matches the
  configuration, as a run with `--no-update` does. Between steps L1 and
  L2, another process can change the index, and a process with another
  configuration of the same `index_path` can rebuild it with other
  settings. If it no longer matches, the host ends the run with `die`. It
  cannot update the index again here: it would wait for an exclusive lock
  while the other host can hold its shared lock.
- **L3.** Each host sends INDEX, exports its hash list, and starts its
  lookup workers (section 5.6).

With `--no-update`, both hosts take only shared locks, and each opens its
index at the start of the run, as with one deduplicated stream.

The rule cannot cover the commands. A command that waits for an
exclusive lock waits for every run that holds a lock of that index, and
a run keeps each lock until the stream that the index deduplicates has
ended or stopped. So when the forward stream is deduplicated, a command
that waits for the lock before it has read all its stdin hangs its own
run once the spools are full. And two runs in opposite directions between
the same hosts, whose commands update the index of their host after they
have read their stdin, hang each other: each update waits for the client
of the other run, which keeps its lock until its own command, the other
update, has exited.

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
Opening, metadata, and directory-listing errors below the root that are
permission errors (`EACCES` or `EPERM`) are the exception, such as a
directory that cannot be searched or listed, or a file that cannot be
opened for reading: the entry is logged and skipped, but the root stays
successful. A copy keeps its permissions just as it keeps its content, so
a retry would be denied again, and a group with one private file would
otherwise be scanned on every update. Reading errors are no exception,
whatever their errno: the permissions of a file are checked when it
opens, so a read that is denied later has another cause, which may not
last. A permission error on the root itself, including a root that cannot
be searched, still marks the root unsuccessful, so a copy that cannot be
read at all never completes empty. Access that the scanning user gains
later, such as a new group membership, reaches a completed group only
through `--scan-immutable`.
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
the soft descriptor limit to the hard limit at startup (section 4.4). The
database handle and the directory and file visited sets are shared.

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
| Dropped | The file has no rows: it is too small to index, its permissions deny opening it for reading, or its processing failed. Other links add nothing. |

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
waiting and later paths unsuccessful. A file that its permissions keep from
being opened for reading makes only a single-file root that is a link of it
unsuccessful: that root cannot be read at all, while for the other links
the file is an entry below the root (section 4.5). Do not retry an
abandoned inode through another hard link during the same update.

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

The table fills while the list exchange goes on (section 5.6), so that
the scanner finds more blocks as more of the list arrives. The scanner's
thread owns the table and takes its updates between scans, so lookups
need no lock (section 10):

- A kept list or a delta gives a whole table, which the sender's side of
  the exchange builds on its own thread.
- A full list gives an empty table sized for its `N` values, once the
  list bytes that have arrived could hold that many values, at least one
  bit each, so that a corrupt count allocates no more than the bytes that
  arrive allow. Then the values that each HASHDATA frame completes go
  into it, before the list is verified: they are hints, and the receiver
  verifies every block that they find (section 3).

### 5.6 The hash list cache and direct deltas

Full lists are immutable and named by content. Account and index IDs
partition cache directories; they do not identify list content. Compute a
direct delta from any available base, without generations, delta logs, or
delta chains.

**Identity and storage.**

Each UNIX account has one random 128-bit `account_id` in
`$XDG_CACHE_HOME/hashsync/account-id` (fallback
`~/.cache/hashsync/account-id`), which all its configuration files share.
Initialize it durably with atomic no-replace publication; concurrent
initializers use the winner's ID. Never overwrite an existing ID. INDEX and
ADVERTISE exchange account IDs, and INDEX names the receiver's index ID;
the sender needs no index for the exchange.

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

The exchange of a deduplicated stream runs on the control session while
the streams run, each side on a thread of its own (section 6.8). A side's
*listing* is the names of the lists and hidden temporaries in its
peer/index directory when it lists the directory, with an open file for
each list. The name of a hidden temporary starts with `.hashsync-`
(section 11). Other hidden names are not part of the cache layout, and
only expiry removes them. For example, an NFS client gives such a name to
a removed file that is still open, and the removal of that name fails
while the file is open.

1. Once its index is ready for the run (section 4.4), the receiver sends
   INDEX, with its account ID and its index ID, and starts to export and
   hash the target from one database snapshot. A receiver whose stream has
   ended or stopped before its index is ready sends HASHNONE instead.
2. At INDEX, the sender lists its peer/index directory. With sender
   caching enabled, it advertises the IDs of all listed lists of the
   stream's strong hash, usually one, in ADVERTISE, with its account ID;
   otherwise none. Right after ADVERTISE, while the receiver exports, it
   prepares the first advertised list: it reads it through the open file
   of its listing, verifies it, decodes it, and builds the table from it.
3. At ADVERTISE, the receiver lists its peer/index directory. During the
   export, before it knows the target, it searches all its
   `local-hashlists/*/*.cache` directories for the advertised lists, in
   their order, and verifies the first one that it finds, the base. Once
   it has the target, if its stream has ended or stopped by then, it sends
   HASHNONE, and the exchange ends. Otherwise, with sender caching enabled,
   it durably publishes the target in its directory before sending list
   data. If its listing holds a file with the target's name, it keeps that
   file instead, unless it read the file as a base and found it damaged.
   Otherwise, if another of its peer/index directories holds the target,
   which happens when it receives the same target from several senders, it
   links that file into this directory, unless it is the file found
   damaged under another name. It does not read a file that it keeps or
   links: a damaged copy shows only when an exchange reads it as a base,
   and costs a full list then. Either way, it retains an open fd. Failure
   to persist the receiver's target ends the run.
4. If the target is one of the advertised lists, the receiver uses
   HASHKEEP. A missing or damaged base uses HASHLIST. With a base, merge
   sorted sets and encode removed and added values as lists; use
   HASHDELTA, which names the base, only when smaller than the full
   transfer. HASHKEEP, HASHDELTA and HASHLIST all name the target, and the
   list bytes of HASHLIST and HASHDELTA follow in HASHDATA frames.
5. The sender takes the list that HASHDELTA or HASHKEEP names from step 2
   if it prepared that list, and otherwise reads it through the open file
   of its listing, and verifies it. It decodes or applies the delta,
   re-encodes canonically, and verifies the full target digest. An invalid
   listed list or a bad delta requests HASHLIST once via HASHFULL; the
   result of a bad delta is never published or used for the table. A bad
   full list, or a full list after HASHFULL that names another target, is
   a protocol error.
6. The sender gives the table to its scanner as soon as it can
   (section 5.5): after HASHKEEP or HASHDELTA, the whole table, and after
   HASHLIST, the values while they arrive. With sender caching enabled, it
   durably publishes the verified target, or after HASHKEEP keeps the
   listed file that it verified. It ends its exchange, and only then
   replies CACHE_RESULT.
7. On CACHE_RESULT, the receiver ends its exchange. Without CACHE_RESULT,
   it removes nothing.

To end its exchange, a side renews its published target, and the receiver
also the base that it used. It removes the files of its listing except
its published target, and then expires the cache (below). The removal of
listed files touches no other peer/index directory. An exchange that
HASHNONE ends removes no listed file on either side, but both sides
expire the cache, as at the end of every exchange.

With `--no-sender-cache`, the sender advertises no list, and neither side
publishes one, so the end of the exchange removes every listed file.
REQUEST carries the setting to the server, so both sides know whether
they publish; no storage-success flag is needed.

**Publication, concurrency, and expiry.**

Publish complete files using a unique hidden temporary, file fsync, rename
to the content-derived name, and directory fsync. Make new directory
hierarchies durable too. A side whose listing already holds its target
keeps that file instead of publishing it again, and the receiver can link
a copy from another of its peer/index directories (steps 3 and 6): the
list takes 110 MB at 21 million values, and its publication about half a
second. To keep or link a file, renew it first (below), check that the
listed name still leads to it, and make the entries of its directories
durable as after a publication, since its publisher may have died before
it did. A link goes through `/proc/self/fd`, to the file that the side has
open, and needs no fsync of the file. A failed link, for example across
filesystems, falls back to a publication. Published files are never edited
in place. When the temporary cannot be
created because its directory is missing, create the directory and try
again: expiry can remove an empty directory at any time, also just after
its creation. Once the temporary exists, the directory is not empty, and
expiry cannot remove it. A directory that is gone at the fsync after the
rename means that a concurrent cleanup removed the file: this is normal
absence.

No cache locks or mutable current-list pointer are needed. Retained fds or
bytes survive replacement and unlink. On NFS, a retained fd survives only on
its own host: once another host removes the file, calls through the fd can
fail with ESTALE. For a list or a temporary, that just means the file is
gone, which is normal absence. Tolerate disappearance during directory
scans and fall back to a full transfer. Cleanup racing with replacement or
removal of a published file may lose a future cache hit; it cannot invalidate
retained bytes. A concurrent cleanup can remove a temporary before its
rename: treat this as normal absence, with the same effect as removal of
the published file just after publication. With sender caching enabled,
acknowledgement promises completed publication, not immunity from later
deletion.

A run with another target needs a modifying process first, which needs the
exclusive lock of the index (section 4.4). The receiver takes its lock
before it sends INDEX and exports its list, and keeps it, exclusive or
shared, until its lookups end, once its stream has ended or stopped
(section 6.4). With both streams deduplicated, each receiver releases its
lock after its update, and takes it again before INDEX (section 4.4,
steps L1 to L3). The sender lists its directory at INDEX, before
ADVERTISE, and the receiver lists its own at ADVERTISE. The receiver
sends its target only if its stream has neither ended nor stopped once
it has the target, after both listings: then it held its lock during
both, and a run with another target starts only after them.
The sender ends its exchange only after HASHLIST, HASHDELTA, or HASHKEEP.
So no exchange that ends has a listing that holds the target of a later
run, also when a sender outlives its failed receiver. A later run can still
publish a listed list again under the same name, when its index has
returned to an earlier state, or keep the listed file as its target. So a
side removes a listed list only if its name still names the listed file,
and nobody renewed the file since the listing: it moves the name to a new
temporary name, compares the file there with its open file and with the
modification time that the file had at the listing, removes it if both
match, and moves it back otherwise. Keeping reuses the inode, so only the
renewal tells a later run's target apart. A rename changes the ctime of
the moved file on most filesystems, but not its modification time, so the
comparison comes after the rename, once the name is the side's alone. A
listed temporary belongs to an earlier run, or to a concurrent run with
the same target, which then publishes nothing: normal absence.

The end of an exchange and expiry can remove the base of a concurrent
exchange at any time. So each side reads its base once, through one open
file, verifies the bytes that it read, and uses only these bytes, or that
file, until the end of the exchange: the sender through the file of its
listing. Neither side opens a list by name a second time.

Touch verified files through their open descriptors after successful use.
Use `ctime` as the renewal marker. A renewal also sets the modification
time later than the file had, by at least a nanosecond, so that a listing
made before it no longer matches the file. To expire the cache, at the
end of each exchange, a side removes every file in every peer/index
directory of both cache trees whose `ctime` is older than the
`hashlist_max_age` of its host (default 30 days, section 7): lists and
abandoned hidden temporaries. Then it removes every peer/index directory
that is empty.

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

Use the SSH command of `--ssh`, `ssh` by default, preserving its agent,
host configuration, ProxyJump, and host-key handling; per-host SSH options
belong in `~/.ssh/config`. Shell-split `--ssh`; the first word is the
executable and the rest are its options. For both sessions, insert
`-o ClearAllForwardings=yes` and `-o RemoteCommand=none` before those
options, then append `-T -x -o Compression=no`, `-l USER` when the run has
it, the unchanged HOST, and the remote command string. The leading options
take precedence under SSH's first-value rule; the final `-T` and `-x`
disable PTYs and X11 forwarding even if the `--ssh` options requested
them. An explicit compression option in `--ssh` still takes precedence.
SSH options must keep the sessions in the foreground, preserve their stdin
and stdout, and execute the specified remote command.

SSH executes its remote command through the remote login shell, which must
support POSIX shell quoting for this bootstrap. Treat `--hashsync-path` as
one executable name or path, not a shell fragment: a wrapper script there
does what a fragment would, such as setting the server's environment. The
client names the server's configuration file in REQUEST (section 7), so
`serve` takes no `--profile` or `--config` of its own. Construct
a remote `exec` command by single-quoting each argument, replacing
embedded single quotes with `'\''`, and preserving all other bytes,
including non-UTF-8 bytes. Passing separate local argv elements to SSH
does not quote them for the remote shell. The server needs a compatible
`hashsync` in PATH or at `--hashsync-path`.

The client generates token `T` and starts both sessions together, with
the signal mask that the client started with (section 6.9):

- Data: `SSH HOST hashsync serve --token T`.
- Control: `SSH HOST hashsync attach --token T`.

`serve` first listens on `$XDG_RUNTIME_DIR/hashsync/T.sock` (fallback: a
private directory under `/tmp`), mode 0600. It exchanges protocol
versions, reads REQUEST and then the configuration file that REQUEST
names (section 7), sends its HELLO when it receives a deduplicated
stream, reads the client's HELLO when it sends one, and starts the
command. Then it starts its update when it receives a deduplicated stream
(section 3), and accepts `attach`, whose session carries the list
exchanges.

`attach` retries a not-yet-listening socket while its stdin remains open,
with no deadline. It relays stdin to the socket and the socket to stdout,
half-closing the socket on stdin EOF. Socket EOF or a peer-closed socket
error (`EPIPE` or `ECONNRESET`) on a read ends the relay from the socket.
`attach` then waits until stdin EOF ends the relay from stdin, joins that
thread, and exits zero. After a peer-closed error on a write, the relay
from stdin writes nothing more to the socket, but reads stdin to EOF and
discards the bytes. This prevents a deadlock: the client closes the
control session's stdin only after its control writer has ended (section
6.9), and that writer could otherwise block on a full pipe. Either
direction may observe closure first. Each host decides from END whether a
closure was expected; other relay failures end the relay unsuccessfully.
Rendezvous waits must also observe the client's disappearance (section
6.9).

The command travels in REQUEST, not on the SSH command line, in one of
two forms (section 8):

- By default, as ssh sends it: COMMAND and ARGS joined with single spaces
  into one line. The server runs it as sshd does, with `$SHELL -c LINE`:
  sshd sets `$SHELL` to the user's shell from the password database. When
  `$SHELL` is unset or empty, the server uses `/bin/sh`, as sshd does for
  an empty shell field.
- With `-s`, the words as they are. The server finds the first one in
  PATH and starts it without a shell, so the command's arguments do not
  depend on the remote user's shell. Starting `serve` and `attach` still
  does: the bootstrap above goes through the login shell.

Start the command with piped stdin, stdout, and stderr, its own process
group for cleanup, and the signal mask (section 6.9) and the descriptor
limit (section 4.4) that `serve` started with.

The **stderr relay**, a server thread, copies command stderr to `serve`'s
stderr with bounded buffering and prompt writes, independently of stdout
scanning and control traffic. SSH forwards these bytes through its stderr
channel; both local SSH subprocesses inherit the client's stderr. Stderr
is outside the hashsync protocol and its compression streams. The relay
finishes only after command stderr reaches EOF and all copied bytes have
been written and flushed to SSH; a successful exit of the server requires
that (section 6.9).

### 6.2 Protocol version, features, and frames

Each side sends `protocol_version u32`, little endian, as the first bytes of
each session. It does not wait for the peer's version before it sends its
own. It reads the peer's version before anything else, and it ends the run
with `die` if the versions are different; the message names both versions.
The server starts the command only after it has checked the client's
version on the data session, and checks it on the control session when
`attach` connects. The remote commands of section 6.1 and this version
exchange never change. Everything after the exchange belongs to one
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

`S` is the full digest length of the strong algorithm that the receiver's
HELLO names for a deduplicated stream. Every digest of the stream uses
that algorithm, including all list IDs of its exchange, so a list ID
travels as its digest alone, `[u8; S]`. A list exchange has one target and
at most one base: HASHLIST, HASHDELTA, and HASHKEEP name the target, and
HASHDELTA also names the base, one of the lists that the sender's
ADVERTISE advertises.

Each session has two levels of frames. On the data session:

- **Session frames** are never compressed: REQUEST, HELLO, CHUNK,
  STREAM_END, and END. They are everything that a reader must find
  without decoding a stream.
- **Stream frames** are LIT, REF, and RLE. The stream frames of one stream
  form one zstd frame, unless compression is disabled, and the payloads of
  the stream's CHUNK frames are consecutive pieces of it. So the frames
  compress together across chunks, and runs of REF stay cheap. With
  compression disabled, the CHUNK payloads are the stream frames as they
  are.

On the control session:

- **Session frames** are never compressed: the frames of the list
  exchanges, INDEX, ADVERTISE, HASHLIST, HASHDELTA, HASHKEEP, HASHNONE,
  HASHDATA, HASHFULL, and CACHE_RESULT, and UPDATED and CHUNK.
- **Control frames** are QUERY, ANSWER, and STOP. The control frames of
  one direction form its control stream, one zstd stream unless
  compression is disabled, and the payloads of the direction's CHUNK
  frames are consecutive pieces of it.

Each direction of the data session carries one stream: the forward stream
goes from the client to the server, and the backward stream from the
server to the client. So CHUNK and STREAM_END name no stream. In each
direction of the control session, QUERY belongs to the stream that the
sending host scans, and ANSWER and STOP to the stream that it receives,
so none of them names a stream either. Neither do the frames of the list
exchanges: those of a receiver belong to the stream that the sending host
receives, and those of a sender to the stream that it sends.

| Level | Types |
|---|---|
| Session, data | 1 REQUEST, 2 HELLO, 3 CHUNK, 4 STREAM_END, 5 END |
| Session, control | 3 CHUNK, 6 INDEX, 7 ADVERTISE, 8 HASHLIST, 9 HASHDELTA, 10 HASHKEEP, 11 HASHNONE, 12 HASHDATA, 13 HASHFULL, 14 CACHE_RESULT, 15 UPDATED |
| Stream | 16 LIT, 17 REF, 18 RLE |
| Control | 19 QUERY, 20 ANSWER, 21 STOP |

| Session | Direction | Frames |
|---|---|---|
| data | client to server | REQUEST; the client's HELLO when it receives a deduplicated stream; the forward stream: CHUNK, STREAM_END |
| data | server to client | The server's HELLO when it receives a deduplicated stream; the backward stream: CHUNK, STREAM_END; END |
| control | receiver to sender | List exchange: INDEX, HASHLIST, HASHDELTA, HASHKEEP, HASHNONE, HASHDATA |
| control | sender to receiver | List exchange: ADVERTISE, HASHFULL, CACHE_RESULT |
| control | either way | UPDATED, after the update of each host (section 4.4) |
| control | sender to receiver | QUERY, in the control stream |
| control | receiver to sender | ANSWER, in the control stream |
| control | receiving host to sending host | STOP, in the control stream |

Payloads:

- REQUEST, the first frame of the client after the features: `command_kind
  u8`, 0 for a command line and 1 for the words of `-s` (section 6.1), and
  `command`, a list of byte strings: one for a command line, at least one
  for the words of `-s`. Then `deduplicated u8`, the deduplicated streams,
  as a set of bits: 1 for the forward stream and 2 for the backward
  stream, so 1 in a push, 2 in a pull, and 3 with both; at least one is
  set. Then the settings of the run (section 8): `no_update u8`,
  `scan_immutable u8`, `compact u8`, `sender_cache u8`,
  `compression_level i32`, `compression_long u8`,
  `compression_threads u32`, `probe_threads u32`, `lookahead u64`
  (bytes; 0 for the default), `negative_cache u64` (bytes),
  `spool_limit u64` (bytes; `2^64 − 1` for `unlimited`),
  `stdin_delivery u8` and `stdout_delivery u8` (0 for eager, 1 for batch),
  and `wait u8`, 1 with `--wait`. Each host applies the settings that
  concern its roles. Then `quiet u8` and `human_readable u8`: the client's
  `-q`, and the opposite of its `--no-human-readable`, which the server
  applies to its own notes. Last, the server's configuration file
  (section 7): `config_kind u8`, 0 for its profile `default`, 1 for the
  profile of `--server-profile`, and 2 for the file of `--server-config`,
  and for 1 and 2, `config`, a byte string, the profile name or the path.
  A profile name is one that the client's options accept. The words of the
  command and `config` come from the client's command line, whose
  arguments hold no NUL byte, so REQUEST is invalid when one of them holds
  one.
- HELLO, from the receiver of a deduplicated stream, on the data session:
  `block_size u32`, `weak_hash_id string`, and `strong_hash_id string`,
  from its configuration, so it needs no index: the client sends it right
  after REQUEST, and the server right after it has read REQUEST.
- INDEX: `account_id [u8; 16]` and `index_id [u8; 16]`, the receiver's
  account and the ID of its index, which partition the list caches. It
  names no target, so the receiver sends it before its export, and the
  sender prepares its cached list meanwhile (section 5.6).
- ADVERTISE: `account_id [u8; 16]`, and `lists`, a list of the IDs
  `[u8; S]` of the advertised lists (section 5.6).
- HASHLIST: `target [u8; S], length u64`. HASHDATA frames with that many
  encoded bytes of the target follow.
- HASHDELTA: `target [u8; S], base [u8; S], removed_length u64,
  added_length u64`. HASHDATA frames with the removed and then the added
  encoded list follow, as one sequence of bytes. They change the base, one
  of the advertised lists, into the target.
- HASHKEEP: `target [u8; S]`. The target is one of the advertised lists;
  no data follows.
- HASHNONE: empty. In place of INDEX, or of the target after ADVERTISE:
  the receiver's stream has ended or stopped, and the exchange ends
  without a list (section 5.6).
- HASHDATA: the next list bytes of HASHLIST or HASHDELTA, at least one and
  at most 16 KiB. The receiver sends nothing else of its exchange until
  they are all out. Enforce exact lengths and canonical encoding,
  including ordering and padding.
- HASHFULL: empty. Request a full list once when the listed list that
  HASHDELTA or HASHKEEP names is invalid, or after a delta digest
  mismatch (section 5.6). The response is HASHLIST, which names the same
  target.
- CACHE_RESULT: empty. The sender's single acknowledgement of the target
  of its exchange (section 5.6). A second one in the same exchange is a
  protocol error.
- UPDATED: empty. With both streams deduplicated and without
  `--no-update`, the host's update is done, and its lock released
  (section 4.4, steps L1 and L2). At most once in each direction.
- CHUNK: on the data session, the next bytes of the stream's zstd frame,
  or of its stream frames when compression is disabled, at least one. A
  CHUNK goes out when it holds 256 KiB, and at each flush of the stream:
  the idle points of eager delivery, every `lookahead` bytes of a
  deduplicated stream, and the end of the stream. On the control session,
  the next bytes of the control stream, at least one, at each flush of the
  control writer (below).
- STREAM_END: empty. The end of the stream: its zstd frame ended exactly
  with the last CHUNK. An empty stream is an empty zstd frame and
  STREAM_END; without compression, STREAM_END alone. A stopped stream has
  none, but a STREAM_END that the sending host sent before it learned of
  the stop is normal (section 6.6).
- LIT: literal bytes, at least one.
- REF: empty; use the pin at the current output offset.
- RLE: `byte u8, length u64`, with a length of at least 1.
- END: `status i32, error string`, then the server's counts of its ends
  of both streams (section 8). Of the forward stream, whose decoder and
  lookups it runs: `bytes u64, literal u64, reference u64, run u64,
  reference_frames u64, run_frames u64, wire u64, false_positives u64,
  not_found u64`. `bytes` are the bytes that the command's stdin took, and
  `literal`, `reference` and `run` their parts by stream frame;
  `reference_frames` and `run_frames` are the bytes of the REF and RLE
  frames that the decoder read, and `wire` the CHUNK bytes that it
  decoded. `false_positives` and `not_found` are the queries that the
  lookups answered no because the index has no candidate for their strong
  hash, and because none of their candidates verified (section 6.4). Of the
  backward stream, whose source reader it runs: `read u64, eof u8`, the
  bytes that it read from command stdout, and 1 if it read command stdout
  to its EOF. Status is the command's exit status, `128 + signal`, or 127
  plus error text if startup failed. END is the server's last
  data-session frame: nothing follows it (section 6.9).
- QUERY: `offset u64, strong [u8; S]`. At most one per offset; offsets may
  go backwards after rescanning.
- ANSWER: `offset u64, found u8` (0 or 1). Answers may arrive out of order.
- STOP: empty. The host that receives a stream stopped it (section 6.6):
  from the server, the forward stream; from the client, the backward
  stream. At most once in each direction.

The receiver's HELLO sets the block size, weak hash, and strong hash of
its stream. The sender uses them, or it ends the run with `die` if it does
not support them: in a pull, the server reads the client's HELLO before
it starts the command. It checks the strong hash as soon as it reads its
name: the length of every list ID of the exchange depends on it. In the
same way, the server ends the run before command startup if it does not
support a setting of REQUEST, such as a compression level. And the sender
of a deduplicated stream reserves the scanner's lookahead buffer and
starts its probe threads at the receiver's HELLO. The client sets their
sizes to any value, so a host that cannot provide them ends the run there
(section 6.3).

The data handshake is the version exchange, the features, REQUEST, and
the HELLO of each deduplicated stream, which its receiver sends at once.
Then the server starts the command, and both streams start, while the
list exchanges run on the control session. A list exchange is INDEX, once
the receiver's index is ready; ADVERTISE during the receiver's export;
durable receiver publication with sender caching; then HASHLIST,
HASHDELTA, or HASHKEEP, and its HASHDATA frames. The sender replies with
CACHE_RESULT, possibly after one HASHFULL fallback. The receiver handles
the result and cache cleanup. HASHNONE in place of INDEX or of the target
ends an exchange early (section 5.6). The lists travel on the control
session, because nothing there waits for a stream. On the data session, a
list would wait behind the other stream of its direction, whose spool on
the peer is full while the command reads none of its stdin, and with
`--wait`, the run would hang (section 10).

Each session frame goes to SSH in a single write. SSH puts the bytes of
each read of its stdin into packets of their own, so a header that arrived
ahead of its payload could cost an extra packet. A side flushes before it
waits for a reply. The frames of the list exchanges are not compressed:
zstd cannot make the hash list smaller (section 10).

**Delivery.** The run gives each stream a delivery (section 8). Eager
delivery, the default, works as SSH does: a command that waits for its
next input before it writes more output, and a consumer of the output that
writes the next input only after it has read that output, such as the two
ends of `rsync -e`, never wait for bytes that a buffer of `hashsync`
holds. It sends the bytes of an idle point on only when they can leave at
once: when the data writer of their stream has nothing left to write. On a
slow link, that writer is busy, so the bytes wait, and the bytes that
follow join them: a deduplicated stream keeps its matches, and the streams
compress better. So the link decides how much eager delivery costs. Batch
delivery keeps the bytes until a buffer is full or the stream ends: it
costs fewer bytes and less CPU also on a fast link, and keeps the matches
that eager delivery loses at idle points there (section 10). REQUEST names
the delivery of both streams, and the sending end of each applies it at
the idle points of its source (sections 6.3, 6.5, and 6.6). In both
deliveries, the scanner of a deduplicated stream also asks for a flush
after every `lookahead` bytes, so that the references and runs of a source
that never pauses, such as a file, do not wait in the buffers until the
end of the stream (section 6.3).

The control stream of each direction of the control session is one zstd
stream at the level of the streams, unless compression is disabled. Its
frames are small, so it uses no long-distance matching and no worker
threads. The start of a zstd frame on either side takes milliseconds, tens
of them on a busy host, in allocations and page faults, which also wait
for the other threads of the process that map memory then, such as a
stream decoder that takes a window of 128 MiB. So the control writer
starts its frame at its own start, and the control reader decodes a frame
of its own first, then starts afresh with the memory that it took: the
first frame of the session, such as STOP, waits for neither. It flushes the stream into a CHUNK each
time its queue of control frames becomes empty, because the scanner waits
for answers, and the sending host of a stream must stop it as soon as the
receiving host has stopped it. When a slow link makes the writer wait,
more frames share each flush and compress better. While that queue is
empty, the writer sends the frames of the list exchanges of its host, one
at a time, from one queue for each side that the host plays, so that
neither exchange waits behind the list of the other: a control frame that
comes while a HASHDATA frame goes out waits for it alone. The reader
passes each frame of an exchange to its side as soon as it arrives. No
frame ends the control stream: the closure of the session ends it
(section 6.9).

### 6.3 The sender's scanner

One scanner owns all output decisions, records, and query states. Other
threads perform I/O and compression, and probe threads probe the weak table
for the scanner. Let `B = block_size`.

**State and invariants.**

- `e`: next output byte to emit; `p`: next byte to scan; `r`: end of
  the bytes of the stream read so far. Initially all are zero, with
  `e <= p <= r`.
- The lookahead buffer retains immutable bytes `[e, r)`, including scanned
  bytes that a rejection may require again. Its capacity is `lookahead +
  B`; the lookahead reader, which takes the stream's bytes from the spool
  of its source (section 6.5), the command's stdout for the backward stream
  and the client's stdin for the forward stream, pauses when the buffer is
  full. The sender reserves the buffer whole, at twice its capacity, at
  the receiver's HELLO (section 6.2). Scanning starts a step only while
  `p - e < lookahead`; a full-window step may advance `p` by `B`.
- `f`: in eager delivery (section 6.2), the end of the bytes before the
  last idle point of the source, initially zero, with `f <= r`. The
  lookahead reader passes each idle point that the spool reports after the
  bytes before it, and the scanner then sets `f = r`. The bytes before `f`
  leave without waiting for more bytes, once they can leave at once.
- The data writer is **idle** when it waits for frames, after it has
  written every frame that the scanner queued, and made the flushes that
  the scanner queued. It tells the scanner when it becomes idle.
- **Records** describe scanned output that is not emitted yet. Ordered
  records cover exactly `[e, p)`, without gaps or overlaps:
  `Literal { offset, length }`, `Run { offset, length, byte }`,
  `Query { offset }`, and `Ref { offset }`. Queries and references each
  cover `B` bytes.
- The query-state map is keyed by offset and stores the full digest with
  state Not_sent, Waiting, Yes, or No. Keep entries until their offsets
  fall below `e`, even when their records are discarded.
- A byte-bounded negative LRU cache (`--negative-cache`) holds full strong
  hashes answered no. It suppresses queries for the same content at other
  offsets, but does not avoid hashing a new window.

A Query blocks emission of itself and every following record. References
require a yes for their exact offset; equal hashes at different offsets do
not share permission to emit REF. A last Literal or Run can grow, and its
ready prefix can be emitted before it stops growing.

**The table.**

The scanner starts at the receiver's HELLO with an empty table, which the
sender's side of the list exchange fills while the list arrives (section
5.5). A window scanned before its weak hash is in the table stays
literal: the scanner does not scan it again when the table grows. So
deduplication grows with the part of the list that the sender has, and
the bytes before the list cost bandwidth. When the table is whole, the
sender says so in a note that names the stream, unless the scanner has
already emitted the whole stream or stopped: from then on, the stream is
deduplicated against the whole list. Without `--wait`, the note also
gives the bytes of the stream that the scanner had emitted by then,
which went out before the whole list could help them. With `--wait`, the
scanner scans only once its table is whole, so that the run sends the
fewest bytes, however long it takes: the bytes of the stream wait in the
lookahead buffer, and then in the spool of the source (section 6.5). That
wait lasts at least until the receiver's update is done, so it must not
look like a hang: if the first bytes of the stream arrive while its list
exchange is still running, the sender says in another note, which names
the stream too, that the stream waits for its whole list. An empty
stream ends without a table. An exchange that HASHNONE ends gives no
table: with `--wait`, the scanner then waits for the stop of its stream,
which has stopped already or stops soon (section 6.6).

**Scanning.**

Each iteration takes the updates of the table that have come, processes
available answers, resolves Query records in offset order, and emits ready
records before scanning more bytes.

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
stretch of windows into consecutive parts and probe them on the probe
threads at once. Every part but the first initializes the rolling hash and
the constant-window check from the bytes of its first window. The first
hit or constant window in offset order ends the stretch, and parts after
it may stop early, so the result equals checking each window in order.
After each weak hit, new or known, the scanner scans at most a short
stretch on its own thread before it processes answers again. With more
than one probe thread, each following stretch without a hit or a constant
window doubles the next one, to at least one part per probe thread and up
to a bound per thread. A constant window, or a hit that the check rejects,
neither doubles nor shortens the next stretch. Short gaps between matches
thus stay on one thread, and a long run of misses soon uses all probe
threads. `--probe-threads` sets the thread count; 0 selects half the
sender's cores, at least one. The sender starts them at the receiver's
HELLO (section 6.2).

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
- Queue a flush of the stream for the data writer, behind the frames
  before it, once `e` reaches an `f` past the last flush, and, in both
  deliveries, once `e` is `lookahead` bytes past the last flush. References
  and runs stand for far more bytes than their frames, so without the
  second flush, those of a source that never pauses, such as a file, would
  wait in the buffers after the scanner until the end of the stream. That
  flush records no tail, so it makes no block literal. The data writer
  makes each flush once it has nothing left to write (section 6.2).

Advance `e` only after the data writer accepts the frame. LIT owns a copy
before those buffer bytes are released. Remove emitted record portions and
map entries below `e`.

Outgoing data is bounded, but queueing it must not block the scanner from
processing answers. Outgoing QUERY messages use an unbounded, nonblocking
queue and remain ordered even when offsets go backwards. Discarded records
can leave obsolete requests; lookahead is not a bound on all outstanding
queries or receiver pins.

When progress requires more bytes of the stream, answers, updates of the
table, data-queue space, or, while a tail before `f` waits, an idle data
writer, wait on notifications for those events, continuing to prioritize
answers.
Backpressure propagates from the data writer through the lookahead buffer
to the spool of the source (section 6.5).

Finish only after the EOF of the source, `p = r`, and no Query records
remain. Drain all records so `e = p = r`. Then the data writer ends the
stream: it ends the zstd frame, and sends the last CHUNK and STREAM_END
(section 6.6). So the bytes before the EOF of the source never wait for
the exit of the command, which END waits for (section 6.9). Obsolete
unanswered requests do not delay the end of the stream.

The stream can stop (section 6.6). Its receiver then sends no answer, so
a query that waits for one would keep the scanner from ending: every wait
of the scanner also waits for the stop. At the stop, the scanner ends at
once, without the end of the stream, and so does the data writer, which
takes none of the frames that the data queue still holds (section 6.6).

### 6.4 The receiver's answering and decoding

Let `B = block_size`. The pins are one user of segments, with extents of
`B` bytes (section 6.5). The receiver keeps:

- `c`: the output offset of the decoder.
- The pin map, from the query offset of each pin to the full strong hash
  of its block.
- The pinned blocks, by full strong hash: the blocks of the pins. Each
  one has one extent, which all its pins share, and `last`, the highest
  query offset of its pins. Extents are independent of output offsets.
- The open segments of the pins.
- A bounded 256 MiB LRU cache of verified buffers, keyed by full strong
  hash, which can serve candidate lookups. It holds at least one buffer,
  also when `B` is larger than 256 MiB. Verified reads populate it; its
  eviction is independent of pin lifetimes, and pins never retain its
  buffers.

One lock protects this state and Completing. Source reads, segment I/O,
and writes of the destination occur outside the lock; the file creation
of step A1 is the one exception, and it occurs once per segment. In-memory
payload storage is limited to the cache and worker and decoder buffers;
pin metadata and queued requests still scale with outstanding work. The
receiver releases an extent when nothing uses it any more: the output has
passed `last` of its block, a copy has replaced it, or its lookup worker
has found its write obsolete or unneeded, or has finished it in
Completing. The receiver does not reject a match merely because many pins
exist, and pin release requires no protocol message.

A new pin at `x` of a pinned block **shares** the extent of the block,
with no lookup and no write, when the share keeps no more segments open,
and no more bytes, than an extent of the pin's own would:

- `x < last`: the extent stays in use until the output passes `last`
  anyway.
- The extent is in the current segment: the current segment is never
  full (section 6.5), so an extent of the pin's own would go to it too,
  and keep it open until the output passes `x`. `last` becomes `x`.
- The extent is the only extent of its sealed segment, there is no
  current segment, and one extent would fill a new segment even with
  that sealed segment closed. A copy would close that segment and fill a
  new one alone. `last` becomes `x`.

Otherwise the extent is in a sealed segment and `x > last`. A share would
keep that segment open for as long as the block recurs: with its released
extents, or as one more sealed segment where a copy would join other
extents. So the pin gets a **copy**: a new extent with the bytes of the
block, which replaces the old extent for all pins of the block.

A pool of 4–8 lookup workers, which share the database handle, answers
queries out of order. Queue all answers nonblockingly under the lock,
checking Completing before publication. The receiver enters
**Completing** once its stream has ended, at STREAM_END, or has stopped
(section 6.6): the sender emits a frame only after its answer, so the
queries that still arrive then are obsolete. Completing suppresses new
lookups, extents, pin publication, and answers, including no answers from
obsolete lookups. It happens under the lock of pin allocation and answer
publication, so that a late lookup cannot prolong the run. The lookup
workers end at Completing, also a worker that waits for its next query; a
lookup in progress finishes first. The index closes with the last worker
(section 4.4). The pins stay until the decoder has ended.

For `QUERY { offset: x, strong: h }`:

1. Under the lock, check `c`. If `x < c`, answer no without a pin. If `h`
   is pinned and the new pin can share its extent, publish `h` at `x`, and
   queue yes. Otherwise try the verified-buffer cache.
2. If uncached and `h` is pinned, read exactly `B` bytes from the extent
   of `h` with `pread` outside the lock, verify the full digest of that
   buffer, and continue at step 6: a pinned block needs no index. Segment
   EOF, read errors, or a digest mismatch end the run with `die`, as for
   REF. Otherwise, if uncached, select blocks matching `h[0..16]` through
   `strong`. Each path of a block's inode gives one candidate; a block
   whose inode has no path is one candidate too. Try candidates in any
   order. Repeated location/offset pairs need only one read.
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
   `x < c`, answer no without an extent. If `h` is pinned now, and the new
   pin can share its extent, publish the pin as in step 1. If still
   needed, allocate an extent under the lock (section 6.5, steps A1 to
   A3).
7. Outside the lock, write the `B` bytes of the verified buffer to the
   extent with `pwrite`. Segments are open without `O_APPEND`; concurrent
   workers write only their assigned, nonoverlapping extents.
8. Reacquire the lock and recheck `x < c`. If now obsolete, leave the extent
   unpublished, release it (section 6.5, steps R1 to R3), and answer no.
   In Completing, release the extent without an answer. Otherwise publish
   `h` at `x`, and queue yes. All pins of `h` keep the extent of the pin
   with the highest query offset:
   - If `h` is not pinned, it is now, with this extent, and `last = x`.
   - If `x > last`, this extent replaces that of `h`, which is released,
     and `last = x`.
   - If `x < last`, another lookup has published a pin of `h` above `x`
     meanwhile. The pin shares the extent of `h`, and this extent is
     released.

A yes is binding because the receiver owns an independent copy of the
verified bytes in a segment. A later source-file change cannot affect
REF. Output may pass `x` before the answer is sent; the sender then
discards that obsolete answer.

The decoder reads the stream from its spool (section 6.5), and writes it
to the stream's destination: the client's stdout for the backward stream,
the command's stdin for the forward stream. Until `--spool-limit` bytes of
the stream wait there, a blocked destination stops only the decoder: the
sender keeps scanning and querying, and the spool and the pins hold the
stream until the destination takes it. The limit does not bound the pins.
The decoder processes the stream frames in order at `c` (section 6.6,
steps C1 to C3):

- LIT writes its payload; RLE writes the repeated byte. Accept any positive
  RLE length, though the sender emits runs of at least `B`.
- REF obtains the pin at `c` and the extent of its block under the lock.
  Outside it, read exactly `B` bytes from that extent with `pread`, and
  verify the full digest against the pin's hash before writing that
  buffer to the destination. The pin stays until the write completes and
  `c` advances. A missing pin is a protocol error; segment EOF, read errors,
  or a digest mismatch end the run with `die` without emitting that block
  or attempting to reread the source file.
- After each frame's output is written, advance `c` under the lock. In
  bounded batches, detach pins whose query offsets are below it, including
  pins covered by literals or runs. A detached pin at `last` is the last
  pin of its block: the block is no longer pinned, and its extent is
  released (section 6.5, steps R1 to R3). After each batch, release the
  lock and close the files of the segments that the batch removed from the
  open segments.
- The end of the stream follows the end contract of section 6.6.

The shared lock makes pin publication and output advancement atomic relative
to each other: a lookup either publishes before the offset passes, and the
pin is detached later, or observes the obsolete offset and releases its
extent. No extent is released while its write is still using it. A read of
an extent, for REF or for a copy, can end after the release of the extent:
a copy can replace the extent, and the output can pass `last` during the
read of a copy. The read still gets the bytes of the extent: it keeps the
file of the segment open until it ends, and the bytes of an extent never
change.

### 6.5 Segments and spools

Both hosts keep temporary data in segments: the receiver its pins
(section 6.4), and both hosts the bytes of their spools. Closing an
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

- **tmp_data directory**: the `tmp_data_dir` of each host (section 7), by
  default `$XDG_CACHE_HOME/hashsync/tmp-data/` (fallback
  `~/.cache/hashsync/tmp-data/`). Use a private directory (0700). A host
  creates it if necessary and keeps it open through the run. When it opens
  it, it creates a segment and closes it at once, to make sure that
  segments can be created: the client before the SSH sessions start, and
  the server once it has read REQUEST and its configuration. So an
  unusable directory ends the run before command startup. A disk-backed
  directory moves the data out of RAM, where tmpfs would keep it in
  memory or swap. Avoid NFS: its client writes the data of a file to the
  server before it closes the file (section 10).
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
  thread that released the extent, or at the end of a read that still
  uses the file (section 6.4). The current segment stays open when its
  count is 0: it is not full.
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
only while no byte waits for it. The writer is the source reader of the
stream, the stdout reader or the stdin reader, or the data reader of the
receiving end (section 6.6).

| Stream | Host | Source | Writer | Reader |
|---|---|---|---|---|
| Backward | Server | Command stdout | The stdout reader | Deduplicated: the lookahead reader, for the lookahead buffer (section 6.3); plain: the data writer |
| Backward | Client | The CHUNK frames of the stream | The data reader | The decoder (section 6.4) |
| Forward | Client | stdin | The stdin reader | Deduplicated: the lookahead reader; plain: the data writer |
| Forward | Server | The CHUNK frames of the stream | The data reader | The decoder |

Each spool holds at most `--spool-limit` bytes, which the server takes
from REQUEST: 2 GiB by default. `unlimited` (`2^64 − 1`, which no spool
reaches) removes the limit; a spool can then grow to almost the whole
stream when its reader stays slower than its source (section 10).

Data structures:

- **Spool extents**: the extents of 1 MiB of the spool's segments, which
  the writer allocates in stream order. The reader is in the first extent
  that is not released, and the writer appends to the last one.
- **Written** and **read**: the bytes that the writer has appended, and
  that the reader has taken. The bytes between them **wait**.
- **End**: the source ended, and the writer has appended all its bytes.
- **Stopped**: a host stopped the spool, before or after its end, because
  its bytes have no use any more (section 6.6).
- **Idle point**: in eager delivery (section 6.2), the source reader of a
  stream marks one in its spool when the source has no byte ready
  (section 6.6). The next append removes it, so the reader reports an idle
  point only when no byte follows it.

The writer:

- **W1.** Wait while bytes wait and at least `limit` bytes wait, unless
  the spool is stopped. The reader wakes the writer when it takes bytes.
  With a limit of 0, the writer takes bytes from its source when no byte
  waits.
- **W2.** Take the next bytes from the source. In eager delivery, when
  the source has no byte ready, a source reader first marks an idle point
  and wakes the reader under the lock, then waits for the source. At the
  end of the source, mark the end, wake the reader, and stop. A read error
  of a source reader ends the run with `die`.
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

A host stops a spool under its lock: it marks the spool stopped, and
wakes the writer and the reader. A stop is final, so the spool needs its
extents no more, nor the full rule: the stop takes all its extents and
open segments at once, without steps R1 to R3, which discards the bytes
that wait. Their files close after the lock is released, or after the
I/O of the writer or the reader that still uses one of them. The reader
can tell the EOF of a stopped spool from the end.

The reader releases the extents in allocation order, so the retention
property holds: with a limit, at most the limit plus one append wait, and
the files hold less than twice the limit plus 40 MiB. An append is at most
256 KiB for a source reader, and one CHUNK payload for the data reader of
a receiving end: at most 256 KiB from a hashsync peer.

Cost: on XFS, a spool passed 1 GiB in 0.36 s, about the cost of the two
copies in memory of a channel of chunks. While the reader keeps up, a
segment can fill and close before the kernel writes back its dirty pages,
by default after 30 s, and nothing reaches the disk. A slow source, such
as the data session, keeps its current segment open longer, and the
kernel then writes its dirty pages to the disk.

### 6.6 Streams

Every run has two streams: the **forward stream** carries the client's
stdin to the command's stdin, and the **backward stream** the command's
stdout to the client's stdout. One or both of them are deduplicated, each
against the index of the host that receives it, and a stream that is not
is plain: in a pull, the default, the backward stream is deduplicated,
with the client as its receiver; in a push, with `--dedup-stdin`, the
forward stream is, with the server as its receiver; and with
`--dedup-stdin --dedup-stdout`, both are (section 8). A stream always uses
the same directions of both sessions: the data session from its sending
end to its receiving end, for its CHUNK frames, and for a deduplicated
stream, the control session in both directions, for its list exchange,
QUERY and ANSWER.

Both streams are built from the same blocks. Deduplication is one more
block, which a deduplicated stream has and a plain stream does not: the
hash table, the scanner and its queries on the sender (section 6.3), and
the index, the lookup workers, and the pins on the receiver (section 6.4).

The sending end:

- The **source reader** copies the stream's source into the stream's
  spool on its host (section 6.5), and in eager delivery marks the idle
  points of the source: for command stdout, the server's **stdout reader**
  (steps O1 to O3), and for the client's stdin, the client's **stdin
  reader** (steps I1 to I3).
- The **encoder** turns the bytes of the spool into stream frames: the
  scanner in a deduplicated stream. A plain stream has no encoder of its
  own: its data writer takes the spool directly, one LIT frame for each
  take of at most 256 KiB, and a flush at each idle point that the spool
  reports. The spool reports an idle point only once its reader has taken
  every byte before it and no byte follows (section 6.5), so the data
  writer then has nothing left to write.
- The **data writer** compresses the stream frames into the stream's zstd
  frame, and sends it in CHUNK frames, then STREAM_END (steps D1 to D3).

The receiving end:

- The **data reader** of the session reads the session frames, and
  appends the payload of each CHUNK to the stream's spool on its host,
  which waits while the spool holds its limit. At STREAM_END, the receiver
  of a deduplicated stream enters Completing (section 6.4), and then the
  data reader ends the spool: so the lookups begin to end before the
  destination can see the end of the stream, and a command that updates
  the index once its stdin has ended waits at most for the lookups under
  way.
- The **decoder** decompresses the spool, reads the stream frames, and
  writes their bytes to the stream's destination: the client's stdout, or
  the command's stdin (steps C1 to C3).

A plain stream has no lookahead buffer, no hash table, no index, and no
pins. Runs of one byte are literals in it: zstd stores them as RLE blocks
of about 4 bytes per 128 KiB, and with compression disabled they travel
as they are. Its decoder takes REF as a protocol error, and accepts RLE,
which needs no state.

Data structures:

- **Spools of a stream**: one on each host (section 6.5). The spool of
  the sending end holds the bytes of the source, and that of the
  receiving end the payloads of the CHUNK frames.
- **Stop**: the stop of a stream on one host, which happens at most
  once. It stops the stream's spool on that host, puts the receiver of a
  deduplicated stream into Completing on the host that receives it, and
  ends the waits for the stop: it closes the write end of the host's
  **stop pipe**, whose EOF ends the waits in `poll`, and disconnects a
  channel that ends the waits of the scanner. Each host creates the stops
  of both streams before anything can raise them: the client before the
  sessions start, and the server at REQUEST, each with the pins of the
  deduplicated stream that it receives.

The client's **stdin reader**, from the start of the run, once the client
has sent REQUEST, reads stdin. With `-n` (section 8), the forward stream
is empty: its spool ends before the sessions start, so before any stop,
and no stdin reader runs. As
`ssh` does, the stdin reader makes a stdin that is not a terminal
non-blocking while it reads it, unless it is non-blocking already, so that
no read waits. A terminal stays as it is, because the shell of the user
shares it. Other processes can share stdin too, so the stdin reader
restores the flags of stdin at the EOF of stdin and at the stop, and every
end of the process restores them, also a fatal one (section 6.9).

- **I1.** In eager delivery, if neither stdin nor the stop pipe is ready,
  which `poll` shows without a wait, mark an idle point in the spool. Wait
  until stdin or the stop pipe is ready. At the EOF of the stop pipe, look
  at whether stdin is at its EOF, without a read, and end.
- **I2.** Read stdin. If the read finds no bytes, because another process
  that shares stdin took them first (`EAGAIN`, section 3), go to step I1.
  At the EOF of stdin, end the spool, and end. A read error ends the run
  with `die`.
- **I3.** Append the bytes to the spool, which waits while the spool holds
  its limit, until the stop (section 6.5, steps W1 and W3). Go to step I1.

The look at the stop keeps the summary from depending on whether STOP or
the EOF of stdin came first (section 8). It reads nothing, so the bytes of
stdin stay for its next reader: a stdin that can seek is at its EOF when a
`pread` at its offset finds no byte, and a pipe or a socket when its other
end has hung up and `FIONREAD` shows no byte. A terminal never is: Ctrl-D
is no hangup.

The server's **stdout reader**, from command startup, reads command
stdout. No other process has hashsync's end of that pipe, so it is
non-blocking: a read takes what the pipe holds, and costs no other system
call.

- **O1.** At the stop, end. Else read command stdout.
- **O2.** If the read finds no bytes (`EAGAIN`), in eager delivery mark an
  idle point in the spool, unless no byte came since the last one. Wait
  until command stdout or the stop pipe is ready, and go to step O1. At the
  EOF of command stdout, end the spool, and end. A read error ends the run
  with `die`.
- **O3.** Append the bytes to the spool, which waits while the spool holds
  its limit, until the stop (section 6.5, steps W1 and W3). Go to step O1.

At its end, the stdout reader closes its end of command stdout. After the
stop, the command's next write to its stdout then fails with `EPIPE`, or
SIGPIPE ends the command, as at the end of the reader of a local
pipeline. The command starts with the default action of SIGPIPE.

The **data writer** of a stream:

- **D1.** Unless the stream has stopped, take the next stream frame from
  the encoder, and compress it into the stream's zstd frame at the level
  and long-distance matching of the run, with `--compression-threads`
  worker threads (section 8). Send each 256 KiB of compressed bytes in a
  CHUNK.
- **D2.** At a flush that the encoder asks for, make every byte so far
  decodable, and send the CHUNK that holds the last bytes.
- **D3.** At the end of the stream, end the zstd frame, send the last CHUNK
  and STREAM_END, and flush the session. At the stop, end at once,
  without STREAM_END: take no more frames, however many the encoder holds,
  and send nothing more, also while zstd ends a job or the zstd frame. Only
  a CHUNK that is on its way when the stop comes still goes out. A failure
  to send is the loss of the session, which ends the run.

With compression disabled, D1 to D3 send the stream frames as they are.

The **decoder** of a stream:

- **C1.** Read the next stream frame from the spool, decompressed. At the
  normal end of the stream, end; on the server, the end of the decoder
  closes the command's stdin. The normal end is the end of the zstd frame,
  with its checksum, where a stream frame ends, with the end of the spool
  right after it; without compression, the end of the spool where a
  stream frame ends. A spool that ends inside the zstd frame or inside a
  stream frame, a zstd frame without its checksum, and bytes after the
  zstd frame, are protocol errors.
- **C2.** Write the bytes of the frame to the destination: the payload of
  LIT, the block of the pin at the output offset for REF (section 6.4), or
  the repeated byte of RLE. Only RLE is written in pieces, of at most
  256 KiB each: its 14 bytes can stand for terabytes.
- **C3.** In a deduplicated stream, advance the output offset of the pins
  (section 6.4). Go to step C1.

After the stop, the decoder writes nothing more and checks nothing: the
stopped spool gives EOF at once. On the server, the destination is the
command's stdin, and the decoder writes it without blocking. Hashsync's
end of that pipe is non-blocking, which no other process shares, so a
write takes at most the room in the pipe. The decoder looks at the stop
before each write, and while the pipe is full, it waits with `poll` for
room or for the stop pipe. So neither a descendant of the command that
holds the command's stdin without reading it, nor an RLE frame of 14
bytes that stands for terabytes, keeps it writing after the stop. A
failed write with `EPIPE` means that no process reads the command's stdin
any more: the decoder raises the stop, closes the command's stdin, and
ends. Other write failures end the run with `die`. On the client, the
destination is stdout, which the decoder writes as `ssh` does: a write
waits while stdout is full, so a consumer that pauses holds the stream
back, and the spool takes the stream meanwhile. A failed write with
`EPIPE` means that no process reads stdout any more: the decoder raises
the stop of the backward stream, and ends. Other write failures end the
run with `die`. Neither host watches an idle destination: the client
learns that its stdout is broken only at its next write to it.

**The stop.** Either stream can stop, and the host that receives it
decides: the server stops the forward stream once no process reads the
command's stdin, or once the command has exited, and the client stops the
backward stream once no process reads its stdout. The first raise of a
host's stop of a stream does this, under a lock that a raise on another
thread at the same time waits for:

- **S1.** Stop the stream's spool on this host (section 6.5): its reader
  reads the EOF of the stop, its writer discards what it appends, and the
  bytes that wait in it are dropped.
- **S2.** On the host that receives a deduplicated stream, put the
  receiver into Completing (section 6.4): its lookups end, and its index
  closes, while the run goes on.
- **S3.** Close the write end of the stop pipe, and disconnect the
  channel of the stop.
- **S4.** On the host that receives the stream, queue STOP for the control
  writer, which sends it at once (section 6.2), so that the sending host
  stops the stream too.

The server raises the stop of the forward stream when a write of the
decoder fails with `EPIPE`, at the exit of the command (step E1), and after
a startup failure. The client raises it at STOP, or at END when END comes
first: the two sessions are independent. The client raises the stop of the
backward stream when a write of the decoder fails with `EPIPE`, and the
server raises it at STOP. A later STOP, or one that comes after the stream
has ended, changes nothing. STOP can come before the client's stdin
reader reads a byte: the server starts the command at once, the command
can exit at once, and STOP travels on the other session. So the client's
stops exist from before the sessions start. After END, the server needs
no STOP: the client's stop of the backward stream then gives up its place
in the queue of the control writer (section 6.9, step N1), and a later
raise stops the stream on the client alone.

A stopped end gives up at once. Every wait of its threads also waits for
the stop: the waits of the source readers for their sources and for room in
the spool, the takes of the spool by the data writer or the lookahead
reader, the scanner's waits (section 6.3), the data reader's waits for room
in the spool, and the decoder's waits for the spool and for room in the
command's stdin. The sending end sends nothing more after the stop: no
frame that its encoder still holds, nothing more of its zstd frame, and no
STREAM_END; only a CHUNK that is on its way still goes out (step D3). The
spool of the receiving host discards what arrives after its stop: its data
reader takes CHUNK and STREAM_END the same way before and after the stop,
since the sending host can send them before STOP reaches it. Between the
two stops, the receiving host discards about what the link carries in one
round trip. STOP travels on the control session, so it does not wait behind
the stream of the other direction of the data session, which its host may
still be taking at a slow consumer's pace (section 10).

The server's **exit watcher**, from command startup:

- **E1.** Wait until the command exits, without reaping it (section 6.8).
  Then raise the stop, and end. The exit can come long before the EOF of
  the command's stdout, which a descendant can hold. A descendant that
  reads the command's stdin then reads EOF, as under SSH, where `sshd`
  closes the stdin of a command that exited.

The server's **session watcher**, from REQUEST:

- **M1.** Wait until the data session hangs up, or the stop pipe reaches
  EOF. At the EOF of the stop pipe, end. A hangup ends the run with `die`:
  the client disappeared. A pipe tells its hangup with `POLLHUP`, and a
  socket whose peer shut down its writes with `POLLRDHUP`; bytes that
  arrive do not end the wait.

The watcher exists because, until the stop, long stretches pass in which
no thread of the server reads the data session: while its main thread
updates its index, waits for `attach`, builds the table and ends its
exchanges, and while the data reader waits at the limit of the forward
stream's spool, where it reads nothing, and so would not see the EOF that
the loss of the session causes. After the stop, the spool discards what
arrives, and the data reader does not wait any more. The client needs no
watcher: it sees the loss of the server on the control session.

As with SSH, the bytes of a source that its sending host has read and not
sent are lost at the stop, a command that neither reads nor closes its
stdin makes a run send up to the server's spool limit of its stdin for
nothing, and a run in the background of an interactive shell stops at its
first read of the terminal, unless it has `-n`.

Three waits can delay the end. When another program that reads the same
terminal, such as a pager in the same pipeline, takes the bytes that
`poll` reported, the stdin reader's read of the terminal waits for its
next bytes or its EOF, and so does the client's join of the stdin reader
after END, as `ssh` waits in its read. zstd cannot cancel a job of its
worker threads: after a stop, the data writer of the stream ends only once
the workers have finished the jobs that zstd holds, about one for each
worker thread; the client closes the data session only after that, and the
server sends END only after that (section 6.9). For a producer that never
pauses, with 12 worker threads, this took less than 0.1 s at level 9,
2.4 s at level 15, and about a minute at level 19. And after the stop of
the backward stream, the server sends END only once the command has
exited: a command that ignores SIGPIPE and does not write again keeps the
run open, as it keeps a local pipeline open.

### 6.7 Failures and trust

- Non-fatal failures: filesystem failures during indexing abandon entries
  (section 4.5); during lookup they reject source candidates (section 6.4).
  Invalid cached lists are removed and treated as misses (section 5.6). A
  failed command still transfers all its output and returns its status in
  END; a startup failure returns status 127 and error text.
- External failures end the run without a core dump (section 6.9).
  Client SIGINT/SIGTERM give status `128 + signal` (section 6.8). These
  end the run with `die`: different protocol versions, features that the
  run needs and the peer does not have, and unsupported HELLO parameters,
  in a pull before command startup; sessions that end or break before END;
  failed updates and indexes that do not match the configuration; stderr
  relay read, write, or flush failures; failed reads of the client's stdin
  and of command stdout; failed writes to the command's stdin or to the
  client's stdout other than `EPIPE`, which stops the stream (section 6.6);
  configuration errors, including conflicting immutable-copy policies and
  duplicate or nested root paths; repeated directory identities detected
  during expansion or traversal; failures of the tmp_data directory setup,
  segment creation, segment I/O, or pin verification, because a yes answer
  cannot be retracted; and cache operation failures (section 5.6).
- Bugs panic and abort: invalid frames, a stream that does not end as
  section 6.6 says, the end of a zstd frame on the control session,
  invalid compressed data, unexpected answers, missing pins, protocol
  arithmetic overflow, database errors, and invariant violations.

Use trusted peers: the sender learns hashes of all indexed content.
`--no-sender-cache` disables their retention and removes earlier caches
for that receiver/index (section 5.6).

### 6.8 Threads at run time

Both hosts use blocking threads and channels, with the threads of the
table below. At the normal end, a thread ends when its work ends: what it
reads reaches EOF or END, its queue closes because no thread can send to
it any more, a write fails at an expected closure (section 6.9), the
command exits, the stream stops (section 6.6), or the lookups reach
Completing (section 6.4). Then the main thread joins it, in the order of
section 6.9. One thread is the exception: the client's signal thread
stays until the process exits, and no thread joins it, so that SIGINT and
SIGTERM during the end still give `128 + signal`.

| Host | Thread | Responsibilities |
|---|---|---|
| Server | main | Run the handshake, reading the data session itself, and start the command. Then obtain the command's exit status without reaping, wait for the END conditions of section 6.9, and send END. |
| Server | data reader | Read the data session once the handshake is done: the forward stream's CHUNK and STREAM_END, and EOF after END. |
| Server | data writer | Send the backward stream (section 6.6). |
| Server | session watcher | Until the stop of the forward stream, end the run at the hangup of the data session (section 6.6). |
| Server | stdout reader | Copy command stdout into its spool until its EOF or the stop of the backward stream, in eager delivery with its idle points, then close command stdout (section 6.6). |
| Server | decoder | Decode the forward stream into the command's stdin, with the pins when it is deduplicated, and raise its stop at `EPIPE` (section 6.6). |
| Server | exit watcher | Raise the stop of the forward stream when the command exits (section 6.6). |
| Server | stderr relay | Copy command stderr to server stderr through EOF and flush (section 6.1). |
| Server | control reader/writer | Pass the queries for a deduplicated forward stream to its lookup workers, the answers for a deduplicated backward stream to its scanner, and the frames of the list exchanges to their sides, and raise the stop of the backward stream at STOP; send answers, queries, and STOP promptly, independently of the streams, and the frames of the server's sides of the list exchanges between them (section 6.2). |
| Client | main | Start SSH sessions, run the handshake, then wait for END, and end the run (section 6.9). |
| Client | data reader | Read the data session once the handshake is done: the backward stream's CHUNK and STREAM_END, END, and EOF. |
| Client | decoder | Decode the backward stream into stdout, with the pins when it is deduplicated, and raise its stop at `EPIPE` (section 6.6). |
| Client | stdin reader | Copy stdin into the forward stream's spool (section 6.6). |
| Client | data writer | Send the forward stream (section 6.6). |
| Client | control reader/writer | Pass the queries for a deduplicated backward stream to its lookup workers, the answers for a deduplicated forward stream to its scanner, and the frames of the list exchanges to their sides, and raise the stop of the forward stream at STOP; send answers, queries, and STOP promptly, and the frames of the client's sides of the list exchanges between them. |
| Client | signal thread | Handle SIGINT/SIGTERM with status `128 + signal`. |
| Sender | lookahead reader | Fill the lookahead from the spool of the stream's source, and pass its idle points to the scanner, until its EOF or the stop; then close the spool at its end. |
| Sender | scanner | Own records and query states; queue queries and frames (section 6.3). |
| Sender | probe threads | Probe the parts of long stretches of windows for the scanner (section 6.3). |
| Sender | list exchange | Run the sender's side of the list exchange, and give the scanner its table (section 5.6). |
| Receiver | receiver | Update the index or open it, as section 4.4 orders it, start the lookup workers, and run the receiver's side of the list exchange (section 5.6). |
| Receiver | list export | Export the hash list, while the receiver's thread reads ADVERTISE and the base of a delta (section 5.6). |
| Receiver | lookup workers | Answer queries out of order, until Completing (section 6.4). |
| Attach | relay | Copy both directions (section 6.1). |

The sender of a deduplicated stream is its sending host, and the receiver
its receiving host: in a pull the server and the client, in a push the
other way around. With both streams deduplicated, each host runs the
threads of both roles, one for each stream.

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

Each host has a `die(message)` callable from any thread. It reports the
error, performs cleanup once, and exits without unwinding and without a
core dump, with status 1, or `128 + signal` when the client has received
SIGINT or SIGTERM (section 6.8). Concurrent callers wait for that cleanup
rather than exiting ahead of it:

- Server: SIGKILL the command's process group and remove the rendezvous
  socket.
- Client: SIGKILL both SSH processes, causing the server's stdin EOF and
  cleanup, and restore the flags of stdin, when the stdin reader made it
  non-blocking (section 6.6). Unlike `ssh`, which leaves them changed at
  a fatal exit, every end restores them, also the normal end.
- Process exit closes the unlinked segments of both hosts, which frees all
  their storage.
- Abandoned cache temporaries remain until the next exchange of that peer
  and index, or until expiry (section 5.6).

Build with `panic = "abort"`; install a panic hook that performs the same
cleanup before aborting. External failures use `die`; bugs and database
errors abort with a core dump (section 6.7).

Both hosts block SIGXFSZ in every thread, so that a write past the
file-size limit fails with `EFBIG` and the run ends through `die`, with its
cleanup; the default action of the signal would end the process without
it. The client also blocks SIGINT and SIGTERM in every thread, for its
signal thread (section 6.8). A blocked signal stays blocked across
`exec`, so each host starts its children with the signal mask that it
started with.

Only the normal end runs the C library's exit handlers, after the
database has closed, which stops its background jobs. A fatal end, through
`die` or a termination signal, calls `_exit` instead, and an abort runs no
handlers either: many threads can be inside RocksDB then, and the process
must neither wait for a background job nor destroy RocksDB's static
objects under them. The index recovers from its write-ahead log, as after
a crash.

The client monitors both sessions and termination signals. The server
monitors stdin and the control socket: its main thread reads stdin until
REQUEST, and in a pull the client's HELLO, the data reader reads it
afterwards, and the session watcher covers the stretches in which neither
reads it (section 6.6).
Before END, EOF or the closure of a session means that the other host
disappeared, which is fatal on both hosts, also during rendezvous and
setup, when no stream flows. The server's fatal end then kills the
command's process group, also while a descendant still holds the command's
stderr. A broken stdout of the client is no failure: it stops the backward
stream (section 6.6), and the run ends with END and the command's status,
as the producer of a local pipeline ends with its own status. A consumer
that exits after it reads all output changes nothing.

**END.** The run ends with END, which the server sends, with the
command's status, once it needs nothing more from the client:

- the command has exited, or failed to start;
- the backward stream has ended or stopped: its STREAM_END goes out
  before END, unless the client stopped it. A command that failed to start
  has an empty backward stream;
- the forward stream has ended or stopped, and its decoder has ended;
- command stderr has reached EOF, and the relay has forwarded and flushed
  all its bytes to SSH;
- the server's sides of the list exchanges have ended: an exchange that
  its receiver began with the target ends with CACHE_RESULT, and one that
  it ended before with HASHNONE (section 5.6). So the exchanges never
  outlive the run, and a short run sends no list that it has no use for;
- the control writer has written the last frames of those sides, such as
  CACHE_RESULT, HASHNONE and UPDATED, for which the client's sides wait:
  the server shuts the control session down after END, once the data
  session has ended, which the client can cause at once, and the shutdown
  would cut them off. They usually went out long before. One that comes
  last waits only behind the control frames queued before it, which go
  first; the control frames that the writer still holds can still be cut
  off.

Before END, the server joins the threads whose work ends with these
conditions, among them the backward stream's data writer and the threads
of its sides of the list exchanges. The exit watcher and the main thread
both wait for the exit of the command, so only
the join of the exit watcher, which raises the stop of the forward stream,
puts STOP before END in the control writer's queue; the control writer can
still hold it when END goes out. The decoder ends at the end of the forward
stream or at that stop. The stdout reader ends at the EOF of command stdout
or at the stop of the backward stream, and its counts go into END. The
scanner joins its probe threads.

The server, after END:

- **F1.** Join the data reader, which reads stdin to EOF; the client causes
  the EOF after END.
- **F2.** Shut down the control socket, only after the EOF of the data
  session, so the client never sees the control session close before
  END. `attach` then relays the rest of the socket to its stdout, and
  discards its stdin until EOF (section 6.1). Join the control reader,
  which reads EOF, and the control writer, which ends once its producers
  are gone, or when a write fails.
- **F3.** Join the session watcher, which ended at the stop of the forward
  stream. With a deduplicated forward stream, join its lookup workers,
  which ended at Completing, and close its pins. Remove the rendezvous
  socket, and exit zero.

The client, at END:

- **N1.** Raise the stop of the forward stream, unless STOP has raised
  it. Let the stop of the backward stream give up its place in the queue
  of the control writer: the server needs no STOP any more.
- **N2.** Close the data session's stdin once the forward data writer has
  ended, then the control session's once the control writer has ended:
  that writer ends once its producers have dropped their senders of its
  queue, the pins of a deduplicated backward stream at Completing (section
  6.4), the scanner of a deduplicated forward stream at the end of its
  scan or at the stop (section 6.3), the stop of the backward stream at
  its raise or at step N1, and the sides of the list exchanges at their
  end. The server ended its sides and wrote their last frames before END,
  so the client's sides end at once, or once those frames arrive. Neither
  close waits for the EOF of a stdout pipe (section 10).
- **N3.** Join the decoder, which writes the rest of the backward stream
  from its spool to stdout, and checks the end of the stream, or ends at
  the stop of the backward stream, also when stdout breaks after END; the
  data reader, which reads the data session to its EOF, and takes any
  byte after END as a protocol error; the control reader, which reads the
  control session to its EOF, so that SSH never fails with a broken pipe;
  the stdin reader; with a deduplicated forward stream, the output reader,
  the scanner, and the sender's side of its list exchange; and with a
  deduplicated backward stream, the receiver's thread.
- **N4.** Observe both SSH exits without reaping. Both must exit zero;
  otherwise the run failed even though END arrived. A failure in steps N2
  to N4 ends the client with `die`, also after END.
- **N5.** With a deduplicated backward stream, join its lookup workers,
  which ended at Completing, and close its pins: all their open segments,
  which frees all their storage. The tmp_data directory closes with its
  last user. The index closed when the lookups ended.
- **N6.** Print the summary (section 8) and exit with END's command
  status.

After END, EOF, also inside a frame, and failed writes on either session
are the normal end. The implementation chooses the ordering and
synchronization of the END conditions; no particular event ordering or
state machine is required.

A descendant retaining command stdout or stderr keeps the run open, as
under plain SSH; this is expected behavior, until the client stops the
backward stream, when no process reads its stdout any more: the stdout
reader then closes command stdout. A descendant that holds the command's
stdin delays nothing: the stop ends the decoder (section 6.6). A process
that inherited a session's stdout pipe from SSH delays the client's end
until it closes the pipe, but not the server's. A lookup or segment I/O
that stalls, for example on an unresponsive NFS mount, delays the end of
its host until the I/O completes. After a client fails after END, the
server ends normally: by then, the only processes left in the command's
group are descendants that have closed the command's stdout and stderr.
After fatal cleanup, a descendant that left the process group can survive
and will get a broken pipe on its next write to either closed output pipe.

## 7. Configuration

Each command reads one configuration file. `--profile NAME` selects the
file of the **profile** NAME, `$XDG_CONFIG_HOME/hashsync/NAME.toml`
(fallback `~/.config/hashsync/NAME.toml`), and `--config PATH` a file
anywhere. Without either, a command uses the profile `default`, and both
together are a usage error. A profile name is a nonempty file name, so it
contains no `/`; any other name is a usage error.

Unless the file sets `index_path`, its index is named after it:
`$XDG_CACHE_HOME/hashsync/STEM.db` (fallback `~/.cache/hashsync/STEM.db`),
where STEM is the name of the file without its extension, NAME for the
profile NAME. The files of the profiles share one directory, so no two
profiles share an index, but two files of the same name in different
directories do. So a command that uses the index of a file that
`--config` names warns when the file sets no `index_path`. All the
configuration files of an account share its account ID, its list cache,
and by default its tmp_data directory.

A configuration file describes only its own host, whatever roles the host
plays in a run. Each host reads its own file: the client the one that its
options select, when it starts, and `serve` the one that the client names
in REQUEST, once REQUEST has told it its roles. In a run, `--profile`
and `--config` select only the client's file. The server uses its
profile `default`, unless `--server-profile NAME` or
`--server-config PATH`, options of `hashsync ssh`, select another
profile or a file anywhere on the server; a relative PATH starts at the
directory where SSH starts `serve`, the remote user's home directory.

Every command says in a note which file it uses once it has read it, on
both hosts of a run, and which index when it uses one, with `~` for the
home directory: `using profile 'work' ['~/.config/hashsync/work.toml']
and index '~/.cache/hashsync/work.db'`, or `using config 'PATH'` for a
file that an option names. A host without a file of `default` says
`['~/.config/hashsync/default.toml' not found]`.

The receiver needs a file, for its roots and its index. A sender uses only
`tmp_data_dir`, where its spools go, and `hashlist_max_age`, how long its
list cache keeps lists. A file that an option names must exist, so that a
mistyped name fails instead of quietly giving the defaults. But a sender
uses the defaults of these two keys when it has no file of the profile
`default`, which it uses without such an option. So the server of a pull
and the client of a push need no file, and with both streams
deduplicated, both hosts need one.

Everything about a run is an option of `hashsync ssh` (section 8), which
the client sends to the server in REQUEST, so it is never ambiguous which
settings apply.

```toml
block_size     = "16KiB"   # at least 512 bytes; changes rebuild the index
strong_hash    = "blake3"  # blake3 or sha256
candidate_attempts = 128  # candidates tried per query, at least 1
hashlist_max_age = "30d"   # how long this host's list cache keeps a list
index_path     = "~/.cache/hashsync/default.db"  # a directory, named after this file by default
threads        = 0         # 0 = 4 × cores, clamped to 16–64
tmp_data_dir   = "~/.cache/hashsync/tmp-data"  # segments of pins and spools; any filesystem

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
the following syntax in `index_path`, `tmp_data_dir`, root `path` and
`exclude` values, and identifier `literal` and `strip_prefix` values.

| Text after TOML parsing | Meaning |
|---|---|
| `\xHH` | One byte; exactly two hexadecimal digits, with either letter case. |
| `\\` | One literal backslash byte. |
| Ordinary Unicode text | Its UTF-8 bytes, without normalization. |

Decode once, from left to right; never interpret decoded bytes as another
escape. A trailing backslash, malformed hex escape, or unsupported escape,
such as an octal one, is a configuration error. NUL is invalid in paths,
path patterns, and process arguments; literal identifiers may contain it.
Non-UTF-8 bytes are valid in all these byte-string fields. An
`index_path` or `tmp_data_dir` that is `~` or that starts with `~/`
starts at the home directory; `\x7E` there is a literal `~`, as every
escape gives a literal byte. Do not apply configuration escapes to other
fields, to names read from the filesystem, or to CLI arguments: a shell
passes any bytes but NUL, such as bash's `$'\xFF'`. Messages show byte
strings in this syntax (section 8).

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

- `hashsync update [--rebuild] [--scan-immutable] [--compact] [-j N]`:
  update or rebuild the index; `--scan-immutable` scans the completed
  immutable groups again too (section 4.2), `--compact` compacts the index
  at the end, as prune does (section 4.4), and `-j` overrides the
  configured thread count.
- `hashsync ssh [OPTIONS] [-l USER] [USER@]HOST [--] COMMAND [ARGS...]`:
  run the remote command as ssh does, reproduce its stdout, and return its
  status. It takes the arguments that rsync gives its remote shell
  (`do_cmd` in rsync's `main.c`), so `rsync -e 'hashsync ssh'` works:
  - By default, the run deduplicates the command's stdout against the
    client's index: a pull. With `--dedup-stdin`, it deduplicates its
    stdin against the server's index instead, and the command's stdout
    comes back plain: a push (section 6.6). So `rsync -e 'hashsync ssh'`
    pulls with deduplication, and `rsync -e 'hashsync ssh --dedup-stdin'`
    pushes with it. `--dedup-stdout` deduplicates the command's stdout,
    which is the default without `--dedup-stdin`; with both options, the
    run deduplicates both streams, and both hosts update their index
    (section 4.4).
  - Options can also follow HOST, as with ssh, which goes on parsing
    options after the destination. The command starts at the first
    argument after HOST that is neither an option nor the value of one,
    or right after `--`, so a command that starts with `-` needs `--`.
  - `-l USER` gives the remote user, and goes to SSH as it is.
  - By default, the client joins COMMAND and ARGS with single spaces, as
    ssh does, and the server runs that line with the user's shell
    (section 6.1). rsync escapes its arguments for that shell (`safe_arg`
    in `options.c`), so a path with spaces arrives intact.
  - `-s` (`--no-shell`) sends the words as they are, and the server runs
    them without a shell. With rsync, use it only together with rsync's
    own `-s`, which sends the paths inside rsync's protocol: without it,
    rsync puts a backslash before the spaces and the other shell
    characters of each path, which only a shell removes. So `rsync -e
    'hashsync ssh -s' src/ nas:'dst dir/'` would write into a new
    directory named `dst\ dir` and report success. An `--rsync-path` with
    shell syntax, such as `sudo rsync`, also needs the default mode.
  - As ssh does, the run sends its stdin to the command (section 6.6). `-n`
    (`--no-stdin`) sends an empty forward stream, as `ssh -n` reads
    `/dev/null` in place of stdin: the command reads EOF at once, and the
    run reads nothing from its stdin; `-n` with `--dedup-stdin` is a usage
    error. As with ssh, a run in the background of an interactive shell
    stops at its first read of the terminal, so such a run needs `-n`.
    Under `nohup`, which replaces a terminal stdin with `/dev/null` opened
    for writing, the read of stdin fails and ends the run with `die`, where
    ssh reads EOF, so `-n` is needed there too.
  - Both streams have eager delivery (section 6.2). `--batch-stdout`
    gives the backward stream batch delivery, and `--batch-stdin` the
    forward stream; `-n` with `--batch-stdin` is a usage error.
  - The streams flow from the start, while each receiver updates its index
    and sends its hash list, so a deduplicated stream finds more blocks as
    more of its list arrives (section 6.3). `--wait` makes each
    deduplicated stream wait for its whole list before its scanner scans,
    while a plain stream flows as before, and the bytes of the waiting
    stream gather in its spool. With both streams deduplicated, each stream
    waits for its own list alone. `--wait`, with the batch delivery of the
    deduplicated streams, sends the fewest bytes, however long the run
    then takes.
  - `--no-update` skips the update of each receiver's index, takes
    read-only index access (section 4.4), and still resolves current roots
    (section 4.2). `--scan-immutable` makes the update of each receiver scan
    its completed immutable groups again, as `hashsync update
    --scan-immutable` does, and `--compact` makes it end with a compaction
    of the index, as `hashsync update --compact` does; with `--no-update`,
    either is a usage error.
  - `--ssh CMD` (default `ssh`) and `--hashsync-path PATH` (default
    `hashsync`) say how the client starts the server (section 6.1).
  - `--server-profile NAME` and `--server-config PATH` select the server's
    configuration file in place of its profile `default` (section 7);
    together they are a usage error.
  - The other options set the transfer: `--lookahead SIZE` (section 6.3),
    `--negative-cache SIZE` (128 MiB), `--probe-threads N` (0 for half the
    cores of the sender), `--compression-level N` (9; from 1 to 19, and 0
    disables compression), `--no-compression-long`,
    `--compression-threads N` (0 for half the cores of the host that
    compresses, at least one), `--spool-limit SIZE` (2 GiB, or
    `unlimited`, for every spool on both hosts; section 6.5), and
    `--no-sender-cache` (section 5.6). The default lookahead is the larger
    of 256 MiB and three blocks of the receiver, which leaves room for the
    probe threads. A `--lookahead` below one block of the receiver is a
    user error, which the client reports: for a deduplicated backward
    stream before SSH starts, and for a deduplicated forward stream at the
    server's HELLO, before its scanner starts. A lookahead buffer or probe
    threads that the sender's host cannot provide end the run at the
    receiver's HELLO (section 6.2).

  After a successful transfer, also when the command fails, the client
  prints a summary to stderr:
  1. One line for each stream, `stdin` first, then `stdout`. The line
     shows the bytes that the stream's source reader read, then `-->` and
     its wire bytes, and in parentheses how it was sent: `deduplicated`,
     `compressed`, or both. A plain stream without compression has no
     parentheses. A plain stream's wire bytes are the bytes of all its
     CHUNK payloads: `stdin -- 6.46 KiB --> 3.28 KiB wire (compressed)`.

     A deduplicated stream's line splits the bytes that its destination
     took into literal, reference, and RLE bytes, and shows how many bytes
     the REF and RLE frames that carried the last two took, before
     compression. Then it gives the queries that the receiver answered no
     after it looked their blocks up in its index. False positives: the
     index has no candidate for the strong hash (section 6.4), so the
     window had the weak hash of an indexed block, but other bytes. Not
     found: the index has candidates, but none of those that the lookup
     tried verified, for example because their files changed since the
     update. Then it adds the lists that the exchange sent: the full list
     or a delta, both when the sender could not use a delta and asked for
     the full list, and none after HASHKEEP. The wire bytes count these
     lists too: `stdout -- 10.00 MiB (2.00 MiB literal, 8.00 MiB ->
     2.50 KiB refs, 0 B -> 0 B RLE, 0 false positives, 0 not found) +
     12.00 KiB hashlist --> 1.11 MiB wire (deduplicated, compressed)`.

     A stream is whole when its source reader reached the EOF of its
     source and its destination took every byte that the source reader
     read, whether or not a stop came. The stdin reader also reaches the
     EOF when it finds stdin at its EOF at the stop (section 6.6). Any
     other stream stopped, as the forward stream of `rsync -e` always
     does: rsync keeps the stdin of its remote shell open until the shell
     exits. The line of a stopped stream adds, after the wire bytes and
     before the parentheses, `pipe closed`, the CHUNK bytes that the
     receiver dropped undecoded, which the wire bytes count too, and the
     bytes that the destination took: `stdin -- 2.09 MiB --> 1.08 MiB
     wire, pipe closed, 36.00 KiB dropped, 1.05 MiB written (compressed)`.
     The server counts what its decoder of the forward stream decoded and
     wrote, what its lookups found, and what its stdout reader read, which
     END carries (section 6.2); the client counts the rest.
  2. The total. As rsync does, the bytes that the client sent and
     received on both sessions, the elapsed time of the run, including its
     index update, and the rate of the sent and received bytes in that
     time. Then the data shrinkage factor, the bytes that the destinations
     of both streams took divided by the sent and received bytes, and the
     bytes saved, the first minus the second, which is negative when the
     run sent more than it delivered: `total -- sent 1.08 MiB and received
     394.82 MiB in 220.4s (1.80 MiB/s), data shrinkage factor: 6.88, saved
     2.27 GiB of data transfer`. The CHUNK bytes of a stopped stream that
     its receiver dropped count as traffic. These counts do not include
     the bytes that SSH adds or the command's stderr.

  The counts are exact: the client prints the summary after the threads
  that count the bytes have ended (section 6.9). The lookups can still run
  when the server sends END, but a lookup answers nothing from Completing
  on (section 6.4), and only answered queries count as false positives or
  not found, so these counts are final once the stream has ended or
  stopped.
- `hashsync serve --token T` and `hashsync attach --token T`: internal
  server entry points. `serve` reads the configuration file that REQUEST
  names, so `--profile` and `--config` are a usage error there.
- `hashsync stats`: report index statistics.
- `hashsync prune [--scan-immutable]`: remove the roots and paths that no
  current copy holds any more, and the paths that exclusions or
  `one_filesystem` now leave out, without hashing, then compact the index;
  it looks up the paths of immutable roots only with `--scan-immutable`
  (section 4.2).
- `hashsync compact`: compact the index as prune does at its end, and do
  nothing else (section 4.4).

Every command also accepts these options, besides `--profile NAME` and
`--config PATH`, which select its configuration file (section 7):

- `-q` (`--quiet`): do not print progress messages and summaries to
  stderr. Warnings and errors stay.
- `--no-human-readable`: show all amounts of bytes as whole bytes.

`hashsync ssh` sends both to the server in REQUEST, and the server applies
them to its own notes.

SSH carries the server's stderr to the client's, where its lines mix with
the client's own. So once `serve` and `attach` run, every line that they
write, notes, warnings, errors, and panic reports alike, starts with
`hashsync (server): ` in place of `hashsync: `, and the user can tell on
which host to act on it. Only a usage error of their command line, which
comes before either runs, has no marker. The command's stderr passes
through unchanged.

Each note, warning, and error is one line, so that a program can read
them line by line. Messages, and the report of `stats`, show each path,
file name, profile name, pattern, and other value from outside the
program, such as a host, a word of a command, or a value of the
configuration file, between single quotes, in the byte-string syntax of
the configuration file (section 7). So a program finds where each one
ends, at the next single quote, and gets its bytes back, and a person can
copy it into the file as a TOML literal string:

| Bytes | Shown as |
|---|---|
| A backslash | `\\` |
| A single quote | `\x27` |
| A control character, U+2028, or U+2029, which some programs take for a line end | `\xHH` for each of its bytes |
| A byte that is not part of a UTF-8 character | `\xHH` |
| A `~` that starts a path, alone or before a `/` | `\x7E`: a `~` there stands for the home directory |
| Any other character | Itself, also when it is not ASCII |

A pattern keeps its glob quoting: `'/srv/\*'` is the root of the literal
name `*`. A control character that another program's text, such as an
error of RocksDB, puts into a message shows as `\xHH` too, and an error
of the configuration file names its line and column instead of quoting
its lines. A command that a message asks the user to run goes between
backquotes, in words that a POSIX shell gives back exactly and that hold
no backquote: a word of other characters than letters, digits, and
`_@%+=:,./-` goes in single quotes, with `\'` for a single quote, and
`$'\xHH'` for each byte of a control character, of a backquote, or that
is not UTF-8, as in `hashsync --profile='a'$'\xFF' update`.

Progress messages, summaries, and statistics show an amount of bytes
below 1 KiB as whole bytes, such as `512 B`, and a larger one with two
decimal digits, rounded half up, such as `1.50 KiB`, in the first of KiB,
MiB, GiB, TiB, and PiB in which its rounded value is less than 1024, else
in PiB. So 1048575 bytes, which round to 1024.00 KiB, show as `1.00 MiB`.
Settings, limits, and messages that help to debug show exact byte counts,
because rounding could make them unclear.

```sh
hashsync ssh -n backup@nas tar -C /srv/data -cf - . | tar -C /srv/data -xf -
tar -C /srv/data -cf - . |
    hashsync ssh --dedup-stdin backup@nas tar -C /srv/data -xf -
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
  outlives its failed receiver. A listing made at INDEX, while the
  receiver holds its lock, needs no clock and holds no later target
  (section 5.6).
- **The target in the receiver's first frame of the exchange:** the
  receiver had to export its list before that frame, then HELLO, and the
  sender could start on its cached list only after HASHKEEP or HASHDELTA,
  so the two sides worked one after the other. With an index of 30.3M
  blocks (21.4M distinct weak hashes, a 110 MB list), a run took 6.6–7.2 s
  after its update with an unchanged list, and 8.8–9.4 s with a changed
  one. With the target in HASHKEEP,
  HASHDELTA, and HASHLIST, the export overlapping the sender's
  preparation, and unchanged lists kept, it takes 3.4–4.0 s and
  5.6–6.4 s.
- **Writing an unchanged target again:** with its fsync, a 110 MB list
  took about half a second on each side, and so did writing a target that
  another of the receiver's peer/index directories holds. The sender keeps
  a list that it has just verified. The receiver reads its copy only when
  it serves as a base, so a damaged copy costs one full list then, which
  is rare enough. Reusing a file reuses its inode, so the cleanup of an
  older exchange also compares the modification time, which a renewal
  changes (section 5.6).
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
- **Prune with the scan of an update, without hashing:** prune would drop
  a changed or shrunk file at once, as an update does, and keep the row of
  a renamed one. But a scan lists every directory and examines every
  entry, also the small files that have no rows, where prune looks up
  only the indexed paths. With all of `/nix/store` as
  immutable roots (88,067 groups, 487,423 paths, and 2.28M small files),
  `prune --scan-immutable` took 0.9–1.0 s, and a scan of every group,
  which `update --scan-immutable` makes without hashing a file, 9.8–9.9 s.
- **Retrying permission errors below the root of an immutable group:** the
  copy's own permissions deny every retry, so a group with one private file
  would be scanned on every update, like a mutable root, and warn every
  time. With all of `/nix/store` scanned that way, an update without
  changes took 10–14 s instead of 0.3–0.5 s. Retrying only the denied
  entries of a completed group would need rows for them and partial scans
  of completed groups.
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
- **Other rules for pins with identical content:** an extent for each pin
  wrote the block of each pin again, and looked the block up in the index
  again when the cache had lost it. A copy for each new pin of a
  pinned block frees nothing where pins pile up: a released extent frees
  its storage only when its segment closes, and the other pins of the
  segment keep it open. Other shares of extents in sealed segments above
  `last` would let a block that recurs keep a sealed segment open, with
  all its released extents, for as long as it recurs; each such segment
  also doubles the size at which the current segment is full.
- **Spooling the reconstructed output:** it would write every referenced
  block a second time, and every run in full. For 3.29 GB of `/nix/store`
  files, 97% references at 4 KiB blocks, the data session took 6.3 MB, and
  such a spool would take 3.29 GB. The spool of the receiving end keeps
  the transfer going instead; only the decoder waits for the destination.
- **Spools without a limit:** with a slow link, the sender's spool would
  grow to almost the whole output: about 1.9 TB for `tar` of 2 TB at
  200 MB/s over 10 MB/s. Only the command would end sooner, not the
  transfer. On the client, a stopped consumer would let almost the whole
  backward stream wait on the disk that the consumer may be writing to.
- **A lookahead mapped from the spool:** it would allow a much larger
  `lookahead`, but a rejection makes the scanner scan again up to
  `lookahead` bytes (section 6.3), 6–44 min for 128 GiB, and the query
  states and pins grow with the lookahead too. The spool already lets the
  command run ahead of the scanner. It would save 0.15–0.35 s per GiB of
  copies, at the cost of unsafe slices over mappings and SIGBUS in place
  of `die`.
- **The scanner reading the spool of its source itself:** the read from
  the segments would move to the scanner thread, which limits the sender
  on new bytes: up to 12% of a scan.
- **Truncating a segment before it closes:** the NFS and SMB clients write
  all dirty data to the server before they send a truncation
  (`nfs_setattr`, Linux 6.18), and on local filesystems, the close of an
  unlinked file already discards its pages.
- **Batch delivery only:** the scanner keeps the last `B − 1` bytes and a
  short last literal until EOF, and each stream keeps its last frames until
  its STREAM_END, so a command that writes its next output only after the
  consumer has answered the last, such as the remote end of `rsync -e`,
  waits forever. Eager delivery costs bytes and CPU instead, where the link
  has room for them: each flush ends a zstd block or job early, and a block
  or a run whose window spans an idle point becomes literal. For a 72 MB
  text tar without matches, at level 9, paced at 2 MB/s and 20 MB/s, eager
  delivery sent 4.0% and 1.0% more bytes than batch delivery, with
  40% and 5% more sender CPU; over a link of 200 KB/s, 0.4% more. For a
  tar of 631 MB of indexed files at 16 KiB blocks, paced at 20 MB/s and
  100 MB/s, it made 300 and 52 blocks literal, and sent 3.04 MB and
  1.55 MB against 1.22 MB. Over links of 200 KB/s and 50 KB/s, it made
  312 and 106 blocks literal, and ended after 31 s and 36 s, where batch
  delivery took 32 s and 49 s: batch delivery sends its last buffers only
  at the end.
- **Sending the bytes of every idle point at once:** recording the tail,
  and flushing, at each idle point that the scanner sees. The scanner
  scans up to `lookahead` ahead of emission, so it still sees idle points
  while the data queue is full behind a slow link, and the literal tails
  then wait in the queue. For the tar of indexed files above, this made
  2,281 and 1,515 blocks literal over links of 200 KB/s and 50 KB/s, and
  took 63 s and 180 s; on a local link, 1,753 blocks at 20 MB/s. For a
  plain forward stream, flushes at the idle points of stdin while the link
  of 200 KB/s held the data writer back sent 0.2–0.3% more bytes.
- **No worker threads for a deduplicated stream in eager delivery:** each
  flush then ends only a zstd block. For the paced text above, eager
  delivery then sent 0.4–0.6% fewer bytes than batch delivery with worker
  threads, at about the same CPU. But a command that is faster than the
  scanner gives no idle points, and its output compressed 2.0–3.7 times
  slower: the unpaced text took 3.0–3.8 s instead of 1.0–1.5 s.
- **No worker threads for a plain forward stream in eager delivery:**
  each flush would then end only a zstd block. For 16 MiB of text paced at
  2 MB/s, eager delivery with 12 worker threads sent 4.2% more bytes than
  without them, and 5.2% more in writes of 4 KiB, with 64–70% more client
  CPU; over a link of 200 KB/s, which holds most flushes back, 1.2% more.
  But without worker threads, a producer that never pauses waits for one
  compressing thread, in either delivery: 72 MB of text took 2.7 s instead
  of 1.3 s, and 631 MB of binaries 24.7 s instead of 4.4 s.
- **Compressing a plain forward stream before its spool:** the stdin reader
  would compress stdin into the client's spool, and flush at an idle point
  only once the data writer had sent every byte of the spool, which a count
  of the bytes sent and an eventfd would tell it. With raw stdin in the
  spool, and the compression in the data writer, the forward stream has the
  flush rule of every stream. Against that design, with 12 zstd worker
  threads at level 9, medians of five or six runs: 631 MB of binaries
  through a pipe took 4.3–4.4 s instead of 8.9–11.0 s in eager delivery,
  because stdin arrives faster than the data writer takes it, so no idle
  point reaches the data writer, and no flush ends a zstd job early. 16 MiB
  of text paced at 2 MB/s sent 1.6% fewer bytes in eager delivery, and 0.3%
  fewer over a link of 200 KB/s. Batch delivery of the binaries took 2–3%
  longer, with 2–4% more client CPU: the spool holds raw bytes, 3.7 times
  as many as the compressed stream. The text took no longer.
- **A flush every `lookahead` bytes for every stream, in the data
  writer:** a plain stream holds back at most what makes 256 KiB of zstd
  output, about 8 GiB of zeros at level 9, and 256 KiB without
  compression, so the flush would cost it CPU and bytes for nothing: each
  flush ends a zstd job early, and waits for the jobs that zstd holds. A
  flush made at once, and not when the data writer has nothing left to
  write, would cost the same on a busy writer, whose CHUNKs flow anyway.
- **One zstd stream for all the frames of a direction of the data
  session:** the receiving end would have to decode the stream, or skip
  through it, to find the frames after it, such as END, also after a stop,
  when the stream has no use any more.
- **END as the end of the backward stream, in place of its STREAM_END:**
  the backward stream's receiving end would need a special case, and the
  client's lookups would run until END, with the index open.
- **A plain stream as the scanner with an empty table,** with a fast path
  that finds runs by checking one byte in every `B`: the scanner's buffer
  grows to 2 × (lookahead + B), about 512 MiB with the defaults, for a
  stream without queries, and its only gain over zstd is cheaper runs.
- **Discarding the forward stream after `EPIPE`:** `sshd` discards the data
  of a channel whose stdin is closed, and goes on reading it, so the client
  would read and send all its stdin while the command runs. Stopping the
  reads of the server's spool instead would hide the loss of the data
  session from the data reader. STOP stops the client at once; the cost is
  that the producer of stdin waits until the run ends, where with SSH it
  could finish.
- **Stopping the forward stream only at END:** END can wait behind
  `--spool-limit` bytes of the backward stream for a slow consumer of
  stdout, and the client would send its stdin for nothing until then:
  3.54 GB in 6.4 s over local pipes, in a test.
- **Credits for the forward stream:** the client could send only what the
  server has room for, as an SSH channel window does, so that the data
  reader never waits at its limit. That needs a report on the control
  session for each part of the stream that the decoder takes; the session
  watcher only watches for the hangup.
- **Ending the run at a broken stdout of the client:** the client would
  end with `die` at `EPIPE` on its stdout, and the server, which then sees
  the client disappear before END, would kill the command's process group.
  The command and its descendants would get SIGKILL where the end of the
  reader of a local pipeline gives them SIGPIPE at their next write, the
  run would print no summary, and the two streams would end in two ways.
  The stop of the backward stream ends the run with END and the command's
  status instead.
- **Watching an idle stdout of the client:** a client thread could wait in
  `poll` for `POLLERR` on a stdout pipe, which Linux reports once the
  pipe's last reader has closed, and stop the backward stream right away.
  Instead, the client learns that its stdout is broken only at its next
  write, as `ssh` does, so the stop waits for the next CHUNK. For output
  that compresses as well as that of `yes`, the next CHUNK can be far off:
  with a release build over a local link, `hashsync ssh localhost yes |
  head -c 1M` ended 0.49 to 12.5 s after `head` exited in a pull, and 0.35
  to 2.6 s in a push, while the server read up to 4.5 GB of `yes`; a debug
  build took up to 146 s.
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
- **Stdout EOF as the end of the server:** the client could close each
  session's stdin at the EOF of that session's stdout, after `serve`
  closes it. With SSH multiplexing, this is a deadlock: the OpenSSH mux
  client (ControlMaster) keeps its stdout open until the session ends
  (`mux.c`, `mux_client_request_session`), and `serve` waits for stdin EOF
  before it exits. A wrapper script that waits for SSH holds the pipe in
  the same way. END needs no stdout EOF, and stdin EOF always arrives: the
  client is the only writer of each stdin pipe.
- **A push mode, or a second command such as `push` or `copy`:** the two
  directions mirror each other, so the streams to deduplicate are options
  of `hashsync ssh` (`--dedup-stdin`, `--dedup-stdout`), and the rest of
  the run is the same for all of them. `copy` also suggests source and
  destination paths.
- **Run settings in configuration files:** a key would apply to some runs
  and be ignored in others, and with a file on each host it would not be
  obvious whose file decides. Every setting of a run is an option of
  `hashsync ssh`, which REQUEST carries to the server (section 7).
- **Sending stdin to the command only with an option:** rsync and other
  programs that use `hashsync ssh` as their remote shell talk both ways and
  expect ssh's default, so a pull through `rsync -e 'hashsync ssh'` would
  fail at once.
- **A limit for each spool:** separate limits for the spools of the two
  hosts would need names after the roles, which are hard to tell apart;
  one `--spool-limit` sets every spool.
- **Keeping the lock after the update, with both streams deduplicated:** a
  host would hold it while the other host can still wait for the exclusive
  lock of its own update, so two such runs in opposite directions between
  the same hosts could each hold one index and wait for the other (section
  4.4).
- **Updating the two indexes one after the other, with both streams
  deduplicated:** the server updated its index only once the client's
  update was done, and kept its lock from its update on, so only the
  client's index had a gap between its update and its use. But each list
  waited for both updates: with two indexes at full scale that need no
  changes, about 13 s, twice the 6.5 s of one update, where it now waits
  only for the slower update. An update waits for nothing else while it
  holds its exclusive lock, so the two updates can overlap; the server's
  index then has the gap too, with the check of step L2.
- **Taking the two exclusive locks in the order of the indexes' IDs:** it
  avoids that deadlock without releasing the locks, but both IDs would
  have to be known before either update, so the hosts would have to
  exchange them before their updates. `index_id` is stored in the
  database, so reading it takes a lock; a rebuild renews it, so it can
  change between the read and the lock; an index has none before its
  first update; and copies of an index directory share it, so two runs
  between such copies could not agree on an order.
- **A lock attempt that fails at once instead of waiting:** contention
  would then fail runs.
- **Starting the command after the list exchanges:** the command and both
  streams waited for each receiver's update and for both exchanges. At
  full scale, an update without changes took 6.5 s and an exchange with an
  unchanged list 3.4–4.0 s, during which no byte flowed. Now the streams
  flow from the start, and a stream deduplicates as much as the part of
  its list that has arrived allows (section 6.3). `--wait` keeps the bytes
  of the earlier design, also with both streams deduplicated, where each
  stream waits only for its own list.
- **The list exchanges on the data session:** a list would wait behind the
  other stream of its direction, whose spool on the peer is full while the
  command reads none of its stdin. In a pull, the client's list would then
  reach the server only after the command had read the forward stream,
  and with `--wait`, a command that writes all its output before it reads
  its input would hang the run. On the control session, nothing waits for
  a stream. Its control frames go first, and a frame of an exchange waits
  for at most one HASHDATA frame of 16 KiB, about 16 ms at 1 MB/s.
- **A hash table that the exchange fills under a lock:** the scanner
  probes the table once for each byte of new output, the hottest loop of a
  run, and a lock or atomic loads there would slow every probe. The
  scanner's thread owns the table instead, and takes the values that have
  arrived between scans, as owned updates on a channel, so the probes are
  as they were (section 5.5).
- **A second scanner with the whole table:** start with a scanner whose
  table is empty, and replace it once the list is whole. It finds no block
  until the whole list has arrived, while a table that fills finds each
  block as soon as its value is in, with the same probes; and the new
  scanner would have to take over the records, the query states, and the
  lookahead buffer of the old one.
- **Rescanning the windows that the table missed:** a window that was
  scanned before its value arrived stays literal. Scanning it again would
  need the bytes after it, which the scanner has emitted already, and the
  emitted frames cannot be taken back. `--wait` gives the stream its whole
  table instead.
- **Scanning with the sender's cached list until the target arrives:** the
  prepared list is usually close to the target, and would let the scanner
  find blocks during the receiver's export. But its values that the target
  no longer has would cost queries and rescans, and the table cannot
  remove values, so a delta would need a new table. Not implemented.
- **The index under `$XDG_STATE_HOME`, and the account ID under
  `$XDG_CONFIG_HOME`:** hashsync can make both again. An update rebuilds
  an index from the roots (section 3), and a new account ID starts new
  peer/index directories, while the old ones expire (section 5.6). That is
  what `$XDG_CACHE_HOME` is for, and backups commonly skip it, which suits
  an index of gigabytes. So every file that hashsync creates is there,
  besides its sockets, and the configuration directory holds only what the
  user writes. A cleaner that empties the cache costs one full update, and
  one full list for each peer and index.
- **One default index for all configuration files:** two files with
  different roots would share it, and the update of each would remove the
  roots of the other, so that the next update of the other would hash
  everything again. Each file names its index after itself instead. Only
  files of the same name in different directories still share one, hence
  the warning of section 7.
- **The server's own options select its configuration file:** a run with
  a profile other than `default` needed a `--hashsync-path` wrapper that
  passed `--profile` to `serve`, one for each profile. The remote command
  cannot carry the option, since it never changes (section 6.2). So the
  client names the file in REQUEST, and `serve` takes no option for it:
  with both, a rule would have to say which one wins.
- **A key in the client's configuration file that names the server's
  file:** it would be a run setting in a configuration file (above), and
  it would name the same file on every server that the client reaches.
- **The client's profile as the server's default:** one profile name
  would stand for the same data on both hosts, but the client's own option
  would then choose the server's file. The server of a pull only sends,
  and reads only `tmp_data_dir` and `hashlist_max_age`, which describe its
  host, not the client's data. Without a file of the client's profile, it
  would take their built-in defaults and skip its file of `default`, so
  its spools would leave the tmp_data directory that this file sets. Each
  host names its profiles on its own, so the server uses `default` unless
  an option of `hashsync ssh` names another file.
- **Paths in messages as they are, or between double quotes:** as they
  are, the bytes that are not UTF-8 would show as U+FFFD, so that
  different names would look the same, and a newline in a name would split
  a report in two. Double quotes, as Rust and Git escape strings, would
  show apostrophes as they are, but TOML reads a basic string's `\xFF` as
  U+00FF, so a path copied from a message into the configuration file
  would quietly name another file. Octal escapes in the commands that
  messages ask for would read the same in every shell, but differ from the
  `\xHH` of the rest of the messages; a `$'...'` part that holds only
  `\xHH` escapes reads the same in every shell too, since no hexadecimal
  digit follows an escape.

## 11. Repository conventions

- One crate, one binary, committed `Cargo.lock`. All Cargo profiles use
  `panic = "abort"`; install cleanup hooks before starting threads.
- Isolate unsafe syscall wrappers and SIMD code; deny unsafe code elsewhere.
- Share protocol definitions between both hosts. Until the first release,
  the protocol version stays 1, and no change adds a feature. After that,
  any change that a peer can observe after the version exchange requires a
  protocol version bump or a new feature (section 6.2), for example a
  change to frame layouts, frame order or meaning, the list encoding, or
  the frames that the zstd streams carry. New weak and strong hashes are the
  exception: HELLO names both, and a sender that does not support them
  ends the run at that HELLO (section 6.2).
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
    open by extents in use, shares and copies of pinned blocks without
    lookups, also of blocks alone in their segments, while a read uses
    the extent, and when lookups of one block race, storage failures,
    corruption detection, and cleanup on exit;
  - spools: limits, segments that close behind the reader, a stop that
    closes all segments and wakes both ends, a consumer of stdout that
    reads nothing, and a producer that waits when both spools of its
    stream are full;
  - delivery: in eager delivery, idle points that the reader of a spool
    reports only when no byte follows them, the tail and the short literal
    before an idle point, one flush after their frames, a tail and flushes
    that wait while the data writer is busy and leave once it is idle, a
    block that spans an idle point while the data session is full, and a
    command and a consumer that answer each other through both streams,
    also after the command closed its stdout; in batch delivery, a full
    buffer, the flush every `lookahead` bytes of a deduplicated stream, and
    the end of the stream as the only flushes; in both, that flush,
    counted from the last flush of any kind, and a deduplicated stream
    whose source never pauses, which reaches its destination before its
    end, in both directions;
  - pushes: the tests of what deduplication does, of the list caches, of
    the forward stream, and of `rsync -e`, in both directions; a client
    without a configuration file; a command that updates the server's index
    once its stdin has ended; a `--lookahead` below the block size of the
    server's index; the loss of the client during the server's update; the
    scanner's wait for an answer at the stop; and the descriptor limit of
    the command;
  - both streams deduplicated: end to end, with updates on both hosts and
    with `--no-update`; a stop of the forward stream while the backward
    one goes on; different block sizes and strong hashes on the two
    hosts; two runs in opposite directions whose servers wait until both
    clients have updated, which must both end; on each host, an update that
    ends while the other host's update waits for its lock, and a rebuild of
    the host's index before it opens it again, which ends the run; with
    `--wait`, a stream that deduplicates while the other waits for its
    list; a command and a consumer that answer each other; and `rsync -e`
    both ways;
  - the start: a command that runs, and whose streams flow, while the
    receiver's update waits for a lock, in both directions; with `--wait`,
    a deduplicated stream that waits for its whole list, with the notes of
    that wait, and then finds every block, and a plain stream that flows
    meanwhile; a table that finds a block once its value has arrived, and
    the note once it is whole while the stream runs; an exchange that ends
    with HASHNONE, before INDEX and after ADVERTISE, which removes no
    listed file and expires the cache; and a list on the control session
    while the spool of a stream is full;
  - the streams: the forward stream with and without compression, in eager
    and batch delivery, and `-n`; a run that ends with its command while
    stdin stays open; a command that reads a part of its stdin, closes its
    stdin, or exits while a descendant holds stdout or reads its stdin; a
    consumer of the client's stdout that exits, which stops the backward
    stream, also with both streams deduplicated; the summary line of a
    stopped stream, with bytes that its receiver dropped, in both
    directions, and with the references of the forward stream of an
    `rsync -e` push, which always stops; the summary line of a
    deduplicated stream: the bytes of its REF and RLE frames, a window
    with the weak hash of an indexed block but other bytes, which counts
    as a false positive, stale and unreadable candidates, and an intact
    copy beyond `candidate_attempts`, which count as not found, and the
    lists that each kind of exchange sent; STOP before the client starts
    its forward stream, which leaves stdin unread, and STREAM_END after
    STOP, on either host; the look at whether stdin is at its EOF, for a
    file, `/dev/null`, a pipe, a socket, and a terminal; a stop inside one
    RLE frame, and while a descendant holds the command's stdin without
    reading it; the loss of the client while the server's data reader
    waits at its limit; the ends of a stream: a zstd frame cut short,
    bytes after it, a cut inside a stream frame, and a stop inside it; a
    lookahead whose buffer the sender cannot reserve, which ends the run
    before the command; a client whose stdout is slow after END; and a
    non-blocking stdin, unless it is a terminal, with its flags restored
    at its EOF and at a fatal end;
  - one write call per frame, and non-blocking standard descriptors;
  - filesystem races and sparse files; reopening files and directories
    after rename or unlink; symlinks and special files, never opened for
    I/O; symlinks and cycles during root expansion; directory cycles and
    aliases in either traversal order; root replacement between expansion
    and scanning; links found before and after their owner's row commits;
    hard links within and across roots, also to a single-file root;
  - immutable reuse after root-directory identity changes; a missing root
    of a pattern without wildcards, and one that cannot be looked up,
    which are no copies of their groups, and a completed group whose
    copies are all missing, which goes; a copy that vanishes after root
    expansion and may vanish, which gives its place to the next copy,
    whose path the row of the group takes also when its scan fails, and
    one that may not vanish, which leaves its group unfinished; entries
    below the root of a group that permissions deny, which leave the group
    complete, and a root that cannot be read or searched, also a
    single-file root that is a hard link of a file that cannot be read,
    which leaves its group unfinished; no tasks or scans solely for
    overlap checks; completion withheld after a late directory conflict;
    conflicting copy policies; a pruned immutable root that returns;
    `update --scan-immutable`, and `ssh --scan-immutable` in both
    directions, which scan completed groups again;
  - prune: roots that no copy holds, paths that no copy holds, in mutable
    roots, and in immutable ones only with `--scan-immutable`, a path that
    another copy holds, which stays, paths that exclusions by name and by
    absolute path, also of a directory above them, or `one_filesystem` now
    leave out, a path that prune cannot look up, whose completed group the
    next update scans again, unless permissions below the root of the copy
    deny the lookup, one warning for a directory that cannot be
    searched, a root that no longer opens as a directory or a regular file,
    a copy that vanishes after root resolution, also the one that the row
    of a group that becomes unfinished named, a missing index, which prune
    does not create, the compaction, and the exclusive lock;
  - compaction: by `hashsync compact`, which changes no entry, needs an
    index that matches the configuration, creates none, and takes the
    exclusive lock; by `update --compact`; by the updates of a run with
    `ssh --compact`, in a pull, in a push, and with both streams
    deduplicated; and `ssh --compact --no-update`, a usage error;
  - configuration byte escapes, TOML quoting, literal glob metacharacters,
    and non-UTF-8 values; a `~` that starts a path, also as `\x7E`;
    messages that show paths, names, patterns, and values of the
    configuration file with control characters, quotes, backslashes, a
    leading `~`, and bytes that are not UTF-8, which a program reads back
    from its line; an error of the configuration file in one line, also on
    the server; commands in messages for names and paths with backquotes,
    newlines, and bytes that are not UTF-8, which a shell runs; a NUL byte
    in REQUEST; the server's configuration file: its file of
    the profile `default`, also when the client has `--profile` or
    `--config`, and without that file in a pull and in a push, the files
    of `--server-profile` and `--server-config`, also when they do not
    exist, and `--profile` or `--config` given to `serve`; the files of
    profiles and of `--config`, their indexes named after them, the note
    that names the file and the index of `update`, `stats`, and both hosts
    of a run, also without a file of `default`, the warning for a file of
    `--config` without `index_path`, a named file that does not exist, an
    invalid profile name, and both options together;
  - the command line of `hashsync ssh`: options after HOST, a command
    without `--`, `-l`, a command line for the user's shell and the words
    of `-s`, and `rsync -e 'hashsync ssh'` with a path with spaces, also
    with rsync's own `-s` and with `-s` on both sides;
  - SSH option precedence; bootstrap quoting of executable paths with
    whitespace, quotes, shell metacharacters, and non-UTF-8 bytes; a
    different protocol version on either side, detected before the
    features; feature bits that a side does not know, on either side;
  - crash recovery, process exclusion, the notes of a process that waits
    for a lock, and reads of later commits; the shared lock of a run after
    its update, and the index closed at Completing while the run goes on;
    cache races, also a publication whose directory is gone and a listed
    name that a later run published again; fatal cache I/O failures, and a
    warning for an empty directory that stays; the advertisement and
    removal of listed files, hidden names that are not temporaries, and the
    removal of empty directories; and invalid cached lists;
  - completion without truncation or premature exit; complete stderr
    forwarding with backpressure and late stderr EOF from descendants;
    compressed control traffic that the closure ends; late lookups; relay
    closure with control writes in flight; EOF or a closed session before
    END on either host, also while a descendant holds the command's stderr
    and after the receiver entered Completing, and the normal end after
    END, also when the data session ends inside a frame; END before STOP;
    and fatal cleanup after a child's exit status was collected without
    reaping.
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
