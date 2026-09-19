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
- **Block**: `block_size` bytes (default 32 KiB) at an aligned offset in an
  indexed file. Only complete blocks are indexed.
- **Window**: `block_size` bytes at any offset in the command's output.
  All protocol offsets count output bytes from zero.
- **Root**: a configured file or directory to index. An **immutable root**
  promises unchanging content; copies sharing an **identifier** are indexed
  as one group.
- **Index**: the receiver's SQLite database of block hashes and locations.
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
- **Pin**: a query offset's verified block, retained in an open, unlinked
  receiver file and identified by its offset in that file.
- **Record**: a sender-side description of scanned but un-emitted output:
  a Literal, Run, Query, or Ref.

## 3. One run, step by step

1. The receiver resolves configured roots and updates the index.
   `--no-update` skips the update and acquires read-only access. Index access
   is held throughout the run.
2. It exports the hash list from one database snapshot, prepares the temporary
   pin file, and starts data and control SSH sessions to the sender.
3. HELLO negotiates parameters and cached lists. The receiver durably
   publishes its target list, then sends a full list, a delta, or confirmation
   that the sender's list is current. The sender verifies it, builds its
   table, and reports whether it stored the list.
4. The sender starts the command. Its scanner sends literals for misses,
   runs for repeated bytes, and queries for possible block matches.
5. The receiver reads and verifies candidates, appending their bytes to its
   temporary pin file before answering yes. The sender emits references for
   yes answers and rescans rejected windows from the next byte. Output is
   always emitted in order; releasing a pin punches a hole in its file range.
6. The receiver reconstructs stdout. After all command stdout is processed,
   and the command's exit status is available, the sender sends END. The
   receiver validates stream completion and flushes stdout before sending
   END_ACK. Both SSH sessions must exit successfully before it returns the
   command's status.

**Verification is authoritative.** The index, hash lists, caches, and weak
hash table are hints. Verify candidates before writing pins and verify the
exact buffer read back from a pin before writing it to stdout. Both checks
use the query's full strong hash. Stale files and collisions can reduce
deduplication, never change the reconstructed output.

**No timeouts.** Waits end when the expected event occurs or a peer,
descriptor, or process goes away. Slow systems must not fail because of a
deadline; a stalled process can be inspected with a debugger. Fatal errors
end the process (section 6.7). Retry intervals and cache expiry do not fail
operations and are not timeouts.

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
relative paths from one scanned copy. A non-NULL `roots.identifier` means
the group was completely indexed. Reuse it without traversal while the
configuration still names that identifier. NULL means a mutable root or an
unfinished traversal; configuration supplies the intended kind and identifier.

Copies sharing an identifier promise equivalent content. Directory copies
must be distinct directories. A change to a copy's root-directory filesystem
identity or inode handle does not invalidate the completed group or trigger
a rescan.

All current locations of an immutable group live in memory. Earlier
`[[roots]]` entries take priority; order within a glob is unspecified. The
database stores one representative path, while candidate reads can try any
current copy. With `--no-update`, map completed rows by identifier and NULL
rows by stored path and current configuration; unmatched rows have no usable
location. A single-file root has an empty `rel_path`.

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

Before scanning, reconcile roots in one transaction:

1. Invalidate every immutable root referencing an incomplete inode
   (`size IS NULL`) before removing any of that inode's paths.
2. Remove paths under unfinished or unconfigured roots, then those roots.
   Remove incomplete inodes' remaining paths, blocks, and rows. Preserve
   complete inodes, including orphans, for reuse.
3. Retain completed immutable groups still configured. Keep their
   representative path if still present, otherwise choose the first current
   location. Create NULL-identifier rows for roots that need scanning.
4. Apply representative-path changes without violating `UNIQUE(path)`,
   including when roots exchange paths. Temporary staging values must remain
   inside this transaction and must never be used for I/O.

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
remove its paths and root row in the same transaction. Its immutable
completion marker must not survive removal, so a root that later returns is
scanned again. An existing root with no indexed paths is still valid.

### 4.3 Identity of filesystems and inodes

Device numbers can change across boots and inode numbers can be reused.
Use persistent identities:

- Filesystem: UUID from `FS_IOC_GETFSUUID`, falling back on unsupported
  filesystems to tagged `f_fsid` from `fstatfs`. An unstable fallback
  identity causes scanned files to be rehashed after reboot.
- Inode: the opaque handle from `name_to_handle_at`, which distinguishes
  inode generations and btrfs subvolumes. On `EOPNOTSUPP`, fall back to a
  tagged inode number.

`(fs_id, handle)` is the identity used both by the index and scan-time
visited sets. SQLite assigns integer IDs for foreign keys.

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

The index is one SQLite file, defaulting to
`$XDG_STATE_HOME/hashsync/index.sqlite`:

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
  -- size: NULL while processing is incomplete; otherwise stx_size bits
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

Use WAL mode with `synchronous=NORMAL`. Power loss may discard recent
commits, which is acceptable for an index of hints. Hash lists identify
their own content and are persisted independently (section 5.6).

**Process access.** SQLite's `unix-excl` VFS provides exclusion without a
separate lock file:

- A modifying process (`update`, `prune`, or normal `run`) first opens
  its writer through `unix-excl`, with no existing connections. Execute a
  real schema read to acquire the process lock before opening worker readers
  or changing the schema; opening a connection alone is insufficient.
- After successful initialization, configure WAL. Every connection in this
  modifying process, including worker readers, opens read/write through
  `unix-excl`, using one SQLite library instance. Set `query_only=ON` on
  worker connections before use. Opening a connection read-only, or through
  the normal Unix VFS, can downgrade the process lock; neither is allowed
  in this process. Normal WAL snapshots still coordinate its connections.
  Do not use `locking_mode=EXCLUSIVE`, `immutable=1`, or `nolock=1`.
- Keep the writer alive through the run's use of the index; close worker
  readers first. There must be no gap in exclusion between update and use.
- Read-only processes (`run --no-update`, `stats`) use normal read-only
  connections. A keeper connection begins a read transaction and executes
  a real schema read, retaining both until index use ends. Such processes
  can coexist but exclude a writer.
- Wait indefinitely on lock contention, replacing SQLite's default busy
  timeout. If initialization returns busy without invoking the handler,
  retry with fresh connections before opening readers. Use fresh
  connections when changing access mode.

Enable foreign keys on every connection before transactions. Delete children
before parents, as required by `ON DELETE RESTRICT`. Each connection belongs
to one thread; the sole writer uses `BEGIN IMMEDIATE`.

**Representation and recovery.**

- Store schema version, block size, weak-hash parameters, and strong-hash
  algorithm. Incompatibility requires an in-place rebuild. Read-only runs
  report that an update is required for a missing or incompatible index.
- Paths and identifiers are BLOBs, preserving non-UTF-8 bytes.
  Weak hashes and unsigned file sizes use their `i64` bit patterns;
  stored sizes are compared only for equality. NULL, not an integer
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
- `blocks_by_strong` supplies candidates using 16-byte prefixes; only a
  full digest comparison verifies a candidate. `blocks_by_weak` serves
  distinct weak-hash export.
- Rebuild under process-exclusive access by resetting schema and metadata
  in one transaction, then populating in batches and creating deferred
  block indexes. Never replace the database file by rename. Every update
  repairs missing indexes from an interrupted rebuild; read-only runs
  requiring them request an update.
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
relevant children with its shared parent descriptor.

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
no further data before EOF. Unsupported seeking (`EINVAL` or
`EOPNOTSUPP`) falls back to ordinary reads; other seek errors abandon the
file. Use separate SEQUENTIAL and NOREUSE cache-advice calls as optional hints.

Opening, metadata, directory-listing, and reading errors are local: log
them, abandon the entry, and continue. An entry that vanishes before it is
opened is skipped silently. Entries that are not indexed are normal and do
not change the exit status. A directory failure also skips unread
descendants. Mark the root unsuccessful so an immutable traversal is retried.
If inode work has begun, remove its partial result through the writer.
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
the hard limit at startup. Workers own reusable buffers and SQLite reader
connections; directory and file visited sets are shared. The default thread
count is `4 × cores`, clamped to 16–64, with a configuration override.

The hashing stage takes an open regular file and streams block rows and a
terminal result. One writer thread owns all database mutations. Its channel
is bounded by bytes, with bounded messages and backpressure; a large file
must never require a whole-file message or transaction.

Messages are keyed by `(fs_id, handle)`. Different files may interleave,
but each owner's messages stay ordered:

| Message | Effect |
|---|---|
| Begin | Mark the inode incomplete with the observed ctime, then remove old blocks in bounded transactions. |
| Blocks | Append a bounded batch of block number, weak hash, and strong prefix rows. |
| End | Store the successful size and the owner's path, marking the inode complete even if no block rows were inserted. |
| Unchanged | Reuse complete rows and record the owner's path. |
| Omit | Remove an inode too small to index and its dependent rows. |
| Path | Record an additional hard-link path after the owner's result is known. |
| Abandon | Invalidate referring immutable roots before removing paths and partial inode data; discard pending paths. |
| RootDone | Record the traversal outcome; completion publication waits until all scan tasks finish and dependent inode and path results commit. |

Every claimed file produces exactly one terminal result: End, Unchanged,
Omit, or Abandon. Only hashed files send Begin and Blocks. A Path can precede
its owner's Begin and must wait even if an old complete row exists. Later
paths use the remembered result; a failure makes every dependent root
unsuccessful. Do not retry an abandoned inode through another hard link
during the same update.

Remember failed roots even after RootDone. Once all scan tasks finish and
the writer has processed their messages, publish immutable identifiers for
successful groups whose dependent inode and path results have committed.
This prevents a group from becoming reusable before a later task detects a
directory overlap. Then remove all orphaned inodes and blocks in bounded
transactions, including orphans left by earlier interrupted cleanup.

## 5. Hashes, hash list, and hash table

### 5.1 Weak hash: mixed polynomial rolling hash modulo 2^64

The definition is shared by both roles and is part of the protocol. All
arithmetic is unsigned 64-bit modulo 2^64.

- `a` is a fixed odd multiplier with `a ≡ 5 (mod 8)`, for example
  `0x9E3779B97F4A7C15`.
- For a window `w[0..B)`, `P = sum(w[i] * a^(B-1-i))`.
  Compute directly with Horner's rule: `P = P*a + byte`.
- The weak hash is `mix64(P)`.
- Roll one byte with `P' = a*P - Tout[out] + in`, where
  `Tout[b] = b * a^B` is precomputed for the block size.

Rolling uses the unmixed `P`; indexing, list encoding, and table lookup use
the mixed value. `weak_hash_id` records the algorithm, multiplier, and mixer.

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

This function is a protocol constant and is not rolling-compatible.

### 5.3 Strong hashes

Initially support `blake3` (default) and `sha256`, both with 32-byte outputs.
Each supported algorithm has one fixed output length `S >= 16` bytes.
Configuration cannot truncate digests or choose an arbitrary XOF length.
HELLO requires agreement on the algorithm; changing it rebuilds the index.

Treat full strong-hash equality as byte equality. Only the index stores
16-byte prefixes. Queries, content caches, and list IDs use full digests.
Candidate verification hashes the exact buffer written to the pin file.
Reconstruction verifies the buffer read from that file before using it for
output.

### 5.4 The hash list

Export distinct weak hashes through `blocks_by_weak`, sort as unsigned
64-bit values, and encode canonically:

1. Width `W = 64`; no hash bits are dropped.
2. For sorted distinct values, gaps are `g_0 = v_0` and
   `g_i = v_i - v_(i-1) - 1`.
3. Encode each gap using Golomb-Rice coding: `g >> k` one bits, a zero,
   then the low `k` bits. Pack least-significant-bit first and pad the last
   byte with zeros.
4. Prefix `format_version u8, W u8, k u8, N u64`.
   Set `k = W - ceil(log2(N))`. For `N = 0`, use `k = 0` and the
   header alone. For `N = 1`, `k = 64`: a zero bit followed by all
   64 value bits, without performing an invalid 64-bit shift.

Equal sets must produce equal bytes. The list ID is the strong algorithm
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
direct delta from any available compatible base, without generations,
delta logs, or segment chains.

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

Directories are private (0700), files 0600. A versioned wrapper stores the
list ID, block size, weak-hash parameters, and encoded length. Hash only the
canonical list bytes, excluding the wrapper and peer identities.

**Exchange.**

1. Export and hash the target from one database snapshot. Once the sender's
   account is known, durably publish the target in its receiver-side peer
   directory before sending list data. Retain bytes or an open fd.
   Failure to persist the receiver's target ends setup.
2. The sender advertises its valid compatible list with greatest `ctime`,
   breaking ties by filename byte order, or explicit absence. Verify files
   before use and retain the selected base. Compatibility requires supported
   encoding and matching block size and weak hash; the base's strong
   algorithm may differ from the target's.
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
   Reply `CACHE_RESULT { target, stored: 1 }` only after publication.
   Then remove the advertised old file from that peer/index directory if
   its ID differs. A store failure warns and replies `stored: 0`, retaining
   the old cache and allowing the command transfer.
6. On `stored: 1`, the receiver may remove that exchange's old list from
   its peer/index directory, unless it equals the target. On `stored: 0`
   or lost acknowledgement, retain it until expiry. Never remove matching
   files from other peer/index directories as part of this cleanup.

`sender_cache = false` advertises no base, stores nothing, removes earlier
files in that receiver/index directory, and reports `stored: 0`.

**Publication, concurrency, and expiry.**

Publish complete files using a unique hidden temporary, file fsync, rename
to the content-derived name, and directory fsync. Make new directory
hierarchies durable too. Republish even an existing target ID rather than
trusting its filename; HASHKEEP can copy its retained verified bytes.
Published files are never edited in place.

No cache locks or mutable current-list pointer are needed. Retained fds or
bytes survive replacement and unlink. Tolerate disappearance during directory
scans and fall back to a full transfer. Cleanup racing with publication may
lose a future cache hit; it cannot invalidate current bytes. Acknowledgement
promises completed publication, not immunity from later deletion.

Touch verified files through their open descriptors after successful use.
The sender renews its accepted target; the receiver renews its target and any
base used after receiving CACHE_RESULT. Use `ctime` as the renewal marker.
After the exchange, both sides expire files in both local and remote cache
trees older than `hashlist_max_age` (default 90 days, sent in HELLO).
Apply the same age bound to abandoned hidden temporaries. Missing files and
cleanup failures are nonfatal; equal base and target IDs never justify
deleting the current target.

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

The receiver generates token `T` and starts both sessions together:

- Data: `SSH HOST hashsync serve --token T`.
- Control: `SSH HOST hashsync attach --token T`.

`serve` first listens on `$XDG_RUNTIME_DIR/hashsync/T.sock` (fallback: a
private directory under `/tmp`), mode 0600. It exchanges data HELLOs, accepts
`attach`, and completes the list exchange and CACHE_RESULT before starting
the command. Flush setup responses before waiting.

`attach` retries a not-yet-listening socket while its stdin remains open,
with no deadline. It relays stdin to the socket and the socket to stdout,
half-closing the socket on stdin EOF. Socket EOF or a peer-closed socket
error (`EPIPE` or `ECONNRESET`) ends the relay successfully after flushing
stdout, without waiting for stdin EOF or joining the other relay thread.
Either direction may observe closure first. The receiver decides whether
that closure was expected from its completion state; other relay failures
end the relay unsuccessfully.
Rendezvous waits must also observe receiver disappearance (section 6.7).

The command's argv travels in HELLO, not on the SSH command line. Start it
without a shell, with stdin from `/dev/null`, piped stdout and stderr, and
its own process group for cleanup.

A dedicated sender thread copies command stderr to `serve`'s stderr with
bounded buffering and prompt writes. SSH forwards these bytes through its
existing stderr channel; both local SSH subprocesses inherit the receiver's
stderr. The relay finishes only after command stderr reaches
EOF and all copied bytes have been written and flushed to SSH. It runs
independently of stdout scanning and control traffic. Stderr is outside the
hashsync protocol and compression stream. Successful sender exit requires
relay completion; its coordination with stdout completion is an implementation
decision (section 6.7). The receiver waits for both SSH exits before returning.

### 6.2 Frames

Every frame is `type u8, length u32, payload`. Integers are little endian.
Strings and byte strings have a `u32` byte length; lists have a `u32` item
count. Output offsets and lengths are `u64`; overflow is a protocol error.

`S` is the negotiated strong algorithm's full digest length. A `list_id`
is an algorithm string followed by its fixed-length full digest. Optional
IDs begin with a 0/1 presence byte. Base and target lists may use different
supported strong algorithms.

| Session | Direction | Types |
|---|---|---|
| data | receiver to sender | HELLO, HASHLIST, HASHDELTA, HASHKEEP, LISTDATA, END_ACK |
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
  (bytes), and `command`, a list of argv byte strings. Find argv[0] in PATH
  and run directly; a shell requires explicit `sh -c`.
- HELLO, sender to receiver (data): `protocol_version u32`,
  `program_version string`, `block_size u32`, `weak_hash_id string`,
  `strong_hash_id string`, `W u8`, `account_id [u8; 16]`,
  `base optional_list_id`.
- HELLO, both directions (control): `protocol_version u32`,
  `program_version string`. Both sides send immediately.
- HASHLIST: `target list_id, length u64`, followed by that many encoded
  bytes in LISTDATA frames.
- HASHDELTA: `base list_id, target list_id, removed_length u64,
  added_length u64`, followed by removed and added encoded lists in
  LISTDATA frames. IDs must match the advertised base and target.
- HASHKEEP: `target list_id`, equal to both advertised IDs. No data follows.
- LISTDATA: at most 1 MiB of the announced bytes. Enforce exact lengths and
  canonical encoding, including ordering and padding. Unexpected frames or
  excess bytes are protocol errors.
- HASHFULL: `target list_id`. Request a full list once after a delta
  digest mismatch, before command output. The response is HASHLIST for that
  target. Failed results are never published or used for the final table.
- CACHE_RESULT: `target list_id, stored u8`. A single response after target
  verification: 1 for durable sender publication, 0 for disabled or failed
  caching. Either permits transfer. Wrong targets or repeated results are
  protocol errors. Flush before starting the command.
- LIT: literal bytes.
- REF: empty; use the pin at the current output offset.
- RLE: `byte u8, length u64`.
- END: `status i32, error string`. Status is the command's exit status,
  `128 + signal`, or 127 plus error text if startup failed. No further
  sender-to-receiver data frames follow.
- END_ACK: empty. The receiver has validated END and compression completion
  and flushed reconstructed stdout. Send exactly once on the data session;
  it is the receiver's last data-session frame. Section 6.7 defines shutdown.
- QUERY: `offset u64, strong [u8; S]`. At most one per offset; offsets may
  go backwards after rescanning.
- ANSWER: `offset u64, found u8` (0 or 1). Answers may arrive out of order.

Before command startup, require compatible program/protocol versions and
equal block size, weak hash, strong hash, and width. The target uses the
negotiated strong algorithm. Reject unsupported algorithms and invalid digest
lengths.

The data handshake is receiver HELLO, sender HELLO, durable receiver
publication, then HASHLIST/HASHDELTA/HASHKEEP and its data. The sender replies
with CACHE_RESULT, possibly after one HASHFULL fallback. The receiver handles
the result and cache cleanup before entering its output loop. Its only
remaining data-session frame is END_ACK; it keeps both session stdin pipes
open until the sender and relay exit.

After HELLO, all sender-to-receiver data-session frames share one zstd stream,
unless compression is disabled. This includes HASHFULL and CACHE_RESULT.
Control traffic and all receiver-to-sender traffic are uncompressed.
The data writer flushes setup responses and, when requested by the scanner,
flushes compression after its queue drains. Finish the stream after END.
These flushes ensure that waiting for answers or command output cannot trap
frames inside the compressor. Compression settings are in section 7.

### 6.3 The sender's scanner

One scanner owns all output decisions, records, and query states. Other
threads perform I/O and compression. Let `B = block_size`.

**State and invariants.**

- `e`: next output byte to emit; `p`: next byte to scan; `r`: end of
  command bytes read. Initially all are zero, with `e <= p <= r`.
- The lookahead buffer retains immutable bytes `[e, r)`, including scanned
  bytes that a rejection may require again. Its capacity is `lookahead + B`;
  the reader pauses when full. Scanning starts a step only while
  `p - e < lookahead`; a full-window step may advance `p` by `B`.
- Ordered records cover exactly `[e, p)`, without gaps or overlaps:
  `Literal { offset, length }`, `Run { offset, length, byte }`,
  `Query { offset }`, and `Ref { offset }`. Queries and references each
  cover `B` bytes.
- The query-state map is keyed by offset and stores the full digest with
  state Not_sent, Waiting, Yes, or No. Keep entries until their offsets
  fall below `e`, even when their records are discarded.
- A byte-bounded negative LRU cache holds full strong hashes answered no
  (default 100 MiB). It suppresses queries for the same content at other
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
  record follows, before waiting, or at EOF; otherwise let it grow.

Advance `e` only after the frame is accepted by the data writer. LIT owns
a copy before those buffer bytes are released. Remove emitted record portions
and map entries below `e`.

Outgoing data is bounded. Queueing it must not block the scanner from
processing answers. Outgoing QUERY messages use an unbounded, nonblocking
queue and remain ordered even when offsets go backwards. Discarded records
can leave obsolete requests; lookahead is not a bound on all outstanding
queries or receiver pins.

When progress requires input, answers, or data-queue space, first emit ready
short literals and request a compressor flush after the queue drains. Wait
on notifications for those events, continuing to prioritize answers.
Backpressure propagates from the data writer through the lookahead buffer
to the command. Pin allocation and release are entirely receiver-local.

Finish only after stdout EOF, `p = r`, and no Query records remain. Drain all
records so `e = p = r`. END can be queued once the exit status is available;
coordinate shutdown according to section 6.7. Flush queued data before waiting
for further completion conditions. Obsolete unanswered requests do not delay
completion.

### 6.4 The receiver's answering and reconstruction

The receiver keeps pin payloads in one open, unlinked temporary file per
run. Its map from query offsets contains only pin-file offsets and full
strong hashes. File offsets are independent of command-output offsets.
Each pin has its own file range, including pins with identical content.

**Pin storage.**

Before command startup, create the file in `pin_directory`, defaulting to
`$XDG_CACHE_HOME/hashsync/pins/` (fallback `~/.cache/hashsync/pins/`). Use a
private directory (0700), a read/write file (0600), and close-on-exec.
Prefer `O_TMPFILE`; otherwise create a unique file securely and unlink it
before writing any payload. Keep the descriptor open through the run.
Choose a disk-backed directory to move payload storage out of RAM; a tmpfs
directory would store it in memory or swap instead.

Append verified blocks at monotonically increasing offsets. Give each pin
an extent aligned to the filesystem's block allocation granularity, with
`stride = round_up(B, granularity)`. The first `B` bytes contain the block;
any padding belongs exclusively to that pin. Releasing it punches the whole
extent using `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`. This
frees its `[offset, offset + B)` data and padding without moving other pins.
Alignment matters because partial filesystem blocks are zeroed rather than
deallocated (see [fallocate(2)](https://man7.org/linux/man-pages/man2/fallocate.2.html)).
Do not reuse released extents during the run; the logical file length grows
with appends while holes reclaim allocated storage.

Require hole-punch support and probe it on a written range during setup.
Pin-file creation, append, read, or hole-punch failures, including exhaustion
of space or file-size limits, end the run with `die` without a core dump.
If writing a pin runs out of disk space, fail the run. No fsync or crash
recovery is required for this transient file; closing its last
descriptor reclaims all remaining storage.

The receiver tracks output offset `c`, its pin map, and the next append
offset. A bounded 256 MiB LRU cache of verified buffers, keyed by full strong
hash, can serve candidate lookups. Populate it on verified reads; its
eviction is independent of pin lifetimes. Pins never retain those buffers.
One lock protects this state and the completion state. Source reads, pin-file
I/O, and stdout writes occur outside the lock. In-memory payload storage is
limited to the cache and worker/reconstruction buffers; pin metadata and
queued requests still scale with outstanding work. The filesystem manages
reclaimable page-cache pages and backing storage for pin payloads.

A pool of 4–8 lookup workers, each with a reader connection, answers queries
out of order. The receiver does not reject a match merely because many pins
exist, and pin release requires no protocol message.
Queue all answers nonblockingly under the shared lock, checking the
completion state before publication. Completing suppresses new work and
answers, including no answers from obsolete lookups.

For `QUERY { offset: x, strong: h }`:

1. Under the lock, check `c`. If `x < c`, answer no without a pin.
   Otherwise try the verified-buffer cache.
2. If uncached, select blocks matching `h[0..16]` through
   `blocks_by_strong`. Enumerate indexed paths and current root locations,
   preferring configured order. Repeated location/offset pairs need only
   one attempt.
3. Try at most `candidate_attempts` locations (default 128, positive),
   across all blocks, paths, and copies. Failed opens/reads count, as does
   a block with no usable location; stale rows must not evade the budget.
4. Open a single-file root directly. Otherwise open its root directory and
   resolve the relative path beneath it without symlinks
   (`openat2` with `RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS`, or component-wise
   `openat` on older kernels). Use the `O_PATH` classification and procfs
   reopening rules of section 4.5: check that the opened object is a regular
   file before obtaining its readable descriptor. Close descriptors after
   each attempt.
5. Read exactly `B` bytes at `block_no * B`, accumulating short reads.
   Verify the full digest of that buffer. Errors, early EOF, and mismatches
   try the next candidate; filesystem failures are misses, not fatal.
   Alternate copies need not share the indexed inode identity.
6. If nothing verified, answer no. Otherwise recheck `c` under the lock. If
   `x < c`, answer no without appending. If still needed, allocate a distinct
   append extent by advancing the file's append offset under the lock.
   Check file-offset arithmetic before allocating it.
7. Outside the lock, write the verified buffer's `B` bytes with `pwrite`,
   accumulating short writes. Open the file without `O_APPEND`; concurrent
   workers write only their assigned, nonoverlapping extents. Finish every
   write before publishing the pin or answering yes.
8. Reacquire the lock and recheck `x < c`. If now obsolete, leave the extent
   unpublished, punch it outside the lock, and answer no. Otherwise publish
   `{ file_offset, strong: h }` at `x` and queue yes nonblockingly under the
   lock. Release the worker's buffer. The Completing state prevents new
   appends or pin publication; late workers discard their results and the
   unlinked file is reclaimed at process exit (section 6.7).

A yes is binding because the receiver owns an independent copy of the
verified bytes in its pin file. A later source-file change cannot affect
REF. Output may pass `x` and release its extent before the answer is sent;
the sender then discards that obsolete answer.

Process data frames in order at `c`:

- LIT writes its payload; RLE writes the repeated byte. Accept any positive
  RLE length, though the sender emits runs of at least `B`.
- REF obtains the pin at `c` under the lock. Outside it, read exactly `B`
  bytes from the pin-file offset with `pread`, accumulating short reads,
  and verify the full digest against the pin's stored hash before writing
  that buffer to stdout. Keep the extent live until the write completes and
  `c` advances. A missing pin is a protocol error; pin-file EOF, read errors,
  or a digest mismatch end the run unsuccessfully without emitting that
  block or attempting to reread the source file.
- After each frame's output is written, advance `c` under the lock. In
  bounded batches, detach pins whose query offsets are below it and punch
  their extents outside the lock, including pins covered by literals or
  runs. Finish this reclamation before processing the next frame, even if
  no further frame arrives. An unpublished write that becomes obsolete is
  cleaned up by its lookup worker after that write finishes.
- END carries the command status into shutdown. Validate compression
  completion and flush stdout before END_ACK (section 6.7). EOF without
  END or before the compressed stream is complete is failure.

The shared lock makes pin publication and output advancement atomic relative
to each other: a lookup either publishes before the offset passes and has its
extent released, or observes the obsolete offset and discards its extent.
No extent is punched while its append or REF read is still using it.

### 6.5 Failures and trust

- Incompatible HELLO parameters fail before command startup.
- Command failure still transfers all output and returns its status in END.
  Startup failure sends status 127 and error text.
- Truncated streams, broken sessions or receiver stdout, and receiver
  SIGINT/SIGTERM end the run unsuccessfully without a core dump.
- Stderr relay read, write, or flush failures also end the run with `die`
  without a core dump.
- Invalid frames, unexpected answers, missing pins, protocol arithmetic
  overflow, database errors, and invariant violations are bugs: panic and
  abort.
- Conflicting immutable-copy policies, duplicate or nested root paths, and
  repeated directory identities detected during expansion or traversal call
  `die` without a core dump.
- Filesystem failures during indexing abandon entries; during lookup they
  reject source candidates. Pin-file setup, I/O, reclamation, or verification
  failures end the run with `die` without a core dump; a yes answer cannot
  be retracted. Sender cache-store failures allow transfer with
  `stored: 0`; failure to persist the receiver's target ends setup without
  a core dump.

Use trusted peers: the sender learns hashes of all indexed content.
`sender_cache = false` disables retention and removes earlier caches for
that receiver/index.

### 6.6 Threads at run time

Both roles use blocking threads and channels. Fatal shutdown ends the process
rather than cancelling or joining blocked threads. Normal completion uses the
coordination chosen by the implementation, subject to section 6.7.

| Role | Responsibilities |
|---|---|
| Sender main | Handshake, command startup, then receive END_ACK or detect receiver disappearance. |
| Sender reader | Fill lookahead until stdout EOF; obtain the command's exit status without reaping. |
| Sender stderr relay | Copy command stderr to sender stderr through EOF and flush. |
| Sender scanner | Own records and query states; queue queries and output. |
| Sender data writer | Drain output in order, compress, and finish and flush the stream after END. |
| Sender control reader/writer | Receive answers and send queries promptly, independently of data output. |
| Receiver main | Start SSH sessions, reconstruct stdout, validate completion, and send END_ACK. |
| Receiver control reader/writer | Dispatch queries to lookup workers and send answers promptly. |
| Receiver signal thread | Handle SIGINT/SIGTERM with status `128 + signal`. |
| Attach relay | Copy both directions; half-close on stdin EOF, exit after flushing on socket EOF. |

Blocking output provides backpressure; it must not prevent independent
control traffic. Completion requirements are in section 6.7.

Keep successfully spawned children unreaped until their owning `hashsync`
process exits. Obtain command and SSH exit statuses with
`waitid(P_PID, pid, WEXITED | WNOWAIT)`; do not use reaping waits such as
`Child::wait` or `Child::try_wait`. Before spawning, set `SIGCHLD` to
`SIG_DFL` with `SA_NOCLDWAIT` clear. Retaining the command's group leader
reserves its process-group ID, and retaining the SSH children reserves their
PIDs, so late cleanup cannot signal an unrelated process after ID reuse.

### 6.7 Shutdown

Fatal errors end the process rather than attempting to interrupt arbitrary
blocked threads. Persistent state is recoverable: SQLite uses WAL and cache
publication is atomic. Kernel descriptor closure propagates EOF or errors
to peers.

Each role has a `die(status, message)` callable from any thread. It reports
the error, performs cleanup once, and exits without unwinding. Concurrent
callers wait for that cleanup rather than exiting ahead of it:

- Sender: SIGKILL the command's process group and remove the rendezvous socket.
- Receiver: SIGKILL both SSH processes, causing sender stdin EOF and cleanup.
- Process exit closes the receiver's unlinked pin file, reclaiming all
  remaining extents without a separate cleanup pass.
- Abandoned cache temporaries remain for expiry.

Build with `panic = "abort"`; install a panic hook that performs the same
cleanup before aborting. External failures use `die` without a core dump;
bugs and database errors abort. Per-entry failures remain local.

The receiver monitors stdout, both sessions, and termination signals.
The sender monitors stdin, stdout, and the control socket. Unexpected EOF or
session loss is fatal. Monitoring also applies during rendezvous and setup,
when no command output is flowing. Normal shutdown uses END_ACK, not stdin
EOF, to establish receiver completion.

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

Keep sender stdout open until process exit. Once the completion conditions
hold, perform normal cleanup once: remove the rendezvous socket and exit zero.
Closing the control socket lets `attach` flush and exit zero even though its
stdin is still open.

Receiver completion:

1. After END, the receiver consumes the rest of that zstd frame, checking its
   trailer and rejecting any further decoded bytes. Stop at frame completion
   without waiting for transport EOF; when compression is disabled END is
   sufficient framing. Flush reconstructed stdout.
2. The receiver enters Completing before sending and flushing END_ACK on
   the data session. It stops admitting lookups and publishing control
   replies; queued replies and late control traffic can be discarded. It
   keeps both session stdin pipes open while awaiting peer exit. Workers
   returning later discard their results, including results of unfinished
   pin-file writes; process exit reclaims the unlinked file. Control EOF,
   broken pipes, and a partial final control frame in Completing are expected
   closure, not failures. Do not wait for blocked lookup or control threads.
3. The receiver drains data-session stdout to transport EOF, rejecting any
   trailing bytes, and observes both SSH exits without reaping. Both must
   exit zero; otherwise the transfer failed even if END was received. After
   success it closes the session pipes and exits with END's command status.

The receiver's Completing transition uses the same lock as pin allocation
and response publication. This prevents late lookup completion from trying
to prolong the exchange. Sender stdin or control EOF before its normal exit
remains a failure: the receiver never closes those pipes to acknowledge
success. Failures during stream validation, stdout flushing, or sending
END_ACK take the fatal cleanup path.

No step has a deadline. A descendant retaining command stdout or stderr
keeps the run open, as under plain SSH; this is expected behavior. After fatal
cleanup, a descendant that left the process group can survive and will get
a broken pipe on its next write to either closed output pipe.

## 7. Configuration

Use `$XDG_CONFIG_HOME/hashsync/config.toml`, or `--config PATH`:

```toml
block_size     = "32KiB"   # at least 4 KiB; changes rebuild the index
strong_hash    = "blake3"  # blake3 or sha256
candidate_attempts = 128  # total attempts per query, at least 1
hashlist_max_age = "90d"
index_path     = "~/.local/state/hashsync/index.sqlite"
threads        = 0         # 0 = 4 × cores, clamped to 16–64
lookahead      = "256MiB"  # at least block_size
pin_directory  = "~/.cache/hashsync/pins"  # receiver file storage; supports hole punching
negative_cache = "100MiB"
compression_level   = 9    # zstd 1–19; 0 disables
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

Width, weak-hash multiplier, and mixer are protocol constants. Account IDs
are persistent state, not TOML options.

**Byte strings.** TOML strings contain Unicode characters, not arbitrary
bytes. Even TOML 1.1's `"\xFF"` denotes U+00FF, whose UTF-8 bytes are
`C3 BF`, not the single byte `FF`. After TOML parsing, `hashsync` interprets
the following syntax in `index_path`, `pin_directory`, `sender_command`,
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
  Print one summary line to stderr: output, literal, reference, run, wire,
  and hash-list/delta byte counts.
- `hashsync serve --token T` and `hashsync attach --token T`: internal
  sender entry points.
- `hashsync stats`: report index statistics.
- `hashsync prune`: acquire exclusive index access, resolve roots, and
  remove paths absent from every configured copy. Remove vanished or
  unconfigured roots together with their paths, then orphaned inodes and
  blocks. Inaccessible locations are not proof of absence (section 4.2).

```sh
hashsync run backup@nas -- tar -C /srv/data -cf - . | tar -C /srv/data -xf -
```

## 9. Libraries

Use stable Rust, edition 2024. Bundle SQLite and libzstd; these and BLAKE3's
SIMD support require a target C compiler. There is no separate pure-Rust build.

| Need | Choice |
|---|---|
| SQLite | `rusqlite` with bundled SQLite |
| Strong hashes | `blake3`, `sha2`, behind one shared strong-hash interface |
| System calls | `rustix`; `libc` and `linux-raw-sys` for missing wrappers/constants |
| Indexing pool and channels | `rayon`, `crossbeam-channel`; blocking standard threads for transfer |
| Compression | `zstd` binding libzstd with worker-thread support |
| Anonymous pin files | `tempfile`; hole punching through `rustix` |
| Configuration and CLI | `serde`, `toml`, `clap`; `gix-glob` for byte patterns and paths |
| Maps and caches | Standard maps with a fast hasher; `lru` for bounded caches |
| Signals | `signal-hook` |
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
- **A separate index lock file:** possible, but all read/write `unix-excl`
  connections with `query_only` worker readers preserve process exclusion
  without a second persistent locking object. Mixing VFSes or read-only
  connections inside that process does not preserve this guarantee.
- **Async runtimes and `io_uring`:** the blocking pool already serves the
  file-oriented workload, and `io_uring` availability would require a
  fallback. Keep one I/O model.
- **Direct I/O or mmap for file hashing:** direct I/O bypasses useful cached
  data; mmap exposes the process to SIGBUS on concurrent truncation.
  Use buffered `pread`.
- **Opening candidates by inode handle:** requires privileges. Open indexed
  paths and verify the resulting bytes instead.

## 11. Repository conventions

- One crate, one binary, committed `Cargo.lock`. All profiles use
  `panic = "abort"`; install cleanup hooks before starting threads.
- Isolate unsafe syscall wrappers and SIMD code; deny unsafe code elsewhere.
- Share protocol definitions between both roles. Changing frame layouts,
  hash constants, list encoding, or HELLO fields requires a protocol version
  bump.
- Run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test` for
  implementation changes.
- Validate hashing/encoding properties, empty-table misses, and absence of
  table false negatives;
  byte-exact reconstruction under rescans, late answers, and backpressure;
  pin-file appends and reclamation with slow stdout, concurrent lookups,
  rejected-query rescans, and late answers; obsolete writes, unaligned block
  sizes, neighbor preservation during hole punching, pin-file failures,
  corruption detection, and cleanup on exit; filesystem races and sparse
  files; reopening regular files and directories after rename or unlink;
  skipping symlinks and special files without opening them for I/O;
  configuration byte escapes, TOML quoting, literal glob metacharacters,
  and non-UTF-8 root, exclusion, and identifier values; symlinks and cycles
  during root expansion; fatal directory cycles and aliases in either
  traversal order; root replacement between expansion and scanning;
  immutable reuse after root-directory identity changes; no tasks or opens
  solely for overlap checks on cached groups or unused copies; completion
  withheld after a late directory conflict; hard links within and across
  roots (including links to a single-file root); conflicting copy policies;
  pruning a vanished immutable root and indexing it again when it returns;
  SSH option precedence and bootstrap quoting of executable paths containing
  whitespace, quotes, shell metacharacters, and non-UTF-8 bytes;
  crash recovery and SQLite process exclusion and snapshots after worker
  connections open; cache publication and cleanup races; completion without
  truncation or premature exit under the chosen synchronization scheme;
  complete stderr forwarding with backpressure and delayed stderr EOF from
  descendants; truncated compression trailers, late lookups, relay
  closure with control writes in flight, shutdown on session loss, and fatal
  cleanup after a child's exit status has been collected without reaping. Use
  separate processes for locking tests and fuzz protocol decoding.
- Package with Nix `rustPlatform.buildRustPackage`, including support for
  cross and static builds.

## 12. Deferred work

Measure real workloads before adding adaptive compression, different
indexing concurrency for spinning disks or very large files, or deltas that
encode removals by rank in the base list.

Measure rejected-query rescans and dense stale hits before adding scanner
state to reduce repeated work. The current design deliberately accepts
rescanning a suffix for simpler state transitions.
