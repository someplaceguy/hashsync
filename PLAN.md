# Implementation plan

[DESIGN.md](DESIGN.md) defines the required behavior and design decisions.
The steps below order the user-visible capabilities. Each step should leave
a working artifact with the capabilities of all earlier steps.

Introduce configuration and command-line options alongside the features they
control, with the defaults and validation specified in DESIGN.md. Apply the
relevant correctness, failure handling, resource limits, and no-timeout
requirements as each feature becomes available.

Apply the build and repository conventions in DESIGN.md §§1, 9, and 11
from the first milestone, including the committed lockfile, panic and cleanup
policy, unsafe-code boundaries, and shared protocol definitions. Introduce
the specified dependencies and their bundling requirements alongside their
features; Nix packaging follows in step 9.

Each milestone includes the applicable behavioral tests, property checks,
process-isolation tests, and protocol fuzzing from DESIGN.md §11. Run
`cargo fmt`, `cargo clippy --all-targets`, and `cargo test` for implementation
changes.

**Status:** Steps 1 to 8 are implemented: `update` (with `--rebuild`,
`--scan-immutable`, and `-j`), `prune` (with `--scan-immutable`), and
`stats` maintain the index for mutable and immutable roots, reuse
unchanged files by identity, size, and ctime, and recover interrupted
work. `prune` removes, without hashing, the roots that no current copy
holds and the paths that none holds, in immutable roots only with
`--scan-immutable`, and then compacts the index (DESIGN.md §4.2).
The copies of an immutable group share one root row: an
update scans the first copy of an unfinished group that is there, the
next one when a copy that may vanish disappears before its scan, marks
the group complete once nothing failed, reuses a complete group without a
scan, and removes a group whose copies are all missing, while candidate
reads try every current copy (DESIGN.md §4.2 and §6.4); `ssh`
(with `--no-update`, `--ssh`, `--hashsync-path`, and the run options),
`serve`, and `attach` run a remote command as ssh does, over two SSH
sessions. The client's stdin is the forward stream, and the command's
stdout the backward stream; `-n` sends an empty forward stream. The
backward stream is deduplicated against the client's index by default,
the forward stream against the server's with `--dedup-stdin`, and both
with `--dedup-stdin --dedup-stdout`, in which both hosts update their
index at the same time, and each one releases its index from the end of
its update until the other's is done (DESIGN.md §4.4). The server
starts the command at once, and the streams flow while each receiver
updates its index and runs its list exchange on the control session: a
deduplicated stream finds more blocks as more of its list arrives, and
with `--wait`, it waits for its whole list (DESIGN.md §3 and §6.3). A
deduplicated stream has the hash-list exchange, account identities, and
durable list caches, replaces indexed blocks with references verified
through the receiver's pins, and sends constant-byte runs compactly. Both
streams and both directions of the control session are compressed with
zstd. The list exchange reuses cached lists: an unchanged list costs no
list bytes, and a changed one is sent as a direct delta from a list both
sides have whenever that is smaller than the full list, which remains the
fallback (DESIGN.md §5.6). Every stream goes through a spool on each
host, so that a slow stage of the transfer does not stop the others, and
reaches its destination at each idle point of its source, once the link
can carry it, unless `--batch-stdout` or `--batch-stdin` asks for batch
delivery (DESIGN.md §6.2). Either stream stops once no process reads its
destination, and the run then ends with the command's status; the summary
shows what each stream read, decoded, dropped, and wrote (DESIGN.md §6.6
and §8). The server marks its lines with `hashsync (server): `, and
reads its own file of the profile `default`, unless `--server-profile` or
`--server-config` selects another; every command names its file and
index in a note, on both hosts of a run (DESIGN.md §7). Step 9 is the next
milestone.

## 1. Build and inspect a local index

Deliver `hashsync update [--rebuild]` and `hashsync stats` for mutable roots.

- Support configured file and directory roots, root and exclusion globs,
  `one_filesystem`, and the configuration and index locations: profiles,
  `--config`, and indexes named after their configuration files.
- Preserve arbitrary filename bytes, including configuration byte escapes
  and literal glob metacharacters.
- Index the complete, useful blocks defined in the design, honoring
  `block_size` and both supported strong-hash choices. Handle sparse files
  and files too small or repetitive to contribute blocks. Retain completed
  inode and path rows for files at least `block_size` even when every
  complete block is skipped.
- Support hard links within and across roots. Enforce the rules for symlinks,
  special files, duplicate or nested root paths, and cycles during root
  expansion. Detect directory aliases and cycles during actual traversal
  with shared visited sets. Open roots when their scan tasks start, without
  a separate registration pass or comparison with an earlier root identity.
- Report local filesystem failures while continuing eligible work, without
  treating incomplete results as complete.
- Make the persistent index inspectable and rebuildable. Apply the specified
  access rules to updates and statistics, including waiting on contention.

Result: users can prepare an index of local content and inspect its statistics.

Design coverage: §§4.1–4.6, 5.1–5.3, 7–8.

## 2. Refresh the index efficiently and recover interrupted work

- Reuse unchanged file content across updates, using the design's identity
  and change-detection rules, including completed files with no useful blocks.
- Reflect additions, removals, renames, hard-link changes, and files that
  shrink or cease to contain useful blocks. Refresh paths even when file
  content can be reused.
- Recover from interrupted updates and rebuilds on a subsequent update,
  preserving reusable complete work and removing incomplete or orphaned work.
- Handle incompatible index settings with the specified rebuild behavior.
- Support the configured indexing concurrency and `-j` override, with the
  specified resource bounds for large files and trees.

Result: repeated updates keep a changing tree current without rehashing all
of its unchanged files, and interrupted work can be resumed safely.

Design coverage: §§4.2–4.6, 7–8.

## 3. Run remote commands and reproduce their output

Deliver `hashsync ssh [OPTIONS] [-l USER] [USER@]HOST [--] COMMAND [ARGS...]`.

At this stage, send command stdout as literals and negotiate compression
disabled. Block references and compressed output follow in steps 4 and 5.

- Reproduce stdout byte for byte and forward stderr promptly, including
  empty output, large output, and output from commands that fail.
- Preserve command arguments as bytes, run the command line with the
  user's shell as sshd does, or with `-s` its words without a shell, and
  return the specified exit or startup-failure status.
- Support `--ssh` and `--hashsync-path`; per-host SSH options belong in
  the user's SSH configuration. Let the client select the server's
  configuration file, by default the server's profile `default`.
  Preserve SSH aliases and the user's authentication and host
  configuration, with the session options required by the design.
- Allow the same installed binary to serve both receiver and sender roles.
  Reject incompatible peers before starting the command.
- Update before normal runs, then keep a shared lock until the run's
  lookups end, so that no modifying process comes between the update and
  the transfer; with both streams deduplicated, each host releases its
  lock from the end of its update until the other's is done (DESIGN.md
  §4.4). Support concurrent read-only `--no-update` runs, resolving
  current roots and reporting missing or incompatible indexes clearly.
- Export the full canonical hash list from one index snapshot. Run the
  list exchange on the control session while the command runs: INDEX,
  ADVERTISE, full-list verification, a table that fills while the list
  arrives, and `CACHE_RESULT`. The sender advertises its listed lists, but
  the receiver always sends a full list; cached-list reuse and deltas
  follow in step 6.
- Introduce account identities and durable list storage with this exchange,
  applying the specified listing, publication, renewal, and cleanup rules:
  each side lists its directory for the peer and index when its part of
  the exchange starts, the sender at INDEX, and at the end removes
  the listed files except its target, a name only while it still names the
  listed file.
  Preserve correct behavior across concurrent transfers, interrupted
  publication, missing acknowledgements, database restore, and rebuild.
- Support `--no-sender-cache`, with which neither side keeps a list for
  that peer and index, and `hashlist_max_age` expiry of lists, abandoned
  temporaries, and empty peer/index directories. Cache directory and file
  operation failures on either role are fatal, with the absence,
  invalid-content, and empty-directory exceptions in DESIGN.md §5.6.
  `CACHE_RESULT` acknowledges the accepted target without a storage-success
  flag; enabled sender caching must publish durably before acknowledgement.
- Complete only after stdout is validated and flushed, stderr is fully
  forwarded, and both SSH sessions exit successfully. Handle interrupted
  output, peer disappearance during setup or transfer, and termination
  signals with the specified failure status and child cleanup.
- Print the specified transfer summary to stderr, including actual full
  hash-list and wire byte counts. Reference, RLE, and delta counts are
  initially zero.

Result: users can pipe a remote command's output directly into a local
consumer and rely on its output and exit status.

Design coverage: §§3, 5.4–5.6, 6.1–6.2, 6.7–6.9, 7–8.

## 4. Replace transferred content with verified local blocks

- Reuse indexed blocks wherever they occur in the command's output,
  including at unaligned offsets. Empty indexes and unmatched content
  continue to transfer successfully.
- Treat stale files, unreadable candidates, and hash collisions as failed
  matches. Honor `candidate_attempts` and continue finding matches after a
  rejection, including matches overlapping a rejected window.
- Verify candidate bytes before accepting a reference and verify retained
  bytes again before output. Once a match is accepted, later source changes
  must not affect reconstruction.
- Support `tmp_data_dir`, its segments, and their closure as output
  advances. Pin storage failures or corruption fail the run without
  emitting the affected block.
- Spool each stream on both hosts, up to `--spool-limit`, so that neither
  the command nor the transfer waits for a slower stage until a spool is
  full (DESIGN.md §6.5).
- Preserve ordered output with slow consumers, concurrent lookups, and
  out-of-order, late, or obsolete answers. Honor `--lookahead` and
  `--negative-cache`, along with the design's limits on retained payloads;
  obsolete lookups must not delay END. The lookups end at Completing, once
  the stream has ended or stopped, and the index closes then (DESIGN.md
  §6.4).
- Include reference byte counts in the summary.

Result: transfers save network traffic by using verified local content,
while producing exactly the remote command's stdout.

Design coverage: §§3, 4.5, 5.1–5.3, 5.5, 6.2–6.9, 7–8.

## 5. Reduce traffic for repeated and compressible output

- Represent constant-byte runs compactly, including runs spanning input
  boundaries and the end-of-stream behavior specified in the design.
- Support zstd compression with `--compression-level` and
  `--compression-threads`, including the option to disable compression.
- Flush the compression stream before the sender waits for a reply and,
  in eager delivery, after each idle point of the stream's source once the
  data writer has nothing left to write, and in either delivery, every
  `lookahead` bytes of a deduplicated stream; end each stream's zstd frame
  before its STREAM_END. Keep query progress independent of compression
  and output backpressure.
- Compress both directions of the control session at the same level, and
  flush each one whenever its writer's queue becomes empty.
- Detect truncated or invalid compressed streams before declaring success.
  Preserve complete stderr forwarding and command-status handling.
- Report RLE byte counts and account for compression in the summary's wire
  byte count.

Result: zeros, repeated bytes, and other compressible content require less
network traffic even when local block reuse is limited.

Design coverage: §§6.2–6.9, 7–8.

## 6. Reuse hash lists across transfers

- Reuse a verified unchanged list without retransmitting it. For a changed
  list, use a cached base and a direct delta when it is smaller than the
  full transfer.
- Find a base among the advertised lists across the receiver's cached peer
  histories as specified, name it in HASHDELTA, and verify it on both
  sides. A base uses the same strong hash as the target. Each side reads
  its base once and keeps the verified bytes, or the open file, until the
  end of the exchange, the sender through its listing: a concurrent
  cleanup can remove the file at any time.
- Fall back to a full list when a base is missing or invalid, or when delta
  verification requests the permitted retry. Remove invalid cached files;
  removal failures are fatal unless the file is already gone. Other cache
  I/O failures are fatal as specified in step 3. Failed cache data must never
  change reconstructed output.
- Extend cache validation to reuse and deltas under concurrent transfers,
  interrupted publication, missing acknowledgements, database restore,
  and rebuild. Retain the storage controls and failure behavior from step 3.
- Include actual hash-list and delta traffic in the summary.

Result: subsequent transfers spend less bandwidth on setup and remain
usable when cached data is missing or stale.

Design coverage: §§5.6, 6.1–6.2, 6.7–6.9, 7–8.

## 7. Reuse immutable trees and their alternate copies

- Support immutable roots and every identifier form, including byte-valued
  identifiers and multiple configured locations sharing one identifier.
- Reuse completed immutable groups without traversing them on each update.
  Preserve that reuse when a group moves, its representative path changes,
  or a copy's root-directory filesystem identity or handle changes.
- Require matching indexing policies across copies. Apply the specified
  root-path and hard-link rules across mutable and immutable roots. Detect
  directory overlap on a best-effort basis during actual traversal; completed
  groups and unused copies have no tasks or opens solely for overlap checks.
- Make current copies available for candidate reads, including during
  `--no-update` runs. When a copy is unavailable or fails verification,
  try the candidate in the other current copies; together, the copies use
  one of the query's `candidate_attempts`.
- Retry incomplete immutable indexing, including failures or interruptions
  involving files shared with mutable roots.
- Publish new immutable completion markers only after all scan tasks finish
  and each group's traversal and dependent inode and path results succeed
  and commit, so a late directory conflict cannot leave a group complete.
- Support `update --rebuild` to apply policy changes to completed immutable
  groups, as required by the design.

Result: large immutable collections can be reused without repeated scans,
and users can move or duplicate those collections without losing their
indexed identity.

Design coverage: §§4.2–4.6, 6.4, 7–8.

## 8. Prune content that is no longer available

Deliver `hashsync prune [--scan-immutable]`, and `--scan-immutable` for
`update` and `ssh`.

- Resolve current roots and remove the paths that no current copy holds,
  without hashing file contents: those of mutable roots, and with
  `--scan-immutable`, those of immutable roots too.
- Remove the entries of locations that cannot be reached too, with a
  warning, as an update does. A completed group that loses a path this way
  is scanned again by the next update.
- Remove vanished or unconfigured roots and their unused indexed content.
  A vanished immutable root that later returns must be indexed again.
- Apply the specified exclusive-access rules and retain existing roots that
  happen to have no indexed paths. Compact the database at the end.
- Let `update --scan-immutable`, and the updates of a run with
  `ssh --scan-immutable`, scan the completed immutable groups again.

Result: users can remove stale index entries without a content scan, and
scan immutable trees again when they did change.

Design coverage: §§4.2, 4.4, 6.2, 8.

## 9. Deliver installable builds

- Provide the Nix package specified in the design, including cross and
  static build support.
- Make the completed command set available from the installed binary, usable
  locally and remotely through PATH or `--hashsync-path`.
- Build the package from the committed lockfile and verify the required
  bundled dependencies and capabilities in the installed binaries.

Result: users can install compatible receiver and sender binaries and use
the complete workflow outside the development checkout.

Design coverage: §§1, 9, 11.

Work deferred by DESIGN.md §12 remains outside these milestones.
