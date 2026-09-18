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

**Status:** Steps 1 and 2 are implemented: `update` (with `--rebuild` and
`-j`) and `stats` maintain the index for mutable roots, reuse unchanged
files by identity, size, and ctime, and recover interrupted work. Step 3
is next.

## 1. Build and inspect a local index

Deliver `hashsync update [--rebuild]` and `hashsync stats` for mutable roots.

- Support configured file and directory roots, root and exclusion globs,
  `one_filesystem`, and the configuration and index locations.
- Preserve arbitrary filename bytes, including configuration byte escapes
  and literal glob metacharacters.
- Index the complete, useful blocks defined in the design, honoring
  `block_size` and both supported strong-hash choices. Handle sparse files
  and files too small or repetitive to contribute blocks.
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
  and change-detection rules.
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

Deliver `hashsync run [--no-update] [--ssh CMD] [USER@]HOST -- COMMAND ARGS...`.

At this stage, send command stdout as literals and negotiate compression
disabled. Block references and compressed output follow in steps 4 and 5.

- Reproduce stdout byte for byte and forward stderr promptly, including
  empty output, large output, and output from commands that fail.
- Preserve command arguments as bytes, run them without an implicit shell,
  and return the specified exit or startup-failure status.
- Support the configured SSH command, host overrides, `sender_command`,
  and CLI precedence. Preserve SSH aliases and the user's authentication
  and host configuration, with the session options required by the design.
- Allow the same installed binary to serve both receiver and sender roles.
  Reject incompatible peers before starting the command.
- Update before normal runs and retain exclusive index access throughout
  transfer. Support concurrent read-only `--no-update` runs, resolving
  current roots and reporting missing or incompatible indexes clearly.
- Export the full canonical hash list from one index snapshot. Complete
  HELLO, full-list verification, table construction, and `CACHE_RESULT`
  before command startup. Initially advertise no cached base and always
  send a full list; cached-list reuse and deltas follow in step 8.
- Introduce account identities and durable list storage with this exchange,
  applying the specified publication, renewal, and cleanup rules. Preserve
  correct behavior across concurrent transfers, interrupted publication,
  missing acknowledgements, database restore, and rebuild.
- Support `sender_cache = false`, including removal of earlier retained
  lists for that receiver/index, and `hashlist_max_age` expiry. Receiver
  publication failure stops setup; sender storage failure allows transfer
  to continue with `stored: 0`.
- Complete only after stdout is validated and flushed, stderr is fully
  forwarded, and both SSH sessions exit successfully. Handle interrupted
  output, peer disappearance during setup or transfer, and termination
  signals with the specified failure status and child cleanup.
- Print the specified transfer summary to stderr, including actual full
  hash-list and wire byte counts. Reference, run, and delta counts are
  initially zero.

Result: users can pipe a remote command's output directly into a local
consumer and rely on its output and exit status.

Design coverage: §§3, 5.4–5.6, 6.1–6.2, 6.5–6.7, 7–8.

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
- Support `pin_directory`, its storage requirements, and reclamation as
  output advances. Pin storage failures or corruption fail the run without
  emitting the affected block.
- Preserve ordered output with slow consumers, concurrent lookups, and
  out-of-order, late, or obsolete answers. Honor `lookahead` and
  `negative_cache`, along with the design's limits on retained payloads;
  obsolete lookups must not delay completion.
- Include reference byte counts in the summary.

Result: transfers save network traffic by using verified local content,
while producing exactly the remote command's stdout.

Design coverage: §§3, 4.5, 5.1–5.3, 5.5, 6.2–6.7, 7–8.

## 5. Reduce traffic for repeated and compressible output

- Represent constant-byte runs compactly, including runs spanning input
  boundaries and the end-of-stream behavior specified in the design.
- Support zstd compression with the configured level and thread count,
  including the option to disable compression.
- Keep setup, short output, and query progress responsive when compression
  and output backpressure are present.
- Detect truncated or invalid compressed streams before declaring success.
  Preserve complete stderr forwarding and command-status handling.
- Report run byte counts and account for compression in the summary's wire
  byte count.

Result: zeros, repeated bytes, and other compressible content require less
network traffic even when local block reuse is limited.

Design coverage: §§6.2–6.7, 7–8.

## 6. Reuse immutable trees and their alternate copies

- Support immutable roots and every identifier form, including byte-valued
  identifiers and multiple configured locations sharing one identifier.
- Reuse completed immutable groups without traversing them on each update.
  Preserve that reuse when a group moves, its representative path changes,
  or a copy's root-directory filesystem identity or handle changes.
- Require matching indexing policies across copies. Apply the specified
  root-path and hard-link rules across mutable and immutable roots. Detect
  directory overlap on a best-effort basis during actual traversal; completed
  groups and unused copies have no tasks or opens solely for overlap checks.
- Make current copies available for candidate reads in configured priority
  order, including during `--no-update` runs. Try other configured copies
  when a candidate is unavailable or fails verification, within the
  `candidate_attempts` limit.
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

## 7. Prune content that is no longer available

Deliver `hashsync prune`.

- Resolve current roots and remove paths only when absent from every
  configured copy, without hashing file contents.
- Preserve entries when inaccessible locations leave their absence uncertain.
- Remove vanished or unconfigured roots and their unused indexed content.
  A vanished immutable root that later returns must be indexed again.
- Apply the specified exclusive-access rules and retain existing roots that
  happen to have no indexed paths.

Result: users can remove stale index entries without a content scan or
mistaking an unavailable copy for deleted data.

Design coverage: §§4.2, 4.4, 8.

## 8. Reuse hash lists across transfers

- Reuse a verified unchanged list without retransmitting it. For a changed
  list, use a compatible cached base and a direct delta when it is smaller
  than the full transfer.
- Advertise the sender's most recent verified compatible list and find
  bases across the receiver's cached peer histories as specified, including
  bases using a different supported strong hash.
- Fall back to a full list when a base is unavailable or damaged, or when
  delta verification requests the permitted retry. Failed cache data must
  never change reconstructed output.
- Extend cache validation to reuse and deltas under concurrent transfers,
  interrupted publication, missing acknowledgements, database restore,
  and rebuild. Retain the storage controls and failure behavior from step 3.
- Include actual hash-list and delta traffic in the summary.

Result: subsequent transfers spend less bandwidth on setup and remain
usable when cached data is missing or stale.

Design coverage: §§5.6, 6.1–6.2, 6.5–6.7, 7–8.

## 9. Deliver installable builds

- Provide the Nix package specified in the design, including cross and
  static build support.
- Make the completed command set available from the installed binary, usable
  locally and remotely through PATH or a configured `sender_command`.
- Build the package from the committed lockfile and verify the required
  bundled dependencies and capabilities in the installed binaries.

Result: users can install compatible receiver and sender binaries and use
the complete workflow outside the development checkout.

Design coverage: §§1, 9, 11.

Work deferred by DESIGN.md §12 remains outside these milestones.
