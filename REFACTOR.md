# Refactor: `hashsync ssh` and two streams

This file records the decisions for the next refactor of hashsync. It was
written on 2026-10-14 against commit 357cbac, when PLAN.md steps 1–6 were
done. Where it disagrees with DESIGN.md, this file wins; update DESIGN.md
to match as the work lands.

## Goal

`hashsync run` deduplicates only the remote command's stdout, against an
index on the host that runs hashsync. After this refactor, a run can
instead deduplicate the stdin that hashsync sends to the remote command,
against an index on the remote host. That covers pushes to a host that
already holds the old data: backups to a NAS, rsync pushes through `rsync
-e`, nix closures copied to another host. Today these need the
destination to log in to the source and pull, which fails when the source
accepts no logins. A run can also deduplicate both streams, each against
the index of the host that receives it.

## Terms

- **Client**: the host that runs `hashsync ssh`. It starts both SSH
  sessions and has the user's stdin, stdout and exit status.
- **Server**: the host where SSH starts `hashsync serve` and `hashsync
  attach`. It runs the command.
- **Forward stream**: the client's stdin to the command's stdin.
  **Backward stream**: the command's stdout to the client's stdout. They
  replace DESIGN.md's "input" and "output".
- **Sending end** and **receiving end**: the parts of a stream on the host
  that reads its source and on the host that writes its destination.
- **Deduplicated stream**: a stream that is deduplicated; a run has one
  or two. Its **receiver** has the index: it sends the hash list, answers
  queries and keeps the pins. Its **sender** scans the stream against the
  list. A stream that isn't deduplicated is a **plain stream**.
- **Session frames** and **stream frames**: the two levels of frames on
  the data session (see Wire format).

Sender and receiver now name roles in a deduplicated stream, not hosts.
With the backward stream deduplicated (a pull), the client is the
receiver; with the forward stream deduplicated (a push), the client is the
sender; with both, each host is the sender of one stream and the receiver
of the other.

## Decisions

### One command, two streams

- `hashsync ssh [OPTIONS] [USER@]HOST [--] COMMAND [ARGS...]` replaces
  `hashsync run`. There is no push mode and no push command.
- Every run has both streams, and one or both of them are deduplicated:
  the backward stream by default, the forward stream with `--dedup-stdin`,
  and both with `--dedup-stdin --dedup-stdout`.
- With both, both hosts use their index locks, and lock waits have no
  timeout. The lock rule under Handshake keeps the runs' own lock waits
  from deadlocking, also for two such runs in opposite directions between
  the same hosts. A command that waits for an index lock can still hang
  runs (see Handshake).

### Roles

What depends on client and server:
- The server runs the command, relays its stderr, and kills the command's
  process group at a fatal end.
- The client has the user's stdin and stdout, returns the command's
  status, prints the summary, handles SIGINT and SIGTERM, and kills the
  SSH children at a fatal end.
- The client sends REQUEST, and closes both session stdin pipes after END.
  The server sends END, with the command's status, once it needs nothing
  more from the client.
- Only the forward stream can be stopped, and only the server stops it.

Everything else (the list exchange and the list cache, QUERY and ANSWER,
LIT/REF/RLE, the pins and the index) depends only on sender and receiver.
The list cache works unchanged whichever host is which.

Keep both SSH sessions: queries always travel the same way as the bulk
data, so they need their own connection. Keep `serve --token T` and
`attach --token T` as the only remote commands, with the rendezvous as it
is. REQUEST says which roles the server plays.

### Streams as building blocks

Compose streams, not session channels: a stream always uses the same
directions of both sessions. Both streams are built from the same blocks,
and deduplication is one more block, which a deduplicated stream has and
a plain stream doesn't.

The sending end:
- The **source reader** copies the source into the stream's spool, with
  the source's idle points in eager delivery.
- The **encoder** turns the bytes of the spool into stream frames: the
  scanner does it in a deduplicated stream, and a plain stream makes LIT
  frames of the bytes as they come.
- The **data writer** compresses the stream frames into the stream's zstd
  frame, and sends it in CHUNKs, then STREAM_END.

The receiving end:
- The **data reader** reads the session frames, and appends the payload of
  each CHUNK to the stream's spool.
- The **decoder** decompresses the spool, reads the stream frames, and
  writes their bytes to the destination.

The **dedup block** is the hash table, the scanner with its lookahead, and
the queries, on the sender; the index, the lookup workers, and the pins
that REF reads, on the receiver; and QUERY and ANSWER on the control
session.

- The client runs the forward sending end and the backward receiving end;
  the server runs the other two.
- The session readers only dispatch. CHUNK and STREAM_END go to the
  incoming stream's spool, QUERY to the lookup workers, ANSWER to the
  scanner, and STOP, on the client, to the forward stream's stop.
- One control writer per side takes queries, answers and STOP from one
  queue.

### Plain streams

A plain stream has no dedup block: no lookahead buffer, no hash table, no
index and no pins. Its stream frames are only LIT, each with the bytes of
one take from the spool, and its decoder takes a REF as a protocol error.
It is compressed like a deduplicated stream, unless compression is
disabled.

- Runs of one byte are literals in a plain stream. zstd stores them as
  RLE blocks of about 4 bytes per 128 KiB: 1 GiB of zeros took 33,230
  bytes at level 9. With compression disabled, they travel as they are.
- The forward stream's sending end now spools raw stdin instead of
  compressed input.
- Eager delivery uses the data writer's rule of today: at each idle point
  of its source, a plain stream's encoder queues a flush, and the data
  writer makes it once it has nothing left to write. This replaces the
  input's rule (`Sent` and its eventfd). A deduplicated stream keeps the
  scanner's rules, with the tail that waits for an idle data writer.

### Handshake

1. The protocol version and the features, as today.
2. The client sends REQUEST: the command, which streams are deduplicated,
   and every run setting (see Settings). Each side applies the settings
   that concern its roles.
3. For each deduplicated stream, its receiver sends HELLO: block size,
   weak and strong hash IDs, target, account ID and index ID. The sender
   answers with its HELLO (account ID and advertised lists). The list
   exchange and CACHE_RESULT follow as today. With both streams
   deduplicated, the forward stream's exchange comes first and the
   backward stream's after it, so their frames need no stream ID. A plain
   stream has no list exchange.
4. Then the server starts the command, and both streams start, as the
   command and the input start today.

Each receiver's index:
- In a pull, the client updates its index (or opens it read-only with
  `--no-update`), exports the list and opens its tmp_data directory before
  it starts SSH, as `run` does today. A failed update then starts no
  remote process, and no session waits idle during the update.
- In a push, the server can do this only once the sessions are up: it
  reads its configuration file and updates its index while the client
  waits for HELLO. Its session watcher (see Stream ends and stops) must
  already run then, so that a client that disappears ends a long update.
  The update writes only to stderr, which SSH forwards; `-q` and
  `--no-human-readable` travel in REQUEST.
- After an update, the receiver closes the read/write database, opens it
  read-only, and turns its exclusive lock into a shared one. The lock
  changes in place, on the same open file description (`F_OFD_SETLKW`
  with `F_RDLCK`), so no modifying process can come between the update
  and the transfer. `finish_update` has already waited for RocksDB's
  flushes and compactions, so the close has nothing to wait for. From then
  on, read-only processes (`stats`, `--no-update` runs) can use the index
  while the run goes on, also from the command of a push. With
  `--no-update`, the receiver opens the index read-only from the start, as
  today. The one exception is the client of a run with both streams
  deduplicated, which releases its lock after its update (see the lock
  rule below).
- The receiver keeps its index open only until its lookups end, which is
  when its deduplicated stream has ended or stopped (see Stream ends and
  stops). Then it closes the index, also while the other stream still
  runs. So in a push, `update` and `prune` in the command work once the
  forward stream has ended: in `tar -xf - && hashsync update`, it has
  ended once tar has read all of it, and the update gets the lock then, or
  a moment later, unless another run holds that index too (below).
- A command that waits for an exclusive index lock waits for every run
  that holds a lock on that index, and a run keeps each lock until the
  stream that the index deduplicates ends: the client's until the
  command's stdout has ended, the server's until the client's stdin has
  ended or the stream has stopped. So such a command can still hang runs:
  - In a push, a command that waits for the lock before it has read all
    its stdin hangs its own run, once the spools are full.
  - Reading all of stdin first doesn't help when another run's client
    holds the lock. Take two runs with both streams deduplicated, in
    opposite directions between the same hosts, whose commands are both
    `cat >file; hashsync update`. Once cat has read its stdin, each update
    waits for the shared lock of the other run's client. That client keeps
    it until its backward stream ends, after its command has exited, and
    that command is the other update, so both runs hang. Two pulls in
    opposite directions whose commands run `hashsync update` hang the same
    way, as they already do today.
- With both streams deduplicated, both hosts update their index, in the
  order that the lock rule below gives.

Lock waits have no timeout, so no run may take part in a cycle of waits.
The rule: a run waits for an exclusive lock only while it holds no other
index lock, and it waits for no other lock while it holds an exclusive
one. Then the locks that runs take for themselves form no cycle. A shared
lock waits only for an exclusive holder, which waits for nothing, and
nobody waits for a run that asks for an exclusive lock, since it holds no
lock. The rule can't cover the commands: one that waits for an index lock
can still close a cycle through the streams (see above). Runs with one
deduplicated stream follow the rule as they are. With both streams
deduplicated:
- The client updates its index before SSH starts, as in a pull, and then
  closes it, which releases its lock. It opens the index again read-only,
  with a shared lock, only once the server's HELLO has come, which
  follows the server's update.
- Keeping the client's lock through the server's update instead would
  deadlock two such runs started in opposite directions between the same
  hosts: each would hold one index and wait for the other.
- Between the update and the reopen, another process can change the
  client's index. So the client exports its list only after the reopen,
  and checks that the index still matches its configuration, as a
  `--no-update` open does. If it doesn't, the client ends with `die`, as
  such a run does, and the server ends at the closure of the sessions,
  before the command starts. Only a process that rebuilt the index with
  other settings in that window causes this: a second configuration with
  the same `index_path` but another `block_size` or `strong_hash`, or a
  hashsync with another schema. The client can't update again then: it
  would wait for an exclusive lock while the run holds the server's.
- With `--no-update`, both hosts take only shared locks, and the client
  opens its index before SSH starts, as in a pull.

The cache argument of DESIGN.md §5.6 still holds. The sender lists its
cache directory only after the receiver's HELLO, which the receiver sends
only once it holds its index lock, and the receiver keeps that lock,
exclusive or shared, until after the list exchange. A run with another
target needs a modifying process first, which needs the exclusive lock,
so it still starts only after this run's listing. Change §5.6's "only
after this run's receiver has ended" to match.

### Wire format

The data session has two levels of frames, both `type u8, length u32,
payload`:
- **Session frames** are never compressed: REQUEST, HELLO,
  HASHLIST/HASHDELTA/HASHKEEP with their list bytes, HASHFULL,
  CACHE_RESULT, CHUNK, STREAM_END and END. These are everything a reader
  must see without decoding.
- **Stream frames** are LIT, REF and RLE. A stream's stream frames form
  one zstd frame, and the CHUNK payloads are consecutive pieces of it, so
  the frames compress together across chunks and runs of REF stay cheap.
  With compression disabled, the CHUNK payloads are the stream frames as
  they are.

Also decided:
- Each direction of the data session carries one stream, so CHUNK has no
  stream ID. Nor do QUERY and ANSWER, also with both streams
  deduplicated: in each direction of the control session, QUERY belongs
  to the stream that the sending host scans, and ANSWER to the one it
  receives.
- A CHUNK goes out when it holds 256 KiB, and at each flush of the stream:
  the idle points of eager delivery, and the end.
- STREAM_END ends a stream; its zstd frame must end exactly with the last
  CHUNK. An empty stream is an empty zstd frame and STREAM_END.
- CHUNK, STREAM_END and STOP replace INPUT, INPUT_END and INPUT_STOP.
  END_ACK and DONE are removed (see End of the run).
- The control session doesn't change: one zstd stream per direction with
  QUERY, ANSWER and STOP, flushed whenever the writer's queue runs empty,
  and ended by closing the session.
- The protocol version stays 1, without feature bits (pre-release,
  DESIGN.md §11).

### Stream ends and stops

A stream ends normally when its source ends. The encoder takes the rest
of the spool, and the data writer ends the zstd frame and sends the last
CHUNK and STREAM_END. At STREAM_END the data reader ends the receiving
spool; the decoder writes the rest of it, checks that the zstd frame ended
exactly there, and closes the destination. Both streams keep STREAM_END:
the command must see its stdin end while the run goes on, nothing else
marks the end when compression is disabled, and the data reader can end
the spool without decoding.

Only the forward stream can stop, and only the server decides it: when a
write to the command's stdin fails with `EPIPE`, when the command exits,
or when it failed to start. A stop is one event on each host, which
happens at most once and wakes every thread of that end of the stream:
- On the server, the decoder raises it at `EPIPE`, the exit watcher at the
  command's exit, and the main thread after a startup failure. The server
  then sends STOP on the control session, at most once.
- On the client, STOP raises it, or END when END comes first. A later
  STOP, or one that comes after the forward stream has ended, changes
  nothing. STOP can also come before the client has started its forward
  stream: the server starts the command right after the list exchange,
  the command can exit at once, and STOP travels on the other session.
  So the client's stop exists from before the sessions start, as the
  input's does today (`ReceiverInput::new`).

A stopped end gives up at once, without finishing the stream:
- Every wait in its threads also waits for the stop: the source reader's
  waits for stdin and for room in the spool, the encoder's waits for the
  spool, for room in the data writer's queue and, in the deduplicated
  stream, for answers, and the decoder's waits for the spool and for room
  in the command's stdin pipe.
- Its spool drops what it holds, and from then on discards what is
  appended. So the data reader takes CHUNK and STREAM_END the same way
  before and after the stop: a STREAM_END after the stop is normal, since
  the client can send it before STOP reaches it.
- The decoder writes the command's stdin without blocking, and checks the
  stop before each write. So neither a descendant that holds the command's
  stdin without reading it, nor an RLE frame of 14 bytes that stands for
  terabytes, keeps it writing.
- Nothing is decoded after the stop, and nothing checks the end of the
  zstd frame. The sending end sends no STREAM_END. Frames already queued
  for the data writer can still go out, and the server discards them.

The server keeps its session watcher. Its data reader waits whenever the
forward stream's receiving spool holds its limit, in a push and in a pull
alike, and while it waits it reads nothing, so it can't see the data
session close. As today, the watcher waits for the data session to hang
up, and ends the run with `die` if it does. It runs from REQUEST, so it
also covers the index update of a push, until the stop of the forward
stream: from then on, the spool discards what arrives, and the data
reader doesn't wait any more. The client needs no watcher, as the
receiver needs none today: it sees the loss of the server on the control
session.

The backward stream is never stopped: a broken stdout on the client ends
the run with `die`, as now.

A receiver's lookups end once its deduplicated stream has ended or
stopped: when its data reader takes STREAM_END, or at the stop. The
scanner emits a window's frame only after its answer, so the queries that
still arrive after STREAM_END are obsolete. The receiver enters Completing
(no more lookups or answers), joins its lookup workers, and closes its
index. The pins stay until the decoder is done. Completing belongs to the
receiver, not to the host: its control writer goes on with STOP, and with
the queries of the other stream when that one is deduplicated too. So
Completing no longer makes the closure of the control session expected,
as it does today: that closure stays fatal until END (see End of the
run).

### End of the run

The run ends with END alone; END_ACK and DONE are gone.

The server sends END, with the command's status, once it needs nothing
more from the client:
- the command has exited or failed to start;
- the backward stream has ended: its STREAM_END goes out before END. A
  command that failed to start has an empty backward stream;
- the forward stream has ended or stopped, and its decoder has ended;
- the stderr relay has finished.

That is the moment it sends DONE today. DESIGN.md §10 rejects "Joining the
stdin writer before END", because the stdin writer could stay blocked in a
write while a descendant of the command held that stdin without reading
it. That reason is gone: the decoder now ends at the stop, which the
command's exit raises, so such a descendant no longer delays the end of
the run at all. Remove that §10 entry, and the text in §6.6 and §6.9 that
says such a descendant delays DONE but not END.

The other two of §6.6's "Three waits can delay the end" stay, and
DESIGN.md keeps them:
- The source reader leaves a terminal blocking. When another program that
  reads the same terminal, such as a pager in the same pipeline, takes the
  bytes that `poll` reported, the read waits for the next bytes or EOF,
  and so does the client's join of the source reader in step 3 below, as
  `ssh` waits in its read.
- zstd can't cancel a job of its worker threads. The forward stream is now
  compressed in the client's data writer, so after a stop, that thread
  ends only once the workers have finished the jobs that zstd holds. Step
  2 below waits for it, and the server's end waits for step 2. With 12
  worker threads and a producer that never pauses, this took the stdin
  reader under 0.1 s at level 9, 2.4 s at level 15, and about a minute at
  level 19.

The client, at END:
1. Stops the forward stream if it is still running.
2. Closes the data session's stdin once its data writer has ended, then
   the control session's once its control writer has ended. It never waits
   for a stdout EOF, which an SSH ControlMaster client delays.
3. Finishes its own part, which can still take long: the decoder writes
   the rest of the backward stream from its spool to stdout and checks the
   end of the stream (see Stream ends and stops), stdout is flushed, and
   the client joins its threads. Both SSH processes must exit with 0. A
   failure in any of these ends the client with `die`, even though END has
   arrived.
4. Prints the summary and exits with the command's status.

The server, after END, waits for the EOF of both sessions, which the
client causes, and exits with 0 if nothing failed. It shuts down the
control socket only after the data session's EOF, so the client never
sees the control session close before END.

Before END, EOF or the closure of a session means that the other side
disappeared, which is fatal on both sides. The server then kills the
command's process group, also while a descendant still holds the
command's stderr.

END_ACK told the server that the client had validated the stream and
flushed stdout. Nothing on the server needs that: the client still makes
those checks in step 3, and its exit status reports their failures. The
one change in behaviour is that a client that fails after END no longer
makes the server kill the command's process group. By then, the only
processes left in that group are descendants that have closed the
command's stdout and stderr.

### Settings: configuration file and options

A configuration file describes only its own host, whatever roles the host
plays in a run: `roots`, `block_size`, `strong_hash`, `index_path`,
`threads`, `candidate_attempts`, `tmp_data_dir` and `hashlist_max_age`.
Each host reads its own file, the client and `serve` alike. The receiver
needs one, for its roots and its index. A sender uses only two keys of
it, which are about its host and not about the run: `tmp_data_dir`, where
its spools go, and `hashlist_max_age`, how long its list cache keeps
lists. Without a file, it uses their defaults. So a client that only
pushes needs no configuration file.

Everything about a run is an option of `hashsync ssh` on the client, sent
to the server in REQUEST. No configuration file holds run settings, so it
is never ambiguous which settings apply. The other keys go:
- `lookahead`, `negative_cache`, `probe_threads`, `compression_level`,
  `compression_long` and `compression_threads` become options.
- The four `*_spool_limit` keys become one `--spool-limit SIZE|unlimited`
  (default 2 GiB) for every spool.
- `sender_cache` becomes `--no-sender-cache`.
- `ssh` remains only as `--ssh`.
- `sender_command` becomes `--hashsync-path PATH`. As with
  `sender_command`, PATH is one executable name or path, which the
  bootstrap quotes as one word (DESIGN.md §6.1), not a shell fragment like
  rsync's `--rsync-path` (`sudo rsync`). A wrapper script there does what
  a fragment would, such as passing `--config` to the server or setting
  its environment.
- `[hosts.*]` goes. Per-host SSH options belong in `~/.ssh/config`.

A scanner's lookahead depends on the block size B of its stream's
receiver, which the sender learns from HELLO. The sender computes it from
`--lookahead`, and without the option it takes the larger of 256 MiB and
3 × B, as today. A `--lookahead` below a receiver's B is a user error,
which the client reports: for its own B before SSH starts, and for the
server's once the server's HELLO arrives. The client then ends with
`die`, as at any other fatal error before END. The server sees the
sessions close and ends; it hasn't started the command yet. No message to
the server is needed.

### Command line

`hashsync ssh` takes the arguments that rsync gives its remote shell,
`[-l USER] HOST COMMAND...` (`do_cmd` in rsync's `main.c`), so that
`rsync -e 'hashsync ssh'` works for pulls, and `rsync -e 'hashsync ssh
--dedup-stdin'` for pushes:
- Options can also follow HOST, as with ssh, which goes on parsing
  options after the destination (`goto again` in OpenSSH's `ssh.c`). The
  command starts at the first argument after HOST that is neither an
  option nor an option's value, or right after `--`, so a command that
  starts with `-` needs `--`. rsync puts no options after HOST.
- `-l USER` gives the remote user, as with ssh, and goes to ssh as it is.
- By default, the command runs as sshd runs it: the client joins COMMAND
  and ARGS with single spaces, as ssh does, and the server runs that
  string with the user's shell and `-c`. The shell is `$SHELL`, which sshd
  sets from the password database, or `/bin/sh` when it is unset. rsync
  escapes its arguments for that shell (`safe_arg` in `options.c`), so a
  path with spaces arrives intact.
- `-s` sends the arguments as they are, and the server runs them without
  a shell, as `run` does today. Then the command's arguments don't depend
  on the remote user's shell. Starting `serve` and `attach` still does:
  SSH runs that bootstrap through the login shell, with the quoting of
  DESIGN.md §6.1, as today.
- With rsync, use `-s` only together with rsync's own `-s`; the help text
  of `-s` says so. rsync's `-s` puts no path on the command line (it
  sends them over its connection, `send_protected_args`), so with it both
  modes work, and with `-s` on both sides no shell sees rsync's
  arguments. Without it, rsync escapes each remote path for the remote
  shell: `do_cmd` passes it through `safe_arg`, which puts a backslash
  before spaces and the other shell characters, and only a shell removes
  them. So `rsync -e 'hashsync ssh -s' src/ nas:'dst dir/'` gives the
  remote rsync the path `dst\ dir/`, and the push writes into a new
  directory of that name and reports success. An `--rsync-path` with
  shell syntax, such as `sudo rsync`, also needs the default mode.

The other options:
- As with ssh, the client sends its stdin to the command, in a plain
  stream, and stdout is deduplicated against the client's index. `-n`
  (`--no-stdin`) makes the command read EOF at once, as `ssh -n` does:
  the client reads `/dev/null` in place of its stdin, and nothing from
  its own stdin.
- Also as with ssh, a run in the background of an interactive shell stops
  at its first read of the terminal, so such a run needs `-n`. Under
  `nohup`, which replaces a terminal stdin with `/dev/null` opened for
  writing, the read of stdin fails and ends the run with `die`, where ssh
  reads EOF, so `-n` is needed there too.
- `--dedup-stdin` and `--dedup-stdout` choose the deduplicated streams:
  stdin against the server's index, stdout against the client's. Without
  either, stdout is deduplicated. With `--dedup-stdin` alone, stdout
  comes back plain.
- Both streams use eager delivery. `--batch-stdin` and `--batch-stdout`
  choose batch delivery for that stream, whichever streams are
  deduplicated. `-n` with `--dedup-stdin` or `--batch-stdin` is a usage
  error.
- `--no-update` applies to each receiver's index.
- `update`, `stats`, `prune`, `serve` and `attach` stay.

Left to the implementation: the names of the other options, the wording
of the summary for a push, and error messages.

## Rejected

- A push mode or a second command: the two directions mirror each other.
  The name `push` was rejected, and so was `copy`, which suggests source
  and destination paths.
- Run settings in configuration files: a key would apply to some runs and
  be ignored in others.
- Sending stdin to the command only with an option, as `run --stdin`
  does: rsync and other programs that use `hashsync ssh` as their remote
  shell talk both ways and expect ssh's default, so a pull through `rsync
  -e 'hashsync ssh'` would fail at once.
- In a run with both streams deduplicated, keeping the client's index
  locked through the server's update: two such runs in opposite
  directions would each hold one index and wait for the other.
- Taking the two exclusive locks in the order of the indexes' IDs, which
  avoids the deadlock without the client's gap. Both IDs would have to be
  known before either update, so a message before the server's update
  would carry the server's. But `index_id` is stored in the database, so
  reading it takes a lock; a rebuild renews it, so it can change between
  the read and the lock; an index has none before its first update; and
  copied index directories share it, so two runs between them couldn't
  agree on an order.
- A lock attempt that fails at once instead of waiting: contention would
  then fail runs.
- The deduplicated stream as one zstd stream of all its frames, as today:
  a stop would have to decode or skip through what is queued to find the
  next frame.
- END marking the end of the backward stream in place of its STREAM_END:
  the backward stream's receiving end would need a special case, and on a
  pull the client's lookups would run until END.
- Plain streams as the scanner with an empty table, with a fast path
  that finds runs by checking one byte in every B: the scanner's buffer
  grows to 2 × (lookahead + B), about 512 MiB with the defaults, for a
  stream without queries, and its only gain over zstd is cheaper runs.

## Implementation recommendations

- Rewrite the orchestration in `run.rs`, `serve.rs` and `input.rs` around
  stream ends, instead of adding mirrored copies for push. These stay as
  they are: the scanner, the hash table, the probe pool, the hash-list
  code, the cache exchange, segments, `Compressor`, `FrameReader` and
  `FrameWriter`, the rendezvous and `attach`, the SSH command line, and
  `die`. Four parts change a little, as the bullets below say: spools
  (idle points from a `SpoolWriter`), the pins (answers into the shared
  control queue), the lookup workers (they end at Completing), and the
  index (the read-only reopen).
- The decoder needs a new end contract. Today `Decompressor::read` panics
  at the end of the zstd frame ("the compressed stream ended early"),
  because an END inside the frame always comes first, and `finish` checks
  the rest of the frame after END. Now the end of the zstd frame is the
  normal end of the stream frames, and the decoder must tell three cases
  apart:
  - the normal end: the zstd frame ends, with its checksum, where a stream
    frame ends, and the spool ends right after it;
  - a spool that ends inside the zstd frame or inside a stream frame
    without a stop, and bytes after the end of the zstd frame: protocol
    errors;
  - the stop, after which nothing is checked.

  Without compression, only the stream-frame boundary is checked.
  `write_stdin` and its `Decoder` in `input.rs` already check these cases
  for the input. The control session keeps `Decompressor`: its frame
  never ends.
- The scanner needs no stop of its own, since frames leave it only through
  `next_frame()`, but its driver (`transfer()` in `serve.rs`) must also
  wait for the stop. In a push, the driver can wait for answers alone
  (lookahead full, a query at the front), and after the stop the server
  sends none. The code around the driver also assumes the normal end:
  `read_output` closes its spool, and `Spool::close` asserts that the
  spool ended and was not stopped; `DataItems::wait_next` expects END. A
  stopped spool is dropped, not closed.
- The forward stream's source reader reads the process's stdin. Base it on
  `read_stdin` in `input.rs`, which leaves a terminal blocking, makes any
  other stdin non-blocking with `die::add_flags` (restored at every end),
  polls a stop pipe, and finds idle points with a poll that doesn't wait.
  Have it write raw bytes and idle points into a `Spool::pipe`. A
  `SpoolWriter` can't mark idle points yet: `Shared::mark_idle` is private
  to `spool.rs`, for the spool's own thread. Don't use `Spool::start`
  there: its thread can't end before the source's EOF, so it couldn't be
  joined after a stop. Don't use `IdlePointReader` either: it never
  restores `O_NONBLOCK`, which is fine only for the command's private
  stdout pipe.
- The server's decoder writes the command's stdin with `O_NONBLOCK` set on
  hashsync's end of the pipe, which no other process shares. At `EAGAIN`
  it polls the pipe and the stop pipe, and it checks the stop before each
  `write`, which then takes at most the pipe's free space. Put this in the
  one function that writes the destination, so that every stream frame
  gets it: today `reconstruct` in `run.rs` writes an RLE frame in pieces
  of 256 KiB and a LIT or REF in one `write_all`, all without a stop
  check, and `Decoder::decode` in `input.rs` checks the stop only between
  writes of up to 128 KiB.
- Each receiving end has a data reader and a decoder (see Streams as
  building blocks). Today's spool thread over the raw data session goes
  away.
- On the server, `EPIPE` on the command's stdin and the exit watcher stop
  the forward stream for both kinds of stream; reuse `InputStop`.
- The session watcher (`watch_session` in `input.rs`, started by
  `SenderInput::new`) moves to the server's forward receiving end, and
  starts at REQUEST (see Stream ends and stops).
- The server's end phases (`Running`, `EndSent`, `Completing` and
  `DoneSent` in `serve.rs`) reduce to before and after END.
- The lookup workers end today at the control session's EOF (`while let
  Ok(query) = queue.recv()` in `lookup.rs`), when the control reader drops
  the queue, and that EOF comes after END. Make them end at Completing
  instead, so that the index can close then: a worker that waits for a
  query must wake up at Completing too.
- The pins own the answer queue today, and Completing ends the control
  writer by dropping it (`answers: Option<Sender<Answer>>` in `pins.rs`,
  which `Pins::complete` sets to `None`). With one control queue per
  host, their sender is one producer among others (STOP, and the queries
  of the other stream), so dropping it at Completing only stops the
  answers. The control writer ends when the last producer is gone.
- Today, Completing also makes the closure of the control session
  expected: `read_control` and `write_answers` in `run.rs` end quietly at
  EOF or a failed write once `pins.is_completing()`. Completing now comes
  at the end of one stream, while STOP or the other stream can still need
  the control session, so tie expected closure to END instead (see End of
  the run).
- The read-only reopen after an update keeps the lock file's descriptor,
  which `index::Shared` holds today next to the database. `lock_file` with
  `exclusive: false` on that descriptor turns the lock into a shared one
  without releasing it. The lookup workers' `Reader`s and the export of
  the list then use the read-only handle.
- Before removing the input path, measure the plain forward stream against
  it on an idle machine: throughput and CPU for plain stdin, in eager and
  batch delivery, and the bytes on the wire for stdin paced faster than a
  slow link carries it, such as 2 MB/s over 200 KB/s. The data writer's
  flush rule replaces `Sent`, which exists so that a slow link gets fewer
  flushes, and the slow link shows whether the new rule keeps that.
  Report the numbers with the step.
- Run the existing transfer tests in both directions where they apply.
  Add tests for:
  - a push, end to end, also from a client without a configuration file;
  - stdin by default, and `-n`, with which the command reads EOF at once
    and the client leaves its stdin unread;
  - both streams deduplicated, end to end, with updates on both hosts and
    with `--no-update`, and a stop of the forward stream while the
    backward one goes on;
  - both streams deduplicated, with different block sizes and strong
    hashes on the two hosts;
  - two runs with both streams deduplicated, started at once in opposite
    directions between two indexes, with `--hashsync-path` wrappers that
    hold each server back until both clients have finished their
    updates: both runs must end;
  - a rebuild of the client's index with another block size between the
    client's update and its reopen, with a `--hashsync-path` wrapper that
    holds the server back until the rebuild is done: the client ends with
    `die`, and the command doesn't start;
  - the lock downgrade: a `stats` that waits during a run's update gets in
    as soon as the update ends, and an `update` waits until the run's
    lookups end, then gets in while the run still writes its stdout;
  - a push whose command runs `hashsync update` on the server's index
    after it has read its stdin;
  - a `--lookahead` below the server's block size in a push;
  - a deduplicated forward stream stopped because the command exits early
    or closes its stdin;
  - a stop in the middle of one RLE frame, and while a descendant of the
    command holds its stdin without reading it;
  - the ends of a stream: a zstd frame cut short, bytes after it, and a
    stop inside it;
  - STOP that arrives before the client has started its forward stream,
    and STREAM_END that arrives after STOP;
  - the loss of the client during the server's index update, and while
    the server's data reader waits at the limit of the forward stream's
    spool;
  - EOF or a closed session before END, on either side, also while a
    descendant holds the command's stderr and after one receiver has
    entered Completing, and the normal end after END;
  - a client whose stdout is slow after END: it still writes the whole
    backward stream before it exits;
  - a command and a consumer that answer each other through both streams,
    as `rsync -e` does, in both directions;
  - `rsync -e 'hashsync ssh …'` to a `USER@HOST` path with spaces:
    without `-s`, with rsync's `-s` alone, and with `-s` on both sides;
  - a command without `--`, options after HOST, and arguments that the
    shell would split or expand, with and without `-s`;
  - compression disabled;
  - removed configuration keys being rejected.
- Update DESIGN.md with each step. Most of §§1–3 and 6–8 changes, plus
  parts of §4.4 (the receiver reopens the index read-only with a shared
  lock, and index access ends with the lookups), §5.6 (HELLO,
  `hashlist_max_age`, the lock argument), and §10 gets the rejected
  alternatives. Update PLAN.md's status at the end.

## Suggested order

Each step leaves a working build with passing tests.

1. Measure the current input path, as a baseline.
2. Settings and command line: `hashsync ssh`, the run options, REQUEST,
   and the smaller configuration file. Pulls only.
3. Stream ends and the new wire format, with both streams of a pull on
   them: the backward stream deduplicated, the forward stream plain.
   Compare the forward stream with the baseline, then remove the old input
   code.
4. Push: `--dedup-stdin`, with the server as the receiver.
5. Both streams deduplicated: `--dedup-stdin --dedup-stdout`, with the
   client's lock released between its update and the server's.
