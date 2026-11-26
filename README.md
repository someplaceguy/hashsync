# hashsync

**Drop-in SSH stream deduplication and compression against files the receiver already has.**

`hashsync` runs remote commands over SSH just like `ssh`, while transparently deduplicating the data flowing over the wire. Whenever a stream contains a block of data that already exists in an indexed file on the receiving host, `hashsync` sends a compact reference instead of the raw bytes—and compresses everything else with zstd.

Because block matching is done at every byte offset of the stream, `hashsync` is completely agnostic to container formats, file paths, and alignment. Pair it with `tar`, `rsync`, `nix-store --export`, `pg_dump`, or raw disk images whenever the destination already holds overlapping data: moved or renamed files, rebuilt packages, sibling machine backups, or older versions of archives and VM images.

`hashsync` is not a backup system and introduces no storage format of its own. Both ends see the exact same byte stream they would over plain `ssh`—only the traffic crossing the network changes.

### At a glance

Pulling `/srv/data/v2`—an updated version of `/srv/data/v1`—from `nas` over a 2 MiB/s link:[^1]

```console
# With hashsync: blocks already present anywhere in v1 travel as references.
$ rsync -a --copy-dest=../v1 -e 'hashsync ssh' nas:/srv/data/v2/ /srv/data/v2/
(...)
hashsync: stdin -- 1000.21 KiB --> 998.97 KiB wire, pipe closed, 0 B dropped, 1000.21 KiB written (compressed)
hashsync: stdout -- 2.34 GiB (458.11 MiB literal, 1.89 GiB -> 606.25 KiB refs, 0 B -> 0 B RLE, 0 false positives, 0 not found) + 1.09 MiB hashlist --> 414.96 MiB wire (deduplicated, compressed)
hashsync: total -- sent 3.14 MiB and received 416.47 MiB in 242.5s (1.73 MiB/s), data shrinkage factor: 5.72, saved 1.93 GiB of data transfer

# With plain rsync: only files sharing the same name in v1 are reused.
$ rsync -az -hh --info=stats1 --copy-dest=../v1 nas:/srv/data/v2/ /srv/data/v2/
sent 1000.17K bytes  received 2.12G bytes  1.89M bytes/sec
total size is 5.25G  speedup is 2.47
```

Over that 2 MiB/s link, plain `rsync` received **2.12 GB in 19 minutes**. With `hashsync`, it received **416 MiB in 4 minutes**—nearly five times faster.

---

## Key capabilities

- **Familiar SSH interface.** `hashsync ssh host command` forwards standard input, streams back standard output and standard error, and exits with the remote command's status. It invokes your existing `ssh` binary, so `~/.ssh/config`, keys, and your SSH agent work out of the box.
- **Pull, push, or bidirectional.** Out of the box, `hashsync ssh` deduplicates the remote command's stdout against the local index (pull). Pass `--dedup-stdin` to deduplicate your stdin against the remote index instead (push), or combine `--dedup-stdin --dedup-stdout` to deduplicate both directions.
- **First-class `rsync` support.** Drop `hashsync ssh` straight into `rsync -e` for pulls, or `hashsync ssh --dedup-stdin` for pushes. Applying [`extras/rsync-3.5.2.patch`](extras/rsync-3.5.2.patch) to the sending `rsync` makes deduplication dramatically more effective (see [Backups with rsync](#backups-with-rsync)).
- **Cryptographically exact reconstruction.** Every candidate block match is verified on the receiver with a cryptographic hash (BLAKE3 by default) before being emitted. Stale indexes, hash collisions, or files modified mid-transfer can only lower the dedup ratio—never corrupt the stream.
- **Self-maintaining indexes.** `hashsync ssh` refreshes the receiver's index when a transfer starts and again right after the command exits, capturing newly written files (from `tar -x`, `rsync`, etc.) while they are still warm in the page cache. Unchanged files are skipped automatically, and *immutable* roots (like Nix store paths) are scanned only once.
- **Zero-wait streaming & delta hash lists.** Data starts flowing immediately rather than blocking on the index update; deduplication ramps up as the hash list arrives. Add `--wait` when bandwidth matters more than startup latency and you want the full hash list loaded first. Both hosts cache hash lists so subsequent runs only exchange the diff.
- **Multithreaded zstd compression.** Everything not replaced by a block reference is compressed with zstd (default level 9, long-distance matching enabled, one worker thread per CPU core).
- **Single binary, no daemon.** There are no background services or ports to expose—`hashsync ssh` launches the same `hashsync` binary on the remote host over SSH. Need to talk to a server without a package? Copy over a single statically linked binary.

## How it works

Each deduplicated stream has a **receiver** (the host receiving the stream and holding the local files) and a **sender** (the host transmitting the stream):

1. **Index blocks on the receiver.** The receiver maintains a RocksDB index of fixed-size, aligned blocks (16 KiB by default) across all regular files inside the directories (*roots*) listed in its configuration.
2. **Share a compact hash list.** At transfer time, the receiver sends the sender a *hash list* containing a short 64-bit rolling hash for every indexed block (or just a delta against a previously cached list).
3. **Scan the stream at every offset.** The sender slides a rolling hash byte-by-byte across the outgoing stream and probes the receiver's hash list.
4. **Verify before referencing.** Whenever the sender sees a hit, it asks the receiver to verify the block against a full cryptographic hash of the stream window. The receiver reads the candidate block from disk, verifies the digest, and replies yes or no.
5. **Reconstruct transparently.** On a confirmed match, the sender emits a tiny reference instead of the block's bytes. The receiver stitches the verified blocks and compressed literal chunks back into an exact copy of the original stream.

## Installation

`hashsync` requires **Linux 5.8 or newer** and must be available on both the local and remote hosts. On the remote side, `hashsync ssh` invokes `hashsync` via the remote shell's `PATH` unless you point to it explicitly with `--hashsync-path`.

### With Nix

From a checkout of this repository:

```sh
# Build ./result/bin/hashsync and run the full test suite
$ nix-build

# Install hashsync (along with its man page and helper script) into your profile
$ nix-env -f . -i
```

On NixOS, add `(pkgs.callPackage /path/to/hashsync { })` to `environment.systemPackages`.

#### Deploying a static binary to another Linux host

Nix can produce a fully static musl binary that runs on any Linux machine of the same architecture—handy for servers where you don't want to install Nix or build from source:

```sh
$ nix-build --arg pkgs '(import <nixpkgs> {}).pkgsStatic'
$ ssh nas mkdir -p bin && scp result/bin/hashsync nas:bin/
$ rsync -a -e 'hashsync ssh --hashsync-path bin/hashsync' nas:/srv/data/ /srv/data/
```

#### Cross-compilation

To build for another architecture—for example, 64-bit ARM—pass `crossSystem` to `pkgs`:

```sh
$ nix-build --arg pkgs 'import <nixpkgs> { crossSystem = "aarch64-linux"; }'
```

### With Cargo

If you aren't using Nix, build directly with Cargo. You will need a recent stable Rust toolchain, C and C++ compilers (for the bundled RocksDB, LZ4, and zstd libraries), and `libclang` (for generating RocksDB bindings). Compiling RocksDB takes a few minutes.

```sh
$ cargo build --release --locked
```

Install `target/release/hashsync` somewhere in your `PATH`, and copy `docs/hashsync.1` to your man path (for instance, `/usr/local/share/man/man1`).

## Quick start

1. **Declare what to index.** On the *receiving* host (your local machine for pulls), create `~/.config/hashsync/default.toml` listing the directories you want `hashsync` to deduplicate against:

   ```toml
   [[roots]]
   path = "/srv/data"
   ```

2. **Build the initial index.** While `hashsync ssh` updates the index automatically, running `hashsync update` once upfront avoids starting your first transfer against an empty index on a large tree (which could take a while to complete):

   ```sh
   $ hashsync update
   $ hashsync stats
   ```

3. **Run a transfer.** With `/srv/data` indexed, commands like the `rsync` pull at the top of this page automatically deduplicate against it. When a run finishes, `hashsync ssh` prints a breakdown of literal bytes, deduplicated references, wire traffic, and overall shrinkage (pass `-q` to silence notes and summaries).

> **Tip:** If a transfer deduplicates much less than expected, the stream may have outpaced the initial index update or hash-list transfer—common on fast networks or after many local file changes. Pass `--wait` to hold the stream until the complete hash list is ready.

## Usage recipes

### Syncing directories with `tar`

Pull a directory tree from `nas` into a local copy that is already indexed. Passing `-n` (like `ssh -n`) gives the remote command an empty stdin so `hashsync` won't read from your terminal:

```sh
$ hashsync ssh -n nas tar -C /srv/data -cf - . | tar -C /srv/data -xf -
```

Push the other direction, deduplicating against the index configured in `~/.config/hashsync/default.toml` on `nas`:

```sh
$ tar -C /srv/data -cf - . | hashsync ssh --dedup-stdin nas tar -C /srv/data -xf -
```

Because `tar` is unidirectional and never waits for responses, you can safely add `--batch-stdout` on pulls or `--batch-stdin` on pushes. Batch mode flushes only full buffers instead of every idle pause, improving both deduplication and compression ratios. Avoid compressing the archive yourself (`tar -z`, `-J`, `--zstd`): compressed streams can't be deduplicated, and `hashsync` already compresses the wire traffic. Note also that unlike `rsync`, `tar` does not delete files removed from the source.

### Backups with `rsync`

Standard `rsync` delta transfers only compare each file against a same-named file on the destination (or a narrow set of candidates with `--fuzzy` / `--copy-dest`). Using `hashsync` as `rsync`'s remote shell extends deduplication across *every* file in the receiver's index—so renamed files, moved directories, duplicated assets, and content already stored in another machine's backup transfer almost for free.

#### Patching `rsync` for much better deduplication

By default, `rsync` transmits missing literal file data in chunks of at most 32 KiB, inserting a length header before each chunk. Any 16 KiB block straddling one of those headers cannot match on the wire, which can cost up to half of all deduplicable blocks.

[`extras/rsync-3.5.2.patch`](extras/rsync-3.5.2.patch) raises that literal chunk cap to 8 MiB. It is backwards compatible with stock `rsync`, and **only the sending side needs the patch** (the remote host when pulling; the local host when pushing)—meaning for backups, install the patched `rsync` on the machine being backed up.

Apply it in an `rsync` 3.5.2 source tree with `patch -p1` and build normally, or override `pkgs.rsync` in Nix:

```nix
(pkgs.rsync.overrideAttrs (old: {
  patches = (old.patches or [ ]) ++ [ /path/to/hashsync/extras/rsync-3.5.2.patch ];
}))
```

#### Recommended flags & scheduling

- **Use `rsync --delete-delay`:** Defers deletions until after the transfer finishes, keeping old files (such as pre-rename paths) on disk so `hashsync` can still reference their blocks.
- **Skip `rsync -z`:** Pre-compressing the stream defeats deduplication; `hashsync` handles compression itself.
- **Do not use `hashsync --batch-stdin` or `hashsync --batch-stdout`:** `rsync` is interactive and will deadlock if either direction buffers indefinitely.
- **In cron jobs, use `-q --wait`:** `-q` keeps cron mail quiet unless errors occur, and `--wait` guarantees minimum network data usage by waiting for the complete hash list before sending data.
- **Stagger backups sharing an index:** Concurrent `hashsync ssh` runs targeting the same index contend for its exclusive update lock, reducing deduplication while waiting (or pausing under `--wait`). Run them sequentially, or separate them into distinct profiles.

#### Pushing backups (`laptop` → `nas`)

Since `nas` receives the backup stream, `nas` owns the index. Configure `~/.config/hashsync/default.toml` on `nas`:

```toml
[[roots]]
path = "/backup"
```

Push from `laptop`:

```console
laptop$ rsync -a --delete-delay -e 'hashsync ssh --dedup-stdin' /home/alice/ nas:/backup/laptop/
```

Nightly crontab entry on `laptop` (ensure `hashsync` and `rsync` are on cron's `PATH`):

```cron
0 3 * * *  rsync -a --delete-delay -e 'hashsync ssh -q --wait --dedup-stdin' /home/alice/ nas:/backup/laptop/
```

#### Pulling backups (`nas` ← `laptop`)

Again, `nas` receives the stream and maintains `/backup` in `~/.config/hashsync/default.toml`:

```toml
[[roots]]
path = "/backup"
```

Pull from `laptop` by running `rsync` on `nas`:

```console
nas$ rsync -a --delete-delay -e 'hashsync ssh' laptop:/home/alice/ /backup/laptop/
```

Nightly crontab entry on `nas`:

```cron
0 3 * * *  rsync -a --delete-delay -e 'hashsync ssh -q --wait' laptop:/home/alice/ /backup/laptop/
```

> **Privacy note for pulled backups:** When pulling, the remote host receives hashes of every block in the receiver's index (see [Security](#security)). If `laptop` shouldn't learn anything about other machines backed up under `/backup`, give it an isolated profile—e.g. `~/.config/hashsync/laptop.toml` indexing only `/backup/laptop`—and pull with `rsync -e 'hashsync ssh --profile laptop'`.

### Copying Nix closures

[`extras/hashsync-copy-closure.sh`](extras/hashsync-copy-closure.sh) (installed on `PATH` as `hashsync-copy-closure` by the Nix package) works like `nix-copy-closure`, transferring only store paths missing on the destination—and deduplicating their contents against everything already in the destination's `/nix/store`. Rebuilt derivations get fresh store paths, yet often share large portions of their binaries and assets with previous builds, which `hashsync` sends as references.

**Prerequisites:**

- Both hosts need `nix-store` and `hashsync` in the default SSH `PATH`.
- Just like `nix-copy-closure`, the destination Nix daemon must trust the SSH user (`root` or listed in `trusted-users` in `nix.conf`), since closures arrive unsigned.
- The script uses a dedicated profile called `nix`. On the **destination**, create `~/.config/hashsync/nix.toml`:

  ```toml
  [[roots]]
  path = "/nix/store/*"
  immutable = true
  exclude = [".links"]
  may_vanish = true
  ```

  Each store path becomes its own immutable root, indexed once and never rescanned. `.links` is excluded because it only duplicates hard links to existing store files, and `may_vanish = true` silences warnings if Nix garbage collection deletes a store path mid-update.

Build the index once with `hashsync update --profile nix`; transfers will keep it current from then on.

```sh
# Push the closure of ./result to server
$ extras/hashsync-copy-closure.sh --to server ./result

# Pull a store path closure from builder
$ extras/hashsync-copy-closure.sh --from builder /nix/store/...-myapp-1.0

# Copy directly from builder to server (server pulls using your forwarded SSH agent)
$ extras/hashsync-copy-closure.sh --from builder --to server /nix/store/...-myapp-1.0
```

For host-to-host copies (`--from` and `--to` together), `builder` must accept a key loaded in your SSH agent and `server` must already have `builder`'s host key in its `known_hosts` (there is no interactive TTY to confirm new host keys).

### Disk images and database dumps

```sh
# Pull an updated VM image alongside the existing indexed copy, then swap it in.
# Redirecting directly to vm.img would truncate it before hashsync can read its blocks.
$ hashsync ssh -n nas cat /srv/images/vm.img > vm.img.new && mv vm.img.new vm.img

# Push a VM image deduplicated against the previous copy on nas.
$ hashsync ssh --dedup-stdin nas 'cat > /srv/images/vm.img.new && mv /srv/images/vm.img.new /srv/images/vm.img' < vm.img

# Stream daily PostgreSQL dumps deduplicated against earlier dumps under /backup/dumps.
$ hashsync ssh -n db pg_dump mydb > /backup/dumps/mydb-$(date +%F).sql
```

## Quick reference

### Commands

| Command | Purpose |
|---|---|
| `hashsync ssh [options] [user@]host command [arg ...]` | Execute `command` over SSH, deduplicating stdout (default), stdin (`--dedup-stdin`), or both |
| `hashsync update` | Create the index or bring it up to date |
| `hashsync stats` | Display statistics for the current index |
| `hashsync prune` | Drop missing or unconfigured roots and files without hashing, then compact |
| `hashsync compact` | Rewrite the RocksDB index files to reclaim deleted space |
| `hashsync bench` | Benchmark local hashing and stream-scanning throughput |

**Global flags** (accepted before or after the subcommand):

- `--profile NAME` — Use `~/.config/hashsync/NAME.toml` and its own dedicated index instead of `default`.
- `--config PATH` — Use a configuration file at an explicit path.
- `-q` / `--quiet` — Suppress progress notes and summaries; show only warnings and errors.

### Common `hashsync ssh` options

| Option | Effect |
|---|---|
| `--dedup-stdin` | Deduplicate stdin against the remote index instead of stdout against the local index |
| `--dedup-stdout` | Combine with `--dedup-stdin` to deduplicate both streams |
| `-n` | Feed an empty stdin to the remote command (equivalent to `ssh -n`) |
| `-l user` | Specify the remote SSH login user |
| `-s` | Execute the remote command directly (`argv`) without invoking a remote shell |
| `--batch-stdin`, `--batch-stdout` | Flush streams only on full buffers rather than idle pauses, improving dedup and compression (incompatible with interactive tools like `rsync`) |
| `--wait` | Hold stream transmission until the receiver's complete hash list has arrived |
| `--no-update` | Skip pre- and post-transfer index updates; allows multiple concurrent runs to share a read-only index |
| `--compact` | Compact updated indexes after the command completes |
| `--compression-level N` | Set zstd compression level (`1`–`19`, or `0` to disable; default `9`) |
| `--server-profile NAME`, `--server-config PATH` | Select a non-default profile or config file on the remote host |
| `--hashsync-path PATH` | Path to the remote `hashsync` executable when not in remote `PATH` |
| `--ssh COMMAND` | Override the SSH command (default `ssh`) |

See the [full man page](docs/hashsync.adoc) for every command, flag, and configuration key.

### Configuration

All settings in `~/.config/hashsync/default.toml` are optional. Here is a typical starting configuration:

```toml
# Indexed block size (default: "16KiB"). Smaller blocks match more data;
# larger blocks shrink the index and hash list. Changing this rebuilds the index.
block_size = "16KiB"

[[roots]]
path = "/srv/data"             # directory, single file, or glob pattern
exclude = ["*.tmp", ".cache"]  # basename patterns, or absolute paths if starting with /
one_filesystem = true          # do not cross mount boundaries

[[roots]]
path = "/srv/photos/*"         # each glob match becomes an independent root
immutable = true               # content never changes: index once, skip future rescans
```

**Separate unrelated datasets into separate profiles.** If two trees share no content and are synced independently (such as `/nix/store` and a photo archive), giving each its own profile speeds up index updates, keeps hash lists compact, and eliminates lock contention between transfers.

### Maintaining the index

`hashsync ssh` updates the receiver's index automatically, so no index maintenance is needed.

That said, if you want you can schedule `hashsync update` periodically (e.g., via cron) to make transfers start up faster, since the less new data there is to index, the quicker the index updates when a transfer starts. Routine updates already drop deleted files. However, you can also use `hashsync prune` to clean up vanished paths without hashing anything new, and `hashsync compact` (or `--compact` with `update` / `ssh`) to reclaim disk space after heavy churn.

## Good to know

- **Small files are ignored.** Only complete blocks are indexed, so files smaller than `block_size` (16 KiB by default) cannot contribute matches.
- **Send uncompressed, unencrypted data.** Pre-compressed or encrypted payloads destroy block similarity. Pass raw streams into `hashsync` and let its built-in zstd compression handle the wire encoding.
- **Index locking and concurrency.** Modifying operations (`update`, `prune`, `compact`, and the pre-/post-transfer updates of `hashsync ssh`) require an exclusive lock on the index. If a second `hashsync ssh` starts against the same index while it is busy, the new run streams without deduplication (though still compressed) until it acquires the lock—or pauses if `--wait` is set. Pass `--no-update` whenever you want multiple concurrent transfers sharing one index read-only.
- **Avoid recursive index deadlocks.** Never invoke an index-modifying `hashsync` command inside the remote command of a `hashsync ssh` session that uses the same index. For example, `hashsync ssh --dedup-stdin nas 'hashsync update; tar -xf -'` will deadlock: `hashsync update` waits for the session to finish using the index, while the session waits for `tar` to consume stdin.
- **Two SSH sessions per transfer.** Every `hashsync ssh` run opens both a data session and a control session. Configure SSH keys and an agent so you aren't prompted for a password twice. OpenSSH connection multiplexing (`ControlMaster`) works, but slows down deduplicated streams because verification round trips queue behind bulk stream data; disable it per call with `--ssh 'ssh -S none'`.
- **Temporary spooling space.** Streams spool through unlinked temporary files in `~/.cache/hashsync/tmp-data` (configurable via `tmp_data_dir`), capped at 2 GiB per stream per host by default (`--spool-limit`). Keep `tmp_data_dir` on a local disk rather than `tmpfs` so streaming buffering doesn't consume RAM.

## Security

To match blocks on the wire, the sender receives a hash of every block in the receiver's index and can test whether the receiver holds any chosen block of data:

- **Pushing (`--dedup-stdin`):** The local client only learns about files on a server it already has SSH access to, so there is no additional exposure.
- **Pulling (default):** The *remote server* learns about blocks present on your local machine. Only pull from servers you trust—or point `--profile` at a narrowly scoped index containing only data that the server is permitted to know about.
- **Cached hash lists:** By default, the sender caches received hash lists on disk to speed up future transfers. If a trusted server is later compromised, those cached lists remain readable there. Pass `--no-sender-cache` to instruct the server not to store your hash list (note that this only applies to honest servers; an untrusted server still receives the hashes during the transfer and can ignore the flag).

## Documentation

- **Man page:** Run `man hashsync` after installation or `man -l docs/hashsync.1` from a checkout for a complete reference to every command, option, and config key. A web-readable version is available at [`docs/hashsync.adoc`](docs/hashsync.adoc).
- **Architecture & protocol:** [`docs/DESIGN.md`](docs/DESIGN.md) documents the internal design, wire protocol, locking rules, and design trade-offs in depth.
- **Implementation status:** [`docs/PLAN.md`](docs/PLAN.md) tracks the implementation roadmap and current status.

## Development

Source repository: <https://github.com/someplaceguy/hashsync>

Enter a ready-to-use development shell or run tests directly through `./shell.sh`:

```sh
$ ./shell.sh                # open an interactive dev shell
$ ./shell.sh cargo test     # run the test suite
$ nix-build                 # full package build, tests, and install checks
```

Whenever you add, rename, or remove a command, option, or configuration key, update `docs/hashsync.1` alongside it. The test suite verifies complete 1-to-1 coverage in the man page and automatically regenerates `docs/hashsync.adoc` when it changes.

## License

Copyright 2026 Ricardo Correia.

`hashsync` is licensed under the GNU General Public License, version 3 or later; see [`COPYING`](COPYING). Compiled binaries bundle third-party libraries whose licenses are collected in [`THIRD-PARTY-NOTICES`](THIRD-PARTY-NOTICES).

[`extras/hashsync-copy-closure.sh`](extras/hashsync-copy-closure.sh) is separately licensed under the Apache License 2.0 so you can modify and embed it freely; see [`extras/LICENSE-Apache-2.0`](extras/LICENSE-Apache-2.0).

[^1]: Measured using the [GPT-2 repository](https://huggingface.co/openai-community/gpt2) on Hugging Face from 2021-05-19 (`v1`) and 2024-02-19 (`v2`), where `v2` adds a safetensors file and three ONNX files sharing weights with earlier checkpoints. Both endpoints ran on a single VM (24 vCPUs on an AMD Ryzen AI 9 HX 370, 16 GiB RAM) using `hashsync` 1.0.0, OpenSSH, and `rsync` 3.5.1 patched with [`extras/rsync-3.5.1.patch`](https://github.com/someplaceguy/hashsync/blob/v1.0.0/extras/rsync-3.5.1.patch), throttled to 2 MiB/s in each direction.
