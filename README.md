# hashsync

hashsync runs a command on another host over SSH, much like `ssh`, and
deduplicates the command's output, its input, or both against the files
that the receiving host already has: wherever the data contains a block of
one of those files, hashsync sends a reference to the block instead of its
contents. Everything else is compressed with zstd.

hashsync finds these blocks at any offset of the stream, so it doesn't
matter what format the stream is in, or which file the receiving host has
the data in. This makes hashsync useful with tar, rsync,
`nix-store --export` and similar tools whenever the receiving host already
has most of the data, even if not in the same place: renamed or moved
files, a rebuilt package, the backup of a similar machine, or an older
version of a dump or a disk image.

```sh
# Update the local copy of /srv/data from the host nas. Blocks that the
# local copy already has are sent as references to it.
hashsync ssh -n nas tar -C /srv/data -cf - . | tar -C /srv/data -xf -
```

hashsync isn't a backup tool with a storage format of its own: the commands
at both ends see exactly the bytes that they would see through `ssh`, and
hashsync only changes what goes over the network.

## How it works

The host that receives a deduplicated stream, the *receiver*, keeps an
*index* of the blocks (16 KiB by default) of the files in the directories
that its configuration file lists, the *roots*. It sends the other host,
the *sender*, its *hash list*: a short hash of each of its blocks. The
sender computes a rolling hash at every offset of the stream and looks it
up in the list. For each match, the receiver reads the block from its file
and checks it against a strong hash of the data in the stream, and if it's
the same, the sender sends a reference to the block instead of the data.
The receiver then puts the original stream back together.

Only the receiver needs a configuration file and an index:

| | Deduplicated stream | Receiver |
|---|---|---|
| Pull, the default | The command's standard output | The local host |
| Push, with `--dedup-stdin` | The standard input of `hashsync ssh`, which it sends to the command | The remote host |
| Both, with `--dedup-stdin --dedup-stdout` | Both | Each host, for the stream that it receives |

## Features

- **Used like ssh.** `hashsync ssh host command` passes its standard input
  to the command, copies the command's standard output and standard error
  back, and exits with the command's exit status. It runs your `ssh`, so
  your SSH configuration, keys and SSH agent apply as usual.
- **Pull, push, or both.** By default, the command's output is
  deduplicated against the files of the local host. With `--dedup-stdin`,
  the standard input of `hashsync ssh` is deduplicated against the files of
  the remote host instead, and with `--dedup-stdin --dedup-stdout`, both
  are.
- **Works with rsync.** `rsync -e 'hashsync ssh'` pulls with
  deduplication, and `rsync -e 'hashsync ssh --dedup-stdin'` pushes with
  it (deduplication is much more effective with
  [`extras/rsync-3.5.1.patch`](extras/rsync-3.5.1.patch) applied to rsync).
- **Always exact.** The receiver always checks block references with a
  strong hash function, so if the index is outdated or a file is changed
  concurrently, this only results in less deduplication: the output always
  matches the original stream byte for byte.
- **Up-to-date index.** `hashsync ssh` updates the receiver's index before
  the transfer, and again after the command, so that the next transfer can
  be deduplicated against the files that were just written, for example by
  `tar -x` or rsync. Updates don't read unchanged files again, and
  *immutable* roots, such as Nix store paths, are indexed only once.
- **Starts right away.** hashsync doesn't wait for the index update
  before it sends the stream, and finds more blocks to deduplicate as the
  hash list arrives. With `--wait`, it waits for the whole list instead,
  so that it sends the fewest bytes. Both hosts cache hash lists, so
  hashsync only sends what changed in the list since the last transfer.
  Each stream is buffered in temporary files on both hosts, so a slow
  reader doesn't hold up the command.
- **Compressed.** What isn't deduplicated is compressed with zstd: by
  default at level 9, with long-distance matching and one thread per core.
- **No daemon.** The server side is the same binary, which `hashsync ssh`
  starts over SSH. There's no service or network port to set up, and a
  statically linked build can be copied to hosts that don't have hashsync
  installed.

## Installation

hashsync runs on Linux 5.8 or later, and needs to be installed on both
hosts. On the server, `hashsync ssh` starts it through the remote shell,
which finds it in its `PATH`, unless `--hashsync-path` says where it is.

With Nix, from a checkout of this repository:

```sh
nix-build          # builds result/bin/hashsync, and runs the tests
nix-env -f . -i    # installs it, with its man page, into your profile
```

On NixOS, you can add `(pkgs.callPackage /path/to/hashsync { })` to
`environment.systemPackages` instead.

A statically linked binary runs on any Linux host of its architecture, so
you can copy it to a server without a hashsync package:

```sh
nix-build --arg pkgs '(import <nixpkgs> {}).pkgsStatic'
ssh nas mkdir -p bin && scp result/bin/hashsync nas:bin/
rsync -a -e 'hashsync ssh --hashsync-path bin/hashsync' nas:/srv/data/ /srv/data/
```

For a cross build, for example for 64-bit ARM, pass
`--arg pkgs 'import <nixpkgs> { crossSystem = "aarch64-linux"; }'` to
`nix-build`.

Without Nix, build hashsync with Cargo. This needs a recent stable Rust, C
and C++ compilers for the bundled RocksDB, LZ4 and zstd, and libclang for
RocksDB's bindings. RocksDB takes a while to build.

```sh
cargo build --release --locked
```

Then copy `target/release/hashsync` to a directory in your `PATH`, and
`docs/hashsync.1` to a directory where `man` finds it, such as
`/usr/local/share/man/man1`.

## Quick start

On the host that receives the data, which for a pull is the local host,
list the directories to index in `~/.config/hashsync/default.toml`:

```toml
[[roots]]
path = "/srv/data"
```

Then build the index, and look at what it holds. `hashsync ssh` would
build it too, but the first update of a large tree takes a while, and
until it's done, hashsync has little to deduplicate against.

```sh
hashsync update
hashsync stats
```

hashsync now deduplicates the pull at the top of this page against
`/srv/data`. There, `-n` gives the command an empty standard input, like
`ssh -n`, so that hashsync doesn't read your terminal. At the end,
`hashsync ssh` prints a summary of what each stream read, sent and
deduplicated, and of the total traffic; `-q` leaves out the summary and the
other notes.

If little was deduplicated, hashsync may have sent much of the stream
before the hash list arrived, which happens on a fast network or when the
receiver's update takes a while. With `--wait`, it waits for the whole
list first.

## Examples

### Copying directories with tar

Pull a directory from `nas` over an older local copy, deduplicated against
the local index:

```sh
hashsync ssh -n nas tar -C /srv/data -cf - . | tar -C /srv/data -xf -
```

Push it the other way, deduplicated against the index of `nas`, whose own
`~/.config/hashsync/default.toml` lists `/srv/data`:

```sh
tar -C /srv/data -cf - . | hashsync ssh --dedup-stdin nas tar -C /srv/data -xf -
```

Since tar never waits for a reply, it's safe to add `--batch-stdout` to
the pull, or `--batch-stdin` to the push. hashsync then sends the data in
full buffers rather than as soon as it's available, so that it can
deduplicate and compress it better. Don't compress the archive (`tar -z`,
`-J`, `--zstd`): hashsync can't deduplicate compressed data, and it
compresses the stream anyway. Also, unlike rsync, tar doesn't delete the
files that are gone from the source.

### Backups with rsync

For its delta transfer, rsync compares each file only with the file of
the same name on the receiving side, or with a few others when you give it
options such as `--fuzzy` or `--copy-dest`. With hashsync as its remote
shell, the transfer is also deduplicated against every file in the
receiving host's index, so renamed, moved and copied files, and files that
another backup already has, cost little.

**Patch rsync.** rsync sends the file data that the receiving side lacks
in pieces of at most 32 KiB, each preceded by its length. hashsync can't
match a block that one of these lengths splits, so with the default
16 KiB blocks, it can miss up to half of the blocks.
[`extras/rsync-3.5.1.patch`](extras/rsync-3.5.1.patch) raises the limit to
8 MiB. Only the rsync that sends the files needs it: the remote one in a
pull and the local one in a push. It works with an unpatched rsync on the
other side. So in a backup, use the patched rsync on the machine being
backed up.

Apply the patch with `patch -p1` in the source directory of rsync 3.5.1,
then build rsync as usual. With Nix, for example in
`environment.systemPackages`:

```nix
(pkgs.rsync.overrideAttrs (old: {
  patches = (old.patches or [ ]) ++ [ /path/to/hashsync/extras/rsync-3.5.1.patch ];
}))
```

**Choose the options.** Pass `--delete-delay` to rsync, so that it
deletes files only after the transfer, and the files about to be deleted,
such as the old names of renamed files, can still be used for
deduplication. Don't use rsync's `-z`, which compresses the data before
hashsync sees it, or hashsync's `--batch-stdin` and `--batch-stdout`, with
which rsync would wait forever.

**Schedule the backups.** To back up every night, add the rsync command
to the crontab of the host that runs it, with `hashsync` and `rsync` in
cron's `PATH`. The crontab lines below pass `-q` and `--wait` to hashsync:
`-q` keeps it quiet unless something goes wrong, and with `--wait`, it
holds back the data until the backup server has updated its index and
sent its hash list. The backup then takes longer, but hashsync sends the
fewest bytes. Run backups that use the same index one after another: if
they run at the same time, each `hashsync ssh` has to wait for the others
before it can update the index, and deduplicates less in the meantime
(unless `--wait` is used).

#### Pushing backups

The backup server, here `nas`, receives the backups, so it has the index.
On `nas`, list the backup directory in `~/.config/hashsync/default.toml`:

```toml
[[roots]]
path = "/backup"
```

Back up the local machine, here `laptop`, to the `nas` server, with rsync
running on `laptop`:

```console
laptop$ rsync -a --delete-delay -e 'hashsync ssh --dedup-stdin' /home/alice/ nas:/backup/laptop/
```

To back up `laptop` every night, add this line to its crontab:

```
0 3 * * *  rsync -a --delete-delay -e 'hashsync ssh -q --wait --dedup-stdin' /home/alice/ nas:/backup/laptop/
```

#### Pulling backups

The backup server, here `nas`, receives the backups, so it has the index.
On `nas`, list the backup directory in `~/.config/hashsync/default.toml`:

```toml
[[roots]]
path = "/backup"
```

Run rsync on the `nas` server to pull the data of `laptop` into the
backup:

```console
nas$ rsync -a --delete-delay -e 'hashsync ssh' laptop:/home/alice/ /backup/laptop/
```

To back up `laptop` every night, add this line to the crontab of `nas`:

```
0 3 * * *  rsync -a --delete-delay -e 'hashsync ssh -q --wait' laptop:/home/alice/ /backup/laptop/
```

In a pull, the machine being backed up learns a hash of every block in the
backup server's index (see [Security](#security)). If it shouldn't learn
anything about the other backups, give it a profile of its own, such as
`~/.config/hashsync/laptop.toml` with `/backup/laptop` as its only root,
and pull with `rsync -e 'hashsync ssh --profile laptop'`.

### Copying Nix closures

[`extras/hashsync-copy-closure.sh`](extras/hashsync-copy-closure.sh)
copies the closures of Nix store paths from one host to another, like
`nix-copy-closure`. It copies only the store paths that the destination
doesn't have, and of those, hashsync sends only the blocks that the
destination doesn't already have in its Nix store. A rebuilt package gets
a new store path, but much of its contents is often the same as in the old
one, which hashsync then sends as references.

Both hosts need Nix and hashsync, with `nix-store` and `hashsync` in the
`PATH` of SSH commands. On the destination, the Nix daemon must trust the
user (root, or a user in `trusted-users` in `nix.conf`), because the paths
arrive without signatures, as with `nix-copy-closure`. The script uses a
separate hashsync profile named `nix`, which on the destination must index
the store, for example with this `~/.config/hashsync/nix.toml`:

```toml
[[roots]]
path = "/nix/store/*"
immutable = true
exclude = [".links"]
may_vanish = true
```

Each store path is a root that never changes, so it's indexed only once.
`.links` only holds extra hard links to the files of store paths, so it's
left out, and with `may_vanish`, hashsync doesn't warn when the garbage
collector removes a store path during an update. Build the index once
with `hashsync update --profile nix`; after that, hashsync updates it
during each copy.

```sh
# Push the closure of ./result to the host server.
extras/hashsync-copy-closure.sh --to server ./result

# Pull the closure of a store path from the host builder.
extras/hashsync-copy-closure.sh --from builder /nix/store/...-myapp-1.0

# Copy it from builder to server: server pulls it, with your SSH agent.
extras/hashsync-copy-closure.sh --from builder --to server /nix/store/...-myapp-1.0
```

In the last form, `builder` must accept a key from your SSH agent, and
`server` must already know the host key of `builder`, since that login has
no terminal to ask on.

### Files and dumps

```sh
# Pull a new version of a disk image next to the old one, which the local
# index covers, then replace it. A redirection to vm.img itself would empty
# it before hashsync could use it.
hashsync ssh -n nas cat /srv/images/vm.img > vm.img.new && mv vm.img.new vm.img

# Push it, deduplicated against the old version on nas.
hashsync ssh --dedup-stdin nas 'cat > /srv/images/vm.img.new && mv /srv/images/vm.img.new /srv/images/vm.img' < vm.img

# Keep a database dump of every day, each deduplicated against the earlier
# ones, with /backup/dumps in the local index.
hashsync ssh -n db pg_dump mydb > /backup/dumps/mydb-$(date +%F).sql
```

## Quick reference

### Commands

| Command | Description |
|---|---|
| `hashsync ssh [options] [user@]host command [arg ...]` | Run `command` on `host`, deduplicating its output or, with `--dedup-stdin`, its input |
| `hashsync update` | Create the index, or bring it up to date |
| `hashsync stats` | Show statistics about the index |
| `hashsync prune` | Remove from the index the files and roots that are gone or no longer configured, without hashing anything, then compact it |
| `hashsync compact` | Compact the index |
| `hashsync bench` | Measure how fast this host hashes and inspects a data stream for blocks to deduplicate |

`--profile NAME` selects the profile `NAME`, with the configuration file
`~/.config/hashsync/NAME.toml` and an index of its own, instead of the
profile `default`. `--config PATH` selects a configuration file anywhere.
`-q` prints only warnings and errors.

### Options of hashsync ssh

| Option | Description |
|---|---|
| `--dedup-stdin` | Deduplicate the standard input of `hashsync ssh`, which it sends to the command, against the server's index, instead of the command's output against the client's |
| `--dedup-stdout` | With `--dedup-stdin`, deduplicate the command's output as well |
| `-n` | Give the command an empty standard input, like `ssh -n` |
| `-l user` | Log in as `user` |
| `-s` | Run the command and its arguments without the remote shell |
| `--batch-stdin`, `--batch-stdout` | Send the stream in full buffers, to deduplicate and compress it better; programs that wait for replies, such as rsync, then hang |
| `--wait` | Wait for the receiver's whole hash list before sending the stream |
| `--no-update` | Leave the indexes as they are, letting several `hashsync ssh` commands that use the same index run at the same time without waiting for each other to update it |
| `--compact` | Compact each index after the command |
| `--compression-level N` | zstd level from 1 to 19, or 0 to disable compression; 9 by default |
| `--server-profile NAME`, `--server-config PATH` | The configuration file on the server |
| `--hashsync-path PATH` | hashsync on the server, if it isn't in the `PATH` there |
| `--ssh COMMAND` | The SSH command, `ssh` by default |

The man page describes the others.

### Configuration

```toml
# ~/.config/hashsync/default.toml. Every key is optional; these are the
# ones you're most likely to need.

# Size of the indexed blocks. With smaller blocks, hashsync finds more
# matches, and with larger ones, the index and the hash list are smaller.
# Changing it rebuilds the index.
block_size = "16KiB"

[[roots]]
path = "/srv/data"             # a directory, a file, or a glob pattern
exclude = ["*.tmp", ".cache"]  # names, or absolute paths if they start with /
one_filesystem = true          # leave out other filesystems mounted inside

[[roots]]
path = "/srv/photos/*"         # each match is a root of its own
immutable = true               # never changes: index it once, don't scan it again
```

Data sets that share no content and are transferred separately, such as a
Nix store and a photo collection, are best kept in separate profiles:
their updates are faster, their hash lists are smaller, and `hashsync ssh`
commands that use one don't wait for those that use the other.

### Index maintenance

`hashsync ssh` keeps the receiver's index up to date, but you can also run
`hashsync update` yourself, for example from cron, so that `hashsync ssh`
has little left to update. Updates also remove from the index the files
that are gone, and `hashsync prune` removes them without hashing anything,
then compacts the index. To shrink the index after an update has removed a
lot, run `hashsync compact`, or pass `--compact` to `update` or `ssh`.

## Things to know

- Only complete blocks are indexed, so files smaller than a block
  (16 KiB by default) don't help.
- Compressed or encrypted data can't be deduplicated. Send data through
  hashsync uncompressed, and let it compress the stream.
- Commands that modify an index, such as `hashsync update`, wait until no
  other command uses it. `hashsync ssh` modifies the index when it updates
  it, so if you start one while another uses the same index, the new one
  sends its stream without deduplication until it can update the index.
  With `--wait`, it waits instead. With `--no-update`, any number of them
  can share an index.
- For the same reason, don't run a command that updates an index inside a
  `hashsync ssh` that uses the same index:
  `hashsync ssh --dedup-stdin nas 'hashsync update; tar -xf -'` never
  finishes, because the update waits for `hashsync ssh`, and
  `hashsync ssh` waits for tar to read all of its input.
- Each `hashsync ssh` opens two SSH sessions to the server, so use SSH keys
  and an SSH agent rather than typing a password twice. Sharing one SSH
  connection between sessions (`ControlMaster`) works, but slows down
  deduplicated streams; `--ssh 'ssh -S none'` turns it off.
- The streams are buffered in temporary files on both hosts, up to 2 GiB
  per stream and host by default (`--spool-limit`), in
  `~/.cache/hashsync/tmp-data` (the `tmp_data_dir` setting). Keep that
  directory on a local disk, not on tmpfs.

## Security

The sender of a deduplicated stream learns a hash of every block in the
receiver's index, and can test whether the receiver has a block with given
contents. In a push, the client learns nothing that it couldn't read
through its SSH login anyway. In a pull, though, the server learns about
the client's files, so only pull from servers that you trust, or with a
profile that indexes only what the server may know about. Even a trusted
server keeps the client's hash list in its cache after a pull, where
anyone who later gets into the server can read it; `--no-sender-cache`
asks the server not to keep it.

## Documentation

- The man page, [`docs/hashsync.1`](docs/hashsync.1), describes every
  command, option and configuration key: `man hashsync` once hashsync is
  installed, or `man -l docs/hashsync.1` in a checkout.
  [`docs/hashsync.adoc`](docs/hashsync.adoc) has the same contents, for
  reading on the web.
- [`docs/DESIGN.md`](docs/DESIGN.md) describes the design and the protocol
  in detail, with the alternatives that were considered.
- [`docs/PLAN.md`](docs/PLAN.md) is the implementation plan, with its
  status.

## Development

`./shell.sh` enters a development shell with everything needed to build
and test hashsync, and `./shell.sh cargo test` runs the tests in it.
`nix-build` builds the package, runs the tests, and checks the installed
result. When you change a command, an option or a configuration key,
change the man page with it: the tests check that it documents each of
them, and they regenerate `docs/hashsync.adoc` from it.

## License

hashsync is licensed under the GNU General Public License, version 3 or
any later version; see [`COPYING`](COPYING). Its binaries also contain
third-party code under other licenses, which
[`THIRD-PARTY-NOTICES`](THIRD-PARTY-NOTICES) lists.

[`extras/hashsync-copy-closure.sh`](extras/hashsync-copy-closure.sh) is
licensed under the Apache License 2.0 instead, so that you can adapt it
freely; see [`extras/LICENSE-Apache-2.0`](extras/LICENSE-Apache-2.0).
