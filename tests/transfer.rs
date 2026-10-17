//! End-to-end `hashsync ssh` transfers (PLAN.md steps 3 to 6) over a fake
//! `ssh` that executes the remote command string with a local `/bin/sh`,
//! under a separate HOME for the server side. This exercises the real bootstrap
//! quoting, the rendezvous socket, both sessions, the cache publication
//! rules on both sides, the reuse of cached lists, block references with
//! their pins, runs, and the compression stream. The tests of what
//! deduplication does run in both directions (`Direction`): in a pull,
//! the command's stdout is deduplicated against the client's index, and
//! in a push, the client's stdin against the server's.
//!
//! The tests wait for events, and no wait has a deadline whose end fails a
//! test: a regression hangs the test, so that a debugger can examine it.
//! A few tests must see that a process does not finish, which no event
//! shows, so they wait a fixed time. On a slow machine, such a wait can
//! miss a fault, but it cannot fail a correct build.

mod common;

use std::cell::RefCell;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use common::{
    ANSWER, BLOCK, CACHE_RESULT, CHUNK, END, Env, FEATURES, HASHDELTA, HASHLIST, HELLO, KillOnDrop,
    LIT, QUERY, REQUEST, STOP, STREAM_END, frame, ofd_lock, proc_state, random, tiled,
    toml_literal, try_recv_frame, version_and_features, wait_for_lock_wait,
};
use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fs::{CWD, Mode, OFlags, inotify, mkfifoat};
use rustix::io::{Errno, read, retry_on_intr};
use rustix::process::{
    Pid, PidfdFlags, Signal, WaitId, WaitIdOptions, kill_process, pidfd_open, waitid,
};
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};

const HOST: &str = "testhost";

/// Amounts in whole bytes, so that the tests can compare the summary's
/// counts exactly.
const EXACT: &str = "--no-human-readable";

/// Batch delivery of the output, for the tests that count references and
/// runs exactly. In eager delivery, a block or a run whose window spans an
/// idle point of the command's stdout becomes literal, and the idle points
/// depend on timing (DESIGN.md §6.3).
const BATCH: &str = "--batch-stdout";

/// The bytes of noise that fill the data session in the tests of a busy
/// data writer: the consumer reads none of the client's stdout, the
/// client's spool holds at most 64 KiB, and compression is disabled, as on
/// a slow link. The noise must be more than all the buffers after the
/// server's data writer, so that the data writer stays busy: its CHUNK of
/// 256 KiB, the client's spool, which holds its limit and one CHUNK of
/// 256 KiB, the client's read buffer of 256 KiB, and two pipes.
/// That is 0.9 MiB, or 2.8 MiB where a pipe holds 1 MiB (pages of 64 KiB).
/// It must also be less than those buffers and the data queue of 4 MiB
/// together, so that every frame of the noise leaves the scanner, and only
/// the busy data writer keeps a tail that follows the noise.
const SESSION_NOISE: usize = 4 << 20;

struct Transfer {
    env: Env,
    /// The options of every run of this transfer, such as `--spool-limit`:
    /// the settings of a run are options of `hashsync ssh`, not keys of a
    /// configuration file (DESIGN.md §7).
    options: Vec<String>,
    /// `--hashsync-path`: this build's binary, unless a test gives another.
    hashsync_path: RefCell<OsString>,
}

impl Transfer {
    /// An environment with a fake `ssh`, split client and server homes, an
    /// indexable `tree` root, and `options` for every run. The server has
    /// no configuration file: the sender of a pull needs none.
    fn new(options: &[&str]) -> Self {
        let env = Env::new();
        for dir in [
            "client/config",
            "client/cache",
            "server/home",
            "server/runtime",
        ] {
            env.mkdir(dir);
        }
        // As sshd does, the fake ssh sets `$SHELL` for the remote command,
        // here to `/bin/sh` unless a test names another shell, and tells
        // the remote command the user that `-l` gave, if any.
        let script = format!(
            r#"#!/bin/sh
set -eu
login=
while [ $# -gt 0 ]; do
    case "$1" in
        -o) shift 2 ;;
        -l) login="$2"; shift 2 ;;
        -*) shift ;;
        *) break ;;
    esac
done
host="$1"; shift
if [ "$host" != "{HOST}" ]; then
    echo "fake ssh: unexpected host: $host" >&2
    exit 42
fi
export HOME="$HASHSYNC_TEST_SERVER_HOME"
export XDG_CONFIG_HOME="$HOME/.config"
export XDG_CACHE_HOME="$HOME/.cache"
export XDG_STATE_HOME="$HOME/.local/state"
export XDG_RUNTIME_DIR="$HASHSYNC_TEST_SERVER_RUNTIME"
export HASHSYNC_TEST_LOGIN="$login"
export SHELL="${{HASHSYNC_TEST_SHELL:-/bin/sh}}"
exec /bin/sh -c "$1"
"#
        );
        let ssh = env.path("ssh");
        std::fs::write(&ssh, script).unwrap();
        std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
        let ssh = ssh.to_str().expect("the test path must be plain text");
        assert!(
            !ssh.contains(['\'', '"', '\\', ' ']),
            "quote the fake ssh path for --ssh"
        );
        let transfer = Transfer {
            env,
            options: options.iter().map(|option| option.to_string()).collect(),
            hashsync_path: RefCell::new(env!("CARGO_BIN_EXE_hashsync").into()),
        };
        transfer.configure("");
        transfer
    }

    /// Write the client's configuration: the `tree` root and `extra`.
    fn configure(&self, extra: &str) {
        self.env
            .configure(&format!("{extra}{}", self.env.root("tree", "")));
    }

    /// Give the next runs `path` as `--hashsync-path`.
    fn set_hashsync_path(&self, path: &OsStr) {
        *self.hashsync_path.borrow_mut() = path.to_owned();
    }

    /// `hashsync ssh [options] [flags] HOST -- command...` with the
    /// environments of both sides prepared. Its stdin is `/dev/null`,
    /// unless the test gives it another: the client sends its stdin to the
    /// command (DESIGN.md §6.6), and must not read the stdin of the test
    /// process.
    fn invocation(&self, flags: &[&str], command: &[&OsStr]) -> Command {
        let mut invocation = self.bare_invocation(flags);
        invocation.arg(HOST).arg("--").args(command);
        invocation
    }

    /// `hashsync ssh [options] [flags]`, to which the test adds HOST and
    /// the command, with the environments of both sides prepared, and
    /// `/dev/null` as stdin.
    fn bare_invocation(&self, flags: &[&str]) -> Command {
        let mut invocation = self.env.command(&["ssh"]);
        self.add_options(&mut invocation, flags);
        invocation
    }

    /// `hashsync ssh --dedup-stdin [options] [flags] HOST -- command...`,
    /// a push, as `invocation` prepares it, but without `--config`: the
    /// client of a push needs no configuration file, and the default one
    /// does not exist (DESIGN.md §7). The server needs one
    /// (`configure_server`).
    fn push(&self, flags: &[&str], command: &[&OsStr]) -> Command {
        let mut invocation = Command::new(env!("CARGO_BIN_EXE_hashsync"));
        invocation.args(["ssh", "--dedup-stdin"]);
        self.add_options(&mut invocation, flags);
        invocation.arg(HOST).arg("--").args(command);
        invocation
    }

    /// `hashsync ssh --dedup-stdin --dedup-stdout [options] [flags] HOST --
    /// command...`, a run with both streams deduplicated, as `invocation`
    /// prepares it: the client deduplicates the backward stream against the
    /// index of its configuration file, and the server the forward stream
    /// against that of its own (`configure_server`).
    fn both(&self, flags: &[&str], command: &[&OsStr]) -> Command {
        let flags = [&["--dedup-stdin", "--dedup-stdout"][..], flags].concat();
        self.invocation(&flags, command)
    }

    /// `invocation` in a pull, and `push` in a push.
    fn invocation_in(&self, direction: Direction, flags: &[&str], command: &[&OsStr]) -> Command {
        match direction {
            Direction::Pull => self.invocation(flags, command),
            Direction::Push => self.push(flags, command),
        }
    }

    /// The options of every invocation of `hashsync ssh`: the fake ssh,
    /// `--hashsync-path`, the options of this transfer, and `flags`, with
    /// the environments of both sides, and `/dev/null` as stdin.
    fn add_options(&self, invocation: &mut Command, flags: &[&str]) {
        invocation
            .arg("--ssh")
            .arg(self.env.path("ssh"))
            .arg("--hashsync-path")
            .arg(&*self.hashsync_path.borrow())
            .args(&self.options)
            .args(flags)
            .stdin(Stdio::null())
            .envs(self.client_environment());
    }

    /// Write the server's default configuration, which the receiver of a
    /// push needs: blocks of `BLOCK` bytes, the index at `server-index`,
    /// the root `server-tree`, and `extra`.
    fn configure_server(&self, extra: &str) {
        self.env.mkdir("server-tree");
        let text = format!(
            "block_size = {BLOCK}\nindex_path = '{}'\n{extra}{}",
            toml_literal(self.server_index().as_os_str().as_bytes()),
            self.env.root("server-tree", "")
        );
        std::fs::create_dir_all(self.server_config().parent().unwrap()).unwrap();
        std::fs::write(self.server_config(), text).unwrap();
    }

    /// The server's default configuration file.
    fn server_config(&self) -> PathBuf {
        self.env.path("server/home/.config/hashsync/config.toml")
    }

    /// The index of the server of a push.
    fn server_index(&self) -> PathBuf {
        self.env.path("server-index")
    }

    /// Configure the receiver of `direction`, whose index deduplicates the
    /// stream, with its root and `extra`: the client in a pull, the server
    /// in a push.
    fn configure_receiver(&self, direction: Direction, extra: &str) {
        match direction {
            Direction::Pull => self.configure(extra),
            Direction::Push => self.configure_server(extra),
        }
    }

    /// The path of `name` in the root of the receiver of `direction`,
    /// relative to the environment.
    fn indexed(&self, direction: Direction, name: &str) -> String {
        match direction {
            Direction::Pull => format!("tree/{name}"),
            Direction::Push => format!("server-tree/{name}"),
        }
    }

    /// Update the index of the receiver of `direction`, with the options
    /// of `update`.
    fn update_receiver(&self, direction: Direction, options: &[&str]) {
        let config = match direction {
            Direction::Pull => self.env.config_path(),
            Direction::Push => self.server_config(),
        };
        let output = Command::new(env!("CARGO_BIN_EXE_hashsync"))
            .arg("--config")
            .arg(config)
            .arg("update")
            .args(options)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "update failed:\n{stderr}");
    }

    /// Start a run whose deduplicated stream, in `direction`, carries the
    /// stdout of `script`, a shell script: in a pull, `script` is the
    /// command, and in a push, its stdout is the client's stdin, which the
    /// command `cat` copies to its own stdout. Either way, the client's
    /// stdout is what `script` writes. The summary is in whole bytes
    /// (`EXACT`).
    fn spawn_stream(&self, direction: Direction, flags: &[&str], script: &str) -> Stream {
        let flags = [&[EXACT], flags].concat();
        let (mut invocation, producer) = match direction {
            Direction::Pull => (self.invocation(&flags, &refs(&args(&[script]))), None),
            Direction::Push => {
                let mut producer = Command::new("/bin/sh")
                    .arg("-c")
                    .arg(script)
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap();
                let stdout = producer.stdout.take().unwrap();
                let mut invocation = self.push(&flags, &refs(&args(&["cat"])));
                invocation.stdin(stdout);
                (invocation, Some(KillOnDrop(producer)))
            }
        };
        let client = invocation
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        // The client holds its own copy of the pipe from the producer.
        drop(invocation);
        Stream {
            client: KillOnDrop(client),
            producer,
        }
    }

    /// Run a stream as `spawn_stream` starts it, and return its output.
    fn run_stream(&self, direction: Direction, flags: &[&str], script: &str) -> Output {
        self.spawn_stream(direction, flags, script).output()
    }

    /// Run a push of `input`, with the summary in whole bytes (`EXACT`).
    fn run_push(&self, flags: &[&str], command: &[&OsStr], input: &[u8]) -> Output {
        let path = self.env.path("push-input");
        std::fs::write(&path, input).unwrap();
        self.push(&[&[EXACT], flags].concat(), command)
            .stdin(File::open(&path).unwrap())
            .output()
            .unwrap()
    }

    /// The environment of the client, and what its fake ssh passes on to
    /// the server. Every directory that hashsync finds through the
    /// environment is one of the test's own, on both sides, so that no run
    /// can touch the files of the user who runs the tests, such as the
    /// default index.
    fn client_environment(&self) -> [(&str, PathBuf); 6] {
        [
            ("HOME", self.env.path("client/home")),
            ("XDG_CONFIG_HOME", self.env.path("client/config")),
            ("XDG_CACHE_HOME", self.env.path("client/cache")),
            ("XDG_STATE_HOME", self.env.path("client/state")),
            ("HASHSYNC_TEST_SERVER_HOME", self.env.path("server/home")),
            (
                "HASHSYNC_TEST_SERVER_RUNTIME",
                self.env.path("server/runtime"),
            ),
        ]
    }

    /// Run with the summary in whole bytes (`EXACT`).
    fn run(&self, flags: &[&str], command: &[&OsStr]) -> Output {
        self.invocation(&[&[EXACT], flags].concat(), command)
            .output()
            .unwrap()
    }

    fn run_ok(&self, command: &[&OsStr]) -> Output {
        let output = self.run(&[], command);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "run failed:\n{stderr}");
        assert!(stderr.contains(DEDUPLICATED), "missing summary:\n{stderr}");
        output
    }

    /// The cache tree `tree` of `side`, "client" or "server".
    fn cache_tree(&self, side: &str, tree: &str) -> PathBuf {
        let root = match side {
            "client" => self.env.path("client/cache/hashsync"),
            _ => self.env.path("server/home/.cache/hashsync"),
        };
        root.join(tree)
    }

    /// The single `<peer>-<index>` directory of a cache tree, or `None`.
    fn peer_dir(&self, side: &str, tree: &str) -> Option<PathBuf> {
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(self.cache_tree(side, tree))
            .ok()?
            .map(|entry| entry.unwrap().path())
            .collect();
        assert!(dirs.len() <= 1, "one peer directory expected: {dirs:?}");
        dirs.pop()
    }

    fn cache_files(&self, side: &str, tree: &str) -> Vec<String> {
        let Some(dir) = self.peer_dir(side, tree) else {
            return Vec::new();
        };
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }
}

/// The direction of the deduplicated stream of a run with one deduplicated
/// stream (DESIGN.md §3). The tests of what deduplication does run in
/// both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    /// The command's stdout, against the client's index.
    Pull,
    /// The client's stdin, against the server's index, with
    /// `--dedup-stdin`.
    Push,
}

const DIRECTIONS: [Direction; 2] = [Direction::Pull, Direction::Push];

impl Direction {
    /// The batch delivery of the deduplicated stream, for the tests that
    /// count references and runs exactly: in eager delivery, a block or a
    /// run whose window spans an idle point becomes literal, and the idle
    /// points depend on timing (DESIGN.md §6.3).
    fn batch(self) -> &'static str {
        match self {
            Direction::Pull => "--batch-stdout",
            Direction::Push => "--batch-stdin",
        }
    }

    /// The host that receives the deduplicated stream, as
    /// `Transfer::cache_files` names it.
    fn receiver(self) -> &'static str {
        match self {
            Direction::Pull => "client",
            Direction::Push => "server",
        }
    }

    /// The host that sends the deduplicated stream.
    fn sender(self) -> &'static str {
        match self {
            Direction::Pull => "server",
            Direction::Push => "client",
        }
    }
}

/// A run that `Transfer::spawn_stream` started: the client, and in a push,
/// the producer of its stdin.
struct Stream {
    client: KillOnDrop,
    producer: Option<KillOnDrop>,
}

impl Stream {
    /// The stdout of the client, which the test can read while the run
    /// goes on.
    fn stdout(&mut self) -> &mut std::process::ChildStdout {
        self.client.0.stdout.as_mut().unwrap()
    }

    /// Wait for the end of the run, and return the client's output. The
    /// producer of a push must succeed.
    fn output(self) -> Output {
        let output = output_of(self.client);
        if let Some(mut producer) = self.producer {
            assert!(producer.0.wait().unwrap().success(), "the producer failed");
        }
        output
    }
}

fn args(words: &[&str]) -> Vec<OsString> {
    words.iter().map(OsString::from).collect()
}

/// The line of the summary that contains `marker`.
fn summary_line(output: &Output, marker: &str) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let line = stderr
        .lines()
        .find(|line| line.contains(marker))
        .unwrap_or_else(|| panic!("no summary line with {marker:?}:\n{stderr}"));
    line.to_owned()
}

/// The numbers of `line`: the words between the characters that are not
/// digits or `.`.
fn numbers(line: &str) -> Vec<&str> {
    line.split(|c: char| !c.is_ascii_digit() && c != '.')
        .filter(|word| !word.is_empty())
        .collect()
}

/// `number`, a count of whole bytes on `line`. An amount in scale units,
/// such as `1.65`, fails the test.
fn whole_bytes(number: &str, line: &str) -> usize {
    number
        .parse()
        .unwrap_or_else(|_| panic!("{number} is not whole bytes: {line}"))
}

/// A part of the summary line of a deduplicated stream, which only such a
/// line has: the plain stream's line gives no literal bytes (DESIGN.md
/// §8).
const DEDUPLICATED: &str = " literal, ";

/// The byte counts of `line`, a summary line of a run with `EXACT`
/// amounts.
fn line_counts(line: &str) -> Vec<usize> {
    numbers(line)
        .into_iter()
        .map(|number| whole_bytes(number, line))
        .collect()
}

/// The summary line of a whole deduplicated stream in a run with `EXACT`
/// amounts (DESIGN.md §8): `NAME -- B (L literal, R -> RF refs, N -> NF
/// RLE, F false positives, M not found) + H hashlist + D hashlist delta
/// --> W wire (deduplicated, compressed)`, where each list shows only when
/// the exchange sent it.
#[derive(Debug)]
struct DeduplicatedLine {
    /// The bytes of the stream, then its literal, reference, and RLE bytes.
    bytes: [usize; 4],
    /// The bytes of the REF frames and of the RLE frames.
    frames: [usize; 2],
    /// The false positives, and the blocks not found.
    misses: [usize; 2],
    /// The bytes of the full list, and of the delta.
    hashlist: Option<usize>,
    delta: Option<usize>,
    /// The CHUNK bytes and the list bytes together.
    wire: usize,
    /// What follows the wire bytes: how the stream went over the wire.
    how: String,
}

impl DeduplicatedLine {
    fn parse(line: &str) -> Self {
        let unexpected = || -> ! { panic!("unexpected summary line: {line}") };
        let Some((data, wire)) = line.split_once(" --> ") else {
            unexpected()
        };
        // The first `)` closes the parts of the stream's bytes, and the
        // lists follow, each after ` + `.
        let Some((data, lists)) = data.split_once(')') else {
            unexpected()
        };
        let [
            bytes,
            literal,
            reference,
            reference_frames,
            run,
            run_frames,
            false_positives,
            not_found,
        ] = line_counts(data)[..]
        else {
            unexpected()
        };
        let mut lists = lists.split(" + ");
        if lists.next() != Some("") {
            unexpected();
        }
        let (mut hashlist, mut delta) = (None, None);
        for list in lists {
            let [size] = line_counts(list)[..] else {
                unexpected()
            };
            match list.strip_prefix(&format!("{size} B ")) {
                Some("hashlist") if hashlist.is_none() && delta.is_none() => hashlist = Some(size),
                Some("hashlist delta") if delta.is_none() => delta = Some(size),
                _ => unexpected(),
            }
        }
        let Some((wire, how)) = wire.split_once(" B wire") else {
            unexpected()
        };
        let [wire] = line_counts(wire)[..] else {
            unexpected()
        };
        DeduplicatedLine {
            bytes: [bytes, literal, reference, run],
            frames: [reference_frames, run_frames],
            misses: [false_positives, not_found],
            hashlist,
            delta,
            wire,
            how: how.to_owned(),
        }
    }

    /// The bytes of the stream's CHUNK payloads: its wire bytes without the
    /// lists.
    fn chunks(&self) -> usize {
        self.wire - self.hashlist.unwrap_or(0) - self.delta.unwrap_or(0)
    }
}

/// The summary line of the deduplicated stream of a run that deduplicates
/// one stream.
fn deduplicated(output: &Output) -> DeduplicatedLine {
    DeduplicatedLine::parse(&summary_line(output, DEDUPLICATED))
}

/// The summary line of the deduplicated stream `stream`, `stdin` or
/// `stdout`.
fn deduplicated_stream(output: &Output, stream: &str) -> DeduplicatedLine {
    DeduplicatedLine::parse(&summary_line(output, &format!("hashsync: {stream} -- ")))
}

/// The bytes of the deduplicated stream, then its literal, reference, and
/// RLE bytes, from its summary line.
fn summary(output: &Output) -> [usize; 4] {
    deduplicated(output).bytes
}

/// The bytes and the wire bytes of the plain stream, `stdin` or `stdout`,
/// from its summary line.
fn plain_counts(output: &Output, stream: &str) -> [usize; 2] {
    let line = summary_line(output, &format!("hashsync: {stream} "));
    assert!(
        !line.contains(DEDUPLICATED),
        "{stream} is deduplicated: {line}"
    );
    let [bytes, wire] = line_counts(&line)[..] else {
        panic!("unexpected summary line: {line}");
    };
    [bytes, wire]
}

/// The last summary line, which reports the traffic as rsync does.
struct Traffic {
    sent: usize,
    received: usize,
    seconds: f64,
    /// Bytes per second.
    rate: usize,
    /// The data shrinkage factor.
    shrinkage: f64,
    /// The bytes saved, negative when the run sent more than it delivered.
    saved: i64,
}

impl Traffic {
    /// Check the data shrinkage factor and the saved bytes of a run in
    /// `direction` against `delivered`, the bytes that the destinations of
    /// both streams took: they are those bytes over the traffic, and those
    /// bytes minus the traffic (DESIGN.md §8). The line rounds the factor to
    /// two decimals.
    fn check_delivered(&self, delivered: usize, direction: Direction) {
        let traffic = self.sent + self.received;
        let shrinkage = delivered as f64 / traffic as f64;
        assert!(
            (self.shrinkage - shrinkage).abs() <= 0.005 + 1e-9,
            "{direction:?}: data shrinkage factor {}, expected {shrinkage}",
            self.shrinkage
        );
        let saved = delivered as i64 - traffic as i64;
        assert_eq!(self.saved, saved, "{direction:?}: saved bytes");
    }
}

/// The last summary line of a run with `EXACT` amounts, `total -- sent S
/// and received R in Ts (X/s), data shrinkage factor: F, saved Z of data
/// transfer`, whose numbers are S, R, T, X, F, and Z, with a sign before Z
/// when it is negative.
fn traffic(output: &Output) -> Traffic {
    let line = summary_line(output, "hashsync: total -- ");
    let [sent, received, seconds, rate, shrinkage, saved] = numbers(&line)[..] else {
        panic!("unexpected traffic line: {line}");
    };
    let saved = whole_bytes(saved, &line) as i64;
    Traffic {
        sent: whole_bytes(sent, &line),
        received: whole_bytes(received, &line),
        seconds: seconds.parse().unwrap(),
        rate: whole_bytes(rate, &line),
        shrinkage: shrinkage.parse().unwrap(),
        saved: if line.contains(" saved -") {
            -saved
        } else {
            saved
        },
    }
}

/// The units of amounts of bytes, from the smallest (DESIGN.md §8).
const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

/// The amounts of bytes on a summary line, as pairs of a word and the
/// unit after it. A rate such as `(1.65 KiB/s)` gives `1.65` and `KiB`.
fn amounts(line: &str) -> Vec<(&str, &str)> {
    let words: Vec<&str> = line.split([' ', '(', '/']).collect();
    words
        .windows(2)
        .filter(|pair| UNITS.contains(&pair[1]))
        .map(|pair| (pair[0], pair[1]))
        .collect()
}

/// Whether `number` and `unit` show an amount in scale units: whole
/// bytes below 1 KiB, else two decimal digits.
fn is_scaled_amount(number: &str, unit: &str) -> bool {
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    match unit {
        "B" => digits(number) && number.parse::<u64>().unwrap() < 1024,
        _ => {
            UNITS.contains(&unit)
                && number.split_once('.').is_some_and(|(whole, fraction)| {
                    digits(whole) && fraction.len() == 2 && digits(fraction)
                })
        }
    }
}

/// Fail the test once `child`, a run or its client, has ended. This exists
/// for the polls for a mark that only a live run makes: without it, they
/// would wait forever when the run ends first, and never show why. The
/// message holds the status and what the run wrote to its stderr. Its
/// descendants can still hold the pipe, so this takes only what the pipe
/// holds, without a wait.
fn assert_running(child: &mut KillOnDrop) {
    let Some(status) = child.0.try_wait().unwrap() else {
        return;
    };
    let mut stderr = Vec::new();
    if let Some(pipe) = &mut child.0.stderr {
        let flags = rustix::fs::fcntl_getfl(&*pipe).unwrap();
        rustix::fs::fcntl_setfl(&*pipe, flags | OFlags::NONBLOCK).unwrap();
        // At `EAGAIN`, the bytes read so far are in `stderr`.
        let _ = pipe.read_to_end(&mut stderr);
    }
    panic!(
        "the run ended first, {status}:\n{}",
        String::from_utf8_lossy(&stderr)
    );
}

/// Wait for `client` to end, and return its output, as
/// `Child::wait_with_output` does. A test that fails before kills the
/// client instead, so that it does not stay. A stdout that the test took
/// from the client already gives no bytes here.
fn output_of(mut client: KillOnDrop) -> Output {
    let mut stderr = client.0.stderr.take().unwrap();
    let errors = std::thread::spawn(move || {
        let mut text = Vec::new();
        stderr.read_to_end(&mut text).unwrap();
        text
    });
    let mut stdout = Vec::new();
    if let Some(mut pipe) = client.0.stdout.take() {
        pipe.read_to_end(&mut stdout).unwrap();
    }
    let status = client.0.wait().unwrap();
    Output {
        status,
        stdout,
        stderr: errors.join().unwrap(),
    }
}

/// Require a successful run; return its output.
fn succeeded(output: Output) -> Output {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "run failed:\n{stderr}");
    output
}

/// The permission bits of `path`.
fn mode(path: &std::path::Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

fn refs(words: &[OsString]) -> Vec<&OsStr> {
    words.iter().map(OsString::as_os_str).collect()
}

#[test]
fn output_reproduces_byte_for_byte_and_lists_are_cached() {
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        // An indexed tree, so the exchanged hash list is not empty.
        let indexed = transfer.indexed(direction, "blocks");
        transfer.env.write(indexed, &common::tiled(64, 7));

        let payload = random(10 << 20, 42);
        let source = transfer.env.path("payload");
        std::fs::write(&source, &payload).unwrap();
        // In a pull, the command reads its empty stdin to the EOF first, so
        // that the forward stream ends before the command's exit can stop
        // it, and its summary line is that of an empty zstd frame.
        let script = match direction {
            Direction::Pull => format!("cat >/dev/null; cat '{}'", source.display()),
            Direction::Push => format!("cat '{}'", source.display()),
        };
        let output = succeeded(transfer.run_stream(direction, &[], &script));
        assert!(
            output.stdout == payload,
            "{direction:?}: stdout must match byte for byte"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        let payload_len = payload.len();
        let stream = match direction {
            Direction::Pull => "stdout",
            Direction::Push => "stdin",
        };
        assert!(
            stderr.contains(&format!(
                "hashsync: {stream} -- {payload_len} B ({payload_len} B literal, \
                 0 B -> 0 B refs, 0 B -> 0 B RLE, 0 false positives, 0 not found) + "
            )),
            "summary counts:\n{stderr}"
        );
        let line = deduplicated(&output);
        assert_eq!(line.how, " (deduplicated, compressed)", "{direction:?}");
        match direction {
            // The empty stdin of the client goes as an empty zstd frame,
            // and its line comes first.
            Direction::Pull => assert!(
                stderr.contains(
                    "hashsync: stdin -- 0 B --> 13 B wire (compressed)\nhashsync: stdout"
                ),
                "summary counts:\n{stderr}"
            ),
            Direction::Push => assert_eq!(plain_counts(&output, "stdout")[0], payload_len),
        }

        // Both sides published the same list under their peer directories
        // (DESIGN.md §5.6), and both created account identities.
        let receiver = transfer.cache_files(direction.receiver(), "local-hashlists");
        let sender = transfer.cache_files(direction.sender(), "remote-hashlists");
        assert_eq!(
            receiver.len(),
            1,
            "{direction:?}: receiver cache: {receiver:?}"
        );
        assert_eq!(receiver, sender, "both sides store the same list name");
        assert!(receiver[0].starts_with("blake3-") && receiver[0].ends_with(".cache"));
        for identity in [
            "client/config/hashsync/account-id",
            "server/home/.config/hashsync/account-id",
        ] {
            assert!(transfer.env.path(identity).exists(), "{identity}");
        }

        // A second run reuses both identities and keeps the same list;
        // nothing accumulates.
        succeeded(transfer.run_stream(direction, &[], "true"));
        let cached = transfer.cache_files(direction.receiver(), "local-hashlists");
        assert_eq!(cached, receiver);
        let cached = transfer.cache_files(direction.sender(), "remote-hashlists");
        assert_eq!(cached, sender);
    }
}

#[test]
fn indexed_blocks_become_references_at_any_offset() {
    // The blocks of an indexed file appear twice in the stream, at
    // unaligned offsets. Every block becomes a reference; the lookups run
    // concurrently, and the pins and the spools leave nothing behind in
    // the tmp_data directories of both sides (DESIGN.md §6.5). The client
    // of a push has no configuration file, and keeps no index (DESIGN.md
    // §7).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let blocks = tiled(64, 7);
        let indexed = transfer.indexed(direction, "blocks");
        transfer.env.write(&indexed, &blocks);
        let script = format!(
            "printf abc; cat '{0}'; printf defg; cat '{0}'",
            transfer.env.path(&indexed).display()
        );
        let output = succeeded(transfer.run_stream(direction, &[direction.batch()], &script));
        let expected = [&b"abc"[..], &blocks, b"defg", &blocks].concat();
        assert!(
            output.stdout == expected,
            "{direction:?}: the output differs"
        );
        let counts = [expected.len(), 7, 2 * blocks.len(), 0];
        assert_eq!(summary(&output), counts, "{direction:?}");

        for tmp_data in [
            "client/cache/hashsync/tmp-data",
            "server/home/.cache/hashsync/tmp-data",
        ] {
            let tmp_data = transfer.env.path(tmp_data);
            assert_eq!(mode(&tmp_data), 0o700, "{tmp_data:?} is not private");
            assert_eq!(std::fs::read_dir(&tmp_data).unwrap().count(), 0);
        }
        // Without a configuration file, the client's index would be the
        // default one, under its `XDG_STATE_HOME`.
        if direction == Direction::Push {
            assert!(transfer.server_index().exists(), "the server has no index");
            let state = transfer.env.path("client/state/hashsync");
            assert!(!state.exists(), "the client has an index");
        }
    }
}

#[test]
fn stale_and_unreadable_candidates_are_misses() {
    // A `--no-update` run reads candidates from a file that changed, then
    // became unreadable, after the update. Verification rejects those
    // candidates, and the output stays exact (DESIGN.md §3). The index has
    // their blocks, so the summary counts them as not found, and not as
    // false positives: the changed block once, and with the unreadable
    // file, each of the 8 blocks (DESIGN.md §8).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let original = tiled(8, 11);
        let indexed = transfer.indexed(direction, "file");
        transfer.env.write(&indexed, &original);
        let payload = transfer.env.path("payload");
        std::fs::write(&payload, &original).unwrap();
        transfer.update_receiver(direction, &[]);
        let script = format!("cat '{}'", payload.display());
        let flags = ["--no-update", direction.batch()];

        let mut changed = original.clone();
        changed[3 * BLOCK + 100] ^= 1;
        std::fs::write(transfer.env.path(&indexed), &changed).unwrap();
        let output = succeeded(transfer.run_stream(direction, &flags, &script));
        assert_eq!(output.stdout, original);
        let line = deduplicated(&output);
        assert_eq!(
            line.bytes,
            [8 * BLOCK, BLOCK, 7 * BLOCK, 0],
            "{direction:?}"
        );
        assert_eq!(line.misses, [0, 1], "{direction:?}");

        transfer.env.chmod(&indexed, 0o000);
        let output = succeeded(transfer.run_stream(direction, &flags, &script));
        transfer.env.chmod(&indexed, 0o644);
        assert_eq!(output.stdout, original);
        let line = deduplicated(&output);
        assert_eq!(line.bytes, [8 * BLOCK, 8 * BLOCK, 0, 0], "{direction:?}");
        assert_eq!(line.misses, [0, 8], "{direction:?}");
    }
}

/// The weak-hash polynomial of `window`, `sum(w[i] * a^(B-1-i))` modulo
/// 2^64 (DESIGN.md §5.1). The weak hash mixes it with a bijection, so
/// windows with equal polynomials have equal weak hashes.
fn weak_polynomial(window: &[u8]) -> u64 {
    const MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;
    window.iter().fold(0, |poly: u64, &byte| {
        poly.wrapping_mul(MULTIPLIER).wrapping_add(byte.into())
    })
}

/// Two different blocks with the same weak hash: the Thue-Morse string of
/// 1024 bytes, and its complement, followed by the same bytes. A string of
/// 2^n bytes differs from its complement by a product of the factors
/// `a^(2^k) - 1` for `k < n`, whatever the bytes for 0 and 1. For an odd
/// multiplier `a` that is 5 modulo 8, such a factor holds `k + 2` factors
/// of 2, so for n = 10, the product holds 65, and vanishes modulo 2^64.
fn weak_collision() -> (Vec<u8>, Vec<u8>) {
    let tail = random(BLOCK - 1024, 81);
    let block = |flip: u32| {
        let thue_morse = (0..1024u32).map(|i| b"ab"[((i.count_ones() + flip) % 2) as usize]);
        thue_morse.chain(tail.iter().copied()).collect::<Vec<u8>>()
    };
    let (indexed, other) = (block(0), block(1));
    assert_ne!(indexed, other);
    assert_eq!(weak_polynomial(&indexed), weak_polynomial(&other));
    (indexed, other)
}

#[test]
fn a_window_that_only_shares_its_weak_hash_is_a_false_positive() {
    // The stream's only window has the weak hash of an indexed block, but
    // other bytes (`weak_collision`). The scanner queries it, the
    // receiver's index has no block with its strong hash, and the answer is
    // no: the window goes as literal bytes, and the summary counts one false
    // positive, and no block not found (DESIGN.md §3 and §8).
    let (indexed, stream) = weak_collision();
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        transfer
            .env
            .write(transfer.indexed(direction, "block"), &indexed);
        let payload = transfer.env.path("payload");
        std::fs::write(&payload, &stream).unwrap();
        let script = format!("cat '{}'", payload.display());
        let output = succeeded(transfer.run_stream(direction, &[direction.batch()], &script));
        assert!(output.stdout == stream, "{direction:?}: the output differs");
        let line = deduplicated(&output);
        assert_eq!(line.bytes, [BLOCK, BLOCK, 0, 0], "{direction:?}");
        assert_eq!(line.misses, [1, 0], "{direction:?}");
    }
}

#[test]
fn candidate_attempts_bound_each_query() {
    // One file holds a block twice, and its first occurrence changes after
    // the update. Candidates follow the block numbers, an order that the
    // index fixes but DESIGN.md §6.4 does not: one attempt reaches only
    // the stale occurrence, and the summary counts the block as not found;
    // two reach the intact one. The receiver's configuration sets the
    // attempts.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let block = random(BLOCK, 12);
        let indexed = transfer.indexed(direction, "twice");
        transfer.env.write(&indexed, &block.repeat(2));
        let payload = transfer.env.path("payload");
        std::fs::write(&payload, &block).unwrap();
        transfer.update_receiver(direction, &[]);
        let mut changed = block.repeat(2);
        changed[5] ^= 1;
        std::fs::write(transfer.env.path(&indexed), &changed).unwrap();

        let script = format!("cat '{}'", payload.display());
        for (attempts, reference, not_found) in [(1, 0, 1), (2, BLOCK, 0)] {
            let attempts = format!("candidate_attempts = {attempts}\n");
            transfer.configure_receiver(direction, &attempts);
            let output = succeeded(transfer.run_stream(direction, &["--no-update"], &script));
            assert_eq!(output.stdout, block);
            let line = deduplicated(&output);
            let counts = [BLOCK, BLOCK - reference, reference, 0];
            assert_eq!(line.bytes, counts, "{direction:?}");
            assert_eq!(line.misses, [0, not_found], "{direction:?}");
        }
    }
}

#[test]
fn a_rejected_window_does_not_hide_an_overlapping_match() {
    // The block of a changed file hides a valid block that starts 8 bytes
    // into it. The rejection makes the scanner rescan from the next byte,
    // which finds the valid block (DESIGN.md §6.3).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let valid = random(BLOCK, 13);
        let hider = [&random(8, 14)[..], &valid[..BLOCK - 8]].concat();
        transfer
            .env
            .write(transfer.indexed(direction, "valid"), &valid);
        transfer
            .env
            .write(transfer.indexed(direction, "hider"), &hider);
        let payload = [&hider[..8], &valid[..]].concat();
        std::fs::write(transfer.env.path("payload"), &payload).unwrap();
        transfer.update_receiver(direction, &[]);
        transfer
            .env
            .write(transfer.indexed(direction, "hider"), &random(BLOCK, 15));

        let script = format!("cat '{}'", transfer.env.path("payload").display());
        let output = succeeded(transfer.run_stream(direction, &["--no-update"], &script));
        assert_eq!(output.stdout, payload);
        assert_eq!(summary(&output), [8 + BLOCK, 8, BLOCK, 0], "{direction:?}");
    }
}

#[test]
fn the_client_keeps_its_temporary_data_in_the_configured_directory() {
    let transfer = Transfer::new(&[]);
    let tmp_data = transfer.env.path("tmp/nested");
    let configure = |path: &std::path::Path| {
        format!(
            "tmp_data_dir = '{}'\n",
            toml_literal(path.as_os_str().as_bytes())
        )
    };
    transfer.configure(&configure(&tmp_data));
    let blocks = tiled(8, 16);
    transfer.env.write("tree/blocks", &blocks);
    let command = args(&["cat", transfer.env.path("tree/blocks").to_str().unwrap()]);
    let output = succeeded(transfer.run(&[BATCH], &refs(&command)));
    assert_eq!(output.stdout, blocks);
    assert_eq!(summary(&output), [8 * BLOCK, 0, 8 * BLOCK, 0]);
    assert_eq!(mode(&tmp_data), 0o700);
    assert_eq!(std::fs::read_dir(&tmp_data).unwrap().count(), 0);

    // A tmp_data directory that cannot exist, or that cannot hold
    // segments, fails the run when it opens, before the sessions start:
    // the command does not run (DESIGN.md §6.5).
    transfer.env.write("not-a-directory", b"x");
    let blocked = transfer.env.path("not-a-directory/tmp");
    transfer.env.mkdir("read-only");
    transfer.env.chmod("read-only", 0o500);
    let read_only = transfer.env.path("read-only");
    let marker = transfer.env.path("marker");
    let command = args(&["touch", marker.to_str().unwrap()]);
    for (path, message) in [
        (&blocked, "cannot create tmp_data directory"),
        (&read_only, "cannot create a segment in tmp_data directory"),
    ] {
        transfer.configure(&configure(path));
        let output = transfer.run(&[], &refs(&command));
        assert_eq!(output.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(message), "{stderr}");
        assert!(!marker.exists(), "the command ran");
    }

    // The client of a push needs no configuration file, but it uses the
    // one that `--config` names, for its tmp_data directory (DESIGN.md §7).
    let transfer = Transfer::new(&[]);
    transfer.configure_server("");
    let tmp_data = transfer.env.path("tmp/pushed");
    transfer.configure(&configure(&tmp_data));
    let config = transfer.env.config_path();
    let flags = ["--config", config.to_str().unwrap()];
    let output = transfer.push(&flags, &refs(&args(&["cat"]))).output();
    succeeded(output.unwrap());
    assert_eq!(mode(&tmp_data), 0o700);
    assert_eq!(std::fs::read_dir(&tmp_data).unwrap().count(), 0);
}

#[test]
fn a_full_tmp_data_directory_fails_the_run() {
    // The client's tmp_data directory is a tmpfs in a new user and mount
    // namespace. It holds 64 KiB, also with pages of 64 KiB, and the pins
    // of the output need 128 KiB in their first segment: a segment write
    // fails, which ends the run (DESIGN.md §6.7).
    if !common::namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    let transfer = Transfer::new(&[]);
    let tmp_data = transfer.env.path("tmpfs");
    std::fs::create_dir(&tmp_data).unwrap();
    transfer.configure(&format!(
        "tmp_data_dir = '{}'\n",
        toml_literal(tmp_data.as_os_str().as_bytes())
    ));
    transfer.env.write("tree/blocks", &tiled(32, 19));
    let command = args(&["cat", transfer.env.path("tree/blocks").to_str().unwrap()]);
    let run = transfer.invocation(&[], &refs(&command));
    let mut unshare = Command::new("unshare");
    unshare
        .args([
            "-Urm",
            "sh",
            "-c",
            "mount -t tmpfs -o size=64k none \"$0\" || exit 99; exec \"$@\"",
        ])
        .arg(&tmp_data)
        .arg(run.get_program())
        .args(run.get_args());
    for (key, value) in run.get_envs() {
        unshare.env(key, value.unwrap());
    }
    let output = unshare.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("cannot write a segment in tmp_data directory"),
        "{stderr}"
    );
}

#[test]
fn a_server_tmp_data_directory_that_cannot_hold_a_segment_fails_before_the_command() {
    // The server's tmp_data directory exists, but it is read-only: the
    // server cannot create segments in it, and the run must end before
    // the command starts (DESIGN.md §6.5). A command that started would
    // be killed at once, before it could leave a mark of its own, so the
    // test watches the command's script: `exec` opens it.
    let transfer = Transfer::new(&[]);
    let tmp_data = "server/home/.cache/hashsync/tmp-data";
    transfer.env.mkdir(tmp_data);
    transfer.env.chmod(tmp_data, 0o500);
    let script = transfer.env.path("command");
    std::fs::write(&script, "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let opens =
        inotify::init(inotify::CreateFlags::CLOEXEC | inotify::CreateFlags::NONBLOCK).unwrap();
    inotify::add_watch(&opens, &script, inotify::WatchFlags::OPEN).unwrap();
    let output = transfer.run(&[], &[script.as_os_str()]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("cannot create a segment in tmp_data directory"),
        "{stderr}"
    );
    let mut event = [0u8; 256];
    assert_eq!(
        read(&opens, &mut event),
        Err(Errno::AGAIN),
        "the command started"
    );
}

#[test]
fn a_file_size_limit_on_the_server_ends_the_run_with_its_cleanup() {
    // The server runs under a file-size limit of 64 KiB (128 KiB where
    // `ulimit -f` counts KiB), and the command writes 1 MiB of new data,
    // which the server's spool writes to a segment. SIGXFSZ stays blocked
    // in the server, so that write fails with `EFBIG`, and `die` ends the
    // run: it kills the command and removes the rendezvous socket
    // (DESIGN.md §6.5 and §6.9). The default end of SIGXFSZ would skip
    // that cleanup.
    let transfer = Transfer::new(&[]);
    let limited = transfer.env.path("limited-hashsync");
    let script = format!(
        "#!/bin/sh\nulimit -f 128\nexec '{}' \"$@\"\n",
        env!("CARGO_BIN_EXE_hashsync")
    );
    std::fs::write(&limited, script).unwrap();
    std::fs::set_permissions(&limited, std::fs::Permissions::from_mode(0o755)).unwrap();
    transfer.set_hashsync_path(limited.as_os_str());
    transfer.env.write("payload", &random(1 << 20, 23));
    let command = args(&["cat", transfer.env.path("payload").to_str().unwrap()]);
    let output = transfer.run(&[], &refs(&command));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("cannot write a segment in tmp_data directory")
            && stderr.contains("File too large"),
        "{stderr}"
    );
    let sockets = transfer.env.path("server/runtime/hashsync");
    assert_eq!(
        std::fs::read_dir(sockets).unwrap().count(),
        0,
        "the rendezvous socket remains"
    );
}

#[test]
fn the_command_starts_without_the_signals_that_hashsync_blocks() {
    // Every process blocks SIGXFSZ in every thread, and the client also
    // blocks SIGINT and SIGTERM. Each gives its children the mask from
    // before, because a blocked signal stays blocked across `exec`. So the
    // command, `cat` here, starts with none of these signals blocked.
    let transfer = Transfer::new(&[]);
    let output = transfer.run_ok(&refs(&args(&["cat", "/proc/self/status"])));
    let status = String::from_utf8(output.stdout).unwrap();
    let blocked = status
        .lines()
        .find_map(|line| line.strip_prefix("SigBlk:"))
        .expect("a SigBlk line");
    let blocked = u64::from_str_radix(blocked.trim(), 16).unwrap();
    for signal in [Signal::INT, Signal::TERM, Signal::XFSZ] {
        // Bit `n - 1` of the mask stands for signal `n`.
        let bit = 1 << (signal.as_raw() - 1);
        assert_eq!(blocked & bit, 0, "{signal:?} is blocked: {blocked:x}");
    }
}

#[test]
fn a_slow_consumer_and_a_one_window_lookahead_keep_the_output_exact() {
    // The scanner may look only one window ahead, so every query waits for
    // its answer before scanning goes on, and the source waits for the
    // scanner once 64 KiB of the stream are in the sender's spool. Stdout
    // drains slowly, so the pins wait for the decoder, which in a push
    // waits for the command, whose stdout waits for the client's. Output,
    // answers, and pin release stay in order (DESIGN.md §6.3 to §6.5).
    let block = BLOCK.to_string();
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&["--lookahead", &block, "--spool-limit", "64KiB"]);
        transfer.configure_receiver(direction, "");
        let blocks = tiled(256, 17);
        let indexed = transfer.indexed(direction, "big");
        transfer.env.write(&indexed, &blocks);
        let script = format!(
            "printf x; cat '{0}'; printf yz; cat '{0}'",
            transfer.env.path(&indexed).display()
        );
        let mut stream = transfer.spawn_stream(direction, &[direction.batch()], &script);
        let mut received = Vec::new();
        let mut chunk = vec![0u8; 16 << 10];
        loop {
            let n = stream.stdout().read(&mut chunk).unwrap();
            if n == 0 {
                break;
            }
            received.extend_from_slice(&chunk[..n]);
            std::thread::sleep(Duration::from_millis(1));
        }
        let output = succeeded(stream.output());
        assert!(
            received == [&b"x"[..], &blocks, b"yz", &blocks].concat(),
            "{direction:?}: the output differs"
        );
        let counts = [received.len(), 3, 2 * blocks.len(), 0];
        assert_eq!(summary(&output), counts, "{direction:?}");
    }
}

#[test]
fn runs_and_compression_shrink_the_wire() {
    // Zeros become runs of whole windows, and the zstd stream compresses
    // the repetitive text. Random bytes shrink neither way (DESIGN.md §6.2
    // and §6.3). The summary lines say which streams were compressed and
    // deduplicated (DESIGN.md §8).
    const ZEROS: usize = 4 << 20;
    for direction in DIRECTIONS {
        for (options, compressed) in [(&[][..], true), (&["--compression-level", "0"], false)] {
            let transfer = Transfer::new(options);
            transfer.configure_receiver(direction, "");
            let text = "hashsync compresses repetitive text\n".repeat(30_000);
            let noise = random(1 << 20, 23);
            transfer.env.write("text", text.as_bytes());
            transfer.env.write("noise", &noise);
            let script = format!(
                "head -c {ZEROS} /dev/zero; cat '{}' '{}'",
                transfer.env.path("text").display(),
                transfer.env.path("noise").display()
            );
            let output = succeeded(transfer.run_stream(direction, &[direction.batch()], &script));
            let expected = [&vec![0; ZEROS][..], text.as_bytes(), &noise].concat();
            assert!(output.stdout == expected, "the output differs");
            let literal = text.len() + noise.len();
            let counts = [expected.len(), literal, 0, ZEROS];
            let line = deduplicated(&output);
            assert_eq!(line.bytes, counts, "{direction:?}");
            // Each RLE frame takes 14 bytes. The scanner sends a run before
            // it stops growing, so the number of frames that carry the
            // zeros depends on its pace.
            let [reference_frames, run_frames] = line.frames;
            assert!(
                reference_frames == 0 && run_frames > 0 && run_frames % 14 == 0,
                "{direction:?}: {line:?}"
            );
            let wire = line.chunks();
            let plain = match direction {
                Direction::Pull => "hashsync: stdin -- ",
                Direction::Push => "hashsync: stdout -- ",
            };
            let plain = summary_line(&output, plain);
            if compressed {
                assert!(
                    (noise.len()..noise.len() + (64 << 10)).contains(&wire),
                    "{direction:?}: {wire} bytes wire"
                );
                assert_eq!(line.how, " (deduplicated, compressed)");
                assert!(plain.ends_with(" B wire (compressed)"), "{plain}");
            } else {
                assert!(wire > literal, "{direction:?}: {wire} bytes wire");
                assert_eq!(line.how, " (deduplicated)");
                assert!(plain.ends_with(" B wire"), "{plain}");
            }
        }
    }
}

#[test]
fn a_source_that_pauses_transfers_exactly() {
    // The source of the stream, the command or the client's stdin, stops
    // for a while, with queries for its indexed blocks in flight. In batch
    // delivery, its frames can wait in the compressor meanwhile: the sender
    // does not flush the stream for the receiver's sake, and no wait
    // depends on those frames (DESIGN.md §6.2).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let blocks = tiled(16, 25);
        let indexed = transfer.indexed(direction, "blocks");
        transfer.env.write(&indexed, &blocks);
        let script = format!(
            "cat '{0}'; sleep 0.5; cat '{0}'",
            transfer.env.path(&indexed).display()
        );
        let output = succeeded(transfer.run_stream(direction, &[direction.batch()], &script));
        assert_eq!(output.stdout, blocks.repeat(2));
        let counts = [2 * blocks.len(), 0, 2 * blocks.len(), 0];
        assert_eq!(summary(&output), counts, "{direction:?}");
    }
}

#[test]
fn empty_output_and_command_failure_status() {
    let transfer = Transfer::new(&[]);
    let command = args(&["true"]);
    let output = transfer.run_ok(&refs(&command));
    assert!(output.stdout.is_empty());

    // A failing command still transfers its output and returns its
    // status (DESIGN.md §6.7).
    let command = args(&["echo partial; exit 3"]);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.stdout, b"partial\n");
    assert_eq!(output.status.code(), Some(3));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(DEDUPLICATED), "summary:\n{stderr}");
}

#[test]
fn the_summary_counts_both_sessions() {
    // Each reference needs a QUERY on the control session and a yes
    // ANSWER, which the data session does not carry. The last summary line
    // counts both sessions in both directions (DESIGN.md §8). Without
    // compression, the control session carries its frames as they are, and
    // the CHUNK payloads are the stream frames as they are.
    let transfer = Transfer::new(&["--compression-level", "0"]);
    let blocks = tiled(64, 7);
    transfer.env.write("tree/blocks", &blocks);
    // The command reads its empty stdin to the EOF first, so that the
    // forward stream ends before the command's exit can stop it.
    let path = transfer.env.path("tree/blocks");
    let command = args(&[&format!("cat >/dev/null; cat '{}'", path.display())]);
    let output = succeeded(transfer.run(&[BATCH], &refs(&command)));
    assert_eq!(output.stdout, blocks);
    let line = deduplicated(&output);
    assert_eq!(line.bytes[2], blocks.len(), "every block is a reference");
    // The first exchange sends the full list. The wire bytes of the stream
    // are that list and its CHUNK bytes: 64 REF frames of 5 bytes, which
    // fit one CHUNK.
    let hash_list = line.hashlist.expect("no full list");
    assert_eq!(line.delta, None);
    assert_eq!(line.frames, [64 * 5, 0]);
    assert_eq!(line.chunks(), 64 * 5, "64 REF frames");
    assert_eq!(line.how, " (deduplicated)");
    // The stdin of the client is empty: without compression, its stream
    // is STREAM_END alone.
    assert_eq!(plain_counts(&output, "stdin"), [0, 0]);

    // On the data session, the client reads the server's version and
    // features, its HELLO with its account ID and no list, CACHE_RESULT,
    // the backward stream in one CHUNK, STREAM_END, and END with its
    // status, an empty error, the server's nine counts of the forward
    // stream, and its count and EOF flag of the command's stdout. On the
    // control session, it reads the version and features, one QUERY per
    // block: a 5-byte frame header, the offset, and the full digest, and
    // the STOP of the forward stream at the command's exit, a header alone.
    // The server does not wait for its control writer at the end, so the
    // closure of the session can cut STOP off (DESIGN.md §6.6 and §6.9).
    // The client writes its versions and features on both sessions, the
    // hash list, and one ANSWER per block: a header, the offset, and the
    // flag.
    let traffic = traffic(&output);
    let start = version_and_features(FEATURES).len();
    let end = 5 + 4 + 4 + 9 * 8 + 8 + 1;
    let data = start + (5 + 16 + 4) + 5 + (5 + line.chunks()) + 5 + end;
    let control = start + 64 * (5 + 8 + 32);
    assert!(
        [data + control, data + control + 5].contains(&traffic.received),
        "received {} bytes",
        traffic.received
    );
    assert!(traffic.sent > hash_list + 2 * start + 64 * (5 + 8 + 1));

    // The data shrinkage factor and the saved bytes compare the bytes of
    // both streams with the traffic. The rate is the traffic over the
    // elapsed time. The line rounds the time to a tenth of a second and the
    // rate to whole bytes per second. So the traffic is between the
    // products of the smallest and of the largest time and rate that round
    // to the shown values, however long the run takes.
    traffic.check_delivered(blocks.len(), Direction::Pull);
    let total = traffic.sent + traffic.received;
    let (seconds, rate) = (traffic.seconds, traffic.rate as f64);
    let smallest = (seconds - 0.05).max(0.0) * (rate - 0.5).max(0.0);
    let largest = (seconds + 0.05) * (rate + 0.5);
    assert!(
        smallest - 1e-9 <= total as f64 && total as f64 <= largest + 1e-9,
        "{total} B in {seconds}s at {rate} B/s"
    );
}

#[test]
fn summaries_show_scale_units() {
    // Without `--no-human-readable`, amounts show in binary units with
    // two decimal digits (DESIGN.md §8). No block of the payload is
    // indexed, so all of it is literal. The indexed file makes the hash
    // list, and so the sent bytes, larger than 1 KiB.
    let transfer = Transfer::new(&[]);
    transfer.env.write("tree/blocks", &tiled(256, 7));
    let payload = random(10 << 20, 42);
    let source = transfer.env.path("payload");
    std::fs::write(&source, &payload).unwrap();
    // The command reads its empty stdin to the EOF first, so that the
    // forward stream ends before the command's exit can stop it.
    let command = args(&[&format!("cat >/dev/null; cat '{}'", source.display())]);
    let output = succeeded(transfer.invocation(&[], &refs(&command)).output().unwrap());
    assert_eq!(output.stdout, payload);
    let line = summary_line(&output, DEDUPLICATED);
    assert!(
        line.starts_with(
            "hashsync: stdout -- 10.00 MiB (10.00 MiB literal, 0 B -> 0 B refs, \
             0 B -> 0 B RLE, 0 false positives, 0 not found) + "
        ),
        "{line}"
    );
    let line = summary_line(&output, "hashsync: stdin ");
    assert_eq!(
        line, "hashsync: stdin -- 0 B --> 13 B wire (compressed)",
        "an empty zstd frame"
    );

    // The sent and received bytes, the rate, whose unit depends on the
    // elapsed time, and the saved bytes. Random bytes do not compress, and
    // the hash list goes too, so the run saves a negative amount, which
    // shows with its sign.
    let line = summary_line(&output, "hashsync: total -- ");
    let amounts = amounts(&line);
    assert!(
        amounts
            .iter()
            .all(|&(number, unit)| is_scaled_amount(number.trim_start_matches('-'), unit)),
        "{line}"
    );
    assert!(
        matches!(amounts[..], [(_, "KiB"), (_, "MiB"), _, (saved, _)] if saved.starts_with('-')),
        "{line}"
    );
}

#[test]
fn quiet_runs_print_only_the_command_stderr() {
    // `--quiet` removes the summary and the notes of the update, which
    // creates the receiver's index here. In a push, the server updates its
    // index, and its notes follow the `--quiet` that REQUEST carries. The
    // command's stderr still arrives (DESIGN.md §8).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let command = args(&["echo to-stdout; echo to-stderr >&2"]);
        let mut invocation = transfer.invocation_in(direction, &["--quiet"], &refs(&command));
        let output = succeeded(invocation.output().unwrap());
        assert_eq!(output.stdout, b"to-stdout\n", "{direction:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(stderr, "to-stderr\n", "{direction:?}");
    }
}

#[test]
fn the_notes_of_the_servers_update_follow_the_options_of_the_client() {
    // In a push, the server updates its index, which creates it here, and
    // its notes reach the client's stderr. They follow the client's
    // `--no-human-readable`, which REQUEST carries: the 16 KiB that the
    // update hashed show in scale units without it, and in whole bytes with
    // it (DESIGN.md §8).
    for (flags, hashed) in [(&[][..], "(16.00 KiB)"), (&[EXACT][..], "(16384 B)")] {
        let transfer = Transfer::new(&[]);
        transfer.configure_server("");
        transfer.env.write("server-tree/blocks", &tiled(4, 48));
        let mut invocation = transfer.push(flags, &refs(&args(&["true"])));
        let output = succeeded(invocation.output().unwrap());
        let line = summary_line(&output, "files hashed");
        assert!(line.contains(&format!("1 files hashed {hashed}")), "{line}");
    }
}

#[test]
fn a_push_to_a_server_without_a_configuration_file_fails() {
    // The receiver of a push needs its configuration file, for its roots
    // and its index (DESIGN.md §7). The server reads it after REQUEST, so
    // its error ends the run before the list exchange, and the command
    // never runs.
    let transfer = Transfer::new(&[]);
    let marker = transfer.env.path("command-ran");
    let command = format!(": >'{}'", marker.display());
    let output = transfer.run_push(&[], &refs(&args(&[&command])), b"input");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("cannot read configuration file"),
        "{stderr}"
    );
    assert!(!marker.exists(), "the command ran");
}

#[test]
fn command_killed_by_a_signal_reports_128_plus_signal() {
    let transfer = Transfer::new(&[]);
    let command = args(&["kill -KILL $$"]);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(128 + 9));
}

#[test]
fn startup_failure_reports_127_and_the_error() {
    // Without a shell, `-s`, the server itself fails to start the command
    // (DESIGN.md §6.2).
    let transfer = Transfer::new(&[]);
    let command = args(&["/nonexistent-hashsync-test-command"]);
    let output = transfer.run(&["-s"], &refs(&command));
    assert_eq!(output.status.code(), Some(127));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to start") && stderr.contains("nonexistent-hashsync-test"),
        "startup error:\n{stderr}"
    );

    // The error is not a note: `--quiet` keeps it, and removes the
    // summary.
    let output = transfer.run(&["-s", "--quiet"], &refs(&command));
    assert_eq!(output.status.code(), Some(127));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("failed to start"), "{stderr}");
    assert!(!stderr.contains(DEDUPLICATED), "{stderr}");

    // In a command line, the shell does not find the command: it starts,
    // and reports that with its own message and status 127, as under
    // sshd.
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(127));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("nonexistent-hashsync-test"), "{stderr}");
    assert!(!stderr.contains("failed to start"), "{stderr}");
}

#[test]
fn stderr_is_forwarded() {
    let transfer = Transfer::new(&[]);
    let command = args(&["echo to-stdout; echo to-stderr >&2"]);
    let output = transfer.run_ok(&refs(&command));
    assert_eq!(output.stdout, b"to-stdout\n");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("to-stderr"), "forwarded stderr:\n{stderr}");
}

#[test]
fn stderr_from_a_descendant_is_forwarded_after_the_command_exits() {
    // The descendant closes stdout but keeps stderr: command stdout hits
    // EOF at once, while stderr EOF and the relayed bytes come later.
    // Successful completion requires the full relay (DESIGN.md §6.9).
    let transfer = Transfer::new(&[]);
    let command =
        args(&["( exec >/dev/null; sleep 0.3; echo late-stderr >&2 ) & echo early-stdout"]);
    let output = transfer.run_ok(&refs(&command));
    assert_eq!(output.stdout, b"early-stdout\n");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("late-stderr"),
        "stderr written after command exit must still arrive:\n{stderr}"
    );
}

#[test]
fn arguments_and_output_preserve_arbitrary_bytes() {
    // In a command line for the remote shell, and as words with `-s`.
    let transfer = Transfer::new(&[]);
    let arg = OsString::from_vec(b"\xff\xfe-arg\x01".to_vec());
    let command: Vec<OsString> = vec!["printf".into(), "%s".into(), arg];
    for flags in [&[][..], &["-s"]] {
        let output = succeeded(transfer.run(flags, &refs(&command)));
        assert_eq!(output.stdout, b"\xff\xfe-arg\x01", "{flags:?}");
    }
}

#[test]
fn a_command_line_runs_in_the_remote_users_shell() {
    // By default, the client joins COMMAND and ARGS with single spaces, as
    // ssh does, and the server runs that line with `$SHELL -c`, as sshd
    // does: the shell splits the words again and expands them. With `-s`,
    // the words arrive as they are, and no shell runs (DESIGN.md §8). The
    // fake ssh passes the user of `-l` to the command, and here names a
    // wrapper as the user's shell, which records its arguments. Options can
    // follow HOST, and the command needs no `--` when its first word is not
    // an option.
    let transfer = Transfer::new(&[]);
    let shell = transfer.env.path("shell");
    let calls = transfer.env.path("shell-calls");
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" >>'{}'\nexec /bin/sh \"$@\"\n",
        calls.display()
    );
    std::fs::write(&shell, script).unwrap();
    std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o755)).unwrap();
    let command = ["printf", "%s.", "a  b", "$HASHSYNC_TEST_LOGIN"];
    let run = |flags: &[&str]| {
        let output = transfer
            .bare_invocation(&[])
            .arg(HOST)
            .args(flags)
            .args(["-l", "tester"])
            .args(command)
            .env("HASHSYNC_TEST_SHELL", &shell)
            .output()
            .unwrap();
        succeeded(output).stdout
    };
    assert_eq!(run(&[]), b"a.b.tester.");
    let calls_text = std::fs::read_to_string(&calls).unwrap();
    assert_eq!(calls_text, "-c\nprintf %s. a  b $HASHSYNC_TEST_LOGIN\n");
    std::fs::remove_file(&calls).unwrap();
    assert_eq!(run(&["-s"]), b"a  b.$HASHSYNC_TEST_LOGIN.");
    assert!(!calls.exists(), "the shell ran the words of -s");
}

#[test]
fn the_server_reads_its_own_configuration() {
    // The server of a pull, the sender of the command's stdout, needs no
    // configuration file, as every other test shows. With one, it keeps
    // the segments of its spools in its own `tmp_data_dir`, and its list
    // cache expires lists after its own `hashlist_max_age`, while the
    // client keeps its default of 30 days (DESIGN.md §5.6 and §7). A
    // `--hashsync-path` wrapper can name another file with `--config`.
    let transfer = Transfer::new(&[]);
    // A stale list in another peer/index directory of the cache of each
    // host.
    let stale = [
        transfer
            .env
            .path("server/home/.cache/hashsync/remote-hashlists/other/stale.cache"),
        transfer
            .env
            .path("client/cache/hashsync/local-hashlists/other/stale.cache"),
    ];
    for path in &stale {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"stale").unwrap();
    }
    // Expiry compares whole seconds of the ctime with the clock, so a list
    // is older than a `hashlist_max_age` of 0 once the second of its ctime
    // has passed. Wait for that, with no deadline, on a probe file of the
    // same filesystem.
    let ctime = |path: &std::path::Path| std::fs::metadata(path).unwrap().ctime();
    let newest = ctime(&stale[0]).max(ctime(&stale[1]));
    let probe = transfer.env.path("probe");
    loop {
        std::fs::write(&probe, b"").unwrap();
        if ctime(&probe) > newest {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    let default_dir = transfer.env.path("server/home/.cache/hashsync/tmp-data");
    let configured = transfer.env.path("server-tmp-data");
    let tmp_data_dir = |path: &std::path::Path| {
        format!(
            "tmp_data_dir = '{}'\n",
            toml_literal(path.as_os_str().as_bytes())
        )
    };
    let server_config = format!("{}hashlist_max_age = \"0\"\n", tmp_data_dir(&configured));
    transfer.env.write(
        "server/home/.config/hashsync/config.toml",
        server_config.as_bytes(),
    );
    let command = args(&["printf", "%s", "configured"]);
    let output = succeeded(transfer.run(&[], &refs(&command)));
    assert_eq!(output.stdout, b"configured");
    assert_eq!(mode(&configured), 0o700);
    assert!(
        !default_dir.exists(),
        "the server used its default directory"
    );
    assert!(
        !stale[0].exists(),
        "the server kept a list older than its hashlist_max_age"
    );
    assert!(
        stale[1].exists(),
        "the client expired a list with the hashlist_max_age of the server"
    );

    let other = transfer.env.path("other-tmp-data");
    transfer
        .env
        .write("other-config.toml", tmp_data_dir(&other).as_bytes());
    let wrapper = transfer.env.path("hashsync-with-config");
    let script = format!(
        "#!/bin/sh\nexec '{}' --config '{}' \"$@\"\n",
        env!("CARGO_BIN_EXE_hashsync"),
        transfer.env.path("other-config.toml").display()
    );
    std::fs::write(&wrapper, script).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    transfer.set_hashsync_path(wrapper.as_os_str());
    succeeded(transfer.run(&[], &refs(&command)));
    assert_eq!(mode(&other), 0o700);
    assert!(
        !default_dir.exists(),
        "the server used its default directory"
    );
}

#[test]
fn a_lookahead_below_the_block_size_ends_the_run_before_the_update() {
    // A `--lookahead` below the block size of the receiver is a user error.
    // The client reports it before it updates its index and before SSH
    // starts (DESIGN.md §8): here the blocks of the client have 64 KiB, the
    // run asks for 32 KiB, and a wrapper of SSH leaves a mark if it runs.
    let transfer = Transfer::new(&["--lookahead", "32KiB"]);
    transfer.env.configure_with(&format!(
        "block_size = \"64KiB\"\n{}",
        transfer.env.root("tree", "")
    ));
    let marker = transfer.env.path("ssh-ran");
    wrap_ssh(
        &transfer,
        &format!(": >'{}'\nexec \"$ssh\" \"$@\"", marker.display()),
    );
    let output = transfer.run(&[], &refs(&args(&["true"])));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("--lookahead must be at least the block size of the index (65536 bytes)"),
        "{stderr}"
    );
    assert!(!marker.exists(), "SSH started");
    assert!(!transfer.env.index_path().exists(), "the index was updated");
}

/// `rsync -a [rsync_flags] -e 'hashsync ssh [flags]' SOURCE DESTINATION`,
/// with this transfer's client and server: rsync runs `hashsync ssh` as its
/// remote shell, with the arguments that it gives ssh, `[-l USER] HOST
/// COMMAND...`, without `--` (DESIGN.md §8).
fn rsync(
    transfer: &Transfer,
    rsync_flags: &[&str],
    flags: &[&str],
    source: &OsStr,
    destination: &OsStr,
) -> Output {
    let words = [
        env!("CARGO_BIN_EXE_hashsync").into(),
        "--config".into(),
        transfer.env.config_path(),
        "ssh".into(),
        "--ssh".into(),
        transfer.env.path("ssh"),
        "--hashsync-path".into(),
        PathBuf::from(&*transfer.hashsync_path.borrow()),
    ];
    let mut remote_shell = String::new();
    for word in words
        .iter()
        .map(|word| word.to_str().unwrap())
        .chain(flags.iter().copied())
    {
        // rsync splits its remote shell at spaces.
        assert!(!word.contains([' ', '\'', '"']), "{word}");
        remote_shell.push_str(word);
        remote_shell.push(' ');
    }
    let output = Command::new("rsync")
        .arg("-a")
        .args(rsync_flags)
        .arg("-e")
        .arg(remote_shell.trim_end())
        .arg(source)
        .arg(destination)
        .stdin(Stdio::null())
        .envs(transfer.client_environment())
        .output()
        .expect("rsync must be installed for the tests");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "rsync failed:\n{stderr}");
    output
}

#[test]
fn rsync_runs_through_hashsync_ssh_in_both_directions() {
    // rsync talks both ways through its remote shell. In a pull, the
    // client's stdin carries the requests of the local rsync to the remote
    // one, and the command's stdout the files, deduplicated against the
    // client's index. In a push, with `--dedup-stdin`, the client's stdin
    // carries the files, deduplicated against the server's index, which
    // holds some of them. The remote path has a space, at `USER@HOST`,
    // which rsync turns into `-l USER HOST`. Without its own `-s`, rsync
    // escapes the path for the remote shell, which only the default command
    // line of `hashsync ssh` gives it. With rsync's `-s`, the path travels
    // inside rsync's protocol, so both modes of `hashsync ssh` work
    // (DESIGN.md §8).
    let files = [
        ("a", tiled(16, 51)),
        ("sub/b", random(100_000, 52)),
        ("sub/c d", b"small".to_vec()),
    ];
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let local = transfer.env.path("local dir");
        let remote = transfer.env.path("remote dir");
        let (source_dir, flags) = match direction {
            Direction::Pull => (&remote, &[][..]),
            Direction::Push => (&local, &["--dedup-stdin"][..]),
        };
        for (name, data) in &files {
            let path = source_dir.join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, data).unwrap();
        }
        // rsync creates the destination of a copy, but not its parent.
        std::fs::create_dir_all(&remote).unwrap();
        // The receiver indexes a copy of the file of whole blocks.
        transfer
            .env
            .write(transfer.indexed(direction, "a"), &files[0].1);
        transfer.update_receiver(direction, &[]);
        let remote_spec = |dir: &std::path::Path| {
            let mut spec = format!("tester@{HOST}:").into_bytes();
            spec.extend_from_slice(dir.as_os_str().as_bytes());
            spec.push(b'/');
            OsString::from_vec(spec)
        };
        for (i, (rsync_flags, shell_flags)) in
            [(&[][..], &[][..]), (&["-s"], &[]), (&["-s"], &["-s"])]
                .into_iter()
                .enumerate()
        {
            let all_flags = [flags, shell_flags].concat();
            let destination = match direction {
                Direction::Pull => transfer.env.path(format!("copy {i}")),
                Direction::Push => remote.join(format!("copy {i}")),
            };
            let (from, to) = match direction {
                Direction::Pull => (remote_spec(&remote), destination.clone().into_os_string()),
                Direction::Push => {
                    let mut source = local.clone().into_os_string();
                    source.push("/");
                    (source, remote_spec(&destination))
                }
            };
            rsync(&transfer, rsync_flags, &all_flags, &from, &to);
            for (name, data) in &files {
                let copy = std::fs::read(destination.join(name)).unwrap();
                let what = format!("{direction:?} {rsync_flags:?} {all_flags:?}");
                assert!(copy == *data, "{what}: {name} differs");
            }
        }
    }
}

#[test]
fn no_update_requires_a_current_index() {
    // `--no-update` applies to the index of the receiver: the client's in a
    // pull, and the server's in a push, whose hint reaches the client's
    // stderr marked as the server's, so that the user runs the update on
    // the right host (DESIGN.md §8).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let command = args(&["true"]);
        let mut invocation = transfer.invocation_in(direction, &["--no-update"], &refs(&command));
        let output = invocation.output().unwrap();
        assert_eq!(output.status.code(), Some(1), "{direction:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        let prefix = match direction {
            Direction::Pull => "hashsync: no index at ",
            Direction::Push => "hashsync (server): no index at ",
        };
        assert!(
            stderr
                .lines()
                .any(|line| line.starts_with(prefix) && line.ends_with("run `hashsync update`")),
            "{direction:?}: missing hint, or one from the wrong host:\n{stderr}"
        );

        let indexed = transfer.indexed(direction, "data");
        transfer.env.write(indexed, &common::tiled(8, 3));
        transfer.update_receiver(direction, &[]);
        let mut invocation = transfer.invocation_in(direction, &["--no-update"], &refs(&command));
        let output = invocation.output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "{direction:?}: read-only run failed:\n{stderr}"
        );
        assert!(
            !stderr.contains("updated index"),
            "{direction:?}: --no-update must not update:\n{stderr}"
        );
    }
}

#[test]
fn cache_failures_are_fatal_on_either_side() {
    // Receiver: an unwritable cache root fails the run at publication
    // (DESIGN.md §5.6 and §6.7). The tmp_data directory is in the same
    // root and opens earlier, so it exists already: else its creation
    // would fail first, and the run would end before the publication.
    let transfer = Transfer::new(&[]);
    let command = args(&["true"]);
    transfer.env.mkdir("client/cache/hashsync/tmp-data");
    transfer.env.chmod("client/cache/hashsync", 0o555);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot create ") && stderr.contains("/hashsync/local-hashlists:"),
        "receiver cache failure:\n{stderr}"
    );
    transfer.env.chmod("client/cache/hashsync", 0o755);

    // Sender: the same policy; its death fails the whole run, and its
    // message reaches the client's stderr through SSH. Its tmp_data
    // directory exists too.
    transfer.env.mkdir("server/home/.cache/hashsync/tmp-data");
    transfer.env.chmod("server/home/.cache/hashsync", 0o555);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot create ") && stderr.contains("/hashsync/remote-hashlists:"),
        "sender cache failure:\n{stderr}"
    );
    transfer.env.chmod("server/home/.cache/hashsync", 0o755);

    // Writable again: the run succeeds.
    transfer.run_ok(&refs(&command));

    // Publication creates its directory again when expiry removed it, but
    // a symlink to a directory that does not exist, in the place of the
    // directory, must fail the run and not make it try forever (DESIGN.md
    // §5.6).
    let dir = transfer.peer_dir("client", "local-hashlists").unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    std::os::unix::fs::symlink(transfer.env.path("missing"), &dir).unwrap();
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot create ") && stderr.contains("/hashsync/local-hashlists/"),
        "receiver cache failure:\n{stderr}"
    );
}

#[test]
fn the_command_ends_while_the_consumer_reads_nothing() {
    // The consumer reads nothing until the command has written all its
    // output. Every spool holds at most 16 MiB (DESIGN.md §6.5), and the
    // output, 32 MiB, is larger than one spool and all the buffers in
    // memory between the command and the consumer together: the scanner
    // looks one window ahead, and the data queue holds 4 MiB, about 6 MiB in
    // all, or 9 MiB where a pipe holds 1 MiB. So the command ends first
    // only because both spools hold the output (DESIGN.md §6.4 and §6.5):
    // the server's, and the client's, which keeps reading the data session
    // while stdout is blocked.
    let block = BLOCK.to_string();
    let transfer = Transfer::new(&[
        "--lookahead",
        &block,
        "--spool-limit",
        "16MiB",
        "--compression-level",
        "0",
    ]);
    let payload = random(32 << 20, 21);
    let path = transfer.env.path("payload");
    std::fs::write(&path, &payload).unwrap();
    let ended = transfer.env.path("ended");
    mkfifoat(CWD, &ended, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!(
        "cat '{}'; echo ended > '{}'",
        path.display(),
        ended.display()
    );
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&[&script])))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // The open waits for the command to open the FIFO, after its output.
    let mut message = String::new();
    File::open(&ended)
        .unwrap()
        .read_to_string(&mut message)
        .unwrap();
    assert_eq!(message, "ended\n");
    let output = succeeded(client.wait_with_output().unwrap());
    assert!(output.stdout == payload, "the output differs");
    assert_eq!(summary(&output), [payload.len(), payload.len(), 0, 0]);
}

#[test]
fn the_command_waits_when_both_spools_reach_their_limits() {
    // The consumer reads nothing at first, and each spool holds at most
    // 64 KiB. The command's output, 16 MiB, is larger than the two spools
    // and all the buffers in memory between the command and the consumer
    // (about 6 MiB, mostly the data queue, or 9 MiB where a pipe holds
    // 1 MiB), so the command cannot end before the consumer reads. If a
    // spool ignored its limit, it would take the rest of the output, and
    // the command would end (DESIGN.md §6.5). No event shows that the
    // command stays blocked, so the test waits a fixed time for its end.
    // On a slow machine, the wait can miss such a fault, but it cannot
    // fail a correct build. Then the consumer reads everything.
    let block = BLOCK.to_string();
    let transfer = Transfer::new(&[
        "--lookahead",
        &block,
        "--spool-limit",
        "64KiB",
        "--compression-level",
        "0",
    ]);
    let payload = random(16 << 20, 22);
    let path = transfer.env.path("payload");
    std::fs::write(&path, &payload).unwrap();
    let ended = transfer.env.path("ended");
    mkfifoat(CWD, &ended, Mode::RUSR | Mode::WUSR).unwrap();
    // Opened for reading without a wait for a writer, and kept open, so
    // that the command never waits to open the FIFO for writing.
    let flags = OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC;
    let fifo = File::from(rustix::fs::open(&ended, flags, Mode::empty()).unwrap());
    let script = format!(
        "cat '{}'; echo ended > '{}'",
        path.display(),
        ended.display()
    );
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&[&script])))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let client = KillOnDrop(client);
    let mut fds = [PollFd::new(&fifo, PollFlags::IN)];
    let wait = Timespec {
        tv_sec: 5,
        tv_nsec: 0,
    };
    let ready = retry_on_intr(|| poll(&mut fds, Some(&wait))).unwrap();
    assert_eq!(
        ready, 0,
        "the command ended while the consumer read nothing: a spool ignored its limit"
    );
    let output = succeeded(output_of(client));
    assert!(output.stdout == payload, "the output differs");
    assert_eq!(summary(&output), [payload.len(), payload.len(), 0, 0]);
    // The command wrote to the FIFO before it ended, and the client ended
    // after the command.
    let mut message = String::new();
    (&fifo).read_to_string(&mut message).unwrap();
    assert_eq!(message, "ended\n");
}

#[test]
fn stdin_reaches_the_command() {
    // Through `cat`, the client's stdin becomes the output, byte for byte,
    // and the command reads EOF after it, so `echo` follows: in eager
    // delivery, the default, and in batch delivery with `--batch-stdin`.
    // The forward stream is compressed as every stream is: the text takes
    // a small part of its size on the wire, and the noise its full size
    // (DESIGN.md §6.6), whether the stream is plain, in a pull, or
    // deduplicated, in a push.
    let text = "hashsync sends its stdin to the command\n".repeat(100_000);
    let noise = random(2 << 20, 31);
    let input = [text.as_bytes(), &noise].concat();
    for direction in DIRECTIONS {
        for (options, compressed) in [(&[][..], true), (&["--compression-level", "0"], false)] {
            let transfer = Transfer::new(options);
            transfer.configure_receiver(direction, "");
            let path = transfer.env.path("input");
            std::fs::write(&path, &input).unwrap();
            let command = args(&["cat; echo end"]);
            for flags in [&[EXACT][..], &[EXACT, "--batch-stdin"]] {
                let output = transfer
                    .invocation_in(direction, flags, &refs(&command))
                    .stdin(File::open(&path).unwrap())
                    .output()
                    .unwrap();
                let output = succeeded(output);
                let what = format!("{direction:?} {flags:?}");
                assert!(
                    output.stdout == [&input[..], b"end\n"].concat(),
                    "{what}: the output differs"
                );
                let sent = traffic(&output).sent;
                if compressed {
                    assert!(sent < noise.len() + (64 << 10), "{what}: {sent} bytes sent");
                } else {
                    assert!(sent > input.len(), "{what}: {sent} bytes sent");
                }
            }
        }
    }
}

#[test]
fn a_command_and_a_consumer_answer_each_other() {
    // As the two ends of `rsync -e` do, the command answers each line of
    // its stdin with a line of output, and the consumer writes its next
    // line only after it has read the answer. In eager delivery, both
    // streams reach their destinations at each idle point of their
    // sources, so neither end waits for bytes that a buffer of hashsync
    // holds (DESIGN.md §6.2), whichever streams are deduplicated. With
    // compression and without; a build that keeps such bytes hangs the
    // test.
    let deduplicated = [
        &[][..],
        &["--dedup-stdin"],
        &["--dedup-stdin", "--dedup-stdout"],
    ];
    for streams in deduplicated {
        for options in [&[][..], &["--compression-level", "0"]] {
            let transfer = Transfer::new(options);
            transfer.configure_server("");
            let script = "while read -r line; do echo \"got $line\"; done";
            let (stdin, mut producer) = std::io::pipe().unwrap();
            let flags = [&[EXACT][..], streams].concat();
            let client = transfer
                .invocation(&flags, &refs(&args(&[script])))
                .stdin(stdin)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let mut client = KillOnDrop(client);
            {
                let mut answers = BufReader::new(client.0.stdout.as_mut().unwrap());
                for line in ["one", "two", "three"] {
                    producer.write_all(format!("{line}\n").as_bytes()).unwrap();
                    let mut answer = String::new();
                    answers.read_line(&mut answer).unwrap();
                    let what = format!("{streams:?} {options:?}");
                    assert_eq!(answer, format!("got {line}\n"), "{what}");
                }
                assert!(answers.buffer().is_empty(), "output after the answers");
            }
            // The EOF of stdin ends the command, and so the run.
            drop(producer);
            let output = succeeded(output_of(client));
            assert!(output.stdout.is_empty(), "output after the answers");
        }
    }
}

#[test]
fn eager_delivery_sends_the_output_before_its_eof_at_once() {
    // The command writes a question, closes its stdout, and waits for the
    // answer on its stdin, which the consumer writes only after it has read
    // the question. The EOF of command stdout ends the backward stream, so
    // the question does not wait for END, which waits for the exit of the
    // command (DESIGN.md §6.6). With compression and without; a build that
    // keeps the question until END hangs the test.
    for options in [&[][..], &["--compression-level", "0"]] {
        let transfer = Transfer::new(options);
        let script = "printf question; exec >&-; read -r answer; test \"$answer\" = yes";
        let (stdin, mut producer) = std::io::pipe().unwrap();
        let client = transfer
            .invocation(&[EXACT], &refs(&args(&[script])))
            .stdin(stdin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut client = KillOnDrop(client);
        let mut question = [0u8; 8];
        let stdout = client.0.stdout.as_mut().unwrap();
        stdout.read_exact(&mut question).unwrap();
        assert_eq!(&question, b"question", "{options:?}");
        producer.write_all(b"yes\n").unwrap();
        drop(producer);
        let output = succeeded(output_of(client));
        assert!(output.stdout.is_empty(), "output after the question");
    }
}

#[test]
fn eager_delivery_sends_the_bytes_before_a_pause_at_once() {
    // The source of the stream writes the first 100 bytes of an indexed
    // file of two blocks, and waits until this test has read them from the
    // client's stdout, then writes the rest. In eager delivery, those 100
    // bytes leave at the idle point that the wait makes. The window of the
    // first block spans the idle point, so that block is literal, and only
    // the second one becomes a reference (DESIGN.md §6.3). A build that
    // keeps the bytes hangs the test.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let blocks = tiled(2, 36);
        transfer
            .env
            .write(transfer.indexed(direction, "blocks"), &blocks);
        transfer.env.write("head", &blocks[..100]);
        transfer.env.write("rest", &blocks[100..]);
        let go = transfer.env.path("go");
        mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
        // `cat` writes each file in one call, so the rest has no idle point.
        let script = format!(
            "cat '{}'; read go <'{}'; cat '{}'",
            transfer.env.path("head").display(),
            go.display(),
            transfer.env.path("rest").display()
        );
        let mut stream = transfer.spawn_stream(direction, &[], &script);
        let mut first = vec![0u8; 100];
        stream.stdout().read_exact(&mut first).unwrap();
        assert_eq!(first, blocks[..100]);
        std::fs::write(&go, b"go\n").unwrap();
        let output = succeeded(stream.output());
        assert!(output.stdout == blocks[100..], "the output differs");
        let counts = [blocks.len(), BLOCK, BLOCK, 0];
        assert_eq!(summary(&output), counts, "{direction:?}");
    }
}

#[test]
fn eager_delivery_keeps_a_block_that_spans_a_pause_while_the_session_is_full() {
    // As in the test above, but the client reads none of its stdout until
    // the command has ended, and its spool holds at most 64 KiB, so noise
    // first fills the data session, as a slow link does (DESIGN.md §6.5,
    // `SESSION_NOISE`). The server's data writer then waits on the
    // session, so the tail before the idle point waits for it too, and the
    // rest of the file fills the window of the first block: both blocks
    // become references (DESIGN.md §6.3). The pause makes the idle point,
    // but its length decides nothing for a correct build: the session
    // stays full until the command has ended.
    let transfer = Transfer::new(&["--spool-limit", "64KiB", "--compression-level", "0"]);
    let blocks = tiled(2, 38);
    let noise = random(SESSION_NOISE, 39);
    transfer.env.write("tree/blocks", &blocks);
    transfer.env.write("noise", &noise);
    transfer.env.write("head", &blocks[..100]);
    transfer.env.write("rest", &blocks[100..]);
    let ended = transfer.env.path("ended");
    mkfifoat(CWD, &ended, Mode::RUSR | Mode::WUSR).unwrap();
    let path = |name: &str| transfer.env.path(name).display().to_string();
    let script = format!(
        "cat '{}' '{}'; sleep 0.3; cat '{}'; echo ended >'{}'",
        path("noise"),
        path("head"),
        path("rest"),
        ended.display()
    );
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&[&script])))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let client = KillOnDrop(client);
    // The open waits for the command to open the FIFO, after its output.
    let mut message = String::new();
    File::open(&ended)
        .unwrap()
        .read_to_string(&mut message)
        .unwrap();
    assert_eq!(message, "ended\n");
    let output = succeeded(output_of(client));
    assert!(
        output.stdout == [&noise[..], &blocks].concat(),
        "the output differs"
    );
    let expected = [noise.len() + blocks.len(), noise.len(), blocks.len(), 0];
    assert_eq!(summary(&output), expected);
}

#[test]
fn a_tail_that_waits_for_a_busy_data_writer_leaves_once_the_writer_is_idle() {
    // The command fills the data session with noise while the client
    // reads none of its stdout, and its spool holds at most 64 KiB
    // (`SESSION_NOISE`). Then the command writes fewer bytes than a window,
    // and waits until this test has read them. The idle point after those
    // bytes comes while the data writer waits on the full session, so their
    // tail waits for it. Once this test reads stdout, the data writer
    // writes the rest and becomes idle, which wakes the scanner: the bytes
    // leave, although no more output comes (DESIGN.md §6.3). A server that
    // is not woken then hangs the test.
    let transfer = Transfer::new(&["--spool-limit", "64KiB", "--compression-level", "0"]);
    let noise = random(SESSION_NOISE, 40);
    transfer.env.write("noise", &noise);
    let (written, go) = (transfer.env.path("written"), transfer.env.path("go"));
    for fifo in [&written, &go] {
        mkfifoat(CWD, fifo, Mode::RUSR | Mode::WUSR).unwrap();
    }
    let script = format!(
        "cat '{}'; printf partial; echo written >'{}'; read go <'{}'",
        transfer.env.path("noise").display(),
        written.display(),
        go.display()
    );
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&[&script])))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut client = KillOnDrop(client);
    // The open waits for the command to open the FIFO, after its output.
    let mut message = String::new();
    File::open(&written)
        .unwrap()
        .read_to_string(&mut message)
        .unwrap();
    assert_eq!(message, "written\n");
    let mut received = vec![0u8; noise.len() + 7];
    let stdout = client.0.stdout.as_mut().unwrap();
    stdout.read_exact(&mut received).unwrap();
    assert!(
        received == [&noise[..], b"partial"].concat(),
        "the output differs"
    );
    std::fs::write(&go, b"go\n").unwrap();
    let output = succeeded(output_of(client));
    assert!(output.stdout.is_empty(), "output after the bytes");
}

#[test]
fn batch_delivery_keeps_a_block_that_spans_a_pause() {
    // The same source as above, with batch delivery, and a pause of its
    // own: no idle point sends the first 100 bytes on, so both blocks
    // become references (DESIGN.md §6.3). The length of the pause decides
    // nothing for a correct build; a slow machine can only make the test
    // miss a build that flushes at idle points.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let blocks = tiled(2, 37);
        transfer
            .env
            .write(transfer.indexed(direction, "blocks"), &blocks);
        transfer.env.write("head", &blocks[..100]);
        transfer.env.write("rest", &blocks[100..]);
        let script = format!(
            "cat '{}'; sleep 0.3; cat '{}'",
            transfer.env.path("head").display(),
            transfer.env.path("rest").display()
        );
        let output = succeeded(transfer.run_stream(direction, &[direction.batch()], &script));
        assert_eq!(output.stdout, blocks);
        let counts = [blocks.len(), 0, blocks.len(), 0];
        assert_eq!(summary(&output), counts, "{direction:?}");
    }
}

#[test]
fn the_run_ends_with_the_command_while_stdin_stays_open() {
    // The client's stdin stays open, as a terminal does, but the run ends
    // with the command: the client stops reading its stdin at STOP or END
    // (DESIGN.md §6.6). A command that reads a part of its stdin gets it at
    // once, also with compression: the forward stream is flushed when
    // stdin has no more bytes ready. A command that fails to start ends
    // the run in the same way. The forward stream is plain in a pull, and
    // deduplicated in a push.
    for direction in DIRECTIONS {
        for options in [&[][..], &["--compression-level", "0"]] {
            let transfer = Transfer::new(options);
            transfer.configure_receiver(direction, "");
            let missing = "/nonexistent-hashsync-test-command";
            // Without a shell, `-s`, the command fails to start.
            for (flags, command, status, expected) in [
                (&[EXACT][..], &["true"][..], 0, &b""[..]),
                (&[EXACT], &["head", "-c", "5"], 0, b"hello"),
                (&[EXACT, "-s"], &[missing], 127, b""),
            ] {
                let (stdin, mut producer) = std::io::pipe().unwrap();
                producer.write_all(b"hello, and more").unwrap();
                let output = transfer
                    .invocation_in(direction, flags, &refs(&args(command)))
                    .stdin(stdin)
                    .output()
                    .unwrap();
                let stderr = String::from_utf8_lossy(&output.stderr);
                let what = format!("{direction:?} {options:?} {command:?}");
                assert_eq!(output.status.code(), Some(status), "{what}:\n{stderr}");
                assert_eq!(output.stdout, expected, "{what}");
                drop(producer);
            }
        }
    }
}

#[test]
fn a_descendant_that_reads_stdin_reads_eof_when_the_command_exits() {
    // The command starts a descendant that reads the command's stdin and
    // holds its stdout, and exits at once, while the client's stdin stays
    // open. As under SSH, the descendant reads EOF once the command has
    // exited: the server stops the forward stream at the exit, and does
    // not wait for the EOF of stdout, which the descendant holds until its
    // stdin ends (DESIGN.md §6.6). A server that stopped the stream only
    // after that EOF would hang the test. The descendant copies what it
    // read before the stop: a part of what the producer wrote.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let (stdin, mut producer) = std::io::pipe().unwrap();
        producer.write_all(b"hello, and more").unwrap();
        let script = "exec 3<&0; cat <&3 & exec 3<&-; exit 0";
        let output = transfer
            .invocation_in(direction, &[EXACT], &refs(&args(&[script])))
            .stdin(stdin)
            .output()
            .unwrap();
        let output = succeeded(output);
        assert!(
            b"hello, and more".starts_with(&output.stdout),
            "{direction:?}: {:?}",
            String::from_utf8_lossy(&output.stdout)
        );
        drop(producer);
    }
}

#[test]
fn a_stdin_that_is_not_a_terminal_is_non_blocking_while_it_is_read() {
    // As ssh does, the client makes a stdin that is not a terminal
    // non-blocking while its stdin reader reads it, and restores its flags
    // when the reader stops: here at the EOF of stdin, while the command
    // still runs (DESIGN.md §6.6). The test keeps a copy of the read end of
    // the stdin pipe, which shares the flags, and waits for each change
    // without a deadline: a client that does not make it hangs the test.
    let transfer = Transfer::new(&[]);
    let go = transfer.env.path("go");
    mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!("cat; read go <'{}'", go.display());
    let (stdin, mut producer) = std::io::pipe().unwrap();
    let shared = stdin.try_clone().unwrap();
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&[&script])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let client = KillOnDrop(client);
    wait_for_non_blocking(&shared, true);
    producer.write_all(b"input\n").unwrap();
    drop(producer);
    wait_for_non_blocking(&shared, false);
    std::fs::write(&go, b"go\n").unwrap();
    let output = succeeded(output_of(client));
    assert_eq!(output.stdout, b"input\n");
}

#[test]
fn a_fatal_end_restores_the_flags_of_stdin() {
    // The flags of stdin go back to what they were also when the run ends
    // fatally while the stdin reader reads stdin: here at the SIGTERM of
    // the client, which ends the run with `128 + signal` (DESIGN.md §6.8
    // and §6.9). Nothing else ends the stdin reader: stdin stays open, and
    // the command, `cat`, goes on reading its own stdin.
    let transfer = Transfer::new(&[]);
    let (stdin, producer) = std::io::pipe().unwrap();
    let shared = stdin.try_clone().unwrap();
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&["cat"])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let client = KillOnDrop(client);
    wait_for_non_blocking(&shared, true);
    signal(client.0.id(), Signal::TERM);
    let output = output_of(client);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(143), "{stderr}");
    let flags = rustix::fs::fcntl_getfl(&shared).unwrap();
    assert!(
        !flags.contains(OFlags::NONBLOCK),
        "stdin stayed non-blocking"
    );
    drop(producer);
}

#[test]
fn a_terminal_stdin_stays_blocking() {
    // As ssh does, the client leaves a stdin that is a terminal as it is:
    // the shell of the user shares it, and must not find it non-blocking
    // (DESIGN.md §6.6). Here stdin is the slave of a new
    // pseudo-terminal, which holds a typed line before the run starts.
    // Only the stdin reader reads the terminal, so once the line is gone,
    // the stdin reader has read it, and the flags of the terminal must
    // still be as they were. The output of `cat`, the command, would show
    // that only later, after the line has crossed both sessions. Ctrl-D
    // then ends stdin.
    let master = match openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY | OpenptFlags::CLOEXEC) {
        Ok(master) => master,
        Err(e) => {
            eprintln!("skipped: no pseudo-terminals: {e}");
            return;
        }
    };
    grantpt(&master).unwrap();
    unlockpt(&master).unwrap();
    let path = ptsname(&master, Vec::new()).unwrap();
    let slave = File::options()
        .read(true)
        .write(true)
        .custom_flags(OFlags::NOCTTY.bits() as i32)
        .open(OsStr::from_bytes(path.as_bytes()))
        .unwrap();
    let shared = slave.try_clone().unwrap();
    let mut master = File::from(master);
    master.write_all(b"typed\n").unwrap();
    // The line discipline takes the line in on its own time.
    wait_for_input(&shared, 6);
    let transfer = Transfer::new(&[]);
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&["cat"])))
        .stdin(slave)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let client = KillOnDrop(client);
    wait_for_input(&shared, 0);
    let flags = rustix::fs::fcntl_getfl(&shared).unwrap();
    assert!(
        !flags.contains(OFlags::NONBLOCK),
        "the terminal is non-blocking"
    );
    // Ctrl-D: the EOF of a terminal in canonical mode.
    master.write_all(b"\x04").unwrap();
    let output = succeeded(output_of(client));
    assert_eq!(output.stdout, b"typed\n");
}

/// Wait until the terminal `fd` holds `bytes` bytes of whole lines in its
/// input, polling with no deadline.
fn wait_for_input(fd: impl AsFd, bytes: u64) {
    while rustix::io::ioctl_fionread(&fd).unwrap() != bytes {
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn with_n_the_command_reads_an_empty_stdin() {
    // With `-n`, the command reads EOF at once, as with `ssh -n`, and the
    // run reads nothing from its own stdin: the bytes in it stay for the
    // next reader. The empty forward stream is whole: an empty zstd frame
    // (DESIGN.md §8).
    let transfer = Transfer::new(&[]);
    let (stdin, mut producer) = std::io::pipe().unwrap();
    let mut unread = stdin.try_clone().unwrap();
    producer.write_all(b"not for the command").unwrap();
    drop(producer);
    let command = args(&["cat; echo end"]);
    let output = transfer
        .invocation(&[EXACT, "-n"], &refs(&command))
        .stdin(stdin)
        .output()
        .unwrap();
    let output = succeeded(output);
    assert_eq!(output.stdout, b"end\n");
    assert_eq!(
        summary_line(&output, "hashsync: stdin"),
        "hashsync: stdin -- 0 B --> 13 B wire (compressed)"
    );
    let mut left = Vec::new();
    unread.read_to_end(&mut left).unwrap();
    assert_eq!(left, b"not for the command");
}

#[test]
fn a_command_that_closes_its_stdin_stops_the_forward_stream() {
    // The command closes its stdin, and runs on until this test ends it.
    // No process reads the forward stream any more, so the server's first
    // write of it fails with EPIPE, and the server stops the stream: STOP
    // makes the client stop it too, while the command still runs
    // (DESIGN.md §6.6). The end of the client's stdin reader shows that
    // stop: the stdin reader is the only reader of the client's stdin, so
    // from then on the producer of stdin can write only what the pipe
    // holds. The test waits for that event, and for no fixed time
    // (DESIGN.md §11). A build that goes on reading stdin, or stops the
    // stream only when the command ends, hangs the test.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let (closed, go) = (transfer.env.path("closed"), transfer.env.path("go"));
        for fifo in [&closed, &go] {
            mkfifoat(CWD, fifo, Mode::RUSR | Mode::WUSR).unwrap();
        }
        let script = format!(
            "exec <&-; echo closed >'{}'; read go <'{}'",
            closed.display(),
            go.display()
        );
        let (stdin, mut producer) = std::io::pipe().unwrap();
        let client = transfer
            .invocation_in(direction, &[EXACT], &refs(&args(&[&script])))
            .stdin(stdin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let client = KillOnDrop(client);
        // The open waits for the command to open the FIFO, after it closed
        // its stdin.
        let mut message = String::new();
        File::open(&closed)
            .unwrap()
            .read_to_string(&mut message)
            .unwrap();
        assert_eq!(message, "closed\n");
        // The client starts its stdin reader after CACHE_RESULT. Only a
        // stop or the EOF of stdin ends it, and neither can come before the
        // bytes that this test writes.
        let pid = client.0.id();
        wait_for_thread(pid, "stdin-reader", true);
        producer.write_all(&random(64 << 10, 35)).unwrap();
        wait_for_thread(pid, "stdin-reader", false);
        std::fs::write(&go, b"go\n").unwrap();
        let output = succeeded(output_of(client));
        assert!(output.stdout.is_empty(), "{direction:?}");
    }
}

#[test]
fn a_command_that_reads_a_part_of_its_input_ends_the_run() {
    // `head` reads the first 100 bytes of 16 MiB of stdin, and exits. Then
    // no process reads the command's stdin, so a write of the server's
    // decoder fails with EPIPE, and the server stops the forward stream:
    // STOP makes the client stop reading its stdin, and the server's data
    // reader discards what is still on its way (DESIGN.md §6.6). With and
    // without compression.
    for direction in DIRECTIONS {
        for options in [
            &["--spool-limit", "64KiB"][..],
            &["--spool-limit", "64KiB", "--compression-level", "0"],
        ] {
            let transfer = Transfer::new(options);
            transfer.configure_receiver(direction, "");
            let input = random(16 << 20, 34);
            let path = transfer.env.path("input");
            std::fs::write(&path, &input).unwrap();
            let output = transfer
                .invocation_in(direction, &[EXACT], &refs(&args(&["head", "-c", "100"])))
                .stdin(File::open(&path).unwrap())
                .output()
                .unwrap();
            let output = succeeded(output);
            assert_eq!(output.stdout, input[..100], "{direction:?} {options:?}");
        }
    }
}

#[test]
fn a_stop_ends_a_forward_stream_whose_stdin_stays_open() {
    // The client's stdin carries 16 MiB, and then stays open, so only the
    // stop that follows the exit of `head` can end the forward stream
    // (DESIGN.md §6.6). A build that does not stop the stream hangs the
    // test. In a push, the stream is deduplicated, in two ways here:
    // - zeros become runs of one block each, since the lookahead is one
    //   window, and the stop comes between them. A stop inside one RLE
    //   frame, which the server's decoder writes in pieces, is the test
    //   `a_stop_ends_the_writes_of_one_run_that_stands_for_terabytes` in
    //   tests/serve.rs;
    // - the blocks of an indexed file need answers, which the scanner, with
    //   a lookahead of one window, waits for one at a time. The server
    //   sends no answer after the stop; the test
    //   `end_stops_the_forward_stream_when_it_comes_before_stop` makes the
    //   scanner wait for one at the stop (DESIGN.md §6.3).
    let block = BLOCK.to_string();
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&["--spool-limit", "64KiB", "--lookahead", &block]);
        transfer.configure_receiver(direction, "");
        let blocks = tiled(64, 41);
        transfer
            .env
            .write(transfer.indexed(direction, "blocks"), &blocks);
        for input in [vec![0; 16 << 20], blocks.repeat(64)] {
            let (stdin, mut producer) = std::io::pipe().unwrap();
            // The writes fail once the client has ended, which closes the
            // last read end of the pipe.
            let produced = std::thread::spawn(move || {
                let _ = producer.write_all(&input);
                producer
            });
            let output = transfer
                .invocation_in(direction, &[EXACT], &refs(&args(&["head", "-c", "100"])))
                .stdin(stdin)
                .output()
                .unwrap();
            let output = succeeded(output);
            assert_eq!(output.stdout.len(), 100, "{direction:?}");
            drop(produced.join().unwrap());
        }
    }
}

#[test]
fn a_deduplicated_stdin_that_never_pauses_reaches_the_command() {
    // `/dev/zero` never pauses, so the stdin reader marks no idle point, and
    // in batch delivery none comes at all. Then only the scanner's flush
    // after every `lookahead` bytes sends the stream on: its runs and
    // references stand for far more bytes than their frames, and would
    // otherwise stay in the zstd job and the CHUNK buffer of the client's
    // data writer until the end of the stream, which never comes, while
    // `head` waits for its first byte (DESIGN.md §6.3). The indexed blocks
    // come from a pipe that stays open after them, in batch delivery. A
    // build without the flush hangs the test.
    let transfer = Transfer::new(&["--spool-limit", "64KiB", "--lookahead", "1MiB"]);
    transfer.configure_server("");
    let blocks = tiled(64, 45);
    transfer.env.write("server-tree/blocks", &blocks);
    for (flags, from_blocks) in [
        (&[][..], false),
        (&["--batch-stdin"][..], false),
        (&["--batch-stdin"][..], true),
    ] {
        let mut invocation = transfer.push(flags, &refs(&args(&["head", "-c", "100"])));
        let (expected, produced) = if from_blocks {
            let (stdin, mut producer) = std::io::pipe().unwrap();
            invocation.stdin(stdin);
            let input = blocks.repeat(64);
            // The writes fail once the client has ended and the invocation
            // is dropped, which closes the last read end of the pipe.
            let produced = std::thread::spawn(move || {
                let _ = producer.write_all(&input);
                producer
            });
            (blocks[..100].to_vec(), Some(produced))
        } else {
            invocation.stdin(File::open("/dev/zero").unwrap());
            (vec![0; 100], None)
        };
        let output = succeeded(invocation.output().unwrap());
        drop(invocation);
        assert_eq!(output.stdout, expected, "{flags:?}, blocks: {from_blocks}");
        if let Some(produced) = produced {
            drop(produced.join().unwrap());
        }
    }
}

#[test]
fn a_deduplicated_command_output_that_never_pauses_reaches_stdout() {
    // In batch delivery, no idle point comes, and the command writes indexed
    // blocks until its stdout closes, so the stream does not end either.
    // Then only the scanner's flush after every `lookahead` bytes sends its
    // references on (DESIGN.md §6.3): a build without it hangs the test,
    // with `head -c 10` waiting for its first byte of the client's stdout.
    // Once `head` has exited, the client's next write to its stdout fails
    // with `EPIPE`, which stops the backward stream, and the command's next
    // `cat` dies of SIGPIPE, whose status ends the run (DESIGN.md §6.6).
    let transfer = Transfer::new(&["--spool-limit", "64KiB", "--lookahead", "1MiB", BATCH]);
    let blocks = tiled(64, 46);
    transfer.env.write("tree/blocks", &blocks);
    let script = format!(
        "while :; do cat '{}' || exit; done",
        transfer.env.path("tree/blocks").display()
    );
    let invocation = transfer.invocation(&[], &refs(&args(&[&script])));
    let mut run = HeadOfStdout::spawn(&invocation, transfer.env.path("."));
    assert_eq!(run.wait_for_head(), blocks[..10]);
    let output = run.output();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(128 + libc::SIGPIPE), "{stderr}");
}

#[test]
fn a_stopped_forward_stream_sends_nothing_more() {
    // The command holds its stdin without reading it until this test lets
    // it exit, and each spool holds one read at most. zstd's workers take
    // the 8 MiB of stdin without output, since a job of theirs holds 16 MiB
    // at the default level, so the CHUNKs go out only once the client's
    // forward data writer ends the zstd frame. The server's decoder and data
    // reader soon wait, and so does that writer, in a write to the data
    // session. Then the command exits, and the stop must end the writer at
    // once: only the CHUNK on its way still goes out, not the rest of the
    // frame (DESIGN.md §6.6). The pipes, the server's buffers and that CHUNK
    // hold less than 4.5 MiB, also where a pipe holds 1 MiB (pages of 64
    // KiB); a writer that went on would send all 8 MiB. A data writer that
    // takes the frames that a push's data queue holds at the stop is the
    // unit test `the_data_writer_takes_no_queued_frame_after_the_stop`'s: in
    // a whole run, no event shows that the queue is full.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&["--spool-limit", "0"]);
        transfer.configure_receiver(direction, "");
        let input = transfer.env.path("input");
        std::fs::write(&input, random(8 << 20, 46)).unwrap();
        let go = transfer.env.path("go");
        mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
        let script = format!("read go <'{}'", go.display());
        let client = transfer
            .invocation_in(direction, &[EXACT], &refs(&args(&[&script])))
            .stdin(File::open(&input).unwrap())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let client = KillOnDrop(client);
        // No deadline: a writer that never waits hangs the test.
        wait_until_asleep_again_in(client.0.id(), "data-writer", libc::SYS_writev as u64);
        std::fs::write(&go, b"go\n").unwrap();
        let output = succeeded(output_of(client));
        let sent = traffic(&output).sent;
        assert!(
            sent < 9 << 19,
            "{direction:?}: {sent} bytes sent, though the stream stopped"
        );
    }
}

#[test]
fn the_forward_stream_waits_in_the_server_spool_until_the_command_reads_it() {
    // The spools have no limit. The command reads its stdin only after all
    // of it, 16 MiB, has passed the wrapper of the data session, which
    // copies what the client sends: the stream waits in the server's spool
    // (DESIGN.md §6.6), plain in a pull, deduplicated in a push. A server
    // that stopped reading the data session while the command does not read
    // would hang the test.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&["--spool-limit", "unlimited", "--compression-level", "0"]);
        transfer.configure_receiver(direction, "");
        let captured = transfer.env.path("captured");
        wrap_ssh(
            &transfer,
            &format!(
                r#"if [ $session = data ]; then
    tee '{}' | "$ssh" "$@"
    exit $?
fi
exec "$ssh" "$@""#,
                captured.display()
            ),
        );
        let input = random(16 << 20, 33);
        let fifo = transfer.env.path("go");
        mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
        let script = format!("read go <'{}'; exec cat", fifo.display());
        let (stdin, mut producer) = std::io::pipe().unwrap();
        let client = transfer
            .invocation_in(direction, &[EXACT], &refs(&args(&[&script])))
            .stdin(stdin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let client = KillOnDrop(client);
        producer.write_all(&input).unwrap();
        drop(producer);
        // No deadline: if the stream does not reach the server, the test
        // hangs.
        while std::fs::metadata(&captured).map_or(0, |meta| meta.len()) < input.len() as u64 {
            std::thread::sleep(Duration::from_millis(10));
        }
        std::fs::write(&fifo, b"go\n").unwrap();
        let output = succeeded(output_of(client));
        assert!(output.stdout == input, "the output differs");
    }
}

#[test]
fn the_producer_waits_when_both_spools_of_the_forward_stream_reach_their_limits() {
    // Each spool of the forward stream holds at most 64 KiB, and the
    // command reads its stdin only after a signal. The producer of the
    // client's stdin writes 32 MiB, more than both spools and all the
    // buffers in memory between the producer and the command, so it cannot
    // finish before the command reads. Those buffers hold about 1 MiB when
    // the forward stream is plain, in a pull, and 4 MiB where a pipe holds
    // 1 MiB (pages of 64 KiB). When it is deduplicated, in a push, the
    // scanner's data queue adds up to 4 MiB of frames, and the lookahead of
    // one block keeps the scanner itself from holding more (DESIGN.md
    // §6.3): 5 MiB, or 8.3 MiB with pipes of 1 MiB. If a spool ignored its
    // limit, it would take the stream, and the producer would write all
    // 32 MiB (DESIGN.md §6.6). No
    // event shows that the producer stays blocked, so the test waits a fixed
    // time for its writes. On a slow machine, the wait can miss such a
    // fault, but it cannot fail a correct build. Then the command reads
    // everything.
    for direction in DIRECTIONS {
        let block = BLOCK.to_string();
        let options = [
            "--spool-limit",
            "64KiB",
            "--compression-level",
            "0",
            "--lookahead",
            &block,
        ];
        let transfer = Transfer::new(&options);
        transfer.configure_receiver(direction, "");
        let input = random(32 << 20, 32);
        let fifo = transfer.env.path("go");
        mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
        let script = format!("read go <'{}'; exec cat", fifo.display());
        let (stdin, mut producer) = std::io::pipe().unwrap();
        let client = transfer
            .invocation_in(direction, &[EXACT], &refs(&args(&[&script])))
            .stdin(stdin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let client = KillOnDrop(client);
        let written = Arc::new(AtomicUsize::new(0));
        let produced = {
            let (written, input) = (Arc::clone(&written), input.clone());
            std::thread::spawn(move || {
                for chunk in input.chunks(64 << 10) {
                    producer.write_all(chunk).unwrap();
                    written.fetch_add(chunk.len(), SeqCst);
                }
            })
        };
        std::thread::sleep(Duration::from_secs(3));
        let so_far = written.load(SeqCst);
        assert!(
            so_far < 16 << 20,
            "the producer wrote {so_far} bytes while the command read nothing: \
         a spool ignored its limit"
        );
        std::fs::write(&fifo, b"go\n").unwrap();
        let output = succeeded(output_of(client));
        produced.join().unwrap();
        assert!(output.stdout == input, "the output differs");
    }
}

#[test]
fn no_sender_cache_stores_nothing_and_removes_earlier_lists() {
    for direction in DIRECTIONS {
        // First populate the sender cache with caching enabled.
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let data = transfer.indexed(direction, "data");
        transfer.env.write(data, &common::tiled(8, 5));
        succeeded(transfer.run_stream(direction, &[], "true"));
        let sender = transfer.cache_files(direction.sender(), "remote-hashlists");
        assert_eq!(sender.len(), 1, "{direction:?}");

        // Then disable it. The sender advertises no list, so the receiver
        // sends the full list; neither side stores a list for this peer and
        // index, and both remove their earlier lists. Expiry then removes
        // the empty directories (DESIGN.md §5.6).
        let output = succeeded(transfer.run_stream(direction, &["--no-sender-cache"], "true"));
        let (full, delta) = list_bytes(&output);
        assert!(
            full.is_some() && delta.is_none(),
            "full {full:?}, delta {delta:?}"
        );
        let sender = transfer.peer_dir(direction.sender(), "remote-hashlists");
        assert_eq!(sender, None, "{direction:?}");
        let receiver = transfer.peer_dir(direction.receiver(), "local-hashlists");
        assert_eq!(receiver, None, "{direction:?}");
    }
}

#[test]
fn each_side_keeps_only_its_latest_list() {
    // Each side lists its directory for the peer and index when its part
    // of the exchange starts, and at the end removes the listed files
    // except its target: the list that the new target replaces, and the
    // list and the temporary that failed runs left. Expiry removes the
    // empty directories of other peers and indexes (DESIGN.md §5.6).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let receiver = (direction.receiver(), "local-hashlists");
        let sender = (direction.sender(), "remote-hashlists");
        let data = transfer.indexed(direction, "data");
        transfer.env.write(data, &common::tiled(8, 5));
        succeeded(transfer.run_stream(direction, &[], "true"));
        let first = transfer.cache_files(receiver.0, receiver.1);
        assert_eq!(first.len(), 1, "{direction:?}: receiver cache: {first:?}");
        assert_eq!(transfer.cache_files(sender.0, sender.1), first);

        // A list and a temporary that failed runs left, and an empty
        // directory, on each side.
        let leftover = format!("blake3-{}.cache", "0".repeat(64));
        let temporary = ".hashsync-0123456789abcdef";
        for (side, tree) in [receiver, sender] {
            let dir = transfer.peer_dir(side, tree).unwrap();
            std::fs::write(dir.join(&leftover), b"").unwrap();
            std::fs::write(dir.join(temporary), b"").unwrap();
            std::fs::create_dir(dir.with_file_name("empty")).unwrap();
        }

        // A changed index has a new target.
        let more = transfer.indexed(direction, "more");
        transfer.env.write(more, &common::tiled(8, 6));
        succeeded(transfer.run_stream(direction, &[], "true"));
        let second = transfer.cache_files(receiver.0, receiver.1);
        assert_eq!(second.len(), 1, "{direction:?}: receiver cache: {second:?}");
        assert!(second[0].ends_with(".cache") && !first.contains(&second[0]));
        // `cache_files` also checks that the empty directory is gone.
        assert_eq!(transfer.cache_files(sender.0, sender.1), second);
    }
}

/// The full-list and delta bytes that a run with `EXACT` amounts sent, from
/// its summary, which shows only the lists that went out.
fn list_bytes(output: &Output) -> (Option<usize>, Option<usize>) {
    let line = deduplicated(output);
    (line.hashlist, line.delta)
}

/// The script `cat` for the file `name` in the receiver's root.
fn cat(transfer: &Transfer, direction: Direction, name: &str) -> String {
    let path = transfer.env.path(transfer.indexed(direction, name));
    format!("cat '{}'", path.display())
}

/// Run `cat` on the file `name` in the receiver's root, through the
/// deduplicated stream of `direction`, in batch delivery, and check the
/// run with `all_references`. Returns the list bytes that the run
/// carried.
fn cat_references(
    transfer: &Transfer,
    direction: Direction,
    name: &str,
) -> (Option<usize>, Option<usize>) {
    let script = cat(transfer, direction, name);
    let output = transfer.run_stream(direction, &[direction.batch()], &script);
    all_references(transfer, direction, name, output)
}

/// Require that `output` comes from a successful run whose stdout is the
/// file `name` of the receiver's root, with every block sent as a
/// reference. That only works if the sender built its table from the
/// whole target. Returns the list bytes that the run carried.
fn all_references(
    transfer: &Transfer,
    direction: Direction,
    name: &str,
    output: Output,
) -> (Option<usize>, Option<usize>) {
    let output = succeeded(output);
    let expected = std::fs::read(transfer.env.path(transfer.indexed(direction, name))).unwrap();
    assert!(
        output.stdout == expected,
        "{direction:?}: the output differs"
    );
    assert_eq!(
        summary(&output)[2],
        expected.len(),
        "{direction:?}: every block is a reference"
    );
    list_bytes(&output)
}

/// The one list that each side keeps. Both sides must keep the same list.
fn kept_list(transfer: &Transfer, direction: Direction) -> String {
    let kept = transfer.cache_files(direction.receiver(), "local-hashlists");
    assert_eq!(kept.len(), 1, "{direction:?}: receiver cache: {kept:?}");
    let sender = transfer.cache_files(direction.sender(), "remote-hashlists");
    assert_eq!(sender, kept, "{direction:?}");
    kept.into_iter().next().unwrap()
}

#[test]
fn an_unchanged_list_is_kept_and_a_changed_one_goes_as_a_delta() {
    // The first run sends the full list. With the index unchanged, the next
    // run sends no list bytes at all, because the sender already has the
    // target, and its summary line shows no list; neither side writes that
    // list again, both keep their files. After a change, a run sends the
    // delta from the list that both sides keep, which is smaller. However
    // the sender gets the target, it builds its table from it, so the
    // blocks of the files become references (DESIGN.md §5.6 and §8).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        transfer
            .env
            .write(transfer.indexed(direction, "first"), &tiled(64, 7));
        let (full, delta) = cat_references(&transfer, direction, "first");
        assert_eq!(delta, None);
        let full = full.expect("no full list");
        let first_list = kept_list(&transfer, direction);
        let inodes = || {
            let receiver = (direction.receiver(), "local-hashlists");
            let sender = (direction.sender(), "remote-hashlists");
            [receiver, sender].map(|(side, tree)| {
                let dir = transfer.peer_dir(side, tree).unwrap();
                std::fs::metadata(dir.join(&first_list)).unwrap().ino()
            })
        };
        let written = inodes();

        assert_eq!(cat_references(&transfer, direction, "first"), (None, None));
        assert_eq!(kept_list(&transfer, direction), first_list);
        assert_eq!(
            inodes(),
            written,
            "{direction:?}: an unchanged list was written again"
        );

        transfer
            .env
            .write(transfer.indexed(direction, "second"), &tiled(4, 9));
        let (none, delta) = cat_references(&transfer, direction, "second");
        assert_eq!(none, None);
        let delta = delta.expect("no delta");
        assert!(delta < full, "delta {delta}, full {full}");
        assert_ne!(kept_list(&transfer, direction), first_list);
    }
}

#[test]
fn a_base_from_the_directory_of_another_peer_serves() {
    // Lists are named by their content, so the receiver looks for a base in
    // the directories of all its peers and indexes (DESIGN.md §5.6). Here
    // the receiver's copy of the list that the sender keeps sits in another
    // peer's directory. The run sends a delta from it and leaves it there,
    // renewed.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        transfer
            .env
            .write(transfer.indexed(direction, "first"), &tiled(64, 7));
        cat_references(&transfer, direction, "first");
        let base = kept_list(&transfer, direction);
        let own = transfer
            .peer_dir(direction.receiver(), "local-hashlists")
            .unwrap();
        let other = own.with_file_name(format!("{}-{}", "ab".repeat(16), "cd".repeat(16)));
        std::fs::rename(&own, &other).unwrap();
        let ctime = |path: &std::path::Path| {
            let meta = std::fs::metadata(path).unwrap();
            (meta.ctime(), meta.ctime_nsec())
        };
        let before = ctime(&other.join(&base));
        // Wait, with no deadline, until the filesystem clock has moved past
        // `before`, so that the renewal shows in the ctime: otherwise the
        // next run could renew the base within the same timestamp tick. The
        // probe is on the same filesystem as the cache.
        let probe = transfer.env.path("probe");
        loop {
            std::fs::write(&probe, b"").unwrap();
            if ctime(&probe) > before {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        transfer
            .env
            .write(transfer.indexed(direction, "second"), &tiled(4, 9));
        let (full, delta) = cat_references(&transfer, direction, "second");
        assert!(
            full.is_none() && delta.is_some(),
            "full {full:?}, delta {delta:?}"
        );
        assert!(
            ctime(&other.join(&base)) > before,
            "{direction:?}: the base must stay, renewed"
        );
        let target = transfer.cache_files(direction.sender(), "remote-hashlists");
        assert_eq!(target.len(), 1);
        assert_ne!(target[0], base);
        assert!(own.join(&target[0]).exists());
    }
}

#[test]
fn a_target_in_the_directory_of_another_sender_is_linked() {
    // A receiver that gets the same target from two senders keeps a copy in
    // its directory for each. Here, after a first run, the sender gets a new
    // account ID, as another sender would have. Its own cache belongs to the
    // receiver's account and index, so it still holds the target, and the
    // run sends HASHKEEP. The receiver's directory for the new account is
    // empty, and the receiver links the copy from its directory for the old
    // account into it, instead of writing the list again. Then the shared
    // file gets damaged: the next run finds the damage in the receiver's
    // link, and writes the target again in place of linking the other,
    // damaged, link back (DESIGN.md §5.6).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        transfer
            .env
            .write(transfer.indexed(direction, "first"), &tiled(64, 7));
        let (full, _) = cat_references(&transfer, direction, "first");
        let full = full.expect("no full list");
        let target = kept_list(&transfer, direction);
        let account = match direction {
            Direction::Pull => "server/home/.config/hashsync/account-id",
            Direction::Push => "client/config/hashsync/account-id",
        };
        std::fs::remove_file(transfer.env.path(account)).unwrap();
        assert_eq!(cat_references(&transfer, direction, "first"), (None, None));
        // The receiver's copies of the target, one per account directory:
        // their paths, inode numbers and sizes.
        let tree = transfer.cache_tree(direction.receiver(), "local-hashlists");
        let copies = || -> Vec<(PathBuf, u64, u64)> {
            std::fs::read_dir(&tree)
                .unwrap()
                .map(|dir| {
                    let path = dir.unwrap().path().join(&target);
                    let meta = std::fs::metadata(&path).unwrap();
                    (path, meta.ino(), meta.len())
                })
                .collect()
        };
        let linked = copies();
        assert_eq!(linked.len(), 2, "{direction:?}: a directory per account");
        assert_eq!(
            linked[0].1, linked[1].1,
            "{direction:?}: the target was written again"
        );

        // Written in place, so both links show the damage.
        std::fs::write(&linked[0].0, b"damaged").unwrap();
        assert_eq!(cat_references(&transfer, direction, "first"), (None, None));
        let mut sizes: Vec<u64> = copies().iter().map(|copy| copy.2).collect();
        sizes.sort_unstable();
        let valid = ("hsynlist\x01".len() + full) as u64;
        assert_eq!(
            sizes,
            [7, valid],
            "{direction:?}: the damaged link came back"
        );
    }
}

#[test]
fn a_damaged_cached_list_gives_way_to_the_full_list_on_either_side() {
    // A damaged cached list is a miss, and its file is removed (DESIGN.md
    // §5.6). If the damage is on the receiver, the receiver finds out when
    // it reads the list as a base: the run sends the full list when the
    // target changed, and nothing more when it did not. The sender only
    // finds out when it reads the list that HASHKEEP or HASHDELTA names, so
    // there the run sends the full list after HASHFULL. Either way, both
    // sides end up keeping only the valid target.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let write = |name: &str, data: &[u8]| {
            transfer.env.write(transfer.indexed(direction, name), data);
        };
        write("first", &tiled(64, 7));
        cat_references(&transfer, direction, "first");
        let receiver = (direction.receiver(), "local-hashlists");
        let sender = (direction.sender(), "remote-hashlists");
        let damage = |(side, tree): (&str, &str)| {
            let dir = transfer.peer_dir(side, tree).unwrap();
            std::fs::write(dir.join(kept_list(&transfer, direction)), b"damaged").unwrap();
        };

        damage(receiver);
        write("second", &tiled(4, 9));
        let (full, delta) = cat_references(&transfer, direction, "second");
        assert_eq!(delta, None);
        let full = full.expect("no full list");

        // HASHKEEP of the unchanged list, then the full list.
        damage(sender);
        let lists = cat_references(&transfer, direction, "second");
        assert_eq!(lists, (Some(full), None));

        // HASHDELTA from the previous run's list, then the full list: the
        // summary line shows both (DESIGN.md §8).
        damage(sender);
        write("third", &tiled(4, 11));
        let (third, delta) = cat_references(&transfer, direction, "third");
        let third = third.expect("no full list");
        assert!(
            third > full && delta.is_some(),
            "full {third}, delta {delta:?}"
        );
        // Both sides keep one valid copy of the target, `full` list bytes
        // after the wrapper.
        let both_keep = |full: usize| {
            for (side, tree) in [receiver, sender] {
                let dir = transfer.peer_dir(side, tree).unwrap();
                let bytes = std::fs::read(dir.join(kept_list(&transfer, direction))).unwrap();
                assert_eq!(bytes.len(), "hsynlist\x01".len() + full, "{side}");
            }
        };
        both_keep(third);

        // With the index unchanged, the receiver reads its copy of the
        // target as the base of a delta while it exports, since the sender
        // advertises that list. So damage there shows at once: the receiver
        // writes its target again instead of keeping the damaged copy, the
        // run needs no list bytes, and the next change goes as a delta.
        damage(receiver);
        assert_eq!(cat_references(&transfer, direction, "third"), (None, None));
        both_keep(third);
        write("fourth", &tiled(4, 13));
        let (full, delta) = cat_references(&transfer, direction, "fourth");
        assert!(
            full.is_none() && delta.is_some(),
            "full {full:?}, delta {delta:?}"
        );
    }
}

#[test]
fn lists_stay_usable_after_failed_runs_and_restores() {
    // If a run's sender ends before it publishes its target, the sender
    // still has the previous run's list, and the receiver, which got no
    // CACHE_RESULT, has both that list and the target. The next run then
    // sends a delta from the list that both have. An index that goes back to
    // an earlier state has that state's list as its target again, which a
    // delta can reach as well. Each time, both sides end up keeping only the
    // target (DESIGN.md §5.6).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let write = |name: &str, data: &[u8]| {
            transfer.env.write(transfer.indexed(direction, name), data);
        };
        write("first", &tiled(64, 7));
        cat_references(&transfer, direction, "first");
        let first = kept_list(&transfer, direction);
        let receiver_dir = transfer
            .peer_dir(direction.receiver(), "local-hashlists")
            .unwrap();
        let sender_dir = transfer
            .peer_dir(direction.sender(), "remote-hashlists")
            .unwrap();
        let first_file = std::fs::read(receiver_dir.join(&first)).unwrap();
        write("second", &tiled(4, 9));
        cat_references(&transfer, direction, "second");
        let second = kept_list(&transfer, direction);

        std::fs::write(receiver_dir.join(&first), &first_file).unwrap();
        std::fs::write(sender_dir.join(&first), &first_file).unwrap();
        std::fs::remove_file(sender_dir.join(&second)).unwrap();
        write("third", &tiled(4, 11));
        let (full, delta) = cat_references(&transfer, direction, "third");
        assert!(
            full.is_none() && delta.is_some(),
            "full {full:?}, delta {delta:?}"
        );
        kept_list(&transfer, direction);

        let third = transfer.env.path(transfer.indexed(direction, "third"));
        std::fs::remove_file(third).unwrap();
        let (full, delta) = cat_references(&transfer, direction, "second");
        assert!(
            full.is_none() && delta.is_some(),
            "full {full:?}, delta {delta:?}"
        );
        assert_eq!(kept_list(&transfer, direction), second);
    }
}

#[test]
fn a_rebuilt_index_starts_its_lists_again() {
    // A rebuild gives the index a new ID, and the caches are partitioned by
    // index: the sender has no list for the new one, so the next run sends
    // the full list. The old index's lists stay until they expire (DESIGN.md
    // §4.4 and §5.6).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        transfer
            .env
            .write(transfer.indexed(direction, "first"), &tiled(64, 7));
        let (full, _) = cat_references(&transfer, direction, "first");
        let old = transfer
            .peer_dir(direction.receiver(), "local-hashlists")
            .unwrap();
        let list = kept_list(&transfer, direction);
        transfer.update_receiver(direction, &["--rebuild"]);
        assert_eq!(cat_references(&transfer, direction, "first"), (full, None));
        for (side, tree) in [
            (direction.receiver(), "local-hashlists"),
            (direction.sender(), "remote-hashlists"),
        ] {
            let dirs: Vec<PathBuf> = std::fs::read_dir(transfer.cache_tree(side, tree))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect();
            assert_eq!(dirs.len(), 2, "{side} {tree}: {dirs:?}");
            assert!(dirs.iter().all(|dir| dir.join(&list).exists()), "{dirs:?}");
        }
        assert!(old.join(&list).exists());
    }
}

#[test]
fn a_sender_reads_its_base_after_a_concurrent_exchange_removed_it() {
    // Read-only runs of one index can overlap, with the same sender account.
    // The sender opens its listing before its HELLO, but reads the base that
    // HASHDELTA names only after `attach` has connected, so a concurrent
    // exchange can remove the base in between (DESIGN.md §5.6). Here a gate
    // holds back the control session of the first run while a second run
    // goes through its whole exchange and removes the base from both caches.
    // The first run must still send a delta, which its sender can only
    // apply through the file that its listing opened.
    let transfer = Transfer::new(&[]);
    transfer.env.write("tree/first", &tiled(64, 7));
    cat_references(&transfer, Direction::Pull, "first");
    let base = kept_list(&transfer, Direction::Pull);
    transfer.env.write("tree/second", &tiled(64, 9));
    transfer.env.update();
    let gate = transfer.env.path("gate");
    let mut gate_writer = held_fifo(&gate);
    wrap_ssh(
        &transfer,
        r#"if [ $session = control ] && [ -n "${HASHSYNC_TEST_GATE:-}" ]; then
    read -r go <"$HASHSYNC_TEST_GATE"
fi
exec "$ssh" "$@""#,
    );
    let first = transfer
        .invocation(
            &[EXACT, BATCH, "--no-update"],
            &refs(&args(&[&cat(&transfer, Direction::Pull, "second")])),
        )
        .env("HASHSYNC_TEST_GATE", &gate)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut first = KillOnDrop(first);

    // The first run's receiver publishes its target only after the sender's
    // HELLO, so once the target shows up, the sender has opened its listing.
    // It is now waiting for `attach`, before it reads anything that names
    // the base. Poll for the target with no deadline.
    let client_dir = transfer.peer_dir("client", "local-hashlists").unwrap();
    let target_published = || {
        let names = transfer.cache_files("client", "local-hashlists");
        names
            .into_iter()
            .any(|name| !name.starts_with('.') && name != base)
    };
    while !target_published() {
        assert!(first.0.try_wait().unwrap().is_none(), "the first run ended");
        std::thread::sleep(Duration::from_millis(1));
    }

    // The second run has no gate. The end of its exchange removes the base
    // on both sides.
    let script = cat(&transfer, Direction::Pull, "second");
    let second = transfer.run(&[BATCH, "--no-update"], &refs(&args(&[&script])));
    let (full, delta) = all_references(&transfer, Direction::Pull, "second", second);
    assert!(
        full.is_none() && delta.is_some(),
        "second run: full {full:?}, delta {delta:?}"
    );
    let server_dir = transfer.peer_dir("server", "remote-hashlists").unwrap();
    assert!(!client_dir.join(&base).exists() && !server_dir.join(&base).exists());

    gate_writer.write_all(b"go\n").unwrap();
    let (full, delta) = all_references(&transfer, Direction::Pull, "second", output_of(first));
    assert!(
        full.is_none() && delta.is_some(),
        "first run: full {full:?}, delta {delta:?}"
    );
    kept_list(&transfer, Direction::Pull);
}

#[test]
fn a_delta_larger_than_the_full_list_is_not_sent() {
    // When every value of a short list changes, the removed and added
    // values take more bytes than the full list, so the run sends the full
    // list instead (DESIGN.md §5.6): 8 bytes of header and 9 for its one
    // value.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let one = transfer.indexed(direction, "one");
        transfer.env.write(&one, &tiled(1, 7));
        assert_eq!(
            cat_references(&transfer, direction, "one"),
            (Some(17), None)
        );
        transfer.env.write(&one, &tiled(1, 8));
        assert_eq!(
            cat_references(&transfer, direction, "one"),
            (Some(17), None)
        );
    }
}

#[test]
fn an_empty_cache_directory_that_cannot_be_removed_only_warns() {
    // Expiry removes empty peer/index directories on both sides. A
    // failure to remove one does not end the run: it warns, and the
    // directory stays until a later run (DESIGN.md §5.6). A read-only
    // tree lets the run publish in its existing directory, but not
    // remove the other directory.
    let transfer = Transfer::new(&[]);
    let command = args(&["true"]);
    transfer.run_ok(&refs(&command));
    let trees = [
        "client/cache/hashsync/local-hashlists",
        "server/home/.cache/hashsync/remote-hashlists",
    ];
    for tree in trees {
        transfer.env.mkdir(format!("{tree}/stale"));
        transfer.env.chmod(tree, 0o555);
    }
    let output = transfer.run_ok(&refs(&command));
    let stderr = String::from_utf8_lossy(&output.stderr);
    for tree in trees {
        transfer.env.chmod(tree, 0o755);
        let stale = transfer.env.path(format!("{tree}/stale"));
        let warning = format!("warning: cannot remove {}: ", stale.display());
        assert!(stderr.contains(&warning), "missing {warning:?}:\n{stderr}");
        assert!(stale.exists());
    }

    // Writable again: the next run removes the directories.
    transfer.run_ok(&refs(&command));
    for tree in trees {
        assert!(!transfer.env.path(format!("{tree}/stale")).exists());
    }
}

#[test]
fn bootstrap_quoting_survives_a_hostile_hashsync_path() {
    // Whitespace, quotes, shell metacharacters, and a non-UTF-8 byte in
    // the executable path (DESIGN.md §11), resolved by a real `sh`.
    let transfer = Transfer::new(&[]);
    let name = b"has hsync's \"$HOME\" `x` \xff bin";
    let mut link = transfer
        .env
        .dir
        .path()
        .as_os_str()
        .as_encoded_bytes()
        .to_vec();
    link.push(b'/');
    link.extend_from_slice(name);
    let link = PathBuf::from(OsString::from_vec(link));
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_hashsync"), &link).unwrap();

    transfer.set_hashsync_path(link.as_os_str());
    let command = args(&["printf", "%s", "quoted-ok"]);
    let output = transfer.run_ok(&refs(&command));
    assert_eq!(output.stdout, b"quoted-ok");
}

/// A fake server that ignores what the client sends. On the data session,
/// it sends `data`, then reads its stdin to EOF. On the control session, it
/// sends `control`, then keeps its stdout open until its stdin ends.
/// Returns the path of the script, for `--hashsync-path`.
fn fake_server(transfer: &Transfer, name: &str, data: &[u8], control: &[u8]) -> PathBuf {
    let data_path = transfer.env.path(format!("{name}.data"));
    std::fs::write(&data_path, data).unwrap();
    let control_path = transfer.env.path(format!("{name}.control"));
    std::fs::write(&control_path, control).unwrap();
    let server = transfer.env.path(name);
    std::fs::write(
        &server,
        format!(
            r#"#!/bin/sh
if [ "$1" = serve ]; then
    cat '{}'
    exec cat >/dev/null
fi
cat '{}'
exec cat 3>&1 >/dev/null
"#,
            data_path.display(),
            control_path.display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();
    server
}

#[test]
fn a_server_with_another_protocol_version_is_rejected() {
    // A fake server that sends only its version, 0xFFFFFFFF, on both
    // sessions, without features: the client checks the version before
    // anything else and ends the run (DESIGN.md §6.2).
    let transfer = Transfer::new(&[]);
    let other = [0xff; 4];
    let server = fake_server(&transfer, "other-version", &other, &other);
    transfer.set_hashsync_path(server.as_os_str());
    let command = args(&["true"]);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("speaks protocol version 4294967295, this hashsync"),
        "version mismatch:\n{stderr}"
    );
}

/// The data frames of a fake server, from HELLO through the backward
/// stream: an account ID and no advertised list in HELLO, then
/// CACHE_RESULT, the stream with `output` in one LIT, and STREAM_END. The
/// stream is not compressed, so a run that gets it needs
/// `--compression-level 0`: with compression, it would be a zstd frame.
fn fake_frames(output: &[u8]) -> Vec<u8> {
    let mut hello = vec![0x11; 16];
    hello.extend_from_slice(&0u32.to_le_bytes());
    [
        frame(HELLO, &hello),
        frame(CACHE_RESULT, b""),
        frame(CHUNK, &frame(LIT, output)),
        frame(STREAM_END, b""),
    ]
    .concat()
}

#[test]
fn a_data_session_that_ends_before_end_fails_the_run() {
    // A fake server sends its version and features, HELLO, CACHE_RESULT,
    // and a whole backward stream, then ends the data session without END,
    // as a server that fails at its end does, and reads what the client
    // sends. The run fails although the stream is complete (DESIGN.md
    // §6.9). The fake control session sends its version and features and
    // stays open until its stdin ends.
    let transfer = Transfer::new(&[]);
    let start = version_and_features(FEATURES);
    let data = [&start[..], &fake_frames(b"output")].concat();
    let server = fake_server(&transfer, "ends-before-end", &data, &start);
    transfer.set_hashsync_path(server.as_os_str());
    let output = transfer.run(&["--compression-level", "0"], &refs(&args(&["true"])));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr:\n{stderr}");
    // The end of the run can come before the decoder writes the output.
    assert!(b"output".starts_with(&output.stdout), "{:?}", output.stdout);
    assert!(
        stderr.contains("the server disappeared"),
        "stderr:\n{stderr}"
    );
}

#[test]
fn feature_bits_that_the_client_does_not_know_are_ignored() {
    // A later server can send feature bits that this client does not
    // know, on both sessions. The client ignores them, and the run ends as
    // with none (DESIGN.md §6.2).
    let transfer = Transfer::new(&[]);
    let start = version_and_features(u64::MAX);
    // END with status 0, no error text, the nine counts of an empty
    // forward stream, and the 6 bytes of the command's stdout, read to its
    // EOF.
    let end = [&[0; 4 + 4 + 9 * 8][..], &6u64.to_le_bytes(), &[1]].concat();
    let data = [&start[..], &fake_frames(b"output"), &frame(END, &end)].concat();
    let server = fake_server(&transfer, "unknown-features", &data, &start);
    transfer.set_hashsync_path(server.as_os_str());
    let output = transfer.run(&["--compression-level", "0"], &refs(&args(&["true"])));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "stderr:\n{stderr}");
    assert_eq!(output.stdout, b"output");
}

fn signal(pid: u32, signal: Signal) {
    let pid = Pid::from_raw(pid as i32).unwrap();
    kill_process(pid, signal).unwrap();
}

/// Wait until the flags of `fd` say non-blocking, when `non_blocking`, or
/// blocking, polling them with no deadline.
fn wait_for_non_blocking(fd: impl AsFd, non_blocking: bool) {
    while rustix::fs::fcntl_getfl(&fd)
        .unwrap()
        .contains(OFlags::NONBLOCK)
        != non_blocking
    {
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Make a FIFO at `path`, and hold it open for writing. A command that
/// opens the FIFO for reading while the returned file is open reads EOF
/// only after the file closes, at the latest when the test process ends.
/// A command that opens the FIFO later waits for a writer forever.
fn held_fifo(path: &std::path::Path) -> File {
    mkfifoat(CWD, path, Mode::RUSR | Mode::WUSR).unwrap();
    // Also for reading, so that the open does not wait for a reader.
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap()
}

/// Wait until `child` stops, and keep that state for a later wait. Fail
/// when the child ends before it stops.
fn wait_until_stopped(child: &Child) {
    let pid = Pid::from_child(child);
    let options = WaitIdOptions::STOPPED | WaitIdOptions::EXITED | WaitIdOptions::NOWAIT;
    let status = retry_on_intr(|| waitid(WaitId::Pid(pid), options))
        .unwrap()
        .expect("waitid without NOHANG returned no status");
    assert!(status.stopped(), "{pid:?} ended instead of stopping");
}

/// Wait until `pid` has exited, also when it is not a child of the test
/// process.
fn wait_until_exited(pid: u32) {
    let pid = Pid::from_raw(pid as i32).unwrap();
    let pidfd = pidfd_open(pid, PidfdFlags::empty()).unwrap();
    let mut fds = [PollFd::new(&pidfd, PollFlags::IN)];
    retry_on_intr(|| poll(&mut fds, None)).unwrap();
}

/// Wait until the process `pid` has a thread named `name`, when
/// `running`, or has none, polling with no deadline. Fail when the process
/// has ended: it stays a zombie until the test reaps it.
fn wait_for_thread(pid: u32, name: &str, running: bool) {
    loop {
        assert_ne!(
            proc_state(format!("/proc/{pid}")),
            Some('Z'),
            "the process ended"
        );
        // A thread that ends meanwhile has no name.
        let found = std::fs::read_dir(format!("/proc/{pid}/task"))
            .unwrap()
            .filter_map(|task| std::fs::read_to_string(task.ok()?.path().join("comm")).ok())
            .any(|comm| comm.trim_end() == name);
        if found == running {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The children of `pid`, started by any of its threads. A thread that
/// ends meanwhile has no children file, and a process that has ended has
/// no children.
fn children(pid: u32) -> Vec<u32> {
    let mut children = Vec::new();
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
        return children;
    };
    for task in tasks {
        let Ok(task) = task else {
            continue;
        };
        let list = std::fs::read_to_string(task.path().join("children")).unwrap_or_default();
        children.extend(
            list.split_whitespace()
                .map(|pid| pid.parse::<u32>().unwrap()),
        );
    }
    children
}

#[test]
fn a_termination_signal_decides_the_status_even_after_the_sessions_break() {
    // Ctrl-C reaches the client and its SSH children together. The dying
    // sessions can make another thread fail before the signal is handled,
    // but the status must report the signal (DESIGN.md §6.8). Force that
    // order: stop the client, send it the signal, kill both sessions,
    // and let the client continue with both events already present.
    let transfer = Transfer::new(&[]);
    // The command opens a FIFO that the test holds open, and reports on
    // stderr that it runs, which the server relays at once (DESIGN.md
    // §6.1). Then it reads the FIFO until EOF, so it runs until the test
    // ends, also after the killed sessions orphan it.
    let fifo = transfer.env.path("hold");
    let _hold = held_fifo(&fifo);
    let running = "the command runs";
    let script = format!("exec <'{}'; echo '{running}' >&2; exec cat", fifo.display());
    let command = args(&[&script]);
    for (sent, status, name) in [(Signal::INT, 130, "SIGINT"), (Signal::TERM, 143, "SIGTERM")] {
        for _ in 0..4 {
            let mut client = KillOnDrop(
                transfer
                    .invocation(&[], &refs(&command))
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap(),
            );
            let mut stderr = BufReader::new(client.0.stderr.take().unwrap());
            let mut text = String::new();
            while !text.contains(running) {
                let n = stderr.read_line(&mut text).unwrap();
                assert!(n > 0, "the run ended before the command ran:\n{text}");
            }

            let pid = client.0.id();
            signal(pid, Signal::STOP);
            wait_until_stopped(&client.0);
            signal(pid, sent);
            let sessions = children(pid);
            assert_eq!(sessions.len(), 2, "both SSH sessions: {sessions:?}");
            for &session in &sessions {
                signal(session, Signal::KILL);
            }
            for &session in &sessions {
                wait_until_exited(session);
            }
            signal(pid, Signal::CONT);

            stderr.read_to_string(&mut text).unwrap();
            let exit = client.0.wait().unwrap();
            assert_eq!(exit.code(), Some(status), "stderr:\n{text}");
            assert!(
                text.contains(&format!("terminated by {name}")),
                "stderr:\n{text}"
            );
        }
    }
}

/// Put a wrapper in place of the fake `ssh`. The wrapper sets `$ssh` to the
/// fake `ssh`, and `$session` to `control` for the session that runs
/// `attach`, else to `data`. Then it runs `body`, which runs `"$ssh" "$@"`
/// and exits with its status.
fn wrap_ssh(transfer: &Transfer, body: &str) {
    let ssh = transfer.env.path("ssh");
    let inner = transfer.env.path("ssh-inner");
    std::fs::rename(&ssh, &inner).unwrap();
    let script = format!(
        r#"#!/bin/sh
ssh='{}'
case "$*" in
*"'attach'"*) session=control ;;
*) session=data ;;
esac
{body}
"#,
        inner.display()
    );
    std::fs::write(&ssh, script).unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn the_client_closes_stdin_before_either_stdout_ends() {
    // Like an OpenSSH multiplexing client, the wrapper keeps the stdout of
    // each session open until its SSH child exits, and the server exits
    // only after stdin EOF. The client closes both stdin pipes after END,
    // without waiting for the EOF of either stdout, so the run still ends
    // (DESIGN.md §6.9 and §10).
    let transfer = Transfer::new(&[]);
    let blocks = tiled(16, 28);
    transfer.env.write("tree/blocks", &blocks);
    wrap_ssh(&transfer, "\"$ssh\" \"$@\"\nexit $?");
    let command = args(&["cat", transfer.env.path("tree/blocks").to_str().unwrap()]);
    let output = succeeded(transfer.run(&[BATCH], &refs(&command)));
    assert_eq!(output.stdout, blocks);
    assert_eq!(summary(&output), [blocks.len(), 0, blocks.len(), 0]);
}

#[test]
fn the_end_waits_for_a_process_that_keeps_the_control_session_open() {
    // After the control session's SSH exits, a process that inherited its
    // stdout keeps the pipe open for a moment, creates the marker, and
    // exits. The client reads each session to EOF and ends its threads
    // before it exits (DESIGN.md §6.9), so it cannot exit before the
    // marker exists, however the processes are scheduled. The normal run,
    // the read-only run, and a command that fails to start end the same
    // way.
    let transfer = Transfer::new(&[]);
    let blocks = tiled(16, 26);
    transfer.env.write("tree/blocks", &blocks);
    let marker = transfer.env.path("marker");
    wrap_ssh(
        &transfer,
        &format!(
            r#""$ssh" "$@"
status=$?
if [ $session = control ]; then
    ( sleep 0.2; : >'{}' ) </dev/null 2>/dev/null &
fi
exit $status"#,
            marker.display()
        ),
    );
    let cat = args(&["cat", transfer.env.path("tree/blocks").to_str().unwrap()]);
    let missing = args(&["/nonexistent-hashsync-test-command"]);
    for (flags, command, status) in [
        (&[BATCH][..], &cat, 0),
        (&["--no-update", BATCH][..], &cat, 0),
        (&[BATCH, "-s"][..], &missing, 127),
    ] {
        let _ = std::fs::remove_file(&marker);
        let output = transfer.run(flags, &refs(command));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(status), "{flags:?}:\n{stderr}");
        assert!(
            marker.exists(),
            "{flags:?}: ended before the EOF:\n{stderr}"
        );
        if status == 0 {
            assert_eq!(output.stdout, blocks);
            assert_eq!(summary(&output), [blocks.len(), 0, blocks.len(), 0]);
        }
    }
}

#[test]
fn the_summary_counts_every_byte_that_the_sessions_deliver() {
    // `tee` copies the stdout of each session to a file. After the control
    // session's SSH exits, a process that inherited its stdout sends one
    // more QUERY, which the client drops: its lookup workers ended at
    // Completing (DESIGN.md §6.4). The client reads both sessions to EOF
    // before the summary (DESIGN.md §6.9 and §8), so the summary counts
    // exactly the bytes of both files, that QUERY included. One changed
    // block makes the scanner rescan. The late QUERY is a plain frame, so
    // this run has no compression: inside a zstd stream, the client could
    // not decode it.
    let transfer = Transfer::new(&["--compression-level", "0"]);
    let mut blocks = tiled(32, 27);
    transfer.env.write("tree/blocks", &blocks);
    transfer.env.update();
    blocks[5 * BLOCK + 9] ^= 1;
    transfer.env.write("tree/blocks", &blocks);
    let captured = transfer.env.path("captured");
    // QUERY: offset 0 and a zero digest.
    let late = frame(QUERY, &[0; 8 + 32]);
    let late_path = transfer.env.path("late-query");
    std::fs::write(&late_path, &late).unwrap();
    wrap_ssh(
        &transfer,
        &format!(
            r#"{{ "$ssh" "$@"; echo $? >'{0}'.$session.status; }} | tee '{0}'.$session
status=$(cat '{0}'.$session.status)
if [ $session = control ]; then
    ( sleep 0.2; tee -a '{0}'.control <'{1}' ) </dev/null 2>/dev/null &
fi
exit $status"#,
            captured.display(),
            late_path.display(),
        ),
    );
    let command = args(&["cat", transfer.env.path("tree/blocks").to_str().unwrap()]);
    let output = succeeded(transfer.run(&["--no-update", BATCH], &refs(&command)));
    assert_eq!(output.stdout, blocks);
    assert_eq!(summary(&output), [blocks.len(), BLOCK, 31 * BLOCK, 0]);
    let read = |session: &str| std::fs::read(format!("{}.{session}", captured.display())).unwrap();
    let (data, control) = (read("data"), read("control"));
    assert!(
        control.ends_with(&late),
        "the run ended before the last QUERY"
    );
    assert_eq!(deduplicated(&output).chunks(), chunk_payload_bytes(&data));
    assert_eq!(traffic(&output).received, data.len() + control.len());
}

#[test]
fn the_summary_of_a_push_counts_every_byte_that_the_sessions_carry() {
    // In a push, the client's data writer counts the stream's bytes and its
    // CHUNK payloads, and the client counts what both sessions carry, as in
    // a pull. `tee` copies both directions of both sessions. The wrapper
    // ends only once its copies have ended, and the client waits for the
    // exit of both sessions before the summary (DESIGN.md §6.9 and §8), so
    // the copies are whole then. One changed block makes the scanner
    // rescan.
    let transfer = Transfer::new(&[]);
    transfer.configure_server("");
    let mut blocks = tiled(32, 49);
    transfer.env.write("server-tree/blocks", &blocks);
    transfer.update_receiver(Direction::Push, &[]);
    blocks[5 * BLOCK + 9] ^= 1;
    let captured = transfer.env.path("captured");
    wrap_ssh(
        &transfer,
        &format!(
            r#"tee '{0}'.$session.in | {{ "$ssh" "$@"; echo $? >'{0}'.$session.status; }} | tee '{0}'.$session.out
exit "$(cat '{0}'.$session.status)""#,
            captured.display(),
        ),
    );
    let flags = ["--no-update", Direction::Push.batch()];
    let output = succeeded(transfer.run_push(&flags, &refs(&args(&["cat"])), &blocks));
    assert_eq!(output.stdout, blocks);
    assert_eq!(summary(&output), [blocks.len(), BLOCK, 31 * BLOCK, 0]);
    let read = |name: &str| std::fs::read(format!("{}.{name}", captured.display())).unwrap();
    let line = deduplicated(&output);
    assert_eq!(line.chunks(), chunk_payload_bytes(&read("data.in")));
    let traffic = traffic(&output);
    assert_eq!(
        traffic.sent,
        read("data.in").len() + read("control.in").len()
    );
    let received = read("data.out").len() + read("control.out").len();
    assert_eq!(traffic.received, received);
}

#[test]
fn the_summary_of_a_stopped_forward_stream_counts_what_reached_the_command() {
    // The command ignores its stdin of 4 MiB, writes a word, and exits,
    // which stops the forward stream (DESIGN.md §6.6). The stdin line then
    // gives the bytes that the client read, `pipe closed`, the CHUNK bytes
    // that the server decoded and that it dropped, which together are every
    // CHUNK byte that the client sent, and the bytes that the command's
    // stdin took, which a pipe holds while no process reads it. The data
    // shrinkage factor and the saved bytes count only the bytes that
    // reached their destinations (DESIGN.md §8).
    // `tee` copies what the client sends on the data session; the wrapper
    // ends only once its copy has ended, and the client waits for the exit
    // of both sessions before the summary.
    let input = random(4 << 20, 50);
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let path = transfer.env.path("input");
        std::fs::write(&path, &input).unwrap();
        let captured = transfer.env.path("captured");
        wrap_ssh(
            &transfer,
            &format!(r#"tee '{0}'.$session | "$ssh" "$@""#, captured.display()),
        );
        let output = transfer
            .invocation_in(direction, &[EXACT], &refs(&args(&["printf done"])))
            .stdin(File::open(&path).unwrap())
            .output()
            .unwrap();
        let output = succeeded(output);
        assert_eq!(output.stdout, b"done");
        let line = summary_line(&output, "hashsync: stdin -- ");
        assert!(line.contains(" read, pipe closed, "), "{line}");
        let [read, decoded, dropped, written] = line_counts(&line)[..] else {
            panic!("unexpected summary line: {line}");
        };
        assert!(
            written <= read && read <= input.len(),
            "{direction:?}: {line}"
        );
        let data = std::fs::read(format!("{}.data", captured.display())).unwrap();
        assert_eq!(decoded + dropped, chunk_payload_bytes(&data), "{line}");
        traffic(&output).check_delivered(output.stdout.len() + written, direction);
    }
}

#[test]
fn a_forward_stream_that_stops_before_the_eof_of_stdin_shows_as_stopped() {
    // The client's stdin is a pipe that the test holds open without a
    // byte, so the client never reads its EOF, and the exit of `true`
    // stops the forward stream. The stdin line shows the stream as
    // stopped, though the command's stdin took every byte that the client
    // read, none: a stream is whole only once its sender read its source
    // to the end (DESIGN.md §8).
    let transfer = Transfer::new(&[]);
    let (stdin, producer) = std::io::pipe().unwrap();
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&["true"])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let output = succeeded(output_of(KillOnDrop(client)));
    assert_eq!(
        summary_line(&output, "hashsync: stdin"),
        "hashsync: stdin -- 0 B read, pipe closed, 0 B wire, 0 B dropped, 0 B written"
    );
    drop(producer);
}

#[test]
fn a_stopped_forward_stream_shows_the_chunk_bytes_that_the_server_dropped() {
    // The command closes its stdin, then waits for this test. The first
    // piece of stdin then fails at the command's stdin with `EPIPE`, which
    // stops the forward stream on the server (DESIGN.md §6.6). A wrapper of
    // the control session copies what the server sends, and holds it after
    // the version and the features, so STOP does not reach the client yet.
    // Once the copy holds STOP, the server has decoded the first piece, and
    // this test writes the second, which the client sends, as a copy of the
    // data session shows; only then does the wrapper let STOP go. The
    // server drops the second piece undecoded. In a pull, the forward
    // stream is plain, and without compression, each piece goes out as one
    // LIT frame, 5 bytes longer than the piece, in a CHUNK of its own, in
    // eager delivery. So the stdin line is exact: the client read both
    // pieces, the server decoded the first frame and dropped the second,
    // and the command's stdin took nothing. The CHUNK bytes that the client
    // sent are both frames, and the dropped bytes are those that the server
    // did not decode (DESIGN.md §8).
    let transfer = Transfer::new(&["--compression-level", "0"]);
    let (stops, sent, status) = (
        transfer.env.path("control-received"),
        transfer.env.path("data-sent"),
        transfer.env.path("status"),
    );
    let mut stop_gate = Gate::new(transfer.env.path("stop-gate"));
    wrap_ssh(
        &transfer,
        &format!(
            r#"if [ $session = control ]; then
    {{ "$ssh" "$@"; echo $? >'{3}'.control; }} | tee '{0}' | {{
        head -c 12
        read go <'{2}'
        exec cat
    }}
    exit "$(cat '{3}'.control)"
fi
tee '{1}' | "$ssh" "$@""#,
            stops.display(),
            sent.display(),
            stop_gate.path.display(),
            status.display()
        ),
    );
    let closed = transfer.env.path("closed");
    let mut go = Gate::new(transfer.env.path("go"));
    let script = format!(
        "exec <&-; : >'{}'; read go <'{}'",
        closed.display(),
        go.path.display()
    );
    let (stdin, mut producer) = std::io::pipe().unwrap();
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&[&script])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut client = KillOnDrop(client);
    wait_for_file(&closed, std::slice::from_mut(&mut client));
    let (first, second) = (
        &b"the first piece of stdin"[..],
        &b"the second piece, dropped"[..],
    );
    producer.write_all(first).unwrap();
    // Without compression, after its version and its features, the server
    // sends nothing on the control session but STOP: the command writes no
    // output to query.
    let stop = frame(STOP, b"");
    wait_for_bytes(&stops, std::slice::from_mut(&mut client), |received| {
        received.len() >= 12 + stop.len()
    });
    assert_eq!(std::fs::read(&stops).unwrap()[12..], stop);
    producer.write_all(second).unwrap();
    wait_for_bytes(&sent, std::slice::from_mut(&mut client), |sent| {
        holds(sent, &frame(LIT, second))
    });
    stop_gate.release();
    go.release();
    let output = succeeded(output_of(client));
    drop(producer);
    assert_eq!(
        summary_line(&output, "hashsync: stdin"),
        format!(
            "hashsync: stdin -- {} B read, pipe closed, {} B wire, {} B dropped, 0 B written",
            first.len() + second.len(),
            5 + first.len(),
            5 + second.len()
        )
    );
}

/// The script of a command that writes 10 bytes to its stdout, waits until
/// the test releases the gate `more`, writes 5 more bytes, waits for the
/// gate `last`, then runs `rest`. The tests of a stopped backward stream
/// run it with the client's stdout to `head -c 10` (`HeadOfStdout`), and
/// release `more` once `head` has exited. The client's write of the 5
/// bytes then fails with `EPIPE`, which stops the backward stream
/// (DESIGN.md §6.6).
fn writes_then_waits(more: &Gate, last: &Gate, rest: &str) -> String {
    format!(
        "printf 0123456789\nread go <'{}'\nprintf abcde\nread go <'{}'\n{rest}",
        more.path.display(),
        last.path.display()
    )
}

/// A FIFO at which a process of a run, a command or a wrapper of a
/// session, waits with `read go <FIFO` until the test lets it go on
/// (`Gate::release`), or another process of the run does, with a line that
/// it writes to the FIFO. The test holds the FIFO open for reading and
/// writing from the start (`held_fifo`), so no open of it waits, only the
/// read. A test that fails before the release must not leave the process
/// waiting forever, with its run: the drop removes the FIFO, then closes
/// it, so a process that has opened it reads EOF, and one that opens it
/// later finds no file. Either way, the process goes on to its end.
struct Gate {
    path: PathBuf,
    file: File,
}

impl Gate {
    /// Make the FIFO at `path`, and hold it.
    fn new(path: PathBuf) -> Gate {
        let file = held_fifo(&path);
        Gate { path, file }
    }

    /// Let the process that waits at this FIFO go on.
    fn release(&mut self) {
        self.file.write_all(b"go\n").unwrap();
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Make the FIFOs `more` and `last` of `writes_then_waits` in the
/// environment of `transfer`.
fn fifos(transfer: &Transfer) -> (Gate, Gate) {
    let gate = |name| Gate::new(transfer.env.path(name));
    (gate("more"), gate("last"))
}

/// A run of `hashsync ssh` whose stdout goes to `head -c 10`, which reads
/// the first 10 bytes that a command of `writes_then_waits` writes, then
/// exits. A shell makes the pipe between them. A pipe of the test process
/// would reach every process that another test starts meanwhile, for as
/// long as that process takes to start its program, and under the load of
/// the whole suite, such a process can still hold a reader of the pipe when
/// the client writes its next bytes, which then go into the pipe instead of
/// failing with `EPIPE`. A shell has one thread, and starts only the two
/// ends of its pipeline.
struct HeadOfStdout {
    shell: KillOnDrop,
    /// The client's program, by which `client` finds the client.
    program: PathBuf,
    /// Where the shell writes the 10 bytes, the mark of the end of `head`,
    /// and the client's exit status.
    dir: PathBuf,
}

impl HeadOfStdout {
    /// Start `invocation`, with its environment, its stdout to `head -c 10`,
    /// and its stderr to a pipe of the test. The files of the run go to
    /// `dir`.
    fn spawn(invocation: &Command, dir: PathBuf) -> Self {
        // After `head` has exited, the subshell of the end of the pipeline
        // closes its stdin too, before it marks the end: then no process
        // reads the client's stdout any more.
        let script = r#"head=$1 done=$2 status=$3
shift 3
{ "$@"; echo $? >"$status"; } | { head -c 10 >"$head"; exec <&-; : >"$done"; }"#;
        let mut shell = Command::new("/bin/sh");
        shell
            .arg("-c")
            .arg(script)
            .arg("sh")
            .arg(dir.join("head"))
            .arg(dir.join("head-done"))
            .arg(dir.join("status"))
            .arg(invocation.get_program())
            .args(invocation.get_args());
        for (key, value) in invocation.get_envs() {
            match value {
                Some(value) => shell.env(key, value),
                None => shell.env_remove(key),
            };
        }
        let shell = shell
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        HeadOfStdout {
            shell: KillOnDrop(shell),
            program: std::fs::canonicalize(invocation.get_program()).unwrap(),
            dir,
        }
    }

    /// Wait until `head` has exited, and no process reads the client's
    /// stdout any more, polling with no deadline. Returns what `head` read.
    fn wait_for_head(&mut self) -> Vec<u8> {
        wait_for_file(
            &self.dir.join("head-done"),
            std::slice::from_mut(&mut self.shell),
        );
        std::fs::read(self.dir.join("head")).unwrap()
    }

    /// The client: the process of `hashsync ssh`, which a subshell of the
    /// shell starts. Polls with no deadline. The client is the child that
    /// runs the client's program. Its arguments cannot tell it: until a
    /// subshell's child starts its program, it has the shell's arguments,
    /// which hold the client's, and the child that starts `head` would
    /// match them too.
    fn client(&mut self) -> u32 {
        loop {
            assert_running(&mut self.shell);
            for subshell in children(self.shell.0.id()) {
                for child in children(subshell) {
                    let exe = std::fs::read_link(format!("/proc/{child}/exe"));
                    if exe.is_ok_and(|exe| exe == self.program) {
                        return child;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Wait for the end of the run, and return the client's exit status and
    /// stderr.
    fn output(mut self) -> Output {
        let mut stderr = Vec::new();
        let mut pipe = self.shell.0.stderr.take().unwrap();
        pipe.read_to_end(&mut stderr).unwrap();
        assert!(self.shell.0.wait().unwrap().success(), "the shell failed");
        let status = std::fs::read_to_string(self.dir.join("status")).unwrap();
        let code: i32 = status.trim().parse().unwrap();
        Output {
            status: std::process::ExitStatus::from_raw(code << 8),
            stdout: Vec::new(),
            stderr,
        }
    }
}

#[test]
fn a_closed_stdout_stops_the_backward_stream_and_the_command_gets_sigpipe() {
    // The command notes SIGPIPE with a trap, starts a descendant that holds
    // none of its standard streams, and runs `writes_then_waits`. `head -c
    // 10` reads its first 10 bytes from the client's stdout and exits, and
    // this test lets the command write 5 more bytes. The client's write of
    // them fails with
    // EPIPE, which stops the backward stream there, and its STOP stops it on
    // the server, whose stdout reader then closes the command's stdout
    // (DESIGN.md §6.6). Once that reader has ended, this test lets the
    // command write once more: the write gets SIGPIPE, and the trap notes it
    // and exits with 7. The run ends normally with that status (DESIGN.md
    // §6.9), and the descendant lives on, where the fatal end of a run kills
    // the command's process group. Without compression, the stdout line
    // shows exact counts: the server read 15 bytes, the client decoded both
    // CHUNK frames, of 15 and 10 bytes, and dropped none, and its stdout
    // took 10 bytes (DESIGN.md §8). A run that did not stop the stream on
    // the server would leave the command waiting for its last write to fail,
    // and hang the test.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&["--compression-level", "0"]);
        transfer.configure_receiver(direction, "");
        let (mut more, mut last) = fifos(&transfer);
        let (marker, descendant) = (
            transfer.env.path("sigpipe"),
            transfer.env.path("descendant"),
        );
        let script = format!(
            "trap 'echo sigpipe >\"{marker}\"; exit 7' PIPE\n\
             sh -c 'exec tail --pid={test} -f /dev/null' </dev/null >/dev/null 2>&1 &\n\
             echo $! >\"{descendant}\"\n\
             {rest}",
            marker = marker.display(),
            test = std::process::id(),
            descendant = descendant.display(),
            rest = writes_then_waits(&more, &last, "printf x\nexit 3")
        );
        let invocation = transfer.invocation_in(direction, &[EXACT], &refs(&args(&[&script])));
        let mut run = HeadOfStdout::spawn(&invocation, transfer.env.path("."));
        let server = server_of(run.client());
        assert_eq!(run.wait_for_head(), b"0123456789");
        more.release();
        wait_for_thread(server, "stdout-reader", false);
        last.release();
        let output = run.output();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(7), "{direction:?}:\n{stderr}");
        assert_eq!(std::fs::read(&marker).unwrap(), b"sigpipe\n");
        assert_eq!(
            summary_line(&output, "hashsync: stdout"),
            "hashsync: stdout -- 15 B read, pipe closed, 25 B wire, 0 B dropped, 10 B written",
            "{direction:?}"
        );
        // The stdout of the client took 10 bytes, and its stdin none.
        traffic(&output).check_delivered(10, direction);
        // The descendant has run on since the command started it, so no
        // other process can have its PID.
        let pid: u32 = std::fs::read_to_string(&descendant)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let state = proc_state(format!("/proc/{pid}"));
        assert!(
            state.is_some_and(|state| state != 'Z'),
            "{direction:?}: the descendant ended ({state:?})"
        );
        signal(pid, Signal::KILL);
    }
}

#[test]
fn a_stopped_backward_stream_shows_the_chunk_bytes_that_the_client_dropped() {
    // `head -c 10` reads the first 10 bytes that the command writes from the
    // client's stdout, and exits, and this test lets the command write 5
    // more. The client's write of them fails with `EPIPE`, which stops the
    // backward stream on the client (DESIGN.md §6.6). A wrapper of the
    // control session copies what the client sends, and holds it after the
    // version and the features, so STOP does not reach the server yet. Once
    // the copy holds STOP, this test lets the command write its last 5
    // bytes, which the server sends in a CHUNK of their own, and once a copy
    // of the data session holds that CHUNK, the wrapper lets STOP go. The
    // client drops that CHUNK undecoded. Without compression, each write of
    // the command goes out as one LIT frame, 5 bytes longer than the write,
    // so the stdout line is exact: the server read 20 bytes, the client
    // decoded the frames of the first two writes and dropped the third, and
    // its stdout took 10 bytes (DESIGN.md §8).
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&["--compression-level", "0"]);
        transfer.configure_receiver(direction, "");
        let (control, data, status) = (
            transfer.env.path("control-sent"),
            transfer.env.path("data-received"),
            transfer.env.path("status"),
        );
        let mut stop_gate = Gate::new(transfer.env.path("stop-gate"));
        wrap_ssh(
            &transfer,
            &format!(
                r#"if [ $session = control ]; then
    tee '{0}' | {{
        head -c 12
        read go <'{2}'
        exec cat
    }} | "$ssh" "$@"
    exit $?
fi
{{ "$ssh" "$@"; echo $? >'{3}'; }} | tee '{1}'
exit "$(cat '{3}')""#,
                control.display(),
                data.display(),
                stop_gate.path.display(),
                status.display()
            ),
        );
        let (mut more, mut last) = fifos(&transfer);
        let script = writes_then_waits(&more, &last, "printf fghij");
        let invocation = transfer.invocation_in(direction, &[EXACT], &refs(&args(&[&script])));
        let mut run = HeadOfStdout::spawn(&invocation, transfer.env.path("."));
        assert_eq!(run.wait_for_head(), b"0123456789");
        more.release();
        // Without compression, after its version and its features, the
        // client sends nothing on the control session but STOP: the output
        // is too short for a query.
        let stop = frame(STOP, b"");
        wait_for_bytes(&control, std::slice::from_mut(&mut run.shell), |sent| {
            sent.len() >= 12 + stop.len()
        });
        assert_eq!(
            std::fs::read(&control).unwrap()[12..],
            stop,
            "{direction:?}"
        );
        last.release();
        wait_for_bytes(&data, std::slice::from_mut(&mut run.shell), |received| {
            holds(received, &frame(LIT, b"fghij"))
        });
        stop_gate.release();
        let output = succeeded(run.output());
        assert_eq!(
            summary_line(&output, "hashsync: stdout"),
            "hashsync: stdout -- 20 B read, pipe closed, 25 B wire, 10 B dropped, 10 B written",
            "{direction:?}"
        );
    }
}

#[test]
fn a_consumer_that_exits_early_ends_the_run_with_the_status_of_the_command() {
    // The command writes without end, as `yes` does, and the consumer of
    // the client's stdout reads 1 MiB of it, then exits, as `head -c 1M`
    // does. The client's next write to stdout fails with EPIPE, which stops
    // the backward stream on both hosts, and the server closes the
    // command's stdout, so `yes` dies of SIGPIPE: the run ends with its
    // status, `128 + 13`, as a local `yes | head -c 1M` does for `yes`
    // (DESIGN.md §6.2 and §6.6). The stdout line shows the stream as
    // stopped (DESIGN.md §8). A run that did not stop the stream would never
    // end. The client sees that its stdout is broken only at its next write
    // to it, so the stream must keep coming: compression would turn the
    // output of `yes` into a few rare CHUNK frames. Small spools and
    // lookahead keep the bytes that `yes` writes meanwhile few.
    let options = [
        "--compression-level",
        "0",
        "--spool-limit",
        "1MiB",
        "--lookahead",
        "64KiB",
    ];
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&options);
        transfer.configure_receiver(direction, "");
        let client = transfer
            .invocation_in(direction, &[EXACT], &refs(&args(&["yes"])))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut client = KillOnDrop(client);
        let mut stdout = client.0.stdout.take().unwrap();
        let mut head = vec![0u8; 1 << 20];
        stdout.read_exact(&mut head).unwrap();
        assert!(head.chunks(2).all(|pair| pair == b"y\n"), "{direction:?}");
        drop(stdout);
        let output = output_of(client);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(128 + 13),
            "{direction:?}:\n{stderr}"
        );
        let line = summary_line(&output, "hashsync: stdout -- ");
        assert!(
            line.contains(" read, pipe closed, "),
            "{direction:?}: {line}"
        );
        let [read, _, _, written] = line_counts(&line)[..] else {
            panic!("unexpected summary line: {line}");
        };
        assert!(
            (1 << 20) <= written && written <= read,
            "{direction:?}: {line}"
        );
    }
}

#[test]
fn the_summary_of_a_stopped_backward_stream_counts_what_reached_stdout() {
    // The command writes 16 MiB that do not compress, and the consumer of
    // the client's stdout reads 1 MiB of it, then exits. The client's next
    // write to stdout fails with EPIPE, which stops the backward stream
    // (DESIGN.md §6.6). The stdout line then gives the bytes that the
    // server read, `pipe closed`, the CHUNK bytes that the client decoded
    // and those that it dropped, which together are every CHUNK byte that
    // the server sent, and the bytes that stdout took (DESIGN.md §8). With
    // compression, the decoded bytes are those of the zstd frame that the
    // decoder consumed. `tee` copies what the server sends on the data
    // session. The command ends with SIGPIPE, or before the stop when its
    // output fits in the buffers of the server.
    let payload = random(16 << 20, 70);
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&["--spool-limit", "1MiB"]);
        transfer.configure_receiver(direction, "");
        let path = transfer.env.path("payload");
        std::fs::write(&path, &payload).unwrap();
        let (captured, status) = (transfer.env.path("captured"), transfer.env.path("status"));
        wrap_ssh(
            &transfer,
            &format!(
                r#"{{ "$ssh" "$@"; echo $? >'{1}'.$session; }} | tee '{0}'.$session
exit "$(cat '{1}'.$session)""#,
                captured.display(),
                status.display()
            ),
        );
        let script = format!("cat '{}'", path.display());
        let client = transfer
            .invocation_in(direction, &[EXACT], &refs(&args(&[&script])))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut client = KillOnDrop(client);
        let mut stdout = client.0.stdout.take().unwrap();
        let mut head = vec![0u8; 1 << 20];
        stdout.read_exact(&mut head).unwrap();
        assert!(
            head == payload[..1 << 20],
            "{direction:?}: the output differs"
        );
        drop(stdout);
        let output = output_of(client);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            matches!(output.status.code(), Some(0 | 141)),
            "{direction:?}:\n{stderr}"
        );
        let line = summary_line(&output, "hashsync: stdout -- ");
        assert!(
            line.contains(" read, pipe closed, "),
            "{direction:?}: {line}"
        );
        let [read, decoded, dropped, written] = line_counts(&line)[..] else {
            panic!("unexpected summary line: {line}");
        };
        assert!(
            (1 << 20) <= written && written <= read && read <= payload.len(),
            "{direction:?}: {line}"
        );
        let data = std::fs::read(format!("{}.data", captured.display())).unwrap();
        assert_eq!(
            decoded + dropped,
            chunk_payload_bytes(&data),
            "{direction:?}: {line}"
        );
    }
}

#[test]
fn the_clients_index_closes_at_the_stop_of_the_backward_stream() {
    // When the backward stream is deduplicated, the client's lookups end at
    // its stop, and its index closes then, while the run goes on (DESIGN.md
    // §6.4 and §6.6). With both streams deduplicated, the index that closes
    // is the one that the client opened again at its HELLO, after it
    // released the index of its update (DESIGN.md §4.4). The command runs
    // `writes_then_waits`: `head -c 10` reads its first 10 bytes from the
    // client's stdout and exits, and this test lets the command write 5
    // more, whose write to stdout stops the stream. Then an `update` of the
    // client's index must get in while the command still waits; only after
    // it does this test let the command end. A client that kept its index
    // until the end of the run would make `update` wait for the run, which
    // waits for this test: the test would hang.
    for both in [false, true] {
        let transfer = Transfer::new(&["--compression-level", "0"]);
        transfer.env.write("tree/blocks", &tiled(16, 46));
        let (mut more, mut last) = fifos(&transfer);
        let script = writes_then_waits(&more, &last, "exit 0");
        let words = args(&[&script]);
        let command = refs(&words);
        let invocation = if both {
            transfer.configure_server("");
            transfer.both(&[EXACT], &command)
        } else {
            transfer.invocation(&[EXACT], &command)
        };
        let mut run = HeadOfStdout::spawn(&invocation, transfer.env.path("."));
        let client = run.client();
        assert_eq!(run.wait_for_head(), b"0123456789");
        more.release();
        let update = transfer
            .env
            .command(&["update"])
            .stdout(Stdio::null())
            .output()
            .unwrap();
        let errors = String::from_utf8_lossy(&update.stderr);
        assert!(update.status.success(), "{both}: update failed:\n{errors}");
        let state = proc_state(format!("/proc/{client}"));
        assert!(
            state.is_some_and(|state| state != 'Z'),
            "{both}: the run ended before the command"
        );
        last.release();
        let output = succeeded(run.output());
        assert_eq!(
            summary_line(&output, "hashsync: stdout"),
            "hashsync: stdout -- 15 B read, pipe closed, 25 B wire, 0 B dropped, 10 B written",
            "{both}"
        );
    }
}

/// The bytes of the CHUNK payloads in `session`, the bytes that one
/// direction of the data session carried: the version and the features,
/// then whole frames, and the list bytes that follow HASHLIST and HASHDELTA
/// outside any frame. Both payloads start with the target's ID, and the
/// one of HASHDELTA then has the base's, 32 bytes each with BLAKE3
/// (DESIGN.md §6.2).
fn chunk_payload_bytes(session: &[u8]) -> usize {
    let mut rest = &session[version_and_features(FEATURES).len()..];
    let mut chunks = 0;
    while !rest.is_empty() {
        let length = u32::from_le_bytes(rest[1..5].try_into().unwrap()) as usize;
        let payload = &rest[5..5 + length];
        let u64_at = |at: usize| u64::from_le_bytes(payload[at..at + 8].try_into().unwrap());
        let follows = match rest[0] {
            CHUNK => {
                chunks += length;
                0
            }
            HASHLIST => u64_at(32),
            HASHDELTA => u64_at(64) + u64_at(72),
            _ => 0,
        };
        rest = &rest[5 + length + follows as usize..];
    }
    chunks
}

#[test]
fn both_directions_of_the_control_session_are_compressed() {
    // After the version and the features, the queries and the STOP of the
    // forward stream at the command's exit form one zstd stream, and the
    // answers another. No frame ends either stream: the closure of the
    // session does (DESIGN.md §6.2). `tee` copies both directions of the
    // control session. Every block of the output is queried once, in
    // order, and answered yes. STOP can come between any two queries, and
    // as the server does not wait for its control writer at the end, the
    // closure can also cut it off (DESIGN.md §6.6 and §6.9).
    let transfer = Transfer::new(&[]);
    let blocks = tiled(16, 29);
    transfer.env.write("tree/blocks", &blocks);
    let captured = transfer.env.path("captured");
    wrap_ssh(
        &transfer,
        &format!(
            r#"if [ $session = control ]; then
    tee '{0}'.answers | {{ "$ssh" "$@"; echo $? >'{0}'.status; }} | tee '{0}'.queries
    exit "$(cat '{0}'.status)"
fi
exec "$ssh" "$@""#,
            captured.display(),
        ),
    );
    let command = args(&["cat", transfer.env.path("tree/blocks").to_str().unwrap()]);
    let output = succeeded(transfer.run(&[BATCH], &refs(&command)));
    assert_eq!(output.stdout, blocks);
    assert_eq!(summary(&output), [blocks.len(), 0, blocks.len(), 0]);

    let offsets: Vec<u64> = (0..16).map(|i| (i * BLOCK) as u64).collect();
    // The frames of one direction, through the end of the copy, which
    // comes inside the zstd frame.
    let decode = |direction: &str| {
        let stream = std::fs::read(format!("{}.{direction}", captured.display())).unwrap();
        let start = version_and_features(FEATURES);
        assert_eq!(stream[..start.len()], start, "{direction}");
        let mut stream = zstd::stream::read::Decoder::new(&stream[start.len()..]).unwrap();
        let mut frames = Vec::new();
        loop {
            match try_recv_frame(&mut stream) {
                Ok(frame) => frames.push(frame),
                Err(error) => {
                    assert_eq!(
                        error.kind(),
                        std::io::ErrorKind::UnexpectedEof,
                        "{direction}"
                    );
                    return frames;
                }
            }
        }
    };
    let mut queries = decode("queries");
    let stops = queries.iter().filter(|(kind, _)| *kind == STOP).count();
    assert!(stops <= 1, "{stops} STOP frames");
    queries.retain(|frame| *frame != (STOP, Vec::new()));
    assert_eq!(queries.len(), offsets.len());
    for ((kind, query), (offset, block)) in queries
        .into_iter()
        .zip(offsets.iter().zip(blocks.chunks(BLOCK)))
    {
        assert_eq!(kind, QUERY);
        assert_eq!(query[..8], offset.to_le_bytes());
        assert_eq!(query[8..], *blake3::hash(block).as_bytes());
    }
    let answers = decode("answers");
    assert_eq!(answers.len(), offsets.len());
    let mut answered: Vec<u64> = answers
        .into_iter()
        .map(|(kind, answer)| {
            assert_eq!((kind, answer.len(), answer[8]), (ANSWER, 9, 1), "a yes");
            u64::from_le_bytes(answer[..8].try_into().unwrap())
        })
        .collect();
    answered.sort_unstable();
    assert_eq!(answered, offsets);
}

/// True once both SSH children of the client `pid` have exited: the
/// client does not reap them, so both are zombies until it exits.
fn sessions_exited(pid: u32) -> bool {
    let exited = children(pid)
        .into_iter()
        .filter(|child| proc_state(format!("/proc/{child}")) == Some('Z'))
        .count();
    exited == 2
}

#[test]
fn a_termination_signal_during_the_end_decides_the_status() {
    // After the control session's SSH exits, a process that inherited its
    // stdout keeps the pipe open while the hold file exists, until the test
    // ends and removes its directory. So the client waits in its end
    // (DESIGN.md §6.9). The signal thread still runs then: SIGTERM ends the
    // run with `128 + signal` (DESIGN.md §6.8).
    let transfer = Transfer::new(&[]);
    let hold = transfer.env.path("hold");
    std::fs::write(&hold, b"").unwrap();
    wrap_ssh(
        &transfer,
        &format!(
            r#""$ssh" "$@"
status=$?
if [ $session = control ]; then
    ( while [ -e '{}' ]; do sleep 0.01; done ) </dev/null 2>/dev/null &
fi
exit $status"#,
            hold.display()
        ),
    );
    let mut client = transfer
        .invocation(&[], &refs(&args(&["true"])))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = client.id();
    while !sessions_exited(pid) {
        assert!(client.try_wait().unwrap().is_none(), "the run ended");
        std::thread::sleep(Duration::from_millis(1));
    }
    kill_process(Pid::from_raw(pid as i32).unwrap(), Signal::TERM).unwrap();
    let output = client.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(143), "stderr:\n{stderr}");
    assert!(
        stderr.contains("terminated by SIGTERM"),
        "stderr:\n{stderr}"
    );
}

/// Wait until the thread `name` of the process `pid` sleeps in the system
/// call `nr` again after something woke it, polling with no deadline. A
/// woken thread shows state `R` until it has run and sleeps again; the
/// kernel can still show its old system call while it waits to run. So the
/// state comes first, then the system call. Reading the `syscall` file
/// needs the ptrace access that a parent has. Fail when the process has
/// ended.
fn wait_until_asleep_again_in(pid: u32, name: &str, nr: u64) {
    loop {
        assert_ne!(
            proc_state(format!("/proc/{pid}")),
            Some('Z'),
            "the process ended"
        );
        for task in std::fs::read_dir(format!("/proc/{pid}/task")).unwrap() {
            let task = task.unwrap().path();
            // A thread that ends meanwhile has no files.
            let Ok(comm) = std::fs::read_to_string(task.join("comm")) else {
                continue;
            };
            if comm.trim_end() != name || proc_state(&task) != Some('S') {
                continue;
            }
            let Ok(syscall) = std::fs::read_to_string(task.join("syscall")) else {
                continue;
            };
            let sleeping = syscall
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok());
            if sleeping == Some(nr) {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn stop_before_the_client_starts_its_forward_stream_ends_it_before_a_read() {
    // The command exits at once, so the server stops the forward stream
    // right after the list exchange. A wrapper of the control session passes
    // STOP on, and a wrapper of the data session holds CACHE_RESULT, which
    // follows the server's version, features and HELLO, until this test
    // releases it. The delivery of STOP woke the client's control reader, so
    // once that thread sleeps in a read again, it has taken STOP and raised
    // the stop. Only then does CACHE_RESULT pass. So the client starts its
    // forward stream after the stop, which exists from before the sessions
    // start, and its stdin reader ends before it reads anything (DESIGN.md
    // §6.6): the bytes of stdin stay for their next reader. Without
    // compression, STOP is the first frame of the control session after the
    // version and the features: the index is empty, so no QUERY comes first.
    // The server's cache is empty too, so its HELLO advertises no list, and
    // takes 25 bytes.
    let transfer = Transfer::new(&["--compression-level", "0"]);
    let stopped = transfer.env.path("stopped");
    let status = transfer.env.path("status");
    let mut cache_result_gate = Gate::new(transfer.env.path("cache-result-gate"));
    wrap_ssh(
        &transfer,
        &format!(
            r#"if [ $session = control ]; then
    {{ "$ssh" "$@"; echo $? >'{2}'.control; }} | {{ head -c 17; : >'{0}'; exec cat; }}
else
    {{ "$ssh" "$@"; echo $? >'{2}'.data; }} | {{
        head -c 37
        read go <'{1}'
        exec cat
    }}
fi
exit "$(cat '{2}'.$session)""#,
            stopped.display(),
            cache_result_gate.path.display(),
            status.display()
        ),
    );
    let (stdin, mut producer) = std::io::pipe().unwrap();
    let mut unread = stdin.try_clone().unwrap();
    producer.write_all(b"stdin that nothing reads").unwrap();
    drop(producer);
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&["true"])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut client = KillOnDrop(client);
    // No deadline: a server that sends no STOP hangs the test.
    wait_for_file(&stopped, std::slice::from_mut(&mut client));
    wait_until_asleep_again_in(client.0.id(), "control-reader", libc::SYS_read as u64);
    cache_result_gate.release();
    let output = succeeded(output_of(client));
    assert!(output.stdout.is_empty());
    let mut left = Vec::new();
    unread.read_to_end(&mut left).unwrap();
    assert_eq!(left, b"stdin that nothing reads");
}

#[test]
fn end_stops_the_forward_stream_when_it_comes_before_stop() {
    // A wrapper of the control session passes the server's version and
    // features, then holds the rest, STOP included, until the data
    // session's SSH has exited. So END reaches the client before STOP, and
    // END must stop the forward stream (DESIGN.md §6.6): stdin stays open,
    // so nothing else ends the stdin reader. A client that waited for STOP
    // would never close the data session, and the server never exit, so
    // the test would hang. The late STOP then changes nothing. The forward
    // stream is plain in a pull, and deduplicated in a push.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&["--compression-level", "0"]);
        transfer.configure_receiver(direction, "");
        let status = transfer.env.path("status");
        // The control session waits here until the wrapper of the data
        // session lets it go on, once its SSH has exited.
        let data_ended = Gate::new(transfer.env.path("data-ended"));
        wrap_ssh(
            &transfer,
            &format!(
                r#"if [ $session = control ]; then
    {{ "$ssh" "$@"; echo $? >'{1}'.control; }} | {{
        head -c 12
        read go <'{0}'
        exec cat
    }}
    exit "$(cat '{1}'.control)"
fi
"$ssh" "$@"
status=$?
echo go >'{0}'
exit $status"#,
                data_ended.path.display(),
                status.display()
            ),
        );
        let (stdin, producer) = std::io::pipe().unwrap();
        let output = transfer
            .invocation_in(direction, &[EXACT], &refs(&args(&["true"])))
            .stdin(stdin)
            .output()
            .unwrap();
        succeeded(output);
        drop(producer);
    }
}

#[test]
fn end_stops_a_forward_scan_that_waits_for_an_answer() {
    // In a push, stdin carries an indexed block and a block that is not
    // indexed, and then stays open, so the client's scanner queries the
    // first block and waits for the answer. With a lookahead of one block
    // and batch delivery, the two blocks fill the scanner's buffer and no
    // idle point comes, so the scanner waits for that answer alone: neither
    // more bytes nor the end of its input would wake it. A wrapper of the
    // control session copies what the client sends, and holds what the
    // server sends after its version and features, the answer and STOP
    // included, until the data session's SSH has exited. Once the copy
    // holds the QUERY, the command exits. END then reaches a scanner that
    // waits for an answer that cannot come, and only the stop that END
    // raises can end it (DESIGN.md §6.3 and §6.6): a scanner that did not
    // wait for the stop would never let the client close the data session,
    // and the test would hang. Without compression, the QUERY is a plain
    // frame after the 12 bytes of the version and the features.
    let transfer = Transfer::new(&["--compression-level", "0"]);
    transfer.configure_server("");
    let block = tiled(1, 47);
    transfer.env.write("server-tree/block", &block);
    let (copied, status) = (
        transfer.env.path("control-copy"),
        transfer.env.path("status"),
    );
    // The control session waits here until the wrapper of the data session
    // lets it go on, once its SSH has exited.
    let data_ended = Gate::new(transfer.env.path("data-ended"));
    wrap_ssh(
        &transfer,
        &format!(
            r#"if [ $session = control ]; then
    tee '{0}' | {{ "$ssh" "$@"; echo $? >'{2}'.control; }} | {{
        head -c 12
        read go <'{1}'
        exec cat
    }}
    exit "$(cat '{2}'.control)"
fi
"$ssh" "$@"
status=$?
echo go >'{1}'
exit $status"#,
            copied.display(),
            data_ended.path.display(),
            status.display()
        ),
    );
    let mut go = Gate::new(transfer.env.path("go"));
    let script = format!("read go <'{}'", go.path.display());
    let (stdin, mut producer) = std::io::pipe().unwrap();
    producer.write_all(&block).unwrap();
    producer.write_all(&tiled(1, 48)).unwrap();
    let lookahead = BLOCK.to_string();
    let client = transfer
        .push(
            &[EXACT, "--batch-stdin", "--lookahead", &lookahead],
            &refs(&args(&[&script])),
        )
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut client = KillOnDrop(client);
    // No deadline: a scanner that queries nothing hangs the test.
    wait_for_bytes(&copied, std::slice::from_mut(&mut client), |copy| {
        copy.len() > 12
    });
    go.release();
    succeeded(output_of(client));
    drop(producer);
}

#[test]
fn closing_the_control_session_before_end_is_fatal_also_after_completing() {
    // A fake server sends a whole backward stream, but no END, and closes
    // the control session once the client's receiver has entered
    // Completing, which ends the client's lookup workers (DESIGN.md §6.4).
    // Before END, that closure is still the loss of the server, which ends
    // the run (DESIGN.md §6.9). The fake server holds STREAM_END until the
    // test has seen the lookup workers, and closes the control session only
    // once they have ended, so the order of these events does not depend on
    // timing.
    let transfer = Transfer::new(&[]);
    let start = version_and_features(FEATURES);
    let frames = fake_frames(b"output");
    let stream_end = frame(STREAM_END, b"");
    let (before, after) = frames.split_at(frames.len() - stream_end.len());
    assert_eq!(after, stream_end);
    let mut stream_end_gate = Gate::new(transfer.env.path("stream-end-gate"));
    let mut close_gate = Gate::new(transfer.env.path("close-gate"));
    let path = |name: &str| {
        let path = transfer.env.path(name);
        path.display().to_string()
    };
    std::fs::write(path("part1"), [&start[..], before].concat()).unwrap();
    std::fs::write(path("part2"), after).unwrap();
    std::fs::write(path("control"), start).unwrap();
    let server = transfer.env.path("fake-server");
    std::fs::write(
        &server,
        format!(
            r#"#!/bin/sh
if [ "$1" = serve ]; then
    cat '{0}'
    read go <'{2}'
    cat '{1}'
    exec cat 3>&1 >/dev/null
fi
cat '{3}'
read go <'{4}'
exec cat >/dev/null
"#,
            path("part1"),
            path("part2"),
            stream_end_gate.path.display(),
            path("control"),
            close_gate.path.display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();
    transfer.set_hashsync_path(server.as_os_str());
    let client = transfer
        .invocation(
            &[EXACT, "--compression-level", "0"],
            &refs(&args(&["true"])),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let client = KillOnDrop(client);
    let pid = client.0.id();
    // The lookup workers start before the sessions, and end at Completing,
    // which STREAM_END causes.
    wait_for_thread(pid, "lookup0", true);
    stream_end_gate.release();
    wait_for_thread(pid, "lookup0", false);
    close_gate.release();
    let output = output_of(client);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr:\n{stderr}");
    assert!(
        stderr.contains("the server disappeared"),
        "stderr:\n{stderr}"
    );
}

#[test]
fn a_client_whose_stdout_is_slow_after_end_writes_the_whole_backward_stream() {
    // The consumer reads nothing until the data session has ended, which
    // happens once the server has exited, after END, when the client has
    // closed the session's stdin. The client's spool has no limit, so it
    // holds the backward stream meanwhile, and the client then writes all
    // of it to stdout before it exits (DESIGN.md §6.9).
    let transfer = Transfer::new(&["--spool-limit", "unlimited", "--compression-level", "0"]);
    let payload = random(16 << 20, 41);
    let source = transfer.env.path("payload");
    std::fs::write(&source, &payload).unwrap();
    let ended = transfer.env.path("data-ended");
    wrap_ssh(
        &transfer,
        &format!(
            r#""$ssh" "$@"
status=$?
if [ $session = data ]; then : >'{}'; fi
exit $status"#,
            ended.display()
        ),
    );
    let command = args(&["cat", source.to_str().unwrap()]);
    let client = transfer
        .invocation(&[EXACT], &refs(&command))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut client = KillOnDrop(client);
    // No deadline: a client that waits for its consumer before END hangs
    // the test.
    while !ended.exists() {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        client.0.try_wait().unwrap().is_none(),
        "the client ended before its consumer read stdout"
    );
    let output = succeeded(output_of(client));
    assert!(output.stdout == payload, "the output differs");
}

/// Wait until the main thread of `pid` sleeps in a write to its stderr:
/// `write` with fd 2, as its `syscall` file shows; no deadline. Fail when
/// the process has ended.
fn wait_for_stderr_write(pid: u32) {
    let path = format!("/proc/{pid}/syscall");
    loop {
        assert_ne!(
            proc_state(format!("/proc/{pid}")),
            Some('Z'),
            "the process ended"
        );
        let syscall = std::fs::read_to_string(&path).unwrap();
        let fields: Vec<&str> = syscall.split_whitespace().collect();
        if fields.first().and_then(|nr| nr.parse().ok()) == Some(libc::SYS_write as u64)
            && fields.get(1) == Some(&"0x2")
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn a_reader_that_waits_during_a_runs_update_gets_in_while_the_run_goes_on() {
    // After its update, the receiver of a run turns its exclusive lock into
    // a shared one, so a `stats` that waits for the lock during the update
    // gets in once the update ends, while the run goes on (DESIGN.md §4.4).
    // The receiver is the client in a pull, and the server in a push, whose
    // stderr is the client's. That stderr is a full pipe, so the note at the
    // end of the update, which the receiver writes while it still holds its
    // exclusive lock, waits in its write. Meanwhile `stats` starts, and
    // waits for the lock. Then this test empties the pipe, and `stats` must
    // end before the command, which waits for this test. A run that kept
    // its exclusive lock would make `stats` wait for the run, and hang the
    // test. The index exists already, so the update writes no note before
    // that one. The receiver's stream stays open meanwhile: the command
    // writes nothing before it exits, and stdin stays open; else the lookups
    // would end, and close the index, whatever its lock.
    for direction in DIRECTIONS {
        let transfer = Transfer::new(&[]);
        transfer.configure_receiver(direction, "");
        let indexed = transfer.indexed(direction, "blocks");
        transfer.env.write(indexed, &tiled(16, 42));
        transfer.update_receiver(direction, &[]);
        let go = transfer.env.path("go");
        mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
        let (mut stderr, filled) = std::io::pipe().unwrap();
        rustix::fs::fcntl_setfl(&filled, OFlags::NONBLOCK).unwrap();
        while rustix::io::write(&filled, &[b'.'; 4096]).is_ok() {}
        rustix::fs::fcntl_setfl(&filled, OFlags::empty()).unwrap();
        let script = format!("read go <'{}'", go.display());
        let (stdin, producer) = std::io::pipe().unwrap();
        let client = transfer
            .invocation_in(direction, &[EXACT], &refs(&args(&[&script])))
            .stdin(stdin)
            .stdout(Stdio::piped())
            .stderr(filled)
            .spawn()
            .unwrap();
        let mut client = KillOnDrop(client);
        let (receiver, config, index) = match direction {
            Direction::Pull => (
                client.0.id(),
                transfer.env.config_path(),
                transfer.env.index_path(),
            ),
            Direction::Push => (
                server_of(client.0.id()),
                transfer.server_config(),
                transfer.server_index(),
            ),
        };
        wait_for_stderr_write(receiver);
        let lock = ofd_lock(receiver, &index.join("hashsync.lock"));
        assert_eq!(lock, Some("WRITE"), "{direction:?}: no exclusive lock");
        let stats = Command::new(env!("CARGO_BIN_EXE_hashsync"))
            .arg("--config")
            .arg(config)
            .arg("stats")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stats = KillOnDrop(stats);
        wait_for_lock_wait(stats.0.id());
        let drained = std::thread::spawn(move || {
            let mut text = Vec::new();
            stderr.read_to_end(&mut text).unwrap();
            text
        });
        let stats = succeeded(output_of(stats));
        let report = String::from_utf8(stats.stdout).unwrap();
        assert!(report.contains("blocks:           16\n"), "{report}");
        assert!(
            client.0.try_wait().unwrap().is_none(),
            "{direction:?}: the run ended before stats did"
        );
        std::fs::write(&go, b"go\n").unwrap();
        let status = client.0.wait().unwrap();
        let text = String::from_utf8_lossy(&drained.join().unwrap()).into_owned();
        assert!(status.success(), "{direction:?}: run failed:\n{text}");
        assert!(text.contains("updated index"), "{text}");
        drop(producer);
    }
}

/// The server of the client `pid`: its child for the data session, once it
/// runs `hashsync serve`, which the bootstrap execs (DESIGN.md §6.1). Polls
/// with no deadline, and fails once the client has ended.
fn server_of(pid: u32) -> u32 {
    loop {
        let state = proc_state(format!("/proc/{pid}"));
        assert!(
            state.is_some_and(|state| state != 'Z'),
            "the client ended before its server started"
        );
        for child in children(pid) {
            let cmdline = std::fs::read(format!("/proc/{child}/cmdline")).unwrap_or_default();
            if cmdline.split(|&byte| byte == 0).any(|arg| arg == b"serve") {
                return child;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn an_update_waits_for_the_lookups_and_gets_in_while_the_run_writes_stdout() {
    // The command writes a part of its output, then waits for this test.
    // Meanwhile `update` waits for the run's lock: the run's lookup workers
    // use the index until the backward stream ends. Once the command
    // writes the rest and exits, the backward stream ends, the lookups end
    // at Completing, and the index closes with its lock (DESIGN.md §4.4 and
    // §6.4). So `update` ends while the client still holds most of the
    // stream, which its consumer has not read yet: the client's spool has
    // no limit. A run that kept its lock to its end would make `update`
    // wait for the consumer, which waits for `update`, and hang the test.
    let transfer = Transfer::new(&["--spool-limit", "unlimited", "--compression-level", "0"]);
    transfer.env.write("tree/blocks", &tiled(16, 43));
    let head = random(4 << 20, 44);
    let rest = random(4 << 20, 45);
    transfer.env.write("head", &head);
    transfer.env.write("rest", &rest);
    let go = transfer.env.path("go");
    mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!(
        "cat '{}'; read go <'{}'; cat '{}'",
        transfer.env.path("head").display(),
        go.display(),
        transfer.env.path("rest").display()
    );
    let client = transfer
        .invocation(&[EXACT], &refs(&args(&[&script])))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut client = KillOnDrop(client);
    // The open waits for the command to open the FIFO, after its first
    // part: the backward stream has not ended.
    let mut fifo = File::options().write(true).open(&go).unwrap();
    let update = transfer
        .env
        .command(&["update"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut update = KillOnDrop(update);
    wait_for_lock_wait(update.0.id());
    fifo.write_all(b"go\n").unwrap();
    drop(fifo);
    let status = update.0.wait().unwrap();
    assert!(status.success(), "update failed");
    assert!(
        client.0.try_wait().unwrap().is_none(),
        "the client ended before its consumer read stdout"
    );
    let output = succeeded(output_of(client));
    assert!(output.stdout == [head, rest].concat(), "the output differs");
}

#[test]
fn the_command_of_a_push_starts_with_the_descriptor_limit_of_the_server() {
    // The server of a push raises its soft limit of descriptors for the
    // update of its index (DESIGN.md §4.6), but the command starts with
    // the limit that the server started with, as it would under SSH alone.
    // A wrapper of the server lowers that limit first.
    let limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    assert!(
        limit.maximum.is_none_or(|hard| hard > 256),
        "this test needs a hard limit of descriptors above 256"
    );
    let transfer = Transfer::new(&[]);
    transfer.configure_server("");
    let wrapper = transfer.env.path("hashsync-with-a-low-limit");
    let script = format!(
        "#!/bin/sh\nulimit -Sn 256\nexec '{}' \"$@\"\n",
        env!("CARGO_BIN_EXE_hashsync")
    );
    std::fs::write(&wrapper, script).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    transfer.set_hashsync_path(wrapper.as_os_str());
    let output = succeeded(transfer.run_push(&[], &refs(&args(&["ulimit -Sn"])), b""));
    assert_eq!(output.stdout, b"256\n");
}

#[test]
fn the_ssh_sessions_of_a_pull_start_with_the_descriptor_limit_of_the_client() {
    // The client of a pull raises its soft limit of descriptors for the
    // update of its index (DESIGN.md §4.6), but its SSH children start with
    // the limit that the client started with, as under ssh alone. Here the
    // fake ssh runs the server, and so the command, which inherits that
    // limit. A shell lowers it before it runs the client.
    let limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    assert!(
        limit.maximum.is_none_or(|hard| hard > 256),
        "this test needs a hard limit of descriptors above 256"
    );
    let transfer = Transfer::new(&[]);
    let client = transfer.invocation(&[EXACT], &refs(&args(&["ulimit -Sn"])));
    let mut invocation = Command::new("/bin/sh");
    invocation
        .args(["-c", "ulimit -Sn 256 && exec \"$0\" \"$@\""])
        .arg(client.get_program())
        .args(client.get_args())
        .stdin(Stdio::null());
    for (key, value) in client.get_envs() {
        match value {
            Some(value) => invocation.env(key, value),
            None => invocation.env_remove(key),
        };
    }
    let output = succeeded(invocation.output().unwrap());
    assert_eq!(output.stdout, b"256\n");
}

#[test]
fn a_push_whose_command_updates_the_servers_index_after_its_stdin_ends() {
    // As `tar -x && hashsync update` does, the command writes its stdin to
    // a file in the root of the server's index, and then updates that
    // index. The lookups of the forward stream end at its STREAM_END, and
    // the index closes with them, so the update gets its exclusive lock
    // while the run goes on (DESIGN.md §4.4 and §6.4). A server that kept
    // its index open until the end of the run would deadlock with its own
    // command: the test would hang.
    let transfer = Transfer::new(&[]);
    transfer.configure_server("");
    let blocks = tiled(16, 43);
    transfer.env.write("server-tree/blocks", &blocks);
    let received = transfer.env.path("server-tree/received");
    let script = format!(
        "cat >'{}' && '{}' update",
        received.display(),
        env!("CARGO_BIN_EXE_hashsync")
    );
    let input = [&b"head"[..], &blocks].concat();
    let output = succeeded(transfer.run_push(&[], &refs(&args(&[&script])), &input));
    assert_eq!(std::fs::read(&received).unwrap(), input);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("updated index"), "{stderr}");
    let index = common::IndexView::open(&transfer.server_index(), false);
    let paths: Vec<Vec<u8>> = index
        .paths()
        .into_iter()
        .map(|path| path.rel_path)
        .collect();
    assert!(paths.contains(&b"received".to_vec()), "{paths:?}");
}

#[test]
fn a_lookahead_below_the_block_size_of_the_servers_index_ends_a_push() {
    // In a push, the server's HELLO gives the block size of the stream,
    // here 64 KiB, and the run asks for a lookahead of 32 KiB: the client
    // reports the user error right after that HELLO, before it sends its
    // own, so the server does no work for the run, and the command never
    // runs (DESIGN.md §8). A copy of what the client sends on the data
    // session shows it: after the version and the features, REQUEST alone.
    // The client kills only the wrapper of the fake ssh, so the copy goes
    // on until the client has exited.
    let transfer = Transfer::new(&["--lookahead", "32KiB"]);
    transfer.configure_server("");
    let config = std::fs::read_to_string(transfer.server_config()).unwrap();
    let config = config.replace(
        &format!("block_size = {BLOCK}\n"),
        "block_size = \"64KiB\"\n",
    );
    std::fs::write(transfer.server_config(), config).unwrap();
    let (captured, copied) = (transfer.env.path("captured"), transfer.env.path("copied"));
    wrap_ssh(
        &transfer,
        &format!(
            r#"if [ $session = data ]; then
    {{ tee '{}'; : >'{}'; }} | "$ssh" "$@"
    exit $?
fi
exec "$ssh" "$@""#,
            captured.display(),
            copied.display()
        ),
    );
    let marker = transfer.env.path("command-ran");
    let command = format!(": >'{}'", marker.display());
    let output = transfer.run_push(&[], &refs(&args(&[&command])), b"input");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains(
            "--lookahead must be at least the block size of the server's index (65536 bytes)"
        ),
        "{stderr}"
    );
    // No deadline: the copy ends with the data session of the client.
    while !copied.exists() {
        std::thread::sleep(Duration::from_millis(1));
    }
    let sent = std::fs::read(&captured).unwrap();
    assert_eq!(frame_types(&sent), [REQUEST], "the client went on");
    assert!(!marker.exists(), "the command ran");
}

#[test]
fn a_lookahead_that_the_sender_cannot_hold_ends_the_run_before_the_command() {
    // The sender of a deduplicated stream reserves the scanner's buffer,
    // twice `lookahead + B`, and starts its probe threads before its HELLO:
    // settings that its host cannot provide end the run there, before the
    // command starts, and not in the middle of the stream (DESIGN.md §6.2).
    // A lookahead of 2^62 bytes needs a buffer above the largest
    // allocation, so the reservation fails on any host. A command that
    // started would be killed at once, before it could leave a mark of its
    // own, so the test watches the command's script: `exec` opens it.
    for direction in DIRECTIONS {
        let lookahead = (1u64 << 62).to_string();
        let transfer = Transfer::new(&["--lookahead", &lookahead]);
        transfer.configure_receiver(direction, "");
        let script = transfer.env.path("command");
        std::fs::write(&script, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let opens =
            inotify::init(inotify::CreateFlags::CLOEXEC | inotify::CreateFlags::NONBLOCK).unwrap();
        inotify::add_watch(&opens, &script, inotify::WatchFlags::OPEN).unwrap();
        let output = transfer
            .invocation_in(direction, &[], &[script.as_os_str()])
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{direction:?}: {stderr}");
        let sender = match direction {
            Direction::Pull => "hashsync (server): ",
            Direction::Push => "hashsync: ",
        };
        let message = format!(
            "cannot reserve the scanner's buffer for a lookahead of {lookahead} bytes \
             (--lookahead): "
        );
        assert!(
            stderr.contains(&format!("{sender}{message}")),
            "{direction:?}: {stderr}"
        );
        let mut event = [0u8; 256];
        assert_eq!(
            read(&opens, &mut event),
            Err(Errno::AGAIN),
            "{direction:?}: the command started"
        );
    }
}

/// The types of the frames in `session`, the bytes of one direction of the
/// data session: the version and the features, then whole frames.
fn frame_types(session: &[u8]) -> Vec<u8> {
    let mut rest = &session[version_and_features(FEATURES).len()..];
    let mut types = Vec::new();
    while !rest.is_empty() {
        let length = u32::from_le_bytes(rest[1..5].try_into().unwrap()) as usize;
        types.push(rest[0]);
        rest = &rest[5 + length..];
    }
    types
}

#[test]
fn both_streams_are_deduplicated_against_the_indexes_of_their_receivers() {
    // With `--dedup-stdin --dedup-stdout`, the client's stdin is
    // deduplicated against the server's index, and the command's stdout
    // against the client's: the blocks that each receiver holds become
    // references (DESIGN.md §6.6). Each host updates its index for the
    // run, or with `--no-update` uses it as it is. Each host keeps the list
    // of the stream that it receives, and the list of the stream that it
    // sends, which the other host keeps too (DESIGN.md §5.6).
    for no_update in [false, true] {
        let transfer = Transfer::new(&[]);
        transfer.configure_server("");
        // Indexes of different sizes have lists of different sizes, so
        // that each summary line shows the list of its own stream.
        let client_blocks = tiled(32, 61);
        let server_blocks = tiled(48, 62);
        transfer.env.write("tree/blocks", &client_blocks);
        transfer.env.write("server-tree/blocks", &server_blocks);
        let mut flags = vec![EXACT, "--batch-stdin", "--batch-stdout"];
        if no_update {
            transfer.update_receiver(Direction::Pull, &[]);
            transfer.update_receiver(Direction::Push, &[]);
            flags.push("--no-update");
        }
        let received = transfer.env.path("received");
        let script = format!(
            "cat >'{}' && printf abc && cat '{}'",
            received.display(),
            transfer.env.path("tree/blocks").display()
        );
        let input = [&b"xy"[..], &server_blocks].concat();
        let path = transfer.env.path("input");
        std::fs::write(&path, &input).unwrap();
        let output = transfer
            .both(&flags, &refs(&args(&[&script])))
            .stdin(File::open(&path).unwrap())
            .output()
            .unwrap();
        let output = succeeded(output);
        assert!(
            std::fs::read(&received).unwrap() == input,
            "{no_update}: the command's stdin differs"
        );
        let expected = [&b"abc"[..], &client_blocks].concat();
        assert!(output.stdout == expected, "{no_update}: the output differs");
        let stdout = deduplicated_stream(&output, "stdout");
        let counts = [expected.len(), 3, client_blocks.len(), 0];
        assert_eq!(stdout.bytes, counts, "{no_update}");
        let stdin = deduplicated_stream(&output, "stdin");
        let counts = [input.len(), 2, server_blocks.len(), 0];
        assert_eq!(stdin.bytes, counts, "{no_update}");
        // Both updates write a note, the first one "rebuilt index", a later
        // one "updated index". With `--no-update`, neither host updates.
        let stderr = String::from_utf8_lossy(&output.stderr);
        for host in ["hashsync", "hashsync (server)"] {
            let updated = ["rebuilt index", "updated index"]
                .iter()
                .any(|note| stderr.contains(&format!("{host}: {note}")));
            assert_eq!(updated, !no_update, "{host}:\n{stderr}");
        }
        let client_list = transfer.cache_files("client", "local-hashlists");
        let server_list = transfer.cache_files("server", "local-hashlists");
        assert_eq!(client_list.len(), 1, "{client_list:?}");
        assert_eq!(server_list.len(), 1, "{server_list:?}");
        assert_ne!(client_list, server_list);
        // The first exchange of each stream sends its full list, which its
        // receiver keeps: the stdout line shows the client's list, and the
        // stdin line the server's (DESIGN.md §8).
        let list = |side: &str, name: &str| {
            let dir = transfer.peer_dir(side, "local-hashlists").unwrap();
            std::fs::read(dir.join(name)).unwrap().len() - "hsynlist\x01".len()
        };
        let lists = [
            list("client", &client_list[0]),
            list("server", &server_list[0]),
        ];
        assert_ne!(lists[0], lists[1]);
        let full = |line: &DeduplicatedLine| (line.hashlist, line.delta);
        assert_eq!(full(&stdout), (Some(lists[0]), None), "{no_update}");
        assert_eq!(full(&stdin), (Some(lists[1]), None), "{no_update}");
        let sent = transfer.cache_files("server", "remote-hashlists");
        assert_eq!(sent, client_list);
        let sent = transfer.cache_files("client", "remote-hashlists");
        assert_eq!(sent, server_list);
    }
}

#[test]
fn the_forward_stream_stops_while_the_deduplicated_backward_stream_goes_on() {
    // With both streams deduplicated, the command reads a part of its
    // stdin, closes it, and waits for this test. The next bytes of stdin
    // then fail at the command's stdin with `EPIPE`, which stops the
    // forward stream, and the client's stdin reader ends (DESIGN.md §6.6).
    // The backward stream goes on: the command then writes blocks of the
    // client's index, which reach the client as references, and the run
    // ends with the command's status.
    let transfer = Transfer::new(&[]);
    transfer.configure_server("");
    let blocks = tiled(16, 63);
    transfer.env.write("tree/blocks", &blocks);
    let closed = transfer.env.path("closed");
    let go = transfer.env.path("go");
    mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!(
        "head -c 4 >/dev/null; exec <&-; : >'{}'; read go <'{}'; cat '{}'",
        closed.display(),
        go.display(),
        transfer.env.path("tree/blocks").display()
    );
    let (stdin, mut producer) = std::io::pipe().unwrap();
    let client = transfer
        .both(&[EXACT, "--batch-stdout"], &refs(&args(&[&script])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut client = KillOnDrop(client);
    producer.write_all(b"abcd").unwrap();
    // The command reads these bytes from the client's stdin reader, so
    // that reader runs once the command has closed its stdin.
    wait_for_file(&closed, std::slice::from_mut(&mut client));
    producer.write_all(&random(64 << 10, 64)).unwrap();
    // The stdin stays open, so only the stop ends the reader.
    wait_for_thread(client.0.id(), "stdin-reader", false);
    std::fs::write(&go, b"go\n").unwrap();
    let output = succeeded(output_of(client));
    drop(producer);
    assert!(output.stdout == blocks, "the output differs");
    let stdout = deduplicated_stream(&output, "stdout");
    assert_eq!(stdout.bytes, [blocks.len(), 0, blocks.len(), 0]);
    let stdin = summary_line(&output, "hashsync: stdin -- ");
    assert!(stdin.contains(" read, pipe closed, "), "{stdin}");
}

#[test]
fn each_stream_uses_the_block_size_and_strong_hash_of_its_receiver() {
    // The client's index has blocks of 4 KiB and BLAKE3, the server's
    // blocks of 16 KiB and SHA-256. The HELLO of each receiver sets the
    // block size and the hashes of the stream that it receives (DESIGN.md
    // §6.2), so the blocks of each index become references in that
    // stream, at an offset that is a multiple of neither block size.
    let transfer = Transfer::new(&[]);
    transfer.env.mkdir("server-tree");
    let config = format!(
        "block_size = 16384\nstrong_hash = \"sha256\"\nindex_path = '{}'\n{}",
        toml_literal(transfer.server_index().as_os_str().as_bytes()),
        transfer.env.root("server-tree", "")
    );
    std::fs::create_dir_all(transfer.server_config().parent().unwrap()).unwrap();
    std::fs::write(transfer.server_config(), config).unwrap();
    let client_blocks = tiled(16, 65);
    let server_blocks = random(4 << 14, 66);
    transfer.env.write("tree/blocks", &client_blocks);
    transfer.env.write("server-tree/blocks", &server_blocks);
    let script = format!(
        "cat >/dev/null; printf abc; cat '{}'",
        transfer.env.path("tree/blocks").display()
    );
    let input = [&b"xy"[..], &server_blocks].concat();
    let path = transfer.env.path("input");
    std::fs::write(&path, &input).unwrap();
    let flags = [EXACT, "--batch-stdin", "--batch-stdout"];
    let output = transfer
        .both(&flags, &refs(&args(&[&script])))
        .stdin(File::open(&path).unwrap())
        .output()
        .unwrap();
    let output = succeeded(output);
    let expected = [&b"abc"[..], &client_blocks].concat();
    assert!(output.stdout == expected, "the output differs");
    let stdout = deduplicated_stream(&output, "stdout");
    assert_eq!(stdout.bytes, [expected.len(), 3, client_blocks.len(), 0]);
    let stdin = deduplicated_stream(&output, "stdin");
    assert_eq!(stdin.bytes, [input.len(), 2, server_blocks.len(), 0]);
    // Each REF frame takes 5 bytes, for a block of its receiver.
    assert_eq!(stdout.frames, [client_blocks.len() / 4096 * 5, 0]);
    assert_eq!(stdin.frames, [server_blocks.len() / 16384 * 5, 0]);
    let lists = [
        transfer.cache_files("client", "local-hashlists"),
        transfer.cache_files("server", "local-hashlists"),
    ];
    assert!(lists[0][0].starts_with("blake3-"), "{lists:?}");
    assert!(lists[1][0].starts_with("sha256-"), "{lists:?}");
}

/// A `--hashsync-path` wrapper that runs this build with the configuration
/// file `config`. For `serve`, it first creates `mark`, and waits until
/// `go` exists, with no deadline. It also stops waiting once `mark` is gone
/// with the test's directory, so that it ends when the test fails first.
fn held_server(
    transfer: &Transfer,
    name: &str,
    config: &std::path::Path,
    mark: &std::path::Path,
    go: &std::path::Path,
) -> PathBuf {
    let wrapper = transfer.env.path(name);
    let script = format!(
        r#"#!/bin/sh
if [ "$1" = serve ]; then
    : >'{mark}'
    until [ -e '{go}' ] || [ ! -e '{mark}' ]; do sleep 0.01; done
fi
exec '{hashsync}' --config '{config}' "$@"
"#,
        mark = mark.display(),
        go = go.display(),
        hashsync = env!("CARGO_BIN_EXE_hashsync"),
        config = config.display()
    );
    std::fs::write(&wrapper, script).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    wrapper
}

/// Wait until `path` exists, polling with no deadline, while every one of
/// `clients` runs: only a live run makes the mark.
fn wait_for_file(path: &std::path::Path, clients: &mut [KillOnDrop]) {
    wait_for_bytes(path, clients, |_| true);
}

/// Wait until the file at `path` exists and its bytes are `ready`, polling
/// with no deadline, while every one of `clients` runs: only a live run
/// writes them.
fn wait_for_bytes(
    path: &std::path::Path,
    clients: &mut [KillOnDrop],
    ready: impl Fn(&[u8]) -> bool,
) {
    while !std::fs::read(path).is_ok_and(|bytes| ready(&bytes)) {
        clients.iter_mut().for_each(assert_running);
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Whether `bytes` hold `part` somewhere.
fn holds(bytes: &[u8], part: &[u8]) -> bool {
    bytes.windows(part.len()).any(|window| window == part)
}

#[test]
fn two_runs_in_opposite_directions_with_both_streams_deduplicated_both_end() {
    // Two hosts, A and B, each with its own index, and two runs at once
    // with both streams deduplicated: one with its client on A and its
    // server on B, and one the other way around. Each client updates its
    // index before SSH starts, and a wrapper holds each server back until
    // both clients have finished their updates. A client that kept a lock
    // of its index then would make the server of the other run wait for it
    // in its update, while its own server waits for the other client in
    // the same way, and both runs would hang. So each client releases its
    // index after its update, and opens it again only at its HELLO, which
    // follows the server's update (DESIGN.md §4.4): neither client holds a
    // lock while the servers update.
    let transfer = Transfer::new(&[]);
    // Host A has the transfer's client configuration, host B a second one.
    transfer.env.mkdir("b-tree");
    let b_index = transfer.env.path("b.db");
    let configs = [transfer.env.config_path(), transfer.env.path("b.toml")];
    std::fs::write(
        &configs[1],
        format!(
            "block_size = {BLOCK}\nindex_path = '{}'\n{}",
            toml_literal(b_index.as_os_str().as_bytes()),
            transfer.env.root("b-tree", "")
        ),
    )
    .unwrap();
    let locks = [transfer.env.index_path(), b_index].map(|index| index.join("hashsync.lock"));
    let go = transfer.env.path("go");
    let marks = [
        transfer.env.path("serving-a"),
        transfer.env.path("serving-b"),
    ];
    let inputs = [random(256 << 10, 67), random(256 << 10, 68)];
    let mut clients = Vec::new();
    for (i, input) in inputs.iter().enumerate() {
        // The client of run `i` is on host `i`, and its server on the other.
        let server = held_server(
            &transfer,
            &format!("server-{i}"),
            &configs[1 - i],
            &marks[i],
            &go,
        );
        transfer.set_hashsync_path(server.as_os_str());
        let path = transfer.env.path(format!("input-{i}"));
        std::fs::write(&path, input).unwrap();
        let mut client = Command::new(env!("CARGO_BIN_EXE_hashsync"));
        client
            .arg("--config")
            .arg(&configs[i])
            .args(["ssh", "--dedup-stdin", "--dedup-stdout"]);
        transfer.add_options(&mut client, &[EXACT]);
        let client = client
            .arg(HOST)
            .arg("--")
            .arg("cat")
            .stdin(File::open(&path).unwrap())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        clients.push(KillOnDrop(client));
    }
    // Each server starts only after its client's update.
    for mark in &marks {
        wait_for_file(mark, &mut clients);
    }
    for (client, lock) in clients.iter().zip(&locks) {
        assert_eq!(
            ofd_lock(client.0.id(), lock),
            None,
            "a client keeps its lock"
        );
    }
    std::fs::write(&go, b"").unwrap();
    for (client, input) in clients.into_iter().zip(&inputs) {
        let output = succeeded(output_of(client));
        assert!(output.stdout == *input, "the output differs");
    }
}

#[test]
fn a_rebuild_of_the_clients_index_before_its_hello_ends_the_run_before_the_command() {
    // With both streams deduplicated, the client's index has no lock
    // between the client's update and its HELLO, which opens it again
    // (DESIGN.md §4.4). A wrapper holds the server back while another
    // configuration with the same index and blocks of another size
    // rebuilds that index. The client then finds an index that does not
    // match its configuration, and ends the run with `die`, before the
    // command starts: its script is never opened, which `exec` would do.
    let transfer = Transfer::new(&[]);
    transfer.configure_server("");
    transfer.env.write("tree/blocks", &tiled(4, 69));
    let other = transfer.env.path("other.toml");
    std::fs::write(
        &other,
        format!(
            "block_size = 8192\nindex_path = '{}'\n{}",
            toml_literal(transfer.env.index_path().as_os_str().as_bytes()),
            transfer.env.root("tree", "")
        ),
    )
    .unwrap();
    let waiting = transfer.env.path("waiting");
    let rebuilt = transfer.env.path("rebuilt");
    let server = held_server(
        &transfer,
        "held-server",
        &transfer.server_config(),
        &waiting,
        &rebuilt,
    );
    transfer.set_hashsync_path(server.as_os_str());
    let script = transfer.env.path("command");
    std::fs::write(&script, "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let opens =
        inotify::init(inotify::CreateFlags::CLOEXEC | inotify::CreateFlags::NONBLOCK).unwrap();
    inotify::add_watch(&opens, &script, inotify::WatchFlags::OPEN).unwrap();
    let client = transfer
        .both(&[], &[script.as_os_str()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut client = KillOnDrop(client);
    // The server starts after the client's update.
    wait_for_file(&waiting, std::slice::from_mut(&mut client));
    let lock = transfer.env.index_path().join("hashsync.lock");
    assert_eq!(
        ofd_lock(client.0.id(), &lock),
        None,
        "the client keeps its lock"
    );
    let rebuild = Command::new(env!("CARGO_BIN_EXE_hashsync"))
        .arg("--config")
        .arg(&other)
        .arg("update")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&rebuild.stderr);
    assert!(rebuild.status.success(), "rebuild failed:\n{stderr}");
    assert!(stderr.contains("rebuilt index"), "{stderr}");
    std::fs::write(&rebuilt, b"").unwrap();
    let output = output_of(client);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("does not match the configuration; run `hashsync update`"),
        "{stderr}"
    );
    let mut event = [0u8; 256];
    assert_eq!(
        read(&opens, &mut event),
        Err(Errno::AGAIN),
        "the command started"
    );
}

#[test]
fn rsync_runs_through_hashsync_ssh_with_both_streams_deduplicated() {
    // rsync talks both ways through its remote shell, so with both streams
    // deduplicated, what each host sends is deduplicated against the index
    // of the other: a push, then a pull of what the push copied.
    let files = [("a", tiled(16, 53)), ("sub/b", random(100_000, 54))];
    let transfer = Transfer::new(&[]);
    transfer.configure_server("");
    // Both indexes hold a copy of the file of whole blocks.
    transfer.env.write("tree/a", &files[0].1);
    transfer.env.write("server-tree/a", &files[0].1);
    let local = transfer.env.path("local");
    for (name, data) in &files {
        transfer.env.write(local.join(name), data);
    }
    let remote = transfer.env.path("remote");
    std::fs::create_dir_all(&remote).unwrap();
    let remote_spec = |dir: &std::path::Path| {
        let mut spec = format!("tester@{HOST}:").into_bytes();
        spec.extend_from_slice(dir.as_os_str().as_bytes());
        spec.push(b'/');
        OsString::from_vec(spec)
    };
    let both = ["--dedup-stdin", "--dedup-stdout"];
    let pushed = remote.join("pushed");
    let mut source = local.into_os_string();
    source.push("/");
    rsync(&transfer, &[], &both, &source, &remote_spec(&pushed));
    let pulled = transfer.env.path("pulled");
    rsync(
        &transfer,
        &[],
        &both,
        &remote_spec(&pushed),
        pulled.as_os_str(),
    );
    for (name, data) in &files {
        assert!(
            std::fs::read(pushed.join(name)).unwrap() == *data,
            "pushed {name} differs"
        );
        assert!(
            std::fs::read(pulled.join(name)).unwrap() == *data,
            "pulled {name} differs"
        );
    }
}
