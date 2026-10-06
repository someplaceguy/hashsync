//! End-to-end `run` transfers (PLAN.md steps 3 to 5) over a fake `ssh`
//! that executes the remote command string with a local `/bin/sh`, under a
//! separate HOME for the sender side. This exercises the real bootstrap
//! quoting, the rendezvous socket, both sessions, the cache publication
//! rules on both sides, block references with their pins, runs, and the
//! compression stream.
//!
//! The tests wait for events, and no wait has a deadline whose end fails a
//! test: a regression hangs the test, so that a debugger can examine it.
//! A few tests must see that a process does not finish, which no event
//! shows, so they wait a fixed time. On a slow machine, such a wait can
//! miss a fault, but it cannot fail a correct build.

mod common;

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use common::{
    ANSWER, BLOCK, CACHE_RESULT, END, Env, HELLO, INPUT_STOP, KillOnDrop, LIT, QUERY, frame,
    proc_state, random, tiled, toml_literal, try_recv_frame, version,
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

struct Transfer {
    env: Env,
}

impl Transfer {
    /// An environment with a fake `ssh`, split receiver and sender
    /// homes, an indexable `tree` root, and `sender_command` pointing at
    /// this build's binary.
    fn new(config_extra: &str) -> Self {
        let env = Env::new();
        for dir in [
            "receiver/config",
            "receiver/cache",
            "sender/home",
            "sender/runtime",
        ] {
            env.mkdir(dir);
        }
        let script = format!(
            r#"#!/bin/sh
set -eu
while [ $# -gt 0 ]; do
    case "$1" in
        -o) shift 2 ;;
        -*) shift ;;
        *) break ;;
    esac
done
host="$1"; shift
if [ "$host" != "{HOST}" ]; then
    echo "fake ssh: unexpected host: $host" >&2
    exit 42
fi
export HOME="$HASHSYNC_TEST_SENDER_HOME"
export XDG_CONFIG_HOME="$HOME/.config"
export XDG_CACHE_HOME="$HOME/.cache"
export XDG_RUNTIME_DIR="$HASHSYNC_TEST_SENDER_RUNTIME"
exec /bin/sh -c "$1"
"#
        );
        let ssh = env.path("ssh");
        std::fs::write(&ssh, script).unwrap();
        std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
        let transfer = Transfer { env };
        transfer.reconfigure(env!("CARGO_BIN_EXE_hashsync").as_bytes(), config_extra);
        transfer
    }

    /// Write the configuration: the fake `ssh` (single-quoted for the
    /// shell-split layer), the sender command, the `tree` root, and
    /// `extra`.
    fn reconfigure(&self, sender_command: &[u8], extra: &str) {
        let ssh = self.env.path("ssh");
        let ssh = ssh.to_str().expect("the test path must be plain text");
        assert!(
            !ssh.contains(['\'', '"', '\\', ' ']),
            "quote the fake ssh path for the config"
        );
        self.env.configure(&format!(
            "ssh = \"'{ssh}'\"\nsender_command = '{}'\n{extra}{}",
            toml_literal(sender_command),
            self.env.root("tree", ""),
        ));
    }

    /// `hashsync run [flags] HOST -- command...` with the environments
    /// of both sides prepared. Its stdin is `/dev/null`, unless the test
    /// gives it another: with `--stdin`, the receiver sends its stdin to
    /// the command (DESIGN.md §6.6), and must not read the stdin of the
    /// test process.
    fn invocation(&self, flags: &[&str], command: &[&OsStr]) -> Command {
        let mut invocation = self.env.command(&["run"]);
        invocation
            .args(flags)
            .arg(HOST)
            .arg("--")
            .args(command)
            .stdin(Stdio::null())
            .env("XDG_CONFIG_HOME", self.env.path("receiver/config"))
            .env("XDG_CACHE_HOME", self.env.path("receiver/cache"))
            .env("HASHSYNC_TEST_SENDER_HOME", self.env.path("sender/home"))
            .env(
                "HASHSYNC_TEST_SENDER_RUNTIME",
                self.env.path("sender/runtime"),
            );
        invocation
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
        assert!(stderr.contains(" output ("), "missing summary:\n{stderr}");
        output
    }

    /// The single `<peer>-<index>` directory of a cache tree, or `None`.
    fn peer_dir(&self, side: &str, tree: &str) -> Option<PathBuf> {
        let root = match side {
            "receiver" => self.env.path("receiver/cache/hashsync"),
            _ => self.env.path("sender/home/.cache/hashsync"),
        };
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(root.join(tree))
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

/// The byte counts of the first summary line of a run with `EXACT`
/// amounts: output, literal, reference, run, wire, hash-list, and delta
/// bytes.
fn summary_counts(output: &Output) -> Vec<usize> {
    let line = summary_line(output, " output (");
    numbers(&line)
        .into_iter()
        .map(|number| whole_bytes(number, &line))
        .collect()
}

/// The first byte counts of the summary line: output, literal, reference,
/// and run bytes.
fn summary(output: &Output) -> [usize; 4] {
    let counts = summary_counts(output);
    [counts[0], counts[1], counts[2], counts[3]]
}

/// The bytes that the data session received, from the summary line.
fn wire_bytes(output: &Output) -> usize {
    summary_counts(output)[4]
}

/// The second summary line, which reports the traffic as rsync does.
struct Traffic {
    sent: usize,
    received: usize,
    seconds: f64,
    /// Bytes per second.
    rate: usize,
    speedup: f64,
}

/// The second summary line of a run with `EXACT` amounts, `sent S and
/// received R in Ts (X/s), speedup Y`, whose numbers are S, R, T, X, and
/// Y.
fn traffic(output: &Output) -> Traffic {
    let line = summary_line(output, "hashsync: sent ");
    let [sent, received, seconds, rate, speedup] = numbers(&line)[..] else {
        panic!("unexpected traffic line: {line}");
    };
    Traffic {
        sent: whole_bytes(sent, &line),
        received: whole_bytes(received, &line),
        seconds: seconds.parse().unwrap(),
        rate: whole_bytes(rate, &line),
        speedup: speedup.parse().unwrap(),
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

/// Wait for `receiver` to end, and return its output, as
/// `Child::wait_with_output` does. A test that fails before kills the
/// receiver instead, so that it does not stay.
fn output_of(mut receiver: KillOnDrop) -> Output {
    let mut stderr = receiver.0.stderr.take().unwrap();
    let errors = std::thread::spawn(move || {
        let mut text = Vec::new();
        stderr.read_to_end(&mut text).unwrap();
        text
    });
    let mut stdout = Vec::new();
    let mut pipe = receiver.0.stdout.take().unwrap();
    pipe.read_to_end(&mut stdout).unwrap();
    let status = receiver.0.wait().unwrap();
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
    let transfer = Transfer::new("");
    // An indexed tree, so the exchanged hash list is not empty.
    transfer.env.write("tree/blocks", &common::tiled(64, 7));

    let payload = random(10 << 20, 42);
    let source = transfer.env.path("payload");
    std::fs::write(&source, &payload).unwrap();
    let command = args(&["cat", source.to_str().unwrap()]);
    let output = transfer.run_ok(&refs(&command));
    assert_eq!(output.stdout, payload, "stdout must match byte for byte");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let payload_len = payload.len();
    assert!(
        stderr.contains(&format!(
            "{payload_len} B output ({payload_len} B literal, 0 B reference, 0 B run)"
        )),
        "summary counts:\n{stderr}"
    );

    // Both sides published the same list under their peer directories
    // (DESIGN.md §5.6), and both created account identities.
    let local = transfer.cache_files("receiver", "local-hashlists");
    let remote = transfer.cache_files("sender", "remote-hashlists");
    assert_eq!(local.len(), 1, "receiver cache: {local:?}");
    assert_eq!(local, remote, "both sides store the same list name");
    assert!(local[0].starts_with("blake3-") && local[0].ends_with(".cache"));
    assert!(
        transfer
            .env
            .path("receiver/config/hashsync/account-id")
            .exists()
    );
    assert!(
        transfer
            .env
            .path("sender/home/.config/hashsync/account-id")
            .exists()
    );

    // A second run reuses both identities and republishes the same
    // list; nothing accumulates.
    let command = args(&["true"]);
    transfer.run_ok(&refs(&command));
    assert_eq!(transfer.cache_files("receiver", "local-hashlists"), local);
    assert_eq!(transfer.cache_files("sender", "remote-hashlists"), remote);
}

#[test]
fn indexed_blocks_become_references_at_any_offset() {
    // The blocks of an indexed file appear twice in the output, at
    // unaligned offsets. Every block becomes a reference; the lookups run
    // concurrently, and the pins and the spools leave nothing behind in
    // the tmp_data directories of both sides (DESIGN.md §6.5).
    let transfer = Transfer::new("");
    let blocks = tiled(64, 7);
    transfer.env.write("tree/blocks", &blocks);
    let script = format!(
        "printf abc; cat '{0}'; printf defg; cat '{0}'",
        transfer.env.path("tree/blocks").display()
    );
    let output = transfer.run_ok(&refs(&args(&["sh", "-c", &script])));
    let expected = [&b"abc"[..], &blocks, b"defg", &blocks].concat();
    assert_eq!(output.stdout, expected);
    assert_eq!(summary(&output), [expected.len(), 7, 2 * blocks.len(), 0]);

    for tmp_data in [
        "receiver/cache/hashsync/tmp-data",
        "sender/home/.cache/hashsync/tmp-data",
    ] {
        let tmp_data = transfer.env.path(tmp_data);
        assert_eq!(mode(&tmp_data), 0o700, "{tmp_data:?} is not private");
        assert_eq!(std::fs::read_dir(&tmp_data).unwrap().count(), 0);
    }
}

#[test]
fn stale_and_unreadable_candidates_are_misses() {
    // A `--no-update` run reads candidates from a file that changed, then
    // became unreadable, after the update. Verification rejects those
    // candidates, and the output stays exact (DESIGN.md §3).
    let transfer = Transfer::new("");
    let original = tiled(8, 11);
    transfer.env.write("tree/file", &original);
    let payload = transfer.env.path("payload");
    std::fs::write(&payload, &original).unwrap();
    transfer.env.update();
    let command = args(&["cat", payload.to_str().unwrap()]);

    let mut changed = original.clone();
    changed[3 * BLOCK + 100] ^= 1;
    std::fs::write(transfer.env.path("tree/file"), &changed).unwrap();
    let output = succeeded(transfer.run(&["--no-update"], &refs(&command)));
    assert_eq!(output.stdout, original);
    assert_eq!(summary(&output), [8 * BLOCK, BLOCK, 7 * BLOCK, 0]);

    transfer.env.chmod("tree/file", 0o000);
    let output = succeeded(transfer.run(&["--no-update"], &refs(&command)));
    transfer.env.chmod("tree/file", 0o644);
    assert_eq!(output.stdout, original);
    assert_eq!(summary(&output), [8 * BLOCK, 8 * BLOCK, 0, 0]);
}

#[test]
fn candidate_attempts_bound_each_query() {
    // One file holds a block twice, and its first occurrence changes after
    // the update. Candidates follow the block numbers, an order that the
    // index fixes but DESIGN.md §6.4 does not: one attempt reaches only
    // the stale occurrence, two reach the intact one.
    let transfer = Transfer::new("");
    let block = random(BLOCK, 12);
    transfer.env.write("tree/twice", &block.repeat(2));
    let payload = transfer.env.path("payload");
    std::fs::write(&payload, &block).unwrap();
    transfer.env.update();
    let mut changed = block.repeat(2);
    changed[5] ^= 1;
    std::fs::write(transfer.env.path("tree/twice"), &changed).unwrap();

    let command = args(&["cat", payload.to_str().unwrap()]);
    for (attempts, reference) in [(1, 0), (2, BLOCK)] {
        transfer.reconfigure(
            env!("CARGO_BIN_EXE_hashsync").as_bytes(),
            &format!("candidate_attempts = {attempts}\n"),
        );
        let output = succeeded(transfer.run(&["--no-update"], &refs(&command)));
        assert_eq!(output.stdout, block);
        assert_eq!(summary(&output), [BLOCK, BLOCK - reference, reference, 0]);
    }
}

#[test]
fn a_rejected_window_does_not_hide_an_overlapping_match() {
    // The block of a changed file hides a valid block that starts 8 bytes
    // into it. The rejection makes the scanner rescan from the next byte,
    // which finds the valid block (DESIGN.md §6.3).
    let transfer = Transfer::new("");
    let valid = random(BLOCK, 13);
    let hider = [&random(8, 14)[..], &valid[..BLOCK - 8]].concat();
    transfer.env.write("tree/valid", &valid);
    transfer.env.write("tree/hider", &hider);
    let payload = [&hider[..8], &valid[..]].concat();
    std::fs::write(transfer.env.path("payload"), &payload).unwrap();
    transfer.env.update();
    transfer.env.write("tree/hider", &random(BLOCK, 15));

    let command = args(&["cat", transfer.env.path("payload").to_str().unwrap()]);
    let output = succeeded(transfer.run(&["--no-update"], &refs(&command)));
    assert_eq!(output.stdout, payload);
    assert_eq!(summary(&output), [8 + BLOCK, 8, BLOCK, 0]);
}

#[test]
fn the_receiver_keeps_its_temporary_data_in_the_configured_directory() {
    let transfer = Transfer::new("");
    let tmp_data = transfer.env.path("tmp/nested");
    let hashsync = env!("CARGO_BIN_EXE_hashsync").as_bytes();
    let configure = |path: &std::path::Path| {
        format!(
            "tmp_data_dir = '{}'\n",
            toml_literal(path.as_os_str().as_bytes())
        )
    };
    transfer.reconfigure(hashsync, &configure(&tmp_data));
    let blocks = tiled(8, 16);
    transfer.env.write("tree/blocks", &blocks);
    let command = args(&["cat", transfer.env.path("tree/blocks").to_str().unwrap()]);
    let output = transfer.run_ok(&refs(&command));
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
        transfer.reconfigure(hashsync, &configure(path));
        let output = transfer.run(&[], &refs(&command));
        assert_eq!(output.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(message), "{stderr}");
        assert!(!marker.exists(), "the command ran");
    }
}

#[test]
fn a_full_tmp_data_directory_fails_the_run() {
    // The receiver's tmp_data directory is a tmpfs in a new user and mount
    // namespace. It holds 64 KiB, also with pages of 64 KiB, and the pins
    // of the output need 128 KiB in their first segment: a segment write
    // fails, which ends the run (DESIGN.md §6.7).
    if !common::namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    let transfer = Transfer::new("");
    let tmp_data = transfer.env.path("tmpfs");
    std::fs::create_dir(&tmp_data).unwrap();
    transfer.reconfigure(
        env!("CARGO_BIN_EXE_hashsync").as_bytes(),
        &format!(
            "tmp_data_dir = '{}'\n",
            toml_literal(tmp_data.as_os_str().as_bytes())
        ),
    );
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
fn a_sender_tmp_data_directory_that_cannot_hold_a_segment_fails_before_the_command() {
    // The sender's tmp_data directory exists, but it is read-only: the
    // sender cannot create segments in it, and the run must end before
    // the command starts (DESIGN.md §6.5). A command that started would
    // be killed at once, before it could leave a mark of its own, so the
    // test watches the command's script: `exec` opens it.
    let transfer = Transfer::new("");
    let tmp_data = "sender/home/.cache/hashsync/tmp-data";
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
fn a_file_size_limit_on_the_sender_ends_the_run_with_its_cleanup() {
    // The sender runs under a file-size limit of 64 KiB (128 KiB where
    // `ulimit -f` counts KiB), and the command writes 1 MiB of new data,
    // which the sender's spool writes to a segment. SIGXFSZ stays blocked
    // in the sender, so that write fails with `EFBIG`, and `die` ends the
    // run: it kills the command and removes the rendezvous socket
    // (DESIGN.md §6.5 and §6.9). The default end of SIGXFSZ would skip
    // that cleanup.
    let transfer = Transfer::new("");
    let limited = transfer.env.path("limited-hashsync");
    let script = format!(
        "#!/bin/sh\nulimit -f 128\nexec '{}' \"$@\"\n",
        env!("CARGO_BIN_EXE_hashsync")
    );
    std::fs::write(&limited, script).unwrap();
    std::fs::set_permissions(&limited, std::fs::Permissions::from_mode(0o755)).unwrap();
    transfer.reconfigure(limited.as_os_str().as_bytes(), "");
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
    let sockets = transfer.env.path("sender/runtime/hashsync");
    assert_eq!(
        std::fs::read_dir(sockets).unwrap().count(),
        0,
        "the rendezvous socket remains"
    );
}

#[test]
fn the_command_starts_without_the_signals_that_hashsync_blocks() {
    // Every role blocks SIGXFSZ in every thread, and the receiver also
    // blocks SIGINT and SIGTERM. Each gives its children the mask from
    // before, because a blocked signal stays blocked across `exec`. So the
    // command, `cat` here, starts with none of these signals blocked.
    let transfer = Transfer::new("");
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
    // its answer before scanning goes on, and the command waits for the
    // scanner once 64 KiB of its output are in the sender's spool. Stdout
    // drains slowly, so the pins wait for stdout. The data session, about
    // 100 bytes of references, waits in the receiver's spool. Output,
    // answers, and pin release stay in order (DESIGN.md §6.3 to §6.5).
    let transfer = Transfer::new(&format!(
        "lookahead = {BLOCK}\nsender_output_spool_limit = \"64KiB\"\n"
    ));
    let blocks = tiled(256, 17);
    transfer.env.write("tree/big", &blocks);
    let script = format!(
        "printf x; cat '{0}'; printf yz; cat '{0}'",
        transfer.env.path("tree/big").display()
    );
    let mut receiver = transfer
        .invocation(&[EXACT], &refs(&args(&["sh", "-c", &script])))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = receiver.stdout.take().unwrap();
    let mut received = Vec::new();
    let mut chunk = vec![0u8; 16 << 10];
    loop {
        let n = stdout.read(&mut chunk).unwrap();
        if n == 0 {
            break;
        }
        received.extend_from_slice(&chunk[..n]);
        std::thread::sleep(Duration::from_millis(1));
    }
    let output = succeeded(receiver.wait_with_output().unwrap());
    assert_eq!(received, [&b"x"[..], &blocks, b"yz", &blocks].concat());
    assert_eq!(summary(&output), [received.len(), 3, 2 * blocks.len(), 0]);
}

#[test]
fn runs_and_compression_shrink_the_wire() {
    // Zeros become runs of whole windows, and the zstd stream compresses
    // the repetitive text. Random bytes shrink neither way (DESIGN.md §6.2
    // and §6.3).
    const ZEROS: usize = 4 << 20;
    for (config, compressed) in [("", true), ("compression_level = 0\n", false)] {
        let transfer = Transfer::new(config);
        let text = "hashsync compresses repetitive text\n".repeat(30_000);
        let noise = random(1 << 20, 23);
        transfer.env.write("text", text.as_bytes());
        transfer.env.write("noise", &noise);
        let script = format!(
            "head -c {ZEROS} /dev/zero; cat '{}' '{}'",
            transfer.env.path("text").display(),
            transfer.env.path("noise").display()
        );
        let output = transfer.run_ok(&refs(&args(&["sh", "-c", &script])));
        let expected = [&vec![0; ZEROS][..], text.as_bytes(), &noise].concat();
        assert!(output.stdout == expected, "the output differs");
        let literal = text.len() + noise.len();
        assert_eq!(summary(&output), [expected.len(), literal, 0, ZEROS]);
        let wire = wire_bytes(&output);
        if compressed {
            assert!(
                (noise.len()..noise.len() + (64 << 10)).contains(&wire),
                "{wire} bytes wire"
            );
        } else {
            assert!(wire > literal, "{wire} bytes wire");
        }
    }
}

#[test]
fn a_command_that_pauses_transfers_exactly() {
    // The command stops for a while, with queries for its indexed blocks in
    // flight. Its frames can wait in the compressor meanwhile: the sender
    // does not flush the stream for the receiver's sake, and no wait
    // depends on those frames (DESIGN.md §6.2).
    let transfer = Transfer::new("");
    let blocks = tiled(16, 25);
    transfer.env.write("tree/blocks", &blocks);
    let script = format!(
        "cat '{0}'; sleep 0.5; cat '{0}'",
        transfer.env.path("tree/blocks").display()
    );
    let output = transfer.run_ok(&refs(&args(&["sh", "-c", &script])));
    assert_eq!(output.stdout, blocks.repeat(2));
    assert_eq!(summary(&output), [2 * blocks.len(), 0, 2 * blocks.len(), 0]);
}

#[test]
fn empty_output_and_command_failure_status() {
    let transfer = Transfer::new("");
    let command = args(&["true"]);
    let output = transfer.run_ok(&refs(&command));
    assert!(output.stdout.is_empty());

    // A failing command still transfers its output and returns its
    // status (DESIGN.md §6.7).
    let command = args(&["sh", "-c", "echo partial; exit 3"]);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.stdout, b"partial\n");
    assert_eq!(output.status.code(), Some(3));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(" output ("), "summary:\n{stderr}");
}

#[test]
fn the_summary_counts_both_sessions() {
    // Each reference needs a QUERY on the control session and a yes
    // ANSWER, which the data session does not carry. The second summary
    // line counts both sessions in both directions (DESIGN.md §8). Without
    // compression, the control session carries its frames as they are.
    let transfer = Transfer::new("compression_level = 0\n");
    let blocks = tiled(64, 7);
    transfer.env.write("tree/blocks", &blocks);
    let command = args(&["cat", transfer.env.path("tree/blocks").to_str().unwrap()]);
    let output = transfer.run_ok(&refs(&command));
    assert_eq!(output.stdout, blocks);
    let counts = summary_counts(&output);
    let (output_bytes, wire, hash_list) = (counts[0], counts[4], counts[5]);
    assert_eq!(counts[2], blocks.len(), "every block is a reference");

    // The receiver reads the sender's control version, one QUERY per
    // block: a 5-byte frame header, the offset, and the full digest, and
    // the INPUT_STOP of the command's exit, a header alone. The sender
    // does not wait for its control writer at the end, so the closure of
    // the session can cut INPUT_STOP off (DESIGN.md §6.6 and §6.9). The
    // receiver writes its two versions, the hash list, and one ANSWER per
    // block: a header, the offset, and the flag.
    let traffic = traffic(&output);
    let queries = wire + 4 + 64 * (5 + 8 + 32);
    assert!(
        [queries, queries + 5].contains(&traffic.received),
        "received {} bytes",
        traffic.received
    );
    assert!(traffic.sent > hash_list + 2 * 4 + 64 * (5 + 8 + 1));

    // The speedup is the output over the traffic. The rate is the
    // traffic over the elapsed time. The line rounds the time to a tenth
    // of a second and the rate to whole bytes per second. So the traffic
    // is between the products of the smallest and of the largest time and
    // rate that round to the shown values, however long the run takes.
    let total = traffic.sent + traffic.received;
    let speedup = output_bytes as f64 / total as f64;
    assert!(
        (traffic.speedup - speedup).abs() <= 0.005 + 1e-9,
        "speedup {speedup}"
    );
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
    let transfer = Transfer::new("");
    transfer.env.write("tree/blocks", &tiled(256, 7));
    let payload = random(10 << 20, 42);
    let source = transfer.env.path("payload");
    std::fs::write(&source, &payload).unwrap();
    let command = args(&["cat", source.to_str().unwrap()]);
    let output = succeeded(transfer.invocation(&[], &refs(&command)).output().unwrap());
    assert_eq!(output.stdout, payload);
    let line = summary_line(&output, " output (");
    assert!(
        line.starts_with(
            "hashsync: 10.00 MiB output (10.00 MiB literal, 0 B reference, 0 B run), "
        ),
        "{line}"
    );

    // The sent and received bytes, and the rate, whose unit depends on
    // the elapsed time.
    let line = summary_line(&output, "hashsync: sent ");
    let amounts = amounts(&line);
    assert!(
        amounts
            .iter()
            .all(|&(number, unit)| is_scaled_amount(number, unit)),
        "{line}"
    );
    assert!(matches!(amounts[..], [(_, "KiB"), (_, "MiB"), _]), "{line}");
}

#[test]
fn quiet_runs_print_only_the_command_stderr() {
    // `--quiet` removes the summary and the notes of the update, which
    // creates the index here. The command's stderr still arrives
    // (DESIGN.md §8).
    let transfer = Transfer::new("");
    let command = args(&["sh", "-c", "echo to-stdout; echo to-stderr >&2"]);
    let output = succeeded(transfer.run(&["--quiet"], &refs(&command)));
    assert_eq!(output.stdout, b"to-stdout\n");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "to-stderr\n");
}

#[test]
fn command_killed_by_a_signal_reports_128_plus_signal() {
    let transfer = Transfer::new("");
    let command = args(&["sh", "-c", "kill -KILL $$"]);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(128 + 9));
}

#[test]
fn startup_failure_reports_127_and_the_error() {
    let transfer = Transfer::new("");
    let command = args(&["/nonexistent-hashsync-test-command"]);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(127));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to start") && stderr.contains("nonexistent-hashsync-test"),
        "startup error:\n{stderr}"
    );

    // The error is not a note: `--quiet` keeps it, and removes the
    // summary.
    let output = transfer.run(&["--quiet"], &refs(&command));
    assert_eq!(output.status.code(), Some(127));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("failed to start"), "{stderr}");
    assert!(!stderr.contains(" output ("), "{stderr}");
}

#[test]
fn stderr_is_forwarded() {
    let transfer = Transfer::new("");
    let command = args(&["sh", "-c", "echo to-stdout; echo to-stderr >&2"]);
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
    let transfer = Transfer::new("");
    let command = args(&[
        "sh",
        "-c",
        "( exec >/dev/null; sleep 0.3; echo late-stderr >&2 ) & echo early-stdout",
    ]);
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
    let transfer = Transfer::new("");
    let arg = OsString::from_vec(b"\xff\xfe-arg\x01".to_vec());
    let command: Vec<OsString> = vec!["printf".into(), "%s".into(), arg];
    let output = transfer.run_ok(&refs(&command));
    assert_eq!(output.stdout, b"\xff\xfe-arg\x01");
}

#[test]
fn no_update_requires_a_current_index() {
    let transfer = Transfer::new("");
    let command = args(&["true"]);
    let output = transfer.run(&["--no-update"], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("run `hashsync update`"),
        "missing hint:\n{stderr}"
    );

    transfer.env.write("tree/data", &common::tiled(8, 3));
    transfer.env.update();
    let output = transfer.run(&["--no-update"], &refs(&command));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "read-only run failed:\n{stderr}");
    assert!(
        !stderr.contains("updated index"),
        "--no-update must not update:\n{stderr}"
    );
}

#[test]
fn cache_failures_are_fatal_on_either_side() {
    // Receiver: an unwritable cache root fails the run at publication
    // (DESIGN.md §5.6 and §6.7). The tmp_data directory is in the same
    // root and opens earlier, so it exists already: else its creation
    // would fail first, and the run would end before the publication.
    let transfer = Transfer::new("");
    let command = args(&["true"]);
    transfer.env.mkdir("receiver/cache/hashsync/tmp-data");
    transfer.env.chmod("receiver/cache/hashsync", 0o555);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot create ") && stderr.contains("/hashsync/local-hashlists:"),
        "receiver cache failure:\n{stderr}"
    );
    transfer.env.chmod("receiver/cache/hashsync", 0o755);

    // Sender: the same policy; its death fails the whole run, and its
    // message reaches the receiver's stderr through SSH. Its tmp_data
    // directory exists too.
    transfer.env.mkdir("sender/home/.cache/hashsync/tmp-data");
    transfer.env.chmod("sender/home/.cache/hashsync", 0o555);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot create ") && stderr.contains("/hashsync/remote-hashlists:"),
        "sender cache failure:\n{stderr}"
    );
    transfer.env.chmod("sender/home/.cache/hashsync", 0o755);

    // Writable again: the run succeeds.
    transfer.run_ok(&refs(&command));
}

#[test]
fn the_command_ends_while_the_consumer_reads_nothing() {
    // The consumer reads nothing until the command has written all its
    // output, which is larger than all the buffers in memory between the
    // command and the consumer: the scanner looks one window ahead, and
    // the data queue holds 4 MiB. One spool holds at most 64 KiB. So the
    // command ends first only because the other spool holds the output
    // (DESIGN.md §6.4 and §6.5): the receiver's, which keeps reading the
    // data session while stdout is blocked, or the sender's, without a
    // limit.
    for limits in [
        "sender_output_spool_limit = \"64KiB\"\n",
        "sender_output_spool_limit = \"unlimited\"\nreceiver_output_spool_limit = \"64KiB\"\n",
    ] {
        let transfer = Transfer::new(&format!(
            "lookahead = {BLOCK}\n{limits}compression_level = 0\n"
        ));
        let payload = random(12 << 20, 21);
        let path = transfer.env.path("payload");
        std::fs::write(&path, &payload).unwrap();
        let ended = transfer.env.path("ended");
        mkfifoat(CWD, &ended, Mode::RUSR | Mode::WUSR).unwrap();
        let script = format!(
            "cat '{}'; echo ended > '{}'",
            path.display(),
            ended.display()
        );
        let receiver = transfer
            .invocation(&[EXACT], &refs(&args(&["sh", "-c", &script])))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        // The open waits for the command to open the FIFO, after its
        // output.
        let mut message = String::new();
        File::open(&ended)
            .unwrap()
            .read_to_string(&mut message)
            .unwrap();
        assert_eq!(message, "ended\n");
        let output = succeeded(receiver.wait_with_output().unwrap());
        assert!(output.stdout == payload, "{limits}: the output differs");
        assert_eq!(summary(&output), [payload.len(), payload.len(), 0, 0]);
    }
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
    let transfer = Transfer::new(&format!(
        "lookahead = {BLOCK}\nsender_output_spool_limit = \"64KiB\"\n\
         receiver_output_spool_limit = \"64KiB\"\ncompression_level = 0\n"
    ));
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
    let receiver = transfer
        .invocation(&[EXACT], &refs(&args(&["sh", "-c", &script])))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
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
    let output = succeeded(receiver.wait_with_output().unwrap());
    assert!(output.stdout == payload, "the output differs");
    assert_eq!(summary(&output), [payload.len(), payload.len(), 0, 0]);
    // The command wrote to the FIFO before it ended, and the receiver
    // ended after the command.
    let mut message = String::new();
    (&fifo).read_to_string(&mut message).unwrap();
    assert_eq!(message, "ended\n");
}

#[test]
fn the_receiver_stops_reading_the_data_session_at_its_spool_limit() {
    // Each spool has its own limit: the sender's comes from HELLO, and the
    // receiver's from the receiver's configuration (DESIGN.md §6.5). Here
    // the sender's spool has no limit, and the receiver's holds at most
    // 64 KiB. The consumer reads nothing until the command has ended, so
    // most of the 24 MiB of output waits in the sender's spool. Then the
    // test kills the sender, and those bytes are lost: the receiver can
    // write only the bytes that it read before, at most about 3.5 MiB of
    // buffers, also where a pipe holds 1 MiB. If the receiver used the
    // sender's limit, it would hold most of the output, and write it.
    let transfer = Transfer::new(&format!(
        "lookahead = {BLOCK}\nsender_output_spool_limit = \"unlimited\"\n\
         receiver_output_spool_limit = \"64KiB\"\ncompression_level = 0\n"
    ));
    let payload = random(24 << 20, 24);
    let path = transfer.env.path("payload");
    std::fs::write(&path, &payload).unwrap();
    let ended = transfer.env.path("ended");
    mkfifoat(CWD, &ended, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!(
        "cat '{}'; echo ended > '{}'",
        path.display(),
        ended.display()
    );
    let receiver = transfer
        .invocation(&[], &refs(&args(&["sh", "-c", &script])))
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
    // The sender is the SSH session that runs `serve`. The control
    // session stays, so that only the end of the data session ends the
    // run, after the receiver has written what it holds.
    let sender = children(receiver.id())
        .into_iter()
        .find(|pid| {
            let argv = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
            argv.split(|&byte| byte == 0).any(|arg| arg == b"serve")
        })
        .expect("no SSH session runs serve");
    signal(sender, Signal::KILL);
    let output = receiver.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    // The kill cuts the data session at any byte: inside a frame, or, now
    // and then, between two frames.
    assert!(
        stderr.contains("the data session ended unexpectedly")
            || stderr.contains("the data session ended before END"),
        "{stderr}"
    );
    assert!(payload.starts_with(&output.stdout), "the output differs");
    assert!(
        output.stdout.len() < 10 << 20,
        "the receiver held {} bytes of output: it read past its spool limit",
        output.stdout.len()
    );
}

#[test]
fn stdin_reaches_the_command() {
    // Through `cat`, the receiver's stdin becomes the output, byte for
    // byte, and the command reads EOF after it, so `echo` follows. The
    // input stream is compressed as the data stream is: the text takes a
    // small part of its size on the wire, and the noise its full size
    // (DESIGN.md §6.6).
    let text = "hashsync sends its stdin to the command\n".repeat(100_000);
    let noise = random(2 << 20, 31);
    let input = [text.as_bytes(), &noise].concat();
    for (config, compressed) in [("", true), ("compression_level = 0\n", false)] {
        let transfer = Transfer::new(config);
        let path = transfer.env.path("input");
        std::fs::write(&path, &input).unwrap();
        let command = args(&["sh", "-c", "cat; echo end"]);
        let output = transfer
            .invocation(&[EXACT, "--stdin"], &refs(&command))
            .stdin(File::open(&path).unwrap())
            .output()
            .unwrap();
        let output = succeeded(output);
        assert!(
            output.stdout == [&input[..], b"end\n"].concat(),
            "the output differs"
        );
        let sent = traffic(&output).sent;
        if compressed {
            assert!(sent < noise.len() + (64 << 10), "{sent} bytes sent");
        } else {
            assert!(sent > input.len(), "{sent} bytes sent");
        }
    }
}

#[test]
fn the_run_ends_with_the_command_while_stdin_stays_open() {
    // The receiver's stdin stays open, as a terminal does, but the run
    // ends with the command: the receiver stops reading its stdin at
    // INPUT_STOP or END (DESIGN.md §6.6). A command that reads a part of
    // the input gets it at once, also with compression: the stdin reader
    // flushes the input stream when stdin has no more bytes ready. A
    // command that fails to start ends the run in the same way.
    for config in ["", "compression_level = 0\n"] {
        let transfer = Transfer::new(config);
        let missing = "/nonexistent-hashsync-test-command";
        for (command, status, expected) in [
            (&["true"][..], 0, &b""[..]),
            (&["head", "-c", "5"], 0, b"hello"),
            (&[missing], 127, b""),
        ] {
            let (stdin, mut producer) = std::io::pipe().unwrap();
            producer.write_all(b"hello, and more").unwrap();
            let output = transfer
                .invocation(&[EXACT, "--stdin"], &refs(&args(command)))
                .stdin(stdin)
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            let what = format!("{config:?} {command:?}");
            assert_eq!(output.status.code(), Some(status), "{what}:\n{stderr}");
            assert_eq!(output.stdout, expected, "{what}");
            drop(producer);
        }
    }
}

#[test]
fn a_descendant_that_reads_stdin_reads_eof_when_the_command_exits() {
    // The command starts a descendant that reads the command's stdin and
    // holds its stdout, and exits at once, while the receiver's stdin
    // stays open. As under SSH, the descendant reads EOF once the command
    // has exited: the sender stops the input at the exit, and does not
    // wait for the EOF of stdout, which the descendant holds until its
    // stdin ends (DESIGN.md §6.6). A sender that stopped the input only
    // after that EOF would hang this test. The descendant copies the input
    // that it read before the stop: a part of what the producer wrote.
    let transfer = Transfer::new("");
    let (stdin, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"hello, and more").unwrap();
    let script = "exec 3<&0; cat <&3 & exec 3<&-; exit 0";
    let output = transfer
        .invocation(&[EXACT, "--stdin"], &refs(&args(&["sh", "-c", script])))
        .stdin(stdin)
        .output()
        .unwrap();
    let output = succeeded(output);
    assert!(
        b"hello, and more".starts_with(&output.stdout),
        "{:?}",
        String::from_utf8_lossy(&output.stdout)
    );
    drop(producer);
}

#[test]
fn a_stdin_that_is_not_a_terminal_is_non_blocking_while_it_is_read() {
    // As ssh does, the receiver makes a stdin that is not a terminal
    // non-blocking while its stdin reader reads it, and restores its flags
    // when the reader stops: here at the EOF of stdin, while the command
    // still runs (DESIGN.md §6.6). The test keeps a copy of the read end of
    // the stdin pipe, which shares the flags, and waits for each change
    // without a deadline: a receiver that does not make it hangs the test.
    let transfer = Transfer::new("");
    let go = transfer.env.path("go");
    mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!("cat; read go <'{}'", go.display());
    let (stdin, mut producer) = std::io::pipe().unwrap();
    let shared = stdin.try_clone().unwrap();
    let receiver = transfer
        .invocation(&[EXACT, "--stdin"], &refs(&args(&["sh", "-c", &script])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let receiver = KillOnDrop(receiver);
    wait_for_non_blocking(&shared, true);
    producer.write_all(b"input\n").unwrap();
    drop(producer);
    wait_for_non_blocking(&shared, false);
    std::fs::write(&go, b"go\n").unwrap();
    let output = succeeded(output_of(receiver));
    assert_eq!(output.stdout, b"input\n");
}

#[test]
fn a_fatal_end_restores_the_flags_of_stdin() {
    // The flags of stdin go back to what they were also when the run ends
    // fatally while the stdin reader reads stdin: here at the SIGTERM of
    // the receiver, which ends the run with `128 + signal` (DESIGN.md §6.8
    // and §6.9). Nothing else ends the stdin reader: stdin stays open, and
    // the command, `cat`, goes on reading its own stdin.
    let transfer = Transfer::new("");
    let (stdin, producer) = std::io::pipe().unwrap();
    let shared = stdin.try_clone().unwrap();
    let receiver = transfer
        .invocation(&[EXACT, "--stdin"], &refs(&args(&["cat"])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let receiver = KillOnDrop(receiver);
    wait_for_non_blocking(&shared, true);
    signal(receiver.0.id(), Signal::TERM);
    let output = output_of(receiver);
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
    // As ssh does, the receiver leaves a stdin that is a terminal as it
    // is: the shell of the user shares it, and must not find it
    // non-blocking (DESIGN.md §6.6). Here stdin is the slave of a new
    // pseudo-terminal, which holds a typed line before the run starts.
    // Only the stdin reader reads the terminal, so once the line is gone,
    // the stdin reader has read it, and the flags of the terminal must
    // still be as they were. The output of `cat`, the command, cannot show
    // that sooner: for so short an output, the sender flushes its data
    // stream only at END (DESIGN.md §6.2). Ctrl-D then ends the input.
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
    let transfer = Transfer::new("");
    let receiver = transfer
        .invocation(&[EXACT, "--stdin"], &refs(&args(&["cat"])))
        .stdin(slave)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let receiver = KillOnDrop(receiver);
    wait_for_input(&shared, 0);
    let flags = rustix::fs::fcntl_getfl(&shared).unwrap();
    assert!(
        !flags.contains(OFlags::NONBLOCK),
        "the terminal is non-blocking"
    );
    // Ctrl-D: the EOF of a terminal in canonical mode.
    master.write_all(b"\x04").unwrap();
    let output = succeeded(output_of(receiver));
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
fn the_command_reads_an_empty_stdin_by_default() {
    // Without `--stdin`, the command reads EOF at once, as with `ssh -n`,
    // and the run reads nothing from its own stdin: the bytes in it stay
    // for the next reader (DESIGN.md §8).
    let transfer = Transfer::new("");
    let (stdin, mut producer) = std::io::pipe().unwrap();
    let mut unread = stdin.try_clone().unwrap();
    producer.write_all(b"not for the command").unwrap();
    drop(producer);
    let command = args(&["sh", "-c", "cat; echo end"]);
    let output = transfer
        .invocation(&[EXACT], &refs(&command))
        .stdin(stdin)
        .output()
        .unwrap();
    assert_eq!(succeeded(output).stdout, b"end\n");
    let mut left = Vec::new();
    unread.read_to_end(&mut left).unwrap();
    assert_eq!(left, b"not for the command");
}

#[test]
fn a_command_that_closes_its_stdin_stops_the_input() {
    // The command closes its stdin, and runs on until this test ends it.
    // No process reads the input any more, so the first write of the
    // sender's stdin writer fails with EPIPE, and the sender stops the
    // input: INPUT_STOP makes the receiver stop it too, while the command
    // still runs (DESIGN.md §6.6). The end of the receiver's stdin reader
    // shows that stop: the stdin reader is the only reader of the
    // receiver's stdin, so from then on the producer of stdin can write
    // only what the pipe holds. The test waits for that event, and for no
    // fixed time (DESIGN.md §11). A build that goes on taking the input, or
    // stops it only when the command ends, hangs the test.
    let transfer = Transfer::new("");
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
    let receiver = transfer
        .invocation(&[EXACT, "--stdin"], &refs(&args(&["sh", "-c", &script])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let receiver = KillOnDrop(receiver);
    // The open waits for the command to open the FIFO, after it closed
    // its stdin.
    let mut message = String::new();
    File::open(&closed)
        .unwrap()
        .read_to_string(&mut message)
        .unwrap();
    assert_eq!(message, "closed\n");
    // The receiver starts its stdin reader after CACHE_RESULT. Only a stop
    // or the EOF of stdin ends it, and neither can come before the input.
    let pid = receiver.0.id();
    wait_for_thread(pid, "stdin-reader", true);
    producer.write_all(&random(64 << 10, 35)).unwrap();
    wait_for_thread(pid, "stdin-reader", false);
    std::fs::write(&go, b"go\n").unwrap();
    let output = succeeded(output_of(receiver));
    assert!(output.stdout.is_empty());
}

#[test]
fn a_command_that_reads_a_part_of_its_input_ends_the_run() {
    // `head` reads the first 100 bytes of 16 MiB of input, and exits. Then
    // no process reads the command's stdin, so a write of the stdin writer
    // fails with EPIPE, and the sender stops the input: INPUT_STOP makes
    // the receiver stop reading its stdin, and the data reader discards
    // the input that is still on its way, until END_ACK (DESIGN.md §6.6).
    // With and without compression.
    for config in ["", "compression_level = 0\n"] {
        let transfer = Transfer::new(&format!(
            "receiver_input_spool_limit = \"64KiB\"\n\
             sender_input_spool_limit = \"64KiB\"\n{config}"
        ));
        let input = random(16 << 20, 34);
        let path = transfer.env.path("input");
        std::fs::write(&path, &input).unwrap();
        let output = transfer
            .invocation(&[EXACT, "--stdin"], &refs(&args(&["head", "-c", "100"])))
            .stdin(File::open(&path).unwrap())
            .output()
            .unwrap();
        let output = succeeded(output);
        assert_eq!(output.stdout, input[..100], "{config}");
    }
}

#[test]
fn the_input_waits_in_the_sender_spool_until_the_command_reads_it() {
    // Each input spool has its own limit: the sender's comes from HELLO,
    // and the receiver's from the receiver's configuration (DESIGN.md
    // §6.6). Here the receiver's input spool holds at most 64 KiB, and the
    // sender's has no limit. The command reads its stdin only after the
    // whole input, 16 MiB, has passed the wrapper of the data session,
    // which copies what the receiver sends: the input waits in the
    // sender's input spool. If the receiver used the sender's limit, the
    // input would wait on the receiver instead, and the test would hang.
    let transfer = Transfer::new(
        "receiver_input_spool_limit = \"64KiB\"\n\
         sender_input_spool_limit = \"unlimited\"\ncompression_level = 0\n",
    );
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
    let receiver = transfer
        .invocation(&[EXACT, "--stdin"], &refs(&args(&["sh", "-c", &script])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let receiver = KillOnDrop(receiver);
    producer.write_all(&input).unwrap();
    drop(producer);
    // No deadline: if the input does not reach the sender, the test hangs.
    while std::fs::metadata(&captured).map_or(0, |meta| meta.len()) < input.len() as u64 {
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::write(&fifo, b"go\n").unwrap();
    let output = succeeded(output_of(receiver));
    assert!(output.stdout == input, "the output differs");
}

#[test]
fn the_producer_waits_when_both_input_spools_reach_their_limits() {
    // Each input spool holds at most 64 KiB, and the command reads its
    // stdin only after a signal. The producer of the receiver's stdin
    // writes 32 MiB, more than both spools and all the buffers in memory
    // between the producer and the command (about 2 MiB), so it cannot
    // finish before the command reads. If an input spool ignored its
    // limit, it would take the input, and the producer would write much
    // more (DESIGN.md §6.6). No event shows that the producer stays
    // blocked, so the test waits a fixed time for its writes. On a slow
    // machine, the wait can miss such a fault, but it cannot fail a
    // correct build. Then the command reads everything.
    let transfer = Transfer::new(
        "receiver_input_spool_limit = \"64KiB\"\n\
         sender_input_spool_limit = \"64KiB\"\ncompression_level = 0\n",
    );
    let input = random(32 << 20, 32);
    let fifo = transfer.env.path("go");
    mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!("read go <'{}'; exec cat", fifo.display());
    let (stdin, mut producer) = std::io::pipe().unwrap();
    let receiver = transfer
        .invocation(&[EXACT, "--stdin"], &refs(&args(&["sh", "-c", &script])))
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let receiver = KillOnDrop(receiver);
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
        so_far < 8 << 20,
        "the producer wrote {so_far} bytes while the command read nothing: \
         an input spool ignored its limit"
    );
    std::fs::write(&fifo, b"go\n").unwrap();
    let output = succeeded(output_of(receiver));
    produced.join().unwrap();
    assert!(output.stdout == input, "the output differs");
}

#[test]
fn sender_cache_false_stores_nothing_and_removes_earlier_lists() {
    // First populate the sender cache with caching enabled.
    let transfer = Transfer::new("");
    transfer.env.write("tree/data", &common::tiled(8, 5));
    let command = args(&["true"]);
    transfer.run_ok(&refs(&command));
    assert_eq!(transfer.cache_files("sender", "remote-hashlists").len(), 1);

    // Then disable it: the earlier list for this receiver/index is
    // removed and nothing new is stored (DESIGN.md §5.6).
    transfer.reconfigure(
        env!("CARGO_BIN_EXE_hashsync").as_bytes(),
        "sender_cache = false\n",
    );
    transfer.run_ok(&refs(&command));
    assert_eq!(transfer.cache_files("sender", "remote-hashlists").len(), 0);
    assert_eq!(
        transfer.cache_files("receiver", "local-hashlists").len(),
        1,
        "the receiver still persists its target"
    );
}

#[test]
fn bootstrap_quoting_survives_a_hostile_sender_command_path() {
    // Whitespace, quotes, shell metacharacters, and a non-UTF-8 byte in
    // the executable path (DESIGN.md §11), resolved by a real `sh`.
    let transfer = Transfer::new("");
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

    transfer.reconfigure(link.as_os_str().as_bytes(), "");
    let command = args(&["printf", "%s", "quoted-ok"]);
    let output = transfer.run_ok(&refs(&command));
    assert_eq!(output.stdout, b"quoted-ok");
}

#[test]
fn a_sender_with_another_protocol_version_is_rejected() {
    // A fake sender that sends only its version, 0xFFFFFFFF, on both
    // sessions: the receiver ends the run before any frame
    // (DESIGN.md §6.2).
    let transfer = Transfer::new("");
    let sender = transfer.env.path("other-version");
    std::fs::write(
        &sender,
        "#!/bin/sh\nprintf '\\377\\377\\377\\377'\nexec cat >/dev/null\n",
    )
    .unwrap();
    std::fs::set_permissions(&sender, std::fs::Permissions::from_mode(0o755)).unwrap();
    transfer.reconfigure(sender.as_os_str().as_bytes(), "");
    let command = args(&["true"]);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("speaks protocol version 4294967295, this hashsync"),
        "version mismatch:\n{stderr}"
    );
}

#[test]
fn a_data_session_that_ends_before_done_fails_the_run() {
    // A fake sender sends its version, HELLO, CACHE_RESULT, output, and
    // END, reads what the receiver sends, END_ACK included, and ends the
    // data session without DONE, as a sender that fails after END_ACK
    // does. The receiver wrote the output, so it read END; without
    // compression, only DONE can then fail. The run fails although the
    // output is complete (DESIGN.md §6.9). The fake control session sends
    // its version and stays open until its stdin ends.
    let transfer = Transfer::new("");
    // HELLO: an account ID and no base. END: status 0 and no error text.
    let mut hello = vec![0x11; 16];
    hello.push(0);
    let frames = [
        version().to_vec(),
        frame(HELLO, &hello),
        frame(CACHE_RESULT, b""),
        frame(LIT, b"output"),
        frame(END, &[0; 8]),
    ]
    .concat();
    let data = transfer.env.path("sender-frames");
    std::fs::write(&data, frames).unwrap();
    let control = transfer.env.path("sender-version");
    std::fs::write(&control, version()).unwrap();
    let sender = transfer.env.path("ends-before-done");
    std::fs::write(
        &sender,
        format!(
            r#"#!/bin/sh
if [ "$1" = serve ]; then
    cat '{}'
    exec cat >/dev/null
fi
cat '{}'
exec cat 3>&1 >/dev/null
"#,
            data.display(),
            control.display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&sender, std::fs::Permissions::from_mode(0o755)).unwrap();
    // Without compression, END needs no zstd frame.
    transfer.reconfigure(sender.as_os_str().as_bytes(), "compression_level = 0\n");
    let output = transfer.run(&[], &refs(&args(&["true"])));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr:\n{stderr}");
    assert_eq!(output.stdout, b"output");
    assert!(
        stderr.contains("the data session ended unexpectedly"),
        "stderr:\n{stderr}"
    );
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
/// ends meanwhile has no children file.
fn children(pid: u32) -> Vec<u32> {
    let mut children = Vec::new();
    for task in std::fs::read_dir(format!("/proc/{pid}/task")).unwrap() {
        let list =
            std::fs::read_to_string(task.unwrap().path().join("children")).unwrap_or_default();
        children.extend(
            list.split_whitespace()
                .map(|pid| pid.parse::<u32>().unwrap()),
        );
    }
    children
}

#[test]
fn a_termination_signal_decides_the_status_even_after_the_sessions_break() {
    // Ctrl-C reaches the receiver and its SSH children together. The dying
    // sessions can make another thread fail before the signal is handled,
    // but the status must report the signal (DESIGN.md §6.8). Force that
    // order: stop the receiver, send it the signal, kill both sessions,
    // and let the receiver continue with both events already present.
    let transfer = Transfer::new("");
    // The command opens a FIFO that the test holds open, and reports on
    // stderr that it runs: the sender relays stderr at once, but a line on
    // stdout, shorter than a block, would stay in the sender's scanner
    // until more output or EOF (DESIGN.md §6.3). Then it reads the FIFO
    // until EOF, so it runs until the test ends, also after the killed
    // sessions orphan it.
    let fifo = transfer.env.path("hold");
    let _hold = held_fifo(&fifo);
    let running = "the command runs";
    let script = format!("exec <'{}'; echo '{running}' >&2; exec cat", fifo.display());
    let command = args(&["sh", "-c", &script]);
    for (sent, status, name) in [(Signal::INT, 130, "SIGINT"), (Signal::TERM, 143, "SIGTERM")] {
        for _ in 0..4 {
            let mut receiver = KillOnDrop(
                transfer
                    .invocation(&[], &refs(&command))
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap(),
            );
            let mut stderr = BufReader::new(receiver.0.stderr.take().unwrap());
            let mut text = String::new();
            while !text.contains(running) {
                let n = stderr.read_line(&mut text).unwrap();
                assert!(n > 0, "the run ended before the command ran:\n{text}");
            }

            let pid = receiver.0.id();
            signal(pid, Signal::STOP);
            wait_until_stopped(&receiver.0);
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
            let exit = receiver.0.wait().unwrap();
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
fn the_receiver_closes_stdin_before_either_stdout_ends() {
    // Like an OpenSSH multiplexing client, the wrapper keeps the stdout of
    // each session open until its SSH child exits, and the sender exits
    // only after stdin EOF. The receiver closes both stdin pipes after
    // DONE, without waiting for the EOF of either stdout, so the run still
    // ends (DESIGN.md §6.9 and §10).
    let transfer = Transfer::new("");
    let blocks = tiled(16, 28);
    transfer.env.write("tree/blocks", &blocks);
    wrap_ssh(&transfer, "\"$ssh\" \"$@\"\nexit $?");
    let command = args(&["cat", transfer.env.path("tree/blocks").to_str().unwrap()]);
    let output = transfer.run_ok(&refs(&command));
    assert_eq!(output.stdout, blocks);
    assert_eq!(summary(&output), [blocks.len(), 0, blocks.len(), 0]);
}

#[test]
fn the_end_waits_for_a_process_that_keeps_the_control_session_open() {
    // After the control session's SSH exits, a process that inherited its
    // stdout keeps the pipe open for a moment, creates the marker, and
    // exits. The receiver reads each session to EOF and ends its threads
    // before it exits (DESIGN.md §6.9), so it cannot exit before the
    // marker exists, however the processes are scheduled. The normal run,
    // the read-only run, and a command that fails to start end the same
    // way.
    let transfer = Transfer::new("");
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
        (&[][..], &cat, 0),
        (&["--no-update"][..], &cat, 0),
        (&[][..], &missing, 127),
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
    // more QUERY, which the receiver skips in Completing. The receiver reads
    // both sessions to EOF before the summary (DESIGN.md §6.9 and §8), so
    // the summary counts exactly the bytes of both files, that QUERY
    // included. One changed block makes the scanner rescan. The late QUERY
    // is a plain frame, so this run has no compression: inside a zstd
    // stream, the receiver could not decode it.
    let transfer = Transfer::new("compression_level = 0\n");
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
    let output = succeeded(transfer.run(&["--no-update"], &refs(&command)));
    assert_eq!(output.stdout, blocks);
    assert_eq!(summary(&output), [blocks.len(), BLOCK, 31 * BLOCK, 0]);
    let read = |session: &str| std::fs::read(format!("{}.{session}", captured.display())).unwrap();
    let (data, control) = (read("data"), read("control"));
    assert!(
        control.ends_with(&late),
        "the run ended before the last QUERY"
    );
    assert_eq!(wire_bytes(&output), data.len());
    assert_eq!(traffic(&output).received, data.len() + control.len());
}

#[test]
fn both_directions_of_the_control_session_are_compressed() {
    // After the protocol version, the queries and the INPUT_STOP of the
    // command's exit form one zstd stream, and the answers another. No
    // frame ends either stream: the closure of the session does
    // (DESIGN.md §6.2). `tee` copies both directions of the control
    // session. Every block of the output is queried once, in order, and
    // answered yes. INPUT_STOP can come between any two queries, and as
    // the sender does not wait for its control writer at the end, the
    // closure can also cut it off (DESIGN.md §6.6 and §6.9).
    let transfer = Transfer::new("");
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
    let output = transfer.run_ok(&refs(&command));
    assert_eq!(output.stdout, blocks);
    assert_eq!(summary(&output), [blocks.len(), 0, blocks.len(), 0]);

    let offsets: Vec<u64> = (0..16).map(|i| (i * BLOCK) as u64).collect();
    // The frames of one direction, through the end of the copy, which
    // comes inside the zstd frame.
    let decode = |direction: &str| {
        let stream = std::fs::read(format!("{}.{direction}", captured.display())).unwrap();
        assert_eq!(stream[..4], version(), "{direction}");
        let mut stream = zstd::stream::read::Decoder::new(&stream[4..]).unwrap();
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
    let stops = queries
        .iter()
        .filter(|(kind, _)| *kind == INPUT_STOP)
        .count();
    assert!(stops <= 1, "{stops} INPUT_STOP frames");
    queries.retain(|frame| *frame != (INPUT_STOP, Vec::new()));
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

/// True once both SSH children of the receiver `pid` have exited: the
/// receiver does not reap them, so both are zombies until it exits.
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
    // ends and removes its directory. So the receiver waits in its end
    // (DESIGN.md §6.9). The signal thread still runs then: SIGTERM ends the
    // run with `128 + signal` (DESIGN.md §6.8).
    let transfer = Transfer::new("");
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
    let mut receiver = transfer
        .invocation(&[], &refs(&args(&["true"])))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = receiver.id();
    while !sessions_exited(pid) {
        assert!(receiver.try_wait().unwrap().is_none(), "the run ended");
        std::thread::sleep(Duration::from_millis(1));
    }
    kill_process(Pid::from_raw(pid as i32).unwrap(), Signal::TERM).unwrap();
    let output = receiver.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(143), "stderr:\n{stderr}");
    assert!(
        stderr.contains("terminated by SIGTERM"),
        "stderr:\n{stderr}"
    );
}
