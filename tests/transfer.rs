//! End-to-end `run` transfers (PLAN.md steps 3 to 5) over a fake `ssh`
//! that executes the remote command string with a local `/bin/sh`, under a
//! separate HOME for the sender side. This exercises the real bootstrap
//! quoting, the rendezvous socket, both sessions, the cache publication
//! rules on both sides, block references with their pins, runs, and the
//! compression stream.
//!
//! No wait of these tests has a deadline: a regression hangs the test, so
//! that a debugger can examine it.

mod common;

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

use common::{
    BLOCK, CACHE_RESULT, END, Env, HELLO, KillOnDrop, LIT, frame, proc_state, random, tiled,
    toml_literal, version,
};
use rustix::event::{PollFd, PollFlags, poll};
use rustix::fs::{CWD, Mode, mkfifoat};
use rustix::io::retry_on_intr;
use rustix::process::{
    Pid, PidfdFlags, Signal, WaitId, WaitIdOptions, kill_process, pidfd_open, waitid,
};

const HOST: &str = "testhost";

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
    /// of both sides prepared.
    fn invocation(&self, flags: &[&str], command: &[&OsStr]) -> Command {
        let mut invocation = self.env.command(&["run"]);
        invocation
            .args(flags)
            .arg(HOST)
            .arg("--")
            .args(command)
            .env("XDG_CONFIG_HOME", self.env.path("receiver/config"))
            .env("XDG_CACHE_HOME", self.env.path("receiver/cache"))
            .env("HASHSYNC_TEST_SENDER_HOME", self.env.path("sender/home"))
            .env(
                "HASHSYNC_TEST_SENDER_RUNTIME",
                self.env.path("sender/runtime"),
            );
        invocation
    }

    fn run(&self, flags: &[&str], command: &[&OsStr]) -> Output {
        self.invocation(flags, command).output().unwrap()
    }

    fn run_ok(&self, command: &[&OsStr]) -> Output {
        let output = self.run(&[], command);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "run failed:\n{stderr}");
        assert!(
            stderr.contains("bytes output"),
            "missing summary:\n{stderr}"
        );
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

/// The byte counts of the summary line: output, literal, reference, run,
/// wire, hash-list, and delta bytes.
fn summary_counts(output: &Output) -> Vec<usize> {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let line = stderr
        .lines()
        .find(|line| line.contains("bytes output"))
        .unwrap_or_else(|| panic!("no summary:\n{stderr}"));
    line.split(|c: char| !c.is_ascii_digit())
        .filter(|word| !word.is_empty())
        .map(|word| word.parse().unwrap())
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
            "{payload_len} bytes output ({payload_len} literal, 0 reference, 0 run)"
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
    // concurrently, and the pins leave nothing behind (DESIGN.md §6.4).
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

    let pins = transfer.env.path("receiver/cache/hashsync/pins");
    assert_eq!(mode(&pins), 0o700, "the pin directory is private");
    assert_eq!(std::fs::read_dir(&pins).unwrap().count(), 0);
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
fn pins_live_in_the_configured_directory() {
    let transfer = Transfer::new("");
    let pins = transfer.env.path("pins/nested");
    let hashsync = env!("CARGO_BIN_EXE_hashsync").as_bytes();
    let pin_directory = |path: &std::path::Path| {
        format!(
            "pin_directory = '{}'\n",
            toml_literal(path.as_os_str().as_bytes())
        )
    };
    transfer.reconfigure(hashsync, &pin_directory(&pins));
    let blocks = tiled(8, 16);
    transfer.env.write("tree/blocks", &blocks);
    let command = args(&["cat", transfer.env.path("tree/blocks").to_str().unwrap()]);
    let output = transfer.run_ok(&refs(&command));
    assert_eq!(output.stdout, blocks);
    assert_eq!(summary(&output), [8 * BLOCK, 0, 8 * BLOCK, 0]);
    assert_eq!(mode(&pins), 0o700);
    assert_eq!(std::fs::read_dir(&pins).unwrap().count(), 0);

    // A pin directory that cannot exist fails the run before the
    // sessions start.
    transfer.env.write("not-a-directory", b"x");
    let blocked = transfer.env.path("not-a-directory/pins");
    transfer.reconfigure(hashsync, &pin_directory(&blocked));
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot create pin directory"), "{stderr}");
}

#[test]
fn a_pin_directory_without_hole_punching_fails_the_run() {
    // ramfs cannot punch holes; the setup probe finds out before the
    // sessions start (DESIGN.md §6.4).
    if !common::namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    let transfer = Transfer::new("");
    let pins = transfer.env.path("ramfs");
    std::fs::create_dir(&pins).unwrap();
    transfer.reconfigure(
        env!("CARGO_BIN_EXE_hashsync").as_bytes(),
        &format!(
            "pin_directory = '{}'\n",
            toml_literal(pins.as_os_str().as_bytes())
        ),
    );
    let run = transfer.invocation(&[], &refs(&args(&["true"])));
    let mut command = Command::new("unshare");
    command
        .args([
            "-Urm",
            "sh",
            "-c",
            "mount -t ramfs none \"$0\" || exit 99; exec \"$@\"",
        ])
        .arg(&pins)
        .arg(run.get_program())
        .args(run.get_args());
    for (key, value) in run.get_envs() {
        command.env(key, value.unwrap());
    }
    let output = command.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("does not support hole punching"),
        "{stderr}"
    );
}

#[test]
fn a_slow_consumer_and_a_one_window_lookahead_keep_the_output_exact() {
    // The scanner may look only one window ahead, so every query waits for
    // its answer before scanning goes on, and stdout drains slowly.
    // Backpressure reaches the sender; output, answers, and pin release
    // stay in order (DESIGN.md §6.3 and §6.4).
    let transfer = Transfer::new(&format!("lookahead = {BLOCK}\n"));
    let blocks = tiled(256, 17);
    transfer.env.write("tree/big", &blocks);
    let script = format!(
        "printf x; cat '{0}'; printf yz; cat '{0}'",
        transfer.env.path("tree/big").display()
    );
    let mut receiver = transfer
        .invocation(&[], &refs(&args(&["sh", "-c", &script])))
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
    // status (DESIGN.md §6.5).
    let command = args(&["sh", "-c", "echo partial; exit 3"]);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.stdout, b"partial\n");
    assert_eq!(output.status.code(), Some(3));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("bytes output"), "summary:\n{stderr}");
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
    // Successful completion requires the full relay (DESIGN.md §6.7).
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
    // (DESIGN.md §5.6 and §6.5).
    let transfer = Transfer::new("");
    let command = args(&["true"]);
    transfer.env.chmod("receiver/cache", 0o555);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot create"),
        "receiver cache failure:\n{stderr}"
    );
    transfer.env.chmod("receiver/cache", 0o755);

    // Sender: the same policy; its death fails the whole run, and its
    // message reaches the receiver's stderr through SSH.
    transfer.env.mkdir("sender/home/.cache");
    transfer.env.chmod("sender/home/.cache", 0o555);
    let output = transfer.run(&[], &refs(&command));
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot create"),
        "sender cache failure:\n{stderr}"
    );
    transfer.env.chmod("sender/home/.cache", 0o755);

    // Writable again: the run succeeds.
    transfer.run_ok(&refs(&command));
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
    // output is complete (DESIGN.md §6.7). The fake control session sends
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
    // but the status must report the signal (DESIGN.md §6.6). Force that
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
    // ends (DESIGN.md §6.7 and §10).
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
    // before it exits (DESIGN.md §6.7), so it cannot exit before the
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
    // (DESIGN.md §6.7). The signal thread still runs then: SIGTERM ends the
    // run with `128 + signal` (DESIGN.md §6.6).
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
