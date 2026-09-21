//! End-to-end `run` transfers (PLAN.md step 3) over a fake `ssh` that
//! executes the remote command string with a local `/bin/sh`, under a
//! separate HOME for the sender side. This exercises the real bootstrap
//! quoting, the rendezvous socket, both sessions, and the cache
//! publication rules on both sides.

mod common;

use std::ffi::{OsStr, OsString};
use std::io::BufRead;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use common::{Env, random, toml_literal};
use rustix::process::{Pid, Signal, kill_process};

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

fn signal(pid: u32, signal: Signal) {
    let pid = Pid::from_raw(pid as i32).unwrap();
    kill_process(pid, signal).unwrap();
}

/// The state letter of `pid` in `/proc`: `T` when stopped, `Z` for a
/// zombie.
fn state(pid: u32) -> Option<char> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit(')').next()?.trim_start().chars().next()
}

/// Wait until `pid` reaches `wanted`, within a bound: a regression must
/// fail the test, not hang it.
fn wait_for_state(pid: u32, wanted: char) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while state(pid) != Some(wanted) {
        assert!(
            Instant::now() < deadline,
            "{pid} never reached state {wanted}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The children of `pid`, started by any of its threads.
fn children(pid: u32) -> Vec<u32> {
    let mut children = Vec::new();
    for task in std::fs::read_dir(format!("/proc/{pid}/task")).unwrap() {
        let list = std::fs::read_to_string(task.unwrap().path().join("children")).unwrap();
        children.extend(
            list.split_whitespace()
                .map(|pid| pid.parse::<u32>().unwrap()),
        );
    }
    children
}

/// Kills a process that the test leaves behind, even when it fails.
struct KillOnDrop(u32);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if state(self.0).is_some_and(|state| state != 'Z') {
            signal(self.0, Signal::KILL);
        }
    }
}

#[test]
fn a_termination_signal_decides_the_status_even_after_the_sessions_break() {
    // Ctrl-C reaches the receiver and its SSH children together. The dying
    // sessions can make another thread fail before the signal is handled,
    // but the status must report the signal (DESIGN.md §6.6). Force that
    // order: stop the receiver, send it the signal, kill both sessions,
    // and let the receiver continue with both events already present.
    let transfer = Transfer::new("");
    let command = args(&["sh", "-c", "echo $$; exec sleep 60"]);
    for (sent, status, name) in [(Signal::INT, 130, "SIGINT"), (Signal::TERM, 143, "SIGTERM")] {
        for _ in 0..4 {
            let mut receiver = transfer
                .invocation(&[], &refs(&command))
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let pid = receiver.id();
            let _receiver_guard = KillOnDrop(pid);
            // The command runs once its first line arrives. Killing the
            // sessions orphans it, so kill it at the end.
            let mut line = String::new();
            std::io::BufReader::new(receiver.stdout.take().unwrap())
                .read_line(&mut line)
                .unwrap();
            let _command_guard = KillOnDrop(line.trim().parse().unwrap());

            signal(pid, Signal::STOP);
            wait_for_state(pid, 'T');
            signal(pid, sent);
            let sessions = children(pid);
            assert_eq!(sessions.len(), 2, "both SSH sessions: {sessions:?}");
            for &session in &sessions {
                signal(session, Signal::KILL);
            }
            for &session in &sessions {
                wait_for_state(session, 'Z');
            }
            signal(pid, Signal::CONT);

            let output = receiver.wait_with_output().unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert_eq!(output.status.code(), Some(status), "stderr:\n{stderr}");
            assert!(
                stderr.contains(&format!("terminated by {name}")),
                "stderr:\n{stderr}"
            );
        }
    }
}
