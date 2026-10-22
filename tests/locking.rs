//! Process exclusion of the index (DESIGN.md §4.4), tested with separate
//! processes: this test process holds a lock on the index's lock file while
//! the binary runs, as another `hashsync` process would. Its classic POSIX
//! locks conflict with the binary's open file description locks, whose
//! owners differ. `/proc/locks` shows when the binary waits for a lock.

mod common;

use std::io::Read;
use std::os::fd::OwnedFd;
use std::process::{Child, ChildStderr, Stdio};
use std::thread::sleep;
use std::time::Duration;

use common::*;
use rustix::fs::{FlockOperation, Mode, OFlags};

/// Hold a lock on the index's lock file until the result is dropped:
/// shared like a read-only process, or exclusive like a modifying one,
/// which needs the file open for writing.
fn lock(env: &Env, operation: FlockOperation) -> OwnedFd {
    let access = match operation {
        FlockOperation::LockExclusive => OFlags::RDWR,
        _ => OFlags::RDONLY,
    };
    let file = rustix::fs::open(
        env.index_path().join("hashsync.lock"),
        access | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    rustix::fs::fcntl_lock(&file, operation).unwrap();
    file
}

/// True when a lock request waits for `lock`, a lock of this process. In
/// `/proc/locks`, the requests that wait for a lock follow its line, on
/// lines whose second field is `->`. A request of the binary shows no PID,
/// because an open file description owns it. Thus this function finds the
/// line of `lock` by the PID of this process and the inode of the lock
/// file.
fn has_waiter(lock: &OwnedFd) -> bool {
    let pid = std::process::id().to_string();
    let inode = rustix::fs::fstat(lock).unwrap().st_ino.to_string();
    let mut after_lock = false;
    for line in std::fs::read_to_string("/proc/locks").unwrap().lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        // Each line ends with the PID, `MAJOR:MINOR:INODE`, and the range.
        let [_, second, .., owner, file, _, _] = fields[..] else {
            continue;
        };
        if second == "->" {
            if after_lock {
                return true;
            }
        } else {
            after_lock = owner == pid && file.rsplit(':').next() == Some(inode.as_str());
        }
    }
    false
}

/// Wait until `child` waits for `lock`; a child that ends first did not
/// wait. No event tells when a request starts to wait, so this polls, with
/// no deadline: a child that neither waits nor ends hangs the test, and a
/// debugger can inspect it.
fn wait_for_waiter(child: &mut Child, lock: &OwnedFd) {
    while !has_waiter(lock) {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("the child ended without waiting for the lock: {status}");
        }
        sleep(Duration::from_millis(10));
    }
}

fn indexed_env() -> Env {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&env.root("tree", ""));
    env.update();
    env
}

/// The note of a command that names the configuration file of `env` and
/// its index, the first that it writes (DESIGN.md §7).
fn files_note(env: &Env) -> String {
    format!(
        "hashsync: using config {} and index {}\n",
        quoted(env.config_path()),
        quoted(env.index_path())
    )
}

/// The note of a process that waits for the lock of the index of `env`.
fn waiting_note(env: &Env) -> String {
    format!(
        "hashsync: waiting for the lock of index {}\n",
        quoted(env.index_path())
    )
}

/// The note of a process that got the lock of the index of `env` after a
/// wait.
fn acquired_note(env: &Env) -> String {
    format!(
        "hashsync: acquired the lock of index {}\n",
        quoted(env.index_path())
    )
}

/// The text that the child has written to `stderr` so far, read without
/// waiting for more.
fn written_so_far(stderr: &mut ChildStderr) -> String {
    rustix::fs::fcntl_setfl(&*stderr, OFlags::NONBLOCK).unwrap();
    let mut text = Vec::new();
    let error = stderr.read_to_end(&mut text).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock, "{error}");
    rustix::fs::fcntl_setfl(&*stderr, OFlags::empty()).unwrap();
    String::from_utf8(text).unwrap()
}

#[test]
fn update_waits_for_a_read_only_process() {
    // The update says that it waits, and for which index, so that a long
    // wait does not look like a hang. Once it has the lock, it says so
    // before it updates, so that the update does not look like more
    // waiting. The note that names the files and the note of the wait are
    // written before the wait starts, so while the child waits, stderr
    // holds these two notes alone.
    let env = indexed_env();
    let reader = lock(&env, FlockOperation::LockShared);

    let mut child = env
        .command(&["update"])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_waiter(&mut child, &reader);
    let mut stderr = child.stderr.take().unwrap();
    let notes = files_note(&env) + &waiting_note(&env);
    assert_eq!(written_so_far(&mut stderr), notes);

    drop(reader);
    let mut rest = String::new();
    stderr.read_to_string(&mut rest).unwrap();
    assert!(child.wait().unwrap().success(), "{rest}");
    assert_eq!(env.count("paths"), 1);
    assert!(rest.starts_with(&acquired_note(&env)), "{rest}");
    assert!(rest.contains("updated index"), "{rest}");
    assert!(!rest.contains("waiting"), "{rest}");
}

#[test]
fn prune_waits_for_a_read_only_process() {
    // Prune changes the index, so it takes the exclusive lock, and waits
    // until no read-only process has the index (DESIGN.md §4.4).
    let env = indexed_env();
    let reader = lock(&env, FlockOperation::LockShared);

    let mut child = env
        .command(&["prune"])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_waiter(&mut child, &reader);

    drop(reader);
    let output = child.wait_with_output().unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(output.status.success(), "{stderr}");
    let notes = files_note(&env) + &waiting_note(&env) + &acquired_note(&env);
    assert!(stderr.starts_with(&notes), "{stderr}");
    assert!(stderr.contains("hashsync: pruned index in "), "{stderr}");
}

#[test]
fn stats_waits_for_a_modifying_process() {
    let env = indexed_env();
    let writer = lock(&env, FlockOperation::LockExclusive);

    let mut child = env
        .command(&["stats"])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_waiter(&mut child, &writer);

    drop(writer);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    let notes = files_note(&env) + &waiting_note(&env) + &acquired_note(&env);
    assert_eq!(stderr, notes);
}

#[test]
fn a_quiet_process_waits_for_the_lock_without_a_note() {
    // The notes of a lock wait are progress messages, which `--quiet`
    // keeps off stderr (DESIGN.md §4.4 and §8).
    let env = indexed_env();
    let reader = lock(&env, FlockOperation::LockShared);

    let mut child = env
        .command(&["update", "--quiet"])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_waiter(&mut child, &reader);

    drop(reader);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stderr).unwrap(), "");
}

#[test]
fn a_lock_taken_without_a_wait_takes_no_note() {
    // Only a wait takes the notes of the lock (DESIGN.md §4.4): `stats`
    // shares the lock with a reader, and writes only the note that names
    // its files, and `update` finds no holder.
    let env = indexed_env();
    let reader = lock(&env, FlockOperation::LockShared);
    let output = env.run(&["stats"]);
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stderr).unwrap(), files_note(&env));
    drop(reader);
    let stderr = env.update();
    assert!(!stderr.contains("the lock of index"), "{stderr}");
}

#[test]
fn two_updates_run_one_after_the_other() {
    let env = indexed_env();
    let writer = lock(&env, FlockOperation::LockExclusive);

    let mut child = env.command(&["update"]).spawn().unwrap();
    wait_for_waiter(&mut child, &writer);
    drop(writer);
    assert!(child.wait().unwrap().success());
}

#[test]
fn read_only_processes_coexist() {
    let env = indexed_env();
    let _reader = lock(&env, FlockOperation::LockShared);
    let stats = env.stats();
    assert!(stats.contains("paths:            1"), "{stats}");
}

/// Updates started together, also on an index that does not exist yet,
/// all succeed one after the other and leave a consistent index. Their
/// order and overlap do not change the result.
#[test]
fn concurrent_updates_all_succeed() {
    let env = Env::new();
    for i in 0..20 {
        env.write(format!("tree/{i}"), &random(BLOCK * 4, i));
    }
    env.configure(&env.root("tree", ""));
    for round in 0..2 {
        let children: Vec<_> = (0..4)
            .map(|_| {
                env.command(&["update"])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap()
            })
            .collect();
        for child in children {
            let output = child.wait_with_output().unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "round {round}:\n{stderr}");
        }
        let index = env.index();
        index.assert_consistent();
        assert_eq!(index.count("paths"), 20);
        assert_eq!(index.count("blocks"), 20 * 4);
        assert_eq!(index.incomplete_inodes(), 0);
    }
}
