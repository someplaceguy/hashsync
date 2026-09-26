//! Process exclusion of the index (DESIGN.md §4.4), tested with separate
//! processes: this test process holds a lock on the index's lock file while
//! the binary runs, as another `hashsync` process would. Its classic POSIX
//! locks conflict with the binary's open file description locks, whose
//! owners differ.

mod common;

use std::os::fd::OwnedFd;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;

use common::*;
use rustix::fs::{FlockOperation, Mode, OFlags};

/// Long enough that a process which does not wait has finished.
const SETTLE: Duration = Duration::from_millis(700);

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

fn indexed_env() -> Env {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&env.root("tree", ""));
    env.update();
    env
}

#[test]
fn update_waits_for_a_read_only_process() {
    let env = indexed_env();
    let reader = lock(&env, FlockOperation::LockShared);

    let mut child = env.command(&["update"]).spawn().unwrap();
    sleep(SETTLE);
    assert!(
        child.try_wait().unwrap().is_none(),
        "update did not wait for the reader"
    );

    drop(reader);
    let status = child.wait().unwrap();
    assert!(status.success());
    assert_eq!(env.count("paths"), 1);
}

#[test]
fn stats_waits_for_a_modifying_process() {
    let env = indexed_env();
    let writer = lock(&env, FlockOperation::LockExclusive);

    let mut child = env.command(&["stats"]).spawn().unwrap();
    sleep(SETTLE);
    assert!(
        child.try_wait().unwrap().is_none(),
        "stats did not wait for the writer"
    );

    drop(writer);
    let status = child.wait().unwrap();
    assert!(status.success());
}

#[test]
fn two_updates_run_one_after_the_other() {
    let env = indexed_env();
    let writer = lock(&env, FlockOperation::LockExclusive);

    let mut child = env.command(&["update"]).spawn().unwrap();
    sleep(SETTLE);
    assert!(
        child.try_wait().unwrap().is_none(),
        "update did not wait for the writer"
    );
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
