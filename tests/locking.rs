//! Process exclusion of the index (DESIGN.md §4.4), tested with separate
//! processes: this test process holds a connection while the binary runs.

mod common;

use std::thread::sleep;
use std::time::Duration;

use common::*;
use rusqlite::{Connection, OpenFlags};

/// Long enough that a process which does not wait has finished.
const SETTLE: Duration = Duration::from_millis(700);

#[test]
fn update_waits_for_a_read_only_keeper() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&env.root("tree", ""));
    env.update();

    let keeper =
        Connection::open_with_flags(env.index_path(), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    keeper.execute_batch("BEGIN").unwrap();
    keeper
        .query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(()))
        .unwrap();

    let mut child = env.command(&["update"]).spawn().unwrap();
    sleep(SETTLE);
    assert!(
        child.try_wait().unwrap().is_none(),
        "update did not wait for the reader"
    );

    drop(keeper);
    let status = child.wait().unwrap();
    assert!(status.success());
    assert_eq!(env.count("SELECT count(*) FROM paths"), 1);
}

#[test]
fn stats_waits_for_a_writer() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&env.root("tree", ""));
    env.update();

    let writer = Connection::open_with_flags_and_vfs(
        env.index_path(),
        OpenFlags::SQLITE_OPEN_READ_WRITE,
        "unix-excl",
    )
    .unwrap();
    writer
        .query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(()))
        .unwrap();

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
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&env.root("tree", ""));
    env.update();

    let writer = Connection::open_with_flags_and_vfs(
        env.index_path(),
        OpenFlags::SQLITE_OPEN_READ_WRITE,
        "unix-excl",
    )
    .unwrap();
    writer
        .query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(()))
        .unwrap();

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
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&env.root("tree", ""));
    env.update();

    let keeper =
        Connection::open_with_flags(env.index_path(), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    keeper.execute_batch("BEGIN").unwrap();
    keeper
        .query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(()))
        .unwrap();
    let stats = env.stats();
    assert!(stats.contains("paths:            1"), "{stats}");
}
