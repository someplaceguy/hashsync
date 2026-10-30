// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! `hashsync prune` through the binary (DESIGN.md §4.2 and §8): the roots
//! and paths that it removes from the index without hashing, the roots
//! that it looks into, and the compaction at its end, which `hashsync
//! compact` and `update --compact` do too.

mod common;

use std::os::unix::ffi::OsStrExt;

use common::*;
use rocksdb::WriteBatch;

/// Run `hashsync WORDS...`, and require success. Returns stderr.
fn succeed(env: &Env, words: &[&str]) -> String {
    let output = env.run(words);
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(output.status.success(), "{words:?} failed:\n{stderr}");
    stderr
}

/// Run `prune` with `options`, and require success. Returns stderr.
fn prune(env: &Env, options: &[&str]) -> String {
    succeed(env, &[&["prune"], options].concat())
}

/// What the note of a prune says that it removed: roots, paths, and stale
/// inodes.
fn pruned(stderr: &str) -> [u64; 3] {
    let line = stderr
        .lines()
        .find_map(|line| line.strip_prefix("hashsync: pruned index in "))
        .unwrap_or_else(|| panic!("no note of the prune:\n{stderr}"));
    let (_, counts) = line.split_once(": ").unwrap();
    let counts: Vec<u64> = counts
        .split(|c: char| !c.is_ascii_digit())
        .filter(|number| !number.is_empty())
        .map(|number| number.parse().unwrap())
        .collect();
    counts.try_into().unwrap()
}

/// Prune removes the paths of the files that a mutable root no longer
/// holds, and the inodes and blocks that no other path keeps, but not those
/// of a hard link that is left. A path that leads to a directory now, or
/// only through a symlink, holds no file of the root either (DESIGN.md
/// §4.5). Prune hashes no file: a changed file keeps its old row until the
/// next update. A root without paths stays (DESIGN.md §4.2).
#[test]
fn prune_removes_the_files_that_are_gone_without_hashing() {
    let env = Env::new();
    env.write("tree/keep", &random(BLOCK, 1));
    env.write("tree/gone", &random(BLOCK, 2));
    env.write("tree/sub/gone", &random(BLOCK, 3));
    env.write("tree/changed", &random(BLOCK, 4));
    env.write("tree/linked", &random(BLOCK, 5));
    env.write("tree/replaced", &random(BLOCK, 7));
    std::fs::hard_link(env.path("tree/linked"), env.path("tree/link")).unwrap();
    env.mkdir("empty");
    env.configure(&format!(
        "{}{}",
        env.root("tree", ""),
        env.root("empty", "")
    ));
    env.update();
    let old = env.strong_prefixes("changed");

    std::fs::remove_file(env.path("tree/gone")).unwrap();
    std::fs::remove_dir_all(env.path("tree/sub")).unwrap();
    env.write("outside/gone", &random(BLOCK, 3));
    std::os::unix::fs::symlink(env.path("outside"), env.path("tree/sub")).unwrap();
    std::fs::remove_file(env.path("tree/linked")).unwrap();
    std::fs::remove_file(env.path("tree/replaced")).unwrap();
    env.mkdir("tree/replaced");
    env.write("tree/changed", &random(BLOCK, 6));
    let stderr = prune(&env, &[]);
    assert!(warnings(&stderr).is_empty(), "{stderr}");
    assert_eq!(pruned(&stderr), [0, 4, 3], "{stderr}");
    assert_eq!(env.tree_paths(), ["changed", "keep", "link"]);
    assert_eq!(env.strong_prefixes("changed"), old);
    assert_eq!(env.count("roots"), 2);
    env.index().assert_consistent();

    // The update hashes the changed file, and the one of `link`, whose
    // ctime the removal of its other link changed.
    let stderr = env.update();
    assert!(stderr.contains(" 2 files hashed"), "{stderr}");
    assert_ne!(env.strong_prefixes("changed"), old);
}

/// A root goes with its paths when the configuration no longer names it,
/// and when none of its copies is there any more, and so do the inodes and
/// blocks that only it kept. An immutable root that comes back is scanned
/// again by the next update (DESIGN.md §4.2).
#[test]
fn roots_that_no_copy_holds_go() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.write("old/f", &random(BLOCK, 2));
    env.write("store/a/f", &random(BLOCK, 3));
    env.write("store/b/f", &random(BLOCK, 4));
    let store = env.root("store/*", "immutable = true\nidentifier = 'name'");
    env.configure(&format!(
        "{}{}{store}",
        env.root("tree", ""),
        env.root("old", "")
    ));
    env.update();

    env.configure(&format!("{}{store}", env.root("tree", "")));
    let b = env.path("store/b");
    std::fs::rename(&b, env.path("b-away")).unwrap();
    let stderr = prune(&env, &[]);
    assert_eq!(pruned(&stderr), [2, 2, 2], "{stderr}");
    assert_eq!(indexed(&env), [pair("store/a", "f"), pair("tree", "f")]);
    assert_eq!(env.count("blocks"), 2);
    env.index().assert_consistent();

    std::fs::rename(env.path("b-away"), &b).unwrap();
    let stderr = env.update();
    assert_eq!(groups(&stderr), [1, 1, 0]);
    assert_eq!(
        indexed(&env),
        [
            pair("store/a", "f"),
            pair("store/b", "f"),
            pair("tree", "f")
        ]
    );
}

/// Prune looks into immutable roots only with `--scan-immutable`: without
/// it, a file that is gone from a completed group keeps its path. With it,
/// the path goes unless another copy of the group holds the file, and the
/// group stays complete: a file that is gone is no failure (DESIGN.md
/// §4.2).
#[test]
fn immutable_roots_are_looked_into_with_scan_immutable() {
    let env = Env::new();
    for copy in ["a", "b"] {
        for (name, seed) in [("f", 1), ("g", 2), ("h", 3)] {
            env.write(format!("copies/{copy}/data/{name}"), &random(BLOCK, seed));
        }
    }
    let copy = "immutable = true\nidentifier = 'name'";
    env.configure(&format!(
        "{}{}",
        env.root("copies/a/data", copy),
        env.root("copies/b/data", copy)
    ));
    env.update();

    // The scanned copy no longer has `g`, and neither copy has `h`.
    std::fs::remove_file(env.path("copies/a/data/g")).unwrap();
    for copy in ["a", "b"] {
        std::fs::remove_file(env.path(format!("copies/{copy}/data/h"))).unwrap();
    }
    let stderr = prune(&env, &[]);
    assert_eq!(pruned(&stderr), [0, 0, 0], "{stderr}");
    assert_eq!(env.count("paths"), 3);

    let stderr = prune(&env, &["--scan-immutable"]);
    assert!(warnings(&stderr).is_empty(), "{stderr}");
    assert!(!stderr.contains("immutable roots"), "{stderr}");
    assert_eq!(pruned(&stderr), [0, 1, 1], "{stderr}");
    let data = "copies/a/data";
    assert_eq!(indexed(&env), [pair(data, "f"), pair(data, "g")]);
    let stderr = env.update();
    assert_eq!(groups(&stderr), [1, 0, 0]);
}

/// Prune removes the paths that the exclusions of their root now leave
/// out, by name and by absolute path, also below an excluded directory,
/// since a scan of the root would skip them: the next update finds nothing
/// else to remove. In a completed group, prune does so only with
/// `--scan-immutable`, and the group stays complete (DESIGN.md §4.2).
#[test]
fn paths_that_exclusions_leave_out_go() {
    let env = Env::new();
    env.write("tree/keep", &random(BLOCK, 1));
    env.write("tree/x.tmp", &random(BLOCK, 2));
    env.write("tree/sub/y.tmp", &random(BLOCK, 3));
    env.write("tree/.cache/sub/f", &random(BLOCK, 4));
    env.write("tree/absolute/f", &random(BLOCK, 5));
    env.write("tree/other/absolute/f", &random(BLOCK, 6));
    env.write("group/f", &random(BLOCK, 7));
    env.write("group/g.tmp", &random(BLOCK, 8));
    let configure = |tree: &str, group: &str| {
        env.configure(&format!(
            "{}{}",
            env.root("tree", tree),
            env.root("group", &format!("immutable = true\n{group}"))
        ));
    };
    configure("", "");
    env.update();

    let absolute = format!("{}/absolute", env.tree().display());
    configure(
        &format!("exclude = ['*.tmp', '.cache', '{absolute}']"),
        "exclude = ['*.tmp']",
    );
    let stderr = prune(&env, &[]);
    assert!(warnings(&stderr).is_empty(), "{stderr}");
    assert_eq!(pruned(&stderr), [0, 4, 4], "{stderr}");
    let tree = [pair("tree", "keep"), pair("tree", "other/absolute/f")];
    let group = [pair("group", "f"), pair("group", "g.tmp")];
    assert_eq!(indexed(&env), [&group[..], &tree].concat());
    let stderr = env.update();
    assert!(stderr.contains(" 0 files hashed"), "{stderr}");
    assert!(stderr.contains(" 0 stale inodes removed"), "{stderr}");
    assert_eq!(groups(&stderr), [1, 0, 0]);

    let stderr = prune(&env, &["--scan-immutable"]);
    assert!(!stderr.contains("immutable roots"), "{stderr}");
    assert_eq!(pruned(&stderr), [0, 1, 1], "{stderr}");
    assert_eq!(indexed(&env), [&group[..1], &tree].concat());
    let stderr = env.update();
    assert_eq!(groups(&stderr), [1, 0, 0]);
}

/// A path whose lookup fails goes too, as an update drops what it cannot
/// reach: no candidate read could reach it either. A directory that cannot
/// be searched gets one warning, as in an update, whatever the number of
/// indexed paths below it, in subdirectories too. In a completed group, such
/// a directory below the root leaves the group complete, as a scan of the
/// copy would, which skips the directory without a failure: here `group`.
/// But a copy whose root cannot be searched cannot be read at all, so its
/// group becomes unfinished, and the next update scans it again, and
/// indexes the files once they can be reached: here `sealed` (DESIGN.md
/// §4.2 and §4.5).
#[test]
fn a_path_that_cannot_be_looked_up_goes() {
    if skip_as_root() {
        return;
    }
    let env = Env::new();
    for root in ["tree", "group"] {
        env.write(format!("{root}/ok"), &random(BLOCK, 1));
        env.write(format!("{root}/closed/f"), &random(BLOCK, 2));
    }
    env.write("tree/closed/g", &random(BLOCK, 3));
    env.write("tree/closed/sub/h", &random(BLOCK, 4));
    env.write("sealed/f", &random(BLOCK, 5));
    env.configure(&format!(
        "{}{}{}",
        env.root("tree", ""),
        env.root("group", "immutable = true"),
        env.root("sealed", "immutable = true")
    ));
    env.update();

    let closed = [
        ("tree/closed", 0o755),
        ("group/closed", 0o755),
        ("sealed", 0o755),
    ];
    let stderr = env.run_with_closed(&["prune", "--scan-immutable"], &closed);
    let mut found = warnings(&stderr);
    found.sort();
    assert_eq!(found.len(), 3, "{stderr}");
    assert_eq!(
        found[0],
        denied(env.path("group/closed/f"), "open"),
        "{stderr}"
    );
    assert_eq!(found[1], denied(env.path("sealed/f"), "open"), "{stderr}");
    // The warning names one of the paths below the closed directory.
    let below =
        ["f", "g", "sub/h"].map(|rel| denied(env.path(format!("tree/closed/{rel}")), "open"));
    assert!(below.contains(&found[2].to_string()), "{stderr}");
    assert_eq!(pruned(&stderr), [0, 5, 5], "{stderr}");
    assert!(
        stderr.contains(
            "hashsync: immutable roots: 1 incomplete, which the next update scans again\n"
        ),
        "{stderr}"
    );
    assert_eq!(indexed(&env), [pair("group", "ok"), pair("tree", "ok")]);
    let stats = env.stats();
    assert!(stats.contains("(1 complete immutable)"), "{stats}");

    let stderr = env.update();
    assert_eq!(groups(&stderr), [1, 1, 0]);
    assert_eq!(
        indexed(&env),
        [
            pair("group", "ok"),
            pair("sealed", "f"),
            pair("tree", "closed/f"),
            pair("tree", "closed/g"),
            pair("tree", "closed/sub/h"),
            pair("tree", "ok")
        ]
    );
}

/// A scan opens one name at a time, so it indexes files at paths longer
/// than the 4095 bytes that the kernel takes in one call. Prune looks such
/// a path up in pieces, so its file stays without a warning, in a mutable
/// root and in a completed group, and goes once it is gone (DESIGN.md
/// §4.2).
#[test]
fn files_at_paths_longer_than_the_kernel_takes_in_one_call_stay() {
    let env = Env::new();
    let rel = format!("{}/f", long_dirs());
    env.write_long(&format!("tree/{rel}"), &random(BLOCK, 1));
    env.write_long(&format!("group/{rel}"), &random(BLOCK, 2));
    env.configure(&format!(
        "{}{}",
        env.root("tree", ""),
        env.root("group", "immutable = true")
    ));
    env.update();
    assert_eq!(indexed(&env), [pair("group", &rel), pair("tree", &rel)]);

    let stderr = prune(&env, &["--scan-immutable"]);
    assert!(warnings(&stderr).is_empty(), "{stderr}");
    assert!(!stderr.contains("immutable roots"), "{stderr}");
    assert_eq!(pruned(&stderr), [0, 0, 0], "{stderr}");

    let top = rel.split('/').next().unwrap();
    std::fs::rename(env.path(format!("tree/{top}")), env.path("moved")).unwrap();
    let stderr = prune(&env, &[]);
    assert!(warnings(&stderr).is_empty(), "{stderr}");
    assert_eq!(pruned(&stderr), [0, 1, 1], "{stderr}");
    assert_eq!(indexed(&env), [pair("group", &rel)]);
}

/// A copy can vanish after root resolution. Prune then finds it missing
/// when it looks into its root, and gives the warning that root resolution
/// would have given, unless the copy may vanish, as the scan of an update
/// does. A root none of whose copies is there goes (DESIGN.md §4.2).
#[test]
fn a_root_that_vanishes_after_resolution_goes() {
    let env = Env::new();
    env.write("a/f", &random(BLOCK, 1));
    env.write("b/f", &random(BLOCK, 2));
    env.configure(&format!(
        "{}{}",
        env.root("a", ""),
        env.root("b", "may_vanish = true")
    ));
    env.update();

    let stderr = env.run_after_resolution(&["prune"], || {
        for root in ["a", "b"] {
            std::fs::rename(env.path(root), env.path(format!("{root}-away"))).unwrap();
        }
    });
    assert_eq!(warnings(&stderr), [missing(env.path("a"))], "{stderr}");
    assert_eq!(pruned(&stderr), [2, 2, 2], "{stderr}");
    assert_eq!(env.count("roots"), 0);
}

/// A completed group that a failed lookup leaves unfinished keeps a row
/// with the path of the first copy that prune opened, by which a run with
/// `--no-update` finds the copies of an unfinished group, and so does the
/// next prune, which keeps the group, and the next update, which scans it
/// there. The paths that a copy holds stay. Here the copy that the row
/// named vanishes after root resolution, which still found it, a file goes
/// from every copy, and the root of the last copy cannot be searched, so
/// prune cannot tell that this copy does not hold the file (DESIGN.md
/// §4.2).
#[test]
fn an_unfinished_group_keeps_the_path_of_a_current_copy() {
    if skip_as_root() {
        return;
    }
    let env = Env::new();
    let copies = ["a", "b", "c"];
    for copy in copies {
        env.write(format!("copies/{copy}/data/ok"), &random(BLOCK, 1));
        env.write(format!("copies/{copy}/data/gone"), &random(BLOCK, 2));
    }
    let copy = "immutable = true\nidentifier = 'name'";
    env.configure(&format!(
        "{}{}{}",
        env.root("copies/a/data", copy),
        env.root("copies/b/data", copy),
        env.root("copies/c/data", copy)
    ));
    env.update();
    for copy in copies {
        std::fs::remove_file(env.path(format!("copies/{copy}/data/gone"))).unwrap();
    }

    let closed = env.close(&[("copies/c/data", 0o755)]);
    let a = env.path("copies/a/data");
    let args = ["prune", "--scan-immutable"];
    let stderr = env.run_after_resolution(&args, || std::fs::remove_dir_all(&a).unwrap());
    drop(closed);
    let expected = [missing(&a), denied(env.path("copies/c/data/gone"), "open")];
    assert_eq!(warnings(&stderr), expected, "{stderr}");
    assert_eq!(pruned(&stderr), [0, 1, 1], "{stderr}");
    let rows = env.index().roots();
    let b = env.path("copies/b/data").as_os_str().as_bytes().to_vec();
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].identifier.as_deref(), &rows[0].path), (None, &b));
    assert_eq!(indexed(&env), [pair("copies/b/data", "ok")]);

    let stderr = prune(&env, &[]);
    assert_eq!(pruned(&stderr), [0, 0, 0], "{stderr}");
    let stderr = env.update();
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(indexed(&env), [pair("copies/b/data", "ok")]);
}

/// A root none of whose copies opens as a directory or a regular file goes
/// with its paths, with the warning of a scan task: here the root is a
/// symlink now, which root expansion still finds (DESIGN.md §4.2 and §4.5).
#[test]
fn a_root_that_does_not_open_goes() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.write("other/f", &random(BLOCK, 2));
    env.configure(&env.root("tree", ""));
    env.update();

    std::fs::rename(env.tree(), env.path("tree-away")).unwrap();
    std::os::unix::fs::symlink(env.path("other"), env.tree()).unwrap();
    let stderr = prune(&env, &[]);
    let expected = format!(
        "hashsync: warning: {}: root is not a directory or a regular file",
        quoted(env.tree())
    );
    assert_eq!(warnings(&stderr), [expected], "{stderr}");
    assert_eq!(pruned(&stderr), [1, 1, 1], "{stderr}");
    assert_eq!(env.count("roots"), 0);
    assert_eq!(env.count("blocks"), 0);
}

/// An index of four files, of which the last update removed one. An update
/// without `--compact` leaves the deletions of the file's entries in the
/// database files, until RocksDB compacts the files on its own (DESIGN.md
/// §4.4).
fn index_with_deletions() -> Env {
    let env = Env::new();
    for i in 0..4 {
        env.write(format!("tree/{i}"), &random(BLOCK * 2, i));
    }
    env.configure(&env.root("tree", ""));
    env.update();
    std::fs::remove_file(env.path("tree/0")).unwrap();
    let stderr = env.update();
    assert!(stderr.contains(" 1 stale inodes removed"), "{stderr}");
    assert!(!stderr.contains("compacted"), "{stderr}");
    assert!(env.index().file_deletions() > 0, "no removal in the files");
    env
}

/// Prune ends with a compaction of the whole database: the entries that it
/// and the updates before it removed leave the database files (DESIGN.md
/// §4.4).
#[test]
fn prune_compacts_the_index() {
    let env = index_with_deletions();
    std::fs::remove_file(env.path("tree/1")).unwrap();
    let stderr = prune(&env, &[]);
    assert_eq!(pruned(&stderr), [0, 1, 1], "{stderr}");
    assert!(stderr.contains("hashsync: compacted index in "), "{stderr}");
    let index = env.index();
    assert_eq!(index.file_deletions(), 0);
    assert_eq!(index.count("paths"), 2);
    index.assert_consistent();
}

/// The compaction also removes a deletion that RocksDB moved to the last
/// level of the database without rewriting it, beside the entry that it
/// deletes: one that deletes the first entry of an older file. Here the
/// only indexed file goes, so that each of its deletions is such a one
/// (DESIGN.md §4.4).
#[test]
fn prune_compacts_deletions_that_reach_the_last_level() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&env.root("tree", ""));
    env.update();
    std::fs::remove_file(env.path("tree/f")).unwrap();
    let stderr = prune(&env, &[]);
    assert_eq!(pruned(&stderr), [0, 1, 1], "{stderr}");
    assert_eq!(env.index().file_deletions(), 0);
}

/// `hashsync compact` compacts the index as a prune does at its end, and
/// changes no entry of it (DESIGN.md §4.4).
#[test]
fn compact_compacts_the_index() {
    let env = index_with_deletions();
    let entries = || {
        let index = env.index();
        let families = env.column_families();
        families
            .iter()
            .map(|cf| index.entries(cf))
            .collect::<Vec<_>>()
    };
    let before = entries();
    let stderr = succeed(&env, &["compact"]);
    assert!(stderr.contains("hashsync: compacted index in "), "{stderr}");
    assert_eq!(env.index().file_deletions(), 0);
    assert!(entries() == before, "the compaction changed the index");
}

/// `update --compact` ends with the same compaction, after the note of the
/// update, so that the entries that the update itself removed leave the
/// database files too (DESIGN.md §4.4).
#[test]
fn update_compact_compacts_the_index() {
    let env = index_with_deletions();
    std::fs::remove_file(env.path("tree/1")).unwrap();
    let stderr = succeed(&env, &["update", "--compact"]);
    let position = |note: &str| {
        let found = stderr.find(note);
        found.unwrap_or_else(|| panic!("no {note:?} in:\n{stderr}"))
    };
    let updated = position(" 1 stale inodes removed");
    assert!(
        updated < position("hashsync: compacted index in "),
        "{stderr}"
    );
    let index = env.index();
    assert_eq!(index.file_deletions(), 0);
    assert_eq!(index.count("paths"), 2);
    index.assert_consistent();
}

/// Prune and compact need an index that matches the configuration, which
/// only an update makes, and say which update to run. Without an index,
/// they create no directory, lock file, or database, so that a wrong index
/// path leaves no files behind. A database without metadata, which the
/// first update leaves when it ends before it writes them, is no index for
/// them or for `stats` either. An error in the roots of the configuration,
/// which prune resolves, changes nothing (DESIGN.md §4.2 and §4.4).
#[test]
fn prune_and_compact_need_a_current_index() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&env.root("tree", ""));
    let update = format!(
        "run `hashsync --config={} update`",
        env.config_path().display()
    );
    let expected = format!(
        "hashsync: no index at {}; {update}\n",
        quoted(env.index_path())
    );
    let need_an_index = || {
        for command in ["prune", "compact", "stats"] {
            let output = env.run(&[command]);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert_eq!(output.status.code(), Some(1), "{command}: {stderr}");
            assert!(stderr.ends_with(&expected), "{command}: {stderr}");
        }
    };
    need_an_index();
    assert!(!env.index_path().exists());

    env.update();
    let index = env.index_rw();
    let mut batch = WriteBatch::default();
    for (name, _) in index.entries("default") {
        batch.delete_cf(index.cf("default"), name);
    }
    index.write(batch);
    drop(index);
    need_an_index();

    env.update();
    env.configure_with(&format!("block_size = 8192\n{}", env.root("tree", "")));
    let expected = format!(
        "hashsync: index {} does not match the configuration; {update}\n",
        quoted(env.index_path())
    );
    for command in ["prune", "compact"] {
        let output = env.run(&[command]);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{command}: {stderr}");
        assert!(stderr.ends_with(&expected), "{command}: {stderr}");
    }

    env.configure(&format!(
        "{}{}",
        env.root("tree", ""),
        env.root("tree/f", "")
    ));
    let output = env.run(&["prune"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("is inside root"), "{stderr}");
    assert_eq!(env.count("paths"), 1);
}
