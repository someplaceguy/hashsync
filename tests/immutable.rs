//! Behavioral tests of immutable roots through the binary (DESIGN.md
//! §4.2): their identifiers, the copies of a group, the reuse of a
//! completed group without a scan, and the scans that complete a group or
//! leave it unfinished.

mod common;

use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use common::*;
use rocksdb::WriteBatch;

fn bytes(path: &Path) -> Vec<u8> {
    path.as_os_str().as_bytes().to_vec()
}

/// The identifier and path of each root row.
fn root_rows(env: &Env) -> Vec<(Option<Vec<u8>>, Vec<u8>)> {
    let rows = env.index().roots().into_iter();
    rows.map(|root| (root.identifier, root.path)).collect()
}

/// An immutable root is scanned once, completely, and then its group is
/// reused without a scan while the configuration names it: a file added
/// later is not indexed, since the user promised that the root does not
/// change (DESIGN.md §4.2). The default identifier is the path.
#[test]
fn a_completed_group_is_reused_without_a_scan() {
    let env = Env::new();
    env.write("tree/a", &random(BLOCK * 2, 1));
    env.write("tree/sub/b", &random(BLOCK, 2));
    env.configure(&env.root("tree", "immutable = true"));
    let stderr = env.update();
    assert!(
        stderr.contains(": 2 directories, 2 files hashed"),
        "{stderr}"
    );
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(
        root_rows(&env),
        [(Some(bytes(&env.tree())), bytes(&env.tree()))]
    );
    let stats = env.stats();
    assert!(
        stats.contains("roots:            1 (1 complete immutable)"),
        "{stats}"
    );

    env.write("tree/later", &random(BLOCK, 3));
    let stderr = env.update();
    assert!(
        stderr.contains(": 0 directories, 0 files hashed (0 B), 0 unchanged"),
        "{stderr}"
    );
    assert_eq!(groups(&stderr), [1, 0, 0]);
    assert_eq!(env.tree_paths(), ["a", "sub/b"]);
    env.index().assert_consistent();

    // Without immutable roots, there is no note about them.
    env.configure(&env.root("tree", ""));
    let stderr = env.update();
    assert!(!stderr.contains("immutable roots"), "{stderr}");
    assert_eq!(env.tree_paths(), ["a", "later", "sub/b"]);
}

/// Matches that share an identifier are copies of one group: an update
/// scans the first of them alone, and the group has one root row, with the
/// relative paths of that copy (DESIGN.md §4.2).
#[test]
fn the_copies_of_a_group_share_one_scan() {
    let env = Env::new();
    for copy in ["a", "b"] {
        env.write(format!("copies/{copy}/data/f"), &random(BLOCK, 1));
        env.write(format!("copies/{copy}/data/sub/g"), &random(BLOCK, 2));
    }
    let copy = "immutable = true\nidentifier = 'name'";
    env.configure(&format!(
        "{}{}",
        env.root("copies/a/data", copy),
        env.root("copies/b/data", copy)
    ));
    let stderr = env.update();
    assert!(
        stderr.contains(": 2 directories, 2 files hashed"),
        "{stderr}"
    );
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(
        root_rows(&env),
        [(Some(b"data".to_vec()), bytes(&env.path("copies/a/data")))]
    );
    assert_eq!(
        indexed(&env),
        [pair("copies/a/data", "f"), pair("copies/a/data", "sub/g")]
    );
}

/// A completed group stays complete when its tree moves, when a new
/// directory takes the place of its root directory, which gives the root
/// another inode handle, and when its representative copy goes away and
/// another copy takes its place (DESIGN.md §4.2).
#[test]
fn a_completed_group_survives_moves_and_new_copies() {
    let env = Env::new();
    let content = random(BLOCK, 1);
    env.write("tree/photos/f", &content);
    let literal = "immutable = true\nidentifier = { literal = 'photos' }";
    env.configure(&env.root("tree/photos", literal));
    let stderr = env.update();
    assert_eq!(groups(&stderr), [0, 1, 0]);
    let reused = |expected_path: &str| {
        let stderr = env.update();
        assert!(stderr.contains(": 0 directories"), "{stderr}");
        assert_eq!(groups(&stderr), [1, 0, 0]);
        let path = bytes(&env.path(expected_path));
        assert_eq!(root_rows(&env), [(Some(b"photos".to_vec()), path)]);
        assert_eq!(indexed(&env), [pair(expected_path, "f")]);
    };

    std::fs::rename(env.path("tree/photos"), env.path("tree/pictures")).unwrap();
    env.configure(&env.root("tree/pictures", literal));
    reused("tree/pictures");

    env.write("tree/fresh/f", &content);
    std::fs::remove_dir_all(env.path("tree/pictures")).unwrap();
    std::fs::rename(env.path("tree/fresh"), env.path("tree/pictures")).unwrap();
    reused("tree/pictures");

    // A new copy, before the old one in the configuration, does not take
    // the place of the old one while the old one is a copy.
    env.write("other/pictures/f", &content);
    env.configure(&format!(
        "{}{}",
        env.root("other/pictures", literal),
        env.root("tree/pictures", literal)
    ));
    reused("tree/pictures");
    env.configure(&env.root("other/pictures", literal));
    reused("other/pictures");
}

/// A group that the configuration no longer names goes with its paths,
/// also when the root becomes another group or a mutable root, which scan
/// the same files again and reuse their unchanged content (DESIGN.md
/// §4.2).
#[test]
fn groups_that_the_configuration_no_longer_names_go() {
    let env = Env::new();
    env.write("tree/x/f", &random(BLOCK, 1));
    env.write("tree/x/g", &random(BLOCK, 2));
    env.configure(&env.root(
        "tree/x",
        "immutable = true\nidentifier = { literal = 'one' }",
    ));
    env.update();

    env.configure(&env.root(
        "tree/x",
        "immutable = true\nidentifier = { literal = 'two' }",
    ));
    let stderr = env.update();
    assert!(
        stderr.contains(": 1 directories, 0 files hashed (0 B), 2 unchanged"),
        "{stderr}"
    );
    assert_eq!(groups(&stderr), [0, 1, 0]);
    let path = bytes(&env.path("tree/x"));
    assert_eq!(root_rows(&env), [(Some(b"two".to_vec()), path.clone())]);

    env.configure(&env.root("tree/x", ""));
    let stderr = env.update();
    assert!(
        stderr.contains(": 1 directories, 0 files hashed (0 B), 2 unchanged"),
        "{stderr}"
    );
    assert_eq!(root_rows(&env), [(None, path)]);

    env.configure(&env.root("tree/x", "immutable = true"));
    env.update();
    env.configure("");
    let stderr = env.update();
    assert!(stderr.contains("2 stale inodes removed"), "{stderr}");
    assert_eq!(env.count("roots"), 0);
    assert_eq!(env.count("inodes"), 0);
    env.index().assert_consistent();
}

/// An entry of an immutable root that cannot be indexed leaves its group
/// unfinished, with the paths that the scan found: the next update scans
/// the group again, until a scan has no failure (DESIGN.md §4.5).
#[test]
fn a_failed_scan_leaves_its_group_unfinished() {
    if skip_as_root() {
        return;
    }
    let env = Env::new();
    env.write("tree/ok", &random(BLOCK, 1));
    env.write("tree/secret", &random(BLOCK, 2));
    env.write("tree/closed/f", &random(BLOCK, 3));
    env.configure(&env.root("tree", "immutable = true"));
    let (secret, closed) = (("tree/secret", 0o644), ("tree/closed", 0o755));
    let stderr = env.run_with_closed(&["update"], &[secret, closed]);
    assert!(
        stderr.contains("secret': reopen: Permission denied"),
        "{stderr}"
    );
    assert!(
        stderr.contains("closed': reopen: Permission denied"),
        "{stderr}"
    );
    assert_eq!(groups(&stderr), [0, 0, 1]);
    assert_eq!(root_rows(&env), [(None, bytes(&env.tree()))]);
    assert_eq!(env.tree_paths(), ["ok"]);
    let stats = env.stats();
    assert!(stats.contains("(0 complete immutable)"), "{stats}");

    let stderr = env.run_with_closed(&["update"], &[secret, closed]);
    assert!(
        stderr.contains(": 1 directories, 0 files hashed (0 B), 1 unchanged"),
        "{stderr}"
    );
    assert_eq!(groups(&stderr), [0, 0, 1]);

    let stderr = env.run_with_closed(&["update"], &[closed]);
    assert_eq!(groups(&stderr), [0, 0, 1]);
    let stderr = env.update();
    assert!(stderr.contains(": 2 directories"), "{stderr}");
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(env.tree_paths(), ["closed/f", "ok", "secret"]);
    let stderr = env.update();
    assert_eq!(groups(&stderr), [1, 0, 0]);
}

/// A copy that is not there is no copy of its group, also for a pattern
/// without wildcards: it gets a warning unless it may vanish, and the
/// update scans the group at the next copy. A completed group keeps
/// the copies that are there, and goes with the last one, with its paths
/// and inodes, whatever its identifier: here a literal one, which no path
/// gives (DESIGN.md §4.2).
#[test]
fn a_missing_copy_is_no_copy() {
    let env = Env::new();
    env.mkdir("copies/a");
    env.write("copies/b/data/f", &random(BLOCK, 1));
    env.write("copies/c/data/f", &random(BLOCK, 1));
    let copy = "immutable = true\nidentifier = { literal = 'data' }";
    env.configure(&format!(
        "{}{}{}",
        env.root("copies/a/data", copy),
        env.root("copies/b/data", copy),
        env.root("copies/c/data", copy)
    ));
    let [a, b, c] = ["a", "b", "c"].map(|copy| env.path(format!("copies/{copy}/data")));
    let stderr = env.update();
    assert_eq!(warnings(&stderr), [missing(&a)], "{stderr}");
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(root_rows(&env), [(Some(b"data".to_vec()), bytes(&b))]);
    assert_eq!(indexed(&env), [pair("copies/b/data", "f")]);

    // The copy whose path the row has goes: the group stays, and its row
    // takes the copy that is left.
    std::fs::remove_dir_all(&b).unwrap();
    let stderr = env.update();
    assert_eq!(warnings(&stderr), [missing(&a), missing(&b)], "{stderr}");
    assert_eq!(groups(&stderr), [1, 0, 0]);
    assert_eq!(indexed(&env), [pair("copies/c/data", "f")]);

    // The last copy goes, and the group goes with it.
    std::fs::remove_dir_all(&c).unwrap();
    let stderr = env.update();
    let all = [missing(&a), missing(&b), missing(&c)];
    assert_eq!(warnings(&stderr), all, "{stderr}");
    assert!(!stderr.contains("immutable roots"), "{stderr}");
    assert!(stderr.contains(" 1 stale inodes removed"), "{stderr}");
    assert!(root_rows(&env).is_empty());
    assert_eq!(env.count("paths"), 0);
    assert_eq!(env.count("inodes"), 0);

    // A copy that comes back is scanned again.
    env.write("copies/a/data/f", &random(BLOCK, 1));
    let stderr = env.update();
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(indexed(&env), [pair("copies/a/data", "f")]);
}

/// A copy that cannot be looked up is no copy either: here its parent
/// directory cannot be searched, so no scan or candidate read could reach
/// it. Root expansion reports it, the update scans the group at the next
/// copy, and a completed group whose copies all fail goes (DESIGN.md
/// §4.2).
#[test]
fn a_copy_that_cannot_be_looked_up_is_no_copy() {
    if skip_as_root() {
        return;
    }
    let env = Env::new();
    for copy in ["a", "b"] {
        env.write(format!("copies/{copy}/data/f"), &random(BLOCK, 1));
    }
    let copy = "immutable = true\nidentifier = 'name'";
    env.configure(&format!(
        "{}{}",
        env.root("copies/a/data", copy),
        env.root("copies/b/data", copy)
    ));
    let [a, b] = ["a", "b"].map(|copy| env.path(format!("copies/{copy}/data")));
    let unreachable = |path: &Path| {
        [
            format!(
                "hashsync: warning: {}: stat: Permission denied (os error 13)",
                quoted(path)
            ),
            format!(
                "hashsync: warning: root pattern {} matched nothing",
                quoted(path)
            ),
        ]
    };
    let stderr = env.run_with_closed(&["update"], &[("copies/a", 0o755)]);
    assert_eq!(warnings(&stderr), unreachable(&a), "{stderr}");
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(root_rows(&env), [(Some(b"data".to_vec()), bytes(&b))]);
    assert_eq!(indexed(&env), [pair("copies/b/data", "f")]);

    let closed = [("copies/a", 0o755), ("copies/b", 0o755)];
    let stderr = env.run_with_closed(&["update"], &closed);
    let all = [unreachable(&a), unreachable(&b)].concat();
    assert_eq!(warnings(&stderr), all, "{stderr}");
    assert!(!stderr.contains("immutable roots"), "{stderr}");
    assert!(stderr.contains(" 1 stale inodes removed"), "{stderr}");
    assert!(root_rows(&env).is_empty());
    assert_eq!(env.count("paths"), 0);
    assert_eq!(env.count("inodes"), 0);

    // Once the copies can be reached again, the group is hashed again.
    let stderr = env.update();
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(indexed(&env), [pair("copies/a/data", "f")]);
}

/// A copy can vanish after root expansion found it. One that may vanish
/// is then no failure: the scan takes the next copy of the group, whose
/// path the row names once the group is complete, and nothing warns.
/// Copies can differ in `may_vanish` (DESIGN.md §4.2).
#[test]
fn a_copy_that_may_vanish_gives_way_to_the_next() {
    let env = Env::new();
    env.write("copies/a/data/f", &random(BLOCK, 1));
    env.write("copies/b/data/f", &random(BLOCK, 1));
    env.write("copies/c/data/g", &random(BLOCK, 2));
    let copy = "immutable = true\nidentifier = 'name'";
    env.configure(&format!(
        "{}{}{}",
        env.root("copies/a/data", &format!("{copy}\nmay_vanish = true")),
        env.root("copies/b/data", copy),
        env.root("copies/c/data", copy)
    ));
    let a = env.path("copies/a/data");
    let stderr = env.run_after_resolution(&["update"], || std::fs::remove_dir_all(&a).unwrap());
    assert!(warnings(&stderr).is_empty(), "{stderr}");
    assert_eq!(groups(&stderr), [0, 1, 0]);
    let scanned = bytes(&env.path("copies/b/data"));
    assert_eq!(root_rows(&env), [(Some(b"data".to_vec()), scanned.clone())]);
    assert_eq!(indexed(&env), [pair("copies/b/data", "f")]);

    // The copy that comes back changes nothing: the group is complete, and
    // its row keeps the copy that it has the paths of.
    env.write("copies/a/data/f", &random(BLOCK, 1));
    let stderr = env.update();
    assert_eq!(groups(&stderr), [1, 0, 0]);
    assert_eq!(root_rows(&env), [(Some(b"data".to_vec()), scanned)]);
}

/// The row of a group that stays unfinished names the copy that the scan
/// traversed too, by which a run with `--no-update` finds the copies of an
/// unfinished group, and so does a prune, which keeps the group. Here the
/// first copy may vanish and vanishes after root expansion, and the scan of
/// the next copy fails (DESIGN.md §4.2).
#[test]
fn an_unfinished_group_names_the_copy_that_its_scan_traversed() {
    if skip_as_root() {
        return;
    }
    let env = Env::new();
    for copy in ["a", "b"] {
        env.write(format!("copies/{copy}/data/f"), &random(BLOCK, 1));
        env.write(format!("copies/{copy}/data/closed/g"), &random(BLOCK, 2));
    }
    let copy = "immutable = true\nidentifier = 'name'";
    env.configure(&format!(
        "{}{}",
        env.root("copies/a/data", &format!("{copy}\nmay_vanish = true")),
        env.root("copies/b/data", copy)
    ));
    let closed = env.close(&[("copies/b/data/closed", 0o755)]);
    let a = env.path("copies/a/data");
    let stderr = env.run_after_resolution(&["update"], || std::fs::remove_dir_all(&a).unwrap());
    drop(closed);
    assert_eq!(groups(&stderr), [0, 0, 1]);
    let scanned = bytes(&env.path("copies/b/data"));
    assert_eq!(root_rows(&env), [(None, scanned)]);
    assert_eq!(indexed(&env), [pair("copies/b/data", "f")]);

    let output = env.run(&["prune"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        stderr.contains(": 0 roots, 0 paths, and 0 stale inodes removed"),
        "{stderr}"
    );
    assert_eq!(indexed(&env), [pair("copies/b/data", "f")]);
}

/// Only a copy that may vanish gives way: one that may not vanish and
/// vanishes after root expansion is a failure, after the copies that may
/// vanish too, and the scan goes no further. When every copy may vanish
/// and all of them vanish, the group stays unfinished without a warning
/// (DESIGN.md §4.2).
#[test]
fn only_copies_that_may_vanish_give_way() {
    let env = Env::new();
    let data = |copy: &str| env.path(format!("copies/{copy}/data"));
    let write = |copies: &[&str]| {
        for copy in copies {
            env.write(data(copy).join("f"), &random(BLOCK, 1));
        }
    };
    let remove = |copies: &[&str]| {
        for copy in copies {
            std::fs::remove_dir_all(data(copy)).unwrap();
        }
    };
    write(&["a", "b", "c"]);
    let copy = "immutable = true\nidentifier = 'name'";
    let vanishing = format!("{copy}\nmay_vanish = true");
    env.configure(&format!(
        "{}{}{}",
        env.root("copies/a/data", &vanishing),
        env.root("copies/b/data", copy),
        env.root("copies/c/data", copy)
    ));
    let stderr = env.run_after_resolution(&["update"], || remove(&["a", "b"]));
    assert_eq!(warnings(&stderr), [missing(data("b"))], "{stderr}");
    assert_eq!(groups(&stderr), [0, 0, 1]);
    assert_eq!(env.count("paths"), 0);

    write(&["a", "b"]);
    env.configure(&format!(
        "{}{}",
        env.root("copies/a/data", &vanishing),
        env.root("copies/b/data", &vanishing)
    ));
    let stderr = env.run_after_resolution(&["update"], || remove(&["a", "b"]));
    assert!(warnings(&stderr).is_empty(), "{stderr}");
    assert_eq!(groups(&stderr), [0, 0, 1]);
}

/// A root that may vanish need not be there, also a mutable root, and its
/// pattern need not match anything: none of this warns, where roots that
/// may not vanish do (DESIGN.md §4.2).
#[test]
fn roots_that_may_vanish_need_not_be_there() {
    let env = Env::new();
    let roots = |extra: &str| {
        format!(
            "{}{}{}",
            env.root("tree/missing", extra),
            env.root("absent/x", extra),
            env.root("tree/*.none", extra)
        )
    };
    env.configure(&roots("may_vanish = true"));
    let stderr = env.update();
    assert!(!stderr.contains("warning"), "{stderr}");

    env.configure(&roots(""));
    let stderr = env.update();
    let unmatched = |pattern: &str| {
        format!(
            "hashsync: warning: root pattern {} matched nothing",
            quoted(env.path(pattern))
        )
    };
    assert_eq!(
        warnings(&stderr),
        [
            missing(env.path("tree/missing")),
            unmatched("absent/x"),
            unmatched("tree/*.none")
        ],
        "{stderr}"
    );
}

/// Hard links join a group to a mutable root: the file is hashed once and
/// has the paths of both, also within the group, and the next update finds
/// it unchanged through the mutable root while it reuses the group
/// (DESIGN.md §4.1 and §4.2).
#[test]
fn hard_links_join_groups_and_mutable_roots() {
    let env = Env::new();
    env.write("tree/g/f", &random(BLOCK * 2, 1));
    env.mkdir("tree/m");
    std::fs::hard_link(env.path("tree/g/f"), env.path("tree/g/f2")).unwrap();
    std::fs::hard_link(env.path("tree/g/f"), env.path("tree/m/f")).unwrap();
    env.configure(&format!(
        "{}{}",
        env.root("tree/g", "immutable = true"),
        env.root("tree/m", "")
    ));
    let stderr = env.update();
    assert!(stderr.contains("1 files hashed"), "{stderr}");
    assert!(stderr.contains("2 hard links"), "{stderr}");
    let all = [
        pair("tree/g", "f"),
        pair("tree/g", "f2"),
        pair("tree/m", "f"),
    ];
    assert_eq!(indexed(&env), all);
    assert_eq!(env.count("inodes"), 1);

    let stderr = env.update();
    assert_eq!(groups(&stderr), [1, 0, 0]);
    assert!(
        stderr.contains(": 1 directories, 0 files hashed (0 B), 1 unchanged"),
        "{stderr}"
    );
    assert_eq!(indexed(&env), all);
    env.index().assert_consistent();
}

/// A file that a group shares with a mutable root and that cannot be read
/// leaves the group unfinished, whichever of its links the scan finds
/// first (DESIGN.md §4.6). The group's link is deep in its tree, so that
/// usually the mutable root's link owns the file, and the group's waits
/// for that owner or comes after it.
#[test]
fn a_shared_file_that_fails_leaves_its_group_unfinished() {
    if skip_as_root() {
        return;
    }
    let env = Env::new();
    env.write("tree/m/f", &random(BLOCK, 1));
    env.mkdir("tree/g/a/b/c");
    std::fs::hard_link(env.path("tree/m/f"), env.path("tree/g/a/b/c/f")).unwrap();
    env.write("tree/g/ok", &random(BLOCK, 2));
    env.configure(&format!(
        "{}{}",
        env.root("tree/m", ""),
        env.root("tree/g", "immutable = true")
    ));
    let stderr = env.run_with_closed(&["update"], &[("tree/m/f", 0o644)]);
    assert!(stderr.contains("reopen: Permission denied"), "{stderr}");
    assert_eq!(groups(&stderr), [0, 0, 1]);
    assert_eq!(indexed(&env), [pair("tree/g", "ok")]);

    let stderr = env.update();
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(
        indexed(&env),
        [
            pair("tree/g", "a/b/c/f"),
            pair("tree/g", "ok"),
            pair("tree/m", "f")
        ]
    );
}

/// An update that ends while it hashes a file again leaves the file's
/// inode incomplete. A completed group with a path of that inode loses its
/// marker before its paths go, and the next update scans it again
/// (DESIGN.md §4.2). Here the mutable root of a hard link found the file
/// changed.
#[test]
fn an_interrupted_rehash_of_a_shared_file_reopens_its_group() {
    let env = Env::new();
    env.write("tree/m/f", &random(BLOCK, 1));
    env.mkdir("tree/g");
    std::fs::hard_link(env.path("tree/m/f"), env.path("tree/g/f")).unwrap();
    env.write("tree/g/other", &random(BLOCK, 2));
    env.configure(&format!(
        "{}{}",
        env.root("tree/m", ""),
        env.root("tree/g", "immutable = true")
    ));
    env.update();

    let index = env.index_rw();
    let shared = index
        .paths()
        .into_iter()
        .find(|path| path.rel_path == b"f")
        .unwrap()
        .inode_id;
    let mut batch = WriteBatch::default();
    for inode in index.inodes() {
        if inode.id == shared {
            let incomplete = Inode {
                size: None,
                ..inode
            };
            let key = incomplete.id.to_be_bytes();
            batch.put_cf(index.cf("inodes"), key, incomplete.value());
        }
    }
    index.write(batch);
    drop(index);

    let stderr = env.update();
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert!(
        stderr.contains(": 2 directories, 1 files hashed"),
        "{stderr}"
    );
    assert!(stderr.contains("1 unchanged, 1 hard links"), "{stderr}");
    assert_eq!(
        indexed(&env),
        [
            pair("tree/g", "f"),
            pair("tree/g", "other"),
            pair("tree/m", "f")
        ]
    );
    env.index().assert_consistent();
}

/// `update --scan-immutable` scans the completed groups again, as an
/// update scans an unfinished one: a file added later is indexed, one that
/// is gone loses its path, unchanged files are reused without hashing, and
/// a changed policy applies. The group is then complete again (DESIGN.md
/// §4.2).
#[test]
fn scan_immutable_scans_completed_groups_again() {
    let env = Env::new();
    env.write("tree/a", &random(BLOCK, 1));
    env.write("tree/b", &random(BLOCK, 2));
    env.write("tree/c.tmp", &random(BLOCK, 3));
    env.configure(&env.root("tree", "immutable = true"));
    env.update();
    env.write("tree/later", &random(BLOCK, 4));
    std::fs::remove_file(env.path("tree/b")).unwrap();
    env.configure(&env.root("tree", "immutable = true\nexclude = ['*.tmp']"));
    let stderr = env.update();
    assert_eq!(groups(&stderr), [1, 0, 0]);
    assert_eq!(env.tree_paths(), ["a", "b", "c.tmp"]);

    let output = env.run(&["update", "--scan-immutable"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        stderr.contains(": 1 directories, 1 files hashed (4.00 KiB), 1 unchanged"),
        "{stderr}"
    );
    assert!(stderr.contains(" 2 stale inodes removed"), "{stderr}");
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(env.tree_paths(), ["a", "later"]);
    let stderr = env.update();
    assert_eq!(groups(&stderr), [1, 0, 0]);
}

/// A changed policy does not reach a completed group until `update
/// --rebuild` (DESIGN.md §4.2).
#[test]
fn policy_changes_wait_for_a_rebuild() {
    let env = Env::new();
    env.write("tree/keep", &random(BLOCK, 1));
    env.write("tree/skip.tmp", &random(BLOCK, 2));
    env.configure(&env.root("tree", "immutable = true"));
    env.update();
    env.configure(&env.root("tree", "immutable = true\nexclude = ['*.tmp']"));
    let stderr = env.update();
    assert_eq!(groups(&stderr), [1, 0, 0]);
    assert_eq!(env.tree_paths(), ["keep", "skip.tmp"]);

    let output = env.run(&["update", "--rebuild"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(env.tree_paths(), ["keep"]);
}

/// The copies of a group must have the same exclusions, compared as sets
/// of patterns after byte-escape decoding, and the same `one_filesystem`.
/// A conflict is a configuration error before the index changes, also for
/// a completed group (DESIGN.md §4.2). The message shows the identifier,
/// whatever its bytes (DESIGN.md §8).
#[test]
fn copies_with_different_policies_are_rejected() {
    let env = Env::new();
    for copy in ["a", "b"] {
        env.write(format!("copies/{copy}/data/f"), &random(BLOCK, 1));
    }
    let configure = |a: &str, b: &str| {
        let copy = |extra: &str| {
            format!("immutable = true\nidentifier = {{ literal = 'id\\xFF\\x27\\x00' }}\n{extra}")
        };
        env.configure(&format!(
            "{}{}",
            env.root("copies/a/data", &copy(a)),
            env.root("copies/b/data", &copy(b))
        ));
    };
    configure(
        "exclude = ['*.tmp', '*.o']",
        r"exclude = ['*.o', '*\x2Etmp', '*.o']",
    );
    let stderr = env.update();
    assert_eq!(groups(&stderr), [0, 1, 0]);
    let identifier = b"id\xff'\0".to_vec();
    assert_eq!(
        root_rows(&env),
        [(Some(identifier), bytes(&env.path("copies/a/data")))]
    );

    let before = (root_rows(&env), env.paths());
    for (a, b, difference) in [
        ("exclude = ['*.tmp']", "exclude = ['*.o']", "exclusions"),
        ("", "one_filesystem = true", "one_filesystem values"),
    ] {
        configure(a, b);
        let stderr = env.update_fails();
        let expected = format!(
            "hashsync: root patterns {} and {} have different {difference}, \
             but their matches {} and {} share the identifier 'id\\xFF\\x27\\x00'\n",
            quoted(env.path("copies/a/data")),
            quoted(env.path("copies/b/data")),
            quoted(env.path("copies/a/data")),
            quoted(env.path("copies/b/data")),
        );
        assert!(stderr.ends_with(&expected), "{stderr}");
        assert_eq!((root_rows(&env), env.paths()), before);
    }
}

/// The checks of root paths cover mutable roots and every copy of a group
/// alike, also a copy that the update would not scan (DESIGN.md §4.2).
#[test]
fn root_paths_are_checked_across_mutable_and_immutable_roots() {
    let env = Env::new();
    for copy in ["a", "b"] {
        env.write(format!("copies/{copy}/data/f"), &random(BLOCK, 1));
    }
    let copy = "immutable = true\nidentifier = 'name'";
    let copies = format!(
        "{}{}",
        env.root("copies/a/data", copy),
        env.root("copies/b/data", copy)
    );
    env.configure(&format!("{copies}{}", env.root("copies/b", "")));
    let stderr = env.update_fails();
    let expected = format!(
        "root {} is inside root {}",
        quoted(env.path("copies/b/data")),
        quoted(env.path("copies/b"))
    );
    assert!(stderr.contains(&expected), "{stderr}");
    env.configure(&format!("{copies}{}", env.root("copies/*/data", "")));
    let stderr = env.update_fails();
    assert!(
        stderr.contains("is matched by both root patterns"),
        "{stderr}"
    );
    assert!(!env.index_path().exists(), "the index must not exist yet");
}

/// Every form of identifier: `strip_prefix` makes a copy below a prefix
/// share the identifier that the default gives the original, a match that
/// is not below the prefix is a configuration error, and paths with bytes
/// that are not UTF-8 are identifiers as they are (DESIGN.md §4.2).
#[test]
fn identifiers_of_every_form() {
    let env = Env::new();
    let content = random(BLOCK, 1);
    env.write("store/pkg/f", &content);
    let mirror = format!("mnt{}", env.path("store").display());
    env.write(format!("{mirror}/pkg/f"), &content);
    let prefix = toml_literal(&bytes(&env.path("mnt")));
    env.configure(&format!(
        "{}{}",
        env.root("store/*", "immutable = true"),
        env.root(
            &format!("{mirror}/*"),
            &format!("immutable = true\nidentifier = {{ strip_prefix = '{prefix}' }}")
        )
    ));
    let stderr = env.update();
    assert_eq!(groups(&stderr), [0, 1, 0]);
    let pkg = bytes(&env.path("store/pkg"));
    assert_eq!(root_rows(&env), [(Some(pkg.clone()), pkg)]);

    env.configure(&env.root(
        "store/*",
        "immutable = true\nidentifier = { strip_prefix = '/nonexistent' }",
    ));
    let stderr = env.update_fails();
    let expected = format!(
        "hashsync: root {} of pattern {} is not below the strip_prefix '/nonexistent' \
         of its identifier\n",
        quoted(env.path("store/pkg")),
        quoted(env.path("store/*")),
    );
    assert!(stderr.ends_with(&expected), "{stderr}");

    let mut path = bytes(&env.tree());
    path.extend_from_slice(b"/\xff\xfe");
    std::fs::create_dir(os(&path)).unwrap();
    std::fs::write(os(&[&path[..], b"/f"].concat()), &content).unwrap();
    env.configure(&root_entry(&path, "immutable = true"));
    let stderr = env.update();
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(root_rows(&env), [(Some(path.clone()), path)]);
}

/// A regular file can be an immutable root, alone or with copies: its one
/// path is empty (DESIGN.md §4.2).
#[test]
fn a_single_file_can_be_an_immutable_root() {
    let env = Env::new();
    env.write("tree/one.iso", &random(BLOCK * 2, 1));
    env.write("tree/two.iso", &random(BLOCK * 2, 1));
    let literal = "immutable = true\nidentifier = { literal = 'iso' }";
    env.configure(&format!(
        "{}{}",
        env.root("tree/one.iso", literal),
        env.root("tree/two.iso", literal)
    ));
    let stderr = env.update();
    assert!(
        stderr.contains(": 0 directories, 1 files hashed"),
        "{stderr}"
    );
    assert_eq!(groups(&stderr), [0, 1, 0]);
    assert_eq!(indexed(&env), [pair("tree/one.iso", "")]);
    let stderr = env.update();
    assert!(
        stderr.contains(" 0 files hashed (0 B), 0 unchanged"),
        "{stderr}"
    );
    assert_eq!(groups(&stderr), [1, 0, 0]);
    assert_eq!(indexed(&env), [pair("tree/one.iso", "")]);
}

/// An update scans neither the copies of a group that it does not choose
/// nor the completed groups (DESIGN.md §4.5): here such a copy cannot be
/// read, so a scan of it would fail with a warning. Root expansion looks
/// the copy up in its parent directory, which can be searched, so it is
/// still a copy (DESIGN.md §4.2).
#[test]
fn unused_copies_and_completed_groups_are_never_scanned() {
    if skip_as_root() {
        return;
    }
    let env = Env::new();
    for copy in ["a", "b"] {
        env.write(format!("copies/{copy}/data/f"), &random(BLOCK, 1));
    }
    let copy = "immutable = true\nidentifier = 'name'";
    env.configure(&format!(
        "{}{}",
        env.root("copies/a/data", copy),
        env.root("copies/b/data", copy)
    ));
    let stderr = env.run_with_closed(&["update"], &[("copies/b/data", 0o755)]);
    assert!(!stderr.contains("warning"), "{stderr}");
    assert_eq!(groups(&stderr), [0, 1, 0]);

    let stderr = env.run_with_closed(&["update"], &[("copies/a/data", 0o755)]);
    assert!(!stderr.contains("warning"), "{stderr}");
    assert_eq!(groups(&stderr), [1, 0, 0]);
    let scanned = bytes(&env.path("copies/a/data"));
    assert_eq!(root_rows(&env), [(Some(b"data".to_vec()), scanned)]);
}
