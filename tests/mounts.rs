//! Bind-mount cycles, aliases, and `one_filesystem`, tested in an
//! unprivileged user and mount namespace. Skipped where `unshare -Urm` is
//! not available.

mod common;

use std::os::unix::ffi::OsStrExt;
use std::process::{Command, Output};

use common::*;

const SAME_DIRECTORY: &str = "are the same directory (cycle or mount alias)";

/// Run `hashsync ARGS...` with the configuration file of `env` after
/// bind-mounting `binds` (source, target pairs) in a fresh mount namespace.
fn run_with_binds(env: &Env, binds: &[(&str, &str)], args: &[&str]) -> Output {
    let mut script = String::new();
    for (source, target) in binds {
        script.push_str(&format!(
            "mount --bind '{}' '{}' || exit 99\n",
            env.path(source).display(),
            env.path(target).display()
        ));
    }
    script.push_str("exec \"$0\" --config \"$@\"\n");
    Command::new("unshare")
        .args(["-Urm", "sh", "-c", &script])
        .arg(env!("CARGO_BIN_EXE_hashsync"))
        .arg(env.config_path())
        .args(args)
        .output()
        .unwrap()
}

/// Run `hashsync update` after bind-mounting `binds` in a fresh mount
/// namespace.
fn update_with_binds(env: &Env, binds: &[(&str, &str)]) -> Output {
    run_with_binds(env, binds, &["update"])
}

#[test]
fn bind_mount_cycle_is_fatal_unless_one_filesystem() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    let env = Env::new();
    env.write("tree/a/f", &random(BLOCK, 1));
    env.mkdir("tree/a/b/c");
    env.configure(&env.root("tree", ""));
    let output = update_with_binds(&env, &[("tree/a", "tree/a/b/c")]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains(SAME_DIRECTORY), "{stderr}");

    env.configure(&env.root("tree", "one_filesystem = true"));
    let output = update_with_binds(&env, &[("tree/a", "tree/a/b/c")]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert_eq!(env.tree_paths(), vec!["a/f"]);
}

#[test]
fn aliased_roots_are_fatal() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    let env = Env::new();
    env.write("tree/x/f", &random(BLOCK, 1));
    env.mkdir("tree/y");
    env.configure(&format!(
        "{}{}",
        env.root("tree/x", ""),
        env.root("tree/y", "")
    ));
    let output = update_with_binds(&env, &[("tree/x", "tree/y")]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains(SAME_DIRECTORY), "{stderr}");
}

#[test]
fn entering_another_root_through_an_alias_is_fatal() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    // Whichever traversal claims the directory first, the other one dies.
    let env = Env::new();
    env.write("tree/sub/f", &random(BLOCK, 1));
    env.mkdir("other/x");
    env.configure(&format!(
        "{}{}",
        env.root("tree", ""),
        env.root("other/x", "")
    ));
    let output = update_with_binds(&env, &[("tree/sub", "other/x")]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains(SAME_DIRECTORY), "{stderr}");
}

#[test]
fn a_file_root_aliased_inside_another_root_is_a_hard_link() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    // Only directories are claimed by identity. A single-file root is
    // handled like any other file, so reaching its inode through a
    // bind-mounted alias of the other root adds a path instead of
    // conflicting.
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.mkdir("mirror");
    env.configure(&format!(
        "{}{}",
        env.root("tree", ""),
        env.root("mirror/f", "")
    ));
    let output = update_with_binds(&env, &[("tree", "mirror")]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert_eq!(env.count("inodes"), 1);
    assert_eq!(env.count("paths"), 2);
}

#[test]
fn one_filesystem_skips_bind_mounted_files() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.write("tree/g", &random(BLOCK, 2));
    env.write("other/g", &random(BLOCK, 3));
    env.configure(&env.root("tree", "one_filesystem = true"));
    let output = update_with_binds(&env, &[("other/g", "tree/g")]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert_eq!(env.tree_paths(), vec!["f"]);

    env.configure(&env.root("tree", ""));
    let output = update_with_binds(&env, &[("other/g", "tree/g")]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert_eq!(env.tree_paths(), vec!["f", "g"]);
}

/// With `one_filesystem`, prune removes a path whose lookup crosses a mount
/// point, also at a bind-mounted file, since the scan of an update skips
/// such an entry: the next update finds nothing else to remove. Without
/// it, the file on the other mount holds the path (DESIGN.md §4.2).
#[test]
fn prune_with_one_filesystem_does_not_cross_mount_points() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    let env = Env::new();
    env.write("tree/keep", &random(BLOCK, 1));
    env.write("tree/f", &random(BLOCK, 2));
    env.write("tree/sub/g", &random(BLOCK, 3));
    env.write("other/f", &random(BLOCK, 4));
    env.write("other/sub/g", &random(BLOCK, 5));
    env.configure(&env.root("tree", ""));
    env.update();

    let binds = [("other/f", "tree/f"), ("other/sub", "tree/sub")];
    let output = run_with_binds(&env, &binds, &["prune"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert_eq!(env.tree_paths(), ["f", "keep", "sub/g"]);

    env.configure(&env.root("tree", "one_filesystem = true"));
    let output = run_with_binds(&env, &binds, &["prune"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(warnings(&stderr).is_empty(), "{stderr}");
    assert_eq!(env.tree_paths(), ["keep"]);
    let output = update_with_binds(&env, &binds);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(stderr.contains(" 0 stale inodes removed"), "{stderr}");
    assert_eq!(env.tree_paths(), ["keep"]);
}

#[test]
fn cycle_during_root_expansion_is_fatal() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    let env = Env::new();
    env.write("tree/a/leaf", &random(BLOCK, 1));
    env.mkdir("tree/a/b/c");
    env.configure(&env.root("tree/**/leaf", ""));
    let output = update_with_binds(&env, &[("tree/a", "tree/a/b/c")]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("root pattern"), "{stderr}");
    assert!(stderr.contains(SAME_DIRECTORY), "{stderr}");
}

/// A directory conflict ends the update before any group completes, also
/// one that a task finds after the scan of the group has ended: the
/// markers come only once every task has finished (DESIGN.md §4.5). The
/// group's own tree is small, and the alias is deep in the mutable root.
#[test]
fn a_directory_conflict_withholds_completion() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    let env = Env::new();
    env.write("tree/g/sub/f", &random(BLOCK, 1));
    env.mkdir("other/a/b/c/d/x");
    env.configure(&format!(
        "{}{}",
        env.root("tree/g", "immutable = true"),
        env.root("other", "")
    ));
    let output = update_with_binds(&env, &[("tree/g/sub", "other/a/b/c/d/x")]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains(SAME_DIRECTORY), "{stderr}");
    let roots = env.index().roots();
    let group = roots
        .iter()
        .find(|root| root.path.ends_with(b"/tree/g"))
        .expect("the group has a row");
    assert_eq!(group.identifier, None, "the group must not be complete");

    let output = update_with_binds(&env, &[]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        stderr.contains("immutable roots: 0 reused, 1 completed"),
        "{stderr}"
    );
}

/// An update does not open a completed group or an unused copy only to
/// look for aliases, so an alias of one of them inside a scanned root goes
/// undetected (DESIGN.md §4.5).
#[test]
fn aliases_of_completed_groups_and_unused_copies_go_undetected() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    let env = Env::new();
    env.write("tree/g/f", &random(BLOCK, 1));
    env.write("tree/h1/f", &random(BLOCK, 2));
    env.write("tree/m/sub/f", &random(BLOCK, 2));
    env.mkdir("tree/m/alias");
    env.mkdir("tree/h2");
    let copy = "immutable = true\nidentifier = { literal = 'h' }";
    env.configure(&format!(
        "{}{}{}{}",
        env.root("tree/g", "immutable = true"),
        env.root("tree/h1", copy),
        env.root("tree/h2", copy),
        env.root("tree/m", "")
    ));
    let output = update_with_binds(&env, &[]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");

    let output = update_with_binds(
        &env,
        &[("tree/g", "tree/m/alias"), ("tree/m/sub", "tree/h2")],
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(stderr.contains("immutable roots: 2 reused"), "{stderr}");
    let tree = env.tree().as_os_str().as_bytes().to_vec();
    let mutable = [&tree[..], b"/m"].concat();
    let mut paths: Vec<Vec<u8>> = env
        .paths()
        .into_iter()
        .filter(|(root, _)| *root == mutable)
        .map(|(_, rel)| rel)
        .collect();
    paths.sort();
    assert_eq!(paths, [b"alias/f".to_vec(), b"sub/f".to_vec()]);
}
