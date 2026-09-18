//! Bind-mount cycles, aliases, and `one_filesystem`, tested in an
//! unprivileged user and mount namespace. Skipped where `unshare -Urm` is
//! not available.

mod common;

use std::process::{Command, Output};

use common::*;

fn namespaces_available() -> bool {
    Command::new("unshare")
        .args(["-Urm", "true"])
        .status()
        .is_ok_and(|status| status.success())
}

/// Run `hashsync update` after bind-mounting `binds` (source, target pairs)
/// in a fresh mount namespace.
fn update_with_binds(env: &Env, binds: &[(&str, &str)]) -> Output {
    let mut script = String::new();
    for (source, target) in binds {
        script.push_str(&format!(
            "mount --bind '{}' '{}' || exit 99\n",
            env.path(source).display(),
            env.path(target).display()
        ));
    }
    script.push_str("exec \"$0\" --config \"$1\" update\n");
    Command::new("unshare")
        .args(["-Urm", "sh", "-c", &script])
        .arg(env!("CARGO_BIN_EXE_hashsync"))
        .arg(env.config_path())
        .output()
        .unwrap()
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
    assert!(
        stderr.contains("reached twice (cycle or mount alias)"),
        "{stderr}"
    );

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
    assert!(
        stderr.contains("are the same object (mount alias)"),
        "{stderr}"
    );
}

#[test]
fn entering_another_root_through_an_alias_is_fatal() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
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
    assert!(stderr.contains("entered root"), "{stderr}");
}

#[test]
fn hard_link_to_a_single_file_root_through_an_alias_is_allowed() {
    if !namespaces_available() {
        eprintln!("skipped: no unprivileged user namespaces");
        return;
    }
    // A bind mount of a directory makes its file entries aliases too. A
    // file root is identified by its directory entry, so the same inode
    // reached through another directory is a hard link, not a conflict.
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.mkdir("mirror");
    env.mkdir("files");
    std::fs::hard_link(env.path("tree/f"), env.path("files/root-file")).unwrap();
    env.configure(&format!(
        "{}{}",
        env.root("tree", ""),
        env.root("files/root-file", "")
    ));
    let output = update_with_binds(&env, &[("tree", "mirror")]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert_eq!(env.count("SELECT count(*) FROM inodes"), 1);
    assert_eq!(env.count("SELECT count(*) FROM paths"), 2);
}
