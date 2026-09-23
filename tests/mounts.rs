//! Bind-mount cycles, aliases, and `one_filesystem`, tested in an
//! unprivileged user and mount namespace. Skipped where `unshare -Urm` is
//! not available.

mod common;

use std::process::{Command, Output};

use common::*;

const SAME_DIRECTORY: &str = "are the same directory (cycle or mount alias)";

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
    assert_eq!(env.count("SELECT count(*) FROM inodes"), 1);
    assert_eq!(env.count("SELECT count(*) FROM paths"), 2);
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
