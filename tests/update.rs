//! Behavioral tests of `hashsync update` and `hashsync stats` through the
//! binary.

mod common;

use std::ffi::OsStr;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::symlink;
use std::process::{Command, Output};

use common::*;
use rocksdb::WriteBatch;

#[test]
fn indexes_complete_useful_blocks() {
    let env = Env::new();
    env.write("tree/empty", &[]);
    env.write("tree/small", &random(BLOCK - 1, 1));
    env.write("tree/one_block", &random(BLOCK, 2));
    env.write("tree/two_and_half", &random(BLOCK * 2 + BLOCK / 2, 3));
    env.write("tree/zeros", &vec![0u8; BLOCK * 3]);
    env.write("tree/pattern16", &pattern16(BLOCK * 2));
    let mut mixed = random(BLOCK * 3, 4);
    mixed[BLOCK..2 * BLOCK].fill(7);
    env.write("tree/sub/dir/mixed", &mixed);
    env.configure(&env.root("tree", ""));

    let stderr = env.update();
    // Periodic-only files are hashed too; they add no block rows but
    // keep complete inode and path rows (DESIGN.md §4.1). Their 11
    // complete blocks were read.
    assert!(stderr.contains("5 files hashed (44.00 KiB)"), "{stderr}");
    assert!(stderr.contains("6 periodic"), "{stderr}");

    assert_eq!(env.count("inodes"), 5);
    assert_eq!(env.index().incomplete_inodes(), 0);
    assert_eq!(env.count("blocks"), 1 + 2 + 2);
    assert_eq!(env.count("roots"), 1);
    // Every inode is on one filesystem. An identity starts with the length
    // of its filesystem ID, then that ID.
    let mut filesystems: Vec<Vec<u8>> = env
        .index()
        .inodes()
        .into_iter()
        .map(|inode| inode.identity[..1 + usize::from(inode.identity[0])].to_vec())
        .collect();
    filesystems.sort();
    filesystems.dedup();
    assert_eq!(filesystems.len(), 1);
    assert_eq!(
        env.tree_paths(),
        vec![
            "one_block",
            "pattern16",
            "sub/dir/mixed",
            "two_and_half",
            "zeros"
        ]
    );
    assert_eq!(env.block_numbers("sub/dir/mixed"), vec![0, 2]);
    assert_eq!(env.block_numbers("two_and_half"), vec![0, 1]);
    assert!(env.block_numbers("zeros").is_empty());

    // Stored strong prefixes are the first 16 bytes of BLAKE3.
    let expected = blake3::hash(&random(BLOCK, 2)).as_bytes()[..16].to_vec();
    assert_eq!(env.strong_prefixes("one_block"), vec![expected]);
    // one_block, two_and_half, zeros, pattern16, and mixed.
    let sizes: u64 = env
        .index()
        .inodes()
        .iter()
        .filter_map(|inode| inode.size)
        .sum();
    assert_eq!(
        sizes as usize,
        BLOCK + (BLOCK * 2 + BLOCK / 2) + BLOCK * 3 + BLOCK * 2 + BLOCK * 3
    );

    // The column families are those of DESIGN.md §4.4: `strong` holds the
    // only other key of each block row, and no column family orders the
    // blocks by weak hash.
    let mut expected: Vec<String> = COLUMN_FAMILIES.map(String::from).to_vec();
    expected.sort();
    assert_eq!(env.column_families(), expected);
    env.index().assert_consistent();
}

#[test]
fn sha256_strong_hash() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 9));
    env.configure(&format!(
        "strong_hash = \"sha256\"\n{}",
        env.root("tree", "")
    ));
    env.update();
    let expected = <sha2::Sha256 as sha2::Digest>::digest(random(BLOCK, 9))[..16].to_vec();
    assert_eq!(env.strong_prefixes("f"), vec![expected]);
    assert!(env.stats().contains("strong hash:      sha256"));
}

#[test]
fn exclusions_by_name_and_path() {
    let env = Env::new();
    env.write("tree/keep", &random(BLOCK, 1));
    env.write("tree/skip.tmp", &random(BLOCK, 2));
    env.write("tree/.cache/f", &random(BLOCK, 3));
    env.write("tree/deep/.cache/g", &random(BLOCK, 4));
    env.write("tree/absolute/f", &random(BLOCK, 5));
    env.write("tree/other/absolute/f", &random(BLOCK, 6));
    let absolute = format!("{}/absolute", env.tree().display());
    env.configure(&env.root(
        "tree",
        &format!("exclude = ['*.tmp', '.cache', '{absolute}']"),
    ));
    env.update();
    assert_eq!(env.tree_paths(), vec!["keep", "other/absolute/f"]);
}

#[test]
fn hard_links_within_and_across_roots() {
    let env = Env::new();
    env.write("tree/a/one", &random(BLOCK * 2, 1));
    env.mkdir("tree/b");
    std::fs::hard_link(env.path("tree/a/one"), env.path("tree/a/two")).unwrap();
    std::fs::hard_link(env.path("tree/a/one"), env.path("tree/b/three")).unwrap();
    // A link to a single-file root outside the directory root.
    env.mkdir("elsewhere");
    std::fs::hard_link(env.path("tree/a/one"), env.path("elsewhere/root-file")).unwrap();
    // A small file with two links has no rows.
    env.write("tree/a/small", &random(10, 2));
    std::fs::hard_link(env.path("tree/a/small"), env.path("tree/b/small")).unwrap();
    env.configure(&format!(
        "{}{}",
        env.root("tree", ""),
        env.root("elsewhere/root-file", "")
    ));
    let stderr = env.update();
    assert!(stderr.contains("1 files hashed"), "{stderr}");
    assert!(stderr.contains("3 hard links"), "{stderr}");

    assert_eq!(env.count("inodes"), 1);
    assert_eq!(env.count("blocks"), 2);
    assert_eq!(env.count("roots"), 2);
    let paths = env.paths();
    assert_eq!(paths.len(), 4);
    assert_eq!(env.tree_paths(), vec!["a/one", "a/two", "b/three"]);
    let root_file = paths
        .iter()
        .find(|(root, _)| root.ends_with(b"root-file"))
        .unwrap();
    assert!(
        root_file.1.is_empty(),
        "a single-file root has an empty rel_path"
    );
}

#[test]
fn hard_links_across_repeated_updates() {
    let env = Env::new();
    env.write("tree/one", &random(BLOCK, 1));
    std::fs::hard_link(env.path("tree/one"), env.path("tree/two")).unwrap();
    env.configure(&env.root("tree", ""));
    env.update();

    // Nothing changed: the owner reuses its row and the other link
    // re-adds its path against that reused row.
    let stderr = env.update();
    assert!(stderr.contains(" 0 files hashed"), "{stderr}");
    assert!(stderr.contains("1 unchanged"), "{stderr}");
    assert!(stderr.contains("1 hard links"), "{stderr}");
    assert_eq!(env.tree_paths(), vec!["one", "two"]);
    assert_eq!(env.count("inodes"), 1);
    assert_eq!(env.count("blocks"), 1);

    // An added link changes the inode's ctime, so the content is hashed
    // again into the same inode row; all three links get paths.
    std::fs::hard_link(env.path("tree/one"), env.path("tree/three")).unwrap();
    let stderr = env.update();
    assert!(stderr.contains("1 files hashed"), "{stderr}");
    assert!(stderr.contains("2 hard links"), "{stderr}");
    assert_eq!(env.tree_paths(), vec!["one", "three", "two"]);
    assert_eq!(env.count("inodes"), 1);
    assert_eq!(env.count("blocks"), 1);
    env.index().assert_consistent();

    // A removed link also changes the ctime; the remaining links keep
    // the inode alive.
    std::fs::remove_file(env.path("tree/two")).unwrap();
    let stderr = env.update();
    assert!(stderr.contains("1 files hashed"), "{stderr}");
    assert!(stderr.contains("1 hard links"), "{stderr}");
    assert!(stderr.contains(" 0 stale inodes removed"), "{stderr}");
    assert_eq!(env.tree_paths(), vec!["one", "three"]);
    assert_eq!(env.count("inodes"), 1);
    assert_eq!(env.count("blocks"), 1);

    // Removing the last links orphans the inode.
    std::fs::remove_file(env.path("tree/one")).unwrap();
    std::fs::remove_file(env.path("tree/three")).unwrap();
    let stderr = env.update();
    assert!(stderr.contains(" 0 files hashed"), "{stderr}");
    assert!(stderr.contains("1 stale inodes removed"), "{stderr}");
    assert_eq!(env.count("paths"), 0);
    assert_eq!(env.count("inodes"), 0);
    assert_eq!(env.count("blocks"), 0);
    env.index().assert_consistent();
}

/// Many files with links in two roots, on many threads: a link found
/// before its owner's row commits waits for it, a later link adds its path
/// itself, and every link gets its path (DESIGN.md §4.6). The same holds
/// while owners hash changed files again and remove shrunken ones.
#[test]
fn many_hard_links_on_many_threads() {
    const FILES: usize = 300;
    const LINKS: usize = 4;
    let env = Env::new();
    let original = |file: usize| format!("tree/d{}/f{file}", file % 7);
    // Every tenth file is too small to index.
    let len = |file: usize| {
        if file.is_multiple_of(10) {
            100
        } else {
            BLOCK * (1 + file % 3)
        }
    };
    for file in 0..FILES {
        env.write(original(file), &random(len(file), file as u64 + 1));
        for link in 1..LINKS {
            let dir = format!(
                "{}/d{}",
                if link % 2 == 0 { "tree" } else { "other" },
                (file + link) % 5
            );
            env.mkdir(&dir);
            std::fs::hard_link(
                env.path(original(file)),
                env.path(format!("{dir}/f{file}-{link}")),
            )
            .unwrap();
        }
    }
    env.configure(&format!(
        "threads = 16\n{}{}",
        env.root("tree", ""),
        env.root("other", "")
    ));
    let check = |stderr: &str, hashed: usize, unchanged: usize, indexed: usize, small: usize| {
        for expected in [
            format!(" {hashed} files hashed"),
            format!(" {unchanged} unchanged"),
            format!(" {} hard links", indexed * (LINKS - 1)),
            format!(" {} small files", small * LINKS),
            format!(" {} paths", indexed * LINKS),
        ] {
            assert!(stderr.contains(&expected), "{expected}: {stderr}");
        }
        assert_eq!(env.count("inodes"), indexed);
        assert_eq!(env.paths().len(), indexed * LINKS);
        env.index().assert_consistent();
    };
    let stderr = env.update();
    check(&stderr, 270, 0, 270, 30);
    // Nothing changed: owners reuse their rows while links are found.
    let stderr = env.update();
    check(&stderr, 0, 270, 270, 30);

    // Rewrite some files in place, which hashes their inodes again, and
    // shrink others below a block, which removes their rows.
    let (mut rewritten, mut shrunk) = (0, 0);
    for file in (0..FILES).filter(|file| !file.is_multiple_of(10)) {
        if file.is_multiple_of(5) {
            env.write(original(file), &random(100, 1));
            shrunk += 1;
        } else if file.is_multiple_of(3) {
            env.write(original(file), &random(len(file), file as u64 + 1000));
            rewritten += 1;
        }
    }
    assert_eq!((rewritten, shrunk), (80, 30));
    let stderr = env.update();
    assert!(stderr.contains(" 0 stale inodes removed"), "{stderr}");
    check(&stderr, 80, 160, 240, 60);
}

#[test]
fn symlinks_and_special_files_are_skipped() {
    let env = Env::new();
    env.write("tree/real", &random(BLOCK, 1));
    env.write("outside/target", &random(BLOCK, 2));
    symlink(env.path("outside/target"), env.path("tree/link")).unwrap();
    symlink(env.path("outside"), env.path("tree/dirlink")).unwrap();
    symlink("nowhere", env.path("tree/dangling")).unwrap();
    let status = Command::new("mkfifo")
        .arg(env.path("tree/fifo"))
        .status()
        .unwrap();
    assert!(status.success());
    env.configure(&env.root("tree", ""));
    env.update();
    assert_eq!(env.tree_paths(), vec!["real"]);
}

#[test]
fn sparse_file_holes_are_skipped() {
    use std::io::{Seek, SeekFrom, Write};
    let env = Env::new();
    env.mkdir("tree");
    let mut file = std::fs::File::create(env.path("tree/sparse")).unwrap();
    file.set_len((BLOCK * 44) as u64).unwrap();
    file.write_all(&random(BLOCK, 1)).unwrap();
    // Blocks 1 to 39 are a hole larger than the 128 KiB hole window, so
    // they are skipped without reading; the small hole behind block 40 is
    // read through.
    file.seek(SeekFrom::Start((BLOCK * 40) as u64)).unwrap();
    file.write_all(&random(BLOCK, 2)).unwrap();
    // A partial overlap with the hole is still read in full.
    file.seek(SeekFrom::Start((BLOCK * 43 + 100) as u64))
        .unwrap();
    file.write_all(&random(100, 3)).unwrap();
    drop(file);
    env.configure(&env.root("tree", ""));
    env.update();
    assert_eq!(env.block_numbers("sparse"), vec![0, 40, 43]);
}

#[test]
fn non_utf8_names_and_literal_metacharacters() {
    let env = Env::new();
    let dir = env.path(os(b"tree/arch-\xff"));
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(dir.join(os(b"f\xfe")), random(BLOCK, 1)).unwrap();
    std::fs::write(dir.join("literal*name"), random(BLOCK, 2)).unwrap();
    std::fs::write(dir.join("literalXname"), random(BLOCK, 3)).unwrap();
    std::fs::write(dir.join("back\\slash"), random(BLOCK, 4)).unwrap();
    std::fs::write(dir.join("[x]"), random(BLOCK, 5)).unwrap();
    std::fs::write(dir.join("x"), random(BLOCK, 6)).unwrap();
    let mut root = env.tree().into_os_string().into_vec();
    root.extend_from_slice(b"/arch-\\xFF");
    env.configure_with(&format!(
        "block_size = {BLOCK}\n[[roots]]\npath = '{}'\nexclude = ['literal\\x2Aname', 'back\\\\slash', '\\[x\\]']\n",
        String::from_utf8_lossy(&root)
    ));
    env.update();
    let mut rels: Vec<Vec<u8>> = env.paths().into_iter().map(|(_, rel)| rel).collect();
    rels.sort();
    assert_eq!(
        rels,
        vec![b"f\xfe".to_vec(), b"literalXname".to_vec(), b"x".to_vec()]
    );
    let roots: Vec<Vec<u8>> = env.paths().into_iter().map(|(root, _)| root).collect();
    assert!(roots[0].ends_with(b"/arch-\xff"));
}

#[test]
fn root_pattern_expansion() {
    let env = Env::new();
    env.write("tree/a/f", &random(BLOCK, 1));
    env.write("tree/b/f", &random(BLOCK, 2));
    env.write("tree/.hidden/f", &random(BLOCK, 3));
    env.write("tree/file", &random(BLOCK, 4));
    env.write("tree/small", &random(10, 5));
    symlink(env.path("tree/a"), env.path("tree/link")).unwrap();
    env.configure(&env.root("tree/*", "exclude = ['.hidden']"));
    env.update();
    // a, b, file, small are roots; .hidden is excluded; link is a symlink.
    assert_eq!(env.count("roots"), 4);
    assert_eq!(env.count("paths"), 3);

    // `**` spans directories; a pattern matching nothing only warns.
    env.write("tree/x/y/leaf", &random(BLOCK, 6));
    env.write("tree/leaf", &random(BLOCK, 7));
    env.configure(&format!(
        "{}{}",
        env.root("tree/**/leaf", ""),
        env.root("tree/missing/*", "")
    ));
    let stderr = env.update();
    assert!(stderr.contains("matched nothing"), "{stderr}");
    assert_eq!(env.count("roots"), 2);
    assert_eq!(env.count("paths"), 2);
}

#[test]
fn missing_and_non_indexable_roots_only_warn() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    symlink(env.path("tree"), env.path("link")).unwrap();
    env.configure(&format!(
        "{}{}{}",
        env.root("tree", ""),
        env.root("missing", ""),
        env.root("link", "")
    ));
    let stderr = env.update();
    assert!(stderr.contains("missing: root does not exist"), "{stderr}");
    assert!(
        stderr.contains("link: root is not a directory or a regular file"),
        "{stderr}"
    );
    assert_eq!(env.tree_paths(), vec!["f"]);
    assert_eq!(env.count("paths"), 1);
}

#[test]
fn duplicate_and_nested_roots_are_rejected() {
    let env = Env::new();
    env.write("tree/a/f", &random(BLOCK, 1));
    env.configure(&env.root("tree", ""));
    env.update();
    let index_id = env.index_id();

    let nested = format!("{}{}", env.root("tree", ""), env.root("tree/a", ""));
    env.configure(&nested);
    let stderr = env.update_fails();
    assert!(stderr.contains("is inside root"), "{stderr}");

    env.configure(&format!(
        "{}{}",
        env.root("tree/*", ""),
        env.root("tree/a", "")
    ));
    let stderr = env.update_fails();
    assert!(stderr.contains("matched by both"), "{stderr}");

    // A rejected configuration is rejected before the index changes, even
    // when a rebuild was requested or the settings changed.
    env.configure(&nested);
    let output = env.run(&["update", "--rebuild"]);
    assert_eq!(output.status.code(), Some(1));
    env.configure_with(&format!("block_size = {}\n{nested}", BLOCK * 2));
    let stderr = env.update_fails();
    assert!(!stderr.contains("rebuilding"), "{stderr}");
    assert_eq!(env.index_id(), index_id, "the index ID is unchanged");
    assert_eq!(env.tree_paths(), vec!["a/f"]);
    assert_eq!(env.count("blocks"), 1);
    assert_eq!(env.index().meta_u64("block_size"), BLOCK as u64);
}

#[test]
fn any_depth_patterns_enter_each_directory_once() {
    let env = Env::new();
    // `tree/**/a/**/leaf` reaches `tree/a` and `tree/a/a` through two
    // alternatives of the first `**`, and `tree/a/a/leaf` through both.
    env.write("tree/a/leaf", &random(BLOCK, 1));
    env.write("tree/a/a/leaf", &random(BLOCK, 2));
    env.write("tree/a/b/leaf", &random(BLOCK, 3));
    env.write("tree/x/leaf", &random(BLOCK, 4));
    env.configure(&env.root("tree/**/a/**/leaf", ""));
    env.update();
    assert_eq!(env.count("roots"), 3);
    assert_eq!(env.count("paths"), 3);

    // Consecutive `**` components overlap completely.
    env.configure(&env.root("tree/**/**/leaf", ""));
    env.update();
    assert_eq!(env.count("roots"), 4);
    assert_eq!(env.count("paths"), 4);
}

#[test]
fn unlistable_directories_are_abandoned_under_any_depth() {
    if rustix::process::geteuid().is_root() {
        eprintln!("skipped: root ignores permissions");
        return;
    }
    let env = Env::new();
    let tree = env.tree().display().to_string();
    let root_paths = |env: &Env| -> Vec<String> {
        let mut paths: Vec<String> = env
            .paths()
            .into_iter()
            .map(|(root, _)| String::from_utf8(root).unwrap())
            .collect();
        paths.sort();
        paths
    };
    env.write("tree/leaf", &random(BLOCK, 1));
    env.write("tree/x/leaf", &random(BLOCK, 2));

    // `tree` can be searched but not listed, and `**` needs the listing:
    // nothing under `tree` matches, not even `tree/leaf`, which a lookup
    // could find. `tree` is the first directory seen on its filesystem,
    // so identifying it, which reopens it for reading, fails before the
    // listing is attempted.
    env.chmod("tree", 0o111);
    env.configure(&env.root("tree/**/leaf", ""));
    let stderr = env.update();
    env.chmod("tree", 0o755);
    assert!(
        stderr.contains("tree: identity: Permission denied"),
        "{stderr}"
    );
    assert!(stderr.contains("matched nothing"), "{stderr}");
    assert_eq!(env.count("roots"), 0);

    // A literal after `*` is looked up, which needs no listing.
    env.chmod("tree/x", 0o111);
    env.configure(&env.root("tree/*/leaf", ""));
    let stderr = env.update();
    assert!(!stderr.contains("Permission denied"), "{stderr}");
    assert_eq!(root_paths(&env), vec![format!("{tree}/x/leaf")]);

    // Under `**`, the same directory is identified through the filesystem
    // that `tree` resolved, then abandoned when its listing fails.
    env.configure(&env.root("tree/**/leaf", ""));
    let stderr = env.update();
    env.chmod("tree/x", 0o755);
    assert!(
        stderr.contains("tree/x: reopen: Permission denied"),
        "{stderr}"
    );
    assert_eq!(root_paths(&env), vec![format!("{tree}/leaf")]);
}

#[test]
fn trailing_any_depth_matches_inside_the_directory_only() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.write("tree/g", &random(BLOCK, 2));
    env.configure(&env.root("tree/**", ""));
    env.update();
    // `tree` itself is not a match; every entry inside it is one.
    assert_eq!(env.count("roots"), 2);
    assert_eq!(env.count("paths"), 2);

    // Nothing is inside a regular file.
    env.configure(&env.root("tree/f/**", ""));
    let stderr = env.update();
    assert!(stderr.contains("matched nothing"), "{stderr}");
    assert_eq!(env.count("roots"), 0);
}

#[test]
fn the_slash_root_can_be_excluded() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&format!(
        "{}{}",
        root_entry(b"/", "exclude = ['/']"),
        env.root("tree", "")
    ));
    let stderr = env.update();
    assert!(
        stderr.contains("root pattern / matched nothing"),
        "{stderr}"
    );
    assert_eq!(env.count("roots"), 1);
    assert_eq!(env.tree_paths(), vec!["f"]);
}

#[test]
fn exclusions_prune_root_expansion() {
    let env = Env::new();
    env.write("tree/leaf", &random(BLOCK, 1));
    env.write("tree/a/leaf", &random(BLOCK, 2));
    env.write("tree/a/b/leaf", &random(BLOCK, 3));
    env.write("tree/x/leaf", &random(BLOCK, 4));
    env.write("tree/x/a/leaf", &random(BLOCK, 5));
    env.write("tree/x/skip/leaf", &random(BLOCK, 6));
    let tree = env.tree().display().to_string();
    let root_paths = |env: &Env| -> Vec<String> {
        let mut paths: Vec<String> = env
            .paths()
            .into_iter()
            .map(|(root, _)| String::from_utf8(root).unwrap())
            .collect();
        paths.sort();
        paths
    };

    // An excluded directory is not entered, by name or by absolute path.
    env.configure(&env.root("tree/**/leaf", &format!("exclude = ['a', '{tree}/x/skip']")));
    env.update();
    assert_eq!(
        root_paths(&env),
        vec![format!("{tree}/leaf"), format!("{tree}/x/leaf")]
    );

    // A literal component of a wildcard pattern is subject to exclusion.
    env.configure(&env.root("tree/*/a/leaf", "exclude = ['a']"));
    let stderr = env.update();
    assert!(stderr.contains("matched nothing"), "{stderr}");
    assert_eq!(env.count("roots"), 0);

    // An exact pattern names its root as written; only the root itself
    // can be excluded.
    env.configure(&format!(
        "{}{}",
        env.root("tree/a/b/leaf", "exclude = ['a']"),
        env.root("tree/x/leaf", "exclude = ['leaf']")
    ));
    let stderr = env.update();
    assert!(stderr.contains("matched nothing"), "{stderr}");
    assert_eq!(root_paths(&env), vec![format!("{tree}/a/b/leaf")]);
}

/// Block rows enough that replacing them commits during the removal, and
/// again during the insertion: each block row is two entries (`blocks` and
/// `strong`), and a transaction holds 50,000 changes (COMMIT_WORK).
const MANY_BLOCKS: usize = 51_200;

#[test]
fn large_files_are_rehashed_in_bounded_batches() {
    let env = Env::new();
    env.write("tree/big", &tiled(MANY_BLOCKS, 1));
    env.configure(&env.root("tree", ""));
    env.update();
    assert_eq!(env.count("blocks"), MANY_BLOCKS);

    // Changed content: all old rows go, across commits, before the new
    // rows arrive.
    env.write("tree/big", &tiled(MANY_BLOCKS, 2));
    env.update();
    assert_eq!(env.count("blocks"), MANY_BLOCKS);
    assert_eq!(env.index().incomplete_inodes(), 0);
    env.index().assert_consistent();

    // A shrunken file loses its rows, again across several transactions.
    env.write("tree/big", &random(10, 1));
    env.update();
    assert_eq!(env.count("blocks"), 0);
    assert_eq!(env.count("inodes"), 0);
    env.index().assert_consistent();
}

#[test]
fn interrupted_replacements_are_recovered() {
    let env = Env::new();
    env.write("tree/big", &tiled(MANY_BLOCKS, 1));
    env.configure(&env.root("tree", ""));
    env.update();

    // A replacement interrupted between commits leaves a committed
    // incomplete inode with only some rows removed. The next update
    // removes the leftover rows and hashes the file again (DESIGN.md §4.2).
    let index = env.index_rw();
    let mut batch = WriteBatch::default();
    for inode in index.inodes() {
        let incomplete = Inode {
            size: None,
            ..inode
        };
        batch.put_cf(
            index.cf("inodes"),
            incomplete.id.to_be_bytes(),
            incomplete.value(),
        );
    }
    for block in index.blocks() {
        if block.block_no >= (MANY_BLOCKS / 2) as u64 {
            batch.delete_cf(index.cf("blocks"), block.key());
            batch.delete_cf(index.cf("strong"), block.strong_key());
        }
    }
    index.write(batch);
    drop(index);
    assert_eq!(env.count("blocks"), MANY_BLOCKS / 2);
    assert_eq!(env.count("strong"), MANY_BLOCKS / 2);
    assert_eq!(env.index().incomplete_inodes(), 1);
    let stderr = env.update();
    assert!(stderr.contains("1 files hashed"), "{stderr}");
    assert_eq!(env.count("blocks"), MANY_BLOCKS);
    assert_eq!(env.index().incomplete_inodes(), 0);
    assert_eq!(env.count("paths"), 1);
    env.index().assert_consistent();
}

#[test]
fn unchanged_files_are_reused_without_rehashing() {
    let env = Env::new();
    env.write("tree/a", &random(BLOCK, 1));
    env.write("tree/b/f", &random(BLOCK * 2, 2));
    env.configure(&env.root("tree", ""));
    let stderr = env.update();
    assert!(stderr.contains("2 files hashed"), "{stderr}");

    // Nothing changed: paths are reconstructed without hashing.
    let stderr = env.update();
    assert!(stderr.contains(" 0 files hashed"), "{stderr}");
    assert!(stderr.contains("2 unchanged"), "{stderr}");
    assert_eq!(env.tree_paths(), vec!["a", "b/f"]);
    assert_eq!(env.count("inodes"), 2);
    assert_eq!(env.count("blocks"), 3);

    // Renaming a directory does not change the ctime of the files inside
    // it: their content is reused while their paths follow the move.
    std::fs::rename(env.path("tree/b"), env.path("tree/c")).unwrap();
    let stderr = env.update();
    assert!(stderr.contains(" 0 files hashed"), "{stderr}");
    assert!(stderr.contains("2 unchanged"), "{stderr}");
    assert_eq!(env.tree_paths(), vec!["a", "c/f"]);
    assert_eq!(env.count("inodes"), 2);

    // New content of the same size, with the mtime put back: the kernel
    // ctime still changes, so the file is hashed again (DESIGN.md §4.2).
    let mtime = std::fs::metadata(env.path("tree/a"))
        .unwrap()
        .modified()
        .unwrap();
    env.write("tree/a", &random(BLOCK, 9));
    let file = std::fs::File::options()
        .write(true)
        .open(env.path("tree/a"))
        .unwrap();
    file.set_times(std::fs::FileTimes::new().set_modified(mtime))
        .unwrap();
    drop(file);
    let stderr = env.update();
    assert!(stderr.contains("1 files hashed"), "{stderr}");
    assert!(stderr.contains("1 unchanged"), "{stderr}");
    let expected = blake3::hash(&random(BLOCK, 9)).as_bytes()[..16].to_vec();
    assert_eq!(env.strong_prefixes("a"), vec![expected]);
}

#[test]
fn files_without_useful_blocks_are_retained_and_reused() {
    let env = Env::new();
    // Every complete block is skipped: as repetitive content, with or
    // without a partial tail, or as a hole (DESIGN.md §4.1).
    env.write("tree/zeros", &vec![0u8; BLOCK * 2]);
    env.write("tree/tail", &pattern16(BLOCK + 100));
    let sparse = std::fs::File::create(env.path("tree/sparse")).unwrap();
    sparse.set_len((BLOCK * 2) as u64).unwrap();
    drop(sparse);
    env.configure(&env.root("tree", ""));
    let stderr = env.update();
    assert!(stderr.contains("3 files hashed"), "{stderr}");

    // The files keep complete inode and path rows, without block rows.
    assert_eq!(env.tree_paths(), vec!["sparse", "tail", "zeros"]);
    assert_eq!(env.count("inodes"), 3);
    assert_eq!(env.index().incomplete_inodes(), 0);
    assert_eq!(env.count("blocks"), 0);

    // The next update reuses the files instead of rehashing them.
    let stderr = env.update();
    assert!(stderr.contains(" 0 files hashed"), "{stderr}");
    assert!(stderr.contains("3 unchanged"), "{stderr}");
    assert_eq!(env.tree_paths(), vec!["sparse", "tail", "zeros"]);
}

#[test]
fn thread_count_options_are_accepted() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&format!("threads = 2\n{}", env.root("tree", "")));
    let output = env.run(&["update", "-j", "1"]);
    assert!(output.status.success());
    assert_eq!(env.tree_paths(), vec!["f"]);
}

#[test]
fn updates_reflect_changes_and_rebuilds() {
    let env = Env::new();
    env.write("tree/stays", &random(BLOCK, 1));
    env.write("tree/goes", &random(BLOCK, 2));
    env.write("tree/shrinks", &random(BLOCK * 2, 3));
    env.write("tree/moves", &random(BLOCK, 4));
    env.write("tree/blanks", &random(BLOCK, 6));
    env.configure(&env.root("tree", ""));
    env.update();
    assert_eq!(env.count("inodes"), 5);
    let index_id = env.index_id();

    std::fs::remove_file(env.path("tree/goes")).unwrap();
    env.write("tree/shrinks", &random(10, 3));
    std::fs::rename(env.path("tree/moves"), env.path("tree/moved")).unwrap();
    env.write("tree/new", &random(BLOCK, 5));
    // Only periodic content: the file keeps a complete inode and its
    // path, but loses its block rows.
    env.write("tree/blanks", &vec![0u8; BLOCK]);
    let stderr = env.update();
    // A rename changes the file's ctime, so `moved` is hashed again;
    // `blanks` and `new` are the other two.
    assert!(stderr.contains("3 files hashed"), "{stderr}");
    assert!(stderr.contains("1 unchanged"), "{stderr}");
    assert!(stderr.contains("1 stale inodes removed"), "{stderr}");
    assert_eq!(env.tree_paths(), vec!["blanks", "moved", "new", "stays"]);
    assert_eq!(env.count("inodes"), 4);
    assert_eq!(env.count("blocks"), 3);
    assert!(env.block_numbers("blanks").is_empty());
    env.index().assert_consistent();
    assert_eq!(
        env.index_id(),
        index_id,
        "a plain update keeps the index ID"
    );

    let output = env.run(&["update", "--rebuild"]);
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("4 files hashed"), "{stderr}");
    assert!(stderr.contains(" 0 unchanged"), "{stderr}");
    assert_ne!(env.index_id(), index_id, "a rebuild renews the index ID");
    assert_eq!(env.count("inodes"), 4);
    env.index().assert_consistent();

    // Changed settings rebuild the index in place.
    let rebuilt_id = env.index_id();
    env.configure_with(&format!(
        "block_size = {}\n{}",
        BLOCK * 2,
        env.root("tree", "")
    ));
    let stderr = env.update();
    assert!(stderr.contains("rebuilding"), "{stderr}");
    assert_ne!(env.index_id(), rebuilt_id);
    assert_eq!(
        env.count("inodes"),
        0,
        "every file is smaller than the 8 KiB block size"
    );
    assert_eq!(env.index().meta_u64("block_size"), (BLOCK * 2) as u64);
}

#[test]
fn small_block_sizes_warn_when_the_index_is_built() {
    const WARNING: &str = "block_size (512 bytes) is less than 4 KiB";
    let env = Env::new();
    env.write("tree/f", &random(BLOCK * 3, 1));
    let update = |block_size: usize| {
        env.configure_with(&format!(
            "block_size = {block_size}\n{}",
            env.root("tree", "")
        ));
        env.update()
    };

    let stderr = update(512);
    assert!(stderr.contains("creating index"), "{stderr}");
    assert!(stderr.contains(WARNING), "{stderr}");
    assert_eq!(env.count("blocks"), BLOCK * 3 / 512);
    let stderr = update(512);
    assert!(!stderr.contains(WARNING), "the index stays: {stderr}");
    let output = env.run(&["update", "--rebuild"]);
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains(WARNING));

    // Changed settings rebuild the index, and the new block size decides.
    let stderr = update(BLOCK);
    assert!(stderr.contains("rebuilding"), "{stderr}");
    assert!(!stderr.contains("less than 4 KiB"), "{stderr}");
    let stderr = update(512);
    assert!(stderr.contains("rebuilding"), "{stderr}");
    assert!(stderr.contains(WARNING), "{stderr}");

    // `--quiet` removes the notes, but not the warning.
    let output = env.run(&["update", "--rebuild", "--quiet"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert_eq!(stderr.lines().count(), 1, "{stderr}");
    assert!(stderr.contains(WARNING), "{stderr}");
}

#[test]
fn stats_reports_the_index() {
    let env = Env::new();
    env.configure(&env.root("tree", ""));
    let output = env.run(&["stats"]);
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("no index"));

    env.write("tree/f", &random(BLOCK * 2, 1));
    env.update();
    let stats = env.stats();
    assert!(stats.contains("schema version:   1"), "{stats}");
    assert!(
        stats.contains(&format!("block size:       {BLOCK}")),
        "{stats}"
    );
    assert!(stats.contains("strong hash:      blake3"), "{stats}");
    assert!(
        stats.contains("inodes:           1 (0 incomplete)"),
        "{stats}"
    );
    assert!(stats.contains("paths:            1"), "{stats}");
    assert!(stats.contains("blocks:           2"), "{stats}");
    assert!(stats.contains("distinct weak:    2"), "{stats}");
    assert!(stats.contains("indexed size:     8.00 KiB"), "{stats}");
    // The end of the update wrote the memtables to the database files
    // (DESIGN.md §4.4).
    assert!(stats.contains("(+0 B write-ahead log)"), "{stats}");

    // `--no-human-readable` shows whole bytes. The block size is a
    // setting, which shows exactly either way.
    let output = env.run(&["stats", "--no-human-readable"]);
    assert!(output.status.success());
    let stats = String::from_utf8(output.stdout).unwrap();
    assert!(
        stats.contains(&format!("block size:       {BLOCK}\n")),
        "{stats}"
    );
    assert!(
        stats.contains(&format!("indexed size:     {} B\n", BLOCK * 2)),
        "{stats}"
    );

    // Stats of an incompatible index still print, with a warning.
    env.configure_with(&format!(
        "block_size = {}\n{}",
        BLOCK * 2,
        env.root("tree", "")
    ));
    let output = env.run(&["stats"]);
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("differ from the configuration"));
}

/// RocksDB keeps all files of the index open: here one for each column
/// family, more than a soft descriptor limit of 8 leaves after the
/// standard streams and the lock file. Every process raises the limit
/// before it opens the index (DESIGN.md §4.4).
#[test]
fn a_low_descriptor_limit_is_raised() {
    let env = Env::new();
    env.write("tree/f", &random(BLOCK, 1));
    env.configure(&env.root("tree", ""));
    for command in ["update", "stats"] {
        let output = Command::new("sh")
            .args(["-c", "ulimit -S -n 8 && exec \"$@\"", "sh"])
            .arg(env!("CARGO_BIN_EXE_hashsync"))
            .arg("--config")
            .arg(env.config_path())
            .arg(command)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{command} failed:\n{stderr}");
    }
}

#[test]
fn unreadable_entries_are_skipped_with_a_warning() {
    if rustix::process::geteuid().is_root() {
        eprintln!("skipped: root ignores permissions");
        return;
    }
    let env = Env::new();
    env.write("tree/ok", &random(BLOCK, 1));
    env.write("tree/secret/f", &random(BLOCK, 2));
    env.write("tree/unreadable", &random(BLOCK, 3));
    env.chmod("tree/secret", 0o000);
    env.chmod("tree/unreadable", 0o000);
    env.configure(&env.root("tree", ""));
    let stderr = env.update();
    env.chmod("tree/secret", 0o755);
    assert!(
        stderr.contains("secret: reopen: Permission denied"),
        "{stderr}"
    );
    assert!(
        stderr.contains("unreadable: reopen: Permission denied"),
        "{stderr}"
    );
    assert_eq!(env.tree_paths(), vec!["ok"]);
    assert_eq!(env.index().incomplete_inodes(), 0);
}

/// A pipe without a reader: every write to it fails with `EPIPE`.
fn broken_pipe() -> std::io::PipeWriter {
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    writer
}

#[test]
fn broken_output_streams_never_abort() {
    // Diagnostics are best effort, so a broken stderr changes no outcome:
    // an update still succeeds after its warnings and summary, and a fatal
    // error still exits with status 1, without a core dump.
    let env = Env::new();
    env.write("tree/a", &random(BLOCK, 1));
    env.write("tree/unreadable", &random(BLOCK, 2));
    env.chmod("tree/unreadable", 0o000);
    env.configure(&env.root("tree", ""));
    let status = env
        .command(&["update"])
        .stderr(broken_pipe())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(0), "{status}");
    let status = Command::new(env!("CARGO_BIN_EXE_hashsync"))
        .args(["--config", "/nonexistent/config.toml", "stats"])
        .stderr(broken_pipe())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(1), "{status}");

    // The report of `stats` is its output: a broken stdout is an external
    // failure.
    let output = env
        .command(&["stats"])
        .stdout(broken_pipe())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("cannot write to stdout"), "{stderr}");
}

#[test]
fn missing_configuration_fails() {
    let env = Env::new();
    let output = env.run(&["update"]);
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot read configuration file"));
}

/// Each profile has its own configuration file and, unless the file sets
/// `index_path`, its own index, named after it: the profile `default`
/// without `--profile`, and the one that it names otherwise. A message
/// that asks for an update names the profile. A named profile needs its
/// file, and a profile name is one nonempty path component (DESIGN.md §7).
#[test]
fn profiles_have_their_own_files_and_indexes() {
    let env = Env::new();
    env.write("tree/a", &random(BLOCK, 1));
    env.write("work/b", &random(BLOCK, 2));
    env.write("work/c", &random(BLOCK, 3));
    let config = |root: &str| format!("block_size = {BLOCK}\n{}", env.root(root, ""));
    env.write_profile("default", &config("tree"));
    env.write_profile("work", &config("work"));
    for args in [&["update"][..], &["--profile", "work", "update"]] {
        let output = env.bare_command(args).output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{args:?}: {stderr}");
        assert!(!stderr.contains("warning"), "{args:?}: {stderr}");
    }
    let indexes = ["default", "work"].map(|name| env.path(format!("cache/hashsync/{name}.db")));
    for (index, blocks) in indexes.iter().zip([1, 2]) {
        assert_eq!(IndexView::open(index, false).count("blocks"), blocks);
    }
    let output = env.bare_command(&["--profile", "work", "stats"]).output();
    let stats = String::from_utf8(output.unwrap().stdout).unwrap();
    let line = format!("index path:       {}\n", indexes[1].display());
    assert!(stats.contains(&line), "{stats}");

    env.write_profile("new", &config("work"));
    let output = env.bare_command(&["--profile", "new", "stats"]).output();
    let output = output.unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("; run `hashsync --profile=new update`"),
        "{stderr}"
    );

    let output = env
        .bare_command(&["--profile", "missing", "update"])
        .output();
    let output = output.unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    let missing = env.profile_path("missing");
    let message = format!("cannot read configuration file {}", missing.display());
    assert!(stderr.contains(&message), "{stderr}");

    for name in ["", "a/b", "/"] {
        let output = env.bare_command(&["--profile", name, "stats"]).output();
        let output = output.unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{name:?}: {stderr}");
        assert!(
            stderr.contains("invalid profile name"),
            "{name:?}: {stderr}"
        );
    }
}

/// `--config` names a configuration file anywhere. Unless the file sets
/// `index_path`, its index is named after it, without its extension, as
/// the index of a profile is. So files of the same name share an index,
/// such as this file and the file of the profile of that name, and a
/// command that uses the index of a file that `--config` names without
/// `index_path` warns about that. A message that asks for an update
/// repeats `--config`. `--profile` and `--config` together are a usage
/// error (DESIGN.md §7).
#[test]
fn a_file_that_config_names_has_an_index_named_after_it() {
    let env = Env::new();
    env.write("tree/a", &random(BLOCK, 1));
    let text = format!("block_size = {BLOCK}\n{}", env.root("tree", ""));
    env.write("elsewhere/nas.conf", text.as_bytes());
    let file = env.path("elsewhere/nas.conf");
    let file = file.to_str().unwrap();
    let index = env.path("cache/hashsync/nas.db");
    let output = env.bare_command(&["--config", file, "update"]).output();
    let output = output.unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    let warning = format!(
        "hashsync: warning: {file} sets no index_path, so its index is {}, ",
        index.display()
    );
    assert!(stderr.contains(&warning), "{stderr}");
    assert_eq!(IndexView::open(&index, false).count("blocks"), 1);

    // The profile `nas` uses the same index, without a warning.
    env.write_profile("nas", &text);
    let output = env.bare_command(&["--profile", "nas", "stats"]).output();
    let output = output.unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(!stderr.contains("warning"), "{stderr}");
    let stats = String::from_utf8(output.stdout).unwrap();
    let line = format!("index path:       {}\n", index.display());
    assert!(stats.contains(&line), "{stats}");

    env.write("elsewhere/new.toml", text.as_bytes());
    let new = env.path("elsewhere/new.toml");
    let new = new.to_str().unwrap();
    let output = env.bare_command(&["--config", new, "stats"]).output();
    let stderr = String::from_utf8_lossy(&output.as_ref().unwrap().stderr);
    assert!(
        stderr.contains("; run `hashsync --config=") && stderr.contains("/new.toml update`"),
        "{stderr}"
    );

    let set = format!(
        "index_path = '{}'\n{text}",
        toml_literal(index.to_str().unwrap().as_bytes())
    );
    env.write("elsewhere/nas.conf", set.as_bytes());
    let output = env.bare_command(&["--config", file, "update"]).output();
    let output = output.unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(!stderr.contains("warning"), "{stderr}");

    for words in [
        &["--profile", "nas", "--config", file, "stats"][..],
        &["--config", file, "stats", "--profile", "nas"],
    ] {
        let output = env.bare_command(words).output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{words:?}: {output:?}");
    }
}

/// The update that a message asks for is a command line that the user can
/// copy into a shell as it is. It gives back a profile name or a path that
/// starts with `-`, or that holds spaces, quotes, control characters, or
/// bytes that are not UTF-8, and updates the index of that file.
#[test]
fn the_update_that_a_message_asks_for_runs_as_it_is() {
    let env = Env::new();
    env.write("tree/a", &random(BLOCK, 1));
    let text = format!("block_size = {BLOCK}\n{}", env.root("tree", ""));
    // The hint of a message such as `no index at PATH; run `COMMAND``.
    let hint = |output: Output| -> String {
        let stderr = String::from_utf8(output.stderr).unwrap();
        let line = stderr.lines().find(|line| line.contains("; run `"));
        let (_, rest) = line.expect(&stderr).split_once("; run `").unwrap();
        rest.strip_suffix('`').unwrap().to_string()
    };
    let profiles: [&[u8]; 4] = [b"work files", b"-x", b"it's", b"a\xff\tb"];
    for name in profiles {
        let name = OsStr::from_bytes(name);
        let file = [name.as_bytes(), b".toml"].concat();
        env.write(env.path("config/hashsync").join(os(&file)), text.as_bytes());
        let mut stats = env.bare_command(&[]);
        let option = [b"--profile=", name.as_bytes()].concat();
        let output = stats.arg(os(&option)).arg("stats").output();
        let line = hint(output.unwrap());
        let output = env.shell(&line).output().unwrap();
        assert!(output.status.success(), "{line}: {output:?}");
        let index = env
            .path("cache/hashsync")
            .join(os(&[name.as_bytes(), b".db"].concat()));
        assert_eq!(IndexView::open(&index, false).count("blocks"), 1, "{line}");
    }

    // A path that starts with `-`, relative to the current directory.
    let dir = env.path("elsewhere");
    env.write("elsewhere/-it's here.conf", text.as_bytes());
    let mut stats = env.bare_command(&["--config=-it's here.conf", "stats"]);
    let line = hint(stats.current_dir(&dir).output().unwrap());
    let output = env.shell(&line).current_dir(&dir).output().unwrap();
    assert!(output.status.success(), "{line}: {output:?}");
    let index = env.path("cache/hashsync/-it's here.db");
    assert_eq!(IndexView::open(&index, false).count("blocks"), 1, "{line}");
}

#[test]
fn bare_relative_index_directory() {
    let env = Env::new();
    env.write("tree/a", &random(BLOCK, 1));
    // A single-component relative name: the index directory is created in
    // the current directory, whose path is empty.
    std::fs::write(
        env.config_path(),
        format!(
            "index_path = 'default.db'\nblock_size = {BLOCK}\n{}",
            env.root("tree", "")
        ),
    )
    .unwrap();
    let mut update = env.command(&["update"]);
    update.current_dir(env.dir.path());
    for run in ["first", "second"] {
        // The second run opens the index that already exists.
        let output = update.output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{run} update failed:\n{stderr}");
    }
    assert!(env.index_path().is_dir());
    assert_eq!(env.count("blocks"), 1);
}
