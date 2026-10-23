//! Shared helpers for integration tests: a temporary environment with a
//! configuration file, an index directory, and a tree to index; the
//! frames of the protocol; and the processes that the tests start.
#![allow(dead_code)]

use std::ffi::OsStr;
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

use rocksdb::{ColumnFamily, DB, IteratorMode, Options, WriteBatch};
use rustix::event::{PollFd, PollFlags, poll};
use rustix::io::retry_on_intr;
use tempfile::TempDir;

pub const BLOCK: usize = 4096;

pub struct Env {
    pub dir: TempDir,
}

impl Env {
    pub fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("hashsync-test-")
            .tempdir()
            .unwrap();
        std::fs::create_dir(dir.path().join("tree")).unwrap();
        Env { dir }
    }

    pub fn path(&self, rel: impl AsRef<Path>) -> PathBuf {
        self.dir.path().join(rel)
    }

    pub fn tree(&self) -> PathBuf {
        self.path("tree")
    }

    /// The index directory.
    pub fn index_path(&self) -> PathBuf {
        self.path("default.db")
    }

    pub fn config_path(&self) -> PathBuf {
        self.path("config.toml")
    }

    /// Write a configuration with 4 KiB blocks and the given extra text.
    pub fn configure(&self, body: &str) {
        self.configure_with(&format!("block_size = {BLOCK}\n{body}"));
    }

    pub fn configure_with(&self, body: &str) {
        let text = format!(
            "index_path = '{}'\n{body}",
            toml_literal(self.index_path().as_os_str().as_bytes())
        );
        std::fs::write(self.config_path(), text).unwrap();
    }

    /// A `[[roots]]` entry for a path under the environment.
    pub fn root(&self, rel: &str, extra: &str) -> String {
        root_entry(self.path(rel).as_os_str().as_bytes(), extra)
    }

    pub fn write(&self, rel: impl AsRef<Path>, data: &[u8]) {
        let path = self.path(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, data).unwrap();
    }

    pub fn mkdir(&self, rel: impl AsRef<Path>) {
        std::fs::create_dir_all(self.path(rel)).unwrap();
    }

    /// `hashsync --config CONFIG ARGS...` with the configuration file of this
    /// environment. Its home directory holds none of the files of the
    /// environment, so the notes show their paths in full, whatever the
    /// home directory of the test (DESIGN.md §7).
    pub fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_hashsync"));
        command
            .env("HOME", self.path("home"))
            .arg("--config")
            .arg(self.config_path())
            .args(args);
        command
    }

    /// `hashsync ARGS...` without the `--config` of `command`, in the
    /// configuration and cache directories of this environment: the files
    /// of its profiles are in `config/hashsync`, and the indexes named
    /// after their files in `cache/hashsync` (DESIGN.md §7).
    pub fn bare_command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_hashsync"));
        self.in_home(&mut command).args(args);
        command
    }

    /// `line` as bash runs a command line that the user types, in the
    /// directories of `bare_command`, with this build as `hashsync`.
    pub fn shell(&self, line: &str) -> Command {
        let mut command = Command::new("bash");
        self.in_home(&mut command)
            .env("HASHSYNC", env!("CARGO_BIN_EXE_hashsync"))
            .args(["-c", r#"hashsync() { "$HASHSYNC" "$@"; }; eval "$1""#])
            .args(["bash", line]);
        command
    }

    /// `command` with the configuration and cache directories of this
    /// environment, and the home directory of `command`.
    fn in_home<'a>(&self, command: &'a mut Command) -> &'a mut Command {
        command
            .env("HOME", self.path("home"))
            .env("XDG_CONFIG_HOME", self.path("config"))
            .env("XDG_CACHE_HOME", self.path("cache"))
    }

    /// The configuration file of the profile `name`, for `bare_command`.
    pub fn profile_path(&self, name: &str) -> PathBuf {
        self.path(format!("config/hashsync/{name}.toml"))
    }

    pub fn write_profile(&self, name: &str, text: &str) {
        self.write(self.profile_path(name), text.as_bytes());
    }

    pub fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }

    /// Run `update` and require success. Returns stderr.
    pub fn update(&self) -> String {
        let output = self.run(&["update"]);
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert!(output.status.success(), "update failed:\n{stderr}");
        stderr
    }

    /// Run `update` and require failure. Returns stderr.
    pub fn update_fails(&self) -> String {
        let output = self.run(&["update"]);
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert_eq!(
            output.status.code(),
            Some(1),
            "update did not fail:\n{stderr}"
        );
        stderr
    }

    pub fn stats(&self) -> String {
        let output = self.run(&["stats"]);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "stats failed:\n{stderr}");
        String::from_utf8(output.stdout).unwrap()
    }

    /// Make the entries `closed` impossible to read or search, each with the
    /// mode that it gets back when the returned value drops.
    pub fn close<'a>(&'a self, closed: &[(&'a str, u32)]) -> Closed<'a> {
        self.close_to(closed, 0o000)
    }

    /// Make the directories `closed` impossible to search, though they can
    /// still be listed, so that no entry in them opens: `close` with mode
    /// 0444.
    pub fn close_search<'a>(&'a self, closed: &[(&'a str, u32)]) -> Closed<'a> {
        self.close_to(closed, 0o444)
    }

    fn close_to<'a>(&'a self, closed: &[(&'a str, u32)], mode: u32) -> Closed<'a> {
        for &(rel, _) in closed {
            self.chmod(rel, mode);
        }
        Closed {
            env: self,
            entries: closed.to_vec(),
        }
    }

    /// Run `hashsync ARGS...` while the entries `closed` cannot be read or
    /// searched (`close`). Requires success. Returns stderr.
    pub fn run_with_closed(&self, args: &[&str], closed: &[(&str, u32)]) -> String {
        let closed = self.close(closed);
        let output = self.run(args);
        drop(closed);
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert!(output.status.success(), "{args:?} failed:\n{stderr}");
        stderr
    }

    /// Run `hashsync ARGS...`, a command that resolves the roots before it
    /// opens the index (DESIGN.md §4.2), such as `update` or `prune`, and
    /// call `change` after the resolution and before the command uses the
    /// index: the command waits while this process holds a lock of the
    /// index, and `change` runs during that wait. Requires success. Returns
    /// stderr.
    pub fn run_after_resolution(&self, args: &[&str], change: impl FnOnce()) -> String {
        // The first update creates the index and its lock file; this process
        // creates them first, to lock that file.
        std::fs::create_dir_all(self.index_path()).unwrap();
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.index_path().join("hashsync.lock"))
            .unwrap();
        let lock = shared_lock(&self.index_path());
        let child = self.command(args).stderr(Stdio::piped()).spawn().unwrap();
        let mut child = KillOnDrop(child);
        wait_for_lock_wait(child.0.id());
        change();
        drop(lock);
        let mut stderr = String::new();
        let mut pipe = child.0.stderr.take().unwrap();
        pipe.read_to_string(&mut stderr).unwrap();
        assert!(
            child.0.wait().unwrap().success(),
            "{args:?} failed:\n{stderr}"
        );
        stderr
    }

    /// The index, open read-only.
    pub fn index(&self) -> IndexView {
        IndexView::open(&self.index_path(), false)
    }

    /// The index, open read/write, for tests that manufacture index
    /// states.
    pub fn index_rw(&self) -> IndexView {
        IndexView::open(&self.index_path(), true)
    }

    /// The number of entries in a column family of the index.
    pub fn count(&self, cf: &str) -> usize {
        self.index().count(cf)
    }

    /// The column families of the index, sorted.
    pub fn column_families(&self) -> Vec<String> {
        let mut names = DB::list_cf(&Options::default(), self.index_path()).unwrap();
        names.sort();
        names
    }

    /// Every indexed `(root path, relative path)`, sorted.
    pub fn paths(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let index = self.index();
        let roots = index.roots();
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = index
            .paths()
            .into_iter()
            .map(|path| {
                let root = roots.iter().find(|root| root.id == path.root_id);
                (
                    root.expect("a path without its root").path.clone(),
                    path.rel_path,
                )
            })
            .collect();
        rows.sort();
        rows
    }

    /// Relative paths under the `tree` root, as strings.
    pub fn tree_paths(&self) -> Vec<String> {
        let tree = self.tree().as_os_str().as_bytes().to_vec();
        self.paths()
            .into_iter()
            .filter(|(root, _)| *root == tree)
            .map(|(_, rel)| String::from_utf8_lossy(&rel).into_owned())
            .collect()
    }

    /// The block rows of the file at `rel` under `tree`, by block number;
    /// none when there is no such path.
    fn file_blocks(&self, rel: &str) -> Vec<Block> {
        let index = self.index();
        let tree = self.tree().as_os_str().as_bytes().to_vec();
        let roots: Vec<u64> = index
            .roots()
            .into_iter()
            .filter(|root| root.path == tree)
            .map(|root| root.id)
            .collect();
        let inodes: Vec<u64> = index
            .paths()
            .into_iter()
            .filter(|path| roots.contains(&path.root_id) && path.rel_path == rel.as_bytes())
            .map(|path| path.inode_id)
            .collect();
        index
            .blocks()
            .into_iter()
            .filter(|block| inodes.contains(&block.inode_id))
            .collect()
    }

    /// Block numbers indexed for the file at `rel` under `tree`.
    pub fn block_numbers(&self, rel: &str) -> Vec<u64> {
        self.file_blocks(rel)
            .into_iter()
            .map(|block| block.block_no)
            .collect()
    }

    /// The strong-hash prefixes stored for the file at `rel` under `tree`.
    pub fn strong_prefixes(&self, rel: &str) -> Vec<Vec<u8>> {
        self.file_blocks(rel)
            .into_iter()
            .map(|block| block.strong_prefix)
            .collect()
    }

    pub fn index_id(&self) -> Vec<u8> {
        self.index().meta("index_id")
    }

    pub fn chmod(&self, rel: &str, mode: u32) {
        std::fs::set_permissions(self.path(rel), std::fs::Permissions::from_mode(mode)).unwrap();
    }
}

/// Entries of an environment that cannot be read or searched while this
/// value lives (`Env::close`, `Env::close_search`). Its drop gives them
/// their modes back, also when an assertion fails, so that the cleanup of
/// the test can remove the tree.
pub struct Closed<'a> {
    env: &'a Env,
    entries: Vec<(&'a str, u32)>,
}

impl Drop for Closed<'_> {
    fn drop(&mut self) {
        for &(rel, mode) in &self.entries {
            // A panic here, while a failed assertion unwinds, would abort
            // the test binary.
            let permissions = std::fs::Permissions::from_mode(mode);
            let _ = std::fs::set_permissions(self.env.path(rel), permissions);
        }
    }
}

/// The indexed paths, as `(root, relative path)` strings, relative to the
/// environment.
pub fn indexed(env: &Env) -> Vec<(String, String)> {
    let base = env.dir.path().as_os_str().as_bytes().to_vec();
    env.paths()
        .into_iter()
        .map(|(root, rel)| {
            let root = root
                .strip_prefix(&base[..])
                .expect("a root outside the test");
            let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
            (text(&root[1..]), text(&rel))
        })
        .collect()
}

pub fn pair(root: &str, rel: &str) -> (String, String) {
    (root.to_string(), rel.to_string())
}

/// What the note of an update says about the immutable groups: how many it
/// reused, completed, and left incomplete.
pub fn groups(stderr: &str) -> [u64; 3] {
    let line = stderr
        .lines()
        .find_map(|line| line.strip_prefix("hashsync: immutable roots: "))
        .unwrap_or_else(|| panic!("no note about immutable roots:\n{stderr}"));
    let counts: Vec<u64> = line
        .split(", ")
        .map(|part| part.split(' ').next().unwrap().parse().unwrap())
        .collect();
    counts.try_into().unwrap()
}

/// The warnings in `stderr`, one for each line.
pub fn warnings(stderr: &str) -> Vec<&str> {
    stderr
        .lines()
        .filter(|line| line.starts_with("hashsync: warning: "))
        .collect()
}

/// The warning for the missing root at `path`.
pub fn missing(path: impl AsRef<Path>) -> String {
    format!(
        "hashsync: warning: {}: root does not exist",
        quoted(path.as_ref())
    )
}

/// The warning for the entry at `path` when permissions deny the operation
/// `what` on it, such as `open` or `reopen`.
pub fn denied(path: impl AsRef<Path>, what: &str) -> String {
    format!(
        "hashsync: warning: {}: {what}: Permission denied (os error 13)",
        quoted(path.as_ref())
    )
}

/// True when the test runs as root, whom permissions do not stop: the tests
/// of entries that cannot be read or searched skip themselves then.
pub fn skip_as_root() -> bool {
    let root = rustix::process::geteuid().is_root();
    if root {
        eprintln!("skipped: root ignores permissions");
    }
    root
}

/// The column families of the index (DESIGN.md §4.4).
pub const COLUMN_FAMILIES: [&str; 8] = [
    "default",
    "roots",
    "inodes",
    "identities",
    "paths",
    "inode_paths",
    "blocks",
    "strong",
];

/// An open index database. This is the tests' own reading of the layout
/// in DESIGN.md §4.4, which does not use the code of `src/index`: integers
/// are big endian, and IDs and block numbers have 8 bytes.
pub struct IndexView {
    db: DB,
}

/// A row of `roots`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Root {
    pub id: u64,
    pub identifier: Option<Vec<u8>>,
    pub path: Vec<u8>,
}

impl Root {
    /// The `roots` value: the length of the identifier, which is zero
    /// without one, the identifier, and the path.
    pub fn value(&self) -> Vec<u8> {
        let identifier = self.identifier.as_deref().unwrap_or_default();
        let mut value = (identifier.len() as u32).to_be_bytes().to_vec();
        value.extend_from_slice(identifier);
        value.extend_from_slice(&self.path);
        value
    }
}

/// A row of `inodes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inode {
    pub id: u64,
    /// `None` while processing is incomplete.
    pub size: Option<u64>,
    pub ctime_ns: i64,
    /// The `identities` key of the inode.
    pub identity: Vec<u8>,
}

impl Inode {
    /// The `inodes` value: a state byte (0 incomplete, 1 complete), the
    /// size (0 while incomplete), the ctime, and the identity.
    pub fn value(&self) -> Vec<u8> {
        let mut value = vec![u8::from(self.size.is_some())];
        value.extend_from_slice(&self.size.unwrap_or(0).to_be_bytes());
        value.extend_from_slice(&self.ctime_ns.to_be_bytes());
        value.extend_from_slice(&self.identity);
        value
    }
}

/// A row of `paths`, whose `inode_paths` entry has the same fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathRow {
    pub root_id: u64,
    pub rel_path: Vec<u8>,
    pub inode_id: u64,
}

/// A row of `blocks`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    pub inode_id: u64,
    pub block_no: u64,
    pub weak: u64,
    pub strong_prefix: Vec<u8>,
}

impl Block {
    /// The `blocks` key: the inode ID and the block number.
    pub fn key(&self) -> Vec<u8> {
        [self.inode_id.to_be_bytes(), self.block_no.to_be_bytes()].concat()
    }

    /// The `strong` key: the strong-hash prefix, then the `blocks` key.
    pub fn strong_key(&self) -> Vec<u8> {
        [self.strong_prefix.clone(), self.key()].concat()
    }
}

/// The big-endian `u64` at `at` in `bytes`.
pub fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(bytes[at..at + 8].try_into().unwrap())
}

impl IndexView {
    pub fn open(path: &Path, writable: bool) -> Self {
        let options = Options::default();
        let db = if writable {
            DB::open_cf(&options, path, COLUMN_FAMILIES)
        } else {
            DB::open_cf_for_read_only(&options, path, COLUMN_FAMILIES, false)
        };
        IndexView { db: db.unwrap() }
    }

    pub fn cf(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).unwrap()
    }

    /// Every entry of a column family, in key order.
    pub fn entries(&self, cf: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.db
            .iterator_cf(self.cf(cf), IteratorMode::Start)
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.to_vec(), value.to_vec())
            })
            .collect()
    }

    pub fn count(&self, cf: &str) -> usize {
        self.entries(cf).len()
    }

    /// A metadata value, which must exist.
    pub fn meta(&self, name: &str) -> Vec<u8> {
        self.db
            .get_cf(self.cf("default"), name)
            .unwrap()
            .unwrap_or_else(|| panic!("no metadata {name}"))
    }

    /// A numeric metadata value.
    pub fn meta_u64(&self, name: &str) -> u64 {
        u64_at(&self.meta(name), 0)
    }

    pub fn roots(&self) -> Vec<Root> {
        self.entries("roots")
            .into_iter()
            .map(|(key, value)| {
                let len = u32::from_be_bytes(value[..4].try_into().unwrap()) as usize;
                Root {
                    id: u64_at(&key, 0),
                    identifier: (len > 0).then(|| value[4..4 + len].to_vec()),
                    path: value[4 + len..].to_vec(),
                }
            })
            .collect()
    }

    pub fn inodes(&self) -> Vec<Inode> {
        self.entries("inodes")
            .into_iter()
            .map(|(key, value)| Inode {
                id: u64_at(&key, 0),
                size: match value[0] {
                    0 => None,
                    1 => Some(u64_at(&value, 1)),
                    state => panic!("inode state {state}"),
                },
                ctime_ns: u64_at(&value, 9) as i64,
                identity: value[17..].to_vec(),
            })
            .collect()
    }

    /// The inodes whose processing is incomplete.
    pub fn incomplete_inodes(&self) -> usize {
        self.inodes()
            .iter()
            .filter(|inode| inode.size.is_none())
            .count()
    }

    pub fn paths(&self) -> Vec<PathRow> {
        self.entries("paths")
            .into_iter()
            .map(|(key, value)| PathRow {
                root_id: u64_at(&key, 0),
                rel_path: key[8..].to_vec(),
                inode_id: u64_at(&value, 0),
            })
            .collect()
    }

    /// The `inode_paths` entries, as paths.
    pub fn inode_paths(&self) -> Vec<PathRow> {
        self.entries("inode_paths")
            .into_iter()
            .map(|(key, value)| {
                assert!(value.is_empty());
                PathRow {
                    root_id: u64_at(&key, 8),
                    rel_path: key[16..].to_vec(),
                    inode_id: u64_at(&key, 0),
                }
            })
            .collect()
    }

    pub fn blocks(&self) -> Vec<Block> {
        self.entries("blocks")
            .into_iter()
            .map(|(key, value)| Block {
                inode_id: u64_at(&key, 0),
                block_no: u64_at(&key, 8),
                weak: u64_at(&value, 0),
                strong_prefix: value[8..].to_vec(),
            })
            .collect()
    }

    pub fn write(&self, batch: WriteBatch) {
        self.db.write(batch).unwrap();
    }

    /// The deletions that the files of the database hold, in all column
    /// families: removals that no compaction to the last level has applied
    /// to the files yet.
    pub fn file_deletions(&self) -> u64 {
        let files = self.db.live_files().unwrap();
        files.iter().map(|file| file.num_deletions).sum()
    }

    /// Assert the pairs of DESIGN.md §4.4: each inode row has its
    /// `identities` entry, each path row its `inode_paths` entry, and each
    /// block row its `strong` entry, with no other entries. Paths and
    /// blocks refer to rows that exist.
    pub fn assert_consistent(&self) {
        let inodes = self.inodes();
        let mut identities: Vec<(Vec<u8>, u64)> = self
            .entries("identities")
            .into_iter()
            .map(|(key, value)| (key, u64_at(&value, 0)))
            .collect();
        let mut expected: Vec<(Vec<u8>, u64)> = inodes
            .iter()
            .map(|inode| (inode.identity.clone(), inode.id))
            .collect();
        identities.sort();
        expected.sort();
        assert_eq!(identities, expected, "identities differ from inodes");

        let inode_ids: Vec<u64> = inodes.iter().map(|inode| inode.id).collect();
        let root_ids: Vec<u64> = self.roots().iter().map(|root| root.id).collect();
        let by_inode = |path: &PathRow| (path.inode_id, path.root_id, path.rel_path.clone());
        let mut paths = self.paths();
        let mut inode_paths = self.inode_paths();
        paths.sort_by_key(by_inode);
        inode_paths.sort_by_key(by_inode);
        assert_eq!(paths, inode_paths, "inode_paths differ from paths");
        for path in &paths {
            assert!(inode_ids.contains(&path.inode_id), "{path:?}: no inode");
            assert!(root_ids.contains(&path.root_id), "{path:?}: no root");
        }

        let blocks = self.blocks();
        let strong: Vec<Vec<u8>> = self
            .entries("strong")
            .into_iter()
            .map(|(key, value)| {
                assert!(value.is_empty());
                key
            })
            .collect();
        let mut expected: Vec<Vec<u8>> = blocks.iter().map(Block::strong_key).collect();
        expected.sort();
        assert_eq!(strong, expected, "strong differs from blocks");
        for block in &blocks {
            assert!(inode_ids.contains(&block.inode_id), "{block:?}: no inode");
            assert_eq!(block.strong_prefix.len(), 16, "{block:?}");
        }
    }
}

/// `path` as the messages of hashsync show a path that needs no escapes,
/// such as most paths of the tests: between single quotes (DESIGN.md §8).
pub fn quoted(path: impl AsRef<OsStr>) -> String {
    let path = path.as_ref().to_str().expect("a UTF-8 path");
    assert!(
        !path.contains(['\'', '\\']) && !path.contains(char::is_control) && !path.starts_with('~'),
        "{path:?} needs escapes"
    );
    format!("'{path}'")
}

/// The bytes of a byte string that a message of hashsync shows, `shown`
/// with its single quotes: the text between them, with `\xHH` and `\\`
/// decoded (DESIGN.md §8). This is how a program reads a path back from a
/// message.
pub fn unquote(shown: &str) -> Vec<u8> {
    let inner = shown.strip_prefix('\'').and_then(|s| s.strip_suffix('\''));
    let inner = inner.unwrap_or_else(|| panic!("{shown:?} is not quoted"));
    assert!(!inner.contains('\''), "{shown:?}");
    let mut bytes = Vec::new();
    let mut rest = inner.as_bytes();
    while let Some((&byte, tail)) = rest.split_first() {
        rest = match (byte, tail) {
            (b'\\', [b'\\', tail @ ..]) => {
                bytes.push(b'\\');
                tail
            }
            (b'\\', [b'x', high, low, tail @ ..]) => {
                let digit = |d: u8| char::from(d).to_digit(16).unwrap() as u8;
                bytes.push(digit(*high) << 4 | digit(*low));
                tail
            }
            (b'\\', _) => panic!("a bad escape in {shown:?}"),
            (byte, tail) => {
                bytes.push(byte);
                tail
            }
        };
    }
    bytes
}

/// A `[[roots]]` entry for an absolute path with non-ASCII bytes escaped.
pub fn root_entry(path: &[u8], extra: &str) -> String {
    format!("\n[[roots]]\npath = '{}'\n{extra}\n", toml_literal(path))
}

/// Spell bytes for a single-quoted TOML string using `\xHH` escapes for
/// everything that is not plain printable ASCII.
pub fn toml_literal(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|&b| {
            if b.is_ascii_graphic() && b != b'\\' && b != b'\'' {
                (b as char).to_string()
            } else {
                format!("\\x{b:02X}")
            }
        })
        .collect()
}

/// Deterministic pseudo-random bytes (xorshift64*).
pub fn random(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            (state.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 56) as u8
        })
        .collect()
}

/// `count` blocks copied from one pseudo-random tile, each stamped with
/// its block number: a large file of distinct useful blocks, built
/// without generating every byte.
pub fn tiled(count: usize, seed: u64) -> Vec<u8> {
    let mut data = random(BLOCK, seed).repeat(count);
    for (block_no, block) in data.chunks_exact_mut(BLOCK).enumerate() {
        block[..8].copy_from_slice(&(block_no as u64).to_le_bytes());
    }
    data
}

/// Bytes repeating a 16-byte pattern: periodic, never indexed.
pub fn pattern16(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 16) as u8 + 1).collect()
}

pub fn os(bytes: &[u8]) -> &OsStr {
    OsStr::from_bytes(bytes)
}

/// True where `unshare -Urm` works: an unprivileged user and mount
/// namespace, in which tests can mount filesystems.
pub fn namespaces_available() -> bool {
    Command::new("unshare")
        .args(["-Urm", "true"])
        .status()
        .is_ok_and(|status| status.success())
}

// The protocol version, the features, and the frame types of DESIGN.md
// §6.2, spelled out because the tests reach only the binary.
pub const PROTOCOL_VERSION: u32 = 1;
/// The features that hashsync sends: it supports none yet.
pub const FEATURES: u64 = 0;
// Session frames of the data session, never compressed. CHUNK is a session
// frame of the control session too.
pub const REQUEST: u8 = 1;
pub const HELLO: u8 = 2;
pub const CHUNK: u8 = 3;
pub const STREAM_END: u8 = 4;
pub const END: u8 = 5;
// Session frames of the control session: the list exchanges, and UPDATED.
pub const INDEX: u8 = 6;
pub const ADVERTISE: u8 = 7;
pub const HASHLIST: u8 = 8;
pub const HASHDELTA: u8 = 9;
pub const HASHKEEP: u8 = 10;
pub const HASHNONE: u8 = 11;
pub const HASHDATA: u8 = 12;
pub const HASHFULL: u8 = 13;
pub const CACHE_RESULT: u8 = 14;
pub const UPDATED: u8 = 15;
// Stream frames: inside the zstd frame of a stream.
pub const LIT: u8 = 16;
pub const REF: u8 = 17;
pub const RLE: u8 = 18;
// Control frames: inside the control stream of the control session.
pub const QUERY: u8 = 19;
pub const ANSWER: u8 = 20;
pub const STOP: u8 = 21;

/// The first bytes of each session, in both directions: the protocol
/// version, then `features`. hashsync sends `FEATURES`, but a later peer
/// can send bits that hashsync does not know.
pub fn version_and_features(features: u64) -> [u8; 12] {
    let mut bytes = [0; 12];
    bytes[..4].copy_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    bytes[4..].copy_from_slice(&features.to_le_bytes());
    bytes
}

/// The name of the weak hash of DESIGN.md §5.1, as HELLO carries it.
pub const WEAK_HASH_ID: &[u8] = b"poly64-9e3779b97f4a7c15-splitmix64";

/// The payload of a receiver's HELLO: its block size, then the names of
/// the weak hash of this build and of its strong hash `strong`, each with
/// its length as a little-endian `u32` (DESIGN.md §6.2).
pub fn hello_payload(block_size: u32, strong: &[u8]) -> Vec<u8> {
    let mut hello = block_size.to_le_bytes().to_vec();
    for name in [WEAK_HASH_ID, strong] {
        hello.extend_from_slice(&(name.len() as u32).to_le_bytes());
        hello.extend_from_slice(name);
    }
    hello
}

/// A frame: the type, the length of the payload as a little-endian `u32`,
/// and the payload.
pub fn frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![kind];
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// One frame from `reader`: its type and its payload.
pub fn recv_frame(reader: &mut impl Read) -> (u8, Vec<u8>) {
    try_recv_frame(reader).unwrap()
}

/// One frame from `reader`, or the error that ends the stream first:
/// `UnexpectedEof` when it ends before or inside the frame.
pub fn try_recv_frame(reader: &mut impl Read) -> std::io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 5];
    reader.read_exact(&mut header)?;
    let length = u32::from_le_bytes(header[1..].try_into().unwrap());
    let mut payload = vec![0; length as usize];
    reader.read_exact(&mut payload)?;
    Ok((header[0], payload))
}

/// The state letter of the process or thread whose directory in `/proc`
/// is `dir`, such as `/proc/PID` or `/proc/PID/task/TID`: `S` when it
/// sleeps, `T` when it is stopped, `Z` for a zombie. `None` once it is
/// gone.
pub fn proc_state(dir: impl AsRef<Path>) -> Option<char> {
    let stat = std::fs::read_to_string(dir.as_ref().join("stat")).ok()?;
    // The state follows the name, which ends at the last `)`.
    stat.rsplit(')').next()?.trim_start().chars().next()
}

/// Wait until every thread of the process `pid` sleeps, polling with no
/// deadline. Fail when the process has ended: it stays a zombie until its
/// parent reaps it.
pub fn wait_until_asleep(pid: u32) {
    loop {
        // A thread that ends meanwhile has no state.
        let states: Vec<Option<char>> = std::fs::read_dir(format!("/proc/{pid}/task"))
            .unwrap()
            .map(|task| proc_state(task.unwrap().path()))
            .collect();
        assert!(!states.contains(&Some('Z')), "the process ended");
        if states.iter().all(|&state| state == Some('S')) {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Kills and reaps a child that a test leaves behind, even when the test
/// fails. `kill` does nothing to a child that `wait` reaped, whose PID
/// another process can have then.
pub struct KillOnDrop(pub Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Wait until a thread of `pid` sleeps in a wait for the lock of an index:
/// `fcntl` with `F_OFD_SETLKW`, its second argument in the `syscall` file
/// (DESIGN.md §4.4); no deadline. The thread is the main thread of `update`
/// or `stats`, and the receiver's thread of a run. Reading that file needs
/// the ptrace access that a parent has. Fail when the process has ended.
pub fn wait_for_lock_wait(pid: u32) {
    loop {
        assert_ne!(
            proc_state(format!("/proc/{pid}")),
            Some('Z'),
            "the process ended"
        );
        let tasks = std::fs::read_dir(format!("/proc/{pid}/task")).unwrap();
        for task in tasks.flatten() {
            // A thread that ends meanwhile has no file any more.
            let Ok(syscall) = std::fs::read_to_string(task.path().join("syscall")) else {
                continue;
            };
            let fields: Vec<&str> = syscall.split_whitespace().collect();
            let argument = |i: usize| {
                fields
                    .get(i)
                    .and_then(|field| u64::from_str_radix(field.trim_start_matches("0x"), 16).ok())
            };
            // A running thread has no number; one that sleeps outside a
            // system call has -1.
            if fields.first().is_some_and(|nr| nr.parse::<u64>().is_ok())
                && argument(2) == Some(libc::F_OFD_SETLKW as u64)
            {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The lock file of the index at `index`, with a shared lock that this
/// process holds until the drop of the file: an `update` of that index
/// then waits (DESIGN.md §4.4). The index takes OFD locks; this is a
/// classic POSIX lock of the whole file, which conflicts with them all the
/// same. Such a lock goes when this process closes any descriptor of the
/// file, so a test that holds it must open the file nowhere else.
pub fn shared_lock(index: &Path) -> std::fs::File {
    let file = std::fs::File::open(index.join("hashsync.lock")).unwrap();
    rustix::fs::fcntl_lock(&file, rustix::fs::FlockOperation::NonBlockingLockShared)
        .unwrap_or_else(|e| panic!("cannot lock {}: {e}", index.display()));
    file
}

/// The lock file of the index at `index`, with an exclusive lock that this
/// process holds until the drop of the file, as `shared_lock` holds a
/// shared one: a run that opens that index as it is, with `--no-update`,
/// then waits (DESIGN.md §4.4).
pub fn exclusive_lock(index: &Path) -> std::fs::File {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(index.join("hashsync.lock"))
        .unwrap();
    rustix::fs::fcntl_lock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .unwrap_or_else(|e| panic!("cannot lock {}: {e}", index.display()));
    file
}

/// The OFD lock that the process `pid` holds on the file at `path`, as the
/// `fdinfo` of its descriptors of that file shows it: `"WRITE"` or
/// `"READ"`, or `None`. An index takes one on its lock file while it is
/// open (DESIGN.md §4.4). The descriptors of the process name the file, so
/// no lock of another process counts, and no device number has to match
/// the one that `/proc/locks` shows, which differs from the one of `stat`
/// on btrfs. The descriptors of another process need the ptrace access
/// that its ancestors have.
pub fn ofd_lock(pid: u32, path: &Path) -> Option<&'static str> {
    use std::os::unix::fs::MetadataExt;
    let file = std::fs::metadata(path).unwrap();
    // A process that has ended has no descriptors.
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?;
    for fd in fds.flatten() {
        // A descriptor that closes meanwhile has no metadata or fdinfo.
        let Ok(open) = std::fs::metadata(fd.path()) else {
            continue;
        };
        if (open.dev(), open.ino()) != (file.dev(), file.ino()) {
            continue;
        }
        let info = format!("/proc/{pid}/fdinfo/{}", fd.file_name().to_string_lossy());
        let Ok(info) = std::fs::read_to_string(info) else {
            continue;
        };
        // Such as `lock:\t1: OFDLCK ADVISORY  WRITE -1 00:2d:4242 0 EOF`.
        for line in info.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.first() == Some(&"lock:") && fields.get(2) == Some(&"OFDLCK") {
                match fields.get(4) {
                    Some(&"WRITE") => return Some("WRITE"),
                    Some(&"READ") => return Some("READ"),
                    _ => {}
                }
            }
        }
    }
    None
}

/// The number of the system call in which the thread whose `syscall` file
/// is at `path` sleeps; `None` while it runs or sleeps outside a system
/// call. The file names a system call only while the thread sleeps in it.
pub fn sleeping_syscall(path: &Path) -> std::io::Result<Option<u64>> {
    let syscall = std::fs::read_to_string(path)?;
    Ok(syscall
        .split_whitespace()
        .next()
        .and_then(|nr| nr.parse().ok()))
}

/// The system call in which a thread of this process sleeps while it runs
/// `wait`, which must wait until the drop of the returned value of `make`.
/// The thread tells its ID before the wait, so the wait is the only call
/// that it can then sleep in. A process can always read the `syscall` files
/// of its own threads.
pub fn sleeping_syscall_of<T: Send + 'static, D>(
    make: impl FnOnce() -> (T, D),
    wait: impl FnOnce(T) + Send + 'static,
) -> u64 {
    let (waited, ends) = make();
    let (tid_tx, tid_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        tid_tx.send(rustix::thread::gettid()).unwrap();
        wait(waited);
    });
    let tid = tid_rx.recv().unwrap().as_raw_nonzero();
    let path = PathBuf::from(format!("/proc/self/task/{tid}/syscall"));
    let nr = loop {
        if let Some(nr) = sleeping_syscall(&path).unwrap() {
            break nr;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    drop(ends);
    waiter.join().unwrap();
    nr
}

/// The system call in which `poll` sleeps, `ppoll` or `poll`, by its number
/// on this architecture: the wait of a pipe that nothing writes, until the
/// write end closes.
pub fn poll_syscall() -> u64 {
    sleeping_syscall_of(
        || {
            let (reader, writer) = std::io::pipe().unwrap();
            (reader, writer)
        },
        |reader| {
            let mut fds = [PollFd::new(&reader, PollFlags::IN)];
            retry_on_intr(|| poll(&mut fds, None)).unwrap();
        },
    )
}
