//! Shared helpers for integration tests: a temporary environment with a
//! configuration file, an index path, and a tree to index.
#![allow(dead_code)]

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use rusqlite::{Connection, OpenFlags};
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

    pub fn index_path(&self) -> PathBuf {
        self.path("index.sqlite")
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

    pub fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_hashsync"));
        command.arg("--config").arg(self.config_path()).args(args);
        command
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

    pub fn db(&self) -> Connection {
        Connection::open_with_flags(self.index_path(), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
    }

    /// A read-write connection, for tests that manufacture index states.
    pub fn db_rw(&self) -> Connection {
        Connection::open_with_flags(self.index_path(), OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap()
    }

    pub fn count(&self, sql: &str) -> i64 {
        self.db().query_row(sql, [], |row| row.get(0)).unwrap()
    }

    /// Every indexed `(root path, relative path)`, sorted.
    pub fn paths(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let db = self.db();
        let mut stmt = db
            .prepare("SELECT roots.path, paths.rel_path FROM paths JOIN roots ON roots.id = paths.root_id")
            .unwrap();
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
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

    /// Block numbers indexed for the file at `rel` under `tree`.
    pub fn block_numbers(&self, rel: &str) -> Vec<i64> {
        let db = self.db();
        let mut stmt = db
            .prepare(
                "SELECT blocks.block_no FROM blocks
                 JOIN paths ON paths.inode_id = blocks.inode_id
                 JOIN roots ON roots.id = paths.root_id
                 WHERE roots.path = ? AND paths.rel_path = ? ORDER BY blocks.block_no",
            )
            .unwrap();
        stmt.query_map(
            rusqlite::params![self.tree().as_os_str().as_bytes(), rel.as_bytes()],
            |row| row.get(0),
        )
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    /// The strong-hash prefixes stored for the file at `rel` under `tree`.
    pub fn strong_prefixes(&self, rel: &str) -> Vec<Vec<u8>> {
        let db = self.db();
        let mut stmt = db
            .prepare(
                "SELECT blocks.strong FROM blocks
                 JOIN paths ON paths.inode_id = blocks.inode_id
                 JOIN roots ON roots.id = paths.root_id
                 WHERE roots.path = ? AND paths.rel_path = ? ORDER BY blocks.block_no",
            )
            .unwrap();
        stmt.query_map(
            rusqlite::params![self.tree().as_os_str().as_bytes(), rel.as_bytes()],
            |row| row.get(0),
        )
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    pub fn index_id(&self) -> Vec<u8> {
        self.db()
            .query_row("SELECT value FROM meta WHERE key = 'index_id'", [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    pub fn chmod(&self, rel: &str, mode: u32) {
        std::fs::set_permissions(self.path(rel), std::fs::Permissions::from_mode(mode)).unwrap();
    }
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
