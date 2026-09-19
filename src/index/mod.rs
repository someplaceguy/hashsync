//! The receiver's SQLite index (DESIGN.md §4.4).
//!
//! A modifying process opens its writer through the `unix-excl` VFS and
//! holds process-exclusive access for the life of the connection. Read-only
//! processes use ordinary read-only connections whose keeper transaction
//! excludes a writer. Lock contention waits forever; there is no busy
//! timeout.

pub mod writer;

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::thread;
use std::time::Duration;

use bstr::ByteSlice;
use rusqlite::types::Value;
use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, params};

use crate::config::Config;
use crate::die::{EXIT_FAILURE, die};
use crate::hash::weak;

pub const SCHEMA_VERSION: i64 = 1;

const SCHEMA: &str = "
CREATE TABLE meta        (key TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE filesystems (id INTEGER PRIMARY KEY, fsid BLOB NOT NULL UNIQUE);
CREATE TABLE roots       (id INTEGER PRIMARY KEY, identifier BLOB UNIQUE,
                          path BLOB NOT NULL UNIQUE);
CREATE TABLE inodes      (id INTEGER PRIMARY KEY,
                          fs_id INTEGER NOT NULL REFERENCES filesystems(id)
                            ON DELETE RESTRICT,
                          handle BLOB NOT NULL, size INTEGER,
                          ctime_ns INTEGER NOT NULL, UNIQUE (fs_id, handle));
CREATE TABLE paths       (root_id INTEGER NOT NULL REFERENCES roots(id)
                            ON DELETE RESTRICT,
                          rel_path BLOB NOT NULL,
                          inode_id INTEGER NOT NULL REFERENCES inodes(id)
                            ON DELETE RESTRICT,
                          PRIMARY KEY (root_id, rel_path)) WITHOUT ROWID;
CREATE INDEX paths_by_inode ON paths (inode_id);
CREATE TABLE blocks      (inode_id INTEGER NOT NULL REFERENCES inodes(id)
                            ON DELETE RESTRICT,
                          block_no INTEGER NOT NULL, weak INTEGER NOT NULL,
                          strong BLOB NOT NULL CHECK (length(strong) = 16),
                          PRIMARY KEY (inode_id, block_no)) WITHOUT ROWID;
";

/// Children before parents, as `ON DELETE RESTRICT` requires.
const DROP_SCHEMA: &str = "
DROP TABLE IF EXISTS blocks;
DROP TABLE IF EXISTS paths;
DROP TABLE IF EXISTS inodes;
DROP TABLE IF EXISTS roots;
DROP TABLE IF EXISTS filesystems;
DROP TABLE IF EXISTS meta;
";

/// Created after population; repaired by every update.
const DEFERRED_BLOCK_INDEXES: &str = "
CREATE INDEX IF NOT EXISTS blocks_by_weak   ON blocks (weak);
CREATE INDEX IF NOT EXISTS blocks_by_strong ON blocks (strong);
";

/// Unwrap a database result. Database errors are fatal bugs or failures
/// and abort (DESIGN.md §6.5).
pub fn db<T>(result: rusqlite::Result<T>) -> T {
    result.unwrap_or_else(|e| panic!("database error: {e}"))
}

/// The settings that must match between the index and the configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub block_size: u32,
    pub weak_hash_id: String,
    pub strong_hash_id: String,
}

impl Settings {
    pub fn from_config(config: &Config) -> Self {
        Settings {
            block_size: config.block_size,
            weak_hash_id: weak::ID.to_string(),
            strong_hash_id: config.strong_hash.id().to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Meta {
    pub schema_version: i64,
    pub settings: Settings,
    pub index_id: [u8; 16],
}

impl Meta {
    pub fn compatible(&self, settings: &Settings) -> bool {
        self.schema_version == SCHEMA_VERSION && self.settings == *settings
    }
}

pub struct Stats {
    pub filesystems: u64,
    pub roots: u64,
    pub roots_complete: u64,
    pub inodes: u64,
    pub inodes_incomplete: u64,
    pub paths: u64,
    pub blocks: u64,
    pub distinct_weak: u64,
    pub indexed_bytes: u64,
    pub deferred_block_indexes: bool,
}

/// An open index connection: the writer of a modifying process, or the
/// keeper of a read-only process.
pub struct Index {
    conn: Connection,
}

/// Busy handler: wait, then retry, forever.
fn wait_forever(_attempts: i32) -> bool {
    thread::sleep(RETRY_INTERVAL);
    true
}

const RETRY_INTERVAL: Duration = Duration::from_millis(20);

fn is_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(failure.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

fn os_path(path: &[u8]) -> &Path {
    Path::new(OsStr::from_bytes(path))
}

/// A real schema read; acquires the process lock of `unix-excl`, or the
/// shared lock of a read-only keeper.
fn schema_read(conn: &Connection) -> rusqlite::Result<()> {
    conn.query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(()))
}

/// Open one connection of a modifying process: read/write through
/// `unix-excl`, waiting forever on contention, like every connection of
/// that process must (DESIGN.md §4.4). The writer and its worker readers
/// share this one recipe: a change to the VFS or the flags here changes
/// the writer, whose exclusion the locking tests verify.
fn open_unix_excl(path: &[u8], extra: OpenFlags) -> rusqlite::Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX | extra;
    let conn = Connection::open_with_flags_and_vfs(os_path(path), flags, "unix-excl")?;
    conn.busy_handler(Some(wait_forever))?;
    Ok(conn)
}

/// Open the writer of a modifying process, waiting for exclusive access.
pub fn open_modifying(path: &[u8]) -> Index {
    if let Some(parent) = os_path(path).parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        die(
            EXIT_FAILURE,
            format!("cannot create index directory {}: {e}", parent.display()),
        );
    }
    loop {
        let conn = match open_unix_excl(path, OpenFlags::SQLITE_OPEN_CREATE) {
            Ok(conn) => conn,
            Err(e) => die(
                EXIT_FAILURE,
                format!("cannot open index {}: {e}", path.as_bstr()),
            ),
        };
        match schema_read(&conn) {
            Ok(()) => {
                db(conn.pragma_update(None, "foreign_keys", "ON"));
                let mode: String =
                    db(conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0)));
                assert_eq!(mode, "wal", "cannot enable WAL mode");
                db(conn.pragma_update(None, "synchronous", "NORMAL"));
                return Index { conn };
            }
            // Busy without the handler running: retry with a fresh connection.
            Err(e) if is_busy(&e) => {
                drop(conn);
                thread::sleep(RETRY_INTERVAL);
            }
            Err(e) => db(Err(e)),
        }
    }
}

/// Open a worker reader of a modifying process (DESIGN.md §4.4): like
/// every connection of that process, read/write through `unix-excl`, with
/// `query_only` set before use. The writer already holds the process
/// lock. Queries run in autocommit, so a reader never pins a WAL snapshot.
pub fn open_worker_reader(path: &[u8]) -> Reader {
    let conn = db(open_unix_excl(path, OpenFlags::empty()));
    db(conn.pragma_update(None, "foreign_keys", "ON"));
    db(conn.pragma_update(None, "query_only", "ON"));
    Reader { conn }
}

/// A scan worker's read-only view of the index during an update.
pub struct Reader {
    conn: Connection,
}

impl Reader {
    /// The ID of the complete inode row that stores this identity with
    /// the observed size and ctime, so the file's content can be reused
    /// without hashing (DESIGN.md §4.2). The comparison uses the stored
    /// `i64` bit pattern of the unsigned size; the equality also rejects
    /// incomplete rows, whose size is NULL.
    pub fn reusable_inode(
        &self,
        fsid: &[u8],
        handle: &[u8],
        size: u64,
        ctime_ns: i64,
    ) -> Option<i64> {
        let mut stmt = db(self.conn.prepare_cached(
            "SELECT inodes.id FROM inodes
             JOIN filesystems ON filesystems.id = inodes.fs_id
             WHERE filesystems.fsid = ? AND inodes.handle = ?
               AND inodes.size = ? AND inodes.ctime_ns = ?",
        ));
        db(stmt
            .query_row(params![fsid, handle, size as i64, ctime_ns], |row| {
                row.get(0)
            })
            .optional())
    }
}

/// Open the keeper connection of a read-only process. Its read
/// transaction and schema read last until the `Index` is dropped. `None`
/// when the index file does not exist.
pub fn open_read_only(path: &[u8]) -> Option<Index> {
    loop {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = match Connection::open_with_flags(os_path(path), flags) {
            Ok(conn) => conn,
            Err(e) => {
                if !os_path(path).exists() {
                    return None;
                }
                die(
                    EXIT_FAILURE,
                    format!("cannot open index {}: {e}", path.as_bstr()),
                )
            }
        };
        db(conn.busy_handler(Some(wait_forever)));
        match conn
            .execute_batch("BEGIN")
            .and_then(|()| schema_read(&conn))
        {
            Ok(()) => return Some(Index { conn }),
            Err(e) if is_busy(&e) => {
                drop(conn);
                thread::sleep(RETRY_INTERVAL);
            }
            Err(e) => db(Err(e)),
        }
    }
}

impl Index {
    /// The stored metadata, or `None` before the schema exists.
    pub fn read_meta(&self) -> Option<Meta> {
        let tables: i64 = db(self.conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'meta'",
            [],
            |row| row.get(0),
        ));
        if tables == 0 {
            return None;
        }
        let mut stmt = db(self.conn.prepare("SELECT key, value FROM meta"));
        let rows = db(stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Value>(1)?))
        }));
        let mut meta = Meta {
            schema_version: -1,
            settings: Settings {
                block_size: 0,
                weak_hash_id: String::new(),
                strong_hash_id: String::new(),
            },
            index_id: [0; 16],
        };
        for row in rows {
            let (key, value) = db(row);
            match (key.as_str(), value) {
                ("schema_version", Value::Integer(v)) => meta.schema_version = v,
                ("block_size", Value::Integer(v)) => {
                    meta.settings.block_size = u32::try_from(v).unwrap_or(0);
                }
                ("weak_hash_id", Value::Text(v)) => meta.settings.weak_hash_id = v,
                ("strong_hash_id", Value::Text(v)) => meta.settings.strong_hash_id = v,
                ("index_id", Value::Blob(v)) if v.len() == 16 => {
                    meta.index_id.copy_from_slice(&v);
                }
                _ => {}
            }
        }
        Some(meta)
    }

    /// Reset schema and metadata in one transaction, with a new index ID.
    /// Deferred block indexes are created after population by
    /// `ensure_deferred_block_indexes`.
    pub fn reset(&self, settings: &Settings) {
        let mut index_id = [0u8; 16];
        rustix::rand::getrandom(&mut index_id, rustix::rand::GetRandomFlags::empty())
            .unwrap_or_else(|e| panic!("getrandom: {e}"));
        db(self
            .conn
            .execute_batch(&format!("BEGIN IMMEDIATE;{DROP_SCHEMA}{SCHEMA}")));
        let mut insert = db(self
            .conn
            .prepare("INSERT INTO meta (key, value) VALUES (?, ?)"));
        db(insert.execute(params!["schema_version", SCHEMA_VERSION]));
        db(insert.execute(params!["block_size", i64::from(settings.block_size)]));
        db(insert.execute(params!["weak_hash_id", settings.weak_hash_id]));
        db(insert.execute(params!["strong_hash_id", settings.strong_hash_id]));
        db(insert.execute(params!["index_id", &index_id[..]]));
        drop(insert);
        db(self.conn.execute_batch("COMMIT"));
    }

    pub fn ensure_deferred_block_indexes(&self) {
        db(self.conn.execute_batch(DEFERRED_BLOCK_INDEXES));
    }

    /// Reconcile roots before scanning (DESIGN.md §4.2): remove paths and
    /// rows of unfinished roots and of incomplete inodes, then create rows
    /// for the roots to scan. Returns their IDs in order.
    pub fn reconcile_roots(&self, paths: &[&[u8]]) -> Vec<i64> {
        db(self.conn.execute_batch(
            "BEGIN IMMEDIATE;
             DELETE FROM paths WHERE root_id IN (SELECT id FROM roots WHERE identifier IS NULL);
             DELETE FROM roots WHERE identifier IS NULL;
             DELETE FROM paths WHERE inode_id IN (SELECT id FROM inodes WHERE size IS NULL);
             DELETE FROM blocks WHERE inode_id IN (SELECT id FROM inodes WHERE size IS NULL);
             DELETE FROM inodes WHERE size IS NULL;",
        ));
        let mut insert = db(self
            .conn
            .prepare("INSERT INTO roots (identifier, path) VALUES (NULL, ?)"));
        let ids = paths
            .iter()
            .map(|path| {
                db(insert.execute(params![*path]));
                self.conn.last_insert_rowid()
            })
            .collect();
        drop(insert);
        db(self.conn.execute_batch("COMMIT"));
        ids
    }

    pub fn stats(&self) -> Stats {
        let count = |sql: &str| -> u64 {
            let n: i64 = db(self.conn.query_row(sql, [], |row| row.get(0)));
            n as u64
        };
        let indexed_bytes: i64 = db(self.conn.query_row(
            "SELECT coalesce(sum(size), 0) FROM inodes WHERE size IS NOT NULL",
            [],
            |row| row.get(0),
        ));
        Stats {
            filesystems: count("SELECT count(*) FROM filesystems"),
            roots: count("SELECT count(*) FROM roots"),
            roots_complete: count("SELECT count(identifier) FROM roots"),
            inodes: count("SELECT count(*) FROM inodes"),
            inodes_incomplete: count("SELECT count(*) FROM inodes WHERE size IS NULL"),
            paths: count("SELECT count(*) FROM paths"),
            blocks: count("SELECT count(*) FROM blocks"),
            distinct_weak: count("SELECT count(DISTINCT weak) FROM blocks"),
            indexed_bytes: indexed_bytes as u64,
            deferred_block_indexes: count(
                "SELECT count(*) FROM sqlite_master WHERE type = 'index'
                 AND name IN ('blocks_by_weak', 'blocks_by_strong')",
            ) == 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStrExt;
    use std::process::Command;

    use super::*;

    fn settings() -> Settings {
        Settings {
            block_size: 4096,
            weak_hash_id: "test".into(),
            strong_hash_id: "test".into(),
        }
    }

    /// The exit code of a completed probe. The probe process exits with
    /// it because a plain successful exit proves nothing: a filter that
    /// selects no test also exits zero.
    const PROBE_RAN: i32 = 42;

    /// Assert, from a separate process, that the index at `path` is
    /// locked (`busy`) or readable (`open`). The probe makes the first
    /// attempt of a read-only process, without the retry loop: this test
    /// binary runs `lock_probe_child` selected by the environment.
    fn probe(path: &Path, expect: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "index::tests::lock_probe_child", "--ignored"])
            .env("HASHSYNC_TEST_LOCK_DB", path)
            .env("HASHSYNC_TEST_LOCK_EXPECT", expect)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(PROBE_RAN),
            "the probe did not run, or did not find the index {expect}:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Not a test of its own: the separate process that
    /// `exclusion_survives_worker_readers` starts through `probe`. On the
    /// expected outcome it exits the process with `PROBE_RAN` instead of
    /// returning, which tells the parent that the probe really ran.
    #[test]
    #[ignore = "helper process of exclusion_survives_worker_readers"]
    fn lock_probe_child() {
        let Some(path) = std::env::var_os("HASHSYNC_TEST_LOCK_DB") else {
            return;
        };
        let expect = std::env::var("HASHSYNC_TEST_LOCK_EXPECT").unwrap();
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = Connection::open_with_flags(Path::new(&path), flags).unwrap();
        // The bundled SQLite installs a default busy timeout on every new
        // connection; remove it so contention reports busy at once.
        conn.busy_handler(None).unwrap();
        let result = conn
            .execute_batch("BEGIN")
            .and_then(|()| schema_read(&conn));
        match (expect.as_str(), result) {
            ("busy", Err(e)) if is_busy(&e) => {}
            ("open", Ok(())) => {}
            (expect, result) => panic!("expected {expect}, got {result:?}"),
        }
        drop(conn);
        std::process::exit(PROBE_RAN);
    }

    /// Process exclusion must survive the worker readers of a modifying
    /// process (DESIGN.md §4.4): opened by the real helpers, they must
    /// not downgrade the process lock, and closing them before the
    /// writer must leave no gap in exclusion. Every probe is a separate
    /// process (DESIGN.md §11).
    #[test]
    fn exclusion_survives_worker_readers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.sqlite");
        let path_bytes = path.as_os_str().as_bytes().to_vec();

        let index = open_modifying(&path_bytes);
        index.reset(&settings());
        let reader = open_worker_reader(&path_bytes);
        assert_eq!(reader.reusable_inode(b"\x01", b"\x02", 4096, 123), None);

        // The connections of this process share the heap WAL index of
        // `unix-excl`; a writer through the normal VFS would create the
        // shared-memory file instead. Readers attach to the index the
        // writer created, so this observes the shared `open_unix_excl`
        // recipe through the writer.
        assert!(
            dir.path().join("index.sqlite-wal").exists(),
            "the index is not in WAL mode"
        );
        assert!(
            !dir.path().join("index.sqlite-shm").exists(),
            "the writer bypassed the unix-excl VFS"
        );
        probe(&path, "busy");

        drop(reader);
        probe(&path, "busy");

        drop(index);
        probe(&path, "open");
    }

    /// A worker reader opens after the writer holds the process lock and
    /// queries in autocommit: each query must observe the writer's latest
    /// commit through the WAL that the `unix-excl` connections share
    /// (DESIGN.md §4.4).
    #[test]
    fn worker_readers_see_later_commits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.sqlite");
        let path_bytes = path.as_os_str().as_bytes().to_vec();

        let index = open_modifying(&path_bytes);
        index.reset(&settings());
        let reader = open_worker_reader(&path_bytes);
        assert_eq!(reader.reusable_inode(b"\x01", b"\x02", 4096, 123), None);

        // Committed after the reader's first query: the reader must not
        // stay on its earlier snapshot.
        db(index.conn.execute_batch(
            "BEGIN IMMEDIATE;
             INSERT INTO filesystems (id, fsid) VALUES (1, x'01');
             INSERT INTO inodes (id, fs_id, handle, size, ctime_ns)
             VALUES (7, 1, x'02', 4096, 123);
             INSERT INTO inodes (id, fs_id, handle, size, ctime_ns)
             VALUES (8, 1, x'03', NULL, 123);
             COMMIT;",
        ));
        assert_eq!(
            reader.reusable_inode(b"\x01", b"\x02", 4096, 123),
            Some(7),
            "the reader must see the commit"
        );
        assert_eq!(reader.reusable_inode(b"\x01", b"\x02", 4096, 124), None);
        assert_eq!(reader.reusable_inode(b"\x01", b"\x02", 4095, 123), None);
        assert_eq!(
            reader.reusable_inode(b"\x01", b"\x03", 4096, 123),
            None,
            "an incomplete row is not reusable"
        );
    }
}
