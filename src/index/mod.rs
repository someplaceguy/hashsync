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
use crate::die::die;
use crate::fsops;
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
    /// The database path, for the other connections of this process.
    path: Vec<u8>,
    /// This process modifies the index, which decides how its other
    /// connections open (DESIGN.md §4.4).
    modifying: bool,
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
        && let Err(e) = fsops::create_dir_all(parent, 0o777)
    {
        die(format!(
            "cannot create index directory {}: {e}",
            parent.display()
        ));
    }
    loop {
        let conn = match open_unix_excl(path, OpenFlags::SQLITE_OPEN_CREATE) {
            Ok(conn) => conn,
            Err(e) => die(format!("cannot open index {}: {e}", path.as_bstr())),
        };
        match schema_read(&conn) {
            Ok(()) => {
                db(conn.pragma_update(None, "foreign_keys", "ON"));
                let mode: String =
                    db(conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0)));
                assert_eq!(mode, "wal", "cannot enable WAL mode");
                db(conn.pragma_update(None, "synchronous", "NORMAL"));
                // A negative cache_size specifies KiB.
                db(conn.pragma_update(None, "cache_size", -64 * 1024));
                db(conn.pragma_update(None, "threads", 8));
                return Index {
                    conn,
                    path: path.to_vec(),
                    modifying: true,
                };
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

/// Open a reader of a read-only process: an ordinary read-only
/// connection, which the keeper's transaction protects from writers
/// (DESIGN.md §4.4).
fn open_plain_reader(path: &[u8]) -> Reader {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = db(Connection::open_with_flags(os_path(path), flags));
    db(conn.busy_handler(Some(wait_forever)));
    Reader { conn }
}

/// A read-only view of the index for one worker thread: a scan worker
/// during an update, or a lookup worker during a transfer. Queries run in
/// autocommit, so a reader never pins a WAL snapshot.
pub struct Reader {
    conn: Connection,
}

/// The candidates of the blocks with one strong-hash prefix.
const CANDIDATES: &str = "
SELECT blocks.block_no, paths.root_id, paths.rel_path FROM blocks
LEFT JOIN paths ON paths.inode_id = blocks.inode_id
WHERE blocks.strong = ?
ORDER BY blocks.inode_id, blocks.block_no";

/// A candidate (DESIGN.md §6.4): an indexed block with one path of its
/// inode.
pub struct Candidate {
    pub block_no: u64,
    /// The `roots.id` and relative path of one path of the block's inode;
    /// `None` for an inode without paths.
    pub path: Option<(i64, Vec<u8>)>,
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

    /// At most `limit` candidates of the blocks whose strong-hash prefix is
    /// `prefix`: one for every path of each block's inode, and one for each
    /// block whose inode has no path. Any order would do; this one is
    /// fixed, by inode row, then by block number, so that the same index
    /// always gives the same candidates.
    ///
    /// The rows come from `blocks_by_strong` in that order without a sort,
    /// so reading stops after `limit` rows. A `LIMIT ?` parameter would do
    /// the same, but SQLite prepares a statement again whenever such a
    /// parameter changes.
    pub fn candidates(&self, prefix: &[u8], limit: u32) -> Vec<Candidate> {
        let mut stmt = db(self.conn.prepare_cached(CANDIDATES));
        let rows = db(stmt.query_map(params![prefix], |row| {
            let root_id: Option<i64> = row.get(1)?;
            Ok(Candidate {
                block_no: row.get::<_, i64>(0)? as u64,
                path: match root_id {
                    Some(root_id) => Some((root_id, row.get(2)?)),
                    None => None,
                },
            })
        }));
        rows.take(limit as usize).map(db).collect()
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
                // Not `Path::exists`: it reports an interrupted check as
                // a missing index.
                match fsops::retry_interrupted(|| os_path(path).try_exists()) {
                    Ok(false) => return None,
                    Ok(true) => die(format!("cannot open index {}: {e}", path.as_bstr())),
                    // SQLite's message omits the errno; the check's error
                    // names it.
                    Err(check) => die(format!(
                        "cannot open index {}: {e} ({check})",
                        path.as_bstr()
                    )),
                }
            }
        };
        db(conn.busy_handler(Some(wait_forever)));
        match conn
            .execute_batch("BEGIN")
            .and_then(|()| schema_read(&conn))
        {
            Ok(()) => {
                return Some(Index {
                    conn,
                    path: path.to_vec(),
                    modifying: false,
                });
            }
            Err(e) if is_busy(&e) => {
                drop(conn);
                thread::sleep(RETRY_INTERVAL);
            }
            Err(e) => db(Err(e)),
        }
    }
}

impl Index {
    /// A reader connection for another thread of this process, opened the
    /// way this process's access requires (DESIGN.md §4.4).
    pub fn reader(&self) -> Reader {
        if self.modifying {
            open_worker_reader(&self.path)
        } else {
            open_plain_reader(&self.path)
        }
    }

    /// Every root row: its ID and its representative path.
    pub fn root_paths(&self) -> Vec<(i64, Vec<u8>)> {
        let mut stmt = db(self.conn.prepare("SELECT id, path FROM roots"));
        let rows = db(stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?))));
        rows.map(db).collect()
    }

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
        rustix::io::retry_on_intr(|| {
            rustix::rand::getrandom(&mut index_id, rustix::rand::GetRandomFlags::empty())
        })
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

    /// True when both deferred block indexes exist. A read-only run needs
    /// `blocks_by_weak` for the export and requests an update otherwise
    /// (DESIGN.md §4.4).
    pub fn has_block_indexes(&self) -> bool {
        let count: i64 = db(self.conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'index'
             AND name IN ('blocks_by_weak', 'blocks_by_strong')",
            [],
            |row| row.get(0),
        ));
        count == 2
    }

    /// The distinct weak hashes, sorted as unsigned 64-bit values, from
    /// one database snapshot (DESIGN.md §5.4). Values are stored as `i64`
    /// bit patterns, so the non-negative rows are the low unsigned half
    /// and the negative rows the high half; both scans use
    /// `blocks_by_weak`.
    pub fn export_weak_hashes(&self) -> Vec<u64> {
        // A read-only keeper is already inside its long transaction;
        // give the autocommit writer a transaction of its own.
        let own_transaction = self.conn.is_autocommit();
        if own_transaction {
            db(self.conn.execute_batch("BEGIN"));
        }
        let mut values: Vec<u64> = Vec::new();
        for range in [
            "SELECT DISTINCT weak FROM blocks WHERE weak >= 0 ORDER BY weak",
            "SELECT DISTINCT weak FROM blocks WHERE weak < 0 ORDER BY weak",
        ] {
            let mut stmt = db(self.conn.prepare(range));
            let rows = db(stmt.query_map([], |row| row.get::<_, i64>(0)));
            for row in rows {
                values.push(db(row) as u64);
            }
        }
        if own_transaction {
            db(self.conn.execute_batch("COMMIT"));
        }
        values
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
            deferred_block_indexes: self.has_block_indexes(),
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

    /// The export sorts the stored `i64` bit patterns as unsigned values
    /// and deduplicates across inodes (DESIGN.md §5.4).
    #[test]
    fn weak_hashes_export_distinct_and_unsigned_sorted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.sqlite");
        let index = open_modifying(path.as_os_str().as_bytes());
        index.reset(&settings());
        assert_eq!(index.export_weak_hashes(), Vec::<u64>::new());
        db(index.conn.execute_batch(
            "BEGIN IMMEDIATE;
             INSERT INTO filesystems (id, fsid) VALUES (1, x'01');
             INSERT INTO inodes (id, fs_id, handle, size, ctime_ns)
             VALUES (1, 1, x'02', 4096, 1), (2, 1, x'03', 4096, 1);
             COMMIT;",
        ));
        let mut insert = db(index
            .conn
            .prepare("INSERT INTO blocks (inode_id, block_no, weak, strong) VALUES (?, ?, ?, ?)"));
        // 2^63 and u64::MAX are negative as i64 and sort after the small
        // values only in unsigned order; 7 repeats on another inode.
        let values: [(i64, u64); 5] = [
            (7i64, 7),
            (0, 0),
            (u64::MAX as i64, u64::MAX),
            ((1u64 << 63) as i64, 1 << 63),
            (i64::MAX, i64::MAX as u64),
        ];
        for (row, (stored, _)) in values.iter().enumerate() {
            db(insert.execute(params![1, row as i64, stored, &[0u8; 16][..]]));
        }
        db(insert.execute(params![2, 0, 7i64, &[0u8; 16][..]]));
        drop(insert);
        assert_eq!(
            index.export_weak_hashes(),
            vec![0, 7, i64::MAX as u64, 1 << 63, u64::MAX]
        );
    }

    /// Candidates follow the inode rows and block numbers, include blocks
    /// of inodes without paths, stop at the limit, and come from the
    /// `blocks_by_strong` index without a sort (DESIGN.md §4.4 and §6.4).
    #[test]
    fn candidates_are_ordered_and_limited() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.sqlite");
        let index = open_modifying(path.as_os_str().as_bytes());
        index.reset(&settings());
        index.ensure_deferred_block_indexes();
        db(index.conn.execute_batch(
            "BEGIN IMMEDIATE;
             INSERT INTO filesystems (id, fsid) VALUES (1, x'01');
             INSERT INTO roots (id, identifier, path) VALUES (1, NULL, x'2f72');
             INSERT INTO inodes (id, fs_id, handle, size, ctime_ns)
             VALUES (1, 1, x'01', 8192, 1), (2, 1, x'02', 8192, 1);
             INSERT INTO paths (root_id, rel_path, inode_id)
             VALUES (1, x'61', 2), (1, x'62', 2);
             INSERT INTO blocks (inode_id, block_no, weak, strong) VALUES
               (2, 1, 0, x'11111111111111111111111111111111'),
               (2, 0, 0, x'11111111111111111111111111111111'),
               (1, 5, 0, x'11111111111111111111111111111111'),
               (1, 6, 0, x'22222222222222222222222222222222');
             COMMIT;",
        ));
        let reader = index.reader();
        let prefix = [0x11u8; 16];
        let found: Vec<String> = reader
            .candidates(&prefix, 10)
            .into_iter()
            .map(|c| match c.path {
                Some((root, rel)) => format!("{} {root}/{}", c.block_no, rel.as_bstr()),
                None => format!("{} -", c.block_no),
            })
            .collect();
        assert_eq!(found, ["5 -", "0 1/a", "0 1/b", "1 1/a", "1 1/b"]);
        assert_eq!(reader.candidates(&prefix, 2).len(), 2);
        assert!(reader.candidates(&[0x33; 16], 10).is_empty());
        // Each lookup reuses the prepared statement.
        let stmt = reader.conn.prepare_cached(CANDIDATES).unwrap();
        assert_eq!(stmt.get_status(rusqlite::StatementStatus::RePrepare), 0);

        let plan: Vec<String> = {
            let mut stmt = index
                .conn
                .prepare(&format!("EXPLAIN QUERY PLAN {CANDIDATES}"))
                .unwrap();
            stmt.query_map(params![&prefix[..]], |row| row.get::<_, String>(3))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        assert!(
            plan.iter().any(|step| step.contains("blocks_by_strong")),
            "{plan:?}"
        );
        assert!(
            !plan.iter().any(|step| step.contains("TEMP B-TREE")),
            "{plan:?}"
        );
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
