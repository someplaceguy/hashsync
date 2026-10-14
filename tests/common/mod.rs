//! Shared helpers for integration tests: a temporary environment with a
//! configuration file, an index directory, and a tree to index; the
//! frames of the protocol; and the processes that the tests start.
#![allow(dead_code)]

use std::ffi::OsStr;
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::time::Duration;

use rocksdb::{ColumnFamily, DB, IteratorMode, Options, WriteBatch};
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
// Session frames: the data session, never compressed.
pub const REQUEST: u8 = 1;
pub const HELLO: u8 = 2;
pub const HASHLIST: u8 = 3;
pub const HASHDELTA: u8 = 4;
pub const CACHE_RESULT: u8 = 7;
pub const CHUNK: u8 = 8;
pub const STREAM_END: u8 = 9;
pub const END: u8 = 10;
// Stream frames: inside the zstd frame of a stream.
pub const LIT: u8 = 11;
pub const REF: u8 = 12;
pub const RLE: u8 = 13;
// Control frames.
pub const QUERY: u8 = 14;
pub const ANSWER: u8 = 15;
pub const STOP: u8 = 16;

/// The first bytes of each session, in both directions: the protocol
/// version, then `features`. hashsync sends `FEATURES`, but a later peer
/// can send bits that hashsync does not know.
pub fn version_and_features(features: u64) -> [u8; 12] {
    let mut bytes = [0; 12];
    bytes[..4].copy_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    bytes[4..].copy_from_slice(&features.to_le_bytes());
    bytes
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

/// Wait until the main thread of `pid` sleeps in a wait for the lock of an
/// index: `fcntl` with `F_OFD_SETLKW`, its second argument in the `syscall`
/// file (DESIGN.md §4.4); no deadline. Reading that file needs the ptrace
/// access that a parent has. Fail when the process has ended.
pub fn wait_for_lock_wait(pid: u32) {
    let path = format!("/proc/{pid}/syscall");
    loop {
        assert_ne!(
            proc_state(format!("/proc/{pid}")),
            Some('Z'),
            "the process ended"
        );
        let syscall = std::fs::read_to_string(&path).unwrap();
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
