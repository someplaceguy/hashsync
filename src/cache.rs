//! Account identity and the hash-list cache (DESIGN.md §5.6).
//!
//! Full lists are immutable and named by content; account and index IDs
//! partition the cache directories without identifying list content.
//! Publication is atomic and durable: a unique hidden temporary, file
//! fsync, rename to the content-derived name, and an fsync of every
//! directory on the path that this program can create (`sync_entries`).
//! Published files are never edited in place.
//!
//! Cache directory and file operation failures end the run with `die`.
//! The exceptions are absence — a missing list is a miss, and missing
//! directories or already-removed files during lookup and cleanup are
//! normal — and invalid file contents, which are removed and treated as
//! a miss (DESIGN.md §5.6).

use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use rustix::fs::{Mode, RenameFlags, Timespec, Timestamps, UTIME_NOW};
use rustix::io::{Errno, retry_on_intr};

use crate::config;
use crate::die::die;
use crate::fsops::retry_interrupted;
use crate::hashlist::ListId;

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    let mut filled = 0;
    // Reads up to 256 bytes are not split, but handle a short read anyway.
    while filled < N {
        let n = retry_on_intr(|| {
            rustix::rand::getrandom(&mut bytes[filled..], rustix::rand::GetRandomFlags::empty())
        })
        .unwrap_or_else(|e| panic!("getrandom: {e}"));
        filled += n;
    }
    bytes
}

/// A cache operation failed for a reason other than absence
/// (DESIGN.md §5.6 and §6.5).
fn fatal(action: &str, path: &Path, error: impl std::fmt::Display) -> ! {
    die(format!("cannot {action} {}: {error}", path.display()))
}

fn absent(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::NotFound
}

/// Remove a file; one that is already gone is normal absence.
fn remove(action: &str, path: &Path) {
    match retry_interrupted(|| std::fs::remove_file(path)) {
        Ok(()) => {}
        Err(e) if absent(&e) => {}
        Err(e) => fatal(action, path, e),
    }
}

fn os_path(bytes: &[u8]) -> PathBuf {
    PathBuf::from(OsStr::from_bytes(bytes))
}

/// `$XDG_CACHE_HOME` (fallback `~/.cache`): the base directory of the
/// cache tree.
fn cache_base() -> PathBuf {
    os_path(&config::xdg_dir("XDG_CACHE_HOME", ".cache"))
}

/// `$XDG_CACHE_HOME/hashsync`.
fn cache_root() -> PathBuf {
    cache_base().join("hashsync")
}

/// This UNIX account's persistent random 128-bit ID, in
/// `$XDG_CONFIG_HOME/hashsync/account-id` (fallback
/// `~/.config/hashsync/account-id`), independent of `--config`. The first
/// use initializes it with atomic no-replace publication; concurrent
/// initializers use the winner's ID. Every use makes it durable first.
/// Failures die: HELLO cannot be built without an identity.
pub fn account_id() -> [u8; 16] {
    let base = os_path(&config::xdg_dir("XDG_CONFIG_HOME", ".config"));
    let dir = base.join("hashsync");
    let path = dir.join("account-id");
    loop {
        match retry_interrupted(|| File::open(&path)) {
            Ok(file) => return use_account_id(&base, &path, file),
            Err(e) if absent(&e) => {}
            Err(e) => fatal("open", &path, e),
        }
        // Publish a fresh ID without replacing a concurrent initializer's;
        // the next iteration uses whichever ID won.
        create_dirs(&base, &dir, 0o777);
        let temp = dir.join(format!(".account-id.{}", hex(&random_bytes::<8>())));
        let written = write_file(&temp, 0o600, |file| {
            file.write_all(format!("{}\n", hex(&random_bytes::<16>())).as_bytes())
        });
        if let Err(e) = written {
            // Nothing expires this directory's temporaries, so try to
            // remove this one before the fatal exit.
            let _ = retry_interrupted(|| std::fs::remove_file(&temp));
            fatal("initialize", &path, e);
        }
        match retry_on_intr(|| {
            rustix::fs::renameat_with(
                rustix::fs::CWD,
                &temp,
                rustix::fs::CWD,
                &path,
                RenameFlags::NOREPLACE,
            )
        }) {
            Ok(()) => {}
            Err(Errno::EXIST) => remove("remove", &temp),
            Err(e) => fatal("publish", &path, e),
        }
    }
}

/// Read the ID from the open account-ID file, and make the ID durable
/// before it is used: the file and every entry on its path that this
/// program can create. That the file exists proves nothing about its
/// durability. Its creator may not have reached its own fsync yet, or may
/// have been killed before it (DESIGN.md §5.6).
fn use_account_id(base: &Path, path: &Path, mut file: File) -> [u8; 16] {
    let mut text = Vec::new();
    if let Err(e) = file.read_to_end(&mut text) {
        fatal("read", path, e);
    }
    let Some(id) = parse_account_id(&text) else {
        fatal(
            "use",
            path,
            "malformed content; remove the file to renew the identity",
        )
    };
    if let Err(e) = retry_interrupted(|| file.sync_all()) {
        fatal("fsync", path, e);
    }
    sync_entries(base, path);
    id
}

/// 32 hexadecimal digits, with optional trailing whitespace.
fn parse_account_id(text: &[u8]) -> Option<[u8; 16]> {
    let digits = text.trim_ascii_end();
    if digits.len() != 32 {
        return None;
    }
    let mut id = [0u8; 16];
    for (byte, pair) in id.iter_mut().zip(digits.chunks_exact(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(id)
}

/// Create `path` exclusively with `mode`, fill it, and fsync it.
fn write_file(
    path: &Path,
    mode: u32,
    fill: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> std::io::Result<File> {
    let mut file = retry_interrupted(|| {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(path)
    })?;
    fill(&mut file)?;
    retry_interrupted(|| file.sync_all())?;
    Ok(file)
}

/// Create `dir` with `mode`, together with the missing directories from
/// `base` down to it. `base` is an XDG base directory, such as `~/.cache`,
/// and `dir` is `base` or below it. The parent of `base` must exist: this
/// program never creates anything above a base directory, so the entries
/// that `sync_entries` must make durable start in that parent.
fn create_dirs(base: &Path, dir: &Path, mode: u32) {
    for dir in descend(base, dir) {
        match retry_on_intr(|| rustix::fs::mkdir(&dir, Mode::from_raw_mode(mode))) {
            // A file in the way fails the next step with `ENOTDIR`.
            Ok(()) | Err(Errno::EXIST) => {}
            Err(e) => fatal("create", &dir, e),
        }
    }
}

/// Make durable the entry of `path` and every entry above it that this
/// program can have created: fsync each directory from the parent of
/// `base` down to the parent of `path` (DESIGN.md §5.6). Failure would
/// acknowledge durability that does not exist, so it is fatal.
///
/// Every process that relies on `path` must do this itself. An entry that
/// exists is not necessarily durable: the process that created it may not
/// have reached its own fsync yet, or may have been killed before it.
fn sync_entries(base: &Path, path: &Path) {
    match base.parent() {
        // The first component of a relative path.
        Some(parent) if parent.as_os_str().is_empty() => fsync_dir(Path::new(".")),
        Some(parent) => fsync_dir(parent),
        // `/` has no entry to make durable.
        None => {}
    }
    for dir in descend(base, path.parent().unwrap()) {
        fsync_dir(&dir);
    }
}

/// `base`, then each directory below it down to `dir`.
fn descend(base: &Path, dir: &Path) -> Vec<PathBuf> {
    let below = dir
        .strip_prefix(base)
        .unwrap_or_else(|_| panic!("{} is not below {}", dir.display(), base.display()));
    let mut dirs = vec![base.to_path_buf()];
    for component in below.components() {
        dirs.push(dirs.last().unwrap().join(component));
    }
    dirs
}

fn fsync_dir(dir: &Path) {
    let file =
        retry_interrupted(|| File::open(dir)).unwrap_or_else(|e| fatal("open for fsync", dir, e));
    if let Err(e) = retry_interrupted(|| file.sync_all()) {
        fatal("fsync", dir, e);
    }
}

/// Give a complete, fsynced temporary its published name. A concurrent
/// cleanup (`sender_cache = false` or expiry) can remove the temporary
/// first. That is normal absence: the effect is the same as removal of
/// the published file just after the rename (DESIGN.md §5.6).
fn rename_temporary(temp: &Path, path: &Path) {
    match retry_interrupted(|| std::fs::rename(temp, path)) {
        Ok(()) => {}
        Err(e) if absent(&e) => {}
        Err(e) => fatal("publish", path, e),
    }
}

/// The versioned wrapper of a stored list (DESIGN.md §5.6): the magic
/// `hsynlist`, then format 1. The list bytes fill the rest of the file,
/// and the file name holds the list ID. The directory's index fixes the
/// block size, weak hash, and strong hash, so the file stores none of
/// them. Only the canonical list bytes are hashed, never the wrapper.
const WRAPPER: &[u8; 9] = b"hsynlist\x01";

/// A durably published list. The open descriptor keeps the verified
/// bytes reachable even if the file is replaced or removed.
pub struct PublishedList {
    file: File,
    path: PathBuf,
}

impl PublishedList {
    /// Renew after successful use, through the open descriptor; `ctime`
    /// is the renewal marker (DESIGN.md §5.6).
    pub fn renew(&self) {
        let now = Timespec {
            tv_sec: 0,
            tv_nsec: UTIME_NOW,
        };
        let times = Timestamps {
            last_access: now,
            last_modification: now,
        };
        if let Err(e) = retry_on_intr(|| rustix::fs::futimens(&self.file, &times)) {
            fatal("renew", &self.path, e);
        }
    }
}

/// One peer/index cache directory of hash lists.
pub struct ListStore {
    /// `$XDG_CACHE_HOME`, where `publish` starts to create directories.
    base: PathBuf,
    /// `base/hashsync/<tree>/<peer>-<index>`.
    dir: PathBuf,
}

impl ListStore {
    /// The receiver's directory for lists shared with sender account
    /// `peer`: `local-hashlists/B-I`.
    pub fn receiver_side(peer: &[u8; 16], index_id: &[u8; 16]) -> Self {
        Self::new("local-hashlists", peer, index_id)
    }

    /// The sender's directory for lists of receiver account `peer`:
    /// `remote-hashlists/A-I`.
    pub fn sender_side(peer: &[u8; 16], index_id: &[u8; 16]) -> Self {
        Self::new("remote-hashlists", peer, index_id)
    }

    fn new(tree: &str, peer: &[u8; 16], index_id: &[u8; 16]) -> Self {
        let base = cache_base();
        let dir = base
            .join("hashsync")
            .join(tree)
            .join(format!("{}-{}", hex(peer), hex(index_id)));
        ListStore { base, dir }
    }

    fn file_name(id: &ListId) -> String {
        format!("{id}.cache")
    }

    /// Durably publish `encoded` under its content-derived name.
    /// Republishes an existing name rather than trusting it. Failures
    /// are fatal; a failure can leave the hidden temporary for expiry
    /// (DESIGN.md §6.7).
    pub fn publish(&self, id: &ListId, encoded: &[u8]) -> PublishedList {
        create_dirs(&self.base, &self.dir, 0o700);
        let temp = self.dir.join(format!(".{}", hex(&random_bytes::<8>())));
        let file = write_file(&temp, 0o600, |file| {
            file.write_all(WRAPPER)?;
            file.write_all(encoded)
        })
        .unwrap_or_else(|e| fatal("write", &temp, e));
        let path = self.dir.join(Self::file_name(id));
        rename_temporary(&temp, &path);
        sync_entries(&self.base, &path);
        PublishedList { file, path }
    }

    /// Read and verify a published list: the wrapper must be complete
    /// and the bytes must match the ID's digest. An absent file is a
    /// miss; an invalid file is removed and is a miss; a read failure is
    /// fatal (DESIGN.md §5.6). Cached-list reuse arrives with PLAN.md
    /// step 8; until then only tests verify the stored format through
    /// this.
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn read(&self, id: &ListId) -> Option<Vec<u8>> {
        let path = self.dir.join(Self::file_name(id));
        let bytes = match retry_interrupted(|| std::fs::read(&path)) {
            Ok(bytes) => bytes,
            Err(e) if absent(&e) => return None,
            Err(e) => fatal("read", &path, e),
        };
        if let Some(list) = parse_list(&bytes, id) {
            return Some(list);
        }
        // Unexpected contents are a miss: remove the file and continue
        // without it.
        remove("remove the invalid list", &path);
        None
    }

    /// Remove every stored list in this directory, for
    /// `sender_cache = false`. A directory that never held lists is
    /// normal absence.
    pub fn remove_all(&self) {
        let listed = for_each_entry(&self.dir, |entry| remove("remove", &entry.path()));
        match listed {
            Ok(()) => {}
            Err(e) if absent(&e) => {}
            Err(e) => fatal("list", &self.dir, e),
        }
    }
}

/// The list bytes of a stored file, verified against `id`, the ID in its
/// file name. `None` for any deviation: reading invalid contents is a
/// miss, not a failure.
fn parse_list(bytes: &[u8], id: &ListId) -> Option<Vec<u8>> {
    let encoded = bytes.strip_prefix(WRAPPER)?;
    (id.algorithm.digest(encoded) == id.digest).then(|| encoded.to_vec())
}

/// Iterate a directory's entries, retrying interrupted reads; any other
/// listing error ends the iteration.
fn for_each_entry(dir: &Path, mut each: impl FnMut(std::fs::DirEntry)) -> std::io::Result<()> {
    let mut entries = retry_interrupted(|| std::fs::read_dir(dir))?;
    loop {
        match entries.next() {
            None => return Ok(()),
            Some(Ok(entry)) => each(entry),
            Some(Err(e)) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Some(Err(e)) => return Err(e),
        }
    }
}

/// Expire files older than `max_age` seconds in both local and remote
/// cache trees, including abandoned hidden temporaries. Run after the
/// exchange. Absence is normal; other failures are fatal.
pub fn expire(max_age: u64) {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs().min(i64::MAX as u64) as i64);
    for tree in ["local-hashlists", "remote-hashlists"] {
        expire_tree(&cache_root().join(tree), now, max_age);
    }
}

fn expire_tree(tree: &Path, now: i64, max_age: u64) {
    let listed = for_each_entry(tree, |peer| {
        // Only the peer directories belong to the cache layout; skip
        // anything else silently.
        match retry_interrupted(|| peer.metadata()) {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => return,
            Err(e) if absent(&e) => return,
            Err(e) => fatal("stat", &peer.path(), e),
        }
        let listed = for_each_entry(&peer.path(), |file| {
            // `DirEntry::metadata` does not follow symlinks.
            let meta = match retry_interrupted(|| file.metadata()) {
                Ok(meta) => meta,
                Err(e) if absent(&e) => return,
                Err(e) => fatal("stat", &file.path(), e),
            };
            if !meta.is_file() {
                return;
            }
            let age = now.saturating_sub(meta.ctime());
            if age > i64::try_from(max_age).unwrap_or(i64::MAX) {
                remove("expire", &file.path());
            }
        });
        match listed {
            Ok(()) => {}
            Err(e) if absent(&e) => {}
            Err(e) => fatal("list", &peer.path(), e),
        }
    });
    match listed {
        Ok(()) => {}
        Err(e) if absent(&e) => {}
        Err(e) => fatal("list", tree, e),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Mutex;

    use super::*;
    use crate::hash::StrongHash;

    /// Serializes tests that change `XDG_*`; the variables are process
    /// state.
    static ENV: Mutex<()> = Mutex::new(());

    fn with_home<T>(test: impl FnOnce(&Path) -> T) -> T {
        let _guard = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        crate::sys::set_var_for_tests("XDG_CONFIG_HOME", dir.path().join("config").as_os_str());
        crate::sys::set_var_for_tests("XDG_CACHE_HOME", dir.path().join("cache").as_os_str());
        test(dir.path())
    }

    #[test]
    fn account_id_is_created_once_and_reread() {
        with_home(|home| {
            let id = account_id();
            let stored = std::fs::read(home.join("config/hashsync/account-id")).unwrap();
            assert_eq!(stored, format!("{}\n", hex(&id)).into_bytes());
            assert_eq!(account_id(), id, "a second read must return the same ID");
            // No abandoned temporary is left behind.
            let names: Vec<_> = std::fs::read_dir(home.join("config/hashsync"))
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            assert_eq!(names, vec![std::ffi::OsString::from("account-id")]);
        });
    }

    #[test]
    fn account_ids_parse_strictly() {
        assert!(parse_account_id(b"0123456789abcdef0123456789abcdef\n").is_some());
        assert!(parse_account_id(b"0123456789ABCDEF0123456789ABCDEF").is_some());
        assert!(parse_account_id(b"0123456789abcdef0123456789abcde\n").is_none());
        assert!(parse_account_id(b"0123456789abcdef0123456789abcdeg\n").is_none());
        assert!(parse_account_id(b"").is_none());
    }

    fn list_id(digest_byte: u8) -> ListId {
        let mut digest = [0u8; 32];
        digest[0] = digest_byte;
        ListId {
            algorithm: StrongHash::Blake3,
            digest,
        }
    }

    #[test]
    fn published_lists_verify_and_reread() {
        with_home(|home| {
            let store = ListStore::receiver_side(&[0xaa; 16], &[0xbb; 16]);
            let encoded = crate::hashlist::encode(&[1, 2, 3]);
            let id = ListId {
                algorithm: StrongHash::Blake3,
                digest: StrongHash::Blake3.digest(&encoded),
            };
            let published = store.publish(&id, &encoded);
            published.renew();

            let dir = home.join(format!(
                "cache/hashsync/local-hashlists/{}-{}",
                hex(&[0xaa; 16]),
                hex(&[0xbb; 16])
            ));
            let mode = std::fs::metadata(&dir).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700, "cache directories are private");
            let file = dir.join(format!("blake3-{}.cache", hex(&id.digest)));
            let mode = std::fs::metadata(&file).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "cache files are private");

            // The file is the wrapper, then the list bytes.
            assert_eq!(
                std::fs::read(&file).unwrap(),
                [&WRAPPER[..], &encoded].concat()
            );
            assert_eq!(store.read(&id).unwrap(), encoded);
            assert!(store.read(&list_id(9)).is_none(), "absence is a miss");

            // A damaged file is a miss and is removed (DESIGN.md §5.6).
            let mut bytes = std::fs::read(&file).unwrap();
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
            std::fs::write(&file, &bytes).unwrap();
            assert!(store.read(&id).is_none());
            assert!(!file.exists(), "the invalid list must be removed");
            assert!(store.read(&id).is_none(), "now an ordinary miss");
        });
    }

    #[test]
    fn remove_all_clears_only_this_directory() {
        with_home(|_| {
            let ours = ListStore::receiver_side(&[1; 16], &[2; 16]);
            let other = ListStore::receiver_side(&[3; 16], &[2; 16]);
            let encoded = crate::hashlist::encode(&[]);
            for store in [&ours, &other] {
                let id = ListId {
                    algorithm: StrongHash::Blake3,
                    digest: StrongHash::Blake3.digest(&encoded),
                };
                store.publish(&id, &encoded);
            }
            ours.remove_all();
            assert_eq!(std::fs::read_dir(&ours.dir).unwrap().count(), 0);
            assert_eq!(std::fs::read_dir(&other.dir).unwrap().count(), 1);
            // A directory that never held lists is fine.
            ListStore::receiver_side(&[9; 16], &[2; 16]).remove_all();
        });
    }

    #[test]
    fn a_temporary_removed_before_its_rename_is_normal_absence() {
        // A concurrent cleanup removes the temporary between its fsync and
        // its rename (DESIGN.md §5.6). Publication continues without the
        // file, as if the cleanup had removed it just after the rename.
        let dir = tempfile::tempdir().unwrap();
        let temp = dir.path().join(".temporary");
        let path = dir.path().join("blake3-list.cache");
        std::fs::write(&temp, b"list").unwrap();
        std::fs::remove_file(&temp).unwrap();
        rename_temporary(&temp, &path);
        assert!(!path.exists());

        // Without a concurrent cleanup, the rename publishes the file.
        std::fs::write(&temp, b"list").unwrap();
        rename_temporary(&temp, &path);
        assert_eq!(std::fs::read(&path).unwrap(), b"list");
        assert!(!temp.exists());
    }

    #[test]
    fn expiry_removes_old_files_and_keeps_fresh_ones() {
        with_home(|_| {
            let store = ListStore::sender_side(&[4; 16], &[5; 16]);
            let encoded = crate::hashlist::encode(&[7]);
            let id = ListId {
                algorithm: StrongHash::Blake3,
                digest: StrongHash::Blake3.digest(&encoded),
            };
            store.publish(&id, &encoded);
            std::fs::write(store.dir.join(".abandoned-temp"), b"x").unwrap();

            // A fresh file survives any positive age bound, and `ctime`
            // cannot be faked: test the boundary with clocks set from the
            // ctimes of the files instead. The current time would count a
            // stall of the test as age.
            let ctimes: Vec<i64> = std::fs::read_dir(&store.dir)
                .unwrap()
                .map(|entry| entry.unwrap().metadata().unwrap().ctime())
                .collect();
            let first = *ctimes.iter().min().unwrap();
            let last = *ctimes.iter().max().unwrap();
            // When the first file was made, no file was older than an
            // hour.
            expire_tree(&cache_root().join("remote-hashlists"), first, 3600);
            assert_eq!(std::fs::read_dir(&store.dir).unwrap().count(), 2);

            // One hour after the last file was made, both the list and the
            // hidden temporary are older than a half-hour bound.
            expire_tree(&cache_root().join("remote-hashlists"), last + 3600, 1800);
            assert_eq!(std::fs::read_dir(&store.dir).unwrap().count(), 0);

            // A missing tree is normal absence.
            expire_tree(&cache_root().join("no-such-tree"), first, 0);
        });
    }
}
