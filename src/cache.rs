//! Account identity and the hash-list cache (DESIGN.md §5.6).
//!
//! Full lists are immutable and named by content; account and index IDs
//! partition the cache directories without identifying list content.
//! Publication is atomic and durable: a unique hidden temporary, file
//! fsync, rename to the content-derived name, and an fsync of every
//! directory on the path that this program can create (`sync_entries`).
//! Published files are never edited in place.
//!
//! Each side handles its part of a list exchange through an `Exchange`:
//! it lists its directory for the peer and index when the exchange
//! starts, can publish its target, and at the end removes the listed
//! files except that target, expires the old files of every directory,
//! and removes the directories that are left empty. No locks are
//! necessary: publication creates its directory again when expiry removed
//! it, a listed list is removed only if its name still names the listed
//! file, and every reader of a list keeps the bytes that it read.
//!
//! Cache directory and file operation failures end the run with `die`.
//! The exceptions are absence — a missing list is a miss, and missing
//! directories or already-removed files during lookup and cleanup are
//! normal — invalid file contents, which are removed and treated as a
//! miss, and an empty directory that cannot be removed, which only warns
//! (DESIGN.md §5.6).

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use rustix::fs::{Mode, OFlags, RenameFlags, Timespec, Timestamps, UTIME_NOW};
use rustix::io::{Errno, retry_on_intr};

use crate::config;
use crate::die::{die, warn};
use crate::fsops::retry_interrupted;
use crate::hash::{Strong, StrongHash};
use crate::hashlist::ListId;
use crate::names::{hex, is_temporary, parse_hex, random_bytes, temporary_name};

/// A cache operation failed for a reason other than absence
/// (DESIGN.md §5.6 and §6.7).
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
        let temp = dir.join(temporary_name());
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
    if !sync_entries(base, path) {
        fatal("make durable", path, "a directory on its path is gone");
    }
    id
}

/// 32 hexadecimal digits, with optional trailing whitespace.
fn parse_account_id(text: &[u8]) -> Option<[u8; 16]> {
    parse_hex(text.trim_ascii_end())
}

/// Create `path` exclusively with `mode`.
fn create_new(path: &Path, mode: u32) -> std::io::Result<File> {
    retry_interrupted(|| {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(path)
    })
}

/// Create `path` exclusively with `mode`, fill it, and fsync it.
fn write_file(
    path: &Path,
    mode: u32,
    fill: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> std::io::Result<File> {
    let mut file = create_new(path, mode)?;
    fill_file(&mut file, fill)?;
    Ok(file)
}

/// Fill a new file, and fsync it.
fn fill_file(
    file: &mut File,
    fill: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    fill(file)?;
    retry_interrupted(|| file.sync_all())
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
///
/// Returns false if a directory on the path is gone. Then the entry of
/// `path` is gone too, and the caller decides whether that is normal
/// absence.
fn sync_entries(base: &Path, path: &Path) -> bool {
    let parent = match base.parent() {
        // The first component of a relative path.
        Some(parent) if parent.as_os_str().is_empty() => Some(Path::new(".")),
        // `/` has no entry to make durable.
        parent => parent,
    };
    if let Some(parent) = parent
        && !fsync_dir(parent)
    {
        return false;
    }
    for dir in descend(base, path.parent().unwrap()) {
        if !fsync_dir(&dir) {
            return false;
        }
    }
    true
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

/// Fsync `dir`. Returns false if it is gone.
fn fsync_dir(dir: &Path) -> bool {
    let file = match retry_interrupted(|| File::open(dir)) {
        Ok(file) => file,
        Err(e) if absent(&e) => return false,
        Err(e) => fatal("open for fsync", dir, e),
    };
    if let Err(e) = retry_interrupted(|| file.sync_all()) {
        fatal("fsync", dir, e);
    }
    true
}

/// Give a complete, fsynced temporary its published name. A concurrent
/// cleanup, the end of another exchange or expiry, can remove the
/// temporary first. That is normal absence: the effect is the same as
/// removal of the published file just after the rename (DESIGN.md §5.6).
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
struct PublishedList {
    file: File,
    path: PathBuf,
}

impl PublishedList {
    /// Renew after successful use, through the open descriptor; `ctime`
    /// is the renewal marker (DESIGN.md §5.6).
    fn renew(&self) {
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

/// One side's part of a list exchange, in its cache directory for the
/// peer and the receiver's index (DESIGN.md §5.6).
///
/// An exchange lists the directory when it starts, can publish this
/// side's target, and at its end removes the listed files except that
/// target. So it removes only files that existed before the exchange: the
/// lists that the target replaces, and the lists and temporaries that
/// failed runs left. A run with another target starts only after this
/// run's receiver has ended. The sender starts its exchange before it
/// sends HELLO, and ends it only after HASHLIST, HASHDELTA, or HASHKEEP,
/// which the receiver sends only after it received that HELLO. So no
/// exchange that ends has a listing that holds the target of a later run,
/// also when the sender outlives its receiver.
pub struct Exchange {
    store: ListStore,
    /// The lists of the directory when the exchange started.
    lists: Vec<Listed>,
    /// The names of the hidden temporaries of the directory then.
    temporaries: Vec<OsString>,
    published: Option<PublishedList>,
}

/// A listed list, and the file that the listing opened. The open file
/// keeps its inode, so its inode number cannot belong to another file
/// until the exchange ends.
struct Listed {
    name: OsString,
    file: File,
}

impl Exchange {
    /// The receiver's exchange with sender account `peer`, in
    /// `local-hashlists/B-I`.
    pub fn receiver(peer: &[u8; 16], index_id: &[u8; 16]) -> Self {
        Self::start(ListStore::new("local-hashlists", peer, index_id))
    }

    /// The sender's exchange with receiver account `peer`, in
    /// `remote-hashlists/A-I`. The sender must start it before it sends
    /// HELLO, so that the listing of an exchange that ends comes before
    /// the end of its receiver (see `Exchange`).
    pub fn sender(peer: &[u8; 16], index_id: &[u8; 16]) -> Self {
        Self::start(ListStore::new("remote-hashlists", peer, index_id))
    }

    /// List the directory, and open its lists. A missing directory gives
    /// an empty listing. Entries that are not regular files, and hidden
    /// names that are not temporaries of this program (`is_temporary`),
    /// are not part of the cache layout, and stay. On NFS, the exchanges
    /// themselves cause such hidden names: they remove listed lists that
    /// the listings of concurrent exchanges still have open.
    fn start(store: ListStore) -> Self {
        let mut lists = Vec::new();
        let mut temporaries = Vec::new();
        let listed = for_each_entry(&store.dir, |entry| {
            match retry_interrupted(|| entry.file_type()) {
                Ok(kind) if kind.is_file() => {}
                Ok(_) => return,
                Err(e) if absent(&e) => return,
                Err(e) => fatal("stat", &entry.path(), e),
            }
            let name = entry.file_name();
            let bytes = name.as_bytes();
            if is_temporary(bytes) {
                temporaries.push(name);
            } else if !bytes.starts_with(b".")
                && bytes.ends_with(b".cache")
                && let Some(file) = open_list(&entry.path())
            {
                lists.push(Listed { name, file });
            }
        });
        match listed {
            Ok(()) => {}
            Err(e) if absent(&e) => {}
            Err(e) => fatal("list", &store.dir, e),
        }
        Exchange {
            store,
            lists,
            temporaries,
            published: None,
        }
    }

    /// The IDs of the listed lists, which the sender advertises in HELLO.
    /// Only names of the run's strong hash, `algorithm`, give IDs: a list
    /// of another strong hash cannot be a base, and is only removed.
    pub fn listed_ids(&self, algorithm: StrongHash) -> Vec<Strong> {
        self.lists
            .iter()
            .filter_map(|listed| parse_name(&listed.name, algorithm))
            .collect()
    }

    /// Durably publish this side's target, `encoded`, under its
    /// content-derived name. Failures are fatal.
    pub fn publish(&mut self, id: &ListId, encoded: &[u8]) {
        self.published = Some(self.store.publish(id, encoded));
    }

    /// End the exchange: renew the published target, remove the listed
    /// files except that target, and expire both cache trees. Without a
    /// published target, this removes every listed file. The sender ends
    /// its exchange before it sends CACHE_RESULT, the receiver after it
    /// received it (DESIGN.md §5.6).
    ///
    /// A listed temporary belongs to an earlier run, or to a concurrent run
    /// with the same target. Without it, that run publishes nothing, which
    /// is normal absence.
    pub fn finish(self, max_age: u64) {
        if let Some(list) = &self.published {
            list.renew();
        }
        let target = self
            .published
            .as_ref()
            .map(|list| list.path.file_name().expect("a list path has a file name"));
        for listed in &self.lists {
            if target != Some(listed.name.as_os_str()) {
                self.store.remove_if_listed(listed);
            }
        }
        for name in &self.temporaries {
            remove("remove", &self.store.dir.join(name));
        }
        expire(max_age);
    }
}

/// The digest of a list file name, `<algorithm>-<digest>.cache`.
fn parse_name(name: &OsStr, algorithm: StrongHash) -> Option<Strong> {
    let digits = name
        .as_bytes()
        .strip_prefix(algorithm.id().as_bytes())?
        .strip_prefix(b"-")?
        .strip_suffix(b".cache")?;
    parse_hex(digits)
}

/// One peer/index cache directory of hash lists: where its files are, and
/// how they are published, read, and removed.
struct ListStore {
    /// `$XDG_CACHE_HOME`, where `publish` starts to create directories.
    base: PathBuf,
    /// `base/hashsync/<tree>/<peer>-<index>`.
    dir: PathBuf,
}

impl ListStore {
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
    /// are fatal; a failure can leave the hidden temporary until the next
    /// exchange of this peer and index, or until expiry (DESIGN.md §6.9).
    fn publish(&self, id: &ListId, encoded: &[u8]) -> PublishedList {
        let (temp, mut file) = self.create_temporary();
        let filled = fill_file(&mut file, |file| {
            file.write_all(WRAPPER)?;
            file.write_all(encoded)
        });
        if let Err(e) = filled {
            fatal("write", &temp, e);
        }
        let path = self.dir.join(Self::file_name(id));
        rename_temporary(&temp, &path);
        // A directory on the path can be gone only if a concurrent cleanup
        // removed the file, and expiry then the empty directory. A file
        // that is gone needs no durability: this is normal absence
        // (DESIGN.md §5.6).
        sync_entries(&self.base, &path);
        PublishedList { file, path }
    }

    /// Create a new hidden temporary in this directory, and create the
    /// directory first when it is missing.
    ///
    /// This function exists so that publication needs no lock against
    /// expiry, which removes this directory when it is empty. Expiry can
    /// do so also just after this function created the directory, and
    /// before the temporary exists. Then the creation of the temporary
    /// fails with ENOENT, and this function creates the directory again.
    /// When the temporary exists, the directory is not empty, and expiry
    /// cannot remove it (DESIGN.md §5.6).
    fn create_temporary(&self) -> (PathBuf, File) {
        loop {
            let temp = self.dir.join(temporary_name());
            let error = match create_new(&temp, 0o600) {
                Ok(file) => return (temp, file),
                Err(e) => e,
            };
            if !absent(&error) {
                fatal("create", &temp, error);
            }
            create_dirs(&self.base, &self.dir, 0o700);
            // Try again if the directory exists now, or if expiry removed
            // it again. Anything else in its place, such as a symlink to a
            // directory that does not exist, would fail every new try in
            // the same way.
            match retry_interrupted(|| std::fs::symlink_metadata(&self.dir)) {
                Ok(meta) if meta.is_dir() => {}
                Err(e) if absent(&e) => {}
                _ => fatal("create", &temp, error),
            }
        }
    }

    /// Read and verify a published list: the wrapper must be complete
    /// and the bytes must match the ID's digest. An absent file is a
    /// miss; an invalid file is removed and is a miss; a read failure is
    /// fatal (DESIGN.md §5.6). Cached-list reuse arrives with PLAN.md
    /// step 8; until then only tests verify the stored format through
    /// this.
    #[cfg_attr(not(test), expect(dead_code))]
    fn read(&self, id: &ListId) -> Option<Vec<u8>> {
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

    /// Remove a listed list, but only if its name still names the file
    /// that the exchange's listing opened.
    ///
    /// This function exists because a later run can publish the same list
    /// again under the same name, after the listing, when its index has
    /// returned to an earlier state. That file must stay. No system call
    /// removes a name only if it names a given file, so this function
    /// moves the name to a new temporary name, which no other process
    /// uses, and compares the file there with the listed file. It removes
    /// the listed file, and moves any other file back (DESIGN.md §5.6).
    fn remove_if_listed(&self, listed: &Listed) {
        let path = self.dir.join(&listed.name);
        let original = match retry_interrupted(|| listed.file.metadata()) {
            Ok(meta) if meta.is_file() => meta,
            // Not part of the cache layout.
            Ok(_) => return,
            Err(e) => fatal("stat", &path, e),
        };
        let moved = self.dir.join(temporary_name());
        match retry_interrupted(|| std::fs::rename(&path, &moved)) {
            Ok(()) => {}
            Err(e) if absent(&e) => return,
            Err(e) => fatal("remove", &path, e),
        }
        let current = match retry_interrupted(|| std::fs::symlink_metadata(&moved)) {
            Ok(meta) => meta,
            // A concurrent cleanup removed it as a temporary.
            Err(e) if absent(&e) => return,
            Err(e) => fatal("stat", &moved, e),
        };
        if (current.dev(), current.ino()) == (original.dev(), original.ino()) {
            remove("remove", &moved);
            return;
        }
        match retry_on_intr(|| {
            rustix::fs::renameat_with(
                rustix::fs::CWD,
                &moved,
                rustix::fs::CWD,
                &path,
                RenameFlags::NOREPLACE,
            )
        }) {
            Ok(()) => {}
            // The list was published again meanwhile: the same content is
            // in place.
            Err(Errno::EXIST) => remove("remove", &moved),
            // A concurrent cleanup removed it as a temporary.
            Err(Errno::NOENT) => {}
            Err(e) => fatal("restore", &path, e),
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

/// Open a list for a listing, without following a symlink. `None` if it is
/// gone, or if a symlink took its place, which is not part of the cache
/// layout.
fn open_list(path: &Path) -> Option<File> {
    match retry_on_intr(|| {
        rustix::fs::open(
            path,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
    }) {
        Ok(fd) => Some(File::from(fd)),
        Err(Errno::NOENT | Errno::LOOP) => None,
        Err(e) => fatal("open", path, e),
    }
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

/// Expire the files older than `max_age` seconds in every peer/index
/// directory of both cache trees, lists and abandoned hidden temporaries,
/// then remove the directories that are left empty. Run after the
/// exchange. Absence is normal; other failures are fatal, except a failed
/// removal of an empty directory, which only warns (DESIGN.md §5.6).
fn expire(max_age: u64) {
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
        // Whether an entry stays in the directory.
        let mut kept = false;
        let listed = for_each_entry(&peer.path(), |file| {
            // `DirEntry::metadata` does not follow symlinks.
            let meta = match retry_interrupted(|| file.metadata()) {
                Ok(meta) => meta,
                Err(e) if absent(&e) => return,
                Err(e) => fatal("stat", &file.path(), e),
            };
            let age = now.saturating_sub(meta.ctime());
            if meta.is_file() && age > i64::try_from(max_age).unwrap_or(i64::MAX) {
                remove("expire", &file.path());
            } else {
                kept = true;
            }
        });
        match listed {
            Ok(()) => {}
            // A concurrent expiry removed the directory.
            Err(e) if absent(&e) => return,
            Err(e) => fatal("list", &peer.path(), e),
        }
        if !kept {
            remove_empty_dir(&peer.path());
        }
    });
    match listed {
        Ok(()) => {}
        Err(e) if absent(&e) => {}
        Err(e) => fatal("list", tree, e),
    }
}

/// Remove a peer/index directory that expiry left empty, so that the
/// directories of old indexes and peers do not accumulate.
///
/// A concurrent publication can create a file in the directory at any
/// time, and then the directory stays. A concurrent expiry can remove it
/// first. Both are normal. Any other failure only warns: an empty
/// directory that stays does no harm, so it must not end the run
/// (DESIGN.md §5.6).
fn remove_empty_dir(dir: &Path) {
    match retry_on_intr(|| rustix::fs::rmdir(dir)) {
        Ok(()) | Err(Errno::NOTEMPTY | Errno::EXIST | Errno::NOENT) => {}
        Err(e) => warn(format!("cannot remove {}: {e}", dir.display())),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Mutex;

    use super::*;

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

    /// The canonical list of `values`, and its BLAKE3 ID.
    fn list(values: &[u64]) -> (ListId, Vec<u8>) {
        let encoded = crate::hashlist::encode(values);
        let id = ListId {
            algorithm: StrongHash::Blake3,
            digest: StrongHash::Blake3.digest(&encoded),
        };
        (id, encoded)
    }

    /// The names in `dir`, sorted.
    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn published_lists_verify_and_reread() {
        with_home(|home| {
            let store = ListStore::new("local-hashlists", &[0xaa; 16], &[0xbb; 16]);
            let (id, encoded) = list(&[1, 2, 3]);
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
    fn an_exchange_keeps_only_its_target_among_the_listed_files() {
        // An exchange lists its directory when it starts, and at its end
        // removes the listed files except its published target: an older
        // list, a list of another strong hash, a file name that is no list
        // ID, and an abandoned temporary. It advertises only the IDs of the
        // run's strong hash. Files that appear after the listing stay,
        // because a later run can own them, and so does an entry that is
        // not a file, or a hidden name that is not a temporary, such as
        // the name that an NFS client gives to a removed file that is
        // still open (DESIGN.md §5.6).
        with_home(|_| {
            let store = ListStore::new("remote-hashlists", &[6; 16], &[7; 16]);
            let (older, older_bytes) = list(&[1]);
            let (target, target_bytes) = list(&[2]);
            store.publish(&older, &older_bytes);
            store.publish(&target, &target_bytes);
            let other_hash = format!("sha256-{}.cache", hex(&[8; 32]));
            let abandoned = temporary_name();
            let nfs = ".nfs000000000123456700000001";
            for name in [other_hash.as_str(), "junk.cache", abandoned.as_str(), nfs] {
                std::fs::write(store.dir.join(name), b"x").unwrap();
            }
            std::fs::create_dir(store.dir.join("directory.cache")).unwrap();

            let mut exchange = Exchange::sender(&[6; 16], &[7; 16]);
            let mut advertised = exchange.listed_ids(StrongHash::Blake3);
            advertised.sort();
            let mut expected = vec![older.digest, target.digest];
            expected.sort();
            assert_eq!(advertised, expected);

            let (later, later_bytes) = list(&[3]);
            store.publish(&later, &later_bytes);
            let later_temporary = temporary_name();
            std::fs::write(store.dir.join(&later_temporary), b"x").unwrap();
            exchange.publish(&target, &target_bytes);
            exchange.finish(u64::MAX);

            let mut expected = vec![
                later_temporary,
                nfs.to_owned(),
                "directory.cache".to_owned(),
                ListStore::file_name(&later),
                ListStore::file_name(&target),
            ];
            expected.sort();
            assert_eq!(names(&store.dir), expected);
        });
    }

    #[test]
    fn an_exchange_without_a_target_removes_every_listed_file() {
        // With caching disabled, a side publishes no target, and the end of
        // the exchange removes every listed file, lists and temporaries,
        // only in its own directory. Expiry then removes the empty
        // directory (DESIGN.md §5.6).
        with_home(|_| {
            let ours = ListStore::new("local-hashlists", &[1; 16], &[2; 16]);
            let other = ListStore::new("local-hashlists", &[3; 16], &[2; 16]);
            let (id, encoded) = list(&[]);
            ours.publish(&id, &encoded);
            other.publish(&id, &encoded);
            std::fs::write(ours.dir.join(temporary_name()), b"x").unwrap();
            Exchange::receiver(&[1; 16], &[2; 16]).finish(u64::MAX);
            assert!(!ours.dir.exists(), "the empty directory must be removed");
            assert_eq!(names(&other.dir), [ListStore::file_name(&id)]);
            // A directory that never held lists is fine.
            Exchange::receiver(&[9; 16], &[2; 16]).finish(u64::MAX);
        });
    }

    #[test]
    fn a_list_published_again_after_the_listing_stays() {
        // A later run can publish a listed list again under the same name,
        // when its index has returned to an earlier state. The end of the
        // exchange removes a listed name only if it still names the listed
        // file, so the new file stays, and no temporary is left behind
        // (DESIGN.md §5.6).
        with_home(|_| {
            let store = ListStore::new("remote-hashlists", &[4; 16], &[5; 16]);
            let (again, again_bytes) = list(&[1]);
            let (target, target_bytes) = list(&[2]);
            store.publish(&again, &again_bytes);
            let mut exchange = Exchange::sender(&[4; 16], &[5; 16]);
            store.publish(&again, &again_bytes);
            exchange.publish(&target, &target_bytes);
            exchange.finish(u64::MAX);
            let mut expected = vec![ListStore::file_name(&again), ListStore::file_name(&target)];
            expected.sort();
            assert_eq!(names(&store.dir), expected);
            assert_eq!(store.read(&again).unwrap(), again_bytes);
        });
    }

    #[test]
    fn publication_creates_its_directory_again() {
        // Expiry removes a directory when it is empty, also just after a
        // concurrent publication created it. Then the creation of the
        // temporary fails with ENOENT, and publication creates the
        // directory again (DESIGN.md §5.6). The first publication in a
        // directory takes the same path.
        with_home(|_| {
            let store = ListStore::new("local-hashlists", &[8; 16], &[9; 16]);
            let (id, encoded) = list(&[5]);
            store.publish(&id, &encoded);
            std::fs::remove_file(store.dir.join(ListStore::file_name(&id))).unwrap();
            std::fs::remove_dir(&store.dir).unwrap();
            store.publish(&id, &encoded);
            assert_eq!(store.read(&id).unwrap(), encoded);
        });
    }

    #[test]
    fn a_directory_gone_before_its_fsync_is_absence() {
        // A concurrent cleanup can remove a published list, and expiry then
        // its directory, before the publication makes the entries durable.
        // The file is gone, so this is absence, not a failure (DESIGN.md
        // §5.6).
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base");
        std::fs::create_dir(&base).unwrap();
        assert!(sync_entries(&base, &base.join("list")));
        assert!(!sync_entries(&base, &base.join("gone/list")));
    }

    #[test]
    fn a_temporary_removed_before_its_rename_is_normal_absence() {
        // A concurrent cleanup removes the temporary between its fsync and
        // its rename (DESIGN.md §5.6). Publication continues without the
        // file, as if the cleanup had removed it just after the rename.
        let dir = tempfile::tempdir().unwrap();
        let temp = dir.path().join(temporary_name());
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
            let store = ListStore::new("remote-hashlists", &[4; 16], &[5; 16]);
            let (id, encoded) = list(&[7]);
            store.publish(&id, &encoded);
            std::fs::write(store.dir.join(temporary_name()), b"x").unwrap();

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

            // A directory with an entry that is not a file stays: that
            // entry is not part of the cache layout, and expiry keeps it.
            let other = cache_root().join("remote-hashlists/other");
            std::fs::create_dir_all(other.join("entry")).unwrap();

            // One hour after the last file was made, both the list and the
            // hidden temporary are older than a half-hour bound. Then the
            // directory is empty, and expiry removes it too.
            expire_tree(&cache_root().join("remote-hashlists"), last + 3600, 1800);
            assert!(!store.dir.exists(), "the empty directory must be removed");
            assert!(other.join("entry").exists());

            // A missing tree is normal absence.
            expire_tree(&cache_root().join("no-such-tree"), first, 0);
        });
    }
}
