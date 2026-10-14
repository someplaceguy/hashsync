//! Account identity and the hash-list cache (DESIGN.md §5.6).
//!
//! Full lists are immutable and named by content; account and index IDs
//! partition the cache directories without identifying list content.
//! Publication is atomic and durable: a unique hidden temporary, file
//! fsync, rename to the content-derived name, and an fsync of every
//! directory on the path that this program can create (`sync_entries`).
//! Published files are never edited in place.
//!
//! Each side runs its part of a list exchange through an `Exchange`. At the
//! start, the exchange lists the side's directory for the peer and index.
//! It then reads the cached list it needs, may publish the side's target,
//! and at the end removes every listed file except that target, expires old
//! files in all directories and removes the directories left empty. None of
//! this needs locks: publication recreates its directory if expiry removed
//! it, a file is only removed while its name still points to the file this
//! process opened, and each list is read once, through one open file, by a
//! reader that keeps the bytes.
//!
//! Cache directory and file operation failures end the run with `die`.
//! The exceptions are absence — a missing list is a miss, and missing
//! directories or already-removed files during lookup and cleanup are
//! normal — invalid file contents, which are removed and treated as a
//! miss, and an empty directory that cannot be removed, which only warns
//! (DESIGN.md §5.6). On NFS, a list file that another host removed while
//! this process had it open counts as absent too (see `ListFile`).

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::SystemTime;

use rustix::fs::{Mode, OFlags, RenameFlags, Timespec, Timestamps, UTIME_NOW};
use rustix::io::{Errno, retry_on_intr};

use crate::config;
use crate::die::{die, warn};
use crate::fsops::retry_interrupted;
use crate::hash::{Strong, StrongHash};
use crate::hashlist::{self, ListId};
use crate::names::{hex, is_temporary, parse_hex, random_bytes, temporary_name};

/// A cache operation failed for a reason other than absence
/// (DESIGN.md §5.6 and §6.7).
fn fatal(action: &str, path: &Path, error: impl std::fmt::Display) -> ! {
    die(format!("cannot {action} {}: {error}", path.display()))
}

fn absent(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::NotFound
}

/// True for ESTALE, which NFS returns for calls through a descriptor whose
/// file another host has removed. The file is gone just as if this host had
/// seen it disappear, so for a list file it means absence (DESIGN.md §5.6).
fn stale(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(Errno::STALE.raw_os_error())
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

/// A list file this process has open, with the path it was opened under.
/// Keeping it open lets the process read the list after another exchange
/// removed its name, and keeps the inode number from being reused while
/// `remove` compares against it (DESIGN.md §5.6).
///
/// On NFS, an open file survives the removal of its name only on the host
/// that has it open. Once another host removes it, calls through the
/// descriptor can fail with ESTALE. The methods below treat that as the
/// file being gone: a miss for `read`, and nothing left to do for `renew`
/// and `remove`.
struct ListFile {
    path: PathBuf,
    file: File,
}

impl ListFile {
    /// Open the list at `path`. Returns `None` if there is nothing at
    /// `path`, or if the entry is not a regular file (a symlink, directory,
    /// socket and so on), which is not part of the cache layout and is left
    /// alone. The open is non-blocking so that a FIFO cannot make it wait
    /// for a writer.
    fn open(path: PathBuf) -> Option<ListFile> {
        let fd = match retry_on_intr(|| {
            rustix::fs::open(
                &path,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
        }) {
            Ok(fd) => fd,
            // A directory on the path may have been removed or replaced as
            // well. Sockets, and device files without a driver, already
            // fail the open with ENXIO, before the file type check below.
            Err(Errno::NOENT | Errno::LOOP | Errno::NOTDIR | Errno::NXIO) => return None,
            Err(e) => fatal("open", &path, e),
        };
        let file = File::from(fd);
        match retry_interrupted(|| file.metadata()) {
            Ok(meta) if meta.is_file() => Some(ListFile { path, file }),
            Ok(_) => None,
            Err(e) if stale(&e) => None,
            Err(e) => fatal("stat", &path, e),
        }
    }

    fn name(&self) -> &OsStr {
        self.path.file_name().expect("a list path has a file name")
    }

    /// Read the whole file through the open descriptor and check that it
    /// holds the list `id`: a complete wrapper, list bytes with the digest
    /// of `id`, and a canonical encoding. Returns the list bytes and the
    /// decoded values.
    ///
    /// Anything else in the file is a cache miss: the file is removed and
    /// the result is `None`. A file that is gone is a miss as well. Other
    /// read errors are fatal (DESIGN.md §5.6).
    fn read(&self, id: &ListId) -> Option<(Vec<u8>, Vec<u64>)> {
        let mut bytes = Vec::new();
        let mut file = &self.file;
        match retry_interrupted(|| file.rewind()).and_then(|()| file.read_to_end(&mut bytes)) {
            Ok(_) => {}
            // Another NFS client removed the file. Its name may already
            // belong to a newer file, which must stay.
            Err(e) if stale(&e) => return None,
            Err(e) => fatal("read", &self.path, e),
        }
        let list = parse_list(bytes, id);
        if list.is_none() {
            self.remove();
        }
        list
    }

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
        match retry_on_intr(|| rustix::fs::futimens(&self.file, &times)) {
            Ok(()) => {}
            // Another NFS client removed the file, so there is nothing to
            // renew.
            Err(Errno::STALE) => {}
            Err(e) => fatal("renew", &self.path, e),
        }
    }

    /// Remove the file, but only while its path still points to the file
    /// this process has open.
    ///
    /// This exists because another run can publish the same list again
    /// under the same name after this process opened it, when its index
    /// went back to an earlier state, and that file must stay. No system
    /// call unlinks a name only if it refers to a given file, so this
    /// renames the name to a fresh temporary name that no other process
    /// uses, and compares the file there with the open one. If they are the
    /// same, it deletes it; otherwise it renames the other file back
    /// (DESIGN.md §5.6).
    fn remove(&self) {
        let original = match retry_interrupted(|| self.file.metadata()) {
            Ok(meta) => meta,
            // Another NFS client removed the file. Whatever the path names
            // now is a newer file, which must stay.
            Err(e) if stale(&e) => return,
            Err(e) => fatal("stat", &self.path, e),
        };
        let moved = self.path.with_file_name(temporary_name());
        match retry_interrupted(|| std::fs::rename(&self.path, &moved)) {
            Ok(()) => {}
            Err(e) if absent(&e) => return,
            Err(e) => fatal("remove", &self.path, e),
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
                &self.path,
                RenameFlags::NOREPLACE,
            )
        }) {
            Ok(()) => {}
            // The list was published again meanwhile: the same content is
            // in place.
            Err(Errno::EXIST) => remove("remove", &moved),
            // A concurrent cleanup removed it as a temporary.
            Err(Errno::NOENT) => {}
            Err(e) => fatal("restore", &self.path, e),
        }
    }
}

/// The base the receiver found for a delta: one of the advertised lists,
/// read from its cache and verified (DESIGN.md §5.6). If the receiver sends
/// a delta from it, the end of the exchange renews the file it was read
/// from, even when the sender then asks for the full list instead. Only a
/// base from another peer/index directory outlives the exchange: a base
/// from the receiver's listing is removed with the rest of the listing.
pub struct Base {
    pub digest: Strong,
    /// Shared with the listing when the base comes from it.
    file: Rc<ListFile>,
}

/// One side's part of a list exchange, in its cache directory for the
/// peer and the receiver's index (DESIGN.md §5.6).
///
/// An exchange lists the directory when it starts, can publish this
/// side's target, and at its end removes the listed files except that
/// target. So it removes only files that existed before the exchange: the
/// lists that the target replaces, and the lists and temporaries that
/// failed runs left. A run with another target needs the exclusive lock of
/// the index for its update, and the receiver holds its lock from before
/// its HELLO until after the list exchange. The sender starts its exchange
/// after the receiver's HELLO and before its own, and ends it only after
/// HASHLIST, HASHDELTA, or HASHKEEP, which the receiver sends only after
/// it received the sender's HELLO. So no exchange that ends has a listing
/// that holds the target of a later run, also when the sender outlives its
/// receiver.
///
/// The end of another exchange, or expiry, can remove a list at any moment,
/// including the base that a concurrent exchange is about to read. So each
/// side reads its base once, through a file it already has open, and never
/// opens a list by name a second time: both sides read the lists of their
/// listing, and the receiver opens a list in one of its other directories
/// only once.
pub struct Exchange {
    store: ListStore,
    /// The lists of the directory when the exchange started. They are shared
    /// with a `Base` that comes from the listing.
    lists: Vec<Rc<ListFile>>,
    /// The names of the hidden temporaries of the directory then.
    temporaries: Vec<OsString>,
    published: Option<ListFile>,
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
    /// the receiver releases the lock of its index (see `Exchange`).
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
                && let Some(file) = ListFile::open(entry.path())
            {
                lists.push(Rc::new(file));
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
    /// Only names of the stream's strong hash, `algorithm`, give IDs: a list
    /// of another strong hash cannot be a base, and is only removed.
    pub fn listed_ids(&self, algorithm: StrongHash) -> Vec<Strong> {
        self.lists
            .iter()
            .filter_map(|listed| parse_name(listed.name(), algorithm))
            .collect()
    }

    /// The list `id` from the listing, if the listing has it.
    fn listed(&self, id: &ListId) -> Option<&Rc<ListFile>> {
        self.lists
            .iter()
            .find(|listed| parse_name(listed.name(), id.algorithm) == Some(id.digest))
    }

    /// For the sender: read and verify the list `id` that HASHKEEP or
    /// HASHDELTA names, through the file that the listing opened. Returns
    /// its bytes and values, or `None` if the listing does not have it, or
    /// if the file turns out to be invalid, which removes it, or gone
    /// (DESIGN.md §5.6).
    pub fn read_listed(&self, id: &ListId) -> Option<(Vec<u8>, Vec<u64>)> {
        self.listed(id)?.read(id)
    }

    /// For the receiver: find a base for a delta to the target, and return
    /// it with its values (DESIGN.md §5.6). The base is the first of the
    /// `advertised` lists, in the stream's strong hash `algorithm`, that is in
    /// any of the receiver's peer/index directories. Lists are named by
    /// their content, so a copy that another peer or index left works just
    /// as well. Each advertised list is looked up in the listing first,
    /// then by name in the other directories. Only the first list found is
    /// read and verified: if it is invalid, which removes it, or gone,
    /// there is no base.
    pub fn find_base(
        &self,
        advertised: &[Strong],
        algorithm: StrongHash,
    ) -> Option<(Base, Vec<u64>)> {
        let mut others = None;
        for &digest in advertised {
            let id = ListId { algorithm, digest };
            let found = match self.listed(&id) {
                Some(listed) => Some(Rc::clone(listed)),
                None => others
                    .get_or_insert_with(|| self.store.other_directories())
                    .iter()
                    .find_map(|dir| ListFile::open(dir.join(ListStore::file_name(&id))))
                    .map(Rc::new),
            };
            if let Some(file) = found {
                let (_, values) = file.read(&id)?;
                return Some((Base { digest, file }, values));
            }
        }
        None
    }

    /// Durably publish this side's target, `encoded`, under its
    /// content-derived name. Failures are fatal.
    pub fn publish(&mut self, id: &ListId, encoded: &[u8]) {
        self.published = Some(self.store.publish(id, encoded));
    }

    /// End the exchange: renew the published target and `base`, which on
    /// the receiver is the base of the delta that it sent; remove every
    /// listed file except the target, or all of them if nothing was
    /// published; and expire both cache trees. The sender ends its exchange
    /// before it sends CACHE_RESULT, the receiver after it receives it
    /// (DESIGN.md §5.6).
    ///
    /// A listed temporary belongs to an earlier run, or to a concurrent run
    /// with the same target. Without it, that run publishes nothing, which
    /// is normal absence.
    pub fn finish(self, base: Option<Base>, max_age: u64) {
        for list in self
            .published
            .iter()
            .chain(base.as_ref().map(|base| &*base.file))
        {
            list.renew();
        }
        let target = self.published.as_ref().map(ListFile::name);
        for listed in &self.lists {
            if target != Some(listed.name()) {
                listed.remove();
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

/// One peer/index directory of cached lists: where its files live, and how
/// they get published.
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
    fn publish(&self, id: &ListId, encoded: &[u8]) -> ListFile {
        let (temp, mut file) = self.create_temporary();
        let path = self.dir.join(Self::file_name(id));
        let filled = fill_file(&mut file, |file| {
            file.write_all(WRAPPER)?;
            file.write_all(encoded)
        });
        match filled {
            Ok(()) => {}
            // Another NFS client removed the temporary. On one host, the
            // same cleanup would only show at the rename below, as ENOENT:
            // either way it is normal absence (DESIGN.md §5.6).
            Err(e) if stale(&e) => return ListFile { path, file },
            Err(e) => fatal("write", &temp, e),
        }
        rename_temporary(&temp, &path);
        // A directory on the path can be gone only if a concurrent cleanup
        // removed the file, and expiry then the empty directory. A file
        // that is gone needs no durability: this is normal absence
        // (DESIGN.md §5.6).
        sync_entries(&self.base, &path);
        ListFile { path, file }
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

    /// The other peer/index directories in this directory's cache tree,
    /// where the receiver also looks for a base. Only real directories
    /// count: a symlink to a directory is not part of the cache layout.
    fn other_directories(&self) -> Vec<PathBuf> {
        let tree = self.dir.parent().expect("a peer directory is in a tree");
        let mut dirs = Vec::new();
        let listed = for_each_entry(tree, |entry| {
            match retry_interrupted(|| entry.file_type()) {
                Ok(kind) if kind.is_dir() => {}
                Ok(_) => return,
                Err(e) if absent(&e) => return,
                Err(e) => fatal("stat", &entry.path(), e),
            }
            if entry.path() != self.dir {
                dirs.push(entry.path());
            }
        });
        match listed {
            Ok(()) => {}
            Err(e) if absent(&e) => {}
            Err(e) => fatal("list", tree, e),
        }
        dirs
    }
}

/// The list bytes and values in the contents of a cache file, checked
/// against `id`, the ID in its name. Returns `None` for anything
/// unexpected: invalid contents are a cache miss, not an error.
fn parse_list(mut bytes: Vec<u8>, id: &ListId) -> Option<(Vec<u8>, Vec<u64>)> {
    let encoded = bytes.strip_prefix(WRAPPER)?;
    if id.algorithm.digest(encoded) != id.digest {
        return None;
    }
    let values = hashlist::decode(encoded).ok()?;
    bytes.drain(..WRAPPER.len());
    Some((bytes, values))
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
pub(crate) mod tests {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Mutex;

    use super::*;
    use crate::hashlist::tests::list;

    /// Serializes tests that change `XDG_*`; the variables are process
    /// state.
    static ENV: Mutex<()> = Mutex::new(());

    /// Run `test` in a fresh home: `XDG_CONFIG_HOME` and `XDG_CACHE_HOME`
    /// point into a new temporary directory, which `test` receives. Every
    /// test that touches the cache runs this way, one at a time, because
    /// the variables belong to the whole process.
    pub(crate) fn with_home<T>(test: impl FnOnce(&Path) -> T) -> T {
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

    /// The names in `dir`, sorted.
    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    /// Open the list `id` in `store` by name and return its verified bytes,
    /// or `None` if it is missing or invalid. An invalid file is removed.
    fn read(store: &ListStore, id: &ListId) -> Option<Vec<u8>> {
        let file = ListFile::open(store.dir.join(ListStore::file_name(id)))?;
        file.read(id).map(|(encoded, _)| encoded)
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
            let opened = ListFile::open(file.clone()).unwrap();
            assert_eq!(opened.read(&id), Some((encoded.clone(), vec![1, 2, 3])));
            // Reading the same open file again starts from the beginning.
            assert_eq!(opened.read(&id), Some((encoded, vec![1, 2, 3])));
            assert!(read(&store, &list_id(9)).is_none(), "absence is a miss");

            // A damaged file is a miss and is removed (DESIGN.md §5.6).
            let mut bytes = std::fs::read(&file).unwrap();
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
            std::fs::write(&file, &bytes).unwrap();
            assert!(read(&store, &id).is_none());
            assert!(!file.exists(), "the invalid list must be removed");
            assert!(read(&store, &id).is_none(), "now an ordinary miss");

            // The same goes for a file whose bytes match the digest in its
            // name but are not a canonical list: here the header announces
            // one value that never follows.
            let truncated = 1u64.to_le_bytes();
            let id = ListId {
                algorithm: StrongHash::Blake3,
                digest: StrongHash::Blake3.digest(&truncated),
            };
            let path = dir.join(ListStore::file_name(&id));
            std::fs::write(&path, [&WRAPPER[..], &truncated].concat()).unwrap();
            assert!(read(&store, &id).is_none());
            assert!(!path.exists(), "the invalid list must be removed");
        });
    }

    #[test]
    fn only_regular_files_are_lists() {
        // A directory, FIFO, socket or symlink where a list should be is not
        // part of the cache layout: it is not a list, and it is left in
        // place. Opening the FIFO must not wait for a writer, and the
        // socket, whose open fails outright, must not end the run.
        let dir = tempfile::tempdir().unwrap();
        let directory = dir.path().join("directory.cache");
        std::fs::create_dir(&directory).unwrap();
        let fifo = dir.path().join("fifo.cache");
        rustix::fs::mkfifoat(rustix::fs::CWD, &fifo, Mode::from_raw_mode(0o600)).unwrap();
        let socket = dir.path().join("socket.cache");
        // Bound through the directory's descriptor: a long `TMPDIR` would
        // exceed the `sun_path` limit of a socket address.
        let opened = std::fs::File::open(dir.path()).unwrap();
        let address = format!("/proc/self/fd/{}/socket.cache", opened.as_raw_fd());
        let _bound = std::os::unix::net::UnixListener::bind(address).unwrap();
        let symlink = dir.path().join("symlink.cache");
        std::os::unix::fs::symlink(&directory, &symlink).unwrap();
        for path in [&directory, &fifo, &socket, &symlink] {
            assert!(ListFile::open(path.clone()).is_none(), "{}", path.display());
            assert!(path.symlink_metadata().is_ok());
        }
        assert!(ListFile::open(dir.path().join("gone/list.cache")).is_none());
    }

    #[test]
    fn an_exchange_keeps_only_its_target_among_the_listed_files() {
        // An exchange lists its directory when it starts, and at its end
        // removes the listed files except its published target: an older
        // list, a list of another strong hash, a file name that is no list
        // ID, and an abandoned temporary. It advertises only the IDs of the
        // stream's strong hash. Files that appear after the listing stay,
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
            exchange.finish(None, u64::MAX);

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
            Exchange::receiver(&[1; 16], &[2; 16]).finish(None, u64::MAX);
            assert!(!ours.dir.exists(), "the empty directory must be removed");
            assert_eq!(names(&other.dir), [ListStore::file_name(&id)]);
            // A directory that never held lists is fine.
            Exchange::receiver(&[9; 16], &[2; 16]).finish(None, u64::MAX);
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
            exchange.finish(None, u64::MAX);
            let mut expected = vec![ListStore::file_name(&again), ListStore::file_name(&target)];
            expected.sort();
            assert_eq!(names(&store.dir), expected);
            assert_eq!(read(&store, &again).unwrap(), again_bytes);
        });
    }

    #[test]
    fn the_sender_reads_a_listed_list_through_its_listing() {
        // The sender reads the list that HASHKEEP or HASHDELTA names through
        // the file its listing opened, because a concurrent exchange may
        // have removed the name in the meantime (DESIGN.md §5.6). A list
        // that was not in the listing is never read, even if it exists by
        // now.
        with_home(|_| {
            let store = ListStore::new("remote-hashlists", &[1; 16], &[2; 16]);
            let (listed, listed_bytes) = list(&[10, 20, 30]);
            store.publish(&listed, &listed_bytes);
            let exchange = Exchange::sender(&[1; 16], &[2; 16]);
            std::fs::remove_file(store.dir.join(ListStore::file_name(&listed))).unwrap();
            let (later, later_bytes) = list(&[40]);
            store.publish(&later, &later_bytes);
            assert_eq!(
                exchange.read_listed(&listed),
                Some((listed_bytes, vec![10, 20, 30]))
            );
            assert_eq!(exchange.read_listed(&later), None);
            // In another strong hash, the same digest is a different list.
            let sha256 = ListId {
                algorithm: StrongHash::Sha256,
                ..listed
            };
            assert_eq!(exchange.read_listed(&sha256), None);
        });
    }

    #[test]
    fn an_invalid_listed_list_is_removed_unless_its_name_moved_on() {
        // An invalid list is a miss and its file is removed (DESIGN.md
        // §5.6), but only while its name still points to the file that the
        // listing opened. In the second half, a later run has published a
        // valid copy of the same list under that name, which must stay.
        with_home(|_| {
            let store = ListStore::new("remote-hashlists", &[1; 16], &[2; 16]);
            let (id, bytes) = list(&[7, 8]);
            let path = store.dir.join(ListStore::file_name(&id));
            store.publish(&id, &bytes);
            std::fs::write(&path, [&WRAPPER[..], b"damaged"].concat()).unwrap();
            let exchange = Exchange::sender(&[1; 16], &[2; 16]);
            assert_eq!(exchange.read_listed(&id), None);
            assert!(!path.exists(), "the invalid list must be removed");

            std::fs::write(&path, [&WRAPPER[..], b"damaged"].concat()).unwrap();
            let exchange = Exchange::sender(&[1; 16], &[2; 16]);
            store.publish(&id, &bytes);
            assert_eq!(exchange.read_listed(&id), None);
            assert_eq!(read(&store, &id), Some(bytes));
            assert_eq!(names(&store.dir), [ListStore::file_name(&id)]);
        });
    }

    #[test]
    fn the_receiver_takes_the_first_advertised_list_that_it_finds() {
        // The receiver looks for the advertised lists in the order the
        // sender gave them: first in its listing, then by name in the
        // directories of its other peers and indexes, whose copies work as
        // well because lists are named by their content. A list from the
        // listing is read through the file the listing opened, so it can
        // still be the base after a concurrent exchange removed its name
        // (DESIGN.md §5.6).
        with_home(|_| {
            let ours = ListStore::new("local-hashlists", &[1; 16], &[2; 16]);
            let other = ListStore::new("local-hashlists", &[3; 16], &[4; 16]);
            let (mine, mine_bytes) = list(&[1, 2]);
            let (theirs, theirs_bytes) = list(&[3, 4]);
            let (nowhere, _) = list(&[5]);
            ours.publish(&mine, &mine_bytes);
            other.publish(&theirs, &theirs_bytes);
            // `remote-hashlists`, the tree this account uses as a sender, is
            // not searched.
            let remote = ListStore::new("remote-hashlists", &[1; 16], &[2; 16]);
            let (remote_only, remote_bytes) = list(&[6]);
            remote.publish(&remote_only, &remote_bytes);

            let exchange = Exchange::receiver(&[1; 16], &[2; 16]);
            std::fs::remove_file(ours.dir.join(ListStore::file_name(&mine))).unwrap();
            let base = |advertised: &[&ListId]| {
                let digests: Vec<Strong> = advertised.iter().map(|id| id.digest).collect();
                exchange
                    .find_base(&digests, StrongHash::Blake3)
                    .map(|(base, values)| (base.digest, values))
            };
            assert_eq!(
                base(&[&nowhere, &mine, &theirs]),
                Some((mine.digest, vec![1, 2]))
            );
            assert_eq!(base(&[&theirs, &mine]), Some((theirs.digest, vec![3, 4])));
            assert_eq!(base(&[&nowhere, &remote_only]), None);
            assert_eq!(base(&[]), None);
            // Only lists in the stream's strong hash can be a base.
            assert!(
                exchange
                    .find_base(&[theirs.digest], StrongHash::Sha256)
                    .is_none()
            );
        });
    }

    #[test]
    fn a_damaged_base_is_removed_and_no_other_list_is_tried() {
        // The receiver verifies only the first advertised list it finds. If
        // that one is damaged, it is removed and the target goes as a full
        // list, even though a later advertised list is valid (DESIGN.md
        // §5.6).
        with_home(|_| {
            let ours = ListStore::new("local-hashlists", &[1; 16], &[2; 16]);
            let other = ListStore::new("local-hashlists", &[3; 16], &[2; 16]);
            let (damaged, damaged_bytes) = list(&[1]);
            let (valid, valid_bytes) = list(&[2]);
            other.publish(&damaged, &damaged_bytes);
            ours.publish(&valid, &valid_bytes);
            let damaged_path = other.dir.join(ListStore::file_name(&damaged));
            std::fs::write(&damaged_path, WRAPPER).unwrap();

            let exchange = Exchange::receiver(&[1; 16], &[2; 16]);
            let advertised = [damaged.digest, valid.digest];
            assert!(
                exchange
                    .find_base(&advertised, StrongHash::Blake3)
                    .is_none()
            );
            assert!(!damaged_path.exists(), "the damaged base must be removed");
            assert_eq!(read(&ours, &valid), Some(valid_bytes));
        });
    }

    #[test]
    fn the_base_of_a_delta_is_renewed() {
        // At the end of its exchange, the receiver renews the base of the
        // delta it sent, through the file it read the base from. Here that
        // file is in another peer's directory, and the other list there
        // keeps its ctime (DESIGN.md §5.6).
        with_home(|home| {
            let other = ListStore::new("local-hashlists", &[3; 16], &[2; 16]);
            let (base_id, base_bytes) = list(&[1]);
            let (unused, unused_bytes) = list(&[2]);
            other.publish(&base_id, &base_bytes);
            other.publish(&unused, &unused_bytes);
            let ctime = |path: &Path| {
                let meta = std::fs::metadata(path).unwrap();
                (meta.ctime(), meta.ctime_nsec())
            };
            let base_path = other.dir.join(ListStore::file_name(&base_id));
            let unused_path = other.dir.join(ListStore::file_name(&unused));
            let (base_before, unused_before) = (ctime(&base_path), ctime(&unused_path));

            let exchange = Exchange::receiver(&[1; 16], &[2; 16]);
            let (base, _) = exchange
                .find_base(&[base_id.digest], StrongHash::Blake3)
                .unwrap();
            // Wait, with no deadline, until the filesystem clock has moved
            // past the base's ctime, so that the renewal shows.
            let probe = home.join("probe");
            loop {
                std::fs::write(&probe, b"").unwrap();
                if ctime(&probe) > base_before {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            exchange.finish(Some(base), u64::MAX);
            assert!(ctime(&base_path) >= ctime(&probe));
            assert_eq!(ctime(&unused_path), unused_before);
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
            assert_eq!(read(&store, &id).unwrap(), encoded);
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
