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
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::SystemTime;

use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags, Timespec, Timestamps, UTIME_NOW};
use rustix::io::{Errno, retry_on_intr};

use crate::config;
use crate::die::die;
use crate::fsops::retry_interrupted;
use crate::hash::{Strong, StrongHash};
use crate::hashlist::{self, ListId};
use crate::names::{hex, is_temporary, parse_hex, random_bytes, temporary_name};
use crate::report::warn;

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
    /// The device and inode numbers of the file. They tell whether two
    /// names lead to one file, a hard link (`Exchange::link`).
    identity: (u64, u64),
    /// The modification time of the file when this process opened it, in
    /// seconds and nanoseconds. A published file is never written again,
    /// so only a renewal changes it (`renew`).
    opened_mtime: (i64, i64),
}

/// Why `ListFile::read` gave no list.
#[derive(Debug, PartialEq, Eq)]
enum Miss {
    /// The file holds something other than the list of its name: a damaged
    /// file, which `read` removed, unless another exchange renewed it.
    Invalid,
    /// Another NFS client removed the file.
    Gone,
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
            Ok(meta) if meta.is_file() => Some(ListFile {
                path,
                file,
                identity: (meta.dev(), meta.ino()),
                opened_mtime: (meta.mtime(), meta.mtime_nsec()),
            }),
            Ok(_) => None,
            Err(e) if stale(&e) => None,
            Err(e) => fatal("stat", &path, e),
        }
    }

    /// The list file that this process has just created at `path`. The
    /// identity and the modification time are 0 when `stat` fails: no
    /// exchange compares the file that it created with another, and none
    /// removes it.
    fn created(path: PathBuf, file: File) -> ListFile {
        let meta = retry_interrupted(|| file.metadata()).ok();
        ListFile {
            path,
            file,
            identity: meta
                .as_ref()
                .map_or((0, 0), |meta| (meta.dev(), meta.ino())),
            opened_mtime: meta.map_or((0, 0), |meta| (meta.mtime(), meta.mtime_nsec())),
        }
    }

    fn name(&self) -> &OsStr {
        self.path.file_name().expect("a list path has a file name")
    }

    /// Whether the path still leads to the file that this process has
    /// open: false once another exchange removed or replaced it.
    fn is_at_its_path(&self) -> bool {
        let open = match retry_interrupted(|| self.file.metadata()) {
            Ok(meta) => meta,
            Err(e) if stale(&e) => return false,
            Err(e) => fatal("stat", &self.path, e),
        };
        match retry_interrupted(|| std::fs::symlink_metadata(&self.path)) {
            Ok(meta) => (meta.dev(), meta.ino()) == (open.dev(), open.ino()),
            Err(e) if absent(&e) => false,
            Err(e) => fatal("stat", &self.path, e),
        }
    }

    /// Read the whole file through the open descriptor and check that it
    /// holds the list `id`: a complete wrapper, list bytes with the digest
    /// of `id`, and a canonical encoding, which `decode` checks as it
    /// decodes the list bytes. Returns what `decode` made of them: the
    /// values, or the sender's hash table.
    ///
    /// Anything else in the file is a cache miss: the file is removed, and
    /// the result is `Miss::Invalid`. A file that is gone is a miss as
    /// well. Other read errors are fatal (DESIGN.md §5.6).
    fn read<T>(
        &self,
        id: &ListId,
        decode: impl FnOnce(&[u8]) -> Result<T, String>,
    ) -> Result<T, Miss> {
        let mut bytes = Vec::new();
        let mut file = &self.file;
        match retry_interrupted(|| file.rewind()).and_then(|()| file.read_to_end(&mut bytes)) {
            Ok(_) => {}
            // Another NFS client removed the file. Its name may already
            // belong to a newer file, which must stay.
            Err(e) if stale(&e) => return Err(Miss::Gone),
            Err(e) => fatal("read", &self.path, e),
        }
        let list = parse_list(&bytes, id, decode);
        if list.is_none() {
            self.remove();
        }
        list.ok_or(Miss::Invalid)
    }

    /// Renew the file after successful use, through the open descriptor
    /// (DESIGN.md §5.6). The ctime, which expiry reads, becomes the current
    /// time. The modification time becomes later than the file had, by at
    /// least a nanosecond, even within one tick of the clock: an older
    /// exchange that listed the file then sees that it is in use, and its
    /// cleanup spares it (`remove`).
    fn renew(&self) {
        let meta = match retry_interrupted(|| self.file.metadata()) {
            Ok(meta) => meta,
            // Another NFS client removed the file, so there is nothing to
            // renew.
            Err(e) if stale(&e) => return,
            Err(e) => fatal("renew", &self.path, e),
        };
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let now = (now.as_secs() as i64, i64::from(now.subsec_nanos()));
        let next = match (meta.mtime(), meta.mtime_nsec()) {
            (seconds, 999_999_999) => (seconds + 1, 0),
            (seconds, nanoseconds) => (seconds, nanoseconds + 1),
        };
        let (tv_sec, tv_nsec) = now.max(next);
        let times = Timestamps {
            last_access: Timespec {
                tv_sec: 0,
                tv_nsec: UTIME_NOW,
            },
            last_modification: Timespec { tv_sec, tv_nsec },
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
    /// this process has open, and nobody renewed it since this process
    /// opened it.
    ///
    /// This exists because another run can make the same list its target
    /// after this process opened it, when its index went back to an earlier
    /// state, and that file must stay. That run publishes the list again
    /// under the same name, which gives the name another file, or keeps the
    /// file, which renews it (`Exchange::keep`). No system call unlinks a
    /// name only if it refers to a given file, so this renames the name to
    /// a fresh temporary name that no other process uses, and compares the
    /// file there with the open one and with the modification time that it
    /// had when this process opened it. If both match, it deletes it;
    /// otherwise it renames the file back. The rename changes the ctime of
    /// the file on most filesystems, but not its modification time, so the
    /// comparison can come after it, once the name is this process's alone
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
        let same = (current.dev(), current.ino()) == (original.dev(), original.ino());
        if same && (current.mtime(), current.mtime_nsec()) == self.opened_mtime {
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
/// INDEX until its lookups end. The sender lists its directory at INDEX,
/// before ADVERTISE, and the receiver its own at ADVERTISE, and the
/// receiver sends its target only if its lookups still run after both
/// listings. The sender ends its exchange only after HASHLIST, HASHDELTA,
/// or HASHKEEP. So no exchange that ends has a listing that holds the
/// target of a later run, also when the sender outlives its receiver.
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
    /// This side's target, which the end of the exchange renews and does
    /// not remove: a file that this side published, or a listed file that
    /// it kept. Shared with the listing in the second case.
    published: Option<Rc<ListFile>>,
    /// The files that a read of this exchange found damaged. Neither `keep`
    /// nor `link` makes one of them the target, under any of its names:
    /// another peer/index directory can hold a hard link to a damaged file,
    /// whose repair would otherwise link it back. They stay open until the
    /// exchange ends, so that their inode numbers cannot be reused.
    rejected: Vec<Rc<ListFile>>,
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
            rejected: Vec::new(),
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

    /// For the sender: read and verify the list `id`, which it advertised,
    /// through the file that the listing opened, and decode it with
    /// `decode`. Returns what `decode` made of it, or `None` if the listing
    /// does not have it, or if the file turns out to be invalid, which
    /// removes it, or gone (DESIGN.md §5.6).
    pub fn read_listed<T>(
        &self,
        id: &ListId,
        decode: impl FnOnce(&[u8]) -> Result<T, String>,
    ) -> Option<T> {
        self.listed(id)?.read(id, decode).ok()
    }

    /// Whether `file` is, under any name, a file that a read of this
    /// exchange found damaged.
    fn rejects(&self, file: &ListFile) -> bool {
        self.rejected
            .iter()
            .any(|rejected| rejected.identity == file.identity)
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
        &mut self,
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
                return match file.read(&id, hashlist::decode) {
                    Ok(values) => Some((Base { digest, file }, values)),
                    Err(Miss::Invalid) => {
                        self.rejected.push(file);
                        None
                    }
                    Err(Miss::Gone) => None,
                };
            }
        }
        None
    }

    /// Durably publish this side's target, `encoded`, under its
    /// content-derived name. Failures are fatal.
    pub fn publish(&mut self, id: &ListId, encoded: &[u8]) {
        self.published = Some(Rc::new(self.store.publish(id, encoded)));
    }

    /// Make the listed list `id` this side's target, as if this side had
    /// published it, without writing it again; false if the listing does
    /// not have it, if a read found it invalid, or if another exchange has
    /// removed or replaced it since the listing. This function exists
    /// because a list takes 110 MB at 21 million values, and writing it
    /// again with its fsync costs about half a second on each side. The
    /// sender keeps a list that it has just read and verified. The receiver
    /// keeps its listed copy of its target, which it has read only if the
    /// copy was the base that it looked for: a damaged copy that it did not
    /// read shows only when a later exchange reads it as the base of a
    /// delta, and costs a full list then (DESIGN.md §5.6).
    ///
    /// Keeping reuses the file, and so its inode, which another exchange
    /// may also have in its listing. The renewal comes first, so that the
    /// cleanup of such an exchange spares the file (`ListFile::remove`).
    /// Then the file must still be at its name: a cleanup may have removed
    /// it before the renewal. Last, the entries of its directories become
    /// durable, as after a publication: the process that published the
    /// file may have died before it made them durable.
    pub fn keep(&mut self, id: &ListId) -> bool {
        let Some(listed) = self.listed(id).filter(|listed| !self.rejects(listed)) else {
            return false;
        };
        let listed = Rc::clone(listed);
        listed.renew();
        if !listed.is_at_its_path() || !sync_entries(&self.store.base, &listed.path) {
            return false;
        }
        self.published = Some(listed);
        true
    }

    /// For the receiver: make a copy of the list `id` from one of its other
    /// peer/index directories this side's target, as a hard link in this
    /// directory, without writing the list again; false if no other
    /// directory has the list, or if the link fails, for example across
    /// filesystems, or on a filesystem without hard links. A copy in the
    /// directory of another sender exists when this host receives the same
    /// target from several senders. The copy is not read, as with `keep`,
    /// and it is renewed first, for the same reason. A copy that is another
    /// link of a file that a read of this exchange found damaged does not
    /// count: linking it back would keep the damage (DESIGN.md §5.6).
    pub fn link(&mut self, id: &ListId) -> bool {
        let name = ListStore::file_name(id);
        let found = self
            .store
            .other_directories()
            .iter()
            .find_map(|dir| ListFile::open(dir.join(&name)).filter(|copy| !self.rejects(copy)));
        let Some(copy) = found else {
            return false;
        };
        copy.renew();
        match self.store.link(id, copy) {
            Some(linked) => {
                self.published = Some(Rc::new(linked));
                true
            }
            None => false,
        }
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
        // A base from the listing goes with the rest of the listing below,
        // and a renewal would make its removal spare it.
        let base = base.filter(|base| {
            !self
                .lists
                .iter()
                .any(|listed| Rc::ptr_eq(listed, &base.file))
        });
        for list in self
            .published
            .as_deref()
            .into_iter()
            .chain(base.as_ref().map(|base| &*base.file))
        {
            list.renew();
        }
        let target = self.published.as_deref().map(ListFile::name);
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
            Err(e) if stale(&e) => return ListFile::created(path, file),
            Err(e) => fatal("write", &temp, e),
        }
        rename_temporary(&temp, &path);
        // A directory on the path can be gone only if a concurrent cleanup
        // removed the file, and expiry then the empty directory. A file
        // that is gone needs no durability: this is normal absence
        // (DESIGN.md §5.6).
        sync_entries(&self.base, &path);
        ListFile::created(path, file)
    }

    /// Give `copy`, the list `id` in another directory of this cache tree,
    /// a name in this directory too, as a hard link, and make the entries
    /// of its directories durable, as `publish` does. Its data became
    /// durable when it was published, so the link needs no fsync of the
    /// file. The link goes through `/proc/self/fd`, so that it links the
    /// file that this process has open, whatever its name leads to by now.
    /// `None` if the link fails, and the caller then writes the list: the
    /// file has lost its last name since it was opened, this directory is
    /// on another filesystem, the filesystem has no hard links, or there is
    /// no `/proc`.
    fn link(&self, id: &ListId, copy: ListFile) -> Option<ListFile> {
        let from = format!("/proc/self/fd/{}", copy.file.as_raw_fd());
        let temp = loop {
            let temp = self.dir.join(temporary_name());
            let linked = retry_on_intr(|| {
                rustix::fs::linkat(
                    rustix::fs::CWD,
                    &from,
                    rustix::fs::CWD,
                    &temp,
                    AtFlags::SYMLINK_FOLLOW,
                )
            });
            match linked {
                Ok(()) => break temp,
                Err(Errno::NOENT) => {}
                Err(_) => return None,
            }
            // ENOENT means that this directory is missing, as in
            // `create_temporary`, or that the copy has no name left.
            match retry_interrupted(|| std::fs::symlink_metadata(&self.dir)) {
                Ok(_) => return None,
                Err(e) if absent(&e) => create_dirs(&self.base, &self.dir, 0o700),
                Err(e) => fatal("stat", &self.dir, e),
            }
        };
        let path = self.dir.join(Self::file_name(id));
        rename_temporary(&temp, &path);
        // As after a publication, a directory that is gone means that a
        // concurrent cleanup removed the file: normal absence.
        sync_entries(&self.base, &path);
        Some(ListFile { path, ..copy })
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

/// What `decode` makes of the list bytes in the contents of a cache file,
/// checked against `id`, the ID in its name. Returns `None` for anything
/// unexpected: invalid contents are a cache miss, not an error.
fn parse_list<T>(
    bytes: &[u8],
    id: &ListId,
    decode: impl FnOnce(&[u8]) -> Result<T, String>,
) -> Option<T> {
    let encoded = bytes.strip_prefix(WRAPPER)?;
    if id.algorithm.digest(encoded) != id.digest {
        return None;
    }
    decode(encoded).ok()
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
/// then remove the directories that are left empty. Run at the end of each
/// exchange, also one that HASHNONE ends. Absence is normal; other failures
/// are fatal, except a failed removal of an empty directory, which only
/// warns (DESIGN.md §5.6).
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

    /// The list bytes themselves, as a decoder that keeps them: the tests
    /// compare them.
    fn bytes(list: &[u8]) -> Result<Vec<u8>, String> {
        hashlist::decode(list)?;
        Ok(list.to_vec())
    }

    /// Open the list `id` in `store` by name and return its verified bytes,
    /// or `None` if it is missing or invalid. An invalid file is removed.
    fn read(store: &ListStore, id: &ListId) -> Option<Vec<u8>> {
        let file = ListFile::open(store.dir.join(ListStore::file_name(id)))?;
        file.read(id, bytes).ok()
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
            assert_eq!(opened.read(&id, hashlist::decode), Ok(vec![1, 2, 3]));
            // Reading the same open file again starts from the beginning.
            assert_eq!(opened.read(&id, bytes), Ok(encoded));
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
                exchange.read_listed(&listed, hashlist::decode),
                Some(vec![10, 20, 30])
            );
            assert_eq!(exchange.read_listed(&later, hashlist::decode), None);
            // In another strong hash, the same digest is a different list.
            let sha256 = ListId {
                algorithm: StrongHash::Sha256,
                ..listed
            };
            assert_eq!(exchange.read_listed(&sha256, hashlist::decode), None);
        });
    }

    #[test]
    fn a_kept_list_is_not_written_again() {
        // A side that keeps a listed list as its target writes nothing: the
        // end of the exchange renews that same file, and removes the other
        // listed files, as after a publication. A list that the listing
        // lacks cannot be kept (DESIGN.md §5.6).
        with_home(|_| {
            let store = ListStore::new("local-hashlists", &[1; 16], &[2; 16]);
            let (kept, kept_bytes) = list(&[1, 2]);
            let (other, other_bytes) = list(&[3]);
            store.publish(&kept, &kept_bytes);
            store.publish(&other, &other_bytes);
            let path = store.dir.join(ListStore::file_name(&kept));
            let inode = std::fs::metadata(&path).unwrap().ino();
            let mut exchange = Exchange::receiver(&[1; 16], &[2; 16]);
            assert!(!exchange.keep(&list_id(9)), "an unlisted list was kept");
            assert!(exchange.keep(&kept));
            exchange.finish(None, u64::MAX);
            assert_eq!(names(&store.dir), [ListStore::file_name(&kept)]);
            let after = std::fs::metadata(&path).unwrap().ino();
            assert_eq!(after, inode, "the kept list was written again");
        });
    }

    #[test]
    fn a_kept_list_survives_the_cleanup_of_an_older_exchange() {
        // An older exchange listed a list that a later exchange then keeps
        // as its target, for example after the later run's index went back
        // to an earlier state, while the older run's sender, which outlived
        // its receiver, was still ending. Keeping reuses the file, so its
        // inode does not tell the later target apart, but its renewal does:
        // the older exchange's cleanup spares the kept list, and still
        // removes the list that nobody renewed (DESIGN.md §5.6).
        with_home(|_| {
            let store = ListStore::new("remote-hashlists", &[1; 16], &[2; 16]);
            let (kept, kept_bytes) = list(&[1, 2]);
            let (stale, stale_bytes) = list(&[3]);
            store.publish(&kept, &kept_bytes);
            store.publish(&stale, &stale_bytes);
            let older = Exchange::sender(&[1; 16], &[2; 16]);
            let mut later = Exchange::sender(&[1; 16], &[2; 16]);
            assert!(later.keep(&kept));
            older.finish(None, u64::MAX);
            assert_eq!(names(&store.dir), [ListStore::file_name(&kept)]);
            later.finish(None, u64::MAX);
            assert_eq!(read(&store, &kept), Some(kept_bytes));
        });
    }

    #[test]
    fn a_list_that_a_read_found_invalid_cannot_be_kept() {
        // A damaged list that the receiver read as a possible base, here its
        // own target, can no longer be kept, whether its removal took it
        // away or spared it because another exchange renewed it in the
        // meantime: the receiver must write its target again (DESIGN.md
        // §5.6).
        with_home(|_| {
            let store = ListStore::new("local-hashlists", &[1; 16], &[2; 16]);
            let (id, bytes) = list(&[5, 6]);
            let path = store.dir.join(ListStore::file_name(&id));
            for spared in [false, true] {
                store.publish(&id, &bytes);
                std::fs::write(&path, b"damaged").unwrap();
                let mut exchange = Exchange::receiver(&[1; 16], &[2; 16]);
                if spared {
                    assert!(Exchange::receiver(&[1; 16], &[2; 16]).keep(&id));
                }
                let base = exchange.find_base(&[id.digest], StrongHash::Blake3);
                assert!(base.is_none(), "a damaged list was a base");
                assert_eq!(path.exists(), spared, "spared {spared}");
                assert!(
                    !exchange.keep(&id),
                    "a damaged list was kept, spared {spared}"
                );
            }
        });
    }

    #[test]
    fn a_list_removed_or_replaced_since_the_listing_cannot_be_kept() {
        // Another exchange can remove a listed list, or publish it again,
        // after the listing: either way the listed file is no longer at its
        // name, and keeping it would make a target of a file without a name
        // (DESIGN.md §5.6).
        with_home(|_| {
            let store = ListStore::new("local-hashlists", &[1; 16], &[2; 16]);
            let (id, bytes) = list(&[4]);
            store.publish(&id, &bytes);
            let mut exchange = Exchange::receiver(&[1; 16], &[2; 16]);
            std::fs::remove_file(store.dir.join(ListStore::file_name(&id))).unwrap();
            assert!(!exchange.keep(&id), "a removed list was kept");
            store.publish(&id, &bytes);
            assert!(!exchange.keep(&id), "a replaced list was kept");
        });
    }

    #[test]
    fn a_copy_from_another_peer_directory_is_linked() {
        // The receiver's directory for one sender lacks the target, but the
        // directory of another sender has it: the receiver links that copy
        // into the first directory instead of writing the list again. Both
        // names then stay, one file (DESIGN.md §5.6).
        with_home(|_| {
            let other = ListStore::new("local-hashlists", &[3; 16], &[2; 16]);
            let own = ListStore::new("local-hashlists", &[1; 16], &[2; 16]);
            let (id, bytes) = list(&[7, 8, 9]);
            other.publish(&id, &bytes);
            let mut exchange = Exchange::receiver(&[1; 16], &[2; 16]);
            assert!(!exchange.keep(&id));
            assert!(exchange.link(&id));
            exchange.finish(None, u64::MAX);
            let inode = |store: &ListStore| {
                let path = store.dir.join(ListStore::file_name(&id));
                std::fs::metadata(path).unwrap().ino()
            };
            assert_eq!(inode(&own), inode(&other), "the copy was not linked");
            assert_eq!(read(&own, &id), Some(bytes.clone()));
            assert_eq!(read(&other, &id), Some(bytes));
            // Without a copy elsewhere, there is nothing to link.
            assert!(!Exchange::receiver(&[1; 16], &[2; 16]).link(&list_id(9)));
        });
    }

    #[test]
    fn a_damaged_file_is_not_linked_back_under_another_name() {
        // Two peer/index directories hold hard links to one list, which
        // then gets damaged. The receiver finds the damage when it reads its
        // own link as a base, and removes that link. The other link is the
        // same damaged file, so the receiver must not link it back, and
        // writes its target again instead (DESIGN.md §5.6).
        with_home(|_| {
            let other = ListStore::new("local-hashlists", &[3; 16], &[2; 16]);
            let own = ListStore::new("local-hashlists", &[1; 16], &[2; 16]);
            let (id, bytes) = list(&[7, 8, 9]);
            other.publish(&id, &bytes);
            let mut linking = Exchange::receiver(&[1; 16], &[2; 16]);
            assert!(linking.link(&id));
            linking.finish(None, u64::MAX);
            // Written in place, so both names show the damage.
            std::fs::write(own.dir.join(ListStore::file_name(&id)), b"damaged").unwrap();

            let mut exchange = Exchange::receiver(&[1; 16], &[2; 16]);
            let base = exchange.find_base(&[id.digest], StrongHash::Blake3);
            assert!(base.is_none(), "a damaged list was a base");
            assert!(!exchange.keep(&id), "the damaged file was kept");
            assert!(!exchange.link(&id), "the damaged file was linked back");
            exchange.publish(&id, &bytes);
            exchange.finish(None, u64::MAX);
            assert_eq!(read(&own, &id), Some(bytes));
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
            assert_eq!(exchange.read_listed(&id, hashlist::decode), None);
            assert!(!path.exists(), "the invalid list must be removed");

            std::fs::write(&path, [&WRAPPER[..], b"damaged"].concat()).unwrap();
            let exchange = Exchange::sender(&[1; 16], &[2; 16]);
            store.publish(&id, &bytes);
            assert_eq!(exchange.read_listed(&id, hashlist::decode), None);
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

            let mut exchange = Exchange::receiver(&[1; 16], &[2; 16]);
            std::fs::remove_file(ours.dir.join(ListStore::file_name(&mine))).unwrap();
            let mut base = |advertised: &[&ListId]| {
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

            let mut exchange = Exchange::receiver(&[1; 16], &[2; 16]);
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

            let mut exchange = Exchange::receiver(&[1; 16], &[2; 16]);
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
