//! Descriptor-relative filesystem access (DESIGN.md §4.5).
//!
//! Entries are opened with `O_PATH|O_NOFOLLOW` for classification with
//! `statx`. An object that needs I/O is reopened through
//! `/proc/self/fd/N`, which reopens the verified object even after a rename
//! or unlink and never follows a symlink in the original pathname.
//!
//! Every wrapper retries a call that a signal interrupts, so callers never
//! see `EINTR`.

use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};
use std::path::Path;

use rustix::fs::{AtFlags, Dir, DirEntry, FileType, Mode, OFlags, StatxFlags};
use rustix::io::{Errno, retry_on_intr};

/// Flags for the initial open of any entry.
pub const PATH_FLAGS: OFlags = OFlags::PATH.union(OFlags::NOFOLLOW).union(OFlags::CLOEXEC);

/// The metadata used by traversal and hashing.
#[derive(Clone, Copy, Debug)]
pub struct Meta {
    pub kind: FileType,
    pub size: u64,
    /// Kernel `ctime` in nanoseconds (wrapping arithmetic; compared only
    /// for equality).
    pub ctime_ns: i64,
    pub ino: u64,
    pub mnt_id: u64,
    pub dev_major: u32,
    pub dev_minor: u32,
}

/// Open `/` for descriptor-relative walking.
pub fn open_root_dir() -> Result<OwnedFd, Errno> {
    retry_on_intr(|| {
        rustix::fs::open(
            "/",
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
    })
}

/// Open the entry `name` of `dir` without following symlinks, for
/// classification only.
pub fn open_path(dir: BorrowedFd<'_>, name: &[u8]) -> Result<OwnedFd, Errno> {
    retry_on_intr(|| rustix::fs::openat(dir, name, PATH_FLAGS, Mode::empty()))
}

/// Reopen the object behind an `O_PATH` descriptor for I/O.
fn reopen(fd: BorrowedFd<'_>, flags: OFlags) -> Result<OwnedFd, Errno> {
    let path = format!("/proc/self/fd/{}", fd.as_raw_fd());
    retry_on_intr(|| rustix::fs::open(path.as_str(), flags | OFlags::CLOEXEC, Mode::empty()))
}

/// Reopen a verified regular file for reading.
pub fn reopen_file(fd: BorrowedFd<'_>) -> Result<OwnedFd, Errno> {
    reopen(fd, OFlags::RDONLY)
}

/// Reopen a verified directory for listing.
pub fn reopen_dir(fd: BorrowedFd<'_>) -> Result<OwnedFd, Errno> {
    reopen(fd, OFlags::RDONLY | OFlags::DIRECTORY)
}

/// True when `dir` has an entry `name`, whatever it is; a symlink counts
/// without being followed. Only existence is reported: an entry's kind is
/// decided from its own descriptor once it is opened.
pub fn entry_exists(dir: BorrowedFd<'_>, name: &[u8]) -> Result<bool, Errno> {
    match retry_on_intr(|| rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)) {
        Ok(_) => Ok(true),
        Err(Errno::NOENT) => Ok(false),
        Err(e) => Err(e),
    }
}

/// List a verified directory: reopen the object behind its `O_PATH`
/// descriptor and iterate the entries. `.` and `..` are skipped, and so
/// is every entry whose type hint says that it can never be indexed nor
/// contain anything indexable: symlinks, fifos, sockets, and devices.
/// Entry types are only hints (DESIGN.md §4.5): the caller opens each
/// remaining entry and decides its kind from that entry's own descriptor.
pub fn list_dir(
    fd: BorrowedFd<'_>,
) -> Result<impl Iterator<Item = Result<DirEntry, Errno>> + use<>, Errno> {
    let dir = Dir::new(reopen_dir(fd)?)?;
    Ok(dir.filter(|entry| match entry {
        Ok(entry) => {
            let name = entry.file_name().to_bytes();
            name != b"."
                && name != b".."
                && !matches!(
                    entry.file_type(),
                    FileType::Symlink
                        | FileType::Fifo
                        | FileType::Socket
                        | FileType::CharacterDevice
                        | FileType::BlockDevice
                )
        }
        // Errors pass through; iteration ends after the first one.
        Err(_) => true,
    }))
}

/// Run a `std::io` operation again while a signal interrupts it. The
/// standard library retries `EINTR` for `open` and inside its read and
/// write loops, but not for calls such as `mkdir` or `stat`. Do not use
/// this for operations that can complete when they are interrupted, such
/// as `close`.
pub fn retry_interrupted<T>(mut op: impl FnMut() -> std::io::Result<T>) -> std::io::Result<T> {
    loop {
        match op() {
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

/// Create `dir` and its missing ancestors. `std::fs::create_dir_all` is
/// unsuitable: it confirms an existing directory with a check that
/// swallows an interruption and then misreports the directory as a
/// conflicting entry.
pub fn create_dir_all(dir: &Path) -> std::io::Result<()> {
    // The empty parent of a bare relative filename needs no directories,
    // as for `std::fs::create_dir_all`.
    if dir.as_os_str().is_empty() {
        return Ok(());
    }
    let mut prefixes: Vec<&Path> = dir.ancestors().collect();
    prefixes.reverse();
    // `mkdir` on an existing ancestor is safe, including `/` and
    // directories that deny writing: Linux reports `EEXIST` for an
    // existing entry before the write-permission and read-only checks.
    for prefix in prefixes {
        if prefix.as_os_str().is_empty() {
            continue;
        }
        match retry_on_intr(|| rustix::fs::mkdir(prefix, Mode::from_raw_mode(0o777))) {
            Ok(()) | Err(Errno::EXIST) => {}
            Err(errno) => return Err(errno.into()),
        }
    }
    // `EXIST` also covers an existing entry that is not a directory.
    if retry_interrupted(|| std::fs::metadata(dir))?.is_dir() {
        Ok(())
    } else {
        Err(std::io::ErrorKind::NotADirectory.into())
    }
}

/// A standard descriptor as an `io::Read` that never reports `EINTR`.
/// The standard library's `Stdin` cannot be used instead: it takes a
/// process-wide reentrant lock whose guard cannot move to another thread,
/// and the transfer roles hand their standard streams between threads.
pub struct FdReader(BorrowedFd<'static>);

impl std::io::Read for FdReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(retry_on_intr(|| rustix::io::read(self.0, &mut *buf))?)
    }
}

/// A standard descriptor as an `io::Write` that never reports `EINTR`.
/// Writes are unbuffered, so `flush` has nothing to do; `Stdout` is
/// unsuitable because it inserts a line buffer.
pub struct FdWriter(BorrowedFd<'static>);

impl std::io::Write for FdWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(retry_on_intr(|| rustix::io::write(self.0, buf))?)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn stdin() -> FdReader {
    FdReader(rustix::stdio::stdin())
}

pub fn stdout() -> FdWriter {
    FdWriter(rustix::stdio::stdout())
}

pub fn stderr() -> FdWriter {
    FdWriter(rustix::stdio::stderr())
}

/// `statx` on the descriptor itself, requiring every field that traversal
/// depends on. A kernel that omits one of them gets `NOTSUP`.
pub fn stat(fd: BorrowedFd<'_>) -> Result<Meta, Errno> {
    const REQUIRED: StatxFlags = StatxFlags::TYPE
        .union(StatxFlags::INO)
        .union(StatxFlags::SIZE)
        .union(StatxFlags::CTIME)
        .union(StatxFlags::MNT_ID);
    let stx = retry_on_intr(|| rustix::fs::statx(fd, "", AtFlags::EMPTY_PATH, REQUIRED))?;
    if !StatxFlags::from_bits_retain(stx.stx_mask).contains(REQUIRED) {
        return Err(Errno::NOTSUP);
    }
    let ctime_ns = stx
        .stx_ctime
        .tv_sec
        .wrapping_mul(1_000_000_000)
        .wrapping_add(i64::from(stx.stx_ctime.tv_nsec));
    Ok(Meta {
        kind: FileType::from_raw_mode(stx.stx_mode.into()),
        size: stx.stx_size,
        ctime_ns,
        ino: stx.stx_ino,
        mnt_id: stx.stx_mnt_id,
        dev_major: stx.stx_dev_major,
        dev_minor: stx.stx_dev_minor,
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};

    use super::*;

    #[test]
    fn interrupted_std_operations_are_retried() {
        let mut attempts = 0;
        let result = retry_interrupted(|| {
            attempts += 1;
            if attempts < 3 {
                Err(Error::from(ErrorKind::Interrupted))
            } else {
                Ok(attempts)
            }
        });
        assert_eq!(result.unwrap(), 3);
    }

    #[test]
    fn create_dir_all_handles_existing_entries() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a/b/c");
        create_dir_all(&nested).unwrap();
        create_dir_all(&nested).unwrap();
        assert!(nested.is_dir());
        let file = dir.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let error = create_dir_all(&file).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotADirectory);
        assert!(create_dir_all(&file.join("d")).is_err());
    }

    #[test]
    fn create_dir_all_accepts_an_empty_path() {
        // The parent of a bare relative filename.
        create_dir_all(Path::new("")).unwrap();
    }

    #[test]
    fn other_std_errors_are_reported() {
        let error =
            retry_interrupted(|| -> std::io::Result<()> { Err(Error::from(ErrorKind::NotFound)) })
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }
}
