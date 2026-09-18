//! Descriptor-relative filesystem access (DESIGN.md §4.5).
//!
//! Entries are opened with `O_PATH|O_NOFOLLOW` for classification with
//! `statx`. An object that needs I/O is reopened through
//! `/proc/self/fd/N`, which reopens the verified object even after a rename
//! or unlink and never follows a symlink in the original pathname.

use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};

use rustix::fs::{AtFlags, FileType, Mode, OFlags, StatxFlags};
use rustix::io::Errno;

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
    rustix::fs::open(
        "/",
        OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
}

/// Open the entry `name` of `dir` without following symlinks, for
/// classification only.
pub fn open_path(dir: BorrowedFd<'_>, name: &[u8]) -> Result<OwnedFd, Errno> {
    rustix::fs::openat(dir, name, PATH_FLAGS, Mode::empty())
}

/// Reopen the object behind an `O_PATH` descriptor for I/O.
fn reopen(fd: BorrowedFd<'_>, flags: OFlags) -> Result<OwnedFd, Errno> {
    let path = format!("/proc/self/fd/{}", fd.as_raw_fd());
    rustix::fs::open(path, flags | OFlags::CLOEXEC, Mode::empty())
}

/// Reopen a verified regular file for reading.
pub fn reopen_file(fd: BorrowedFd<'_>) -> Result<OwnedFd, Errno> {
    reopen(fd, OFlags::RDONLY)
}

/// Reopen a verified directory for listing.
pub fn reopen_dir(fd: BorrowedFd<'_>) -> Result<OwnedFd, Errno> {
    reopen(fd, OFlags::RDONLY | OFlags::DIRECTORY)
}

/// `statx` on the descriptor itself, requiring every field that traversal
/// depends on. A kernel that omits one of them gets `NOTSUP`.
pub fn stat(fd: BorrowedFd<'_>) -> Result<Meta, Errno> {
    const REQUIRED: StatxFlags = StatxFlags::TYPE
        .union(StatxFlags::INO)
        .union(StatxFlags::SIZE)
        .union(StatxFlags::CTIME)
        .union(StatxFlags::MNT_ID);
    let stx = rustix::fs::statx(fd, "", AtFlags::EMPTY_PATH, REQUIRED)?;
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
