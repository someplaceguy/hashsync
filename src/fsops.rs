// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! Descriptor-relative filesystem access (DESIGN.md §4.5).
//!
//! Entries are opened with `O_PATH|O_NOFOLLOW` for classification with
//! `statx`. An object that needs I/O is reopened through
//! `/proc/self/fd/N`, which reopens the verified object even after a rename
//! or unlink and never follows a symlink in the original pathname.
//!
//! Every wrapper retries a call that a signal interrupts, so callers never
//! see `EINTR`.

use std::io::IoSlice;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::path::Path;
use std::sync::OnceLock;

use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fs::{AtFlags, Dir, DirEntry, FileType, Mode, OFlags, ResolveFlags, StatxFlags};
use rustix::io::{Errno, retry_on_intr};
use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

/// Flags for the initial open of any entry.
pub const PATH_FLAGS: OFlags = OFlags::PATH.union(OFlags::NOFOLLOW).union(OFlags::CLOEXEC);

/// The longest path that the kernel takes in one call: `PATH_MAX` counts
/// the terminating NUL.
const LONGEST_PATH: usize = libc::PATH_MAX as usize - 1;

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

/// Open the relative path `rel` beneath the directory `dir` for
/// classification only, without following a symlink in any component and
/// without leaving `dir` (DESIGN.md §6.4). A final symlink yields a
/// descriptor of the link itself. With `same_mount`, the path must not
/// cross a mount point either, including at a bind-mounted file, and the
/// open fails with `EXDEV` where it would.
///
/// A scan opens one name at a time, so the index can hold paths longer
/// than `LONGEST_PATH`, which the kernel rejects in a single call. Such a
/// path is opened in pieces, each beneath the directory where the previous
/// piece ends. It fails as a shorter path would, at the same name and with
/// the same error, unless it has a `..` or an empty name, which no indexed
/// path has. A name of more than 4093 bytes does not fit in a piece, so
/// the open fails with `ENAMETOOLONG` at such a name if another name
/// follows it, even on a filesystem that takes it, as FUSE can. The pieces
/// can end outside `dir`, though, if a directory on the path moves out of
/// `dir` between two of them, as can happen during a scan.
pub fn open_beneath(dir: BorrowedFd<'_>, rel: &[u8], same_mount: bool) -> Result<OwnedFd, Errno> {
    let mut resolve = ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS;
    if same_mount {
        resolve |= ResolveFlags::NO_XDEV;
    }
    let open = |dir: BorrowedFd<'_>, rel: &[u8]| {
        retry_on_intr(|| rustix::fs::openat2(dir, rel, PATH_FLAGS, Mode::empty(), resolve))
    };
    // The directory where the pieces opened so far end.
    let mut reached: Option<OwnedFd> = None;
    let mut rest = rel;
    while rest.len() > LONGEST_PATH {
        // Each piece ends with `/.`, so that the kernel resolves the last
        // name of the piece as a name in the middle of a path: a symlink
        // there fails with `ELOOP`, and anything but a directory with
        // `ENOTDIR`. With a `/` alone, that name would still be the last
        // one, and the kernel checks a symlink there against
        // `fs.protected_symlinks` before `RESOLVE_NO_SYMLINKS`, so the open
        // could fail with `EACCES` instead.
        let Some(slash) = rest[..LONGEST_PATH - 1].iter().rposition(|&b| b == b'/') else {
            // The next name does not fit in a piece with the `/.` after it.
            // Most filesystems reject a name that long with this error.
            return Err(Errno::NAMETOOLONG);
        };
        let (piece, tail) = rest.split_at(slash + 1);
        let piece = [piece, b"."].concat();
        reached = Some(open(reached.as_ref().map_or(dir, AsFd::as_fd), &piece)?);
        rest = tail;
    }
    open(reached.as_ref().map_or(dir, AsFd::as_fd), rest)
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

/// Create `dir` and its missing ancestors. `dir` itself gets `mode`, the
/// ancestors 0777; the umask applies to both, and existing directories
/// keep their modes. `std::fs::create_dir_all` is unsuitable: it confirms
/// an existing directory with a check that swallows an interruption and
/// then misreports the directory as a conflicting entry.
pub fn create_dir_all(dir: &Path, mode: u32) -> std::io::Result<()> {
    let mut prefixes: Vec<&Path> = dir.ancestors().collect();
    prefixes.reverse();
    // `mkdir` on an existing ancestor is safe, including `/` and
    // directories that deny writing: Linux reports `EEXIST` for an
    // existing entry before the write-permission and read-only checks.
    for prefix in prefixes {
        if prefix.as_os_str().is_empty() {
            continue;
        }
        let mode = if prefix == dir { mode } else { 0o777 };
        match retry_on_intr(|| rustix::fs::mkdir(prefix, Mode::from_raw_mode(mode))) {
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

/// The descriptor limit that this process had before the first
/// `raise_descriptor_limit`.
static ORIGINAL_DESCRIPTOR_LIMIT: OnceLock<Rlimit> = OnceLock::new();

/// Raise the soft descriptor limit to the hard limit (DESIGN.md §4.6):
/// traversal tasks hold their parents' descriptors, and root locations
/// hold theirs for a whole run. The children of this process start with
/// the limit from before the first raise (`original_descriptor_limit`).
pub fn raise_descriptor_limit() {
    let mut limit = getrlimit(Resource::Nofile);
    // A later raise finds the limit raised, and keeps the first one.
    let _ = ORIGINAL_DESCRIPTOR_LIMIT.set(limit);
    if limit.current != limit.maximum {
        limit.current = limit.maximum;
        // Failure only means fewer descriptors; opens report it later.
        let _ = retry_on_intr(|| setrlimit(Resource::Nofile, limit));
    }
}

/// The descriptor limit from before the first `raise_descriptor_limit`,
/// or `None` if it never ran. This function exists so that a child, such
/// as the command of a run whose server updated its index for a
/// deduplicated forward stream, starts with the limit that it would have
/// under SSH alone (`die::spawn_guarded`):
/// a program that takes the soft limit as a hint, such as one that sizes
/// a table by it, must not see this process's need.
pub fn original_descriptor_limit() -> Option<Rlimit> {
    ORIGINAL_DESCRIPTOR_LIMIT.get().copied()
}

/// A standard descriptor as an `io::Read` that never reports `EINTR` or
/// `EAGAIN` (`blocking_read`). The standard library's `Stdin` cannot be
/// used instead: it takes a process-wide reentrant lock whose guard cannot
/// move to another thread, and the client and the server hand their
/// standard streams between threads.
pub struct FdReader(BorrowedFd<'static>);

impl std::io::Read for FdReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(blocking_read(self.0, buf)?)
    }
}

/// A standard descriptor as an `io::Write` that never reports `EINTR` or
/// `EAGAIN` (`blocking_write`). Writes are unbuffered, so `flush` has
/// nothing to do; `Stdout` is unsuitable because it inserts a line buffer.
/// The descriptor is generic so that a test can give it a socket that
/// keeps the bytes of each write call apart.
pub struct FdWriter<F: AsFd = BorrowedFd<'static>>(F);

impl<F: AsFd> std::io::Write for FdWriter<F> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(blocking_write(self.0.as_fd(), buf)?)
    }

    /// One `writev` of all of `bufs`, which `FrameWriter::send` needs: the
    /// default would write only the first buffer, so the server's data
    /// session would send the header of each frame alone, which can cost
    /// an SSH packet of its own (DESIGN.md §6.2).
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        Ok(blocking_writev(self.0.as_fd(), bufs)?)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Read `fd` as a blocking descriptor is read, also when it is not
/// blocking. This function exists because a standard descriptor shares
/// its open file description with other processes, and one of them can
/// make it non-blocking (DESIGN.md §3). Then a read with no bytes ready
/// fails with `EAGAIN`, and the wait happens here instead.
fn blocking_read(fd: BorrowedFd<'_>, buf: &mut [u8]) -> Result<usize, Errno> {
    loop {
        match retry_on_intr(|| rustix::io::read(fd, &mut *buf)) {
            Err(Errno::AGAIN) => wait_until_ready(fd, PollFlags::IN),
            result => return result,
        }
    }
}

/// Write `fd` as a blocking descriptor is written, also when it is not
/// blocking, as `blocking_read` reads it.
fn blocking_write(fd: BorrowedFd<'_>, buf: &[u8]) -> Result<usize, Errno> {
    loop {
        match retry_on_intr(|| rustix::io::write(fd, buf)) {
            Err(Errno::AGAIN) => wait_until_ready(fd, PollFlags::OUT),
            result => return result,
        }
    }
}

/// `blocking_write` for several buffers in one `writev`.
fn blocking_writev(fd: BorrowedFd<'_>, bufs: &[IoSlice<'_>]) -> Result<usize, Errno> {
    loop {
        match retry_on_intr(|| rustix::io::writev(fd, bufs)) {
            Err(Errno::AGAIN) => wait_until_ready(fd, PollFlags::OUT),
            result => return result,
        }
    }
}

/// Wait until `fd` is ready for `events`, or until a hangup or an error,
/// which the next read or write reports; no timeout (DESIGN.md §3).
fn wait_until_ready(fd: BorrowedFd<'_>, events: PollFlags) {
    poll_fds(&mut [PollFd::new(&fd, events)]);
}

/// Wait with `poll` until one of the descriptors of `fds` is ready, without
/// a timeout (DESIGN.md §3). The ready events are in `fds`. Interrupted
/// calls are retried, and a failure panics.
pub fn poll_fds(fds: &mut [PollFd<'_>]) {
    retry_on_intr(|| poll(fds, None)).unwrap_or_else(|e| panic!("poll: {e}"));
}

/// True when one of the descriptors of `fds` is ready now: `poll` only
/// looks, and does not wait. The ready events are in `fds`, as after
/// `poll_fds`. A failure panics.
pub fn any_ready(fds: &mut [PollFd<'_>]) -> bool {
    let ready = retry_on_intr(|| poll(fds, Some(&Timespec::default())));
    ready.unwrap_or_else(|e| panic!("poll: {e}")) > 0
}

/// True when a read of `fd` would find its EOF now. This function finds out
/// without a read, so that the bytes of `fd` stay for its next reader. A
/// descriptor that can seek, such as a file, a block device or `/dev/null`,
/// is at its EOF when a `pread` at its offset, which leaves the offset where
/// it is, finds no byte. One that cannot seek, such as a pipe, a socket or a
/// terminal, is at its EOF when its other end has hung up and `FIONREAD`
/// shows that it holds no byte. So a terminal never is: Ctrl-D is no
/// hangup, and `FIONREAD` fails on a terminal that has hung up.
pub fn at_eof(fd: BorrowedFd<'_>) -> bool {
    match rustix::fs::tell(fd) {
        Ok(offset) => retry_on_intr(|| rustix::io::pread(fd, &mut [0u8; 1], offset)) == Ok(0),
        Err(Errno::SPIPE) => {
            let hung_up = PollFlags::HUP | PollFlags::RDHUP;
            let mut fds = [PollFd::new(&fd, PollFlags::IN | PollFlags::RDHUP)];
            any_ready(&mut fds)
                && fds[0].revents().intersects(hung_up)
                && rustix::io::ioctl_fionread(fd) == Ok(0)
        }
        // A descriptor that it cannot look at, such as a closed one.
        Err(_) => false,
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
    use std::os::fd::AsFd;

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
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a/b/c");
        create_dir_all(&nested, 0o700).unwrap();
        create_dir_all(&nested, 0o777).unwrap();
        assert!(nested.is_dir());
        let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&nested), 0o700, "the directory itself gets the mode");
        assert_ne!(mode(&dir.path().join("a")), 0o700, "ancestors do not");
        let file = dir.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let error = create_dir_all(&file, 0o777).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotADirectory);
        assert!(create_dir_all(&file.join("d"), 0o777).is_err());
    }

    #[test]
    fn open_beneath_rejects_symlinks_and_escapes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("root/sub")).unwrap();
        std::fs::write(dir.path().join("root/sub/file"), b"data").unwrap();
        std::fs::write(dir.path().join("outside"), b"secret").unwrap();
        std::os::unix::fs::symlink("sub", dir.path().join("root/link")).unwrap();
        std::os::unix::fs::symlink("file", dir.path().join("root/sub/filelink")).unwrap();
        let root = rustix::fs::open(
            dir.path().join("root").as_path(),
            PATH_FLAGS | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .unwrap();

        let kind =
            |rel: &[u8]| stat(open_beneath(root.as_fd(), rel, false)?.as_fd()).map(|m| m.kind);
        assert_eq!(kind(b"sub/file"), Ok(FileType::RegularFile));
        // A symlink in the middle cannot be traversed; a final one is not
        // followed.
        assert!(kind(b"link/file").is_err());
        assert_eq!(kind(b"sub/filelink"), Ok(FileType::Symlink));
        for escape in [&b"../outside"[..], b"sub/../../outside", b"/etc/passwd"] {
            assert!(kind(escape).is_err(), "{}", String::from_utf8_lossy(escape));
        }
    }

    #[test]
    fn open_beneath_opens_long_paths_in_pieces() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("root")).unwrap();
        std::fs::write(dir.path().join("outside"), b"secret").unwrap();
        let root = rustix::fs::open(
            dir.path().join("root").as_path(),
            PATH_FLAGS | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .unwrap();
        // A chain of 33 directories with names of 255 bytes, the longest
        // that Linux filesystems take, and a file at its end. Each 16 names
        // of the path take 4096 bytes with their `/`, one byte more than
        // `LONGEST_PATH`, so a piece that ended at the 16th `/` would be too
        // long, and the path needs three pieces. A symlink beside the 15th
        // directory ends the first piece, and one beside the 16th starts the
        // second.
        let name = [b'd'; 255];
        let link = [b'l'; 255];
        let mut chain = open_path(root.as_fd(), b".").unwrap();
        for depth in 0..33 {
            rustix::fs::mkdirat(&chain, &name[..], Mode::from_raw_mode(0o755)).unwrap();
            if depth == 14 || depth == 15 {
                rustix::fs::symlinkat(&name[..], &chain, &link[..]).unwrap();
            }
            chain = open_path(chain.as_fd(), &name).unwrap();
        }
        let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::CLOEXEC;
        rustix::fs::openat(&chain, "f", flags, Mode::from_raw_mode(0o644)).unwrap();

        let kind =
            |rel: &[u8]| stat(open_beneath(root.as_fd(), rel, false)?.as_fd()).map(|m| m.kind);
        let mut names = vec![&name[..]; 33];
        names.push(b"f");
        assert_eq!(kind(&names.join(&b'/')), Ok(FileType::RegularFile));
        // A symlink fails as in a short path, at the end of a piece too.
        for depth in [14, 15] {
            let mut names = names.clone();
            names[depth] = &link;
            assert_eq!(kind(&names.join(&b'/')), Err(Errno::LOOP), "{depth}");
        }
        // A `..` cannot climb above the directory where its piece starts:
        // the second of these 17 fails, before they reach `outside` beside
        // the root.
        let (prefix, up) = (names[..16].join(&b'/'), b"../".repeat(17));
        let escape: [&[u8]; 4] = [&prefix, b"/", &up, b"outside"];
        assert_eq!(kind(&escape.concat()), Err(Errno::XDEV));
        assert_eq!(kind(&[b'x'; LONGEST_PATH + 1]), Err(Errno::NAMETOOLONG));
    }

    /// The state of the thread `tid` of this process, as `/proc` shows it,
    /// such as `S` while it sleeps; `None` once it has ended.
    fn thread_state(tid: rustix::thread::Pid) -> Option<char> {
        let path = format!("/proc/self/task/{}/stat", tid.as_raw_nonzero());
        let stat = std::fs::read_to_string(path).ok()?;
        // The state follows the name, which ends with the last `)`.
        let after_name = &stat[stat.rfind(')')? + 1..];
        after_name.split_whitespace().next()?.chars().next()
    }

    /// Run `blocked` on a new thread, then `unblock` on this thread once
    /// that thread sleeps, and return what `blocked` returned. A `blocked`
    /// that returns without a sleep ends the wait too, so a fault cannot
    /// hang the test. The wait has no timeout.
    fn unblock_when_asleep<T: Send + 'static>(
        blocked: impl FnOnce() -> T + Send + 'static,
        unblock: impl FnOnce(),
    ) -> T {
        let (tid_tx, tid_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            tid_tx.send(rustix::thread::gettid()).unwrap();
            blocked()
        });
        let tid = tid_rx.recv().unwrap();
        while !thread.is_finished() && thread_state(tid) != Some('S') {
            std::thread::yield_now();
        }
        unblock();
        thread.join().unwrap()
    }

    #[test]
    fn non_blocking_descriptors_wait_as_blocking_ones_do() {
        // Another process can make a standard descriptor that it shares
        // non-blocking (DESIGN.md §3). Then a write to a full pipe waits
        // for room, and a read of an empty pipe waits for bytes, where
        // both would fail with EAGAIN.
        let (reader, writer) = std::io::pipe().unwrap();
        rustix::fs::fcntl_setfl(&reader, OFlags::NONBLOCK).unwrap();
        rustix::fs::fcntl_setfl(&writer, OFlags::NONBLOCK).unwrap();
        let mut full = 0;
        while let Ok(n) = rustix::io::write(&writer, &[1u8; 4096]) {
            full += n;
        }
        let written = {
            let writer = writer.try_clone().unwrap();
            let drain = || {
                let mut buf = vec![0u8; full];
                let mut drained = 0;
                while drained < full {
                    drained += rustix::io::read(&reader, &mut buf[drained..]).unwrap();
                }
            };
            unblock_when_asleep(move || blocking_write(writer.as_fd(), b"x"), drain)
        };
        assert_eq!(written, Ok(1));
        assert_eq!(rustix::io::read(&reader, &mut [0u8; 8][..]), Ok(1));
        let read = {
            let reader = reader.try_clone().unwrap();
            let read = move || {
                let mut buf = [0u8; 8];
                blocking_read(reader.as_fd(), &mut buf).map(|n| buf[..n].to_vec())
            };
            let fill = || assert_eq!(rustix::io::write(&writer, b"y"), Ok(1));
            unblock_when_asleep(read, fill)
        };
        assert_eq!(read, Ok(b"y".to_vec()));
    }

    #[test]
    fn at_eof_finds_the_eof_without_a_read() {
        // The client looks at stdin this way when the stop of the forward
        // stream ends its reads (DESIGN.md §6.6). The look takes none of
        // the bytes: each descriptor here still gives them after its looks.
        use std::io::{Read, Seek, SeekFrom, Write};
        use std::net::Shutdown;
        use std::os::unix::net::UnixStream;

        use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};

        // A file is at its EOF only at its end, and the look leaves its
        // offset where it was.
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"ab").unwrap();
        file.seek(SeekFrom::Start(1)).unwrap();
        assert!(!at_eof(file.as_fd()));
        let mut rest = Vec::new();
        file.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, b"b");
        assert!(at_eof(file.as_fd()));
        // `/dev/null` always is, `/dev/zero` never.
        assert!(at_eof(std::fs::File::open("/dev/null").unwrap().as_fd()));
        assert!(!at_eof(std::fs::File::open("/dev/zero").unwrap().as_fd()));

        // A pipe is at its EOF once its writer has closed it and it holds
        // no byte.
        let (mut reader, mut writer) = std::io::pipe().unwrap();
        assert!(!at_eof(reader.as_fd()));
        writer.write_all(b"x").unwrap();
        drop(writer);
        assert!(!at_eof(reader.as_fd()));
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, b"x");
        assert!(at_eof(reader.as_fd()));

        // A socket is at its EOF once its peer has shut down its writes,
        // with `POLLRDHUP` and no `POLLHUP`, and it holds no byte.
        let (mut socket, mut peer) = UnixStream::pair().unwrap();
        assert!(!at_eof(socket.as_fd()));
        peer.write_all(b"y").unwrap();
        peer.shutdown(Shutdown::Write).unwrap();
        assert!(!at_eof(socket.as_fd()));
        let mut rest = Vec::new();
        socket.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, b"y");
        assert!(at_eof(socket.as_fd()));

        // A terminal never is, also once its user typed Ctrl-D, which the
        // next read would find.
        let master = match openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY | OpenptFlags::CLOEXEC) {
            Ok(master) => master,
            Err(e) => {
                eprintln!("skipped the terminal: no pseudo-terminals: {e}");
                return;
            }
        };
        grantpt(&master).unwrap();
        unlockpt(&master).unwrap();
        let path = ptsname(&master, Vec::new()).unwrap();
        let flags = OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC;
        let terminal = rustix::fs::open(path.as_c_str(), flags, Mode::empty()).unwrap();
        assert!(!at_eof(terminal.as_fd()));
        assert_eq!(rustix::io::write(&master, b"\x04"), Ok(1));
        // The line discipline takes the Ctrl-D in on its own time.
        poll_fds(&mut [PollFd::new(&terminal, PollFlags::IN)]);
        assert!(!at_eof(terminal.as_fd()));
        assert_eq!(rustix::io::read(&terminal, &mut [0u8; 8]), Ok(0));
    }

    #[test]
    fn a_frame_is_one_write_call_of_a_standard_descriptor() {
        // The server writes its data session to its stdout through this
        // writer. A datagram socket keeps the bytes of each write call
        // apart, and here each frame must arrive as one datagram: header
        // and payload together (DESIGN.md §6.2).
        use crate::protocol::{FrameWriter, frame};

        let (writer, reader) = std::os::unix::net::UnixDatagram::pair().unwrap();
        let mut frames = FrameWriter::new(FdWriter(writer));
        frames.send(frame::CHUNK, &[7; 1000]).unwrap();
        frames.send(frame::STREAM_END, b"").unwrap();
        let mut buf = vec![0u8; 4096];
        let n = reader.recv(&mut buf).unwrap();
        assert_eq!(buf[..5], [frame::CHUNK, 0xe8, 3, 0, 0]);
        assert_eq!(n, 1005, "the header went out alone");
        let n = reader.recv(&mut buf).unwrap();
        assert_eq!(buf[..n], [frame::STREAM_END, 0, 0, 0, 0]);
    }

    #[test]
    fn other_std_errors_are_reported() {
        let error =
            retry_interrupted(|| -> std::io::Result<()> { Err(Error::from(ErrorKind::NotFound)) })
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }
}
