//! Descriptor-relative filesystem access (DESIGN.md §4.5).
//!
//! Entries are opened with `O_PATH|O_NOFOLLOW` for classification with
//! `statx`. An object that needs I/O is reopened through
//! `/proc/self/fd/N`, which reopens the verified object even after a rename
//! or unlink and never follows a symlink in the original pathname.
//!
//! Every wrapper retries a call that a signal interrupts, so callers never
//! see `EINTR`.

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::path::Path;

use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fs::{AtFlags, Dir, DirEntry, FileType, Mode, OFlags, ResolveFlags, StatxFlags};
use rustix::io::{Errno, retry_on_intr};
use rustix::process::{Resource, getrlimit, setrlimit};

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

/// Open the relative path `rel` beneath the directory `dir` for
/// classification only, without following a symlink in any component and
/// without leaving `dir` (DESIGN.md §6.4). A final symlink yields a
/// descriptor of the link itself.
pub fn open_beneath(dir: BorrowedFd<'_>, rel: &[u8]) -> Result<OwnedFd, Errno> {
    let resolve = ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS;
    retry_on_intr(|| rustix::fs::openat2(dir, rel, PATH_FLAGS, Mode::empty(), resolve))
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

/// Raise the soft descriptor limit to the hard limit (DESIGN.md §4.6):
/// traversal tasks hold their parents' descriptors, and root locations
/// hold theirs for a whole run.
pub fn raise_descriptor_limit() {
    let mut limit = getrlimit(Resource::Nofile);
    if limit.current != limit.maximum {
        limit.current = limit.maximum;
        // Failure only means fewer descriptors; opens report it later.
        let _ = retry_on_intr(|| setrlimit(Resource::Nofile, limit));
    }
}

/// A standard descriptor as an `io::Read` that never reports `EINTR` or
/// `EAGAIN` (`blocking_read`). The standard library's `Stdin` cannot be
/// used instead: it takes a process-wide reentrant lock whose guard cannot
/// move to another thread, and the transfer roles hand their standard
/// streams between threads.
pub struct FdReader(BorrowedFd<'static>);

impl std::io::Read for FdReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(blocking_read(self.0, buf)?)
    }
}

/// A standard descriptor as an `io::Write` that never reports `EINTR` or
/// `EAGAIN` (`blocking_write`). Writes are unbuffered, so `flush` has
/// nothing to do; `Stdout` is unsuitable because it inserts a line buffer.
pub struct FdWriter(BorrowedFd<'static>);

impl std::io::Write for FdWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(blocking_write(self.0, buf)?)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A pipe that only this process reads, as the source of a spool that marks
/// its idle points (DESIGN.md §6.5): the command's stdout in eager delivery
/// of the output. The pipe is non-blocking, so a read that finds no byte
/// fails with `EAGAIN`. Then the read fails once with `WouldBlock`, which
/// tells the idle point, and the next read waits for a byte or EOF. EOF is
/// the last idle point: a read that finds EOF also fails once with
/// `WouldBlock`, unless the read before it told an idle point, so that the
/// bytes before EOF do not wait for the end of the command, which can come
/// much later. This type exists so that the idle points cost no system call
/// for a read that finds bytes, and only two more calls at each idle point.
pub struct IdlePointReader<F: AsFd> {
    fd: F,
    /// The last read failed with `WouldBlock`.
    told: bool,
}

impl<F: AsFd> IdlePointReader<F> {
    /// Make `fd` non-blocking. Only the read end changes: the writer of
    /// the pipe has an open file description of its own.
    pub fn new(fd: F) -> Self {
        let flags = rustix::fs::fcntl_getfl(&fd).expect("fcntl F_GETFL on a pipe failed");
        rustix::fs::fcntl_setfl(&fd, flags | OFlags::NONBLOCK)
            .expect("fcntl F_SETFL on a pipe failed");
        IdlePointReader { fd, told: false }
    }
}

impl<F: AsFd> std::io::Read for IdlePointReader<F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // A read into an empty buffer also returns 0, which is not EOF, so
        // it must not tell an idle point.
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            // After the read that told the idle point, wait for a byte or
            // EOF; no timeout (DESIGN.md §3).
            if self.told {
                wait_until_ready(self.fd.as_fd(), PollFlags::IN);
            }
            match retry_on_intr(|| rustix::io::read(&self.fd, &mut *buf)) {
                // Only a spurious wakeup can leave no byte after the wait.
                Err(Errno::AGAIN) if self.told => {}
                // No byte is ready, or EOF comes without an idle point
                // before it.
                Err(Errno::AGAIN) | Ok(0) if !self.told => {
                    self.told = true;
                    return Err(std::io::ErrorKind::WouldBlock.into());
                }
                result => {
                    self.told = false;
                    return Ok(result?);
                }
            }
        }
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

        let kind = |rel: &[u8]| stat(open_beneath(root.as_fd(), rel)?.as_fd()).map(|m| m.kind);
        assert_eq!(kind(b"sub/file"), Ok(FileType::RegularFile));
        // A symlink in the middle cannot be traversed; a final one is not
        // followed.
        assert!(kind(b"link/file").is_err());
        assert_eq!(kind(b"sub/filelink"), Ok(FileType::Symlink));
        for escape in [&b"../outside"[..], b"sub/../../outside", b"/etc/passwd"] {
            assert!(kind(escape).is_err(), "{}", String::from_utf8_lossy(escape));
        }
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
    fn idle_points_are_told_once_and_eof_is_the_last_one() {
        // A read that finds no byte fails once with `WouldBlock`, and the
        // next read waits for a byte. EOF right after bytes is the last
        // idle point: a read fails once with `WouldBlock` before EOF comes.
        // EOF right after an idle point needs no second one (DESIGN.md
        // §6.5).
        use std::io::{Read, Write};

        fn would_block(result: std::io::Result<usize>) -> bool {
            result.is_err_and(|e| e.kind() == ErrorKind::WouldBlock)
        }
        let mut buf = [0u8; 8];
        let (reader, mut writer) = std::io::pipe().unwrap();
        let mut idle = IdlePointReader::new(reader);
        assert!(would_block(idle.read(&mut buf)), "no idle point");
        let (mut idle, read) = unblock_when_asleep(
            move || {
                let mut buf = [0u8; 8];
                let n = idle.read(&mut buf).unwrap();
                (idle, buf[..n].to_vec())
            },
            || writer.write_all(b"ab").unwrap(),
        );
        assert_eq!(read, b"ab");
        writer.write_all(b"cd").unwrap();
        drop(writer);
        assert!(matches!(idle.read(&mut buf), Ok(2)));
        assert!(would_block(idle.read(&mut buf)), "no idle point before EOF");
        assert!(matches!(idle.read(&mut buf), Ok(0)), "no EOF");

        let (reader, writer) = std::io::pipe().unwrap();
        let mut idle = IdlePointReader::new(reader);
        assert!(would_block(idle.read(&mut buf)), "no idle point");
        drop(writer);
        assert!(matches!(idle.read(&mut buf), Ok(0)), "a second idle point");
    }

    #[test]
    fn other_std_errors_are_reported() {
        let error =
            retry_interrupted(|| -> std::io::Result<()> { Err(Error::from(ErrorKind::NotFound)) })
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }
}
