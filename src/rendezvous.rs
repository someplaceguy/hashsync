//! The rendezvous socket between `serve` and `attach` (DESIGN.md §6.1).
//!
//! `serve` listens on `$XDG_RUNTIME_DIR/hashsync/T.sock`, falling back to
//! a private directory under `/tmp`. `attach` retries a not-yet-listening
//! socket while its stdin remains open, with no deadline. Both waits notice
//! when the receiver goes away: `attach` polls its stdin, whose write side
//! closes when the SSH session dies, and `serve`'s session watcher waits
//! for the same hangup on `serve`'s stdin (see `Listener::accept`).

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::Duration;

use rustix::event::{PollFd, PollFlags};
use rustix::fs::{FileType, Mode, OFlags};
use rustix::io::retry_on_intr;

use crate::die::{self, die};
use crate::fsops::{any_ready, retry_interrupted};
use crate::names;

const RETRY_INTERVAL: Duration = Duration::from_millis(20);

/// A fresh rendezvous token: the receiver generates it, both sender
/// entry points receive it with `--token`.
pub fn new_token() -> String {
    names::hex(&names::random_bytes::<16>())
}

/// The token as a safe path component: exactly the form `new_token`
/// produces. Both entry points receive it from our own receiver.
fn checked(token: &str) -> &str {
    if token.len() != 32 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
        die(format!("invalid token {token:?}"));
    }
    token
}

/// The directory holding rendezvous sockets, without creating it.
fn socket_dir() -> PathBuf {
    if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR")
        && runtime.as_encoded_bytes().first() == Some(&b'/')
    {
        return PathBuf::from(runtime).join("hashsync");
    }
    let uid = rustix::process::geteuid().as_raw();
    PathBuf::from(format!("/tmp/hashsync-{uid}"))
}

/// Open the existing `dir` and require this user's private directory:
/// owned by the effective UID, mode 0700, not reached through a symlink.
/// Anything else dies; sockets must not live in foreign directories.
/// The returned descriptor anchors the socket operations.
fn open_private(dir: &std::path::Path) -> OwnedFd {
    let flags = OFlags::PATH | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let fd = match retry_on_intr(|| rustix::fs::open(dir, flags, Mode::empty())) {
        Ok(fd) => fd,
        Err(e) => die(format!("cannot open {}: {e}", dir.display())),
    };
    let stat = match rustix::fs::fstat(&fd) {
        Ok(stat) => stat,
        Err(e) => die(format!("cannot stat {}: {e}", dir.display())),
    };
    let insecure = if stat.st_uid != rustix::process::geteuid().as_raw() {
        Some("is not owned by this user")
    } else if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
        Some("is not a directory")
    } else if stat.st_mode & 0o077 != 0 {
        Some("is accessible by other users")
    } else {
        None
    };
    if let Some(problem) = insecure {
        die(format!("{} {problem}", dir.display()));
    }
    fd
}

/// The socket's address through the verified directory's descriptor.
/// `sockaddr_un` paths are limited to about 107 bytes, which a long
/// `XDG_RUNTIME_DIR` would exceed; this procfs path stays short and
/// resolves inside the checked directory, like the reopening rule of
/// DESIGN.md §4.5.
fn address_in(dir: BorrowedFd<'_>, name: &str) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}/{name}", dir.as_raw_fd()))
}

/// True when this process's stdin has no writer left: the SSH session
/// that carries the peer is gone.
fn stdin_gone() -> bool {
    // No event is requested, so only a hangup or an error makes stdin
    // ready: `poll` always reports those. Pending readable data must not
    // end the wait.
    any_ready(&mut [PollFd::new(&rustix::stdio::stdin(), PollFlags::empty())])
}

/// `serve`'s listening socket. The socket file is removed when the
/// process ends, normally or fatally.
pub struct Listener {
    listener: UnixListener,
}

/// Create the socket directory if needed and listen (DESIGN.md §6.1:
/// `serve` first listens, so `attach`'s retries can end).
pub fn listen(token: &str) -> Listener {
    let dir = socket_dir();
    match retry_on_intr(|| rustix::fs::mkdir(&dir, Mode::from_raw_mode(0o700))) {
        Ok(()) | Err(rustix::io::Errno::EXIST) => {}
        Err(e) => die(format!("cannot create {}: {e}", dir.display())),
    }
    let dir_fd = open_private(&dir);
    let name = format!("{}.sock", checked(token));
    let path = dir.join(&name);
    let address = address_in(dir_fd.as_fd(), &name);
    // Register before binding, so a concurrent end cannot miss the socket
    // file; removing a file that does not exist yet does nothing.
    die::remove_at_exit(path.clone());
    let listener = match retry_interrupted(|| UnixListener::bind(&address)) {
        Ok(listener) => listener,
        Err(e) => die(format!("cannot listen on {}: {e}", path.display())),
    };
    // The directory already denies other users; make the socket itself
    // 0600 too.
    if let Err(e) = retry_interrupted(|| {
        std::fs::set_permissions(&address, std::fs::Permissions::from_mode(0o600))
    }) {
        die(format!("cannot protect {}: {e}", path.display()));
    }
    Listener { listener }
}

impl Listener {
    /// Accept the control connection. The wait needs no check of its own:
    /// nothing in `serve` reads stdin meanwhile, but the input's session
    /// watcher sees the hangup when the receiver goes away, and ends the
    /// run (DESIGN.md §6.9).
    pub fn accept(&self) -> UnixStream {
        match retry_interrupted(|| self.listener.accept()) {
            Ok((stream, _)) => stream,
            Err(e) => die(format!("accept: {e}")),
        }
    }
}

/// `attach`'s connection: retry until `serve` listens, with no deadline,
/// while stdin remains open.
pub fn connect(token: &str) -> UnixStream {
    let dir = socket_dir();
    let name = format!("{}.sock", checked(token));
    let path = dir.join(&name);
    // The directory appears when `serve` creates it; open and verify it
    // once, then connect through its descriptor.
    let mut dir_fd: Option<OwnedFd> = None;
    loop {
        let Some(fd) = &dir_fd else {
            match retry_interrupted(|| dir.try_exists()) {
                Ok(true) => dir_fd = Some(open_private(&dir)),
                Ok(false) | Err(_) => {
                    wait_for_serve(&path, &"the socket directory does not exist yet");
                }
            }
            continue;
        };
        let address = address_in(fd.as_fd(), &name);
        match retry_interrupted(|| UnixStream::connect(&address)) {
            Ok(stream) => return stream,
            // Not created or not listening yet. Retrying `connect` on
            // `EINTR` is safe for Unix sockets (DESIGN.md §3).
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                wait_for_serve(&path, &e);
            }
            Err(e) => die(format!("cannot connect to {}: {e}", path.display())),
        }
    }
}

fn wait_for_serve(path: &std::path::Path, reason: &dyn std::fmt::Display) {
    if stdin_gone() {
        die(format!(
            "the receiver disappeared while waiting for {}: {reason}",
            path.display()
        ));
    }
    std::thread::sleep(RETRY_INTERVAL);
}
