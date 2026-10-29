// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! The rendezvous socket between `serve` and `attach` (DESIGN.md §6.1).
//!
//! `serve` listens on `$XDG_RUNTIME_DIR/hashsync/T.sock`, falling back to
//! a private directory under `/tmp`. `attach` retries a not-yet-listening
//! socket while its stdin remains open, with no deadline. Both waits notice
//! when the client goes away: `attach` polls its stdin, whose write side
//! closes when the SSH session dies, and `serve`'s session watcher waits
//! for the same hangup on `serve`'s stdin (see `Listener::accept`).
//!
//! Once connected, each end checks that the process at the other end runs
//! as the same user (see `foreign_peer`).

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::Duration;

use rustix::event::{PollFd, PollFlags};
use rustix::fs::{FileType, Mode, OFlags};
use rustix::io::{Errno, retry_on_intr};

use crate::die::{self, die};
use crate::escapes::{quoted, quoted_path};
use crate::fsops::{self, any_ready, retry_interrupted};
use crate::names;

const RETRY_INTERVAL: Duration = Duration::from_millis(20);

/// A fresh rendezvous token: the client generates it, and both server
/// entry points receive it with `--token`.
pub fn new_token() -> String {
    names::hex(&names::random_bytes::<16>())
}

/// The token as a safe path component: exactly the form `new_token`
/// produces. Both entry points receive it from our own client.
fn checked(token: &str) -> &str {
    if token.len() != 32 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
        die(format!("invalid token {}", quoted(token.as_bytes())));
    }
    token
}

/// The directory that holds this user's rendezvous sockets:
/// `$XDG_RUNTIME_DIR/hashsync`, or `/tmp/hashsync-UID` when that variable
/// doesn't hold an absolute path. It's held as a descriptor of its parent
/// and its name there, so that the directory is created and opened beneath
/// the parent, never by its path.
struct SocketDir {
    parent: OwnedFd,
    name: String,
    /// The directory's path, for messages.
    path: PathBuf,
}

impl SocketDir {
    /// Open the parent of this user's socket directory. Creates nothing.
    fn new() -> SocketDir {
        let (parent, name) = match std::env::var_os("XDG_RUNTIME_DIR") {
            Some(runtime) if runtime.as_encoded_bytes().first() == Some(&b'/') => {
                (PathBuf::from(runtime), "hashsync".to_string())
            }
            _ => {
                let uid = rustix::process::geteuid().as_raw();
                (PathBuf::from("/tmp"), format!("hashsync-{uid}"))
            }
        };
        let flags = OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let fd = match retry_on_intr(|| rustix::fs::open(&parent, flags, Mode::empty())) {
            Ok(fd) => fd,
            Err(e) => die(format!("cannot open {}: {e}", quoted_path(&parent))),
        };
        SocketDir {
            path: parent.join(&name),
            parent: fd,
            name,
        }
    }

    /// Create the directory if it doesn't exist yet, then open it as
    /// `open` does.
    fn open_or_create(&self) -> OwnedFd {
        let mode = Mode::from_raw_mode(0o700);
        match retry_on_intr(|| rustix::fs::mkdirat(&self.parent, self.name.as_str(), mode)) {
            Ok(()) | Err(Errno::EXIST) => {}
            Err(e) => die(format!("cannot create {}: {e}", quoted_path(&self.path))),
        }
        match self.open() {
            Some(fd) => fd,
            None => die(format!(
                "{} was removed before it could be opened",
                quoted_path(&self.path)
            )),
        }
    }

    /// Open the directory, or `None` if it doesn't exist yet, and require
    /// this user's private directory: owned by the effective UID, mode
    /// 0700, not a symlink, and not a mount point. Anything else dies;
    /// sockets must not live in foreign directories. The returned
    /// descriptor anchors the socket operations.
    ///
    /// A mount point is refused because the other checks trust the owner
    /// and mode that the filesystem reports, and a FUSE filesystem reports
    /// whatever its daemon says. If the server allows `user_allow_other`,
    /// another user can create `/tmp/hashsync-UID` whenever it doesn't
    /// exist, before the first run or after a cleanup of `/tmp`, and mount
    /// one there that passes those checks. Its daemon then decides what
    /// every name in the directory refers to. The open doesn't cross a
    /// mount point, so it fails on such a directory (DESIGN.md §6.1).
    fn open(&self) -> Option<OwnedFd> {
        let fd = match fsops::open_beneath(self.parent.as_fd(), self.name.as_bytes(), true) {
            Ok(fd) => fd,
            Err(Errno::NOENT) => return None,
            Err(Errno::XDEV) => die(format!("{} is a mount point", quoted_path(&self.path))),
            Err(e) => die(format!("cannot open {}: {e}", quoted_path(&self.path))),
        };
        let stat = match rustix::fs::fstat(&fd) {
            Ok(stat) => stat,
            Err(e) => die(format!("cannot stat {}: {e}", quoted_path(&self.path))),
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
            die(format!("{} {problem}", quoted_path(&self.path)));
        }
        Some(fd)
    }
}

/// The socket's address through the verified directory's descriptor.
/// `sockaddr_un` paths are limited to about 107 bytes, which a long
/// `XDG_RUNTIME_DIR` would exceed; this procfs path stays short and
/// resolves inside the checked directory, like the reopening rule of
/// DESIGN.md §4.5.
fn address_in(dir: BorrowedFd<'_>, name: &str) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}/{name}", dir.as_raw_fd()))
}

/// The user ID of the process at the other end of `socket`, if it isn't
/// this process's user. The kernel records it when that process listens
/// or connects.
///
/// `serve` and `attach` check this on top of the checks of
/// `SocketDir::open`. Those keep other users out of the directory only as
/// long as the filesystem tells the truth about it. This check asks the
/// kernel who is at the other end, so it still holds if the directory
/// checks are ever fooled.
fn foreign_peer(socket: &UnixStream) -> Option<u32> {
    let peer = match rustix::net::sockopt::socket_peercred(socket) {
        Ok(peer) => peer,
        Err(e) => die(format!("cannot get the peer's credentials: {e}")),
    };
    let uid = peer.uid.as_raw();
    (uid != rustix::process::geteuid().as_raw()).then_some(uid)
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
    /// The socket's path, for messages.
    path: PathBuf,
}

/// Create the socket directory if needed and listen (DESIGN.md §6.1:
/// `serve` first listens, so `attach`'s retries can end).
pub fn listen(token: &str) -> Listener {
    let name = format!("{}.sock", checked(token));
    let dir = SocketDir::new();
    let dir_fd = dir.open_or_create();
    let path = dir.path.join(&name);
    let address = address_in(dir_fd.as_fd(), &name);
    // Register before binding, so a concurrent end cannot miss the socket
    // file; removing a file that does not exist yet does nothing.
    die::remove_at_exit(path.clone());
    let listener = match retry_interrupted(|| UnixListener::bind(&address)) {
        Ok(listener) => listener,
        Err(e) => die(format!("cannot listen on {}: {e}", quoted_path(&path))),
    };
    // The directory already denies other users; make the socket itself
    // 0600 too.
    if let Err(e) = retry_interrupted(|| {
        std::fs::set_permissions(&address, std::fs::Permissions::from_mode(0o600))
    }) {
        die(format!("cannot protect {}: {e}", quoted_path(&path)));
    }
    Listener { listener, path }
}

impl Listener {
    /// Accept the control connection, which must come from a process of
    /// this user. The wait needs no check of its own: nothing in `serve`
    /// reads stdin meanwhile, but the session watcher sees the hangup when
    /// the client goes away, and ends the run (DESIGN.md §6.6).
    pub fn accept(&self) -> UnixStream {
        let stream = match retry_interrupted(|| self.listener.accept()) {
            Ok((stream, _)) => stream,
            Err(e) => die(format!("accept: {e}")),
        };
        if let Some(uid) = foreign_peer(&stream) {
            die(format!(
                "a process running as another user (uid {uid}) connected to {}",
                quoted_path(&self.path)
            ));
        }
        stream
    }
}

/// `attach`'s connection: retry until `serve` listens, with no deadline,
/// while stdin remains open.
pub fn connect(token: &str) -> UnixStream {
    let name = format!("{}.sock", checked(token));
    let dir = SocketDir::new();
    let path = dir.path.join(&name);
    // The directory appears when `serve` creates it; open and verify it
    // once, then connect through its descriptor.
    let dir_fd = loop {
        match dir.open() {
            Some(fd) => break fd,
            None => wait_for_serve(&path, &"the socket directory does not exist yet"),
        }
    };
    let address = address_in(dir_fd.as_fd(), &name);
    loop {
        match retry_interrupted(|| UnixStream::connect(&address)) {
            Ok(stream) => {
                if let Some(uid) = foreign_peer(&stream) {
                    die(format!(
                        "the process listening on {} runs as another user (uid {uid})",
                        quoted_path(&path)
                    ));
                }
                return stream;
            }
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
            Err(e) => die(format!("cannot connect to {}: {e}", quoted_path(&path))),
        }
    }
}

fn wait_for_serve(path: &std::path::Path, reason: &dyn std::fmt::Display) {
    if stdin_gone() {
        die(format!(
            "the client disappeared while waiting for {}: {reason}",
            quoted_path(path)
        ));
    }
    std::thread::sleep(RETRY_INTERVAL);
}
