//! How the process ends (DESIGN.md §6.9).
//!
//! Every end goes through this module: `die` after an external failure,
//! `finish` at the normal end, and the client's termination signals. So
//! one place chooses the reported cause and the exit status, and does the
//! cleanup that the kernel does not do. The first end wins; a later one,
//! from another thread, waits for the process to exit instead of exiting
//! ahead of that cleanup.
//!
//! External failures exit without a core dump. Bugs and database errors
//! panic instead: the crate builds with `panic = "abort"`, so the panic
//! hook of this module runs the fatal cleanup, and the process aborts with
//! a core dump. A panic does not wait for another end in progress: it
//! reports a bug. Only the normal end runs the C library's exit handlers
//! (DESIGN.md §6.9).

use std::fmt::Display;
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard, OnceLock};

use rustix::event::{PollFd, PollFlags};
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use rustix::process::{Pid, Signal, kill_process, kill_process_group};

use crate::fsops;
use crate::report;
use crate::sys;

/// The exit status of configuration errors, traversal conflicts, failed
/// transfers, and every other external failure.
const EXIT_FAILURE: i32 = 1;

/// Report `message`, clean up once, and exit with status 1.
///
/// When the client has received SIGINT or SIGTERM, the signal is the
/// reported cause instead, and the status is `128 + signal`. The same
/// signal usually also ends the SSH sessions and the consumer of stdout,
/// so the failure that `message` describes can be its consequence
/// (DESIGN.md §6.7 and §6.8).
pub fn die(message: impl Display) -> ! {
    end(End::Failure(&message))
}

/// The normal end: restore the flags that `add_flags` added and that no
/// guard restored yet, remove the files registered with `remove_at_exit`,
/// and exit with `status`. A received termination signal takes
/// precedence, as for `die`.
pub fn finish(status: i32) -> ! {
    end(End::Normal(status))
}

/// Why the process ends.
enum End<'a> {
    Normal(i32),
    Failure(&'a dyn Display),
    /// A termination signal that the client received.
    Signal(i32),
}

/// Held by the thread that ends the process, until the process exits.
static ENDING: Mutex<()> = Mutex::new(());

fn end(end: End<'_>) -> ! {
    // Later callers wait here forever.
    let _ending = ENDING.lock().unwrap_or_else(|e| e.into_inner());
    let end = match received_termination_signal() {
        Some(signal) => End::Signal(signal),
        None => end,
    };
    let status = match end {
        End::Normal(status) => {
            begin_cleanup().undo();
            // The exit handlers run. One of them, a C++ static destructor
            // of RocksDB, joins its background threads, which have no work
            // left at a normal end.
            std::process::exit(status)
        }
        End::Failure(message) => {
            report::error(message);
            fatal_cleanup();
            EXIT_FAILURE
        }
        End::Signal(signal) => {
            report::error(format_args!("terminated by {}", signal_name(signal)));
            fatal_cleanup();
            128 + signal
        }
    };
    // A fatal end runs no exit handlers: it does not wait for a flush or
    // compaction of RocksDB, nor destroy RocksDB's static objects while
    // other threads use them. The index recovers from its write-ahead log,
    // as after a crash.
    sys::exit_now(status)
}

fn signal_name(signal: i32) -> String {
    match signal {
        libc::SIGINT => "SIGINT".into(),
        libc::SIGTERM => "SIGTERM".into(),
        _ => format!("signal {signal}"),
    }
}

/// The signal mask that this process started with, which children get
/// back. Every process blocks SIGXFSZ in every thread
/// (`block_file_size_signal`), and the client also blocks SIGINT and
/// SIGTERM (`handle_termination_signals`).
static CHILD_SIGNAL_MASK: OnceLock<sys::SignalMask> = OnceLock::new();

/// Block SIGXFSZ in every thread, in every process. Then a write past the
/// file-size limit fails with `EFBIG`, and the writer handles that failure
/// as any other one. A write to a segment, stdout, or a cache file ends
/// the run with `die`, whose cleanup kills the children and removes the
/// rendezvous socket (DESIGN.md §6.7 and §6.9). A database write panics.
/// The default action of SIGXFSZ would end the process with a core dump,
/// without a message and without that cleanup.
///
/// Call this at the start of `main`, before any other thread starts:
/// threads inherit the signal mask of the thread that starts them.
/// Children get the old mask back (`spawn_guarded`).
pub fn block_file_size_signal() {
    let old_mask = sys::block_signals(&[libc::SIGXFSZ]);
    if CHILD_SIGNAL_MASK.set(old_mask).is_err() {
        panic!("SIGXFSZ blocked twice");
    }
}

/// Set in a process that holds the termination signals: the client.
static HOLDS_TERMINATION_SIGNALS: AtomicBool = AtomicBool::new(false);

/// The client's handling of SIGINT and SIGTERM (DESIGN.md §6.8): end the
/// run with `128 + signal`, whatever failure the signal causes first.
///
/// Both signals stay blocked in every thread and are never accepted, so a
/// received signal stays pending until the process exits and every end
/// sees it. A signal handler cannot give that guarantee: until a thread
/// runs the handler, no other thread can tell that the signal arrived,
/// while the SSH children or the consumer of stdout that the same Ctrl-C
/// killed can already make another thread fail. A dedicated thread waits
/// until a signal is pending, and then ends the process.
///
/// Call this before any other thread starts: threads inherit the signal
/// mask of the thread that starts them. Children start without these
/// signals blocked (`spawn_guarded`).
pub fn handle_termination_signals() {
    let pending = sys::block_termination_signals();
    if HOLDS_TERMINATION_SIGNALS.swap(true, Relaxed) {
        panic!("termination signals handled twice");
    }
    std::thread::Builder::new()
        .name("signals".into())
        .spawn(move || {
            fsops::poll_fds(&mut [PollFd::new(&pending, PollFlags::IN)]);
            let signal = received_termination_signal()
                .expect("the signal descriptor is readable without a pending signal");
            end(End::Signal(signal))
        })
        .expect("cannot start the signal thread");
}

/// The termination signal that the client received, if any. A process
/// that does not hold the signals has none to report, even when one stays
/// pending under a mask that it inherited.
fn received_termination_signal() -> Option<i32> {
    if !HOLDS_TERMINATION_SIGNALS.load(Relaxed) {
        return None;
    }
    sys::pending_termination_signal()
}

/// Run the fatal cleanup after the panic report (`report::panic`): the
/// default one, or on the server, the same content with the server's
/// marker on every line.
pub fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        report::panic(info, &*default_hook);
        fatal_cleanup();
    }));
}

/// What an end must undo (DESIGN.md §6.9). A fatal end kills the children:
/// the client's SSH sessions, the server's command with its process group
/// until END is out, and the child that updates a receiver's index after
/// the command. Every end restores the file status flags that this process
/// added to an open file description that other processes can share: the
/// client's stdin, which its stdin reader makes non-blocking. Every end
/// also removes the files: the server's rendezvous socket. Registered
/// identifiers stay valid because children are never reaped: their PIDs
/// and the command's group ID stay reserved until this process exits.
///
/// `ending` marks a cleanup that has begun: from then on nothing new may
/// be registered, or the cleanup would miss it.
struct Registry {
    ending: bool,
    kill_pids: Vec<Pid>,
    kill_pgroups: Vec<Pid>,
    /// The open file descriptions whose flags changed, each with the flags
    /// to restore. A duplicate descriptor refers to each description: it
    /// stays valid whatever happens to the descriptor of the change.
    restore_flags: Vec<(OwnedFd, OFlags)>,
    remove_files: Vec<PathBuf>,
}

static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
    ending: false,
    kill_pids: Vec::new(),
    kill_pgroups: Vec::new(),
    restore_flags: Vec::new(),
    remove_files: Vec::new(),
});

fn registry() -> MutexGuard<'static, Registry> {
    let registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    if registry.ending {
        // Another thread ends the process; wait for its exit instead of
        // creating something that the cleanup no longer sees.
        drop(registry);
        loop {
            std::thread::park();
        }
    }
    registry
}

/// What SIGKILL must reach on a fatal end.
pub enum KillGuard {
    /// The child itself: an SSH session.
    Process,
    /// The child's process group, whose ID is the child's PID: the
    /// command started with `process_group(0)`.
    ProcessGroup,
}

/// Spawn `command` and register its fatal-end kill as one step, so that a
/// concurrent end either runs after the registration or blocks the spawn.
/// The child starts with the signal mask that this process started with,
/// without the signals that this process blocks in every thread, and with
/// the descriptor limit from before any raise of this process.
pub fn spawn_guarded(guard: KillGuard, command: &mut Command) -> std::io::Result<Child> {
    if let Some(&mask) = CHILD_SIGNAL_MASK.get() {
        sys::set_child_signal_mask(command, mask);
    }
    if let Some(limit) = fsops::original_descriptor_limit() {
        sys::set_child_descriptor_limit(command, limit);
    }
    let mut registry = registry();
    let child = command.spawn()?;
    let pid = Pid::from_child(&child);
    match guard {
        KillGuard::Process => registry.kill_pids.push(pid),
        KillGuard::ProcessGroup => registry.kill_pgroups.push(pid),
    }
    Ok(child)
}

/// Leave the process group `pgroup`, which `spawn_guarded` registered, out
/// of a fatal end from now on, as the normal end leaves it.
pub fn spare_process_group(pgroup: Pid) {
    registry().kill_pgroups.retain(|&group| group != pgroup);
}

/// Remove this file when the process ends, normally or fatally. Register
/// the path before creating the file, so that a concurrent end cannot miss
/// it; removing a file that does not exist yet does nothing.
pub fn remove_at_exit(path: PathBuf) {
    registry().remove_files.push(path);
}

/// Add `added` to the file status flags of the open file description of
/// `fd` while the returned guard lives: the drop of the guard restores the
/// flags. Every end of the process restores them too, also a fatal one,
/// which runs no destructors, because other processes can share the
/// description, and must find it as it was. `None`, without a change, when
/// the flags hold `added` already: then this process has nothing to
/// restore.
#[must_use = "the drop of the guard restores the flags at once"]
pub fn add_flags(fd: BorrowedFd<'_>, added: OFlags) -> Option<AddedFlags> {
    let flags = fcntl_getfl(fd).unwrap_or_else(|e| panic!("fcntl: {e}"));
    if flags.contains(added) {
        return None;
    }
    let description = fd
        .try_clone_to_owned()
        .unwrap_or_else(|e| die(format!("cannot duplicate a descriptor: {e}")));
    let id = description.as_raw_fd();
    // The registration and the change are one step under the lock, as in
    // `spawn_guarded`. So an end that begins later restores the flags after
    // the change, and an end that began earlier stops this thread before
    // both. A failed change panics only after the lock is released: the
    // panic hook takes the lock for its cleanup.
    let mut registry = registry();
    registry.restore_flags.push((description, flags));
    let changed = fcntl_setfl(fd, flags | added);
    drop(registry);
    changed.unwrap_or_else(|e| panic!("fcntl: {e}"));
    Some(AddedFlags { id })
}

/// The flags that `add_flags` added. The drop of this guard restores them,
/// and removes them from what the end of the process restores. Once an end
/// has begun, the drop waits for the exit instead, as every registration
/// does, and that end restores them.
pub struct AddedFlags {
    /// The duplicate descriptor of the registration, whose number is
    /// unique while it stays open.
    id: RawFd,
}

impl Drop for AddedFlags {
    fn drop(&mut self) {
        let mut registry = registry();
        let at = registry
            .restore_flags
            .iter()
            .position(|(description, _)| description.as_raw_fd() == self.id)
            .expect("the flags were restored already");
        let (description, flags) = registry.restore_flags.swap_remove(at);
        // Under the lock: an end that begins now finds them restored.
        let _ = fcntl_setfl(&description, flags);
    }
}

/// Freeze the registry for the cleanup, which the kernel does not do at
/// process exit. Cleanup failures are ignored: the target may already be
/// gone, and the cleanup must not panic.
fn begin_cleanup() -> MutexGuard<'static, Registry> {
    let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    registry.ending = true;
    registry
}

fn fatal_cleanup() {
    let registry = begin_cleanup();
    for &pid in &registry.kill_pids {
        let _ = kill_process(pid, Signal::KILL);
    }
    for &pgroup in &registry.kill_pgroups {
        let _ = kill_process_group(pgroup, Signal::KILL);
    }
    registry.undo();
}

impl Registry {
    /// What every end undoes: restore the flags, and remove the files.
    fn undo(&self) {
        for (description, flags) in &self.restore_flags {
            let _ = fcntl_setfl(description, *flags);
        }
        for path in &self.remove_files {
            let _ = fsops::retry_interrupted(|| std::fs::remove_file(path));
        }
    }
}
