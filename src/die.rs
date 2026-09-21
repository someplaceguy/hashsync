//! How the process ends, and what it reports on stderr (DESIGN.md §6.7).
//!
//! Every end goes through this module: `die` after an external failure,
//! `finish` at the normal end, and the receiver's termination signals. So
//! one place chooses the reported cause and the exit status, and does the
//! cleanup that the kernel does not do. The first end wins; a later one,
//! from another thread, waits for the process to exit instead of exiting
//! ahead of that cleanup.
//!
//! External failures exit without a core dump. Bugs and database errors
//! panic instead: the crate builds with `panic = "abort"`, so the panic
//! hook of this module runs the fatal cleanup, and the process aborts with
//! a core dump. A panic does not wait for another end in progress: it
//! reports a bug.
//!
//! Diagnostics are best effort: a failed write to stderr is ignored. There
//! is no other channel to report it on, and a panic would turn the failure
//! into an abort with a core dump.

use std::fmt::Display;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Mutex, MutexGuard, OnceLock};

use rustix::event::{PollFd, PollFlags};
use rustix::io::retry_on_intr;
use rustix::process::{Pid, Signal, kill_process, kill_process_group};

use crate::fsops;
use crate::sys;

/// The exit status of configuration errors, traversal conflicts, failed
/// transfers, and every other external failure.
const EXIT_FAILURE: i32 = 1;

/// Report `message`, clean up once, and exit with status 1.
///
/// When the receiver has received SIGINT or SIGTERM, the signal is the
/// reported cause instead, and the status is `128 + signal`. The same
/// signal usually also ends the SSH sessions and the consumer of stdout,
/// so the failure that `message` describes can be its consequence
/// (DESIGN.md §6.5 and §6.6).
pub fn die(message: impl Display) -> ! {
    end(End::Failure(&message))
}

/// The normal end: remove the files registered with `remove_at_exit`, and
/// exit with `status`. A received termination signal takes precedence, as
/// for `die`.
pub fn finish(status: i32) -> ! {
    end(End::Normal(status))
}

/// Report a local problem that does not stop the work, such as an entry
/// that cannot be read and is therefore not indexed.
pub fn warn(message: impl Display) {
    report(format_args!("warning: {message}"));
}

/// Report progress or a summary.
pub fn note(message: impl Display) {
    report(format_args!("{message}"));
}

/// Write `hashsync: `, the message, and a newline to stderr as one write.
/// A pipe keeps a write of up to `PIPE_BUF` (4 KiB) whole, so the line
/// does not interleave with those of other threads or of the processes
/// that share stderr, such as the SSH children. Failures are ignored (see
/// the module documentation).
fn report(message: std::fmt::Arguments<'_>) {
    let line = format!("hashsync: {message}\n");
    let _ = fsops::stderr().write_all(line.as_bytes());
}

/// Why the process ends.
enum End<'a> {
    Normal(i32),
    Failure(&'a dyn Display),
    /// A termination signal that the receiver received.
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
            begin_cleanup().remove_files();
            status
        }
        End::Failure(message) => {
            report(format_args!("{message}"));
            fatal_cleanup();
            EXIT_FAILURE
        }
        End::Signal(signal) => {
            report(format_args!("terminated by {}", signal_name(signal)));
            fatal_cleanup();
            128 + signal
        }
    };
    std::process::exit(status)
}

fn signal_name(signal: i32) -> String {
    match signal {
        libc::SIGINT => "SIGINT".into(),
        libc::SIGTERM => "SIGTERM".into(),
        _ => format!("signal {signal}"),
    }
}

/// The signal mask from before `handle_termination_signals`, which
/// children get back. Set only in a process that holds the termination
/// signals: the receiver.
static CHILD_SIGNAL_MASK: OnceLock<sys::SignalMask> = OnceLock::new();

/// The receiver's handling of SIGINT and SIGTERM (DESIGN.md §6.6): end the
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
/// mask of the thread that starts them. Children get the old mask back
/// (`spawn_guarded`).
pub fn handle_termination_signals() {
    let (old_mask, pending) = sys::block_termination_signals();
    if CHILD_SIGNAL_MASK.set(old_mask).is_err() {
        panic!("termination signals handled twice");
    }
    std::thread::Builder::new()
        .name("signals".into())
        .spawn(move || {
            let mut fds = [PollFd::new(&pending, PollFlags::IN)];
            retry_on_intr(|| rustix::event::poll(&mut fds, None))
                .unwrap_or_else(|e| panic!("poll: {e}"));
            let signal = received_termination_signal()
                .expect("the signal descriptor is readable without a pending signal");
            end(End::Signal(signal))
        })
        .expect("cannot start the signal thread");
}

/// The termination signal that the receiver received, if any. A process
/// that does not hold the signals has none to report, even when one stays
/// pending under a mask that it inherited.
fn received_termination_signal() -> Option<i32> {
    CHILD_SIGNAL_MASK.get()?;
    sys::pending_termination_signal()
}

/// Run the fatal cleanup after the default panic report.
pub fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        fatal_cleanup();
    }));
}

/// What an end must undo (DESIGN.md §6.7). A fatal end kills the children:
/// the receiver's SSH sessions, and the sender's command with its process
/// group. Every end removes the files: the sender's rendezvous socket.
/// Registered identifiers stay valid because children are never reaped:
/// their PIDs and the command's group ID stay reserved until this process
/// exits.
///
/// `ending` marks a cleanup that has begun: from then on nothing new may
/// be registered, or the cleanup would miss it.
struct Registry {
    ending: bool,
    kill_pids: Vec<Pid>,
    kill_pgroups: Vec<Pid>,
    remove_files: Vec<PathBuf>,
}

static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
    ending: false,
    kill_pids: Vec::new(),
    kill_pgroups: Vec::new(),
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
/// without the termination signals that the receiver blocks.
pub fn spawn_guarded(guard: KillGuard, command: &mut Command) -> std::io::Result<Child> {
    if let Some(&mask) = CHILD_SIGNAL_MASK.get() {
        sys::set_child_signal_mask(command, mask);
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

/// Remove this file when the process ends, normally or fatally. Register
/// the path before creating the file, so that a concurrent end cannot miss
/// it; removing a file that does not exist yet does nothing.
pub fn remove_at_exit(path: PathBuf) {
    registry().remove_files.push(path);
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
    registry.remove_files();
}

impl Registry {
    fn remove_files(&self) {
        for path in &self.remove_files {
            let _ = fsops::retry_interrupted(|| std::fs::remove_file(path));
        }
    }
}
