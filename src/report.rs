//! What the process reports on stderr (DESIGN.md §8): notes, which give
//! progress and summaries, warnings, errors, and the server's panic
//! reports. Each report is one line that starts with `hashsync: `, or on
//! the server with `hashsync (server): `.
//!
//! Reports are best effort: a failed write to stderr is ignored. There is
//! no other channel to report it on, and a panic would turn the failure
//! into an abort with a core dump. `--quiet` removes the notes, and keeps
//! warnings and errors.

use std::fmt::Display;
use std::io::Write;
use std::panic::PanicHookInfo;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use crate::fsops;

/// Report progress or a summary, unless `--quiet` disabled the notes.
pub fn note(message: impl Display) {
    if NOTES.load(Relaxed) {
        line(format_args!("{message}"));
    }
}

/// Report a local problem that does not stop the work, such as an entry
/// that cannot be read and is therefore not indexed.
pub fn warn(message: impl Display) {
    line(format_args!("warning: {message}"));
}

/// Report a failure: the cause of a fatal end, which `die` reports, or a
/// failure that does not end the process, such as the remote command's
/// failure to start, after which the run ends normally with the command's
/// status.
pub fn error(message: impl Display) {
    line(format_args!("{message}"));
}

/// Cleared by `--quiet`, before other threads start.
static NOTES: AtomicBool = AtomicBool::new(true);

/// Report no more progress or summaries.
pub fn disable_notes() {
    NOTES.store(false, Relaxed);
}

/// Set once this process is the server, by `serve` and `attach`. SSH
/// carries the server's stderr to the client's, where its lines mix with
/// the client's own: the marker tells the user which host a message comes
/// from, and so on which host to act on it (DESIGN.md §8).
static SERVER: AtomicBool = AtomicBool::new(false);

/// Mark every later line of this process, and its panic reports, as the
/// server's. Called first in the server's entry points, before any other
/// thread starts.
pub fn mark_as_server() {
    SERVER.store(true, Relaxed);
}

/// Report a panic. Outside the server, `default`, the standard library's
/// report, writes it. A protocol error panics on the server too
/// (DESIGN.md §6.7), and SSH carries the report to the client's stderr,
/// so there each of its lines goes out as a line of this module, which
/// marks it as the server's, as the default report would not. It gives
/// what the default report gives: the thread, the location, the message,
/// and the backtrace when `RUST_BACKTRACE` asks for one.
pub fn panic(info: &PanicHookInfo<'_>, default: &dyn Fn(&PanicHookInfo<'_>)) {
    if !SERVER.load(Relaxed) {
        default(info);
        return;
    }
    let thread = std::thread::current();
    let name = thread.name().unwrap_or("<unnamed>");
    match info.location() {
        Some(location) => line(format_args!("thread '{name}' panicked at {location}:")),
        None => line(format_args!("thread '{name}' panicked:")),
    }
    for text in info.payload_as_str().unwrap_or("Box<dyn Any>").lines() {
        line(format_args!("{text}"));
    }
    let backtrace = std::backtrace::Backtrace::capture();
    if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
        for text in backtrace.to_string().lines() {
            line(format_args!("{text}"));
        }
    } else {
        line(format_args!(
            "note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"
        ));
    }
}

/// Write the prefix of this process's lines, `hashsync: ` or, on the
/// server, `hashsync (server): `, then the message and a newline, to
/// stderr as one write. A pipe keeps a write of up to `PIPE_BUF` (4 KiB)
/// whole, so the line does not interleave with those of other threads or
/// of the processes that share stderr, such as the SSH children. Failures
/// are ignored (see the module documentation).
fn line(message: std::fmt::Arguments<'_>) {
    let host = if SERVER.load(Relaxed) {
        " (server)"
    } else {
        ""
    };
    let line = format!("hashsync{host}: {message}\n");
    let _ = fsops::stderr().write_all(line.as_bytes());
}
