//! Fatal errors end the process (DESIGN.md §6.7).
//!
//! `die` reports an external failure and exits without a core dump. Bugs and
//! database errors panic; the crate builds with `panic = "abort"`, so a panic
//! runs the same cleanup and then aborts with a core dump.

use std::fmt::Display;
use std::sync::Mutex;

/// Exit status for configuration errors and traversal conflicts.
pub const EXIT_FAILURE: i32 = 1;

static DYING: Mutex<()> = Mutex::new(());

/// Report `message`, clean up once, and exit with `status`.
///
/// The first caller keeps the lock until the process exits. Later callers
/// from other threads wait here instead of exiting ahead of the cleanup.
pub fn die(status: i32, message: impl Display) -> ! {
    let _guard = DYING.lock().unwrap_or_else(|e| e.into_inner());
    eprintln!("hashsync: {message}");
    cleanup();
    std::process::exit(status)
}

/// Report a local problem that does not stop the work, such as an entry
/// that cannot be read and is therefore not indexed.
pub fn warn(message: impl Display) {
    eprintln!("hashsync: warning: {message}");
}

/// Run the fatal cleanup after the default panic report.
pub fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        cleanup();
    }));
}

/// Cleanup that the kernel does not do at process exit.
///
/// Nothing yet: the index is a WAL-mode SQLite database and recovers by
/// itself. Later milestones kill child processes and remove sockets here.
fn cleanup() {}
