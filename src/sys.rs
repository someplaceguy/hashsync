//! Unsafe system-call and C-library wrappers. This is the only module that
//! may use unsafe code (DESIGN.md §11).
//!
//! Every system-call wrapper retries a call that a signal interrupts, so
//! callers never see `EINTR`.
#![allow(unsafe_code)]

use std::ffi::{c_int, c_void};
use std::mem;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;

use libmimalloc_sys::{
    mi_free, mi_good_size, mi_is_in_heap_region, mi_malloc, mi_realloc, mi_usable_size,
};
use linux_raw_sys::general::fsuuid2;
use rusqlite::ffi;
use rustix::fs::Fsid;
use rustix::io::Errno;
use rustix::ioctl::{self, Getter, Opcode};

/// The opaque inode handle of the object behind `fd`, encoded as
/// `handle_type` (little-endian `i32`) followed by the handle bytes.
/// `Ok(None)` when the filesystem does not support handles.
pub fn name_to_handle(fd: BorrowedFd<'_>) -> Result<Option<Vec<u8>>, Errno> {
    const HEADER: usize = mem::size_of::<libc::file_handle>();
    let max = libc::MAX_HANDLE_SZ as usize;
    // `file_handle` has alignment 4; a `u32` buffer keeps it aligned.
    let mut buf = vec![0u32; (HEADER + max).div_ceil(4)];
    let handle = buf.as_mut_ptr().cast::<libc::file_handle>();
    let mut mount_id: libc::c_int = 0;
    // SAFETY: `handle` points to a buffer of at least `HEADER + max` bytes
    // and `handle_bytes` tells the kernel that capacity. The empty pathname
    // with `AT_EMPTY_PATH` refers to `fd` itself.
    unsafe {
        loop {
            // Set the capacity before every attempt; a failed call can
            // overwrite it with the required size.
            (*handle).handle_bytes = max as libc::c_uint;
            let rc = libc::name_to_handle_at(
                fd.as_raw_fd(),
                c"".as_ptr(),
                handle,
                &mut mount_id,
                libc::AT_EMPTY_PATH,
            );
            if rc == 0 {
                break;
            }
            // Read the errno before another call can change it.
            match last_errno() {
                Errno::INTR => {}
                Errno::NOTSUP => return Ok(None),
                errno => return Err(errno),
            }
        }
        let len = (*handle).handle_bytes as usize;
        let handle_type = (*handle).handle_type;
        let bytes = std::slice::from_raw_parts(handle.cast::<u8>().add(HEADER), len);
        let mut out = Vec::with_capacity(4 + len);
        out.extend_from_slice(&handle_type.to_le_bytes());
        out.extend_from_slice(bytes);
        Ok(Some(out))
    }
}

/// `FS_IOC_GETFSUUID`: the UUID of the filesystem holding `fd`, which must
/// be open for reading (not an `O_PATH` descriptor). `Ok(None)` when the
/// kernel or filesystem does not support it.
pub fn fs_uuid(fd: BorrowedFd<'_>) -> Result<Option<Vec<u8>>, Errno> {
    const FS_IOC_GETFSUUID: Opcode = ioctl::opcode::read::<fsuuid2>(0x15, 0);
    let result = rustix::io::retry_on_intr(|| {
        // SAFETY: the opcode is `_IOR(0x15, 0, struct fsuuid2)` from
        // `linux/fs.h`, and the kernel writes a `struct fsuuid2`.
        let getter = unsafe { Getter::<FS_IOC_GETFSUUID, fsuuid2>::new() };
        // SAFETY: the getter describes the opcode's output type.
        unsafe { ioctl::ioctl(fd, getter) }
    });
    match result {
        Ok(out) => {
            let len = usize::from(out.len);
            if len == 0 || len > out.uuid.len() {
                return Ok(None);
            }
            Ok(Some(out.uuid[..len].to_vec()))
        }
        Err(Errno::NOTTY | Errno::NOTSUP | Errno::INVAL | Errno::NOSYS) => Ok(None),
        Err(errno) => Err(errno),
    }
}

/// The raw bytes of an `f_fsid` value.
pub fn fsid_bytes(fsid: &Fsid) -> [u8; 8] {
    const _: () = assert!(mem::size_of::<Fsid>() == 8);
    // SAFETY: `Fsid` is `repr(C)` with two `c_int` fields, so it is eight
    // initialized bytes.
    unsafe { std::ptr::read_unaligned(std::ptr::from_ref(fsid).cast::<[u8; 8]>()) }
}

fn last_errno() -> Errno {
    Errno::from_io_error(&std::io::Error::last_os_error()).unwrap_or(Errno::IO)
}

/// Wait for a child to exit without reaping it (`WEXITED | WNOWAIT`), so
/// its process and group IDs stay reserved until this process exits
/// (DESIGN.md §6.6). Returns the child's exit status, or `128 + signal`
/// when a signal terminated it.
pub fn wait_exit_status(pid: rustix::process::Pid) -> i32 {
    use rustix::process::{WaitId, WaitIdOptions, waitid};
    let status = rustix::io::retry_on_intr(|| {
        waitid(
            WaitId::Pid(pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOWAIT,
        )
    })
    .unwrap_or_else(|e| panic!("waitid: {e}"))
    .expect("waitid without NOHANG returned no status");
    if let Some(code) = status.exit_status() {
        code
    } else if let Some(signal) = status.terminating_signal() {
        128 + signal
    } else {
        panic!("waitid: neither an exit status nor a signal")
    }
}

/// Set `SIGCHLD` to `SIG_DFL` with `SA_NOCLDWAIT` clear, so children stay
/// unreaped until process exit and `waitid` with `WNOWAIT` can observe
/// them (DESIGN.md §6.6). Call before spawning the first child.
pub fn reset_sigchld() {
    // SAFETY: a zeroed `sigaction` with an empty mask installs `SIG_DFL`
    // with no flags, which is a valid disposition for `SIGCHLD`.
    unsafe {
        let mut action: libc::sigaction = mem::zeroed();
        action.sa_sigaction = libc::SIG_DFL;
        libc::sigemptyset(&mut action.sa_mask);
        let rc = libc::sigaction(libc::SIGCHLD, &action, std::ptr::null_mut());
        assert_eq!(rc, 0, "sigaction(SIGCHLD): {}", last_errno());
    }
}

/// SIGINT and SIGTERM: the signals that end a receiver's run.
fn termination_signals() -> libc::sigset_t {
    // SAFETY: `sigemptyset` initializes the set before `sigaddset` adds
    // valid signal numbers to it.
    unsafe {
        let mut set: libc::sigset_t = mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::sigaddset(&mut set, libc::SIGTERM);
        set
    }
}

/// A thread's set of blocked signals.
#[derive(Clone, Copy)]
pub struct SignalMask(libc::sigset_t);

/// Block SIGINT and SIGTERM in the calling thread. Returns the mask from
/// before, and a `signalfd` that is readable while one of the two signals
/// is pending. The descriptor is only polled, never read, so the signal
/// stays pending.
pub fn block_termination_signals() -> (SignalMask, OwnedFd) {
    let set = termination_signals();
    // SAFETY: `set` is an initialized signal set, and the kernel writes
    // the old mask to `old`.
    let old = unsafe {
        let mut old: libc::sigset_t = mem::zeroed();
        let rc = libc::pthread_sigmask(libc::SIG_BLOCK, &set, &mut old);
        assert_eq!(rc, 0, "pthread_sigmask: {}", Errno::from_raw_os_error(rc));
        old
    };
    // SAFETY: `-1` requests a new descriptor for the signals in `set`.
    let fd = unsafe { libc::signalfd(-1, &set, libc::SFD_CLOEXEC) };
    assert!(fd >= 0, "signalfd: {}", last_errno());
    // SAFETY: `signalfd` returned a new descriptor that nothing else owns.
    (SignalMask(old), unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Start `command`'s child with `mask` blocked, instead of the mask of the
/// thread that spawns it, which `Command` keeps.
pub fn set_child_signal_mask(command: &mut std::process::Command, mask: SignalMask) {
    let hook = move || {
        // SAFETY: `mask.0` is an initialized signal set.
        match unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &mask.0, std::ptr::null_mut()) } {
            0 => Ok(()),
            rc => Err(std::io::Error::from_raw_os_error(rc)),
        }
    };
    // SAFETY: the hook runs in the child between `fork` and `exec`, where
    // only async-signal-safe functions may run; `pthread_sigmask` is one.
    unsafe { command.pre_exec(hook) };
}

/// SIGINT or SIGTERM when one is pending: sent, blocked, and not yet
/// accepted. SIGINT comes first when both are pending.
pub fn pending_termination_signal() -> Option<i32> {
    // SAFETY: `sigpending` initializes the set.
    let pending = unsafe {
        let mut set: libc::sigset_t = mem::zeroed();
        let rc = libc::sigpending(&mut set);
        assert_eq!(rc, 0, "sigpending: {}", last_errno());
        set
    };
    [libc::SIGINT, libc::SIGTERM]
        .into_iter()
        // SAFETY: `pending` is an initialized signal set.
        .find(|&signal| unsafe { libc::sigismember(&pending, signal) } == 1)
}

/// Make SQLite allocate through mimalloc (`SQLITE_CONFIG_MALLOC`).
///
/// Call this one time, before the first use of SQLite: SQLite refuses the
/// configuration after `sqlite3_initialize()` has run. When this returns,
/// SQLite is initialized and verified to allocate through mimalloc.
pub fn sqlite_use_mimalloc() {
    /// SQLite passes sizes as positive `int` values.
    fn from_sqlite(size: c_int) -> usize {
        usize::try_from(size).expect("negative size from SQLite")
    }

    /// SQLite counts sizes in `int`, so a larger usable size saturates.
    /// The usable size of a pointer does not change, so the allocation
    /// and free sides of SQLite's memory accounting stay equal.
    fn to_sqlite(size: usize) -> c_int {
        c_int::try_from(size).unwrap_or(c_int::MAX)
    }

    unsafe extern "C" fn malloc(size: c_int) -> *mut c_void {
        // SAFETY: `mi_malloc` accepts any size.
        unsafe { mi_malloc(from_sqlite(size)) }
    }

    unsafe extern "C" fn free(p: *mut c_void) {
        // SAFETY: SQLite frees only pointers from `malloc` or `realloc`.
        unsafe { mi_free(p) }
    }

    unsafe extern "C" fn realloc(p: *mut c_void, size: c_int) -> *mut c_void {
        // SAFETY: SQLite reallocates only pointers from `malloc` or
        // `realloc`.
        unsafe { mi_realloc(p, from_sqlite(size)) }
    }

    unsafe extern "C" fn size(p: *mut c_void) -> c_int {
        // SAFETY: SQLite measures only pointers from `malloc` or `realloc`.
        to_sqlite(unsafe { mi_usable_size(p) })
    }

    /// The usable size of an allocation of `size` bytes, so SQLite
    /// requests block sizes that mimalloc serves without waste.
    unsafe extern "C" fn roundup(size: c_int) -> c_int {
        // SAFETY: `mi_good_size` accepts any size.
        to_sqlite(unsafe { mi_good_size(from_sqlite(size)) })
    }

    /// SQLite calls `xInit` without a null check; `xShutdown` may be null.
    unsafe extern "C" fn init(_app_data: *mut c_void) -> c_int {
        ffi::SQLITE_OK
    }

    let methods = ffi::sqlite3_mem_methods {
        xMalloc: Some(malloc),
        xFree: Some(free),
        xRealloc: Some(realloc),
        xSize: Some(size),
        xRoundup: Some(roundup),
        xInit: Some(init),
        xShutdown: None,
        pAppData: std::ptr::null_mut(),
    };
    // SAFETY: `SQLITE_CONFIG_MALLOC` takes one `sqlite3_mem_methods`
    // pointer, and SQLite copies the structure before it returns.
    let rc =
        unsafe { ffi::sqlite3_config(ffi::SQLITE_CONFIG_MALLOC, std::ptr::from_ref(&methods)) };
    assert_eq!(rc, ffi::SQLITE_OK, "SQLITE_CONFIG_MALLOC failed");

    // SAFETY: `sqlite3_malloc` has no preconditions; it initializes SQLite.
    let p = unsafe { ffi::sqlite3_malloc(1) };
    assert!(!p.is_null(), "SQLite cannot allocate through mimalloc");
    // SAFETY: `mi_is_in_heap_region` accepts any pointer.
    let effective = unsafe { mi_is_in_heap_region(p) };
    // SAFETY: `p` came from `sqlite3_malloc` and is freed one time.
    unsafe { ffi::sqlite3_free(p) };
    assert!(effective, "SQLite does not allocate through mimalloc");
}

/// Test-only: set an environment variable. `set_var` is unsafe because
/// other threads may read the environment concurrently; the tests that
/// use this serialize themselves with a lock.
#[cfg(test)]
pub fn set_var_for_tests(key: &str, value: &std::ffi::OsStr) {
    // SAFETY: the caller serializes environment access.
    unsafe { std::env::set_var(key, value) }
}

#[cfg(test)]
mod tests {
    use super::mi_is_in_heap_region;

    /// The build must not let mimalloc interpose the libc allocator: a
    /// dependency that enables `libmimalloc-sys/override` would replace
    /// `malloc` for the whole process. This check fails the build in that
    /// case.
    #[test]
    fn mimalloc_does_not_interpose_libc_malloc() {
        // SAFETY: `malloc` has no preconditions.
        let p = unsafe { libc::malloc(64) };
        assert!(!p.is_null());
        // SAFETY: `mi_is_in_heap_region` accepts any pointer.
        let interposed = unsafe { mi_is_in_heap_region(p) };
        // SAFETY: `p` came from `malloc` and is freed one time.
        unsafe { libc::free(p) };
        assert!(
            !interposed,
            "libc malloc returns mimalloc memory; \
             a dependency enables `libmimalloc-sys/override`"
        );
    }

    /// The global allocator is mimalloc. This also shows that the
    /// interposition check above can see mimalloc memory.
    #[test]
    fn global_allocator_is_mimalloc() {
        let heap = Box::new([0u8; 64]);
        // SAFETY: `mi_is_in_heap_region` accepts any pointer.
        assert!(unsafe { mi_is_in_heap_region(heap.as_ptr().cast()) });
    }
}
