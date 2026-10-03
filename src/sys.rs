//! Unsafe system-call, C-library, and CPU-instruction wrappers. This is the
//! only module that may use unsafe code (DESIGN.md §11).
//!
//! Every system-call wrapper retries a call that a signal interrupts, so
//! callers never see `EINTR`.
#![allow(unsafe_code)]

use std::ffi::{CStr, c_int};
use std::mem;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::ptr::NonNull;

use linux_raw_sys::general::fsuuid2;
use rustix::fs::Fsid;
use rustix::io::Errno;
use rustix::ioctl::{self, Getter, Opcode};
use zstd_sys::{ZSTD_EndDirective, ZSTD_cParameter};

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

/// Lock the whole file behind `fd` for its open file description
/// (`F_OFD_SETLKW`): shared, or exclusive, which needs `fd` open for
/// writing. Waits as long as another owner holds a conflicting lock. The
/// lock goes when the last descriptor of the open file description closes;
/// unlike a classic POSIX lock, it does not go when another descriptor of
/// the same file closes.
pub fn lock_file(fd: BorrowedFd<'_>, exclusive: bool) -> Result<(), Errno> {
    let lock_type = if exclusive {
        libc::F_WRLCK
    } else {
        libc::F_RDLCK
    };
    // SAFETY: `flock` holds only integers, for which zero is valid.
    let mut lock: libc::flock = unsafe { mem::zeroed() };
    // Both are small constants that fit a `c_short`.
    lock.l_type = lock_type as libc::c_short;
    lock.l_whence = libc::SEEK_SET as libc::c_short;
    // `l_start` and `l_len` 0: from the start to the end of the file,
    // however long it grows. `l_pid` must be 0 for this command.
    loop {
        // SAFETY: `F_OFD_SETLKW` takes a pointer to an initialized `flock`
        // and only reads it.
        let rc = unsafe {
            libc::fcntl(
                fd.as_raw_fd(),
                libc::F_OFD_SETLKW,
                std::ptr::from_ref(&lock),
            )
        };
        if rc == 0 {
            return Ok(());
        }
        // Read the errno before another call can change it.
        match last_errno() {
            Errno::INTR => {}
            errno => return Err(errno),
        }
    }
}

fn last_errno() -> Errno {
    Errno::from_io_error(&std::io::Error::last_os_error()).unwrap_or(Errno::IO)
}

/// Wait for a child to exit without reaping it (`WEXITED | WNOWAIT`), so
/// its process and group IDs stay reserved until this process exits
/// (DESIGN.md §6.7). Returns the child's exit status, or `128 + signal`
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

/// End the process at once with `status`: `_exit` runs none of the C
/// library's exit handlers, and the other threads stop where they are.
pub fn exit_now(status: i32) -> ! {
    // SAFETY: `_exit` accepts any status and does not return.
    unsafe { libc::_exit(status) }
}

/// Set `SIGCHLD` to `SIG_DFL` with `SA_NOCLDWAIT` clear, so children stay
/// unreaped until process exit and `waitid` with `WNOWAIT` can observe
/// them (DESIGN.md §6.7). Call before spawning the first child.
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

/// A signal set with `signals`.
fn signal_set(signals: &[c_int]) -> libc::sigset_t {
    // SAFETY: `sigemptyset` initializes the set before `sigaddset` adds
    // valid signal numbers to it.
    unsafe {
        let mut set: libc::sigset_t = mem::zeroed();
        libc::sigemptyset(&mut set);
        for &signal in signals {
            libc::sigaddset(&mut set, signal);
        }
        set
    }
}

/// SIGINT and SIGTERM: the signals that end a receiver's run.
const TERMINATION_SIGNALS: [c_int; 2] = [libc::SIGINT, libc::SIGTERM];

/// A thread's set of blocked signals.
#[derive(Clone, Copy)]
pub struct SignalMask(libc::sigset_t);

/// Block `signals` in the calling thread. Returns the mask from before.
pub fn block_signals(signals: &[c_int]) -> SignalMask {
    let blocked = signal_set(signals);
    // SAFETY: `blocked` is an initialized signal set, and the kernel
    // writes the old mask to `old`.
    let old = unsafe {
        let mut old: libc::sigset_t = mem::zeroed();
        let rc = libc::pthread_sigmask(libc::SIG_BLOCK, &blocked, &mut old);
        assert_eq!(rc, 0, "pthread_sigmask: {}", Errno::from_raw_os_error(rc));
        old
    };
    SignalMask(old)
}

/// Block SIGINT and SIGTERM in the calling thread. Returns a `signalfd`
/// that is readable while one of them is pending. The descriptor is only
/// polled, never read, so the signal stays pending.
pub fn block_termination_signals() -> OwnedFd {
    block_signals(&TERMINATION_SIGNALS);
    let set = signal_set(&TERMINATION_SIGNALS);
    // SAFETY: `-1` requests a new descriptor for the signals in `set`.
    let fd = unsafe { libc::signalfd(-1, &set, libc::SFD_CLOEXEC) };
    assert!(fd >= 0, "signalfd: {}", last_errno());
    // SAFETY: `signalfd` returned a new descriptor that nothing else owns.
    unsafe { OwnedFd::from_raw_fd(fd) }
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
    TERMINATION_SIGNALS
        .into_iter()
        // SAFETY: `pending` is an initialized signal set.
        .find(|&signal| unsafe { libc::sigismember(&pending, signal) } == 1)
}

// The replacements of the C++ allocation functions, which `build.rs`
// compiles from `src/cxx_allocation.cc` into a static library. The linker
// takes the whole library, although no Rust code calls its functions.
// rustc gives the libraries of this crate to the linker before the
// libraries of its dependencies, so the linker reads the replacements
// before RocksDB and libstdc++. With a static libstdc++, this order is
// necessary: else the linker takes the definitions of libstdc++ for the
// calls of RocksDB, and the replacements then define each function a
// second time.
#[cfg(feature = "mimalloc")]
#[link(name = "cxx_allocation", kind = "static", modifiers = "+whole-archive")]
unsafe extern "C" {}

/// mimalloc as the allocator of a zstd context (`ZSTD_customMem`). zstd
/// has no global allocator setting: each context gets its allocator when
/// it is created, and uses it for the objects that it creates, such as
/// the worker threads' contexts and buffers.
///
/// In libzstd 1.5.7, one exception remains: with worker threads,
/// long-distance matching allocates its tables through libc `malloc`. zstd
/// takes their allocator from the compression parameters, which never get
/// the context's allocator (`ZSTDMT_serialState_reset`).
#[cfg(feature = "mimalloc")]
fn zstd_mimalloc() -> zstd_sys::ZSTD_customMem {
    use std::ffi::c_void;

    use libmimalloc_sys::{mi_free, mi_malloc};

    unsafe extern "C" fn alloc(_opaque: *mut c_void, size: usize) -> *mut c_void {
        // SAFETY: `mi_malloc` accepts any size.
        let p = unsafe { mi_malloc(size) };
        // zstd writes zeros through the null result of a failed calloc
        // (`ZSTD_customCalloc`). End the process before that.
        assert!(!p.is_null(), "zstd cannot allocate {size} bytes");
        p
    }

    unsafe extern "C" fn free(_opaque: *mut c_void, p: *mut c_void) {
        // SAFETY: zstd frees only pointers from `alloc`.
        unsafe { mi_free(p) }
    }

    zstd_sys::ZSTD_customMem {
        customAlloc: Some(alloc),
        customFree: Some(free),
        opaque: std::ptr::null_mut(),
    }
}

/// A zstd compression context for one stream (`ZSTD_CCtx`). With the
/// `mimalloc` feature, it allocates through mimalloc.
///
/// The first error ends the stream. It can leave the context in an
/// undefined state, in which another streaming call is undefined behavior
/// (zstd.h). So each later call returns the same error and does not call
/// zstd.
pub struct ZstdCCtx {
    ptr: NonNull<zstd_sys::ZSTD_CCtx>,
    /// The error that ended the stream.
    error: Option<&'static str>,
}

/// A zstd decompression context for one stream (`ZSTD_DCtx`). With the
/// `mimalloc` feature, it allocates through mimalloc. The first error ends
/// the stream, as in `ZstdCCtx`.
pub struct ZstdDCtx {
    ptr: NonNull<zstd_sys::ZSTD_DCtx>,
    /// The error that ended the stream.
    error: Option<&'static str>,
}

// SAFETY: zstd keeps no thread-local state for a context, so each thread
// can use it in turn. Its worker threads belong to the context.
unsafe impl Send for ZstdCCtx {}
// SAFETY: as for `ZstdCCtx`.
unsafe impl Send for ZstdDCtx {}

/// What one streaming call of zstd did.
pub struct ZstdStatus {
    /// The input bytes that zstd consumed.
    pub read: usize,
    /// The output bytes that zstd wrote.
    pub written: usize,
    /// Compression: a lower bound on the bytes that zstd still holds; 0
    /// when the flush or the frame is complete. Decompression: 0 at the end
    /// of a frame.
    pub remaining: usize,
}

impl ZstdCCtx {
    /// A context that compresses at `level` and adds a checksum to the
    /// frame. It uses `threads` worker threads, or the calling thread when
    /// `threads` is 0. With `long_window_log`, it also does long-distance
    /// matching in a window of `1 << long_window_log` bytes.
    pub fn new(
        level: i32,
        threads: u32,
        long_window_log: Option<u32>,
    ) -> Result<Self, &'static str> {
        use ZSTD_cParameter::*;
        #[cfg(feature = "mimalloc")]
        // SAFETY: `zstd_mimalloc` gives a matching pair of allocation
        // functions.
        let cctx = unsafe { zstd_sys::ZSTD_createCCtx_advanced(zstd_mimalloc()) };
        #[cfg(not(feature = "mimalloc"))]
        // SAFETY: `ZSTD_createCCtx` has no preconditions.
        let cctx = unsafe { zstd_sys::ZSTD_createCCtx() };
        let cctx = ZstdCCtx {
            ptr: NonNull::new(cctx).expect("zstd cannot create a compression context"),
            error: None,
        };
        let set = |parameter, value| {
            // SAFETY: `cctx.ptr` is a valid context.
            let rc =
                unsafe { zstd_sys::ZSTD_CCtx_setParameter(cctx.ptr.as_ptr(), parameter, value) };
            zstd_result(rc)
        };
        // A value that `c_int` cannot hold saturates: zstd decreases a
        // worker count that is too large to its maximum, and refuses a
        // window log that is too large.
        let saturate = |value: u32| c_int::try_from(value).unwrap_or(c_int::MAX);
        // No experimental parameter: with some of them, zstd keeps pointers
        // to the buffers of `compress_stream` after the call.
        set(ZSTD_c_compressionLevel, level)?;
        set(ZSTD_c_checksumFlag, 1)?;
        set(ZSTD_c_nbWorkers, saturate(threads))?;
        if let Some(log) = long_window_log {
            set(ZSTD_c_enableLongDistanceMatching, 1)?;
            set(ZSTD_c_windowLog, saturate(log))?;
        }
        Ok(cctx)
    }

    /// `ZSTD_compressStream2`: compress bytes from `input` into `output`,
    /// then flush or end the frame as `end_op` tells.
    pub fn compress_stream(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        end_op: ZSTD_EndDirective,
    ) -> Result<ZstdStatus, &'static str> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: input.as_ptr().cast(),
            size: input.len(),
            pos: 0,
        };
        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: output.as_mut_ptr().cast(),
            size: output.len(),
            pos: 0,
        };
        // SAFETY: `self.ptr` is a valid context that no error left in an
        // undefined state, and the buffers describe `input` and `output`.
        // `new` sets no experimental parameter, so zstd keeps no pointer to
        // them after the call.
        let rc = unsafe {
            zstd_sys::ZSTD_compressStream2(
                self.ptr.as_ptr(),
                &mut out_buffer,
                &mut in_buffer,
                end_op,
            )
        };
        let remaining = zstd_result(rc).inspect_err(|&e| self.error = Some(e))?;
        Ok(ZstdStatus {
            remaining,
            read: in_buffer.pos,
            written: out_buffer.pos,
        })
    }

    /// The output size for `compress_stream` that holds a full block
    /// (`ZSTD_CStreamOutSize`).
    pub fn out_size() -> usize {
        // SAFETY: `ZSTD_CStreamOutSize` has no preconditions.
        unsafe { zstd_sys::ZSTD_CStreamOutSize() }
    }
}

impl Drop for ZstdCCtx {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` is a valid context that is not used again.
        // zstd frees it with the allocator that created it.
        unsafe { zstd_sys::ZSTD_freeCCtx(self.ptr.as_ptr()) };
    }
}

impl ZstdDCtx {
    pub fn new() -> Self {
        #[cfg(feature = "mimalloc")]
        // SAFETY: `zstd_mimalloc` gives a matching pair of allocation
        // functions.
        let dctx = unsafe { zstd_sys::ZSTD_createDCtx_advanced(zstd_mimalloc()) };
        #[cfg(not(feature = "mimalloc"))]
        // SAFETY: `ZSTD_createDCtx` has no preconditions.
        let dctx = unsafe { zstd_sys::ZSTD_createDCtx() };
        ZstdDCtx {
            ptr: NonNull::new(dctx).expect("zstd cannot create a decompression context"),
            error: None,
        }
    }

    /// `ZSTD_decompressStream`: decompress bytes from `input` into
    /// `output`.
    pub fn decompress_stream(
        &mut self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<ZstdStatus, &'static str> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let mut in_buffer = zstd_sys::ZSTD_inBuffer {
            src: input.as_ptr().cast(),
            size: input.len(),
            pos: 0,
        };
        let mut out_buffer = zstd_sys::ZSTD_outBuffer {
            dst: output.as_mut_ptr().cast(),
            size: output.len(),
            pos: 0,
        };
        // SAFETY: `self.ptr` is a valid context that no error left in an
        // undefined state, and the buffers describe `input` and `output`.
        // zstd keeps no pointer to them after the call.
        let rc = unsafe {
            zstd_sys::ZSTD_decompressStream(self.ptr.as_ptr(), &mut out_buffer, &mut in_buffer)
        };
        let remaining = zstd_result(rc).inspect_err(|&e| self.error = Some(e))?;
        Ok(ZstdStatus {
            remaining,
            read: in_buffer.pos,
            written: out_buffer.pos,
        })
    }

    /// The output size for `decompress_stream` that holds a full block
    /// (`ZSTD_DStreamOutSize`).
    pub fn out_size() -> usize {
        // SAFETY: `ZSTD_DStreamOutSize` has no preconditions.
        unsafe { zstd_sys::ZSTD_DStreamOutSize() }
    }
}

impl Drop for ZstdDCtx {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` is a valid context that is not used again.
        // zstd frees it with the allocator that created it.
        unsafe { zstd_sys::ZSTD_freeDCtx(self.ptr.as_ptr()) };
    }
}

/// A zstd return value: a size, or the name of an error.
fn zstd_result(code: usize) -> Result<usize, &'static str> {
    // SAFETY: `ZSTD_isError` accepts any value.
    if unsafe { zstd_sys::ZSTD_isError(code) } == 0 {
        return Ok(code);
    }
    // SAFETY: `ZSTD_getErrorName` returns a static string for any value.
    let name = unsafe { CStr::from_ptr(zstd_sys::ZSTD_getErrorName(code)) };
    Err(name.to_str().expect("zstd error names are ASCII"))
}

/// Start to load the cache line that holds `value`, for a read soon after.
/// This is only a hint: it cannot fault and changes no result. Other
/// architectures ignore it.
#[inline(always)]
pub fn prefetch<T>(value: &T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: a prefetch only loads a cache line, and `value` is a valid
    // reference.
    unsafe {
        use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
        _mm_prefetch::<_MM_HINT_T0>(std::ptr::from_ref(value).cast());
    }
    // `core::arch::aarch64::_prefetch` is unstable, so issue the
    // instruction: `pldl1keep` loads into L1 for a read, like
    // `_MM_HINT_T0`.
    #[cfg(target_arch = "aarch64")]
    // SAFETY: `prfm` only loads a cache line: it cannot fault and writes
    // no register, memory, or flag. `value` is a valid reference.
    unsafe {
        std::arch::asm!(
            "prfm pldl1keep, [{0}]",
            in(reg) std::ptr::from_ref(value),
            options(nostack, readonly, preserves_flags),
        );
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = value;
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
    #[cfg(feature = "mimalloc")]
    use libmimalloc_sys::mi_is_in_heap_region;
    use zstd_sys::ZSTD_EndDirective;

    use super::{ZstdCCtx, ZstdDCtx};

    /// After an error, a context returns it for each later call and does
    /// not call zstd. Here, zstd would accept the right checksum after the
    /// wrong one and end the frame.
    #[test]
    fn a_zstd_error_ends_the_stream() {
        let mut frame = vec![0; ZstdCCtx::out_size()];
        let status = ZstdCCtx::new(1, 0, None)
            .unwrap()
            .compress_stream(b"data", &mut frame, ZSTD_EndDirective::ZSTD_e_end)
            .unwrap();
        assert_eq!(status.remaining, 0);
        frame.truncate(status.written);
        // The frame ends with its 4-byte checksum.
        let checksum = frame.split_off(frame.len() - 4);
        let wrong: Vec<u8> = checksum.iter().map(|byte| byte ^ 1).collect();

        let mut dctx = ZstdDCtx::new();
        let mut out = vec![0; ZstdDCtx::out_size()];
        let mut decompress = |input: &[u8]| {
            dctx.decompress_stream(input, &mut out)
                .map(|status| status.read)
        };
        assert_eq!(decompress(&frame), Ok(frame.len()));
        let error = decompress(&wrong).unwrap_err();
        assert_eq!(decompress(&checksum), Err(error));
    }

    /// The build must not let mimalloc interpose the libc allocator: a
    /// dependency that enables `libmimalloc-sys/override` would replace
    /// `malloc` for the whole process. This check fails the build in that
    /// case.
    #[cfg(feature = "mimalloc")]
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
    #[cfg(feature = "mimalloc")]
    #[test]
    fn global_allocator_is_mimalloc() {
        let heap = Box::new([0u8; 64]);
        // SAFETY: `mi_is_in_heap_region` accepts any pointer.
        assert!(unsafe { mi_is_in_heap_region(heap.as_ptr().cast()) });
    }

    /// RocksDB allocates through mimalloc (`src/cxx_allocation.cc`). A
    /// pinned get copies a memtable value into a `std::string` that
    /// libstdc++ grows in its shared library, so this also shows that
    /// libstdc++ binds its own calls of `operator new` to the replacements.
    #[cfg(feature = "mimalloc")]
    #[test]
    fn rocksdb_allocates_through_mimalloc() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = rocksdb::DB::open(&options, dir.path()).unwrap();
        // Longer than the 15 bytes that a `std::string` holds inline.
        db.put(b"key", [7u8; 100]).unwrap();
        let value = db.get_pinned(b"key").unwrap().unwrap();
        // SAFETY: `mi_is_in_heap_region` accepts any pointer.
        assert!(unsafe { mi_is_in_heap_region(value.as_ptr().cast()) });
    }

    /// zstd allocates through mimalloc, starting with its contexts.
    #[cfg(feature = "mimalloc")]
    #[test]
    fn zstd_allocates_through_mimalloc() {
        let (cctx, dctx) = (ZstdCCtx::new(1, 0, None).unwrap(), ZstdDCtx::new());
        // SAFETY: `mi_is_in_heap_region` accepts any pointer.
        unsafe {
            assert!(mi_is_in_heap_region(cctx.ptr.as_ptr().cast()));
            assert!(mi_is_in_heap_region(dctx.ptr.as_ptr().cast()));
        }
    }
}
