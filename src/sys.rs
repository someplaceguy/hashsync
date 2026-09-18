//! Unsafe system-call wrappers. This is the only module that may use unsafe
//! code (DESIGN.md §11).
#![allow(unsafe_code)]

use std::mem;
use std::os::fd::{AsRawFd, BorrowedFd};

use linux_raw_sys::general::fsuuid2;
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
        (*handle).handle_bytes = max as libc::c_uint;
        let rc = libc::name_to_handle_at(
            fd.as_raw_fd(),
            c"".as_ptr(),
            handle,
            &mut mount_id,
            libc::AT_EMPTY_PATH,
        );
        if rc != 0 {
            let errno = last_errno();
            return if errno == Errno::NOTSUP {
                Ok(None)
            } else {
                Err(errno)
            };
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
    // SAFETY: the opcode is `_IOR(0x15, 0, struct fsuuid2)` from
    // `linux/fs.h`, and the kernel writes a `struct fsuuid2`.
    let getter = unsafe { Getter::<FS_IOC_GETFSUUID, fsuuid2>::new() };
    // SAFETY: the getter describes the opcode's output type.
    match unsafe { ioctl::ioctl(fd, getter) } {
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
