// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! Persistent identities of filesystems and inodes (DESIGN.md §4.3).
//!
//! A filesystem is identified by its UUID from `FS_IOC_GETFSUUID`, or by
//! `f_fsid` when the filesystem has no UUID. An inode is identified by its
//! `name_to_handle_at` handle, or by its inode number when the filesystem
//! has no handles. Both are stored with a tag byte that says which form was
//! used. `(filesystem, handle)` is the identity used by the index and by
//! the visited sets of one scan.

use std::collections::HashMap;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::{Arc, Mutex};

use rustix::fs::FileType;
use rustix::io::Errno;

use crate::fsops::{self, Meta};
use crate::sys;

pub const FSID_TAG_UUID: u8 = 1;
pub const FSID_TAG_FSID: u8 = 2;
pub const HANDLE_TAG_HANDLE: u8 = 1;
pub const HANDLE_TAG_INO: u8 = 2;

/// The key of the per-scan filesystem cache.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Triple {
    pub mnt_id: u64,
    pub dev_major: u32,
    pub dev_minor: u32,
}

impl Triple {
    pub fn of(meta: &Meta) -> Self {
        Triple {
            mnt_id: meta.mnt_id,
            dev_major: meta.dev_major,
            dev_minor: meta.dev_minor,
        }
    }
}

/// The persistent identity bytes of a filesystem, which the index stores
/// in the identity of each inode (DESIGN.md §4.4).
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct FsInfo {
    pub fsid: Vec<u8>,
}

/// Scan-local number of a filesystem. Equal identity bytes get one number,
/// even when reached through different mounts.
pub type FsIndex = u32;

/// Filesystem resolution cache for one scan.
#[derive(Default)]
pub struct FsCache {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    by_triple: HashMap<Triple, FsIndex>,
    by_fsid: HashMap<Vec<u8>, FsIndex>,
    infos: Vec<Arc<FsInfo>>,
}

impl FsCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// The filesystem of `triple`. `readable` must open any object on that
    /// filesystem for reading; it is called only when the triple is new.
    pub fn resolve(
        &self,
        triple: Triple,
        readable: impl FnOnce() -> Result<OwnedFd, Errno>,
    ) -> Result<FsIndex, Errno> {
        if let Some(&index) = self.inner.lock().unwrap().by_triple.get(&triple) {
            return Ok(index);
        }
        let fd = readable()?;
        let fsid = match sys::fs_uuid(fd.as_fd())? {
            Some(uuid) => {
                let mut v = vec![FSID_TAG_UUID];
                v.extend_from_slice(&uuid);
                v
            }
            None => {
                let statfs = rustix::io::retry_on_intr(|| rustix::fs::fstatfs(fd.as_fd()))?;
                let mut v = vec![FSID_TAG_FSID];
                v.extend_from_slice(&sys::fsid_bytes(&statfs.f_fsid));
                v
            }
        };
        let mut inner = self.inner.lock().unwrap();
        let index = match inner.by_fsid.get(&fsid) {
            Some(&index) => index,
            None => {
                let index = FsIndex::try_from(inner.infos.len()).expect("too many filesystems");
                inner.infos.push(Arc::new(FsInfo { fsid: fsid.clone() }));
                inner.by_fsid.insert(fsid, index);
                index
            }
        };
        inner.by_triple.insert(triple, index);
        Ok(index)
    }

    /// The persistent identity of a resolved filesystem.
    pub fn info(&self, fs: FsIndex) -> Arc<FsInfo> {
        self.inner.lock().unwrap().infos[fs as usize].clone()
    }
}

/// Identity of a directory or regular file within one scan.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Identity {
    pub fs: FsIndex,
    /// The tagged handle, which the index stores in the inode's identity.
    pub handle: Box<[u8]>,
}

/// The tagged inode handle of the object behind an `O_PATH` descriptor.
fn inode_handle(fd: BorrowedFd<'_>, meta: &Meta) -> Result<Box<[u8]>, Errno> {
    let mut v = match sys::name_to_handle(fd)? {
        Some(handle) => {
            let mut v = Vec::with_capacity(1 + handle.len());
            v.push(HANDLE_TAG_HANDLE);
            v.extend_from_slice(&handle);
            v
        }
        None => {
            let mut v = Vec::with_capacity(9);
            v.push(HANDLE_TAG_INO);
            v.extend_from_slice(&meta.ino.to_le_bytes());
            v
        }
    };
    v.shrink_to_fit();
    Ok(v.into_boxed_slice())
}

/// The identity of the object behind an `O_PATH` descriptor. Reopens the
/// object for reading when its filesystem is seen for the first time.
pub fn identify(cache: &FsCache, fd: BorrowedFd<'_>, meta: &Meta) -> Result<Identity, Errno> {
    let fs = cache.resolve(Triple::of(meta), || {
        if meta.kind == FileType::Directory {
            fsops::reopen_dir(fd)
        } else {
            fsops::reopen_file(fd)
        }
    })?;
    Ok(Identity {
        fs,
        handle: inode_handle(fd, meta)?,
    })
}
