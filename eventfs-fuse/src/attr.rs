use std::time::SystemTime;

use eventfs_transport::VersionStamp;
use fuser::{FileAttr, FileType};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StableOwner {
    pub uid: u32,
    pub gid: u32,
}

impl StableOwner {
    pub fn current_process() -> Self {
        Self {
            uid: nix_like_uid(),
            gid: nix_like_gid(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StableTimestamps {
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
}

impl StableTimestamps {
    pub fn from_version(version: VersionStamp) -> Self {
        Self {
            atime: version.modified,
            mtime: version.modified,
            ctime: version.modified,
            crtime: version.created,
        }
    }
}

pub fn build_attr(
    ino: u64,
    kind: FileType,
    perm: u16,
    owner: StableOwner,
    timestamps: StableTimestamps,
    size: u64,
) -> FileAttr {
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: timestamps.atime,
        mtime: timestamps.mtime,
        ctime: timestamps.ctime,
        crtime: timestamps.crtime,
        kind,
        perm,
        nlink: if kind == FileType::Directory { 2 } else { 1 },
        uid: owner.uid,
        gid: owner.gid,
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

fn nix_like_uid() -> u32 {
    #[cfg(target_family = "unix")]
    {
        unsafe { libc::geteuid() }
    }
    #[cfg(not(target_family = "unix"))]
    {
        0
    }
}

fn nix_like_gid() -> u32 {
    #[cfg(target_family = "unix")]
    {
        unsafe { libc::getegid() }
    }
    #[cfg(not(target_family = "unix"))]
    {
        0
    }
}
