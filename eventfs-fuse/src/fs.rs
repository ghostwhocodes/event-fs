#[cfg(test)]
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventfs_protocol::{json_lines, Errno, JetStreamAction, MaterializedTarget, MountPath};
#[cfg(test)]
use eventfs_protocol::{JetStreamPath, MetadataFile, AGENTS_BUCKET};
use eventfs_transport::{DirectoryEntry, EntryKind, MountStorage, VersionStamp, WritebackQueue};
#[cfg(test)]
use eventfs_transport::{
    FailedWrite, FailedWriteOperation, InvalidationPlan, KvSourceGeneration,
    ObjectSourceGeneration, TransportResult, WatchEvent,
};
use fuser::{
    FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
};

use crate::attr::{build_attr, StableOwner, StableTimestamps};
use crate::path_cache::{PathCache, ROOT_INO};

mod queued_overlay;
mod runtime;

use runtime::MountProjectionKind;

const TTL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandleMode {
    WholeValue,
    JsonLines,
}

struct Handle {
    path: MountPath,
    buffer: Vec<u8>,
    dirty: bool,
    committed: bool,
    writable: bool,
    commit_seed: String,
    mode: HandleMode,
    base_offset: usize,
    staged_create: Option<StagedCreate>,
}

pub(super) trait MountPathInput {
    fn into_mount_path(self) -> Result<MountPath, i32>;
}

impl MountPathInput for MountPath {
    fn into_mount_path(self) -> Result<MountPath, i32> {
        Ok(self)
    }
}

impl MountPathInput for &MountPath {
    fn into_mount_path(self) -> Result<MountPath, i32> {
        Ok(self.clone())
    }
}

impl MountPathInput for &str {
    fn into_mount_path(self) -> Result<MountPath, i32> {
        MountPath::new(self).map_err(|err| err.errno().get())
    }
}

impl MountPathInput for String {
    fn into_mount_path(self) -> Result<MountPath, i32> {
        MountPath::new(self).map_err(|err| err.errno().get())
    }
}

impl MountPathInput for &String {
    fn into_mount_path(self) -> Result<MountPath, i32> {
        MountPath::new(self).map_err(|err| err.errno().get())
    }
}

#[derive(Clone, Debug)]
struct FileSnapshot {
    bytes: Vec<u8>,
    version: VersionStamp,
}

#[derive(Clone, Copy, Debug)]
struct FileMetadata {
    size: u64,
    version: VersionStamp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DynamicPathKind {
    Missing,
    File,
    Directory,
}

struct QueuedJsonlEntry {
    idempotency_key: String,
    stream: String,
    subject: String,
    bytes: Vec<u8>,
    applied_lines: usize,
    version: VersionStamp,
}

#[derive(Clone, Debug)]
struct StagedCreate {
    default_payload: Vec<u8>,
    version: VersionStamp,
}

struct KvRenameCompletion {
    from_path: MountPath,
    to_path: MountPath,
    from_bucket: String,
    from_key: String,
    expected_from_revision: u64,
    to_bucket: String,
    to_key: String,
    bytes: Vec<u8>,
    seed: String,
}

struct ObjectRenameCompletion {
    from_path: MountPath,
    to_path: MountPath,
    from_bucket: String,
    from_object: String,
    expected_from_sequence: u64,
    expected_from_nuid: String,
    to_bucket: String,
    to_object: String,
    bytes: Vec<u8>,
    seed: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SetattrMetadataFields {
    mode: bool,
    uid: bool,
    gid: bool,
    atime: bool,
    mtime: bool,
    ctime: bool,
    crtime: bool,
    chgtime: bool,
    bkuptime: bool,
    flags: bool,
}

impl SetattrMetadataFields {
    fn requested(self) -> bool {
        self.mode
            || self.uid
            || self.gid
            || self.atime
            || self.mtime
            || self.ctime
            || self.crtime
            || self.chgtime
            || self.bkuptime
            || self.flags
    }
}

pub struct JetStreamFuse {
    runtime_state: runtime::MountRuntimeState,
    paths: PathCache,
    owner: StableOwner,
}

impl JetStreamFuse {
    pub fn new(
        backend: Box<dyn MountStorage>,
        queue: WritebackQueue,
        mount_name: impl Into<String>,
    ) -> Self {
        let mut fs = Self {
            runtime_state: runtime::MountRuntimeState::new(backend, queue, mount_name),
            paths: PathCache::new(),
            owner: StableOwner::current_process(),
        };
        fs.runtime_state.replay_queue(&mut fs.paths);
        fs
    }

    pub(crate) fn mount_runtime(&mut self) -> runtime::MountRuntime<'_> {
        runtime::MountRuntime::new(&mut self.runtime_state, &mut self.paths)
    }

    fn path_for_ino(&self, ino: u64) -> Result<MountPath, i32> {
        self.paths.path(ino).cloned().ok_or(libc::ENOENT)
    }

    fn allocate_for_path(&mut self, path: &MountPath, kind: FileType) -> u64 {
        self.runtime_state.clear_stale_path(path);
        self.paths.ensure(path, kind)
    }

    fn parent_ino_for_path(&mut self, path: &MountPath) -> u64 {
        if path.is_root() {
            return ROOT_INO;
        }
        self.allocate_for_path(&path.parent(), FileType::Directory)
    }

    fn directory_reply_entries(
        &mut self,
        path: &MountPath,
        ino: u64,
    ) -> Result<Vec<(u64, FileType, String)>, i32> {
        let entries = self.mount_runtime().list(path).map_err(|err| err.errno())?;
        let mut reply_entries = Vec::with_capacity(entries.len() + 2);
        reply_entries.push((ino, FileType::Directory, ".".to_string()));
        reply_entries.push((
            self.parent_ino_for_path(path),
            FileType::Directory,
            "..".to_string(),
        ));
        for entry in entries {
            let child_path = path.join_child(&entry.name).map_err(|err| {
                self.runtime_state
                    .record_error(format!("invalid directory child {}: {err}", entry.name));
                err.errno().get()
            })?;
            let kind = file_type_from_runtime(entry.kind);
            let child_ino = self.allocate_for_path(&child_path, kind);
            reply_entries.push((child_ino, kind, entry.name));
        }
        Ok(reply_entries)
    }

    fn attr_for_path(
        &mut self,
        path: impl MountPathInput,
        _uid: u32,
        _gid: u32,
    ) -> Result<fuser::FileAttr, i32> {
        let path = path.into_mount_path()?;
        let runtime_attr = self
            .mount_runtime()
            .stat(&path)
            .map_err(|err| err.errno())?;
        Ok(self.file_attr_from_runtime(&path, runtime_attr, None))
    }

    fn file_attr_from_runtime(
        &mut self,
        path: &MountPath,
        runtime_attr: runtime::RuntimeAttr,
        perm: Option<u16>,
    ) -> fuser::FileAttr {
        let kind = file_type_from_runtime(runtime_attr.kind);
        let ino = self.allocate_for_path(path, kind);
        let perm = perm.unwrap_or(if kind == FileType::Directory {
            0o755
        } else {
            0o644
        });
        self.build_path_attr(
            ino,
            kind,
            perm,
            runtime_attr.size,
            StableTimestamps::from_version(runtime_attr.version),
        )
    }

    fn build_path_attr(
        &self,
        ino: u64,
        kind: FileType,
        perm: u16,
        size: u64,
        timestamps: StableTimestamps,
    ) -> fuser::FileAttr {
        build_attr(ino, kind, perm, self.owner, timestamps, size)
    }

    fn placeholder_regular_file_attr(
        &mut self,
        path: impl MountPathInput,
        perm: u16,
        size: u64,
    ) -> fuser::FileAttr {
        let path = path
            .into_mount_path()
            .expect("placeholder attr path must be a valid mount path");
        let ino = self.allocate_for_path(&path, FileType::RegularFile);
        self.build_path_attr(
            ino,
            FileType::RegularFile,
            perm,
            size,
            StableTimestamps::from_version(self.mount_version()),
        )
    }

    fn regular_file_reply_attr(
        &mut self,
        path: impl MountPathInput,
        perm: u16,
        fallback_size: u64,
    ) -> Result<fuser::FileAttr, i32> {
        let path = path.into_mount_path()?;
        match self.mount_runtime().stat(&path) {
            Ok(attr) => Ok(self.file_attr_from_runtime(&path, attr, Some(perm))),
            Err(err) if err.errno() == libc::ENOENT => {
                Ok(self.placeholder_regular_file_attr(&path, perm, fallback_size))
            }
            Err(err) => Err(err.errno()),
        }
    }

    fn create_reply_attr(
        &mut self,
        path: impl MountPathInput,
        fh: u64,
        perm: u16,
    ) -> Result<fuser::FileAttr, i32> {
        let path = path.into_mount_path()?;
        let runtime_attr = self
            .mount_runtime()
            .created_handle_attr(runtime::RuntimeHandleId::from_raw(fh), &path)
            .map_err(|err| err.errno())?;
        Ok(self.file_attr_from_runtime(&path, runtime_attr, Some(perm)))
    }

    fn truncate_handle_reply_attr(
        &mut self,
        path: &MountPath,
        fh: u64,
    ) -> Result<fuser::FileAttr, i32> {
        let runtime_attr = self
            .mount_runtime()
            .created_handle_attr(runtime::RuntimeHandleId::from_raw(fh), path)
            .map_err(|err| err.errno())?;
        self.mount_runtime()
            .truncate_handle(runtime::RuntimeHandleId::from_raw(fh))
            .map_err(|err| err.errno())?;
        let kind = file_type_from_runtime(runtime_attr.kind);
        let ino = self.allocate_for_path(path, kind);
        let perm = if kind == FileType::Directory {
            0o755
        } else {
            0o644
        };
        Ok(self.build_path_attr(
            ino,
            kind,
            perm,
            0,
            StableTimestamps::from_version(runtime_attr.version),
        ))
    }

    fn mount_version(&self) -> VersionStamp {
        self.runtime_state.mount_version()
    }
}

#[cfg(test)]
#[allow(dead_code)]
impl JetStreamFuse {
    fn replay_queue(&mut self) {
        self.runtime_state.replay_queue(&mut self.paths);
    }

    fn apply_pending_watch_events(&mut self) {
        self.runtime_state
            .apply_pending_watch_events(&mut self.paths);
    }

    fn writes_blocked(&self) -> bool {
        self.runtime_state.writes_blocked()
    }

    fn pending_write_count(&self) -> usize {
        self.runtime_state.pending_write_count()
    }

    fn pending_overlays(&self) -> Vec<eventfs_transport::PendingWriteOverlay> {
        self.runtime_state.pending_overlays()
    }

    fn cache_contains(&self, path: impl AsRef<str>) -> bool {
        self.runtime_state.cache_contains(path.as_ref())
    }

    fn cache_is_empty(&self) -> bool {
        self.runtime_state.cache_is_empty()
    }

    fn stale_paths_is_empty(&self) -> bool {
        self.runtime_state.stale_paths_is_empty()
    }

    fn stale_paths_contains(&self, path: &MountPath) -> bool {
        self.runtime_state.stale_paths_contains(path)
    }

    fn handle_committed(&self, fh: u64) -> Option<bool> {
        self.runtime_state.handle_committed(fh)
    }

    fn handle_probe(&self, fh: u64) -> Option<runtime::RuntimeHandleProbe> {
        self.runtime_state.handle_probe(fh)
    }

    fn set_mounted_at_for_test(&mut self, mounted_at: SystemTime) {
        self.runtime_state.set_mounted_at_for_test(mounted_at);
    }

    fn enqueue_failed_write_for_test(&mut self, write: FailedWrite) -> TransportResult<()> {
        self.runtime_state.enqueue_failed_write_for_test(write)
    }

    fn kind_for_path(
        &mut self,
        path: impl MountPathInput,
        parsed: &JetStreamPath,
    ) -> Result<FileType, i32> {
        let path = path.into_mount_path()?;
        let _ = parsed;
        self.mount_runtime()
            .stat(&path)
            .map(|attr| file_type_from_runtime(attr.kind))
            .map_err(|err| err.errno())
    }

    fn read_bytes(
        &mut self,
        path: impl MountPathInput,
        parsed: &JetStreamPath,
    ) -> Result<Vec<u8>, i32> {
        let path = path.into_mount_path()?;
        let _ = parsed;
        self.mount_runtime().read(&path).map_err(|err| err.errno())
    }

    fn read_bytes_for_handle(
        &mut self,
        fh: u64,
        path: impl MountPathInput,
        parsed: &JetStreamPath,
    ) -> Result<Vec<u8>, i32> {
        let path = path.into_mount_path()?;
        self.runtime_state
            .read_bytes_for_handle(&mut self.paths, fh, &path, parsed)
    }

    fn directory_entries(&mut self, parsed: &JetStreamPath) -> Result<Vec<DirectoryEntry>, i32> {
        let path = eventfs_protocol::mount_path_from_jetstream_path(parsed)
            .map_err(|err| err.errno().get())?;
        self.mount_runtime()
            .list(&path)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(directory_entry_from_runtime)
                    .collect()
            })
            .map_err(|err| err.errno())
    }

    fn metadata_bytes(&self, file: MetadataFile) -> Vec<u8> {
        self.runtime_state.metadata_bytes(&self.paths, file)
    }

    fn open_handle(
        &mut self,
        path: impl MountPathInput,
        writable: bool,
        truncate: bool,
        create: bool,
    ) -> Result<u64, i32> {
        let path = path.into_mount_path()?;
        self.runtime_state
            .open_handle(&mut self.paths, &path, writable, truncate, create)
    }

    fn write_to_handle_buffer(&mut self, fh: u64, offset: i64, data: &[u8]) -> Result<usize, i32> {
        self.runtime_state.write_to_handle_buffer(fh, offset, data)
    }

    fn stage_truncate(&mut self, fh: u64) -> Result<(), i32> {
        self.runtime_state.stage_truncate(fh)
    }

    fn reject_append_only_truncate(&mut self, path: impl MountPathInput) -> Result<(), i32> {
        let path = path.into_mount_path()?;
        self.runtime_state
            .reject_append_only_truncate(&mut self.paths, &path)
    }

    fn commit_handle(&mut self, fh: u64) -> Result<(), i32> {
        self.runtime_state.commit_handle(&mut self.paths, fh)
    }

    fn flush_handle(&mut self, fh: u64) -> Result<(), i32> {
        self.runtime_state.flush_handle(fh)
    }

    fn commit_path(&mut self, path: &str, bytes: &[u8]) -> Result<(), i32> {
        let path = MountPath::new(path).map_err(|err| err.errno().get())?;
        self.runtime_state
            .commit_path(&mut self.paths, &path, bytes)
    }

    fn execute_unlink(&mut self, path: impl MountPathInput) -> Result<(), i32> {
        let path = path.into_mount_path()?;
        self.runtime_state.execute_unlink(&mut self.paths, &path)
    }

    fn execute_mkdir(&mut self, path: impl MountPathInput) -> Result<(), i32> {
        let path = path.into_mount_path()?;
        self.runtime_state.execute_mkdir(&mut self.paths, &path)
    }

    fn execute_rename(
        &mut self,
        from_path: impl MountPathInput,
        to_path: impl MountPathInput,
        flags: u32,
    ) -> Result<(), i32> {
        let from_path = from_path.into_mount_path()?;
        let to_path = to_path.into_mount_path()?;
        self.runtime_state
            .execute_rename(&mut self.paths, &from_path, &to_path, flags)
    }

    fn execute_mknod(&mut self, path: impl MountPathInput, mode: u32) -> Result<u64, i32> {
        let path = path.into_mount_path()?;
        self.runtime_state
            .execute_mknod(&mut self.paths, &path, mode)
    }
}

impl Filesystem for JetStreamFuse {
    fn lookup(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(path) = self.paths.child_path(parent, name) else {
            reply.error(libc::ENOENT);
            return;
        };
        match self.attr_for_path(&path, req.uid(), req.gid()) {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn getattr(&mut self, req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self
            .path_for_ino(ino)
            .and_then(|path| self.attr_for_path(&path, req.uid(), req.gid()))
        {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(errno) => reply.error(errno),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, flags: i32, reply: ReplyOpen) {
        reply.opened(0, fuse_open_reply_flags(flags));
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let reply_entries = match self.directory_reply_entries(&path, ino) {
            Ok(entries) => entries,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        let start = offset.max(0) as usize;
        for (index, (entry_ino, kind, name)) in reply_entries.into_iter().enumerate().skip(start) {
            if reply.add(entry_ino, (index + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        match self
            .mount_runtime()
            .open(&path, runtime_open_options_from_flags(flags, false))
        {
            Ok(handle) => reply.opened(handle.raw(), fuse_open_reply_flags(flags)),
            Err(err) => reply.error(err.errno()),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        match self
            .mount_runtime()
            .read_handle(runtime::RuntimeHandleId::from_raw(fh), &path)
        {
            Ok(bytes) => {
                let start = offset as usize;
                let end = start.saturating_add(size as usize).min(bytes.len());
                let data = if start >= bytes.len() {
                    &[]
                } else {
                    &bytes[start..end]
                };
                reply.data(data);
            }
            Err(err) => reply.error(err.errno()),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let handle = match self.mount_runtime().writable_handle(fh, &path) {
            Ok(handle) => handle,
            Err(err) => {
                reply.error(err.errno());
                return;
            }
        };
        match self.mount_runtime().write(handle, offset, data) {
            Ok(written) => reply.written(written as u32),
            Err(err) => reply.error(err.errno()),
        }
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let Some(path) = self.paths.child_path(parent, name) else {
            reply.error(libc::ENOENT);
            return;
        };
        if let Err(err) = self.mount_runtime().mknod(&path, mode) {
            reply.error(err.errno());
            return;
        }
        let attr = match self.regular_file_reply_attr(&path, (mode & 0o7777) as u16, 0) {
            Ok(attr) => attr,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        reply.entry(&TTL, &attr, 0);
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(path) = self.paths.child_path(parent, name) else {
            reply.error(libc::ENOENT);
            return;
        };
        if let Err(err) = self.mount_runtime().mkdir(&path) {
            reply.error(err.errno());
            return;
        };
        let ino = self.allocate_for_path(&path, FileType::Directory);
        let attr = self.build_path_attr(
            ino,
            FileType::Directory,
            (mode & 0o7777) as u16,
            0,
            StableTimestamps::from_version(self.mount_version()),
        );
        reply.entry(&TTL, &attr, 0);
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(path) = self.paths.child_path(parent, name) else {
            reply.error(libc::ENOENT);
            return;
        };
        let handle = match self
            .mount_runtime()
            .open(&path, runtime_open_options_from_flags(flags, true))
        {
            Ok(handle) => handle,
            Err(err) => {
                reply.error(err.errno());
                return;
            }
        };
        let fh = handle.raw();
        let attr = match self.create_reply_attr(&path, fh, (mode & 0o7777) as u16) {
            Ok(attr) => attr,
            Err(errno) => {
                self.mount_runtime()
                    .discard(runtime::RuntimeHandleId::from_raw(fh));
                reply.error(errno);
                return;
            }
        };
        reply.created(&TTL, &attr, 0, fh, fuse_open_reply_flags(flags));
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(path) = self.paths.child_path(parent, name) else {
            reply.error(libc::ENOENT);
            return;
        };
        match self.mount_runtime().unlink(&path) {
            Ok(_) => reply.ok(),
            Err(err) => reply.error(err.errno()),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::NOT_SUPPORTED.get());
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        let Some(path) = self.paths.child_path(parent, name) else {
            reply.error(libc::ENOENT);
            return;
        };
        let Some(new_path) = self.paths.child_path(newparent, newname) else {
            reply.error(libc::ENOENT);
            return;
        };
        match self.mount_runtime().rename(&path, &new_path, flags) {
            Ok(_) => reply.ok(),
            Err(err) => reply.error(err.errno()),
        }
    }

    fn setattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<std::time::SystemTime>,
        fh: Option<u64>,
        crtime: Option<std::time::SystemTime>,
        chgtime: Option<std::time::SystemTime>,
        bkuptime: Option<std::time::SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let metadata_fields = SetattrMetadataFields {
            mode: mode.is_some(),
            uid: uid.is_some(),
            gid: gid.is_some(),
            atime: atime.is_some(),
            mtime: mtime.is_some(),
            ctime: ctime.is_some(),
            crtime: crtime.is_some(),
            chgtime: chgtime.is_some(),
            bkuptime: bkuptime.is_some(),
            flags: flags.is_some(),
        };
        if metadata_fields.requested() {
            reply.error(libc::ENOTSUP);
            return;
        }
        if let Some(requested_size) = size {
            if requested_size != 0 {
                reply.error(libc::ENOTSUP);
                return;
            }
            if let Some(fh) = fh {
                match self.truncate_handle_reply_attr(&path, fh) {
                    Ok(attr) => reply.attr(&TTL, &attr),
                    Err(errno) => reply.error(errno),
                }
                return;
            }
            if let Err(err) = self.mount_runtime().truncate_path(&path) {
                reply.error(err.errno());
                return;
            }
        }
        match self.attr_for_path(&path, req.uid(), req.gid()) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(errno) => reply.error(errno),
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        match self
            .mount_runtime()
            .flush(runtime::RuntimeHandleId::from_raw(fh))
        {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err.errno()),
        }
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self
            .mount_runtime()
            .commit(runtime::RuntimeHandleId::from_raw(fh))
        {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err.errno()),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self
            .mount_runtime()
            .release(runtime::RuntimeHandleId::from_raw(fh))
        {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err.errno()),
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 1_000_000, 1_000_000, 512, 255, 512);
    }
}

fn names_as_dirs(names: Vec<String>) -> Vec<DirectoryEntry> {
    names
        .into_iter()
        .map(|name| DirectoryEntry {
            name,
            kind: EntryKind::Directory,
        })
        .collect()
}

fn names_as_event_files(names: Vec<String>) -> Vec<DirectoryEntry> {
    names
        .into_iter()
        .map(|name| DirectoryEntry {
            name: format!("{name}.jsonl"),
            kind: EntryKind::File,
        })
        .collect()
}

fn file_type_from_runtime(kind: runtime::RuntimeEntryKind) -> FileType {
    match kind {
        runtime::RuntimeEntryKind::Directory => FileType::Directory,
        runtime::RuntimeEntryKind::File => FileType::RegularFile,
    }
}

fn projection_kind_from_file_type(kind: FileType) -> MountProjectionKind {
    match kind {
        FileType::Directory => MountProjectionKind::Directory,
        _ => MountProjectionKind::File,
    }
}

#[cfg(test)]
fn directory_entry_from_runtime(entry: runtime::RuntimeDirectoryEntry) -> DirectoryEntry {
    DirectoryEntry {
        name: entry.name,
        kind: match entry.kind {
            runtime::RuntimeEntryKind::Directory => EntryKind::Directory,
            runtime::RuntimeEntryKind::File => EntryKind::File,
        },
    }
}

#[cfg(test)]
fn unlink_invalidation_paths(path: &str) -> Vec<String> {
    local_invalidation_paths(path)
}

#[cfg(test)]
fn local_invalidation_paths(path: &str) -> Vec<String> {
    let path = MountPath::new(path).unwrap();
    InvalidationPlan::for_local_mutation(&path)
        .map(|plan| {
            plan.actions()
                .iter()
                .map(|action| action.path.as_str().to_string())
                .collect()
        })
        .unwrap_or_else(|_| vec![path.into_string()])
}

fn directory_listing_unavailable(errno: i32) -> bool {
    matches!(errno, libc::ENOENT | libc::EIO)
}

fn insert_directory_entry(entries: &mut Vec<DirectoryEntry>, entry: DirectoryEntry) {
    if let Some(existing) = entries
        .iter_mut()
        .find(|candidate| candidate.name == entry.name)
    {
        if entry.kind == EntryKind::File {
            existing.kind = EntryKind::File;
        }
        return;
    }
    entries.push(entry);
}

fn insert_directory_entry_sorted(entries: &mut Vec<DirectoryEntry>, entry: DirectoryEntry) {
    if let Some(existing) = entries
        .iter_mut()
        .find(|candidate| candidate.name == entry.name)
    {
        if entry.kind == EntryKind::Directory {
            existing.kind = EntryKind::Directory;
        }
        return;
    }
    let position = entries
        .iter()
        .position(|candidate| candidate.name > entry.name)
        .unwrap_or(entries.len());
    entries.insert(position, entry);
}

fn directory_entries_contain_exact_file(entries: &[DirectoryEntry], name: &str) -> bool {
    entries
        .iter()
        .any(|entry| entry.name == name && entry.kind == EntryKind::File)
}

fn fuse_open_reply_flags(_request_flags: i32) -> u32 {
    0
}

fn runtime_open_options_from_flags(flags: i32, create: bool) -> runtime::RuntimeOpenOptions {
    let writable = create || flags & libc::O_ACCMODE != libc::O_RDONLY;
    runtime::RuntimeOpenOptions {
        writable,
        truncate: !create && writable && flags & libc::O_TRUNC != 0,
        create,
    }
}

fn handle_commit_bytes(handle: &Handle) -> Vec<u8> {
    if handle.mode == HandleMode::WholeValue && handle.buffer.is_empty() {
        if let Some(staged_create) = &handle.staged_create {
            return staged_create.default_payload.clone();
        }
    }
    handle.buffer.clone()
}

fn message_sequence(entry: &DirectoryEntry) -> Option<u64> {
    if entry.kind != EntryKind::File {
        return None;
    }
    entry.name.strip_suffix(".json")?.parse().ok()
}

fn append_json_line(output: &mut Vec<u8>, payload: &[u8]) {
    output.extend_from_slice(payload);
    if !payload.ends_with(b"\n") {
        output.push(b'\n');
    }
}

fn action_publishes_jsonl(action: &JetStreamAction) -> bool {
    matches!(
        action,
        JetStreamAction::PublishJsonLines { .. }
            | JetStreamAction::MaterializedPut {
                target: MaterializedTarget::Stream { .. },
            }
    )
}

fn jsonl_canonical_lines(path: &str, bytes: &[u8]) -> Result<Vec<Vec<u8>>, i32> {
    json_lines(path, bytes)
        .map_err(|err| err.errno().get())
        .map(|lines| {
            lines
                .into_iter()
                .map(|line| {
                    let mut bytes = Vec::new();
                    append_json_line(&mut bytes, line.as_bytes());
                    bytes
                })
                .collect()
        })
}

fn jsonl_applied_prefix_count(
    path: &str,
    base: &[u8],
    current: &[u8],
    pending: &[u8],
) -> Result<usize, i32> {
    let base = jsonl_canonical_lines(path, base)?;
    let current = jsonl_canonical_lines(path, current)?;
    let pending = jsonl_canonical_lines(path, pending)?;
    if current.len() < base.len() || current[..base.len()] != base[..] {
        return Ok(0);
    }
    let appended = &current[base.len()..];
    Ok(appended
        .iter()
        .zip(pending.iter())
        .take_while(|(left, right)| left == right)
        .count())
}

fn unapplied_jsonl_bytes(path: &str, bytes: &[u8], applied_lines: usize) -> Result<Vec<u8>, i32> {
    let mut output = Vec::new();
    for line in jsonl_canonical_lines(path, bytes)?
        .into_iter()
        .skip(applied_lines)
    {
        output.extend_from_slice(&line);
    }
    Ok(output)
}

fn mount_instance_seed() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

fn idempotency_key(path: &str, bytes: &[u8], commit_seed: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    commit_seed.hash(&mut hasher);
    path.hash(&mut hasher);
    bytes.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn rename_completion_key_material(
    from_path: &str,
    source_generation: u64,
    bytes: &[u8],
) -> Vec<u8> {
    let mut material = Vec::new();
    material.extend_from_slice(from_path.as_bytes());
    material.push(0);
    material.extend_from_slice(source_generation.to_string().as_bytes());
    material.push(0);
    material.extend_from_slice(bytes);
    material
}

fn max_system_time(current: Option<SystemTime>, candidate: SystemTime) -> SystemTime {
    match current {
        Some(current) if current.duration_since(candidate).is_ok() => current,
        _ => candidate,
    }
}

fn max_version_stamp(current: Option<VersionStamp>, candidate: VersionStamp) -> VersionStamp {
    match current {
        Some(current) if current.modified.duration_since(candidate.modified).is_ok() => current,
        _ => candidate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventfs_protocol::AffectedPathReason;
    use eventfs_protocol::{SEMANTIC_BUCKET, TASKS_BUCKET};
    use eventfs_transport::{
        KeyRevision, MemoryStorage, ObjectMetadata, ObjectVersion, PendingWritePayload,
        ReplayStorage, StreamMessageView, TransportError, WritebackReplay,
    };
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    type BytesByPath = Arc<Mutex<HashMap<(String, String), Vec<u8>>>>;
    type TimesByPath = Arc<Mutex<HashMap<(String, String), SystemTime>>>;
    type RevisionsByPath = Arc<Mutex<HashMap<(String, String), u64>>>;
    type KvHistories = Arc<Mutex<HashMap<(String, String), Vec<KeyRevision>>>>;
    type KvPuts = Arc<Mutex<Vec<(String, String, Vec<u8>)>>>;
    type Publishes = Arc<Mutex<Vec<(String, String, Vec<u8>, String)>>>;
    type Streams = Arc<Mutex<HashMap<String, Vec<StreamMessageView>>>>;
    type ObjectSequences = Arc<Mutex<HashMap<(String, String), u64>>>;
    type MissingNames = Arc<Mutex<Vec<String>>>;
    type WatchEvents = Arc<Mutex<Vec<WatchEvent>>>;
    type WatchFailure = Arc<Mutex<bool>>;
    type DirectoryListFailure = Arc<Mutex<bool>>;
    type KvReadFailure = Arc<Mutex<bool>>;
    type ObjectReadFailure = Arc<Mutex<bool>>;
    type WriteFailure = Arc<Mutex<bool>>;
    type DeleteFailure = Arc<Mutex<bool>>;
    type JsonlFailure = Arc<Mutex<Option<usize>>>;

    fn mp(path: impl AsRef<str>) -> MountPath {
        MountPath::new(path.as_ref()).unwrap()
    }

    #[derive(Clone, Default)]
    struct MemoryBackend {
        kv: BytesByPath,
        kv_times: TimesByPath,
        kv_revisions: RevisionsByPath,
        histories: KvHistories,
        objects: BytesByPath,
        object_times: TimesByPath,
        object_sequences: ObjectSequences,
        streams: Streams,
        kv_puts: KvPuts,
        publishes: Publishes,
        missing_kv_buckets: MissingNames,
        missing_streams: MissingNames,
        watch_events: WatchEvents,
        watch_fails: WatchFailure,
        directory_list_fails: DirectoryListFailure,
        kv_get_fails: KvReadFailure,
        object_get_fails: ObjectReadFailure,
        write_fails: WriteFailure,
        delete_fails: DeleteFailure,
        jsonl_fail_after_lines: JsonlFailure,
    }

    impl MemoryBackend {
        fn push_watch_event(&self, event: WatchEvent) {
            self.watch_events.lock().unwrap().push(event);
        }

        fn fail_watch_events(&self) {
            *self.watch_fails.lock().unwrap() = true;
        }

        fn fail_directory_lists(&self) {
            *self.directory_list_fails.lock().unwrap() = true;
        }

        fn fail_kv_reads(&self) {
            *self.kv_get_fails.lock().unwrap() = true;
        }

        fn fail_writes(&self) {
            *self.write_fails.lock().unwrap() = true;
        }

        fn fail_deletes(&self) {
            *self.delete_fails.lock().unwrap() = true;
        }

        fn allow_deletes(&self) {
            *self.delete_fails.lock().unwrap() = false;
        }

        fn fail_jsonl_after_lines(&self, lines: usize) {
            *self.jsonl_fail_after_lines.lock().unwrap() = Some(lines);
        }

        fn allow_writes(&self) {
            *self.write_fails.lock().unwrap() = false;
        }

        fn fail_object_reads(&self) {
            *self.object_get_fails.lock().unwrap() = true;
        }

        fn kv_timestamp(&self, bucket: &str, key: &str) -> SystemTime {
            self.kv_times
                .lock()
                .unwrap()
                .get(&(bucket.into(), key.into()))
                .copied()
                .unwrap_or_else(|| test_time(1))
        }

        fn kv_revision_number(&self, bucket: &str, key: &str) -> u64 {
            self.kv_revisions
                .lock()
                .unwrap()
                .get(&(bucket.into(), key.into()))
                .copied()
                .unwrap_or(1)
        }

        fn object_timestamp(&self, bucket: &str, object: &str) -> SystemTime {
            self.object_times
                .lock()
                .unwrap()
                .get(&(bucket.into(), object.into()))
                .copied()
                .unwrap_or_else(|| test_time(1))
        }

        fn object_sequence(&self, bucket: &str, object: &str) -> u64 {
            self.object_sequences
                .lock()
                .unwrap()
                .get(&(bucket.into(), object.into()))
                .copied()
                .unwrap_or(1)
        }
    }

    impl ReplayStorage for MemoryBackend {
        fn kv_put_idempotent(
            &self,
            bucket: &str,
            key: &str,
            bytes: &[u8],
            _idempotency_key: &str,
        ) -> TransportResult<u64> {
            self.kv_put(bucket, key, bytes)
        }

        fn kv_put_applied(
            &self,
            _bucket: &str,
            _key: &str,
            _idempotency_key: &str,
        ) -> TransportResult<bool> {
            Ok(false)
        }

        fn kv_delete_if_revision(
            &self,
            bucket: &str,
            key: &str,
            expected_revision: u64,
        ) -> TransportResult<()> {
            match self.kv_get(bucket, key)? {
                None => Ok(()),
                Some(entry) if entry.revision != expected_revision => Ok(()),
                Some(_) => self.kv_delete(bucket, key),
            }
        }

        fn kv_delete_if_revision_applied(
            &self,
            bucket: &str,
            key: &str,
            expected_revision: u64,
        ) -> TransportResult<bool> {
            Ok(match self.kv_get(bucket, key)? {
                None => true,
                Some(entry) => entry.revision != expected_revision,
            })
        }

        fn publish_json_lines(
            &self,
            stream: &str,
            subject: &str,
            bytes: &[u8],
            idempotency_seed: &str,
        ) -> TransportResult<Vec<u64>> {
            if *self.write_fails.lock().unwrap() {
                return Err(TransportError::Invalid("write down".into()));
            }
            self.publishes.lock().unwrap().push((
                stream.into(),
                subject.into(),
                bytes.to_vec(),
                idempotency_seed.into(),
            ));
            let lines = eventfs_protocol::json_lines(subject, bytes)
                .map_err(|err| TransportError::Invalid(err.to_string()))?;
            let fail_after = *self.jsonl_fail_after_lines.lock().unwrap();
            let mut streams = self.streams.lock().unwrap();
            let messages = streams.entry(stream.into()).or_default();
            let mut sequences = Vec::new();
            for (index, line) in lines.iter().enumerate() {
                if fail_after.is_some_and(|limit| index >= limit) {
                    return Err(TransportError::Invalid("partial publish down".into()));
                }
                let sequence = messages
                    .iter()
                    .map(|message| message.sequence)
                    .max()
                    .unwrap_or(0)
                    + 1;
                messages.push(stream_message_at(
                    stream,
                    sequence,
                    SystemTime::now(),
                    subject,
                    line.as_bytes(),
                ));
                sequences.push(sequence);
            }
            Ok(sequences)
        }

        fn publish_json_lines_applied(
            &self,
            stream: &str,
            subject: &str,
            bytes: &[u8],
            idempotency_seed: &str,
        ) -> TransportResult<bool> {
            Ok(
                self.publish_json_lines_applied_prefix(stream, subject, bytes, idempotency_seed)?
                    == eventfs_protocol::json_lines(subject, bytes)
                        .map_err(|err| TransportError::Invalid(err.to_string()))?
                        .len(),
            )
        }

        fn publish_json_lines_applied_prefix(
            &self,
            stream: &str,
            subject: &str,
            bytes: &[u8],
            _idempotency_seed: &str,
        ) -> TransportResult<usize> {
            let messages = self.streams.lock().unwrap();
            let mut current = Vec::new();
            for message in messages.get(stream).into_iter().flatten() {
                if message.subject == subject {
                    append_json_line(&mut current, &message.payload);
                }
            }
            jsonl_applied_prefix_count(subject, &[], &current, bytes)
                .map_err(|errno| TransportError::Invalid(format!("jsonl prefix errno {errno}")))
        }

        fn object_put_idempotent(
            &self,
            bucket: &str,
            object: &str,
            bytes: &[u8],
            _idempotency_key: &str,
        ) -> TransportResult<()> {
            self.object_put(bucket, object, bytes)
        }

        fn object_put_applied(
            &self,
            _bucket: &str,
            _object: &str,
            _idempotency_key: &str,
        ) -> TransportResult<bool> {
            Ok(false)
        }

        fn object_delete_if_sequence(
            &self,
            bucket: &str,
            object: &str,
            expected_sequence: u64,
            expected_nuid: &str,
        ) -> TransportResult<()> {
            match self.object_get(bucket, object)? {
                None => Ok(()),
                Some(entry)
                    if entry.sequence != expected_sequence
                        || (!expected_nuid.is_empty() && entry.nuid != expected_nuid) =>
                {
                    Ok(())
                }
                Some(_) => self.object_delete(bucket, object),
            }
        }

        fn object_delete_if_sequence_applied(
            &self,
            bucket: &str,
            object: &str,
            expected_sequence: u64,
            expected_nuid: &str,
        ) -> TransportResult<bool> {
            Ok(match self.object_get(bucket, object)? {
                None => true,
                Some(entry) => {
                    entry.sequence != expected_sequence
                        || (!expected_nuid.is_empty() && entry.nuid != expected_nuid)
                }
            })
        }
    }

    impl MountStorage for MemoryBackend {
        fn list_kv_buckets(&self) -> TransportResult<Vec<String>> {
            if *self.directory_list_fails.lock().unwrap() {
                return Err(TransportError::Invalid("list down".into()));
            }
            let mut buckets = Vec::new();
            buckets.extend(
                self.kv
                    .lock()
                    .unwrap()
                    .keys()
                    .map(|(bucket, _)| bucket.clone()),
            );
            buckets.extend(
                self.histories
                    .lock()
                    .unwrap()
                    .keys()
                    .map(|(bucket, _)| bucket.clone()),
            );
            buckets.extend(
                self.kv_puts
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(bucket, _, _)| bucket.clone()),
            );
            buckets.sort();
            buckets.dedup();
            Ok(buckets)
        }

        fn ensure_kv_bucket(&self, _bucket: &str) -> TransportResult<()> {
            Ok(())
        }

        fn list_kv_prefix(
            &self,
            bucket: &str,
            prefix: &str,
        ) -> TransportResult<Vec<DirectoryEntry>> {
            if *self.directory_list_fails.lock().unwrap() {
                return Err(TransportError::Invalid("list down".into()));
            }
            if self
                .missing_kv_buckets
                .lock()
                .unwrap()
                .iter()
                .any(|candidate| candidate == bucket)
            {
                return Err(TransportError::NotFound);
            }
            let kv = self.kv.lock().unwrap();
            let keys = kv
                .keys()
                .filter_map(|(entry_bucket, key)| (entry_bucket == bucket).then_some(key.as_str()));
            Ok(immediate_children_for_test(keys, prefix))
        }

        fn kv_get(&self, bucket: &str, key: &str) -> TransportResult<Option<KeyRevision>> {
            if *self.kv_get_fails.lock().unwrap() {
                return Err(TransportError::Invalid("kv read down".into()));
            }
            Ok(self
                .kv
                .lock()
                .unwrap()
                .get(&(bucket.into(), key.into()))
                .map(|bytes| KeyRevision {
                    revision: self.kv_revision_number(bucket, key),
                    created: self.kv_timestamp(bucket, key),
                    bytes: bytes.clone(),
                }))
        }

        fn kv_put(&self, bucket: &str, key: &str, bytes: &[u8]) -> TransportResult<u64> {
            if *self.write_fails.lock().unwrap() {
                return Err(TransportError::Invalid("write down".into()));
            }
            let revision_key = (bucket.to_string(), key.to_string());
            let previous_revision = {
                let revisions = self.kv_revisions.lock().unwrap();
                revisions.get(&revision_key).copied()
            }
            .unwrap_or_else(|| {
                if self.kv.lock().unwrap().contains_key(&revision_key) {
                    1
                } else {
                    0
                }
            });
            self.kv
                .lock()
                .unwrap()
                .insert((bucket.into(), key.into()), bytes.to_vec());
            self.kv_times
                .lock()
                .unwrap()
                .insert((bucket.into(), key.into()), SystemTime::now());
            let mut revisions = self.kv_revisions.lock().unwrap();
            let next = previous_revision + 1;
            revisions.insert(revision_key, next);
            self.kv_puts
                .lock()
                .unwrap()
                .push((bucket.into(), key.into(), bytes.to_vec()));
            Ok(1)
        }

        fn kv_delete(&self, bucket: &str, key: &str) -> TransportResult<()> {
            if *self.delete_fails.lock().unwrap() {
                return Err(TransportError::Invalid("delete down".into()));
            }
            if self
                .kv
                .lock()
                .unwrap()
                .remove(&(bucket.into(), key.into()))
                .is_some()
            {
                Ok(())
            } else {
                Err(TransportError::NotFound)
            }
        }

        fn kv_history(&self, bucket: &str, key: &str) -> TransportResult<Vec<KeyRevision>> {
            Ok(self
                .histories
                .lock()
                .unwrap()
                .get(&(bucket.into(), key.into()))
                .cloned()
                .unwrap_or_default())
        }

        fn list_kv_history_prefix(
            &self,
            bucket: &str,
            prefix: &str,
        ) -> TransportResult<Vec<DirectoryEntry>> {
            if *self.directory_list_fails.lock().unwrap() {
                return Err(TransportError::Invalid("list down".into()));
            }
            let histories = self.histories.lock().unwrap();
            let keys = histories
                .keys()
                .filter_map(|(entry_bucket, key)| (entry_bucket == bucket).then_some(key.as_str()));
            Ok(immediate_history_children_for_test(keys, prefix))
        }

        fn kv_revision(
            &self,
            bucket: &str,
            key: &str,
            revision: u64,
        ) -> TransportResult<Option<KeyRevision>> {
            Ok(self
                .histories
                .lock()
                .unwrap()
                .get(&(bucket.into(), key.into()))
                .and_then(|items| {
                    items
                        .iter()
                        .find(|entry| entry.revision == revision)
                        .cloned()
                }))
        }

        fn list_streams(&self) -> TransportResult<Vec<String>> {
            if *self.directory_list_fails.lock().unwrap() {
                return Err(TransportError::Invalid("list down".into()));
            }
            let mut streams: Vec<_> = self.streams.lock().unwrap().keys().cloned().collect();
            streams.sort();
            Ok(streams)
        }

        fn ensure_stream(&self, _stream: &str) -> TransportResult<()> {
            Ok(())
        }

        fn list_stream_messages(&self, _stream: &str) -> TransportResult<Vec<DirectoryEntry>> {
            if *self.directory_list_fails.lock().unwrap() {
                return Err(TransportError::Invalid("list down".into()));
            }
            if self
                .missing_streams
                .lock()
                .unwrap()
                .iter()
                .any(|candidate| candidate == _stream)
            {
                return Err(TransportError::NotFound);
            }
            let messages = self.streams.lock().unwrap();
            Ok(messages
                .get(_stream)
                .into_iter()
                .flatten()
                .map(|message| DirectoryEntry {
                    name: format!("{}.json", message.sequence),
                    kind: EntryKind::File,
                })
                .collect())
        }

        fn list_stream_subjects(&self, stream: &str) -> TransportResult<Vec<DirectoryEntry>> {
            if *self.directory_list_fails.lock().unwrap() {
                return Err(TransportError::Invalid("list down".into()));
            }
            if self
                .missing_streams
                .lock()
                .unwrap()
                .iter()
                .any(|candidate| candidate == stream)
            {
                return Err(TransportError::NotFound);
            }
            let mut subjects = BTreeSet::new();
            if let Some(messages) = self.streams.lock().unwrap().get(stream) {
                for message in messages {
                    subjects.insert(message.subject.clone());
                }
            }
            Ok(subjects
                .into_iter()
                .map(|subject| DirectoryEntry {
                    name: eventfs_protocol::stream_subject_file_name_from_str(&subject),
                    kind: EntryKind::File,
                })
                .collect())
        }

        fn list_agent_names(&self) -> TransportResult<Vec<String>> {
            if *self.directory_list_fails.lock().unwrap() {
                return Err(TransportError::Invalid("list down".into()));
            }
            if self
                .missing_streams
                .lock()
                .unwrap()
                .iter()
                .any(|candidate| candidate == eventfs_protocol::subjects::AGENTS_STREAM)
            {
                return Err(TransportError::NotFound);
            }
            let mut agents = BTreeSet::new();
            if let Some(messages) = self
                .streams
                .lock()
                .unwrap()
                .get(eventfs_protocol::subjects::AGENTS_STREAM)
            {
                for message in messages {
                    let mut parts = message.subject.split('.');
                    if matches!(parts.next(), Some("agents")) {
                        if let (Some(agent), Some(area), None) =
                            (parts.next(), parts.next(), parts.next())
                        {
                            if matches!(area, "inbox" | "outbox")
                                && !eventfs_protocol::is_reserved_kv_key(agent)
                            {
                                agents.insert(agent.to_string());
                            }
                        }
                    }
                }
            }
            Ok(agents.into_iter().collect())
        }

        fn stream_message(
            &self,
            stream: &str,
            sequence: u64,
        ) -> TransportResult<StreamMessageView> {
            self.streams
                .lock()
                .unwrap()
                .get(stream)
                .and_then(|messages| {
                    messages
                        .iter()
                        .find(|message| message.sequence == sequence)
                        .cloned()
                })
                .ok_or(TransportError::NotFound)
        }

        fn list_object_buckets(&self) -> TransportResult<Vec<String>> {
            if *self.directory_list_fails.lock().unwrap() {
                return Err(TransportError::Invalid("list down".into()));
            }
            let mut buckets: Vec<_> = self
                .objects
                .lock()
                .unwrap()
                .keys()
                .map(|(bucket, _)| bucket.clone())
                .collect();
            buckets.sort();
            buckets.dedup();
            Ok(buckets)
        }

        fn ensure_object_bucket(&self, _bucket: &str) -> TransportResult<()> {
            Ok(())
        }

        fn list_object_prefix(
            &self,
            bucket: &str,
            prefix: &str,
        ) -> TransportResult<Vec<DirectoryEntry>> {
            if *self.directory_list_fails.lock().unwrap() {
                return Err(TransportError::Invalid("list down".into()));
            }
            let objects = self.objects.lock().unwrap();
            let keys = objects
                .keys()
                .filter_map(|(entry_bucket, key)| (entry_bucket == bucket).then_some(key.as_str()));
            Ok(immediate_children_for_test(keys, prefix))
        }

        fn object_get(&self, bucket: &str, object: &str) -> TransportResult<Option<ObjectVersion>> {
            if *self.object_get_fails.lock().unwrap() {
                return Err(TransportError::Invalid("object read down".into()));
            }
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(&(bucket.into(), object.into()))
                .map(|bytes| ObjectVersion {
                    modified: self.object_timestamp(bucket, object),
                    sequence: self.object_sequence(bucket, object),
                    nuid: format!("memory-object-{}", self.object_sequence(bucket, object)),
                    bytes: bytes.clone(),
                }))
        }

        fn object_metadata(
            &self,
            bucket: &str,
            object: &str,
        ) -> TransportResult<Option<ObjectMetadata>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(&(bucket.into(), object.into()))
                .map(|bytes| ObjectMetadata {
                    modified: self.object_timestamp(bucket, object),
                    size: bytes.len() as u64,
                    sequence: self.object_sequence(bucket, object),
                }))
        }

        fn object_put(&self, bucket: &str, object: &str, bytes: &[u8]) -> TransportResult<()> {
            if *self.write_fails.lock().unwrap() {
                return Err(TransportError::Invalid("write down".into()));
            }
            let sequence_key = (bucket.to_string(), object.to_string());
            let previous_sequence = {
                let sequences = self.object_sequences.lock().unwrap();
                sequences.get(&sequence_key).copied()
            }
            .unwrap_or_else(|| {
                if self.objects.lock().unwrap().contains_key(&sequence_key) {
                    1
                } else {
                    0
                }
            });
            self.objects
                .lock()
                .unwrap()
                .insert((bucket.into(), object.into()), bytes.to_vec());
            self.object_times
                .lock()
                .unwrap()
                .insert((bucket.into(), object.into()), SystemTime::now());
            let mut sequences = self.object_sequences.lock().unwrap();
            let next = previous_sequence + 1;
            sequences.insert(sequence_key, next);
            Ok(())
        }

        fn object_delete(&self, bucket: &str, object: &str) -> TransportResult<()> {
            if *self.delete_fails.lock().unwrap() {
                return Err(TransportError::Invalid("delete down".into()));
            }
            if self
                .objects
                .lock()
                .unwrap()
                .remove(&(bucket.into(), object.into()))
                .is_some()
            {
                Ok(())
            } else {
                Err(TransportError::NotFound)
            }
        }

        fn watch_events(&self) -> TransportResult<Vec<WatchEvent>> {
            if *self.watch_fails.lock().unwrap() {
                return Err(TransportError::Invalid("watch gap".into()));
            }
            Ok(self.watch_events.lock().unwrap().drain(..).collect())
        }
    }

    fn test_fs(backend: MemoryBackend) -> JetStreamFuse {
        let queue = WritebackQueue::open(test_queue_dir(), 16).unwrap();
        JetStreamFuse::new(Box::new(backend), queue, "test")
    }

    fn test_queue_dir() -> std::path::PathBuf {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        std::mem::forget(dir);
        path
    }

    fn enqueue_pending_operation(
        fs: &mut JetStreamFuse,
        id: &str,
        operation: FailedWriteOperation,
    ) {
        let diagnostic_path = failed_write_diagnostic_path(&operation);
        fs.enqueue_failed_write_for_test(FailedWrite::new(
            id,
            VersionStamp::at(test_time(20)),
            diagnostic_path,
            operation,
        ))
        .unwrap();
    }

    fn seed_pending_write(queue_dir: &std::path::Path, id: &str, operation: FailedWriteOperation) {
        let diagnostic_path = failed_write_diagnostic_path(&operation);
        let mut replay = WritebackReplay::open(queue_dir, 16).unwrap();
        replay
            .enqueue_failed_write(FailedWrite::new(
                id,
                VersionStamp::at(test_time(20)),
                diagnostic_path,
                operation,
            ))
            .unwrap();
    }

    fn failed_write_diagnostic_path(operation: &FailedWriteOperation) -> MountPath {
        match operation {
            FailedWriteOperation::KvPut { bucket, key, .. }
            | FailedWriteOperation::MaterializedPut { bucket, key, .. } => {
                MountPath::new(format!("/kv/{bucket}/{key}")).unwrap()
            }
            FailedWriteOperation::KvRenameComplete {
                to_bucket, to_key, ..
            } => MountPath::new(format!("/kv/{to_bucket}/{to_key}")).unwrap(),
            FailedWriteOperation::ObjectPut { bucket, object, .. } => {
                MountPath::new(format!("/objects/{bucket}/{object}")).unwrap()
            }
            FailedWriteOperation::ObjectRenameComplete {
                to_bucket,
                to_object,
                ..
            } => MountPath::new(format!("/objects/{to_bucket}/{to_object}")).unwrap(),
            FailedWriteOperation::PublishJsonLines {
                stream, subject, ..
            } => MountPath::new(format!(
                "/streams/{stream}/subjects/{}.jsonl",
                eventfs_protocol::stream_subject_file_name_from_str(subject)
            ))
            .unwrap(),
        }
    }

    fn queued_kv_source_delete(bucket: &str, key: &str) -> FailedWriteOperation {
        FailedWriteOperation::KvRenameComplete {
            from_bucket: bucket.into(),
            from_key: key.into(),
            source: KvSourceGeneration { revision: 1 },
            to_bucket: "__eventfs_queued_rename_target".into(),
            to_key: format!("{bucket}/{key}.moved"),
            bytes: b"queued rename target".to_vec(),
        }
    }

    fn queued_object_source_delete(bucket: &str, object: &str) -> FailedWriteOperation {
        FailedWriteOperation::ObjectRenameComplete {
            from_bucket: bucket.into(),
            from_object: object.into(),
            source: ObjectSourceGeneration {
                sequence: 1,
                nuid: "queued-source-nuid".into(),
            },
            to_bucket: "__eventfs_queued_rename_target".into(),
            to_object: format!("{bucket}/{object}.moved"),
            bytes: b"queued object rename target".to_vec(),
        }
    }

    #[test]
    fn mount_runtime_stats_reads_lists_and_metadata_without_fuser_types() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (
                eventfs_protocol::subjects::TASKS_BUCKET.into(),
                "demo/render.json".into(),
            ),
            br#"{"state":"ready"}"#.to_vec(),
        );
        let mut fs = test_fs(backend);
        let mut runtime = fs.mount_runtime();

        let attr = runtime.stat(&mp("/tasks/demo/render.json")).unwrap();
        assert_eq!(attr.kind, runtime::RuntimeEntryKind::File);
        assert_eq!(attr.size, br#"{"state":"ready"}"#.len() as u64);
        assert_eq!(
            runtime.read(&mp("/tasks/demo/render.json")).unwrap(),
            br#"{"state":"ready"}"#
        );
        assert!(runtime
            .list(&mp("/tasks/demo"))
            .unwrap()
            .iter()
            .any(|entry| entry.name == "render.json"
                && entry.kind == runtime::RuntimeEntryKind::File));
        let status =
            serde_json::from_slice::<serde_json::Value>(&runtime.metadata(MetadataFile::Status))
                .unwrap();
        assert_eq!(status["mount_name"], "test");
    }

    #[test]
    fn mount_runtime_handle_lifecycle_commits_whole_value_write() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend.clone());
        let path = mp("/tasks/demo/runtime-created.json");

        {
            let mut runtime = fs.mount_runtime();
            let handle = runtime
                .open(
                    &path,
                    runtime::RuntimeOpenOptions {
                        writable: true,
                        create: true,
                        truncate: false,
                    },
                )
                .unwrap();
            assert_eq!(
                runtime
                    .write(handle, 0, br#"{"created":"through-runtime"}"#)
                    .unwrap(),
                br#"{"created":"through-runtime"}"#.len()
            );
            runtime.commit(handle).unwrap();
            runtime.flush(handle).unwrap();
            runtime.release(handle).unwrap();
        }

        assert_eq!(
            backend
                .kv
                .lock()
                .unwrap()
                .get(&(
                    eventfs_protocol::subjects::TASKS_BUCKET.into(),
                    "demo/runtime-created.json".into(),
                ))
                .unwrap(),
            br#"{"created":"through-runtime"}"#
        );
    }

    #[test]
    fn mount_runtime_mutations_update_mount_visible_state() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);
        let bucket = mp("/kv/runtime");
        let file = mp("/kv/runtime/item.json");

        let mut runtime = fs.mount_runtime();
        runtime.mkdir(&bucket).unwrap();
        runtime.mknod(&file, libc::S_IFREG | 0o644).unwrap();
        assert!(runtime.list(&bucket).unwrap().iter().any(
            |entry| entry.name == "item.json" && entry.kind == runtime::RuntimeEntryKind::File
        ));
        let attr = runtime.stat(&file).unwrap();
        assert_eq!(attr.kind, runtime::RuntimeEntryKind::File);
        assert_eq!(attr.size, 0);

        runtime.unlink(&file).unwrap();
        assert_eq!(runtime.stat(&file).unwrap_err().errno(), libc::ENOENT);
    }

    #[test]
    fn mount_runtime_boundary_does_not_borrow_fuse_adapter() {
        let runtime_source = include_str!("fs/runtime.rs");
        assert!(
            !runtime_source.contains("JetStreamFuse"),
            "MountRuntime must depend on MountRuntimeState, not the FUSE adapter"
        );
        assert!(
            !runtime_source.contains("self.fs"),
            "runtime methods must not delegate mount-visible behavior through JetStreamFuse"
        );

        let fs_source = include_str!("fs.rs");
        let exported_projection_module = concat!("pub mod ", "mount_projection");
        let exported_projection_reexport = concat!("pub use ", "mount_projection");
        assert!(
            !fs_source.contains(exported_projection_module),
            "mount projection must not be an exported module seam"
        );
        assert!(
            !fs_source.contains(exported_projection_reexport),
            "mount projection types must not be publicly re-exported from the FUSE adapter"
        );
        let adapter_owned_runtime_state_impl = concat!("impl runtime::", "MountRuntimeState");
        assert!(
            !fs_source.contains(adapter_owned_runtime_state_impl),
            "MountRuntimeState behavior must live in runtime.rs or runtime-owned submodules"
        );
        assert!(
            runtime_source.contains(concat!("impl ", "MountRuntimeState")),
            "runtime.rs must own the MountRuntimeState behavior implementation"
        );
        for exposed_field in [
            concat!("pub(super) ", "backend"),
            concat!("pub(super) ", "stale_paths"),
            concat!("pub(super) ", "handles"),
            concat!("pub(super) ", "cache"),
            concat!("pub(super) ", "replay"),
            concat!("pub(super) ", "recent_errors"),
        ] {
            assert!(
                !runtime_source.contains(exposed_field),
                "MountRuntimeState fields must stay private inside the runtime module"
            );
        }
        let production_adapter = fs_source
            .split(concat!("\nimpl ", "JetStreamFuse {\n"))
            .nth(1)
            .and_then(|tail| {
                tail.split(concat!(
                    "#[cfg(test)]\n",
                    "#[allow(dead_code)]\n",
                    "impl JetStreamFuse"
                ))
                .next()
            })
            .expect("production JetStreamFuse impl must be separated from test harness helpers");
        for forbidden in [
            "fn apply_pending_watch_events(",
            "fn apply_invalidation_plan(",
            "fn classify_kv_dynamic_path(",
            "fn classify_object_dynamic_path(",
            "fn metadata_bytes(",
            "fn open_handle(",
            "fn build_handle(",
            "fn write_to_handle_buffer(",
            "fn stage_truncate(",
            "fn commit_handle(",
            "fn release_handle(",
            "fn commit_mount_path_with_seed(",
            "fn enqueue_action(",
            "fn execute_unlink(",
            "fn execute_mkdir(",
            "fn execute_rename(",
            "fn queue_kv_rename_completion(",
            "fn queue_object_rename_completion(",
        ] {
            assert!(
                !production_adapter.contains(forbidden),
                "production JetStreamFuse adapter must not own mount policy helper {forbidden}"
            );
        }
    }

    #[test]
    fn fuse_adapter_open_flags_map_to_runtime_options() {
        assert_eq!(
            runtime_open_options_from_flags(libc::O_RDONLY, false),
            runtime::RuntimeOpenOptions {
                writable: false,
                truncate: false,
                create: false,
            }
        );
        assert_eq!(
            runtime_open_options_from_flags(libc::O_WRONLY | libc::O_TRUNC, false),
            runtime::RuntimeOpenOptions {
                writable: true,
                truncate: true,
                create: false,
            }
        );
        assert_eq!(
            runtime_open_options_from_flags(libc::O_RDONLY, true),
            runtime::RuntimeOpenOptions {
                writable: true,
                truncate: false,
                create: true,
            }
        );
    }

    #[test]
    fn fuse_adapter_directory_reply_entries_map_runtime_entries_to_inodes() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "a.json".into()),
            br#"{"value":1}"#.to_vec(),
        );
        let mut fs = test_fs(backend);
        let bucket = mp("/kv/bucket");
        let bucket_ino = fs.allocate_for_path(&bucket, FileType::Directory);

        let entries = fs.directory_reply_entries(&bucket, bucket_ino).unwrap();

        assert!(entries.iter().any(|(ino, kind, name)| *ino == bucket_ino
            && *kind == FileType::Directory
            && name == "."));
        assert!(entries
            .iter()
            .any(|(_, kind, name)| *kind == FileType::Directory && name == ".."));
        let (file_ino, file_kind, _) = entries
            .iter()
            .find(|(_, _, name)| name == "a.json")
            .cloned()
            .expect("runtime file entry should be adapted into a FUSE reply entry");
        assert_eq!(file_kind, FileType::RegularFile);
        assert_eq!(fs.paths.path(file_ino), Some(&mp("/kv/bucket/a.json")));
    }

    fn assert_directory_entry(entries: &[DirectoryEntry], name: &str, kind: EntryKind) {
        assert!(
            entries
                .iter()
                .any(|entry| entry.name == name && entry.kind == kind),
            "expected {name:?} with kind {kind:?} in {entries:?}"
        );
    }

    fn assert_no_directory_entry(entries: &[DirectoryEntry], name: &str) {
        assert!(
            entries.iter().all(|entry| entry.name != name),
            "did not expect {name:?} in {entries:?}"
        );
    }

    fn immediate_children_for_test<'a>(
        keys: impl IntoIterator<Item = &'a str>,
        prefix: &str,
    ) -> Vec<DirectoryEntry> {
        let normalized_prefix = prefix.trim_matches('/');
        let prefix_with_slash = if normalized_prefix.is_empty() {
            String::new()
        } else {
            format!("{normalized_prefix}/")
        };

        let mut entries = Vec::<DirectoryEntry>::new();
        for key in keys {
            if !key.starts_with(&prefix_with_slash) {
                continue;
            }
            let rest = &key[prefix_with_slash.len()..];
            if rest.is_empty() {
                continue;
            }
            let (name, kind) = match rest.split_once('/') {
                Some((dir, _)) => (dir.to_string(), EntryKind::Directory),
                None => (rest.to_string(), EntryKind::File),
            };
            merge_child_entry_for_test(&mut entries, name, kind);
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        entries
    }

    fn merge_child_entry_for_test(
        entries: &mut Vec<DirectoryEntry>,
        name: String,
        kind: EntryKind,
    ) {
        if let Some(entry) = entries.iter_mut().find(|entry| entry.name == name) {
            if kind == EntryKind::File {
                entry.kind = EntryKind::File;
            }
        } else {
            entries.push(DirectoryEntry { name, kind });
        }
    }

    fn immediate_history_children_for_test<'a>(
        keys: impl IntoIterator<Item = &'a str>,
        prefix: &str,
    ) -> Vec<DirectoryEntry> {
        immediate_children_for_test(keys, prefix)
            .into_iter()
            .map(|entry| DirectoryEntry {
                name: entry.name,
                kind: EntryKind::Directory,
            })
            .collect()
    }

    fn stream_message(
        stream: &str,
        sequence: u64,
        subject: &str,
        payload: &[u8],
    ) -> StreamMessageView {
        stream_message_at(stream, sequence, test_time(sequence), subject, payload)
    }

    fn stream_message_at(
        stream: &str,
        sequence: u64,
        published: SystemTime,
        subject: &str,
        payload: &[u8],
    ) -> StreamMessageView {
        StreamMessageView {
            stream: stream.into(),
            sequence,
            published,
            subject: subject.into(),
            payload: payload.to_vec(),
        }
    }

    fn key_revision(revision: u64, bytes: &[u8]) -> KeyRevision {
        key_revision_at(revision, test_time(revision), bytes)
    }

    fn key_revision_at(revision: u64, created: SystemTime, bytes: &[u8]) -> KeyRevision {
        KeyRevision {
            revision,
            created,
            bytes: bytes.to_vec(),
        }
    }

    fn test_time(seconds: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(seconds)
    }

    #[test]
    fn fuse_stage_commit_publishes_handle_once() {
        let backend = MemoryBackend::default();
        let publishes = backend.publishes.clone();
        let mut fs = test_fs(backend);
        let fh = fs
            .open_handle(
                "/streams/ORDERS/subjects/orders.created.jsonl",
                true,
                false,
                false,
            )
            .unwrap();
        fs.write_to_handle_buffer(
            fh,
            0,
            br#"{"id":1}
"#,
        )
        .unwrap();

        fs.commit_handle(fh).unwrap();
        fs.commit_handle(fh).unwrap();

        let publishes = publishes.lock().unwrap();
        assert_eq!(publishes.len(), 1);
        assert_eq!(publishes[0].0, "ORDERS");
        assert_eq!(publishes[0].1, "orders.created");
    }

    #[test]
    fn fuse_flush_keeps_dirty_handle_staged_until_commit() {
        let backend = MemoryBackend::default();
        let kv_puts = backend.kv_puts.clone();
        let mut fs = test_fs(backend);
        let fh = fs
            .open_handle(mp("/tasks/demo/render.json"), true, false, true)
            .unwrap();
        fs.write_to_handle_buffer(fh, 0, b"not-json").unwrap();

        fs.flush_handle(fh).unwrap();
        assert!(kv_puts.lock().unwrap().is_empty());
        assert_eq!(fs.pending_write_count(), 0);
        assert!(!fs.handle_committed(fh).unwrap());

        assert_eq!(fs.commit_handle(fh).unwrap_err(), libc::EINVAL);
        assert!(kv_puts.lock().unwrap().is_empty());

        fs.write_to_handle_buffer(fh, 0, br#"{"state":"new"}"#)
            .unwrap();
        fs.flush_handle(fh).unwrap();
        assert!(kv_puts.lock().unwrap().is_empty());
        assert!(!fs.handle_committed(fh).unwrap());
        fs.commit_handle(fh).unwrap();
        fs.flush_handle(fh).unwrap();

        let puts = kv_puts.lock().unwrap();
        assert_eq!(puts.len(), 1);
        assert_eq!(puts[0].0, eventfs_protocol::subjects::TASKS_BUCKET);
        assert_eq!(puts[0].1, "demo/render.json");
        assert_eq!(puts[0].2, br#"{"state":"new"}"#);
    }

    #[test]
    fn fuse_scopes_publish_idempotency_to_handle_commit() {
        let backend = MemoryBackend::default();
        let publishes = backend.publishes.clone();
        let mut fs = test_fs(backend);

        let line = br#"{"id":1}
"#;
        for index in 0..2 {
            let fh = fs
                .open_handle(
                    "/streams/ORDERS/subjects/orders.created.jsonl",
                    true,
                    false,
                    false,
                )
                .unwrap();
            fs.write_to_handle_buffer(fh, (index * line.len()) as i64, line)
                .unwrap();
            fs.commit_handle(fh).unwrap();
        }

        let publishes = publishes.lock().unwrap();
        assert_eq!(publishes.len(), 2);
        assert_ne!(publishes[0].3, publishes[1].3);
    }

    #[test]
    fn fuse_jsonl_handle_publishes_only_new_bytes_after_fsync() {
        let backend = MemoryBackend::default();
        let publishes = backend.publishes.clone();
        let mut fs = test_fs(backend);
        let first = br#"{"id":1}
"#;
        let second = br#"{"id":2}
"#;
        let fh = fs
            .open_handle(
                "/streams/ORDERS/subjects/orders.created.jsonl",
                true,
                false,
                false,
            )
            .unwrap();

        fs.write_to_handle_buffer(fh, 0, first).unwrap();
        fs.commit_handle(fh).unwrap();
        fs.write_to_handle_buffer(fh, first.len() as i64, second)
            .unwrap();
        fs.commit_handle(fh).unwrap();

        let publishes = publishes.lock().unwrap();
        assert_eq!(publishes.len(), 2);
        assert_eq!(publishes[0].2, first);
        assert_eq!(publishes[1].2, second);
    }

    #[test]
    fn fuse_jsonl_append_handle_starts_at_materialized_eof() {
        let backend = MemoryBackend::default();
        backend.streams.lock().unwrap().insert(
            "system".into(),
            vec![stream_message(
                "system",
                1,
                "events.system",
                br#"{"event":1}"#,
            )],
        );
        let publishes = backend.publishes.clone();
        let mut fs = test_fs(backend);
        let existing = br#"{"event":1}
"#;
        let next = br#"{"event":2}
"#;
        let fh = fs
            .open_handle(mp("/events/system.jsonl"), true, false, false)
            .unwrap();

        fs.write_to_handle_buffer(fh, existing.len() as i64, next)
            .unwrap();
        fs.commit_handle(fh).unwrap();

        let publishes = publishes.lock().unwrap();
        assert_eq!(publishes.len(), 1);
        assert_eq!(publishes[0].0, "system");
        assert_eq!(publishes[0].1, "events.system");
        assert_eq!(publishes[0].2, next);
    }

    #[test]
    fn fuse_preserves_existing_whole_value_bytes_for_partial_writes() {
        let backend = MemoryBackend::default();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "file.txt".into()), b"hello".to_vec());
        let kv_puts = backend.kv_puts.clone();
        let mut fs = test_fs(backend);
        let fh = fs
            .open_handle(mp("/kv/bucket/file.txt"), true, false, false)
            .unwrap();

        fs.write_to_handle_buffer(fh, 1, b"a").unwrap();
        fs.commit_handle(fh).unwrap();

        let puts = kv_puts.lock().unwrap();
        assert_eq!(puts.len(), 1);
        assert_eq!(puts[0].2, b"hallo");
    }

    #[test]
    fn fuse_stages_truncate_until_replacement_payload_is_written() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (
                eventfs_protocol::subjects::TASKS_BUCKET.into(),
                "demo/render.json".into(),
            ),
            br#"{"state":"old"}"#.to_vec(),
        );
        let kv_puts = backend.kv_puts.clone();
        let mut fs = test_fs(backend);
        let fh = fs
            .open_handle(mp("/tasks/demo/render.json"), true, false, false)
            .unwrap();

        fs.stage_truncate(fh).unwrap();
        assert!(kv_puts.lock().unwrap().is_empty());
        fs.write_to_handle_buffer(fh, 0, br#"{"state":"new"}"#)
            .unwrap();
        fs.commit_handle(fh).unwrap();

        let puts = kv_puts.lock().unwrap();
        assert_eq!(puts.len(), 1);
        assert_eq!(puts[0].2, br#"{"state":"new"}"#);
    }

    #[test]
    fn fuse_rejects_truncate_on_append_only_jsonl_surfaces() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.open_handle(mp("/events/system.jsonl"), true, true, false)
                .unwrap_err(),
            libc::EROFS
        );

        let fh = fs
            .open_handle(mp("/events/system.jsonl"), true, false, false)
            .unwrap();
        assert_eq!(fs.stage_truncate(fh).unwrap_err(), libc::EROFS);
        assert_eq!(
            fs.reject_append_only_truncate("/agents/bot/inbox")
                .unwrap_err(),
            libc::EROFS
        );
    }

    #[test]
    fn fuse_reads_dirty_whole_value_handle_before_commit() {
        let backend = MemoryBackend::default();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "file.txt".into()), b"hello".to_vec());
        let mut fs = test_fs(backend);
        let parsed = JetStreamPath::parse("/kv/bucket/file.txt").unwrap();
        let fh = fs
            .open_handle(mp("/kv/bucket/file.txt"), true, false, false)
            .unwrap();

        fs.write_to_handle_buffer(fh, 1, b"a").unwrap();

        assert_eq!(
            fs.read_bytes_for_handle(fh, mp("/kv/bucket/file.txt"), &parsed)
                .unwrap(),
            b"hallo"
        );
    }

    #[test]
    fn fuse_reads_dirty_jsonl_handle_before_commit() {
        let backend = MemoryBackend::default();
        backend.streams.lock().unwrap().insert(
            "system".into(),
            vec![stream_message(
                "system",
                1,
                "events.system",
                br#"{"event":1}"#,
            )],
        );
        let mut fs = test_fs(backend);
        let parsed = JetStreamPath::parse("/events/system.jsonl").unwrap();
        let existing = br#"{"event":1}
"#;
        let next = br#"{"event":2}
"#;
        let fh = fs
            .open_handle(mp("/events/system.jsonl"), true, false, false)
            .unwrap();

        fs.write_to_handle_buffer(fh, existing.len() as i64, next)
            .unwrap();

        let mut expected = existing.to_vec();
        expected.extend_from_slice(next);
        assert_eq!(
            fs.read_bytes_for_handle(fh, mp("/events/system.jsonl"), &parsed)
                .unwrap(),
            expected
        );
    }

    #[test]
    fn fuse_detects_unsupported_setattr_metadata_fields() {
        assert!(!SetattrMetadataFields::default().requested());
        assert!(SetattrMetadataFields {
            mode: true,
            ..Default::default()
        }
        .requested());
        assert!(SetattrMetadataFields {
            uid: true,
            ..Default::default()
        }
        .requested());
        assert!(SetattrMetadataFields {
            atime: true,
            ..Default::default()
        }
        .requested());
    }

    #[test]
    fn fuse_resolves_uncached_nested_kv_and_object_prefixes_as_directories() {
        let backend = MemoryBackend::default();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "dir/file.json".into()), b"{}".to_vec());
        backend.objects.lock().unwrap().insert(
            ("bucket".into(), "assets/blob.txt".into()),
            b"blob".to_vec(),
        );
        let mut fs = test_fs(backend);

        let kv_attr = fs.attr_for_path(mp("/kv/bucket/dir"), 0, 0).unwrap();
        assert_eq!(kv_attr.kind, FileType::Directory);

        let object_attr = fs
            .attr_for_path(mp("/objects/bucket/assets"), 0, 0)
            .unwrap();
        assert_eq!(object_attr.kind, FileType::Directory);
    }

    #[test]
    fn fuse_resolves_key_prefix_conflicts_as_exact_files() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().extend([
            (("bucket".into(), "dir".into()), b"exact".to_vec()),
            (("bucket".into(), "dir/file.json".into()), b"{}".to_vec()),
        ]);
        backend.objects.lock().unwrap().extend([
            (("bucket".into(), "asset".into()), b"exact".to_vec()),
            (("bucket".into(), "asset/blob.txt".into()), b"blob".to_vec()),
        ]);
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.attr_for_path(mp("/kv/bucket/dir"), 0, 0).unwrap().kind,
            FileType::RegularFile
        );
        assert_eq!(
            fs.attr_for_path(mp("/objects/bucket/asset"), 0, 0)
                .unwrap()
                .kind,
            FileType::RegularFile
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/kv/bucket").unwrap())
                .unwrap(),
            vec![
                DirectoryEntry {
                    name: ".history".into(),
                    kind: EntryKind::Directory,
                },
                DirectoryEntry {
                    name: "dir".into(),
                    kind: EntryKind::File,
                },
            ]
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/objects/bucket").unwrap())
                .unwrap(),
            vec![DirectoryEntry {
                name: "asset".into(),
                kind: EntryKind::File,
            }]
        );
    }

    #[test]
    fn fuse_mkdir_rejects_existing_kv_and_object_files() {
        let backend = MemoryBackend::default();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "file.json".into()), b"{}".to_vec());
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("bucket".into(), "blob".into()), b"bytes".to_vec());
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.execute_mkdir(mp("/kv/bucket/file.json")).unwrap_err(),
            libc::EEXIST
        );
        assert_eq!(
            fs.execute_mkdir(mp("/objects/bucket/blob")).unwrap_err(),
            libc::EEXIST
        );
    }

    #[test]
    fn fuse_mkdir_allows_only_existing_synthetic_prefixes() {
        let backend = MemoryBackend::default();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "dir/file.json".into()), b"{}".to_vec());
        backend.objects.lock().unwrap().insert(
            ("bucket".into(), "assets/blob.txt".into()),
            b"blob".to_vec(),
        );
        let mut fs = test_fs(backend);

        fs.execute_mkdir(mp("/kv/bucket/dir")).unwrap();
        fs.execute_mkdir(mp("/objects/bucket/assets")).unwrap();
        assert_eq!(
            fs.execute_mkdir(mp("/kv/bucket/missing")).unwrap_err(),
            libc::ENOENT
        );
        assert_eq!(
            fs.execute_mkdir(mp("/objects/bucket/missing")).unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_reports_missing_storage_roots_as_enoent() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.attr_for_path(mp("/kv/no-such"), 0, 0).unwrap_err(),
            libc::ENOENT
        );
        assert_eq!(
            fs.attr_for_path(mp("/objects/no-such"), 0, 0).unwrap_err(),
            libc::ENOENT
        );
        assert_eq!(
            fs.attr_for_path(mp("/streams/no-such"), 0, 0).unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_preserves_static_directory_entry_kinds() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/.eventfs").unwrap())
                .unwrap(),
            vec![
                DirectoryEntry {
                    name: "status.json".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "cache.json".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "queue.json".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "capabilities.json".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "errors.jsonl".into(),
                    kind: EntryKind::File,
                },
            ]
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/agents/bot").unwrap())
                .unwrap(),
            vec![
                DirectoryEntry {
                    name: "inbox".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "outbox".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "tasks".into(),
                    kind: EntryKind::Directory,
                },
                DirectoryEntry {
                    name: "memory".into(),
                    kind: EntryKind::Directory,
                },
            ]
        );
    }

    #[test]
    fn fuse_lists_kv_history_revisions_with_marker_names() {
        let backend = MemoryBackend::default();
        backend.histories.lock().unwrap().insert(
            ("app".into(), "jobs/2026".into()),
            vec![key_revision(1, b"old"), key_revision(2, b"new")],
        );
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/kv/app/.history/jobs/2026").unwrap())
                .unwrap(),
            vec![
                DirectoryEntry {
                    name: "@1".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "@2".into(),
                    kind: EntryKind::File,
                },
            ]
        );
        let revision = JetStreamPath::parse("/kv/app/.history/jobs/2026/@2").unwrap();
        assert_eq!(
            fs.read_bytes(mp("/kv/app/.history/jobs/2026/@2"), &revision)
                .unwrap(),
            b"new"
        );
    }

    #[test]
    fn fuse_resolves_uncached_nested_kv_history_prefixes_as_directories() {
        let backend = MemoryBackend::default();
        backend.histories.lock().unwrap().insert(
            ("app".into(), "jobs/2026".into()),
            vec![key_revision(2, b"queued")],
        );
        let mut fs = test_fs(backend);

        assert!(fs.paths.entry(&mp("/kv/app/.history/jobs")).is_none());
        let attr = fs.attr_for_path(mp("/kv/app/.history/jobs"), 0, 0).unwrap();
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/kv/app/.history/jobs").unwrap())
                .unwrap(),
            vec![DirectoryEntry {
                name: "2026".into(),
                kind: EntryKind::Directory,
            }]
        );

        let revision = JetStreamPath::parse("/kv/app/.history/jobs/2026/@2").unwrap();
        assert_eq!(
            fs.read_bytes(mp("/kv/app/.history/jobs/2026/@2"), &revision)
                .unwrap(),
            b"queued"
        );
    }

    #[test]
    fn fuse_history_directory_preserves_exact_revisions_and_descendant_prefixes() {
        let backend = MemoryBackend::default();
        backend.histories.lock().unwrap().extend([
            (
                ("app".into(), "config".into()),
                vec![
                    key_revision(2, br#"{"root":1}"#),
                    key_revision(5, br#"{"root":2}"#),
                ],
            ),
            (
                ("app".into(), "config/db.json".into()),
                vec![key_revision(7, br#"{"dsn":"postgres"}"#)],
            ),
        ]);
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/kv/app/.history/config").unwrap())
                .unwrap(),
            vec![
                DirectoryEntry {
                    name: "@2".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "@5".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "db.json".into(),
                    kind: EntryKind::Directory,
                },
            ]
        );

        let nested = JetStreamPath::parse("/kv/app/.history/config/db.json").unwrap();
        let attr = fs
            .attr_for_path("/kv/app/.history/config/db.json", 0, 0)
            .unwrap();
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(
            fs.directory_entries(&nested).unwrap(),
            vec![DirectoryEntry {
                name: "@7".into(),
                kind: EntryKind::File,
            }]
        );

        let revision = JetStreamPath::parse("/kv/app/.history/config/db.json/@7").unwrap();
        assert_eq!(
            fs.read_bytes(mp("/kv/app/.history/config/db.json/@7"), &revision)
                .unwrap(),
            br#"{"dsn":"postgres"}"#
        );
    }

    #[test]
    fn fuse_lists_materialized_kv_directories_from_backing_buckets() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().extend([
            (
                (
                    eventfs_protocol::subjects::TASKS_BUCKET.into(),
                    "demo/render.json".into(),
                ),
                b"{}".to_vec(),
            ),
            (
                (
                    eventfs_protocol::subjects::TASKS_BUCKET.into(),
                    "ops/deploy.json".into(),
                ),
                b"{}".to_vec(),
            ),
            (
                (
                    eventfs_protocol::subjects::AGENTS_BUCKET.into(),
                    "bot/tasks/render.json".into(),
                ),
                b"{}".to_vec(),
            ),
            (
                (
                    eventfs_protocol::subjects::SEMANTIC_BUCKET.into(),
                    "tags/release.json".into(),
                ),
                b"{}".to_vec(),
            ),
        ]);
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/tasks").unwrap())
                .unwrap(),
            vec![
                DirectoryEntry {
                    name: "demo".into(),
                    kind: EntryKind::Directory,
                },
                DirectoryEntry {
                    name: "ops".into(),
                    kind: EntryKind::Directory,
                },
            ]
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/tasks/demo").unwrap())
                .unwrap(),
            vec![DirectoryEntry {
                name: "render.json".into(),
                kind: EntryKind::File,
            }]
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/agents").unwrap())
                .unwrap(),
            vec![DirectoryEntry {
                name: "bot".into(),
                kind: EntryKind::Directory,
            }]
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/agents/bot/tasks").unwrap())
                .unwrap(),
            vec![DirectoryEntry {
                name: "render.json".into(),
                kind: EntryKind::File,
            }]
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/semantic/tags").unwrap())
                .unwrap(),
            vec![DirectoryEntry {
                name: "release.json".into(),
                kind: EntryKind::File,
            }]
        );
    }

    #[test]
    fn fuse_lists_mailbox_only_agents_from_stream_facts() {
        let backend = MemoryBackend::default();
        backend.streams.lock().unwrap().insert(
            eventfs_protocol::subjects::AGENTS_STREAM.into(),
            vec![
                stream_message(
                    eventfs_protocol::subjects::AGENTS_STREAM,
                    1,
                    "agents.bot.inbox",
                    br#"{"task":"run"}"#,
                ),
                stream_message(
                    eventfs_protocol::subjects::AGENTS_STREAM,
                    2,
                    "agents.bot.outbox",
                    br#"{"status":"ok"}"#,
                ),
                stream_message(
                    eventfs_protocol::subjects::AGENTS_STREAM,
                    3,
                    "agents.worker.inbox",
                    br#"{"task":"deploy"}"#,
                ),
                stream_message(
                    eventfs_protocol::subjects::AGENTS_STREAM,
                    4,
                    "agents.__eventfs_applied.inbox",
                    br#"{"task":"hidden"}"#,
                ),
                stream_message(
                    eventfs_protocol::subjects::AGENTS_STREAM,
                    5,
                    "agents.__eventfs_writeback.outbox",
                    br#"{"status":"hidden"}"#,
                ),
            ],
        );
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/agents").unwrap())
                .unwrap(),
            vec![
                DirectoryEntry {
                    name: "bot".into(),
                    kind: EntryKind::Directory,
                },
                DirectoryEntry {
                    name: "worker".into(),
                    kind: EntryKind::Directory,
                },
            ]
        );
    }

    #[test]
    fn fuse_resolves_uncached_nested_materialized_prefixes_as_directories() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().extend([
            (
                (
                    eventfs_protocol::SEMANTIC_BUCKET.into(),
                    "tags/project/a.json".into(),
                ),
                b"{}".to_vec(),
            ),
            (
                (
                    eventfs_protocol::AGENTS_BUCKET.into(),
                    "bot/memory/facts/a.json".into(),
                ),
                b"{}".to_vec(),
            ),
        ]);
        let mut fs = test_fs(backend);

        assert!(fs.paths.entry(&mp("/semantic/tags/project")).is_none());
        let semantic_attr = fs
            .attr_for_path(mp("/semantic/tags/project"), 0, 0)
            .unwrap();
        assert_eq!(semantic_attr.kind, FileType::Directory);
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/semantic/tags/project").unwrap())
                .unwrap(),
            vec![DirectoryEntry {
                name: "a.json".into(),
                kind: EntryKind::File,
            }]
        );

        assert!(fs.paths.entry(&mp("/agents/bot/memory/facts")).is_none());
        let agent_attr = fs
            .attr_for_path(mp("/agents/bot/memory/facts"), 0, 0)
            .unwrap();
        assert_eq!(agent_attr.kind, FileType::Directory);
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/agents/bot/memory/facts").unwrap())
                .unwrap(),
            vec![DirectoryEntry {
                name: "a.json".into(),
                kind: EntryKind::File,
            }]
        );
    }

    #[test]
    fn fuse_treats_missing_materialized_kv_buckets_as_empty_directories() {
        let backend = MemoryBackend::default();
        backend.missing_kv_buckets.lock().unwrap().extend([
            eventfs_protocol::TASKS_BUCKET.into(),
            eventfs_protocol::AGENTS_BUCKET.into(),
            eventfs_protocol::SEMANTIC_BUCKET.into(),
            "ordinary".into(),
        ]);
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/tasks").unwrap())
                .unwrap(),
            Vec::<DirectoryEntry>::new()
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/agents").unwrap())
                .unwrap(),
            Vec::<DirectoryEntry>::new()
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/agents/bot/tasks").unwrap())
                .unwrap(),
            Vec::<DirectoryEntry>::new()
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/semantic/tags").unwrap())
                .unwrap(),
            Vec::<DirectoryEntry>::new()
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/kv/ordinary").unwrap())
                .unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_lists_event_streams_as_jsonl_files() {
        let backend = MemoryBackend::default();
        backend.streams.lock().unwrap().insert(
            "system".into(),
            vec![stream_message(
                "system",
                1,
                "events.system",
                br#"{"ok":true}"#,
            )],
        );
        let mut fs = test_fs(backend);
        let entries = fs
            .directory_entries(&JetStreamPath::parse("/events").unwrap())
            .unwrap();

        assert_eq!(
            entries,
            vec![DirectoryEntry {
                name: "system.jsonl".into(),
                kind: EntryKind::File,
            }]
        );
    }

    #[test]
    fn fuse_reads_materialized_stream_views_as_jsonl() {
        let backend = MemoryBackend::default();
        backend.streams.lock().unwrap().insert(
            "system".into(),
            vec![
                stream_message("system", 1, "events.system", br#"{"event":1}"#),
                stream_message("system", 2, "events.other", br#"{"event":2}"#),
                stream_message("system", 3, "events.system", br#"{"event":3}"#),
            ],
        );
        backend.streams.lock().unwrap().insert(
            eventfs_protocol::subjects::AGENTS_STREAM.into(),
            vec![stream_message(
                eventfs_protocol::subjects::AGENTS_STREAM,
                1,
                "agents.bot.inbox",
                br#"{"task":"run"}"#,
            )],
        );
        let mut fs = test_fs(backend);

        let events_path = JetStreamPath::parse("/events/system.jsonl").unwrap();
        assert_eq!(
            fs.read_bytes(mp("/events/system.jsonl"), &events_path)
                .unwrap(),
            br#"{"event":1}
{"event":3}
"#
        );

        let inbox_path = JetStreamPath::parse("/agents/bot/inbox").unwrap();
        assert_eq!(
            fs.read_bytes(mp("/agents/bot/inbox"), &inbox_path).unwrap(),
            br#"{"task":"run"}
"#
        );
    }

    #[test]
    fn fuse_lists_stream_subjects_from_durable_stream_facts() {
        let backend = MemoryBackend::default();
        backend.streams.lock().unwrap().insert(
            "ORDERS".into(),
            vec![
                stream_message("ORDERS", 1, "orders.created", br#"{"id":1}"#),
                stream_message("ORDERS", 2, "orders.updated", br#"{"id":2}"#),
                stream_message("ORDERS", 3, "orders/shipped@v1", br#"{"id":3}"#),
            ],
        );
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/streams/ORDERS/subjects").unwrap())
                .unwrap(),
            vec![
                DirectoryEntry {
                    name: "orders.created.jsonl".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "orders.updated.jsonl".into(),
                    kind: EntryKind::File,
                },
                DirectoryEntry {
                    name: "__eventfs_subject_hex_6f72646572732f73686970706564407631.jsonl".into(),
                    kind: EntryKind::File,
                },
            ]
        );

        let attr = fs
            .attr_for_path("/streams/ORDERS/subjects/orders.created.jsonl", 1000, 1000)
            .unwrap();
        assert_eq!(attr.kind, FileType::RegularFile);
        let encoded_attr = fs
            .attr_for_path(
                "/streams/ORDERS/subjects/__eventfs_subject_hex_6f72646572732f73686970706564407631.jsonl",
                1000,
                1000,
            )
            .unwrap();
        assert_eq!(encoded_attr.kind, FileType::RegularFile);
        assert_eq!(
            fs.attr_for_path(
                mp("/streams/ORDERS/subjects/orders.missing.jsonl"),
                1000,
                1000
            )
            .unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_stream_subject_attr_uses_newest_matching_message_timestamp() {
        let backend = MemoryBackend::default();
        backend.streams.lock().unwrap().insert(
            "ORDERS".into(),
            vec![
                stream_message("ORDERS", 1, "orders.created", br#"{"id":1}"#),
                stream_message("ORDERS", 9, "orders.updated", br#"{"id":9}"#),
                stream_message("ORDERS", 7, "orders.created", br#"{"id":7}"#),
            ],
        );
        let mut fs = test_fs(backend);

        let attr = fs
            .attr_for_path("/streams/ORDERS/subjects/orders.created.jsonl", 1000, 1000)
            .unwrap();

        assert_eq!(attr.mtime, test_time(7));
        assert_eq!(attr.ctime, test_time(7));
        assert_eq!(attr.crtime, test_time(7));
    }

    #[test]
    fn fuse_cache_serves_materialized_bytes_until_watch_invalidates() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (
                eventfs_protocol::subjects::TASKS_BUCKET.into(),
                "demo/render.json".into(),
            ),
            br#"{"state":"old"}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());
        let path = JetStreamPath::parse("/tasks/demo/render.json").unwrap();

        assert_eq!(
            fs.read_bytes(mp("/tasks/demo/render.json"), &path).unwrap(),
            br#"{"state":"old"}"#
        );
        backend.kv.lock().unwrap().insert(
            (
                eventfs_protocol::subjects::TASKS_BUCKET.into(),
                "demo/render.json".into(),
            ),
            br#"{"state":"new"}"#.to_vec(),
        );
        assert_eq!(
            fs.read_bytes(mp("/tasks/demo/render.json"), &path).unwrap(),
            br#"{"state":"old"}"#
        );

        backend.push_watch_event(WatchEvent::invalidate_path(mp("/tasks/demo/render.json")));

        assert_eq!(
            fs.read_bytes(mp("/tasks/demo/render.json"), &path).unwrap(),
            br#"{"state":"new"}"#
        );
    }

    #[test]
    fn fuse_successful_materialized_write_invalidates_cached_native_alias() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (TASKS_BUCKET.into(), "demo/render.json".into()),
            br#"{"state":"old"}"#.to_vec(),
        );
        let mut fs = test_fs(backend);
        let native_path = format!("/kv/{TASKS_BUCKET}/demo/render.json");
        let native = JetStreamPath::parse(&native_path).unwrap();

        assert_eq!(
            fs.read_bytes(&native_path, &native).unwrap(),
            br#"{"state":"old"}"#
        );

        fs.commit_path("/tasks/demo/render.json", br#"{"state":"new"}"#)
            .unwrap();

        assert_eq!(
            fs.read_bytes(&native_path, &native).unwrap(),
            br#"{"state":"new"}"#
        );
    }

    #[test]
    fn fuse_successful_native_write_invalidates_cached_materialized_alias() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (TASKS_BUCKET.into(), "demo/render.json".into()),
            br#"{"state":"old"}"#.to_vec(),
        );
        let mut fs = test_fs(backend);
        let materialized = JetStreamPath::parse("/tasks/demo/render.json").unwrap();

        assert_eq!(
            fs.read_bytes(mp("/tasks/demo/render.json"), &materialized)
                .unwrap(),
            br#"{"state":"old"}"#
        );

        fs.commit_path(
            &format!("/kv/{TASKS_BUCKET}/demo/render.json"),
            br#"{"state":"new"}"#,
        )
        .unwrap();

        assert_eq!(
            fs.read_bytes(mp("/tasks/demo/render.json"), &materialized)
                .unwrap(),
            br#"{"state":"new"}"#
        );
    }

    #[test]
    fn fuse_queued_materialized_write_invalidates_cached_native_alias() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (TASKS_BUCKET.into(), "demo/render.json".into()),
            br#"{"state":"old"}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());
        let native_path = format!("/kv/{TASKS_BUCKET}/demo/render.json");
        let native = JetStreamPath::parse(&native_path).unwrap();

        assert_eq!(
            fs.read_bytes(&native_path, &native).unwrap(),
            br#"{"state":"old"}"#
        );
        assert!(fs.cache_contains(&native_path));

        backend.fail_writes();
        fs.commit_path("/tasks/demo/render.json", br#"{"state":"new"}"#)
            .unwrap();

        assert!(!fs.cache_contains(&native_path));
        assert_eq!(fs.pending_write_count(), 1);
    }

    #[test]
    fn fuse_queued_materialized_write_serves_pending_bytes_across_aliases() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (TASKS_BUCKET.into(), "demo/render.json".into()),
            br#"{"state":"old"}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());
        let materialized_path = "/tasks/demo/render.json";
        let materialized = JetStreamPath::parse(materialized_path).unwrap();
        let native_path = format!("/kv/{TASKS_BUCKET}/demo/render.json");
        let native = JetStreamPath::parse(&native_path).unwrap();

        assert_eq!(
            fs.read_bytes(materialized_path, &materialized).unwrap(),
            br#"{"state":"old"}"#
        );
        assert_eq!(
            fs.read_bytes(&native_path, &native).unwrap(),
            br#"{"state":"old"}"#
        );

        backend.fail_writes();
        fs.commit_path(materialized_path, br#"{"state":"queued"}"#)
            .unwrap();

        assert_eq!(
            fs.read_bytes(materialized_path, &materialized).unwrap(),
            br#"{"state":"queued"}"#
        );
        assert_eq!(
            fs.read_bytes(&native_path, &native).unwrap(),
            br#"{"state":"queued"}"#
        );
    }

    #[test]
    fn storage_contract_fuse_mount_uses_production_memory_store() {
        let backend = MemoryStorage::new();
        backend
            .kv_put_idempotent(
                TASKS_BUCKET,
                "demo/render.json",
                br#"{"state":"ready"}"#,
                "initial-task",
            )
            .unwrap();
        backend
            .object_put_idempotent("assets", "logo.txt", b"logo-bytes", "initial-object")
            .unwrap();
        let sequences = backend
            .publish_json_lines("ORDERS", "orders.created", br#"{"id":1}"#, "initial-stream")
            .unwrap();
        let mut fs = JetStreamFuse::new(
            Box::new(backend.clone()),
            WritebackQueue::open(test_queue_dir(), 16).unwrap(),
            "test",
        );

        let task_path = "/tasks/demo/render.json";
        assert_eq!(
            fs.read_bytes(task_path, &JetStreamPath::parse(task_path).unwrap())
                .unwrap(),
            br#"{"state":"ready"}"#
        );
        let object_path = "/objects/assets/logo.txt";
        assert_eq!(
            fs.read_bytes(object_path, &JetStreamPath::parse(object_path).unwrap())
                .unwrap(),
            b"logo-bytes"
        );
        let stream_path = format!("/streams/ORDERS/messages/{}.json", sequences[0]);
        let stream_message: serde_json::Value = serde_json::from_slice(
            &fs.read_bytes(&stream_path, &JetStreamPath::parse(&stream_path).unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(stream_message["stream"], "ORDERS");
        assert_eq!(stream_message["subject"], "orders.created");
        assert_eq!(stream_message["payload"], serde_json::json!({"id": 1}));

        fs.commit_path("/tasks/demo/next.json", br#"{"state":"done"}"#)
            .unwrap();
        assert_eq!(
            backend
                .kv_get(TASKS_BUCKET, "demo/next.json")
                .unwrap()
                .unwrap()
                .bytes,
            br#"{"state":"done"}"#
        );
    }

    #[test]
    fn mount_runtime_serves_queued_materialized_write_across_aliases() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (TASKS_BUCKET.into(), "demo/render.json".into()),
            br#"{"state":"old"}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());

        backend.fail_writes();
        fs.commit_path("/tasks/demo/render.json", br#"{"state":"queued"}"#)
            .unwrap();

        {
            let mut runtime = fs.mount_runtime();
            assert_eq!(
                runtime.read(&mp("/tasks/demo/render.json")).unwrap(),
                br#"{"state":"queued"}"#
            );
            assert_eq!(
                runtime
                    .read(&mp(format!("/kv/{TASKS_BUCKET}/demo/render.json")))
                    .unwrap(),
                br#"{"state":"queued"}"#
            );
            assert_eq!(
                runtime.stat(&mp("/tasks/demo/render.json")).unwrap().kind,
                runtime::RuntimeEntryKind::File
            );
        }
    }

    #[test]
    fn mount_runtime_returns_attrs_and_directory_entries_with_queued_overlay() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend.clone());
        let payload = br#"{"queued":true}"#;

        backend.fail_writes();
        fs.commit_path("/tasks/demo/render.json", payload).unwrap();

        let mut runtime = fs.mount_runtime();
        let attr = runtime.stat(&mp("/tasks/demo/render.json")).unwrap();
        assert_eq!(attr.kind, runtime::RuntimeEntryKind::File);
        assert_eq!(attr.size, payload.len() as u64);

        let tasks = runtime.list(&mp("/tasks")).unwrap();
        assert!(tasks.iter().any(
            |entry| entry.name == "demo" && entry.kind == runtime::RuntimeEntryKind::Directory
        ));
        let demo = runtime.list(&mp("/tasks/demo")).unwrap();
        assert!(demo.iter().any(|entry| {
            entry.name == "render.json" && entry.kind == runtime::RuntimeEntryKind::File
        }));
    }

    #[test]
    fn mount_runtime_applies_queued_exact_delete_directory_conflict_policy() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().extend([
            (("bucket".into(), "dir".into()), b"exact".to_vec()),
            (
                ("bucket".into(), "dir/keep.json".into()),
                br#"{"keep":true}"#.to_vec(),
            ),
        ]);
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-kv-exact",
            queued_kv_source_delete("bucket", "dir"),
        );

        let mut runtime = fs.mount_runtime();

        let bucket_entries = runtime.list(&mp("/kv/bucket")).unwrap();
        assert!(bucket_entries
            .iter()
            .any(|entry| entry.name == ".history"
                && entry.kind == runtime::RuntimeEntryKind::Directory));
        assert!(
            bucket_entries
                .iter()
                .any(|entry| entry.name == "dir"
                    && entry.kind == runtime::RuntimeEntryKind::Directory)
        );
        assert_eq!(
            runtime.stat(&mp("/kv/bucket/dir")).unwrap().kind,
            runtime::RuntimeEntryKind::Directory
        );
        let dir_entries = runtime.list(&mp("/kv/bucket/dir")).unwrap();
        assert!(dir_entries.iter().any(|entry| {
            entry.name == "keep.json" && entry.kind == runtime::RuntimeEntryKind::File
        }));
    }

    #[test]
    fn mount_runtime_classifies_create_mutations() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "dir/child.json".into()),
            br#"{"child":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());
        let mut runtime = fs.mount_runtime();

        runtime
            .mknod(&mp("/tasks/demo/new.json"), libc::S_IFREG | 0o644)
            .unwrap();
        assert_eq!(
            backend
                .kv
                .lock()
                .unwrap()
                .get(&(TASKS_BUCKET.into(), "demo/new.json".into()))
                .unwrap(),
            b"null"
        );
        assert_eq!(
            runtime
                .mknod(&mp("/kv/bucket/dir"), libc::S_IFREG | 0o644)
                .unwrap_err()
                .errno(),
            libc::EISDIR
        );
        assert_eq!(
            runtime
                .open(
                    &mp("/events/system.jsonl"),
                    runtime::RuntimeOpenOptions {
                        writable: true,
                        create: true,
                        truncate: false,
                    },
                )
                .unwrap_err(),
            runtime::RuntimeError::from(libc::ENOTSUP)
        );
    }

    #[test]
    fn fuse_create_and_mknod_cross_mount_projection_for_create_target_policy() {
        let fuse_source = include_str!("fs.rs");
        let direct_create_plan = concat!("plan_operation(FileIntent::", "Create");
        let local_create_classifier = concat!("ensure_", "create_target_absent");

        assert!(
            !fuse_source.contains(direct_create_plan),
            "FUSE create/mknod paths must route create-target classification through mount projection"
        );
        assert!(
            !fuse_source.contains(local_create_classifier),
            "create-target errno policy belongs in mount projection, not a FUSE-local helper"
        );
    }

    #[test]
    fn fuse_queued_native_create_stays_visible_in_listings_and_attrs() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend.clone());
        let path = "/kv/bucket/nested/file.json";
        let parsed = JetStreamPath::parse(path).unwrap();

        backend.fail_writes();
        fs.commit_path(path, br#"{"queued":true}"#).unwrap();

        assert_eq!(fs.read_bytes(path, &parsed).unwrap(), br#"{"queued":true}"#);
        assert_eq!(
            fs.attr_for_path(path, 0, 0).unwrap().kind,
            FileType::RegularFile
        );
        assert_eq!(
            fs.attr_for_path(mp("/kv/bucket"), 0, 0).unwrap().kind,
            FileType::Directory
        );
        assert_eq!(
            fs.attr_for_path(mp("/kv/bucket/nested"), 0, 0)
                .unwrap()
                .kind,
            FileType::Directory
        );

        let root_entries = fs
            .directory_entries(&JetStreamPath::parse("/kv").unwrap())
            .unwrap();
        assert!(root_entries
            .iter()
            .any(|entry| { entry.name == "bucket" && entry.kind == EntryKind::Directory }));

        let bucket_entries = fs
            .directory_entries(&JetStreamPath::parse("/kv/bucket").unwrap())
            .unwrap();
        assert!(bucket_entries
            .iter()
            .any(|entry| { entry.name == "nested" && entry.kind == EntryKind::Directory }));

        let nested_entries = fs
            .directory_entries(&JetStreamPath::parse("/kv/bucket/nested").unwrap())
            .unwrap();
        assert!(nested_entries
            .iter()
            .any(|entry| { entry.name == "file.json" && entry.kind == EntryKind::File }));
    }

    #[test]
    fn fuse_queued_whole_value_overlay_keeps_readdir_visible_during_backend_outage() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend.clone());
        let path = "/tasks/demo/render.json";
        let parsed = JetStreamPath::parse(path).unwrap();
        let native_bucket = eventfs_protocol::subjects::TASKS_BUCKET;
        let native_root = JetStreamPath::parse("/kv").unwrap();
        let native_bucket_path = JetStreamPath::parse(&format!("/kv/{native_bucket}")).unwrap();
        let native_namespace = JetStreamPath::parse(&format!("/kv/{native_bucket}/demo")).unwrap();

        backend.fail_writes();
        fs.commit_path(path, br#"{"queued":true}"#).unwrap();
        backend.allow_writes();
        backend.fail_directory_lists();

        assert!(fs.writes_blocked());
        assert_eq!(fs.read_bytes(path, &parsed).unwrap(), br#"{"queued":true}"#);
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/tasks").unwrap())
                .unwrap(),
            vec![DirectoryEntry {
                name: "demo".into(),
                kind: EntryKind::Directory,
            }]
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/tasks/demo").unwrap())
                .unwrap(),
            vec![DirectoryEntry {
                name: "render.json".into(),
                kind: EntryKind::File,
            }]
        );
        assert_eq!(
            fs.directory_entries(&native_root).unwrap(),
            vec![DirectoryEntry {
                name: native_bucket.into(),
                kind: EntryKind::Directory,
            }]
        );
        assert_eq!(
            fs.directory_entries(&native_bucket_path).unwrap(),
            vec![
                DirectoryEntry {
                    name: ".history".into(),
                    kind: EntryKind::Directory,
                },
                DirectoryEntry {
                    name: "demo".into(),
                    kind: EntryKind::Directory,
                },
            ]
        );
        assert_eq!(
            fs.directory_entries(&native_namespace).unwrap(),
            vec![DirectoryEntry {
                name: "render.json".into(),
                kind: EntryKind::File,
            }]
        );
    }

    #[test]
    fn fuse_rename_same_surface_within_writable_surfaces() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "tmp.json".into()),
            br#"{"kv":true}"#.to_vec(),
        );
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "tmp/blob.txt".into()), b"object".to_vec());
        backend.kv.lock().unwrap().insert(
            (TASKS_BUCKET.into(), "demo/tmp.json".into()),
            br#"{"task":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());

        fs.execute_rename("/kv/bucket/tmp.json", "/kv/bucket/final.json", 0)
            .unwrap();
        fs.execute_rename(
            "/objects/assets/tmp/blob.txt",
            "/objects/assets/final/blob.txt",
            0,
        )
        .unwrap();
        fs.execute_rename("/tasks/demo/tmp.json", "/tasks/demo/final.json", 0)
            .unwrap();

        assert!(backend
            .kv
            .lock()
            .unwrap()
            .get(&("bucket".into(), "tmp.json".into()))
            .is_none());
        assert_eq!(
            backend
                .kv
                .lock()
                .unwrap()
                .get(&("bucket".into(), "final.json".into()))
                .unwrap(),
            br#"{"kv":true}"#
        );
        assert!(backend
            .objects
            .lock()
            .unwrap()
            .get(&("assets".into(), "tmp/blob.txt".into()))
            .is_none());
        assert_eq!(
            backend
                .objects
                .lock()
                .unwrap()
                .get(&("assets".into(), "final/blob.txt".into()))
                .unwrap(),
            b"object"
        );
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .get(&(TASKS_BUCKET.into(), "demo/tmp.json".into()))
            .is_none());
        assert_eq!(
            backend
                .kv
                .lock()
                .unwrap()
                .get(&(TASKS_BUCKET.into(), "demo/final.json".into()))
                .unwrap(),
            br#"{"task":true}"#
        );
        assert_eq!(
            fs.execute_rename("/kv/bucket/final.json", "/objects/assets/final.json", 0)
                .unwrap_err(),
            Errno::CROSS_DEVICE.get()
        );
    }

    #[test]
    fn fuse_rename_rejects_kv_prefix_directory_destination() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "src.json".into()),
            br#"{"src":true}"#.to_vec(),
        );
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "dir/child.json".into()),
            br#"{"child":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());

        assert_eq!(
            fs.execute_rename("/kv/bucket/src.json", "/kv/bucket/dir", 0)
                .unwrap_err(),
            libc::EISDIR
        );
        assert_eq!(fs.pending_write_count(), 0);
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .contains_key(&("bucket".into(), "src.json".into())));
        assert!(!backend
            .kv
            .lock()
            .unwrap()
            .contains_key(&("bucket".into(), "dir".into())));
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .contains_key(&("bucket".into(), "dir/child.json".into())));
        assert_eq!(
            fs.kind_for_path(
                "/kv/bucket/dir",
                &JetStreamPath::parse("/kv/bucket/dir").unwrap()
            )
            .unwrap(),
            FileType::Directory
        );
    }

    #[test]
    fn fuse_rename_rejects_object_prefix_directory_destination() {
        let backend = MemoryBackend::default();
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "src.bin".into()), b"src".to_vec());
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "dir/child.bin".into()), b"child".to_vec());
        let mut fs = test_fs(backend.clone());

        assert_eq!(
            fs.execute_rename("/objects/assets/src.bin", "/objects/assets/dir", 0)
                .unwrap_err(),
            libc::EISDIR
        );
        assert_eq!(fs.pending_write_count(), 0);
        assert!(backend
            .objects
            .lock()
            .unwrap()
            .contains_key(&("assets".into(), "src.bin".into())));
        assert!(!backend
            .objects
            .lock()
            .unwrap()
            .contains_key(&("assets".into(), "dir".into())));
        assert!(backend
            .objects
            .lock()
            .unwrap()
            .contains_key(&("assets".into(), "dir/child.bin".into())));
        assert_eq!(
            fs.kind_for_path(
                "/objects/assets/dir",
                &JetStreamPath::parse("/objects/assets/dir").unwrap()
            )
            .unwrap(),
            FileType::Directory
        );
    }

    #[test]
    fn fuse_rename_rejects_kv_prefix_directory_source() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "dir/child.json".into()),
            br#"{"child":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());

        assert_eq!(
            fs.execute_rename("/kv/bucket/dir", "/kv/bucket/final.json", 0)
                .unwrap_err(),
            libc::EISDIR
        );
        assert_eq!(fs.pending_write_count(), 0);
        assert!(!backend
            .kv
            .lock()
            .unwrap()
            .contains_key(&("bucket".into(), "final.json".into())));
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .contains_key(&("bucket".into(), "dir/child.json".into())));
        assert_eq!(
            fs.kind_for_path(
                "/kv/bucket/dir",
                &JetStreamPath::parse("/kv/bucket/dir").unwrap()
            )
            .unwrap(),
            FileType::Directory
        );
    }

    #[test]
    fn fuse_rename_rejects_materialized_kv_prefix_directory_source() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (SEMANTIC_BUCKET.into(), "tags/project/a.json".into()),
            br#"{"tag":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());

        assert_eq!(
            fs.execute_rename("/semantic/tags/project", "/semantic/tags/archive", 0)
                .unwrap_err(),
            libc::EISDIR
        );
        assert_eq!(fs.pending_write_count(), 0);
        assert!(!backend
            .kv
            .lock()
            .unwrap()
            .contains_key(&(SEMANTIC_BUCKET.into(), "tags/archive".into())));
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .contains_key(&(SEMANTIC_BUCKET.into(), "tags/project/a.json".into())));
        assert_eq!(
            fs.kind_for_path(
                "/semantic/tags/project",
                &JetStreamPath::parse("/semantic/tags/project").unwrap()
            )
            .unwrap(),
            FileType::Directory
        );
    }

    #[test]
    fn fuse_rename_rejects_object_prefix_directory_source() {
        let backend = MemoryBackend::default();
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "dir/child.bin".into()), b"child".to_vec());
        let mut fs = test_fs(backend.clone());

        assert_eq!(
            fs.execute_rename("/objects/assets/dir", "/objects/assets/final.bin", 0)
                .unwrap_err(),
            libc::EISDIR
        );
        assert_eq!(fs.pending_write_count(), 0);
        assert!(!backend
            .objects
            .lock()
            .unwrap()
            .contains_key(&("assets".into(), "final.bin".into())));
        assert!(backend
            .objects
            .lock()
            .unwrap()
            .contains_key(&("assets".into(), "dir/child.bin".into())));
        assert_eq!(
            fs.kind_for_path(
                "/objects/assets/dir",
                &JetStreamPath::parse("/objects/assets/dir").unwrap()
            )
            .unwrap(),
            FileType::Directory
        );
    }

    #[test]
    fn fuse_rename_queues_source_delete_when_delete_fails_after_destination_write() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "tmp.json".into()),
            br#"{"kv":true}"#.to_vec(),
        );
        backend.fail_deletes();
        let mut fs = test_fs(backend.clone());

        fs.execute_rename("/kv/bucket/tmp.json", "/kv/bucket/final.json", 0)
            .unwrap();

        assert_eq!(fs.pending_write_count(), 1);
        assert_eq!(
            fs.read_bytes(
                "/kv/bucket/tmp.json",
                &JetStreamPath::parse("/kv/bucket/tmp.json").unwrap()
            )
            .unwrap_err(),
            libc::ENOENT
        );
        assert_eq!(
            fs.read_bytes(
                "/objects/assets/tmp/blob.txt",
                &JetStreamPath::parse("/objects/assets/tmp/blob.txt").unwrap()
            )
            .unwrap_err(),
            libc::ENOENT
        );
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/kv/bucket").unwrap())
                .unwrap()
                .into_iter()
                .filter(|entry| entry.name == "tmp.json")
                .count(),
            0
        );
        assert_eq!(
            fs.commit_path("/kv/bucket/newer.json", br#"{"newer":true}"#)
                .unwrap_err(),
            libc::EROFS
        );
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .contains_key(&("bucket".into(), "tmp.json".into())));

        backend.allow_deletes();
        fs.replay_queue();

        assert_eq!(fs.pending_write_count(), 0);
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .get(&("bucket".into(), "tmp.json".into()))
            .is_none());
    }

    #[test]
    fn fuse_rename_persists_completion_before_destination_write() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "tmp.json".into()),
            br#"{"kv":true}"#.to_vec(),
        );
        backend.fail_writes();
        let mut fs = test_fs(backend.clone());

        fs.execute_rename("/kv/bucket/tmp.json", "/kv/bucket/final.json", 0)
            .unwrap();

        assert_eq!(fs.pending_write_count(), 1);
        assert!(fs.writes_blocked());
        assert_eq!(
            fs.read_bytes(
                "/kv/bucket/final.json",
                &JetStreamPath::parse("/kv/bucket/final.json").unwrap()
            )
            .unwrap(),
            br#"{"kv":true}"#
        );
        assert_eq!(
            fs.read_bytes(
                "/kv/bucket/tmp.json",
                &JetStreamPath::parse("/kv/bucket/tmp.json").unwrap()
            )
            .unwrap_err(),
            libc::ENOENT
        );
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .contains_key(&("bucket".into(), "tmp.json".into())));
        assert!(!backend
            .kv
            .lock()
            .unwrap()
            .contains_key(&("bucket".into(), "final.json".into())));

        backend.allow_writes();
        fs.replay_queue();

        assert_eq!(fs.pending_write_count(), 0);
        assert_eq!(
            backend
                .kv
                .lock()
                .unwrap()
                .get(&("bucket".into(), "final.json".into()))
                .unwrap(),
            br#"{"kv":true}"#
        );
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .get(&("bucket".into(), "tmp.json".into()))
            .is_none());
    }

    #[test]
    fn fuse_object_rename_queues_source_delete_when_delete_fails_after_destination_write() {
        let backend = MemoryBackend::default();
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "tmp/blob.txt".into()), b"object".to_vec());
        backend.fail_deletes();
        let mut fs = test_fs(backend.clone());

        fs.execute_rename(
            "/objects/assets/tmp/blob.txt",
            "/objects/assets/final/blob.txt",
            0,
        )
        .unwrap();

        assert_eq!(fs.pending_write_count(), 1);
        assert_eq!(
            fs.read_bytes(
                "/objects/assets/tmp/blob.txt",
                &JetStreamPath::parse("/objects/assets/tmp/blob.txt").unwrap()
            )
            .unwrap_err(),
            libc::ENOENT
        );
        assert_eq!(
            fs.commit_path("/objects/assets/newer.txt", b"newer")
                .unwrap_err(),
            libc::EROFS
        );
        assert!(backend
            .objects
            .lock()
            .unwrap()
            .contains_key(&("assets".into(), "tmp/blob.txt".into())));

        backend.allow_deletes();
        fs.replay_queue();

        assert_eq!(fs.pending_write_count(), 0);
        assert!(backend
            .objects
            .lock()
            .unwrap()
            .get(&("assets".into(), "tmp/blob.txt".into()))
            .is_none());
    }

    #[test]
    fn fuse_object_rename_persists_completion_before_destination_write() {
        let backend = MemoryBackend::default();
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "tmp/blob.txt".into()), b"object".to_vec());
        backend.fail_writes();
        let mut fs = test_fs(backend.clone());

        fs.execute_rename(
            "/objects/assets/tmp/blob.txt",
            "/objects/assets/final/blob.txt",
            0,
        )
        .unwrap();

        assert_eq!(fs.pending_write_count(), 1);
        assert!(fs.writes_blocked());
        assert_eq!(
            fs.read_bytes(
                "/objects/assets/final/blob.txt",
                &JetStreamPath::parse("/objects/assets/final/blob.txt").unwrap()
            )
            .unwrap(),
            b"object"
        );
        assert_eq!(
            fs.read_bytes(
                "/objects/assets/tmp/blob.txt",
                &JetStreamPath::parse("/objects/assets/tmp/blob.txt").unwrap()
            )
            .unwrap_err(),
            libc::ENOENT
        );
        assert!(backend
            .objects
            .lock()
            .unwrap()
            .contains_key(&("assets".into(), "tmp/blob.txt".into())));
        assert!(!backend
            .objects
            .lock()
            .unwrap()
            .contains_key(&("assets".into(), "final/blob.txt".into())));

        backend.allow_writes();
        fs.replay_queue();

        assert_eq!(fs.pending_write_count(), 0);
        assert_eq!(
            backend
                .objects
                .lock()
                .unwrap()
                .get(&("assets".into(), "final/blob.txt".into()))
                .unwrap(),
            b"object"
        );
        assert!(backend
            .objects
            .lock()
            .unwrap()
            .get(&("assets".into(), "tmp/blob.txt".into()))
            .is_none());
    }

    #[test]
    fn fuse_rename_keeps_destination_overlay_when_source_delete_remains_pending() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "tmp.json".into()),
            br#"{"kv":true}"#.to_vec(),
        );
        backend.fail_deletes();
        let mut fs = test_fs(backend.clone());

        fs.execute_rename("/kv/bucket/tmp.json", "/kv/bucket/final.json", 0)
            .unwrap();

        assert_eq!(fs.pending_write_count(), 1);
        backend.fail_kv_reads();
        backend.fail_directory_lists();

        assert_eq!(
            fs.read_bytes(
                "/kv/bucket/final.json",
                &JetStreamPath::parse("/kv/bucket/final.json").unwrap()
            )
            .unwrap(),
            br#"{"kv":true}"#
        );
        let entries = fs
            .directory_entries(&JetStreamPath::parse("/kv/bucket").unwrap())
            .unwrap();
        assert!(entries.iter().any(|entry| entry.name == "final.json"));
        assert!(!entries.iter().any(|entry| entry.name == "tmp.json"));
    }

    #[test]
    fn fuse_queued_nested_rename_delete_preserves_live_sibling_directory() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().extend([
            (
                ("bucket".into(), "dir/move.json".into()),
                br#"{"move":true}"#.to_vec(),
            ),
            (
                ("bucket".into(), "dir/keep.json".into()),
                br#"{"keep":true}"#.to_vec(),
            ),
        ]);
        backend.fail_deletes();
        let mut fs = test_fs(backend);

        fs.execute_rename("/kv/bucket/dir/move.json", "/kv/bucket/moved.json", 0)
            .unwrap();

        assert_eq!(fs.pending_write_count(), 1);
        let bucket_entries = fs
            .directory_entries(&JetStreamPath::parse("/kv/bucket").unwrap())
            .unwrap();
        assert!(bucket_entries
            .iter()
            .any(|entry| entry.name == "dir" && entry.kind == EntryKind::Directory));
        assert!(bucket_entries
            .iter()
            .any(|entry| entry.name == "moved.json" && entry.kind == EntryKind::File));

        let dir_entries = fs
            .directory_entries(&JetStreamPath::parse("/kv/bucket/dir").unwrap())
            .unwrap();
        assert_eq!(
            dir_entries,
            vec![DirectoryEntry {
                name: "keep.json".into(),
                kind: EntryKind::File,
            }]
        );
    }

    #[test]
    fn fuse_queued_nested_rename_delete_hides_empty_source_directory() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "dir/move.json".into()),
            br#"{"move":true}"#.to_vec(),
        );
        backend.fail_deletes();
        let mut fs = test_fs(backend);

        fs.execute_rename("/kv/bucket/dir/move.json", "/kv/bucket/moved.json", 0)
            .unwrap();

        assert_eq!(fs.pending_write_count(), 1);
        let bucket_entries = fs
            .directory_entries(&JetStreamPath::parse("/kv/bucket").unwrap())
            .unwrap();
        assert!(!bucket_entries.iter().any(|entry| entry.name == "dir"));
        assert!(bucket_entries
            .iter()
            .any(|entry| entry.name == "moved.json" && entry.kind == EntryKind::File));
        assert_eq!(
            fs.kind_for_path(
                "/kv/bucket/dir",
                &JetStreamPath::parse("/kv/bucket/dir").unwrap()
            )
            .unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_queued_nested_rename_delete_preserves_exact_parent_file() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().extend([
            (("bucket".into(), "dir".into()), b"exact".to_vec()),
            (
                ("bucket".into(), "dir/move.json".into()),
                br#"{"move":true}"#.to_vec(),
            ),
        ]);
        backend.fail_deletes();
        let mut fs = test_fs(backend);

        fs.execute_rename("/kv/bucket/dir/move.json", "/kv/bucket/moved.json", 0)
            .unwrap();

        assert_eq!(
            fs.kind_for_path(
                "/kv/bucket/dir",
                &JetStreamPath::parse("/kv/bucket/dir").unwrap()
            )
            .unwrap(),
            FileType::RegularFile
        );
    }

    #[test]
    fn fuse_queued_pending_kv_descendant_delete_preserves_exact_file_listing() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().extend([
            (("bucket".into(), "dir".into()), b"exact".to_vec()),
            (
                ("bucket".into(), "dir/file.json".into()),
                br#"{"nested":true}"#.to_vec(),
            ),
        ]);
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-kv-descendant",
            queued_kv_source_delete("bucket", "dir/file.json"),
        );

        let entries = fs
            .directory_entries(&JetStreamPath::parse("/kv/bucket").unwrap())
            .unwrap();
        assert_directory_entry(&entries, "dir", EntryKind::File);
    }

    #[test]
    fn fuse_queued_pending_object_descendant_delete_preserves_exact_file_listing() {
        let backend = MemoryBackend::default();
        backend.objects.lock().unwrap().extend([
            (("assets".into(), "dir".into()), b"exact".to_vec()),
            (("assets".into(), "dir/file.bin".into()), b"nested".to_vec()),
        ]);
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-object-descendant",
            queued_object_source_delete("assets", "dir/file.bin"),
        );

        let entries = fs
            .directory_entries(&JetStreamPath::parse("/objects/assets").unwrap())
            .unwrap();
        assert_directory_entry(&entries, "dir", EntryKind::File);
    }

    #[test]
    fn fuse_queued_pending_materialized_descendant_delete_preserves_exact_file_listing() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().extend([
            (
                (
                    eventfs_protocol::subjects::AGENTS_BUCKET.into(),
                    "bot/memory/dir".into(),
                ),
                b"exact".to_vec(),
            ),
            (
                (
                    eventfs_protocol::subjects::AGENTS_BUCKET.into(),
                    "bot/memory/dir/file.json".into(),
                ),
                br#"{"nested":true}"#.to_vec(),
            ),
        ]);
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-materialized-descendant",
            queued_kv_source_delete(
                eventfs_protocol::subjects::AGENTS_BUCKET,
                "bot/memory/dir/file.json",
            ),
        );

        let entries = fs
            .directory_entries(&JetStreamPath::parse("/agents/bot/memory").unwrap())
            .unwrap();
        assert_directory_entry(&entries, "dir", EntryKind::File);
    }

    #[test]
    fn fuse_queued_pending_kv_exact_file_delete_reveals_descendant_directory() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().extend([
            (("bucket".into(), "dir".into()), b"exact".to_vec()),
            (
                ("bucket".into(), "dir/keep.json".into()),
                br#"{"keep":true}"#.to_vec(),
            ),
        ]);
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-kv-exact",
            queued_kv_source_delete("bucket", "dir"),
        );

        let entries = fs
            .directory_entries(&JetStreamPath::parse("/kv/bucket").unwrap())
            .unwrap();
        assert_directory_entry(&entries, "dir", EntryKind::Directory);
        assert_eq!(
            fs.kind_for_path(
                "/kv/bucket/dir",
                &JetStreamPath::parse("/kv/bucket/dir").unwrap()
            )
            .unwrap(),
            FileType::Directory
        );
        let child_entries = fs
            .directory_entries(&JetStreamPath::parse("/kv/bucket/dir").unwrap())
            .unwrap();
        assert_directory_entry(&child_entries, "keep.json", EntryKind::File);
    }

    #[test]
    fn fuse_queued_pending_object_exact_file_delete_reveals_descendant_directory() {
        let backend = MemoryBackend::default();
        backend.objects.lock().unwrap().extend([
            (("assets".into(), "dir".into()), b"exact".to_vec()),
            (("assets".into(), "dir/keep.bin".into()), b"nested".to_vec()),
        ]);
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-object-exact",
            queued_object_source_delete("assets", "dir"),
        );

        let entries = fs
            .directory_entries(&JetStreamPath::parse("/objects/assets").unwrap())
            .unwrap();
        assert_directory_entry(&entries, "dir", EntryKind::Directory);
        assert_eq!(
            fs.kind_for_path(
                "/objects/assets/dir",
                &JetStreamPath::parse("/objects/assets/dir").unwrap()
            )
            .unwrap(),
            FileType::Directory
        );
        let child_entries = fs
            .directory_entries(&JetStreamPath::parse("/objects/assets/dir").unwrap())
            .unwrap();
        assert_directory_entry(&child_entries, "keep.bin", EntryKind::File);
    }

    #[test]
    fn fuse_queued_pending_materialized_exact_file_delete_reveals_descendant_directory() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().extend([
            (
                (
                    eventfs_protocol::subjects::AGENTS_BUCKET.into(),
                    "bot/memory/dir".into(),
                ),
                b"exact".to_vec(),
            ),
            (
                (
                    eventfs_protocol::subjects::AGENTS_BUCKET.into(),
                    "bot/memory/dir/keep.json".into(),
                ),
                br#"{"keep":true}"#.to_vec(),
            ),
        ]);
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-materialized-exact",
            queued_kv_source_delete(eventfs_protocol::subjects::AGENTS_BUCKET, "bot/memory/dir"),
        );

        let entries = fs
            .directory_entries(&JetStreamPath::parse("/agents/bot/memory").unwrap())
            .unwrap();
        assert_directory_entry(&entries, "dir", EntryKind::Directory);
        assert_eq!(
            fs.kind_for_path(
                "/agents/bot/memory/dir",
                &JetStreamPath::parse("/agents/bot/memory/dir").unwrap()
            )
            .unwrap(),
            FileType::Directory
        );
        let child_entries = fs
            .directory_entries(&JetStreamPath::parse("/agents/bot/memory/dir").unwrap())
            .unwrap();
        assert_directory_entry(&child_entries, "keep.json", EntryKind::File);
    }

    #[test]
    fn fuse_queued_pending_kv_delete_preserves_root_and_bucket_namespace_entries() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "only.json".into()),
            br#"{"only":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-kv-only",
            queued_kv_source_delete("bucket", "only.json"),
        );

        let root_entries = fs
            .directory_entries(&JetStreamPath::parse("/").unwrap())
            .unwrap();
        assert_directory_entry(&root_entries, "kv", EntryKind::Directory);
        let kv_entries = fs
            .directory_entries(&JetStreamPath::parse("/kv").unwrap())
            .unwrap();
        assert_directory_entry(&kv_entries, "bucket", EntryKind::Directory);
        assert_eq!(
            fs.kind_for_path("/kv/bucket", &JetStreamPath::parse("/kv/bucket").unwrap())
                .unwrap(),
            FileType::Directory
        );
        let entries = fs
            .directory_entries(&JetStreamPath::parse("/kv/bucket").unwrap())
            .unwrap();
        assert_no_directory_entry(&entries, "only.json");
    }

    #[test]
    fn fuse_queued_pending_object_delete_preserves_objects_bucket_namespace_entry() {
        let backend = MemoryBackend::default();
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "only.bin".into()), b"object".to_vec());
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-object-only",
            queued_object_source_delete("assets", "only.bin"),
        );

        let objects_entries = fs
            .directory_entries(&JetStreamPath::parse("/objects").unwrap())
            .unwrap();
        assert_directory_entry(&objects_entries, "assets", EntryKind::Directory);
        let bucket_entries = fs
            .directory_entries(&JetStreamPath::parse("/objects/assets").unwrap())
            .unwrap();
        assert_no_directory_entry(&bucket_entries, "only.bin");
    }

    #[test]
    fn fuse_queued_pending_agent_record_delete_preserves_static_agent_area_entry() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (
                eventfs_protocol::subjects::AGENTS_BUCKET.into(),
                "bot/memory/facts/a.json".into(),
            ),
            br#"{"fact":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-agent-record",
            queued_kv_source_delete(
                eventfs_protocol::subjects::AGENTS_BUCKET,
                "bot/memory/facts/a.json",
            ),
        );

        let agent_entries = fs
            .directory_entries(&JetStreamPath::parse("/agents/bot").unwrap())
            .unwrap();
        assert_directory_entry(&agent_entries, "memory", EntryKind::Directory);
        let memory_entries = fs
            .directory_entries(&JetStreamPath::parse("/agents/bot/memory").unwrap())
            .unwrap();
        assert_no_directory_entry(&memory_entries, "facts");
    }

    #[test]
    fn fuse_queued_pending_semantic_record_delete_preserves_static_semantic_area_entry() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (
                eventfs_protocol::subjects::SEMANTIC_BUCKET.into(),
                "tags/project/a.json".into(),
            ),
            br#"{"tag":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "delete-semantic-record",
            queued_kv_source_delete(
                eventfs_protocol::subjects::SEMANTIC_BUCKET,
                "tags/project/a.json",
            ),
        );

        let semantic_entries = fs
            .directory_entries(&JetStreamPath::parse("/semantic").unwrap())
            .unwrap();
        assert_directory_entry(&semantic_entries, "tags", EntryKind::Directory);
        let tag_entries = fs
            .directory_entries(&JetStreamPath::parse("/semantic/tags").unwrap())
            .unwrap();
        assert_no_directory_entry(&tag_entries, "project");
    }

    #[test]
    fn fuse_rename_replay_skips_recreated_kv_source_generation() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "tmp.json".into()),
            br#"{"old":true}"#.to_vec(),
        );
        backend.fail_deletes();
        let mut fs = test_fs(backend.clone());

        fs.execute_rename("/kv/bucket/tmp.json", "/kv/bucket/final.json", 0)
            .unwrap();
        backend
            .kv_put("bucket", "tmp.json", br#"{"newer":true}"#)
            .unwrap();
        backend.allow_deletes();
        fs.replay_queue();

        assert_eq!(fs.pending_write_count(), 0);
        assert_eq!(
            backend
                .kv
                .lock()
                .unwrap()
                .get(&("bucket".into(), "tmp.json".into()))
                .unwrap(),
            br#"{"newer":true}"#
        );
        assert_eq!(
            backend
                .kv
                .lock()
                .unwrap()
                .get(&("bucket".into(), "final.json".into()))
                .unwrap(),
            br#"{"old":true}"#
        );
    }

    #[test]
    fn fuse_object_rename_replay_skips_recreated_source_generation() {
        let backend = MemoryBackend::default();
        backend.objects.lock().unwrap().insert(
            ("assets".into(), "tmp/blob.txt".into()),
            b"old-object".to_vec(),
        );
        backend.fail_deletes();
        let mut fs = test_fs(backend.clone());

        fs.execute_rename(
            "/objects/assets/tmp/blob.txt",
            "/objects/assets/final/blob.txt",
            0,
        )
        .unwrap();
        backend
            .object_put("assets", "tmp/blob.txt", b"new-object")
            .unwrap();
        backend.allow_deletes();
        fs.replay_queue();

        assert_eq!(fs.pending_write_count(), 0);
        assert_eq!(
            backend
                .objects
                .lock()
                .unwrap()
                .get(&("assets".into(), "tmp/blob.txt".into()))
                .unwrap(),
            b"new-object"
        );
        assert_eq!(
            backend
                .objects
                .lock()
                .unwrap()
                .get(&("assets".into(), "final/blob.txt".into()))
                .unwrap(),
            b"old-object"
        );
    }

    #[test]
    fn fuse_queued_jsonl_publish_remains_locally_visible_until_replay() {
        assert_queued_jsonl_visible(
            "/events/system.jsonl",
            "/streams/system/subjects/events.system.jsonl",
            "/events",
            "system.jsonl",
        );
        assert_queued_jsonl_visible(
            "/agents/bot/inbox",
            "/streams/EVENTFS_AGENTS/subjects/agents.bot.inbox.jsonl",
            "/agents",
            "bot",
        );
        assert_queued_jsonl_visible(
            "/streams/ORDERS/subjects/orders.created.jsonl",
            "/streams/ORDERS/subjects/orders.created.jsonl",
            "/streams/ORDERS/subjects",
            "orders.created.jsonl",
        );
    }

    fn assert_queued_jsonl_visible(
        path: &str,
        subject_path: &str,
        list_path: &str,
        expected_child: &str,
    ) {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend.clone());
        let parsed = JetStreamPath::parse(path).unwrap();
        let subject_view = JetStreamPath::parse(subject_path).unwrap();
        let list = JetStreamPath::parse(list_path).unwrap();
        let payload = br#"{"queued":true}
"#;

        backend.fail_writes();
        fs.commit_path(path, payload).unwrap();

        assert!(fs.writes_blocked());
        assert_eq!(fs.pending_write_count(), 1);
        assert_eq!(fs.read_bytes(path, &parsed).unwrap(), payload);
        assert_eq!(fs.read_bytes(subject_path, &subject_view).unwrap(), payload);
        assert_eq!(
            fs.attr_for_path(path, 0, 0).unwrap().size,
            payload.len() as u64
        );
        assert!(fs
            .directory_entries(&list)
            .unwrap()
            .iter()
            .any(|entry| entry.name == expected_child));
    }

    #[test]
    fn fuse_queued_jsonl_overlay_skips_partially_published_lines() {
        let backend = MemoryBackend::default();
        backend.fail_jsonl_after_lines(1);
        let mut fs = test_fs(backend.clone());
        let path = "/events/system.jsonl";
        let parsed = JetStreamPath::parse(path).unwrap();
        let subject_path = "/streams/system/subjects/events.system.jsonl";
        let subject_view = JetStreamPath::parse(subject_path).unwrap();
        let payload = br#"{"line":1}
{"line":2}
"#;

        fs.commit_path(path, payload).unwrap();

        assert_eq!(fs.pending_write_count(), 1);
        assert_eq!(backend.streams.lock().unwrap()["system"].len(), 1);
        assert_eq!(fs.read_bytes(path, &parsed).unwrap(), payload);
        assert_eq!(fs.read_bytes(subject_path, &subject_view).unwrap(), payload);
    }

    #[test]
    fn fuse_queued_jsonl_overlay_uses_backend_snapshot_for_partially_published_aliases() {
        let backend = MemoryBackend::default();
        backend.streams.lock().unwrap().insert(
            "system".into(),
            vec![stream_message(
                "system",
                1,
                "events.system",
                br#"{"line":0}"#,
            )],
        );
        let mut fs = test_fs(backend.clone());
        let path = "/events/system.jsonl";
        let parsed = JetStreamPath::parse(path).unwrap();
        let subject_path = "/streams/system/subjects/events.system.jsonl";
        let subject_view = JetStreamPath::parse(subject_path).unwrap();
        let existing = br#"{"line":0}
"#;
        let payload = br#"{"line":1}
{"line":2}
"#;
        let expected = br#"{"line":0}
{"line":1}
{"line":2}
"#;

        assert_eq!(
            fs.read_bytes(subject_path, &subject_view).unwrap(),
            existing
        );
        backend.fail_jsonl_after_lines(1);
        fs.commit_path(path, payload).unwrap();

        assert_eq!(fs.read_bytes(path, &parsed).unwrap(), expected);
        assert_eq!(
            fs.read_bytes(subject_path, &subject_view).unwrap(),
            expected
        );
    }

    #[test]
    fn writeback_startup_replay_updates_jsonl_applied_progress_after_partial_failure() {
        let backend = MemoryBackend::default();
        backend.fail_jsonl_after_lines(1);
        let queue_dir = test_queue_dir();
        let payload = br#"{"line":1}
{"line":2}
"#;
        seed_pending_write(
            &queue_dir,
            "queued-jsonl",
            FailedWriteOperation::PublishJsonLines {
                stream: "system".into(),
                subject: "events.system".into(),
                bytes: payload.to_vec(),
                applied_lines: 0,
            },
        );

        let queue = WritebackQueue::open(&queue_dir, 16).unwrap();
        let mut fs = JetStreamFuse::new(Box::new(backend.clone()), queue, "test");
        let path = "/events/system.jsonl";
        let parsed = JetStreamPath::parse(path).unwrap();
        let subject_path = "/streams/system/subjects/events.system.jsonl";
        let subject_view = JetStreamPath::parse(subject_path).unwrap();

        assert!(fs.writes_blocked());
        assert_eq!(backend.streams.lock().unwrap()["system"].len(), 1);
        assert_eq!(fs.pending_write_count(), 1);
        match &fs.pending_overlays()[0].payload {
            PendingWritePayload::JsonLines { applied_lines, .. } => {
                assert_eq!(*applied_lines, 1);
            }
            payload => panic!("unexpected pending payload after startup replay: {payload:?}"),
        }

        assert_eq!(fs.read_bytes(path, &parsed).unwrap(), payload);
        assert_eq!(fs.read_bytes(subject_path, &subject_view).unwrap(), payload);
        drop(fs);

        let reopened = WritebackReplay::open(&queue_dir, 16).unwrap();
        match &reopened.pending_overlays()[0].payload {
            PendingWritePayload::JsonLines { applied_lines, .. } => {
                assert_eq!(*applied_lines, 1);
            }
            payload => panic!("unexpected persisted payload after replay: {payload:?}"),
        }
    }

    #[test]
    fn fuse_queued_jsonl_overlay_derives_durable_prefix_when_entry_progress_is_stale() {
        let backend = MemoryBackend::default();
        backend.streams.lock().unwrap().insert(
            "system".into(),
            vec![stream_message(
                "system",
                1,
                "events.system",
                br#"{"line":1}"#,
            )],
        );
        let mut fs = test_fs(backend);
        let payload = br#"{"line":1}
{"line":2}
"#;
        enqueue_pending_operation(
            &mut fs,
            "stale-jsonl-progress",
            FailedWriteOperation::PublishJsonLines {
                stream: "system".into(),
                subject: "events.system".into(),
                bytes: payload.to_vec(),
                applied_lines: 0,
            },
        );

        let path = "/events/system.jsonl";
        let parsed = JetStreamPath::parse(path).unwrap();
        let subject_path = "/streams/system/subjects/events.system.jsonl";
        let subject_view = JetStreamPath::parse(subject_path).unwrap();

        assert_eq!(fs.read_bytes(path, &parsed).unwrap(), payload);
        assert_eq!(fs.read_bytes(subject_path, &subject_view).unwrap(), payload);
    }

    #[test]
    fn writeback_blocks_new_writes_when_startup_replay_leaves_pending_entries() {
        let backend = MemoryBackend::default();
        backend.fail_writes();
        let queue_dir = test_queue_dir();
        seed_pending_write(
            &queue_dir,
            "queued-a",
            FailedWriteOperation::KvPut {
                bucket: "bucket".into(),
                key: "file.json".into(),
                bytes: br#"{"a":true}"#.to_vec(),
            },
        );

        let queue = WritebackQueue::open(&queue_dir, 16).unwrap();
        let mut fs = JetStreamFuse::new(Box::new(backend.clone()), queue, "test");
        backend.allow_writes();

        assert!(fs.writes_blocked());
        assert_eq!(
            fs.commit_path("/kv/bucket/file.json", br#"{"b":true}"#)
                .unwrap_err(),
            libc::EROFS
        );
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .get(&("bucket".into(), "file.json".into()))
            .is_none());
        assert_eq!(fs.pending_write_count(), 1);
    }

    #[test]
    fn writeback_startup_pending_queue_rebuilds_visible_overlay() {
        let backend = MemoryBackend::default();
        backend.fail_writes();
        let queue_dir = test_queue_dir();
        seed_pending_write(
            &queue_dir,
            "queued-visible",
            FailedWriteOperation::KvPut {
                bucket: "bucket".into(),
                key: "nested/file.json".into(),
                bytes: br#"{"queued":true}"#.to_vec(),
            },
        );

        let queue = WritebackQueue::open(&queue_dir, 16).unwrap();
        let mut fs = JetStreamFuse::new(Box::new(backend), queue, "test");
        let path = "/kv/bucket/nested/file.json";
        let parsed = JetStreamPath::parse(path).unwrap();

        assert!(fs.writes_blocked());
        assert_eq!(fs.read_bytes(path, &parsed).unwrap(), br#"{"queued":true}"#);
        assert_eq!(
            fs.attr_for_path(path, 0, 0).unwrap().kind,
            FileType::RegularFile
        );
        assert_eq!(
            fs.attr_for_path(mp("/kv/bucket"), 0, 0).unwrap().kind,
            FileType::Directory
        );

        let root_entries = fs
            .directory_entries(&JetStreamPath::parse("/kv").unwrap())
            .unwrap();
        assert!(root_entries
            .iter()
            .any(|entry| { entry.name == "bucket" && entry.kind == EntryKind::Directory }));
    }

    #[test]
    fn writeback_blocks_new_writes_after_runtime_write_is_queued() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend.clone());

        backend.fail_writes();
        fs.commit_path("/kv/bucket/file.json", br#"{"a":true}"#)
            .unwrap();
        backend.allow_writes();

        assert!(fs.writes_blocked());
        assert_eq!(
            fs.commit_path("/kv/bucket/file.json", br#"{"b":true}"#)
                .unwrap_err(),
            libc::EROFS
        );
        assert!(backend
            .kv
            .lock()
            .unwrap()
            .get(&("bucket".into(), "file.json".into()))
            .is_none());
        assert_eq!(fs.pending_write_count(), 1);
    }

    #[test]
    fn writeback_reenables_new_writes_after_replay_drains_pending_entries() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend.clone());

        backend.fail_writes();
        fs.commit_path("/kv/bucket/file.json", br#"{"a":true}"#)
            .unwrap();
        assert!(fs.writes_blocked());
        assert_eq!(fs.pending_write_count(), 1);

        backend.allow_writes();
        fs.replay_queue();

        assert_eq!(fs.pending_write_count(), 0);
        assert!(!fs.writes_blocked());
        fs.commit_path("/kv/bucket/file.json", br#"{"b":true}"#)
            .unwrap();
        assert_eq!(
            backend
                .kv
                .lock()
                .unwrap()
                .get(&("bucket".into(), "file.json".into()))
                .unwrap(),
            br#"{"b":true}"#
        );
    }

    #[test]
    fn fuse_watch_invalidation_ignores_uncached_external_paths() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend.clone());

        backend.push_watch_event(WatchEvent::invalidate_path(mp(
            "/kv/UNRELATED_BUCKET/external/noise.json",
        )));
        fs.apply_pending_watch_events();

        assert!(fs.stale_paths_is_empty());
        assert!(fs.cache_is_empty());
    }

    #[test]
    fn fuse_watch_affected_ancestor_preserves_unrelated_descendant_cached_bytes() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "changed/file.txt".into()),
            b"changed".to_vec(),
        );
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "unrelated/file.txt".into()),
            b"old".to_vec(),
        );
        let mut fs = test_fs(backend.clone());
        let unrelated = JetStreamPath::parse("/kv/bucket/unrelated/file.txt").unwrap();

        assert_eq!(
            fs.read_bytes(mp("/kv/bucket/unrelated/file.txt"), &unrelated)
                .unwrap(),
            b"old"
        );

        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "unrelated/file.txt".into()),
            b"new".to_vec(),
        );
        backend.push_watch_event(WatchEvent::invalidate_affected_path(
            mp("/kv/bucket"),
            AffectedPathReason::Ancestor,
        ));

        assert_eq!(
            fs.read_bytes(mp("/kv/bucket/unrelated/file.txt"), &unrelated)
                .unwrap(),
            b"old"
        );
        assert!(fs.cache_contains("/kv/bucket/unrelated/file.txt"));
        assert!(!fs.stale_paths_contains(&mp("/kv/bucket/unrelated/file.txt")));
    }

    #[test]
    fn fuse_unlink_invalidates_cached_file_bytes_before_watch_echo() {
        let backend = MemoryBackend::default();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "file.txt".into()), b"old".to_vec());
        let mut fs = test_fs(backend.clone());
        let path = JetStreamPath::parse("/kv/bucket/file.txt").unwrap();

        assert_eq!(
            fs.read_bytes(mp("/kv/bucket/file.txt"), &path).unwrap(),
            b"old"
        );

        fs.execute_unlink(mp("/kv/bucket/file.txt")).unwrap();

        assert_eq!(
            fs.read_bytes(mp("/kv/bucket/file.txt"), &path).unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_unlink_invalidates_cached_parent_kind_before_watch_echo() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "node/child.json".into()),
            br#"{"child":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());

        let first = fs.attr_for_path(mp("/kv/bucket/node"), 0, 0).unwrap();
        assert_eq!(first.kind, FileType::Directory);

        fs.execute_unlink(mp("/kv/bucket/node/child.json")).unwrap();

        assert_eq!(
            fs.attr_for_path(mp("/kv/bucket/node"), 0, 0).unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_watch_gap_rebuilds_cached_inode_kind() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "node/child.json".into()),
            br#"{"child":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());

        let first = fs.attr_for_path(mp("/kv/bucket/node"), 0, 0).unwrap();
        assert_eq!(first.kind, FileType::Directory);

        backend.kv.lock().unwrap().clear();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "node".into()), b"file".to_vec());
        assert_eq!(
            fs.attr_for_path(mp("/kv/bucket/node"), 0, 0).unwrap().kind,
            FileType::Directory
        );

        backend.push_watch_event(WatchEvent::Gap);

        let rebuilt = fs.attr_for_path(mp("/kv/bucket/node"), 0, 0).unwrap();
        assert_eq!(rebuilt.kind, FileType::RegularFile);
        assert_eq!(rebuilt.size, 4);
    }

    #[test]
    fn fuse_watch_ancestor_invalidation_rebuilds_cached_inode_kind() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "node/child.json".into()),
            br#"{"child":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());

        let first = fs.attr_for_path(mp("/kv/bucket/node"), 0, 0).unwrap();
        assert_eq!(first.kind, FileType::Directory);

        backend.kv.lock().unwrap().clear();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "node".into()), b"file".to_vec());
        backend.push_watch_event(WatchEvent::invalidate_path(mp("/kv/bucket/node")));

        let rebuilt = fs.attr_for_path(mp("/kv/bucket/node"), 0, 0).unwrap();
        assert_eq!(rebuilt.kind, FileType::RegularFile);
        assert_eq!(rebuilt.size, 4);
    }

    #[test]
    fn fuse_repeated_unchanged_stats_keep_stable_timestamps() {
        let backend = MemoryBackend::default();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "node".into()), b"file".to_vec());
        let mut fs = test_fs(backend);

        let first = fs.attr_for_path(mp("/kv/bucket/node"), 1000, 1000).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        let second = fs.attr_for_path(mp("/kv/bucket/node"), 2000, 2000).unwrap();

        assert_eq!(first.mtime, second.mtime);
        assert_eq!(first.ctime, second.ctime);
        assert_eq!(first.crtime, second.crtime);
    }

    #[test]
    fn fuse_kv_attrs_follow_durable_revision_times_and_advance_on_update() {
        let backend = MemoryBackend::default();
        let first = test_time(10);
        let second = test_time(20);
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "node".into()), b"file".to_vec());
        backend
            .kv_times
            .lock()
            .unwrap()
            .insert(("bucket".into(), "node".into()), first);
        let mut fs = test_fs(backend.clone());

        let initial = fs.attr_for_path(mp("/kv/bucket/node"), 0, 0).unwrap();
        assert_eq!(initial.mtime, first);
        assert_eq!(initial.ctime, first);
        assert_eq!(initial.crtime, first);

        backend
            .kv_times
            .lock()
            .unwrap()
            .insert(("bucket".into(), "node".into()), second);
        backend.push_watch_event(WatchEvent::invalidate_path(mp("/kv/bucket/node")));

        let updated = fs.attr_for_path(mp("/kv/bucket/node"), 0, 0).unwrap();
        assert_eq!(updated.mtime, second);
        assert_eq!(updated.ctime, second);
        assert_eq!(updated.crtime, second);
    }

    #[test]
    fn fuse_stream_message_attrs_follow_publish_time() {
        let backend = MemoryBackend::default();
        let published = test_time(42);
        backend.streams.lock().unwrap().insert(
            "ORDERS".into(),
            vec![stream_message_at(
                "ORDERS",
                7,
                published,
                "orders.created",
                br#"{"id":7}"#,
            )],
        );
        let mut fs = test_fs(backend);

        let attr = fs
            .attr_for_path("/streams/ORDERS/messages/7.json", 0, 0)
            .unwrap();
        assert_eq!(attr.mtime, published);
        assert_eq!(attr.ctime, published);
        assert_eq!(attr.crtime, published);
    }

    #[test]
    fn fuse_object_attrs_follow_modified_time() {
        let backend = MemoryBackend::default();
        let modified = test_time(84);
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("bucket".into(), "blob.txt".into()), b"blob".to_vec());
        backend
            .object_times
            .lock()
            .unwrap()
            .insert(("bucket".into(), "blob.txt".into()), modified);
        let mut fs = test_fs(backend);

        let attr = fs
            .attr_for_path(mp("/objects/bucket/blob.txt"), 0, 0)
            .unwrap();
        assert_eq!(attr.mtime, modified);
        assert_eq!(attr.ctime, modified);
        assert_eq!(attr.crtime, modified);
    }

    #[test]
    fn fuse_object_getattr_uses_metadata_without_hydrating_blob() {
        let backend = MemoryBackend::default();
        let modified = test_time(84);
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("bucket".into(), "blob.txt".into()), b"blob".to_vec());
        backend
            .object_times
            .lock()
            .unwrap()
            .insert(("bucket".into(), "blob.txt".into()), modified);
        backend.fail_object_reads();
        let mut fs = test_fs(backend);

        let attr = fs
            .attr_for_path(mp("/objects/bucket/blob.txt"), 0, 0)
            .unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.size, 4);
        assert_eq!(attr.mtime, modified);
        assert_eq!(attr.ctime, modified);
        assert_eq!(attr.crtime, modified);
        assert!(!fs.cache_contains("/objects/bucket/blob.txt"));
    }

    #[test]
    fn fuse_object_lookup_avoids_cache_hydration_on_cold_stat() {
        let backend = MemoryBackend::default();
        let modified = test_time(84);
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("bucket".into(), "blob.txt".into()), b"blob".to_vec());
        backend
            .object_times
            .lock()
            .unwrap()
            .insert(("bucket".into(), "blob.txt".into()), modified);
        backend.fail_object_reads();
        let mut fs = test_fs(backend);
        let parent = fs.allocate_for_path(&mp("/objects/bucket"), FileType::Directory);
        let path = fs.paths.child_path(parent, OsStr::new("blob.txt")).unwrap();
        let parsed = JetStreamPath::parse(path.as_str()).unwrap();

        let attr = fs.attr_for_path(&path, 0, 0).unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert!(!fs.cache_contains(path.as_str()));
        assert_eq!(fs.read_bytes(&path, &parsed).unwrap_err(), libc::EIO);
    }

    #[test]
    fn fuse_object_write_cache_refresh_does_not_require_object_readback() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend.clone());

        fs.commit_path("/objects/bucket/blob.txt", b"blob").unwrap();
        backend.fail_object_reads();

        let parsed = JetStreamPath::parse("/objects/bucket/blob.txt").unwrap();
        assert_eq!(
            fs.read_bytes(mp("/objects/bucket/blob.txt"), &parsed)
                .unwrap(),
            b"blob"
        );
    }

    #[test]
    fn fuse_keeps_ownership_stable_across_request_contexts() {
        let backend = MemoryBackend::default();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "node".into()), b"file".to_vec());
        let mut fs = test_fs(backend);

        let first = fs.attr_for_path(mp("/kv/bucket/node"), 1000, 1000).unwrap();
        let second = fs.attr_for_path(mp("/kv/bucket/node"), 2000, 3000).unwrap();

        assert_eq!(first.uid, second.uid);
        assert_eq!(first.gid, second.gid);
    }

    #[test]
    fn fuse_watch_ancestor_invalidation_preserves_live_descendant_inode_paths() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (
                eventfs_protocol::subjects::TASKS_BUCKET.into(),
                "demo/render-001.json".into(),
            ),
            br#"{"task":"render","state":"new"}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());

        let attr = fs
            .attr_for_path("/tasks/demo/render-001.json", 0, 0)
            .unwrap();
        backend.push_watch_event(WatchEvent::invalidate_path(mp("/tasks/demo")));

        let reread = fs
            .path_for_ino(attr.ino)
            .and_then(|path| {
                let parsed =
                    JetStreamPath::parse(path.as_str()).map_err(|err| err.errno().get())?;
                fs.read_bytes(&path, &parsed)
            })
            .unwrap();

        assert_eq!(reread, br#"{"task":"render","state":"new"}"#);
        assert_eq!(
            fs.path_for_ino(attr.ino).unwrap(),
            mp("/tasks/demo/render-001.json")
        );
    }

    #[test]
    fn fuse_watch_drain_error_invalidates_cached_bytes() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            (
                eventfs_protocol::subjects::TASKS_BUCKET.into(),
                "demo/render.json".into(),
            ),
            br#"{"state":"old"}"#.to_vec(),
        );
        let mut fs = test_fs(backend.clone());
        let path = JetStreamPath::parse("/tasks/demo/render.json").unwrap();

        assert_eq!(
            fs.read_bytes(mp("/tasks/demo/render.json"), &path).unwrap(),
            br#"{"state":"old"}"#
        );
        backend.kv.lock().unwrap().insert(
            (
                eventfs_protocol::subjects::TASKS_BUCKET.into(),
                "demo/render.json".into(),
            ),
            br#"{"state":"new"}"#.to_vec(),
        );
        backend.fail_watch_events();

        assert_eq!(
            fs.read_bytes(mp("/tasks/demo/render.json"), &path).unwrap(),
            br#"{"state":"new"}"#
        );
    }

    #[test]
    fn fuse_treats_missing_materialized_streams_as_empty_files() {
        let backend = MemoryBackend::default();
        backend
            .missing_streams
            .lock()
            .unwrap()
            .extend([eventfs_protocol::AGENTS_STREAM.into(), "ordinary".into()]);
        let mut fs = test_fs(backend);
        let inbox_path = JetStreamPath::parse("/agents/bot/inbox").unwrap();
        let event_path = JetStreamPath::parse("/events/ordinary.jsonl").unwrap();

        let attr = fs.attr_for_path(mp("/agents/bot/inbox"), 0, 0).unwrap();
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.size, 0);
        assert_eq!(
            fs.read_bytes(mp("/agents/bot/inbox"), &inbox_path).unwrap(),
            Vec::<u8>::new()
        );
        assert!(fs
            .open_handle(mp("/agents/bot/inbox"), true, false, false)
            .is_ok());
        let event_attr = fs
            .attr_for_path(mp("/events/ordinary.jsonl"), 0, 0)
            .unwrap();
        assert_eq!(event_attr.kind, FileType::RegularFile);
        assert_eq!(event_attr.size, 0);
        assert_eq!(
            fs.read_bytes(mp("/events/ordinary.jsonl"), &event_path)
                .unwrap(),
            Vec::<u8>::new()
        );
        assert!(fs
            .open_handle(mp("/events/ordinary.jsonl"), true, false, false)
            .is_ok());
        assert_eq!(
            fs.directory_entries(&JetStreamPath::parse("/streams/ordinary/messages").unwrap())
                .unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_maps_missing_stream_messages_to_enoent() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);
        let path = JetStreamPath::parse("/streams/ORDERS/messages/44.json").unwrap();

        assert_eq!(
            fs.read_bytes(mp("/streams/ORDERS/messages/44.json"), &path)
                .unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_regular_file_mknod_creates_durable_state() {
        let backend = MemoryBackend::default();
        let kv_puts = backend.kv_puts.clone();
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.execute_mknod(mp("/kv/smoke/greeting.json"), libc::S_IFREG | 0o644)
                .unwrap(),
            0
        );
        assert_eq!(kv_puts.lock().unwrap().len(), 1);
        assert_eq!(fs.pending_write_count(), 0);
    }

    #[test]
    fn fuse_queue_metadata_exposes_pending_operation_details() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);
        enqueue_pending_operation(
            &mut fs,
            "commit-1",
            FailedWriteOperation::KvPut {
                bucket: "bucket".into(),
                key: "path/file.json".into(),
                bytes: br#"{"secret":true}"#.to_vec(),
            },
        );

        let value: serde_json::Value =
            serde_json::from_slice(&fs.metadata_bytes(MetadataFile::Queue)).unwrap();

        assert_eq!(value["state"]["pending"], 1);
        assert_eq!(value["pending"][0]["idempotency_key"], "commit-1");
        assert!(value["pending"][0]["version"].is_object());
        assert_eq!(value["pending"][0]["operation_kind"], "kv_put");
        assert_eq!(value["pending"][0]["target"], "/kv/bucket/path/file.json");
        assert_eq!(value["pending"][0]["bytes_len"], 15);
        assert_eq!(value["pending"][0]["attempts"], 0);
        assert!(value.to_string().find("secret").is_none());
    }

    #[test]
    fn fuse_materialized_json_mknod_uses_valid_json_placeholder() {
        let backend = MemoryBackend::default();
        let kv_puts = backend.kv_puts.clone();
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.execute_mknod(mp("/tasks/demo/render.json"), libc::S_IFREG | 0o644)
                .unwrap(),
            4
        );
        let puts = kv_puts.lock().unwrap();
        assert_eq!(puts.len(), 1);
        assert_eq!(puts[0].0, eventfs_protocol::subjects::TASKS_BUCKET);
        assert_eq!(puts[0].1, "demo/render.json");
        assert_eq!(puts[0].2, b"null");
    }

    #[test]
    fn fuse_create_handle_for_materialized_json_commits_valid_placeholder_without_write() {
        let backend = MemoryBackend::default();
        let kv_puts = backend.kv_puts.clone();
        let mut fs = test_fs(backend);
        let fh = fs
            .open_handle(mp("/tasks/demo/created.json"), true, false, true)
            .unwrap();

        fs.commit_handle(fh).unwrap();

        let puts = kv_puts.lock().unwrap();
        assert_eq!(puts.len(), 1);
        assert_eq!(puts[0].0, eventfs_protocol::subjects::TASKS_BUCKET);
        assert_eq!(puts[0].1, "demo/created.json");
        assert_eq!(puts[0].2, b"null");
    }

    #[test]
    fn fuse_materialized_create_treats_missing_stable_bucket_as_absent_target() {
        let backend = MemoryBackend::default();
        backend
            .missing_kv_buckets
            .lock()
            .unwrap()
            .push(eventfs_protocol::subjects::TASKS_BUCKET.into());
        let mut fs = test_fs(backend);

        let fh = fs
            .open_handle("/tasks/demo/from-empty-bucket.json", true, false, true)
            .unwrap();

        fs.write_to_handle_buffer(fh, 0, br#"{"state":"new"}"#)
            .unwrap();
    }

    #[test]
    fn fuse_mknod_reply_attr_uses_durable_timestamp() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend.clone());
        let path = "/kv/smoke/greeting.json";

        let size = fs.execute_mknod(path, libc::S_IFREG | 0o644).unwrap();
        let attr = fs.regular_file_reply_attr(path, 0o644, size).unwrap();

        let modified = backend.kv_timestamp("smoke", "greeting.json");
        assert_eq!(attr.size, size);
        assert_eq!(attr.mtime, modified);
        assert_eq!(attr.ctime, modified);
        assert_eq!(attr.crtime, modified);
    }

    #[test]
    fn fuse_create_reply_attr_keeps_whole_value_create_staged_until_commit() {
        let backend = MemoryBackend::default();
        let kv_puts = backend.kv_puts.clone();
        let mut fs = test_fs(backend.clone());
        let path = "/tasks/demo/created.json";

        let fh = fs.open_handle(mp(path), true, false, true).unwrap();
        let attr = fs.create_reply_attr(path, fh, 0o644).unwrap();

        let puts = kv_puts.lock().unwrap();
        assert!(puts.is_empty());
        assert_eq!(attr.size, 0);
        assert!(!fs.handle_committed(fh).unwrap());

        drop(puts);
        fs.commit_handle(fh).unwrap();
        let puts = kv_puts.lock().unwrap();
        assert_eq!(puts.len(), 1);
        assert_eq!(puts[0].2, b"null");
        assert!(fs.handle_committed(fh).unwrap());
    }

    #[test]
    fn fuse_object_create_reply_attr_uses_staged_metadata_without_durable_write_or_readback() {
        let backend = MemoryBackend::default();
        let before = SystemTime::now();
        let mut fs = test_fs(backend.clone());
        let path = "/objects/assets/blob.bin";
        let fh = fs.open_handle(mp(path), true, false, true).unwrap();

        backend.fail_object_reads();

        let attr = fs.create_reply_attr(path, fh, 0o644).unwrap();
        assert_eq!(attr.size, 0);
        assert!(attr.mtime >= before);
        assert!(attr.ctime >= before);
        assert!(attr.crtime >= before);
        assert!(!fs.handle_committed(fh).unwrap());
    }

    #[test]
    fn fuse_truncate_handle_reply_attr_uses_staged_create_without_durable_metadata() {
        let backend = MemoryBackend::default();
        let before = SystemTime::now();
        let mut fs = test_fs(backend);
        let path = mp("/objects/assets/truncated.bin");
        let fh = fs.open_handle(&path, true, false, true).unwrap();

        let attr = fs.truncate_handle_reply_attr(&path, fh).unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.size, 0);
        assert!(attr.mtime >= before);
        assert!(!fs.handle_committed(fh).unwrap());
    }

    #[test]
    fn fuse_truncate_handle_reply_attr_rejects_mismatched_path_without_mutating_handle() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);
        let path = mp("/tasks/demo/original.json");
        let other_path = mp("/tasks/demo/other.json");
        let fh = fs.open_handle(&path, true, false, true).unwrap();
        fs.write_to_handle_buffer(fh, 0, br#"{"state":"draft"}"#)
            .unwrap();

        assert_eq!(
            fs.truncate_handle_reply_attr(&other_path, fh).unwrap_err(),
            libc::EINVAL
        );

        let handle = fs.handle_probe(fh).unwrap();
        assert_eq!(handle.buffer, br#"{"state":"draft"}"#);
        assert!(!handle.committed);
    }

    #[test]
    fn fuse_materialized_create_first_write_replaces_staged_default_payload() {
        let cases = [
            (
                "/tasks/demo/created.json",
                eventfs_protocol::subjects::TASKS_BUCKET,
                "demo/created.json",
                b"[]".as_slice(),
            ),
            (
                "/agents/bot/tasks/run.json",
                eventfs_protocol::subjects::AGENTS_BUCKET,
                "bot/tasks/run.json",
                b"{}".as_slice(),
            ),
            (
                "/semantic/tags/release.json",
                eventfs_protocol::subjects::SEMANTIC_BUCKET,
                "tags/release.json",
                b"0".as_slice(),
            ),
        ];

        for (path, bucket, key, payload) in cases {
            let backend = MemoryBackend::default();
            let kv_puts = backend.kv_puts.clone();
            let mut fs = test_fs(backend);
            let fh = fs.open_handle(mp(path), true, false, true).unwrap();

            fs.write_to_handle_buffer(fh, 0, payload).unwrap();
            fs.commit_handle(fh).unwrap();

            let puts = kv_puts.lock().unwrap();
            assert_eq!(puts.len(), 1, "path: {path}");
            assert_eq!(puts[0].0, bucket, "path: {path}");
            assert_eq!(puts[0].1, key, "path: {path}");
            assert_eq!(puts[0].2, payload, "path: {path}");
        }
    }

    #[test]
    fn fuse_create_handle_rejects_existing_whole_value_targets() {
        let backend = MemoryBackend::default();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "file.txt".into()), b"hello".to_vec());
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "blob.bin".into()), b"blob".to_vec());
        backend.kv.lock().unwrap().insert(
            (TASKS_BUCKET.into(), "demo/render.json".into()),
            br#"{"state":"old"}"#.to_vec(),
        );
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.open_handle(mp("/kv/bucket/file.txt"), true, false, true)
                .unwrap_err(),
            libc::EEXIST
        );
        assert_eq!(
            fs.open_handle(mp("/objects/assets/blob.bin"), true, false, true)
                .unwrap_err(),
            libc::EEXIST
        );
        assert_eq!(
            fs.open_handle(mp("/tasks/demo/render.json"), true, false, true)
                .unwrap_err(),
            libc::EEXIST
        );
    }

    #[test]
    fn fuse_create_handle_rejects_synthetic_directory_prefixes() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "dir/child.json".into()),
            br#"{"child":true}"#.to_vec(),
        );
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "dir/child.bin".into()), b"child".to_vec());
        backend.kv.lock().unwrap().insert(
            (SEMANTIC_BUCKET.into(), "tags/project/a.json".into()),
            br#"{"tag":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend);

        for path in [
            "/kv/bucket/dir",
            "/objects/assets/dir",
            "/semantic/tags/project",
        ] {
            assert_eq!(
                fs.open_handle(mp(path), true, false, true).unwrap_err(),
                libc::EISDIR,
                "path: {path}"
            );
        }
    }

    #[test]
    fn fuse_non_create_whole_value_write_open_rejects_missing_targets() {
        let mut fs = test_fs(MemoryBackend::default());

        for path in [
            "/kv/bucket/missing.txt",
            "/objects/assets/missing.bin",
            "/tasks/demo/missing.json",
        ] {
            assert_eq!(
                fs.open_handle(mp(path), true, false, false).unwrap_err(),
                libc::ENOENT,
                "path: {path}"
            );
        }
    }

    #[test]
    fn fuse_non_create_jsonl_write_open_allows_missing_subject_snapshot() {
        let mut fs = test_fs(MemoryBackend::default());

        let fh = fs
            .open_handle(
                "/streams/ORDERS/subjects/orders.created.jsonl",
                true,
                false,
                false,
            )
            .unwrap();

        let handle = fs.handle_probe(fh).unwrap();
        assert_eq!(handle.mode, HandleMode::JsonLines);
        assert_eq!(handle.base_offset, 0);
        assert!(handle.buffer.is_empty());
        assert!(!handle.staged_create);
    }

    #[test]
    fn fuse_create_handle_rejects_jsonl_surfaces() {
        let mut fs = test_fs(MemoryBackend::default());

        for path in [
            "/events/system.jsonl",
            "/agents/bot/inbox",
            "/streams/ORDERS/subjects/orders.created.jsonl",
        ] {
            assert_eq!(
                fs.open_handle(mp(path), true, false, true).unwrap_err(),
                libc::ENOTSUP,
                "path: {path}"
            );
        }
    }

    #[test]
    fn fuse_mknod_rejects_jsonl_surfaces() {
        let mut fs = test_fs(MemoryBackend::default());

        for path in [
            "/events/system.jsonl",
            "/agents/bot/inbox",
            "/streams/ORDERS/subjects/orders.created.jsonl",
        ] {
            assert_eq!(
                fs.execute_mknod(path, libc::S_IFREG | 0o644).unwrap_err(),
                libc::ENOTSUP,
                "path: {path}"
            );
        }
    }

    #[test]
    fn fuse_mknod_rejects_existing_whole_value_targets() {
        let backend = MemoryBackend::default();
        backend
            .kv
            .lock()
            .unwrap()
            .insert(("bucket".into(), "file.txt".into()), b"hello".to_vec());
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "blob.bin".into()), b"blob".to_vec());
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.execute_mknod(mp("/kv/bucket/file.txt"), libc::S_IFREG | 0o644)
                .unwrap_err(),
            libc::EEXIST
        );
        assert_eq!(
            fs.execute_mknod(mp("/objects/assets/blob.bin"), libc::S_IFREG | 0o644)
                .unwrap_err(),
            libc::EEXIST
        );
    }

    #[test]
    fn fuse_mknod_rejects_synthetic_directory_prefixes() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "dir/child.json".into()),
            br#"{"child":true}"#.to_vec(),
        );
        backend
            .objects
            .lock()
            .unwrap()
            .insert(("assets".into(), "dir/child.bin".into()), b"child".to_vec());
        backend.kv.lock().unwrap().insert(
            (SEMANTIC_BUCKET.into(), "tags/project/a.json".into()),
            br#"{"tag":true}"#.to_vec(),
        );
        let mut fs = test_fs(backend);

        for path in [
            "/kv/bucket/dir",
            "/objects/assets/dir",
            "/semantic/tags/project",
        ] {
            assert_eq!(
                fs.execute_mknod(path, libc::S_IFREG | 0o644).unwrap_err(),
                libc::EISDIR,
                "path: {path}"
            );
        }
    }

    #[test]
    fn fuse_object_mknod_creates_durable_empty_object() {
        let backend = MemoryBackend::default();
        let objects = backend.objects.clone();
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.execute_mknod(mp("/objects/assets/blob.bin"), libc::S_IFREG | 0o644)
                .unwrap(),
            0
        );
        assert_eq!(
            objects
                .lock()
                .unwrap()
                .get(&("assets".into(), "blob.bin".into()))
                .cloned(),
            Some(Vec::new())
        );
    }

    #[test]
    fn fuse_missing_object_unlink_returns_enoent() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.execute_unlink(mp("/objects/assets/missing.bin"))
                .unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_missing_native_and_materialized_kv_unlink_returns_enoent() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);

        assert_eq!(
            fs.execute_unlink(mp("/kv/bucket/missing.json"))
                .unwrap_err(),
            libc::ENOENT
        );
        assert_eq!(
            fs.execute_unlink(mp("/tasks/demo/missing.json"))
                .unwrap_err(),
            libc::ENOENT
        );
    }

    #[test]
    fn fuse_open_reply_flags_ignore_posix_request_flags() {
        assert_eq!(fuse_open_reply_flags(libc::O_WRONLY), 0);
        assert_eq!(fuse_open_reply_flags(libc::O_RDWR), 0);
        assert_eq!(fuse_open_reply_flags(libc::O_APPEND | libc::O_CREAT), 0);
        assert_eq!(fuse_open_reply_flags(libc::O_TRUNC | libc::O_WRONLY), 0);
    }

    #[test]
    fn mount_path_parent_resolves_root_and_nested_paths() {
        assert_eq!(mp("/").parent().as_str(), "/");
        assert_eq!(mp("/kv").parent().as_str(), "/");
        assert_eq!(mp("/kv/bucket/dir").parent().as_str(), "/kv/bucket");
    }

    #[test]
    fn fuse_parent_ino_tracks_nested_directories() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);

        let kv_ino = fs.allocate_for_path(&mp("/kv"), FileType::Directory);
        let bucket_ino = fs.allocate_for_path(&mp("/kv/bucket"), FileType::Directory);
        let dir_ino = fs.allocate_for_path(&mp("/kv/bucket/dir"), FileType::Directory);

        assert_eq!(fs.parent_ino_for_path(&mp("/")), ROOT_INO);
        assert_eq!(fs.parent_ino_for_path(&mp("/kv/bucket")), kv_ino);
        assert_eq!(fs.parent_ino_for_path(&mp("/kv/bucket/dir")), bucket_ino);
        assert_eq!(fs.path_for_ino(dir_ino).unwrap(), mp("/kv/bucket/dir"));
    }

    #[test]
    fn unlink_invalidation_paths_include_dynamic_surface_ancestors() {
        assert_eq!(
            unlink_invalidation_paths("/kv/bucket/dir/file.txt"),
            vec!["/kv/bucket/dir/file.txt", "/kv/bucket/dir", "/kv/bucket"]
        );
        assert_eq!(
            unlink_invalidation_paths("/objects/assets/images/logo.png"),
            vec![
                "/objects/assets/images/logo.png",
                "/objects/assets/images",
                "/objects/assets",
            ]
        );
    }

    #[test]
    fn unlink_invalidation_paths_include_materialized_kv_aliases() {
        assert_eq!(
            unlink_invalidation_paths(&format!("/kv/{TASKS_BUCKET}/demo/render.json")),
            vec![
                format!("/kv/{TASKS_BUCKET}/demo/render.json"),
                format!("/kv/{TASKS_BUCKET}/demo"),
                format!("/kv/{TASKS_BUCKET}"),
                "/tasks/demo/render.json".to_string(),
                "/tasks/demo".to_string(),
            ]
        );
        assert_eq!(
            unlink_invalidation_paths("/agents/bot/memory/facts/a.json"),
            vec![
                "/agents/bot/memory/facts/a.json".to_string(),
                "/agents/bot/memory/facts".to_string(),
                "/agents/bot/memory".to_string(),
                "/agents/bot".to_string(),
                format!("/kv/{AGENTS_BUCKET}/bot/memory/facts/a.json"),
                format!("/kv/{AGENTS_BUCKET}/bot/memory/facts"),
                format!("/kv/{AGENTS_BUCKET}/bot/memory"),
                format!("/kv/{AGENTS_BUCKET}/bot"),
                format!("/kv/{AGENTS_BUCKET}"),
            ]
        );
        assert_eq!(
            unlink_invalidation_paths(&format!("/kv/{SEMANTIC_BUCKET}/tags/project/a.json")),
            vec![
                format!("/kv/{SEMANTIC_BUCKET}/tags/project/a.json"),
                format!("/kv/{SEMANTIC_BUCKET}/tags/project"),
                format!("/kv/{SEMANTIC_BUCKET}/tags"),
                format!("/kv/{SEMANTIC_BUCKET}"),
                "/semantic/tags/project/a.json".to_string(),
                "/semantic/tags/project".to_string(),
                "/semantic/tags".to_string(),
            ]
        );
    }

    #[test]
    fn fuse_rejects_malformed_task_json_before_queueing() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);
        let err = fs
            .commit_path("/tasks/demo/render.json", b"not-json")
            .unwrap_err();
        assert_eq!(err, libc::EINVAL);
        assert_eq!(fs.pending_write_count(), 0);
    }

    #[test]
    fn fuse_rejects_invalid_materialized_kv_keys_before_queueing() {
        let backend = MemoryBackend::default();
        let mut fs = test_fs(backend);
        let err = fs
            .commit_path("/tasks/demo/bad key.json", br#"{"state":"new"}"#)
            .unwrap_err();

        assert_eq!(err, libc::EINVAL);
        assert_eq!(fs.pending_write_count(), 0);
    }

    #[test]
    fn fuse_commits_materialized_task_json_to_stable_bucket() {
        let backend = MemoryBackend::default();
        let kv_puts = backend.kv_puts.clone();
        let mut fs = test_fs(backend);
        fs.commit_path("/tasks/demo/render.json", br#"{"state":"new"}"#)
            .unwrap();

        let puts = kv_puts.lock().unwrap();
        assert_eq!(puts.len(), 1);
        assert_eq!(puts[0].0, eventfs_protocol::subjects::TASKS_BUCKET);
        assert_eq!(puts[0].1, "demo/render.json");
    }

    #[test]
    fn fuse_queues_valid_write_when_backend_fails() {
        #[derive(Clone, Default)]
        struct FailingBackend;

        impl ReplayStorage for FailingBackend {
            fn kv_put_idempotent(
                &self,
                _bucket: &str,
                _key: &str,
                _bytes: &[u8],
                _idempotency_key: &str,
            ) -> TransportResult<u64> {
                Err(TransportError::Invalid("down".into()))
            }

            fn kv_put_applied(
                &self,
                _bucket: &str,
                _key: &str,
                _idempotency_key: &str,
            ) -> TransportResult<bool> {
                Ok(false)
            }

            fn kv_delete_if_revision(
                &self,
                _bucket: &str,
                _key: &str,
                _expected_revision: u64,
            ) -> TransportResult<()> {
                Ok(())
            }

            fn kv_delete_if_revision_applied(
                &self,
                _bucket: &str,
                _key: &str,
                _expected_revision: u64,
            ) -> TransportResult<bool> {
                Ok(true)
            }

            fn publish_json_lines(
                &self,
                _stream: &str,
                _subject: &str,
                _bytes: &[u8],
                _idempotency_seed: &str,
            ) -> TransportResult<Vec<u64>> {
                Err(TransportError::Invalid("down".into()))
            }

            fn publish_json_lines_applied(
                &self,
                _stream: &str,
                _subject: &str,
                _bytes: &[u8],
                _idempotency_seed: &str,
            ) -> TransportResult<bool> {
                Ok(false)
            }

            fn publish_json_lines_applied_prefix(
                &self,
                _stream: &str,
                _subject: &str,
                _bytes: &[u8],
                _idempotency_seed: &str,
            ) -> TransportResult<usize> {
                Ok(0)
            }

            fn object_put_idempotent(
                &self,
                _bucket: &str,
                _object: &str,
                _bytes: &[u8],
                _idempotency_key: &str,
            ) -> TransportResult<()> {
                Err(TransportError::Invalid("down".into()))
            }

            fn object_put_applied(
                &self,
                _bucket: &str,
                _object: &str,
                _idempotency_key: &str,
            ) -> TransportResult<bool> {
                Ok(false)
            }

            fn object_delete_if_sequence(
                &self,
                _bucket: &str,
                _object: &str,
                _expected_sequence: u64,
                _expected_nuid: &str,
            ) -> TransportResult<()> {
                Ok(())
            }

            fn object_delete_if_sequence_applied(
                &self,
                _bucket: &str,
                _object: &str,
                _expected_sequence: u64,
                _expected_nuid: &str,
            ) -> TransportResult<bool> {
                Ok(true)
            }
        }

        impl MountStorage for FailingBackend {
            fn list_kv_buckets(&self) -> TransportResult<Vec<String>> {
                Ok(Vec::new())
            }
            fn ensure_kv_bucket(&self, _bucket: &str) -> TransportResult<()> {
                Ok(())
            }
            fn list_kv_prefix(
                &self,
                _bucket: &str,
                _prefix: &str,
            ) -> TransportResult<Vec<DirectoryEntry>> {
                Ok(Vec::new())
            }
            fn kv_get(&self, _bucket: &str, _key: &str) -> TransportResult<Option<KeyRevision>> {
                Ok(None)
            }
            fn kv_put(&self, _bucket: &str, _key: &str, _bytes: &[u8]) -> TransportResult<u64> {
                Err(TransportError::Invalid("down".into()))
            }
            fn kv_delete(&self, _bucket: &str, _key: &str) -> TransportResult<()> {
                Ok(())
            }
            fn kv_history(&self, _bucket: &str, _key: &str) -> TransportResult<Vec<KeyRevision>> {
                Ok(Vec::new())
            }
            fn kv_revision(
                &self,
                _bucket: &str,
                _key: &str,
                _revision: u64,
            ) -> TransportResult<Option<KeyRevision>> {
                Ok(None)
            }
            fn list_streams(&self) -> TransportResult<Vec<String>> {
                Ok(Vec::new())
            }
            fn ensure_stream(&self, _stream: &str) -> TransportResult<()> {
                Ok(())
            }
            fn list_stream_messages(&self, _stream: &str) -> TransportResult<Vec<DirectoryEntry>> {
                Ok(Vec::new())
            }
            fn list_stream_subjects(&self, _stream: &str) -> TransportResult<Vec<DirectoryEntry>> {
                Ok(Vec::new())
            }
            fn stream_message(
                &self,
                stream: &str,
                sequence: u64,
            ) -> TransportResult<StreamMessageView> {
                Ok(StreamMessageView {
                    stream: stream.into(),
                    sequence,
                    published: UNIX_EPOCH,
                    subject: "test".into(),
                    payload: Vec::new(),
                })
            }
            fn list_object_buckets(&self) -> TransportResult<Vec<String>> {
                Ok(Vec::new())
            }
            fn ensure_object_bucket(&self, _bucket: &str) -> TransportResult<()> {
                Ok(())
            }
            fn list_object_prefix(
                &self,
                _bucket: &str,
                _prefix: &str,
            ) -> TransportResult<Vec<DirectoryEntry>> {
                Ok(Vec::new())
            }
            fn object_get(
                &self,
                _bucket: &str,
                _object: &str,
            ) -> TransportResult<Option<ObjectVersion>> {
                Ok(None)
            }
            fn object_put(
                &self,
                _bucket: &str,
                _object: &str,
                _bytes: &[u8],
            ) -> TransportResult<()> {
                Err(TransportError::Invalid("down".into()))
            }
            fn object_delete(&self, _bucket: &str, _object: &str) -> TransportResult<()> {
                Ok(())
            }
        }

        let queue = WritebackQueue::open(test_queue_dir(), 16).unwrap();
        let mut fs = JetStreamFuse::new(Box::new(FailingBackend), queue, "test");
        fs.commit_path("/tasks/demo/render.json", br#"{"state":"new"}"#)
            .unwrap();
        assert_eq!(fs.pending_write_count(), 1);
    }

    #[test]
    fn fuse_queued_whole_value_overlay_attr_uses_enqueue_timestamp() {
        let backend = MemoryBackend::default();
        backend.kv.lock().unwrap().insert(
            ("bucket".into(), "node".into()),
            br#"{"durable":true}"#.to_vec(),
        );
        backend
            .kv_times
            .lock()
            .unwrap()
            .insert(("bucket".into(), "node".into()), test_time(10));
        let mut fs = test_fs(backend);
        fs.set_mounted_at_for_test(test_time(5));
        enqueue_pending_operation(
            &mut fs,
            "queued-node",
            FailedWriteOperation::KvPut {
                bucket: "bucket".into(),
                key: "node".into(),
                bytes: br#"{"queued":true}"#.to_vec(),
            },
        );

        let attr = fs.attr_for_path(mp("/kv/bucket/node"), 0, 0).unwrap();

        assert_eq!(attr.mtime, test_time(20));
        assert_eq!(attr.ctime, test_time(20));
        assert_eq!(attr.crtime, test_time(20));
    }
}
