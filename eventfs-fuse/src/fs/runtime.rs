use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::SystemTime;

use eventfs_protocol::{
    plan_operation, validate_json_document, validate_json_lines, FileIntent, JetStreamAction,
    JetStreamPath, MaterializedTarget, MetadataFile, MountPath, RenamePlan, AGENTS_BUCKET,
};
use eventfs_transport::{
    CacheEntry, CacheOrigin, DirectoryEntry, EntryKind, FailedWrite, FailedWriteOperation,
    InvalidationPlan, InvalidationScope, KeyRevision, KvSourceGeneration, LocalCache, MountStorage,
    ObjectMetadata, ObjectSourceGeneration, ObjectVersion, TransportResult, VersionStamp,
    WatchEvent, WritebackGateTransition, WritebackQueue, WritebackReplay,
};

use crate::path_cache::PathCache;

#[path = "mount_projection.rs"]
mod mount_projection;

pub(super) use mount_projection::{MountProjectionKind, MountProjectionWriteMode};

use super::{
    action_publishes_jsonl, append_json_line, directory_listing_unavailable, handle_commit_bytes,
    idempotency_key, jsonl_applied_prefix_count, max_system_time, message_sequence,
    mount_instance_seed, rename_completion_key_material, DynamicPathKind, FileSnapshot, Handle,
    HandleMode, KvRenameCompletion, ObjectRenameCompletion, QueuedJsonlEntry, StagedCreate,
};

pub(super) struct MountRuntimeState {
    backend: Box<dyn MountStorage>,
    stale_paths: HashSet<MountPath>,
    handles: HashMap<u64, Handle>,
    next_fh: u64,
    next_commit_id: u64,
    mount_instance: u128,
    mounted_at: SystemTime,
    cache: LocalCache,
    replay: WritebackReplay,
    mount_name: String,
    recent_errors: Vec<String>,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::fs) struct RuntimeHandleProbe {
    pub mode: HandleMode,
    pub base_offset: usize,
    pub buffer: Vec<u8>,
    pub staged_create: bool,
    pub committed: bool,
}

impl MountRuntimeState {
    pub(super) fn new(
        backend: Box<dyn MountStorage>,
        queue: WritebackQueue,
        mount_name: impl Into<String>,
    ) -> Self {
        Self {
            backend,
            stale_paths: HashSet::new(),
            handles: HashMap::new(),
            next_fh: 2,
            next_commit_id: 1,
            mount_instance: mount_instance_seed(),
            mounted_at: SystemTime::now(),
            cache: LocalCache::new(),
            replay: WritebackReplay::from_queue(queue),
            mount_name: mount_name.into(),
            recent_errors: Vec::new(),
        }
    }
}

impl MountRuntimeState {
    pub(super) fn replay_queue(&mut self, paths: &mut PathCache) {
        if let Err(err) = self.replay.replay(self.backend.as_ref()) {
            self.record_error(format!("queue replay failed: {err}"));
        }
        self.record_blocked_writeback_gate(
            "writeback queue has pending entries after replay; mount is read-only",
        );
        self.apply_invalidation_plan(paths, &InvalidationPlan::gap());
    }

    pub(super) fn record_error(&mut self, message: String) {
        eprintln!("[eventfs] {message}");
        self.recent_errors.push(message);
        if self.recent_errors.len() > 32 {
            self.recent_errors.remove(0);
        }
    }

    pub(super) fn clear_stale_path(&mut self, path: &MountPath) {
        self.stale_paths.remove(path);
    }

    fn sync_writeback_gate(&mut self, reason: &str) {
        if self.replay.sync_gate().gate_transition == WritebackGateTransition::Blocked {
            self.record_error(reason.to_string());
        }
    }

    fn record_blocked_writeback_gate(&mut self, reason: &str) {
        if !self.replay.write_gate().accepting_writes {
            self.record_error(reason.to_string());
        }
    }

    fn ensure_writes_allowed(&self) -> Result<(), i32> {
        if !self.replay.write_gate().accepting_writes {
            Err(libc::EROFS)
        } else {
            Ok(())
        }
    }

    pub(in crate::fs) fn writes_blocked(&self) -> bool {
        !self.replay.write_gate().accepting_writes
    }

    pub(in crate::fs) fn apply_pending_watch_events(&mut self, paths: &mut PathCache) {
        match self.backend.watch_events() {
            Ok(events) => {
                for event in events {
                    self.apply_watch_event(paths, event);
                }
            }
            Err(err) => {
                self.record_error(format!("watch event drain failed: {err}"));
                self.apply_watch_event(paths, WatchEvent::Gap);
            }
        }
    }

    fn apply_watch_event(&mut self, paths: &mut PathCache, event: WatchEvent) {
        let plan = InvalidationPlan::for_watch_event(&event);
        self.apply_path_invalidation_plan(paths, &plan);
        self.cache.apply(event);
    }

    fn apply_invalidation_plan(&mut self, paths: &mut PathCache, plan: &InvalidationPlan) {
        self.apply_path_invalidation_plan(paths, plan);
        self.cache.apply_invalidation(plan);
    }

    fn apply_path_invalidation_plan(&mut self, paths: &PathCache, plan: &InvalidationPlan) {
        for action in plan.actions() {
            match action.scope {
                InvalidationScope::ExactEntry => self.mark_path_entry_stale(paths, &action.path),
                InvalidationScope::Subtree => self.mark_path_subtree_stale(paths, &action.path),
                InvalidationScope::All => {
                    for (path, _, _) in paths.snapshot() {
                        if !path.is_root() {
                            self.stale_paths.insert(path);
                        }
                    }
                }
            }
        }
    }

    fn mark_path_entry_stale(&mut self, paths: &PathCache, path: &MountPath) {
        if !path.is_root() && paths.entry(path).is_some() {
            self.stale_paths.insert(path.clone());
        }
    }

    fn mark_path_subtree_stale(&mut self, paths: &PathCache, path: &MountPath) {
        for (candidate, _, _) in paths.snapshot() {
            if !candidate.is_root() && candidate.is_self_or_descendant_of(path) {
                self.stale_paths.insert(candidate);
            }
        }
    }

    fn invalidate_after_local_mutation(&mut self, paths: &mut PathCache, path: &MountPath) {
        match InvalidationPlan::for_local_mutation(path) {
            Ok(plan) => self.apply_invalidation_plan(paths, &plan),
            Err(err) => {
                self.record_error(format!(
                    "local invalidation planning failed for {path}: {err}"
                ));
                self.apply_invalidation_plan(paths, &InvalidationPlan::subtree(path.clone()));
            }
        }
    }

    fn invalidate_after_local_write(&mut self, paths: &mut PathCache, path: &MountPath) {
        self.invalidate_after_local_mutation(paths, path);
    }

    fn invalidate_after_local_unlink(&mut self, paths: &mut PathCache, path: &MountPath) {
        self.invalidate_after_local_mutation(paths, path);
        paths.remove(path);
    }

    fn parse_path(&self, path: &MountPath) -> Result<JetStreamPath, i32> {
        JetStreamPath::parse(path.as_str()).map_err(|err| err.errno().get())
    }

    fn kv_prefix_projection_kind(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<MountProjectionKind, i32> {
        match self.classify_kv_dynamic_path(bucket, key)? {
            DynamicPathKind::Directory => Ok(MountProjectionKind::Directory),
            DynamicPathKind::File | DynamicPathKind::Missing => Ok(MountProjectionKind::File),
        }
    }

    fn classify_kv_dynamic_path(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<DynamicPathKind, i32> {
        if self
            .backend
            .kv_get(bucket, key)
            .map_err(|err| self.io_error(err))?
            .is_some()
        {
            return Ok(DynamicPathKind::File);
        }
        if self
            .backend
            .list_kv_prefix(bucket, key)
            .map_err(|err| self.io_error(err))?
            .is_empty()
        {
            Ok(DynamicPathKind::Missing)
        } else {
            Ok(DynamicPathKind::Directory)
        }
    }

    fn classify_object_dynamic_path(
        &mut self,
        bucket: &str,
        object: &str,
    ) -> Result<DynamicPathKind, i32> {
        if self.object_metadata(bucket, object)?.is_some() {
            return Ok(DynamicPathKind::File);
        }
        if self
            .backend
            .list_object_prefix(bucket, object)
            .map_err(|err| self.io_error(err))?
            .is_empty()
        {
            Ok(DynamicPathKind::Missing)
        } else {
            Ok(DynamicPathKind::Directory)
        }
    }

    fn require_kv_bucket(&mut self, bucket: &str) -> Result<(), i32> {
        let buckets = self
            .backend
            .list_kv_buckets()
            .map_err(|err| self.io_error(err))?;
        buckets
            .iter()
            .any(|candidate| candidate == bucket)
            .then_some(())
            .ok_or(libc::ENOENT)
    }

    fn require_stream(&mut self, stream: &str) -> Result<(), i32> {
        let streams = self
            .backend
            .list_streams()
            .map_err(|err| self.io_error(err))?;
        streams
            .iter()
            .any(|candidate| candidate == stream)
            .then_some(())
            .ok_or(libc::ENOENT)
    }

    fn require_object_bucket(&mut self, bucket: &str) -> Result<(), i32> {
        let buckets = self
            .backend
            .list_object_buckets()
            .map_err(|err| self.io_error(err))?;
        buckets
            .iter()
            .any(|candidate| candidate == bucket)
            .then_some(())
            .ok_or(libc::ENOENT)
    }

    pub(in crate::fs) fn read_bytes_for_handle(
        &mut self,
        paths: &mut PathCache,
        fh: u64,
        path: &MountPath,
        _parsed: &JetStreamPath,
    ) -> Result<Vec<u8>, i32> {
        let staged = self
            .handles
            .get(&fh)
            .filter(|handle| handle.path == *path && handle.dirty)
            .map(|handle| (handle.mode, handle.base_offset, handle.buffer.clone()));
        match staged {
            Some((HandleMode::WholeValue, _, buffer)) => Ok(buffer),
            Some((HandleMode::JsonLines, base_offset, buffer)) => {
                let mut bytes = match self.mount_projection(paths).read_bytes(path) {
                    Ok(bytes) => bytes,
                    Err(errno) if errno == libc::ENOENT => Vec::new(),
                    Err(errno) => return Err(errno),
                };
                if bytes.len() > base_offset {
                    bytes.truncate(base_offset);
                }
                if bytes.len() < base_offset {
                    bytes.resize(base_offset, 0);
                }
                bytes.extend_from_slice(&buffer);
                Ok(bytes)
            }
            None => self.mount_projection(paths).read_bytes(path),
        }
    }

    fn read_materialized(&mut self, target: MaterializedTarget) -> Result<FileSnapshot, i32> {
        match target {
            MaterializedTarget::Kv { bucket, key } => self
                .backend
                .kv_get(&bucket, &key)
                .map_err(|err| self.io_error(err))?
                .map(|entry| FileSnapshot {
                    bytes: entry.bytes,
                    version: VersionStamp::at(entry.created),
                })
                .ok_or(libc::ENOENT),
            MaterializedTarget::Stream { stream, subject } => {
                self.read_stream_subject_snapshot_allow_missing(&stream, &subject)
            }
        }
    }

    fn read_stream_subject_snapshot(
        &mut self,
        stream: &str,
        subject: &str,
    ) -> Result<FileSnapshot, i32> {
        let entries = self.list_stream_messages(stream, false)?;
        self.collect_stream_subject_snapshot(stream, subject, entries)
    }

    fn read_stream_subject_snapshot_allow_missing(
        &mut self,
        stream: &str,
        subject: &str,
    ) -> Result<FileSnapshot, i32> {
        let entries = self.list_stream_messages(stream, true)?;
        self.collect_stream_subject_snapshot(stream, subject, entries)
    }

    fn collect_stream_subject_snapshot(
        &mut self,
        stream: &str,
        subject: &str,
        entries: Vec<DirectoryEntry>,
    ) -> Result<FileSnapshot, i32> {
        let mut bytes = Vec::new();
        let mut newest = None;
        for sequence in entries.iter().filter_map(message_sequence) {
            let message = self
                .backend
                .stream_message(stream, sequence)
                .map_err(|err| self.io_error(err))?;
            if message.subject == subject {
                append_json_line(&mut bytes, &message.payload);
                newest = Some(max_system_time(newest, message.published));
            }
        }
        Ok(FileSnapshot {
            bytes,
            version: VersionStamp::at(newest.unwrap_or(self.mounted_at)),
        })
    }

    pub(in crate::fs) fn metadata_bytes(&self, paths: &PathCache, file: MetadataFile) -> Vec<u8> {
        match file {
            MetadataFile::Status => serde_json::json!({
                "mount_name": self.mount_name,
                "state": if self.writes_blocked() { "read_only" } else { "mounted" },
                "writes_blocked": self.writes_blocked(),
                "queue_pending": self.replay.state().pending,
            }),
            MetadataFile::Cache => serde_json::json!({
                "path_entries": paths.snapshot().len(),
                "entries": self.cache.snapshot().len(),
                "generation": self.cache.generation(),
            }),
            MetadataFile::Queue => serde_json::to_value(self.replay.snapshot()).unwrap_or_default(),
            MetadataFile::Capabilities => serde_json::json!({
                "roots": eventfs_protocol::ROOT_DIRECTORIES,
                "kv_history": true,
                "stream_messages_read_only": true,
                "jsonl_publish": true,
                "object_store": true,
                "writeback_queue": true,
            }),
            MetadataFile::Errors => {
                return self
                    .recent_errors
                    .iter()
                    .map(|error| serde_json::json!({ "error": error }).to_string())
                    .collect::<Vec<_>>()
                    .join("\n")
                    .into_bytes();
            }
        }
        .to_string()
        .into_bytes()
    }

    fn kv_history_directory_entries(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> TransportResult<Vec<DirectoryEntry>> {
        let revisions = self.backend.kv_history(bucket, key)?;
        let mut entries: Vec<_> = revisions
            .into_iter()
            .map(|entry| DirectoryEntry {
                name: format!("@{}", entry.revision),
                kind: EntryKind::File,
            })
            .collect();
        for child in self.backend.list_kv_history_prefix(bucket, key)? {
            if !entries.iter().any(|entry| entry.name == child.name) {
                entries.push(child);
            }
        }
        Ok(entries)
    }

    fn list_agents_root(&mut self) -> Result<Vec<DirectoryEntry>, i32> {
        let mut agent_names = BTreeSet::new();
        match self.backend.list_kv_prefix(AGENTS_BUCKET, "") {
            Ok(entries) => {
                for entry in entries {
                    agent_names.insert(entry.name);
                }
            }
            Err(eventfs_transport::TransportError::NotFound) => {}
            Err(err) => return Err(self.io_error(err)),
        }
        match self.backend.list_agent_names() {
            Ok(names) => {
                for name in names {
                    agent_names.insert(name);
                }
            }
            Err(eventfs_transport::TransportError::NotFound) => {}
            Err(err) => return Err(self.io_error(err)),
        }
        Ok(agent_names
            .into_iter()
            .map(|name| DirectoryEntry {
                name,
                kind: EntryKind::Directory,
            })
            .collect())
    }

    fn list_stream_messages(
        &mut self,
        stream: &str,
        allow_missing_empty: bool,
    ) -> Result<Vec<DirectoryEntry>, i32> {
        match self.backend.list_stream_messages(stream) {
            Ok(entries) => Ok(entries),
            Err(eventfs_transport::TransportError::NotFound) if allow_missing_empty => {
                Ok(Vec::new())
            }
            Err(err) => Err(self.io_error(err)),
        }
    }

    fn stream_subject_exists(&mut self, stream: &str, subject: &str) -> Result<bool, i32> {
        let subject_file = eventfs_protocol::stream_subject_file_name_from_str(subject);
        let entries = self
            .backend
            .list_stream_subjects(stream)
            .map_err(|err| self.io_error(err))?;
        Ok(entries.iter().any(|entry| entry.name == subject_file))
    }

    fn object_metadata(
        &mut self,
        bucket: &str,
        object: &str,
    ) -> Result<Option<ObjectMetadata>, i32> {
        self.backend
            .object_metadata(bucket, object)
            .map_err(|err| self.io_error(err))
    }

    pub(super) fn mount_version(&self) -> VersionStamp {
        VersionStamp::at(self.mounted_at)
    }

    pub(in crate::fs) fn open_handle(
        &mut self,
        paths: &mut PathCache,
        path: &MountPath,
        writable: bool,
        truncate: bool,
        create: bool,
    ) -> Result<u64, i32> {
        let fh = self.next_fh;
        self.next_fh += 1;
        let handle = self.build_handle(paths, path.clone(), writable, truncate, create)?;
        self.handles.insert(fh, handle);
        Ok(fh)
    }

    pub(in crate::fs) fn build_handle(
        &mut self,
        paths: &mut PathCache,
        path: MountPath,
        writable: bool,
        truncate: bool,
        create: bool,
    ) -> Result<Handle, i32> {
        if writable {
            self.ensure_writes_allowed()?;
        }
        let mode = if writable {
            self.write_mode_for_path(paths, &path)?
        } else {
            HandleMode::WholeValue
        };
        if truncate && mode == HandleMode::JsonLines {
            return Err(libc::EROFS);
        }
        let (buffer, base_offset, staged_create) =
            self.initial_handle_state(paths, &path, writable, mode, truncate, create)?;
        let dirty = writable && (truncate || create);
        let commit_seed = self.next_commit_seed();
        Ok(Handle {
            path,
            buffer,
            dirty,
            committed: false,
            writable,
            commit_seed,
            mode,
            base_offset,
            staged_create,
        })
    }

    pub(in crate::fs) fn write_mode_for_path(
        &mut self,
        paths: &mut PathCache,
        path: &MountPath,
    ) -> Result<HandleMode, i32> {
        self.mount_projection(paths)
            .write_mode(path)
            .map(|mode| match mode {
                MountProjectionWriteMode::WholeValue => HandleMode::WholeValue,
                MountProjectionWriteMode::JsonLines => HandleMode::JsonLines,
            })
    }

    pub(in crate::fs) fn initial_handle_state(
        &mut self,
        paths: &mut PathCache,
        path: &MountPath,
        writable: bool,
        mode: HandleMode,
        truncate: bool,
        create: bool,
    ) -> Result<(Vec<u8>, usize, Option<StagedCreate>), i32> {
        if !writable {
            return Ok((Vec::new(), 0, None));
        }
        if create {
            let target = self.mount_projection(paths).create_target(path)?;
            return Ok((
                Vec::new(),
                0,
                Some(self.staged_create(target.into_default_payload())),
            ));
        }
        if truncate {
            return Ok((Vec::new(), 0, None));
        }
        if mode == HandleMode::JsonLines {
            return match self.mount_projection(paths).read_bytes(path) {
                Ok(bytes) => Ok((Vec::new(), bytes.len(), None)),
                Err(errno) if errno == libc::ENOENT => Ok((Vec::new(), 0, None)),
                Err(errno) => Err(errno),
            };
        }
        match self.mount_projection(paths).read_bytes(path) {
            Ok(bytes) => Ok((bytes, 0, None)),
            Err(errno) => Err(errno),
        }
    }

    fn staged_create(&self, default_payload: Vec<u8>) -> StagedCreate {
        StagedCreate {
            default_payload,
            version: VersionStamp::at(SystemTime::now()),
        }
    }

    fn next_commit_seed(&mut self) -> String {
        let seed = format!(
            "{}:{}:{}",
            self.mount_name, self.mount_instance, self.next_commit_id
        );
        self.next_commit_id = self.next_commit_id.saturating_add(1);
        seed
    }

    pub(in crate::fs) fn write_to_handle_buffer(
        &mut self,
        fh: u64,
        offset: i64,
        data: &[u8],
    ) -> Result<usize, i32> {
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let refresh_commit_seed = self
            .handles
            .get(&fh)
            .map(|handle| handle.committed)
            .ok_or(libc::EBADF)?;
        let new_commit_seed = refresh_commit_seed.then(|| self.next_commit_seed());
        self.ensure_writes_allowed()?;
        let handle = self.handles.get_mut(&fh).ok_or(libc::EBADF)?;
        if let Some(commit_seed) = new_commit_seed {
            handle.commit_seed = commit_seed;
        }
        if !handle.writable {
            return Err(libc::EBADF);
        }
        let absolute_start = offset as usize;
        let start = match handle.mode {
            HandleMode::WholeValue => absolute_start,
            HandleMode::JsonLines => absolute_start
                .checked_sub(handle.base_offset)
                .ok_or(libc::EINVAL)?,
        };
        if handle.buffer.len() < start {
            handle.buffer.resize(start, 0);
        }
        let end = start.checked_add(data.len()).ok_or(libc::EINVAL)?;
        if handle.buffer.len() < end {
            handle.buffer.resize(end, 0);
        }
        handle.buffer[start..end].copy_from_slice(data);
        handle.dirty = true;
        handle.committed = false;
        Ok(data.len())
    }

    pub(in crate::fs) fn stage_truncate(&mut self, fh: u64) -> Result<(), i32> {
        if self
            .handles
            .get(&fh)
            .map(|handle| handle.mode == HandleMode::JsonLines)
            .ok_or(libc::EBADF)?
        {
            return Err(libc::EROFS);
        }
        let refresh_commit_seed = self
            .handles
            .get(&fh)
            .map(|handle| handle.committed)
            .ok_or(libc::EBADF)?;
        let new_commit_seed = refresh_commit_seed.then(|| self.next_commit_seed());
        self.ensure_writes_allowed()?;
        let handle = self.handles.get_mut(&fh).ok_or(libc::EBADF)?;
        if let Some(commit_seed) = new_commit_seed {
            handle.commit_seed = commit_seed;
        }
        if !handle.writable {
            return Err(libc::EBADF);
        }
        handle.buffer.clear();
        handle.base_offset = 0;
        handle.dirty = handle.mode == HandleMode::WholeValue;
        handle.committed = false;
        Ok(())
    }

    pub(in crate::fs) fn reject_append_only_truncate(
        &mut self,
        paths: &mut PathCache,
        path: &MountPath,
    ) -> Result<(), i32> {
        if self.write_mode_for_path(paths, path)? == HandleMode::JsonLines {
            Err(libc::EROFS)
        } else {
            Ok(())
        }
    }

    pub(in crate::fs) fn commit_handle(
        &mut self,
        paths: &mut PathCache,
        fh: u64,
    ) -> Result<(), i32> {
        let Some(handle) = self.handles.get_mut(&fh) else {
            return Ok(());
        };
        if !handle.writable || !handle.dirty || handle.committed {
            return Ok(());
        }
        let path = handle.path.clone();
        let bytes = handle_commit_bytes(handle);
        let commit_seed = handle.commit_seed.clone();
        let mode = handle.mode;
        self.commit_mount_path_with_seed(paths, &path, &bytes, &commit_seed)?;
        if let Some(handle) = self.handles.get_mut(&fh) {
            if mode == HandleMode::JsonLines {
                handle.base_offset = handle.base_offset.saturating_add(bytes.len());
                handle.buffer.clear();
            } else {
                handle.buffer = bytes.clone();
            }
            handle.dirty = false;
            handle.committed = true;
        }
        Ok(())
    }

    pub(in crate::fs) fn flush_handle(&mut self, fh: u64) -> Result<(), i32> {
        self.handles
            .contains_key(&fh)
            .then_some(())
            .ok_or(libc::EBADF)
    }

    pub(in crate::fs) fn release_handle(
        &mut self,
        paths: &mut PathCache,
        fh: u64,
    ) -> Result<(), i32> {
        let result = self.commit_handle(paths, fh);
        self.handles.remove(&fh);
        result
    }

    fn commit_mount_path_with_seed(
        &mut self,
        paths: &mut PathCache,
        path: &MountPath,
        bytes: &[u8],
        commit_seed: &str,
    ) -> Result<(), i32> {
        self.ensure_writes_allowed()?;
        let parsed = self.parse_path(path)?;
        let action = plan_operation(FileIntent::Write, &parsed).map_err(|err| err.errno().get())?;
        self.validate_payload(&action, path, bytes)?;
        let idempotency_key = idempotency_key(path.as_str(), bytes, commit_seed);
        let jsonl_base = self.jsonl_base_snapshot(&parsed, &action);
        let result = self.execute_write_action(&action, bytes, &idempotency_key);
        match result {
            Ok(()) => {
                self.refresh_cache_after_write(paths, path, &action, bytes);
                Ok(())
            }
            Err(err) => {
                self.record_error(format!("commit queued for {path}: {err}"));
                let queued = self
                    .enqueue_action(path, action, bytes, idempotency_key, jsonl_base.as_deref())
                    .map_err(|err| self.io_error(err));
                if queued.is_ok() {
                    self.invalidate_after_local_write(paths, path);
                    self.sync_writeback_gate(
                        "writeback queue has pending entries after a queued write; mount is read-only",
                    );
                }
                queued
            }
        }
    }

    fn refresh_cache_after_write(
        &mut self,
        paths: &mut PathCache,
        path: &MountPath,
        action: &JetStreamAction,
        bytes: &[u8],
    ) {
        self.invalidate_after_local_write(paths, path);
        match self.cache_entry_after_write(path, action, bytes) {
            Ok(Some(entry)) => self.cache.insert(entry),
            Ok(None) => {}
            Err(err) => self.record_error(format!(
                "cache refresh after write failed for {path}: {err}"
            )),
        }
    }

    fn cache_entry_after_write(
        &self,
        path: &MountPath,
        action: &JetStreamAction,
        bytes: &[u8],
    ) -> TransportResult<Option<CacheEntry>> {
        match action {
            JetStreamAction::KvPut { bucket, key } => {
                Ok(self.backend.kv_get(bucket, key)?.map(|entry| CacheEntry {
                    path: path.as_str().into(),
                    origin: CacheOrigin::KvRevision {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        revision: entry.revision,
                    },
                    version: VersionStamp::at(entry.created),
                    bytes: entry.bytes,
                    valid: true,
                }))
            }
            JetStreamAction::ObjectPut { bucket, object } => Ok(self
                .backend
                .object_metadata(bucket, object)?
                .map(|metadata| self.object_cache_entry(path, bucket, object, metadata, bytes))),
            JetStreamAction::MaterializedPut {
                target: MaterializedTarget::Kv { bucket, key },
            } => Ok(self.backend.kv_get(bucket, key)?.map(|entry| CacheEntry {
                path: path.as_str().into(),
                origin: CacheOrigin::MaterializedView {
                    path: path.as_str().into(),
                },
                version: VersionStamp::at(entry.created),
                bytes: entry.bytes,
                valid: true,
            })),
            JetStreamAction::PublishJsonLines { .. }
            | JetStreamAction::MaterializedPut {
                target: MaterializedTarget::Stream { .. },
            } => Ok(None),
            _ => Ok(None),
        }
    }

    fn object_cache_entry(
        &self,
        path: &MountPath,
        bucket: &str,
        object: &str,
        metadata: ObjectMetadata,
        bytes: &[u8],
    ) -> CacheEntry {
        CacheEntry {
            path: path.as_str().into(),
            origin: CacheOrigin::ObjectVersion {
                bucket: bucket.into(),
                object: object.into(),
            },
            version: VersionStamp::at(metadata.modified),
            bytes: bytes.to_vec(),
            valid: true,
        }
    }

    fn execute_write_action(
        &self,
        action: &JetStreamAction,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<()> {
        match action {
            JetStreamAction::KvPut { bucket, key } => {
                self.backend
                    .kv_put_idempotent(bucket, key, bytes, idempotency_key)?;
            }
            JetStreamAction::ObjectPut { bucket, object } => {
                self.backend
                    .object_put_idempotent(bucket, object, bytes, idempotency_key)?;
            }
            JetStreamAction::PublishJsonLines { stream, subject } => {
                self.backend
                    .publish_json_lines(stream, subject, bytes, idempotency_key)?;
            }
            JetStreamAction::MaterializedPut { target } => match target {
                MaterializedTarget::Kv { bucket, key } => {
                    validate_json_document(key, bytes).map_err(|err| {
                        eventfs_transport::TransportError::Invalid(err.to_string())
                    })?;
                    self.backend
                        .kv_put_idempotent(bucket, key, bytes, idempotency_key)?;
                }
                MaterializedTarget::Stream { stream, subject } => {
                    self.backend
                        .publish_json_lines(stream, subject, bytes, idempotency_key)?;
                }
            },
            _ => {
                return Err(eventfs_transport::TransportError::Invalid(
                    "not a write".into(),
                ))
            }
        }
        Ok(())
    }

    fn validate_payload(
        &self,
        action: &JetStreamAction,
        path: &MountPath,
        bytes: &[u8],
    ) -> Result<(), i32> {
        match action {
            JetStreamAction::PublishJsonLines { .. } => {
                validate_json_lines(path.as_str(), bytes).map_err(|err| err.errno().get())
            }
            JetStreamAction::MaterializedPut {
                target: MaterializedTarget::Stream { .. },
            } => validate_json_lines(path.as_str(), bytes).map_err(|err| err.errno().get()),
            JetStreamAction::MaterializedPut {
                target: MaterializedTarget::Kv { .. },
            } => validate_json_document(path.as_str(), bytes).map_err(|err| err.errno().get()),
            _ => Ok(()),
        }
    }

    fn jsonl_base_snapshot(
        &mut self,
        parsed: &JetStreamPath,
        action: &JetStreamAction,
    ) -> Option<Vec<u8>> {
        if !action_publishes_jsonl(action) {
            return None;
        }
        let (stream, subject) = self.jsonl_target_for_path(parsed)?;
        match self.read_stream_subject_snapshot_allow_missing(&stream, &subject) {
            Ok(snapshot) => Some(snapshot.bytes),
            Err(errno) if directory_listing_unavailable(errno) || errno == libc::ENOENT => {
                Some(Vec::new())
            }
            Err(errno) => {
                self.record_error(format!(
                    "jsonl base snapshot unavailable for {stream}:{subject}: errno {errno}"
                ));
                None
            }
        }
    }

    fn applied_jsonl_lines_for_queue(
        &mut self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        base: Option<&[u8]>,
    ) -> usize {
        let Some(base) = base else {
            return 0;
        };
        let current = match self.read_stream_subject_snapshot_allow_missing(stream, subject) {
            Ok(snapshot) => snapshot.bytes,
            Err(errno) if directory_listing_unavailable(errno) || errno == libc::ENOENT => {
                Vec::new()
            }
            Err(errno) => {
                self.record_error(format!(
                    "jsonl progress snapshot unavailable for {stream}:{subject}: errno {errno}"
                ));
                return 0;
            }
        };
        jsonl_applied_prefix_count(subject, base, &current, bytes).unwrap_or(0)
    }

    fn enqueue_action(
        &mut self,
        path: &MountPath,
        action: JetStreamAction,
        bytes: &[u8],
        idempotency_key: String,
        jsonl_base: Option<&[u8]>,
    ) -> TransportResult<()> {
        let operation = match action {
            JetStreamAction::KvPut { bucket, key } => FailedWriteOperation::KvPut {
                bucket,
                key,
                bytes: bytes.to_vec(),
            },
            JetStreamAction::ObjectPut { bucket, object } => FailedWriteOperation::ObjectPut {
                bucket,
                object,
                bytes: bytes.to_vec(),
            },
            JetStreamAction::PublishJsonLines { stream, subject } => {
                let applied_lines =
                    self.applied_jsonl_lines_for_queue(&stream, &subject, bytes, jsonl_base);
                FailedWriteOperation::PublishJsonLines {
                    stream,
                    subject,
                    bytes: bytes.to_vec(),
                    applied_lines,
                }
            }
            JetStreamAction::MaterializedPut { target } => match target {
                MaterializedTarget::Kv { bucket, key } => FailedWriteOperation::MaterializedPut {
                    bucket,
                    key,
                    bytes: bytes.to_vec(),
                },
                MaterializedTarget::Stream { stream, subject } => {
                    let applied_lines =
                        self.applied_jsonl_lines_for_queue(&stream, &subject, bytes, jsonl_base);
                    FailedWriteOperation::PublishJsonLines {
                        stream,
                        subject,
                        bytes: bytes.to_vec(),
                        applied_lines,
                    }
                }
            },
            _ => return Ok(()),
        };
        self.replay.enqueue_failed_write(FailedWrite::new(
            idempotency_key,
            VersionStamp::at(SystemTime::now()),
            path.clone(),
            operation,
        ))?;
        Ok(())
    }

    fn durable_jsonl_applied_prefix(&mut self, entry: &QueuedJsonlEntry) -> usize {
        match self.backend.publish_json_lines_applied_prefix(
            &entry.stream,
            &entry.subject,
            &entry.bytes,
            &entry.idempotency_key,
        ) {
            Ok(applied) => applied,
            Err(err) => {
                self.record_error(format!(
                    "jsonl durable progress unavailable for {}:{}: {err}",
                    entry.stream, entry.subject
                ));
                0
            }
        }
    }

    fn jsonl_target_for_path(&self, parsed: &JetStreamPath) -> Option<(String, String)> {
        match parsed {
            JetStreamPath::StreamSubject { stream, subject } => {
                Some((stream.clone(), subject.as_str().to_string()))
            }
            _ => MaterializedTarget::from_path(parsed).and_then(|target| match target {
                MaterializedTarget::Stream { stream, subject } => Some((stream, subject)),
                MaterializedTarget::Kv { .. } => None,
            }),
        }
    }

    fn exact_dynamic_file_exists(&mut self, parsed: &JetStreamPath) -> Result<bool, i32> {
        match parsed {
            JetStreamPath::KvKey { bucket, key } => self
                .backend
                .kv_get(bucket, key)
                .map(|entry| entry.is_some())
                .map_err(|err| self.io_error(err)),
            JetStreamPath::Object { bucket, object } => self
                .object_metadata(bucket, object)
                .map(|entry| entry.is_some()),
            JetStreamPath::AgentRecord { .. } | JetStreamPath::SemanticRecord { .. } => {
                match MaterializedTarget::from_path(parsed) {
                    Some(MaterializedTarget::Kv { bucket, key }) => self
                        .backend
                        .kv_get(&bucket, &key)
                        .map(|entry| entry.is_some())
                        .map_err(|err| self.io_error(err)),
                    Some(MaterializedTarget::Stream { .. }) | None => Ok(false),
                }
            }
            _ => Ok(false),
        }
    }

    #[cfg(test)]
    pub(in crate::fs) fn commit_path(
        &mut self,
        paths: &mut PathCache,
        path: &MountPath,
        bytes: &[u8],
    ) -> Result<(), i32> {
        let commit_seed = self.next_commit_seed();
        self.commit_mount_path_with_seed(paths, path, bytes, &commit_seed)
    }

    pub(in crate::fs) fn execute_unlink(
        &mut self,
        paths: &mut PathCache,
        path: &MountPath,
    ) -> Result<(), i32> {
        self.ensure_writes_allowed()?;
        let parsed = self.parse_path(path)?;
        let action =
            plan_operation(FileIntent::Unlink, &parsed).map_err(|err| err.errno().get())?;
        let result = match action {
            JetStreamAction::KvDelete { bucket, key } => self
                .backend
                .kv_delete(&bucket, &key)
                .map_err(|err| self.io_error(err)),
            JetStreamAction::ObjectDelete { bucket, object } => self
                .backend
                .object_delete(&bucket, &object)
                .map_err(|err| self.io_error(err)),
            JetStreamAction::MaterializedDelete { target } => match target {
                MaterializedTarget::Kv { bucket, key } => self
                    .backend
                    .kv_delete(&bucket, &key)
                    .map_err(|err| self.io_error(err)),
                MaterializedTarget::Stream { .. } => Err(libc::EROFS),
            },
            _ => Err(libc::ENOTSUP),
        };
        if result.is_ok() {
            self.invalidate_after_local_unlink(paths, path);
        }
        result
    }

    pub(in crate::fs) fn execute_mkdir(
        &mut self,
        _paths: &mut PathCache,
        path: &MountPath,
    ) -> Result<(), i32> {
        self.ensure_writes_allowed()?;
        let parsed = self.parse_path(path)?;
        let action = plan_operation(FileIntent::Mkdir, &parsed).map_err(|err| err.errno().get())?;
        match action {
            JetStreamAction::EnsureKvBucket { bucket } => self
                .backend
                .ensure_kv_bucket(&bucket)
                .map_err(|err| self.io_error(err)),
            JetStreamAction::EnsureKvDirectory { bucket, prefix } => {
                self.ensure_kv_synthetic_directory(&bucket, &prefix)
            }
            JetStreamAction::EnsureStream { stream } => self
                .backend
                .ensure_stream(&stream)
                .map_err(|err| self.io_error(err)),
            JetStreamAction::EnsureObjectBucket { bucket } => self
                .backend
                .ensure_object_bucket(&bucket)
                .map_err(|err| self.io_error(err)),
            JetStreamAction::EnsureObjectDirectory { bucket, prefix } => {
                self.ensure_object_synthetic_directory(&bucket, &prefix)
            }
            JetStreamAction::NoopDirectory => Ok(()),
            _ => Err(libc::ENOTSUP),
        }
    }

    pub(in crate::fs) fn execute_rename(
        &mut self,
        paths: &mut PathCache,
        from_path: &MountPath,
        to_path: &MountPath,
        flags: u32,
    ) -> Result<(), i32> {
        self.ensure_writes_allowed()?;
        if flags != 0 {
            return Err(libc::EINVAL);
        }
        if from_path == to_path {
            return Ok(());
        }
        let from = self.parse_path(from_path)?;
        let to = self.parse_path(to_path)?;
        let action =
            plan_operation(FileIntent::Rename { to }, &from).map_err(|err| err.errno().get())?;
        let JetStreamAction::Rename { plan } = action else {
            return Err(libc::ENOTSUP);
        };
        match plan {
            RenamePlan::Kv {
                from_bucket,
                from_key,
                to_bucket,
                to_key,
            } => {
                let entry = self.kv_rename_source(&from_bucket, &from_key)?;
                self.ensure_kv_rename_destination(&to_bucket, &to_key)?;
                let seed = self.next_commit_seed();
                self.queue_kv_rename_completion(KvRenameCompletion {
                    from_path: from_path.clone(),
                    to_path: to_path.clone(),
                    from_bucket,
                    from_key,
                    expected_from_revision: entry.revision,
                    to_bucket,
                    to_key,
                    bytes: entry.bytes,
                    seed,
                })?;
            }
            RenamePlan::Object {
                from_bucket,
                from_object,
                to_bucket,
                to_object,
            } => {
                let object = self.object_rename_source(&from_bucket, &from_object)?;
                self.ensure_object_rename_destination(&to_bucket, &to_object)?;
                let seed = self.next_commit_seed();
                self.queue_object_rename_completion(ObjectRenameCompletion {
                    from_path: from_path.clone(),
                    to_path: to_path.clone(),
                    from_bucket,
                    from_object,
                    expected_from_sequence: object.sequence,
                    expected_from_nuid: object.nuid,
                    to_bucket,
                    to_object,
                    bytes: object.bytes,
                    seed,
                })?;
            }
        }
        self.replay_queue(paths);
        self.invalidate_after_local_unlink(paths, from_path);
        self.invalidate_after_local_write(paths, to_path);
        Ok(())
    }

    fn kv_rename_source(&mut self, bucket: &str, key: &str) -> Result<KeyRevision, i32> {
        match self.classify_kv_dynamic_path(bucket, key)? {
            DynamicPathKind::Missing => Err(libc::ENOENT),
            DynamicPathKind::Directory => Err(libc::EISDIR),
            DynamicPathKind::File => self
                .backend
                .kv_get(bucket, key)
                .map_err(|err| self.io_error(err))?
                .ok_or(libc::ENOENT),
        }
    }

    fn object_rename_source(&mut self, bucket: &str, object: &str) -> Result<ObjectVersion, i32> {
        match self.classify_object_dynamic_path(bucket, object)? {
            DynamicPathKind::Missing => Err(libc::ENOENT),
            DynamicPathKind::Directory => Err(libc::EISDIR),
            DynamicPathKind::File => self
                .backend
                .object_get(bucket, object)
                .map_err(|err| self.io_error(err))?
                .ok_or(libc::ENOENT),
        }
    }

    fn queue_kv_rename_completion(&mut self, completion: KvRenameCompletion) -> Result<(), i32> {
        let idempotency_key = idempotency_key(
            completion.to_path.as_str(),
            &rename_completion_key_material(
                completion.from_path.as_str(),
                completion.expected_from_revision,
                &completion.bytes,
            ),
            &completion.seed,
        );
        let version = VersionStamp::at(SystemTime::now());
        self.replay
            .enqueue_failed_write(FailedWrite::new(
                idempotency_key,
                version,
                completion.to_path,
                FailedWriteOperation::KvRenameComplete {
                    from_bucket: completion.from_bucket,
                    from_key: completion.from_key,
                    source: KvSourceGeneration {
                        revision: completion.expected_from_revision,
                    },
                    to_bucket: completion.to_bucket,
                    to_key: completion.to_key,
                    bytes: completion.bytes,
                },
            ))
            .map_err(|err| self.io_error(err))?;
        Ok(())
    }

    fn queue_object_rename_completion(
        &mut self,
        completion: ObjectRenameCompletion,
    ) -> Result<(), i32> {
        let idempotency_key = idempotency_key(
            completion.to_path.as_str(),
            &rename_completion_key_material(
                completion.from_path.as_str(),
                completion.expected_from_sequence,
                &completion.bytes,
            ),
            &completion.seed,
        );
        let version = VersionStamp::at(SystemTime::now());
        self.replay
            .enqueue_failed_write(FailedWrite::new(
                idempotency_key,
                version,
                completion.to_path,
                FailedWriteOperation::ObjectRenameComplete {
                    from_bucket: completion.from_bucket,
                    from_object: completion.from_object,
                    source: ObjectSourceGeneration {
                        sequence: completion.expected_from_sequence,
                        nuid: completion.expected_from_nuid,
                    },
                    to_bucket: completion.to_bucket,
                    to_object: completion.to_object,
                    bytes: completion.bytes,
                },
            ))
            .map_err(|err| self.io_error(err))?;
        Ok(())
    }

    fn ensure_kv_synthetic_directory(&mut self, bucket: &str, prefix: &str) -> Result<(), i32> {
        match self.classify_kv_dynamic_path(bucket, prefix)? {
            DynamicPathKind::File => Err(libc::EEXIST),
            DynamicPathKind::Missing => Err(libc::ENOENT),
            DynamicPathKind::Directory => Ok(()),
        }
    }

    fn ensure_object_synthetic_directory(&mut self, bucket: &str, prefix: &str) -> Result<(), i32> {
        match self.classify_object_dynamic_path(bucket, prefix)? {
            DynamicPathKind::File => Err(libc::EEXIST),
            DynamicPathKind::Missing => Err(libc::ENOENT),
            DynamicPathKind::Directory => Ok(()),
        }
    }

    fn ensure_kv_rename_destination(&mut self, bucket: &str, key: &str) -> Result<(), i32> {
        match self.classify_kv_dynamic_path(bucket, key)? {
            DynamicPathKind::Directory => Err(libc::EISDIR),
            DynamicPathKind::File | DynamicPathKind::Missing => Ok(()),
        }
    }

    fn ensure_object_rename_destination(&mut self, bucket: &str, object: &str) -> Result<(), i32> {
        match self.classify_object_dynamic_path(bucket, object)? {
            DynamicPathKind::Directory => Err(libc::EISDIR),
            DynamicPathKind::File | DynamicPathKind::Missing => Ok(()),
        }
    }

    pub(in crate::fs) fn execute_mknod(
        &mut self,
        paths: &mut PathCache,
        path: &MountPath,
        mode: u32,
    ) -> Result<u64, i32> {
        self.ensure_writes_allowed()?;
        if mode & libc::S_IFMT != libc::S_IFREG {
            return Err(libc::ENOTSUP);
        }
        let payload = self
            .mount_projection(paths)
            .create_target(path)?
            .into_default_payload();
        let commit_seed = self.next_commit_seed();
        self.commit_mount_path_with_seed(paths, path, &payload, &commit_seed)?;
        Ok(payload.len() as u64)
    }

    fn io_error(&mut self, err: eventfs_transport::TransportError) -> i32 {
        if matches!(err, eventfs_transport::TransportError::NotFound) {
            return libc::ENOENT;
        }
        self.record_error(err.to_string());
        libc::EIO
    }

    #[cfg(test)]
    pub(in crate::fs) fn pending_write_count(&self) -> usize {
        self.replay.state().pending
    }

    #[cfg(test)]
    pub(in crate::fs) fn pending_overlays(&self) -> Vec<eventfs_transport::PendingWriteOverlay> {
        self.replay.pending_overlays()
    }

    #[cfg(test)]
    pub(in crate::fs) fn cache_contains(&self, path: &str) -> bool {
        self.cache.get(path).is_some()
    }

    #[cfg(test)]
    pub(in crate::fs) fn cache_is_empty(&self) -> bool {
        self.cache.snapshot().is_empty()
    }

    #[cfg(test)]
    pub(in crate::fs) fn stale_paths_is_empty(&self) -> bool {
        self.stale_paths.is_empty()
    }

    #[cfg(test)]
    pub(in crate::fs) fn stale_paths_contains(&self, path: &MountPath) -> bool {
        self.stale_paths.contains(path)
    }

    #[cfg(test)]
    pub(in crate::fs) fn handle_committed(&self, fh: u64) -> Option<bool> {
        self.handles.get(&fh).map(|handle| handle.committed)
    }

    #[cfg(test)]
    pub(in crate::fs) fn handle_probe(&self, fh: u64) -> Option<RuntimeHandleProbe> {
        self.handles.get(&fh).map(|handle| RuntimeHandleProbe {
            mode: handle.mode,
            base_offset: handle.base_offset,
            buffer: handle.buffer.clone(),
            staged_create: handle.staged_create.is_some(),
            committed: handle.committed,
        })
    }

    #[cfg(test)]
    pub(in crate::fs) fn set_mounted_at_for_test(&mut self, mounted_at: SystemTime) {
        self.mounted_at = mounted_at;
    }

    #[cfg(test)]
    pub(in crate::fs) fn enqueue_failed_write_for_test(
        &mut self,
        write: FailedWrite,
    ) -> TransportResult<()> {
        self.replay.enqueue_failed_write(write).map(|_| ())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeEntryKind {
    Directory,
    File,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeAttr {
    pub kind: RuntimeEntryKind,
    pub size: u64,
    pub version: VersionStamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeDirectoryEntry {
    pub name: String,
    pub kind: RuntimeEntryKind,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RuntimeHandleId(u64);

impl RuntimeHandleId {
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeOpenOptions {
    pub writable: bool,
    pub truncate: bool,
    pub create: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeMutation {
    pub path: MountPath,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeError {
    errno: i32,
}

impl RuntimeError {
    pub fn errno(self) -> i32 {
        self.errno
    }
}

impl From<i32> for RuntimeError {
    fn from(errno: i32) -> Self {
        Self { errno }
    }
}

pub(crate) type RuntimeResult<T> = Result<T, RuntimeError>;

pub(crate) struct MountRuntime<'a> {
    state: &'a mut MountRuntimeState,
    paths: &'a mut PathCache,
}

impl<'a> MountRuntime<'a> {
    pub(super) fn new(state: &'a mut MountRuntimeState, paths: &'a mut PathCache) -> Self {
        Self { state, paths }
    }

    pub(crate) fn stat(&mut self, path: &MountPath) -> RuntimeResult<RuntimeAttr> {
        let attr = self.state.mount_projection(self.paths).attr(path)?;
        Ok(RuntimeAttr {
            kind: runtime_kind(attr.kind),
            size: attr.size,
            version: attr.version,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn read(&mut self, path: &MountPath) -> RuntimeResult<Vec<u8>> {
        self.state
            .mount_projection(self.paths)
            .read_bytes(path)
            .map_err(Into::into)
    }

    pub(crate) fn read_handle(
        &mut self,
        handle: RuntimeHandleId,
        path: &MountPath,
    ) -> RuntimeResult<Vec<u8>> {
        let parsed = self.state.parse_path(path)?;
        self.state
            .read_bytes_for_handle(self.paths, handle.raw(), path, &parsed)
            .map_err(Into::into)
    }

    pub(crate) fn list(&mut self, path: &MountPath) -> RuntimeResult<Vec<RuntimeDirectoryEntry>> {
        self.state
            .mount_projection(self.paths)
            .directory_entries(path)?
            .into_iter()
            .map(|entry| {
                Ok(RuntimeDirectoryEntry {
                    name: entry.name,
                    kind: runtime_kind(entry.kind),
                })
            })
            .collect()
    }

    #[allow(dead_code)]
    pub(crate) fn metadata(&self, file: MetadataFile) -> Vec<u8> {
        self.state.metadata_bytes(self.paths, file)
    }

    pub(crate) fn open(
        &mut self,
        path: &MountPath,
        options: RuntimeOpenOptions,
    ) -> RuntimeResult<RuntimeHandleId> {
        self.state
            .open_handle(
                self.paths,
                path,
                options.writable,
                options.truncate,
                options.create,
            )
            .map(RuntimeHandleId)
            .map_err(Into::into)
    }

    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn created_handle_attr(
        &mut self,
        handle: RuntimeHandleId,
        path: &MountPath,
    ) -> RuntimeResult<RuntimeAttr> {
        let handle = self
            .state
            .handles
            .get(&handle.raw())
            .ok_or(RuntimeError::from(libc::EBADF))?;
        if handle.path != *path {
            return Err(RuntimeError::from(libc::EINVAL));
        }
        if handle.mode == HandleMode::WholeValue {
            if let Some(staged) = &handle.staged_create {
                return Ok(RuntimeAttr {
                    kind: RuntimeEntryKind::File,
                    size: handle.buffer.len() as u64,
                    version: staged.version,
                });
            }
        }
        self.stat(path)
    }

    pub(crate) fn write(
        &mut self,
        handle: RuntimeHandleId,
        offset: i64,
        data: &[u8],
    ) -> RuntimeResult<usize> {
        self.state
            .write_to_handle_buffer(handle.raw(), offset, data)
            .map_err(Into::into)
    }

    pub(crate) fn truncate_handle(&mut self, handle: RuntimeHandleId) -> RuntimeResult<()> {
        self.state.stage_truncate(handle.raw()).map_err(Into::into)
    }

    pub(crate) fn truncate_path(&mut self, path: &MountPath) -> RuntimeResult<()> {
        self.state.reject_append_only_truncate(self.paths, path)?;
        let commit_seed = self.state.next_commit_seed();
        self.state
            .commit_mount_path_with_seed(self.paths, path, &[], &commit_seed)
            .map_err(Into::into)
    }

    pub(crate) fn writable_handle(
        &mut self,
        raw_handle: u64,
        path: &MountPath,
    ) -> RuntimeResult<RuntimeHandleId> {
        if !self.state.handles.contains_key(&raw_handle) {
            let handle = self
                .state
                .build_handle(self.paths, path.clone(), true, false, false)?;
            self.state.handles.insert(raw_handle, handle);
        }
        Ok(RuntimeHandleId::from_raw(raw_handle))
    }

    pub(crate) fn commit(&mut self, handle: RuntimeHandleId) -> RuntimeResult<()> {
        self.state
            .commit_handle(self.paths, handle.raw())
            .map_err(Into::into)
    }

    pub(crate) fn flush(&mut self, handle: RuntimeHandleId) -> RuntimeResult<()> {
        self.state.flush_handle(handle.raw()).map_err(Into::into)
    }

    pub(crate) fn release(&mut self, handle: RuntimeHandleId) -> RuntimeResult<()> {
        self.state
            .release_handle(self.paths, handle.raw())
            .map_err(Into::into)
    }

    pub(crate) fn discard(&mut self, handle: RuntimeHandleId) {
        self.state.handles.remove(&handle.raw());
    }

    pub(crate) fn mkdir(&mut self, path: &MountPath) -> RuntimeResult<RuntimeMutation> {
        self.state.execute_mkdir(self.paths, path)?;
        Ok(RuntimeMutation { path: path.clone() })
    }

    pub(crate) fn mknod(&mut self, path: &MountPath, mode: u32) -> RuntimeResult<RuntimeMutation> {
        self.state.execute_mknod(self.paths, path, mode)?;
        Ok(RuntimeMutation { path: path.clone() })
    }

    pub(crate) fn unlink(&mut self, path: &MountPath) -> RuntimeResult<RuntimeMutation> {
        self.state.execute_unlink(self.paths, path)?;
        Ok(RuntimeMutation { path: path.clone() })
    }

    pub(crate) fn rename(
        &mut self,
        from_path: &MountPath,
        to_path: &MountPath,
        flags: u32,
    ) -> RuntimeResult<RuntimeMutation> {
        self.state
            .execute_rename(self.paths, from_path, to_path, flags)?;
        Ok(RuntimeMutation {
            path: to_path.clone(),
        })
    }
}

fn runtime_kind(kind: MountProjectionKind) -> RuntimeEntryKind {
    match kind {
        MountProjectionKind::Directory => RuntimeEntryKind::Directory,
        MountProjectionKind::File => RuntimeEntryKind::File,
    }
}
