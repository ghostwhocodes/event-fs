use std::collections::HashSet;

use eventfs_protocol::{
    plan_operation, FileIntent, JetStreamAction, JetStreamPath, MaterializedTarget, MountPath,
    StaticEntryKind, AGENTS_BUCKET, SEMANTIC_BUCKET, TASKS_BUCKET,
};
use eventfs_transport::{CacheEntry, CacheOrigin, DirectoryEntry, EntryKind, VersionStamp};

use crate::path_cache::PathCache;

use super::super::queued_overlay::QueuedOverlayProjection;
use super::super::{
    action_publishes_jsonl, directory_entries_contain_exact_file, directory_listing_unavailable,
    insert_directory_entry, insert_directory_entry_sorted, max_version_stamp, names_as_dirs,
    names_as_event_files, projection_kind_from_file_type, unapplied_jsonl_bytes, DynamicPathKind,
    FileMetadata, FileSnapshot, MountPathInput, QueuedJsonlEntry,
};
use super::MountRuntimeState;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::fs) enum MountProjectionKind {
    Directory,
    File,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct MountProjectionAttr {
    pub kind: MountProjectionKind,
    pub size: u64,
    pub version: VersionStamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MountProjectionEntry {
    pub name: String,
    pub kind: MountProjectionKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::fs) enum MountProjectionWriteMode {
    WholeValue,
    JsonLines,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum MountProjectionMutation {
    CreateFile { default_payload: Vec<u8> },
}

impl MountProjectionMutation {
    pub fn into_default_payload(self) -> Vec<u8> {
        match self {
            Self::CreateFile { default_payload } => default_payload,
        }
    }
}

pub(super) struct MountProjection<'a> {
    state: &'a mut MountRuntimeState,
    paths: &'a mut PathCache,
}

impl MountRuntimeState {
    pub(super) fn mount_projection<'a>(
        &'a mut self,
        paths: &'a mut PathCache,
    ) -> MountProjection<'a> {
        MountProjection { state: self, paths }
    }
}

impl MountProjection<'_> {
    fn queued_overlay(&self) -> QueuedOverlayProjection {
        QueuedOverlayProjection::new(self.state.replay.pending_overlays())
    }

    pub(super) fn kind_for_parsed_kind(
        &mut self,
        path: &MountPath,
        parsed: &eventfs_protocol::JetStreamPath,
    ) -> Result<MountProjectionKind, i32> {
        self.kind_for_parsed(path, parsed)
    }

    pub(crate) fn attr(&mut self, path: impl MountPathInput) -> Result<MountProjectionAttr, i32> {
        let path = path.into_mount_path()?;
        self.state.apply_pending_watch_events(self.paths);
        let parsed = self.state.parse_path(&path)?;
        let kind = self.kind_for_parsed_kind(&path, &parsed)?;
        let metadata = if kind == MountProjectionKind::Directory {
            None
        } else {
            Some(self.attr_metadata(&path, &parsed)?)
        };
        Ok(MountProjectionAttr {
            kind,
            size: metadata.as_ref().map_or(0, |file| file.size),
            version: metadata
                .map(|file| file.version)
                .or_else(|| self.queued_overlay_version(&path))
                .unwrap_or_else(|| self.state.mount_version()),
        })
    }

    pub(crate) fn read_bytes(&mut self, path: impl MountPathInput) -> Result<Vec<u8>, i32> {
        let path = path.into_mount_path()?;
        self.state.apply_pending_watch_events(self.paths);
        let parsed = self.state.parse_path(&path)?;
        self.read_snapshot(&path, &parsed)
            .map(|snapshot| snapshot.bytes)
    }

    pub(crate) fn directory_entries(
        &mut self,
        path: impl MountPathInput,
    ) -> Result<Vec<MountProjectionEntry>, i32> {
        let path = path.into_mount_path()?;
        self.state.apply_pending_watch_events(self.paths);
        let parsed = self.state.parse_path(&path)?;
        let entries = self.durable_directory_entries(&parsed);
        self.merge_queued_directory_entries(&parsed, &path, entries)
            .map(|entries| entries.into_iter().map(project_entry).collect())
    }

    pub(crate) fn write_mode(
        &mut self,
        path: impl MountPathInput,
    ) -> Result<MountProjectionWriteMode, i32> {
        let path = path.into_mount_path()?;
        let parsed = self.state.parse_path(&path)?;
        let action = plan_operation(FileIntent::Write, &parsed).map_err(|err| err.errno().get())?;
        Ok(match action {
            JetStreamAction::PublishJsonLines { .. }
            | JetStreamAction::MaterializedPut {
                target: MaterializedTarget::Stream { .. },
            } => MountProjectionWriteMode::JsonLines,
            _ => MountProjectionWriteMode::WholeValue,
        })
    }

    pub(crate) fn create_target(
        &mut self,
        path: impl MountPathInput,
    ) -> Result<MountProjectionMutation, i32> {
        let path = path.into_mount_path()?;
        self.state.apply_pending_watch_events(self.paths);
        let parsed = self.state.parse_path(&path)?;
        let action =
            plan_operation(FileIntent::Create, &parsed).map_err(|err| err.errno().get())?;
        if action_publishes_jsonl(&action) {
            return Err(libc::ENOTSUP);
        }
        match self.create_target_kind(&path, &parsed, &action)? {
            DynamicPathKind::Missing => Ok(MountProjectionMutation::CreateFile {
                default_payload: create_default_payload(&action),
            }),
            DynamicPathKind::File => Err(libc::EEXIST),
            DynamicPathKind::Directory => Err(libc::EISDIR),
        }
    }

    fn create_target_kind(
        &mut self,
        path: &MountPath,
        parsed: &JetStreamPath,
        action: &JetStreamAction,
    ) -> Result<DynamicPathKind, i32> {
        if let Some(kind) = self.queued_create_target_kind(path, parsed)? {
            return Ok(kind);
        }
        match action {
            JetStreamAction::KvPut { bucket, key } => {
                self.state.classify_kv_dynamic_path(bucket, key)
            }
            JetStreamAction::ObjectPut { bucket, object } => {
                self.state.classify_object_dynamic_path(bucket, object)
            }
            JetStreamAction::MaterializedPut {
                target: MaterializedTarget::Kv { bucket, key },
            } => self.materialized_kv_create_target_kind(bucket, key),
            _ => Ok(DynamicPathKind::Missing),
        }
    }

    fn queued_create_target_kind(
        &mut self,
        path: &MountPath,
        parsed: &JetStreamPath,
    ) -> Result<Option<DynamicPathKind>, i32> {
        if self.latest_queued_delete_covers(path) {
            return Ok(Some(
                if self.queued_exact_delete_reveals_synthetic_directory(path, parsed) {
                    DynamicPathKind::Directory
                } else {
                    DynamicPathKind::Missing
                },
            ));
        }
        if self.queued_whole_value_snapshot(path).is_some() || self.has_queued_jsonl_entry(path) {
            return Ok(Some(DynamicPathKind::File));
        }
        if !self.queued_directory_entries(path).is_empty() {
            return Ok(Some(DynamicPathKind::Directory));
        }
        if self.queued_deleted_descendant_empties_synthetic_directory(path, parsed)? {
            return Ok(Some(DynamicPathKind::Missing));
        }
        Ok(None)
    }

    fn materialized_kv_create_target_kind(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<DynamicPathKind, i32> {
        match self.state.classify_kv_dynamic_path(bucket, key) {
            Err(errno) if errno == libc::ENOENT => Ok(DynamicPathKind::Missing),
            result => result,
        }
    }

    fn kind_for_parsed(
        &mut self,
        path: &MountPath,
        parsed: &eventfs_protocol::JetStreamPath,
    ) -> Result<MountProjectionKind, i32> {
        if self.latest_queued_delete_covers(path) {
            if self.queued_exact_delete_reveals_synthetic_directory(path, parsed) {
                self.state.stale_paths.remove(path);
                return Ok(MountProjectionKind::Directory);
            }
            self.state.stale_paths.remove(path);
            return Err(libc::ENOENT);
        }
        if self.queued_whole_value_snapshot(path).is_some() {
            self.state.stale_paths.remove(path);
            return Ok(MountProjectionKind::File);
        }
        if self.has_queued_jsonl_entry(path) {
            self.state.stale_paths.remove(path);
            return Ok(MountProjectionKind::File);
        }
        if !self.queued_directory_entries(path).is_empty() {
            self.state.stale_paths.remove(path);
            return Ok(MountProjectionKind::Directory);
        }
        if self.queued_deleted_descendant_empties_synthetic_directory(path, parsed)? {
            self.state.stale_paths.remove(path);
            return Err(libc::ENOENT);
        }
        if !self.state.stale_paths.contains(path) {
            if let Some(kind) = cached_projection_kind(self.paths, path) {
                return Ok(kind);
            }
        }
        if self.state.cache.get(path.as_str()).is_some() {
            self.state.stale_paths.remove(path);
            return Ok(MountProjectionKind::File);
        }
        match parsed {
            eventfs_protocol::JetStreamPath::Root
            | eventfs_protocol::JetStreamPath::KvRoot
            | eventfs_protocol::JetStreamPath::StreamsRoot
            | eventfs_protocol::JetStreamPath::ObjectsRoot
            | eventfs_protocol::JetStreamPath::EventsRoot
            | eventfs_protocol::JetStreamPath::TasksRoot
            | eventfs_protocol::JetStreamPath::AgentsRoot
            | eventfs_protocol::JetStreamPath::SemanticRoot
            | eventfs_protocol::JetStreamPath::MetadataRoot => Ok(MountProjectionKind::Directory),
            eventfs_protocol::JetStreamPath::KvBucket { bucket }
            | eventfs_protocol::JetStreamPath::KvHistoryRoot { bucket } => {
                self.state.require_kv_bucket(bucket)?;
                Ok(MountProjectionKind::Directory)
            }
            eventfs_protocol::JetStreamPath::KvHistoryKey { bucket, key } => {
                let revisions = self
                    .state
                    .backend
                    .kv_history(bucket, key)
                    .map_err(|err| self.state.io_error(err))?;
                if !revisions.is_empty() {
                    return Ok(MountProjectionKind::Directory);
                }
                if self
                    .state
                    .backend
                    .list_kv_history_prefix(bucket, key)
                    .map_err(|err| self.state.io_error(err))?
                    .is_empty()
                {
                    Err(libc::ENOENT)
                } else {
                    Ok(MountProjectionKind::Directory)
                }
            }
            eventfs_protocol::JetStreamPath::StreamRoot { stream }
            | eventfs_protocol::JetStreamPath::StreamMessages { stream }
            | eventfs_protocol::JetStreamPath::StreamSubjects { stream } => {
                self.state.require_stream(stream)?;
                Ok(MountProjectionKind::Directory)
            }
            eventfs_protocol::JetStreamPath::StreamSubject { stream, subject } => {
                self.state.require_stream(stream)?;
                if self.state.stream_subject_exists(stream, subject.as_str())? {
                    Ok(MountProjectionKind::File)
                } else {
                    Err(libc::ENOENT)
                }
            }
            eventfs_protocol::JetStreamPath::ObjectBucket { bucket } => {
                self.state.require_object_bucket(bucket)?;
                Ok(MountProjectionKind::Directory)
            }
            eventfs_protocol::JetStreamPath::TaskNamespace { .. }
            | eventfs_protocol::JetStreamPath::AgentRoot { .. }
            | eventfs_protocol::JetStreamPath::AgentDirectory { .. }
            | eventfs_protocol::JetStreamPath::SemanticArea { .. } => {
                Ok(MountProjectionKind::Directory)
            }
            eventfs_protocol::JetStreamPath::KvKey { bucket, key } => {
                self.state.kv_prefix_projection_kind(bucket, key)
            }
            eventfs_protocol::JetStreamPath::AgentRecord { .. }
            | eventfs_protocol::JetStreamPath::SemanticRecord { .. } => {
                match eventfs_protocol::MaterializedTarget::from_path(parsed) {
                    Some(eventfs_protocol::MaterializedTarget::Kv { bucket, key }) => {
                        self.state.kv_prefix_projection_kind(&bucket, &key)
                    }
                    _ => Ok(MountProjectionKind::File),
                }
            }
            eventfs_protocol::JetStreamPath::Object { bucket, object } => {
                match self.state.classify_object_dynamic_path(bucket, object)? {
                    DynamicPathKind::Directory => Ok(MountProjectionKind::Directory),
                    DynamicPathKind::File | DynamicPathKind::Missing => {
                        Ok(MountProjectionKind::File)
                    }
                }
            }
            _ => Ok(MountProjectionKind::File),
        }
    }

    pub(super) fn attr_metadata(
        &mut self,
        path: &MountPath,
        parsed: &JetStreamPath,
    ) -> Result<FileMetadata, i32> {
        if self.latest_queued_delete_covers(path) {
            return Err(libc::ENOENT);
        }
        if let Some(snapshot) = self.queued_whole_value_snapshot(path) {
            return Ok(FileMetadata {
                size: snapshot.bytes.len() as u64,
                version: snapshot.version,
            });
        }
        if let Some(snapshot) = self.queued_jsonl_snapshot(path, parsed)? {
            return Ok(FileMetadata {
                size: snapshot.bytes.len() as u64,
                version: snapshot.version,
            });
        }
        if let Some(entry) = self.state.cache.get(path.as_str()) {
            return Ok(FileMetadata {
                size: entry.bytes.len() as u64,
                version: entry.version,
            });
        }
        if let JetStreamPath::Object { bucket, object } = parsed {
            return self
                .state
                .object_metadata(bucket, object)?
                .map(|metadata| FileMetadata {
                    size: metadata.size,
                    version: VersionStamp::at(metadata.modified),
                })
                .ok_or(libc::ENOENT);
        }
        self.read_snapshot(path, parsed)
            .map(|snapshot| FileMetadata {
                size: snapshot.bytes.len() as u64,
                version: snapshot.version,
            })
    }

    pub(super) fn read_snapshot(
        &mut self,
        path: &MountPath,
        parsed: &JetStreamPath,
    ) -> Result<FileSnapshot, i32> {
        if self.latest_queued_delete_covers(path) {
            return Err(libc::ENOENT);
        }
        if let Some(snapshot) = self.queued_whole_value_snapshot(path) {
            return Ok(snapshot);
        }
        if let Some(snapshot) = self.queued_jsonl_snapshot(path, parsed)? {
            return Ok(snapshot);
        }
        if let Some(entry) = self.state.cache.get(path.as_str()) {
            return Ok(FileSnapshot {
                bytes: entry.bytes.clone(),
                version: entry.version,
            });
        }
        if let JetStreamPath::StreamSubject { stream, subject } = parsed {
            self.state.require_stream(stream)?;
            if !self.state.stream_subject_exists(stream, subject.as_str())? {
                return Err(libc::ENOENT);
            }
        }
        let action = plan_operation(FileIntent::Read, parsed).map_err(|err| err.errno().get())?;
        match action {
            JetStreamAction::KvGet { bucket, key } => {
                let entry = self
                    .state
                    .backend
                    .kv_get(&bucket, &key)
                    .map_err(|err| self.state.io_error(err))?
                    .ok_or(libc::ENOENT)?;
                let version = VersionStamp::at(entry.created);
                let bytes = entry.bytes;
                self.state.cache.insert(CacheEntry {
                    path: path.as_str().into(),
                    origin: CacheOrigin::KvRevision {
                        bucket,
                        key,
                        revision: entry.revision,
                    },
                    version,
                    bytes: bytes.clone(),
                    valid: true,
                });
                Ok(FileSnapshot { bytes, version })
            }
            JetStreamAction::KvRevision {
                bucket,
                key,
                revision,
            } => {
                let entry = self
                    .state
                    .backend
                    .kv_revision(&bucket, &key, revision)
                    .map_err(|err| self.state.io_error(err))?
                    .ok_or(libc::ENOENT)?;
                let version = VersionStamp::at(entry.created);
                let bytes = entry.bytes;
                self.state.cache.insert(CacheEntry {
                    path: path.as_str().into(),
                    origin: CacheOrigin::KvRevision {
                        bucket,
                        key,
                        revision,
                    },
                    version,
                    bytes: bytes.clone(),
                    valid: true,
                });
                Ok(FileSnapshot { bytes, version })
            }
            JetStreamAction::StreamMessage { stream, sequence } => {
                let message = self
                    .state
                    .backend
                    .stream_message(&stream, sequence)
                    .map_err(|err| self.state.io_error(err))?;
                let version = VersionStamp::at(message.published);
                let payload = serde_json::from_slice::<serde_json::Value>(&message.payload)
                    .unwrap_or_else(|_| {
                        serde_json::Value::String(String::from_utf8_lossy(&message.payload).into())
                    });
                let bytes = serde_json::to_vec_pretty(&serde_json::json!({
                    "stream": message.stream,
                    "sequence": message.sequence,
                    "subject": message.subject,
                    "payload": payload
                }))
                .map_err(|err| self.state.io_error(err.into()))?;
                self.state.cache.insert(CacheEntry {
                    path: path.as_str().into(),
                    origin: CacheOrigin::StreamSequence { stream, sequence },
                    version,
                    bytes: bytes.clone(),
                    valid: true,
                });
                Ok(FileSnapshot { bytes, version })
            }
            JetStreamAction::ObjectGet { bucket, object } => {
                let entry = self
                    .state
                    .backend
                    .object_get(&bucket, &object)
                    .map_err(|err| self.state.io_error(err))?
                    .ok_or(libc::ENOENT)?;
                let version = VersionStamp::at(entry.modified);
                let bytes = entry.bytes;
                self.state.cache.insert(CacheEntry {
                    path: path.as_str().into(),
                    origin: CacheOrigin::ObjectVersion { bucket, object },
                    version,
                    bytes: bytes.clone(),
                    valid: true,
                });
                Ok(FileSnapshot { bytes, version })
            }
            JetStreamAction::MetadataRead { file } => Ok(FileSnapshot {
                bytes: self.state.metadata_bytes(self.paths, file),
                version: self.state.mount_version(),
            }),
            JetStreamAction::MaterializedGet { target } => {
                let snapshot = self.state.read_materialized(target)?;
                self.state.cache.insert(CacheEntry {
                    path: path.as_str().into(),
                    origin: CacheOrigin::MaterializedView {
                        path: path.as_str().into(),
                    },
                    version: snapshot.version,
                    bytes: snapshot.bytes.clone(),
                    valid: true,
                });
                Ok(snapshot)
            }
            JetStreamAction::PublishJsonLines { .. } => Ok(self
                .state
                .jsonl_target_for_path(parsed)
                .map(|(stream, subject)| self.state.read_stream_subject_snapshot(&stream, &subject))
                .transpose()?
                .unwrap_or_else(|| FileSnapshot {
                    bytes: Vec::new(),
                    version: self.state.mount_version(),
                })),
            _ => {
                let _ = path;
                Err(libc::EISDIR)
            }
        }
    }

    fn durable_directory_entries(
        &mut self,
        parsed: &JetStreamPath,
    ) -> Result<Vec<DirectoryEntry>, i32> {
        let action =
            plan_operation(FileIntent::ReadDir, parsed).map_err(|err| err.errno().get())?;
        match action {
            JetStreamAction::StaticDirectory { entries } => Ok(entries
                .into_iter()
                .map(|name| DirectoryEntry {
                    name: name.name,
                    kind: match name.kind {
                        StaticEntryKind::Directory => EntryKind::Directory,
                        StaticEntryKind::File => EntryKind::File,
                    },
                })
                .collect()),
            JetStreamAction::ListKvBuckets => self
                .state
                .backend
                .list_kv_buckets()
                .map(names_as_dirs)
                .map_err(|err| self.state.io_error(err)),
            JetStreamAction::ListKvPrefix { bucket, prefix } => {
                self.list_kv_prefix(&bucket, &prefix, parsed)
            }
            JetStreamAction::KvHistory { bucket, key } => self
                .state
                .kv_history_directory_entries(&bucket, &key)
                .map_err(|err| self.state.io_error(err)),
            JetStreamAction::ListKvHistoryPrefix { bucket, prefix } => self
                .state
                .backend
                .list_kv_history_prefix(&bucket, &prefix)
                .map_err(|err| self.state.io_error(err)),
            JetStreamAction::ListStreams => self
                .state
                .backend
                .list_streams()
                .map(names_as_dirs)
                .map_err(|err| self.state.io_error(err)),
            JetStreamAction::ListEventLogs => self
                .state
                .backend
                .list_streams()
                .map(names_as_event_files)
                .map_err(|err| self.state.io_error(err)),
            JetStreamAction::ListAgents => self.state.list_agents_root(),
            JetStreamAction::ListStreamMessages { stream } => {
                self.state.list_stream_messages(&stream, false)
            }
            JetStreamAction::ListStreamSubjects { stream } => self
                .state
                .backend
                .list_stream_subjects(&stream)
                .map_err(|err| self.state.io_error(err)),
            JetStreamAction::ListObjectBuckets => self
                .state
                .backend
                .list_object_buckets()
                .map(names_as_dirs)
                .map_err(|err| self.state.io_error(err)),
            JetStreamAction::ListObjectPrefix { bucket, prefix } => self
                .state
                .backend
                .list_object_prefix(&bucket, &prefix)
                .map_err(|err| self.state.io_error(err)),
            JetStreamAction::NoopDirectory => Ok(Vec::new()),
            _ => Err(libc::ENOTDIR),
        }
    }

    fn list_kv_prefix(
        &mut self,
        bucket: &str,
        prefix: &str,
        parsed: &JetStreamPath,
    ) -> Result<Vec<DirectoryEntry>, i32> {
        match self.state.backend.list_kv_prefix(bucket, prefix) {
            Ok(entries) => Ok(entries),
            Err(eventfs_transport::TransportError::NotFound)
                if projection_allows_missing_materialized_kv_bucket(parsed, bucket) =>
            {
                Ok(Vec::new())
            }
            Err(err) => Err(self.state.io_error(err)),
        }
    }

    fn queued_whole_value_snapshot(&self, path: &MountPath) -> Option<FileSnapshot> {
        self.queued_overlay().whole_value_snapshot(path)
    }

    fn queued_jsonl_snapshot(
        &mut self,
        path: &MountPath,
        parsed: &JetStreamPath,
    ) -> Result<Option<FileSnapshot>, i32> {
        let pending = self.queued_jsonl_entries(path);
        if pending.is_empty() {
            return Ok(None);
        }
        let mut snapshot = match self.state.cache.get(path.as_str()) {
            Some(entry) => FileSnapshot {
                bytes: entry.bytes.clone(),
                version: entry.version,
            },
            None => match self.state.jsonl_target_for_path(parsed) {
                Some((stream, subject)) => {
                    match self
                        .state
                        .read_stream_subject_snapshot_allow_missing(&stream, &subject)
                    {
                        Ok(snapshot) => snapshot,
                        Err(errno) if directory_listing_unavailable(errno) => FileSnapshot {
                            bytes: Vec::new(),
                            version: self.state.mount_version(),
                        },
                        Err(errno) => return Err(errno),
                    }
                }
                None => FileSnapshot {
                    bytes: Vec::new(),
                    version: self.state.mount_version(),
                },
            },
        };
        let mut pending_version = None;
        for entry in pending {
            let applied_lines = self
                .state
                .durable_jsonl_applied_prefix(&entry)
                .max(entry.applied_lines);
            let unapplied = unapplied_jsonl_bytes(path.as_str(), &entry.bytes, applied_lines)?;
            snapshot.bytes.extend_from_slice(&unapplied);
            pending_version = Some(max_version_stamp(pending_version, entry.version));
        }
        if let Some(pending_version) = pending_version {
            snapshot.version = max_version_stamp(Some(snapshot.version), pending_version);
        }
        Ok(Some(snapshot))
    }

    fn queued_jsonl_entries(&self, path: &MountPath) -> Vec<QueuedJsonlEntry> {
        self.queued_overlay().jsonl_entries(path)
    }

    fn has_queued_jsonl_entry(&self, path: &MountPath) -> bool {
        self.queued_overlay().has_jsonl_entry(path)
    }

    fn queued_overlay_version(&self, path: &MountPath) -> Option<VersionStamp> {
        self.queued_overlay().overlay_version(path)
    }

    fn latest_queued_delete_covers(&self, path: &MountPath) -> bool {
        self.queued_overlay().latest_delete_covers(path)
    }

    fn queued_directory_entries(&self, path: &MountPath) -> Vec<DirectoryEntry> {
        self.queued_overlay().directory_entries(path)
    }

    fn merge_queued_directory_entries(
        &mut self,
        parsed: &JetStreamPath,
        path: &MountPath,
        entries: Result<Vec<DirectoryEntry>, i32>,
    ) -> Result<Vec<DirectoryEntry>, i32> {
        let local = deterministic_directory_entries(parsed);
        let mut queued = self.queued_directory_entries(path);
        match entries {
            Ok(mut entries) => {
                for entry in local {
                    insert_directory_entry_sorted(&mut entries, entry);
                }
                for entry in queued.drain(..) {
                    insert_directory_entry(&mut entries, entry);
                }
                self.apply_queued_deletes_to_directory(path, &mut entries);
                Ok(entries)
            }
            Err(errno) if directory_listing_unavailable(errno) && !queued.is_empty() => {
                let mut fallback = local;
                for entry in queued.drain(..) {
                    insert_directory_entry_sorted(&mut fallback, entry);
                }
                self.apply_queued_deletes_to_directory(path, &mut fallback);
                Ok(fallback)
            }
            Err(errno) => Err(errno),
        }
    }

    fn apply_queued_deletes_to_directory(
        &mut self,
        path: &MountPath,
        entries: &mut Vec<DirectoryEntry>,
    ) {
        let mut visited = HashSet::new();
        self.apply_queued_overlay_to_directory(path, entries, &mut visited);
    }

    fn apply_queued_overlay_to_directory(
        &mut self,
        path: &MountPath,
        entries: &mut Vec<DirectoryEntry>,
        visited: &mut HashSet<MountPath>,
    ) {
        for change in self.queued_overlay().changes() {
            for deleted_path in change.deleted_paths {
                if let Some(child) = QueuedOverlayProjection::directory_child(path, &deleted_path) {
                    match child.kind {
                        EntryKind::File => {
                            let Ok(child_path) = path.join_child(&child.name) else {
                                continue;
                            };
                            if self.queued_exact_file_delete_reveals_directory(&child_path, visited)
                            {
                                insert_directory_entry_sorted(
                                    entries,
                                    DirectoryEntry {
                                        name: child.name,
                                        kind: EntryKind::Directory,
                                    },
                                );
                            } else {
                                entries.retain(|entry| entry.name != child.name);
                            }
                        }
                        EntryKind::Directory => {
                            let Ok(child_path) = path.join_child(&child.name) else {
                                continue;
                            };
                            if QueuedOverlayProjection::delete_can_elide_directory_child(
                                &child_path,
                            ) && !directory_entries_contain_exact_file(entries, &child.name)
                                && !self.queued_directory_child_has_exact_dynamic_file(&child_path)
                                && !self.queued_directory_child_has_visible_entries(
                                    &child_path,
                                    visited,
                                )
                            {
                                entries.retain(|entry| entry.name != child.name);
                            }
                        }
                    }
                }
            }
            for visible_path in change.visible_paths {
                if let Some(child) = QueuedOverlayProjection::directory_child(path, &visible_path) {
                    insert_directory_entry(entries, child);
                }
            }
        }
    }

    fn queued_exact_delete_reveals_synthetic_directory(
        &mut self,
        path: &MountPath,
        parsed: &JetStreamPath,
    ) -> bool {
        QueuedOverlayProjection::exact_delete_reveals_synthetic_directory(
            parsed,
            self.queued_directory_child_has_known_visible_entries(path, &mut HashSet::new()),
        )
    }

    fn queued_exact_file_delete_reveals_directory(
        &mut self,
        path: &MountPath,
        visited: &mut HashSet<MountPath>,
    ) -> bool {
        QueuedOverlayProjection::delete_can_elide_directory_child(path)
            && self.queued_directory_child_has_known_visible_entries(path, visited)
    }

    fn queued_directory_child_has_known_visible_entries(
        &mut self,
        path: &MountPath,
        visited: &mut HashSet<MountPath>,
    ) -> bool {
        if !visited.insert(path.clone()) {
            return false;
        }
        let mut entries = self
            .state
            .parse_path(path)
            .ok()
            .and_then(|parsed| self.durable_directory_entries(&parsed).ok())
            .unwrap_or_default();
        self.apply_queued_overlay_to_directory(path, &mut entries, visited);
        let visible = !entries.is_empty();
        visited.remove(path);
        visible
    }

    fn queued_directory_child_has_visible_entries(
        &mut self,
        path: &MountPath,
        visited: &mut HashSet<MountPath>,
    ) -> bool {
        if !visited.insert(path.clone()) {
            return true;
        }
        let visible = self
            .state
            .parse_path(path)
            .ok()
            .and_then(|parsed| self.durable_directory_entries(&parsed).ok())
            .map(|mut entries| {
                self.apply_queued_overlay_to_directory(path, &mut entries, visited);
                !entries.is_empty()
            })
            .unwrap_or(true);
        visited.remove(path);
        visible
    }

    fn queued_directory_child_has_exact_dynamic_file(&mut self, path: &MountPath) -> bool {
        self.state
            .parse_path(path)
            .ok()
            .and_then(|parsed| self.state.exact_dynamic_file_exists(&parsed).ok())
            .unwrap_or(false)
    }

    fn queued_deleted_descendant_empties_synthetic_directory(
        &mut self,
        path: &MountPath,
        parsed: &JetStreamPath,
    ) -> Result<bool, i32> {
        let overlay = self.queued_overlay();
        if !overlay.has_deleted_descendant(path) {
            return Ok(false);
        }
        let exact_dynamic_file_exists = self.state.exact_dynamic_file_exists(parsed)?;
        let has_visible_entries =
            self.queued_directory_child_has_visible_entries(path, &mut HashSet::new());
        Ok(
            QueuedOverlayProjection::deleted_descendant_empties_synthetic_directory(
                parsed,
                exact_dynamic_file_exists,
                has_visible_entries,
            ),
        )
    }
}

fn project_entry(entry: DirectoryEntry) -> MountProjectionEntry {
    MountProjectionEntry {
        name: entry.name,
        kind: match entry.kind {
            EntryKind::Directory => MountProjectionKind::Directory,
            EntryKind::File => MountProjectionKind::File,
        },
    }
}

fn cached_projection_kind(paths: &PathCache, path: &MountPath) -> Option<MountProjectionKind> {
    paths
        .entry(path)
        .map(|(_, kind)| projection_kind_from_file_type(kind))
}

fn create_default_payload(action: &JetStreamAction) -> Vec<u8> {
    match action {
        JetStreamAction::MaterializedPut {
            target: MaterializedTarget::Kv { .. },
        } => b"null".to_vec(),
        _ => Vec::new(),
    }
}

fn projection_allows_missing_materialized_kv_bucket(parsed: &JetStreamPath, bucket: &str) -> bool {
    match parsed {
        JetStreamPath::TasksRoot | JetStreamPath::TaskNamespace { .. } => bucket == TASKS_BUCKET,
        JetStreamPath::AgentsRoot | JetStreamPath::AgentDirectory { .. } => bucket == AGENTS_BUCKET,
        JetStreamPath::SemanticArea { .. } => bucket == SEMANTIC_BUCKET,
        _ => false,
    }
}

fn deterministic_directory_entries(parsed: &JetStreamPath) -> Vec<DirectoryEntry> {
    match parsed {
        JetStreamPath::KvBucket { .. } => vec![DirectoryEntry {
            name: ".history".into(),
            kind: EntryKind::Directory,
        }],
        _ => Vec::new(),
    }
}
