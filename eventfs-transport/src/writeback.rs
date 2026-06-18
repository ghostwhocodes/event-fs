use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use eventfs_protocol::{
    stream_subject_file_name_from_str, visible_paths, AffectedPath, MountPath, StorageFact,
};

use crate::{ReplayStorage, TransportError, TransportResult, VersionStamp};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum QueueOperation {
    KvPut {
        bucket: String,
        key: String,
        bytes: Vec<u8>,
    },
    KvRenameComplete {
        from_bucket: String,
        from_key: String,
        expected_from_revision: u64,
        to_bucket: String,
        to_key: String,
        bytes: Vec<u8>,
    },
    ObjectPut {
        bucket: String,
        object: String,
        bytes: Vec<u8>,
    },
    ObjectRenameComplete {
        from_bucket: String,
        from_object: String,
        expected_from_sequence: u64,
        expected_from_nuid: String,
        to_bucket: String,
        to_object: String,
        bytes: Vec<u8>,
    },
    PublishJsonLines {
        stream: String,
        subject: String,
        bytes: Vec<u8>,
        applied_lines: usize,
    },
    MaterializedPut {
        bucket: String,
        key: String,
        bytes: Vec<u8>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct QueueEntry {
    idempotency_key: String,
    version: VersionStamp,
    operation: QueueOperation,
    attempts: u32,
    done: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvSourceGeneration {
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectSourceGeneration {
    pub sequence: u64,
    pub nuid: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FailedWriteOperation {
    KvPut {
        bucket: String,
        key: String,
        bytes: Vec<u8>,
    },
    KvRenameComplete {
        from_bucket: String,
        from_key: String,
        source: KvSourceGeneration,
        to_bucket: String,
        to_key: String,
        bytes: Vec<u8>,
    },
    ObjectPut {
        bucket: String,
        object: String,
        bytes: Vec<u8>,
    },
    ObjectRenameComplete {
        from_bucket: String,
        from_object: String,
        source: ObjectSourceGeneration,
        to_bucket: String,
        to_object: String,
        bytes: Vec<u8>,
    },
    PublishJsonLines {
        stream: String,
        subject: String,
        bytes: Vec<u8>,
        applied_lines: usize,
    },
    MaterializedPut {
        bucket: String,
        key: String,
        bytes: Vec<u8>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailedWrite {
    pub idempotency_key: String,
    pub version: VersionStamp,
    pub diagnostic_path: MountPath,
    pub operation: FailedWriteOperation,
}

impl FailedWrite {
    pub fn new(
        idempotency_key: impl Into<String>,
        version: VersionStamp,
        diagnostic_path: MountPath,
        operation: FailedWriteOperation,
    ) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            version,
            diagnostic_path,
            operation,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueState {
    pub capacity: usize,
    pub pending: usize,
    pub done: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueEntryView {
    pub idempotency_key: String,
    pub version: VersionStamp,
    pub operation_kind: String,
    pub target: String,
    pub bytes_len: usize,
    pub attempts: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueSnapshot {
    pub state: QueueState,
    pub pending: Vec<QueueEntryView>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteGateState {
    pub accepting_writes: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayQueueOutcome {
    pub queue: QueueState,
    pub write_gate: WriteGateState,
    pub gate_transition: WritebackGateTransition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PendingWritePayload {
    WholeValue(Vec<u8>),
    JsonLines {
        stream: String,
        subject: String,
        bytes: Vec<u8>,
        applied_lines: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingWriteOverlay {
    pub idempotency_key: String,
    pub version: VersionStamp,
    pub visible_paths: Vec<MountPath>,
    pub deleted_paths: Vec<MountPath>,
    pub payload: PendingWritePayload,
}

trait WriteExecutor {
    fn is_applied(&mut self, _entry: &QueueEntry) -> TransportResult<bool> {
        Ok(false)
    }

    fn refresh_after_failed_execute(&mut self, _entry: &mut QueueEntry) -> TransportResult<()> {
        Ok(())
    }

    fn execute(&mut self, entry: &QueueEntry) -> TransportResult<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WritebackGate {
    accepting_writes: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WritebackGateTransition {
    Unchanged,
    Blocked,
    Unblocked,
}

impl Default for WritebackGate {
    fn default() -> Self {
        Self {
            accepting_writes: true,
        }
    }
}

impl WritebackGate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn accepts_writes(&self) -> bool {
        self.accepting_writes
    }

    pub fn state(&self) -> WriteGateState {
        WriteGateState {
            accepting_writes: self.accepting_writes,
        }
    }

    pub fn sync_with_queue(&mut self, queue: &WritebackQueue) -> WritebackGateTransition {
        self.sync_pending(queue.has_pending())
    }

    pub fn sync_pending(&mut self, has_pending: bool) -> WritebackGateTransition {
        match (self.accepting_writes, has_pending) {
            (true, true) => {
                self.accepting_writes = false;
                WritebackGateTransition::Blocked
            }
            (false, false) => {
                self.accepting_writes = true;
                WritebackGateTransition::Unblocked
            }
            _ => WritebackGateTransition::Unchanged,
        }
    }

    pub fn block_new_writes(&mut self) -> bool {
        self.sync_pending(true) == WritebackGateTransition::Blocked
    }
}

pub struct WritebackQueue {
    dir: PathBuf,
    path: PathBuf,
    _lock: QueueDirectoryLock,
    capacity: usize,
    entries: Vec<QueueEntry>,
}

impl WritebackQueue {
    pub fn open(dir: impl AsRef<Path>, capacity: usize) -> TransportResult<Self> {
        fs::create_dir_all(dir.as_ref())?;
        let dir = dir.as_ref().to_path_buf();
        let lock = QueueDirectoryLock::acquire(&dir)?;
        let path = dir.join("writeback.jsonl");
        let mut entries = if path.exists() {
            read_entries(&path)?
        } else {
            Vec::new()
        };
        let compacted = compact_completed_entries(&mut entries);
        let queue = Self {
            dir,
            path,
            _lock: lock,
            capacity,
            entries,
        };
        if compacted {
            queue.persist()?;
        }
        Ok(queue)
    }

    fn enqueue(&mut self, entry: QueueEntry) -> TransportResult<()> {
        self.enqueue_all([entry])
    }

    fn enqueue_failed_write(&mut self, failed: FailedWrite) -> TransportResult<()> {
        self.enqueue(durable_entry_from_failed_write(failed))
    }

    fn enqueue_all<I>(&mut self, entries: I) -> TransportResult<()>
    where
        I: IntoIterator<Item = QueueEntry>,
    {
        let mut pending = Vec::new();
        for entry in entries {
            if self
                .entries
                .iter()
                .any(|existing| existing.idempotency_key == entry.idempotency_key)
                || pending
                    .iter()
                    .any(|existing: &QueueEntry| existing.idempotency_key == entry.idempotency_key)
            {
                continue;
            }
            pending.push(entry);
        }
        if pending.is_empty() {
            return Ok(());
        }
        if self.entries.len() + pending.len() > self.capacity {
            return Err(TransportError::QueueFull);
        }
        let mut candidate = self.entries.clone();
        candidate.extend(pending);
        self.replace_entries(candidate)
    }

    fn replay_with_store(&mut self, store: &dyn ReplayStorage) -> TransportResult<()> {
        let mut executor = StoreReplayExecutor { store };
        self.replay(&mut executor)
    }

    fn replay<E: WriteExecutor>(&mut self, executor: &mut E) -> TransportResult<()> {
        let index = 0;
        while index < self.entries.len() {
            if self.entries[index].done {
                let mut candidate = self.entries.clone();
                candidate.remove(index);
                self.replace_entries(candidate)?;
                continue;
            }
            if executor.is_applied(&self.entries[index])? {
                let mut candidate = self.entries.clone();
                candidate.remove(index);
                self.replace_entries(candidate)?;
                continue;
            }
            let mut candidate = self.entries.clone();
            candidate[index].attempts = candidate[index].attempts.saturating_add(1);
            self.replace_entries(candidate)?;
            if let Err(err) = executor.execute(&self.entries[index]) {
                let mut candidate = self.entries.clone();
                if let Err(progress_err) =
                    executor.refresh_after_failed_execute(&mut candidate[index])
                {
                    tracing::warn!(
                        idempotency_key = %self.entries[index].idempotency_key,
                        error = %progress_err,
                        "failed to refresh writeback queue progress after replay error"
                    );
                }
                self.replace_entries(candidate)?;
                return Err(err);
            }
            let mut candidate = self.entries.clone();
            candidate.remove(index);
            self.replace_entries(candidate)?;
        }
        Ok(())
    }

    pub fn state(&self) -> QueueState {
        QueueState {
            capacity: self.capacity,
            pending: self.pending_len(),
            done: self.entries.iter().filter(|entry| entry.done).count(),
        }
    }

    pub fn has_pending(&self) -> bool {
        self.pending_len() > 0
    }

    #[cfg(test)]
    fn entries(&self) -> &[QueueEntry] {
        &self.entries
    }

    pub fn snapshot(&self) -> QueueSnapshot {
        QueueSnapshot {
            state: self.state(),
            pending: self
                .entries
                .iter()
                .filter(|entry| !entry.done)
                .map(queue_entry_view)
                .collect(),
        }
    }

    pub fn pending_overlays(&self) -> Vec<PendingWriteOverlay> {
        self.entries
            .iter()
            .filter(|entry| !entry.done)
            .map(queue_entry_pending_overlay)
            .collect()
    }

    fn pending_len(&self) -> usize {
        self.entries.iter().filter(|entry| !entry.done).count()
    }

    fn replace_entries(&mut self, entries: Vec<QueueEntry>) -> TransportResult<()> {
        self.persist_entries(&entries)?;
        self.entries = entries;
        Ok(())
    }

    fn persist(&self) -> TransportResult<()> {
        self.persist_entries(&self.entries)
    }

    fn persist_entries(&self, entries: &[QueueEntry]) -> TransportResult<()> {
        let mut content = String::new();
        for entry in entries {
            content.push_str(&serde_json::to_string(entry)?);
            content.push('\n');
        }
        let tmp_path = self.path.with_extension("jsonl.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, &self.path)?;
        fsync_dir(&self.dir)?;
        Ok(())
    }
}

struct QueueDirectoryLock {
    file: File,
    path: PathBuf,
}

impl QueueDirectoryLock {
    fn acquire(dir: &Path) -> TransportResult<Self> {
        let path = dir.join("writeback.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { file, path }),
            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                Err(TransportError::QueueLocked(path))
            }
            Err(err) => Err(err.into()),
        }
    }
}

impl Drop for QueueDirectoryLock {
    fn drop(&mut self) {
        if let Err(err) = self.file.unlock() {
            tracing::warn!(
                path = %self.path.display(),
                error = %err,
                "failed to unlock writeback queue"
            );
        }
    }
}

pub struct WritebackReplay {
    queue: WritebackQueue,
    gate: WritebackGate,
}

impl WritebackReplay {
    pub fn open(dir: impl AsRef<Path>, capacity: usize) -> TransportResult<Self> {
        let queue = WritebackQueue::open(dir, capacity)?;
        Ok(Self::from_queue(queue))
    }

    pub fn from_queue(queue: WritebackQueue) -> Self {
        let mut replay = Self {
            queue,
            gate: WritebackGate::new(),
        };
        replay.sync_gate();
        replay
    }

    pub fn enqueue_failed_write(
        &mut self,
        failed: FailedWrite,
    ) -> TransportResult<ReplayQueueOutcome> {
        self.queue.enqueue_failed_write(failed)?;
        Ok(self.sync_gate())
    }

    pub fn replay(&mut self, store: &dyn ReplayStorage) -> TransportResult<ReplayQueueOutcome> {
        let result = self.queue.replay_with_store(store);
        let outcome = self.sync_gate();
        result.map(|_| outcome)
    }

    pub fn snapshot(&self) -> QueueSnapshot {
        self.queue.snapshot()
    }

    pub fn pending_overlays(&self) -> Vec<PendingWriteOverlay> {
        self.queue.pending_overlays()
    }

    pub fn state(&self) -> QueueState {
        self.queue.state()
    }

    pub fn has_pending(&self) -> bool {
        self.queue.has_pending()
    }

    pub fn write_gate(&self) -> WriteGateState {
        self.gate.state()
    }

    pub fn sync_gate(&mut self) -> ReplayQueueOutcome {
        let gate_transition = self.gate.sync_with_queue(&self.queue);
        ReplayQueueOutcome {
            queue: self.queue.state(),
            write_gate: self.gate.state(),
            gate_transition,
        }
    }
}

fn durable_entry_from_failed_write(failed: FailedWrite) -> QueueEntry {
    QueueEntry {
        idempotency_key: failed.idempotency_key,
        version: failed.version,
        operation: durable_operation_from_failed_write(failed.operation),
        attempts: 0,
        done: false,
    }
}

fn durable_operation_from_failed_write(operation: FailedWriteOperation) -> QueueOperation {
    match operation {
        FailedWriteOperation::KvPut { bucket, key, bytes } => {
            QueueOperation::KvPut { bucket, key, bytes }
        }
        FailedWriteOperation::KvRenameComplete {
            from_bucket,
            from_key,
            source,
            to_bucket,
            to_key,
            bytes,
        } => QueueOperation::KvRenameComplete {
            from_bucket,
            from_key,
            expected_from_revision: source.revision,
            to_bucket,
            to_key,
            bytes,
        },
        FailedWriteOperation::ObjectPut {
            bucket,
            object,
            bytes,
        } => QueueOperation::ObjectPut {
            bucket,
            object,
            bytes,
        },
        FailedWriteOperation::ObjectRenameComplete {
            from_bucket,
            from_object,
            source,
            to_bucket,
            to_object,
            bytes,
        } => QueueOperation::ObjectRenameComplete {
            from_bucket,
            from_object,
            expected_from_sequence: source.sequence,
            expected_from_nuid: source.nuid,
            to_bucket,
            to_object,
            bytes,
        },
        FailedWriteOperation::PublishJsonLines {
            stream,
            subject,
            bytes,
            applied_lines,
        } => QueueOperation::PublishJsonLines {
            stream,
            subject,
            bytes,
            applied_lines,
        },
        FailedWriteOperation::MaterializedPut { bucket, key, bytes } => {
            QueueOperation::MaterializedPut { bucket, key, bytes }
        }
    }
}

struct StoreReplayExecutor<'a> {
    store: &'a dyn ReplayStorage,
}

impl WriteExecutor for StoreReplayExecutor<'_> {
    fn is_applied(&mut self, entry: &QueueEntry) -> TransportResult<bool> {
        match &entry.operation {
            QueueOperation::KvPut { bucket, key, .. }
            | QueueOperation::MaterializedPut { bucket, key, .. } => {
                self.store
                    .kv_put_applied(bucket, key, &entry.idempotency_key)
            }
            QueueOperation::KvRenameComplete {
                from_bucket,
                from_key,
                expected_from_revision,
                to_bucket,
                to_key,
                ..
            } => {
                let destination_applied =
                    self.store
                        .kv_put_applied(to_bucket, to_key, &entry.idempotency_key)?;
                if !destination_applied {
                    return Ok(false);
                }
                self.store.kv_delete_if_revision_applied(
                    from_bucket,
                    from_key,
                    *expected_from_revision,
                )
            }
            QueueOperation::ObjectPut { bucket, object, .. } => {
                self.store
                    .object_put_applied(bucket, object, &entry.idempotency_key)
            }
            QueueOperation::ObjectRenameComplete {
                from_bucket,
                from_object,
                expected_from_sequence,
                expected_from_nuid,
                to_bucket,
                to_object,
                ..
            } => {
                let destination_applied =
                    self.store
                        .object_put_applied(to_bucket, to_object, &entry.idempotency_key)?;
                if !destination_applied {
                    return Ok(false);
                }
                self.store.object_delete_if_sequence_applied(
                    from_bucket,
                    from_object,
                    *expected_from_sequence,
                    expected_from_nuid,
                )
            }
            QueueOperation::PublishJsonLines {
                stream,
                subject,
                bytes,
                ..
            } => self.store.publish_json_lines_applied(
                stream,
                subject,
                bytes,
                &entry.idempotency_key,
            ),
        }
    }

    fn refresh_after_failed_execute(&mut self, entry: &mut QueueEntry) -> TransportResult<()> {
        let idempotency_key = entry.idempotency_key.clone();
        if let QueueOperation::PublishJsonLines {
            stream,
            subject,
            bytes,
            applied_lines,
        } = &mut entry.operation
        {
            let durable_prefix = self.store.publish_json_lines_applied_prefix(
                stream,
                subject,
                bytes,
                &idempotency_key,
            )?;
            *applied_lines = (*applied_lines).max(durable_prefix);
        }
        Ok(())
    }

    fn execute(&mut self, entry: &QueueEntry) -> TransportResult<()> {
        match &entry.operation {
            QueueOperation::KvPut { bucket, key, bytes }
            | QueueOperation::MaterializedPut { bucket, key, bytes } => {
                self.store
                    .kv_put_idempotent(bucket, key, bytes, &entry.idempotency_key)?;
            }
            QueueOperation::KvRenameComplete {
                from_bucket,
                from_key,
                expected_from_revision,
                to_bucket,
                to_key,
                bytes,
            } => {
                self.store
                    .kv_put_idempotent(to_bucket, to_key, bytes, &entry.idempotency_key)?;
                self.store
                    .kv_delete_if_revision(from_bucket, from_key, *expected_from_revision)?;
            }
            QueueOperation::ObjectPut {
                bucket,
                object,
                bytes,
            } => {
                self.store
                    .object_put_idempotent(bucket, object, bytes, &entry.idempotency_key)?;
            }
            QueueOperation::ObjectRenameComplete {
                from_bucket,
                from_object,
                expected_from_sequence,
                expected_from_nuid,
                to_bucket,
                to_object,
                bytes,
            } => {
                self.store.object_put_idempotent(
                    to_bucket,
                    to_object,
                    bytes,
                    &entry.idempotency_key,
                )?;
                self.store.object_delete_if_sequence(
                    from_bucket,
                    from_object,
                    *expected_from_sequence,
                    expected_from_nuid,
                )?;
            }
            QueueOperation::PublishJsonLines {
                stream,
                subject,
                bytes,
                ..
            } => {
                self.store
                    .publish_json_lines(stream, subject, bytes, &entry.idempotency_key)?;
            }
        }
        Ok(())
    }
}

fn compact_completed_entries(entries: &mut Vec<QueueEntry>) -> bool {
    let before = entries.len();
    entries.retain(|entry| !entry.done);
    entries.len() != before
}

fn read_entries(path: &Path) -> TransportResult<Vec<QueueEntry>> {
    let content = fs::read_to_string(path)?;
    let lines: Vec<&str> = content.lines().collect();
    let trailing_newline = content.ends_with('\n');
    let mut entries = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<QueueEntry>(line) {
            Ok(entry) => entries.push(entry),
            Err(err) if is_torn_final_record(index, lines.len(), trailing_newline, &err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "ignoring torn final writeback queue record"
                );
                break;
            }
            Err(err) => return Err(err.into()),
        }
    }
    Ok(entries)
}

fn is_torn_final_record(
    index: usize,
    line_count: usize,
    trailing_newline: bool,
    err: &serde_json::Error,
) -> bool {
    index == line_count.saturating_sub(1)
        && !trailing_newline
        && err.classify() == serde_json::error::Category::Eof
}

fn fsync_dir(path: &Path) -> TransportResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn queue_entry_view(entry: &QueueEntry) -> QueueEntryView {
    let (operation_kind, target, bytes_len) = match &entry.operation {
        QueueOperation::KvPut { bucket, key, bytes } => (
            "kv_put".to_string(),
            format!("/kv/{bucket}/{key}"),
            bytes.len(),
        ),
        QueueOperation::KvRenameComplete {
            from_bucket,
            from_key,
            to_bucket,
            to_key,
            bytes,
            ..
        } => (
            "kv_rename_complete".to_string(),
            format!("/kv/{from_bucket}/{from_key} -> /kv/{to_bucket}/{to_key}"),
            bytes.len(),
        ),
        QueueOperation::ObjectPut {
            bucket,
            object,
            bytes,
        } => (
            "object_put".to_string(),
            format!("/objects/{bucket}/{object}"),
            bytes.len(),
        ),
        QueueOperation::ObjectRenameComplete {
            from_bucket,
            from_object,
            to_bucket,
            to_object,
            bytes,
            ..
        } => (
            "object_rename_complete".to_string(),
            format!("/objects/{from_bucket}/{from_object} -> /objects/{to_bucket}/{to_object}"),
            bytes.len(),
        ),
        QueueOperation::PublishJsonLines {
            stream,
            subject,
            bytes,
            ..
        } => (
            "publish_json_lines".to_string(),
            format!(
                "/streams/{stream}/subjects/{}",
                stream_subject_file_name_from_str(subject)
            ),
            bytes.len(),
        ),
        QueueOperation::MaterializedPut { bucket, key, bytes } => (
            "materialized_put".to_string(),
            format!("kv://{bucket}/{key}"),
            bytes.len(),
        ),
    };
    QueueEntryView {
        idempotency_key: entry.idempotency_key.clone(),
        version: entry.version,
        operation_kind,
        target,
        bytes_len,
        attempts: entry.attempts,
    }
}

fn queue_entry_pending_overlay(entry: &QueueEntry) -> PendingWriteOverlay {
    let (visible_paths, deleted_paths, payload) = match &entry.operation {
        QueueOperation::KvPut { bucket, key, bytes }
        | QueueOperation::MaterializedPut { bucket, key, bytes } => (
            kv_visible_mount_paths(bucket, key),
            Vec::new(),
            PendingWritePayload::WholeValue(bytes.clone()),
        ),
        QueueOperation::KvRenameComplete {
            from_bucket,
            from_key,
            to_bucket,
            to_key,
            bytes,
            ..
        } => (
            kv_visible_mount_paths(to_bucket, to_key),
            kv_visible_mount_paths(from_bucket, from_key),
            PendingWritePayload::WholeValue(bytes.clone()),
        ),
        QueueOperation::ObjectPut {
            bucket,
            object,
            bytes,
        } => (
            object_visible_mount_paths(bucket, object),
            Vec::new(),
            PendingWritePayload::WholeValue(bytes.clone()),
        ),
        QueueOperation::ObjectRenameComplete {
            from_bucket,
            from_object,
            to_bucket,
            to_object,
            bytes,
            ..
        } => (
            object_visible_mount_paths(to_bucket, to_object),
            object_visible_mount_paths(from_bucket, from_object),
            PendingWritePayload::WholeValue(bytes.clone()),
        ),
        QueueOperation::PublishJsonLines {
            stream,
            subject,
            bytes,
            applied_lines,
        } => (
            publish_jsonl_visible_mount_paths(stream, subject),
            Vec::new(),
            PendingWritePayload::JsonLines {
                stream: stream.clone(),
                subject: subject.clone(),
                bytes: bytes.clone(),
                applied_lines: *applied_lines,
            },
        ),
    };
    PendingWriteOverlay {
        idempotency_key: entry.idempotency_key.clone(),
        version: entry.version,
        visible_paths,
        deleted_paths,
        payload,
    }
}

fn kv_visible_mount_paths(bucket: &str, key: &str) -> Vec<MountPath> {
    storage_fact_visible_paths(StorageFact::Kv {
        bucket: bucket.into(),
        key: key.into(),
    })
}

fn object_visible_mount_paths(bucket: &str, name: &str) -> Vec<MountPath> {
    storage_fact_visible_paths(StorageFact::Object {
        bucket: bucket.into(),
        name: name.into(),
    })
}

fn publish_jsonl_visible_mount_paths(stream: &str, subject: &str) -> Vec<MountPath> {
    storage_fact_visible_paths(StorageFact::StreamSubject {
        stream: stream.into(),
        subject: subject.into(),
    })
}

fn storage_fact_visible_paths(fact: StorageFact) -> Vec<MountPath> {
    visible_paths(&fact)
        .map(affected_mount_paths)
        .unwrap_or_default()
}

fn affected_mount_paths(paths: Vec<AffectedPath>) -> Vec<MountPath> {
    paths.into_iter().map(|affected| affected.path).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ReplayStorage;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemoryExecutor {
        seen: Vec<String>,
    }

    impl WriteExecutor for MemoryExecutor {
        fn execute(&mut self, entry: &QueueEntry) -> TransportResult<()> {
            self.seen.push(entry.idempotency_key.clone());
            Ok(())
        }
    }

    #[derive(Default)]
    struct FailingAfterFirstExecutor {
        seen: Vec<String>,
    }

    #[derive(Default)]
    struct CrashWindowExecutor {
        applied: Vec<String>,
        kv_revisions: usize,
        object_replacements: usize,
        interrupt_after: Option<String>,
    }

    impl WriteExecutor for FailingAfterFirstExecutor {
        fn execute(&mut self, entry: &QueueEntry) -> TransportResult<()> {
            self.seen.push(entry.idempotency_key.clone());
            if entry.idempotency_key == "b" {
                Err(TransportError::Invalid("stop".into()))
            } else {
                Ok(())
            }
        }
    }

    impl WriteExecutor for CrashWindowExecutor {
        fn is_applied(&mut self, entry: &QueueEntry) -> TransportResult<bool> {
            Ok(self
                .applied
                .iter()
                .any(|idempotency_key| idempotency_key == &entry.idempotency_key))
        }

        fn execute(&mut self, entry: &QueueEntry) -> TransportResult<()> {
            match &entry.operation {
                QueueOperation::KvPut { .. }
                | QueueOperation::KvRenameComplete { .. }
                | QueueOperation::MaterializedPut { .. } => {
                    self.kv_revisions += 1;
                }
                QueueOperation::ObjectPut { .. } | QueueOperation::ObjectRenameComplete { .. } => {
                    self.object_replacements += 1;
                }
                QueueOperation::PublishJsonLines { .. } => {}
            }
            self.applied.push(entry.idempotency_key.clone());
            if self.interrupt_after.as_deref() == Some(entry.idempotency_key.as_str()) {
                return Err(TransportError::Invalid(
                    "simulated interruption after durable side effect".into(),
                ));
            }
            Ok(())
        }
    }

    fn entry(id: &str) -> QueueEntry {
        QueueEntry {
            idempotency_key: id.into(),
            version: VersionStamp::at(std::time::UNIX_EPOCH),
            operation: QueueOperation::KvPut {
                bucket: "b".into(),
                key: "k".into(),
                bytes: b"v".to_vec(),
            },
            attempts: 0,
            done: false,
        }
    }

    fn object_entry(id: &str) -> QueueEntry {
        QueueEntry {
            idempotency_key: id.into(),
            version: VersionStamp::at(std::time::UNIX_EPOCH),
            operation: QueueOperation::ObjectPut {
                bucket: "b".into(),
                object: "o".into(),
                bytes: b"v".to_vec(),
            },
            attempts: 0,
            done: false,
        }
    }

    fn jsonl_entry(id: &str, applied_lines: usize) -> QueueEntry {
        QueueEntry {
            idempotency_key: id.into(),
            version: VersionStamp::at(std::time::UNIX_EPOCH),
            operation: QueueOperation::PublishJsonLines {
                stream: "s".into(),
                subject: "events.s".into(),
                bytes: br#"{"line":1}
{"line":2}
"#
                .to_vec(),
                applied_lines,
            },
            attempts: 0,
            done: false,
        }
    }

    fn failed_kv_write(id: &str) -> FailedWrite {
        FailedWrite::new(
            id,
            VersionStamp::at(std::time::UNIX_EPOCH),
            MountPath::new("/kv/b/k").unwrap(),
            FailedWriteOperation::KvPut {
                bucket: "b".into(),
                key: "k".into(),
                bytes: b"v".to_vec(),
            },
        )
    }

    struct PartialJsonlExecutor {
        applied_prefix: usize,
    }

    struct TmpCollisionExecutor {
        tmp_path: PathBuf,
        seen: Vec<String>,
    }

    struct RecordingReplayStorage {
        applied_ids: Mutex<Vec<String>>,
        fail_jsonl: bool,
        jsonl_applied_prefix: usize,
    }

    impl Default for RecordingReplayStorage {
        fn default() -> Self {
            Self {
                applied_ids: Mutex::new(Vec::new()),
                fail_jsonl: false,
                jsonl_applied_prefix: 0,
            }
        }
    }

    impl RecordingReplayStorage {
        fn failing_jsonl(applied_prefix: usize) -> Self {
            Self {
                fail_jsonl: true,
                jsonl_applied_prefix: applied_prefix,
                ..Default::default()
            }
        }

        fn applied_ids(&self) -> Vec<String> {
            self.applied_ids.lock().unwrap().clone()
        }

        fn mark_applied(&self, idempotency_key: &str) {
            self.applied_ids
                .lock()
                .unwrap()
                .push(idempotency_key.to_string());
        }

        fn is_marked_applied(&self, idempotency_key: &str) -> bool {
            self.applied_ids
                .lock()
                .unwrap()
                .iter()
                .any(|applied| applied == idempotency_key)
        }
    }

    impl WriteExecutor for PartialJsonlExecutor {
        fn refresh_after_failed_execute(&mut self, entry: &mut QueueEntry) -> TransportResult<()> {
            if let QueueOperation::PublishJsonLines { applied_lines, .. } = &mut entry.operation {
                *applied_lines = (*applied_lines).max(self.applied_prefix);
            }
            Ok(())
        }

        fn execute(&mut self, _entry: &QueueEntry) -> TransportResult<()> {
            Err(TransportError::Invalid(
                "simulated partial JSONL replay failure".into(),
            ))
        }
    }

    impl WriteExecutor for TmpCollisionExecutor {
        fn execute(&mut self, entry: &QueueEntry) -> TransportResult<()> {
            self.seen.push(entry.idempotency_key.clone());
            fs::create_dir(&self.tmp_path)?;
            Ok(())
        }
    }

    impl ReplayStorage for RecordingReplayStorage {
        fn kv_put_idempotent(
            &self,
            _bucket: &str,
            _key: &str,
            _bytes: &[u8],
            idempotency_key: &str,
        ) -> TransportResult<u64> {
            self.mark_applied(idempotency_key);
            Ok(1)
        }

        fn kv_put_applied(
            &self,
            _bucket: &str,
            _key: &str,
            idempotency_key: &str,
        ) -> TransportResult<bool> {
            Ok(self.is_marked_applied(idempotency_key))
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
            idempotency_seed: &str,
        ) -> TransportResult<Vec<u64>> {
            if self.fail_jsonl {
                return Err(TransportError::Invalid("jsonl replay failed".into()));
            }
            self.mark_applied(idempotency_seed);
            Ok(vec![1])
        }

        fn publish_json_lines_applied(
            &self,
            _stream: &str,
            _subject: &str,
            _bytes: &[u8],
            idempotency_seed: &str,
        ) -> TransportResult<bool> {
            Ok(self.is_marked_applied(idempotency_seed))
        }

        fn publish_json_lines_applied_prefix(
            &self,
            _stream: &str,
            _subject: &str,
            _bytes: &[u8],
            _idempotency_seed: &str,
        ) -> TransportResult<usize> {
            Ok(self.jsonl_applied_prefix)
        }

        fn object_put_idempotent(
            &self,
            _bucket: &str,
            _object: &str,
            _bytes: &[u8],
            idempotency_key: &str,
        ) -> TransportResult<()> {
            self.mark_applied(idempotency_key);
            Ok(())
        }

        fn object_put_applied(
            &self,
            _bucket: &str,
            _object: &str,
            idempotency_key: &str,
        ) -> TransportResult<bool> {
            Ok(self.is_marked_applied(idempotency_key))
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

    #[test]
    fn writeback_is_bounded_and_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let mut queue = WritebackQueue::open(tmp.path(), 1).unwrap();
        queue.enqueue(entry("same")).unwrap();
        queue.enqueue(entry("same")).unwrap();
        assert_eq!(queue.state().pending, 1);
        assert!(matches!(
            queue.enqueue(entry("other")),
            Err(TransportError::QueueFull)
        ));
    }

    #[test]
    fn writeback_gate_blocks_new_writes_after_pending_replay() {
        let mut gate = WritebackGate::new();

        assert!(gate.accepts_writes());
        assert!(gate.block_new_writes());
        assert!(!gate.accepts_writes());
        assert!(!gate.block_new_writes());
    }

    #[test]
    fn writeback_gate_syncs_acceptance_with_current_queue_state() {
        let tmp = tempfile::tempdir().unwrap();
        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        let mut gate = WritebackGate::new();

        assert_eq!(
            gate.sync_with_queue(&queue),
            WritebackGateTransition::Unchanged
        );
        queue.enqueue(entry("pending")).unwrap();
        assert_eq!(
            gate.sync_with_queue(&queue),
            WritebackGateTransition::Blocked
        );
        assert!(!gate.accepts_writes());

        queue.replay(&mut MemoryExecutor::default()).unwrap();

        assert_eq!(queue.state().pending, 0);
        assert_eq!(
            gate.sync_with_queue(&queue),
            WritebackGateTransition::Unblocked
        );
        assert!(gate.accepts_writes());
    }

    #[test]
    fn writeback_replay_enqueues_failed_write_without_queue_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();

        let outcome = replay
            .enqueue_failed_write(failed_kv_write("failed-kv"))
            .unwrap();

        assert_eq!(outcome.queue.pending, 1);
        assert_eq!(outcome.gate_transition, WritebackGateTransition::Blocked);
        assert!(!outcome.write_gate.accepting_writes);

        let duplicate = replay
            .enqueue_failed_write(failed_kv_write("failed-kv"))
            .unwrap();
        assert_eq!(duplicate.queue.pending, 1);
        assert_eq!(
            duplicate.gate_transition,
            WritebackGateTransition::Unchanged
        );

        drop(replay);
        let reopened = WritebackReplay::open(tmp.path(), 4).unwrap();
        assert_eq!(reopened.state().pending, 1);
        assert!(!reopened.write_gate().accepting_writes);
        assert_eq!(reopened.snapshot().pending[0].operation_kind, "kv_put");
        assert_eq!(reopened.snapshot().pending[0].target, "/kv/b/k");
        assert!(reopened.pending_overlays()[0]
            .visible_paths
            .contains(&MountPath::new("/kv/b/k").unwrap()));
    }

    #[test]
    fn writeback_queue_rejects_second_open_while_first_queue_lives() {
        let tmp = tempfile::tempdir().unwrap();
        let first = WritebackQueue::open(tmp.path(), 4).unwrap();

        match WritebackQueue::open(tmp.path(), 4) {
            Err(TransportError::QueueLocked(path)) => {
                assert_eq!(path, tmp.path().join("writeback.lock"));
            }
            Ok(_) => panic!("second writeback queue open unexpectedly succeeded"),
            Err(err) => panic!("unexpected second writeback queue open error: {err}"),
        }

        drop(first);
        assert!(WritebackQueue::open(tmp.path(), 4).is_ok());
    }

    #[test]
    fn writeback_replay_persists_failed_rename_source_evidence() {
        let tmp = tempfile::tempdir().unwrap();
        let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();

        replay
            .enqueue_failed_write(FailedWrite::new(
                "rename-kv",
                VersionStamp::at(std::time::UNIX_EPOCH),
                MountPath::new("/kv/b/new").unwrap(),
                FailedWriteOperation::KvRenameComplete {
                    from_bucket: "b".into(),
                    from_key: "old".into(),
                    source: KvSourceGeneration { revision: 7 },
                    to_bucket: "b".into(),
                    to_key: "new".into(),
                    bytes: b"renamed".to_vec(),
                },
            ))
            .unwrap();
        replay
            .enqueue_failed_write(FailedWrite::new(
                "rename-object",
                VersionStamp::at(std::time::UNIX_EPOCH),
                MountPath::new("/objects/o/new.bin").unwrap(),
                FailedWriteOperation::ObjectRenameComplete {
                    from_bucket: "o".into(),
                    from_object: "old.bin".into(),
                    source: ObjectSourceGeneration {
                        sequence: 42,
                        nuid: "old-nuid".into(),
                    },
                    to_bucket: "o".into(),
                    to_object: "new.bin".into(),
                    bytes: b"blob".to_vec(),
                },
            ))
            .unwrap();

        let content = fs::read_to_string(tmp.path().join("writeback.jsonl")).unwrap();
        assert!(content.contains(r#""expected_from_revision":7"#));
        assert!(content.contains(r#""expected_from_sequence":42"#));
        assert!(content.contains(r#""expected_from_nuid":"old-nuid""#));

        drop(replay);
        let reopened = WritebackReplay::open(tmp.path(), 4).unwrap();
        let snapshot = reopened.snapshot();
        assert_eq!(snapshot.pending.len(), 2);
        assert_eq!(snapshot.pending[0].operation_kind, "kv_rename_complete");
        assert_eq!(snapshot.pending[1].operation_kind, "object_rename_complete");
    }

    #[test]
    fn writeback_replay_executes_with_store_and_unblocks_gate_after_drain() {
        let tmp = tempfile::tempdir().unwrap();
        let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
        let store = RecordingReplayStorage::default();

        replay
            .enqueue_failed_write(failed_kv_write("failed-kv"))
            .unwrap();
        assert!(!replay.write_gate().accepting_writes);

        let outcome = replay.replay(&store).unwrap();

        assert_eq!(store.applied_ids(), vec!["failed-kv".to_string()]);
        assert_eq!(outcome.queue.pending, 0);
        assert_eq!(outcome.gate_transition, WritebackGateTransition::Unblocked);
        assert!(outcome.write_gate.accepting_writes);
        assert!(replay.snapshot().pending.is_empty());
    }

    #[test]
    fn writeback_replay_refreshes_jsonl_progress_and_keeps_gate_blocked_after_store_error() {
        let tmp = tempfile::tempdir().unwrap();
        let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
        let store = RecordingReplayStorage::failing_jsonl(1);

        replay
            .enqueue_failed_write(FailedWrite::new(
                "jsonl",
                VersionStamp::at(std::time::UNIX_EPOCH),
                MountPath::new("/streams/s/subjects/events.s.jsonl").unwrap(),
                FailedWriteOperation::PublishJsonLines {
                    stream: "s".into(),
                    subject: "events.s".into(),
                    bytes: br#"{"line":1}
{"line":2}
"#
                    .to_vec(),
                    applied_lines: 0,
                },
            ))
            .unwrap();

        assert!(replay.replay(&store).is_err());

        drop(replay);
        let reopened = WritebackReplay::open(tmp.path(), 4).unwrap();
        assert_eq!(reopened.state().pending, 1);
        assert_eq!(reopened.snapshot().pending[0].attempts, 1);
        assert!(!reopened.write_gate().accepting_writes);
        match &reopened.pending_overlays()[0].payload {
            PendingWritePayload::JsonLines { applied_lines, .. } => {
                assert_eq!(*applied_lines, 1);
            }
            payload => panic!("unexpected pending payload: {payload:?}"),
        }
    }

    #[test]
    fn writeback_enqueue_all_is_atomic_when_capacity_would_overflow() {
        let tmp = tempfile::tempdir().unwrap();
        let mut queue = WritebackQueue::open(tmp.path(), 1).unwrap();

        assert!(matches!(
            queue.enqueue_all([entry("put"), entry("delete")]),
            Err(TransportError::QueueFull)
        ));

        assert!(queue.entries().is_empty());
        assert_eq!(
            fs::read_to_string(tmp.path().join("writeback.jsonl")).unwrap_or_default(),
            ""
        );
    }

    #[test]
    fn writeback_enqueue_does_not_mutate_memory_when_persist_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        fs::create_dir(tmp.path().join("writeback.jsonl.tmp")).unwrap();

        assert!(queue.enqueue(entry("failed")).is_err());

        assert!(queue.entries().is_empty());
        fs::remove_dir(tmp.path().join("writeback.jsonl.tmp")).unwrap();
        drop(queue);
        let reopened = WritebackQueue::open(tmp.path(), 4).unwrap();
        assert!(reopened.entries().is_empty());
    }

    #[test]
    fn writeback_replay_does_not_attempt_execute_when_attempt_persist_fails() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
            queue.enqueue(entry("pending")).unwrap();
        }
        fs::create_dir(tmp.path().join("writeback.jsonl.tmp")).unwrap();

        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        let mut executor = MemoryExecutor::default();
        assert!(queue.replay(&mut executor).is_err());

        assert!(executor.seen.is_empty());
        assert_eq!(queue.entries()[0].attempts, 0);
        fs::remove_dir(tmp.path().join("writeback.jsonl.tmp")).unwrap();
        drop(queue);
        let reopened = WritebackQueue::open(tmp.path(), 4).unwrap();
        assert_eq!(reopened.entries()[0].attempts, 0);
    }

    #[test]
    fn writeback_replay_keeps_entry_live_when_post_execute_remove_persist_fails() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
            queue.enqueue(entry("pending")).unwrap();
        }

        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        let mut executor = TmpCollisionExecutor {
            tmp_path: tmp.path().join("writeback.jsonl.tmp"),
            seen: Vec::new(),
        };
        assert!(queue.replay(&mut executor).is_err());

        assert_eq!(executor.seen, vec!["pending".to_string()]);
        assert_eq!(queue.entries().len(), 1);
        assert_eq!(queue.entries()[0].idempotency_key, "pending");
        assert_eq!(queue.entries()[0].attempts, 1);
        fs::remove_dir(tmp.path().join("writeback.jsonl.tmp")).unwrap();
        drop(queue);
        let reopened = WritebackQueue::open(tmp.path(), 4).unwrap();
        assert_eq!(reopened.entries().len(), 1);
        assert_eq!(reopened.entries()[0].idempotency_key, "pending");
        assert_eq!(reopened.entries()[0].attempts, 1);
    }

    #[test]
    fn writeback_replays_after_restart_without_duplicates() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
            queue.enqueue(entry("a")).unwrap();
            queue.enqueue(entry("b")).unwrap();
        }

        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        let mut executor = MemoryExecutor::default();
        queue.replay(&mut executor).unwrap();
        queue.replay(&mut executor).unwrap();

        assert_eq!(executor.seen, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(queue.state().pending, 0);
        assert_eq!(queue.state().done, 0);
        assert!(queue.entries().is_empty());
        assert_eq!(
            fs::read_to_string(tmp.path().join("writeback.jsonl")).unwrap(),
            ""
        );
    }

    #[test]
    fn writeback_persists_replay_progress_before_returning_error() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
            queue.enqueue(entry("a")).unwrap();
            queue.enqueue(entry("b")).unwrap();
        }

        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        let mut failing = FailingAfterFirstExecutor::default();
        assert!(queue.replay(&mut failing).is_err());
        assert_eq!(failing.seen, vec!["a".to_string(), "b".to_string()]);

        drop(queue);
        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        assert_eq!(queue.entries().len(), 1);
        assert_eq!(queue.entries()[0].idempotency_key, "b");
        assert_eq!(queue.entries()[0].attempts, 1);
        let mut executor = MemoryExecutor::default();
        queue.replay(&mut executor).unwrap();

        assert_eq!(executor.seen, vec!["b".to_string()]);
        assert_eq!(queue.state().pending, 0);
        assert!(queue.entries().is_empty());
    }

    #[test]
    fn writeback_persists_jsonl_applied_progress_after_failed_replay() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
            queue.enqueue(jsonl_entry("jsonl", 0)).unwrap();
        }

        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        let mut executor = PartialJsonlExecutor { applied_prefix: 1 };

        assert!(queue.replay(&mut executor).is_err());

        drop(queue);
        let queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        assert_eq!(queue.entries().len(), 1);
        assert_eq!(queue.entries()[0].attempts, 1);
        match &queue.entries()[0].operation {
            QueueOperation::PublishJsonLines { applied_lines, .. } => {
                assert_eq!(*applied_lines, 1);
            }
            operation => panic!("unexpected operation after replay: {operation:?}"),
        }
        assert!(fs::read_to_string(tmp.path().join("writeback.jsonl"))
            .unwrap()
            .contains(r#""applied_lines":1"#));
    }

    #[test]
    fn writeback_replay_skips_kv_and_object_entries_applied_before_crash_window() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
            queue.enqueue(entry("kv")).unwrap();
            queue.enqueue(object_entry("object")).unwrap();
        }

        let mut executor = CrashWindowExecutor {
            interrupt_after: Some("kv".into()),
            ..Default::default()
        };
        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        assert!(queue.replay(&mut executor).is_err());
        assert_eq!(executor.kv_revisions, 1);

        executor.interrupt_after = Some("object".into());
        drop(queue);
        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        assert!(queue.replay(&mut executor).is_err());
        assert_eq!(executor.kv_revisions, 1);
        assert_eq!(executor.object_replacements, 1);

        executor.interrupt_after = None;
        drop(queue);
        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        queue.replay(&mut executor).unwrap();

        assert_eq!(executor.kv_revisions, 1);
        assert_eq!(executor.object_replacements, 1);
        assert!(queue.entries().is_empty());
        assert_eq!(
            fs::read_to_string(tmp.path().join("writeback.jsonl")).unwrap(),
            ""
        );
    }

    #[test]
    fn writeback_compacts_completed_entries_on_open_before_capacity_checks() {
        let tmp = tempfile::tempdir().unwrap();
        let mut done = entry("done");
        done.done = true;
        fs::write(
            tmp.path().join("writeback.jsonl"),
            format!("{}\n", serde_json::to_string(&done).unwrap()),
        )
        .unwrap();

        let mut queue = WritebackQueue::open(tmp.path(), 1).unwrap();

        assert_eq!(queue.state().pending, 0);
        assert_eq!(queue.state().done, 0);
        assert!(queue.entries().is_empty());
        queue.enqueue(entry("new")).unwrap();
        assert_eq!(queue.state().pending, 1);
        let content = fs::read_to_string(tmp.path().join("writeback.jsonl")).unwrap();
        assert!(!content.contains(r#""idempotency_key":"done""#));
        assert!(content.contains("new"));
    }

    #[test]
    fn writeback_ignores_torn_final_record_on_open() {
        let tmp = tempfile::tempdir().unwrap();
        let valid = serde_json::to_string(&entry("a")).unwrap();
        fs::write(
            tmp.path().join("writeback.jsonl"),
            format!("{valid}\n{{\"idempotency_key\":\"partial\""),
        )
        .unwrap();

        let queue = WritebackQueue::open(tmp.path(), 4).unwrap();

        assert_eq!(
            queue
                .entries()
                .iter()
                .map(|entry| entry.idempotency_key.as_str())
                .collect::<Vec<_>>(),
            vec!["a"]
        );
    }

    #[test]
    fn writeback_rejects_corrupt_complete_record() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join("writeback.jsonl"),
            "{\"idempotency_key\":\"corrupt\"\n",
        )
        .unwrap();

        assert!(WritebackQueue::open(tmp.path(), 4).is_err());
    }

    #[test]
    fn writeback_rejects_complete_records_missing_required_version() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join("writeback.jsonl"),
            r#"{"idempotency_key":"missing-version","operation":{"KvPut":{"bucket":"b","key":"k","bytes":[118]}},"attempts":0,"done":false}
"#,
        )
        .unwrap();

        assert!(WritebackQueue::open(tmp.path(), 4).is_err());
    }

    #[test]
    fn writeback_rejects_object_rename_records_missing_required_nuid() {
        let tmp = tempfile::tempdir().unwrap();
        let mut value = serde_json::to_value(QueueEntry {
            idempotency_key: "missing-nuid".into(),
            version: VersionStamp::at(std::time::UNIX_EPOCH),
            operation: QueueOperation::ObjectRenameComplete {
                from_bucket: "objects".into(),
                from_object: "old.bin".into(),
                expected_from_sequence: 7,
                expected_from_nuid: "old-nuid".into(),
                to_bucket: "objects".into(),
                to_object: "new.bin".into(),
                bytes: b"blob".to_vec(),
            },
            attempts: 0,
            done: false,
        })
        .unwrap();
        let operation = value
            .get_mut("operation")
            .and_then(|operation| operation.get_mut("ObjectRenameComplete"))
            .expect("object rename payload is present");
        operation
            .as_object_mut()
            .unwrap()
            .remove("expected_from_nuid");
        fs::write(
            tmp.path().join("writeback.jsonl"),
            format!("{}\n", serde_json::to_string(&value).unwrap()),
        )
        .unwrap();

        assert!(WritebackQueue::open(tmp.path(), 4).is_err());
    }

    #[test]
    fn writeback_rejects_jsonl_records_missing_required_progress() {
        let tmp = tempfile::tempdir().unwrap();
        let mut value = serde_json::to_value(QueueEntry {
            idempotency_key: "missing-progress".into(),
            version: VersionStamp::at(std::time::UNIX_EPOCH),
            operation: QueueOperation::PublishJsonLines {
                stream: "ORDERS".into(),
                subject: "orders.created".into(),
                bytes: br#"{"id":1}"#.to_vec(),
                applied_lines: 0,
            },
            attempts: 0,
            done: false,
        })
        .unwrap();
        let operation = value
            .get_mut("operation")
            .and_then(|operation| operation.get_mut("PublishJsonLines"))
            .expect("jsonl payload is present");
        operation.as_object_mut().unwrap().remove("applied_lines");
        fs::write(
            tmp.path().join("writeback.jsonl"),
            format!("{}\n", serde_json::to_string(&value).unwrap()),
        )
        .unwrap();

        assert!(WritebackQueue::open(tmp.path(), 4).is_err());
    }

    #[test]
    fn writeback_rejects_standalone_delete_records() {
        let kv = tempfile::tempdir().unwrap();
        let mut kv_value = serde_json::to_value(entry("unsupported-kv-delete")).unwrap();
        kv_value["operation"] = serde_json::json!({
            "KvDelete": {
                "bucket": "b",
                "key": "k"
            }
        });
        fs::write(
            kv.path().join("writeback.jsonl"),
            format!("{}\n", serde_json::to_string(&kv_value).unwrap()),
        )
        .unwrap();

        assert!(WritebackQueue::open(kv.path(), 4).is_err());

        let object = tempfile::tempdir().unwrap();
        let mut object_value = serde_json::to_value(entry("unsupported-object-delete")).unwrap();
        object_value["operation"] = serde_json::json!({
            "ObjectDelete": {
                "bucket": "objects",
                "object": "blob.bin"
            }
        });
        fs::write(
            object.path().join("writeback.jsonl"),
            format!("{}\n", serde_json::to_string(&object_value).unwrap()),
        )
        .unwrap();

        assert!(WritebackQueue::open(object.path(), 4).is_err());
    }

    #[test]
    fn writeback_rejects_complete_final_non_newline_schema_violations() {
        let mut missing_nuid = serde_json::to_value(QueueEntry {
            idempotency_key: "missing-nuid".into(),
            version: VersionStamp::at(std::time::UNIX_EPOCH),
            operation: QueueOperation::ObjectRenameComplete {
                from_bucket: "objects".into(),
                from_object: "old.bin".into(),
                expected_from_sequence: 7,
                expected_from_nuid: "old-nuid".into(),
                to_bucket: "objects".into(),
                to_object: "new.bin".into(),
                bytes: b"blob".to_vec(),
            },
            attempts: 0,
            done: false,
        })
        .unwrap();
        missing_nuid
            .get_mut("operation")
            .and_then(|operation| operation.get_mut("ObjectRenameComplete"))
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("expected_from_nuid");

        let mut missing_progress =
            serde_json::to_value(jsonl_entry("missing-progress", 0)).unwrap();
        missing_progress
            .get_mut("operation")
            .and_then(|operation| operation.get_mut("PublishJsonLines"))
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("applied_lines");

        let mut unsupported_delete = serde_json::to_value(entry("unsupported-delete")).unwrap();
        unsupported_delete["operation"] = serde_json::json!({
            "ObjectDelete": {
                "bucket": "objects",
                "object": "blob.bin"
            }
        });

        for invalid_record in [missing_nuid, missing_progress, unsupported_delete] {
            let tmp = tempfile::tempdir().unwrap();
            fs::write(
                tmp.path().join("writeback.jsonl"),
                serde_json::to_string(&invalid_record).unwrap(),
            )
            .unwrap();

            assert!(WritebackQueue::open(tmp.path(), 4).is_err());
        }
    }

    #[test]
    fn writeback_persists_with_atomic_replacement_file_and_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        queue.enqueue(entry("a")).unwrap();

        let content = fs::read_to_string(tmp.path().join("writeback.jsonl")).unwrap();
        assert!(content.ends_with('\n'));
        assert!(!tmp.path().join("writeback.jsonl.tmp").exists());

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.state.pending, 1);
        assert_eq!(snapshot.pending.len(), 1);
        assert_eq!(snapshot.pending[0].idempotency_key, "a");
        assert_eq!(snapshot.pending[0].operation_kind, "kv_put");
        assert_eq!(snapshot.pending[0].target, "/kv/b/k");
        assert_eq!(snapshot.pending[0].bytes_len, 1);
        assert_eq!(snapshot.pending[0].attempts, 0);
    }

    #[test]
    fn writeback_snapshot_encodes_jsonl_subject_targets() {
        let tmp = tempfile::tempdir().unwrap();
        let mut queue = WritebackQueue::open(tmp.path(), 4).unwrap();
        queue
            .enqueue(QueueEntry {
                idempotency_key: "jsonl".into(),
                version: VersionStamp::at(std::time::UNIX_EPOCH),
                operation: QueueOperation::PublishJsonLines {
                    stream: "ORDERS".into(),
                    subject: "orders/shipped@v1".into(),
                    bytes: br#"{"id":1}"#.to_vec(),
                    applied_lines: 0,
                },
                attempts: 0,
                done: false,
            })
            .unwrap();

        let snapshot = queue.snapshot();

        assert_eq!(
            snapshot.pending[0].target,
            "/streams/ORDERS/subjects/__eventfs_subject_hex_6f72646572732f73686970706564407631.jsonl"
        );
    }
}
