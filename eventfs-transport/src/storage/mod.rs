use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::cache::WatchEvent;
use crate::TransportResult;

mod memory;
mod nats;

pub use memory::MemoryStorage;
pub use nats::{NatsStorage, NatsStorageConfig};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EntryKind {
    Directory,
    File,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub kind: EntryKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyRevision {
    pub revision: u64,
    pub created: SystemTime,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamMessageView {
    pub stream: String,
    pub sequence: u64,
    pub published: SystemTime,
    pub subject: String,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectVersion {
    pub modified: SystemTime,
    pub sequence: u64,
    pub nuid: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    pub modified: SystemTime,
    pub size: u64,
    pub sequence: u64,
}

pub trait ReplayStorage: Send + Sync {
    fn kv_put_idempotent(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<u64>;
    fn kv_put_applied(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<bool>;
    fn kv_delete_if_revision(
        &self,
        bucket: &str,
        key: &str,
        expected_revision: u64,
    ) -> TransportResult<()>;
    fn kv_delete_if_revision_applied(
        &self,
        bucket: &str,
        key: &str,
        expected_revision: u64,
    ) -> TransportResult<bool>;

    fn publish_json_lines(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<Vec<u64>>;
    fn publish_json_lines_applied(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<bool>;
    fn publish_json_lines_applied_prefix(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<usize>;

    fn object_put_idempotent(
        &self,
        bucket: &str,
        object: &str,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<()>;
    fn object_put_applied(
        &self,
        bucket: &str,
        object: &str,
        idempotency_key: &str,
    ) -> TransportResult<bool>;
    fn object_delete_if_sequence(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<()>;
    fn object_delete_if_sequence_applied(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<bool>;
}

pub trait MountStorage: ReplayStorage {
    fn list_kv_buckets(&self) -> TransportResult<Vec<String>>;
    fn ensure_kv_bucket(&self, bucket: &str) -> TransportResult<()>;
    fn list_kv_prefix(&self, bucket: &str, prefix: &str) -> TransportResult<Vec<DirectoryEntry>>;
    fn kv_get(&self, bucket: &str, key: &str) -> TransportResult<Option<KeyRevision>>;
    fn kv_put(&self, bucket: &str, key: &str, bytes: &[u8]) -> TransportResult<u64>;
    fn kv_delete(&self, bucket: &str, key: &str) -> TransportResult<()>;
    fn kv_history(&self, bucket: &str, key: &str) -> TransportResult<Vec<KeyRevision>>;
    fn list_kv_history_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> TransportResult<Vec<DirectoryEntry>> {
        self.list_kv_prefix(bucket, prefix)
    }
    fn kv_revision(
        &self,
        bucket: &str,
        key: &str,
        revision: u64,
    ) -> TransportResult<Option<KeyRevision>>;

    fn list_streams(&self) -> TransportResult<Vec<String>>;
    fn ensure_stream(&self, stream: &str) -> TransportResult<()>;
    fn list_stream_messages(&self, stream: &str) -> TransportResult<Vec<DirectoryEntry>>;
    fn list_stream_subjects(&self, stream: &str) -> TransportResult<Vec<DirectoryEntry>>;
    fn list_agent_names(&self) -> TransportResult<Vec<String>> {
        Ok(Vec::new())
    }
    fn stream_message(&self, stream: &str, sequence: u64) -> TransportResult<StreamMessageView>;

    fn list_object_buckets(&self) -> TransportResult<Vec<String>>;
    fn ensure_object_bucket(&self, bucket: &str) -> TransportResult<()>;
    fn list_object_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> TransportResult<Vec<DirectoryEntry>>;
    fn object_get(&self, bucket: &str, object: &str) -> TransportResult<Option<ObjectVersion>>;
    fn object_metadata(
        &self,
        bucket: &str,
        object: &str,
    ) -> TransportResult<Option<ObjectMetadata>> {
        Ok(self
            .object_get(bucket, object)?
            .map(|entry| ObjectMetadata {
                modified: entry.modified,
                size: entry.bytes.len() as u64,
                sequence: entry.sequence,
            }))
    }
    fn object_put(&self, bucket: &str, object: &str, bytes: &[u8]) -> TransportResult<()>;
    fn object_delete(&self, bucket: &str, object: &str) -> TransportResult<()>;

    fn watch_events(&self) -> TransportResult<Vec<WatchEvent>> {
        Ok(Vec::new())
    }
}
