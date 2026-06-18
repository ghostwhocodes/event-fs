use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use eventfs_protocol::{
    is_reserved_kv_key, json_lines, stream_subject_file_name_from_str, validate_json_lines,
    AGENTS_STREAM,
};

use crate::{TransportError, TransportResult};

use super::{
    DirectoryEntry, EntryKind, KeyRevision, MountStorage, ObjectVersion, ReplayStorage,
    StreamMessageView,
};

#[derive(Clone, Debug, Default)]
pub struct MemoryStorage {
    state: Arc<Mutex<MemoryStorageState>>,
}

#[derive(Clone, Debug, Default)]
struct MemoryStorageState {
    kv_buckets: BTreeSet<String>,
    kv: BTreeMap<(String, String), KeyRevision>,
    kv_history: BTreeMap<(String, String), Vec<KeyRevision>>,
    kv_applied: BTreeMap<(String, String, String), u64>,
    streams: BTreeMap<String, Vec<StreamMessageView>>,
    stream_applied: BTreeMap<(String, String, String), Vec<u64>>,
    object_buckets: BTreeSet<String>,
    objects: BTreeMap<(String, String), ObjectVersion>,
    object_generations: BTreeMap<(String, String), u64>,
    object_applied: BTreeSet<(String, String, String)>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn kv_key(bucket: &str, key: &str) -> (String, String) {
        (bucket.to_string(), key.to_string())
    }

    fn stream_key(stream: &str, subject: &str, idempotency_seed: &str) -> (String, String, String) {
        (
            stream.to_string(),
            subject.to_string(),
            idempotency_seed.to_string(),
        )
    }

    fn object_key(bucket: &str, object: &str) -> (String, String) {
        (bucket.to_string(), object.to_string())
    }

    fn object_applied_key(
        bucket: &str,
        object: &str,
        idempotency_key: &str,
    ) -> (String, String, String) {
        (
            bucket.to_string(),
            object.to_string(),
            idempotency_key.to_string(),
        )
    }

    fn put_kv(state: &mut MemoryStorageState, bucket: &str, key: &str, bytes: &[u8]) -> u64 {
        state.kv_buckets.insert(bucket.to_string());
        let map_key = Self::kv_key(bucket, key);
        let revision = state
            .kv_history
            .get(&map_key)
            .and_then(|history| history.last())
            .map(|entry| entry.revision.saturating_add(1))
            .unwrap_or(1);
        let entry = KeyRevision {
            revision,
            created: SystemTime::now(),
            bytes: bytes.to_vec(),
        };
        state.kv.insert(map_key.clone(), entry.clone());
        state.kv_history.entry(map_key).or_default().push(entry);
        revision
    }

    fn put_object(state: &mut MemoryStorageState, bucket: &str, object: &str, bytes: &[u8]) -> u64 {
        state.object_buckets.insert(bucket.to_string());
        let map_key = Self::object_key(bucket, object);
        let generation = state.object_generations.entry(map_key.clone()).or_insert(0);
        let sequence = generation.saturating_add(1);
        *generation = sequence;
        state.objects.insert(
            map_key,
            ObjectVersion {
                modified: SystemTime::now(),
                sequence,
                nuid: format!("memory-object-{sequence}"),
                bytes: bytes.to_vec(),
            },
        );
        sequence
    }
}

fn immediate_children<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    prefix: &str,
) -> Vec<DirectoryEntry> {
    let normalized_prefix = prefix.trim_matches('/');
    let prefix_with_sep = (!normalized_prefix.is_empty()).then(|| format!("{normalized_prefix}/"));
    let mut entries = Vec::new();
    for key in keys {
        let remainder = match &prefix_with_sep {
            Some(prefix) => key.strip_prefix(prefix.as_str()),
            None => Some(key),
        };
        let Some(remainder) = remainder else {
            continue;
        };
        if remainder.is_empty() {
            continue;
        }
        match remainder.split_once('/') {
            Some((name, _)) => {
                merge_child_entry(&mut entries, name.to_string(), EntryKind::Directory)
            }
            None => merge_child_entry(&mut entries, remainder.to_string(), EntryKind::File),
        }
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    entries
}

fn immediate_history_children<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    prefix: &str,
) -> Vec<DirectoryEntry> {
    immediate_children(keys, prefix)
        .into_iter()
        .map(|mut entry| {
            if entry.kind == EntryKind::File {
                entry.kind = EntryKind::Directory;
            }
            entry
        })
        .collect()
}

fn merge_child_entry(entries: &mut Vec<DirectoryEntry>, name: String, kind: EntryKind) {
    if let Some(existing) = entries.iter_mut().find(|entry| entry.name == name) {
        if matches!(
            (existing.kind, kind),
            (EntryKind::Directory, EntryKind::File)
        ) {
            existing.kind = EntryKind::File;
        }
        return;
    }
    entries.push(DirectoryEntry { name, kind });
}

impl ReplayStorage for MemoryStorage {
    fn kv_put_idempotent(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<u64> {
        let mut state = self.state.lock().unwrap();
        let applied_key = (
            bucket.to_string(),
            key.to_string(),
            idempotency_key.to_string(),
        );
        if let Some(revision) = state.kv_applied.get(&applied_key).copied() {
            return Ok(revision);
        }
        let revision = Self::put_kv(&mut state, bucket, key, bytes);
        state.kv_applied.insert(applied_key, revision);
        Ok(revision)
    }

    fn kv_put_applied(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<bool> {
        Ok(self.state.lock().unwrap().kv_applied.contains_key(&(
            bucket.to_string(),
            key.to_string(),
            idempotency_key.to_string(),
        )))
    }

    fn kv_delete_if_revision(
        &self,
        bucket: &str,
        key: &str,
        expected_revision: u64,
    ) -> TransportResult<()> {
        let mut state = self.state.lock().unwrap();
        let map_key = Self::kv_key(bucket, key);
        match state.kv.get(&map_key) {
            None => Ok(()),
            Some(entry) if entry.revision != expected_revision => Ok(()),
            Some(_) => {
                state.kv.remove(&map_key);
                Ok(())
            }
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
        validate_json_lines(subject, bytes)
            .map_err(|err| TransportError::Invalid(err.to_string()))?;
        let lines =
            json_lines(subject, bytes).map_err(|err| TransportError::Invalid(err.to_string()))?;
        let mut state = self.state.lock().unwrap();
        let applied_key = Self::stream_key(stream, subject, idempotency_seed);
        if let Some(sequences) = state.stream_applied.get(&applied_key) {
            return Ok(sequences.clone());
        }
        let messages = state.streams.entry(stream.to_string()).or_default();
        let mut sequences = Vec::new();
        for line in lines {
            let sequence = messages
                .last()
                .map(|message| message.sequence.saturating_add(1))
                .unwrap_or(1);
            messages.push(StreamMessageView {
                stream: stream.to_string(),
                sequence,
                published: SystemTime::now(),
                subject: subject.to_string(),
                payload: line.into_bytes(),
            });
            sequences.push(sequence);
        }
        state.stream_applied.insert(applied_key, sequences.clone());
        Ok(sequences)
    }

    fn publish_json_lines_applied(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<bool> {
        validate_json_lines(subject, bytes)
            .map_err(|err| TransportError::Invalid(err.to_string()))?;
        Ok(self
            .state
            .lock()
            .unwrap()
            .stream_applied
            .contains_key(&Self::stream_key(stream, subject, idempotency_seed)))
    }

    fn publish_json_lines_applied_prefix(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<usize> {
        validate_json_lines(subject, bytes)
            .map_err(|err| TransportError::Invalid(err.to_string()))?;
        Ok(self
            .state
            .lock()
            .unwrap()
            .stream_applied
            .get(&Self::stream_key(stream, subject, idempotency_seed))
            .map(Vec::len)
            .unwrap_or(0))
    }

    fn object_put_idempotent(
        &self,
        bucket: &str,
        object: &str,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<()> {
        let mut state = self.state.lock().unwrap();
        let applied_key = Self::object_applied_key(bucket, object, idempotency_key);
        if state.object_applied.contains(&applied_key) {
            return Ok(());
        }
        Self::put_object(&mut state, bucket, object, bytes);
        state.object_applied.insert(applied_key);
        Ok(())
    }

    fn object_put_applied(
        &self,
        bucket: &str,
        object: &str,
        idempotency_key: &str,
    ) -> TransportResult<bool> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .object_applied
            .contains(&Self::object_applied_key(bucket, object, idempotency_key)))
    }

    fn object_delete_if_sequence(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<()> {
        let mut state = self.state.lock().unwrap();
        let map_key = Self::object_key(bucket, object);
        match state.objects.get(&map_key) {
            None => Ok(()),
            Some(entry)
                if entry.sequence != expected_sequence
                    || (!expected_nuid.is_empty() && entry.nuid != expected_nuid) =>
            {
                Ok(())
            }
            Some(_) => {
                state.objects.remove(&map_key);
                Ok(())
            }
        }
    }

    fn object_delete_if_sequence_applied(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<bool> {
        Ok(
            match self
                .state
                .lock()
                .unwrap()
                .objects
                .get(&Self::object_key(bucket, object))
            {
                None => true,
                Some(entry) => {
                    entry.sequence != expected_sequence
                        || (!expected_nuid.is_empty() && entry.nuid != expected_nuid)
                }
            },
        )
    }
}

impl MountStorage for MemoryStorage {
    fn list_kv_buckets(&self) -> TransportResult<Vec<String>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .kv_buckets
            .iter()
            .cloned()
            .collect())
    }

    fn ensure_kv_bucket(&self, bucket: &str) -> TransportResult<()> {
        self.state
            .lock()
            .unwrap()
            .kv_buckets
            .insert(bucket.to_string());
        Ok(())
    }

    fn list_kv_prefix(&self, bucket: &str, prefix: &str) -> TransportResult<Vec<DirectoryEntry>> {
        let state = self.state.lock().unwrap();
        if !state.kv_buckets.contains(bucket) {
            return Err(TransportError::NotFound);
        }
        let keys = state
            .kv
            .keys()
            .filter_map(|(entry_bucket, key)| (entry_bucket == bucket).then_some(key.as_str()));
        Ok(immediate_children(keys, prefix))
    }

    fn kv_get(&self, bucket: &str, key: &str) -> TransportResult<Option<KeyRevision>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .kv
            .get(&Self::kv_key(bucket, key))
            .cloned())
    }

    fn kv_put(&self, bucket: &str, key: &str, bytes: &[u8]) -> TransportResult<u64> {
        Ok(Self::put_kv(
            &mut self.state.lock().unwrap(),
            bucket,
            key,
            bytes,
        ))
    }

    fn kv_delete(&self, bucket: &str, key: &str) -> TransportResult<()> {
        match self
            .state
            .lock()
            .unwrap()
            .kv
            .remove(&Self::kv_key(bucket, key))
        {
            Some(_) => Ok(()),
            None => Err(TransportError::NotFound),
        }
    }

    fn kv_history(&self, bucket: &str, key: &str) -> TransportResult<Vec<KeyRevision>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .kv_history
            .get(&Self::kv_key(bucket, key))
            .cloned()
            .unwrap_or_default())
    }

    fn list_kv_history_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> TransportResult<Vec<DirectoryEntry>> {
        let state = self.state.lock().unwrap();
        if !state.kv_buckets.contains(bucket) {
            return Err(TransportError::NotFound);
        }
        let keys = state
            .kv_history
            .keys()
            .filter_map(|(entry_bucket, key)| (entry_bucket == bucket).then_some(key.as_str()));
        Ok(immediate_history_children(keys, prefix))
    }

    fn kv_revision(
        &self,
        bucket: &str,
        key: &str,
        revision: u64,
    ) -> TransportResult<Option<KeyRevision>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .kv_history
            .get(&Self::kv_key(bucket, key))
            .and_then(|history| {
                history
                    .iter()
                    .find(|entry| entry.revision == revision)
                    .cloned()
            }))
    }

    fn list_streams(&self) -> TransportResult<Vec<String>> {
        Ok(self.state.lock().unwrap().streams.keys().cloned().collect())
    }

    fn ensure_stream(&self, stream: &str) -> TransportResult<()> {
        self.state
            .lock()
            .unwrap()
            .streams
            .entry(stream.to_string())
            .or_default();
        Ok(())
    }

    fn list_stream_messages(&self, stream: &str) -> TransportResult<Vec<DirectoryEntry>> {
        let state = self.state.lock().unwrap();
        let Some(messages) = state.streams.get(stream) else {
            return Err(TransportError::NotFound);
        };
        Ok(messages
            .iter()
            .map(|message| DirectoryEntry {
                name: format!("{}.json", message.sequence),
                kind: EntryKind::File,
            })
            .collect())
    }

    fn list_stream_subjects(&self, stream: &str) -> TransportResult<Vec<DirectoryEntry>> {
        let state = self.state.lock().unwrap();
        let Some(messages) = state.streams.get(stream) else {
            return Err(TransportError::NotFound);
        };
        Ok(messages
            .iter()
            .map(|message| message.subject.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|subject| DirectoryEntry {
                name: stream_subject_file_name_from_str(subject),
                kind: EntryKind::File,
            })
            .collect())
    }

    fn list_agent_names(&self) -> TransportResult<Vec<String>> {
        let state = self.state.lock().unwrap();
        let mut agents = BTreeSet::new();
        for message in state.streams.get(AGENTS_STREAM).into_iter().flatten() {
            let mut parts = message.subject.split('.');
            if matches!(parts.next(), Some("agents")) {
                if let (Some(agent), Some(area), None) = (parts.next(), parts.next(), parts.next())
                {
                    if matches!(area, "inbox" | "outbox") && !is_reserved_kv_key(agent) {
                        agents.insert(agent.to_string());
                    }
                }
            }
        }
        Ok(agents.into_iter().collect())
    }

    fn stream_message(&self, stream: &str, sequence: u64) -> TransportResult<StreamMessageView> {
        self.state
            .lock()
            .unwrap()
            .streams
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
        Ok(self
            .state
            .lock()
            .unwrap()
            .object_buckets
            .iter()
            .cloned()
            .collect())
    }

    fn ensure_object_bucket(&self, bucket: &str) -> TransportResult<()> {
        self.state
            .lock()
            .unwrap()
            .object_buckets
            .insert(bucket.to_string());
        Ok(())
    }

    fn list_object_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> TransportResult<Vec<DirectoryEntry>> {
        let state = self.state.lock().unwrap();
        if !state.object_buckets.contains(bucket) {
            return Err(TransportError::NotFound);
        }
        let keys = state.objects.keys().filter_map(|(entry_bucket, object)| {
            (entry_bucket == bucket).then_some(object.as_str())
        });
        Ok(immediate_children(keys, prefix))
    }

    fn object_get(&self, bucket: &str, object: &str) -> TransportResult<Option<ObjectVersion>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .objects
            .get(&Self::object_key(bucket, object))
            .cloned())
    }

    fn object_put(&self, bucket: &str, object: &str, bytes: &[u8]) -> TransportResult<()> {
        Self::put_object(&mut self.state.lock().unwrap(), bucket, object, bytes);
        Ok(())
    }

    fn object_delete(&self, bucket: &str, object: &str) -> TransportResult<()> {
        match self
            .state
            .lock()
            .unwrap()
            .objects
            .remove(&Self::object_key(bucket, object))
        {
            Some(_) => Ok(()),
            None => Err(TransportError::NotFound),
        }
    }
}
