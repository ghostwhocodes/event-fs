use nats::header::HeaderMap;
use nats::jetstream::{PublishOptions, StorageType, StreamInfo, SubscribeOptions};
use nats::Message;
use serde::{Deserialize, Serialize};

use eventfs_protocol::{
    is_reserved_kv_key, KV_APPLIED_MARKER_KEY_PREFIX, KV_WRITEBACK_MARKER_KEY_PREFIX,
};

use crate::{TransportError, TransportResult};

use super::super::{DirectoryEntry, KeyRevision};
use super::core::{is_already_exists, is_not_found, is_timeout, store_lookup_error, NatsCore};
use super::ledger::{scoped_key, WritebackLedger};
use super::{
    hex_encode, immediate_children, immediate_history_children, system_time_from_datetime,
};

const WRITEBACK_IDEMPOTENCY_HEADER: &str = "EventFS-Idempotency-Key";
const WRITEBACK_MARKER_OPERATION: &str = "kv-writeback-marker";
const APPLIED_MARKER_OPERATION: &str = "kv-applied-marker";
const OPERATION_HEADER: &str = "KV-Operation";
const OPERATION_DELETE: &str = "DEL";
const OPERATION_PURGE: &str = "PURGE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum KvDeleteRevision {
    Deleted,
    Missing,
    RevisionChanged,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct KvWritebackMarker {
    operation: String,
    idempotency_key: String,
    key: String,
}

#[derive(Clone)]
pub(super) struct NatsKv {
    core: NatsCore,
    ledger: WritebackLedger,
}

impl NatsKv {
    pub(super) fn new(core: NatsCore, ledger: WritebackLedger) -> Self {
        Self { core, ledger }
    }

    pub(super) fn list_buckets(&self) -> TransportResult<Vec<String>> {
        let mut buckets = Vec::new();
        for stream_name in self.core.context.stream_names() {
            let stream_name = stream_name?;
            let Some(bucket) = bucket_name_from_stream(&stream_name) else {
                continue;
            };
            match self.core.stream_info(&stream_name) {
                Ok(info) if stream_looks_like_bucket(&info, bucket) => {
                    buckets.push(bucket.to_string());
                }
                Ok(_) | Err(TransportError::NotFound) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(buckets)
    }

    pub(super) fn ensure_bucket(&self, bucket: &str) -> TransportResult<()> {
        match self.store(bucket) {
            Ok(_) => return Ok(()),
            Err(TransportError::NotFound) => {}
            Err(err) => return Err(err),
        }
        match self.core.retry_io(|| {
            self.core.context.create_key_value(&nats::kv::Config {
                bucket: bucket.to_string(),
                history: self.core.config.kv_history,
                storage: StorageType::File,
                ..Default::default()
            })
        }) {
            Ok(_) => {}
            Err(err) if is_already_exists(&err) => {}
            Err(err) => return Err(err),
        }
        Ok(())
    }

    pub(super) fn list_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> TransportResult<Vec<DirectoryEntry>> {
        let store = self.store(bucket)?;
        let keys = store
            .keys()?
            .filter(|key| !is_writeback_marker_key(key))
            .collect::<Vec<_>>();
        Ok(immediate_children(keys.iter().map(String::as_str), prefix))
    }

    pub(super) fn get(&self, bucket: &str, key: &str) -> TransportResult<Option<KeyRevision>> {
        let store = match self.store(bucket) {
            Ok(store) => store,
            Err(TransportError::NotFound) => return Ok(None),
            Err(err) => return Err(err),
        };
        match store.entry(key)? {
            Some(entry) if matches!(entry.operation, nats::kv::Operation::Put) => {
                Ok(Some(KeyRevision {
                    revision: entry.revision,
                    created: system_time_from_datetime(entry.created)?,
                    bytes: entry.value,
                }))
            }
            _ => Ok(None),
        }
    }

    pub(super) fn put(&self, bucket: &str, key: &str, bytes: &[u8]) -> TransportResult<u64> {
        let store = self.store_or_create(bucket)?;
        self.core.retry_io(|| store.put(key, bytes))
    }

    pub(super) fn delete(&self, bucket: &str, key: &str) -> TransportResult<()> {
        retry_plain_kv_delete(
            self.core.config.retries,
            || Ok(self.get(bucket, key)?.map(|entry| entry.revision)),
            |revision| self.delete_revision_once(bucket, key, revision),
        )
    }

    pub(super) fn history(&self, bucket: &str, key: &str) -> TransportResult<Vec<KeyRevision>> {
        let _store = self.store(bucket)?;
        let stream = stream_name(bucket);
        let subject = subject(bucket, key);
        match self.core.context.get_last_message(&stream, &subject) {
            Ok(_) => {}
            Err(err) if is_not_found(&err) => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        }
        let sub = self.core.context.subscribe_with_options(
            &subject,
            &SubscribeOptions::bind_stream(stream).deliver_all(),
        )?;
        let mut revisions = Vec::new();
        loop {
            let message = match sub.next_timeout(self.core.config.timeout) {
                Ok(message) => message,
                Err(err) if is_timeout(&err) => break,
                Err(err) => return Err(err.into()),
            };
            let complete = message
                .jetstream_message_info()
                .map(|info| info.pending == 0)
                .unwrap_or(false);
            if message_is_put(message.headers.as_ref()) {
                if let Some(revision) = message_to_key_revision(message) {
                    revisions.push(revision);
                }
            }
            if complete {
                break;
            }
        }
        Ok(revisions)
    }

    pub(super) fn list_history_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> TransportResult<Vec<DirectoryEntry>> {
        let _store = self.store(bucket)?;
        let subject = format!("$KV.{bucket}.>");
        let sub = self.core.context.subscribe_with_options(
            &subject,
            &SubscribeOptions::bind_stream(stream_name(bucket)).deliver_last_per_subject(),
        )?;
        let mut keys = Vec::new();
        loop {
            let message = match sub.next_timeout(self.core.config.timeout) {
                Ok(message) => message,
                Err(err) if keys.is_empty() && is_timeout(&err) => break,
                Err(err) => return Err(err.into()),
            };
            let complete = message
                .jetstream_message_info()
                .map(|info| info.pending == 0)
                .unwrap_or(false);
            if let Some((entry_bucket, key)) = subject_parts(&message.subject) {
                if entry_bucket == bucket && !is_writeback_marker_key(key) {
                    keys.push(key.to_string());
                }
            }
            if complete {
                break;
            }
        }
        Ok(immediate_history_children(
            keys.iter().map(String::as_str),
            prefix,
        ))
    }

    pub(super) fn revision(
        &self,
        bucket: &str,
        key: &str,
        revision: u64,
    ) -> TransportResult<Option<KeyRevision>> {
        Ok(self
            .history(bucket, key)?
            .into_iter()
            .find(|entry| entry.revision == revision))
    }

    pub(super) fn put_idempotent(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<u64> {
        let ledger_key = writeback_ledger_key(bucket, key, idempotency_key);
        if let Some(revision) = self.any_applied_revision(bucket, key, idempotency_key)? {
            return Ok(revision);
        }
        self.ensure_bucket(bucket)?;
        let stream = stream_name(bucket);
        self.core.ensure_duplicate_window(&stream)?;
        self.ensure_apply_marker(bucket, key, idempotency_key)?;

        let mut headers = HeaderMap::default();
        headers.insert(WRITEBACK_IDEMPOTENCY_HEADER, idempotency_key.to_string());
        let subject = subject(bucket, key);
        let message = Message::new(&subject, None, bytes, Some(headers));
        let ack = self.core.retry_io(|| {
            self.core.context.publish_message_with_options(
                &message,
                &PublishOptions {
                    timeout: Some(self.core.config.timeout),
                    id: Some(idempotency_key.to_string()),
                    expected_stream: Some(stream.clone()),
                    ..Default::default()
                },
            )
        })?;
        self.record_applied_marker(bucket, key, idempotency_key)?;
        self.ledger.record_applied(&ledger_key)?;
        Ok(ack.sequence)
    }

    pub(super) fn put_applied(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<bool> {
        Ok(self
            .any_applied_revision(bucket, key, idempotency_key)?
            .is_some())
    }

    pub(super) fn delete_if_revision(
        &self,
        bucket: &str,
        key: &str,
        expected_revision: u64,
    ) -> TransportResult<()> {
        match self.get(bucket, key)? {
            None => return Ok(()),
            Some(entry) if entry.revision != expected_revision => return Ok(()),
            Some(_) => {}
        }

        match self.delete_revision_once(bucket, key, expected_revision)? {
            KvDeleteRevision::Deleted | KvDeleteRevision::Missing => Ok(()),
            KvDeleteRevision::RevisionChanged => Ok(()),
        }
    }

    pub(super) fn delete_if_revision_applied(
        &self,
        bucket: &str,
        key: &str,
        expected_revision: u64,
    ) -> TransportResult<bool> {
        Ok(match self.get(bucket, key)? {
            None => true,
            Some(entry) => entry.revision != expected_revision,
        })
    }

    pub(super) fn watch_once(
        &self,
        bucket: &str,
        key: &str,
        timeout: std::time::Duration,
    ) -> TransportResult<Option<KeyRevision>> {
        self.ensure_bucket(bucket)?;
        let subject = format!("$KV.{bucket}.{key}");
        let sub = self.core.context.subscribe_with_options(
            &subject,
            &SubscribeOptions::bind_stream(format!("KV_{bucket}")).deliver_new(),
        )?;
        match sub.next_timeout(timeout) {
            Ok(message) => Ok(message_to_key_revision(message)),
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn store(&self, bucket: &str) -> TransportResult<nats::kv::Store> {
        self.core
            .context
            .key_value(bucket)
            .map_err(store_lookup_error)
    }

    fn store_or_create(&self, bucket: &str) -> TransportResult<nats::kv::Store> {
        match self.store(bucket) {
            Ok(store) => Ok(store),
            Err(TransportError::NotFound) => self.ensure_bucket(bucket).and_then(|_| {
                self.core
                    .context
                    .key_value(bucket)
                    .map_err(TransportError::from)
            }),
            Err(err) => Err(err),
        }
    }

    fn delete_revision_once(
        &self,
        bucket: &str,
        key: &str,
        expected_revision: u64,
    ) -> TransportResult<KvDeleteRevision> {
        let stream = stream_name(bucket);
        let subject = subject(bucket, key);
        let mut headers = HeaderMap::default();
        headers.insert(OPERATION_HEADER, OPERATION_DELETE.to_string());
        let message = Message::new(&subject, None, b"", Some(headers));
        match self.core.retry_io(|| {
            self.core.context.publish_message_with_options(
                &message,
                &PublishOptions {
                    timeout: Some(self.core.config.timeout),
                    expected_stream: Some(stream.clone()),
                    expected_last_subject_sequence: Some(expected_revision),
                    ..Default::default()
                },
            )
        }) {
            Ok(_) => Ok(KvDeleteRevision::Deleted),
            Err(err) => match self.get(bucket, key)? {
                None => Ok(KvDeleteRevision::Missing),
                Some(entry) if entry.revision != expected_revision => {
                    Ok(KvDeleteRevision::RevisionChanged)
                }
                Some(_) => Err(err),
            },
        }
    }

    fn latest_applied_revision(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<Option<u64>> {
        let stream = stream_name(bucket);
        let subject = subject(bucket, key);
        match self.core.context.get_last_message(&stream, &subject) {
            Ok(message)
                if headers_contain_idempotency_key(message.headers.as_ref(), idempotency_key) =>
            {
                Ok(Some(message.sequence))
            }
            Ok(_) => Ok(None),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn target_sequence_after_apply_marker(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<Option<u64>> {
        let Some(marker_sequence) = self.apply_marker_sequence(bucket, key, idempotency_key)?
        else {
            return Ok(None);
        };
        let stream = stream_name(bucket);
        let subject = subject(bucket, key);
        match self.core.context.get_last_message(&stream, &subject) {
            Ok(message) if message.sequence > marker_sequence => Ok(Some(message.sequence)),
            Ok(_) => Ok(None),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn apply_marker_sequence(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<Option<u64>> {
        match self.core.context.get_last_message(
            stream_name(bucket),
            &writeback_marker_subject(bucket, idempotency_key),
        ) {
            Ok(message)
                if marker_matches(
                    &message.data,
                    WRITEBACK_MARKER_OPERATION,
                    key,
                    idempotency_key,
                )? =>
            {
                Ok(Some(message.sequence))
            }
            Ok(_) => Ok(None),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn ensure_apply_marker(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<u64> {
        if let Some(sequence) = self.apply_marker_sequence(bucket, key, idempotency_key)? {
            return Ok(sequence);
        }
        self.ensure_bucket(bucket)?;
        let stream = stream_name(bucket);
        self.core.ensure_duplicate_window(&stream)?;
        let subject = writeback_marker_subject(bucket, idempotency_key);
        let payload = marker_payload(WRITEBACK_MARKER_OPERATION, key, idempotency_key)?;
        let ack = self.core.retry_io(|| {
            self.core.context.publish_with_options(
                &subject,
                payload.clone(),
                &PublishOptions {
                    timeout: Some(self.core.config.timeout),
                    id: Some(format!("writeback-marker:{idempotency_key}")),
                    expected_stream: Some(stream.clone()),
                    ..Default::default()
                },
            )
        })?;
        Ok(ack.sequence)
    }

    fn applied_marker_sequence(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<Option<u64>> {
        match self.core.context.get_last_message(
            stream_name(bucket),
            &applied_marker_subject(bucket, idempotency_key),
        ) {
            Ok(message)
                if marker_matches(
                    &message.data,
                    APPLIED_MARKER_OPERATION,
                    key,
                    idempotency_key,
                )? =>
            {
                Ok(Some(message.sequence))
            }
            Ok(_) => Ok(None),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn record_applied_marker(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<u64> {
        if let Some(sequence) = self.applied_marker_sequence(bucket, key, idempotency_key)? {
            return Ok(sequence);
        }
        self.ensure_bucket(bucket)?;
        let stream = stream_name(bucket);
        self.core.ensure_duplicate_window(&stream)?;
        let subject = applied_marker_subject(bucket, idempotency_key);
        let payload = marker_payload(APPLIED_MARKER_OPERATION, key, idempotency_key)?;
        let ack = self.core.retry_io(|| {
            self.core.context.publish_with_options(
                &subject,
                payload.clone(),
                &PublishOptions {
                    timeout: Some(self.core.config.timeout),
                    id: Some(format!("writeback-applied-marker:{idempotency_key}")),
                    expected_stream: Some(stream.clone()),
                    ..Default::default()
                },
            )
        })?;
        Ok(ack.sequence)
    }

    fn any_applied_revision(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<Option<u64>> {
        let ledger_key = writeback_ledger_key(bucket, key, idempotency_key);
        if self.ledger.has_applied(&ledger_key)? {
            return Ok(Some(0));
        }
        if let Some(sequence) = self.applied_marker_sequence(bucket, key, idempotency_key)? {
            self.ledger.record_applied(&ledger_key)?;
            return Ok(Some(sequence));
        }
        if let Some(revision) = self.latest_applied_revision(bucket, key, idempotency_key)? {
            self.ledger.record_applied(&ledger_key)?;
            return Ok(Some(revision));
        }
        if let Some(sequence) =
            self.target_sequence_after_apply_marker(bucket, key, idempotency_key)?
        {
            self.ledger.record_applied(&ledger_key)?;
            return Ok(Some(sequence));
        }

        let stream = stream_name(bucket);
        let subject = subject(bucket, key);
        let applied_revision =
            self.core
                .scan_subject_history_until(&stream, &subject, |message| {
                    if !headers_contain_idempotency_key(message.headers.as_ref(), idempotency_key) {
                        return Ok(None);
                    }
                    Ok(Some(message_stream_sequence(message)?))
                })?;
        if applied_revision.is_some() {
            self.ledger.record_applied(&ledger_key)?;
        }
        Ok(applied_revision)
    }
}

pub(super) fn subject_parts(subject: &str) -> Option<(&str, &str)> {
    let rest = subject.strip_prefix("$KV.")?;
    rest.split_once('.')
}

pub(super) fn stream_looks_like_bucket(stream_info: &StreamInfo, bucket: &str) -> bool {
    stream_info.config.max_msgs_per_subject >= 1
        && stream_info
            .config
            .subjects
            .iter()
            .any(|subject| subject == &format!("$KV.{bucket}.>"))
}

pub(super) fn retry_plain_kv_delete(
    retries: usize,
    mut current_revision: impl FnMut() -> TransportResult<Option<u64>>,
    mut delete_revision: impl FnMut(u64) -> TransportResult<KvDeleteRevision>,
) -> TransportResult<()> {
    let attempts = retries.saturating_add(1);
    for attempt in 0..attempts {
        let Some(revision) = current_revision()? else {
            return Err(TransportError::NotFound);
        };
        match delete_revision(revision)? {
            KvDeleteRevision::Deleted => return Ok(()),
            KvDeleteRevision::Missing => return Err(TransportError::NotFound),
            KvDeleteRevision::RevisionChanged if attempt + 1 < attempts => {}
            KvDeleteRevision::RevisionChanged => {
                return Err(TransportError::Invalid(format!(
                    "KV delete conflict after {attempts} attempts"
                )));
            }
        }
    }
    Err(TransportError::Invalid(
        "KV delete conflict with no attempts configured".into(),
    ))
}

fn stream_name(bucket: &str) -> String {
    format!("KV_{bucket}")
}

fn bucket_name_from_stream(stream_name: &str) -> Option<&str> {
    stream_name.strip_prefix("KV_")
}

fn subject(bucket: &str, key: &str) -> String {
    format!("$KV.{bucket}.{key}")
}

fn writeback_marker_key(idempotency_key: &str) -> String {
    format!(
        "{KV_WRITEBACK_MARKER_KEY_PREFIX}.{}",
        hex_encode(idempotency_key.as_bytes())
    )
}

fn writeback_marker_subject(bucket: &str, idempotency_key: &str) -> String {
    subject(bucket, &writeback_marker_key(idempotency_key))
}

fn applied_marker_key(idempotency_key: &str) -> String {
    format!(
        "{KV_APPLIED_MARKER_KEY_PREFIX}.{}",
        hex_encode(idempotency_key.as_bytes())
    )
}

fn applied_marker_subject(bucket: &str, idempotency_key: &str) -> String {
    subject(bucket, &applied_marker_key(idempotency_key))
}

fn is_writeback_marker_key(key: &str) -> bool {
    is_reserved_kv_key(key)
}

fn marker_payload(operation: &str, key: &str, idempotency_key: &str) -> TransportResult<Vec<u8>> {
    Ok(serde_json::to_vec(&KvWritebackMarker {
        operation: operation.to_string(),
        idempotency_key: idempotency_key.to_string(),
        key: key.to_string(),
    })?)
}

fn marker_matches(
    data: &[u8],
    operation: &str,
    key: &str,
    idempotency_key: &str,
) -> TransportResult<bool> {
    let Ok(marker) = serde_json::from_slice::<KvWritebackMarker>(data) else {
        return Ok(false);
    };
    Ok(marker.operation == operation
        && marker.key == key
        && marker.idempotency_key == idempotency_key)
}

fn message_is_put(headers: Option<&HeaderMap>) -> bool {
    !headers
        .and_then(|headers| headers.get(OPERATION_HEADER))
        .is_some_and(|operation| matches!(operation.as_str(), OPERATION_DELETE | OPERATION_PURGE))
}

fn headers_contain_idempotency_key(headers: Option<&HeaderMap>, idempotency_key: &str) -> bool {
    headers
        .and_then(|headers| headers.get(WRITEBACK_IDEMPOTENCY_HEADER))
        .map(|value| value.as_str() == idempotency_key)
        .unwrap_or(false)
}

fn message_stream_sequence(message: &Message) -> TransportResult<u64> {
    message
        .jetstream_message_info()
        .map(|info| info.stream_seq)
        .ok_or_else(|| {
            TransportError::Invalid(
                "bound JetStream delivery is missing stream sequence metadata".into(),
            )
        })
}

fn writeback_ledger_key(bucket: &str, key: &str, idempotency_key: &str) -> String {
    scoped_key("kv", &[bucket, key, idempotency_key])
}

fn message_to_key_revision(message: Message) -> Option<KeyRevision> {
    message.jetstream_message_info().and_then(|info| {
        Some(KeyRevision {
            revision: info.stream_seq,
            created: system_time_from_datetime(info.published).ok()?,
            bytes: message.data.clone(),
        })
    })
}
