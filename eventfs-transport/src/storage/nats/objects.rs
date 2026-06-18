use std::io::{Cursor, Read};

use nats::header::HeaderMap;
use nats::jetstream::{PublishOptions, StorageType, StreamInfo};
use nats::object_store::ObjectMeta;
use nats::Message;
use serde::{Deserialize, Serialize};

use crate::{TransportError, TransportResult};

use super::super::{DirectoryEntry, ObjectMetadata, ObjectVersion};
use super::core::{is_already_exists, is_not_found, store_lookup_error, NatsCore};
use super::ledger::{scoped_key, WritebackLedger};
use super::{immediate_children, subject_matches, system_time_from_datetime};

const IDEMPOTENCY_DESCRIPTION_PREFIX: &str = "eventfs-idempotency:";
const NATS_ROLLUP: &str = "Nats-Rollup";
const ROLLUP_SUBJECT: &str = "sub";

#[derive(Clone)]
pub(super) struct NatsObjects {
    core: NatsCore,
    ledger: WritebackLedger,
}

impl NatsObjects {
    pub(super) fn new(core: NatsCore, ledger: WritebackLedger) -> Self {
        Self { core, ledger }
    }

    pub(super) fn list_buckets(&self) -> TransportResult<Vec<String>> {
        let mut buckets = Vec::new();
        for stream_name in self.core.context.stream_names() {
            let stream_name = stream_name?;
            let Some(bucket) = object_bucket_name_from_stream(&stream_name) else {
                continue;
            };
            match self.core.stream_info(&stream_name) {
                Ok(info) if object_stream_looks_like_bucket(&info, bucket) => {
                    buckets.push(bucket.to_string());
                }
                Ok(_) | Err(TransportError::NotFound) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(buckets)
    }

    pub(super) fn ensure_bucket(&self, bucket: &str) -> TransportResult<()> {
        match self.object_store(bucket) {
            Ok(_) => return Ok(()),
            Err(TransportError::NotFound) => {}
            Err(err) => return Err(err),
        }
        match self.core.retry_io(|| {
            self.core
                .context
                .create_object_store(&nats::object_store::Config {
                    bucket: bucket.to_string(),
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
        let _store = self.object_store(bucket)?;
        let subject = format!("$O.{bucket}.M.>");
        let sub = self.core.context.subscribe_with_options(
            &subject,
            &nats::jetstream::SubscribeOptions::ordered().deliver_last_per_subject(),
        )?;
        let mut names = Vec::new();
        loop {
            let message = match sub.next_timeout(self.core.config.timeout) {
                Ok(message) => message,
                Err(err) if names.is_empty() && super::core::is_timeout(&err) => break,
                Err(err) => return Err(err.into()),
            };
            let complete = message
                .jetstream_message_info()
                .map(|info| info.pending == 0)
                .unwrap_or(false);
            let info: nats::object_store::ObjectInfo = serde_json::from_slice(&message.data)?;
            if !info.deleted {
                names.push(info.name);
            }
            if complete {
                break;
            }
        }
        Ok(immediate_children(names.iter().map(String::as_str), prefix))
    }

    pub(super) fn get(&self, bucket: &str, object: &str) -> TransportResult<Option<ObjectVersion>> {
        let store = match self.object_store(bucket) {
            Ok(store) => store,
            Err(TransportError::NotFound) => return Ok(None),
            Err(err) => return Err(err),
        };
        let mut object_reader = match store.get(object) {
            Ok(object) => object,
            Err(err) if is_not_found(&err) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let info = object_reader.info().clone();
        if info.deleted {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        object_reader.read_to_end(&mut bytes)?;
        let sequence = match self.metadata_message(bucket, object)? {
            Some((latest, sequence)) if !latest.deleted && latest.nuid == info.nuid => sequence,
            Some(_) => {
                return Err(TransportError::Invalid(format!(
                    "object {bucket}/{object} changed while reading"
                )));
            }
            None => return Ok(None),
        };
        Ok(Some(ObjectVersion {
            modified: system_time_from_datetime(info.modified)?,
            sequence,
            nuid: info.nuid.clone(),
            bytes,
        }))
    }

    pub(super) fn metadata(
        &self,
        bucket: &str,
        object: &str,
    ) -> TransportResult<Option<ObjectMetadata>> {
        match self.metadata_message(bucket, object)? {
            Some((info, sequence)) if !info.deleted => Ok(Some(ObjectMetadata {
                modified: system_time_from_datetime(info.modified)?,
                size: info.size as u64,
                sequence,
            })),
            _ => Ok(None),
        }
    }

    pub(super) fn put(&self, bucket: &str, object: &str, bytes: &[u8]) -> TransportResult<()> {
        let store = self.object_store_or_create(bucket)?;
        self.core.retry_io(|| {
            let mut cursor = Cursor::new(bytes);
            store.put(object, &mut cursor)
        })?;
        Ok(())
    }

    pub(super) fn delete(&self, bucket: &str, object: &str) -> TransportResult<()> {
        let store = self.object_store(bucket)?;
        match store.delete(object) {
            Ok(()) => Ok(()),
            Err(err) if is_not_found(&err) => Err(TransportError::NotFound),
            Err(err) => Err(err.into()),
        }
    }

    pub(super) fn put_idempotent(
        &self,
        bucket: &str,
        object: &str,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<()> {
        let ledger_key = object_writeback_ledger_key(bucket, object, idempotency_key);
        if self.put_applied(bucket, object, idempotency_key)? {
            self.ledger.record_applied(&ledger_key)?;
            return Ok(());
        }
        let previous_nuid = match self.metadata_message(bucket, object)? {
            Some((info, _)) if !info.deleted => Some(info.nuid),
            _ => None,
        };
        self.ensure_apply_marker(bucket, idempotency_key, previous_nuid.as_deref())?;
        let store = self.object_store_or_create(bucket)?;
        let marker = object_idempotency_description(idempotency_key, previous_nuid.as_deref())?;
        self.core.retry_io(|| {
            let mut cursor = Cursor::new(bytes);
            store.put(
                ObjectMeta {
                    name: object.to_string(),
                    description: Some(marker.clone()),
                    link: None,
                },
                &mut cursor,
            )
        })?;
        self.purge_chunks_if_known(bucket, previous_nuid.as_deref().unwrap_or(""))?;
        self.ledger.record_applied(&ledger_key)?;
        Ok(())
    }

    pub(super) fn put_applied(
        &self,
        bucket: &str,
        object: &str,
        idempotency_key: &str,
    ) -> TransportResult<bool> {
        let ledger_key = object_writeback_ledger_key(bucket, object, idempotency_key);
        let marker = self.apply_marker(bucket, idempotency_key)?;
        if self.ledger.has_applied(&ledger_key)? {
            self.purge_chunks_if_known(
                bucket,
                object_put_previous_nuid(None, marker.as_ref()).unwrap_or(""),
            )?;
            return Ok(true);
        }
        let store = match self.object_store(bucket) {
            Ok(store) => Some(store),
            Err(TransportError::NotFound) => None,
            Err(err) => return Err(err),
        };
        if let Some(store) = store {
            match store.info(object) {
                Ok(info) if !info.deleted => {
                    if let Some(identity) = info
                        .description
                        .as_deref()
                        .and_then(object_writeback_identity_from_description)
                        .filter(|identity| identity.idempotency_key == idempotency_key)
                    {
                        self.purge_chunks_if_known(
                            bucket,
                            object_put_previous_nuid(Some(&identity), marker.as_ref())
                                .unwrap_or(""),
                        )?;
                        self.ledger.record_applied(&ledger_key)?;
                        return Ok(true);
                    }
                }
                Ok(_) => {}
                Err(err) if is_not_found(&err) => {}
                Err(err) => return Err(err.into()),
            }
        }
        if let Some(identity) = self.history_applied_identity(bucket, object, idempotency_key)? {
            self.purge_chunks_if_known(
                bucket,
                object_put_previous_nuid(Some(&identity), marker.as_ref()).unwrap_or(""),
            )?;
            self.ledger.record_applied(&ledger_key)?;
            return Ok(true);
        }
        if let Some(marker) =
            self.target_sequence_after_apply_marker(bucket, object, idempotency_key)?
        {
            self.purge_chunks_if_known(
                bucket,
                object_put_previous_nuid(None, Some(&marker)).unwrap_or(""),
            )?;
            self.ledger.record_applied(&ledger_key)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(super) fn delete_if_sequence(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<()> {
        let Some((mut info, current_sequence)) = self.metadata_message(bucket, object)? else {
            self.purge_chunks_if_known(bucket, expected_nuid)?;
            return Ok(());
        };
        if info.deleted {
            if expected_nuid.is_empty() {
                self.purge_chunks(bucket, &info.nuid)?;
            } else {
                self.purge_chunks(bucket, expected_nuid)?;
            }
            return Ok(());
        }
        if current_sequence != expected_sequence
            || (!expected_nuid.is_empty() && info.nuid != expected_nuid)
        {
            self.purge_chunks_if_known(bucket, expected_nuid)?;
            return Ok(());
        }

        let old_nuid = info.nuid.clone();
        info.chunks = 0;
        info.size = 0;
        info.deleted = true;

        let stream = object_stream_name(bucket);
        let subject = object_meta_subject(bucket, object);
        let data = serde_json::to_vec(&info)?;
        let mut headers = HeaderMap::default();
        headers.insert(NATS_ROLLUP, ROLLUP_SUBJECT.to_string());
        let message = Message::new(&subject, None, data, Some(headers));
        match self.core.retry_io(|| {
            self.core.context.publish_message_with_options(
                &message,
                &PublishOptions {
                    timeout: Some(self.core.config.timeout),
                    expected_stream: Some(stream.clone()),
                    expected_last_subject_sequence: Some(expected_sequence),
                    ..Default::default()
                },
            )
        }) {
            Ok(_) => {
                self.purge_chunks(bucket, &old_nuid)?;
                Ok(())
            }
            Err(err) => {
                if self.delete_superseded(bucket, object, expected_sequence, &old_nuid)? {
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    pub(super) fn delete_if_sequence_applied(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<bool> {
        self.delete_superseded(bucket, object, expected_sequence, expected_nuid)
    }

    fn object_store(&self, bucket: &str) -> TransportResult<nats::object_store::ObjectStore> {
        self.core
            .context
            .object_store(bucket)
            .map_err(store_lookup_error)
    }

    fn object_store_or_create(
        &self,
        bucket: &str,
    ) -> TransportResult<nats::object_store::ObjectStore> {
        match self.object_store(bucket) {
            Ok(store) => Ok(store),
            Err(TransportError::NotFound) => self.ensure_bucket(bucket).and_then(|_| {
                self.core
                    .context
                    .object_store(bucket)
                    .map_err(TransportError::from)
            }),
            Err(err) => Err(err),
        }
    }

    fn apply_marker(
        &self,
        bucket: &str,
        idempotency_key: &str,
    ) -> TransportResult<Option<ObjectApplyMarker>> {
        match self.core.context.get_last_message(
            object_stream_name(bucket),
            &object_writeback_marker_subject(bucket, idempotency_key),
        ) {
            Ok(message) => {
                let identity =
                    object_writeback_identity_from_marker(&message.data, idempotency_key)?;
                Ok(Some(ObjectApplyMarker {
                    sequence: message.sequence,
                    identity,
                }))
            }
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn apply_marker_sequence(
        &self,
        bucket: &str,
        idempotency_key: &str,
    ) -> TransportResult<Option<u64>> {
        Ok(self
            .apply_marker(bucket, idempotency_key)?
            .map(|marker| marker.sequence))
    }

    fn target_sequence_after_apply_marker(
        &self,
        bucket: &str,
        object: &str,
        idempotency_key: &str,
    ) -> TransportResult<Option<ObjectApplyMarker>> {
        let Some(marker) = self.apply_marker(bucket, idempotency_key)? else {
            return Ok(None);
        };
        let stream = object_stream_name(bucket);
        let subject = object_meta_subject(bucket, object);
        match self.core.context.get_last_message(&stream, &subject) {
            Ok(message) if message.sequence > marker.sequence => Ok(Some(marker)),
            Ok(_) => Ok(None),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn metadata_message(
        &self,
        bucket: &str,
        object: &str,
    ) -> TransportResult<Option<(nats::object_store::ObjectInfo, u64)>> {
        let stream = object_stream_name(bucket);
        let subject = object_meta_subject(bucket, object);
        match self.core.context.get_last_message(&stream, &subject) {
            Ok(message) => {
                let sequence = message.sequence;
                let info = serde_json::from_slice::<nats::object_store::ObjectInfo>(&message.data)?;
                Ok(Some((info, sequence)))
            }
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn delete_superseded(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<bool> {
        match self.metadata_message(bucket, object)? {
            None => {
                self.purge_chunks_if_known(bucket, expected_nuid)?;
                Ok(true)
            }
            Some((info, _sequence)) if info.deleted => {
                if expected_nuid.is_empty() {
                    self.purge_chunks(bucket, &info.nuid)?;
                } else {
                    self.purge_chunks(bucket, expected_nuid)?;
                }
                Ok(true)
            }
            Some((info, sequence))
                if sequence != expected_sequence
                    || (!expected_nuid.is_empty() && info.nuid != expected_nuid) =>
            {
                self.purge_chunks_if_known(bucket, expected_nuid)?;
                Ok(true)
            }
            Some(_) => Ok(false),
        }
    }

    fn purge_chunks_if_known(&self, bucket: &str, nuid: &str) -> TransportResult<()> {
        if nuid.is_empty() {
            return Ok(());
        }
        self.purge_chunks(bucket, nuid)
    }

    fn purge_chunks(&self, bucket: &str, nuid: &str) -> TransportResult<()> {
        let stream = object_stream_name(bucket);
        let chunk_subject = object_chunk_subject(bucket, nuid);
        match self
            .core
            .context
            .purge_stream_subject(stream, &chunk_subject)
        {
            Ok(_) => Ok(()),
            Err(err) if is_not_found(&err) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn ensure_apply_marker(
        &self,
        bucket: &str,
        idempotency_key: &str,
        previous_nuid: Option<&str>,
    ) -> TransportResult<u64> {
        if let Some(sequence) = self.apply_marker_sequence(bucket, idempotency_key)? {
            return Ok(sequence);
        }
        self.ensure_bucket(bucket)?;
        let stream = object_stream_name(bucket);
        self.core.ensure_duplicate_window(&stream)?;
        self.ensure_marker_subject(bucket)?;
        let subject = object_writeback_marker_subject(bucket, idempotency_key);
        let payload = object_writeback_marker_payload(idempotency_key, previous_nuid)?;
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

    fn ensure_marker_subject(&self, bucket: &str) -> TransportResult<()> {
        let stream = object_stream_name(bucket);
        let subject = object_writeback_marker_subject_filter(bucket);
        let mut info = self.core.stream_info(&stream)?;
        if info
            .config
            .subjects
            .iter()
            .any(|candidate| subject_matches(candidate, &subject))
        {
            return Ok(());
        }
        info.config.subjects.push(subject);
        self.core.context.update_stream(&info.config)?;
        Ok(())
    }

    fn history_applied_identity(
        &self,
        bucket: &str,
        object: &str,
        idempotency_key: &str,
    ) -> TransportResult<Option<ObjectWritebackIdentity>> {
        let stream = object_stream_name(bucket);
        let subject = object_meta_subject(bucket, object);
        self.core
            .scan_subject_history_until(&stream, &subject, |message| {
                let info: nats::object_store::ObjectInfo = serde_json::from_slice(&message.data)?;
                let Some(description) = info.description.as_deref() else {
                    return Ok(None);
                };
                let Some(identity) = object_writeback_identity_from_description(description) else {
                    return Ok(None);
                };
                Ok(
                    (!info.deleted && identity.idempotency_key == idempotency_key)
                        .then_some(identity),
                )
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ObjectWritebackIdentity {
    idempotency_key: String,
    #[serde(default)]
    previous_nuid: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObjectApplyMarker {
    sequence: u64,
    identity: ObjectWritebackIdentity,
}

pub(super) fn object_idempotency_description(
    idempotency_key: &str,
    previous_nuid: Option<&str>,
) -> TransportResult<String> {
    Ok(format!(
        "{IDEMPOTENCY_DESCRIPTION_PREFIX}{}",
        serde_json::to_string(&object_writeback_identity(idempotency_key, previous_nuid))?
    ))
}

pub(super) fn object_writeback_identity_from_description(
    description: &str,
) -> Option<ObjectWritebackIdentity> {
    let payload = description.strip_prefix(IDEMPOTENCY_DESCRIPTION_PREFIX)?;
    serde_json::from_str::<ObjectWritebackIdentity>(payload).ok()
}

pub(super) fn object_stream_name(bucket: &str) -> String {
    format!("OBJ_{bucket}")
}

pub(super) fn object_bucket_name_from_stream(stream_name: &str) -> Option<&str> {
    stream_name.strip_prefix("OBJ_")
}

pub(super) fn object_meta_subject(bucket: &str, object: &str) -> String {
    format!(
        "$O.{bucket}.M.{}",
        base64::encode_config(object, base64::URL_SAFE)
    )
}

pub(super) fn object_chunk_subject(bucket: &str, nuid: &str) -> String {
    format!("$O.{bucket}.C.{nuid}")
}

pub(super) fn object_stream_looks_like_bucket(stream_info: &StreamInfo, bucket: &str) -> bool {
    let metadata_subject = format!("$O.{bucket}.M.>");
    let chunk_subject = format!("$O.{bucket}.C.>");
    stream_info
        .config
        .subjects
        .iter()
        .any(|subject| subject == &metadata_subject)
        && stream_info
            .config
            .subjects
            .iter()
            .any(|subject| subject == &chunk_subject)
}

pub(super) fn object_writeback_marker_subject(bucket: &str, idempotency_key: &str) -> String {
    format!(
        "$O.{bucket}.W.{}",
        super::hex_encode(idempotency_key.as_bytes())
    )
}

fn object_writeback_marker_subject_filter(bucket: &str) -> String {
    format!("$O.{bucket}.W.>")
}

pub(super) fn object_writeback_marker_payload(
    idempotency_key: &str,
    previous_nuid: Option<&str>,
) -> TransportResult<Vec<u8>> {
    Ok(serde_json::to_vec(&object_writeback_identity(
        idempotency_key,
        previous_nuid,
    ))?)
}

pub(super) fn object_writeback_identity_from_marker(
    payload: &[u8],
    expected_idempotency_key: &str,
) -> TransportResult<ObjectWritebackIdentity> {
    let identity = serde_json::from_slice::<ObjectWritebackIdentity>(payload)?;
    if identity.idempotency_key == expected_idempotency_key {
        return Ok(identity);
    }

    Err(TransportError::Invalid(format!(
        "object writeback marker identity mismatch for {expected_idempotency_key}"
    )))
}

fn object_writeback_identity(
    idempotency_key: &str,
    previous_nuid: Option<&str>,
) -> ObjectWritebackIdentity {
    ObjectWritebackIdentity {
        idempotency_key: idempotency_key.to_string(),
        previous_nuid: previous_nuid
            .filter(|nuid| !nuid.is_empty())
            .map(str::to_string),
    }
}

fn object_put_previous_nuid<'a>(
    identity: Option<&'a ObjectWritebackIdentity>,
    marker: Option<&'a ObjectApplyMarker>,
) -> Option<&'a str> {
    identity
        .and_then(|identity| identity.previous_nuid.as_deref())
        .or_else(|| marker.and_then(|marker| marker.identity.previous_nuid.as_deref()))
        .filter(|nuid| !nuid.is_empty())
}

fn object_writeback_ledger_key(bucket: &str, object: &str, idempotency_key: &str) -> String {
    scoped_key("object", &[bucket, object, idempotency_key])
}

pub(super) fn object_watch_fact(
    bucket: &str,
    message: &Message,
) -> Option<eventfs_protocol::StorageFact> {
    let info: nats::object_store::ObjectInfo = serde_json::from_slice(&message.data).ok()?;
    Some(eventfs_protocol::StorageFact::Object {
        bucket: bucket.into(),
        name: info.name,
    })
}

pub(super) fn object_watch_subject(subject: &str) -> Option<ObjectWatchSubject<'_>> {
    let rest = subject.strip_prefix("$O.")?;
    let (bucket, kind) = rest.split_once('.')?;
    if kind.starts_with("M.") {
        return Some(ObjectWatchSubject::Metadata { bucket });
    }
    if kind.starts_with("W.") {
        return Some(ObjectWatchSubject::Marker);
    }
    if kind.starts_with("C.") {
        return Some(ObjectWatchSubject::Chunk);
    }
    None
}

pub(super) enum ObjectWatchSubject<'a> {
    Metadata { bucket: &'a str },
    Marker,
    Chunk,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_writeback_identity_requires_current_json_description_schema() {
        let description = object_idempotency_description("queued-write", Some("previous")).unwrap();
        assert_eq!(
            object_writeback_identity_from_description(&description),
            Some(ObjectWritebackIdentity {
                idempotency_key: "queued-write".into(),
                previous_nuid: Some("previous".into())
            })
        );
        assert_eq!(
            object_writeback_identity_from_description("eventfs-idempotency:queued-write"),
            None
        );
    }

    #[test]
    fn object_writeback_marker_requires_current_json_payload_schema() {
        let payload = object_writeback_marker_payload("queued-write", Some("previous")).unwrap();
        assert_eq!(
            object_writeback_identity_from_marker(&payload, "queued-write").unwrap(),
            ObjectWritebackIdentity {
                idempotency_key: "queued-write".into(),
                previous_nuid: Some("previous".into())
            }
        );
        assert!(object_writeback_identity_from_marker(b"queued-write", "queued-write").is_err());
        assert!(object_writeback_identity_from_marker(&payload, "other-write").is_err());
    }
}
