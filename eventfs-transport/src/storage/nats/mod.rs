use std::fmt::Write as _;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod core;
mod kv;
mod ledger;
mod objects;
mod streams;
mod watch;

#[cfg(test)]
use eventfs_protocol::AGENTS_STREAM;
#[cfg(test)]
use nats::jetstream::StreamConfig;
#[cfg(test)]
use nats::jetstream::StreamInfo;
#[cfg(test)]
use nats::Message;

use crate::cache::WatchEvent;
use crate::{TransportError, TransportResult};

use super::{
    DirectoryEntry, EntryKind, KeyRevision, MountStorage, ObjectMetadata, ObjectVersion,
    ReplayStorage, StreamMessageView,
};
use core::NatsCore;
pub use core::NatsStorageConfig;
#[cfg(test)]
use core::{
    duration_as_nanos, is_already_exists, is_not_found, is_timeout, retry_transport,
    store_lookup_error, stream_lookup_error,
};
#[cfg(test)]
use kv::KvDeleteRevision;
use kv::NatsKv;
#[cfg(test)]
use kv::{retry_plain_kv_delete, stream_looks_like_bucket as kv_stream_looks_like_bucket};
use ledger::WritebackLedger;
use objects::NatsObjects;
#[cfg(test)]
use objects::{
    object_meta_subject, object_stream_looks_like_bucket, object_writeback_marker_payload,
    object_writeback_marker_subject,
};
use streams::NatsStreams;
#[cfg(test)]
use streams::{agent_mailbox_name, stream_visible_in_mount};
use watch::NatsWatch;
#[cfg(test)]
use watch::{drain_watch_messages, watch_events_from_message, WatchContinuity, WATCH_DRAIN_LIMIT};

#[derive(Clone)]
pub struct NatsStorage {
    kv: NatsKv,
    objects: NatsObjects,
    streams: NatsStreams,
    watch: NatsWatch,
}

impl NatsStorage {
    pub fn connect(
        url: &str,
        creds_file: Option<&str>,
        config: NatsStorageConfig,
    ) -> TransportResult<Self> {
        let (connection, watch) = NatsWatch::connect(url, creds_file)?;
        let core = NatsCore::new(nats::jetstream::new(connection.clone()), config);
        let ledger = WritebackLedger::new(core.clone());
        Ok(Self {
            kv: NatsKv::new(core.clone(), ledger.clone()),
            objects: NatsObjects::new(core.clone(), ledger.clone()),
            streams: NatsStreams::new(core.clone(), ledger.clone()),
            watch,
        })
    }

    pub fn watch_kv_once(
        &self,
        bucket: &str,
        key: &str,
        timeout: Duration,
    ) -> TransportResult<Option<KeyRevision>> {
        self.kv.watch_once(bucket, key, timeout)
    }

    pub fn watch_stream_once(
        &self,
        stream: &str,
        subject: &str,
        timeout: Duration,
    ) -> TransportResult<Option<StreamMessageView>> {
        self.streams.watch_once(stream, subject, timeout)
    }
}

impl ReplayStorage for NatsStorage {
    fn kv_put_idempotent(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<u64> {
        self.kv.put_idempotent(bucket, key, bytes, idempotency_key)
    }

    fn kv_put_applied(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<bool> {
        self.kv.put_applied(bucket, key, idempotency_key)
    }

    fn kv_delete_if_revision(
        &self,
        bucket: &str,
        key: &str,
        expected_revision: u64,
    ) -> TransportResult<()> {
        self.kv.delete_if_revision(bucket, key, expected_revision)
    }

    fn kv_delete_if_revision_applied(
        &self,
        bucket: &str,
        key: &str,
        expected_revision: u64,
    ) -> TransportResult<bool> {
        self.kv
            .delete_if_revision_applied(bucket, key, expected_revision)
    }

    fn publish_json_lines(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<Vec<u64>> {
        self.streams
            .publish_json_lines(stream, subject, bytes, idempotency_seed)
    }

    fn publish_json_lines_applied(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<bool> {
        self.streams
            .publish_json_lines_applied(stream, subject, bytes, idempotency_seed)
    }

    fn publish_json_lines_applied_prefix(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<usize> {
        self.streams
            .publish_json_lines_applied_prefix(stream, subject, bytes, idempotency_seed)
    }

    fn object_put_idempotent(
        &self,
        bucket: &str,
        object: &str,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<()> {
        self.objects
            .put_idempotent(bucket, object, bytes, idempotency_key)
    }

    fn object_put_applied(
        &self,
        bucket: &str,
        object: &str,
        idempotency_key: &str,
    ) -> TransportResult<bool> {
        self.objects.put_applied(bucket, object, idempotency_key)
    }

    fn object_delete_if_sequence(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<()> {
        self.objects
            .delete_if_sequence(bucket, object, expected_sequence, expected_nuid)
    }

    fn object_delete_if_sequence_applied(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<bool> {
        self.objects
            .delete_if_sequence_applied(bucket, object, expected_sequence, expected_nuid)
    }
}

impl MountStorage for NatsStorage {
    fn list_kv_buckets(&self) -> TransportResult<Vec<String>> {
        self.kv.list_buckets()
    }

    fn ensure_kv_bucket(&self, bucket: &str) -> TransportResult<()> {
        self.kv.ensure_bucket(bucket)
    }

    fn list_kv_prefix(&self, bucket: &str, prefix: &str) -> TransportResult<Vec<DirectoryEntry>> {
        self.kv.list_prefix(bucket, prefix)
    }

    fn kv_get(&self, bucket: &str, key: &str) -> TransportResult<Option<KeyRevision>> {
        self.kv.get(bucket, key)
    }

    fn kv_put(&self, bucket: &str, key: &str, bytes: &[u8]) -> TransportResult<u64> {
        self.kv.put(bucket, key, bytes)
    }

    fn kv_delete(&self, bucket: &str, key: &str) -> TransportResult<()> {
        self.kv.delete(bucket, key)
    }

    fn kv_history(&self, bucket: &str, key: &str) -> TransportResult<Vec<KeyRevision>> {
        self.kv.history(bucket, key)
    }

    fn list_kv_history_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> TransportResult<Vec<DirectoryEntry>> {
        self.kv.list_history_prefix(bucket, prefix)
    }

    fn kv_revision(
        &self,
        bucket: &str,
        key: &str,
        revision: u64,
    ) -> TransportResult<Option<KeyRevision>> {
        self.kv.revision(bucket, key, revision)
    }

    fn list_streams(&self) -> TransportResult<Vec<String>> {
        self.streams.list()
    }

    fn ensure_stream(&self, stream: &str) -> TransportResult<()> {
        self.streams.ensure(stream)
    }

    fn list_stream_messages(&self, stream: &str) -> TransportResult<Vec<DirectoryEntry>> {
        self.streams.list_messages(stream)
    }

    fn list_stream_subjects(&self, stream: &str) -> TransportResult<Vec<DirectoryEntry>> {
        self.streams.list_subjects(stream)
    }

    fn list_agent_names(&self) -> TransportResult<Vec<String>> {
        self.streams.list_agent_names()
    }

    fn stream_message(&self, stream: &str, sequence: u64) -> TransportResult<StreamMessageView> {
        self.streams.message(stream, sequence)
    }

    fn list_object_buckets(&self) -> TransportResult<Vec<String>> {
        self.objects.list_buckets()
    }

    fn ensure_object_bucket(&self, bucket: &str) -> TransportResult<()> {
        self.objects.ensure_bucket(bucket)
    }

    fn list_object_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> TransportResult<Vec<DirectoryEntry>> {
        self.objects.list_prefix(bucket, prefix)
    }

    fn object_get(&self, bucket: &str, object: &str) -> TransportResult<Option<ObjectVersion>> {
        self.objects.get(bucket, object)
    }

    fn object_metadata(
        &self,
        bucket: &str,
        object: &str,
    ) -> TransportResult<Option<ObjectMetadata>> {
        self.objects.metadata(bucket, object)
    }

    fn object_put(&self, bucket: &str, object: &str, bytes: &[u8]) -> TransportResult<()> {
        self.objects.put(bucket, object, bytes)
    }

    fn object_delete(&self, bucket: &str, object: &str) -> TransportResult<()> {
        self.objects.delete(bucket, object)
    }

    fn watch_events(&self) -> TransportResult<Vec<WatchEvent>> {
        self.watch.events()
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

fn immediate_children<'a>(
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
        merge_child_entry(&mut entries, name, kind);
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
        .map(|entry| DirectoryEntry {
            name: entry.name,
            kind: EntryKind::Directory,
        })
        .collect()
}

fn merge_child_entry(entries: &mut Vec<DirectoryEntry>, name: String, kind: EntryKind) {
    if let Some(entry) = entries.iter_mut().find(|entry| entry.name == name) {
        if kind == EntryKind::File {
            entry.kind = EntryKind::File;
        }
    } else {
        entries.push(DirectoryEntry { name, kind });
    }
}

fn system_time_from_datetime(datetime: nats::jetstream::DateTime) -> TransportResult<SystemTime> {
    let nanos = u64::try_from(datetime.unix_timestamp_nanos()).map_err(|_| {
        TransportError::Invalid("JetStream timestamp is outside SystemTime range".into())
    })?;
    Ok(UNIX_EPOCH + Duration::from_nanos(nanos))
}

fn subject_matches(pattern: &str, subject: &str) -> bool {
    if pattern == subject || pattern == ">" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(".>") {
        return subject.starts_with(&format!("{prefix}."));
    }
    let pattern_parts: Vec<&str> = pattern.split('.').collect();
    let subject_parts: Vec<&str> = subject.split('.').collect();
    pattern_parts.len() == subject_parts.len()
        && pattern_parts
            .iter()
            .zip(subject_parts.iter())
            .all(|(want, got)| *want == "*" || want == got)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventfs_protocol::{
        AffectedPathReason, MountPath, AGENTS_BUCKET, SEMANTIC_BUCKET, TASKS_BUCKET,
    };

    fn watch_invalidate(path: impl AsRef<str>, reason: AffectedPathReason) -> WatchEvent {
        WatchEvent::invalidate_affected_path(MountPath::new(path.as_ref()).unwrap(), reason)
    }

    #[test]
    fn lists_immediate_children_from_slash_keys() {
        let entries = immediate_children(["a/b.json", "a/c/d.json", "root.json"], "a");
        assert_eq!(
            entries,
            vec![
                DirectoryEntry {
                    name: "b.json".into(),
                    kind: EntryKind::File
                },
                DirectoryEntry {
                    name: "c".into(),
                    kind: EntryKind::Directory
                }
            ]
        );
    }

    #[test]
    fn immediate_children_resolves_file_directory_conflicts_deterministically() {
        for keys in [
            vec!["dir/file.json", "dir"],
            vec!["dir", "dir/file.json"],
            vec!["dir/nested/file.json", "dir/file.json", "dir"],
        ] {
            assert_eq!(
                immediate_children(keys, ""),
                vec![DirectoryEntry {
                    name: "dir".into(),
                    kind: EntryKind::File
                }]
            );
        }
    }

    #[test]
    fn immediate_history_children_are_revision_directories() {
        assert_eq!(
            immediate_history_children(["jobs/2026", "jobs/2027/q1"], "jobs"),
            vec![
                DirectoryEntry {
                    name: "2026".into(),
                    kind: EntryKind::Directory
                },
                DirectoryEntry {
                    name: "2027".into(),
                    kind: EntryKind::Directory
                },
            ]
        );
    }

    #[test]
    fn subject_matching_accepts_exact_wildcard_and_prefix() {
        assert!(subject_matches("orders.created", "orders.created"));
        assert!(subject_matches("orders.*", "orders.created"));
        assert!(subject_matches("orders.>", "orders.created.eu"));
        assert!(!subject_matches("orders.>", "orders"));
        assert!(!subject_matches("orders.*", "orders.created.eu"));
    }

    #[test]
    fn stream_visibility_hides_only_internal_ledger_stream() {
        assert!(!stream_visible_in_mount("KV_DEMO"));
        assert!(!stream_visible_in_mount("OBJ_DEMO"));
        assert!(!stream_visible_in_mount(WritebackLedger::stream_name()));
        assert!(stream_visible_in_mount("EVENTFS_AGENTS"));
        assert!(stream_visible_in_mount("EVENTFS_REPORTS"));
    }

    #[test]
    fn kv_bucket_validation_requires_kv_subject_and_history() {
        let mut info = test_stream_info(vec!["$KV.demo.>"], 1);
        assert!(kv_stream_looks_like_bucket(&info, "demo"));

        info.config.max_msgs_per_subject = 0;
        assert!(!kv_stream_looks_like_bucket(&info, "demo"));

        info.config.max_msgs_per_subject = 1;
        info.config.subjects = vec!["demo.>".into()];
        assert!(!kv_stream_looks_like_bucket(&info, "demo"));
    }

    #[test]
    fn object_bucket_validation_requires_chunk_and_metadata_subjects() {
        let mut info = test_stream_info(vec!["$O.assets.C.>", "$O.assets.M.>"], 0);
        assert!(object_stream_looks_like_bucket(&info, "assets"));

        info.config.subjects = vec!["$O.assets.M.>".into()];
        assert!(!object_stream_looks_like_bucket(&info, "assets"));

        info.config.subjects = vec!["$O.assets.C.>".into()];
        assert!(!object_stream_looks_like_bucket(&info, "assets"));
    }

    #[test]
    fn retry_policy_retries_then_returns_success() {
        let mut attempts = 0;
        let result = retry_transport(2, Duration::ZERO, || {
            attempts += 1;
            if attempts < 3 {
                Err(TransportError::Invalid("not yet".into()))
            } else {
                Ok("done")
            }
        })
        .unwrap();
        assert_eq!(result, "done");
        assert_eq!(attempts, 3);
    }

    #[test]
    fn retry_policy_returns_last_error_after_budget() {
        let mut attempts = 0;
        let err = retry_transport::<()>(1, Duration::ZERO, || {
            attempts += 1;
            Err(TransportError::Invalid(format!("attempt-{attempts}")))
        })
        .unwrap_err();
        assert_eq!(attempts, 2);
        assert!(err.to_string().contains("attempt-2"));
    }

    #[test]
    fn store_lookup_maps_missing_buckets_to_not_found() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "bucket not found");
        assert!(matches!(store_lookup_error(err), TransportError::NotFound));
    }

    #[test]
    fn store_lookup_preserves_no_responders_as_io_error() {
        let err = std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "nats: no responders available for request",
        );
        assert!(matches!(store_lookup_error(err), TransportError::Io(_)));
    }

    #[test]
    fn no_responders_are_not_missing_resources() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "no responders");
        assert!(!is_not_found(&err));
    }

    #[test]
    fn stream_message_lookup_maps_missing_sequences_to_not_found() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "404 no message found");
        assert!(is_not_found(&err));
    }

    #[test]
    fn broker_create_race_errors_are_idempotent_successes() {
        let err = TransportError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "stream already exists",
        ));
        assert!(is_already_exists(&err));
    }

    #[test]
    fn stream_lookup_maps_missing_streams_to_not_found() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "stream not found");
        assert!(matches!(stream_lookup_error(err), TransportError::NotFound));
    }

    #[test]
    fn stream_lookup_preserves_no_responders_as_io_error() {
        let err = std::io::Error::other("nats: no responders available for request");
        assert!(matches!(stream_lookup_error(err), TransportError::Io(_)));
    }

    #[test]
    fn timeout_detection_accepts_nats_next_timeout_errors() {
        let err = std::io::Error::other("next_timeout: timed out");
        assert!(is_timeout(&err));
    }

    #[test]
    fn watch_events_map_kv_subjects_to_native_and_materialized_paths() {
        let message = test_message(format!("$KV.{}.demo/render/output.json", TASKS_BUCKET));

        assert_eq!(
            watch_events_from_message(&message),
            vec![
                watch_invalidate(
                    format!("/kv/{}/demo/render/output.json", TASKS_BUCKET),
                    AffectedPathReason::Exact,
                ),
                watch_invalidate(
                    format!("/kv/{}/demo/render", TASKS_BUCKET),
                    AffectedPathReason::Ancestor,
                ),
                watch_invalidate(
                    format!("/kv/{}/demo", TASKS_BUCKET),
                    AffectedPathReason::Ancestor,
                ),
                watch_invalidate(
                    format!("/kv/{}", TASKS_BUCKET),
                    AffectedPathReason::Ancestor,
                ),
                watch_invalidate("/tasks/demo/render/output.json", AffectedPathReason::Alias,),
                watch_invalidate("/tasks/demo/render", AffectedPathReason::Ancestor),
                watch_invalidate("/tasks/demo", AffectedPathReason::Ancestor),
            ]
        );
    }

    #[test]
    fn watch_events_map_kv_subjects_to_agent_and_semantic_ancestors() {
        let agent_message = test_message(format!("$KV.{}.bot/memory/facts/a.json", AGENTS_BUCKET));
        assert_eq!(
            watch_events_from_message(&agent_message),
            vec![
                watch_invalidate(
                    format!("/kv/{}/bot/memory/facts/a.json", AGENTS_BUCKET),
                    AffectedPathReason::Exact,
                ),
                watch_invalidate(
                    format!("/kv/{}/bot/memory/facts", AGENTS_BUCKET),
                    AffectedPathReason::Ancestor,
                ),
                watch_invalidate(
                    format!("/kv/{}/bot/memory", AGENTS_BUCKET),
                    AffectedPathReason::Ancestor,
                ),
                watch_invalidate(
                    format!("/kv/{}/bot", AGENTS_BUCKET),
                    AffectedPathReason::Ancestor,
                ),
                watch_invalidate(
                    format!("/kv/{}", AGENTS_BUCKET),
                    AffectedPathReason::Ancestor
                ),
                watch_invalidate("/agents/bot/memory/facts/a.json", AffectedPathReason::Alias,),
                watch_invalidate("/agents/bot/memory/facts", AffectedPathReason::Ancestor),
                watch_invalidate("/agents/bot/memory", AffectedPathReason::Ancestor),
                watch_invalidate("/agents/bot", AffectedPathReason::Ancestor),
            ]
        );

        let semantic_message = test_message(format!("$KV.{}.tags/project/a.json", SEMANTIC_BUCKET));
        assert_eq!(
            watch_events_from_message(&semantic_message),
            vec![
                watch_invalidate(
                    format!("/kv/{}/tags/project/a.json", SEMANTIC_BUCKET),
                    AffectedPathReason::Exact,
                ),
                watch_invalidate(
                    format!("/kv/{}/tags/project", SEMANTIC_BUCKET),
                    AffectedPathReason::Ancestor,
                ),
                watch_invalidate(
                    format!("/kv/{}/tags", SEMANTIC_BUCKET),
                    AffectedPathReason::Ancestor,
                ),
                watch_invalidate(
                    format!("/kv/{}", SEMANTIC_BUCKET),
                    AffectedPathReason::Ancestor,
                ),
                watch_invalidate("/semantic/tags/project/a.json", AffectedPathReason::Alias),
                watch_invalidate("/semantic/tags/project", AffectedPathReason::Ancestor),
                watch_invalidate("/semantic/tags", AffectedPathReason::Ancestor),
            ]
        );
    }

    #[test]
    fn watch_events_map_materialized_event_subjects_to_native_and_alias_paths() {
        let message = test_message("events.system");

        assert_eq!(
            watch_events_from_message(&message),
            vec![
                watch_invalidate(
                    "/streams/system/subjects/events.system.jsonl",
                    AffectedPathReason::Exact,
                ),
                watch_invalidate("/streams/system/subjects", AffectedPathReason::Ancestor),
                watch_invalidate("/events/system.jsonl", AffectedPathReason::Alias),
                watch_invalidate("/events", AffectedPathReason::Ancestor),
            ]
        );
    }

    #[test]
    fn watch_events_map_materialized_stream_subjects_to_mailbox_paths() {
        let message = test_message("agents.bot.inbox");

        assert_eq!(
            watch_events_from_message(&message),
            vec![
                watch_invalidate(
                    format!("/streams/{}/subjects/agents.bot.inbox.jsonl", AGENTS_STREAM),
                    AffectedPathReason::Exact,
                ),
                watch_invalidate(
                    format!("/streams/{}/subjects", AGENTS_STREAM),
                    AffectedPathReason::Ancestor,
                ),
                watch_invalidate("/agents/bot/inbox", AffectedPathReason::Mailbox),
                watch_invalidate("/agents/bot", AffectedPathReason::Ancestor),
                watch_invalidate("/agents", AffectedPathReason::Ancestor),
            ]
        );
    }

    #[test]
    fn watch_events_ignore_reserved_agent_mailbox_subjects() {
        let message = test_message("agents.__eventfs_applied.inbox");

        assert!(watch_events_from_message(&message).is_empty());
    }

    #[test]
    fn agent_mailbox_name_rejects_reserved_internal_roots() {
        assert_eq!(agent_mailbox_name("agents.__eventfs_applied.inbox"), None);
        assert_eq!(
            agent_mailbox_name("agents.__eventfs_writeback.outbox"),
            None
        );
        assert_eq!(agent_mailbox_name("agents.bot.inbox"), Some("bot"));
    }

    #[test]
    fn plain_kv_delete_retries_after_revision_change() {
        let mut reads = [Some(11), Some(12)].into_iter();
        let mut attempts = Vec::new();
        let mut outcomes =
            [KvDeleteRevision::RevisionChanged, KvDeleteRevision::Deleted].into_iter();

        retry_plain_kv_delete(
            2,
            || {
                Ok(reads
                    .next()
                    .expect("delete should re-read current revision"))
            },
            |revision| {
                attempts.push(revision);
                Ok(outcomes.next().expect("delete attempt outcome"))
            },
        )
        .unwrap();

        assert_eq!(attempts, vec![11, 12]);
    }

    #[test]
    fn plain_kv_delete_reports_missing_after_revision_change() {
        let mut reads = [Some(11), None].into_iter();
        let mut attempts = Vec::new();
        let mut outcomes = [KvDeleteRevision::RevisionChanged].into_iter();

        let err = retry_plain_kv_delete(
            2,
            || Ok(reads.next().expect("delete should re-read after conflict")),
            |revision| {
                attempts.push(revision);
                Ok(outcomes.next().expect("delete attempt outcome"))
            },
        )
        .unwrap_err();

        assert!(matches!(err, TransportError::NotFound));
        assert_eq!(attempts, vec![11]);
    }

    #[test]
    fn plain_kv_delete_reports_conflict_after_retry_budget() {
        let mut reads = [Some(11), Some(12)].into_iter();
        let mut attempts = Vec::new();

        let err = retry_plain_kv_delete(
            1,
            || Ok(reads.next().expect("delete should use bounded retries")),
            |revision| {
                attempts.push(revision);
                Ok(KvDeleteRevision::RevisionChanged)
            },
        )
        .unwrap_err();

        assert!(matches!(
            err,
            TransportError::Invalid(message)
                if message == "KV delete conflict after 2 attempts"
        ));
        assert_eq!(attempts, vec![11, 12]);
    }

    #[test]
    fn watch_events_map_object_updates_to_precise_paths() {
        let message = test_message_with_payload(
            object_meta_subject("assets", "images/logo.png"),
            serde_json::json!({
                "name": "images/logo.png",
                "description": null,
                "link": null,
                "bucket": "assets",
                "nuid": "logo",
                "size": 7,
                "chunks": 1,
                "mtime": "1970-01-01T00:00:00Z",
                "digest": "sha-256=logo",
                "deleted": false,
            })
            .to_string()
            .into_bytes(),
        );

        assert_eq!(
            watch_events_from_message(&message),
            vec![
                watch_invalidate("/objects/assets/images/logo.png", AffectedPathReason::Exact,),
                watch_invalidate("/objects/assets/images", AffectedPathReason::Ancestor),
                watch_invalidate("/objects/assets", AffectedPathReason::Ancestor),
            ]
        );
    }

    #[test]
    fn watch_events_ignore_hidden_object_writeback_markers() {
        let message = test_message_with_payload(
            object_writeback_marker_subject("assets", "queued-write"),
            object_writeback_marker_payload("queued-write", None).unwrap(),
        );

        assert!(watch_events_from_message(&message).is_empty());
    }

    #[test]
    fn watch_events_ignore_object_chunk_subjects_without_gap() {
        let message = test_message_with_payload("$O.assets.C.chunk-1", b"chunk-bytes".to_vec());

        assert!(watch_events_from_message(&message).is_empty());
    }

    #[test]
    fn watch_drain_limit_applies_to_ignored_marker_bursts() {
        let messages = (0..=WATCH_DRAIN_LIMIT)
            .map(|index| {
                test_message_with_payload(
                    object_writeback_marker_subject("assets", &format!("queued-write-{index}")),
                    object_writeback_marker_payload(&format!("queued-write-{index}"), None)
                        .unwrap(),
                )
            })
            .chain(std::iter::once(test_message_with_payload(
                object_meta_subject("assets", "images/logo.png"),
                serde_json::json!({
                    "name": "images/logo.png",
                    "description": null,
                    "link": null,
                    "bucket": "assets",
                    "nuid": "logo",
                    "size": 7,
                    "chunks": 1,
                    "mtime": "1970-01-01T00:00:00Z",
                    "digest": "sha-256=logo",
                    "deleted": false,
                })
                .to_string()
                .into_bytes(),
            )));

        assert_eq!(drain_watch_messages(messages), vec![WatchEvent::Gap]);
    }

    #[test]
    fn watch_continuity_reports_gap_once() {
        let continuity = WatchContinuity::new();

        assert!(!continuity.take_gap());
        continuity.mark_gap();
        assert!(continuity.take_gap());
        assert!(!continuity.take_gap());
    }

    #[test]
    fn default_stream_duplicate_window_is_not_tied_to_request_timeout() {
        let config = NatsStorageConfig {
            timeout: Duration::from_secs(1),
            ..Default::default()
        };

        assert!(config.stream_duplicate_window > config.timeout);
        assert_eq!(
            duration_as_nanos(config.stream_duplicate_window),
            24 * 60 * 60 * 1_000_000_000
        );
    }

    fn test_stream_info(subjects: Vec<&str>, max_msgs_per_subject: i64) -> StreamInfo {
        StreamInfo {
            config: StreamConfig {
                name: "TEST".into(),
                subjects: subjects.into_iter().map(str::to_string).collect(),
                max_msgs_per_subject,
                ..Default::default()
            },
            created: nats::jetstream::DateTime::UNIX_EPOCH,
            state: nats::jetstream::StreamState {
                messages: 0,
                bytes: 0,
                first_seq: 0,
                first_ts: nats::jetstream::DateTime::UNIX_EPOCH,
                last_seq: 0,
                last_ts: nats::jetstream::DateTime::UNIX_EPOCH,
                consumer_count: 0,
            },
            cluster: nats::jetstream::ClusterInfo {
                name: None,
                leader: None,
                replicas: Vec::new(),
            },
        }
    }

    fn test_message(subject: impl Into<String>) -> Message {
        test_message_with_payload(subject, br#"{"event":true}"#.to_vec())
    }

    fn test_message_with_payload(subject: impl Into<String>, data: Vec<u8>) -> Message {
        Message {
            subject: subject.into(),
            reply: None,
            data,
            headers: None,
            client: None,
            double_acked: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}
