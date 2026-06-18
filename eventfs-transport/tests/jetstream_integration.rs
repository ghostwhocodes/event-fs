#![cfg(feature = "jetstream-tests")]

use std::io::Cursor;
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventfs_protocol::{AffectedPathReason, MountPath};
use eventfs_transport::{
    cache::{VersionStamp, WatchEvent},
    FailedWrite, FailedWriteOperation, MountStorage, NatsStorage, NatsStorageConfig, ReplayStorage,
    TransportError, TransportResult, WritebackReplay,
};
use nats::header::HeaderMap;
use nats::jetstream::{PublishOptions, StorageType, StreamConfig};
use nats::object_store::ObjectMeta;
use nats::Message;

fn watch_invalidate(path: impl AsRef<str>, reason: AffectedPathReason) -> WatchEvent {
    WatchEvent::invalidate_affected_path(MountPath::new(path.as_ref()).unwrap(), reason)
}

fn enqueue_failed_write(
    replay: &mut WritebackReplay,
    id: impl Into<String>,
    operation: FailedWriteOperation,
) {
    let diagnostic_path = failed_write_diagnostic_path(&operation);
    replay
        .enqueue_failed_write(FailedWrite::new(
            id,
            VersionStamp::at(SystemTime::now()),
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
        FailedWriteOperation::ObjectPut { bucket, object, .. } => {
            MountPath::new(format!("/objects/{bucket}/{object}")).unwrap()
        }
        FailedWriteOperation::PublishJsonLines {
            stream, subject, ..
        } => MountPath::new(format!(
            "/streams/{stream}/subjects/{}.jsonl",
            eventfs_protocol::stream_subject_file_name_from_str(subject)
        ))
        .unwrap(),
        FailedWriteOperation::KvRenameComplete {
            to_bucket, to_key, ..
        } => MountPath::new(format!("/kv/{to_bucket}/{to_key}")).unwrap(),
        FailedWriteOperation::ObjectRenameComplete {
            to_bucket,
            to_object,
            ..
        } => MountPath::new(format!("/objects/{to_bucket}/{to_object}")).unwrap(),
    }
}

#[test]
fn jetstream_adapter_exercises_kv_stream_object_watch_and_idempotency() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");

    let suffix = unique_suffix();
    let kv_bucket = format!("JSFSKV{suffix}");
    let object_bucket = format!("JSFSOBJ{suffix}");
    let stream = format!("JSFSSTREAM{suffix}");
    let subject = format!("eventfs.{suffix}.created");
    let key = "config/app.json";

    backend.ensure_kv_bucket(&kv_bucket).unwrap();
    let watcher = {
        let backend = backend.clone();
        let bucket = kv_bucket.clone();
        std::thread::spawn(move || backend.watch_kv_once(&bucket, key, Duration::from_secs(3)))
    };
    std::thread::sleep(Duration::from_millis(100));
    let rev1 = backend.kv_put(&kv_bucket, key, br#"{"rev":1}"#).unwrap();
    let rev2 = backend.kv_put(&kv_bucket, key, br#"{"rev":2}"#).unwrap();
    let current = backend.kv_get(&kv_bucket, key).unwrap().unwrap();
    assert_eq!(current.revision, rev2);
    assert_eq!(current.bytes, br#"{"rev":2}"#);
    assert!(current.created >= UNIX_EPOCH);
    assert_eq!(
        backend
            .kv_revision(&kv_bucket, key, rev1)
            .unwrap()
            .unwrap()
            .bytes,
        br#"{"rev":1}"#
    );
    assert!(watcher.join().unwrap().unwrap().is_some());
    backend.kv_delete(&kv_bucket, key).unwrap();
    assert!(backend.kv_get(&kv_bucket, key).unwrap().is_none());

    backend.ensure_stream(&stream).unwrap();
    let stream_watcher = {
        let backend = backend.clone();
        let stream = stream.clone();
        let subject = subject.clone();
        std::thread::spawn(move || {
            backend.watch_stream_once(&stream, &subject, Duration::from_secs(3))
        })
    };
    std::thread::sleep(Duration::from_millis(100));
    let published = backend
        .publish_json_lines(&stream, &subject, br#"{"event":1}"#, "same-id")
        .unwrap();
    backend
        .publish_json_lines(&stream, &subject, br#"{"event":1}"#, "same-id")
        .unwrap();
    let messages = backend.list_stream_messages(&stream).unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(
        backend.list_stream_subjects(&stream).unwrap(),
        vec![eventfs_transport::DirectoryEntry {
            name: format!("{subject}.jsonl"),
            kind: eventfs_transport::EntryKind::File,
        }]
    );
    let message = backend.stream_message(&stream, published[0]).unwrap();
    assert_eq!(message.subject, subject);
    assert_eq!(message.payload, br#"{"event":1}"#);
    assert!(message.published >= UNIX_EPOCH);
    assert!(stream_watcher.join().unwrap().unwrap().is_some());

    backend
        .publish_json_lines(
            eventfs_protocol::subjects::AGENTS_STREAM,
            "agents.bot.inbox",
            br#"{"task":"run"}"#,
            "agent-mailbox",
        )
        .unwrap();
    assert_eq!(backend.list_agent_names().unwrap(), vec!["bot".to_string()]);

    backend.ensure_object_bucket(&object_bucket).unwrap();
    backend
        .object_put(&object_bucket, "payloads/blob.txt", b"object-data")
        .unwrap();
    let object = backend
        .object_get(&object_bucket, "payloads/blob.txt")
        .unwrap()
        .unwrap();
    assert_eq!(object.bytes, b"object-data");
    assert!(object.modified >= UNIX_EPOCH);
    let entries = backend
        .list_object_prefix(&object_bucket, "payloads")
        .unwrap();
    assert!(entries.iter().any(|entry| entry.name == "blob.txt"));
    backend
        .object_delete(&object_bucket, "payloads/blob.txt")
        .unwrap();
    assert!(backend
        .object_get(&object_bucket, "payloads/blob.txt")
        .unwrap()
        .is_none());
}

#[test]
fn reserved_mailbox_only_agents_are_not_listed() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");

    let suffix = unique_suffix();
    backend
        .publish_json_lines(
            eventfs_protocol::subjects::AGENTS_STREAM,
            "agents.__eventfs_applied.inbox",
            br#"{"task":"hidden"}"#,
            &format!("reserved-applied-mailbox-{suffix}"),
        )
        .unwrap();
    backend
        .publish_json_lines(
            eventfs_protocol::subjects::AGENTS_STREAM,
            "agents.__eventfs_writeback.outbox",
            br#"{"status":"hidden"}"#,
            &format!("reserved-writeback-mailbox-{suffix}"),
        )
        .unwrap();

    let agents = backend.list_agent_names().unwrap();
    assert!(
        !agents
            .iter()
            .any(|agent| agent == "__eventfs_applied" || agent == "__eventfs_writeback"),
        "reserved mailbox-only agents leaked into listing: {agents:?}"
    );
}

#[test]
fn publish_json_lines_scopes_applied_ledger_to_stream_and_subject() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");

    let suffix = unique_suffix();
    let stream_a = format!("JSFSLEDGERA{suffix}");
    let stream_b = format!("JSFSLEDGERB{suffix}");
    let subject_a = format!("eventfs.{suffix}.alpha");
    let subject_b = format!("eventfs.{suffix}.beta");

    backend.ensure_stream(&stream_a).unwrap();
    backend.ensure_stream(&stream_b).unwrap();

    assert_eq!(
        backend
            .publish_json_lines(&stream_a, &subject_a, br#"{"event":1}"#, "shared-seed")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        backend
            .publish_json_lines(&stream_b, &subject_b, br#"{"event":1}"#, "shared-seed")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(backend.list_stream_messages(&stream_a).unwrap().len(), 1);
    assert_eq!(backend.list_stream_messages(&stream_b).unwrap().len(), 1);
}

#[test]
fn direct_stream_listing_keeps_eventfs_namespace_streams_visible() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");

    let suffix = unique_suffix();
    let user_stream = format!("EVENTFS_REPORTS{suffix}");
    let user_subject = format!("{}.created", user_stream.to_ascii_lowercase());
    let kv_bucket = format!("JSFSLEDGER{suffix}");

    backend.ensure_stream(&user_stream).unwrap();
    backend
        .publish_json_lines(
            &user_stream,
            &user_subject,
            br#"{"report":"ready"}"#,
            "user-visible-stream",
        )
        .unwrap();
    backend
        .publish_json_lines(
            eventfs_protocol::subjects::AGENTS_STREAM,
            "agents.bot.inbox",
            br#"{"task":"run"}"#,
            "agent-visible-stream",
        )
        .unwrap();
    backend
        .kv_put_idempotent(&kv_bucket, "state.json", br#"{"ok":true}"#, "create-ledger")
        .unwrap();

    let streams = backend.list_streams().unwrap();
    assert!(
        streams.iter().any(|stream| stream == &user_stream),
        "user-prefixed stream should stay visible: {streams:?}"
    );
    assert!(
        streams
            .iter()
            .any(|stream| stream == eventfs_protocol::subjects::AGENTS_STREAM),
        "agent stream should stay visible: {streams:?}"
    );
    assert!(
        !streams.iter().any(|stream| stream == "EVENTFS_WRITEBACK"),
        "internal writeback ledger must stay hidden: {streams:?}"
    );
}

#[test]
fn kv_history_missing_prefix_returns_without_blocking() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_millis(150),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");

    let suffix = unique_suffix();
    let kv_bucket = format!("JSFSHISTORY{suffix}");
    backend.ensure_kv_bucket(&kv_bucket).unwrap();
    backend
        .kv_put(&kv_bucket, "jobs/2026", br#"{"queued":true}"#)
        .unwrap();

    let (tx, rx) = mpsc::channel();
    let lookup_backend = backend.clone();
    let lookup_bucket = kv_bucket.clone();
    std::thread::spawn(move || {
        tx.send(lookup_backend.kv_history(&lookup_bucket, "jobs"))
            .unwrap();
    });

    let history = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("missing KV history key should return before the FUSE request path blocks")
        .unwrap();
    assert!(history.is_empty());
    assert_eq!(
        backend.list_kv_history_prefix(&kv_bucket, "jobs").unwrap(),
        vec![eventfs_transport::DirectoryEntry {
            name: "2026".into(),
            kind: eventfs_transport::EntryKind::Directory,
        }]
    );
}

#[test]
fn kv_delete_missing_key_returns_not_found_without_delete_marker() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let bucket = format!("JSFSMISSINGDEL{suffix}");
    let key = "missing/value.json";

    backend.ensure_kv_bucket(&bucket).unwrap();

    assert!(matches!(
        backend.kv_delete(&bucket, key),
        Err(TransportError::NotFound)
    ));
    assert!(
        context
            .get_last_message(kv_stream_name(&bucket), &kv_subject(&bucket, key))
            .is_err(),
        "missing KV unlink must not publish a delete-only marker"
    );
}

#[test]
fn bucket_listing_skips_plain_prefix_matching_streams() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let real_kv_bucket = format!("JSFSREALKV{suffix}");
    let fake_kv_bucket = format!("JSFSFAKEKV{suffix}");
    let real_object_bucket = format!("JSFSREALOBJ{suffix}");
    let fake_object_bucket = format!("JSFSFAKEOBJ{suffix}");

    backend.ensure_kv_bucket(&real_kv_bucket).unwrap();
    backend.ensure_object_bucket(&real_object_bucket).unwrap();

    context
        .add_stream(StreamConfig {
            name: format!("KV_{fake_kv_bucket}"),
            subjects: vec![format!("plain.kv.{suffix}.>")],
            storage: StorageType::File,
            ..Default::default()
        })
        .unwrap();
    context
        .add_stream(StreamConfig {
            name: format!("OBJ_{fake_object_bucket}"),
            subjects: vec![format!("plain.obj.{suffix}.>")],
            storage: StorageType::File,
            ..Default::default()
        })
        .unwrap();

    let kv_buckets = backend.list_kv_buckets().unwrap();
    assert!(
        kv_buckets.iter().any(|bucket| bucket == &real_kv_bucket),
        "real KV bucket missing from listing: {kv_buckets:?}"
    );
    assert!(
        !kv_buckets.iter().any(|bucket| bucket == &fake_kv_bucket),
        "plain KV_-prefixed stream leaked into bucket listing: {kv_buckets:?}"
    );

    let object_buckets = backend.list_object_buckets().unwrap();
    assert!(
        object_buckets
            .iter()
            .any(|bucket| bucket == &real_object_bucket),
        "real object bucket missing from listing: {object_buckets:?}"
    );
    assert!(
        !object_buckets
            .iter()
            .any(|bucket| bucket == &fake_object_bucket),
        "plain OBJ_-prefixed stream leaked into bucket listing: {object_buckets:?}"
    );
}

#[test]
fn guarded_source_deletes_preserve_newer_kv_and_object_generations() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");

    let suffix = unique_suffix();
    let kv_bucket = format!("JSFSKV{suffix}");
    let object_bucket = format!("JSFSOBJ{suffix}");
    let key = "rename/source.json";
    let object = "rename/source.bin";

    backend.ensure_kv_bucket(&kv_bucket).unwrap();
    let old_revision = backend.kv_put(&kv_bucket, key, b"old-kv").unwrap();
    backend.kv_put(&kv_bucket, key, b"new-kv").unwrap();
    backend
        .kv_delete_if_revision(&kv_bucket, key, old_revision)
        .unwrap();
    assert_eq!(
        backend.kv_get(&kv_bucket, key).unwrap().unwrap().bytes,
        b"new-kv"
    );

    backend.ensure_object_bucket(&object_bucket).unwrap();
    backend
        .object_put(&object_bucket, object, b"old-object")
        .unwrap();
    let old_object = backend.object_get(&object_bucket, object).unwrap().unwrap();
    backend
        .object_put(&object_bucket, object, b"new-object")
        .unwrap();
    backend
        .object_delete_if_sequence(
            &object_bucket,
            object,
            old_object.sequence,
            &old_object.nuid,
        )
        .unwrap();
    assert_eq!(
        backend
            .object_get(&object_bucket, object)
            .unwrap()
            .unwrap()
            .bytes,
        b"new-object"
    );
}

#[test]
fn guarded_object_delete_retries_chunk_purge_after_delete_marker() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let bucket = format!("JSFSOBJDEL{suffix}");
    let object = "queued/blob.bin";

    backend.ensure_object_bucket(&bucket).unwrap();
    backend.object_put(&bucket, object, b"old-object").unwrap();
    let store = context.object_store(&bucket).unwrap();
    let mut info = store.info(object).unwrap();
    let expected_sequence = backend
        .object_metadata(&bucket, object)
        .unwrap()
        .unwrap()
        .sequence;
    let old_nuid = info.nuid.clone();
    let stream = object_stream_name(&bucket);
    let chunk_subject = object_chunk_subject(&bucket, &old_nuid);
    assert!(
        context.get_last_message(&stream, &chunk_subject).is_ok(),
        "test setup expected object chunks before guarded delete retry"
    );

    info.chunks = 0;
    info.size = 0;
    info.deleted = true;
    let mut headers = HeaderMap::default();
    headers.insert("Nats-Rollup", "sub");
    let message = Message::new(
        &object_meta_subject(&bucket, object),
        None,
        serde_json::to_vec(&info).unwrap(),
        Some(headers),
    );
    context
        .publish_message_with_options(
            &message,
            &PublishOptions {
                timeout: Some(Duration::from_secs(2)),
                expected_stream: Some(stream.clone()),
                expected_last_subject_sequence: Some(expected_sequence),
                ..Default::default()
            },
        )
        .unwrap();

    backend
        .object_delete_if_sequence(&bucket, object, expected_sequence, &old_nuid)
        .unwrap();

    assert!(
        context.get_last_message(&stream, &chunk_subject).is_err(),
        "guarded delete replay must purge chunks before it is considered applied"
    );
}

#[test]
fn queued_object_replace_replay_purges_previous_chunks_before_marking_applied() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let bucket = format!("JSFSOBJPUTCLEAN{suffix}");
    let object = "queued/blob.bin";
    let idempotency_key = format!("queued-object-put-cleanup-{suffix}");

    backend.ensure_object_bucket(&bucket).unwrap();
    backend.object_put(&bucket, object, b"old-object").unwrap();
    let old_nuid = context
        .object_store(&bucket)
        .unwrap()
        .info(object)
        .unwrap()
        .nuid;
    let stream = object_stream_name(&bucket);
    let old_chunk_subject = object_chunk_subject(&bucket, &old_nuid);
    assert!(
        context
            .get_last_message(&stream, &old_chunk_subject)
            .is_ok(),
        "test setup expected old object chunks before queued replace"
    );

    publish_object_writeback_marker_with_previous_nuid(
        &context,
        &bucket,
        &idempotency_key,
        Some(&old_nuid),
    );
    publish_object_without_ledger_with_previous_nuid(
        &context,
        &bucket,
        object,
        b"queued-object",
        &idempotency_key,
        Some(&old_nuid),
    );
    context
        .publish(&old_chunk_subject, b"stale-old-chunk")
        .unwrap();
    assert!(
        context
            .get_last_message(&stream, &old_chunk_subject)
            .is_ok(),
        "test setup expected stale old chunks after metadata publish"
    );

    let tmp = tempfile::tempdir().unwrap();
    let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
    enqueue_failed_write(
        &mut replay,
        idempotency_key.clone(),
        FailedWriteOperation::ObjectPut {
            bucket: bucket.clone(),
            object: object.into(),
            bytes: b"queued-object".to_vec(),
        },
    );

    replay.replay(&backend).unwrap();

    assert_eq!(replay.state().pending, 0);
    assert_eq!(
        backend.object_get(&bucket, object).unwrap().unwrap().bytes,
        b"queued-object"
    );
    assert!(
        context
            .get_last_message(&stream, &old_chunk_subject)
            .is_err(),
        "queued object-put replay must purge previous chunks before the queue entry is removed"
    );
    assert!(backend
        .object_put_applied(&bucket, object, &idempotency_key)
        .unwrap());
}

#[test]
fn sparse_stream_message_listing_returns_only_existing_sequences() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let stream = format!("JSFSSPARSE{suffix}");
    let subject = format!("eventfs.{suffix}.events");
    backend.ensure_stream(&stream).unwrap();

    let sequences = backend
        .publish_json_lines(
            &stream,
            &subject,
            br#"{"event":1}
{"event":2}
{"event":3}"#,
            "sparse-seed",
        )
        .unwrap();
    assert_eq!(sequences.len(), 3);

    assert!(context.delete_message(&stream, sequences[1]).unwrap());

    assert_eq!(
        backend.list_stream_messages(&stream).unwrap(),
        vec![
            eventfs_transport::DirectoryEntry {
                name: format!("{}.json", sequences[0]),
                kind: eventfs_transport::EntryKind::File,
            },
            eventfs_transport::DirectoryEntry {
                name: format!("{}.json", sequences[2]),
                kind: eventfs_transport::EntryKind::File,
            },
        ]
    );
}

#[test]
fn stream_subject_listing_survives_stream_subject_reconfiguration() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let stream = format!("JSFSSUBJECTS{suffix}");
    let prefix = stream.to_ascii_lowercase();
    let subject_a = format!("{prefix}.alpha");
    let subject_b = format!("{prefix}.beta");

    backend.ensure_stream(&stream).unwrap();
    backend
        .publish_json_lines(&stream, &subject_a, br#"{"event":"a"}"#, "subject-a")
        .unwrap();
    backend
        .publish_json_lines(&stream, &subject_b, br#"{"event":"b"}"#, "subject-b")
        .unwrap();

    let mut info = context.stream_info(&stream).unwrap();
    info.config.subjects = vec![subject_a.clone(), format!("events.{stream}")];
    context.update_stream(&info.config).unwrap();

    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("reconnect to local JetStream");

    assert_eq!(
        backend.list_stream_subjects(&stream).unwrap(),
        vec![
            eventfs_transport::DirectoryEntry {
                name: format!("{subject_a}.jsonl"),
                kind: eventfs_transport::EntryKind::File,
            },
            eventfs_transport::DirectoryEntry {
                name: format!("{subject_b}.jsonl"),
                kind: eventfs_transport::EntryKind::File,
            },
        ]
    );
}

#[test]
fn object_store_watch_put_invalidates_precise_paths_without_gap() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");

    let suffix = unique_suffix();
    let bucket = format!("JSFSWATCHOBJ{suffix}");
    let path = "payloads/blob.txt";

    backend.ensure_object_bucket(&bucket).unwrap();
    let _ = backend.watch_events().unwrap();
    backend.object_put(&bucket, path, b"object-data").unwrap();

    let expected = vec![
        watch_invalidate(
            format!("/objects/{bucket}/{path}"),
            AffectedPathReason::Exact,
        ),
        watch_invalidate(
            format!("/objects/{bucket}/payloads"),
            AffectedPathReason::Ancestor,
        ),
        watch_invalidate(format!("/objects/{bucket}"), AffectedPathReason::Ancestor),
    ];
    let events = wait_for_watch_events(&backend, |events| {
        expected.iter().all(|event| events.contains(event))
    });

    assert!(
        !events.iter().any(|event| matches!(event, WatchEvent::Gap)),
        "object put should not produce cache-wide gaps: {events:?}"
    );
    for event in expected {
        assert!(
            events.contains(&event),
            "missing watch event {event:?} in {events:?}"
        );
    }
}

#[test]
fn object_store_watch_chunk_burst_does_not_force_gap_before_metadata() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let raw = nats::connect(&url).expect("connect raw NATS client");

    let suffix = unique_suffix();
    let bucket = format!("JSFSWATCHBURST{suffix}");
    let path = "payloads/large.bin";
    let chunk_subject = format!("$O.{bucket}.C.burst");

    backend.ensure_object_bucket(&bucket).unwrap();
    let _ = backend.watch_events().unwrap();

    for _ in 0..5000 {
        raw.publish(&chunk_subject, b"chunk-bytes").unwrap();
    }
    raw.flush_timeout(Duration::from_secs(2)).unwrap();

    backend.object_put(&bucket, path, b"object-data").unwrap();

    let expected = vec![
        watch_invalidate(
            format!("/objects/{bucket}/{path}"),
            AffectedPathReason::Exact,
        ),
        watch_invalidate(
            format!("/objects/{bucket}/payloads"),
            AffectedPathReason::Ancestor,
        ),
        watch_invalidate(format!("/objects/{bucket}"), AffectedPathReason::Ancestor),
    ];
    let events = wait_for_watch_events(&backend, |events| {
        expected.iter().all(|event| events.contains(event))
    });

    assert!(
        !events.iter().any(|event| matches!(event, WatchEvent::Gap)),
        "chunk burst should not degrade to cache-wide gap: {events:?}"
    );
    for event in expected {
        assert!(
            events.contains(&event),
            "missing watch event {event:?} in {events:?}"
        );
    }
}

#[test]
fn object_store_watch_marker_burst_stays_bounded_and_precise() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));
    let raw = nats::connect(&url).expect("connect raw NATS publisher");

    let suffix = unique_suffix();
    let bucket = format!("JSFSWATCHMARKER{suffix}");
    let path = "payloads/blob.txt";

    backend.ensure_object_bucket(&bucket).unwrap();
    ensure_object_writeback_marker_subject(&context, &bucket);
    let _ = backend.watch_events().unwrap();

    for index in 0..5000 {
        raw.publish(
            &object_writeback_marker_subject(&bucket, &format!("marker-{index}")),
            b"marker",
        )
        .unwrap();
    }
    raw.flush_timeout(Duration::from_secs(2)).unwrap();
    backend.object_put(&bucket, path, b"object-data").unwrap();

    let expected = vec![
        watch_invalidate(
            format!("/objects/{bucket}/{path}"),
            AffectedPathReason::Exact,
        ),
        watch_invalidate(
            format!("/objects/{bucket}/payloads"),
            AffectedPathReason::Ancestor,
        ),
        watch_invalidate(format!("/objects/{bucket}"), AffectedPathReason::Ancestor),
    ];
    let events = wait_for_first_non_empty_watch_events(&backend);

    assert!(
        !events.iter().any(|event| matches!(event, WatchEvent::Gap)),
        "marker burst should not degrade to cache-wide gap: {events:?}"
    );
    for event in expected {
        assert!(
            events.contains(&event),
            "missing watch event {event:?} in {events:?}"
        );
    }
}

#[test]
fn writeback_replay_crash_window_is_idempotent_for_kv_and_objects() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let kv_bucket = format!("JSFSREPLAYKV{suffix}");
    let object_bucket = format!("JSFSREPLAYOBJ{suffix}");
    let kv_key = "queued/value.json";
    let object_name = "queued/blob.txt";
    let kv_id = format!("queued-kv-{suffix}");
    let object_id = format!("queued-object-{suffix}");
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
        enqueue_failed_write(
            &mut replay,
            kv_id.clone(),
            FailedWriteOperation::KvPut {
                bucket: kv_bucket.clone(),
                key: kv_key.into(),
                bytes: br#"{"queued":true}"#.to_vec(),
            },
        );
        enqueue_failed_write(
            &mut replay,
            object_id.clone(),
            FailedWriteOperation::ObjectPut {
                bucket: object_bucket.clone(),
                object: object_name.into(),
                bytes: b"queued-object".to_vec(),
            },
        );
    }

    // Each reopen models a new process owner after the previous crash-window
    // attempt releases the single-writer queue lock.
    {
        let interrupting = InterruptingReplayStorage {
            store: &backend,
            interrupt_after: Some(kv_id.clone()),
        };
        let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
        assert!(replay.replay(&interrupting).is_err());
        assert_eq!(backend.kv_history(&kv_bucket, kv_key).unwrap().len(), 1);
    }

    let first_object_nuid = {
        let interrupting = InterruptingReplayStorage {
            store: &backend,
            interrupt_after: Some(object_id.clone()),
        };
        let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
        assert!(replay.replay(&interrupting).is_err());
        assert_eq!(backend.kv_history(&kv_bucket, kv_key).unwrap().len(), 1);
        let object_store = context.object_store(&object_bucket).unwrap();
        object_store.info(object_name).unwrap().nuid
    };

    {
        let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
        replay.replay(&backend).unwrap();
        assert_eq!(replay.state().pending, 0);
    }

    assert_eq!(backend.kv_history(&kv_bucket, kv_key).unwrap().len(), 1);
    assert_eq!(
        backend.kv_get(&kv_bucket, kv_key).unwrap().unwrap().bytes,
        br#"{"queued":true}"#
    );
    assert_eq!(
        backend
            .object_get(&object_bucket, object_name)
            .unwrap()
            .unwrap()
            .bytes,
        b"queued-object"
    );
    assert_eq!(
        context
            .object_store(&object_bucket)
            .unwrap()
            .info(object_name)
            .unwrap()
            .nuid,
        first_object_nuid
    );

    backend
        .kv_put(&kv_bucket, kv_key, br#"{"later":true}"#)
        .unwrap();
    assert!(
        !backend
            .list_kv_prefix(&kv_bucket, "")
            .unwrap()
            .iter()
            .any(|entry| entry.name.contains("__eventfs_writeback")),
        "hidden KV writeback marker leaked into bucket listing"
    );
    backend
        .object_put(&object_bucket, object_name, b"later-object")
        .unwrap();
    assert!(backend.kv_put_applied(&kv_bucket, kv_key, &kv_id).unwrap());
    assert!(backend
        .object_put_applied(&object_bucket, object_name, &object_id)
        .unwrap());
}

#[test]
fn writeback_replay_requires_durable_proof_after_later_overwrites() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            stream_duplicate_window: Duration::from_millis(100),
            kv_history: 1,
            retries: 1,
            backoff: Duration::from_millis(10),
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let kv_bucket = format!("JSFSPRELEDGERKV{suffix}");
    let object_bucket = format!("JSFSPRELEDGEROBJ{suffix}");
    let stream = format!("JSFSPRELEDGERSTREAM{suffix}");
    let subject = format!("{}.events", stream.to_ascii_lowercase());
    let kv_key = "queued/value.json";
    let object_name = "queued/blob.txt";
    let kv_id = format!("pre-ledger-kv-{suffix}");
    let object_id = format!("pre-ledger-object-{suffix}");
    let stream_id = format!("pre-ledger-stream-{suffix}");

    backend.ensure_kv_bucket(&kv_bucket).unwrap();
    publish_kv_writeback_marker(&context, &kv_bucket, kv_key, &kv_id);
    publish_kv_value_without_ledger(&context, &kv_bucket, kv_key, br#"{"queued":true}"#, &kv_id);
    publish_kv_applied_marker(&context, &kv_bucket, kv_key, &kv_id);
    backend
        .kv_put(&kv_bucket, kv_key, br#"{"later":true}"#)
        .unwrap();

    backend.ensure_object_bucket(&object_bucket).unwrap();
    publish_object_writeback_marker(&context, &object_bucket, &object_id);
    publish_object_without_ledger(
        &context,
        &object_bucket,
        object_name,
        b"queued-object",
        &object_id,
    );
    backend
        .object_put(&object_bucket, object_name, b"later-object")
        .unwrap();
    let later_object_nuid = context
        .object_store(&object_bucket)
        .unwrap()
        .info(object_name)
        .unwrap()
        .nuid;
    assert!(
        !backend
            .list_object_prefix(&object_bucket, "")
            .unwrap()
            .iter()
            .any(|entry| entry.name.contains("__eventfs_writeback")),
        "hidden object writeback marker leaked into bucket listing"
    );

    backend.ensure_stream(&stream).unwrap();
    publish_stream_line_without_ledger(
        &context,
        &stream,
        &subject,
        br#"{"queued":true}"#,
        &format!("{stream_id}:0"),
    );
    std::thread::sleep(Duration::from_millis(150));

    let tmp = tempfile::tempdir().unwrap();
    let mut replay = WritebackReplay::open(tmp.path(), 8).unwrap();
    enqueue_failed_write(
        &mut replay,
        kv_id,
        FailedWriteOperation::KvPut {
            bucket: kv_bucket.clone(),
            key: kv_key.into(),
            bytes: br#"{"queued":true}"#.to_vec(),
        },
    );
    enqueue_failed_write(
        &mut replay,
        object_id,
        FailedWriteOperation::ObjectPut {
            bucket: object_bucket.clone(),
            object: object_name.into(),
            bytes: b"queued-object".to_vec(),
        },
    );
    enqueue_failed_write(
        &mut replay,
        stream_id.clone(),
        FailedWriteOperation::PublishJsonLines {
            stream: stream.clone(),
            subject: subject.clone(),
            bytes: br#"{"queued":true}"#.to_vec(),
            applied_lines: 0,
        },
    );

    replay.replay(&backend).unwrap();

    assert_eq!(replay.state().pending, 0);
    assert_eq!(
        backend.kv_get(&kv_bucket, kv_key).unwrap().unwrap().bytes,
        br#"{"later":true}"#
    );
    assert_eq!(
        backend
            .object_get(&object_bucket, object_name)
            .unwrap()
            .unwrap()
            .bytes,
        b"later-object"
    );
    assert_eq!(
        context
            .object_store(&object_bucket)
            .unwrap()
            .info(object_name)
            .unwrap()
            .nuid,
        later_object_nuid
    );
    assert_eq!(backend.list_stream_messages(&stream).unwrap().len(), 1);
    assert!(backend
        .publish_json_lines_applied(&stream, &subject, br#"{"queued":true}"#, &stream_id)
        .unwrap());
}

#[test]
fn kv_replay_respects_apply_marker_after_history_churn() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            stream_duplicate_window: Duration::from_millis(100),
            kv_history: 1,
            retries: 1,
            backoff: Duration::from_millis(10),
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let kv_bucket = format!("JSFSAPPLIEDMARKERKV{suffix}");
    let kv_key = "queued/value.json";
    let kv_id = format!("applied-marker-kv-{suffix}");

    backend.ensure_kv_bucket(&kv_bucket).unwrap();
    publish_kv_writeback_marker(&context, &kv_bucket, kv_key, &kv_id);
    publish_kv_value_without_ledger(&context, &kv_bucket, kv_key, br#"{"queued":true}"#, &kv_id);
    publish_kv_applied_marker(&context, &kv_bucket, kv_key, &kv_id);
    for index in 0..4 {
        backend
            .kv_put(
                &kv_bucket,
                kv_key,
                format!(r#"{{"later":{index}}}"#).as_bytes(),
            )
            .unwrap();
    }

    let tmp = tempfile::tempdir().unwrap();
    let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
    enqueue_failed_write(
        &mut replay,
        kv_id.clone(),
        FailedWriteOperation::KvPut {
            bucket: kv_bucket.clone(),
            key: kv_key.into(),
            bytes: br#"{"queued":true}"#.to_vec(),
        },
    );

    replay.replay(&backend).unwrap();

    assert_eq!(replay.state().pending, 0);
    assert_eq!(
        backend.kv_get(&kv_bucket, kv_key).unwrap().unwrap().bytes,
        br#"{"later":3}"#
    );
    assert!(backend.kv_put_applied(&kv_bucket, kv_key, &kv_id).unwrap());
    assert!(
        !backend
            .list_kv_prefix(&kv_bucket, "")
            .unwrap()
            .iter()
            .any(|entry| entry.name.contains("__eventfs_applied")),
        "hidden KV applied marker leaked into bucket listing"
    );
}

#[test]
fn kv_replay_uses_pre_marker_target_sequence_after_history_churn() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            stream_duplicate_window: Duration::from_millis(100),
            kv_history: 1,
            retries: 1,
            backoff: Duration::from_millis(10),
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let kv_bucket = format!("JSFSPREMARKERKV{suffix}");
    let kv_key = "queued/value.json";
    let kv_id = format!("pre-marker-kv-{suffix}");

    backend.ensure_kv_bucket(&kv_bucket).unwrap();
    set_stream_duplicate_window(
        &context,
        &kv_stream_name(&kv_bucket),
        Duration::from_millis(100),
    );
    publish_kv_writeback_marker(&context, &kv_bucket, kv_key, &kv_id);
    publish_kv_value_without_ledger(&context, &kv_bucket, kv_key, br#"{"queued":true}"#, &kv_id);
    for index in 0..4 {
        backend
            .kv_put(
                &kv_bucket,
                kv_key,
                format!(r#"{{"later":{index}}}"#).as_bytes(),
            )
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(150));

    let tmp = tempfile::tempdir().unwrap();
    let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
    enqueue_failed_write(
        &mut replay,
        kv_id.clone(),
        FailedWriteOperation::KvPut {
            bucket: kv_bucket.clone(),
            key: kv_key.into(),
            bytes: br#"{"queued":true}"#.to_vec(),
        },
    );

    replay.replay(&backend).unwrap();

    assert_eq!(replay.state().pending, 0);
    assert_eq!(
        backend.kv_get(&kv_bucket, kv_key).unwrap().unwrap().bytes,
        br#"{"later":3}"#
    );
    assert!(backend.kv_put_applied(&kv_bucket, kv_key, &kv_id).unwrap());
}

#[test]
fn kv_replay_ignores_forged_marker_payloads() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let applied_bucket = format!("JSFSFORGEDAPPLIED{suffix}");
    let pre_marker_bucket = format!("JSFSFORGEDPRE{suffix}");
    let key = "queued/value.json";
    let applied_id = format!("forged-applied-{suffix}");
    let pre_marker_id = format!("forged-pre-{suffix}");

    backend.ensure_kv_bucket(&applied_bucket).unwrap();
    publish_kv_marker_payload(
        &context,
        &kv_applied_marker_subject(&applied_bucket, &applied_id),
        b"not-the-expected-marker-payload",
    );

    let tmp = tempfile::tempdir().unwrap();
    let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
    enqueue_failed_write(
        &mut replay,
        applied_id.clone(),
        FailedWriteOperation::KvPut {
            bucket: applied_bucket.clone(),
            key: key.into(),
            bytes: br#"{"queued":"applied"}"#.to_vec(),
        },
    );

    replay.replay(&backend).unwrap();

    assert_eq!(replay.state().pending, 0);
    assert_eq!(
        backend.kv_get(&applied_bucket, key).unwrap().unwrap().bytes,
        br#"{"queued":"applied"}"#
    );

    backend.ensure_kv_bucket(&pre_marker_bucket).unwrap();
    publish_kv_marker_payload(
        &context,
        &kv_writeback_marker_subject(&pre_marker_bucket, &pre_marker_id),
        b"not-the-expected-marker-payload",
    );
    backend
        .kv_put(&pre_marker_bucket, key, br#"{"later":true}"#)
        .unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
    enqueue_failed_write(
        &mut replay,
        pre_marker_id.clone(),
        FailedWriteOperation::KvPut {
            bucket: pre_marker_bucket.clone(),
            key: key.into(),
            bytes: br#"{"queued":"pre"}"#.to_vec(),
        },
    );

    replay.replay(&backend).unwrap();

    assert_eq!(replay.state().pending, 0);
    assert_eq!(
        backend
            .kv_get(&pre_marker_bucket, key)
            .unwrap()
            .unwrap()
            .bytes,
        br#"{"queued":"pre"}"#
    );
}

#[test]
fn writeback_replay_handles_marker_only_kv_and_object_writes_after_later_overwrites() {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream");
    let context = nats::jetstream::new(nats::connect(&url).expect("connect raw NATS client"));

    let suffix = unique_suffix();
    let kv_bucket = format!("JSFSMARKERONLYKV{suffix}");
    let object_bucket = format!("JSFSMARKERONLYOBJ{suffix}");
    let kv_key = "queued/value.json";
    let object_name = "queued/blob.txt";
    let kv_id = format!("marker-only-kv-{suffix}");
    let object_id = format!("marker-only-object-{suffix}");

    backend.ensure_kv_bucket(&kv_bucket).unwrap();
    publish_kv_writeback_marker(&context, &kv_bucket, kv_key, &kv_id);
    backend
        .kv_put(&kv_bucket, kv_key, br#"{"later":true}"#)
        .unwrap();

    backend.ensure_object_bucket(&object_bucket).unwrap();
    publish_object_writeback_marker(&context, &object_bucket, &object_id);
    backend
        .object_put(&object_bucket, object_name, b"later-object")
        .unwrap();
    let later_object_nuid = context
        .object_store(&object_bucket)
        .unwrap()
        .info(object_name)
        .unwrap()
        .nuid;

    let tmp = tempfile::tempdir().unwrap();
    let mut replay = WritebackReplay::open(tmp.path(), 4).unwrap();
    enqueue_failed_write(
        &mut replay,
        kv_id,
        FailedWriteOperation::KvPut {
            bucket: kv_bucket.clone(),
            key: kv_key.into(),
            bytes: br#"{"queued":true}"#.to_vec(),
        },
    );
    enqueue_failed_write(
        &mut replay,
        object_id,
        FailedWriteOperation::ObjectPut {
            bucket: object_bucket.clone(),
            object: object_name.into(),
            bytes: b"queued-object".to_vec(),
        },
    );

    replay.replay(&backend).unwrap();

    assert_eq!(replay.state().pending, 0);
    assert_eq!(
        backend.kv_get(&kv_bucket, kv_key).unwrap().unwrap().bytes,
        br#"{"later":true}"#
    );
    assert_eq!(
        backend
            .object_get(&object_bucket, object_name)
            .unwrap()
            .unwrap()
            .bytes,
        b"later-object"
    );
    assert_eq!(
        context
            .object_store(&object_bucket)
            .unwrap()
            .info(object_name)
            .unwrap()
            .nuid,
        later_object_nuid
    );
}

struct InterruptingReplayStorage<'a> {
    store: &'a dyn ReplayStorage,
    interrupt_after: Option<String>,
}

impl InterruptingReplayStorage<'_> {
    fn maybe_interrupt(&self, idempotency_key: &str) -> TransportResult<()> {
        if self.interrupt_after.as_deref() == Some(idempotency_key) {
            return Err(TransportError::Invalid(
                "simulated interruption after durable side effect".into(),
            ));
        }
        Ok(())
    }
}

impl ReplayStorage for InterruptingReplayStorage<'_> {
    fn kv_put_idempotent(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<u64> {
        let revision = self
            .store
            .kv_put_idempotent(bucket, key, bytes, idempotency_key)?;
        self.maybe_interrupt(idempotency_key)?;
        Ok(revision)
    }

    fn kv_put_applied(
        &self,
        bucket: &str,
        key: &str,
        idempotency_key: &str,
    ) -> TransportResult<bool> {
        self.store.kv_put_applied(bucket, key, idempotency_key)
    }

    fn kv_delete_if_revision(
        &self,
        bucket: &str,
        key: &str,
        expected_revision: u64,
    ) -> TransportResult<()> {
        self.store
            .kv_delete_if_revision(bucket, key, expected_revision)
    }

    fn kv_delete_if_revision_applied(
        &self,
        bucket: &str,
        key: &str,
        expected_revision: u64,
    ) -> TransportResult<bool> {
        self.store
            .kv_delete_if_revision_applied(bucket, key, expected_revision)
    }

    fn publish_json_lines(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<Vec<u64>> {
        let sequences = self
            .store
            .publish_json_lines(stream, subject, bytes, idempotency_seed)?;
        self.maybe_interrupt(idempotency_seed)?;
        Ok(sequences)
    }

    fn publish_json_lines_applied(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<bool> {
        self.store
            .publish_json_lines_applied(stream, subject, bytes, idempotency_seed)
    }

    fn publish_json_lines_applied_prefix(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<usize> {
        self.store
            .publish_json_lines_applied_prefix(stream, subject, bytes, idempotency_seed)
    }

    fn object_put_idempotent(
        &self,
        bucket: &str,
        object: &str,
        bytes: &[u8],
        idempotency_key: &str,
    ) -> TransportResult<()> {
        self.store
            .object_put_idempotent(bucket, object, bytes, idempotency_key)?;
        self.maybe_interrupt(idempotency_key)
    }

    fn object_put_applied(
        &self,
        bucket: &str,
        object: &str,
        idempotency_key: &str,
    ) -> TransportResult<bool> {
        self.store
            .object_put_applied(bucket, object, idempotency_key)
    }

    fn object_delete_if_sequence(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<()> {
        self.store
            .object_delete_if_sequence(bucket, object, expected_sequence, expected_nuid)
    }

    fn object_delete_if_sequence_applied(
        &self,
        bucket: &str,
        object: &str,
        expected_sequence: u64,
        expected_nuid: &str,
    ) -> TransportResult<bool> {
        self.store.object_delete_if_sequence_applied(
            bucket,
            object,
            expected_sequence,
            expected_nuid,
        )
    }
}

fn publish_kv_writeback_marker(
    context: &nats::jetstream::JetStream,
    bucket: &str,
    key: &str,
    idempotency_key: &str,
) {
    context
        .publish_with_options(
            &kv_writeback_marker_subject(bucket, idempotency_key),
            kv_marker_payload("kv-writeback-marker", key, idempotency_key),
            &PublishOptions {
                timeout: Some(Duration::from_secs(2)),
                id: Some(format!("writeback-marker:{idempotency_key}")),
                expected_stream: Some(kv_stream_name(bucket)),
                ..Default::default()
            },
        )
        .unwrap();
}

fn set_stream_duplicate_window(
    context: &nats::jetstream::JetStream,
    stream: &str,
    duplicate_window: Duration,
) {
    let mut info = context.stream_info(stream).unwrap();
    info.config.duplicate_window = duplicate_window.as_nanos() as i64;
    context.update_stream(&info.config).unwrap();
}

fn publish_kv_value_without_ledger(
    context: &nats::jetstream::JetStream,
    bucket: &str,
    key: &str,
    bytes: &[u8],
    idempotency_key: &str,
) {
    let mut headers = HeaderMap::default();
    headers.insert("EventFS-Idempotency-Key", idempotency_key.to_string());
    let message = Message::new(&kv_subject(bucket, key), None, bytes, Some(headers));
    context
        .publish_message_with_options(
            &message,
            &PublishOptions {
                timeout: Some(Duration::from_secs(2)),
                id: Some(idempotency_key.to_string()),
                expected_stream: Some(kv_stream_name(bucket)),
                ..Default::default()
            },
        )
        .unwrap();
}

fn publish_kv_applied_marker(
    context: &nats::jetstream::JetStream,
    bucket: &str,
    key: &str,
    idempotency_key: &str,
) {
    context
        .publish_with_options(
            &kv_applied_marker_subject(bucket, idempotency_key),
            kv_marker_payload("kv-applied-marker", key, idempotency_key),
            &PublishOptions {
                timeout: Some(Duration::from_secs(2)),
                id: Some(format!("writeback-applied-marker:{idempotency_key}")),
                expected_stream: Some(kv_stream_name(bucket)),
                ..Default::default()
            },
        )
        .unwrap();
}

fn publish_kv_marker_payload(context: &nats::jetstream::JetStream, subject: &str, payload: &[u8]) {
    context
        .publish_with_options(
            subject,
            payload,
            &PublishOptions {
                timeout: Some(Duration::from_secs(2)),
                expected_stream: None,
                ..Default::default()
            },
        )
        .unwrap();
}

fn kv_marker_payload(operation: &str, key: &str, idempotency_key: &str) -> Vec<u8> {
    serde_json::json!({
        "operation": operation,
        "idempotency_key": idempotency_key,
        "key": key,
    })
    .to_string()
    .into_bytes()
}

fn publish_object_writeback_marker(
    context: &nats::jetstream::JetStream,
    bucket: &str,
    idempotency_key: &str,
) {
    publish_object_writeback_marker_with_previous_nuid(context, bucket, idempotency_key, None);
}

fn publish_object_writeback_marker_with_previous_nuid(
    context: &nats::jetstream::JetStream,
    bucket: &str,
    idempotency_key: &str,
    previous_nuid: Option<&str>,
) {
    ensure_object_writeback_marker_subject(context, bucket);
    context
        .publish_with_options(
            &object_writeback_marker_subject(bucket, idempotency_key),
            object_writeback_marker_payload(idempotency_key, previous_nuid),
            &PublishOptions {
                timeout: Some(Duration::from_secs(2)),
                id: Some(format!("writeback-marker:{idempotency_key}")),
                expected_stream: Some(object_stream_name(bucket)),
                ..Default::default()
            },
        )
        .unwrap();
}

fn ensure_object_writeback_marker_subject(context: &nats::jetstream::JetStream, bucket: &str) {
    let stream = object_stream_name(bucket);
    let subject = object_writeback_marker_subject_filter(bucket);
    let mut info = context.stream_info(&stream).unwrap();
    if !info
        .config
        .subjects
        .iter()
        .any(|candidate| candidate == &subject)
    {
        info.config.subjects.push(subject);
        context.update_stream(&info.config).unwrap();
    }
}

fn publish_object_without_ledger(
    context: &nats::jetstream::JetStream,
    bucket: &str,
    object: &str,
    bytes: &[u8],
    idempotency_key: &str,
) {
    publish_object_without_ledger_with_previous_nuid(
        context,
        bucket,
        object,
        bytes,
        idempotency_key,
        None,
    );
}

fn publish_object_without_ledger_with_previous_nuid(
    context: &nats::jetstream::JetStream,
    bucket: &str,
    object: &str,
    bytes: &[u8],
    idempotency_key: &str,
    previous_nuid: Option<&str>,
) {
    let store = context.object_store(bucket).unwrap();
    let mut cursor = Cursor::new(bytes);
    store
        .put(
            ObjectMeta {
                name: object.to_string(),
                description: Some(object_idempotency_description(
                    idempotency_key,
                    previous_nuid,
                )),
                link: None,
            },
            &mut cursor,
        )
        .unwrap();
}

fn wait_for_watch_events(
    backend: &NatsStorage,
    done: impl Fn(&[WatchEvent]) -> bool,
) -> Vec<WatchEvent> {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut events = Vec::new();
    loop {
        events.extend(backend.watch_events().unwrap());
        if done(&events) {
            return events;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for watch events: {events:?}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_first_non_empty_watch_events(backend: &NatsStorage) -> Vec<WatchEvent> {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let events = backend.watch_events().unwrap();
        if !events.is_empty() {
            return events;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for watch events");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn publish_stream_line_without_ledger(
    context: &nats::jetstream::JetStream,
    stream: &str,
    subject: &str,
    bytes: &[u8],
    message_id: &str,
) {
    context
        .publish_with_options(
            subject,
            bytes,
            &PublishOptions {
                timeout: Some(Duration::from_secs(2)),
                id: Some(message_id.to_string()),
                expected_stream: Some(stream.to_string()),
                ..Default::default()
            },
        )
        .unwrap();
}

fn kv_stream_name(bucket: &str) -> String {
    format!("KV_{bucket}")
}

fn kv_subject(bucket: &str, key: &str) -> String {
    format!("$KV.{bucket}.{key}")
}

fn kv_writeback_marker_subject(bucket: &str, idempotency_key: &str) -> String {
    kv_subject(
        bucket,
        &format!(
            "__eventfs_writeback.{}",
            hex_encode(idempotency_key.as_bytes())
        ),
    )
}

fn kv_applied_marker_subject(bucket: &str, idempotency_key: &str) -> String {
    kv_subject(
        bucket,
        &format!(
            "__eventfs_applied.{}",
            hex_encode(idempotency_key.as_bytes())
        ),
    )
}

fn object_stream_name(bucket: &str) -> String {
    format!("OBJ_{bucket}")
}

fn object_meta_subject(bucket: &str, object: &str) -> String {
    format!(
        "$O.{bucket}.M.{}",
        base64::encode_config(object, base64::URL_SAFE)
    )
}

fn object_chunk_subject(bucket: &str, nuid: &str) -> String {
    format!("$O.{bucket}.C.{nuid}")
}

fn object_writeback_marker_subject(bucket: &str, idempotency_key: &str) -> String {
    format!("$O.{bucket}.W.{}", hex_encode(idempotency_key.as_bytes()))
}

fn object_writeback_marker_subject_filter(bucket: &str) -> String {
    format!("$O.{bucket}.W.>")
}

fn object_writeback_marker_payload(idempotency_key: &str, previous_nuid: Option<&str>) -> Vec<u8> {
    serde_json::json!({
        "idempotency_key": idempotency_key,
        "previous_nuid": previous_nuid,
    })
    .to_string()
    .into_bytes()
}

fn object_idempotency_description(idempotency_key: &str, previous_nuid: Option<&str>) -> String {
    format!(
        "eventfs-idempotency:{}",
        serde_json::json!({
            "idempotency_key": idempotency_key,
            "previous_nuid": previous_nuid,
        })
    )
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

fn unique_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string()
}
