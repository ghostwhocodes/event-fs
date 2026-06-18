use std::time::{SystemTime, UNIX_EPOCH};

use eventfs_transport::{
    DirectoryEntry, EntryKind, MemoryStorage, MountStorage, StreamMessageView,
};

#[cfg(feature = "jetstream-tests")]
use std::time::Duration;

#[cfg(feature = "jetstream-tests")]
use eventfs_transport::{NatsStorage, NatsStorageConfig};

fn assert_entry(entries: &[DirectoryEntry], name: &str, kind: EntryKind) {
    assert!(
        entries
            .iter()
            .any(|entry| entry.name == name && entry.kind == kind),
        "missing {name:?} {kind:?} in {entries:?}"
    );
}

fn assert_message(message: StreamMessageView, stream: &str, subject: &str, payload: &[u8]) {
    assert_eq!(message.stream, stream);
    assert_eq!(message.subject, subject);
    assert_eq!(message.payload, payload);
    assert!(message.published >= UNIX_EPOCH);
}

fn storage_contract_storage_surfaces(store: &dyn MountStorage, suffix: &str) {
    let kv_bucket = format!("ADAPTERKV{suffix}");
    let object_bucket = format!("ADAPTEROBJ{suffix}");
    let stream = format!("ADAPTERSTREAM{suffix}");
    let subject = format!("adapter.{suffix}.created");

    store.ensure_kv_bucket(&kv_bucket).unwrap();
    assert!(store.list_kv_buckets().unwrap().contains(&kv_bucket));
    let rev1 = store
        .kv_put(&kv_bucket, "config/app.json", br#"{"rev":1}"#)
        .unwrap();
    let rev2 = store
        .kv_put(&kv_bucket, "config/app.json", br#"{"rev":2}"#)
        .unwrap();
    assert!(rev2 > rev1);
    assert_eq!(
        store
            .kv_get(&kv_bucket, "config/app.json")
            .unwrap()
            .unwrap()
            .bytes,
        br#"{"rev":2}"#
    );
    assert_eq!(
        store
            .kv_revision(&kv_bucket, "config/app.json", rev1)
            .unwrap()
            .unwrap()
            .bytes,
        br#"{"rev":1}"#
    );
    assert_entry(
        &store.list_kv_prefix(&kv_bucket, "").unwrap(),
        "config",
        EntryKind::Directory,
    );
    assert_entry(
        &store.list_kv_history_prefix(&kv_bucket, "").unwrap(),
        "config",
        EntryKind::Directory,
    );

    store.ensure_stream(&stream).unwrap();
    let sequences = store
        .publish_json_lines(
            &stream,
            &subject,
            br#"{"event":1}
{"event":2}
"#,
            "stream-seed",
        )
        .unwrap();
    assert_eq!(sequences.len(), 2);
    store
        .publish_json_lines(
            &stream,
            &subject,
            br#"{"event":1}
{"event":2}
"#,
            "stream-seed",
        )
        .unwrap();
    assert_eq!(store.list_stream_messages(&stream).unwrap().len(), 2);
    assert_entry(
        &store.list_stream_subjects(&stream).unwrap(),
        &eventfs_protocol::stream_subject_file_name_from_str(&subject),
        EntryKind::File,
    );
    assert_message(
        store.stream_message(&stream, sequences[0]).unwrap(),
        &stream,
        &subject,
        br#"{"event":1}"#,
    );

    store
        .publish_json_lines(
            eventfs_protocol::subjects::AGENTS_STREAM,
            "agents.bot.inbox",
            br#"{"task":"run"}"#,
            "agent-seed",
        )
        .unwrap();
    assert_eq!(store.list_agent_names().unwrap(), vec!["bot".to_string()]);

    store.ensure_object_bucket(&object_bucket).unwrap();
    store
        .object_put(&object_bucket, "assets/blob.txt", b"object-data")
        .unwrap();
    let object = store
        .object_get(&object_bucket, "assets/blob.txt")
        .unwrap()
        .unwrap();
    assert_eq!(object.bytes, b"object-data");
    assert!(object.modified >= UNIX_EPOCH);
    assert_eq!(
        store
            .object_metadata(&object_bucket, "assets/blob.txt")
            .unwrap()
            .unwrap()
            .size,
        b"object-data".len() as u64
    );
    assert_entry(
        &store.list_object_prefix(&object_bucket, "").unwrap(),
        "assets",
        EntryKind::Directory,
    );
    store
        .object_delete(&object_bucket, "assets/blob.txt")
        .unwrap();
    assert!(store
        .object_get(&object_bucket, "assets/blob.txt")
        .unwrap()
        .is_none());
}

fn storage_contract_writeback_surface(store: &dyn MountStorage, suffix: &str) {
    let kv_bucket = format!("ADAPTERWBKV{suffix}");
    let object_bucket = format!("ADAPTERWBOBJ{suffix}");
    let stream = format!("ADAPTERWBSTREAM{suffix}");
    let subject = format!("adapter.{suffix}.writeback");

    let first_revision = store
        .kv_put_idempotent(&kv_bucket, "state.json", br#"{"state":1}"#, "kv-once")
        .unwrap();
    let second_revision = store
        .kv_put_idempotent(&kv_bucket, "state.json", br#"{"state":2}"#, "kv-once")
        .unwrap();
    let current = store.kv_get(&kv_bucket, "state.json").unwrap().unwrap();
    assert_eq!(current.bytes, br#"{"state":1}"#);
    assert_eq!(current.revision, first_revision);
    assert!(second_revision == first_revision || second_revision == 0);
    assert!(store
        .kv_put_applied(&kv_bucket, "state.json", "kv-once")
        .unwrap());
    assert!(!store
        .kv_delete_if_revision_applied(&kv_bucket, "state.json", current.revision)
        .unwrap());
    store
        .kv_delete_if_revision(
            &kv_bucket,
            "state.json",
            current.revision.saturating_add(10),
        )
        .unwrap();
    assert!(!store
        .kv_delete_if_revision_applied(&kv_bucket, "state.json", current.revision)
        .unwrap());
    store
        .kv_delete_if_revision(&kv_bucket, "state.json", current.revision)
        .unwrap();
    assert!(store
        .kv_delete_if_revision_applied(&kv_bucket, "state.json", current.revision)
        .unwrap());

    let payload = br#"{"line":1}
{"line":2}
"#;
    let sequences = store
        .publish_json_lines(&stream, &subject, payload, "jsonl-once")
        .unwrap();
    assert_eq!(sequences.len(), 2);
    store
        .publish_json_lines(&stream, &subject, payload, "jsonl-once")
        .unwrap();
    assert!(store
        .publish_json_lines_applied(&stream, &subject, payload, "jsonl-once")
        .unwrap());
    assert_eq!(
        store
            .publish_json_lines_applied_prefix(&stream, &subject, payload, "jsonl-once")
            .unwrap(),
        2
    );

    store
        .object_put_idempotent(&object_bucket, "blob.txt", b"object-v1", "object-once")
        .unwrap();
    store
        .object_put_idempotent(&object_bucket, "blob.txt", b"object-v2", "object-once")
        .unwrap();
    assert!(store
        .object_put_applied(&object_bucket, "blob.txt", "object-once")
        .unwrap());
    let object = store
        .object_get(&object_bucket, "blob.txt")
        .unwrap()
        .unwrap();
    assert!(!store
        .object_delete_if_sequence_applied(
            &object_bucket,
            "blob.txt",
            object.sequence,
            &object.nuid
        )
        .unwrap());
    store
        .object_delete_if_sequence(&object_bucket, "blob.txt", 99, "")
        .unwrap();
    assert!(!store
        .object_delete_if_sequence_applied(
            &object_bucket,
            "blob.txt",
            object.sequence,
            &object.nuid
        )
        .unwrap());
    store
        .object_delete_if_sequence(&object_bucket, "blob.txt", object.sequence, &object.nuid)
        .unwrap();
    assert!(store
        .object_delete_if_sequence_applied(
            &object_bucket,
            "blob.txt",
            object.sequence,
            &object.nuid
        )
        .unwrap());

    store
        .object_put(&object_bucket, "blob.txt", b"object-v3")
        .unwrap();
    let recreated = store
        .object_get(&object_bucket, "blob.txt")
        .unwrap()
        .unwrap();
    assert_ne!(
        (recreated.sequence, recreated.nuid.as_str()),
        (object.sequence, object.nuid.as_str())
    );
    assert!(store
        .object_delete_if_sequence_applied(
            &object_bucket,
            "blob.txt",
            object.sequence,
            &object.nuid
        )
        .unwrap());
    store
        .object_delete_if_sequence(&object_bucket, "blob.txt", object.sequence, &object.nuid)
        .unwrap();
    assert_eq!(
        store
            .object_get(&object_bucket, "blob.txt")
            .unwrap()
            .unwrap()
            .bytes,
        b"object-v3"
    );
}

fn suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string()
}

#[test]
fn storage_contract_memory_storage_surfaces() {
    let store = MemoryStorage::new();
    storage_contract_storage_surfaces(&store, &suffix());
}

#[test]
fn storage_contract_memory_writeback_surface() {
    let store = MemoryStorage::new();
    storage_contract_writeback_surface(&store, &suffix());
}

#[cfg(feature = "jetstream-tests")]
fn nats_store() -> NatsStorage {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .expect("connect to local JetStream")
}

#[cfg(feature = "jetstream-tests")]
#[test]
fn storage_contract_nats_storage_surfaces() {
    let store = nats_store();
    storage_contract_storage_surfaces(&store, &suffix());
}

#[cfg(feature = "jetstream-tests")]
#[test]
fn storage_contract_nats_writeback_surface() {
    let store = nats_store();
    storage_contract_writeback_surface(&store, &suffix());
}
