use std::time::Duration;

use eventfs_protocol::TASKS_BUCKET;
use eventfs_transport::{MountStorage, NatsStorage, NatsStorageConfig, TransportError};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("NATS_URL").ok())
        .unwrap_or_else(|| "nats://127.0.0.1:4222".into());

    let backend = NatsStorage::connect(
        &url,
        None,
        NatsStorageConfig {
            timeout: Duration::from_secs(2),
            retries: 1,
            backoff: Duration::from_millis(10),
            ..Default::default()
        },
    )?;

    expect_kv(&backend, "smoke", "greeting.json", br#"{"hello":"kv"}"#)?;
    expect_stream(
        &backend,
        "system",
        "events.system",
        br#"{"kind":"event","n":1}"#,
    )?;
    expect_stream(
        &backend,
        "system",
        "events.system",
        br#"{"kind":"stream","n":1}"#,
    )?;
    expect_object(&backend, "smoke", "payload.txt", b"object payload")?;
    expect_kv(
        &backend,
        TASKS_BUCKET,
        "demo/render-001.json",
        br#"{"task":"render","state":"new"}"#,
    )?;

    println!("[probe] broker state verified");
    Ok(())
}

fn expect_kv(
    backend: &impl MountStorage,
    bucket: &str,
    key: &str,
    expected: &[u8],
) -> Result<(), TransportError> {
    let entry = backend
        .kv_get(bucket, key)?
        .ok_or(TransportError::NotFound)?;
    if entry.bytes != expected {
        return Err(TransportError::Invalid(format!(
            "KV {bucket}/{key} contained {:?}, expected {:?}",
            String::from_utf8_lossy(&entry.bytes),
            String::from_utf8_lossy(expected)
        )));
    }
    Ok(())
}

fn expect_stream(
    backend: &impl MountStorage,
    stream: &str,
    subject: &str,
    expected: &[u8],
) -> Result<(), TransportError> {
    let entries = backend.list_stream_messages(stream)?;
    for entry in entries {
        let Some(sequence) = entry.name.strip_suffix(".json") else {
            continue;
        };
        let Ok(sequence) = sequence.parse::<u64>() else {
            continue;
        };
        let message = backend.stream_message(stream, sequence)?;
        if message.subject == subject && message.payload == expected {
            return Ok(());
        }
    }
    Err(TransportError::NotFound)
}

fn expect_object(
    backend: &impl MountStorage,
    bucket: &str,
    object: &str,
    expected: &[u8],
) -> Result<(), TransportError> {
    let bytes = backend
        .object_get(bucket, object)?
        .ok_or(TransportError::NotFound)?
        .bytes;
    if bytes != expected {
        return Err(TransportError::Invalid(format!(
            "object {bucket}/{object} contained {:?}, expected {:?}",
            String::from_utf8_lossy(&bytes),
            String::from_utf8_lossy(expected)
        )));
    }
    Ok(())
}
