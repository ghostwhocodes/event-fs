use std::fmt::Write as _;

use nats::jetstream::{PublishOptions, StorageType, StreamConfig};

use crate::TransportResult;

use super::core::{duration_as_nanos, is_already_exists, is_not_found, NatsCore};

const STREAM: &str = "EVENTFS_WRITEBACK";
const SUBJECT_PREFIX: &str = "eventfs.writeback";

#[derive(Clone)]
pub(super) struct WritebackLedger {
    core: NatsCore,
}

impl WritebackLedger {
    pub(super) fn new(core: NatsCore) -> Self {
        Self { core }
    }

    pub(super) fn stream_name() -> &'static str {
        STREAM
    }

    pub(super) fn has_applied(&self, ledger_key: &str) -> TransportResult<bool> {
        match self
            .core
            .context
            .get_last_message(STREAM, &ledger_subject(ledger_key))
        {
            Ok(message) => Ok(ledger_payload_matches(ledger_key, &message.data)),
            Err(err) if is_not_found(&err) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    pub(super) fn record_applied(&self, ledger_key: &str) -> TransportResult<()> {
        self.ensure_stream()?;
        let publish_id = format!("writeback-applied:{}", hex_encode(ledger_key.as_bytes()));
        self.core.retry_io(|| {
            self.core.context.publish_with_options(
                &ledger_subject(ledger_key),
                ledger_key.as_bytes(),
                &PublishOptions {
                    timeout: Some(self.core.config.timeout),
                    id: Some(publish_id.clone()),
                    expected_stream: Some(STREAM.to_string()),
                    ..Default::default()
                },
            )
        })?;
        Ok(())
    }

    fn ensure_stream(&self) -> TransportResult<()> {
        match self.core.context.stream_info(STREAM) {
            Ok(info) => {
                self.core.ensure_duplicate_window(&info.config.name)?;
                return Ok(());
            }
            Err(err) if is_not_found(&err) => {}
            Err(err) => return Err(err.into()),
        }
        let duplicate_window = duration_as_nanos(self.core.config.stream_duplicate_window);
        match self.core.retry_io(|| {
            self.core.context.add_stream(StreamConfig {
                name: STREAM.to_string(),
                subjects: vec![format!("{SUBJECT_PREFIX}.>")],
                storage: StorageType::File,
                duplicate_window,
                ..Default::default()
            })
        }) {
            Ok(_) => {}
            Err(err) if is_already_exists(&err) => {
                self.core.ensure_duplicate_window(STREAM)?;
            }
            Err(err) => return Err(err),
        }
        Ok(())
    }
}

pub(super) fn scoped_key(kind: &str, parts: &[&str]) -> String {
    let mut key = kind.to_string();
    for part in parts {
        let _ = write!(&mut key, "|{}:", part.len());
        key.push_str(part);
    }
    key
}

fn ledger_subject(ledger_key: &str) -> String {
    format!("{SUBJECT_PREFIX}.{}", hex_encode(ledger_key.as_bytes()))
}

fn ledger_payload_matches(ledger_key: &str, payload: &[u8]) -> bool {
    payload == ledger_key.as_bytes()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_payload_must_match_key() {
        assert!(ledger_payload_matches("kind|4:item", b"kind|4:item"));
        assert!(!ledger_payload_matches("kind|4:item", b"kind|5:other"));
    }
}
