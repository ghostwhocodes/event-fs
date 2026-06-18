use std::time::Duration;

use nats::jetstream::{StorageType, StreamConfig, StreamInfo, SubscribeOptions};
use nats::Message;

use crate::{TransportError, TransportResult};

#[derive(Clone, Copy, Debug)]
pub struct NatsStorageConfig {
    pub timeout: Duration,
    pub stream_duplicate_window: Duration,
    pub kv_history: i64,
    pub retries: usize,
    pub backoff: Duration,
}

impl Default for NatsStorageConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(2),
            stream_duplicate_window: Duration::from_secs(24 * 60 * 60),
            kv_history: 64,
            retries: 2,
            backoff: Duration::from_millis(25),
        }
    }
}

#[derive(Clone)]
pub(super) struct NatsCore {
    pub(super) context: nats::jetstream::JetStream,
    pub(super) config: NatsStorageConfig,
}

impl NatsCore {
    pub(super) fn new(context: nats::jetstream::JetStream, config: NatsStorageConfig) -> Self {
        Self { context, config }
    }

    pub(super) fn retry_io<T>(
        &self,
        mut f: impl FnMut() -> std::io::Result<T>,
    ) -> TransportResult<T> {
        retry_transport(self.config.retries, self.config.backoff, || {
            f().map_err(TransportError::from)
        })
    }

    pub(super) fn stream_info(&self, stream: &str) -> TransportResult<StreamInfo> {
        self.context
            .stream_info(stream)
            .map_err(stream_lookup_error)
    }

    pub(super) fn ensure_duplicate_window(&self, stream: &str) -> TransportResult<()> {
        let duplicate_window = duration_as_nanos(self.config.stream_duplicate_window);
        let mut info = self.stream_info(stream)?;
        if info.config.duplicate_window < duplicate_window {
            info.config.duplicate_window = duplicate_window;
            self.context.update_stream(&info.config)?;
        }
        Ok(())
    }

    pub(super) fn ensure_stream_with_subjects(
        &self,
        stream: &str,
        subjects: Vec<String>,
    ) -> TransportResult<()> {
        let duplicate_window = duration_as_nanos(self.config.stream_duplicate_window);
        match self.stream_info(stream) {
            Ok(info) => {
                self.ensure_duplicate_window(&info.config.name)?;
                return Ok(());
            }
            Err(TransportError::NotFound) => {}
            Err(err) => return Err(err),
        }
        match self.retry_io(|| {
            self.context.add_stream(StreamConfig {
                name: stream.to_string(),
                subjects: subjects.clone(),
                storage: StorageType::File,
                duplicate_window,
                ..Default::default()
            })
        }) {
            Ok(_) => {}
            Err(err) if is_already_exists(&err) => {
                self.ensure_duplicate_window(stream)?;
            }
            Err(err) => return Err(err),
        }
        Ok(())
    }

    pub(super) fn last_subject_sequence(
        &self,
        stream: &str,
        subject: &str,
    ) -> TransportResult<Option<u64>> {
        match self.context.get_last_message(stream, subject) {
            Ok(message) => Ok(Some(message.sequence)),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub(super) fn scan_subject_history_until<T>(
        &self,
        stream: &str,
        subject: &str,
        mut on_message: impl FnMut(&Message) -> TransportResult<Option<T>>,
    ) -> TransportResult<Option<T>> {
        let last_sequence = match self.last_subject_sequence(stream, subject)? {
            Some(last_sequence) => last_sequence,
            None => return Ok(None),
        };
        let subscription = match self.context.subscribe_with_options(
            subject,
            &SubscribeOptions::bind_stream(stream.to_string()).deliver_all(),
        ) {
            Ok(subscription) => subscription,
            Err(err) if is_not_found(&err) => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        loop {
            let message = match subscription.next_timeout(self.config.timeout) {
                Ok(message) => message,
                Err(err) if is_timeout(&err) => {
                    return Err(TransportError::Invalid(format!(
                        "timed out scanning {stream}:{subject} before reaching retained sequence {last_sequence}",
                    )));
                }
                Err(err) => return Err(err.into()),
            };
            if let Some(result) = on_message(&message)? {
                return Ok(Some(result));
            }
            if message_stream_sequence(&message)? >= last_sequence {
                return Ok(None);
            }
        }
    }
}

pub(super) fn retry_transport<T>(
    retries: usize,
    backoff: Duration,
    mut operation: impl FnMut() -> TransportResult<T>,
) -> TransportResult<T> {
    let mut attempts = 0usize;
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(err) if attempts < retries => {
                attempts += 1;
                std::thread::sleep(backoff);
                let _ = err;
            }
            Err(err) => return Err(err),
        }
    }
}

pub(super) fn duration_as_nanos(duration: Duration) -> i64 {
    duration.as_nanos().min(i64::MAX as u128) as i64
}

pub(super) fn is_not_found(err: &std::io::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    if is_no_responders(&message) {
        return false;
    }
    err.kind() == std::io::ErrorKind::NotFound
        || message.contains("no message found")
        || message.contains("not found")
        || message.contains("404")
}

fn is_no_responders(message: &str) -> bool {
    message.contains("no responders") || message.contains("no-responders")
}

pub(super) fn is_timeout(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::TimedOut || err.to_string().contains("timed out")
}

pub(super) fn is_already_exists(err: &TransportError) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    matches!(err, TransportError::Io(io) if io.kind() == std::io::ErrorKind::AlreadyExists)
        || message.contains("already in use")
        || message.contains("already exists")
}

pub(super) fn store_lookup_error(err: std::io::Error) -> TransportError {
    if is_not_found(&err) {
        TransportError::NotFound
    } else {
        err.into()
    }
}

pub(super) fn stream_lookup_error(err: std::io::Error) -> TransportError {
    if is_not_found(&err) {
        TransportError::NotFound
    } else {
        err.into()
    }
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
