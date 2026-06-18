use std::collections::BTreeSet;
use std::time::Duration;

use nats::header::{HeaderMap, NATS_MSG_ID};
use nats::jetstream::{
    AckPolicy, BatchOptions, ConsumerConfig, DeliverPolicy, PublishOptions, PullSubscribeOptions,
    ReplayPolicy, SubscribeOptions,
};
use nats::Message;

use eventfs_protocol::{
    is_reserved_kv_key, json_lines, stream_subject_file_name_from_str, validate_json_lines,
    AGENTS_STREAM,
};

use crate::{TransportError, TransportResult};

use super::super::{DirectoryEntry, EntryKind, StreamMessageView};
use super::core::{is_not_found, NatsCore};
use super::ledger::{scoped_key, WritebackLedger};
use super::{subject_matches, system_time_from_datetime};

const STREAM_LIST_BATCH_SIZE: usize = 256;

#[derive(Clone)]
pub(super) struct NatsStreams {
    core: NatsCore,
    ledger: WritebackLedger,
}

impl NatsStreams {
    pub(super) fn new(core: NatsCore, ledger: WritebackLedger) -> Self {
        Self { core, ledger }
    }

    pub(super) fn list(&self) -> TransportResult<Vec<String>> {
        let mut streams = Vec::new();
        for name in self.core.context.stream_names() {
            let name = name?;
            if stream_visible_in_mount(&name) {
                streams.push(name);
            }
        }
        Ok(streams)
    }

    pub(super) fn ensure(&self, stream: &str) -> TransportResult<()> {
        let lower = stream.to_ascii_lowercase();
        self.core.ensure_stream_with_subjects(
            stream,
            vec![format!("{lower}.>"), format!("events.{stream}")],
        )
    }

    pub(super) fn ensure_subject(&self, stream: &str, subject: &str) -> TransportResult<()> {
        self.ensure(stream)?;
        let mut info = self.core.stream_info(stream)?;
        if info
            .config
            .subjects
            .iter()
            .any(|candidate| subject_matches(candidate, subject))
        {
            return Ok(());
        }
        info.config.subjects.push(subject.to_string());
        self.core.context.update_stream(&info.config)?;
        Ok(())
    }

    pub(super) fn list_messages(&self, stream: &str) -> TransportResult<Vec<DirectoryEntry>> {
        let info = match self.core.stream_info(stream) {
            Ok(info) => info,
            Err(TransportError::NotFound) => return Err(TransportError::NotFound),
            Err(err) => return Err(err),
        };
        if info.state.messages == 0 {
            return Ok(Vec::new());
        }
        let consumer = self.stream_headers(stream, "", DeliverPolicy::All)?;
        let mut sequences = Vec::new();
        self.drain_stream_headers(&consumer, info.state.messages as usize, |message| {
            let info = message.jetstream_message_info().ok_or_else(|| {
                TransportError::Invalid("stream listing message missing JetStream metadata".into())
            })?;
            sequences.push(info.stream_seq);
            Ok(())
        })?;
        sequences.sort_unstable();
        sequences.dedup();
        let entries = sequences
            .into_iter()
            .map(|sequence| DirectoryEntry {
                name: format!("{sequence}.json"),
                kind: EntryKind::File,
            })
            .collect();
        Ok(entries)
    }

    pub(super) fn list_subjects(&self, stream: &str) -> TransportResult<Vec<DirectoryEntry>> {
        Ok(self
            .retained_subjects(stream)?
            .into_iter()
            .map(|subject| DirectoryEntry {
                name: stream_subject_file_name_from_str(&subject),
                kind: EntryKind::File,
            })
            .collect())
    }

    pub(super) fn list_agent_names(&self) -> TransportResult<Vec<String>> {
        let subjects = match self.retained_subjects(AGENTS_STREAM) {
            Ok(subjects) => subjects,
            Err(TransportError::NotFound) => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };
        let mut agents = BTreeSet::new();
        for subject in subjects {
            if let Some(agent) = agent_mailbox_name(&subject) {
                agents.insert(agent.to_string());
            }
        }
        Ok(agents.into_iter().collect())
    }

    pub(super) fn message(
        &self,
        stream: &str,
        sequence: u64,
    ) -> TransportResult<StreamMessageView> {
        let message = match self.core.context.get_message(stream, sequence) {
            Ok(message) => message,
            Err(err) if is_not_found(&err) => return Err(TransportError::NotFound),
            Err(err) => return Err(err.into()),
        };
        Ok(StreamMessageView {
            stream: stream.to_string(),
            sequence: message.sequence,
            published: system_time_from_datetime(message.time)?,
            subject: message.subject,
            payload: message.data,
        })
    }

    pub(super) fn publish_json_lines(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<Vec<u64>> {
        validate_json_lines(subject, bytes)
            .map_err(|err| TransportError::Invalid(err.to_string()))?;
        self.ensure_subject(stream, subject)?;
        let mut sequences = Vec::new();
        for (index, line) in json_lines(subject, bytes)
            .map_err(|err| TransportError::Invalid(err.to_string()))?
            .into_iter()
            .enumerate()
        {
            let message_id = format!("{idempotency_seed}:{index}");
            if self.line_applied(stream, subject, &message_id)? {
                continue;
            }
            let ack = self.core.retry_io(|| {
                self.core.context.publish_with_options(
                    subject,
                    line.as_bytes(),
                    &PublishOptions {
                        timeout: Some(self.core.config.timeout),
                        id: Some(message_id.clone()),
                        expected_stream: Some(stream.to_string()),
                        ..Default::default()
                    },
                )
            })?;
            let ledger_key = writeback_ledger_key(stream, subject, &message_id);
            self.ledger.record_applied(&ledger_key)?;
            sequences.push(ack.sequence);
        }
        Ok(sequences)
    }

    pub(super) fn publish_json_lines_applied(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<bool> {
        validate_json_lines(subject, bytes)
            .map_err(|err| TransportError::Invalid(err.to_string()))?;
        let lines =
            json_lines(subject, bytes).map_err(|err| TransportError::Invalid(err.to_string()))?;
        if lines.is_empty() {
            return Ok(true);
        }
        for index in 0..lines.len() {
            if !self.line_applied(stream, subject, &format!("{idempotency_seed}:{index}"))? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn publish_json_lines_applied_prefix(
        &self,
        stream: &str,
        subject: &str,
        bytes: &[u8],
        idempotency_seed: &str,
    ) -> TransportResult<usize> {
        validate_json_lines(subject, bytes)
            .map_err(|err| TransportError::Invalid(err.to_string()))?;
        let lines =
            json_lines(subject, bytes).map_err(|err| TransportError::Invalid(err.to_string()))?;
        let mut applied = 0;
        for index in 0..lines.len() {
            if self.line_applied(stream, subject, &format!("{idempotency_seed}:{index}"))? {
                applied += 1;
            } else {
                break;
            }
        }
        Ok(applied)
    }

    pub(super) fn watch_once(
        &self,
        stream: &str,
        subject: &str,
        timeout: Duration,
    ) -> TransportResult<Option<StreamMessageView>> {
        self.ensure_subject(stream, subject)?;
        let sub = self.core.context.subscribe_with_options(
            subject,
            &SubscribeOptions::bind_stream(stream.to_string()).deliver_new(),
        )?;
        match sub.next_timeout(timeout) {
            Ok(message) => Ok(message_to_stream_view(stream, message)),
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn stream_headers(
        &self,
        stream: &str,
        filter_subject: &str,
        deliver_policy: DeliverPolicy,
    ) -> TransportResult<nats::jetstream::PullSubscription> {
        let subscribe_subject = if filter_subject.is_empty() {
            stream
        } else {
            filter_subject
        };
        self.core
            .context
            .pull_subscribe_with_options(
                subscribe_subject,
                &PullSubscribeOptions::new()
                    .bind_stream(stream.to_string())
                    .consumer_config(ConsumerConfig {
                        deliver_policy,
                        ack_policy: AckPolicy::Explicit,
                        filter_subject: filter_subject.to_string(),
                        replay_policy: ReplayPolicy::Instant,
                        headers_only: true,
                        ..Default::default()
                    }),
            )
            .map_err(TransportError::from)
    }

    fn drain_stream_headers(
        &self,
        consumer: &nats::jetstream::PullSubscription,
        upper_bound: usize,
        mut on_message: impl FnMut(&Message) -> TransportResult<()>,
    ) -> TransportResult<()> {
        if upper_bound == 0 {
            return Ok(());
        }

        let mut remaining = upper_bound;
        while remaining > 0 {
            let batch_size = remaining.min(STREAM_LIST_BATCH_SIZE);
            let batch = consumer
                .fetch(BatchOptions {
                    batch: batch_size,
                    expires: None,
                    no_wait: true,
                })
                .map_err(TransportError::from)?;
            let mut delivered = 0usize;
            for message in batch {
                on_message(&message)?;
                message.ack().map_err(TransportError::from)?;
                delivered += 1;
            }
            if delivered < batch_size {
                break;
            }
            remaining = remaining.saturating_sub(delivered);
        }

        Ok(())
    }

    fn retained_subjects(&self, stream: &str) -> TransportResult<BTreeSet<String>> {
        let info = match self.core.stream_info(stream) {
            Ok(info) => info,
            Err(TransportError::NotFound) => return Err(TransportError::NotFound),
            Err(err) => return Err(err),
        };
        if info.state.messages == 0 {
            return Ok(BTreeSet::new());
        }
        let consumer = self.stream_headers(stream, "", DeliverPolicy::All)?;
        let mut subjects = BTreeSet::new();
        self.drain_stream_headers(&consumer, info.state.messages as usize, |message| {
            subjects.insert(message.subject.clone());
            Ok(())
        })?;
        Ok(subjects)
    }

    fn line_applied(&self, stream: &str, subject: &str, message_id: &str) -> TransportResult<bool> {
        let ledger_key = writeback_ledger_key(stream, subject, message_id);
        if self.ledger.has_applied(&ledger_key)? {
            return Ok(true);
        }
        let applied = self
            .core
            .scan_subject_history_until(stream, subject, |message| {
                Ok(headers_contain_message_id(message.headers.as_ref(), message_id).then_some(()))
            })?
            .is_some();
        if applied {
            self.ledger.record_applied(&ledger_key)?;
            return Ok(true);
        }
        Ok(false)
    }
}

pub(super) fn stream_visible_in_mount(stream_name: &str) -> bool {
    !stream_name.starts_with("KV_")
        && !stream_name.starts_with("OBJ_")
        && stream_name != WritebackLedger::stream_name()
}

pub(super) fn agent_mailbox_name(subject: &str) -> Option<&str> {
    agent_mailbox_parts(subject).map(|(agent, _)| agent)
}

pub(super) fn is_reserved_agent_mailbox_subject(subject: &str) -> bool {
    agent_mailbox_subject_parts(subject)
        .map(|(agent, _)| is_reserved_kv_key(agent))
        .unwrap_or(false)
}

pub(super) fn agent_mailbox_parts(subject: &str) -> Option<(&str, &str)> {
    let (agent, area) = agent_mailbox_subject_parts(subject)?;
    if is_reserved_kv_key(agent) {
        return None;
    }
    Some((agent, area))
}

fn agent_mailbox_subject_parts(subject: &str) -> Option<(&str, &str)> {
    let mut parts = subject.split('.');
    if parts.next()? != "agents" {
        return None;
    }
    let agent = parts.next()?;
    let area = parts.next()?;
    if parts.next().is_some() || !matches!(area, "inbox" | "outbox") {
        return None;
    }
    Some((agent, area))
}

fn headers_contain_message_id(headers: Option<&HeaderMap>, message_id: &str) -> bool {
    headers
        .and_then(|headers| headers.get(NATS_MSG_ID))
        .map(|value| value.as_str() == message_id)
        .unwrap_or(false)
}

fn writeback_ledger_key(stream: &str, subject: &str, message_id: &str) -> String {
    scoped_key("stream", &[stream, subject, message_id])
}

fn message_to_stream_view(stream: &str, message: Message) -> Option<StreamMessageView> {
    message.jetstream_message_info().and_then(|info| {
        Some(StreamMessageView {
            stream: stream.to_string(),
            sequence: info.stream_seq,
            published: system_time_from_datetime(info.published).ok()?,
            subject: message.subject.clone(),
            payload: message.data.clone(),
        })
    })
}
