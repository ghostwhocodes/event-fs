use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use eventfs_protocol::{invalidation_paths, AffectedPath, StorageFact, AGENTS_STREAM};
use nats::Message;

use crate::cache::WatchEvent;
use crate::{TransportError, TransportResult};

use super::kv;
use super::objects::{object_watch_fact, object_watch_subject, ObjectWatchSubject};
use super::streams;

pub(super) const WATCH_DRAIN_LIMIT: usize = 4096;
const OBJECT_METADATA_WATCH_SUBJECT: &str = "$O.*.M.>";

#[derive(Clone)]
pub(super) struct NatsWatch {
    subscriptions: Vec<Arc<nats::Subscription>>,
    continuity: Arc<WatchContinuity>,
}

impl NatsWatch {
    pub(super) fn connect(
        url: &str,
        creds_file: Option<&str>,
    ) -> TransportResult<(nats::Connection, Self)> {
        let continuity = Arc::new(WatchContinuity::new());
        let connection = connection_options(creds_file, Arc::clone(&continuity)).connect(url)?;
        let subscriptions = live_watch_subscriptions(&connection)?;
        Ok((
            connection,
            Self {
                subscriptions,
                continuity,
            },
        ))
    }

    pub(super) fn events(&self) -> TransportResult<Vec<WatchEvent>> {
        let mut events = Vec::new();
        if self.continuity.take_gap() {
            events.push(WatchEvent::Gap);
        }
        for subscription in &self.subscriptions {
            match subscription.dropped_messages() {
                Ok(0) => {}
                Ok(_) | Err(_) => events.push(WatchEvent::Gap),
            }
            events.extend(drain_watch_messages(subscription.try_iter()));
        }
        Ok(events)
    }
}

#[derive(Debug)]
pub(super) struct WatchContinuity {
    gap: AtomicBool,
}

impl WatchContinuity {
    pub(super) fn new() -> Self {
        Self {
            gap: AtomicBool::new(false),
        }
    }

    pub(super) fn mark_gap(&self) {
        self.gap.store(true, Ordering::SeqCst);
    }

    pub(super) fn take_gap(&self) -> bool {
        self.gap.swap(false, Ordering::SeqCst)
    }
}

fn connection_options(
    creds_file: Option<&str>,
    watch_continuity: Arc<WatchContinuity>,
) -> nats::Options {
    let options = match creds_file {
        Some(path) => nats::Options::with_credentials(path),
        None => nats::Options::new(),
    };
    let disconnect_gap = Arc::clone(&watch_continuity);
    let reconnect_gap = Arc::clone(&watch_continuity);
    let close_gap = Arc::clone(&watch_continuity);
    let error_gap = Arc::clone(&watch_continuity);
    options
        .disconnect_callback(move || disconnect_gap.mark_gap())
        .reconnect_callback(move || reconnect_gap.mark_gap())
        .close_callback(move || close_gap.mark_gap())
        .error_callback(move |err| {
            tracing::warn!(error = %err, "NATS watcher continuity is uncertain");
            error_gap.mark_gap();
        })
}

fn live_watch_subscriptions(
    connection: &nats::Connection,
) -> TransportResult<Vec<Arc<nats::Subscription>>> {
    [
        "$KV.>",
        OBJECT_METADATA_WATCH_SUBJECT,
        "events.>",
        "agents.>",
    ]
    .into_iter()
    .map(|subject| {
        connection
            .subscribe(subject)
            .map(Arc::new)
            .map_err(TransportError::from)
    })
    .collect()
}

pub(super) fn watch_events_from_message(message: &Message) -> Vec<WatchEvent> {
    watch_message_dispatch(message).events
}

pub(super) fn drain_watch_messages(messages: impl Iterator<Item = Message>) -> Vec<WatchEvent> {
    let mut events = Vec::new();
    let mut drained = 0;
    for message in messages.take(WATCH_DRAIN_LIMIT) {
        events.extend(watch_events_from_message(&message));
        drained += 1;
    }
    if drained == WATCH_DRAIN_LIMIT {
        events.push(WatchEvent::Gap);
    }
    events
}

fn watch_message_dispatch(message: &Message) -> WatchMessageDispatch {
    let subject = message.subject.as_str();
    if let Some((bucket, key)) = kv::subject_parts(subject) {
        return storage_fact_watch_dispatch(StorageFact::Kv {
            bucket: bucket.into(),
            key: key.into(),
        });
    }
    if let Some(dispatch) = object_watch_dispatch(message) {
        return dispatch;
    }
    if let Some(stream) = subject.strip_prefix("events.") {
        return storage_fact_watch_dispatch(StorageFact::StreamSubject {
            stream: stream.into(),
            subject: subject.into(),
        });
    }
    if streams::agent_mailbox_parts(subject).is_some() {
        return storage_fact_watch_dispatch(StorageFact::StreamSubject {
            stream: AGENTS_STREAM.into(),
            subject: subject.into(),
        });
    }
    if streams::is_reserved_agent_mailbox_subject(subject) {
        return WatchMessageDispatch::ignored();
    }
    WatchMessageDispatch::gap()
}

fn storage_fact_watch_dispatch(fact: StorageFact) -> WatchMessageDispatch {
    match invalidation_paths(&fact) {
        Ok(paths) => WatchMessageDispatch::invalidate_affected_paths(paths),
        Err(_) => WatchMessageDispatch::gap(),
    }
}

fn object_watch_dispatch(message: &Message) -> Option<WatchMessageDispatch> {
    match object_watch_subject(&message.subject)? {
        ObjectWatchSubject::Chunk | ObjectWatchSubject::Marker => {
            Some(WatchMessageDispatch::ignored())
        }
        ObjectWatchSubject::Metadata { bucket } => match object_watch_fact(bucket, message) {
            Some(fact) => Some(storage_fact_watch_dispatch(fact)),
            None => Some(WatchMessageDispatch::gap()),
        },
    }
}

struct WatchMessageDispatch {
    events: Vec<WatchEvent>,
}

impl WatchMessageDispatch {
    fn ignored() -> Self {
        Self { events: Vec::new() }
    }

    fn gap() -> Self {
        Self {
            events: vec![WatchEvent::Gap],
        }
    }

    fn invalidate_affected_paths(paths: impl IntoIterator<Item = AffectedPath>) -> Self {
        Self {
            events: paths
                .into_iter()
                .map(|affected| {
                    WatchEvent::invalidate_affected_path(affected.path, affected.reason)
                })
                .collect(),
        }
    }
}
