pub use crate::file_names::{stream_subject_file_name, stream_subject_file_name_from_str};
use crate::{AgentArea, JetStreamPath};

pub const TASKS_BUCKET: &str = "EVENTFS_TASKS";
pub const AGENTS_BUCKET: &str = "EVENTFS_AGENTS";
pub const AGENTS_STREAM: &str = "EVENTFS_AGENTS";
pub const SEMANTIC_BUCKET: &str = "EVENTFS_SEMANTIC";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaterializedTarget {
    Kv { bucket: String, key: String },
    Stream { stream: String, subject: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentSubject {
    pub stream: String,
    pub subject: String,
}

impl MaterializedTarget {
    pub fn from_path(path: &JetStreamPath) -> Option<Self> {
        match path {
            JetStreamPath::EventLog { stream } => Some(Self::Stream {
                stream: stream.clone(),
                subject: format!("events.{stream}"),
            }),
            JetStreamPath::Task { namespace, task } => Some(Self::Kv {
                bucket: TASKS_BUCKET.into(),
                key: format!("{namespace}/{task}"),
            }),
            JetStreamPath::AgentMailbox { agent, area } => {
                agent_subject(agent, *area).map(|subject| Self::Stream {
                    stream: subject.stream,
                    subject: subject.subject,
                })
            }
            JetStreamPath::AgentRecord { agent, area, path } => Some(Self::Kv {
                bucket: AGENTS_BUCKET.into(),
                key: format!("{}/{}/{}", agent, area.as_str(), path),
            }),
            JetStreamPath::SemanticRecord { area, path } => Some(Self::Kv {
                bucket: SEMANTIC_BUCKET.into(),
                key: format!("{}/{}", area.as_str(), path),
            }),
            _ => None,
        }
    }
}

pub fn agent_subject(agent: &str, area: AgentArea) -> Option<AgentSubject> {
    if !area.is_mailbox() {
        return None;
    }
    Some(AgentSubject {
        stream: AGENTS_STREAM.into(),
        subject: format!("agents.{}.{}", agent, area.as_str()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JetStreamPath;

    #[test]
    fn eventfs_path_maps_tasks_agents_and_semantic_to_stable_targets() {
        let task = JetStreamPath::parse("/tasks/ns/job.json").unwrap();
        assert_eq!(
            MaterializedTarget::from_path(&task),
            Some(MaterializedTarget::Kv {
                bucket: TASKS_BUCKET.into(),
                key: "ns/job.json".into()
            })
        );

        let inbox = JetStreamPath::parse("/agents/bot/inbox").unwrap();
        assert_eq!(
            MaterializedTarget::from_path(&inbox),
            Some(MaterializedTarget::Stream {
                stream: AGENTS_STREAM.into(),
                subject: "agents.bot.inbox".into()
            })
        );

        let semantic = JetStreamPath::parse("/semantic/tags/a.json").unwrap();
        assert_eq!(
            MaterializedTarget::from_path(&semantic),
            Some(MaterializedTarget::Kv {
                bucket: SEMANTIC_BUCKET.into(),
                key: "tags/a.json".into()
            })
        );
    }

    #[test]
    fn stream_subject_file_names_encode_non_path_safe_subjects() {
        assert_eq!(
            stream_subject_file_name_from_str("orders.created"),
            "orders.created.jsonl"
        );
        assert_eq!(
            stream_subject_file_name_from_str("foo/bar@v1"),
            "__eventfs_subject_hex_666f6f2f626172407631.jsonl"
        );
        assert_eq!(
            stream_subject_file_name_from_str("__eventfs_subject_hex_666f6f"),
            "__eventfs_subject_hex_5f5f6576656e7466735f7375626a6563745f6865785f363636663666.jsonl"
        );
    }
}
