use std::fmt;

use crate::EventFsError;

pub const ROOT_DIRECTORIES: [&str; 8] = [
    "kv", "streams", "objects", "events", "tasks", "agents", "semantic", ".eventfs",
];

pub const KV_WRITEBACK_MARKER_KEY_PREFIX: &str = "__eventfs_writeback";
pub const KV_APPLIED_MARKER_KEY_PREFIX: &str = "__eventfs_applied";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JetStreamPath {
    Root,
    KvRoot,
    KvBucket {
        bucket: String,
    },
    KvKey {
        bucket: String,
        key: String,
    },
    KvHistoryRoot {
        bucket: String,
    },
    KvHistoryKey {
        bucket: String,
        key: String,
    },
    KvRevision {
        bucket: String,
        key: String,
        revision: u64,
    },
    StreamsRoot,
    StreamRoot {
        stream: String,
    },
    StreamMessages {
        stream: String,
    },
    StreamMessage {
        stream: String,
        sequence: u64,
    },
    StreamSubjects {
        stream: String,
    },
    StreamSubject {
        stream: String,
        subject: StreamSubject,
    },
    ObjectsRoot,
    ObjectBucket {
        bucket: String,
    },
    Object {
        bucket: String,
        object: String,
    },
    EventsRoot,
    EventLog {
        stream: String,
    },
    TasksRoot,
    TaskNamespace {
        namespace: String,
    },
    Task {
        namespace: String,
        task: String,
    },
    AgentsRoot,
    AgentRoot {
        agent: String,
    },
    AgentMailbox {
        agent: String,
        area: AgentArea,
    },
    AgentDirectory {
        agent: String,
        area: AgentArea,
    },
    AgentRecord {
        agent: String,
        area: AgentArea,
        path: String,
    },
    SemanticRoot,
    SemanticArea {
        area: SemanticArea,
    },
    SemanticRecord {
        area: SemanticArea,
        path: String,
    },
    MetadataRoot,
    MetadataFile(MetadataFile),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamSubject(String);

impl StreamSubject {
    pub fn parse_file_name(name: &str) -> Result<Self, EventFsError> {
        let subject = crate::file_names::parse_stream_subject_file_name(name)?;
        validate_subject(&subject)?;
        Ok(Self(subject))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentArea {
    Inbox,
    Outbox,
    Tasks,
    Memory,
}

impl AgentArea {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "inbox" => Some(Self::Inbox),
            "outbox" => Some(Self::Outbox),
            "tasks" => Some(Self::Tasks),
            "memory" => Some(Self::Memory),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inbox => "inbox",
            Self::Outbox => "outbox",
            Self::Tasks => "tasks",
            Self::Memory => "memory",
        }
    }

    pub fn is_mailbox(self) -> bool {
        matches!(self, Self::Inbox | Self::Outbox)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticArea {
    Summaries,
    Tags,
    Relations,
    Timelines,
    Embeddings,
}

impl SemanticArea {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "summaries" => Some(Self::Summaries),
            "tags" => Some(Self::Tags),
            "relations" => Some(Self::Relations),
            "timelines" => Some(Self::Timelines),
            "embeddings" => Some(Self::Embeddings),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Summaries => "summaries",
            Self::Tags => "tags",
            Self::Relations => "relations",
            Self::Timelines => "timelines",
            Self::Embeddings => "embeddings",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataFile {
    Status,
    Cache,
    Queue,
    Capabilities,
    Errors,
}

impl MetadataFile {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "status.json" => Some(Self::Status),
            "cache.json" => Some(Self::Cache),
            "queue.json" => Some(Self::Queue),
            "capabilities.json" => Some(Self::Capabilities),
            "errors.jsonl" => Some(Self::Errors),
            _ => None,
        }
    }

    pub fn file_name(self) -> &'static str {
        match self {
            Self::Status => "status.json",
            Self::Cache => "cache.json",
            Self::Queue => "queue.json",
            Self::Capabilities => "capabilities.json",
            Self::Errors => "errors.jsonl",
        }
    }
}

impl JetStreamPath {
    pub fn parse(path: &str) -> Result<Self, EventFsError> {
        let components = components(path)?;
        match components.as_slice() {
            [] => Ok(Self::Root),
            ["kv"] => Ok(Self::KvRoot),
            ["kv", bucket] => {
                validate_bucket(bucket)?;
                Ok(Self::KvBucket {
                    bucket: (*bucket).to_string(),
                })
            }
            ["kv", bucket, ".history"] => {
                validate_bucket(bucket)?;
                Ok(Self::KvHistoryRoot {
                    bucket: (*bucket).to_string(),
                })
            }
            ["kv", bucket, ".history", rest @ ..] => parse_kv_history(bucket, rest),
            ["kv", bucket, rest @ ..] => {
                validate_bucket(bucket)?;
                Ok(Self::KvKey {
                    bucket: (*bucket).to_string(),
                    key: join_kv_key(rest)?,
                })
            }
            ["streams"] => Ok(Self::StreamsRoot),
            ["streams", stream] => {
                validate_stream(stream)?;
                Ok(Self::StreamRoot {
                    stream: (*stream).to_string(),
                })
            }
            ["streams", stream, "messages"] => {
                validate_stream(stream)?;
                Ok(Self::StreamMessages {
                    stream: (*stream).to_string(),
                })
            }
            ["streams", stream, "messages", message] => {
                validate_stream(stream)?;
                Ok(Self::StreamMessage {
                    stream: (*stream).to_string(),
                    sequence: parse_sequence_file(message)?,
                })
            }
            ["streams", stream, "subjects"] => {
                validate_stream(stream)?;
                Ok(Self::StreamSubjects {
                    stream: (*stream).to_string(),
                })
            }
            ["streams", stream, "subjects", subject] => {
                validate_stream(stream)?;
                Ok(Self::StreamSubject {
                    stream: (*stream).to_string(),
                    subject: StreamSubject::parse_file_name(subject)?,
                })
            }
            ["objects"] => Ok(Self::ObjectsRoot),
            ["objects", bucket] => {
                validate_bucket(bucket)?;
                Ok(Self::ObjectBucket {
                    bucket: (*bucket).to_string(),
                })
            }
            ["objects", bucket, rest @ ..] => {
                validate_bucket(bucket)?;
                Ok(Self::Object {
                    bucket: (*bucket).to_string(),
                    object: join_path(rest)?,
                })
            }
            ["events"] => Ok(Self::EventsRoot),
            ["events", event_file] => Ok(Self::EventLog {
                stream: parse_jsonl_stem(event_file, "event files")?,
            }),
            ["tasks"] => Ok(Self::TasksRoot),
            ["tasks", namespace] => {
                validate_materialized_root_name(namespace, "task namespace")?;
                Ok(Self::TaskNamespace {
                    namespace: (*namespace).to_string(),
                })
            }
            ["tasks", namespace, task] => {
                validate_materialized_root_name(namespace, "task namespace")?;
                validate_json_file(task, "task files")?;
                Ok(Self::Task {
                    namespace: (*namespace).to_string(),
                    task: (*task).to_string(),
                })
            }
            ["agents"] => Ok(Self::AgentsRoot),
            ["agents", agent] => {
                validate_materialized_root_name(agent, "agent")?;
                Ok(Self::AgentRoot {
                    agent: (*agent).to_string(),
                })
            }
            ["agents", agent, area] => parse_agent_area(agent, area, &[]),
            ["agents", agent, area, rest @ ..] => parse_agent_area(agent, area, rest),
            ["semantic"] => Ok(Self::SemanticRoot),
            ["semantic", area] => {
                let area = SemanticArea::parse(area)
                    .ok_or_else(|| EventFsError::invalid_path("unknown semantic area"))?;
                Ok(Self::SemanticArea { area })
            }
            ["semantic", area, rest @ ..] => {
                let area = SemanticArea::parse(area)
                    .ok_or_else(|| EventFsError::invalid_path("unknown semantic area"))?;
                Ok(Self::SemanticRecord {
                    area,
                    path: join_kv_key(rest)?,
                })
            }
            [".eventfs"] => Ok(Self::MetadataRoot),
            [".eventfs", file] => MetadataFile::parse(file)
                .map(Self::MetadataFile)
                .ok_or_else(|| EventFsError::invalid_path("unknown metadata file")),
            _ => Err(EventFsError::invalid_path("unsupported mount path")),
        }
    }

    pub fn is_directory(&self) -> bool {
        matches!(
            self,
            Self::Root
                | Self::KvRoot
                | Self::KvBucket { .. }
                | Self::KvHistoryRoot { .. }
                | Self::KvHistoryKey { .. }
                | Self::StreamsRoot
                | Self::StreamRoot { .. }
                | Self::StreamMessages { .. }
                | Self::StreamSubjects { .. }
                | Self::ObjectsRoot
                | Self::ObjectBucket { .. }
                | Self::EventsRoot
                | Self::TasksRoot
                | Self::TaskNamespace { .. }
                | Self::AgentsRoot
                | Self::AgentRoot { .. }
                | Self::AgentDirectory { .. }
                | Self::SemanticRoot
                | Self::SemanticArea { .. }
                | Self::MetadataRoot
        )
    }
}

impl fmt::Display for JetStreamPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

fn parse_kv_history(bucket: &str, rest: &[&str]) -> Result<JetStreamPath, EventFsError> {
    validate_bucket(bucket)?;
    if rest.is_empty() {
        return Ok(JetStreamPath::KvHistoryRoot {
            bucket: bucket.to_string(),
        });
    }
    if let Some(revision) = parse_kv_revision_marker(rest.last().copied().unwrap_or_default())? {
        if rest.len() == 1 {
            return Err(EventFsError::invalid_path(
                "kv history revision paths require a key",
            ));
        }
        return Ok(JetStreamPath::KvRevision {
            bucket: bucket.to_string(),
            key: join_kv_key(&rest[..rest.len() - 1])?,
            revision,
        });
    }

    Ok(JetStreamPath::KvHistoryKey {
        bucket: bucket.to_string(),
        key: join_kv_key(rest)?,
    })
}

fn parse_kv_revision_marker(value: &str) -> Result<Option<u64>, EventFsError> {
    let Some(revision) = value.strip_prefix('@') else {
        return Ok(None);
    };
    if revision.is_empty() {
        return Err(EventFsError::invalid_path(
            "kv history revision marker must include a revision",
        ));
    }
    revision
        .parse::<u64>()
        .map(Some)
        .map_err(|_| EventFsError::invalid_path("kv history revision must be numeric"))
}

fn parse_agent_area(agent: &str, area: &str, rest: &[&str]) -> Result<JetStreamPath, EventFsError> {
    validate_materialized_root_name(agent, "agent")?;
    let area =
        AgentArea::parse(area).ok_or_else(|| EventFsError::invalid_path("unknown agent area"))?;
    match (area.is_mailbox(), rest.is_empty()) {
        (true, true) => Ok(JetStreamPath::AgentMailbox {
            agent: agent.to_string(),
            area,
        }),
        (true, false) => Err(EventFsError::invalid_path(
            "agent mailbox paths are JSONL files, not directories",
        )),
        (false, true) => Ok(JetStreamPath::AgentDirectory {
            agent: agent.to_string(),
            area,
        }),
        (false, false) => Ok(JetStreamPath::AgentRecord {
            agent: agent.to_string(),
            area,
            path: join_kv_key(rest)?,
        }),
    }
}

fn components(path: &str) -> Result<Vec<&str>, EventFsError> {
    if path.as_bytes().contains(&0) {
        return Err(EventFsError::invalid_path("path contains NUL"));
    }
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let parts: Vec<&str> = trimmed.split('/').collect();
    for part in &parts {
        if part.is_empty() || *part == "." || *part == ".." {
            return Err(EventFsError::invalid_path("invalid path component"));
        }
    }
    Ok(parts)
}

fn join_path(parts: &[&str]) -> Result<String, EventFsError> {
    if parts.is_empty() {
        return Err(EventFsError::invalid_path("missing path"));
    }
    for part in parts {
        validate_component(part)?;
    }
    Ok(parts.join("/"))
}

fn join_kv_key(parts: &[&str]) -> Result<String, EventFsError> {
    let key = join_path(parts)?;
    validate_kv_key(&key)?;
    Ok(key)
}

fn parse_sequence_file(value: &str) -> Result<u64, EventFsError> {
    let Some(stem) = value.strip_suffix(".json") else {
        return Err(EventFsError::invalid_path(
            "stream message files must end in .json",
        ));
    };
    stem.parse::<u64>()
        .map_err(|_| EventFsError::invalid_path("stream message sequence must be numeric"))
}

fn parse_jsonl_stem(value: &str, label: &str) -> Result<String, EventFsError> {
    let Some(stem) = value.strip_suffix(".jsonl") else {
        return Err(EventFsError::invalid_path(format!(
            "{label} must end in .jsonl"
        )));
    };
    validate_stream(stem)?;
    Ok(stem.to_string())
}

fn validate_json_file(value: &str, label: &str) -> Result<(), EventFsError> {
    if value.ends_with(".json") {
        validate_component(value)?;
        validate_kv_key(value)
    } else {
        Err(EventFsError::invalid_path(format!(
            "{label} must end in .json"
        )))
    }
}

fn validate_bucket(value: &str) -> Result<(), EventFsError> {
    validate_token(value, "bucket")
}

fn validate_stream(value: &str) -> Result<(), EventFsError> {
    validate_token(value, "stream")
}

fn validate_name(value: &str, label: &str) -> Result<(), EventFsError> {
    validate_token(value, label)
}

fn validate_materialized_root_name(value: &str, label: &str) -> Result<(), EventFsError> {
    validate_name(value, label)?;
    if is_reserved_kv_key(value) {
        Err(EventFsError::invalid_path(format!(
            "{label} uses a reserved internal KV prefix"
        )))
    } else {
        Ok(())
    }
}

fn validate_subject(value: &str) -> Result<(), EventFsError> {
    if value.is_empty() || value.starts_with('.') || value.ends_with('.') || value.contains("..") {
        return Err(EventFsError::invalid_path("invalid subject"));
    }
    for ch in value.chars() {
        if ch.is_control() || ch.is_whitespace() || matches!(ch, '*' | '>') {
            return Err(EventFsError::invalid_path("invalid subject"));
        }
    }
    Ok(())
}

fn validate_token(value: &str, label: &str) -> Result<(), EventFsError> {
    if value.is_empty() {
        return Err(EventFsError::invalid_path(format!("missing {label}")));
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
    {
        Ok(())
    } else {
        Err(EventFsError::invalid_path(format!("invalid {label}")))
    }
}

fn validate_component(value: &str) -> Result<(), EventFsError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.as_bytes().contains(&0)
        || value.starts_with('.')
    {
        return Err(EventFsError::invalid_path("invalid path component"));
    }
    Ok(())
}

fn validate_kv_key(value: &str) -> Result<(), EventFsError> {
    if value.is_empty() || value.starts_with('.') || value.ends_with('.') {
        return Err(EventFsError::invalid_path("invalid kv key"));
    }
    if is_reserved_kv_key(value) {
        return Err(EventFsError::invalid_path(
            "kv key uses a reserved internal prefix",
        ));
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '=' | '.' | '/'))
    {
        Ok(())
    } else {
        Err(EventFsError::invalid_path("invalid kv key"))
    }
}

pub fn is_reserved_kv_key(key: &str) -> bool {
    key_has_reserved_prefix(key, KV_WRITEBACK_MARKER_KEY_PREFIX)
        || key_has_reserved_prefix(key, KV_APPLIED_MARKER_KEY_PREFIX)
}

fn key_has_reserved_prefix(key: &str, prefix: &str) -> bool {
    key == prefix
        || key
            .strip_prefix(prefix)
            .map(|rest| rest.starts_with('.') || rest.starts_with('/'))
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eventfs_path_parses_root_directories() {
        assert_eq!(JetStreamPath::parse("/").unwrap(), JetStreamPath::Root);
        assert_eq!(JetStreamPath::parse("/kv").unwrap(), JetStreamPath::KvRoot);
        for path in [
            "/kv",
            "/streams",
            "/objects",
            "/events",
            "/tasks",
            "/agents",
            "/semantic",
            "/.eventfs",
        ] {
            assert!(JetStreamPath::parse(path).unwrap().is_directory(), "{path}");
        }
        assert_eq!(
            JetStreamPath::parse("/.eventfs/status.json").unwrap(),
            JetStreamPath::MetadataFile(MetadataFile::Status)
        );
    }

    #[test]
    fn eventfs_path_parses_kv_current_and_history() {
        assert_eq!(
            JetStreamPath::parse("/kv/app/config/service.json").unwrap(),
            JetStreamPath::KvKey {
                bucket: "app".into(),
                key: "config/service.json".into()
            }
        );
        assert_eq!(
            JetStreamPath::parse("/kv/app/.history/config/service.json/@7").unwrap(),
            JetStreamPath::KvRevision {
                bucket: "app".into(),
                key: "config/service.json".into(),
                revision: 7
            }
        );
        assert_eq!(
            JetStreamPath::parse("/kv/app/.history/jobs/2026").unwrap(),
            JetStreamPath::KvHistoryKey {
                bucket: "app".into(),
                key: "jobs/2026".into()
            }
        );
        assert_eq!(
            JetStreamPath::parse("/kv/app/.history/jobs/2026/@7").unwrap(),
            JetStreamPath::KvRevision {
                bucket: "app".into(),
                key: "jobs/2026".into(),
                revision: 7
            }
        );
        assert!(JetStreamPath::parse("/kv/app/.history/jobs/2026/@latest").is_err());
    }

    #[test]
    fn eventfs_path_parses_stream_messages_and_subject_publishers() {
        assert_eq!(
            JetStreamPath::parse("/streams/ORDERS/messages/42.json").unwrap(),
            JetStreamPath::StreamMessage {
                stream: "ORDERS".into(),
                sequence: 42
            }
        );
        assert_eq!(
            JetStreamPath::parse("/streams/ORDERS/subjects/orders.created.jsonl").unwrap(),
            JetStreamPath::StreamSubject {
                stream: "ORDERS".into(),
                subject: StreamSubject("orders.created".into())
            }
        );
        assert_eq!(
            JetStreamPath::parse(
                "/streams/ORDERS/subjects/__eventfs_subject_hex_666f6f2f626172407631.jsonl"
            )
            .unwrap(),
            JetStreamPath::StreamSubject {
                stream: "ORDERS".into(),
                subject: StreamSubject("foo/bar@v1".into())
            }
        );
    }

    #[test]
    fn eventfs_path_parses_materialized_json_surfaces() {
        assert_eq!(
            JetStreamPath::parse("/events/system.jsonl").unwrap(),
            JetStreamPath::EventLog {
                stream: "system".into()
            }
        );
        assert_eq!(
            JetStreamPath::parse("/tasks/comfy/render-001.json").unwrap(),
            JetStreamPath::Task {
                namespace: "comfy".into(),
                task: "render-001.json".into()
            }
        );
        assert_eq!(
            JetStreamPath::parse("/agents/worker/inbox").unwrap(),
            JetStreamPath::AgentMailbox {
                agent: "worker".into(),
                area: AgentArea::Inbox
            }
        );
        assert_eq!(
            JetStreamPath::parse("/semantic/summaries/file.json").unwrap(),
            JetStreamPath::SemanticRecord {
                area: SemanticArea::Summaries,
                path: "file.json".into()
            }
        );
    }

    #[test]
    fn eventfs_path_rejects_invalid_or_ambiguous_paths() {
        assert!(JetStreamPath::parse("/kv/bucket/.hidden").is_err());
        assert!(JetStreamPath::parse("/kv/bucket/bad key.json").is_err());
        assert!(JetStreamPath::parse("/kv/bucket/name.").is_err());
        assert!(JetStreamPath::parse("/kv/bucket/__eventfs_writeback.abc").is_err());
        assert!(JetStreamPath::parse("/kv/bucket/__eventfs_applied/abc").is_err());
        assert!(JetStreamPath::parse("/kv/bucket/.history/__eventfs_applied.abc").is_err());
        assert!(JetStreamPath::parse("/kv/bucket/.history/__eventfs_writeback.abc/@1").is_err());
        assert!(JetStreamPath::parse("/tasks/__eventfs_writeback/job.json").is_err());
        assert!(JetStreamPath::parse("/agents/__eventfs_applied/tasks/job.json").is_err());
        assert!(JetStreamPath::parse("/tasks/ns/bad key.json").is_err());
        assert!(JetStreamPath::parse("/agents/a/tasks/bad key.json").is_err());
        assert!(JetStreamPath::parse("/semantic/tags/name.").is_err());
        assert!(JetStreamPath::parse("/streams/ORDERS/messages/latest.json").is_err());
        assert!(JetStreamPath::parse("/streams/ORDERS/subjects/bad..subject.jsonl").is_err());
        assert!(JetStreamPath::parse("/streams/ORDERS/subjects/foo@bar.jsonl").is_err());
        assert!(JetStreamPath::parse(
            "/streams/ORDERS/subjects/__eventfs_subject_hex_6f72646572732e.jsonl"
        )
        .is_err());
        assert!(JetStreamPath::parse(
            "/streams/ORDERS/subjects/__eventfs_subject_hex_6f72646572733e.jsonl"
        )
        .is_err());
        assert!(JetStreamPath::parse("/agents/a/inbox/message.json").is_err());
        assert!(JetStreamPath::parse("/semantic/unknown/file.json").is_err());
    }

    #[test]
    fn eventfs_path_accepts_nats_kv_key_grammar() {
        assert_eq!(
            JetStreamPath::parse("/kv/bucket/dir/name=1.json").unwrap(),
            JetStreamPath::KvKey {
                bucket: "bucket".into(),
                key: "dir/name=1.json".into()
            }
        );
    }
}
