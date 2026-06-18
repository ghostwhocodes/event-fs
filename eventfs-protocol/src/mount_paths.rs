use std::fmt;

use crate::{
    is_reserved_kv_key, stream_subject_file_name_from_str, AgentArea, EventFsError, JetStreamPath,
    MaterializedTarget, AGENTS_BUCKET, AGENTS_STREAM, SEMANTIC_BUCKET, TASKS_BUCKET,
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MountPath(String);

impl MountPath {
    pub fn new(path: impl AsRef<str>) -> Result<Self, EventFsError> {
        let path = path.as_ref();
        if path.as_bytes().contains(&0) {
            return Err(EventFsError::invalid_path("path contains NUL"));
        }
        let components: Vec<&str> = path
            .split('/')
            .filter(|component| !component.is_empty())
            .collect();
        Self::from_components(&components)
    }

    pub fn root() -> Self {
        Self("/".into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn is_root(&self) -> bool {
        self.0 == "/"
    }

    pub fn parent(&self) -> Self {
        let components = self.components();
        if components.len() <= 1 {
            return Self::root();
        }
        Self(format!("/{}", components[..components.len() - 1].join("/")))
    }

    pub fn join_child(&self, child: impl AsRef<str>) -> Result<Self, EventFsError> {
        let child = child.as_ref();
        if self.is_root() {
            Self::new(format!("/{child}"))
        } else {
            Self::new(format!("{}/{child}", self.as_str()))
        }
    }

    pub fn is_descendant_of(&self, root: &MountPath) -> bool {
        if root.is_root() {
            return !self.is_root();
        }
        self.0
            .strip_prefix(root.as_str())
            .and_then(|remainder| remainder.strip_prefix('/'))
            .is_some()
    }

    pub fn is_self_or_descendant_of(&self, root: &MountPath) -> bool {
        self == root || self.is_descendant_of(root)
    }

    fn from_components(components: &[&str]) -> Result<Self, EventFsError> {
        if components.is_empty() {
            return Ok(Self::root());
        }
        for component in components {
            validate_mount_component(component)?;
        }
        Ok(Self(format!("/{}", components.join("/"))))
    }

    fn components(&self) -> Vec<&str> {
        self.0
            .trim_matches('/')
            .split('/')
            .filter(|component| !component.is_empty())
            .collect()
    }
}

impl fmt::Display for MountPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for MountPath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<&str> for MountPath {
    type Error = EventFsError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageFact {
    Kv { bucket: String, key: String },
    StreamSubject { stream: String, subject: String },
    Object { bucket: String, name: String },
}

impl StorageFact {
    pub fn from_jetstream_path(path: &JetStreamPath) -> Option<Self> {
        match path {
            JetStreamPath::KvKey { bucket, key } => Some(Self::Kv {
                bucket: bucket.clone(),
                key: key.clone(),
            }),
            JetStreamPath::StreamSubject { stream, subject } => Some(Self::StreamSubject {
                stream: stream.clone(),
                subject: subject.as_str().to_string(),
            }),
            JetStreamPath::Object { bucket, object } => Some(Self::Object {
                bucket: bucket.clone(),
                name: object.clone(),
            }),
            _ => MaterializedTarget::from_path(path).map(Self::from_materialized_target),
        }
    }

    pub fn from_materialized_target(target: MaterializedTarget) -> Self {
        match target {
            MaterializedTarget::Kv { bucket, key } => Self::Kv { bucket, key },
            MaterializedTarget::Stream { stream, subject } => {
                Self::StreamSubject { stream, subject }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AffectedPathReason {
    Exact,
    Ancestor,
    Alias,
    Mailbox,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AffectedPath {
    pub path: MountPath,
    pub reason: AffectedPathReason,
}

impl AffectedPath {
    pub fn new(path: MountPath, reason: AffectedPathReason) -> Self {
        Self { path, reason }
    }
}

pub fn visible_paths(fact: &StorageFact) -> Result<Vec<AffectedPath>, EventFsError> {
    let mut paths = Vec::new();
    match fact {
        StorageFact::Kv { bucket, key } => {
            push_storage_path(
                &mut paths,
                AffectedPathReason::Exact,
                &["kv", bucket],
                Some(key),
            )?;
            push_kv_alias_paths(&mut paths, bucket, key)?;
        }
        StorageFact::StreamSubject { stream, subject } => {
            push_storage_path(
                &mut paths,
                AffectedPathReason::Exact,
                &["streams", stream, "subjects"],
                Some(&stream_subject_file_name_from_str(subject)),
            )?;
            push_stream_alias_paths(&mut paths, stream, subject)?;
        }
        StorageFact::Object { bucket, name } => {
            push_storage_path(
                &mut paths,
                AffectedPathReason::Exact,
                &["objects", bucket],
                Some(name),
            )?;
        }
    }
    Ok(paths)
}

pub fn invalidation_paths(fact: &StorageFact) -> Result<Vec<AffectedPath>, EventFsError> {
    let visible = visible_paths(fact)?;
    let mut paths = Vec::new();
    for affected in visible {
        push_unique(&mut paths, affected.clone());
        push_ancestors(&mut paths, &affected.path, affected.reason)?;
    }
    Ok(paths)
}

pub fn invalidation_paths_for_mount_path(
    path: &JetStreamPath,
) -> Result<Vec<AffectedPath>, EventFsError> {
    let source_path = mount_path_from_jetstream_path(path)?;
    if let Some(fact) = StorageFact::from_jetstream_path(path) {
        return prioritize_source_path(invalidation_paths(&fact)?, &source_path);
    }
    let mut paths = vec![AffectedPath::new(
        source_path.clone(),
        AffectedPathReason::Exact,
    )];
    push_ancestors(&mut paths, &source_path, AffectedPathReason::Exact)?;
    Ok(paths)
}

pub fn mount_path_from_jetstream_path(path: &JetStreamPath) -> Result<MountPath, EventFsError> {
    match path {
        JetStreamPath::Root => MountPath::new("/"),
        JetStreamPath::KvRoot => MountPath::new("/kv"),
        JetStreamPath::KvBucket { bucket } => join_storage_path(&["kv", bucket], None),
        JetStreamPath::KvKey { bucket, key } => join_storage_path(&["kv", bucket], Some(key)),
        JetStreamPath::KvHistoryRoot { bucket } => {
            join_storage_path(&["kv", bucket, ".history"], None)
        }
        JetStreamPath::KvHistoryKey { bucket, key } => {
            join_storage_path(&["kv", bucket, ".history"], Some(key))
        }
        JetStreamPath::KvRevision {
            bucket,
            key,
            revision,
        } => join_storage_path(
            &["kv", bucket, ".history"],
            Some(&format!("{key}/@{revision}")),
        ),
        JetStreamPath::StreamsRoot => MountPath::new("/streams"),
        JetStreamPath::StreamRoot { stream } => join_storage_path(&["streams", stream], None),
        JetStreamPath::StreamMessages { stream } => {
            join_storage_path(&["streams", stream, "messages"], None)
        }
        JetStreamPath::StreamMessage { stream, sequence } => join_storage_path(
            &["streams", stream, "messages"],
            Some(&format!("{sequence}.json")),
        ),
        JetStreamPath::StreamSubjects { stream } => {
            join_storage_path(&["streams", stream, "subjects"], None)
        }
        JetStreamPath::StreamSubject { stream, subject } => join_storage_path(
            &["streams", stream, "subjects"],
            Some(&stream_subject_file_name_from_str(subject.as_str())),
        ),
        JetStreamPath::ObjectsRoot => MountPath::new("/objects"),
        JetStreamPath::ObjectBucket { bucket } => join_storage_path(&["objects", bucket], None),
        JetStreamPath::Object { bucket, object } => {
            join_storage_path(&["objects", bucket], Some(object))
        }
        JetStreamPath::EventsRoot => MountPath::new("/events"),
        JetStreamPath::EventLog { stream } => {
            join_storage_path(&["events"], Some(&format!("{stream}.jsonl")))
        }
        JetStreamPath::TasksRoot => MountPath::new("/tasks"),
        JetStreamPath::TaskNamespace { namespace } => {
            join_storage_path(&["tasks", namespace], None)
        }
        JetStreamPath::Task { namespace, task } => {
            join_storage_path(&["tasks", namespace], Some(task))
        }
        JetStreamPath::AgentsRoot => MountPath::new("/agents"),
        JetStreamPath::AgentRoot { agent } => join_storage_path(&["agents", agent], None),
        JetStreamPath::AgentMailbox { agent, area }
        | JetStreamPath::AgentDirectory { agent, area } => {
            join_storage_path(&["agents", agent, area.as_str()], None)
        }
        JetStreamPath::AgentRecord { agent, area, path } => {
            join_storage_path(&["agents", agent, area.as_str()], Some(path))
        }
        JetStreamPath::SemanticRoot => MountPath::new("/semantic"),
        JetStreamPath::SemanticArea { area } => {
            join_storage_path(&["semantic", area.as_str()], None)
        }
        JetStreamPath::SemanticRecord { area, path } => {
            join_storage_path(&["semantic", area.as_str()], Some(path))
        }
        JetStreamPath::MetadataRoot => MountPath::new("/.eventfs"),
        JetStreamPath::MetadataFile(file) => {
            join_storage_path(&[".eventfs", file.file_name()], None)
        }
    }
}

fn push_kv_alias_paths(
    paths: &mut Vec<AffectedPath>,
    bucket: &str,
    key: &str,
) -> Result<(), EventFsError> {
    match bucket {
        TASKS_BUCKET => push_storage_path(paths, AffectedPathReason::Alias, &["tasks"], Some(key)),
        AGENTS_BUCKET => {
            let Some((agent, area, rest)) = agent_kv_alias_parts(key) else {
                return Ok(());
            };
            push_storage_path(
                paths,
                AffectedPathReason::Alias,
                &["agents", agent, area],
                Some(rest),
            )
        }
        SEMANTIC_BUCKET => {
            push_storage_path(paths, AffectedPathReason::Alias, &["semantic"], Some(key))
        }
        _ => Ok(()),
    }
}

fn push_stream_alias_paths(
    paths: &mut Vec<AffectedPath>,
    stream: &str,
    subject: &str,
) -> Result<(), EventFsError> {
    if subject
        .strip_prefix("events.")
        .is_some_and(|event_stream| event_stream == stream)
    {
        push_storage_path(
            paths,
            AffectedPathReason::Alias,
            &["events"],
            Some(&format!("{stream}.jsonl")),
        )?;
    }
    if stream == AGENTS_STREAM {
        if let Some((agent, area)) = agent_mailbox_subject_parts(subject) {
            push_storage_path(
                paths,
                AffectedPathReason::Mailbox,
                &["agents", agent, area.as_str()],
                None,
            )?;
        }
    }
    Ok(())
}

fn push_ancestors(
    paths: &mut Vec<AffectedPath>,
    path: &MountPath,
    source_reason: AffectedPathReason,
) -> Result<(), EventFsError> {
    let components = path.components();
    let floor = ancestor_floor(&components, source_reason);
    if components.len() <= floor {
        return Ok(());
    }
    for len in (floor..components.len()).rev() {
        push_unique(
            paths,
            AffectedPath::new(
                MountPath::from_components(&components[..len])?,
                AffectedPathReason::Ancestor,
            ),
        );
    }
    Ok(())
}

fn ancestor_floor(components: &[&str], source_reason: AffectedPathReason) -> usize {
    match components.first().copied() {
        Some("kv") | Some("objects") => 2,
        Some("streams") => 3,
        Some("tasks") | Some("semantic") => 2,
        Some("events") => 1,
        Some("agents") if source_reason == AffectedPathReason::Mailbox => 1,
        Some("agents") => 2,
        _ => components.len(),
    }
}

fn push_storage_path(
    paths: &mut Vec<AffectedPath>,
    reason: AffectedPathReason,
    fixed: &[&str],
    tail: Option<&str>,
) -> Result<(), EventFsError> {
    push_unique(
        paths,
        AffectedPath::new(join_storage_path(fixed, tail)?, reason),
    );
    Ok(())
}

fn join_storage_path(fixed: &[&str], tail: Option<&str>) -> Result<MountPath, EventFsError> {
    let mut components = fixed.to_vec();
    if let Some(tail) = tail {
        if tail.is_empty() {
            return Err(EventFsError::invalid_path("missing path tail"));
        }
        components.extend(tail.split('/'));
    }
    MountPath::from_components(&components)
}

fn push_unique(paths: &mut Vec<AffectedPath>, affected: AffectedPath) {
    if !paths
        .iter()
        .any(|candidate| candidate.path == affected.path)
    {
        paths.push(affected);
    }
}

fn prioritize_source_path(
    paths: Vec<AffectedPath>,
    source_path: &MountPath,
) -> Result<Vec<AffectedPath>, EventFsError> {
    let Some(source) = paths
        .iter()
        .find(|affected| &affected.path == source_path)
        .cloned()
    else {
        return Ok(paths);
    };

    let mut reordered = Vec::new();
    push_unique(&mut reordered, source.clone());
    push_ancestors(&mut reordered, &source.path, source.reason)?;
    for affected in paths {
        push_unique(&mut reordered, affected);
    }
    Ok(reordered)
}

fn agent_kv_alias_parts(key: &str) -> Option<(&str, &str, &str)> {
    let mut parts = key.splitn(3, '/');
    let agent = parts.next()?;
    let area = parts.next()?;
    let rest = parts.next()?;
    (!agent.is_empty() && !area.is_empty() && !rest.is_empty()).then_some((agent, area, rest))
}

fn agent_mailbox_subject_parts(subject: &str) -> Option<(&str, AgentArea)> {
    let mut parts = subject.split('.');
    let (Some("agents"), Some(agent), Some(area), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    if agent.is_empty() || is_reserved_kv_key(agent) {
        return None;
    }
    let area = AgentArea::parse(area)?;
    area.is_mailbox().then_some((agent, area))
}

fn validate_mount_component(component: &str) -> Result<(), EventFsError> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component.as_bytes().contains(&0)
        || component.contains('/')
    {
        Err(EventFsError::invalid_path("invalid path component"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(paths: Vec<AffectedPath>) -> Vec<String> {
        paths
            .into_iter()
            .map(|affected| affected.path.into_string())
            .collect()
    }

    fn reasons(paths: Vec<AffectedPath>) -> Vec<AffectedPathReason> {
        paths.into_iter().map(|affected| affected.reason).collect()
    }

    #[test]
    fn mount_path_normalizes_to_absolute_without_trailing_or_redundant_slashes() {
        assert_eq!(MountPath::new("").unwrap().as_str(), "/");
        assert_eq!(
            MountPath::new("kv/bucket/key").unwrap().as_str(),
            "/kv/bucket/key"
        );
        assert_eq!(
            MountPath::new("//kv//bucket/key//").unwrap().as_str(),
            "/kv/bucket/key"
        );
        assert_eq!(MountPath::new("/").unwrap().as_str(), "/");
    }

    #[test]
    fn mount_path_rejects_ambiguous_or_invalid_components() {
        assert!(MountPath::new("/kv/../bucket").is_err());
        assert!(MountPath::new("/kv/./bucket").is_err());
        assert!(MountPath::new("/kv/bucket/\0").is_err());
    }

    #[test]
    fn mount_path_owns_parent_child_and_descendant_identity() {
        let path = MountPath::new("/kv/bucket/dir/file.json").unwrap();
        assert_eq!(path.parent().as_str(), "/kv/bucket/dir");
        assert_eq!(MountPath::root().parent(), MountPath::root());
        assert_eq!(
            MountPath::new("/kv/bucket")
                .unwrap()
                .join_child("dir")
                .unwrap(),
            MountPath::new("/kv/bucket/dir").unwrap()
        );
        assert_eq!(
            MountPath::root().join_child("kv").unwrap(),
            MountPath::new("/kv").unwrap()
        );
        assert!(path.is_descendant_of(&MountPath::new("/kv/bucket").unwrap()));
        assert!(path.is_self_or_descendant_of(&path));
        assert!(!MountPath::new("/kv/bucketish")
            .unwrap()
            .is_descendant_of(&MountPath::new("/kv/bucket").unwrap()));
    }

    #[test]
    fn materialized_kv_visible_paths_include_native_and_aliases() {
        assert_eq!(
            strings(
                visible_paths(&StorageFact::Kv {
                    bucket: TASKS_BUCKET.into(),
                    key: "demo/render.json".into(),
                })
                .unwrap()
            ),
            vec![
                format!("/kv/{TASKS_BUCKET}/demo/render.json"),
                "/tasks/demo/render.json".into(),
            ]
        );
        assert_eq!(
            strings(
                visible_paths(&StorageFact::Kv {
                    bucket: AGENTS_BUCKET.into(),
                    key: "bot/memory/facts/a.json".into(),
                })
                .unwrap()
            ),
            vec![
                format!("/kv/{AGENTS_BUCKET}/bot/memory/facts/a.json"),
                "/agents/bot/memory/facts/a.json".into(),
            ]
        );
        assert_eq!(
            strings(
                visible_paths(&StorageFact::Kv {
                    bucket: SEMANTIC_BUCKET.into(),
                    key: "tags/project/a.json".into(),
                })
                .unwrap()
            ),
            vec![
                format!("/kv/{SEMANTIC_BUCKET}/tags/project/a.json"),
                "/semantic/tags/project/a.json".into(),
            ]
        );
    }

    #[test]
    fn native_kv_and_object_paths_have_exact_visible_paths() {
        assert_eq!(
            strings(
                visible_paths(&StorageFact::Kv {
                    bucket: "app".into(),
                    key: "config/service.json".into(),
                })
                .unwrap()
            ),
            vec!["/kv/app/config/service.json"]
        );
        assert_eq!(
            strings(
                visible_paths(&StorageFact::Object {
                    bucket: "assets".into(),
                    name: "images/logo.png".into(),
                })
                .unwrap()
            ),
            vec!["/objects/assets/images/logo.png"]
        );
    }

    #[test]
    fn stream_subject_visible_paths_include_events_and_mailboxes() {
        assert_eq!(
            strings(
                visible_paths(&StorageFact::StreamSubject {
                    stream: "system".into(),
                    subject: "events.system".into(),
                })
                .unwrap()
            ),
            vec![
                "/streams/system/subjects/events.system.jsonl",
                "/events/system.jsonl",
            ]
        );
        assert_eq!(
            strings(
                visible_paths(&StorageFact::StreamSubject {
                    stream: AGENTS_STREAM.into(),
                    subject: "agents.bot.inbox".into(),
                })
                .unwrap()
            ),
            vec![
                format!("/streams/{AGENTS_STREAM}/subjects/agents.bot.inbox.jsonl"),
                "/agents/bot/inbox".into(),
            ]
        );
    }

    #[test]
    fn stream_subject_mailbox_aliases_reject_reserved_internal_agents() {
        assert_eq!(
            strings(
                visible_paths(&StorageFact::StreamSubject {
                    stream: AGENTS_STREAM.into(),
                    subject: "agents.__eventfs_applied.inbox".into(),
                })
                .unwrap()
            ),
            vec![format!(
                "/streams/{AGENTS_STREAM}/subjects/agents.__eventfs_applied.inbox.jsonl"
            )]
        );
    }

    #[test]
    fn materialized_invalidation_paths_include_alias_ancestors() {
        assert_eq!(
            strings(
                invalidation_paths(&StorageFact::Kv {
                    bucket: TASKS_BUCKET.into(),
                    key: "demo/render/output.json".into(),
                })
                .unwrap()
            ),
            vec![
                format!("/kv/{TASKS_BUCKET}/demo/render/output.json"),
                format!("/kv/{TASKS_BUCKET}/demo/render"),
                format!("/kv/{TASKS_BUCKET}/demo"),
                format!("/kv/{TASKS_BUCKET}"),
                "/tasks/demo/render/output.json".into(),
                "/tasks/demo/render".into(),
                "/tasks/demo".into(),
            ]
        );
        assert_eq!(
            strings(
                invalidation_paths(&StorageFact::Object {
                    bucket: "assets".into(),
                    name: "images/logo.png".into(),
                })
                .unwrap()
            ),
            vec![
                "/objects/assets/images/logo.png",
                "/objects/assets/images",
                "/objects/assets",
            ]
        );
    }

    #[test]
    fn stream_invalidation_paths_include_native_subject_and_alias_ancestors() {
        let event_paths = invalidation_paths(&StorageFact::StreamSubject {
            stream: "system".into(),
            subject: "events.system".into(),
        })
        .unwrap();
        assert_eq!(
            strings(event_paths.clone()),
            vec![
                "/streams/system/subjects/events.system.jsonl",
                "/streams/system/subjects",
                "/events/system.jsonl",
                "/events",
            ]
        );
        assert_eq!(
            reasons(event_paths),
            vec![
                AffectedPathReason::Exact,
                AffectedPathReason::Ancestor,
                AffectedPathReason::Alias,
                AffectedPathReason::Ancestor,
            ]
        );

        let mailbox_paths = invalidation_paths(&StorageFact::StreamSubject {
            stream: AGENTS_STREAM.into(),
            subject: "agents.bot.outbox".into(),
        })
        .unwrap();
        assert_eq!(
            strings(mailbox_paths.clone()),
            vec![
                format!("/streams/{AGENTS_STREAM}/subjects/agents.bot.outbox.jsonl"),
                format!("/streams/{AGENTS_STREAM}/subjects"),
                "/agents/bot/outbox".into(),
                "/agents/bot".into(),
                "/agents".into(),
            ]
        );
        assert_eq!(
            reasons(mailbox_paths),
            vec![
                AffectedPathReason::Exact,
                AffectedPathReason::Ancestor,
                AffectedPathReason::Mailbox,
                AffectedPathReason::Ancestor,
                AffectedPathReason::Ancestor,
            ]
        );
    }

    #[test]
    fn storage_facts_adapt_from_native_and_materialized_mount_paths() {
        assert_eq!(
            StorageFact::from_jetstream_path(
                &JetStreamPath::parse("/tasks/demo/render.json").unwrap()
            ),
            Some(StorageFact::Kv {
                bucket: TASKS_BUCKET.into(),
                key: "demo/render.json".into(),
            })
        );
        assert_eq!(
            StorageFact::from_jetstream_path(
                &JetStreamPath::parse("/events/system.jsonl").unwrap()
            ),
            Some(StorageFact::StreamSubject {
                stream: "system".into(),
                subject: "events.system".into(),
            })
        );
        assert_eq!(
            StorageFact::from_jetstream_path(
                &JetStreamPath::parse("/objects/assets/a.bin").unwrap()
            ),
            Some(StorageFact::Object {
                bucket: "assets".into(),
                name: "a.bin".into(),
            })
        );
    }

    #[test]
    fn mount_path_invalidation_prioritizes_the_changed_alias() {
        assert_eq!(
            strings(
                invalidation_paths_for_mount_path(
                    &JetStreamPath::parse("/agents/bot/memory/facts/a.json").unwrap()
                )
                .unwrap()
            ),
            vec![
                "/agents/bot/memory/facts/a.json".to_string(),
                "/agents/bot/memory/facts".to_string(),
                "/agents/bot/memory".to_string(),
                "/agents/bot".to_string(),
                format!("/kv/{AGENTS_BUCKET}/bot/memory/facts/a.json"),
                format!("/kv/{AGENTS_BUCKET}/bot/memory/facts"),
                format!("/kv/{AGENTS_BUCKET}/bot/memory"),
                format!("/kv/{AGENTS_BUCKET}/bot"),
                format!("/kv/{AGENTS_BUCKET}"),
            ]
        );
    }
}
