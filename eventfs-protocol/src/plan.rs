use crate::{
    AgentArea, Errno, EventFsError, JetStreamPath, MaterializedTarget, MetadataFile, StreamSubject,
    AGENTS_BUCKET, SEMANTIC_BUCKET, TASKS_BUCKET,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileIntent {
    GetAttr,
    ReadDir,
    Read,
    Write,
    Create,
    Mkdir,
    Unlink,
    Rename { to: JetStreamPath },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JetStreamAction {
    StaticDirectory {
        entries: Vec<StaticDirectoryEntry>,
    },
    MetadataRead {
        file: MetadataFile,
    },
    ListKvBuckets,
    EnsureKvBucket {
        bucket: String,
    },
    EnsureKvDirectory {
        bucket: String,
        prefix: String,
    },
    ListKvPrefix {
        bucket: String,
        prefix: String,
    },
    KvGet {
        bucket: String,
        key: String,
    },
    KvPut {
        bucket: String,
        key: String,
    },
    KvDelete {
        bucket: String,
        key: String,
    },
    KvHistory {
        bucket: String,
        key: String,
    },
    ListKvHistoryPrefix {
        bucket: String,
        prefix: String,
    },
    KvRevision {
        bucket: String,
        key: String,
        revision: u64,
    },
    ListStreams,
    ListEventLogs,
    ListAgents,
    EnsureStream {
        stream: String,
    },
    ListStreamMessages {
        stream: String,
    },
    ListStreamSubjects {
        stream: String,
    },
    StreamMessage {
        stream: String,
        sequence: u64,
    },
    PublishJsonLines {
        stream: String,
        subject: String,
    },
    ListObjectBuckets,
    EnsureObjectBucket {
        bucket: String,
    },
    EnsureObjectDirectory {
        bucket: String,
        prefix: String,
    },
    ListObjectPrefix {
        bucket: String,
        prefix: String,
    },
    ObjectGet {
        bucket: String,
        object: String,
    },
    ObjectPut {
        bucket: String,
        object: String,
    },
    ObjectDelete {
        bucket: String,
        object: String,
    },
    MaterializedGet {
        target: MaterializedTarget,
    },
    MaterializedPut {
        target: MaterializedTarget,
    },
    MaterializedDelete {
        target: MaterializedTarget,
    },
    Rename {
        plan: RenamePlan,
    },
    NoopDirectory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RenamePlan {
    Kv {
        from_bucket: String,
        from_key: String,
        to_bucket: String,
        to_key: String,
    },
    Object {
        from_bucket: String,
        from_object: String,
        to_bucket: String,
        to_object: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RenameSurface {
    Kv,
    Object,
    Tasks,
    Agent { area: AgentArea },
    Semantic { area: crate::SemanticArea },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticEntryKind {
    Directory,
    File,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticDirectoryEntry {
    pub name: String,
    pub kind: StaticEntryKind,
}

impl StaticDirectoryEntry {
    pub fn directory(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: StaticEntryKind::Directory,
        }
    }

    pub fn file(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: StaticEntryKind::File,
        }
    }
}

pub fn plan_operation(
    intent: FileIntent,
    path: &JetStreamPath,
) -> Result<JetStreamAction, EventFsError> {
    match intent {
        FileIntent::GetAttr => plan_getattr(path),
        FileIntent::ReadDir => plan_readdir(path),
        FileIntent::Read => plan_read(path),
        FileIntent::Write | FileIntent::Create => plan_write(path),
        FileIntent::Mkdir => plan_mkdir(path),
        FileIntent::Unlink => plan_unlink(path),
        FileIntent::Rename { to } => plan_rename(path, &to),
    }
}

fn plan_getattr(path: &JetStreamPath) -> Result<JetStreamAction, EventFsError> {
    if path.is_directory() {
        Ok(JetStreamAction::NoopDirectory)
    } else {
        plan_read(path)
    }
}

fn plan_readdir(path: &JetStreamPath) -> Result<JetStreamAction, EventFsError> {
    match path {
        JetStreamPath::Root => Ok(JetStreamAction::StaticDirectory {
            entries: crate::ROOT_DIRECTORIES
                .iter()
                .map(|entry| StaticDirectoryEntry::directory(*entry))
                .collect(),
        }),
        JetStreamPath::KvRoot => Ok(JetStreamAction::ListKvBuckets),
        JetStreamPath::KvBucket { bucket } => Ok(JetStreamAction::ListKvPrefix {
            bucket: bucket.clone(),
            prefix: String::new(),
        }),
        JetStreamPath::KvKey { bucket, key } => Ok(JetStreamAction::ListKvPrefix {
            bucket: bucket.clone(),
            prefix: key.clone(),
        }),
        JetStreamPath::KvHistoryRoot { bucket } => Ok(JetStreamAction::ListKvHistoryPrefix {
            bucket: bucket.clone(),
            prefix: String::new(),
        }),
        JetStreamPath::KvHistoryKey { bucket, key } => Ok(JetStreamAction::KvHistory {
            bucket: bucket.clone(),
            key: key.clone(),
        }),
        JetStreamPath::StreamsRoot => Ok(JetStreamAction::ListStreams),
        JetStreamPath::StreamRoot { .. } => Ok(JetStreamAction::StaticDirectory {
            entries: vec![
                StaticDirectoryEntry::directory("messages"),
                StaticDirectoryEntry::directory("subjects"),
            ],
        }),
        JetStreamPath::StreamMessages { stream } => Ok(JetStreamAction::ListStreamMessages {
            stream: stream.clone(),
        }),
        JetStreamPath::StreamSubjects { stream } => Ok(JetStreamAction::ListStreamSubjects {
            stream: stream.clone(),
        }),
        JetStreamPath::ObjectsRoot => Ok(JetStreamAction::ListObjectBuckets),
        JetStreamPath::ObjectBucket { bucket } => Ok(JetStreamAction::ListObjectPrefix {
            bucket: bucket.clone(),
            prefix: String::new(),
        }),
        JetStreamPath::Object { bucket, object } => Ok(JetStreamAction::ListObjectPrefix {
            bucket: bucket.clone(),
            prefix: object.clone(),
        }),
        JetStreamPath::EventsRoot => Ok(JetStreamAction::ListEventLogs),
        JetStreamPath::TasksRoot => Ok(JetStreamAction::ListKvPrefix {
            bucket: TASKS_BUCKET.into(),
            prefix: String::new(),
        }),
        JetStreamPath::TaskNamespace { namespace } => Ok(JetStreamAction::ListKvPrefix {
            bucket: TASKS_BUCKET.into(),
            prefix: namespace.clone(),
        }),
        JetStreamPath::AgentsRoot => Ok(JetStreamAction::ListAgents),
        JetStreamPath::AgentRoot { .. } => Ok(JetStreamAction::StaticDirectory {
            entries: vec![
                StaticDirectoryEntry::file("inbox"),
                StaticDirectoryEntry::file("outbox"),
                StaticDirectoryEntry::directory("tasks"),
                StaticDirectoryEntry::directory("memory"),
            ],
        }),
        JetStreamPath::AgentDirectory { agent, area } => Ok(JetStreamAction::ListKvPrefix {
            bucket: AGENTS_BUCKET.into(),
            prefix: format!("{}/{}", agent, area.as_str()),
        }),
        JetStreamPath::SemanticRoot => Ok(JetStreamAction::StaticDirectory {
            entries: vec![
                StaticDirectoryEntry::directory("summaries"),
                StaticDirectoryEntry::directory("tags"),
                StaticDirectoryEntry::directory("relations"),
                StaticDirectoryEntry::directory("timelines"),
                StaticDirectoryEntry::directory("embeddings"),
            ],
        }),
        JetStreamPath::SemanticArea { area } => Ok(JetStreamAction::ListKvPrefix {
            bucket: SEMANTIC_BUCKET.into(),
            prefix: area.as_str().into(),
        }),
        JetStreamPath::AgentRecord { agent, area, path } => Ok(JetStreamAction::ListKvPrefix {
            bucket: AGENTS_BUCKET.into(),
            prefix: format!("{}/{}/{}", agent, area.as_str(), path),
        }),
        JetStreamPath::SemanticRecord { area, path } => Ok(JetStreamAction::ListKvPrefix {
            bucket: SEMANTIC_BUCKET.into(),
            prefix: format!("{}/{}", area.as_str(), path),
        }),
        JetStreamPath::MetadataRoot => Ok(JetStreamAction::StaticDirectory {
            entries: vec![
                StaticDirectoryEntry::file(MetadataFile::Status.file_name()),
                StaticDirectoryEntry::file(MetadataFile::Cache.file_name()),
                StaticDirectoryEntry::file(MetadataFile::Queue.file_name()),
                StaticDirectoryEntry::file(MetadataFile::Capabilities.file_name()),
                StaticDirectoryEntry::file(MetadataFile::Errors.file_name()),
            ],
        }),
        _ => Err(EventFsError::unsupported("readdir", format!("{path:?}"))),
    }
}

fn plan_read(path: &JetStreamPath) -> Result<JetStreamAction, EventFsError> {
    match path {
        JetStreamPath::KvKey { bucket, key } => Ok(JetStreamAction::KvGet {
            bucket: bucket.clone(),
            key: key.clone(),
        }),
        JetStreamPath::KvRevision {
            bucket,
            key,
            revision,
        } => Ok(JetStreamAction::KvRevision {
            bucket: bucket.clone(),
            key: key.clone(),
            revision: *revision,
        }),
        JetStreamPath::StreamMessage { stream, sequence } => Ok(JetStreamAction::StreamMessage {
            stream: stream.clone(),
            sequence: *sequence,
        }),
        JetStreamPath::StreamSubject { stream, subject } => publish_action(stream, subject),
        JetStreamPath::Object { bucket, object } => Ok(JetStreamAction::ObjectGet {
            bucket: bucket.clone(),
            object: object.clone(),
        }),
        JetStreamPath::MetadataFile(file) => Ok(JetStreamAction::MetadataRead { file: *file }),
        _ => MaterializedTarget::from_path(path)
            .map(|target| JetStreamAction::MaterializedGet { target })
            .ok_or_else(|| EventFsError::unsupported("read", format!("{path:?}"))),
    }
}

fn plan_write(path: &JetStreamPath) -> Result<JetStreamAction, EventFsError> {
    match path {
        JetStreamPath::KvKey { bucket, key } => Ok(JetStreamAction::KvPut {
            bucket: bucket.clone(),
            key: key.clone(),
        }),
        JetStreamPath::StreamSubject { stream, subject } => publish_action(stream, subject),
        JetStreamPath::EventLog { .. } | JetStreamPath::AgentMailbox { .. } => {
            MaterializedTarget::from_path(path)
                .and_then(|target| match target {
                    MaterializedTarget::Stream { stream, subject } => {
                        Some(JetStreamAction::PublishJsonLines { stream, subject })
                    }
                    MaterializedTarget::Kv { .. } => None,
                })
                .ok_or_else(|| EventFsError::unsupported("write", format!("{path:?}")))
        }
        JetStreamPath::Object { bucket, object } => Ok(JetStreamAction::ObjectPut {
            bucket: bucket.clone(),
            object: object.clone(),
        }),
        JetStreamPath::Task { .. }
        | JetStreamPath::AgentRecord {
            area: AgentArea::Tasks | AgentArea::Memory,
            ..
        }
        | JetStreamPath::SemanticRecord { .. } => MaterializedTarget::from_path(path)
            .map(|target| JetStreamAction::MaterializedPut { target })
            .ok_or_else(|| EventFsError::unsupported("write", format!("{path:?}"))),
        JetStreamPath::KvRevision { .. } | JetStreamPath::StreamMessage { .. } => {
            Err(EventFsError::read_only(format!("{path:?}")))
        }
        _ if path.is_directory() => Err(EventFsError::Unsupported {
            operation: "write",
            path: format!("{path:?}"),
            errno: Errno::IS_DIRECTORY,
        }),
        _ => Err(EventFsError::unsupported("write", format!("{path:?}"))),
    }
}

fn plan_mkdir(path: &JetStreamPath) -> Result<JetStreamAction, EventFsError> {
    match path {
        JetStreamPath::KvBucket { bucket } => Ok(JetStreamAction::EnsureKvBucket {
            bucket: bucket.clone(),
        }),
        JetStreamPath::StreamRoot { stream } => Ok(JetStreamAction::EnsureStream {
            stream: stream.clone(),
        }),
        JetStreamPath::StreamMessages { .. } | JetStreamPath::StreamSubjects { .. } => {
            Ok(JetStreamAction::NoopDirectory)
        }
        JetStreamPath::ObjectBucket { bucket } => Ok(JetStreamAction::EnsureObjectBucket {
            bucket: bucket.clone(),
        }),
        JetStreamPath::TaskNamespace { .. }
        | JetStreamPath::AgentRoot { .. }
        | JetStreamPath::AgentDirectory { .. }
        | JetStreamPath::SemanticArea { .. } => Ok(JetStreamAction::NoopDirectory),
        JetStreamPath::KvKey { bucket, key } => Ok(JetStreamAction::EnsureKvDirectory {
            bucket: bucket.clone(),
            prefix: key.clone(),
        }),
        JetStreamPath::Object { bucket, object } => Ok(JetStreamAction::EnsureObjectDirectory {
            bucket: bucket.clone(),
            prefix: object.clone(),
        }),
        _ => Err(EventFsError::unsupported("mkdir", format!("{path:?}"))),
    }
}

fn plan_unlink(path: &JetStreamPath) -> Result<JetStreamAction, EventFsError> {
    match path {
        JetStreamPath::KvKey { bucket, key } => Ok(JetStreamAction::KvDelete {
            bucket: bucket.clone(),
            key: key.clone(),
        }),
        JetStreamPath::Object { bucket, object } => Ok(JetStreamAction::ObjectDelete {
            bucket: bucket.clone(),
            object: object.clone(),
        }),
        JetStreamPath::Task { .. }
        | JetStreamPath::AgentRecord { .. }
        | JetStreamPath::SemanticRecord { .. } => MaterializedTarget::from_path(path)
            .map(|target| JetStreamAction::MaterializedDelete { target })
            .ok_or_else(|| EventFsError::unsupported("unlink", format!("{path:?}"))),
        JetStreamPath::KvRevision { .. } | JetStreamPath::StreamMessage { .. } => {
            Err(EventFsError::read_only(format!("{path:?}")))
        }
        _ => Err(EventFsError::unsupported("unlink", format!("{path:?}"))),
    }
}

fn plan_rename(from: &JetStreamPath, to: &JetStreamPath) -> Result<JetStreamAction, EventFsError> {
    let from_surface = rename_surface(from).ok_or_else(|| cross_device_rename(from))?;
    let to_surface = rename_surface(to).ok_or_else(|| cross_device_rename(from))?;
    if from_surface != to_surface {
        return Err(cross_device_rename(from));
    }

    let plan = match (from, to) {
        (
            JetStreamPath::KvKey {
                bucket: from_bucket,
                key: from_key,
            },
            JetStreamPath::KvKey {
                bucket: to_bucket,
                key: to_key,
            },
        ) if from_bucket == to_bucket => RenamePlan::Kv {
            from_bucket: from_bucket.clone(),
            from_key: from_key.clone(),
            to_bucket: to_bucket.clone(),
            to_key: to_key.clone(),
        },
        (
            JetStreamPath::Object {
                bucket: from_bucket,
                object: from_object,
            },
            JetStreamPath::Object {
                bucket: to_bucket,
                object: to_object,
            },
        ) if from_bucket == to_bucket => RenamePlan::Object {
            from_bucket: from_bucket.clone(),
            from_object: from_object.clone(),
            to_bucket: to_bucket.clone(),
            to_object: to_object.clone(),
        },
        _ => {
            let Some(MaterializedTarget::Kv {
                bucket: from_bucket,
                key: from_key,
            }) = MaterializedTarget::from_path(from)
            else {
                return Err(cross_device_rename(from));
            };
            let Some(MaterializedTarget::Kv {
                bucket: to_bucket,
                key: to_key,
            }) = MaterializedTarget::from_path(to)
            else {
                return Err(cross_device_rename(from));
            };
            if from_bucket != to_bucket {
                return Err(cross_device_rename(from));
            }
            RenamePlan::Kv {
                from_bucket,
                from_key,
                to_bucket,
                to_key,
            }
        }
    };

    Ok(JetStreamAction::Rename { plan })
}

fn rename_surface(path: &JetStreamPath) -> Option<RenameSurface> {
    match path {
        JetStreamPath::KvKey { .. } => Some(RenameSurface::Kv),
        JetStreamPath::Object { .. } => Some(RenameSurface::Object),
        JetStreamPath::Task { .. } => Some(RenameSurface::Tasks),
        JetStreamPath::AgentRecord { area, .. } => Some(RenameSurface::Agent { area: *area }),
        JetStreamPath::SemanticRecord { area, .. } => Some(RenameSurface::Semantic { area: *area }),
        _ => None,
    }
}

fn cross_device_rename(path: &JetStreamPath) -> EventFsError {
    EventFsError::Unsupported {
        operation: "rename",
        path: format!("{path:?}"),
        errno: Errno::CROSS_DEVICE,
    }
}

fn publish_action(stream: &str, subject: &StreamSubject) -> Result<JetStreamAction, EventFsError> {
    Ok(JetStreamAction::PublishJsonLines {
        stream: stream.to_string(),
        subject: subject.as_str().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eventfs_path_plans_event_native_writes() {
        let kv = JetStreamPath::parse("/kv/config/app.json").unwrap();
        assert_eq!(
            plan_operation(FileIntent::Write, &kv).unwrap(),
            JetStreamAction::KvPut {
                bucket: "config".into(),
                key: "app.json".into()
            }
        );

        let stream = JetStreamPath::parse("/streams/ORDERS/subjects/orders.created.jsonl").unwrap();
        assert_eq!(
            plan_operation(FileIntent::Write, &stream).unwrap(),
            JetStreamAction::PublishJsonLines {
                stream: "ORDERS".into(),
                subject: "orders.created".into()
            }
        );
    }

    #[test]
    fn eventfs_path_keeps_immutable_projections_read_only() {
        let history = JetStreamPath::parse("/kv/config/.history/app.json/@1").unwrap();
        let err = plan_operation(FileIntent::Write, &history).unwrap_err();
        assert_eq!(err.errno(), Errno::READ_ONLY);

        let message = JetStreamPath::parse("/streams/ORDERS/messages/1.json").unwrap();
        let err = plan_operation(FileIntent::Unlink, &message).unwrap_err();
        assert_eq!(err.errno(), Errno::READ_ONLY);
    }

    #[test]
    fn eventfs_path_maps_directories_to_lists_or_capabilities() {
        let root = JetStreamPath::parse("/").unwrap();
        assert!(matches!(
            plan_operation(FileIntent::ReadDir, &root).unwrap(),
            JetStreamAction::StaticDirectory { .. }
        ));

        let stream = JetStreamPath::parse("/streams/ORDERS").unwrap();
        assert_eq!(
            plan_operation(FileIntent::ReadDir, &stream).unwrap(),
            JetStreamAction::StaticDirectory {
                entries: vec![
                    StaticDirectoryEntry::directory("messages"),
                    StaticDirectoryEntry::directory("subjects"),
                ]
            }
        );

        let stream_subjects = JetStreamPath::parse("/streams/ORDERS/subjects").unwrap();
        assert_eq!(
            plan_operation(FileIntent::ReadDir, &stream_subjects).unwrap(),
            JetStreamAction::ListStreamSubjects {
                stream: "ORDERS".into(),
            }
        );

        let events = JetStreamPath::parse("/events").unwrap();
        assert_eq!(
            plan_operation(FileIntent::ReadDir, &events).unwrap(),
            JetStreamAction::ListEventLogs
        );

        let tasks = JetStreamPath::parse("/tasks").unwrap();
        assert_eq!(
            plan_operation(FileIntent::ReadDir, &tasks).unwrap(),
            JetStreamAction::ListKvPrefix {
                bucket: TASKS_BUCKET.into(),
                prefix: String::new(),
            }
        );

        let task_namespace = JetStreamPath::parse("/tasks/demo").unwrap();
        assert_eq!(
            plan_operation(FileIntent::ReadDir, &task_namespace).unwrap(),
            JetStreamAction::ListKvPrefix {
                bucket: TASKS_BUCKET.into(),
                prefix: "demo".into(),
            }
        );

        let agents = JetStreamPath::parse("/agents").unwrap();
        assert_eq!(
            plan_operation(FileIntent::ReadDir, &agents).unwrap(),
            JetStreamAction::ListAgents
        );

        let agent_tasks = JetStreamPath::parse("/agents/bot/tasks").unwrap();
        assert_eq!(
            plan_operation(FileIntent::ReadDir, &agent_tasks).unwrap(),
            JetStreamAction::ListKvPrefix {
                bucket: AGENTS_BUCKET.into(),
                prefix: "bot/tasks".into(),
            }
        );

        let semantic_tags = JetStreamPath::parse("/semantic/tags").unwrap();
        assert_eq!(
            plan_operation(FileIntent::ReadDir, &semantic_tags).unwrap(),
            JetStreamAction::ListKvPrefix {
                bucket: SEMANTIC_BUCKET.into(),
                prefix: "tags".into(),
            }
        );

        let semantic_prefix = JetStreamPath::parse("/semantic/tags/project").unwrap();
        assert_eq!(
            plan_operation(FileIntent::ReadDir, &semantic_prefix).unwrap(),
            JetStreamAction::ListKvPrefix {
                bucket: SEMANTIC_BUCKET.into(),
                prefix: "tags/project".into(),
            }
        );

        let agent_prefix = JetStreamPath::parse("/agents/bot/memory/facts").unwrap();
        assert_eq!(
            plan_operation(FileIntent::ReadDir, &agent_prefix).unwrap(),
            JetStreamAction::ListKvPrefix {
                bucket: AGENTS_BUCKET.into(),
                prefix: "bot/memory/facts".into(),
            }
        );
    }

    #[test]
    fn eventfs_path_rejects_cross_surface_rename() {
        let from = JetStreamPath::parse("/kv/a/file.json").unwrap();
        let to = JetStreamPath::parse("/objects/a/file.json").unwrap();
        let err = plan_operation(FileIntent::Rename { to }, &from).unwrap_err();
        assert_eq!(err.errno(), Errno::CROSS_DEVICE);
    }

    #[test]
    fn eventfs_path_plans_same_surface_rename() {
        let from = JetStreamPath::parse("/kv/a/tmp.json").unwrap();
        let to = JetStreamPath::parse("/kv/a/final.json").unwrap();
        assert_eq!(
            plan_operation(FileIntent::Rename { to }, &from).unwrap(),
            JetStreamAction::Rename {
                plan: RenamePlan::Kv {
                    from_bucket: "a".into(),
                    from_key: "tmp.json".into(),
                    to_bucket: "a".into(),
                    to_key: "final.json".into(),
                }
            }
        );

        let from = JetStreamPath::parse("/objects/assets/tmp/blob.txt").unwrap();
        let to = JetStreamPath::parse("/objects/assets/final/blob.txt").unwrap();
        assert_eq!(
            plan_operation(FileIntent::Rename { to }, &from).unwrap(),
            JetStreamAction::Rename {
                plan: RenamePlan::Object {
                    from_bucket: "assets".into(),
                    from_object: "tmp/blob.txt".into(),
                    to_bucket: "assets".into(),
                    to_object: "final/blob.txt".into(),
                }
            }
        );

        let from = JetStreamPath::parse("/tasks/demo/tmp.json").unwrap();
        let to = JetStreamPath::parse("/tasks/demo/final.json").unwrap();
        assert_eq!(
            plan_operation(FileIntent::Rename { to }, &from).unwrap(),
            JetStreamAction::Rename {
                plan: RenamePlan::Kv {
                    from_bucket: TASKS_BUCKET.into(),
                    from_key: "demo/tmp.json".into(),
                    to_bucket: TASKS_BUCKET.into(),
                    to_key: "demo/final.json".into(),
                }
            }
        );
    }

    #[test]
    fn eventfs_path_rejects_cross_bucket_or_cross_materialized_rename() {
        let from = JetStreamPath::parse("/kv/a/file.json").unwrap();
        let to = JetStreamPath::parse("/kv/b/file.json").unwrap();
        let err = plan_operation(FileIntent::Rename { to }, &from).unwrap_err();
        assert_eq!(err.errno(), Errno::CROSS_DEVICE);

        let from = JetStreamPath::parse("/semantic/tags/a.json").unwrap();
        let to = JetStreamPath::parse("/semantic/summaries/a.json").unwrap();
        let err = plan_operation(FileIntent::Rename { to }, &from).unwrap_err();
        assert_eq!(err.errno(), Errno::CROSS_DEVICE);

        let from = JetStreamPath::parse("/agents/bot/tasks/a.json").unwrap();
        let to = JetStreamPath::parse("/agents/bot/memory/a.json").unwrap();
        let err = plan_operation(FileIntent::Rename { to }, &from).unwrap_err();
        assert_eq!(err.errno(), Errno::CROSS_DEVICE);
    }

    #[test]
    fn eventfs_path_plans_dynamic_mkdir_as_prefix_validation() {
        let kv = JetStreamPath::parse("/kv/bucket/path").unwrap();
        assert_eq!(
            plan_operation(FileIntent::Mkdir, &kv).unwrap(),
            JetStreamAction::EnsureKvDirectory {
                bucket: "bucket".into(),
                prefix: "path".into(),
            }
        );

        let object = JetStreamPath::parse("/objects/assets/images").unwrap();
        assert_eq!(
            plan_operation(FileIntent::Mkdir, &object).unwrap(),
            JetStreamAction::EnsureObjectDirectory {
                bucket: "assets".into(),
                prefix: "images".into(),
            }
        );
    }

    #[test]
    fn eventfs_path_validates_json_surfaces() {
        crate::validate_json_document("/tasks/ns/a.json", br#"{"ok":true}"#).unwrap();
        crate::validate_json_lines(
            "/events/system.jsonl",
            br#"{"ok":true}
{"ok":false}"#,
        )
        .unwrap();
        assert!(crate::validate_json_document("/tasks/ns/a.json", b"not-json").is_err());
        assert!(crate::validate_json_lines("/events/system.jsonl", b"{bad}\n").is_err());
    }
}
