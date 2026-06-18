use eventfs_protocol::{JetStreamPath, MountPath};
use eventfs_transport::{
    DirectoryEntry, EntryKind, PendingWriteOverlay, PendingWritePayload, VersionStamp,
};
#[cfg(test)]
use std::time::{Duration, UNIX_EPOCH};

use super::{max_version_stamp, FileSnapshot, QueuedJsonlEntry};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct QueuedOverlayChange {
    pub deleted_paths: Vec<MountPath>,
    pub visible_paths: Vec<MountPath>,
}

pub(super) struct QueuedOverlayProjection {
    facts: Vec<PendingWriteOverlay>,
}

impl QueuedOverlayProjection {
    pub fn new(facts: Vec<PendingWriteOverlay>) -> Self {
        Self { facts }
    }

    pub fn whole_value_snapshot(&self, path: &MountPath) -> Option<FileSnapshot> {
        for fact in self.facts.iter().rev() {
            if fact.deleted_paths.iter().any(|candidate| candidate == path) {
                return None;
            }
            if !fact.visible_paths.iter().any(|candidate| candidate == path) {
                continue;
            }
            if let PendingWritePayload::WholeValue(bytes) = &fact.payload {
                return Some(FileSnapshot {
                    bytes: bytes.clone(),
                    version: fact.version,
                });
            }
        }
        None
    }

    pub fn jsonl_entries(&self, path: &MountPath) -> Vec<QueuedJsonlEntry> {
        self.facts
            .iter()
            .filter_map(|fact| {
                if !fact.visible_paths.iter().any(|candidate| candidate == path) {
                    return None;
                }
                let PendingWritePayload::JsonLines {
                    stream,
                    subject,
                    bytes,
                    applied_lines,
                } = &fact.payload
                else {
                    return None;
                };
                Some(QueuedJsonlEntry {
                    idempotency_key: fact.idempotency_key.clone(),
                    stream: stream.clone(),
                    subject: subject.clone(),
                    bytes: bytes.clone(),
                    applied_lines: *applied_lines,
                    version: fact.version,
                })
            })
            .collect()
    }

    pub fn has_jsonl_entry(&self, path: &MountPath) -> bool {
        self.facts.iter().any(|fact| {
            matches!(fact.payload, PendingWritePayload::JsonLines { .. })
                && fact.visible_paths.iter().any(|candidate| candidate == path)
        })
    }

    pub fn overlay_version(&self, path: &MountPath) -> Option<VersionStamp> {
        let mut newest = None;
        for fact in &self.facts {
            if fact
                .visible_paths
                .iter()
                .any(|candidate| candidate.is_self_or_descendant_of(path))
            {
                newest = Some(max_version_stamp(newest, fact.version));
            }
        }
        newest
    }

    pub fn latest_delete_covers(&self, path: &MountPath) -> bool {
        for fact in self.facts.iter().rev() {
            if fact.deleted_paths.iter().any(|candidate| candidate == path) {
                return true;
            }
            if fact.visible_paths.iter().any(|candidate| candidate == path) {
                return false;
            }
        }
        false
    }

    pub fn directory_entries(&self, path: &MountPath) -> Vec<DirectoryEntry> {
        let mut entries = Vec::new();
        for fact in &self.facts {
            for queued_path in &fact.visible_paths {
                if let Some(child) = queued_directory_child(path, queued_path) {
                    insert_directory_entry(&mut entries, child);
                }
            }
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        entries
    }

    pub fn changes(&self) -> Vec<QueuedOverlayChange> {
        self.facts
            .iter()
            .map(|fact| QueuedOverlayChange {
                deleted_paths: fact.deleted_paths.clone(),
                visible_paths: fact.visible_paths.clone(),
            })
            .collect()
    }

    pub fn has_deleted_descendant(&self, path: &MountPath) -> bool {
        self.facts
            .iter()
            .flat_map(|fact| fact.deleted_paths.iter())
            .any(|deleted_path| deleted_path.is_descendant_of(path))
    }

    pub fn exact_delete_reveals_synthetic_directory(
        parsed: &JetStreamPath,
        has_known_visible_entries: bool,
    ) -> bool {
        queued_delete_can_empty_synthetic_directory(parsed) && has_known_visible_entries
    }

    pub fn deleted_descendant_empties_synthetic_directory(
        parsed: &JetStreamPath,
        exact_dynamic_file_exists: bool,
        has_visible_entries: bool,
    ) -> bool {
        queued_delete_can_empty_synthetic_directory(parsed)
            && !exact_dynamic_file_exists
            && !has_visible_entries
    }

    pub fn delete_can_elide_directory_child(path: &MountPath) -> bool {
        JetStreamPath::parse(path.as_str())
            .map(|parsed| queued_delete_can_empty_synthetic_directory(&parsed))
            .unwrap_or(false)
    }

    pub fn directory_child(dir_path: &MountPath, file_path: &MountPath) -> Option<DirectoryEntry> {
        queued_directory_child(dir_path, file_path)
    }
}

fn insert_directory_entry(entries: &mut Vec<DirectoryEntry>, entry: DirectoryEntry) {
    if let Some(existing) = entries
        .iter_mut()
        .find(|candidate| candidate.name == entry.name)
    {
        if entry.kind == EntryKind::File {
            existing.kind = EntryKind::File;
        }
        return;
    }
    entries.push(entry);
}

fn queued_delete_can_empty_synthetic_directory(parsed: &JetStreamPath) -> bool {
    matches!(
        parsed,
        JetStreamPath::KvKey { .. }
            | JetStreamPath::Object { .. }
            | JetStreamPath::TaskNamespace { .. }
            | JetStreamPath::AgentRecord { .. }
            | JetStreamPath::SemanticRecord { .. }
    )
}

fn queued_directory_child(dir_path: &MountPath, file_path: &MountPath) -> Option<DirectoryEntry> {
    if !file_path.is_descendant_of(dir_path) {
        return None;
    }
    let remainder = if dir_path.is_root() {
        file_path.as_str().strip_prefix('/')?
    } else {
        file_path
            .as_str()
            .strip_prefix(dir_path.as_str())?
            .strip_prefix('/')?
    };
    if remainder.is_empty() {
        return None;
    }
    let mut parts = remainder.split('/');
    let name = parts.next()?.to_string();
    let kind = if parts.next().is_some() {
        EntryKind::Directory
    } else {
        EntryKind::File
    };
    Some(DirectoryEntry { name, kind })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mp(path: &str) -> MountPath {
        MountPath::new(path).unwrap()
    }

    fn fact(
        id: &str,
        version: u64,
        visible_paths: Vec<MountPath>,
        deleted_paths: Vec<MountPath>,
        payload: PendingWritePayload,
    ) -> PendingWriteOverlay {
        PendingWriteOverlay {
            idempotency_key: id.into(),
            version: VersionStamp::at(UNIX_EPOCH + Duration::from_secs(version)),
            visible_paths,
            deleted_paths,
            payload,
        }
    }

    #[test]
    fn queued_overlay_whole_value_uses_latest_visible_fact_and_delete_coverage() {
        let overlay = QueuedOverlayProjection::new(vec![
            fact(
                "old",
                1,
                vec![mp("/kv/bucket/file.json")],
                Vec::new(),
                PendingWritePayload::WholeValue(b"old".to_vec()),
            ),
            fact(
                "delete",
                2,
                Vec::new(),
                vec![mp("/kv/bucket/file.json")],
                PendingWritePayload::WholeValue(Vec::new()),
            ),
        ]);

        assert!(overlay
            .whole_value_snapshot(&mp("/kv/bucket/file.json"))
            .is_none());
        assert!(overlay.latest_delete_covers(&mp("/kv/bucket/file.json")));
    }

    #[test]
    fn queued_overlay_exposes_jsonl_entries_without_durable_queue_shape() {
        let overlay = QueuedOverlayProjection::new(vec![fact(
            "jsonl",
            3,
            vec![
                mp("/events/system.jsonl"),
                mp("/streams/system/subjects/events.system.jsonl"),
            ],
            Vec::new(),
            PendingWritePayload::JsonLines {
                stream: "system".into(),
                subject: "events.system".into(),
                bytes: br#"{"line":1}
"#
                .to_vec(),
                applied_lines: 0,
            },
        )]);

        let entries = overlay.jsonl_entries(&mp("/events/system.jsonl"));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].idempotency_key, "jsonl");
        assert!(overlay.has_jsonl_entry(&mp("/streams/system/subjects/events.system.jsonl")));
    }

    #[test]
    fn queued_overlay_derives_directory_children_and_versions_from_mount_paths() {
        let overlay = QueuedOverlayProjection::new(vec![fact(
            "nested",
            4,
            vec![mp("/kv/bucket/dir/file.json")],
            Vec::new(),
            PendingWritePayload::WholeValue(b"{}".to_vec()),
        )]);

        assert_eq!(
            overlay.directory_entries(&mp("/kv/bucket")),
            vec![DirectoryEntry {
                name: "dir".into(),
                kind: EntryKind::Directory,
            }]
        );
        assert_eq!(
            overlay.overlay_version(&mp("/kv/bucket")).unwrap(),
            VersionStamp::at(UNIX_EPOCH + Duration::from_secs(4))
        );
    }

    #[test]
    fn queued_overlay_owns_synthetic_directory_delete_policy() {
        let parsed = JetStreamPath::parse("/kv/bucket/dir").unwrap();

        assert!(QueuedOverlayProjection::exact_delete_reveals_synthetic_directory(&parsed, true));
        assert!(
            QueuedOverlayProjection::deleted_descendant_empties_synthetic_directory(
                &parsed, false, false
            )
        );
        assert!(QueuedOverlayProjection::delete_can_elide_directory_child(
            &mp("/kv/bucket/dir")
        ));
    }
}
