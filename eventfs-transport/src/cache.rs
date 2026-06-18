use std::collections::HashMap;
use std::time::SystemTime;

use eventfs_protocol::{AffectedPathReason, MountPath};
use serde::{Deserialize, Serialize};

use crate::invalidation::{path_matches_action, InvalidationPlan, InvalidationScope};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CacheOrigin {
    KvRevision {
        bucket: String,
        key: String,
        revision: u64,
    },
    StreamSequence {
        stream: String,
        sequence: u64,
    },
    ObjectVersion {
        bucket: String,
        object: String,
    },
    MaterializedView {
        path: String,
    },
    LocalDiagnostic,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VersionStamp {
    pub created: SystemTime,
    pub modified: SystemTime,
}

impl VersionStamp {
    pub fn at(time: SystemTime) -> Self {
        Self {
            created: time,
            modified: time,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheEntry {
    pub path: String,
    pub origin: CacheOrigin,
    pub version: VersionStamp,
    pub bytes: Vec<u8>,
    pub valid: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchEvent {
    InvalidatePath(MountPath),
    InvalidateAffectedPath {
        path: MountPath,
        reason: AffectedPathReason,
    },
    Refresh(CacheEntry),
    Gap,
}

impl WatchEvent {
    pub fn invalidate_path(path: MountPath) -> Self {
        Self::InvalidatePath(path)
    }

    pub fn invalidate_affected_path(path: MountPath, reason: AffectedPathReason) -> Self {
        Self::InvalidateAffectedPath { path, reason }
    }
}

#[derive(Default)]
pub struct LocalCache {
    entries: HashMap<String, CacheEntry>,
    generation: u64,
}

impl LocalCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, entry: CacheEntry) {
        self.entries.insert(entry.path.clone(), entry);
    }

    pub fn get(&self, path: &str) -> Option<&CacheEntry> {
        self.entries.get(path).filter(|entry| entry.valid)
    }

    pub fn invalidate(&mut self, path: &str) {
        self.entries.remove(path);
    }

    pub fn apply_invalidation(&mut self, plan: &InvalidationPlan) {
        for action in plan.actions() {
            match action.scope {
                InvalidationScope::All => {
                    self.generation = self.generation.saturating_add(1);
                    self.entries.clear();
                }
                InvalidationScope::ExactEntry | InvalidationScope::Subtree => {
                    self.entries.retain(|path, _| {
                        MountPath::new(path)
                            .map(|path| !path_matches_action(&path, action))
                            .unwrap_or(false)
                    });
                }
            }
        }
    }

    pub fn apply(&mut self, event: WatchEvent) {
        let plan = InvalidationPlan::for_watch_event(&event);
        self.apply_invalidation(&plan);
        if let WatchEvent::Refresh(entry) = event {
            self.insert(entry);
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn snapshot(&self) -> Vec<CacheEntry> {
        let mut entries: Vec<CacheEntry> = self.entries.values().cloned().collect();
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str) -> CacheEntry {
        CacheEntry {
            path: path.into(),
            origin: CacheOrigin::LocalDiagnostic,
            version: VersionStamp::at(SystemTime::UNIX_EPOCH),
            bytes: b"cached".to_vec(),
            valid: true,
        }
    }

    fn mp(path: &str) -> MountPath {
        MountPath::new(path).unwrap()
    }

    #[test]
    fn cache_invalidates_precise_paths() {
        let mut cache = LocalCache::new();
        cache.insert(entry("/kv/a.json"));
        cache.apply(WatchEvent::invalidate_path(mp("/kv/a.json")));
        assert!(cache.get("/kv/a.json").is_none());
        assert!(cache.snapshot().is_empty());
    }

    #[test]
    fn cache_gap_invalidates_all_entries_and_advances_generation() {
        let mut cache = LocalCache::new();
        cache.insert(entry("/a"));
        cache.insert(entry("/b"));
        cache.apply(WatchEvent::Gap);
        assert_eq!(cache.generation(), 1);
        assert!(cache.get("/a").is_none());
        assert!(cache.get("/b").is_none());
        assert!(cache.snapshot().is_empty());
    }

    #[test]
    fn invalidation_plan_cache_consumes_exact_subtree_and_gap_actions() {
        let mut cache = LocalCache::new();
        cache.insert(entry("/kv/bucket"));
        cache.insert(entry("/kv/bucket/file.json"));
        cache.insert(entry("/kv/other/file.json"));

        cache.apply_invalidation(&InvalidationPlan::exact_entry(mp("/kv/bucket")));

        assert!(cache.get("/kv/bucket").is_none());
        assert!(cache.get("/kv/bucket/file.json").is_some());

        cache.apply_invalidation(&InvalidationPlan::subtree(mp("/kv/bucket")));

        assert!(cache.get("/kv/bucket/file.json").is_none());
        assert!(cache.get("/kv/other/file.json").is_some());

        cache.apply_invalidation(&InvalidationPlan::gap());

        assert_eq!(cache.generation(), 1);
        assert!(cache.snapshot().is_empty());
    }
}
