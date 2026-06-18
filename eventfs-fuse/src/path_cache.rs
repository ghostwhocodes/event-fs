use std::collections::HashMap;
use std::ffi::OsStr;

use eventfs_protocol::MountPath;
use fuser::FileType;

pub const ROOT_INO: u64 = 1;

#[derive(Default)]
pub struct PathCache {
    by_path: HashMap<MountPath, (u64, FileType)>,
    by_ino: HashMap<u64, MountPath>,
    next_ino: u64,
}

impl PathCache {
    pub fn new() -> Self {
        let mut cache = Self {
            by_path: HashMap::new(),
            by_ino: HashMap::new(),
            next_ino: ROOT_INO + 1,
        };
        cache.remember(MountPath::root(), ROOT_INO, FileType::Directory);
        cache
    }

    pub fn path(&self, ino: u64) -> Option<&MountPath> {
        self.by_ino.get(&ino)
    }

    pub fn entry(&self, path: &MountPath) -> Option<(u64, FileType)> {
        self.by_path.get(path).copied()
    }

    pub fn ensure(&mut self, path: &MountPath, kind: FileType) -> u64 {
        if let Some((ino, _)) = self.by_path.get(path).copied() {
            self.by_path.insert(path.clone(), (ino, kind));
            return ino;
        }
        let ino = self.next_ino;
        self.next_ino += 1;
        self.remember(path.clone(), ino, kind);
        ino
    }

    pub fn remove(&mut self, path: &MountPath) {
        if let Some((ino, _)) = self.by_path.remove(path) {
            self.by_ino.remove(&ino);
        }
    }

    pub fn remove_recursive(&mut self, path: &MountPath) {
        if path.is_root() {
            let root = self.by_path.get(&MountPath::root()).copied();
            self.by_path.clear();
            self.by_ino.clear();
            if let Some((ino, kind)) = root {
                self.remember(MountPath::root(), ino, kind);
            }
            return;
        }

        let removed: Vec<_> = self
            .by_path
            .iter()
            .filter_map(|(candidate, (ino, _))| {
                candidate.is_self_or_descendant_of(path).then_some(*ino)
            })
            .collect();
        self.by_path
            .retain(|candidate, _| !candidate.is_self_or_descendant_of(path));
        for ino in removed {
            self.by_ino.remove(&ino);
        }
    }

    pub fn remove_descendants(&mut self, path: &MountPath) {
        let removed: Vec<_> = self
            .by_path
            .iter()
            .filter_map(|(candidate, (ino, _))| {
                (candidate != &MountPath::root() && candidate.is_descendant_of(path))
                    .then_some(*ino)
            })
            .collect();
        self.by_path.retain(|candidate, _| {
            candidate == &MountPath::root() || !candidate.is_descendant_of(path)
        });
        for ino in removed {
            self.by_ino.remove(&ino);
        }
    }

    pub fn child_path(&self, parent: u64, name: &OsStr) -> Option<MountPath> {
        let parent = self.path(parent)?;
        parent.join_child(name.to_string_lossy()).ok()
    }

    pub fn snapshot(&self) -> Vec<(MountPath, u64, FileType)> {
        let mut entries: Vec<_> = self
            .by_path
            .iter()
            .map(|(path, (ino, kind))| (path.clone(), *ino, *kind))
            .collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    }

    pub fn diagnostic_snapshot(&self) -> Vec<(String, u64, FileType)> {
        self.snapshot()
            .into_iter()
            .map(|(path, ino, kind)| (path.into_string(), ino, kind))
            .collect()
    }

    fn remember(&mut self, path: MountPath, ino: u64, kind: FileType) {
        self.by_path.insert(path.clone(), (ino, kind));
        self.by_ino.insert(ino, path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mp(path: &str) -> MountPath {
        MountPath::new(path).unwrap()
    }

    #[test]
    fn path_cache_reuses_inodes_and_updates_kind() {
        let mut cache = PathCache::new();
        let ino = cache.ensure(&mp("/kv/demo"), FileType::Directory);
        let again = cache.ensure(&mp("kv/demo"), FileType::RegularFile);
        assert_eq!(ino, again);
        assert_eq!(
            cache.entry(&mp("/kv/demo")).unwrap().1,
            FileType::RegularFile
        );
    }

    #[test]
    fn path_cache_builds_child_paths_from_root_and_nested_dirs() {
        let mut cache = PathCache::new();
        let parent = cache.ensure(&mp("/kv/demo"), FileType::Directory);
        assert_eq!(
            cache.child_path(ROOT_INO, OsStr::new("kv")).unwrap(),
            mp("/kv")
        );
        assert_eq!(
            cache.child_path(parent, OsStr::new("file.json")).unwrap(),
            mp("/kv/demo/file.json")
        );
    }

    #[test]
    fn path_cache_removes_descendants_for_watch_invalidation() {
        let mut cache = PathCache::new();
        let parent = cache.ensure(&mp("/kv/demo/dir"), FileType::Directory);
        let child = cache.ensure(&mp("/kv/demo/dir/file.json"), FileType::RegularFile);
        let sibling = cache.ensure(&mp("/kv/demo/other.json"), FileType::RegularFile);

        cache.remove_recursive(&mp("/kv/demo/dir"));

        assert!(cache.entry(&mp("/kv/demo/dir")).is_none());
        assert!(cache.path(parent).is_none());
        assert!(cache.entry(&mp("/kv/demo/dir/file.json")).is_none());
        assert!(cache.path(child).is_none());
        assert!(cache.entry(&mp("/kv/demo/other.json")).is_some());
        assert_eq!(cache.path(sibling), Some(&mp("/kv/demo/other.json")));
    }

    #[test]
    fn path_cache_can_preserve_invalidated_parent_inode() {
        let mut cache = PathCache::new();
        let parent = cache.ensure(&mp("/kv/demo/dir"), FileType::Directory);
        let child = cache.ensure(&mp("/kv/demo/dir/file.json"), FileType::RegularFile);

        cache.remove_descendants(&mp("/kv/demo/dir"));

        assert!(cache.entry(&mp("/kv/demo/dir")).is_some());
        assert_eq!(cache.path(parent), Some(&mp("/kv/demo/dir")));
        assert!(cache.entry(&mp("/kv/demo/dir/file.json")).is_none());
        assert!(cache.path(child).is_none());
    }
}
