use eventfs_protocol::{
    invalidation_paths_for_mount_path, AffectedPath, AffectedPathReason, EventFsError,
    JetStreamPath, MountPath,
};

use crate::cache::WatchEvent;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvalidationScope {
    ExactEntry,
    Subtree,
    All,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidationAction {
    pub path: MountPath,
    pub scope: InvalidationScope,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InvalidationPlan {
    actions: Vec<InvalidationAction>,
}

impl InvalidationPlan {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn gap() -> Self {
        Self {
            actions: vec![InvalidationAction {
                path: MountPath::root(),
                scope: InvalidationScope::All,
            }],
        }
    }

    pub fn for_watch_event(event: &WatchEvent) -> Self {
        match event {
            WatchEvent::InvalidatePath(path) => Self::subtree(path.clone()),
            WatchEvent::InvalidateAffectedPath { path, reason } => {
                Self::for_affected_path(path.clone(), *reason)
            }
            WatchEvent::Refresh(entry) => MountPath::new(&entry.path)
                .map(Self::subtree)
                .unwrap_or_else(|_| Self::gap()),
            WatchEvent::Gap => Self::gap(),
        }
    }

    pub fn for_affected_path(path: MountPath, reason: AffectedPathReason) -> Self {
        match reason {
            AffectedPathReason::Ancestor => Self::exact_entry(path),
            AffectedPathReason::Exact | AffectedPathReason::Alias | AffectedPathReason::Mailbox => {
                Self::subtree(path)
            }
        }
    }

    pub fn for_local_mutation(path: &MountPath) -> Result<Self, EventFsError> {
        let parsed = JetStreamPath::parse(path.as_str())?;
        let affected = invalidation_paths_for_mount_path(&parsed)?;
        Ok(Self::from_affected_paths(affected))
    }

    pub fn from_affected_paths(paths: impl IntoIterator<Item = AffectedPath>) -> Self {
        let mut plan = Self::empty();
        for affected in paths {
            plan.extend(Self::for_affected_path(affected.path, affected.reason));
        }
        plan
    }

    pub fn exact_entry(path: MountPath) -> Self {
        Self {
            actions: vec![InvalidationAction {
                path,
                scope: InvalidationScope::ExactEntry,
            }],
        }
    }

    pub fn subtree(path: MountPath) -> Self {
        Self {
            actions: vec![InvalidationAction {
                path,
                scope: InvalidationScope::Subtree,
            }],
        }
    }

    pub fn actions(&self) -> &[InvalidationAction] {
        &self.actions
    }

    pub fn extend(&mut self, other: Self) {
        for action in other.actions {
            self.push(action);
        }
    }

    fn push(&mut self, action: InvalidationAction) {
        if !self.actions.iter().any(|candidate| candidate == &action) {
            self.actions.push(action);
        }
    }
}

pub fn path_matches_action(path: &MountPath, action: &InvalidationAction) -> bool {
    match action.scope {
        InvalidationScope::ExactEntry => path == &action.path,
        InvalidationScope::Subtree => path.is_self_or_descendant_of(&action.path),
        InvalidationScope::All => !path.is_root(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventfs_protocol::TASKS_BUCKET;

    fn mp(path: &str) -> MountPath {
        MountPath::new(path).unwrap()
    }

    #[test]
    fn invalidation_plan_maps_reasons_to_precise_scopes() {
        assert_eq!(
            InvalidationPlan::for_affected_path(mp("/kv/bucket"), AffectedPathReason::Ancestor)
                .actions(),
            &[InvalidationAction {
                path: mp("/kv/bucket"),
                scope: InvalidationScope::ExactEntry,
            }]
        );
        assert_eq!(
            InvalidationPlan::for_affected_path(mp("/tasks/demo"), AffectedPathReason::Alias)
                .actions(),
            &[InvalidationAction {
                path: mp("/tasks/demo"),
                scope: InvalidationScope::Subtree,
            }]
        );
    }

    #[test]
    fn invalidation_plan_local_mutation_includes_aliases_and_ancestors() {
        let path = mp(&format!("/kv/{TASKS_BUCKET}/demo/render.json"));
        let plan = InvalidationPlan::for_local_mutation(&path).unwrap();
        let actions = plan.actions();

        assert!(actions.contains(&InvalidationAction {
            path: path.clone(),
            scope: InvalidationScope::Subtree,
        }));
        assert!(actions.contains(&InvalidationAction {
            path: mp(&format!("/kv/{TASKS_BUCKET}/demo")),
            scope: InvalidationScope::ExactEntry,
        }));
        assert!(actions.contains(&InvalidationAction {
            path: mp("/tasks/demo/render.json"),
            scope: InvalidationScope::Subtree,
        }));
        assert!(actions.contains(&InvalidationAction {
            path: mp("/tasks/demo"),
            scope: InvalidationScope::ExactEntry,
        }));
    }

    #[test]
    fn invalidation_plan_matches_exact_subtree_and_gap_actions() {
        let exact = InvalidationAction {
            path: mp("/kv/bucket"),
            scope: InvalidationScope::ExactEntry,
        };
        assert!(path_matches_action(&mp("/kv/bucket"), &exact));
        assert!(!path_matches_action(&mp("/kv/bucket/file.json"), &exact));

        let subtree = InvalidationAction {
            path: mp("/kv/bucket"),
            scope: InvalidationScope::Subtree,
        };
        assert!(path_matches_action(&mp("/kv/bucket/file.json"), &subtree));

        let gap = InvalidationAction {
            path: MountPath::root(),
            scope: InvalidationScope::All,
        };
        assert!(path_matches_action(&mp("/kv/bucket/file.json"), &gap));
        assert!(!path_matches_action(&MountPath::root(), &gap));
    }
}
