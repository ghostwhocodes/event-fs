use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use eventfs_fuse::{Cli, JetStreamFuse};
use eventfs_transport::{NatsStorage, NatsStorageConfig, WritebackQueue};

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cache_dir = cli
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_cache_dir(&cli));
    let queue = WritebackQueue::open(&cache_dir, cli.queue_capacity).context("open queue")?;
    let backend = NatsStorage::connect(
        &cli.nats_url,
        cli.nats_creds_file.as_deref(),
        NatsStorageConfig {
            timeout: cli.timeout(),
            stream_duplicate_window: cli.stream_duplicate_window(),
            ..Default::default()
        },
    )
    .context("connect to JetStream")?;
    let fs = JetStreamFuse::new(Box::new(backend), queue, cli.mount_name);
    let options = vec![
        fuser::MountOption::FSName("eventfs".into()),
        fuser::MountOption::DefaultPermissions,
    ];
    fuser::mount2(fs, cli.mountpoint, &options).context("mount EventFS")
}

fn default_cache_dir(cli: &Cli) -> PathBuf {
    cache_dir_for_identity(&default_cache_root(), cli)
}

fn default_cache_root() -> PathBuf {
    let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    default_cache_root_from(
        nonempty_env_path("XDG_STATE_HOME"),
        nonempty_env_path("HOME"),
        current_dir,
    )
}

fn default_cache_root_from(
    xdg_state_home: Option<PathBuf>,
    home: Option<PathBuf>,
    current_dir: PathBuf,
) -> PathBuf {
    if let Some(path) = xdg_state_home {
        return path.join("eventfs");
    }
    if let Some(home) = home {
        return home.join(".local").join("state").join("eventfs");
    }
    current_dir.join(".eventfs-state")
}

fn nonempty_env_path(key: &str) -> Option<PathBuf> {
    std::env::var_os(key).and_then(|value| {
        if value.is_empty() {
            None
        } else {
            Some(PathBuf::from(value))
        }
    })
}

fn cache_dir_for_identity(root: &Path, cli: &Cli) -> PathBuf {
    let identity = format!(
        "mount_name={}\nnats_url={}\nmountpoint={}\ncreds={}",
        cli.mount_name,
        cli.nats_url,
        cli.mountpoint.display(),
        cli.nats_creds_file.as_deref().unwrap_or("")
    );
    let mount_name = sanitize_cache_segment(&cli.mount_name);
    root.join("mounts").join(format!(
        "{mount_name}-{:016x}",
        stable_hash(identity.as_bytes())
    ))
}

fn sanitize_cache_segment(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "mount".into()
    } else {
        sanitized
    }
}

fn stable_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001b3;
    bytes.iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(mountpoint: &str, nats_url: &str, mount_name: &str) -> Cli {
        Cli {
            mountpoint: mountpoint.into(),
            nats_url: nats_url.into(),
            nats_creds_file: None,
            mount_name: mount_name.into(),
            cache_dir: None,
            timeout_ms: 2_000,
            duplicate_window_ms: 86_400_000,
            queue_capacity: 1024,
            foreground: false,
        }
    }

    #[test]
    fn default_cache_dir_is_scoped_to_mount_identity_and_url() {
        let root = PathBuf::from("/var/lib/eventfs-test");
        let first =
            cache_dir_for_identity(&root, &cli("/mnt/a", "nats://127.0.0.1:4222", "eventfs"));
        let other_url =
            cache_dir_for_identity(&root, &cli("/mnt/a", "nats://127.0.0.1:4333", "eventfs"));
        let other_mount =
            cache_dir_for_identity(&root, &cli("/mnt/b", "nats://127.0.0.1:4222", "eventfs"));
        let other_name =
            cache_dir_for_identity(&root, &cli("/mnt/a", "nats://127.0.0.1:4222", "other"));

        assert_ne!(first, other_url);
        assert_ne!(first, other_mount);
        assert_ne!(first, other_name);
        assert!(first.starts_with(root.join("mounts")));
    }

    #[test]
    fn default_cache_root_prefers_persistent_state_locations() {
        assert_eq!(
            default_cache_root_from(
                Some(PathBuf::from("/state")),
                Some(PathBuf::from("/home/user")),
                PathBuf::from("/work")
            ),
            PathBuf::from("/state/eventfs")
        );
        assert_eq!(
            default_cache_root_from(
                None,
                Some(PathBuf::from("/home/user")),
                PathBuf::from("/work")
            ),
            PathBuf::from("/home/user/.local/state/eventfs")
        );
        assert_eq!(
            default_cache_root_from(None, None, PathBuf::from("/work")),
            PathBuf::from("/work/.eventfs-state")
        );
    }
}
