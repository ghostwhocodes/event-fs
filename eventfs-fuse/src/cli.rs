use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "eventfs-fuse")]
#[command(about = "Mount NATS JetStream as an event-native filesystem")]
pub struct Cli {
    pub mountpoint: PathBuf,
    #[arg(default_value = "nats://127.0.0.1:4222")]
    pub nats_url: String,
    #[arg(long, env = "NATS_CREDS_FILE")]
    pub nats_creds_file: Option<String>,
    #[arg(long, default_value = "eventfs")]
    pub mount_name: String,
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 2_000)]
    pub timeout_ms: u64,
    #[arg(long, default_value_t = 86_400_000)]
    pub duplicate_window_ms: u64,
    #[arg(long, default_value_t = 1024)]
    pub queue_capacity: usize,
    #[arg(long)]
    pub foreground: bool,
}

impl Cli {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    pub fn stream_duplicate_window(&self) -> Duration {
        Duration::from_millis(self.duplicate_window_ms)
    }
}
