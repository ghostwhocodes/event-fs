use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("NATS IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid transport request: {0}")]
    Invalid(String),
    #[error("writeback queue is full")]
    QueueFull,
    #[error("writeback queue is already open: {}", .0.display())]
    QueueLocked(PathBuf),
    #[error("entry not found")]
    NotFound,
}

pub type TransportResult<T> = Result<T, TransportError>;
