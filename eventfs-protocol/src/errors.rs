use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Errno(i32);

impl Errno {
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i32 {
        self.0
    }

    pub const NOT_FOUND: Self = Self(libc::ENOENT);
    pub const NOT_SUPPORTED: Self = Self(libc::ENOTSUP);
    pub const READ_ONLY: Self = Self(libc::EROFS);
    pub const INVALID_INPUT: Self = Self(libc::EINVAL);
    pub const IS_DIRECTORY: Self = Self(libc::EISDIR);
    pub const NOT_DIRECTORY: Self = Self(libc::ENOTDIR);
    pub const EXISTS: Self = Self(libc::EEXIST);
    pub const CROSS_DEVICE: Self = Self(libc::EXDEV);
    pub const IO: Self = Self(libc::EIO);
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EventFsError {
    #[error("{message}")]
    InvalidPath { message: String, errno: Errno },
    #[error("{operation} is unsupported for {path}")]
    Unsupported {
        operation: &'static str,
        path: String,
        errno: Errno,
    },
    #[error("{path} is read only")]
    ReadOnly { path: String, errno: Errno },
    #[error("invalid json at {path}: {message}")]
    InvalidJson {
        path: String,
        message: String,
        errno: Errno,
    },
}

impl EventFsError {
    pub fn invalid_path(message: impl Into<String>) -> Self {
        Self::InvalidPath {
            message: message.into(),
            errno: Errno::INVALID_INPUT,
        }
    }

    pub fn unsupported(operation: &'static str, path: impl Into<String>) -> Self {
        Self::Unsupported {
            operation,
            path: path.into(),
            errno: Errno::NOT_SUPPORTED,
        }
    }

    pub fn read_only(path: impl Into<String>) -> Self {
        Self::ReadOnly {
            path: path.into(),
            errno: Errno::READ_ONLY,
        }
    }

    pub fn invalid_json(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self::InvalidJson {
            path: path.into(),
            message: message.into(),
            errno: Errno::INVALID_INPUT,
        }
    }

    pub fn errno(&self) -> Errno {
        match self {
            Self::InvalidPath { errno, .. }
            | Self::Unsupported { errno, .. }
            | Self::ReadOnly { errno, .. }
            | Self::InvalidJson { errno, .. } => *errno,
        }
    }
}
