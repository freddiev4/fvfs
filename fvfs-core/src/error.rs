use thiserror::Error;

#[derive(Debug, Error)]
pub enum FvfsError {
    #[error("file not found: {path}")]
    NotFound { path: String },

    #[error("path already exists: {path}")]
    AlreadyExists { path: String },

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("storage backend error ({tier}): {source}")]
    Backend {
        tier: String,
        #[source]
        source: anyhow::Error,
    },

    #[error("metadata store error: {0}")]
    Metadata(#[from] rusqlite::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("S3 error: {0}")]
    S3(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("not a directory: {path}")]
    NotADirectory { path: String },

    #[error("is a directory: {path}")]
    IsADirectory { path: String },

    #[error("directory not empty: {path}")]
    DirectoryNotEmpty { path: String },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, FvfsError>;
