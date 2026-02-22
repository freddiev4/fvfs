pub mod local;
pub mod nas;
pub mod s3;

use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

use crate::error::Result;
use crate::types::{FileEntry, Tier, FvfsPath};

/// Central storage abstraction. All tier implementations satisfy this trait.
/// The routing layer operates against the trait, never concrete types.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Write `data` at `path`.
    async fn put(&self, path: &FvfsPath, data: Bytes) -> Result<()>;

    /// Read the full content at `path`.
    async fn get(&self, path: &FvfsPath) -> Result<Bytes>;

    /// Delete the object at `path`.
    async fn delete(&self, path: &FvfsPath) -> Result<()>;

    /// Check whether `path` exists.
    async fn exists(&self, path: &FvfsPath) -> Result<bool>;

    /// List all entries whose path starts with `prefix`.
    async fn list(&self, prefix: &FvfsPath) -> Result<Vec<FileEntry>>;

    /// Stat a single entry.
    async fn metadata(&self, path: &FvfsPath) -> Result<FileEntry>;

    /// Which tier this backend represents.
    fn tier(&self) -> Tier;
}

/// Type-erased storage backend.
pub type ArcBackend = Arc<dyn StorageBackend>;
