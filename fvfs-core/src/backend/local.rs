use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, instrument};

use crate::backend::StorageBackend;
use crate::error::{Result, FvfsError};
use crate::types::{EntryKind, FileEntry, Tier, FvfsPath};

/// StorageBackend backed by a local filesystem path (used for both the Mac
/// mini hot tier and as a base for the NAS warm tier).
#[derive(Debug, Clone)]
pub struct LocalDiskBackend {
    root: PathBuf,
    tier: Tier,
}

impl LocalDiskBackend {
    pub fn new(root: impl Into<PathBuf>, tier: Tier) -> Self {
        LocalDiskBackend {
            root: root.into(),
            tier,
        }
    }

    /// Convert a FvfsPath to a concrete filesystem path under `root`.
    fn fs_path(&self, vfs_path: &FvfsPath) -> PathBuf {
        // Strip the leading '/' so it's relative, then join under root.
        let rel = vfs_path.as_str().trim_start_matches('/');
        self.root.join(rel)
    }
}

#[async_trait]
impl StorageBackend for LocalDiskBackend {
    #[instrument(skip(self, data), fields(tier = %self.tier, path = %path))]
    async fn put(&self, path: &FvfsPath, data: Bytes) -> Result<()> {
        let fs_path = self.fs_path(path);
        if let Some(parent) = fs_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut file = fs::File::create(&fs_path).await?;
        file.write_all(&data).await?;
        file.flush().await?;
        debug!(tier = %self.tier, path = %path, bytes = data.len(), "put");
        Ok(())
    }

    #[instrument(skip(self), fields(tier = %self.tier, path = %path))]
    async fn get(&self, path: &FvfsPath) -> Result<Bytes> {
        let fs_path = self.fs_path(path);
        let data = fs::read(&fs_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                FvfsError::NotFound {
                    path: path.to_string(),
                }
            } else {
                FvfsError::Io(e)
            }
        })?;
        debug!(tier = %self.tier, path = %path, bytes = data.len(), "get");
        Ok(Bytes::from(data))
    }

    #[instrument(skip(self), fields(tier = %self.tier, path = %path))]
    async fn delete(&self, path: &FvfsPath) -> Result<()> {
        let fs_path = self.fs_path(path);
        fs::remove_file(&fs_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                FvfsError::NotFound {
                    path: path.to_string(),
                }
            } else {
                FvfsError::Io(e)
            }
        })?;
        debug!(tier = %self.tier, path = %path, "delete");
        Ok(())
    }

    async fn exists(&self, path: &FvfsPath) -> Result<bool> {
        let fs_path = self.fs_path(path);
        Ok(fs_path.exists())
    }

    #[instrument(skip(self), fields(tier = %self.tier, prefix = %prefix))]
    async fn list(&self, prefix: &FvfsPath) -> Result<Vec<FileEntry>> {
        let dir_path = self.fs_path(prefix);
        if !dir_path.exists() {
            return Ok(vec![]);
        }
        let mut entries = Vec::new();
        collect_entries(&dir_path, &self.root, &mut entries).await?;
        Ok(entries)
    }

    #[instrument(skip(self), fields(tier = %self.tier, path = %path))]
    async fn metadata(&self, path: &FvfsPath) -> Result<FileEntry> {
        let fs_path = self.fs_path(path);
        let meta = fs::metadata(&fs_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                FvfsError::NotFound {
                    path: path.to_string(),
                }
            } else {
                FvfsError::Io(e)
            }
        })?;

        let kind = if meta.is_dir() {
            EntryKind::Directory
        } else {
            EntryKind::File
        };
        let modified_at = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        Ok(FileEntry {
            path: path.clone(),
            kind,
            size_bytes: meta.len(),
            modified_at,
            tier_bitmask: {
                let mut bm = crate::types::TierBitmask::default();
                bm.set(self.tier);
                bm
            },
        })
    }

    fn tier(&self) -> Tier {
        self.tier
    }
}

/// Recursively collect FileEntry values for all files under `dir`.
async fn collect_entries(
    dir: &Path,
    root: &Path,
    out: &mut Vec<FileEntry>,
) -> std::io::Result<()> {
    let mut read_dir = fs::read_dir(dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let meta = entry.metadata().await?;
        let fs_path = entry.path();
        // Convert back to a FvfsPath by stripping the root prefix.
        let rel = fs_path
            .strip_prefix(root)
            .unwrap_or(&fs_path)
            .to_string_lossy()
            .to_string();
        let vfs_path_str = format!("/{}", rel.replace('\\', "/"));
        let vfs_path = FvfsPath::new(vfs_path_str).unwrap_or_else(|_| {
            FvfsPath::new("/unknown").unwrap()
        });

        let kind = if meta.is_dir() {
            EntryKind::Directory
        } else {
            EntryKind::File
        };
        let modified_at = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        out.push(FileEntry {
            path: vfs_path,
            kind: kind.clone(),
            size_bytes: meta.len(),
            modified_at,
            tier_bitmask: crate::types::TierBitmask::default(),
        });

        if meta.is_dir() {
            // Recurse using Box::pin to handle the async recursion.
            Box::pin(collect_entries(&fs_path, root, out)).await?;
        }
    }
    Ok(())
}
