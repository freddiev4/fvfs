/// TierRouter — resolves read/write/delete operations across the tier stack.
///
/// Write path:
///   1. Write to local disk (sync)
///   2. Upsert metadata in SQLite
///   3. Enqueue WAL entries for NAS replication and S3 upload
///
/// Read path:
///   1. Look up metadata → get tier bitmask
///   2. Serve from fastest available tier
///   3. Async-promote to local if served from NAS or S3
///   4. Update access stats
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, instrument, warn};

use fvfs_core::backend::StorageBackend;
use fvfs_core::metadata::MetadataStore;
use fvfs_core::{
    now_unix, EntryKind, FileEntry, FileMetadata, Result, Tier, TierBitmask, FvfsError, FvfsPath,
    WalOp,
};

/// Signals sent to the background promotion task.
pub enum PromoteSignal {
    Promote { path: FvfsPath, from: Tier },
}

pub struct TierRouter {
    pub local: Arc<dyn StorageBackend>,
    pub nas: Arc<dyn StorageBackend>,
    pub s3: Arc<dyn StorageBackend>,
    pub meta: MetadataStore,
    pub promote_tx: mpsc::UnboundedSender<PromoteSignal>,
}

impl TierRouter {
    // -----------------------------------------------------------------------
    // Writes

    /// Write `data` to the FVFS at `path`. Completes once the local disk write
    /// and SQLite upsert are done; NAS and S3 replications are enqueued.
    #[instrument(skip(self, data), fields(path = %path, bytes = data.len()))]
    pub async fn write(&self, path: &FvfsPath, data: Bytes) -> Result<()> {
        // Compute sha256
        let sha256 = {
            let mut h = Sha256::new();
            h.update(&data);
            hex::encode(h.finalize())
        };
        let size_bytes = data.len() as u64;

        // 1. Write to local disk
        self.local.put(path, data).await?;

        // 2. Upsert metadata
        let now = now_unix();
        let mut meta = FileMetadata {
            id: 0,
            path: path.clone(),
            kind: EntryKind::File,
            size_bytes,
            sha256,
            tier_bitmask: {
                let mut bm = TierBitmask::NONE;
                bm.set(Tier::Local);
                bm
            },
            created_at: now,
            modified_at: now,
            accessed_at: now,
            access_count_30d: 0,
            mime_type: None,
        };

        let meta_store = self.meta.clone();
        let meta_clone = meta.clone();
        let file_id = tokio::task::spawn_blocking(move || meta_store.upsert(&meta_clone))
            .await
            .map_err(|e| FvfsError::Other(anyhow::anyhow!("spawn_blocking: {e}")))??;
        meta.id = file_id;

        // Ensure parent directories exist in metadata
        self.ensure_parent_dirs(path).await?;

        // 3. Enqueue WAL entries
        let meta_store2 = self.meta.clone();
        tokio::task::spawn_blocking(move || {
            meta_store2.wal_enqueue(file_id, &WalOp::ReplicateNas)?;
            meta_store2.wal_enqueue(file_id, &WalOp::UploadS3)?;
            Ok::<_, FvfsError>(())
        })
        .await
        .map_err(|e| FvfsError::Other(anyhow::anyhow!("spawn_blocking: {e}")))??;

        info!(path = %path, file_id = file_id, "write complete, WAL enqueued");
        Ok(())
    }

    /// Create a directory entry.
    pub async fn mkdir(&self, path: &FvfsPath) -> Result<()> {
        let meta = FileMetadata::new_dir(path.clone());
        let meta_store = self.meta.clone();
        tokio::task::spawn_blocking(move || meta_store.upsert(&meta))
            .await
            .map_err(|e| FvfsError::Other(anyhow::anyhow!("spawn_blocking: {e}")))??;
        self.ensure_parent_dirs(path).await?;
        Ok(())
    }

    /// Ensure all ancestor directories are present in the metadata store.
    async fn ensure_parent_dirs(&self, path: &FvfsPath) -> Result<()> {
        let mut current = path.clone();
        loop {
            match current.parent() {
                None => break,
                Some(parent) => {
                    let meta_store = self.meta.clone();
                    let parent_clone = parent.clone();
                    let existing = tokio::task::spawn_blocking(move || meta_store.get(&parent_clone))
                        .await
                        .map_err(|e| FvfsError::Other(anyhow::anyhow!("{e}")))??;
                    if existing.is_none() {
                        let dir_meta = FileMetadata::new_dir(parent.clone());
                        let meta_store2 = self.meta.clone();
                        tokio::task::spawn_blocking(move || meta_store2.upsert(&dir_meta))
                            .await
                            .map_err(|e| FvfsError::Other(anyhow::anyhow!("{e}")))??;
                    }
                    current = parent;
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Reads

    /// Read `path`, serving from the fastest available tier.
    #[instrument(skip(self), fields(path = %path))]
    pub async fn read(&self, path: &FvfsPath) -> Result<Bytes> {
        let meta = self.require_metadata(path).await?;

        if meta.is_dir() {
            return Err(FvfsError::IsADirectory {
                path: path.to_string(),
            });
        }

        let bm = meta.tier_bitmask;
        let file_id = meta.id;

        // Try tiers from fastest to slowest.
        let data = if bm.has(Tier::Local) {
            match self.local.get(path).await {
                Ok(d) => d,
                Err(e) => {
                    warn!(path=%path, err=%e, "local read failed, trying NAS");
                    self.read_from_nas_or_s3(path, &bm).await?
                }
            }
        } else if bm.has(Tier::Nas) {
            let data = self.nas.get(path).await?;
            // Promote to local asynchronously
            self.promote_tx
                .send(PromoteSignal::Promote {
                    path: path.clone(),
                    from: Tier::Nas,
                })
                .ok();
            data
        } else if bm.has(Tier::S3) {
            let data = self.s3.get(path).await?;
            self.promote_tx
                .send(PromoteSignal::Promote {
                    path: path.clone(),
                    from: Tier::S3,
                })
                .ok();
            data
        } else {
            return Err(FvfsError::NotFound {
                path: path.to_string(),
            });
        };

        // Update access stats
        let meta_store = self.meta.clone();
        tokio::task::spawn_blocking(move || meta_store.record_access(file_id))
            .await
            .ok();

        Ok(data)
    }

    async fn read_from_nas_or_s3(&self, path: &FvfsPath, bm: &TierBitmask) -> Result<Bytes> {
        if bm.has(Tier::Nas) {
            let data = self.nas.get(path).await?;
            self.promote_tx
                .send(PromoteSignal::Promote {
                    path: path.clone(),
                    from: Tier::Nas,
                })
                .ok();
            Ok(data)
        } else if bm.has(Tier::S3) {
            let data = self.s3.get(path).await?;
            self.promote_tx
                .send(PromoteSignal::Promote {
                    path: path.clone(),
                    from: Tier::S3,
                })
                .ok();
            Ok(data)
        } else {
            Err(FvfsError::NotFound {
                path: path.to_string(),
            })
        }
    }

    // -----------------------------------------------------------------------
    // Delete

    #[instrument(skip(self), fields(path = %path))]
    pub async fn delete(&self, path: &FvfsPath) -> Result<()> {
        let meta = self.require_metadata(path).await?;
        let bm = meta.tier_bitmask;

        // Delete from all present tiers concurrently.
        let mut tasks: Vec<tokio::task::JoinHandle<Result<()>>> = vec![];

        if bm.has(Tier::Local) {
            let local = self.local.clone();
            let p = path.clone();
            tasks.push(tokio::spawn(async move { local.delete(&p).await }));
        }
        if bm.has(Tier::Nas) {
            let nas = self.nas.clone();
            let p = path.clone();
            tasks.push(tokio::spawn(async move { nas.delete(&p).await }));
        }
        if bm.has(Tier::S3) {
            let s3 = self.s3.clone();
            let p = path.clone();
            tasks.push(tokio::spawn(async move { s3.delete(&p).await }));
        }

        for task in tasks {
            task.await
                .map_err(|e| FvfsError::Other(anyhow::anyhow!("{e}")))?
                .ok(); // best-effort
        }

        // Remove from metadata
        let meta_store = self.meta.clone();
        let path_clone = path.clone();
        tokio::task::spawn_blocking(move || meta_store.delete(&path_clone))
            .await
            .map_err(|e| FvfsError::Other(anyhow::anyhow!("{e}")))??;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Metadata / listing

    pub async fn stat(&self, path: &FvfsPath) -> Result<FileMetadata> {
        self.require_metadata(path).await
    }

    pub async fn list(&self, dir_path: &FvfsPath) -> Result<Vec<FileEntry>> {
        let meta_store = self.meta.clone();
        let dir_clone = dir_path.clone();
        let entries = tokio::task::spawn_blocking(move || meta_store.list_dir(&dir_clone))
            .await
            .map_err(|e| FvfsError::Other(anyhow::anyhow!("{e}")))??;
        Ok(entries.iter().map(FileEntry::from).collect())
    }

    async fn require_metadata(&self, path: &FvfsPath) -> Result<FileMetadata> {
        let meta_store = self.meta.clone();
        let path_clone = path.clone();
        let meta = tokio::task::spawn_blocking(move || meta_store.get(&path_clone))
            .await
            .map_err(|e| FvfsError::Other(anyhow::anyhow!("{e}")))??;
        meta.ok_or_else(|| FvfsError::NotFound {
            path: path.to_string(),
        })
    }

    // -----------------------------------------------------------------------
    // Eviction helpers

    /// Evict a file from a tier (remove local copy, keep metadata).
    pub async fn evict_from_tier(&self, path: &FvfsPath, tier: Tier) -> Result<()> {
        let meta = self.require_metadata(path).await?;
        if !meta.tier_bitmask.safe_to_evict_from(tier) {
            return Err(FvfsError::Other(anyhow::anyhow!(
                "cannot evict: file not present on colder tier"
            )));
        }

        let backend: Arc<dyn StorageBackend> = match tier {
            Tier::Local => self.local.clone(),
            Tier::Nas => self.nas.clone(),
            Tier::S3 => {
                return Err(FvfsError::Other(anyhow::anyhow!(
                    "S3 tier is never evicted"
                )));
            }
        };
        backend.delete(path).await?;

        // Update bitmask
        let mut bm = meta.tier_bitmask;
        bm.clear(tier);
        let meta_store = self.meta.clone();
        let file_id = meta.id;
        tokio::task::spawn_blocking(move || meta_store.set_tier_bitmask(file_id, bm))
            .await
            .map_err(|e| FvfsError::Other(anyhow::anyhow!("{e}")))??;

        Ok(())
    }
}
