/// WAL replay — runs on daemon startup before serving any requests.
///
/// Any `wal_pending` entries left over from a previous crash are re-executed
/// with exponential backoff.
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use fvfs_core::metadata::MetadataStore;
use fvfs_core::{VfsError, WalOp};

use crate::router::TierRouter;

const MAX_ATTEMPTS: u32 = 10;
const BASE_DELAY_MS: u64 = 500;

pub async fn replay_wal(router: Arc<TierRouter>, meta: MetadataStore) {
    let entries = match tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.wal_pending()
    })
    .await
    {
        Ok(Ok(e)) => e,
        Ok(Err(e)) => {
            error!("Failed to load WAL entries: {e}");
            return;
        }
        Err(e) => {
            error!("spawn_blocking error: {e}");
            return;
        }
    };

    if entries.is_empty() {
        info!("WAL replay: no pending entries");
        return;
    }

    info!("WAL replay: {} pending entries", entries.len());

    for entry in entries {
        let mut delay = BASE_DELAY_MS;
        let mut attempt = entry.attempts;

        loop {
            if attempt >= MAX_ATTEMPTS {
                error!(
                    wal_id = entry.id,
                    file_id = entry.file_id,
                    op = ?entry.op,
                    "WAL entry exceeded max attempts, skipping"
                );
                break;
            }

            // Get file metadata to know the path.
            let file_meta = match tokio::task::spawn_blocking({
                let m = meta.clone();
                let fid = entry.file_id;
                move || m.get_by_id(fid)
            })
            .await
            {
                Ok(Ok(Some(m))) => m,
                Ok(Ok(None)) => {
                    // File was deleted; clean up the WAL entry.
                    warn!(wal_id = entry.id, "WAL entry references deleted file; removing");
                    let m = meta.clone();
                    let id = entry.id;
                    let _ = tokio::task::spawn_blocking(move || m.wal_complete(id)).await;
                    break;
                }
                Ok(Err(e)) => {
                    warn!(wal_id = entry.id, err = %e, "Failed to load file metadata");
                    break;
                }
                Err(e) => {
                    warn!(wal_id = entry.id, err = %e, "spawn_blocking error");
                    break;
                }
            };

            let result = match entry.op {
                WalOp::ReplicateNas => {
                    // Read from local, write to NAS.
                    async {
                        let data = router.local.get(&file_meta.path).await?;
                        router.nas.put(&file_meta.path, data).await?;
                        // Update bitmask
                        let mut bm = file_meta.tier_bitmask;
                        bm.set(fvfs_core::Tier::Nas);
                        let m = meta.clone();
                        let fid = file_meta.id;
                        tokio::task::spawn_blocking(move || m.set_tier_bitmask(fid, bm))
                            .await
                            .map_err(|e| VfsError::Other(anyhow::anyhow!("{e}")))?
                    }
                    .await
                }
                WalOp::UploadS3 => {
                    // Read from the fastest available tier, upload to S3.
                    async {
                        let data = router.read(&file_meta.path).await?;
                        router.s3.put(&file_meta.path, data).await?;
                        // Update bitmask
                        let mut bm = file_meta.tier_bitmask;
                        bm.set(fvfs_core::Tier::S3);
                        let m = meta.clone();
                        let fid = file_meta.id;
                        tokio::task::spawn_blocking(move || m.set_tier_bitmask(fid, bm))
                            .await
                            .map_err(|e| VfsError::Other(anyhow::anyhow!("{e}")))?
                    }
                    .await
                }
            };

            match result {
                Ok(()) => {
                    info!(wal_id = entry.id, op = ?entry.op, path = %file_meta.path, "WAL entry replayed");
                    let m = meta.clone();
                    let id = entry.id;
                    let _ = tokio::task::spawn_blocking(move || m.wal_complete(id)).await;
                    break;
                }
                Err(e) => {
                    warn!(
                        wal_id = entry.id,
                        op = ?entry.op,
                        attempt,
                        err = %e,
                        "WAL replay attempt failed"
                    );
                    let m = meta.clone();
                    let id = entry.id;
                    let err_str = e.to_string();
                    let _ = tokio::task::spawn_blocking(move || m.wal_record_failure(id, &err_str)).await;
                    attempt += 1;
                    sleep(Duration::from_millis(delay)).await;
                    delay = (delay * 2).min(30_000);
                }
            }
        }
    }

    info!("WAL replay complete");
}
