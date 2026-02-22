/// S3 uploader background task.
///
/// Drains the `wal_pending` table for `upload_s3` operations. Flushes when:
///   - Accumulated pending size exceeds `flush_size_bytes`, OR
///   - `flush_interval` elapses, whichever comes first.
///
/// Each upload is individually committed; a batch failure only retries the
/// failed files, not the whole batch.
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::interval;
use tracing::{error, info, warn};

use fvfs_core::metadata::MetadataStore;
use fvfs_core::{VfsError, WalOp};

use crate::router::TierRouter;

const MAX_ATTEMPTS: u32 = 10;

pub async fn run_s3_uploader(
    router: Arc<TierRouter>,
    meta: MetadataStore,
    flush_size_bytes: u64,
    flush_interval: Duration,
    flush_notify: Arc<Notify>,
) {
    let mut ticker = interval(flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        // Wait for ticker OR manual flush signal.
        tokio::select! {
            _ = ticker.tick() => {
                info!("S3 uploader: interval triggered flush");
            }
            _ = flush_notify.notified() => {
                info!("S3 uploader: manual flush triggered");
            }
        }

        flush_pending(&router, &meta, flush_size_bytes).await;
    }
}

async fn flush_pending(router: &TierRouter, meta: &MetadataStore, _flush_size_bytes: u64) {
    // Load all pending S3 upload entries.
    let entries = match tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.wal_pending()
    })
    .await
    {
        Ok(Ok(e)) => e,
        Ok(Err(e)) => {
            error!("s3_uploader: failed to load WAL: {e}");
            return;
        }
        Err(e) => {
            error!("s3_uploader: spawn_blocking error: {e}");
            return;
        }
    };

    let s3_entries: Vec<_> = entries
        .into_iter()
        .filter(|e| e.op == WalOp::UploadS3 && e.attempts < MAX_ATTEMPTS)
        .collect();

    if s3_entries.is_empty() {
        return;
    }

    info!("S3 uploader: flushing {} files", s3_entries.len());

    for entry in s3_entries {
        // Look up file metadata.
        let file_meta = match tokio::task::spawn_blocking({
            let m = meta.clone();
            let fid = entry.file_id;
            move || m.get_by_id(fid)
        })
        .await
        {
            Ok(Ok(Some(m))) => m,
            Ok(Ok(None)) => {
                // File deleted; clean up WAL entry.
                let m = meta.clone();
                let id = entry.id;
                let _ = tokio::task::spawn_blocking(move || m.wal_complete(id)).await;
                continue;
            }
            Ok(Err(e)) => {
                warn!(err = %e, "s3_uploader: failed to load file meta");
                continue;
            }
            Err(e) => {
                warn!(err = %e, "s3_uploader: spawn_blocking error");
                continue;
            }
        };

        let result: Result<(), VfsError> = async {
            let data = router.read(&file_meta.path).await?;
            router.s3.put(&file_meta.path, data).await?;

            // Update tier bitmask.
            let mut bm = file_meta.tier_bitmask;
            bm.set(fvfs_core::Tier::S3);
            let m = meta.clone();
            let fid = file_meta.id;
            tokio::task::spawn_blocking(move || m.set_tier_bitmask(fid, bm))
                .await
                .map_err(|e| VfsError::Other(anyhow::anyhow!("{e}")))?
        }
        .await;

        match result {
            Ok(()) => {
                info!(path = %file_meta.path, "s3_uploader: uploaded successfully");
                let m = meta.clone();
                let id = entry.id;
                let _ = tokio::task::spawn_blocking(move || m.wal_complete(id)).await;
            }
            Err(e) => {
                warn!(
                    path = %file_meta.path,
                    attempt = entry.attempts,
                    err = %e,
                    "s3_uploader: upload failed"
                );
                let m = meta.clone();
                let id = entry.id;
                let err_str = e.to_string();
                let _ = tokio::task::spawn_blocking(move || m.wal_record_failure(id, &err_str)).await;
            }
        }
    }
}
