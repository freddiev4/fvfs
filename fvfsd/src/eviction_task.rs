/// Eviction background task.
///
/// Runs on a configurable interval. For each tier (local, NAS) it:
///   1. Computes total bytes on tier
///   2. If above the high watermark, evicts files (lowest score first) until
///      usage drops to the low watermark
///   3. Only evicts a file if it is confirmed present on all colder tiers
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::interval;
use tracing::{info, warn};

use fvfs_core::eviction::{ArcInspiredPolicy, EvictionPolicy};
use fvfs_core::metadata::MetadataStore;
use fvfs_core::Tier;

use crate::router::TierRouter;

pub struct TierWatermarks {
    pub high_bytes: u64,
    pub low_bytes: u64,
}

pub async fn run_eviction(
    router: Arc<TierRouter>,
    meta: MetadataStore,
    interval_dur: Duration,
    evict_notify: Arc<Notify>,
    local_wm: TierWatermarks,
    nas_wm: TierWatermarks,
    recency_weight: f64,
    frequency_weight: f64,
) {
    let policy = ArcInspiredPolicy {
        recency_weight,
        frequency_weight,
    };

    let mut ticker = interval(interval_dur);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = evict_notify.notified() => {
                info!("eviction: manually triggered");
            }
        }

        run_eviction_cycle(&router, &meta, &policy, Tier::Local, &local_wm).await;
        run_eviction_cycle(&router, &meta, &policy, Tier::Nas, &nas_wm).await;
    }
}

async fn run_eviction_cycle<P: EvictionPolicy>(
    router: &TierRouter,
    meta: &MetadataStore,
    policy: &P,
    tier: Tier,
    wm: &TierWatermarks,
) {
    let tier_bit = tier.bitmask();

    // Check current usage.
    let used = match tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.bytes_on_tier(tier_bit)
    })
    .await
    {
        Ok(Ok(b)) => b,
        _ => return,
    };

    if used <= wm.high_bytes {
        return;
    }

    info!(
        tier = %tier,
        used_gb = used / (1 << 30),
        high_wm_gb = wm.high_bytes / (1 << 30),
        "eviction: above high watermark, evicting"
    );

    // Load files on this tier.
    let mut files = match tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.files_on_tier(tier_bit)
    })
    .await
    {
        Ok(Ok(f)) => f,
        _ => return,
    };

    // Sort: lowest score first.
    files.sort_by(|a, b| {
        policy
            .score(a)
            .partial_cmp(&policy.score(b))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut freed: u64 = 0;
    let target_free = used.saturating_sub(wm.low_bytes);

    for file in &files {
        if freed >= target_free {
            break;
        }
        // Only evict if present on all colder tiers (S3 is sufficient).
        if !file.tier_bitmask.safe_to_evict_from(tier) {
            continue;
        }

        match router.evict_from_tier(&file.path, tier).await {
            Ok(()) => {
                freed += file.size_bytes;
                info!(
                    tier = %tier,
                    path = %file.path,
                    bytes = file.size_bytes,
                    "evicted file"
                );
            }
            Err(e) => {
                warn!(tier = %tier, path = %file.path, err = %e, "eviction failed");
            }
        }
    }

    info!(
        tier = %tier,
        freed_mb = freed / (1 << 20),
        "eviction cycle complete"
    );
}
