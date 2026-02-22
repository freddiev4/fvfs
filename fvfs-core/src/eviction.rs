use crate::types::FileMetadata;

/// An eviction policy assigns a score to a file.
///
/// **Lower score = evict first.**
pub trait EvictionPolicy: Send + Sync {
    fn score(&self, entry: &FileMetadata) -> f64;
}

/// ARC-inspired combined recency + frequency score.
///
/// ```
/// score = (recency_weight / seconds_since_access) + (frequency_weight * access_count_30d)
/// ```
pub struct ArcInspiredPolicy {
    pub recency_weight: f64,
    pub frequency_weight: f64,
}

impl Default for ArcInspiredPolicy {
    fn default() -> Self {
        ArcInspiredPolicy {
            recency_weight: 0.7,
            frequency_weight: 0.3,
        }
    }
}

impl EvictionPolicy for ArcInspiredPolicy {
    fn score(&self, entry: &FileMetadata) -> f64 {
        let now = crate::types::now_unix();
        let seconds_since_access = (now - entry.accessed_at).max(1) as f64;
        let frequency = entry.access_count_30d as f64;

        (self.recency_weight / seconds_since_access) + (self.frequency_weight * frequency)
    }
}

/// Sort `entries` in ascending score order (coldest / evict-first at index 0).
pub fn rank_for_eviction<P: EvictionPolicy>(
    policy: &P,
    entries: &mut Vec<FileMetadata>,
) {
    entries.sort_by(|a, b| {
        policy
            .score(a)
            .partial_cmp(&policy.score(b))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}
