/// Local read cache for fvfsc.
///
/// Files are stored keyed by their SHA-256 hash, avoiding redundant fetches
/// when the same content is read multiple times (e.g. the same file from
/// different paths, or the same file re-opened after a page-cache miss).
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

#[derive(Clone)]
pub struct LocalCache {
    root: PathBuf,
    max_bytes: u64,
}

impl LocalCache {
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> Self {
        LocalCache {
            root: root.into(),
            max_bytes,
        }
    }

    pub async fn init(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.root).await
    }

    /// Look up cached content by SHA-256 hash. Returns `None` on a cache miss.
    pub async fn get_by_hash(&self, sha256: &str) -> Option<Bytes> {
        let cache_path = self.cache_path(sha256);
        match fs::read(&cache_path).await {
            Ok(data) => {
                debug!(hash = %sha256, "cache hit");
                Some(Bytes::from(data))
            }
            Err(_) => {
                debug!(hash = %sha256, "cache miss");
                None
            }
        }
    }

    /// Store `data` in the cache under its SHA-256 hash.
    pub async fn put(&self, data: &Bytes) -> String {
        let hash = {
            let mut h = Sha256::new();
            h.update(data.as_ref());
            hex::encode(h.finalize())
        };
        let cache_path = self.cache_path(&hash);

        if let Some(parent) = cache_path.parent() {
            if let Err(e) = fs::create_dir_all(parent).await {
                warn!(err = %e, "cache: failed to create parent dir");
                return hash;
            }
        }

        if !cache_path.exists() {
            match fs::File::create(&cache_path).await {
                Ok(mut f) => {
                    if let Err(e) = f.write_all(data.as_ref()).await {
                        warn!(hash = %hash, err = %e, "cache: write failed");
                    } else {
                        debug!(hash = %hash, bytes = data.len(), "cache: stored");
                    }
                }
                Err(e) => {
                    warn!(hash = %hash, err = %e, "cache: create failed");
                }
            }
        }

        hash
    }

    /// Compute total bytes used in the cache.
    pub async fn total_bytes(&self) -> u64 {
        compute_dir_size(&self.root).await
    }

    /// Evict oldest entries until total size is below `max_bytes`.
    pub async fn evict_if_needed(&self) {
        let used = self.total_bytes().await;
        if used <= self.max_bytes {
            return;
        }

        // Collect (mtime, path, size) for all cache entries.
        let mut entries: Vec<(std::time::SystemTime, PathBuf, u64)> = Vec::new();
        collect_cache_files(&self.root, &mut entries).await;

        // Sort oldest first.
        entries.sort_by_key(|(t, _, _)| *t);

        let target = (used * 3 / 4).min(self.max_bytes);
        let mut freed = 0u64;

        for (_, path, size) in &entries {
            if (used - freed) <= target {
                break;
            }
            if fs::remove_file(path).await.is_ok() {
                freed += size;
                debug!(path = %path.display(), "cache: evicted");
            }
        }
    }

    fn cache_path(&self, sha256: &str) -> PathBuf {
        // Use first 2 chars as directory prefix to avoid too many files in one dir.
        let (prefix, rest) = sha256.split_at(2.min(sha256.len()));
        self.root.join(prefix).join(rest)
    }
}

async fn compute_dir_size(root: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut rd = match fs::read_dir(&dir).await {
            Ok(r) => r,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(meta) = entry.metadata().await {
                total += meta.len();
            }
        }
    }
    total
}

async fn collect_cache_files(
    root: &Path,
    out: &mut Vec<(std::time::SystemTime, PathBuf, u64)>,
) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut rd = match fs::read_dir(&dir).await {
            Ok(r) => r,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(meta) = entry.metadata().await {
                let mtime = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                out.push((mtime, path, meta.len()));
            }
        }
    }
}
