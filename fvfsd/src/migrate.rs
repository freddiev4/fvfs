/// Migration subcommand.
///
/// Crawls Mac mini local disk and NAS concurrently, computes SHA-256 for
/// every file, deduplicates by hash, uploads unique content to S3, and
/// populates the metadata index.
///
/// Progress is reported to stdout.
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{info, warn};

use fvfs_core::backend::StorageBackend;
use fvfs_core::metadata::MetadataStore;
use fvfs_core::types::{FileMetadata, Tier, TierBitmask, FvfsPath};

pub struct MigrateOptions {
    pub local_path: PathBuf,
    pub nas_path: PathBuf,
    pub s3: Arc<dyn StorageBackend>,
    pub meta: MetadataStore,
}

pub struct MigrateReport {
    pub total_files: u64,
    pub total_bytes: u64,
    pub duplicates: u64,
    pub s3_upload_bytes: u64,
    pub errors: u64,
}

pub async fn run_migration(opts: MigrateOptions) -> MigrateReport {
    info!("Migration: crawling local ({}) and NAS ({})", opts.local_path.display(), opts.nas_path.display());

    // Collect all files from both sources in parallel.
    let (local_files, nas_files) = tokio::join!(
        tokio::task::spawn_blocking({
            let p = opts.local_path.clone();
            move || collect_files(&p)
        }),
        tokio::task::spawn_blocking({
            let p = opts.nas_path.clone();
            move || collect_files(&p)
        }),
    );

    let mut all_sources: Vec<(PathBuf, PathBuf, Tier)> = Vec::new(); // (root, file, tier)

    match local_files {
        Ok(Ok(files)) => {
            for f in files {
                all_sources.push((opts.local_path.clone(), f, Tier::Local));
            }
        }
        Ok(Err(e)) => warn!("Failed to crawl local: {e}"),
        Err(e) => warn!("spawn_blocking error (local): {e}"),
    }
    match nas_files {
        Ok(Ok(files)) => {
            for f in files {
                all_sources.push((opts.nas_path.clone(), f, Tier::Nas));
            }
        }
        Ok(Err(e)) => warn!("Failed to crawl NAS: {e}"),
        Err(e) => warn!("spawn_blocking error (NAS): {e}"),
    }

    let total_files = AtomicU64::new(0);
    let total_bytes = AtomicU64::new(0);
    let duplicates = AtomicU64::new(0);
    let s3_upload_bytes = AtomicU64::new(0);
    let errors = AtomicU64::new(0);

    // Hash all files in parallel (rayon), collect (hash -> file info).
    let hash_map: Arc<std::sync::Mutex<HashMap<String, (PathBuf, Tier, u64)>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));

    let results: Vec<_> = all_sources
        .par_iter()
        .map(|(root, file_path, tier)| {
            match hash_file(file_path) {
                Ok((hash, size)) => Some((root.clone(), file_path.clone(), *tier, hash, size)),
                Err(e) => {
                    warn!(path = %file_path.display(), err = %e, "hash failed");
                    None
                }
            }
        })
        .collect();

    // Build dedup map and prepare upload list.
    let mut to_upload: Vec<(PathBuf, PathBuf, Tier, String, u64)> = Vec::new();
    {
        let mut map = hash_map.lock().unwrap();
        for item in results.into_iter().flatten() {
            let (root, file_path, tier, hash, size) = item;
            total_files.fetch_add(1, Ordering::Relaxed);
            total_bytes.fetch_add(size, Ordering::Relaxed);

            if map.contains_key(&hash) {
                duplicates.fetch_add(1, Ordering::Relaxed);
            } else {
                map.insert(hash.clone(), (file_path.clone(), tier, size));
                to_upload.push((root, file_path, tier, hash, size));
            }
        }
    }

    info!(
        "Migration: {} files, {} unique, uploading to S3",
        total_files.load(Ordering::Relaxed),
        to_upload.len()
    );

    // Upload to S3 and update metadata.
    for (root, file_path, tier, hash, size) in &to_upload {
        // Derive FVFS path from file path relative to root.
        let rel = file_path
            .strip_prefix(root)
            .unwrap_or(file_path)
            .to_string_lossy()
            .replace('\\', "/");
        let vfs_path = match FvfsPath::new(format!("/{}", rel)) {
            Ok(p) => p,
            Err(e) => {
                warn!(path = %file_path.display(), err = %e, "invalid FVFS path");
                errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        // Read file content.
        let data = match std::fs::read(file_path) {
            Ok(d) => bytes::Bytes::from(d),
            Err(e) => {
                warn!(path = %file_path.display(), err = %e, "read failed");
                errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        // Upload to S3.
        match opts.s3.put(&vfs_path, data).await {
            Ok(()) => {
                s3_upload_bytes.fetch_add(*size, Ordering::Relaxed);
            }
            Err(e) => {
                warn!(path = %vfs_path, err = %e, "S3 upload failed");
                errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        }

        // Upsert metadata.
        let mut bitmask = TierBitmask::NONE;
        bitmask.set(*tier);
        bitmask.set(Tier::S3);

        let meta_entry = FileMetadata {
            id: 0,
            path: vfs_path.clone(),
            kind: fvfs_core::EntryKind::File,
            size_bytes: *size,
            sha256: hash.clone(),
            tier_bitmask: bitmask,
            created_at: fvfs_core::now_unix(),
            modified_at: fvfs_core::now_unix(),
            accessed_at: fvfs_core::now_unix(),
            access_count_30d: 0,
            mime_type: None,
        };

        let meta_store = opts.meta.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || meta_store.upsert(&meta_entry)).await {
            warn!(path = %vfs_path, err = %e, "metadata upsert failed");
        }
    }

    let report = MigrateReport {
        total_files: total_files.load(Ordering::Relaxed),
        total_bytes: total_bytes.load(Ordering::Relaxed),
        duplicates: duplicates.load(Ordering::Relaxed),
        s3_upload_bytes: s3_upload_bytes.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
    };

    info!(
        "Migration complete: {} files, {} bytes, {} duplicates, {} S3 bytes, {} errors",
        report.total_files,
        report.total_bytes,
        report.duplicates,
        report.s3_upload_bytes,
        report.errors,
    );

    report
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut files = Vec::new();
    collect_recursive(root, &mut files)?;
    Ok(files)
}

fn collect_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_recursive(&path, out)?;
        } else if path.is_file() {
            out.push(path);
        }
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<(String, u64), std::io::Error> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024]; // 1 MB buffer
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((hex::encode(hasher.finalize()), total))
}
