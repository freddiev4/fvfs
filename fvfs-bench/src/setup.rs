/// Shared benchmark setup: builds a fully wired TierRouter backed by
/// three local temp-dirs (local / NAS / S3 stand-in), an in-memory
/// SQLite metadata store, and a drained promotion channel.
///
/// The S3 "backend" is a third LocalDiskBackend pointing at a separate
/// sub-dir — adequate for exercising the routing logic without real AWS
/// credentials.
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use fvfs_core::backend::local::LocalDiskBackend;
use fvfs_core::metadata::MetadataStore;
use fvfs_core::{FvfsPath, Tier, TierBitmask};
use fvfsd::router::{PromoteSignal, TierRouter};
use tempfile::TempDir;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Shared tokio runtime

static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// Return the shared multi-threaded tokio runtime used by all benchmarks.
pub fn rt() -> &'static tokio::runtime::Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime")
    })
}

// ---------------------------------------------------------------------------
// BenchSetup

pub struct BenchSetup {
    pub router: Arc<TierRouter>,
    /// Keep the TempDir alive for the lifetime of the benchmark.
    pub _tmp: TempDir,
}

/// Create a fresh BenchSetup.  Call once per benchmark group, not per
/// iteration — the setup is intentionally *not* part of the measured path.
pub fn make_setup() -> BenchSetup {
    let tmp = TempDir::new().expect("tempdir");
    let local_dir = tmp.path().join("local");
    let nas_dir = tmp.path().join("nas");
    let s3_dir = tmp.path().join("s3");
    std::fs::create_dir_all(&local_dir).unwrap();
    std::fs::create_dir_all(&nas_dir).unwrap();
    std::fs::create_dir_all(&s3_dir).unwrap();

    let local: Arc<dyn fvfs_core::backend::StorageBackend> =
        Arc::new(LocalDiskBackend::new(&local_dir, Tier::Local));
    let nas: Arc<dyn fvfs_core::backend::StorageBackend> =
        Arc::new(LocalDiskBackend::new(&nas_dir, Tier::Nas));
    // S3 stand-in: LocalDiskBackend reporting Tier::S3.
    let s3: Arc<dyn fvfs_core::backend::StorageBackend> =
        Arc::new(LocalDiskBackend::new(&s3_dir, Tier::S3));

    let meta = MetadataStore::open_in_memory().expect("in-memory sqlite");

    // Drain promotion signals in a background task so the channel never blocks.
    let (promote_tx, mut promote_rx) = mpsc::unbounded_channel::<PromoteSignal>();
    rt().spawn(async move {
        while promote_rx.recv().await.is_some() {}
    });

    BenchSetup {
        router: Arc::new(TierRouter {
            local,
            nas,
            s3,
            meta,
            promote_tx,
        }),
        _tmp: tmp,
    }
}

// ---------------------------------------------------------------------------
// Convenience constructors

/// 4 KiB payload of repeating byte `fill`.
pub fn payload_4k(fill: u8) -> Bytes {
    Bytes::from(vec![fill; 4 * 1024])
}

/// 64 MiB payload of repeating byte `fill`.
pub fn payload_64m(fill: u8) -> Bytes {
    Bytes::from(vec![fill; 64 * 1024 * 1024])
}

/// Write `data` at `path` via the router, then set `extra_tiers` in the
/// bitmask directly (bypassing the WAL) so benchmarks can simulate any
/// desired tier configuration.
pub fn seed_file(
    setup: &BenchSetup,
    path: &FvfsPath,
    data: Bytes,
    extra_tiers: &[Tier],
) {
    rt().block_on(async {
        setup.router.write(path, data).await.expect("seed write");
        // Optionally mark file as present on additional tiers.
        if !extra_tiers.is_empty() {
            let meta_store = setup.router.meta.clone();
            let p = path.clone();
            let meta = tokio::task::spawn_blocking(move || meta_store.get(&p))
                .await
                .unwrap()
                .unwrap()
                .expect("seeded file not found");
            let mut bm = meta.tier_bitmask;
            for &t in extra_tiers {
                bm.set(t);
            }
            let id = meta.id;
            let meta_store2 = setup.router.meta.clone();
            tokio::task::spawn_blocking(move || meta_store2.set_tier_bitmask(id, bm))
                .await
                .unwrap()
                .unwrap();
        }
    });
}

/// Write a file directly to the NAS backend (bypassing local) and record
/// it in the metadata store as NAS-only (`tier_bitmask = 0x2`).
pub fn seed_nas_only(setup: &BenchSetup, path: &FvfsPath, data: Bytes) {
    rt().block_on(async {
        // Write to NAS storage backend directly.
        setup
            .router
            .nas
            .put(path, data.clone())
            .await
            .expect("nas put");

        // Upsert metadata with NAS-only bitmask.
        use fvfs_core::{EntryKind, FileMetadata, now_unix};
        let sha = {
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(&data);
            hex::encode(h.finalize())
        };
        let now = now_unix();
        let mut meta_entry = FileMetadata {
            id: 0,
            path: path.clone(),
            kind: EntryKind::File,
            size_bytes: data.len() as u64,
            sha256: sha,
            tier_bitmask: {
                let mut bm = TierBitmask::NONE;
                bm.set(Tier::Nas);
                bm
            },
            created_at: now,
            modified_at: now,
            accessed_at: now,
            access_count_30d: 0,
            mime_type: None,
        };
        let ms = setup.router.meta.clone();
        tokio::task::spawn_blocking(move || ms.upsert(&meta_entry))
            .await
            .unwrap()
            .expect("upsert");
    });
}
