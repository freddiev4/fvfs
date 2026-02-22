/// Tier-promotion benchmark.
///
/// Measures the latency difference between reading from the local (hot) tier
/// versus reading from the NAS (warm) tier, and verifies that a second read
/// after promotion hits the local cache.
///
/// Three sub-benchmarks:
///
///   local_read    — file exists only in local; pure local disk read (baseline)
///   nas_read      — file exists only in NAS; NAS disk read + promotion signal
///   post_promote  — file was in NAS, then explicitly promoted to local;
///                   should match local_read latency
///
/// Note: the promotion signal from `nas_read` is fire-and-forget — it is
/// drained by the background task in BenchSetup without performing the actual
/// copy.  `post_promote` therefore simulates the outcome of promotion by
/// manually writing the file to local and updating the bitmask before timing.
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fvfs_bench::{make_setup, rt, setup::payload_4k};
use fvfs_core::{FvfsPath, Tier};

fn bench_tier_promotion(c: &mut Criterion) {
    let setup = make_setup();

    let local_path = FvfsPath::new("/promo/local_only.bin").unwrap();
    let nas_path = FvfsPath::new("/promo/nas_only.bin").unwrap();
    let promoted_path = FvfsPath::new("/promo/promoted.bin").unwrap();

    // --- seed local-only file ---
    fvfs_bench::setup::seed_file(&setup, &local_path, payload_4k(0x10), &[]);

    // --- seed NAS-only file (bypasses local, sets bitmask = NAS) ---
    fvfs_bench::setup::seed_nas_only(&setup, &nas_path, payload_4k(0x20));

    // --- seed "post-promotion" file: write to NAS first, then copy to local
    //     and update bitmask to simulate a completed promotion cycle ---
    fvfs_bench::setup::seed_nas_only(&setup, &promoted_path, payload_4k(0x30));
    rt().block_on(async {
        // Manually promote: copy from NAS to local.
        let data = setup.router.nas.get(&promoted_path).await.unwrap();
        setup.router.local.put(&promoted_path, data).await.unwrap();
        // Update bitmask to reflect both tiers.
        let ms = setup.router.meta.clone();
        let pp = promoted_path.clone();
        let meta = tokio::task::spawn_blocking(move || ms.get(&pp))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut bm = meta.tier_bitmask;
        bm.set(Tier::Local);
        let ms2 = setup.router.meta.clone();
        tokio::task::spawn_blocking(move || ms2.set_tier_bitmask(meta.id, bm))
            .await
            .unwrap()
            .unwrap();
    });

    let mut g = c.benchmark_group("tier_promotion");
    g.throughput(Throughput::Bytes(4 * 1024));

    // Baseline: local tier read.
    g.bench_function(BenchmarkId::new("tier", "local"), |b| {
        b.iter(|| {
            rt().block_on(async { setup.router.read(&local_path).await.unwrap() })
        });
    });

    // NAS-only read (triggers async promotion signal but does NOT wait for it).
    g.bench_function(BenchmarkId::new("tier", "nas_only"), |b| {
        b.iter(|| {
            rt().block_on(async { setup.router.read(&nas_path).await.unwrap() })
        });
    });

    // Post-promotion read: file is in local after a completed promotion cycle.
    // Should match the local_read latency.
    g.bench_function(BenchmarkId::new("tier", "post_promote"), |b| {
        b.iter(|| {
            rt().block_on(async { setup.router.read(&promoted_path).await.unwrap() })
        });
    });

    g.finish();
}

criterion_group!(benches, bench_tier_promotion);
criterion_main!(benches);
