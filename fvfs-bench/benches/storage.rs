/// Storage benchmarks — direct TierRouter read/write paths.
///
/// Groups:
///   write_small   4 KiB write (local disk + SQLite upsert + WAL enqueue)
///   write_large  64 MiB write
///   read_small    4 KiB read from local (hot) tier
///   read_large   64 MiB read from local (hot) tier
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fvfs_bench::{make_setup, rt};
use fvfs_core::FvfsPath;

fn bench_write(c: &mut Criterion) {
    let setup = make_setup();

    let mut g = c.benchmark_group("write");

    // ---- 4 KiB ----
    let data_4k = fvfs_bench::setup::payload_4k(0xAB);
    g.throughput(Throughput::Bytes(data_4k.len() as u64));
    g.bench_function(BenchmarkId::new("small", "4KiB"), |b| {
        let path = FvfsPath::new("/bench/write_small.bin").unwrap();
        b.iter(|| {
            rt().block_on(async {
                setup.router.write(&path, data_4k.clone()).await.unwrap();
            })
        });
    });

    // ---- 64 MiB ----
    let data_64m = fvfs_bench::setup::payload_64m(0xCD);
    g.throughput(Throughput::Bytes(data_64m.len() as u64));
    g.bench_function(BenchmarkId::new("large", "64MiB"), |b| {
        let path = FvfsPath::new("/bench/write_large.bin").unwrap();
        b.iter(|| {
            rt().block_on(async {
                setup.router.write(&path, data_64m.clone()).await.unwrap();
            })
        });
    });

    g.finish();
}

fn bench_read(c: &mut Criterion) {
    let setup = make_setup();

    // Seed the files once (not measured).
    let path_4k = FvfsPath::new("/bench/read_small.bin").unwrap();
    let path_64m = FvfsPath::new("/bench/read_large.bin").unwrap();
    fvfs_bench::setup::seed_file(&setup, &path_4k, fvfs_bench::setup::payload_4k(0x11), &[]);
    fvfs_bench::setup::seed_file(&setup, &path_64m, fvfs_bench::setup::payload_64m(0x22), &[]);

    let mut g = c.benchmark_group("read");

    // ---- 4 KiB hot ----
    g.throughput(Throughput::Bytes(4 * 1024));
    g.bench_function(BenchmarkId::new("small_hot", "4KiB"), |b| {
        b.iter(|| {
            rt().block_on(async {
                setup.router.read(&path_4k).await.unwrap()
            })
        });
    });

    // ---- 64 MiB hot ----
    g.throughput(Throughput::Bytes(64 * 1024 * 1024));
    g.bench_function(BenchmarkId::new("large_hot", "64MiB"), |b| {
        b.iter(|| {
            rt().block_on(async {
                setup.router.read(&path_64m).await.unwrap()
            })
        });
    });

    g.finish();
}

criterion_group!(benches, bench_write, bench_read);
criterion_main!(benches);
