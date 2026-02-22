/// Metadata benchmarks — SQLite lookup paths.
///
/// Groups:
///   stat          Single-path lookup (MetadataStore::get)
///   list_dir      Directory scan for 100 and 1 000 immediate children
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use fvfs_bench::{make_setup, rt, setup::payload_4k};
use fvfs_core::FvfsPath;

fn bench_stat(c: &mut Criterion) {
    let setup = make_setup();

    // Seed one file (not measured).
    let target = FvfsPath::new("/docs/report.pdf").unwrap();
    fvfs_bench::setup::seed_file(&setup, &target, payload_4k(0x01), &[]);

    c.bench_function("stat/single_path", |b| {
        let meta = setup.router.meta.clone();
        let p = target.clone();
        b.iter(|| {
            // MetadataStore::get is synchronous (behind a Mutex); use
            // spawn_blocking to measure the same code path the router uses.
            rt().block_on(async {
                let m = meta.clone();
                let pp = p.clone();
                tokio::task::spawn_blocking(move || m.get(&pp))
                    .await
                    .unwrap()
                    .unwrap()
            })
        });
    });
}

fn bench_list_dir(c: &mut Criterion) {
    let setup = make_setup();

    // Seed directories with 100 and 1 000 files respectively.
    for n in [100usize, 1_000] {
        for i in 0..n {
            let p = FvfsPath::new(format!("/list/{n}/file_{i:04}.bin")).unwrap();
            fvfs_bench::setup::seed_file(&setup, &p, payload_4k(0x02), &[]);
        }
    }

    let mut g = c.benchmark_group("list_dir");

    for n in [100usize, 1_000] {
        let dir = FvfsPath::new(format!("/list/{n}")).unwrap();
        g.bench_with_input(BenchmarkId::new("entries", n), &n, |b, _| {
            let meta = setup.router.meta.clone();
            let d = dir.clone();
            b.iter(|| {
                rt().block_on(async {
                    let m = meta.clone();
                    let dd = d.clone();
                    tokio::task::spawn_blocking(move || m.list_dir(&dd))
                        .await
                        .unwrap()
                        .unwrap()
                })
            });
        });
    }

    g.finish();
}

criterion_group!(benches, bench_stat, bench_list_dir);
criterion_main!(benches);
