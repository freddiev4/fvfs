/// Concurrent-writes benchmark.
///
/// Spawns N tokio tasks that each write a distinct 4 KiB file simultaneously
/// through a single shared TierRouter.  Measures contention on the SQLite
/// Mutex (metadata upsert) and the local filesystem under parallel load.
///
/// Groups:
///   concurrent_writes/tasks=1   baseline (serial)
///   concurrent_writes/tasks=4
///   concurrent_writes/tasks=8
///   concurrent_writes/tasks=16
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fvfs_bench::{make_setup, rt, setup::payload_4k};
use fvfs_core::FvfsPath;
use futures::future::join_all;
use std::sync::Arc;

fn bench_concurrent_writes(c: &mut Criterion) {
    let setup = Arc::new(make_setup());

    let mut g = c.benchmark_group("concurrent_writes");

    for &task_count in &[1usize, 4, 8, 16] {
        // Throughput: all tasks write 4 KiB each per iteration.
        g.throughput(Throughput::Bytes((task_count * 4 * 1024) as u64));

        g.bench_with_input(
            BenchmarkId::new("tasks", task_count),
            &task_count,
            |b, &n| {
                let router = setup.router.clone();
                b.iter(|| {
                    rt().block_on(async {
                        let tasks: Vec<_> = (0..n)
                            .map(|i| {
                                let r = router.clone();
                                let path =
                                    FvfsPath::new(format!("/concurrent/{n}/task_{i}.bin")).unwrap();
                                let data = payload_4k(i as u8);
                                tokio::spawn(async move {
                                    r.write(&path, data).await.expect("concurrent write")
                                })
                            })
                            .collect();
                        join_all(tasks).await;
                    })
                });
            },
        );
    }

    g.finish();
}

criterion_group!(benches, bench_concurrent_writes);
criterion_main!(benches);
