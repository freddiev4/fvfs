/// HTTP round-trip benchmarks — full axum stack over a real TCP loopback.
///
/// Spins up a fvfsd HTTP server once on an OS-assigned port, then measures
/// the time for a complete client request → server processing → response
/// cycle, including TCP, serde, axum routing, and TierRouter I/O.
///
/// Groups:
///   http_roundtrip/PUT_4KiB   write a 4 KiB file via PUT /v1/files/*
///   http_roundtrip/GET_4KiB   read that file back via GET /v1/files/*
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use fvfs_bench::{make_setup, rt, setup::payload_4k};
use fvfsd::http_api::{AppState, build_router};
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// One-time server startup

struct BenchServer {
    addr: SocketAddr,
    /// Keep BenchSetup alive so temp dirs aren't deleted.
    _setup: fvfs_bench::BenchSetup,
}

// Safety: BenchSetup contains TempDir (Send+Sync) and TierRouter (Send+Sync).
unsafe impl Sync for BenchServer {}

static SERVER: OnceLock<BenchServer> = OnceLock::new();

fn get_server() -> &'static BenchServer {
    SERVER.get_or_init(|| {
        let setup = make_setup();
        let router = setup.router.clone();
        let meta = setup.router.meta.clone();

        let evict = Arc::new(Notify::new());
        let flush = Arc::new(Notify::new());
        let state = Arc::new(AppState::new(router, meta, evict, flush));
        let app = build_router(state);

        let addr = rt().block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind bench server");
            let addr = listener.local_addr().unwrap();
            rt().spawn(async move {
                axum::serve(listener, app).await.ok();
            });
            addr
        });

        // Pre-populate the file we'll read in the GET benchmark.
        let seed_url = format!("http://{}/v1/files/bench/read.bin", addr);
        rt().block_on(async {
            reqwest::Client::new()
                .put(&seed_url)
                .body(payload_4k(0xFF).to_vec())
                .send()
                .await
                .expect("seed PUT");
        });

        BenchServer { addr, _setup: setup }
    })
}

// ---------------------------------------------------------------------------
// Benchmarks

fn bench_http_put(c: &mut Criterion) {
    let server = get_server();
    let url = format!("http://{}/v1/files/bench/write.bin", server.addr);
    let client = reqwest::Client::new();
    let body = payload_4k(0xAA);

    let mut g = c.benchmark_group("http_roundtrip");
    g.throughput(Throughput::Bytes(body.len() as u64));

    g.bench_function("PUT_4KiB", |b| {
        b.iter(|| {
            rt().block_on(async {
                client
                    .put(&url)
                    .body(body.clone().to_vec())
                    .send()
                    .await
                    .expect("PUT")
                    .error_for_status()
                    .expect("PUT status")
            })
        });
    });

    g.finish();
}

fn bench_http_get(c: &mut Criterion) {
    let server = get_server();
    let url = format!("http://{}/v1/files/bench/read.bin", server.addr);
    let client = reqwest::Client::new();

    let mut g = c.benchmark_group("http_roundtrip");
    g.throughput(Throughput::Bytes(4 * 1024));

    g.bench_function("GET_4KiB", |b| {
        b.iter(|| {
            rt().block_on(async {
                client
                    .get(&url)
                    .send()
                    .await
                    .expect("GET")
                    .bytes()
                    .await
                    .expect("body")
            })
        });
    });

    g.finish();
}

criterion_group!(benches, bench_http_put, bench_http_get);
criterion_main!(benches);
