mod eviction_task;
mod fuse_handler;
mod http_api;
mod mdns_server;
mod migrate;
mod router;
mod s3_uploader;
mod wal_replay;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use fvfs_core::backend::local::LocalDiskBackend;
use fvfs_core::backend::nas::new_nas_backend;
use fvfs_core::backend::s3::S3Backend;
use fvfs_core::config::Config;
use fvfs_core::metadata::MetadataStore;
use fvfs_core::Tier;

use crate::eviction_task::{TierWatermarks, run_eviction};
use crate::http_api::{AppState, build_router};
use crate::router::{PromoteSignal, TierRouter};
use crate::s3_uploader::run_s3_uploader;

// ---------------------------------------------------------------------------
// CLI

#[derive(Parser)]
#[command(name = "vfsd", about = "fvfs daemon — distributed VFS for the Mac mini")]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "/etc/vfsd/config.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the daemon (FUSE mount + HTTP API + background tasks).
    Serve,

    /// Migrate existing data from local disk and NAS into the VFS.
    Migrate {
        /// Source path on the Mac mini local disk.
        #[arg(long)]
        local_src: Option<PathBuf>,
        /// Source path on the NAS.
        #[arg(long)]
        nas_src: Option<PathBuf>,
    },

    /// Print daemon status (connects to a running vfsd over HTTP).
    Status {
        /// vfsd HTTP address (default: http://localhost:7734).
        #[arg(long, default_value = "http://localhost:7734")]
        url: String,
    },
}

// ---------------------------------------------------------------------------
// Entry point

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("vfsd=info".parse().unwrap()))
        .init();

    let cli = Cli::parse();
    let cfg = match Config::load_or_default(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load config: {e}");
            std::process::exit(1);
        }
    };

    match cli.command {
        Command::Serve => run_serve(cfg).await,
        Command::Migrate { local_src, nas_src } => run_migrate(cfg, local_src, nas_src).await,
        Command::Status { url } => run_status(url).await,
    }
}

// ---------------------------------------------------------------------------
// Serve

async fn run_serve(cfg: Config) {
    info!("vfsd starting up");

    // Create storage directories.
    tokio::fs::create_dir_all(&cfg.tiers.local.path)
        .await
        .expect("create local tier dir");
    tokio::fs::create_dir_all(
        cfg.daemon
            .metadata_db
            .parent()
            .unwrap_or(std::path::Path::new(".")),
    )
    .await
    .ok();

    // Open metadata store.
    let meta = MetadataStore::open(&cfg.daemon.metadata_db).expect("open metadata db");

    // Build storage backends.
    let local_backend = Arc::new(LocalDiskBackend::new(&cfg.tiers.local.path, Tier::Local));
    let nas_backend = Arc::new(new_nas_backend(&cfg.tiers.nas.path));
    let s3_backend = Arc::new(
        S3Backend::new(
            cfg.tiers.s3.bucket.clone(),
            cfg.tiers.s3.region.clone(),
            cfg.tiers.s3.prefix.clone(),
        )
        .await
        .expect("init S3 backend"),
    );

    // Promotion channel: background task reads blocks from NAS/S3 and writes to local.
    let (promote_tx, mut promote_rx) = mpsc::unbounded_channel::<PromoteSignal>();

    let evict_notify = Arc::new(Notify::new());
    let flush_notify = Arc::new(Notify::new());

    let router = Arc::new(TierRouter {
        local: local_backend,
        nas: nas_backend,
        s3: s3_backend.clone(),
        meta: meta.clone(),
        promote_tx,
    });

    // WAL replay before serving.
    info!("Replaying WAL");
    wal_replay::replay_wal(router.clone(), meta.clone()).await;

    // Promotion background task.
    {
        let router = router.clone();
        tokio::spawn(async move {
            while let Some(signal) = promote_rx.recv().await {
                let PromoteSignal::Promote { path, from } = signal;
                if let Ok(data) = router.read(&path).await {
                    if let Err(e) = router.local.put(&path, data).await {
                        tracing::warn!(path = %path, from = %from, err = %e, "promotion failed");
                    } else {
                        // Update local bitmask.
                        let m = router.meta.clone();
                        let p = path.clone();
                        if let Ok(Some(meta)) = tokio::task::spawn_blocking(move || m.get(&p)).await.unwrap_or(Ok(None)) {
                            let mut bm = meta.tier_bitmask;
                            bm.set(Tier::Local);
                            let m2 = router.meta.clone();
                            let _ = tokio::task::spawn_blocking(move || m2.set_tier_bitmask(meta.id, bm)).await;
                        }
                    }
                }
            }
        });
    }

    // S3 uploader task.
    {
        let router = router.clone();
        let meta = meta.clone();
        let flush_notify = flush_notify.clone();
        let flush_size = cfg.upload.flush_size_mb * 1024 * 1024;
        let flush_interval = Duration::from_secs(cfg.upload.flush_interval_secs);
        tokio::spawn(async move {
            run_s3_uploader(router, meta, flush_size, flush_interval, flush_notify).await;
        });
    }

    // Eviction task.
    {
        let router = router.clone();
        let meta = meta.clone();
        let evict_notify = evict_notify.clone();
        let local_wm = TierWatermarks {
            high_bytes: cfg.tiers.local.high_watermark_gb * 1024 * 1024 * 1024,
            low_bytes: cfg.tiers.local.low_watermark_gb * 1024 * 1024 * 1024,
        };
        let nas_wm = TierWatermarks {
            high_bytes: cfg.tiers.nas.high_watermark_gb * 1024 * 1024 * 1024,
            low_bytes: cfg.tiers.nas.low_watermark_gb * 1024 * 1024 * 1024,
        };
        let interval_secs = cfg.eviction.interval_secs;
        let recency = cfg.eviction.recency_weight;
        let freq = cfg.eviction.frequency_weight;
        tokio::spawn(async move {
            run_eviction(
                router,
                meta,
                Duration::from_secs(interval_secs),
                evict_notify,
                local_wm,
                nas_wm,
                recency,
                freq,
            )
            .await;
        });
    }

    // mDNS registration.
    {
        let port = cfg.daemon.http_port;
        tokio::spawn(async move {
            mdns_server::register_mdns(port).await;
        });
    }

    // FUSE mount (optional).
    #[cfg(feature = "fuse")]
    {
        let mount_path = cfg.daemon.mount_path.clone();
        let router_fuse = router.clone();
        let meta_fuse = meta.clone();
        let rt = tokio::runtime::Handle::current();
        std::thread::spawn(move || {
            use crate::fuse_handler::fuse_impl::VfsdFuse;
            let fs = VfsdFuse::new(router_fuse, meta_fuse, rt);
            let options = vec![
                fuser::MountOption::RW,
                fuser::MountOption::FSName("vfsd".into()),
                fuser::MountOption::AutoUnmount,
            ];
            if let Err(e) = fuser::mount2(fs, &mount_path, &options) {
                error!("FUSE mount failed: {e}");
            }
        });
    }

    // HTTP server.
    let state = Arc::new(AppState::new(
        router,
        meta,
        evict_notify,
        flush_notify,
    ));
    let app = build_router(state);

    let addr = format!("0.0.0.0:{}", cfg.daemon.http_port);
    info!("HTTP API listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind HTTP port");
    axum::serve(listener, app)
        .await
        .expect("HTTP server error");
}

// ---------------------------------------------------------------------------
// Migrate

async fn run_migrate(cfg: Config, local_src: Option<PathBuf>, nas_src: Option<PathBuf>) {
    let local_path = local_src.unwrap_or_else(|| cfg.tiers.local.path.clone());
    let nas_path = nas_src.unwrap_or_else(|| cfg.tiers.nas.path.clone());

    let meta = MetadataStore::open(&cfg.daemon.metadata_db).expect("open metadata db");
    let s3 = Arc::new(
        S3Backend::new(
            cfg.tiers.s3.bucket.clone(),
            cfg.tiers.s3.region.clone(),
            cfg.tiers.s3.prefix.clone(),
        )
        .await
        .expect("init S3 backend"),
    );

    let report = migrate::run_migration(migrate::MigrateOptions {
        local_path,
        nas_path,
        s3,
        meta,
    })
    .await;

    println!("Migration complete:");
    println!("  Total files  : {}", report.total_files);
    println!("  Total bytes  : {} MB", report.total_bytes / (1 << 20));
    println!("  Duplicates   : {}", report.duplicates);
    println!("  S3 uploaded  : {} MB", report.s3_upload_bytes / (1 << 20));
    println!("  Errors       : {}", report.errors);
}

// ---------------------------------------------------------------------------
// Status

async fn run_status(url: String) {
    let status_url = format!("{}/v1/status", url.trim_end_matches('/'));
    match reqwest::get(&status_url).await {
        Ok(resp) => {
            match resp.json::<fvfs_core::DaemonStatus>().await {
                Ok(status) => {
                    println!("vfsd v{} — uptime {}s", status.version, status.uptime_secs);
                    for tier in &status.tiers {
                        println!(
                            "  {} : {} files, {} MB",
                            tier.name,
                            tier.files,
                            tier.bytes / (1 << 20),
                        );
                    }
                    println!("  WAL pending: {}", status.wal_pending);
                }
                Err(e) => eprintln!("Failed to parse status response: {e}"),
            }
        }
        Err(e) => eprintln!("Failed to reach vfsd at {status_url}: {e}"),
    }
}
