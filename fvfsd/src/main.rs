// All module declarations live in lib.rs; import them here.
use fvfsd::eviction_task::{TierWatermarks, run_eviction};
use fvfsd::http_api::{AppState, build_router};
use fvfsd::router::{PromoteSignal, TierRouter};
use fvfsd::s3_uploader::run_s3_uploader;
use fvfsd::{migrate, mdns_server, wal_replay};

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

// ---------------------------------------------------------------------------
// CLI

#[derive(Parser)]
#[command(name = "fvfsd", about = "fvfs daemon — distributed FVFS for the Mac mini")]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "/etc/fvfsd/config.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the daemon (FUSE mount + HTTP API + background tasks).
    Serve,

    /// Migrate existing data from local disk and NAS into the FVFS.
    Migrate {
        #[arg(long)]
        local_src: Option<PathBuf>,
        #[arg(long)]
        nas_src: Option<PathBuf>,
    },

    /// Print daemon status (connects to a running fvfsd over HTTP).
    Status {
        #[arg(long, default_value = "http://localhost:7734")]
        url: String,
    },
}

// ---------------------------------------------------------------------------
// Entry point

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("fvfsd=info".parse().unwrap()))
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
    info!("fvfsd starting up");

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

    let meta = MetadataStore::open(&cfg.daemon.metadata_db).expect("open metadata db");

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

    let (promote_tx, mut promote_rx) = mpsc::unbounded_channel::<PromoteSignal>();
    let evict_notify = Arc::new(Notify::new());
    let flush_notify = Arc::new(Notify::new());

    let router = Arc::new(TierRouter {
        local: local_backend,
        nas: nas_backend,
        s3: s3_backend,
        meta: meta.clone(),
        promote_tx,
    });

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
                        let m = router.meta.clone();
                        let p = path.clone();
                        if let Ok(Some(meta)) =
                            tokio::task::spawn_blocking(move || m.get(&p))
                                .await
                                .unwrap_or(Ok(None))
                        {
                            let mut bm = meta.tier_bitmask;
                            bm.set(Tier::Local);
                            let m2 = router.meta.clone();
                            let _ =
                                tokio::task::spawn_blocking(move || m2.set_tier_bitmask(meta.id, bm))
                                    .await;
                        }
                    }
                }
            }
        });
    }

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

    {
        let router = router.clone();
        let meta = meta.clone();
        let evict_notify = evict_notify.clone();
        let local_wm = TierWatermarks {
            high_bytes: cfg.tiers.local.high_watermark_gb << 30,
            low_bytes: cfg.tiers.local.low_watermark_gb << 30,
        };
        let nas_wm = TierWatermarks {
            high_bytes: cfg.tiers.nas.high_watermark_gb << 30,
            low_bytes: cfg.tiers.nas.low_watermark_gb << 30,
        };
        tokio::spawn(async move {
            run_eviction(
                router,
                meta,
                Duration::from_secs(cfg.eviction.interval_secs),
                evict_notify,
                local_wm,
                nas_wm,
                cfg.eviction.recency_weight,
                cfg.eviction.frequency_weight,
            )
            .await;
        });
    }

    {
        let port = cfg.daemon.http_port;
        tokio::spawn(async move {
            mdns_server::register_mdns(port).await;
        });
    }

    #[cfg(feature = "fuse")]
    {
        let mount_path = cfg.daemon.mount_path.clone();
        let router_fuse = router.clone();
        let meta_fuse = meta.clone();
        let rt = tokio::runtime::Handle::current();
        std::thread::spawn(move || {
            use fvfsd::fuse_handler::fuse_impl::VfsdFuse;
            let fs = VfsdFuse::new(router_fuse, meta_fuse, rt);
            let options = vec![
                fuser::MountOption::RW,
                fuser::MountOption::FSName("fvfsd".into()),
                fuser::MountOption::AutoUnmount,
            ];
            if let Err(e) = fuser::mount2(fs, &mount_path, &options) {
                error!("FUSE mount failed: {e}");
            }
        });
    }

    let state = Arc::new(AppState::new(router, meta, evict_notify, flush_notify));
    let app = build_router(state);

    let addr = format!("0.0.0.0:{}", cfg.daemon.http_port);
    info!("HTTP API listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind HTTP port");
    axum::serve(listener, app).await.expect("HTTP server error");
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
    println!("  Total bytes  : {} MB", report.total_bytes >> 20);
    println!("  Duplicates   : {}", report.duplicates);
    println!("  S3 uploaded  : {} MB", report.s3_upload_bytes >> 20);
    println!("  Errors       : {}", report.errors);
}

// ---------------------------------------------------------------------------
// Status

async fn run_status(url: String) {
    let status_url = format!("{}/v1/status", url.trim_end_matches('/'));
    match reqwest::get(&status_url).await {
        Ok(resp) => match resp.json::<fvfs_core::DaemonStatus>().await {
            Ok(status) => {
                println!("fvfsd v{} — uptime {}s", status.version, status.uptime_secs);
                for tier in &status.tiers {
                    println!("  {} : {} files, {} MB", tier.name, tier.files, tier.bytes >> 20);
                }
                println!("  WAL pending: {}", status.wal_pending);
            }
            Err(e) => eprintln!("Failed to parse status response: {e}"),
        },
        Err(e) => eprintln!("Failed to reach fvfsd at {status_url}: {e}"),
    }
}
