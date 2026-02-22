mod fuse_handler;
mod http_client;
mod local_cache;
mod mdns_client;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use fvfs_core::config::Config;
use fvfs_core::FvfsPath;

use crate::http_client::VfsdClient;
use crate::local_cache::LocalCache;

// ---------------------------------------------------------------------------
// CLI

#[derive(Parser)]
#[command(name = "fvfsc", about = "fvfs client — mounts the FVFS locally via FUSE")]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "/etc/fvfsd/config.toml")]
    config: PathBuf,

    /// Explicit fvfsd URL; skips mDNS discovery.
    #[arg(long, env = "VFSD_URL")]
    url: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Mount the FVFS locally (requires the `fuse` feature).
    Mount {
        /// Mount point (overrides config).
        mountpoint: Option<PathBuf>,
    },

    /// List a directory in the FVFS.
    Ls {
        #[arg(default_value = "/")]
        path: String,
    },

    /// Read a file and print it to stdout.
    Cat { path: String },

    /// Write stdin to a file in the FVFS.
    Put { path: String },

    /// Delete a file from the FVFS.
    Rm { path: String },

    /// Show daemon status.
    Status,

    /// Inspect pending WAL entries.
    Wal,
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("fvfsc=info".parse().unwrap()))
        .init();

    let cli = Cli::parse();
    let cfg = match Config::load_or_default(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load config: {e}");
            std::process::exit(1);
        }
    };

    // Resolve fvfsd URL.
    let vfsd_url = if let Some(url) = cli.url {
        url
    } else {
        match mdns_client::discover().await {
            Some(url) => url,
            None => {
                // Fallback to localhost.
                let url = format!("http://localhost:{}", cfg.daemon.http_port);
                info!("mDNS discovery failed, falling back to {}", url);
                url
            }
        }
    };

    let client = match VfsdClient::new(&vfsd_url) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create HTTP client: {e}");
            std::process::exit(1);
        }
    };

    let cache = LocalCache::new(
        &cfg.client.local_cache_path,
        cfg.client.local_cache_gb * 1024 * 1024 * 1024,
    );
    if let Err(e) = cache.init().await {
        error!("Failed to init local cache: {e}");
    }

    match cli.command {
        Command::Mount { mountpoint } => {
            run_mount(cfg, client, cache, mountpoint).await;
        }
        Command::Ls { path } => run_ls(client, &path).await,
        Command::Cat { path } => run_cat(client, &path).await,
        Command::Put { path } => run_put(client, &path).await,
        Command::Rm { path } => run_rm(client, &path).await,
        Command::Status => run_status(client).await,
        Command::Wal => run_wal(client).await,
    }
}

// ---------------------------------------------------------------------------
// Mount

async fn run_mount(cfg: Config, client: VfsdClient, cache: LocalCache, mountpoint: Option<PathBuf>) {
    #[cfg(feature = "fuse")]
    {
        use crate::fuse_handler::fuse_impl::VfscFuse;
        let mp = mountpoint.unwrap_or_else(|| cfg.client.mount_path.clone());
        tokio::fs::create_dir_all(&mp).await.ok();
        info!("Mounting FVFS at {}", mp.display());
        let rt = tokio::runtime::Handle::current();
        let fs = VfscFuse::new(client, cache, rt);
        let options = vec![
            fuser::MountOption::RO,
            fuser::MountOption::FSName("fvfsc".into()),
            fuser::MountOption::AutoUnmount,
        ];
        // Run FUSE in a blocking thread to avoid blocking the async runtime.
        tokio::task::spawn_blocking(move || {
            if let Err(e) = fuser::mount2(fs, &mp, &options) {
                error!("FUSE mount failed: {e}");
            }
        })
        .await
        .ok();
    }
    #[cfg(not(feature = "fuse"))]
    {
        let _ = (cfg, client, cache, mountpoint);
        eprintln!("FUSE support not compiled in. Rebuild with `--features fuse`.");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Simple CLI commands

async fn run_ls(client: VfsdClient, path: &str) {
    let vfs_path = match FvfsPath::new(path) {
        Ok(p) => p,
        Err(e) => { eprintln!("Invalid path: {e}"); return; }
    };
    match client.list(&vfs_path).await {
        Ok(entries) => {
            for entry in entries {
                let kind = match entry.kind {
                    fvfs_core::EntryKind::Directory => "d",
                    fvfs_core::EntryKind::File => "-",
                };
                println!(
                    "{} {:>12} {}",
                    kind,
                    entry.size_bytes,
                    entry.path,
                );
            }
        }
        Err(e) => eprintln!("Error: {e}"),
    }
}

async fn run_cat(client: VfsdClient, path: &str) {
    let vfs_path = match FvfsPath::new(path) {
        Ok(p) => p,
        Err(e) => { eprintln!("Invalid path: {e}"); return; }
    };
    match client.get(&vfs_path).await {
        Ok(data) => {
            use std::io::Write;
            std::io::stdout().write_all(&data).ok();
        }
        Err(e) => eprintln!("Error: {e}"),
    }
}

async fn run_put(client: VfsdClient, path: &str) {
    use std::io::Read;
    let vfs_path = match FvfsPath::new(path) {
        Ok(p) => p,
        Err(e) => { eprintln!("Invalid path: {e}"); return; }
    };
    let mut buf = Vec::new();
    std::io::stdin().read_to_end(&mut buf).ok();
    match client.put(&vfs_path, bytes::Bytes::from(buf)).await {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("Error: {e}"),
    }
}

async fn run_rm(client: VfsdClient, path: &str) {
    let vfs_path = match FvfsPath::new(path) {
        Ok(p) => p,
        Err(e) => { eprintln!("Invalid path: {e}"); return; }
    };
    match client.delete(&vfs_path).await {
        Ok(()) => println!("deleted"),
        Err(e) => eprintln!("Error: {e}"),
    }
}

async fn run_status(client: VfsdClient) {
    match client.status().await {
        Ok(status) => {
            println!("fvfsd v{} — uptime {}s", status.version, status.uptime_secs);
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
        Err(e) => eprintln!("Error: {e}"),
    }
}

async fn run_wal(client: VfsdClient) {
    match client.wal().await {
        Ok(entries) => {
            if entries.is_empty() {
                println!("No pending WAL entries.");
            } else {
                println!("{} pending WAL entries:", entries.len());
                for e in &entries {
                    println!(
                        "  id={} file_id={} op={:?} attempts={} err={:?}",
                        e.id, e.file_id, e.op, e.attempts, e.last_error
                    );
                }
            }
        }
        Err(e) => eprintln!("Error: {e}"),
    }
}
