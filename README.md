# fvfs — Freddie's Virtual Filesystem

A personal distributed virtual filesystem written in Rust. Unifies storage across a Mac mini (local disk), a NAS on the local network, and AWS S3 — presenting them as a single coherent filesystem to any device on the network.

Inspired by turbopuffer's object-storage-first tiered architecture and Modal Labs' FUSE-backed content-addressed virtual filesystems, scoped to personal infrastructure.

```
┌─────────────────────────────────────────────────────┐
│                   Any device on LAN                  │
│   (PC / laptop)  ──── fvfsc ──── FUSE mount         │
└─────────────────────────┬───────────────────────────┘
                          │ HTTP / mDNS
┌─────────────────────────▼───────────────────────────┐
│                      Mac mini                        │
│  fvfsd daemon                                        │
│  ├── Tier 0 (hot)   local disk  ~1 ms               │
│  ├── Tier 1 (warm)  NAS (LAN)   ~5–20 ms            │
│  └── Tier 2 (cold)  AWS S3      ~100–500 ms  ←SoT   │
│                                                      │
│  SQLite metadata index (WAL mode)                    │
└─────────────────────────────────────────────────────┘
```

## Features

- **Single unified mount point** accessible from any device on the local network via `fvfsc`
- **Automatic tiering** — hot data on local disk, warm on NAS, everything durably in S3 (source of truth)
- **Write pipeline** — local write is synchronous; NAS replication and S3 upload are WAL-backed async
- **Crash-safe WAL** — `wal_pending` table survives daemon restarts; replayed on startup with exponential backoff
- **Byte-based cache eviction** — ARC-inspired recency + frequency score, per-tier high/low watermarks
- **Zero-config device discovery** — mDNS/DNS-SD (`_fvfs._tcp.local.`) via `mdns-sd`
- **HTTP REST API** — all operations accessible without FUSE
- **Optional FUSE mount** — requires `libfuse3-dev`; enabled via `--features fuse`
- **One-time migration** — `fvfsd migrate` crawls existing data, deduplicates by SHA-256, uploads to S3
- **Pluggable backends** — `StorageBackend` async trait; add Backblaze B2, yts3, etc. without touching routing

## Workspace layout

```
fvfs/
├── fvfs-core/          Shared library: types, backends, metadata store, eviction, WAL
│   └── src/
│       ├── types.rs    FvfsPath, Tier, TierBitmask, FileMetadata, WalEntry, …
│       ├── config.rs   TOML config structs
│       ├── error.rs    FvfsError / Result
│       ├── eviction.rs EvictionPolicy trait + ArcInspiredPolicy
│       ├── metadata.rs MetadataStore (SQLite, WAL mode)
│       └── backend/
│           ├── mod.rs  StorageBackend async trait
│           ├── local.rs LocalDiskBackend (tokio::fs)
│           ├── nas.rs  NasBackend (wraps LocalDiskBackend at NAS mount)
│           └── s3.rs   S3Backend (aws-sdk-s3)
│
├── fvfsd/              Mac mini daemon binary
│   └── src/
│       ├── main.rs         CLI: serve / migrate / status
│       ├── router.rs       TierRouter — write/read/delete/evict across tiers
│       ├── http_api.rs     axum HTTP API (/v1/*)
│       ├── wal_replay.rs   Startup WAL replay with backoff
│       ├── s3_uploader.rs  Background S3 flush task
│       ├── eviction_task.rs  Watermark-based eviction loop
│       ├── mdns_server.rs  mDNS service registration
│       ├── migrate.rs      Parallel SHA-256 crawl + S3 upload
│       └── fuse_handler.rs FUSE mount (optional, --features fuse)
│
├── fvfsc/              Client binary (any LAN device)
│   └── src/
│       ├── main.rs         CLI: mount / ls / cat / put / rm / status / wal
│       ├── http_client.rs  Typed reqwest client for all fvfsd endpoints
│       ├── local_cache.rs  SHA-256 content-addressed read cache
│       ├── mdns_client.rs  mDNS discovery with timeout + fallback
│       └── fuse_handler.rs FUSE proxy (optional, --features fuse)
│
└── config.example.toml  Annotated reference configuration
```

## Build

**Without FUSE** (default — no system dependencies beyond Rust):
```bash
cargo build --release
```

**With FUSE** (requires `libfuse3-dev` on Linux, macFUSE on macOS):
```bash
# Linux
sudo apt install libfuse3-dev pkg-config
# macOS
brew install macfuse

cargo build --release --features fuse
```

Binaries land in `target/release/fvfsd` and `target/release/fvfsc`.

## Configuration

Copy and edit the example config:
```bash
sudo mkdir -p /etc/fvfsd
sudo cp config.example.toml /etc/fvfsd/config.toml
$EDITOR /etc/fvfsd/config.toml
```

Key sections:

```toml
[daemon]
mount_path  = "/mnt/fvfs"      # FUSE mount point on the Mac mini
http_port   = 7734
metadata_db = "/var/fvfsd/meta.db"

[tiers.local]
path              = "/Users/freddie/fvfs-cache"
high_watermark_gb = 200
low_watermark_gb  = 150

[tiers.nas]
path              = "/Volumes/NAS/fvfs"   # must already be mounted via SMB/NFS
high_watermark_gb = 2000
low_watermark_gb  = 1500

[tiers.s3]
bucket = "freddie-fvfs"
region = "us-east-1"
prefix = "fvfs/"

[upload]
flush_size_mb       = 256   # flush to S3 when this many MB are pending
flush_interval_secs = 300   # also flush every 5 minutes

[eviction]
interval_secs    = 600
recency_weight   = 0.7
frequency_weight = 0.3
```

AWS credentials are resolved via the standard chain (`~/.aws/credentials`, `AWS_PROFILE`, instance role, etc.).

## Running fvfsd (Mac mini)

```bash
# Start the daemon (HTTP API on :7734, optional FUSE at mount_path)
fvfsd --config /etc/fvfsd/config.toml serve

# Check status
fvfsd status

# One-time migration of existing data
fvfsd migrate --local-src /Users/freddie/Documents --nas-src /Volumes/NAS/data
```

The daemon registers itself as `_fvfs._tcp.local.` via mDNS so client devices find it automatically.

## Running fvfsc (any LAN device)

```bash
# Mount (requires --features fuse build)
fvfsc mount /mnt/fvfs

# Or use the CLI without mounting
fvfsc ls /photos/2024
fvfsc cat /notes/todo.txt
echo "hello" | fvfsc put /notes/hello.txt
fvfsc rm /old/file.bin

# Daemon health
fvfsc status

# Inspect pending WAL entries
fvfsc wal
```

`fvfsc` discovers `fvfsd` automatically via mDNS (10 s timeout, falls back to `localhost:7734`). Override with:
```bash
VFSD_URL=http://192.168.1.10:7734 fvfsc ls /
```

## HTTP API

`fvfsd` exposes a REST API on port 7734:

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/v1/files/*path` | Write a file |
| `GET` | `/v1/files/*path` | Read a file |
| `DELETE` | `/v1/files/*path` | Delete a file |
| `GET` | `/v1/meta/*path` | Get file metadata |
| `GET` | `/v1/ls/*prefix` | List directory |
| `GET` | `/v1/status` | Daemon health + tier stats |
| `GET` | `/v1/devices` | Known devices on the network |
| `POST` | `/v1/admin/evict` | Manually trigger eviction |
| `POST` | `/v1/admin/flush` | Manually trigger S3 flush |
| `GET` | `/v1/admin/wal` | Inspect pending WAL entries |

## Tier bitmask

Each file's `tier_bitmask` field records which tiers currently hold a copy:

| Bit | Value | Tier |
|-----|-------|------|
| 0 | `0x1` | Local disk (Mac mini) |
| 1 | `0x2` | NAS |
| 2 | `0x4` | S3 |

A file with `tier_bitmask = 0x7` is fully replicated across all tiers. S3 is never evicted.

## Eviction scoring

```
score = (recency_weight / seconds_since_access) + (frequency_weight × access_count_30d)
```

Lower score = evict first. A file is only evicted from a tier when it is confirmed present on all colder tiers (i.e. the S3 bit is set).

## Adding a new storage backend

Implement the `StorageBackend` trait in `fvfs-core/src/backend/`:

```rust
#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn put(&self, path: &FvfsPath, data: Bytes) -> Result<()>;
    async fn get(&self, path: &FvfsPath) -> Result<Bytes>;
    async fn delete(&self, path: &FvfsPath) -> Result<()>;
    async fn exists(&self, path: &FvfsPath) -> Result<bool>;
    async fn list(&self, prefix: &FvfsPath) -> Result<Vec<FileEntry>>;
    async fn metadata(&self, path: &FvfsPath) -> Result<FileEntry>;
    fn tier(&self) -> Tier;
}
```

Then wire the new backend into `TierRouter` in `fvfsd/src/router.rs`.

## Design decisions (v1)

- **NAS as local path** — the NAS is mounted via SMB/NFS and `NasBackend` reuses `LocalDiskBackend` pointed at the mount. Simpler than a custom TCP protocol.
- **Single writer** — `fvfsd` on the Mac mini is the sole writer. No distributed locking needed.
- **SQLite WAL** — metadata lives in a single SQLite database in WAL journal mode. Fast reads, safe concurrent access, trivially backed up.
- **S3 as source of truth** — S3 is never evicted. If local and NAS copies are lost, the file is recoverable from S3.
- **FUSE optional** — the HTTP API is always available. FUSE is a convenience layer that requires system libraries.
