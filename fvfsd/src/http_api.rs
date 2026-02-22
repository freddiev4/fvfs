/// Axum HTTP API for fvfsd.
///
/// Routes:
///   GET    /v1/files/*path         - read file
///   PUT    /v1/files/*path         - write file
///   DELETE /v1/files/*path         - delete file
///   GET    /v1/files/*path/meta    - get metadata
///   GET    /v1/ls/*prefix          - list directory
///   GET    /v1/status              - daemon health + tier stats
///   GET    /v1/devices             - known devices
///   POST   /v1/admin/evict         - trigger eviction
///   POST   /v1/admin/flush         - trigger S3 flush
///   GET    /v1/admin/wal           - inspect WAL
use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::instrument;

use fvfs_core::metadata::MetadataStore;
use fvfs_core::{DaemonStatus, DeviceInfo, FileEntry, TierStats, FvfsError, FvfsPath, WalEntry};

use crate::router::TierRouter;

// ---------------------------------------------------------------------------
// Shared app state

pub struct AppState {
    pub router: Arc<TierRouter>,
    pub meta: MetadataStore,
    pub evict_notify: Arc<Notify>,
    pub flush_notify: Arc<Notify>,
    pub start_time: std::time::Instant,
}

impl AppState {
    pub fn new(
        router: Arc<TierRouter>,
        meta: MetadataStore,
        evict_notify: Arc<Notify>,
        flush_notify: Arc<Notify>,
    ) -> Self {
        AppState {
            router,
            meta,
            evict_notify,
            flush_notify,
            start_time: std::time::Instant::now(),
        }
    }
}

pub type SharedState = Arc<AppState>;

// ---------------------------------------------------------------------------
// Error response helper

struct ApiError(FvfsError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self.0 {
            FvfsError::NotFound { path } => (
                StatusCode::NOT_FOUND,
                format!("not found: {}", path),
            ),
            FvfsError::AlreadyExists { path } => (
                StatusCode::CONFLICT,
                format!("already exists: {}", path),
            ),
            FvfsError::IsADirectory { path } => (
                StatusCode::BAD_REQUEST,
                format!("is a directory: {}", path),
            ),
            FvfsError::NotADirectory { path } => (
                StatusCode::BAD_REQUEST,
                format!("not a directory: {}", path),
            ),
            FvfsError::PermissionDenied(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                self.0.to_string(),
            ),
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

impl From<FvfsError> for ApiError {
    fn from(e: FvfsError) -> Self {
        ApiError(e)
    }
}

// ---------------------------------------------------------------------------
// Router builder

pub fn build_router(state: SharedState) -> Router {
    Router::new()
        // File operations
        .route("/v1/files/*path", get(handle_get_file))
        .route("/v1/files/*path", put(handle_put_file))
        .route("/v1/files/*path", delete(handle_delete_file))
        .route("/v1/meta/*path", get(handle_get_meta))
        // Directory listing
        .route("/v1/ls/*prefix", get(handle_list))
        .route("/v1/ls/", get(handle_list_root))
        // Status & admin
        .route("/v1/status", get(handle_status))
        .route("/v1/devices", get(handle_devices))
        .route("/v1/admin/evict", post(handle_admin_evict))
        .route("/v1/admin/flush", post(handle_admin_flush))
        .route("/v1/admin/wal", get(handle_admin_wal))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// File handlers

#[instrument(skip(state, body))]
async fn handle_put_file(
    State(state): State<SharedState>,
    Path(path): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let vfs_path = parse_path(&path)?;
    state.router.write(&vfs_path, body).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip(state))]
async fn handle_get_file(
    State(state): State<SharedState>,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let vfs_path = parse_path(&path)?;
    let data = state.router.read(&vfs_path).await?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", data.len().to_string())
        .body(Body::from(data))
        .unwrap())
}

#[instrument(skip(state))]
async fn handle_delete_file(
    State(state): State<SharedState>,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let vfs_path = parse_path(&path)?;
    state.router.delete(&vfs_path).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[instrument(skip(state))]
async fn handle_get_meta(
    State(state): State<SharedState>,
    Path(path): Path<String>,
) -> Result<Json<fvfs_core::FileMetadata>, ApiError> {
    let vfs_path = parse_path(&path)?;
    let meta = state.router.stat(&vfs_path).await?;
    Ok(Json(meta))
}

// ---------------------------------------------------------------------------
// Directory listing

async fn handle_list(
    State(state): State<SharedState>,
    Path(prefix): Path<String>,
) -> Result<Json<Vec<FileEntry>>, ApiError> {
    let vfs_path = parse_path(&prefix)?;
    list_entries(state, vfs_path).await
}

async fn handle_list_root(
    State(state): State<SharedState>,
) -> Result<Json<Vec<FileEntry>>, ApiError> {
    let vfs_path = FvfsPath::new("/").map_err(ApiError::from)?;
    list_entries(state, vfs_path).await
}

async fn list_entries(
    state: SharedState,
    path: FvfsPath,
) -> Result<Json<Vec<FileEntry>>, ApiError> {
    let entries = state.router.list(&path).await?;
    Ok(Json(entries))
}

// ---------------------------------------------------------------------------
// Status

async fn handle_status(State(state): State<SharedState>) -> Json<DaemonStatus> {
    let uptime = state.start_time.elapsed().as_secs();
    let meta = state.meta.clone();

    let local_bytes = tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.bytes_on_tier(fvfs_core::Tier::Local.bitmask())
    })
    .await
    .ok()
    .and_then(|r| r.ok())
    .unwrap_or(0);

    let local_count = tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.count_on_tier(fvfs_core::Tier::Local.bitmask())
    })
    .await
    .ok()
    .and_then(|r| r.ok())
    .unwrap_or(0);

    let nas_bytes = tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.bytes_on_tier(fvfs_core::Tier::Nas.bitmask())
    })
    .await
    .ok()
    .and_then(|r| r.ok())
    .unwrap_or(0);

    let nas_count = tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.count_on_tier(fvfs_core::Tier::Nas.bitmask())
    })
    .await
    .ok()
    .and_then(|r| r.ok())
    .unwrap_or(0);

    let s3_bytes = tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.bytes_on_tier(fvfs_core::Tier::S3.bitmask())
    })
    .await
    .ok()
    .and_then(|r| r.ok())
    .unwrap_or(0);

    let s3_count = tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.count_on_tier(fvfs_core::Tier::S3.bitmask())
    })
    .await
    .ok()
    .and_then(|r| r.ok())
    .unwrap_or(0);

    let wal_pending = tokio::task::spawn_blocking({
        let m = meta.clone();
        move || m.wal_pending().map(|v| v.len() as u64)
    })
    .await
    .ok()
    .and_then(|r| r.ok())
    .unwrap_or(0);

    Json(DaemonStatus {
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: uptime,
        tiers: vec![
            TierStats {
                name: "local".into(),
                files: local_count,
                bytes: local_bytes,
            },
            TierStats {
                name: "nas".into(),
                files: nas_count,
                bytes: nas_bytes,
            },
            TierStats {
                name: "s3".into(),
                files: s3_count,
                bytes: s3_bytes,
            },
        ],
        wal_pending,
    })
}

// ---------------------------------------------------------------------------
// Device listing

async fn handle_devices(State(state): State<SharedState>) -> Result<Json<Vec<DeviceInfo>>, ApiError> {
    let meta = state.meta.clone();
    let devices = tokio::task::spawn_blocking(move || meta.list_devices())
        .await
        .map_err(|e| ApiError(FvfsError::Other(anyhow::anyhow!("{e}"))))?
        .map_err(ApiError::from)?;
    Ok(Json(devices))
}

// ---------------------------------------------------------------------------
// Admin handlers

async fn handle_admin_evict(State(state): State<SharedState>) -> StatusCode {
    state.evict_notify.notify_one();
    StatusCode::ACCEPTED
}

async fn handle_admin_flush(State(state): State<SharedState>) -> StatusCode {
    state.flush_notify.notify_one();
    StatusCode::ACCEPTED
}

async fn handle_admin_wal(
    State(state): State<SharedState>,
) -> Result<Json<Vec<WalEntry>>, ApiError> {
    let meta = state.meta.clone();
    let entries = tokio::task::spawn_blocking(move || meta.wal_pending())
        .await
        .map_err(|e| ApiError(FvfsError::Other(anyhow::anyhow!("{e}"))))?
        .map_err(ApiError::from)?;
    Ok(Json(entries))
}

// ---------------------------------------------------------------------------
// Path helper

fn parse_path(raw: &str) -> Result<FvfsPath, ApiError> {
    FvfsPath::new(format!("/{}", raw.trim_start_matches('/'))).map_err(ApiError::from)
}
