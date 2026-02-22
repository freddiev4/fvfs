/// HTTP client wrapper for communicating with vfsd.
use bytes::Bytes;
use reqwest::{Client, StatusCode, Url};
use serde::de::DeserializeOwned;
use tracing::instrument;

use fvfs_core::{DaemonStatus, FileEntry, FileMetadata, VfsError, VfsPath, WalEntry};

#[derive(Clone)]
pub struct VfsdClient {
    client: Client,
    base_url: Url,
}

impl VfsdClient {
    pub fn new(base_url: &str) -> Result<Self, anyhow::Error> {
        let base = Url::parse(base_url)?;
        Ok(VfsdClient {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
            base_url: base,
        })
    }

    fn url(&self, path: &str) -> Url {
        self.base_url
            .join(path.trim_start_matches('/'))
            .unwrap_or_else(|_| self.base_url.clone())
    }

    // -----------------------------------------------------------------------
    // File operations

    #[instrument(skip(self, data), fields(path = %path))]
    pub async fn put(&self, path: &VfsPath, data: Bytes) -> Result<(), VfsError> {
        let url = self.url(&format!("/v1/files{}", path.as_str()));
        let resp = self
            .client
            .put(url)
            .header("Content-Type", "application/octet-stream")
            .body(data)
            .send()
            .await
            .map_err(|e| VfsError::Other(anyhow::anyhow!("HTTP put: {e}")))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(map_http_error(resp.status(), path))
        }
    }

    #[instrument(skip(self), fields(path = %path))]
    pub async fn get(&self, path: &VfsPath) -> Result<Bytes, VfsError> {
        let url = self.url(&format!("/v1/files{}", path.as_str()));
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| VfsError::Other(anyhow::anyhow!("HTTP get: {e}")))?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Err(VfsError::NotFound {
                path: path.to_string(),
            });
        }
        if !resp.status().is_success() {
            return Err(map_http_error(resp.status(), path));
        }

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| VfsError::Other(anyhow::anyhow!("reading body: {e}")))?;
        Ok(bytes)
    }

    #[instrument(skip(self), fields(path = %path))]
    pub async fn delete(&self, path: &VfsPath) -> Result<(), VfsError> {
        let url = self.url(&format!("/v1/files{}", path.as_str()));
        let resp = self
            .client
            .delete(url)
            .send()
            .await
            .map_err(|e| VfsError::Other(anyhow::anyhow!("HTTP delete: {e}")))?;

        if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(map_http_error(resp.status(), path))
        }
    }

    pub async fn stat(&self, path: &VfsPath) -> Result<FileMetadata, VfsError> {
        self.get_json(&format!("/v1/meta{}", path.as_str()), path)
            .await
    }

    pub async fn list(&self, prefix: &VfsPath) -> Result<Vec<FileEntry>, VfsError> {
        self.get_json(&format!("/v1/ls{}", prefix.as_str()), prefix)
            .await
    }

    pub async fn status(&self) -> Result<DaemonStatus, VfsError> {
        let url = self.url("/v1/status");
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| VfsError::Other(anyhow::anyhow!("HTTP status: {e}")))?;
        let data: reqwest::Result<DaemonStatus> = resp.json().await;
        data.map_err(|e| VfsError::Other(anyhow::anyhow!("parse status: {e}")))
    }

    pub async fn wal(&self) -> Result<Vec<WalEntry>, VfsError> {
        let url = self.url("/v1/admin/wal");
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| VfsError::Other(anyhow::anyhow!("HTTP wal: {e}")))?;
        resp.json::<Vec<WalEntry>>()
            .await
            .map_err(|e| VfsError::Other(anyhow::anyhow!("parse wal: {e}")))
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        api_path: &str,
        vfs_path: &VfsPath,
    ) -> Result<T, VfsError> {
        let url = self.url(api_path);
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| VfsError::Other(anyhow::anyhow!("HTTP: {e}")))?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Err(VfsError::NotFound {
                path: vfs_path.to_string(),
            });
        }
        if !resp.status().is_success() {
            return Err(map_http_error(resp.status(), vfs_path));
        }

        resp.json::<T>()
            .await
            .map_err(|e| VfsError::Other(anyhow::anyhow!("parse response: {e}")))
    }
}

fn map_http_error(status: StatusCode, path: &VfsPath) -> VfsError {
    match status {
        StatusCode::NOT_FOUND => VfsError::NotFound {
            path: path.to_string(),
        },
        StatusCode::CONFLICT => VfsError::AlreadyExists {
            path: path.to_string(),
        },
        StatusCode::FORBIDDEN => VfsError::PermissionDenied(path.to_string()),
        StatusCode::BAD_REQUEST => VfsError::InvalidPath(path.to_string()),
        _ => VfsError::Other(anyhow::anyhow!("HTTP {}", status)),
    }
}
