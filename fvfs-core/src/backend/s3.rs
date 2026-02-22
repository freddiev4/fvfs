use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use tracing::{debug, instrument};

use crate::backend::StorageBackend;
use crate::error::{Result, VfsError};
use crate::types::{EntryKind, FileEntry, Tier, TierBitmask, VfsPath};

/// S3 cold-tier backend — the source of truth. Data here is never evicted.
#[derive(Clone)]
pub struct S3Backend {
    client: S3Client,
    bucket: String,
    prefix: String,
}

impl S3Backend {
    pub async fn new(bucket: String, region: String, prefix: String) -> Result<Self> {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region))
            .load()
            .await;
        let client = S3Client::new(&config);
        Ok(S3Backend {
            client,
            bucket,
            prefix,
        })
    }

    /// Convert a VfsPath to an S3 object key.
    fn s3_key(&self, path: &VfsPath) -> String {
        let rel = path.as_str().trim_start_matches('/');
        if self.prefix.is_empty() {
            rel.to_string()
        } else {
            format!("{}{}", self.prefix.trim_end_matches('/'), if rel.is_empty() { "".to_string() } else { format!("/{}", rel) })
        }
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    #[instrument(skip(self, data), fields(tier = "s3", path = %path))]
    async fn put(&self, path: &VfsPath, data: Bytes) -> Result<()> {
        let key = self.s3_key(path);
        let len = data.len() as i64;
        debug!(bucket = %self.bucket, key = %key, bytes = len, "s3 put");
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .content_length(len)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| VfsError::S3(format!("put_object: {e}")))?;
        Ok(())
    }

    #[instrument(skip(self), fields(tier = "s3", path = %path))]
    async fn get(&self, path: &VfsPath) -> Result<Bytes> {
        let key = self.s3_key(path);
        debug!(bucket = %self.bucket, key = %key, "s3 get");
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                // Map NoSuchKey to NotFound
                if e.to_string().contains("NoSuchKey") || e.to_string().contains("404") {
                    VfsError::NotFound {
                        path: path.to_string(),
                    }
                } else {
                    VfsError::S3(format!("get_object: {e}"))
                }
            })?;

        let body = resp
            .body
            .collect()
            .await
            .map_err(|e| VfsError::S3(format!("collecting body: {e}")))?;
        Ok(body.into_bytes())
    }

    #[instrument(skip(self), fields(tier = "s3", path = %path))]
    async fn delete(&self, path: &VfsPath) -> Result<()> {
        let key = self.s3_key(path);
        debug!(bucket = %self.bucket, key = %key, "s3 delete");
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| VfsError::S3(format!("delete_object: {e}")))?;
        Ok(())
    }

    async fn exists(&self, path: &VfsPath) -> Result<bool> {
        let key = self.s3_key(path);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let s = e.to_string();
                if s.contains("NoSuchKey") || s.contains("404") {
                    Ok(false)
                } else {
                    Err(VfsError::S3(format!("head_object: {e}")))
                }
            }
        }
    }

    #[instrument(skip(self), fields(tier = "s3", prefix = %prefix))]
    async fn list(&self, prefix: &VfsPath) -> Result<Vec<FileEntry>> {
        let key_prefix = self.s3_key(prefix);
        let prefix_str = if key_prefix == "/" || key_prefix.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/", key_prefix.trim_end_matches('/'))
        };

        let mut entries = Vec::new();
        let mut continuation: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix_str);
            if let Some(tok) = continuation {
                req = req.continuation_token(tok);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| VfsError::S3(format!("list_objects_v2: {e}")))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    // Strip the backend prefix to get back the VFS path.
                    let rel = if self.prefix.is_empty() {
                        key.to_string()
                    } else {
                        key.strip_prefix(self.prefix.trim_end_matches('/'))
                            .unwrap_or(key)
                            .trim_start_matches('/')
                            .to_string()
                    };
                    let vfs_path = VfsPath::new(format!("/{}", rel))
                        .unwrap_or_else(|_| VfsPath::new("/unknown").unwrap());
                    let modified_at = obj
                        .last_modified()
                        .and_then(|t| t.secs().try_into().ok())
                        .unwrap_or(0);
                    let mut bm = TierBitmask::default();
                    bm.set(Tier::S3);
                    entries.push(FileEntry {
                        path: vfs_path,
                        kind: EntryKind::File,
                        size_bytes: obj.size().unwrap_or(0) as u64,
                        modified_at,
                        tier_bitmask: bm,
                    });
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(entries)
    }

    #[instrument(skip(self), fields(tier = "s3", path = %path))]
    async fn metadata(&self, path: &VfsPath) -> Result<FileEntry> {
        let key = self.s3_key(path);
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                let s = e.to_string();
                if s.contains("NoSuchKey") || s.contains("404") {
                    VfsError::NotFound {
                        path: path.to_string(),
                    }
                } else {
                    VfsError::S3(format!("head_object: {e}"))
                }
            })?;

        let modified_at = resp
            .last_modified()
            .and_then(|t| t.secs().try_into().ok())
            .unwrap_or(0);
        let mut bm = TierBitmask::default();
        bm.set(Tier::S3);
        Ok(FileEntry {
            path: path.clone(),
            kind: EntryKind::File,
            size_bytes: resp.content_length().unwrap_or(0) as u64,
            modified_at,
            tier_bitmask: bm,
        })
    }

    fn tier(&self) -> Tier {
        Tier::S3
    }
}
