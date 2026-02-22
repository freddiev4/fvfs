use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, VfsError};

/// A normalized, absolute, unix-style virtual filesystem path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct VfsPath(String);

impl VfsPath {
    /// Create a new VfsPath, normalizing and validating the input.
    pub fn new(raw: impl Into<String>) -> Result<Self> {
        let s = raw.into();
        let normalized = Self::normalize(&s)?;
        Ok(VfsPath(normalized))
    }

    /// Get the raw path string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parent path. Returns None for root "/".
    pub fn parent(&self) -> Option<VfsPath> {
        if self.0 == "/" {
            return None;
        }
        let idx = self.0.rfind('/')?;
        if idx == 0 {
            Some(VfsPath("/".to_string()))
        } else {
            Some(VfsPath(self.0[..idx].to_string()))
        }
    }

    /// File name component (last segment).
    pub fn file_name(&self) -> &str {
        if self.0 == "/" {
            return "/";
        }
        self.0.rsplit('/').next().unwrap_or(&self.0)
    }

    /// Check whether this path is a prefix of (or equal to) `other`.
    pub fn is_prefix_of(&self, other: &VfsPath) -> bool {
        if self.0 == "/" {
            return true;
        }
        other.0.starts_with(&self.0)
            && (other.0.len() == self.0.len()
                || other.0.as_bytes().get(self.0.len()) == Some(&b'/'))
    }

    /// Join a path segment.
    pub fn join(&self, segment: &str) -> Result<VfsPath> {
        let raw = if self.0 == "/" {
            format!("/{}", segment)
        } else {
            format!("{}/{}", self.0, segment)
        };
        VfsPath::new(raw)
    }

    fn normalize(s: &str) -> Result<String> {
        if s.is_empty() {
            return Err(VfsError::InvalidPath("path cannot be empty".into()));
        }
        // Ensure absolute path
        let s = if s.starts_with('/') {
            s.to_string()
        } else {
            format!("/{}", s)
        };

        // Resolve . and .. components
        let mut components: Vec<&str> = Vec::new();
        for part in s.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    components.pop();
                }
                c => {
                    if c.contains('\0') {
                        return Err(VfsError::InvalidPath(
                            "path cannot contain null bytes".into(),
                        ));
                    }
                    components.push(c);
                }
            }
        }

        if components.is_empty() {
            Ok("/".to_string())
        } else {
            Ok(format!("/{}", components.join("/")))
        }
    }
}

impl fmt::Display for VfsPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<VfsPath> for String {
    fn from(p: VfsPath) -> Self {
        p.0
    }
}

// ---------------------------------------------------------------------------
// Tier

/// Storage tier: how hot the data is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    /// Hot — Mac mini local disk.
    Local = 0,
    /// Warm — NAS on local network.
    Nas = 1,
    /// Cold (source of truth) — AWS S3.
    S3 = 2,
}

impl Tier {
    pub fn bitmask(self) -> u8 {
        1 << (self as u8)
    }

    pub fn label(&self) -> &'static str {
        match self {
            Tier::Local => "local",
            Tier::Nas => "nas",
            Tier::S3 => "s3",
        }
    }
}

impl fmt::Display for Tier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

// ---------------------------------------------------------------------------
// TierBitmask

/// Encodes which tiers currently hold a copy of a file.
///
/// bit 0 (0x1) = local disk
/// bit 1 (0x2) = NAS
/// bit 2 (0x4) = S3
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TierBitmask(pub u8);

impl TierBitmask {
    pub const NONE: TierBitmask = TierBitmask(0);
    pub const ALL: TierBitmask = TierBitmask(0x07);

    pub fn set(&mut self, tier: Tier) {
        self.0 |= tier.bitmask();
    }

    pub fn clear(&mut self, tier: Tier) {
        self.0 &= !tier.bitmask();
    }

    pub fn has(&self, tier: Tier) -> bool {
        self.0 & tier.bitmask() != 0
    }

    pub fn is_fully_replicated(&self) -> bool {
        self.0 & 0x07 == 0x07
    }

    /// Returns true if the file is present on all tiers below (colder than) `tier`.
    pub fn safe_to_evict_from(&self, tier: Tier) -> bool {
        match tier {
            Tier::Local => self.has(Tier::S3),
            Tier::Nas => self.has(Tier::S3),
            Tier::S3 => false, // S3 is never evicted
        }
    }

    pub fn as_u8(self) -> u8 {
        self.0
    }
}

impl From<u8> for TierBitmask {
    fn from(v: u8) -> Self {
        TierBitmask(v)
    }
}

impl From<i64> for TierBitmask {
    fn from(v: i64) -> Self {
        TierBitmask(v as u8)
    }
}

// ---------------------------------------------------------------------------
// FileEntry / FileMetadata

/// Kind of filesystem entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    File,
    Directory,
}

/// Rich metadata for a single filesystem entry (file or directory).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub id: i64,
    pub path: VfsPath,
    pub kind: EntryKind,
    pub size_bytes: u64,
    pub sha256: String,
    pub tier_bitmask: TierBitmask,
    pub created_at: i64,
    pub modified_at: i64,
    pub accessed_at: i64,
    pub access_count_30d: u32,
    pub mime_type: Option<String>,
}

impl FileMetadata {
    pub fn new_file(path: VfsPath, size_bytes: u64, sha256: String) -> Self {
        let now = now_unix();
        FileMetadata {
            id: 0,
            path,
            kind: EntryKind::File,
            size_bytes,
            sha256,
            tier_bitmask: TierBitmask::NONE,
            created_at: now,
            modified_at: now,
            accessed_at: now,
            access_count_30d: 0,
            mime_type: None,
        }
    }

    pub fn new_dir(path: VfsPath) -> Self {
        let now = now_unix();
        FileMetadata {
            id: 0,
            path,
            kind: EntryKind::Directory,
            size_bytes: 0,
            sha256: String::new(),
            tier_bitmask: TierBitmask::ALL, // dirs are "everywhere"
            created_at: now,
            modified_at: now,
            accessed_at: now,
            access_count_30d: 0,
            mime_type: Some("inode/directory".into()),
        }
    }

    pub fn is_dir(&self) -> bool {
        self.kind == EntryKind::Directory
    }

    pub fn is_file(&self) -> bool {
        self.kind == EntryKind::File
    }
}

/// Lightweight listing entry returned by `list` operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: VfsPath,
    pub kind: EntryKind,
    pub size_bytes: u64,
    pub modified_at: i64,
    pub tier_bitmask: TierBitmask,
}

impl From<&FileMetadata> for FileEntry {
    fn from(m: &FileMetadata) -> Self {
        FileEntry {
            path: m.path.clone(),
            kind: m.kind.clone(),
            size_bytes: m.size_bytes,
            modified_at: m.modified_at,
            tier_bitmask: m.tier_bitmask,
        }
    }
}

// ---------------------------------------------------------------------------
// WAL operation types

/// Pending WAL operation kinds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalOp {
    ReplicateNas,
    UploadS3,
}

impl WalOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            WalOp::ReplicateNas => "replicate_nas",
            WalOp::UploadS3 => "upload_s3",
        }
    }
}

impl TryFrom<&str> for WalOp {
    type Error = VfsError;
    fn try_from(s: &str) -> Result<Self> {
        match s {
            "replicate_nas" => Ok(WalOp::ReplicateNas),
            "upload_s3" => Ok(WalOp::UploadS3),
            other => Err(VfsError::Wal(format!("unknown WAL op: {other}"))),
        }
    }
}

/// A row from the `wal_pending` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub id: i64,
    pub file_id: i64,
    pub op: WalOp,
    pub enqueued_at: i64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

// ---------------------------------------------------------------------------
// Device info

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub id: i64,
    pub name: String,
    pub mdns_name: String,
    pub last_seen_at: i64,
    pub ip: String,
}

// ---------------------------------------------------------------------------
// Daemon status

#[derive(Debug, Serialize, Deserialize)]
pub struct TierStats {
    pub name: String,
    pub files: u64,
    pub bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub version: String,
    pub uptime_secs: u64,
    pub tiers: Vec<TierStats>,
    pub wal_pending: u64,
}

// ---------------------------------------------------------------------------
// Helpers

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
