use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::{Result, FvfsError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub daemon: DaemonConfig,
    pub tiers: TiersConfig,
    #[serde(default)]
    pub upload: UploadConfig,
    #[serde(default)]
    pub eviction: EvictionConfig,
    #[serde(default)]
    pub client: ClientConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    pub mount_path: PathBuf,
    pub http_port: u16,
    pub metadata_db: PathBuf,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        DaemonConfig {
            mount_path: PathBuf::from("/mnt/fvfs"),
            http_port: 7734,
            metadata_db: PathBuf::from("/var/fvfsd/meta.db"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TiersConfig {
    pub local: LocalTierConfig,
    pub nas: NasTierConfig,
    pub s3: S3TierConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalTierConfig {
    pub path: PathBuf,
    pub high_watermark_gb: u64,
    pub low_watermark_gb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NasTierConfig {
    pub path: PathBuf,
    pub high_watermark_gb: u64,
    pub low_watermark_gb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3TierConfig {
    pub bucket: String,
    pub region: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadConfig {
    /// Flush to S3 when accumulated size exceeds this (MB).
    pub flush_size_mb: u64,
    /// Flush to S3 every this many seconds, regardless of size.
    pub flush_interval_secs: u64,
}

impl Default for UploadConfig {
    fn default() -> Self {
        UploadConfig {
            flush_size_mb: 256,
            flush_interval_secs: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictionConfig {
    /// Run eviction every N seconds.
    pub interval_secs: u64,
    pub recency_weight: f64,
    pub frequency_weight: f64,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        EvictionConfig {
            interval_secs: 600,
            recency_weight: 0.7,
            frequency_weight: 0.3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub mount_path: PathBuf,
    pub local_cache_path: PathBuf,
    pub local_cache_gb: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            mount_path: PathBuf::from("/mnt/fvfs"),
            local_cache_path: PathBuf::from("/tmp/fvfsc-cache"),
            local_cache_gb: 20,
        }
    }
}

impl Config {
    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let contents = std::fs::read_to_string(path.as_ref())
            .map_err(|e| FvfsError::Config(format!("reading config: {e}")))?;
        toml::from_str(&contents).map_err(|e| FvfsError::Config(format!("parsing config: {e}")))
    }

    /// Load from a file if it exists, otherwise return a default configuration.
    pub fn load_or_default(path: impl AsRef<Path>) -> Result<Self> {
        if path.as_ref().exists() {
            Self::from_file(path)
        } else {
            Ok(Self::default_config())
        }
    }

    fn default_config() -> Self {
        Config {
            daemon: DaemonConfig::default(),
            tiers: TiersConfig {
                local: LocalTierConfig {
                    path: PathBuf::from("/tmp/fvfs-local"),
                    high_watermark_gb: 200,
                    low_watermark_gb: 150,
                },
                nas: NasTierConfig {
                    path: PathBuf::from("/tmp/fvfs-nas"),
                    high_watermark_gb: 2000,
                    low_watermark_gb: 1500,
                },
                s3: S3TierConfig {
                    bucket: "freddie-fvfs".into(),
                    region: "us-east-1".into(),
                    prefix: "fvfs/".into(),
                },
            },
            upload: UploadConfig::default(),
            eviction: EvictionConfig::default(),
            client: ClientConfig::default(),
        }
    }
}
