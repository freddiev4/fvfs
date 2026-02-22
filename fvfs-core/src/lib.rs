pub mod backend;
pub mod config;
pub mod error;
pub mod eviction;
pub mod metadata;
pub mod types;

pub use error::{Result, VfsError};
pub use types::{
    DaemonStatus, DeviceInfo, EntryKind, FileEntry, FileMetadata, Tier, TierBitmask, TierStats,
    VfsPath, WalEntry, WalOp, now_unix,
};
