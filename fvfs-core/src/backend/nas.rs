use std::path::PathBuf;

use crate::backend::local::LocalDiskBackend;
use crate::types::Tier;

/// NAS warm-tier backend.
///
/// In v1 the NAS is mounted via SMB/NFS and accessible as a local path;
/// we re-use `LocalDiskBackend` pointed at that mount point.  This keeps
/// the implementation simple while still giving us a distinct `Tier::Nas`
/// label in the tier bitmask.
pub fn new_nas_backend(root: impl Into<PathBuf>) -> LocalDiskBackend {
    LocalDiskBackend::new(root, Tier::Nas)
}
