/// FUSE handler for vfsd (optional — requires the `fuse` cargo feature).
///
/// Translates POSIX syscalls into TierRouter operations.
/// Maintains an in-memory inode ↔ VfsPath map; inodes are the SQLite `id` values.
/// Root is inode 1 (special-cased, not stored in SQLite).
#[cfg(feature = "fuse")]
pub mod fuse_impl {
    use fuser::{
        FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
        ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyWrite, Request, TimeOrNow,
    };
    use libc::{EACCES, EEXIST, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, ENOTEMPTY};
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::runtime::Handle;

    use fvfs_core::metadata::MetadataStore;
    use fvfs_core::{EntryKind, FileMetadata, VfsError, VfsPath};

    use crate::router::TierRouter;

    const TTL: Duration = Duration::from_secs(1);
    const ROOT_INO: u64 = 1;
    const BLOCK_SIZE: u32 = 4096;

    /// In-memory inode table.
    struct InodeTable {
        ino_to_path: HashMap<u64, VfsPath>,
        path_to_ino: HashMap<VfsPath, u64>,
        next_ino: u64,
    }

    impl InodeTable {
        fn new() -> Self {
            let mut t = InodeTable {
                ino_to_path: HashMap::new(),
                path_to_ino: HashMap::new(),
                next_ino: 2, // 1 is root
            };
            let root = VfsPath::new("/").unwrap();
            t.ino_to_path.insert(ROOT_INO, root.clone());
            t.path_to_ino.insert(root, ROOT_INO);
            t
        }

        fn get_or_alloc(&mut self, path: VfsPath, hint_id: Option<u64>) -> u64 {
            if let Some(&ino) = self.path_to_ino.get(&path) {
                return ino;
            }
            let ino = hint_id.unwrap_or_else(|| {
                let i = self.next_ino;
                self.next_ino += 1;
                i
            });
            self.ino_to_path.insert(ino, path.clone());
            self.path_to_ino.insert(path, ino);
            ino
        }

        fn path(&self, ino: u64) -> Option<&VfsPath> {
            self.ino_to_path.get(&ino)
        }

        fn remove(&mut self, path: &VfsPath) {
            if let Some(ino) = self.path_to_ino.remove(path) {
                self.ino_to_path.remove(&ino);
            }
        }
    }

    pub struct VfsdFuse {
        router: Arc<TierRouter>,
        meta: MetadataStore,
        inodes: Mutex<InodeTable>,
        rt: Handle,
    }

    impl VfsdFuse {
        pub fn new(router: Arc<TierRouter>, meta: MetadataStore, rt: Handle) -> Self {
            VfsdFuse {
                router,
                meta,
                inodes: Mutex::new(InodeTable::new()),
                rt,
            }
        }

        fn block_on<F, T>(&self, fut: F) -> T
        where
            F: std::future::Future<Output = T>,
        {
            self.rt.block_on(fut)
        }

        fn meta_to_attr(&self, ino: u64, meta: &FileMetadata) -> FileAttr {
            let kind = if meta.is_dir() {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let perm = if meta.is_dir() { 0o755 } else { 0o644 };
            let nlink = if meta.is_dir() { 2 } else { 1 };
            let size = meta.size_bytes;
            let blocks = size.div_ceil(BLOCK_SIZE as u64);

            let ctime = UNIX_EPOCH + Duration::from_secs(meta.created_at as u64);
            let mtime = UNIX_EPOCH + Duration::from_secs(meta.modified_at as u64);
            let atime = UNIX_EPOCH + Duration::from_secs(meta.accessed_at as u64);

            FileAttr {
                ino,
                size,
                blocks,
                atime,
                mtime,
                ctime,
                crtime: ctime,
                kind,
                perm,
                nlink,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                rdev: 0,
                flags: 0,
                blksize: BLOCK_SIZE,
            }
        }

        fn root_attr(&self) -> FileAttr {
            let now = SystemTime::now();
            FileAttr {
                ino: ROOT_INO,
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                rdev: 0,
                flags: 0,
                blksize: BLOCK_SIZE,
            }
        }
    }

    impl Filesystem for VfsdFuse {
        fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), i32> {
            Ok(())
        }

        fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
            let parent_path = {
                let inodes = self.inodes.lock().unwrap();
                match inodes.path(parent).cloned() {
                    Some(p) => p,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            };

            let name_str = match name.to_str() {
                Some(s) => s,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let child_path = match parent_path.join(name_str) {
                Ok(p) => p,
                Err(_) => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let meta_store = self.meta.clone();
            let child_clone = child_path.clone();
            let meta_opt = self.block_on(async move {
                tokio::task::spawn_blocking(move || meta_store.get(&child_clone))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .flatten()
            });

            match meta_opt {
                Some(meta) => {
                    let ino = {
                        let mut inodes = self.inodes.lock().unwrap();
                        inodes.get_or_alloc(child_path, Some(meta.id as u64 + 10))
                    };
                    let attr = self.meta_to_attr(ino, &meta);
                    reply.entry(&TTL, &attr, 0);
                }
                None => reply.error(ENOENT),
            }
        }

        fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
            if ino == ROOT_INO {
                reply.attr(&TTL, &self.root_attr());
                return;
            }

            let path = {
                let inodes = self.inodes.lock().unwrap();
                match inodes.path(ino).cloned() {
                    Some(p) => p,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            };

            let meta_store = self.meta.clone();
            let path_clone = path.clone();
            let meta_opt = self.block_on(async move {
                tokio::task::spawn_blocking(move || meta_store.get(&path_clone))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .flatten()
            });

            match meta_opt {
                Some(meta) => reply.attr(&TTL, &self.meta_to_attr(ino, &meta)),
                None => reply.error(ENOENT),
            }
        }

        fn read(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            size: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: ReplyData,
        ) {
            let path = {
                let inodes = self.inodes.lock().unwrap();
                match inodes.path(ino).cloned() {
                    Some(p) => p,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            };

            let router = self.router.clone();
            let result = self.block_on(async move { router.read(&path).await });

            match result {
                Ok(data) => {
                    let start = (offset as usize).min(data.len());
                    let end = (start + size as usize).min(data.len());
                    reply.data(&data[start..end]);
                }
                Err(VfsError::NotFound { .. }) => reply.error(ENOENT),
                Err(VfsError::IsADirectory { .. }) => reply.error(EISDIR),
                Err(_) => reply.error(EIO),
            }
        }

        fn write(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            data: &[u8],
            _write_flags: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: ReplyWrite,
        ) {
            let path = {
                let inodes = self.inodes.lock().unwrap();
                match inodes.path(ino).cloned() {
                    Some(p) => p,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            };

            // For simplicity: read existing data, splice in new bytes, write back.
            // A production implementation would use sparse writes or O_APPEND logic.
            let router = self.router.clone();
            let path_clone = path.clone();
            let existing = self
                .block_on(async move { router.read(&path_clone).await })
                .unwrap_or_default();

            let offset = offset as usize;
            let mut buf = existing.to_vec();
            let end = offset + data.len();
            if buf.len() < end {
                buf.resize(end, 0);
            }
            buf[offset..end].copy_from_slice(data);

            let router = self.router.clone();
            let written = data.len() as u32;
            let result =
                self.block_on(async move { router.write(&path, bytes::Bytes::from(buf)).await });

            match result {
                Ok(()) => reply.written(written),
                Err(_) => reply.error(EIO),
            }
        }

        fn create(
            &mut self,
            _req: &Request<'_>,
            parent: u64,
            name: &OsStr,
            _mode: u32,
            _umask: u32,
            _flags: i32,
            reply: ReplyCreate,
        ) {
            let parent_path = {
                let inodes = self.inodes.lock().unwrap();
                match inodes.path(parent).cloned() {
                    Some(p) => p,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            };

            let name_str = match name.to_str() {
                Some(s) => s,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let child_path = match parent_path.join(name_str) {
                Ok(p) => p,
                Err(_) => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let router = self.router.clone();
            let path_clone = child_path.clone();
            let result = self.block_on(async move {
                router
                    .write(&path_clone, bytes::Bytes::new())
                    .await
            });

            match result {
                Ok(()) => {
                    let ino = {
                        let mut inodes = self.inodes.lock().unwrap();
                        inodes.get_or_alloc(child_path.clone(), None)
                    };
                    let now = SystemTime::now();
                    let attr = FileAttr {
                        ino,
                        size: 0,
                        blocks: 0,
                        atime: now,
                        mtime: now,
                        ctime: now,
                        crtime: now,
                        kind: FileType::RegularFile,
                        perm: 0o644,
                        nlink: 1,
                        uid: unsafe { libc::getuid() },
                        gid: unsafe { libc::getgid() },
                        rdev: 0,
                        flags: 0,
                        blksize: BLOCK_SIZE,
                    };
                    reply.created(&TTL, &attr, 0, 0, 0);
                }
                Err(_) => reply.error(EIO),
            }
        }

        fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
            let parent_path = {
                let inodes = self.inodes.lock().unwrap();
                match inodes.path(parent).cloned() {
                    Some(p) => p,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            };

            let name_str = match name.to_str() {
                Some(s) => s,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let child_path = match parent_path.join(name_str) {
                Ok(p) => p,
                Err(_) => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let router = self.router.clone();
            let path_clone = child_path.clone();
            let result = self.block_on(async move { router.delete(&path_clone).await });

            match result {
                Ok(()) => {
                    self.inodes.lock().unwrap().remove(&child_path);
                    reply.ok();
                }
                Err(VfsError::NotFound { .. }) => reply.error(ENOENT),
                Err(_) => reply.error(EIO),
            }
        }

        fn mkdir(
            &mut self,
            _req: &Request<'_>,
            parent: u64,
            name: &OsStr,
            _mode: u32,
            _umask: u32,
            reply: ReplyEntry,
        ) {
            let parent_path = {
                let inodes = self.inodes.lock().unwrap();
                match inodes.path(parent).cloned() {
                    Some(p) => p,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            };

            let name_str = match name.to_str() {
                Some(s) => s,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let child_path = match parent_path.join(name_str) {
                Ok(p) => p,
                Err(_) => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let router = self.router.clone();
            let path_clone = child_path.clone();
            let result = self.block_on(async move { router.mkdir(&path_clone).await });

            match result {
                Ok(()) => {
                    let ino = {
                        let mut inodes = self.inodes.lock().unwrap();
                        inodes.get_or_alloc(child_path, None)
                    };
                    let now = SystemTime::now();
                    let attr = FileAttr {
                        ino,
                        size: 0,
                        blocks: 0,
                        atime: now,
                        mtime: now,
                        ctime: now,
                        crtime: now,
                        kind: FileType::Directory,
                        perm: 0o755,
                        nlink: 2,
                        uid: unsafe { libc::getuid() },
                        gid: unsafe { libc::getgid() },
                        rdev: 0,
                        flags: 0,
                        blksize: BLOCK_SIZE,
                    };
                    reply.entry(&TTL, &attr, 0);
                }
                Err(_) => reply.error(EIO),
            }
        }

        fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
            // Reuse unlink for now — metadata store will handle directories.
            self.unlink(_req, parent, name, reply);
        }

        fn readdir(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            mut reply: ReplyDirectory,
        ) {
            let dir_path = {
                let inodes = self.inodes.lock().unwrap();
                match inodes.path(ino).cloned() {
                    Some(p) => p,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            };

            let router = self.router.clone();
            let path_clone = dir_path.clone();
            let entries = match self.block_on(async move { router.list(&path_clone).await }) {
                Ok(e) => e,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };

            // Always emit . and ..
            let mut idx = 0i64;
            if offset <= idx {
                if reply.add(ino, idx + 1, FileType::Directory, ".") {
                    reply.ok();
                    return;
                }
            }
            idx += 1;

            let parent_ino = if ino == ROOT_INO { ROOT_INO } else { ROOT_INO }; // simplified
            if offset <= idx {
                if reply.add(parent_ino, idx + 1, FileType::Directory, "..") {
                    reply.ok();
                    return;
                }
            }
            idx += 1;

            for entry in entries {
                if offset > idx {
                    idx += 1;
                    continue;
                }
                let child_ino = {
                    let mut inodes = self.inodes.lock().unwrap();
                    inodes.get_or_alloc(entry.path.clone(), None)
                };
                let kind = match entry.kind {
                    fvfs_core::EntryKind::Directory => FileType::Directory,
                    fvfs_core::EntryKind::File => FileType::RegularFile,
                };
                let name = entry.path.file_name().to_string();
                if reply.add(child_ino, idx + 1, kind, &name) {
                    reply.ok();
                    return;
                }
                idx += 1;
            }

            reply.ok();
        }

        fn setattr(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _mode: Option<u32>,
            _uid: Option<u32>,
            _gid: Option<u32>,
            size: Option<u64>,
            _atime: Option<TimeOrNow>,
            _mtime: Option<TimeOrNow>,
            _ctime: Option<std::time::SystemTime>,
            _fh: Option<u64>,
            _crtime: Option<std::time::SystemTime>,
            _chgtime: Option<std::time::SystemTime>,
            _bkuptime: Option<std::time::SystemTime>,
            _flags: Option<u32>,
            reply: ReplyAttr,
        ) {
            // Handle truncate (size = 0 most commonly from O_TRUNC).
            if let Some(new_size) = size {
                let path = {
                    let inodes = self.inodes.lock().unwrap();
                    match inodes.path(ino).cloned() {
                        Some(p) => p,
                        None => {
                            reply.error(ENOENT);
                            return;
                        }
                    }
                };

                let router = self.router.clone();
                let path_clone = path.clone();
                if new_size == 0 {
                    let _ = self.block_on(async move {
                        router.write(&path_clone, bytes::Bytes::new()).await
                    });
                }
                // For non-zero truncate, a full implementation would read/resize.
            }

            self.getattr(_req, ino, None, reply);
        }
    }
}

#[cfg(not(feature = "fuse"))]
pub mod fuse_impl {}
