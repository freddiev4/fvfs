/// FUSE handler for vfsc (optional — requires the `fuse` cargo feature).
///
/// Proxies all POSIX syscalls to vfsd over HTTP. Read results are cached
/// locally by SHA-256 to avoid redundant fetches.
#[cfg(feature = "fuse")]
pub mod fuse_impl {
    use fuser::{
        FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
        ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyWrite, Request, TimeOrNow,
    };
    use libc::{EACCES, EIO, EISDIR, ENOENT, EINVAL};
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::sync::Mutex;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::runtime::Handle;

    use fvfs_core::{EntryKind, FileMetadata, VfsError, VfsPath};

    use crate::http_client::VfsdClient;
    use crate::local_cache::LocalCache;

    const TTL: Duration = Duration::from_secs(1);
    const ROOT_INO: u64 = 1;
    const BLOCK_SIZE: u32 = 4096;

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
                next_ino: 2,
            };
            let root = VfsPath::new("/").unwrap();
            t.ino_to_path.insert(ROOT_INO, root.clone());
            t.path_to_ino.insert(root, ROOT_INO);
            t
        }

        fn get_or_alloc(&mut self, path: VfsPath) -> u64 {
            if let Some(&ino) = self.path_to_ino.get(&path) {
                return ino;
            }
            let ino = self.next_ino;
            self.next_ino += 1;
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

    pub struct VfscFuse {
        client: VfsdClient,
        cache: LocalCache,
        inodes: Mutex<InodeTable>,
        rt: Handle,
    }

    impl VfscFuse {
        pub fn new(client: VfsdClient, cache: LocalCache, rt: Handle) -> Self {
            VfscFuse {
                client,
                cache,
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

    impl Filesystem for VfscFuse {
        fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), i32> {
            Ok(())
        }

        fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
            let parent_path = {
                let inodes = self.inodes.lock().unwrap();
                match inodes.path(parent).cloned() {
                    Some(p) => p,
                    None => { reply.error(ENOENT); return; }
                }
            };
            let name_str = match name.to_str() {
                Some(s) => s,
                None => { reply.error(EINVAL); return; }
            };
            let child_path = match parent_path.join(name_str) {
                Ok(p) => p,
                Err(_) => { reply.error(EINVAL); return; }
            };

            let meta_result = self.block_on(self.client.stat(&child_path));
            match meta_result {
                Ok(meta) => {
                    let ino = self.inodes.lock().unwrap().get_or_alloc(child_path);
                    reply.entry(&TTL, &self.meta_to_attr(ino, &meta), 0);
                }
                Err(VfsError::NotFound { .. }) => reply.error(ENOENT),
                Err(_) => reply.error(EIO),
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
                    None => { reply.error(ENOENT); return; }
                }
            };
            match self.block_on(self.client.stat(&path)) {
                Ok(meta) => reply.attr(&TTL, &self.meta_to_attr(ino, &meta)),
                Err(VfsError::NotFound { .. }) => reply.error(ENOENT),
                Err(_) => reply.error(EIO),
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
                    None => { reply.error(ENOENT); return; }
                }
            };

            // Try local cache first via stat + hash lookup.
            let cached = self.block_on(async {
                if let Ok(meta) = self.client.stat(&path).await {
                    if !meta.sha256.is_empty() {
                        if let Some(data) = self.cache.get_by_hash(&meta.sha256).await {
                            return Some(data);
                        }
                    }
                }
                None
            });

            let data = if let Some(d) = cached {
                d
            } else {
                match self.block_on(self.client.get(&path)) {
                    Ok(d) => {
                        let cache = self.cache.clone();
                        let d_clone = d.clone();
                        self.rt.spawn(async move { cache.put(&d_clone).await; });
                        d
                    }
                    Err(VfsError::NotFound { .. }) => { reply.error(ENOENT); return; }
                    Err(VfsError::IsADirectory { .. }) => { reply.error(EISDIR); return; }
                    Err(_) => { reply.error(EIO); return; }
                }
            };

            let start = (offset as usize).min(data.len());
            let end = (start + size as usize).min(data.len());
            reply.data(&data[start..end]);
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
                    None => { reply.error(ENOENT); return; }
                }
            };

            let existing = self
                .block_on(self.client.get(&path))
                .unwrap_or_default();

            let offset = offset as usize;
            let mut buf = existing.to_vec();
            let end = offset + data.len();
            if buf.len() < end { buf.resize(end, 0); }
            buf[offset..end].copy_from_slice(data);

            let written = data.len() as u32;
            match self.block_on(self.client.put(&path, bytes::Bytes::from(buf))) {
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
                    None => { reply.error(ENOENT); return; }
                }
            };
            let name_str = match name.to_str() {
                Some(s) => s,
                None => { reply.error(EINVAL); return; }
            };
            let child_path = match parent_path.join(name_str) {
                Ok(p) => p,
                Err(_) => { reply.error(EINVAL); return; }
            };

            match self.block_on(self.client.put(&child_path, bytes::Bytes::new())) {
                Ok(()) => {
                    let ino = self.inodes.lock().unwrap().get_or_alloc(child_path);
                    let now = SystemTime::now();
                    let attr = FileAttr {
                        ino, size: 0, blocks: 0,
                        atime: now, mtime: now, ctime: now, crtime: now,
                        kind: FileType::RegularFile, perm: 0o644, nlink: 1,
                        uid: unsafe { libc::getuid() },
                        gid: unsafe { libc::getgid() },
                        rdev: 0, flags: 0, blksize: BLOCK_SIZE,
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
                    None => { reply.error(ENOENT); return; }
                }
            };
            let name_str = match name.to_str() {
                Some(s) => s,
                None => { reply.error(EINVAL); return; }
            };
            let child_path = match parent_path.join(name_str) {
                Ok(p) => p,
                Err(_) => { reply.error(EINVAL); return; }
            };

            match self.block_on(self.client.delete(&child_path)) {
                Ok(()) => {
                    self.inodes.lock().unwrap().remove(&child_path);
                    reply.ok();
                }
                Err(_) => reply.error(EIO),
            }
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
                    None => { reply.error(ENOENT); return; }
                }
            };

            let entries = match self.block_on(self.client.list(&dir_path)) {
                Ok(e) => e,
                Err(_) => { reply.error(EIO); return; }
            };

            let mut idx = 0i64;
            if offset <= idx {
                if reply.add(ino, idx + 1, FileType::Directory, ".") {
                    reply.ok(); return;
                }
            }
            idx += 1;
            if offset <= idx {
                if reply.add(ROOT_INO, idx + 1, FileType::Directory, "..") {
                    reply.ok(); return;
                }
            }
            idx += 1;

            for entry in entries {
                if offset > idx { idx += 1; continue; }
                let child_ino = self.inodes.lock().unwrap().get_or_alloc(entry.path.clone());
                let kind = match entry.kind {
                    EntryKind::Directory => FileType::Directory,
                    EntryKind::File => FileType::RegularFile,
                };
                let name = entry.path.file_name().to_string();
                if reply.add(child_ino, idx + 1, kind, &name) {
                    reply.ok(); return;
                }
                idx += 1;
            }
            reply.ok();
        }
    }
}

#[cfg(not(feature = "fuse"))]
pub mod fuse_impl {}
