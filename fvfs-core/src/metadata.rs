use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::types::{
    EntryKind, FileMetadata, TierBitmask, FvfsPath, WalEntry, WalOp, now_unix,
};

/// Thread-safe SQLite metadata store.
///
/// Wraps a `rusqlite::Connection` behind a `Mutex` so it can be shared across
/// async tasks via `spawn_blocking`.  All public methods perform the blocking
/// SQLite operation synchronously and are intended to be called from within
/// `tokio::task::spawn_blocking` closures.
#[derive(Clone)]
pub struct MetadataStore {
    conn: Arc<Mutex<Connection>>,
}

impl MetadataStore {
    /// Open (or create) the database at `path` and run migrations.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let store = MetadataStore {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.run_migrations()?;
        Ok(store)
    }

    /// Open an in-memory database (useful for tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let store = MetadataStore {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.run_migrations()?;
        Ok(store)
    }

    fn run_migrations(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(SCHEMA_SQL)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // File operations

    /// Insert or update a file/directory entry; returns the row id.
    pub fn upsert(&self, meta: &FileMetadata) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let mime = meta
            .mime_type
            .as_deref()
            .unwrap_or(if meta.is_dir() { "inode/directory" } else { "" });
        let kind_flag: i64 = if meta.is_dir() { 1 } else { 0 };

        conn.execute(
            "INSERT INTO files
               (path, size_bytes, sha256, tier_bitmask, created_at, modified_at, accessed_at,
                access_count_30d, mime_type, is_dir)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
             ON CONFLICT(path) DO UPDATE SET
               size_bytes       = excluded.size_bytes,
               sha256           = excluded.sha256,
               tier_bitmask     = excluded.tier_bitmask,
               modified_at      = excluded.modified_at,
               accessed_at      = excluded.accessed_at,
               access_count_30d = excluded.access_count_30d,
               mime_type        = excluded.mime_type,
               is_dir           = excluded.is_dir",
            params![
                meta.path.as_str(),
                meta.size_bytes as i64,
                meta.sha256,
                meta.tier_bitmask.as_u8() as i64,
                meta.created_at,
                meta.modified_at,
                meta.accessed_at,
                meta.access_count_30d as i64,
                mime,
                kind_flag,
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Retrieve metadata for a single path.
    pub fn get(&self, path: &FvfsPath) -> Result<Option<FileMetadata>> {
        let conn = self.conn.lock().unwrap();
        let result = conn
            .query_row(
                "SELECT id, path, size_bytes, sha256, tier_bitmask, created_at, modified_at,
                        accessed_at, access_count_30d, mime_type, is_dir
                 FROM files WHERE path = ?1",
                params![path.as_str()],
                row_to_metadata,
            )
            .optional()?;
        Ok(result)
    }

    /// Get by id.
    pub fn get_by_id(&self, id: i64) -> Result<Option<FileMetadata>> {
        let conn = self.conn.lock().unwrap();
        let result = conn
            .query_row(
                "SELECT id, path, size_bytes, sha256, tier_bitmask, created_at, modified_at,
                        accessed_at, access_count_30d, mime_type, is_dir
                 FROM files WHERE id = ?1",
                params![id],
                row_to_metadata,
            )
            .optional()?;
        Ok(result)
    }

    /// List direct children of `dir_path` (non-recursive).
    pub fn list_dir(&self, dir_path: &FvfsPath) -> Result<Vec<FileMetadata>> {
        let conn = self.conn.lock().unwrap();
        // Match paths that are exactly one segment deeper than dir_path.
        let prefix = if dir_path.as_str() == "/" {
            "/".to_string()
        } else {
            format!("{}/", dir_path.as_str())
        };
        let mut stmt = conn.prepare(
            "SELECT id, path, size_bytes, sha256, tier_bitmask, created_at, modified_at,
                    accessed_at, access_count_30d, mime_type, is_dir
             FROM files
             WHERE path LIKE ?1 ESCAPE '\\'
               AND path NOT LIKE ?2 ESCAPE '\\'
             ORDER BY path",
        )?;
        // ?1 matches anything under the prefix; ?2 excludes paths with another '/' after prefix.
        let like_prefix = format!("{}%", escape_like(&prefix));
        let like_deeper = format!("{}%/%", escape_like(&prefix));
        let rows = stmt
            .query_map(params![like_prefix, like_deeper], row_to_metadata)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Update the tier bitmask for a file.
    pub fn set_tier_bitmask(&self, id: i64, bitmask: TierBitmask) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE files SET tier_bitmask = ?1 WHERE id = ?2",
            params![bitmask.as_u8() as i64, id],
        )?;
        Ok(())
    }

    /// Bump accessed_at and increment access_count_30d.
    pub fn record_access(&self, id: i64) -> Result<()> {
        let now = now_unix();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE files SET accessed_at = ?1, access_count_30d = access_count_30d + 1 WHERE id = ?2",
            params![now, id],
        )?;
        Ok(())
    }

    /// Delete a file entry.
    pub fn delete(&self, path: &FvfsPath) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM files WHERE path = ?1", params![path.as_str()])?;
        Ok(())
    }

    /// Return all files on `tier` ordered by eviction score (coldest first).
    pub fn files_on_tier(
        &self,
        tier_bit: u8,
    ) -> Result<Vec<FileMetadata>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, path, size_bytes, sha256, tier_bitmask, created_at, modified_at,
                    accessed_at, access_count_30d, mime_type, is_dir
             FROM files
             WHERE (tier_bitmask & ?1) != 0 AND is_dir = 0
             ORDER BY accessed_at ASC",
        )?;
        let rows = stmt
            .query_map(params![tier_bit as i64], row_to_metadata)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Total bytes stored on a tier.
    pub fn bytes_on_tier(&self, tier_bit: u8) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let bytes: i64 = conn.query_row(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM files WHERE (tier_bitmask & ?1) != 0 AND is_dir = 0",
            params![tier_bit as i64],
            |row| row.get(0),
        )?;
        Ok(bytes as u64)
    }

    /// Count of entries on a tier.
    pub fn count_on_tier(&self, tier_bit: u8) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files WHERE (tier_bitmask & ?1) != 0 AND is_dir = 0",
            params![tier_bit as i64],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Reset access_count_30d for all files (called by the nightly decay job).
    pub fn decay_access_counts(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE files SET access_count_30d = MAX(0, access_count_30d - 1)",
            [],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // WAL operations

    /// Enqueue a WAL entry.
    pub fn wal_enqueue(&self, file_id: i64, op: &WalOp) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let now = now_unix();
        conn.execute(
            "INSERT INTO wal_pending (file_id, op, enqueued_at, attempts) VALUES (?1, ?2, ?3, 0)",
            params![file_id, op.as_str(), now],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Mark a WAL entry as done by deleting it.
    pub fn wal_complete(&self, wal_id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM wal_pending WHERE id = ?1", params![wal_id])?;
        Ok(())
    }

    /// Record a failed attempt on a WAL entry.
    pub fn wal_record_failure(&self, wal_id: i64, error: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE wal_pending SET attempts = attempts + 1, last_error = ?1 WHERE id = ?2",
            params![error, wal_id],
        )?;
        Ok(())
    }

    /// List all pending WAL entries ordered by enqueue time.
    pub fn wal_pending(&self) -> Result<Vec<WalEntry>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, file_id, op, enqueued_at, attempts, last_error FROM wal_pending ORDER BY enqueued_at ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                let op_str: String = row.get(2)?;
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    op_str,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .filter_map(|(id, file_id, op_str, enqueued_at, attempts, last_error)| {
                let op = WalOp::try_from(op_str.as_str()).ok()?;
                Some(WalEntry {
                    id,
                    file_id,
                    op,
                    enqueued_at,
                    attempts: attempts as u32,
                    last_error,
                })
            })
            .collect();
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Device registry

    /// Upsert a device record.
    pub fn upsert_device(
        &self,
        name: &str,
        mdns_name: &str,
        ip: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = now_unix();
        conn.execute(
            "INSERT INTO devices (name, mdns_name, last_seen_at, ip)
             VALUES (?1,?2,?3,?4)
             ON CONFLICT(mdns_name) DO UPDATE SET last_seen_at = excluded.last_seen_at, ip = excluded.ip",
            params![name, mdns_name, now, ip],
        )?;
        Ok(())
    }

    /// List all known devices.
    pub fn list_devices(&self) -> Result<Vec<crate::types::DeviceInfo>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, name, mdns_name, last_seen_at, ip FROM devices ORDER BY last_seen_at DESC")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::types::DeviceInfo {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    mdns_name: row.get(2)?,
                    last_seen_at: row.get(3)?,
                    ip: row.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

// ---------------------------------------------------------------------------
// Helpers

fn row_to_metadata(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileMetadata> {
    let is_dir: i64 = row.get(10)?;
    let mime: Option<String> = row.get(9)?;
    Ok(FileMetadata {
        id: row.get(0)?,
        path: FvfsPath::new(row.get::<_, String>(1)?).unwrap_or_else(|_| FvfsPath::new("/").unwrap()),
        kind: if is_dir != 0 {
            EntryKind::Directory
        } else {
            EntryKind::File
        },
        size_bytes: row.get::<_, i64>(2)? as u64,
        sha256: row.get(3)?,
        tier_bitmask: TierBitmask::from(row.get::<_, i64>(4)?),
        created_at: row.get(5)?,
        modified_at: row.get(6)?,
        accessed_at: row.get(7)?,
        access_count_30d: row.get::<_, i64>(8)? as u32,
        mime_type: mime,
    })
}

fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

// ---------------------------------------------------------------------------
// Schema

const SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS files (
    id               INTEGER PRIMARY KEY,
    path             TEXT    NOT NULL UNIQUE,
    size_bytes       INTEGER NOT NULL DEFAULT 0,
    sha256           TEXT    NOT NULL DEFAULT '',
    tier_bitmask     INTEGER NOT NULL DEFAULT 0,
    created_at       INTEGER NOT NULL,
    modified_at      INTEGER NOT NULL,
    accessed_at      INTEGER NOT NULL,
    access_count_30d INTEGER NOT NULL DEFAULT 0,
    mime_type        TEXT,
    is_dir           INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_files_path     ON files(path);
CREATE INDEX IF NOT EXISTS idx_files_accessed ON files(accessed_at);
CREATE INDEX IF NOT EXISTS idx_files_tier     ON files(tier_bitmask);

CREATE TABLE IF NOT EXISTS wal_pending (
    id          INTEGER PRIMARY KEY,
    file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    op          TEXT    NOT NULL,
    enqueued_at INTEGER NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT
);

CREATE TABLE IF NOT EXISTS devices (
    id           INTEGER PRIMARY KEY,
    name         TEXT    NOT NULL,
    mdns_name    TEXT    NOT NULL UNIQUE,
    last_seen_at INTEGER NOT NULL,
    ip           TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
";
