use crate::self_metrics::SelfMetrics;
/// Crash-safe segmented disk spool for ClickHouse write backpressure.
///
/// Each segment is a flat file of framed records:
///   [u32 LE: table name len] [table name bytes] [u32 LE: payload len] [payload bytes]
///
/// Segments are named `seg-<unix_millis>-<seq>.spool`.
/// The current (open) segment is rotated once it reaches SEGMENT_MAX_BYTES (32 MiB).
/// On open, existing `*.spool` files are scanned to restore total_bytes.
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const SEGMENT_MAX_BYTES: u64 = 32 * 1024 * 1024; // 32 MiB per segment

pub(crate) fn record_spool_discard(
    metrics: &SelfMetrics,
    backend: &'static str,
    reason: &'static str,
    unit: &'static str,
    bytes: u64,
) {
    let labels = [("backend", backend), ("reason", reason), ("unit", unit)];
    metrics.inc_counter("rush_ingest_spool_discarded_total", &labels, 1);
    metrics.inc_counter("rush_ingest_spool_discarded_bytes_total", &labels, bytes);
}

pub(crate) fn record_spool_error(
    metrics: &SelfMetrics,
    backend: &'static str,
    operation: &'static str,
) {
    metrics.inc_counter(
        "rush_ingest_spool_errors_total",
        &[("backend", backend), ("operation", operation)],
        1,
    );
}

/// Returned by `Spool::append` when the byte cap has been reached.
#[derive(Debug)]
pub struct SpoolFull;

impl std::fmt::Display for SpoolFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "spool is full")
    }
}

struct SpoolInner {
    dir: PathBuf,
    max_bytes: u64,
    /// Sum of all segment file sizes.
    total_bytes: u64,
    /// Sequence counter to disambiguate segments created in the same millisecond.
    seq: u64,
    /// Currently open segment file + its current size.
    current: Option<(File, u64, PathBuf)>,
    /// All segment paths (open + sealed), kept sorted. Zero-padded `seg-<ms>-<seq>`
    /// names sort chronologically, so `first()` is the oldest. Maintained on
    /// open/rotate/remove so stats and the replayer never have to read_dir.
    segments: std::collections::BTreeSet<PathBuf>,
}

struct SegmentRead {
    records: Vec<(String, Vec<u8>)>,
    discarded_bytes: u64,
}

pub struct Spool {
    metrics: OnceLock<Arc<SelfMetrics>>,
    inner: Mutex<SpoolInner>,
    /// Monotonic count of segments successfully drained (for drain-rate metric).
    committed: AtomicU64,
}

impl Spool {
    /// Open (or create) a spool directory.  Scans existing `*.spool` files to
    /// restore `total_bytes` so the cap is honoured across restarts.
    pub fn open(dir: impl AsRef<Path>, max_bytes: u64) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        match fs::symlink_metadata(&dir) {
            Ok(metadata) => ensure_real_directory(&dir, &metadata)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&dir)?;
            }
            Err(error) => return Err(error),
        }
        let metadata = fs::symlink_metadata(&dir)?;
        ensure_real_directory(&dir, &metadata)?;
        set_private_directory_permissions(&dir)?;

        let mut total_bytes: u64 = 0;
        let mut max_seq: u64 = 0;
        let mut segments = std::collections::BTreeSet::new();

        // Scan existing segments once at startup to restore byte count, max
        // sequence, and the in-memory segment index.
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".spool") {
                let file_type = entry.file_type()?;
                if file_type.is_symlink() || !file_type.is_file() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "spool segment is not a regular file: {}",
                            entry.path().display()
                        ),
                    ));
                }
                set_private_file_permissions(&entry.path())?;
                let size = entry.metadata()?.len();
                total_bytes += size;
                segments.insert(entry.path());

                // Parse seq from seg-<ms>-<seq>.spool
                if let Some(seq) = parse_seq(&name_str) {
                    if seq > max_seq {
                        max_seq = seq;
                    }
                }
            }
        }

        Ok(Spool {
            metrics: OnceLock::new(),
            inner: Mutex::new(SpoolInner {
                dir,
                max_bytes,
                total_bytes,
                seq: max_seq,
                current: None,
                segments,
            }),
            committed: AtomicU64::new(0),
        })
    }

    pub fn committed_total(&self) -> u64 {
        self.committed.load(Ordering::Relaxed)
    }

    /// Age (seconds) of the oldest pending segment, parsed from its `seg-<ms>-`
    /// filename. `None` when empty. Served from the in-memory index — no I/O.
    pub fn oldest_age_secs(&self) -> Option<u64> {
        let g = self.inner.lock().unwrap();
        let oldest = g.segments.first()?;
        let name = oldest.file_name()?.to_string_lossy().into_owned();
        let ms = name
            .strip_prefix("seg-")
            .and_then(|r| r.split('-').next())
            .and_then(|s| s.parse::<u64>().ok())?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Some(now.saturating_sub(ms) / 1000)
    }

    /// Append a record to the spool.  Returns `Err(SpoolFull)` when
    /// `total_bytes + record_len > max_bytes`.  Thread-safe.
    pub fn append(&self, table: &str, payload: &[u8]) -> Result<(), SpoolFull> {
        let mut g = self.inner.lock().unwrap();

        // Frame: 4 bytes table-len + table + 4 bytes payload-len + payload
        let table_bytes = table.as_bytes();
        let frame_len = 4 + table_bytes.len() + 4 + payload.len();

        if g.total_bytes + frame_len as u64 > g.max_bytes {
            return Err(SpoolFull);
        }

        // Rotate if the current segment would exceed SEGMENT_MAX_BYTES.
        let needs_rotate = match &g.current {
            Some((_, size, _)) => *size + frame_len as u64 > SEGMENT_MAX_BYTES,
            None => true,
        };

        if needs_rotate {
            if let Some((mut f, _, _)) = g.current.take() {
                let _ = f.flush();
                let _ = sync_file(&mut f);
            }
            g.seq += 1;
            let path = g.dir.join(seg_name(g.seq));
            let mut options = OpenOptions::new();
            options.create_new(true).append(true);
            secure_open_options(&mut options);
            let file = options.open(&path).map_err(|_| SpoolFull)?;
            g.segments.insert(path.clone());
            g.current = Some((file, 0, path));
        }

        if let Some((file, size, _)) = g.current.as_mut() {
            // Write frame
            let tbl_len = table_bytes.len() as u32;
            let pay_len = payload.len() as u32;
            file.write_all(&tbl_len.to_le_bytes())
                .map_err(|_| SpoolFull)?;
            file.write_all(table_bytes).map_err(|_| SpoolFull)?;
            file.write_all(&pay_len.to_le_bytes())
                .map_err(|_| SpoolFull)?;
            file.write_all(payload).map_err(|_| SpoolFull)?;
            *size += frame_len as u64;
            g.total_bytes += frame_len as u64;
        }

        Ok(())
    }

    /// Take the path of the oldest closed (completed) segment, for replay.
    /// Does NOT include the currently open segment. Served from the in-memory
    /// index — no directory scan.
    pub fn take_oldest_segment(&self) -> Option<PathBuf> {
        let g = self.inner.lock().unwrap();
        let current_path = g.current.as_ref().map(|(_, _, p)| p.clone());
        g.segments
            .iter()
            .find(|seg| current_path.as_ref() != Some(seg))
            .cloned()
    }

    /// Read all framed records from a segment file.
    ///
    /// Records are not fsync'd per-append (only on rotation), so an unclean
    /// shutdown can leave a torn trailing record. We tolerate that: any EOF
    /// encountered partway through a frame ends parsing and returns the records
    /// read so far, rather than discarding the whole (otherwise-valid) segment.
    pub fn read_segment(path: &Path) -> std::io::Result<Vec<(String, Vec<u8>)>> {
        Self::read_segment_with_loss(path).map(|read| read.records)
    }

    fn read_segment_with_loss(path: &Path) -> std::io::Result<SegmentRead> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "spool segment is not a regular file",
            ));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        secure_open_options(&mut options);
        let mut file = options.open(path)?;
        let mut records = Vec::new();

        let mut discarded_bytes = 0;
        loop {
            let frame_start = file.stream_position()?;
            if frame_start == metadata.len() {
                break;
            }
            let mut buf4 = [0u8; 4];
            if read_exact_or_eof(&mut file, &mut buf4)?.is_none() {
                discarded_bytes = metadata.len().saturating_sub(frame_start);
                break;
            }
            let tbl_len = u32::from_le_bytes(buf4) as usize;
            // Bound corrupt lengths by the actual file before allocating.
            if tbl_len as u64 > metadata.len().saturating_sub(file.stream_position()?) {
                discarded_bytes = metadata.len().saturating_sub(frame_start);
                break;
            }
            let mut tbl_buf = vec![0u8; tbl_len];
            if read_exact_or_eof(&mut file, &mut tbl_buf)?.is_none() {
                discarded_bytes = metadata.len().saturating_sub(frame_start);
                break;
            }
            let table = match String::from_utf8(tbl_buf) {
                Ok(t) => t,
                Err(_) => {
                    discarded_bytes = metadata.len().saturating_sub(frame_start);
                    break;
                }
            };
            if read_exact_or_eof(&mut file, &mut buf4)?.is_none() {
                discarded_bytes = metadata.len().saturating_sub(frame_start);
                break;
            }
            let pay_len = u32::from_le_bytes(buf4) as usize;
            if pay_len as u64 > metadata.len().saturating_sub(file.stream_position()?) {
                discarded_bytes = metadata.len().saturating_sub(frame_start);
                break;
            }
            let mut payload = vec![0u8; pay_len];
            if read_exact_or_eof(&mut file, &mut payload)?.is_none() {
                discarded_bytes = metadata.len().saturating_sub(frame_start);
                break;
            }
            records.push((table, payload));
        }
        Ok(SegmentRead {
            records,
            discarded_bytes,
        })
    }

    /// Seal (close + fsync) the currently open segment so it becomes eligible
    /// for replay. No-op if there is no open segment. The replayer calls this
    /// when data remains in the spool but no closed segment is available, so a
    /// partial final batch is drained promptly after ClickHouse recovers rather
    /// than waiting for the segment to reach the 32 MiB rotation threshold.
    pub fn seal_current(&self) {
        let mut g = self.inner.lock().unwrap();
        if let Some((mut f, _, _)) = g.current.take() {
            let _ = f.flush();
            let _ = sync_file(&mut f);
        }
    }

    /// Remove a segment file and update total_bytes + the in-memory index.
    pub fn remove_segment(&self, path: &Path) -> bool {
        let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if let Err(error) = fs::remove_file(path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            if let Some(metrics) = self.metrics.get() {
                record_spool_error(metrics, "disk", "delete");
            }
            return false;
        }
        let mut g = self.inner.lock().unwrap();
        g.total_bytes = g.total_bytes.saturating_sub(size);
        g.segments.remove(path);
        true
    }

    pub fn total_bytes(&self) -> u64 {
        self.inner.lock().unwrap().total_bytes
    }

    pub fn max_bytes(&self) -> u64 {
        self.inner.lock().unwrap().max_bytes
    }

    /// Number of segments (open + sealed). In-memory index — no I/O.
    pub fn segment_count(&self) -> usize {
        self.inner.lock().unwrap().segments.len()
    }
}

fn ensure_real_directory(path: &Path, metadata: &fs::Metadata) -> std::io::Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("spool path is not a regular directory: {}", path.display()),
        ));
    }
    Ok(())
}

fn secure_open_options(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
}

fn set_private_directory_permissions(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn set_private_file_permissions(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Like `read_exact`, but returns `Ok(None)` on EOF (clean or partial) instead
/// of erroring, so a torn trailing record terminates parsing gracefully.
fn read_exact_or_eof(file: &mut File, buf: &mut [u8]) -> std::io::Result<Option<()>> {
    match file.read_exact(buf) {
        Ok(_) => Ok(Some(())),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

// ─── Backend seam ────────────────────────────────────────────────────────────
//
// `IngestBuffer` is the backend the durable write path (`ChWriter`) writes
// through. `Disk` is the default and needs no object store — it wraps the
// segmented `Spool` above and preserves today's behavior exactly. An
// `ObjectStore` variant (MinIO/S3 + manifest) can be added without changing the
// replayer, which stays backend-agnostic by going through `next_batch()` /
// `commit()`.

/// A unit of spooled work for the replayer: the records to insert plus an opaque
/// handle used to `commit` (remove/ack) them once successfully written to CH.
pub struct DrainBatch {
    pub records: Vec<(String, Vec<u8>)>,
    pub discarded_bytes: u64,
    handle: BatchHandle,
}

enum BatchHandle {
    Disk(PathBuf),
    ObjectStore {
        key: object_store::path::Path,
        size: u64,
    },
}

pub enum IngestBuffer {
    Disk(Spool),
    ObjectStore(crate::object_store_spool::ObjectStoreSpool),
}

impl IngestBuffer {
    pub fn attach_metrics(&self, metrics: Arc<SelfMetrics>) {
        match self {
            Self::Disk(spool) => {
                let _ = spool.metrics.set(metrics);
            }
            Self::ObjectStore(spool) => spool.attach_metrics(metrics),
        }
    }

    /// Append a framed record (failure-path hot call). 429 when over the cap.
    /// Disk appends are blocking file writes serialized by the spool mutex, so
    /// they run on the blocking pool — otherwise, exactly when ClickHouse is
    /// down, every ingest request would do blocking I/O on a tokio worker.
    pub async fn append(self: &Arc<Self>, table: &str, payload: Vec<u8>) -> Result<(), SpoolFull> {
        match &**self {
            IngestBuffer::Disk(_) => {
                let me = Arc::clone(self);
                let table = table.to_string();
                tokio::task::spawn_blocking(move || match &*me {
                    IngestBuffer::Disk(s) => s.append(&table, &payload),
                    IngestBuffer::ObjectStore(_) => unreachable!(),
                })
                .await
                .map_err(|_| SpoolFull)?
            }
            IngestBuffer::ObjectStore(s) => s.append(table, &payload).await,
        }
    }

    pub fn total_bytes(&self) -> u64 {
        match self {
            IngestBuffer::Disk(s) => s.total_bytes(),
            IngestBuffer::ObjectStore(s) => s.total_bytes(),
        }
    }

    pub fn segment_count(&self) -> usize {
        match self {
            IngestBuffer::Disk(s) => s.segment_count(),
            IngestBuffer::ObjectStore(s) => s.segment_count(),
        }
    }

    pub fn backend_name(&self) -> &'static str {
        match self {
            IngestBuffer::Disk(_) => "disk",
            IngestBuffer::ObjectStore(_) => "object_store",
        }
    }

    pub fn max_bytes(&self) -> u64 {
        match self {
            IngestBuffer::Disk(s) => s.max_bytes(),
            IngestBuffer::ObjectStore(s) => s.max_bytes(),
        }
    }

    /// Monotonic count of batches drained+committed (for the drain-rate metric).
    pub fn committed_total(&self) -> u64 {
        match self {
            IngestBuffer::Disk(s) => s.committed_total(),
            IngestBuffer::ObjectStore(s) => s.committed_total(),
        }
    }

    /// Reconcile shared-backend counters with the durable store. This is a
    /// no-op for the local disk backend and is called from the stats cadence,
    /// not the ingest hot path.
    pub async fn refresh_counts(&self) -> anyhow::Result<()> {
        match self {
            IngestBuffer::Disk(_) => Ok(()),
            IngestBuffer::ObjectStore(s) => s.refresh_counts().await,
        }
    }

    /// Age (seconds) of the oldest pending batch — the replay lag. `None`/0 when empty.
    pub async fn oldest_age_secs(&self) -> Option<u64> {
        match self {
            IngestBuffer::Disk(s) => s.oldest_age_secs(),
            IngestBuffer::ObjectStore(s) => s.oldest_age_secs().await,
        }
    }

    /// The next unit of work to replay, or `None` when empty. For the disk
    /// backend this seals the open segment when no closed segment is available
    /// (so a partial trailing batch still drains), and discards an unreadable
    /// segment before trying the next. Disk reads (up to a 32 MiB segment) and
    /// the seal fsync run on the blocking pool.
    pub async fn next_batch(self: &Arc<Self>) -> Option<DrainBatch> {
        match &**self {
            IngestBuffer::Disk(_) => {
                let me = Arc::clone(self);
                tokio::task::spawn_blocking(move || {
                    let IngestBuffer::Disk(s) = &*me else { unreachable!() };
                    // Loop (not recursion) to skip unreadable segments.
                    loop {
                        let path = match s.take_oldest_segment() {
                            Some(p) => p,
                            None => {
                                if s.total_bytes() > 0 {
                                    s.seal_current();
                                    match s.take_oldest_segment() {
                                        Some(p) => p,
                                        None => return None,
                                    }
                                } else {
                                    return None;
                                }
                            }
                        };
                        match Spool::read_segment_with_loss(&path) {
                            Ok(read) => {
                                return Some(DrainBatch { records: read.records, discarded_bytes: read.discarded_bytes, handle: BatchHandle::Disk(path) });
                            },
                            Err(e) => {
                                tracing::error!(error = %e, segment = %path.display(), "ingest buffer: failed to read segment — discarding");
                                let bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                                if !s.remove_segment(&path) { return None; }
                                if let Some(metrics) = s.metrics.get() {
                                    record_spool_discard(metrics, "disk", "segment_read", "segment", bytes);
                                }
                                continue;
                            }
                        }
                    }
                })
                .await
                .ok()
                .flatten()
            }
            IngestBuffer::ObjectStore(s) => {
                s.next_batch().await.map(|(key, size, records)| DrainBatch {
                    records,
                    discarded_bytes: 0,
                    handle: BatchHandle::ObjectStore { key, size },
                })
            }
        }
    }

    /// Commit (remove/ack) a batch that was successfully drained to ClickHouse.
    pub async fn commit(&self, batch: DrainBatch) -> bool {
        match (self, batch.handle) {
            (IngestBuffer::Disk(s), BatchHandle::Disk(path)) => {
                if s.remove_segment(&path) {
                    if batch.discarded_bytes > 0
                        && let Some(metrics) = s.metrics.get()
                    {
                        record_spool_discard(
                            metrics,
                            "disk",
                            "frame_decode",
                            "tail",
                            batch.discarded_bytes,
                        );
                    }
                    s.committed.fetch_add(1, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            }
            (IngestBuffer::ObjectStore(s), BatchHandle::ObjectStore { key, size }) => {
                s.commit(&key, size).await
            }
            // Mismatched handle/backend can't happen (handles are minted by the same backend).
            _ => {
                tracing::error!("ingest buffer: commit handle/backend mismatch");
                false
            }
        }
    }
}

fn seg_name(seq: u64) -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    format!("seg-{ms:020}-{seq:016}.spool")
}

fn parse_seq(name: &str) -> Option<u64> {
    // seg-<ms>-<seq>.spool
    let stripped = name.strip_suffix(".spool")?;
    let parts: Vec<&str> = stripped.splitn(3, '-').collect();
    if parts.len() == 3 {
        parts[2].parse().ok()
    } else {
        None
    }
}

/// Flush a sealed segment's data + metadata to disk before it is considered
/// durable for replay.
fn sync_file(f: &mut File) -> std::io::Result<()> {
    f.sync_all()
}

#[cfg(test)]
mod tests {
    use super::{IngestBuffer, SelfMetrics, Spool};
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::sync::Arc;

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rush-spool-{label}-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn torn_tail_is_counted_only_when_committed_not_on_read_retries() {
        let path = temp_path("torn-tail-metrics");
        let spool = Spool::open(&path, 1024 * 1024).unwrap();
        spool.append("logs", b"accepted").unwrap();
        spool.seal_current();
        let segment = spool.take_oldest_segment().unwrap();
        OpenOptions::new()
            .append(true)
            .open(&segment)
            .unwrap()
            .write_all(&[1, 2])
            .unwrap();
        let buffer = Arc::new(IngestBuffer::Disk(spool));
        let metrics = Arc::new(SelfMetrics::new());
        buffer.attach_metrics(metrics.clone());
        let first = buffer.next_batch().await.unwrap();
        assert_eq!(first.records.len(), 1);
        assert_eq!(first.discarded_bytes, 2);
        drop(first); // insert failure leaves the segment available
        let retry = buffer.next_batch().await.unwrap();
        assert!(!metrics.render_prometheus().contains("discarded_total"));
        assert!(buffer.commit(retry).await);
        assert_eq!(buffer.committed_total(), 1);
        let text = metrics.render_prometheus();
        assert!(text.contains("rush_ingest_spool_discarded_total{backend=\"disk\",reason=\"frame_decode\",unit=\"tail\"} 1"));
        assert!(text.contains("rush_ingest_spool_discarded_bytes_total{backend=\"disk\",reason=\"frame_decode\",unit=\"tail\"} 2"));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn spool_migrates_directory_and_segment_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_path("permissions");
        fs::create_dir_all(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        let old = path.join("seg-00000000000000000001-0000000000000001.spool");
        fs::write(&old, []).unwrap();
        fs::set_permissions(&old, fs::Permissions::from_mode(0o644)).unwrap();

        let spool = Spool::open(&path, 1024 * 1024).unwrap();
        spool.append("logs", b"payload").unwrap();
        spool.seal_current();

        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for entry in fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            assert_eq!(
                entry.metadata().unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn spool_rejects_symlink_directory_and_segment() {
        use std::os::unix::fs::symlink;

        let root = temp_path("symlink");
        let real = root.join("real");
        fs::create_dir_all(&real).unwrap();
        let linked = root.join("linked");
        symlink(&real, &linked).unwrap();
        assert!(Spool::open(&linked, 1024).is_err());

        let target = root.join("target");
        fs::write(&target, b"payload").unwrap();
        symlink(
            &target,
            real.join("seg-00000000000000000001-0000000000000001.spool"),
        )
        .unwrap();
        assert!(Spool::open(&real, 1024).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
