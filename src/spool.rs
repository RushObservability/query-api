/// Crash-safe segmented disk spool for ClickHouse write backpressure.
///
/// Each segment is a flat file of framed records:
///   [u32 LE: table name len] [table name bytes] [u32 LE: payload len] [payload bytes]
///
/// Segments are named `seg-<unix_millis>-<seq>.spool`.
/// The current (open) segment is rotated once it reaches SEGMENT_MAX_BYTES (32 MiB).
/// On open, existing `*.spool` files are scanned to restore total_bytes.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const SEGMENT_MAX_BYTES: u64 = 32 * 1024 * 1024; // 32 MiB per segment

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
}

pub struct Spool {
    inner: Mutex<SpoolInner>,
    /// Monotonic count of segments successfully drained (for drain-rate metric).
    committed: AtomicU64,
}

impl Spool {
    /// Open (or create) a spool directory.  Scans existing `*.spool` files to
    /// restore `total_bytes` so the cap is honoured across restarts.
    pub fn open(dir: impl AsRef<Path>, max_bytes: u64) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut total_bytes: u64 = 0;
        let mut max_seq: u64 = 0;

        // Scan existing segments to restore byte count and find max sequence.
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".spool") {
                let size = entry.metadata()?.len();
                total_bytes += size;

                // Parse seq from seg-<ms>-<seq>.spool
                if let Some(seq) = parse_seq(&name_str) {
                    if seq > max_seq {
                        max_seq = seq;
                    }
                }
            }
        }

        Ok(Spool {
            inner: Mutex::new(SpoolInner {
                dir,
                max_bytes,
                total_bytes,
                seq: max_seq,
                current: None,
            }),
            committed: AtomicU64::new(0),
        })
    }

    pub fn committed_total(&self) -> u64 {
        self.committed.load(Ordering::Relaxed)
    }

    /// Age (seconds) of the oldest pending segment, parsed from its `seg-<ms>-`
    /// filename. `None` when empty.
    pub fn oldest_age_secs(&self) -> Option<u64> {
        let g = self.inner.lock().unwrap();
        let mut oldest_ms: Option<u64> = None;
        for entry in fs::read_dir(&g.dir).ok()?.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.ends_with(".spool") { continue; }
            // seg-<ms:020>-<seq>.spool
            if let Some(ms) = name.strip_prefix("seg-").and_then(|r| r.split('-').next()).and_then(|s| s.parse::<u64>().ok()) {
                oldest_ms = Some(oldest_ms.map_or(ms, |o| o.min(ms)));
            }
        }
        let ms = oldest_ms?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
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
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|_| SpoolFull)?;
            g.current = Some((file, 0, path));
        }

        if let Some((file, size, _)) = g.current.as_mut() {
            // Write frame
            let tbl_len = table_bytes.len() as u32;
            let pay_len = payload.len() as u32;
            file.write_all(&tbl_len.to_le_bytes()).map_err(|_| SpoolFull)?;
            file.write_all(table_bytes).map_err(|_| SpoolFull)?;
            file.write_all(&pay_len.to_le_bytes()).map_err(|_| SpoolFull)?;
            file.write_all(payload).map_err(|_| SpoolFull)?;
            *size += frame_len as u64;
            g.total_bytes += frame_len as u64;
        }

        Ok(())
    }

    /// Take the path of the oldest closed (completed) segment, for replay.
    /// Does NOT include the currently open segment.
    pub fn take_oldest_segment(&self) -> Option<PathBuf> {
        let g = self.inner.lock().unwrap();
        let current_path = g.current.as_ref().map(|(_, _, p)| p.clone());

        let mut segments: Vec<PathBuf> = {
            fs::read_dir(&g.dir)
                .ok()?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.extension().and_then(|e| e.to_str()) == Some("spool")
                })
                .collect()
        };
        segments.sort();

        for seg in segments {
            if let Some(ref cur) = current_path {
                if &seg == cur {
                    continue; // skip the open segment
                }
            }
            return Some(seg);
        }
        None
    }

    /// Read all framed records from a segment file.
    ///
    /// Records are not fsync'd per-append (only on rotation), so an unclean
    /// shutdown can leave a torn trailing record. We tolerate that: any EOF
    /// encountered partway through a frame ends parsing and returns the records
    /// read so far, rather than discarding the whole (otherwise-valid) segment.
    pub fn read_segment(path: &Path) -> std::io::Result<Vec<(String, Vec<u8>)>> {
        let mut file = File::open(path)?;
        let mut records = Vec::new();

        loop {
            // Read table name length. A clean EOF here is the normal end.
            let mut buf4 = [0u8; 4];
            match file.read_exact(&mut buf4) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let tbl_len = u32::from_le_bytes(buf4) as usize;
            let mut tbl_buf = vec![0u8; tbl_len];
            if read_exact_or_eof(&mut file, &mut tbl_buf)?.is_none() {
                break; // torn record — stop here
            }
            let table = match String::from_utf8(tbl_buf) {
                Ok(t) => t,
                Err(_) => break, // garbage tail — stop
            };

            // Read payload length + payload.
            if read_exact_or_eof(&mut file, &mut buf4)?.is_none() {
                break;
            }
            let pay_len = u32::from_le_bytes(buf4) as usize;
            let mut payload = vec![0u8; pay_len];
            if read_exact_or_eof(&mut file, &mut payload)?.is_none() {
                break;
            }

            records.push((table, payload));
        }

        Ok(records)
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

    /// Remove a segment file and update total_bytes.
    pub fn remove_segment(&self, path: &Path) {
        let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let _ = fs::remove_file(path);
        let mut g = self.inner.lock().unwrap();
        g.total_bytes = g.total_bytes.saturating_sub(size);
        self.committed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn total_bytes(&self) -> u64 {
        self.inner.lock().unwrap().total_bytes
    }

    pub fn max_bytes(&self) -> u64 {
        self.inner.lock().unwrap().max_bytes
    }

    pub fn segment_count(&self) -> usize {
        let g = self.inner.lock().unwrap();
        fs::read_dir(&g.dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| {
                        e.path().extension().and_then(|x| x.to_str()) == Some("spool")
                    })
                    .count()
            })
            .unwrap_or(0)
    }
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
// `ObjectStore` variant (MinIO/S3 + manifest) is added later (see
// docs/PRD-object-store-ingest-buffer.md); the replayer stays backend-agnostic
// by going through `next_batch()` / `commit()`.

/// A unit of spooled work for the replayer: the records to insert plus an opaque
/// handle used to `commit` (remove/ack) them once successfully written to CH.
pub struct DrainBatch {
    pub records: Vec<(String, Vec<u8>)>,
    handle: BatchHandle,
}

enum BatchHandle {
    Disk(PathBuf),
    ObjectStore(object_store::path::Path),
}

pub enum IngestBuffer {
    Disk(Spool),
    ObjectStore(crate::object_store_spool::ObjectStoreSpool),
}

impl IngestBuffer {
    /// Append a framed record (failure-path hot call). 429 when over the cap.
    pub async fn append(&self, table: &str, payload: &[u8]) -> Result<(), SpoolFull> {
        match self {
            IngestBuffer::Disk(s) => s.append(table, payload),
            IngestBuffer::ObjectStore(s) => s.append(table, payload).await,
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
    /// segment before trying the next.
    pub async fn next_batch(&self) -> Option<DrainBatch> {
        match self {
            IngestBuffer::Disk(s) => {
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
                    match Spool::read_segment(&path) {
                        Ok(records) => return Some(DrainBatch { records, handle: BatchHandle::Disk(path) }),
                        Err(e) => {
                            tracing::error!(error = %e, segment = %path.display(), "ingest buffer: failed to read segment — discarding");
                            s.remove_segment(&path);
                            continue;
                        }
                    }
                }
            }
            IngestBuffer::ObjectStore(s) => {
                s.next_batch().await.map(|(key, records)| DrainBatch {
                    records,
                    handle: BatchHandle::ObjectStore(key),
                })
            }
        }
    }

    /// Commit (remove/ack) a batch that was successfully drained to ClickHouse.
    pub async fn commit(&self, batch: DrainBatch) {
        match (self, batch.handle) {
            (IngestBuffer::Disk(s), BatchHandle::Disk(path)) => s.remove_segment(&path),
            (IngestBuffer::ObjectStore(s), BatchHandle::ObjectStore(key)) => s.commit(&key).await,
            // Mismatched handle/backend can't happen (handles are minted by the same backend).
            _ => tracing::error!("ingest buffer: commit handle/backend mismatch"),
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
