/// Durable ClickHouse write path with disk-based backpressure spool.
///
/// Normal path:  caller builds a `SpoolBatch`, calls `ChWriter::write`.
///               `write` calls `try_insert` directly → zero extra latency.
///
/// Failure path: if `try_insert` returns an error the batch is serialised (see
///               `encode_spool`: zstd-compressed MessagePack) and handed to
///               `Spool::append`.  If the spool is full a
///               `WriteError::Backpressure` is returned (→ HTTP 429).
///
/// Replay:       `spawn_replayer` consumes the oldest segment every ~5 s,
///               retrying with exponential back-off up to 60 s.
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse::Client;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

use crate::models::ingest::{
    ExpHistogramRow, GaugeRow, HistogramRow, LogInsertRow, RumReplayChunk, SumRow, SummaryRow,
    TraceInsertRow,
};
use crate::models::profile::ProfileRow;
use crate::models::rum::RumRecord;
use crate::models::trace::WideEvent;
use crate::shutdown::{ShutdownController, ShutdownPhase};
use crate::spool::{IngestBuffer, SpoolFull};

// ─── Public error type ───────────────────────────────────────────────────────

#[derive(Debug)]
pub enum WriteError {
    /// The accumulator is busy or its durable spool is full. Return HTTP 429.
    Backpressure,
    /// A serialisation or other non-CH error occurred.
    Fatal(String),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Backpressure => write!(f, "backpressure: ingest buffer unavailable"),
            WriteError::Fatal(s) => write!(f, "fatal write error: {s}"),
        }
    }
}

// ─── SpoolBatch enum ─────────────────────────────────────────────────────────

/// A typed batch for one ClickHouse table.  Each variant serialises cleanly
/// to/from JSON so it can be stored in the spool and replayed later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpoolBatch {
    SpansRaw(Vec<TraceInsertRow>),
    Spans(Vec<WideEvent>),
    Logs(Vec<LogInsertRow>),
    Gauge(Vec<GaugeRow>),
    Sum(Vec<SumRow>),
    Rum(Vec<RumRecord>),
    RumReplay(Vec<RumReplayChunk>),
    Histogram(Vec<HistogramRow>),
    ExpHistogram(Vec<ExpHistogramRow>),
    Summary(Vec<SummaryRow>),
    Profiles(Vec<ProfileRow>),
}

impl SpoolBatch {
    /// Target ClickHouse table name for this batch variant.
    pub fn table(&self) -> &'static str {
        match self {
            SpoolBatch::SpansRaw(_) => "spans_raw",
            SpoolBatch::Spans(_) => "spans",
            SpoolBatch::Logs(_) => "logs",
            SpoolBatch::Gauge(_) => "metrics_gauge",
            SpoolBatch::Sum(_) => "metrics_sum",
            SpoolBatch::Rum(_) => "rum",
            SpoolBatch::RumReplay(_) => "rum_replay",
            SpoolBatch::Histogram(_) => "metrics_histogram",
            SpoolBatch::ExpHistogram(_) => "metrics_exp_histogram",
            SpoolBatch::Summary(_) => "metrics_summary",
            SpoolBatch::Profiles(_) => "profile_samples",
        }
    }

    /// Number of rows in the batch (for logging).
    pub fn len(&self) -> usize {
        match self {
            SpoolBatch::SpansRaw(v) => v.len(),
            SpoolBatch::Spans(v) => v.len(),
            SpoolBatch::Logs(v) => v.len(),
            SpoolBatch::Gauge(v) => v.len(),
            SpoolBatch::Sum(v) => v.len(),
            SpoolBatch::Rum(v) => v.len(),
            SpoolBatch::RumReplay(v) => v.len(),
            SpoolBatch::Histogram(v) => v.len(),
            SpoolBatch::ExpHistogram(v) => v.len(),
            SpoolBatch::Summary(v) => v.len(),
            SpoolBatch::Profiles(v) => v.len(),
        }
    }

    /// Whether the batch has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate in-memory size of the batch in bytes: row count × the fixed in-memory
    /// size of the row struct. This is a cheap, real, bounded figure (no serialization on
    /// the hot path) used for the `rush_ingest_bytes_total` self-metric. It reflects
    /// in-memory row footprint, not compressed wire/on-disk bytes — good enough for
    /// relative ingest-volume trends without adding hot-path cost.
    pub fn approx_bytes(&self) -> u64 {
        use std::mem::size_of;
        let per_row = match self {
            SpoolBatch::SpansRaw(_) => size_of::<TraceInsertRow>(),
            SpoolBatch::Spans(_) => size_of::<WideEvent>(),
            SpoolBatch::Logs(_) => size_of::<LogInsertRow>(),
            SpoolBatch::Gauge(_) => size_of::<GaugeRow>(),
            SpoolBatch::Sum(_) => size_of::<SumRow>(),
            SpoolBatch::Rum(_) => size_of::<RumRecord>(),
            SpoolBatch::RumReplay(_) => size_of::<RumReplayChunk>(),
            SpoolBatch::Histogram(_) => size_of::<HistogramRow>(),
            SpoolBatch::ExpHistogram(_) => size_of::<ExpHistogramRow>(),
            SpoolBatch::Summary(_) => size_of::<SummaryRow>(),
            SpoolBatch::Profiles(_) => size_of::<ProfileRow>(),
        };
        (self.len() as u64).saturating_mul(per_row as u64)
    }

    /// Canonical ingest-signal category for this batch variant. One of
    /// "logs", "apm", "metrics", "rum", "profiles" — used by the per-tenant signal gate.
    /// `apm` covers traces (spans), `metrics` covers all metric types.
    pub fn signal_category(&self) -> &'static str {
        match self {
            SpoolBatch::Logs(_) => "logs",
            SpoolBatch::Spans(_) | SpoolBatch::SpansRaw(_) => "apm",
            SpoolBatch::Gauge(_)
            | SpoolBatch::Sum(_)
            | SpoolBatch::Histogram(_)
            | SpoolBatch::ExpHistogram(_)
            | SpoolBatch::Summary(_) => "metrics",
            SpoolBatch::Rum(_) | SpoolBatch::RumReplay(_) => "rum",
            SpoolBatch::Profiles(_) => "profiles",
        }
    }

    /// Stable index for this variant's per-table buffer slot (one slot per
    /// ClickHouse table). Used by the cross-request batcher to coalesce rows of
    /// the same variant from independent requests into one larger insert.
    fn slot(&self) -> usize {
        match self {
            SpoolBatch::SpansRaw(_) => 0,
            SpoolBatch::Spans(_) => 1,
            SpoolBatch::Logs(_) => 2,
            SpoolBatch::Gauge(_) => 3,
            SpoolBatch::Sum(_) => 4,
            SpoolBatch::Rum(_) => 5,
            SpoolBatch::RumReplay(_) => 6,
            SpoolBatch::Histogram(_) => 7,
            SpoolBatch::ExpHistogram(_) => 8,
            SpoolBatch::Summary(_) => 9,
            SpoolBatch::Profiles(_) => 10,
        }
    }

    /// Number of distinct per-table buffer slots.
    const SLOTS: usize = 11;

    /// Append the rows of `other` (same variant) into `self`. Both args must be
    /// the same variant — callers guarantee this via `slot()`.
    fn extend_from(&mut self, other: SpoolBatch) {
        match (self, other) {
            (SpoolBatch::SpansRaw(a), SpoolBatch::SpansRaw(b)) => a.extend(b),
            (SpoolBatch::Spans(a), SpoolBatch::Spans(b)) => a.extend(b),
            (SpoolBatch::Logs(a), SpoolBatch::Logs(b)) => a.extend(b),
            (SpoolBatch::Gauge(a), SpoolBatch::Gauge(b)) => a.extend(b),
            (SpoolBatch::Sum(a), SpoolBatch::Sum(b)) => a.extend(b),
            (SpoolBatch::Rum(a), SpoolBatch::Rum(b)) => a.extend(b),
            (SpoolBatch::RumReplay(a), SpoolBatch::RumReplay(b)) => a.extend(b),
            (SpoolBatch::Histogram(a), SpoolBatch::Histogram(b)) => a.extend(b),
            (SpoolBatch::ExpHistogram(a), SpoolBatch::ExpHistogram(b)) => a.extend(b),
            (SpoolBatch::Summary(a), SpoolBatch::Summary(b)) => a.extend(b),
            (SpoolBatch::Profiles(a), SpoolBatch::Profiles(b)) => a.extend(b),
            // Variant mismatch is a programming error (slots keep them apart).
            _ => debug_assert!(
                false,
                "extend_from called with mismatched SpoolBatch variants"
            ),
        }
    }
}

// ─── Spool payload codec ───────────────────────────────────────────────────────
//
// Spooled batches are stored as `[1-byte format tag][body]`. New writes use
// zstd-compressed MessagePack — far cheaper in CPU and bytes than the JSON this
// path used previously, which matters because a CH outage can spool large
// batches while the process is already under pressure. Reads accept every
// historical format so a rolling deploy (or a restart with already-spooled
// segments) drains cleanly:
//
//   0x01  MessagePack, uncompressed
//   0x02  MessagePack, zstd-compressed   ← current write format
//   else  legacy JSON — serde_json output for the externally-tagged SpoolBatch
//         enum always begins with '{' (0x7B), so it can never collide with a tag.

const SPOOL_FMT_MSGPACK: u8 = 0x01;
const SPOOL_FMT_MSGPACK_ZSTD: u8 = 0x02;
/// zstd level for spool payloads: low (fast). The spool is a failure-path buffer,
/// so cheap CPU matters more than squeezing out the last few percent of ratio.
const SPOOL_ZSTD_LEVEL: i32 = 3;

/// Encode a batch for the spool: zstd-compressed MessagePack behind a format tag.
fn encode_spool(batch: &SpoolBatch) -> Result<Vec<u8>, WriteError> {
    let mp = rmp_serde::to_vec(batch)
        .map_err(|e| WriteError::Fatal(format!("spool msgpack encode: {e}")))?;
    let compressed = zstd::encode_all(mp.as_slice(), SPOOL_ZSTD_LEVEL)
        .map_err(|e| WriteError::Fatal(format!("spool zstd encode: {e}")))?;
    let mut out = Vec::with_capacity(compressed.len() + 1);
    out.push(SPOOL_FMT_MSGPACK_ZSTD);
    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Decode a spool payload written by `encode_spool` OR any earlier format
/// (uncompressed MessagePack, or legacy JSON). The replayer logs and skips the
/// record on error, so any decode failure surfaces as an `Err` here.
fn decode_spool(payload: &[u8]) -> anyhow::Result<SpoolBatch> {
    match payload.first() {
        Some(&SPOOL_FMT_MSGPACK_ZSTD) => {
            let mp = zstd::decode_all(&payload[1..])?;
            Ok(rmp_serde::from_slice(&mp)?)
        }
        Some(&SPOOL_FMT_MSGPACK) => Ok(rmp_serde::from_slice(&payload[1..])?),
        // Legacy JSON (or empty/unknown) — fall back to serde_json.
        _ => Ok(serde_json::from_slice(payload)?),
    }
}

fn batch_serialized_size(batch: &SpoolBatch) -> Result<usize, WriteError> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter(0);
    serde_json::to_writer(&mut count, batch)
        .map_err(|error| WriteError::Fatal(error.to_string()))?;
    Ok(count.0)
}

// ─── Cross-request insert batching ─────────────────────────────────────────────
//
// Each ingest HTTP request previously triggered an immediate typed insert (and a
// metrics request fired up to 5 — one per metric type). Under high request rates
// that means many tiny inserts, which ClickHouse dislikes (each insert is a part).
//
// `BatchAccumulator` coalesces requests into bounded per-table buffers. Before
// admitting rows that reach a row/byte limit, the writer flushes the previously
// accepted buffer. Failed flushes retain those rows and reject new requests
// without retaining them. The same slot lock covers admission and delivery.
//
// With batching enabled, success can still mean an in-memory acknowledgment;
// a hard process crash can lose rows that have not reached the durable spool.
// Graceful shutdown retries buffered writes. Set RUSH_INGEST_BATCH_ROWS=1 or
// RUSH_INGEST_BATCH_MS=0 to use synchronous insert/spool acknowledgment instead.
// No in-memory byte accounting changes that durability tradeoff.

#[derive(Clone, Copy, Debug)]
pub struct BatchConfig {
    /// Flush a table's buffer once it holds at least this many rows.
    pub max_rows: usize,
    /// Flush a table's buffer once this long has elapsed since its first row.
    pub max_age: Duration,
    /// Maximum serialized bytes retained per table. Oversized requests bypass
    /// the accumulator and receive synchronous insert/spool backpressure.
    pub max_bytes: usize,
}

impl BatchConfig {
    /// Read `RUSH_INGEST_BATCH_ROWS` (5000), `RUSH_INGEST_BATCH_MS` (500), and
    /// `RUSH_INGEST_BATCH_BYTES` (8 MiB per table). Zero bytes, `rows <= 1`, or `ms == 0` disables
    /// batching (every `write` flushes immediately — today's behavior).
    pub fn from_env() -> Self {
        let max_rows = std::env::var("RUSH_INGEST_BATCH_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5_000usize);
        let max_ms = std::env::var("RUSH_INGEST_BATCH_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500u64);
        BatchConfig {
            max_rows,
            max_age: Duration::from_millis(max_ms),
            max_bytes: std::env::var("RUSH_INGEST_BATCH_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(8 * 1024 * 1024),
        }
    }

    /// True when batching is effectively disabled (flush every write inline).
    fn disabled(&self) -> bool {
        self.max_rows <= 1 || self.max_age.is_zero() || self.max_bytes == 0
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        BatchConfig {
            max_rows: 5_000,
            max_age: Duration::from_millis(500),
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

/// One per-table buffer slot: the coalesced batch plus the instant its first
/// row was buffered (for the age-based flush trigger).
#[derive(Default)]
struct TableBuffer {
    batch: Option<SpoolBatch>,
    first_row_at: Option<Instant>,
    bytes: usize,
    /// Set before an attempted flush, including one cancelled by its caller.
    /// New writes cannot enter the slot until these acknowledged rows drain.
    needs_flush: bool,
}

/// The in-process batching layer: one buffer per ClickHouse table, each behind
/// its own async mutex so independent ingest tasks contend only with same-table
/// peers and the flusher.
pub struct BatchAccumulator {
    cfg: BatchConfig,
    slots: [AsyncMutex<TableBuffer>; SpoolBatch::SLOTS],
}

impl BatchAccumulator {
    fn new(cfg: BatchConfig) -> Self {
        BatchAccumulator {
            cfg,
            slots: std::array::from_fn(|_| AsyncMutex::new(TableBuffer::default())),
        }
    }

    async fn is_empty(&self) -> bool {
        for slot in &self.slots {
            match slot.try_lock() {
                Ok(buffer) if buffer.batch.is_none() => {}
                // A write in flight must also prevent shutdown from treating
                // the accumulator as drained, without blocking on its network I/O.
                _ => return false,
            }
        }
        true
    }
}

// ─── ChWriter ────────────────────────────────────────────────────────────────

/// Cloneable ClickHouse writer with an integrated durable buffer (spool).
#[derive(Clone)]
pub struct ChWriter {
    ch: Client,
    /// Durable failure-path buffer. Backend-agnostic: `Disk` (default, no object
    /// store) or, later, `ObjectStore`. See `crate::spool::IngestBuffer`.
    pub buffer: Arc<IngestBuffer>,
    /// Hot-swappable compiled metric firewall (applied to metric batches before
    /// insert/spool). Refreshed by a background task and on config change.
    pub firewall: Arc<std::sync::RwLock<Arc<crate::metric_firewall::MetricFirewall>>>,
    /// Cross-request insert batcher. Rows from multiple ingest requests coalesce
    /// here into fewer, larger ClickHouse inserts (see `BatchAccumulator`).
    batcher: Arc<BatchAccumulator>,
}

impl ChWriter {
    /// Construct with batching config from the environment (see `BatchConfig`).
    pub fn new(ch: Client, buffer: Arc<IngestBuffer>) -> Self {
        Self::with_batch_config(ch, buffer, BatchConfig::from_env())
    }

    pub fn with_batch_config(ch: Client, buffer: Arc<IngestBuffer>, cfg: BatchConfig) -> Self {
        ChWriter {
            ch,
            buffer,
            firewall: Arc::new(std::sync::RwLock::new(Arc::new(
                crate::metric_firewall::MetricFirewall::default(),
            ))),
            batcher: Arc::new(BatchAccumulator::new(cfg)),
        }
    }

    /// The active batching configuration.
    pub fn batch_config(&self) -> BatchConfig {
        self.batcher.cfg
    }

    /// Apply the metric firewall to a metric batch in place (drops blocked
    /// datapoints, strips dropped labels). No-op for non-metric batches.
    fn apply_firewall(&self, batch: &mut SpoolBatch) {
        let fw = match self.firewall.read() {
            Ok(g) => g.clone(), // cheap Arc clone; guard released immediately
            Err(_) => return,
        };
        if fw.is_empty() {
            return;
        }
        let dropped = match batch {
            SpoolBatch::Gauge(rows) => fw.apply(rows),
            SpoolBatch::Sum(rows) => fw.apply(rows),
            SpoolBatch::Histogram(rows) => fw.apply(rows),
            SpoolBatch::ExpHistogram(rows) => fw.apply(rows),
            SpoolBatch::Summary(rows) => fw.apply(rows),
            _ => 0,
        };
        if dropped > 0 {
            tracing::debug!(
                dropped = dropped,
                table = batch.table(),
                "metric firewall blocked datapoints"
            );
        }
    }

    /// Write a batch to ClickHouse.
    ///
    /// Batching accepts rows into a bounded per-table accumulator. Reaching a
    /// limit flushes previously accepted rows before admitting this request;
    /// a failed flush rejects it without adding its rows to the queue. Requests
    /// exceeding either limit are written synchronously. The age flusher and
    /// shutdown share the same lock and preserve accepted rows across failures.
    /// Disabling batching makes every request use synchronous insert/spool.
    pub async fn write(&self, mut batch: SpoolBatch) -> Result<(), WriteError> {
        // Metric firewall runs once here over the request's rows so the firewall
        // semantics are unchanged whether or not batching coalesces afterward
        // (allow→block precedence + label stripping all happen pre-buffer). The
        // spooled/inserted data is therefore already filtered, exactly as before.
        self.apply_firewall(&mut batch);
        if batch.is_empty() {
            return Ok(());
        }

        if self.batcher.cfg.disabled() {
            // Batching off: preserve today's synchronous insert→spool→429 path.
            return self.write_now(&batch).await;
        }

        // Keep the slot locked through delivery. Background and inline flushes
        // must not detach a batch while another request fills the same slot.
        let bytes = batch_serialized_size(&batch)?;
        // Do not turn pending mutex waiters into an unbounded second queue of
        // request bodies while ClickHouse is slow or unresponsive.
        let mut slot = self.batcher.slots[batch.slot()]
            .try_lock()
            .map_err(|_| WriteError::Backpressure)?;
        let rows = slot.batch.as_ref().map_or(0, SpoolBatch::len);
        if slot.needs_flush
            || rows.saturating_add(batch.len()) >= self.batcher.cfg.max_rows
            || slot.bytes.saturating_add(bytes) >= self.batcher.cfg.max_bytes
        {
            // Only previously accepted rows are retained on failure. The current
            // request has not entered the accumulator and is safe to retry.
            self.flush_slot(&mut slot).await?;
        }
        if batch.len() >= self.batcher.cfg.max_rows || bytes >= self.batcher.cfg.max_bytes {
            return self.write_now(&batch).await;
        }
        match slot.batch.as_mut() {
            Some(existing) => existing.extend_from(batch),
            None => {
                slot.first_row_at = Some(Instant::now());
                slot.batch = Some(batch);
            }
        }
        slot.bytes = slot.bytes.saturating_add(bytes);
        Ok(())
    }

    /// Borrow the queued batch across delivery so cancellation or failure leaves
    /// acknowledged rows in place. Clearing it requires a successful write.
    async fn flush_slot(&self, slot: &mut TableBuffer) -> Result<(), WriteError> {
        if let Some(batch) = &slot.batch {
            slot.needs_flush = true;
            self.write_now(batch).await?;
        }
        *slot = TableBuffer::default();
        Ok(())
    }

    /// Perform the actual insert for a (firewall-already-applied) batch, spooling
    /// on CH failure. This is the single durable write path shared by the inline
    /// `write` (batching-disabled), the row-threshold inline flush, the
    /// background flusher, and graceful shutdown.
    async fn write_now(&self, batch: &SpoolBatch) -> Result<(), WriteError> {
        if batch.is_empty() {
            return Ok(());
        }
        let row_count = batch.len();
        let table = batch.table();

        match try_insert(&self.ch, batch).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    table = table,
                    rows = row_count,
                    "ch insert failed — spooling batch"
                );

                // Serialise for the spool (zstd-compressed MessagePack).
                let payload = encode_spool(batch)?;

                match self.buffer.append(table, payload).await {
                    Ok(()) => Ok(()),
                    Err(SpoolFull) => Err(WriteError::Backpressure),
                }
            }
        }
    }

    /// Flush every buffered table batch to ClickHouse (spooling on failure).
    /// Called on graceful shutdown so no buffered rows are silently dropped.
    pub async fn flush_all(&self) {
        stream::iter(0..SpoolBatch::SLOTS)
            .for_each_concurrent(4, |index| async move {
                let mut slot = self.batcher.slots[index].lock().await;
                if let Err(error) = self.flush_slot(&mut slot).await {
                    tracing::warn!(%error, index, "flush_all: buffered rows retained");
                }
            })
            .await;
    }

    /// Whether the cross-request batcher still contains rows that have not
    /// reached ClickHouse or the durable spool.
    pub async fn has_pending_batches(&self) -> bool {
        !self.batcher.is_empty().await
    }

    /// Spawn the background flush task. It wakes on a fixed cadence (a fraction of
    /// `max_age`, clamped to a sane range) and flushes any table buffer whose
    /// oldest row has aged past `max_age`. The row-count trigger is handled inline
    /// in `write`, so this task only enforces the time bound. No-op when batching
    /// is disabled.
    pub fn spawn_flusher(&self) {
        if self.batcher.cfg.disabled() {
            return;
        }
        let me = self.clone();
        let tick = me
            .batcher
            .cfg
            .max_age
            .min(Duration::from_millis(100))
            .max(Duration::from_millis(10));
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let now = Instant::now();
                stream::iter(0..SpoolBatch::SLOTS)
                    .for_each_concurrent(4, |index| {
                        let me = &me;
                        async move {
                            let mut slot = me.batcher.slots[index].lock().await;
                            if slot.first_row_at.is_some_and(|at| now.saturating_duration_since(at) >= me.batcher.cfg.max_age) {
                                if let Err(error) = me.flush_slot(&mut slot).await {
                                    tracing::warn!(%error, index, "batch flush failed; buffered rows retained");
                                }
                            }
                        }
                    })
                    .await;
            }
        });
    }

    /// Total bytes currently occupying the buffer.
    pub fn spool_bytes(&self) -> u64 {
        self.buffer.total_bytes()
    }

    /// Number of pending segments/objects in the buffer.
    pub fn spool_segments(&self) -> usize {
        self.buffer.segment_count()
    }

    /// Spawn a background tokio task that replays spooled segments to CH.
    pub fn spawn_replayer(self, shutdown: ShutdownController) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(5);
            const MAX_BACKOFF: Duration = Duration::from_secs(60);
            const POLL_INTERVAL: Duration = Duration::from_secs(5);

            loop {
                match shutdown.phase() {
                    ShutdownPhase::Running => {
                        tokio::select! {
                            _ = tokio::time::sleep(POLL_INTERVAL) => {}
                            _ = shutdown.wait_for_request() => {}
                        }
                        if shutdown.phase() != ShutdownPhase::Running {
                            continue;
                        }
                    }
                    ShutdownPhase::Stopping => {
                        // The HTTP server has stopped admitting work, but the
                        // main task must flush in-memory batches first.
                        shutdown.wait_for_drain().await;
                        continue;
                    }
                    ShutdownPhase::Finished => return,
                    ShutdownPhase::Draining => {}
                }

                let drain = match self.buffer.next_batch().await {
                    Some(d) => d,
                    None => {
                        // Stay alive during draining: the main task may need
                        // to retry flushing a batch after the spool frees space.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };

                let mut all_ok = true;
                for (table, payload) in &drain.records {
                    let batch: SpoolBatch = match decode_spool(payload) {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                table = table,
                                "replayer: deserialise failed — skipping record"
                            );
                            continue;
                        }
                    };

                    if let Err(e) = try_insert(&self.ch, &batch).await {
                        tracing::warn!(
                            error = %e,
                            table = table,
                            "replayer: CH insert failed — backing off"
                        );
                        all_ok = false;
                        break;
                    }
                }

                if all_ok {
                    let n = drain.records.len();
                    self.buffer.commit(drain).await;
                    tracing::info!(records = n, "replayer: batch replayed and committed");
                    backoff = Duration::from_secs(5); // reset on success
                } else {
                    // CH is still down — back off and stop this replay pass.
                    // (drain not committed → retried next pass.) During graceful
                    // shutdown this intentionally waits rather than exiting;
                    // Kubernetes can enforce its termination grace period.
                    tracing::warn!(
                        backoff_secs = backoff.as_secs(),
                        "replayer: CH unavailable, backing off"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        })
    }
}

// ─── try_insert helper ───────────────────────────────────────────────────────

/// Perform the actual typed ClickHouse INSERT for any `SpoolBatch` variant.
/// This is used both by `ChWriter::write` (normal path) and the replayer.
pub async fn try_insert(ch: &Client, batch: &SpoolBatch) -> Result<(), clickhouse::error::Error> {
    match batch {
        SpoolBatch::Profiles(rows) => {
            let mut ins = ch.insert("profile_samples")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::SpansRaw(rows) => {
            let mut ins = ch.insert("spans_raw")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Spans(rows) => {
            let mut ins = ch.insert("spans")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Logs(rows) => {
            let mut ins = ch.insert("logs")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Gauge(rows) => {
            let mut ins = ch.insert("metrics_gauge")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Sum(rows) => {
            let mut ins = ch.insert("metrics_sum")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Rum(rows) => {
            let mut ins = ch.insert("rum")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::RumReplay(rows) => {
            let mut ins = ch.insert("rum_replay")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Histogram(rows) => {
            let mut ins = ch.insert("metrics_histogram")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::ExpHistogram(rows) => {
            let mut ins = ch.insert("metrics_exp_histogram")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Summary(rows) => {
            let mut ins = ch.insert("metrics_summary")?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
    }
}

// ─── Batching trigger unit tests (no live ClickHouse) ───────────────────────────

#[cfg(test)]
mod batch_tests {
    use super::*;
    #[test]
    fn profiles_have_a_distinct_signal_slot_and_spool_round_trip() {
        let batch = SpoolBatch::Profiles(vec![]);
        assert_eq!(batch.signal_category(), "profiles");
        assert_eq!(batch.table(), "profile_samples");
        assert_ne!(batch.slot(), SpoolBatch::Logs(vec![]).slot());
        assert!(matches!(
            decode_spool(&encode_spool(&batch).unwrap()).unwrap(),
            SpoolBatch::Profiles(_)
        ));
    }
    use crate::models::ingest::GaugeRow;

    fn gauge(n: usize) -> SpoolBatch {
        let mk = || GaugeRow {
            tenant_id: "t".into(),
            resource_attributes: std::sync::Arc::new(Vec::new()),
            resource_schema_url: "".into(),
            scope_name: "".into(),
            scope_version: "".into(),
            scope_attributes: std::sync::Arc::new(Vec::new()),
            scope_dropped_attr_count: 0,
            scope_schema_url: "".into(),
            service_name: "".into(),
            metric_name: "m".into(),
            metric_description: "".into(),
            metric_unit: "".into(),
            attributes: Vec::new(),
            start_time_unix: 0,
            time_unix: 0,
            value: 0.0,
            flags: 0,
            exemplars_filtered_attributes: vec![],
            exemplars_time_unix: vec![],
            exemplars_value: vec![],
            exemplars_span_id: vec![],
            exemplars_trace_id: vec![],
        };
        SpoolBatch::Gauge((0..n).map(|_| mk()).collect())
    }

    #[test]
    fn config_disabled_detection() {
        assert!(
            BatchConfig {
                max_rows: 1,
                max_age: Duration::from_millis(500),
                ..BatchConfig::default()
            }
            .disabled()
        );
        assert!(
            BatchConfig {
                max_rows: 5000,
                max_age: Duration::ZERO,
                ..BatchConfig::default()
            }
            .disabled()
        );
        assert!(
            BatchConfig {
                max_rows: 0,
                max_age: Duration::from_millis(500),
                ..BatchConfig::default()
            }
            .disabled()
        );
        assert!(
            !BatchConfig {
                max_rows: 5000,
                max_age: Duration::from_millis(500),
                ..BatchConfig::default()
            }
            .disabled()
        );
    }

    // ── Spool payload codec ──
    //
    // SpoolBatch's field tree doesn't derive PartialEq, so equality is checked
    // via canonical JSON (serde_json::to_value), which is enough to confirm the
    // decoded batch is structurally identical to the original.
    fn same_batch(a: &SpoolBatch, b: &SpoolBatch) -> bool {
        serde_json::to_value(a).unwrap() == serde_json::to_value(b).unwrap()
    }

    #[test]
    fn spool_roundtrips_through_zstd_msgpack() {
        let batch = gauge(3);
        let encoded = encode_spool(&batch).expect("encode");
        assert_eq!(
            encoded[0], SPOOL_FMT_MSGPACK_ZSTD,
            "new writes use the zstd-msgpack tag"
        );
        let decoded = decode_spool(&encoded).expect("decode");
        assert!(same_batch(&batch, &decoded));
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn spool_decode_accepts_legacy_json() {
        // A payload written by the pre-change binary: raw serde_json, no tag byte.
        let batch = gauge(2);
        let legacy = serde_json::to_vec(&batch).unwrap();
        assert_eq!(
            legacy[0], b'{',
            "legacy JSON starts with '{{', never a tag byte"
        );
        let decoded = decode_spool(&legacy).expect("legacy json still decodes");
        assert!(same_batch(&batch, &decoded));
    }

    #[test]
    fn spool_decode_accepts_uncompressed_msgpack() {
        // The 0x01 (uncompressed MessagePack) format is also accepted on read.
        let batch = gauge(4);
        let mut payload = vec![SPOOL_FMT_MSGPACK];
        payload.extend_from_slice(&rmp_serde::to_vec(&batch).unwrap());
        let decoded = decode_spool(&payload).expect("uncompressed msgpack decodes");
        assert!(same_batch(&batch, &decoded));
    }

    #[test]
    fn spool_encoded_payload_is_not_json() {
        // Guards the tag/JSON disambiguation: a new encoded payload must not be
        // mistakable for legacy JSON.
        let encoded = encode_spool(&gauge(1)).expect("encode");
        assert!(serde_json::from_slice::<serde_json::Value>(&encoded).is_err());
    }
    struct TestWriter(ChWriter, std::path::PathBuf);
    impl TestWriter {
        fn new(url: &str, cfg: BatchConfig) -> Self {
            let dir =
                std::env::temp_dir().join(format!("rush-ingest-test-{}", uuid::Uuid::new_v4()));
            let spool = crate::spool::Spool::open(&dir, 0).unwrap();
            Self(
                ChWriter::with_batch_config(
                    Client::default()
                        .with_url(url)
                        .with_compression(clickhouse::Compression::None),
                    Arc::new(IngestBuffer::Disk(spool)),
                    cfg,
                ),
                dir,
            )
        }
    }
    impl Drop for TestWriter {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.1);
        }
    }

    #[tokio::test]
    async fn rejected_retries_do_not_enter_the_buffer() {
        let test = TestWriter::new(
            "http://127.0.0.1:9",
            BatchConfig {
                max_rows: 2,
                ..BatchConfig::default()
            },
        );
        let accepted = gauge(1);
        test.0.write(accepted.clone()).await.unwrap();
        let mut tasks = Vec::new();
        for _ in 0..20 {
            let writer = test.0.clone();
            tasks.push(tokio::spawn(async move { writer.write(gauge(1)).await }));
        }
        for task in tasks {
            assert!(matches!(task.await.unwrap(), Err(WriteError::Backpressure)));
        }
        let slot = test.0.batcher.slots[accepted.slot()].lock().await;
        assert!(same_batch(slot.batch.as_ref().unwrap(), &accepted));
        assert!(slot.needs_flush);
    }

    #[tokio::test]
    async fn failed_background_flush_blocks_even_small_new_writes() {
        let test = TestWriter::new("http://127.0.0.1:9", BatchConfig::default());
        test.0.write(gauge(1)).await.unwrap();
        test.0.flush_all().await;
        assert!(matches!(
            test.0.write(gauge(1)).await,
            Err(WriteError::Backpressure)
        ));
        let slot = test.0.batcher.slots[gauge(1).slot()].lock().await;
        assert_eq!(slot.batch.as_ref().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn byte_budget_applies_before_row_threshold() {
        let one = batch_serialized_size(&gauge(1)).unwrap();
        let test = TestWriter::new(
            "http://127.0.0.1:9",
            BatchConfig {
                max_rows: 1000,
                max_bytes: one + 1,
                ..BatchConfig::default()
            },
        );
        test.0.write(gauge(1)).await.unwrap();
        assert!(matches!(
            test.0.write(gauge(1)).await,
            Err(WriteError::Backpressure)
        ));
        let slot = test.0.batcher.slots[gauge(1).slot()].lock().await;
        assert_eq!(slot.bytes, one);
        assert_eq!(slot.batch.as_ref().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn oversized_rejected_request_is_never_retained() {
        let test = TestWriter::new(
            "http://127.0.0.1:9",
            BatchConfig {
                max_bytes: 1,
                ..BatchConfig::default()
            },
        );
        assert!(matches!(
            test.0.write(gauge(1)).await,
            Err(WriteError::Backpressure)
        ));
        assert!(!test.0.has_pending_batches().await);
    }

    #[tokio::test]
    async fn recovery_delivers_accepted_rows_once_and_allows_new_writes() {
        use clickhouse::test::{Mock, handlers};
        let mock = Mock::new();
        let test = TestWriter::new(
            mock.url(),
            BatchConfig {
                max_rows: 3,
                ..BatchConfig::default()
            },
        );
        test.0.write(gauge(2)).await.unwrap();
        mock.add(handlers::failure(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        ));
        assert!(matches!(
            test.0.write(gauge(1)).await,
            Err(WriteError::Backpressure)
        ));
        let old = mock.add(handlers::record::<GaugeRow>());
        test.0.write(gauge(1)).await.unwrap();
        assert_eq!(old.collect::<Vec<_>>().await.len(), 2);
        let new = mock.add(handlers::record::<GaugeRow>());
        test.0.flush_all().await;
        assert_eq!(new.collect::<Vec<_>>().await.len(), 1);
        assert!(!test.0.has_pending_batches().await);
        assert_eq!(test.0.spool_segments(), 0);
    }

    #[tokio::test]
    async fn aged_buffer_flushes_and_shutdown_does_not_send_it_twice() {
        use clickhouse::test::{Mock, handlers};
        let mock = Mock::new();
        let test = TestWriter::new(
            mock.url(),
            BatchConfig {
                max_age: Duration::from_millis(10),
                ..BatchConfig::default()
            },
        );
        let received = mock.add(handlers::record::<GaugeRow>());
        test.0.write(gauge(2)).await.unwrap();
        test.0.spawn_flusher();
        let rows = tokio::time::timeout(Duration::from_secs(2), received.collect::<Vec<_>>())
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        test.0.flush_all().await;
        assert!(!test.0.has_pending_batches().await);
    }

    #[tokio::test]
    async fn stalled_flush_rejects_new_writes_and_cancellation_preserves_accepted_rows() {
        use axum::{Router, routing::any};
        let reached = Arc::new(tokio::sync::Notify::new());
        let signal = reached.clone();
        let router = Router::new().route(
            "/",
            any(move || {
                let signal = signal.clone();
                async move {
                    signal.notify_one();
                    std::future::pending::<()>().await;
                    ""
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let test = TestWriter::new(&url, BatchConfig::default());
        test.0.write(gauge(1)).await.unwrap();
        let writer = test.0.clone();
        let flush = tokio::spawn(async move {
            writer.flush_all().await;
        });
        tokio::time::timeout(Duration::from_secs(2), reached.notified())
            .await
            .unwrap();
        assert!(test.0.has_pending_batches().await);
        let rejected = tokio::time::timeout(Duration::from_millis(50), test.0.write(gauge(1)))
            .await
            .unwrap();
        assert!(matches!(rejected, Err(WriteError::Backpressure)));
        flush.abort();
        assert!(flush.await.unwrap_err().is_cancelled());
        let slot = test.0.batcher.slots[gauge(1).slot()].lock().await;
        assert_eq!(slot.batch.as_ref().unwrap().len(), 1);
        assert!(slot.needs_flush);
        server.abort();
    }
}
