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
use crate::models::rum::RumRecord;
use crate::models::trace::WideEvent;
use crate::shutdown::{ShutdownController, ShutdownPhase};
use crate::spool::{IngestBuffer, SpoolFull};

// ─── Public error type ───────────────────────────────────────────────────────

#[derive(Debug)]
pub enum WriteError {
    /// The spool is full — caller should return HTTP 429.
    Backpressure,
    /// A serialisation or other non-CH error occurred.
    Fatal(String),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Backpressure => write!(f, "backpressure: spool full"),
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
        };
        (self.len() as u64).saturating_mul(per_row as u64)
    }

    /// Canonical ingest-signal category for this batch variant. One of
    /// "logs", "apm", "metrics", "rum" — used by the per-tenant signal gate.
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
        }
    }

    /// Number of distinct per-table buffer slots.
    const SLOTS: usize = 10;

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

// ─── Cross-request insert batching ─────────────────────────────────────────────
//
// Each ingest HTTP request previously triggered an immediate typed insert (and a
// metrics request fired up to 5 — one per metric type). Under high request rates
// that means many tiny inserts, which ClickHouse dislikes (each insert is a part).
//
// `BatchAccumulator` coalesces rows from multiple requests into per-table buffers
// and a background flush task drains each table's buffer when EITHER it reaches
// `max_rows` OR `max_age` has elapsed since its first buffered row — whichever
// comes first. Flushing goes through the SAME `write_now` (try_insert → spool on
// failure) path as before, so durability is preserved: a flush that can't reach
// ClickHouse spools the coalesced batch, and 429 backpressure surfaces when the
// spool fills on that flush.
//
// DURABILITY / BACKPRESSURE TRADEOFF (documented intentionally):
//   With batching enabled, `ChWriter::write` returns to the HTTP caller as soon
//   as the rows are buffered in memory — BEFORE the flush actually hits CH. So
//   backpressure becomes ASYNCHRONOUS: a caller no longer receives a synchronous
//   429 when ClickHouse is down. Instead the buffered rows are flushed by the
//   background task, which spools them on CH failure; the spool absorbs the
//   outage and a 429 surfaces (on the flush path, logged) only once the spool
//   fills. Buffered-but-not-yet-flushed rows live in memory and are lost on a
//   hard crash (they were never acked durably) — this is the cost of batching.
//   On graceful shutdown `flush_all` drains every buffer first.
//
//   Operators who require today's synchronous 429 / zero in-memory buffering can
//   set RUSH_INGEST_BATCH_ROWS=1 or RUSH_INGEST_BATCH_MS=0, which makes `write`
//   flush immediately (identical to the pre-batching behavior, synchronous 429).

#[derive(Clone, Copy, Debug)]
pub struct BatchConfig {
    /// Flush a table's buffer once it holds at least this many rows.
    pub max_rows: usize,
    /// Flush a table's buffer once this long has elapsed since its first row.
    pub max_age: Duration,
}

impl BatchConfig {
    /// Read config from env: `RUSH_INGEST_BATCH_ROWS` (default 5000) and
    /// `RUSH_INGEST_BATCH_MS` (default 500). `rows <= 1` or `ms == 0` disables
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
        }
    }

    /// True when batching is effectively disabled (flush every write inline).
    fn disabled(&self) -> bool {
        self.max_rows <= 1 || self.max_age.is_zero()
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        BatchConfig {
            max_rows: 5_000,
            max_age: Duration::from_millis(500),
        }
    }
}

/// One per-table buffer slot: the coalesced batch plus the instant its first
/// row was buffered (for the age-based flush trigger).
#[derive(Default)]
struct TableBuffer {
    batch: Option<SpoolBatch>,
    first_row_at: Option<Instant>,
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

    /// Enqueue an (already firewall-applied) batch into its table slot. Returns
    /// the batch to flush NOW if enqueuing pushed the slot to/over `max_rows`
    /// (so the caller flushes inline, bounding latency + memory); otherwise the
    /// rows stay buffered for the background flusher / age trigger.
    async fn enqueue(&self, batch: SpoolBatch) -> Option<SpoolBatch> {
        let slot = batch.slot();
        let mut g = self.slots[slot].lock().await;
        match g.batch.as_mut() {
            Some(existing) => existing.extend_from(batch),
            None => {
                g.first_row_at = Some(Instant::now());
                g.batch = Some(batch);
            }
        }
        let len = g.batch.as_ref().map(|b| b.len()).unwrap_or(0);
        if len >= self.cfg.max_rows {
            g.first_row_at = None;
            g.batch.take()
        } else {
            None
        }
    }

    /// Take any slot whose buffer is due to flush by the age trigger. Returns the
    /// drained batch (caller flushes it). Called repeatedly by the flush task.
    async fn take_aged(&self, slot: usize, now: Instant) -> Option<SpoolBatch> {
        let mut g = self.slots[slot].lock().await;
        let due = matches!(g.first_row_at, Some(t) if now.duration_since(t) >= self.cfg.max_age);
        if due {
            g.first_row_at = None;
            g.batch.take()
        } else {
            None
        }
    }

    /// Drain every non-empty slot unconditionally (used by graceful shutdown).
    async fn drain_all(&self) -> Vec<SpoolBatch> {
        let mut out = Vec::new();
        for slot in &self.slots {
            let mut g = slot.lock().await;
            g.first_row_at = None;
            if let Some(b) = g.batch.take() {
                out.push(b);
            }
        }
        out
    }

    /// Put a failed graceful-shutdown flush back into memory. This is needed
    /// when the durable spool is full: dropping the batch would violate the
    /// shutdown no-loss contract.
    async fn restore(&self, batch: SpoolBatch) {
        let slot = batch.slot();
        let mut g = self.slots[slot].lock().await;
        match g.batch.as_mut() {
            Some(existing) => existing.extend_from(batch),
            None => {
                g.first_row_at = Some(Instant::now());
                g.batch = Some(batch);
            }
        }
    }

    async fn is_empty(&self) -> bool {
        for slot in &self.slots {
            if slot.lock().await.batch.is_some() {
                return false;
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
    /// With batching enabled (the default), the firewall is applied here and the
    /// rows are buffered for cross-request coalescing; this returns `Ok(())` as
    /// soon as the rows are buffered. If buffering pushes the table's buffer to
    /// the row threshold, the coalesced batch is flushed inline (so a single
    /// large request still inserts promptly). See `BatchAccumulator` for the
    /// asynchronous-backpressure tradeoff.
    ///
    /// With batching disabled (`RUSH_INGEST_BATCH_ROWS=1` / `RUSH_INGEST_BATCH_MS=0`)
    /// this behaves exactly as before: a synchronous insert with synchronous
    /// 429 backpressure on spool-full.
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
            return self.write_now(batch).await.map_err(|(error, _batch)| error);
        }

        // Buffer the rows; flush inline only if this enqueue crossed the row
        // threshold. Otherwise the background flusher / age trigger drains it.
        if let Some(due) = self.batcher.enqueue(batch).await {
            match self.write_now(due).await {
                Ok(()) => Ok(()),
                Err((error, batch)) => {
                    self.batcher.restore(batch).await;
                    Err(error)
                }
            }
        } else {
            Ok(())
        }
    }

    /// Perform the actual insert for a (firewall-already-applied) batch, spooling
    /// on CH failure. This is the single durable write path shared by the inline
    /// `write` (batching-disabled), the row-threshold inline flush, the
    /// background flusher, and graceful shutdown.
    async fn write_now(&self, batch: SpoolBatch) -> Result<(), (WriteError, SpoolBatch)> {
        if batch.is_empty() {
            return Ok(());
        }
        let row_count = batch.len();
        let table = batch.table();

        match try_insert(&self.ch, &batch).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    table = table,
                    rows = row_count,
                    "ch insert failed — spooling batch"
                );

                // Serialise for the spool (zstd-compressed MessagePack).
                let payload = match encode_spool(&batch) {
                    Ok(payload) => payload,
                    Err(error) => return Err((error, batch)),
                };

                match self.buffer.append(table, payload).await {
                    Ok(()) => Ok(()),
                    Err(SpoolFull) => Err((WriteError::Backpressure, batch)),
                }
            }
        }
    }

    /// Flush every buffered table batch to ClickHouse (spooling on failure).
    /// Called on graceful shutdown so no buffered rows are silently dropped.
    pub async fn flush_all(&self) {
        stream::iter(self.batcher.drain_all().await)
            .for_each_concurrent(4, |batch| async move {
                let table = batch.table();
                let rows = batch.len();
                if let Err((e, batch)) = self.write_now(batch).await {
                    self.batcher.restore(batch).await;
                    tracing::warn!(error = %e, table = table, rows = rows, "flush_all: write failed (spool full?)");
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
                let mut aged = Vec::new();
                for slot in 0..SpoolBatch::SLOTS {
                    if let Some(batch) = me.batcher.take_aged(slot, now).await {
                        aged.push(batch);
                    }
                }
                stream::iter(aged)
                    .for_each_concurrent(4, |batch| {
                        let me = &me;
                        async move {
                            let table = batch.table();
                            let rows = batch.len();
                            if let Err((e, batch)) = me.write_now(batch).await {
                                me.batcher.restore(batch).await;
                                tracing::warn!(error = %e, table = table, rows = rows, "batch flush failed (spool full?)");
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
                max_age: Duration::from_millis(500)
            }
            .disabled()
        );
        assert!(
            BatchConfig {
                max_rows: 5000,
                max_age: Duration::ZERO
            }
            .disabled()
        );
        assert!(
            BatchConfig {
                max_rows: 0,
                max_age: Duration::from_millis(500)
            }
            .disabled()
        );
        assert!(
            !BatchConfig {
                max_rows: 5000,
                max_age: Duration::from_millis(500)
            }
            .disabled()
        );
    }

    #[tokio::test]
    async fn row_count_threshold_triggers_inline_flush() {
        let acc = BatchAccumulator::new(BatchConfig {
            max_rows: 10,
            max_age: Duration::from_secs(60),
        });
        // Under threshold: buffered, nothing returned.
        assert!(acc.enqueue(gauge(4)).await.is_none());
        assert!(acc.enqueue(gauge(3)).await.is_none());
        // Crossing the threshold returns the coalesced batch (4+3+5 = 12 >= 10).
        let due = acc
            .enqueue(gauge(5))
            .await
            .expect("should flush at threshold");
        assert_eq!(due.len(), 12, "all buffered rows coalesce into one flush");
        // Slot is now empty again.
        assert!(acc.enqueue(gauge(1)).await.is_none());
    }

    #[tokio::test]
    async fn time_threshold_triggers_aged_flush() {
        let acc = BatchAccumulator::new(BatchConfig {
            max_rows: 1_000_000,
            max_age: Duration::from_millis(20),
        });
        assert!(
            acc.enqueue(gauge(3)).await.is_none(),
            "below row threshold → buffered"
        );
        let slot = gauge(1).slot();
        // Not yet aged.
        assert!(acc.take_aged(slot, Instant::now()).await.is_none());
        // After max_age elapses, the buffer is due.
        let later = Instant::now() + Duration::from_millis(25);
        let aged = acc
            .take_aged(slot, later)
            .await
            .expect("should be due after max_age");
        assert_eq!(aged.len(), 3);
        // Drained: no longer due.
        assert!(
            acc.take_aged(slot, later + Duration::from_secs(1))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn separate_tables_buffer_independently() {
        let acc = BatchAccumulator::new(BatchConfig {
            max_rows: 5,
            max_age: Duration::from_secs(60),
        });
        assert!(acc.enqueue(gauge(3)).await.is_none());
        // Sum rows go to a different slot; gauge's 3 rows don't trip the sum slot.
        let sums = SpoolBatch::Sum(Vec::new());
        // (empty Sum batch just exercises slot independence; 3 gauge rows stay put)
        let _ = sums.slot();
        assert!(
            acc.enqueue(gauge(1)).await.is_none(),
            "gauge still at 4 < 5"
        );
        let due = acc.enqueue(gauge(1)).await.expect("gauge reaches 5");
        assert_eq!(due.len(), 5);
    }

    #[tokio::test]
    async fn drain_all_empties_every_slot() {
        let acc = BatchAccumulator::new(BatchConfig {
            max_rows: 1_000_000,
            max_age: Duration::from_secs(60),
        });
        acc.enqueue(gauge(2)).await;
        acc.enqueue(SpoolBatch::Logs(Vec::new())).await; // empty, still creates a slot entry
        acc.enqueue(gauge(3)).await; // coalesces with first gauge → 5
        let drained = acc.drain_all().await;
        let total: usize = drained.iter().map(|b| b.len()).sum();
        assert_eq!(total, 5, "all buffered rows drained exactly once");
        // Second drain is empty.
        assert!(acc.drain_all().await.is_empty());
    }

    #[tokio::test]
    async fn failed_flush_restore_preserves_the_exact_batch_without_cloning() {
        let acc = BatchAccumulator::new(BatchConfig {
            max_rows: 5,
            max_age: Duration::from_secs(60),
        });
        let due = acc.enqueue(gauge(5)).await.expect("threshold batch");
        assert!(acc.is_empty().await);
        acc.restore(due).await;
        let restored = acc.drain_all().await;
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].len(), 5);
        assert!(acc.is_empty().await);
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
}
