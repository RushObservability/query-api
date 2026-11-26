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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clickhouse::Client;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::models::ingest::{
    ExpHistogramRow, GaugeRow, HistogramRow, LogInsertRow, RumReplayChunk, SumRow, SummaryRow,
    TraceInsertRow,
};
use crate::models::profile::ProfileRow;
use crate::models::rum::RumRecord;
use crate::models::trace::WideEvent;
use crate::self_metrics::SelfMetrics;
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
    // Named row fields let older replicas ignore columns introduced by a newer writer.
    let mp = rmp_serde::to_vec_named(batch)
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

/// Estimated size of a batch for the per-table byte budget: string lengths,
/// field names and fixed widths for numbers, summed by walking the rows through
/// serde without producing any output. It tracks the JSON size this budget was
/// first defined in closely enough, without formatting every row on each write.
fn batch_estimated_size(batch: &SpoolBatch) -> usize {
    let mut size = SizeEstimate(0);
    // Counting cannot fail; serde only needs an error type.
    let _ = batch.serialize(&mut size);
    size.0
}

struct SizeEstimate(usize);

type EstimateResult = Result<(), serde_json::Error>;

impl SizeEstimate {
    fn add(&mut self, bytes: usize) -> EstimateResult {
        self.0 = self.0.saturating_add(bytes);
        Ok(())
    }
}

macro_rules! fixed_width {
    ($($method:ident($ty:ty) = $width:expr;)*) => {
        $(fn $method(self, _: $ty) -> EstimateResult {
            self.add($width)
        })*
    };
}

impl serde::Serializer for &mut SizeEstimate {
    type Ok = ();
    type Error = serde_json::Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fixed_width! {
        serialize_bool(bool) = 1;
        serialize_i8(i8) = 1;
        serialize_i16(i16) = 2;
        serialize_i32(i32) = 4;
        serialize_i64(i64) = 8;
        serialize_u8(u8) = 1;
        serialize_u16(u16) = 2;
        serialize_u32(u32) = 4;
        serialize_u64(u64) = 8;
        serialize_f32(f32) = 4;
        serialize_f64(f64) = 8;
        serialize_char(char) = 4;
    }

    fn serialize_str(self, value: &str) -> EstimateResult {
        self.add(value.len() + 2)
    }
    fn serialize_bytes(self, value: &[u8]) -> EstimateResult {
        self.add(value.len())
    }
    fn serialize_none(self) -> EstimateResult {
        self.add(1)
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> EstimateResult {
        value.serialize(self)
    }
    fn serialize_unit(self) -> EstimateResult {
        self.add(1)
    }
    fn serialize_unit_struct(self, _: &'static str) -> EstimateResult {
        self.add(1)
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> EstimateResult {
        self.add(variant.len() + 2)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        value: &T,
    ) -> EstimateResult {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> EstimateResult {
        self.add(variant.len() + 4)?;
        value.serialize(self)
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self, serde_json::Error> {
        self.add(2)?;
        Ok(self)
    }
    fn serialize_tuple(self, _: usize) -> Result<Self, serde_json::Error> {
        self.add(2)?;
        Ok(self)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, serde_json::Error> {
        self.add(2)?;
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Self, serde_json::Error> {
        self.add(variant.len() + 6)?;
        Ok(self)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self, serde_json::Error> {
        self.add(2)?;
        Ok(self)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, serde_json::Error> {
        self.add(2)?;
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Self, serde_json::Error> {
        self.add(variant.len() + 6)?;
        Ok(self)
    }
}

impl serde::ser::SerializeSeq for &mut SizeEstimate {
    type Ok = ();
    type Error = serde_json::Error;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> EstimateResult {
        self.add(1)?;
        value.serialize(&mut **self)
    }
    fn end(self) -> EstimateResult {
        Ok(())
    }
}

impl serde::ser::SerializeTuple for &mut SizeEstimate {
    type Ok = ();
    type Error = serde_json::Error;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> EstimateResult {
        self.add(1)?;
        value.serialize(&mut **self)
    }
    fn end(self) -> EstimateResult {
        Ok(())
    }
}

impl serde::ser::SerializeTupleStruct for &mut SizeEstimate {
    type Ok = ();
    type Error = serde_json::Error;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> EstimateResult {
        self.add(1)?;
        value.serialize(&mut **self)
    }
    fn end(self) -> EstimateResult {
        Ok(())
    }
}

impl serde::ser::SerializeTupleVariant for &mut SizeEstimate {
    type Ok = ();
    type Error = serde_json::Error;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> EstimateResult {
        self.add(1)?;
        value.serialize(&mut **self)
    }
    fn end(self) -> EstimateResult {
        Ok(())
    }
}

impl serde::ser::SerializeMap for &mut SizeEstimate {
    type Ok = ();
    type Error = serde_json::Error;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> EstimateResult {
        self.add(2)?;
        key.serialize(&mut **self)
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> EstimateResult {
        value.serialize(&mut **self)
    }
    fn end(self) -> EstimateResult {
        Ok(())
    }
}

impl serde::ser::SerializeStruct for &mut SizeEstimate {
    type Ok = ();
    type Error = serde_json::Error;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> EstimateResult {
        self.add(key.len() + 4)?;
        value.serialize(&mut **self)
    }
    fn end(self) -> EstimateResult {
        Ok(())
    }
}

impl serde::ser::SerializeStructVariant for &mut SizeEstimate {
    type Ok = ();
    type Error = serde_json::Error;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> EstimateResult {
        self.add(key.len() + 4)?;
        value.serialize(&mut **self)
    }
    fn end(self) -> EstimateResult {
        Ok(())
    }
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
// without retaining them. The slot lock only moves rows in and out; a per-table
// lane serializes deliveries, so requests keep being admitted during an insert.
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
    /// Maximum estimated bytes retained per table. Oversized requests bypass
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

const BATCH_TABLES: [&str; SpoolBatch::SLOTS] = [
    "spans_raw",
    "spans",
    "logs",
    "metrics_gauge",
    "metrics_sum",
    "rum",
    "rum_replay",
    "metrics_histogram",
    "metrics_exp_histogram",
    "metrics_summary",
    "profile_samples",
];

#[derive(Default)]
struct BatchHealth {
    rows: AtomicU64,
    bytes: AtomicU64,
    first_row_ms: AtomicU64,
    blocked: AtomicBool,
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Records a cancelled attempt too: cancellation must not look like delivery.
struct DeliveryObservation<'a> {
    metrics: &'a SelfMetrics,
    table: &'static str,
    rows: u64,
    started: Instant,
    outcome: &'static str,
}

impl Drop for DeliveryObservation<'_> {
    fn drop(&mut self) {
        let labels = [("table", self.table), ("outcome", self.outcome)];
        self.metrics
            .inc_counter("rush_ingest_delivery_total", &labels, 1);
        self.metrics
            .inc_counter("rush_ingest_delivery_rows_total", &labels, self.rows);
        self.metrics.observe_histogram(
            "rush_ingest_delivery_duration_ms",
            &labels,
            self.started.elapsed().as_secs_f64() * 1000.0,
        );
    }
}

/// The in-process batching layer: one buffer per ClickHouse table, each behind
/// its own async mutex so independent ingest tasks contend only with same-table
/// peers and the flusher.
pub struct BatchAccumulator {
    cfg: BatchConfig,
    /// Held only to move rows in or out, never across a ClickHouse insert.
    slots: [AsyncMutex<TableBuffer>; SpoolBatch::SLOTS],
    /// One delivery per table at a time; held for the whole insert.
    lanes: [Arc<AsyncMutex<()>>; SpoolBatch::SLOTS],
    health: [BatchHealth; SpoolBatch::SLOTS],
}

/// How long a request that needs a flush waits for the table's in-flight
/// insert before getting 429. Bounded so waiting request bodies cannot become
/// an unbounded second queue while ClickHouse is slow.
const SLOT_WAIT: Duration = Duration::from_secs(1);

impl BatchAccumulator {
    fn new(cfg: BatchConfig) -> Self {
        BatchAccumulator {
            cfg,
            slots: std::array::from_fn(|_| AsyncMutex::new(TableBuffer::default())),
            lanes: std::array::from_fn(|_| Arc::new(AsyncMutex::new(()))),
            health: std::array::from_fn(|_| BatchHealth::default()),
        }
    }

    async fn is_empty(&self) -> bool {
        for (slot, lane) in self.slots.iter().zip(&self.lanes) {
            match slot.try_lock() {
                Ok(buffer) if buffer.batch.is_none() => {}
                _ => return false,
            }
            // A delivery in flight holds its rows outside the slot; shutdown
            // must not treat them as drained or block on their network I/O.
            if lane.try_lock().is_err() {
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
    metrics: Arc<SelfMetrics>,
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
            metrics: Arc::new(SelfMetrics::new()),
            buffer,
            firewall: Arc::new(std::sync::RwLock::new(Arc::new(
                crate::metric_firewall::MetricFirewall::default(),
            ))),
            batcher: Arc::new(BatchAccumulator::new(cfg)),
        }
    }

    /// Attach the process registry before cloning or starting background work.
    pub fn with_metrics(mut self, metrics: Arc<SelfMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    fn record_pending(&self, index: usize) {
        let health = &self.batcher.health[index];
        let labels = [("table", BATCH_TABLES[index])];
        let first = health.first_row_ms.load(Ordering::Relaxed);
        for (name, value) in [
            (
                "rush_ingest_batch_pending_rows",
                health.rows.load(Ordering::Relaxed) as f64,
            ),
            (
                "rush_ingest_batch_pending_bytes",
                health.bytes.load(Ordering::Relaxed) as f64,
            ),
            (
                "rush_ingest_batch_oldest_age_seconds",
                if first == 0 {
                    0.0
                } else {
                    unix_millis().saturating_sub(first) as f64 / 1000.0
                },
            ),
            (
                "rush_ingest_batch_flush_blocked",
                u8::from(health.blocked.load(Ordering::Relaxed)) as f64,
            ),
        ] {
            self.metrics.set_gauge(name, &labels, value);
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
    /// exceeding either limit are written synchronously. A request that needs a
    /// flush waits up to `SLOT_WAIT` for the table's in-flight insert. The age
    /// flusher and shutdown share the same lane and preserve accepted rows
    /// across failures.
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

        let bytes = batch_estimated_size(&batch);
        let index = batch.slot();
        let oversized =
            batch.len() >= self.batcher.cfg.max_rows || bytes >= self.batcher.cfg.max_bytes;
        let batch = if oversized {
            batch
        } else {
            match self.admit(index, batch, bytes).await {
                Ok(()) => return Ok(()),
                Err(batch) => batch,
            }
        };
        let lane = tokio::time::timeout(SLOT_WAIT, self.batcher.lanes[index].clone().lock_owned())
            .await
            .map_err(|_| self.busy(&batch))?;
        // Only previously accepted rows are retained on failure. The current
        // request has not entered the accumulator and is safe to retry.
        self.flush_slot(index, lane).await?;
        if oversized {
            return self.write_now(&batch).await;
        }
        // Requests admitted while the insert ran may have refilled the slot.
        self.admit(index, batch, bytes)
            .await
            .map_err(|batch| self.busy(&batch))
    }

    fn busy(&self, batch: &SpoolBatch) -> WriteError {
        self.metrics.inc_counter(
            "rush_ingest_batch_rejections_total",
            &[("table", batch.table()), ("reason", "busy")],
            1,
        );
        WriteError::Backpressure
    }

    /// Add rows to the slot when they fit without a flush first; otherwise
    /// hand the batch back untouched.
    async fn admit(&self, index: usize, batch: SpoolBatch, bytes: usize) -> Result<(), SpoolBatch> {
        let cfg = self.batcher.cfg;
        let mut slot = self.batcher.slots[index].lock().await;
        let rows = slot.batch.as_ref().map_or(0, SpoolBatch::len);
        if slot.needs_flush
            || rows.saturating_add(batch.len()) >= cfg.max_rows
            || slot.bytes.saturating_add(bytes) >= cfg.max_bytes
        {
            return Err(batch);
        }
        let added = batch.len() as u64;
        match slot.batch.as_mut() {
            Some(existing) => existing.extend_from(batch),
            None => {
                slot.first_row_at = Some(Instant::now());
                slot.batch = Some(batch);
            }
        }
        slot.bytes = slot.bytes.saturating_add(bytes);
        // Pending counts include rows of an insert in flight until it succeeds.
        let health = &self.batcher.health[index];
        health.rows.fetch_add(added, Ordering::Relaxed);
        health.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        let _ = health.first_row_ms.compare_exchange(
            0,
            unix_millis(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        // The independent sampler publishes these atomics off the ingest hot path.
        Ok(())
    }

    /// Deliver the slot's rows while holding the table's lane. The rows leave
    /// the slot for the insert so other requests keep being admitted; the insert
    /// runs in its own task so a caller that goes away cannot drop them.
    async fn flush_slot(&self, index: usize, lane: OwnedMutexGuard<()>) -> Result<(), WriteError> {
        let me = self.clone();
        tokio::spawn(async move {
            let _lane = lane;
            me.deliver(index).await
        })
        .await
        .map_err(|error| WriteError::Fatal(format!("batch flush task: {error}")))?
    }

    async fn deliver(&self, index: usize) -> Result<(), WriteError> {
        let taken = std::mem::take(&mut *self.batcher.slots[index].lock().await);
        let Some(batch) = &taken.batch else {
            return Ok(());
        };
        let health = &self.batcher.health[index];
        health.blocked.store(true, Ordering::Relaxed);
        self.record_pending(index);
        let result = self.write_now(batch).await;
        let mut slot = self.batcher.slots[index].lock().await;
        match result {
            Ok(()) => {
                health.rows.store(
                    slot.batch.as_ref().map_or(0, SpoolBatch::len) as u64,
                    Ordering::Relaxed,
                );
                health.bytes.store(slot.bytes as u64, Ordering::Relaxed);
                health.first_row_ms.store(
                    slot.first_row_at.map_or(0, |at| {
                        unix_millis().saturating_sub(at.elapsed().as_millis() as u64)
                    }),
                    Ordering::Relaxed,
                );
                health.blocked.store(false, Ordering::Relaxed);
                self.record_pending(index);
                Ok(())
            }
            Err(error) => {
                // Put the acknowledged rows back ahead of any admitted since,
                // and make new writes retry this flush before entering.
                let newer = std::mem::replace(&mut *slot, taken);
                if let (Some(restored), Some(rows)) = (slot.batch.as_mut(), newer.batch) {
                    restored.extend_from(rows);
                }
                slot.bytes = slot.bytes.saturating_add(newer.bytes);
                slot.needs_flush = true;
                Err(error)
            }
        }
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

        let mut observation = DeliveryObservation {
            metrics: &self.metrics,
            table,
            rows: row_count as u64,
            started: Instant::now(),
            outcome: "cancelled",
        };
        match try_insert(&self.ch, batch).await {
            Ok(()) => {
                observation.outcome = "inserted";
                Ok(())
            }
            Err(e) => {
                self.metrics.inc_counter(
                    "rush_ingest_insert_failures_total",
                    &[("table", table)],
                    1,
                );
                tracing::warn!(
                    error = %e,
                    table = table,
                    rows = row_count,
                    "ch insert failed — spooling batch"
                );

                // Serialise for the spool (zstd-compressed MessagePack).
                let payload = encode_spool(batch).inspect_err(|_| {
                    observation.outcome = "encode_error";
                })?;

                match self.buffer.append(table, payload).await {
                    Ok(()) => {
                        observation.outcome = "spooled";
                        Ok(())
                    }
                    Err(SpoolFull) => {
                        observation.outcome = "backpressure";
                        Err(WriteError::Backpressure)
                    }
                }
            }
        }
    }

    /// Flush every buffered table batch to ClickHouse (spooling on failure).
    /// Called on graceful shutdown so no buffered rows are silently dropped.
    pub async fn flush_all(&self) {
        stream::iter(0..SpoolBatch::SLOTS)
            .for_each_concurrent(4, |index| async move {
                let lane = self.batcher.lanes[index].clone().lock_owned().await;
                if let Err(error) = self.flush_slot(index, lane).await {
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
        // A separate sampler uses atomics, so a network-stalled flush cannot
        // freeze the age or hide the accepted rows behind the slot mutex.
        let sampler = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                for index in 0..SpoolBatch::SLOTS {
                    sampler.record_pending(index);
                }
            }
        });
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
                            // An insert already in flight owns this table; look again next tick.
                            let Ok(lane) = me.batcher.lanes[index].clone().try_lock_owned() else {
                                return;
                            };
                            let aged = me.batcher.slots[index].lock().await.first_row_at.is_some_and(|at| now.saturating_duration_since(at) >= me.batcher.cfg.max_age);
                            if aged {
                                if let Err(error) = me.flush_slot(index, lane).await {
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
        self.buffer.attach_metrics(self.metrics.clone());
        let sampler = self.clone();
        let sampler_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = sampler_shutdown.wait_for_finish() => break,
                    _ = interval.tick() => {}
                }
                let backend = sampler.buffer.backend_name();
                if sampler.buffer.refresh_counts().await.is_err() {
                    crate::spool::record_spool_error(&sampler.metrics, backend, "refresh");
                }
                let labels = [("backend", backend)];
                for (name, value) in [
                    (
                        "rush_ingest_replay_pending_bytes",
                        sampler.buffer.total_bytes() as f64,
                    ),
                    (
                        "rush_ingest_replay_pending_batches",
                        sampler.buffer.segment_count() as f64,
                    ),
                    (
                        "rush_ingest_replay_oldest_age_seconds",
                        sampler.buffer.oldest_age_secs().await.unwrap_or(0) as f64,
                    ),
                    (
                        "rush_ingest_replay_max_bytes",
                        sampler.buffer.max_bytes() as f64,
                    ),
                ] {
                    sampler.metrics.set_gauge(name, &labels, value);
                }
            }
        });
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

                let started = Instant::now();
                let backend = self.buffer.backend_name();
                let mut all_ok = true;
                let mut partial = drain.discarded_bytes > 0;
                let mut discarded_records = 0;
                let mut discarded_bytes = 0;
                for (table, payload) in &drain.records {
                    let batch: SpoolBatch = match decode_spool(payload) {
                        Ok(b) => b,
                        Err(e) => {
                            discarded_records += 1;
                            discarded_bytes += payload.len() as u64;
                            partial = true;
                            tracing::warn!(error = %e, table = table, "replayer: deserialise failed — skipping record");
                            continue;
                        }
                    };

                    if let Err(e) = try_insert(&self.ch, &batch).await {
                        tracing::warn!(
                            error = %e,
                            table = table,
                            "replayer: CH insert failed — backing off"
                        );
                        self.metrics.inc_counter(
                            "rush_ingest_replay_records_total",
                            &[("backend", backend), ("outcome", "insert_error")],
                            1,
                        );
                        all_ok = false;
                        break;
                    }
                    self.metrics.inc_counter(
                        "rush_ingest_replay_records_total",
                        &[("backend", backend), ("outcome", "inserted")],
                        1,
                    );
                }

                self.metrics.observe_histogram(
                    "rush_ingest_replay_duration_ms",
                    &[("backend", backend)],
                    started.elapsed().as_secs_f64() * 1000.0,
                );
                if all_ok {
                    let n = drain.records.len();
                    let committed = self.buffer.commit(drain).await;
                    if committed && discarded_records > 0 {
                        let labels = [
                            ("backend", backend),
                            ("reason", "payload_decode"),
                            ("unit", "record"),
                        ];
                        self.metrics.inc_counter(
                            "rush_ingest_spool_discarded_total",
                            &labels,
                            discarded_records,
                        );
                        self.metrics.inc_counter(
                            "rush_ingest_spool_discarded_bytes_total",
                            &labels,
                            discarded_bytes,
                        );
                    }
                    let outcome = if !committed {
                        "commit_error"
                    } else if partial {
                        "partial"
                    } else {
                        "committed"
                    };
                    self.metrics.inc_counter(
                        "rush_ingest_replay_batches_total",
                        &[("backend", backend), ("outcome", outcome)],
                        1,
                    );
                    if committed && !partial {
                        self.metrics.set_gauge(
                            "rush_ingest_replay_last_success_timestamp_seconds",
                            &[("backend", backend)],
                            time::OffsetDateTime::now_utc().unix_timestamp() as f64,
                        );
                    }
                    tracing::info!(records = n, "replayer: batch replayed and committed");
                    backoff = Duration::from_secs(5); // reset on success
                } else {
                    self.metrics.inc_counter(
                        "rush_ingest_replay_batches_total",
                        &[("backend", backend), ("outcome", "insert_error")],
                        1,
                    );
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
            let mut ins = ch.insert::<ProfileRow>("profile_samples").await?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::SpansRaw(rows) => {
            let mut ins = ch.insert::<TraceInsertRow>("spans_raw").await?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Spans(rows) => {
            let mut ins = ch.insert::<WideEvent>("spans").await?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Logs(rows) => {
            let mut ins = ch.insert::<LogInsertRow>("logs").await?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Gauge(rows) => {
            let mut ins = ch.insert::<GaugeRow>("metrics_gauge").await?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Sum(rows) => {
            let mut ins = ch.insert::<SumRow>("metrics_sum").await?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Rum(rows) => {
            let mut ins = ch.insert::<RumRecord>("rum").await?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::RumReplay(rows) => {
            let mut ins = ch.insert::<RumReplayChunk>("rum_replay").await?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Histogram(rows) => {
            let mut ins = ch.insert::<HistogramRow>("metrics_histogram").await?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::ExpHistogram(rows) => {
            let mut ins = ch
                .insert::<ExpHistogramRow>("metrics_exp_histogram")
                .await?;
            for r in rows {
                ins.write(r).await?;
            }
            ins.end().await
        }
        SpoolBatch::Summary(rows) => {
            let mut ins = ch.insert::<SummaryRow>("metrics_summary").await?;
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
    fn named_spool_rows_allow_older_readers_to_ignore_new_fields() {
        let batch = gauge(1);
        let encoded = encode_spool(&batch).unwrap();
        let mp = zstd::decode_all(&encoded[1..]).unwrap();
        let mut future: serde_json::Value = rmp_serde::from_slice(&mp).unwrap();
        let row = future["Gauge"][0]
            .as_object_mut()
            .expect("rows encode named fields, not positional arrays");
        row.insert(
            "future_column".into(),
            serde_json::json!("a value from a newer binary"),
        );
        let future_mp = rmp_serde::to_vec_named(&future).unwrap();
        let mut payload = vec![SPOOL_FMT_MSGPACK_ZSTD];
        payload
            .extend_from_slice(&zstd::encode_all(future_mp.as_slice(), SPOOL_ZSTD_LEVEL).unwrap());
        assert!(same_batch(&batch, &decode_spool(&payload).unwrap()));
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

    #[test]
    fn size_estimate_stays_close_to_the_json_size_it_replaces() {
        let attrs = |n: usize| -> Vec<(String, String)> {
            (0..n)
                .map(|i| {
                    (
                        format!("attribute.key.{i}"),
                        format!("value-{i}-{}", "x".repeat(i)),
                    )
                })
                .collect()
        };
        let SpoolBatch::Gauge(mut rows) = gauge(50) else {
            unreachable!()
        };
        for (i, row) in rows.iter_mut().enumerate() {
            row.resource_attributes = Arc::new(attrs(12));
            row.service_name = "checkout-service".into();
            row.metric_name = "http.server.request.duration".into();
            row.attributes = attrs(i % 8);
            row.time_unix = 1_764_000_000_000_000_000 + i as i64;
            row.value = i as f64 * 1.37;
        }
        let log = |i: usize| LogInsertRow {
            tenant_id: "tenant-a".into(),
            timestamp: 1_764_000_000_000_000_000 + i as i64,
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".into(),
            span_id: "00f067aa0ba902b7".into(),
            trace_flags: 1,
            severity_text: "INFO".into(),
            severity_number: 9,
            body: format!(
                "GET /api/cart/{i} completed in {i}ms \"ok\"\n{}",
                "y".repeat(i * 7)
            ),
            service_name: "checkout-service".into(),
            resource_schema_url: "".into(),
            resource_attributes: Arc::new(attrs(10)),
            scope_schema_url: "".into(),
            scope_name: "io.opentelemetry.http".into(),
            scope_version: "1.2.0".into(),
            scope_attributes: Arc::new(Vec::new()),
            log_attributes: attrs(i % 5),
            event_name: "".into(),
        };
        for batch in [
            gauge(1),
            SpoolBatch::Gauge(rows),
            SpoolBatch::Logs((0..50).map(log).collect()),
        ] {
            let json = serde_json::to_vec(&batch).unwrap().len() as f64;
            let ratio = batch_estimated_size(&batch) as f64 / json;
            assert!((0.5..=2.0).contains(&ratio), "estimate/json = {ratio}");
        }
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
    async fn pending_and_delivery_metrics_distinguish_acceptance_failure_and_recovery() {
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
        test.0.batcher.health[gauge(1).slot()]
            .first_row_ms
            .store(unix_millis() - 2_000, Ordering::Relaxed);
        test.0.record_pending(gauge(1).slot());
        let before = test.0.metrics.render_prometheus();
        assert!(before.contains("rush_ingest_batch_pending_rows{table=\"metrics_gauge\"} 2"));
        assert!(!before.contains("rush_ingest_delivery_total"));
        mock.add(handlers::failure(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        ));
        assert!(matches!(
            test.0.write(gauge(1)).await,
            Err(WriteError::Backpressure)
        ));
        let failed = test.0.metrics.render_prometheus();
        assert!(failed.contains("rush_ingest_batch_flush_blocked{table=\"metrics_gauge\"} 1"));
        assert!(failed.contains(
            "rush_ingest_delivery_rows_total{outcome=\"backpressure\",table=\"metrics_gauge\"} 2"
        ));
        let received = mock.add(handlers::record::<GaugeRow>());
        test.0.flush_all().await;
        assert_eq!(received.collect::<Vec<_>>().await.len(), 2);
        let recovered = test.0.metrics.render_prometheus();
        assert!(recovered.contains("rush_ingest_batch_pending_rows{table=\"metrics_gauge\"} 0"));
        assert!(
            recovered.contains("rush_ingest_batch_oldest_age_seconds{table=\"metrics_gauge\"} 0")
        );
        assert!(recovered.contains(
            "rush_ingest_delivery_rows_total{outcome=\"inserted\",table=\"metrics_gauge\"} 2"
        ));
    }

    #[tokio::test]
    async fn invalid_replay_payload_is_partial_not_successful_delivery() {
        let test = TestWriter::new("http://127.0.0.1:9", BatchConfig::default());
        // TestWriter's spool has a zero-byte cap; use a separate bounded spool.
        let dir = test.1.join("replay");
        let buffer = Arc::new(IngestBuffer::Disk(
            crate::spool::Spool::open(&dir, 1024).unwrap(),
        ));
        buffer
            .append("untrusted-table-name", b"bad".to_vec())
            .await
            .unwrap();
        let metrics = Arc::new(SelfMetrics::new());
        let writer = ChWriter::new(
            Client::default().with_url("http://127.0.0.1:9"),
            buffer.clone(),
        )
        .with_metrics(metrics.clone());
        let shutdown = ShutdownController::new();
        shutdown.begin_drain();
        let task = writer.spawn_replayer(shutdown.clone());
        tokio::time::timeout(Duration::from_secs(2), async {
            while buffer.segment_count() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown.finish();
        task.await.unwrap();
        let text = metrics.render_prometheus();
        assert!(text.contains("rush_ingest_spool_discarded_total{backend=\"disk\",reason=\"payload_decode\",unit=\"record\"} 1"));
        assert!(
            text.contains(
                "rush_ingest_replay_batches_total{backend=\"disk\",outcome=\"partial\"} 1"
            )
        );
        assert!(!text.contains("last_success_timestamp"));
        assert!(!text.contains("untrusted-table-name"));
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
        let mut admitted = 0;
        for task in tasks {
            match task.await.unwrap() {
                Ok(()) => admitted += 1,
                Err(error) => assert!(matches!(error, WriteError::Backpressure)),
            }
        }
        // At most one write fits beside the in-flight row (max_rows 2).
        assert!(admitted <= 1);
        let slot = test.0.batcher.slots[accepted.slot()].lock().await;
        assert_eq!(slot.batch.as_ref().unwrap().len(), 1 + admitted);
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
        let one = batch_estimated_size(&gauge(1));
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
    async fn slow_insert_does_not_block_admission_and_survives_its_caller() {
        use axum::{Router, http::StatusCode, routing::any};
        let reached = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (signal, gate) = (reached.clone(), release.clone());
        let router = Router::new().route(
            "/",
            any(move || {
                let (signal, gate) = (signal.clone(), gate.clone());
                async move {
                    signal.notify_one();
                    gate.notified().await;
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let test = TestWriter::new(
            &url,
            BatchConfig {
                max_rows: 2,
                ..BatchConfig::default()
            },
        );
        let index = gauge(1).slot();
        test.0.write(gauge(1)).await.unwrap();
        let writer = test.0.clone();
        let flush = tokio::spawn(async move {
            writer.flush_all().await;
        });
        tokio::time::timeout(Duration::from_secs(2), reached.notified())
            .await
            .unwrap();
        // The insert is in flight without the slot lock: a new request is admitted.
        tokio::time::timeout(Duration::from_millis(50), test.0.write(gauge(1)))
            .await
            .unwrap()
            .unwrap();
        assert!(test.0.has_pending_batches().await);
        // The slot is full again; the next request waits a bounded time, then 429.
        let started = Instant::now();
        let busy = tokio::time::timeout(SLOT_WAIT * 3, test.0.write(gauge(1)))
            .await
            .unwrap();
        assert!(matches!(busy, Err(WriteError::Backpressure)));
        assert!(started.elapsed() >= SLOT_WAIT);
        // The flush's caller goes away; the delivery does not.
        flush.abort();
        assert!(flush.await.unwrap_err().is_cancelled());
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !test.0.batcher.slots[index].lock().await.needs_flush {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // The failed insert (spool full) put its row back ahead of the one
        // admitted meanwhile; neither acknowledged row is lost.
        let slot = test.0.batcher.slots[index].lock().await;
        assert_eq!(slot.batch.as_ref().unwrap().len(), 2);
        assert_eq!(slot.bytes, 2 * batch_estimated_size(&gauge(1)));
        drop(slot);
        let metrics = test.0.metrics.render_prometheus();
        assert!(metrics.contains(
            "rush_ingest_delivery_total{outcome=\"backpressure\",table=\"metrics_gauge\"} 1"
        ));
        assert!(metrics.contains(
            "rush_ingest_batch_rejections_total{reason=\"busy\",table=\"metrics_gauge\"} 1"
        ));
        server.abort();
    }
}
