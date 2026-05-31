/// Durable ClickHouse write path with disk-based backpressure spool.
///
/// Normal path:  caller builds a `SpoolBatch`, calls `ChWriter::write`.
///               `write` calls `try_insert` directly → zero extra latency.
///
/// Failure path: if `try_insert` returns an error the batch is serialised to
///               JSON and handed to `Spool::append`.  If the spool is full a
///               `WriteError::Backpressure` is returned (→ HTTP 429).
///
/// Replay:       `spawn_replayer` consumes the oldest segment every ~5 s,
///               retrying with exponential back-off up to 60 s.

use std::sync::Arc;
use std::time::Duration;

use clickhouse::Client;
use serde::{Deserialize, Serialize};

use crate::models::ingest::{ExpHistogramRow, GaugeRow, HistogramRow, LogInsertRow, RumReplayChunk, SumRow, SummaryRow, TraceInsertRow};
use crate::models::rum::RumRecord;
use crate::models::trace::WideEvent;
use crate::spool::{Spool, SpoolFull};

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
}

// ─── ChWriter ────────────────────────────────────────────────────────────────

/// Cloneable ClickHouse writer with integrated spool.
#[derive(Clone)]
pub struct ChWriter {
    ch: Client,
    pub spool: Arc<Spool>,
}

impl ChWriter {
    pub fn new(ch: Client, spool: Arc<Spool>) -> Self {
        ChWriter { ch, spool }
    }

    /// Write a batch to ClickHouse.
    ///
    /// - On success: returns `Ok(())`.
    /// - On CH failure: serialises the batch and spools it; returns `Ok(())`.
    /// - On spool full: returns `Err(WriteError::Backpressure)`.
    pub async fn write(&self, batch: SpoolBatch) -> Result<(), WriteError> {
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

                // Serialise to JSON for the spool.
                let payload = serde_json::to_vec(&batch)
                    .map_err(|e| WriteError::Fatal(format!("serde_json serialise: {e}")))?;

                match self.spool.append(table, &payload) {
                    Ok(()) => Ok(()),
                    Err(SpoolFull) => Err(WriteError::Backpressure),
                }
            }
        }
    }

    /// Total bytes currently occupying the spool.
    pub fn spool_bytes(&self) -> u64 {
        self.spool.total_bytes()
    }

    /// Number of segment files in the spool.
    pub fn spool_segments(&self) -> usize {
        self.spool.segment_count()
    }

    /// Spawn a background tokio task that replays spooled segments to CH.
    pub fn spawn_replayer(self) {
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(5);
            const MAX_BACKOFF: Duration = Duration::from_secs(60);
            const POLL_INTERVAL: Duration = Duration::from_secs(5);

            loop {
                tokio::time::sleep(POLL_INTERVAL).await;

                loop {
                    let path = match self.spool.take_oldest_segment() {
                        Some(p) => p,
                        None => {
                            // No closed segment, but the open segment may still
                            // hold a partial batch. Seal it so it can be drained
                            // (e.g. after CH recovers with no further traffic),
                            // then retry once. If nothing got sealed, we're done.
                            if self.spool.total_bytes() > 0 {
                                self.spool.seal_current();
                                match self.spool.take_oldest_segment() {
                                    Some(p) => p,
                                    None => break, // spool truly empty
                                }
                            } else {
                                break; // spool empty
                            }
                        }
                    };

                    tracing::debug!(
                        segment = %path.display(),
                        "replayer: reading segment"
                    );

                    let records = match Spool::read_segment(&path) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                segment = %path.display(),
                                "replayer: failed to read segment — discarding"
                            );
                            self.spool.remove_segment(&path);
                            continue;
                        }
                    };

                    let mut all_ok = true;
                    for (table, payload) in &records {
                        let batch: SpoolBatch = match serde_json::from_slice(payload) {
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
                        self.spool.remove_segment(&path);
                        tracing::info!(
                            segment = %path.display(),
                            records = records.len(),
                            "replayer: segment replayed and removed"
                        );
                        backoff = Duration::from_secs(5); // reset on success
                    } else {
                        // CH is still down — back off and stop this replay pass.
                        tracing::warn!(
                            backoff_secs = backoff.as_secs(),
                            "replayer: CH unavailable, backing off"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        break;
                    }
                }
            }
        });
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
