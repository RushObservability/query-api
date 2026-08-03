//! Per-tenant ingest signal gate.
//!
//! Every ingest write goes through [`write_gated`] instead of calling
//! `state.writer.write(...)` directly. If the tenant has the batch's signal
//! disabled the batch is silently accepted (returns `Ok(())`, so the sender
//! still gets a 2xx) but dropped, and the dropped row count is recorded under
//! `"<signal>_dropped"` so admins can see blocked volume. Tenants without an
//! explicit signal config default to all-enabled (backward compatible).

use crate::AppState;
use crate::ch_writer::{SpoolBatch, WriteError};
use std::time::Instant;

/// Map the internal signal category ("apm") to the `signal` label value used by the
/// `rush_ingest_*` self-metrics ("spans"). Other categories pass through unchanged.
/// Returns a small, finite set: logs | spans | metrics | rum.
fn signal_label(cat: &str) -> &'static str {
    match cat {
        "logs" => "logs",
        "apm" => "spans",
        "metrics" => "metrics",
        "rum" => "rum",
        _ => "other",
    }
}

/// Write `batch` for `tenant_id`, unless the tenant has that signal disabled —
/// in which case the batch is dropped (counted) and `Ok(())` is returned.
pub async fn write_gated(
    state: &AppState,
    tenant_id: &str,
    batch: SpoolBatch,
) -> Result<(), WriteError> {
    let started = Instant::now();
    let cat = batch.signal_category();
    let n = batch.len();
    // Captured before the batch is moved into the writer. `signal` and `outcome` are the
    // only labels — both finite (no tenant_id / per-path cardinality).
    let signal = signal_label(cat);
    let bytes = batch.approx_bytes();

    if !state.config_db.tenant_signal_enabled(tenant_id, cat).await {
        // Accepted-but-dropped: no error to the sender, but count it so the
        // tenant-signals endpoint can surface the blocked volume.
        state
            .usage_accumulator
            .record_dropped(tenant_id, cat, n as u64, 0);
        record_ingest(
            state,
            signal,
            "dropped",
            n as u64,
            bytes,
            started.elapsed().as_secs_f64() * 1000.0,
        );
        tracing::debug!(
            tenant_id = %tenant_id,
            signal = %cat,
            dropped = n,
            "ingest dropped: signal disabled for tenant"
        );
        return Ok(());
    }

    match state.writer.write(batch).await {
        Ok(()) => {
            record_ingest(
                state,
                signal,
                "accepted",
                n as u64,
                bytes,
                started.elapsed().as_secs_f64() * 1000.0,
            );
            Ok(())
        }
        Err(WriteError::Backpressure) => {
            // 429 — spool full / backpressure.
            record_ingest(
                state,
                signal,
                "rejected",
                n as u64,
                bytes,
                started.elapsed().as_secs_f64() * 1000.0,
            );
            Err(WriteError::Backpressure)
        }
        Err(e) => {
            // Fatal write error — also counted as rejected (not durably accepted).
            record_ingest(
                state,
                signal,
                "rejected",
                n as u64,
                bytes,
                started.elapsed().as_secs_f64() * 1000.0,
            );
            Err(e)
        }
    }
}

/// Record one ingest outcome into the self-metrics registry (counters only — cheap).
fn record_ingest(
    state: &AppState,
    signal: &'static str,
    outcome: &'static str,
    events: u64,
    bytes: u64,
    duration_ms: f64,
) {
    let labels = [("signal", signal), ("outcome", outcome)];
    state
        .self_metrics
        .inc_counter("rush_ingest_events_total", &labels, events);
    state
        .self_metrics
        .inc_counter("rush_ingest_bytes_total", &labels, bytes);
    state
        .self_metrics
        .observe_histogram("rush_ingest_batch_duration_ms", &labels, duration_ms);
}
