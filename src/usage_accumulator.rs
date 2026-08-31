use clickhouse::Client;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::self_metrics::SelfMetrics;

const FLUSH_INTERVAL: Duration = Duration::from_secs(10);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
/// A tenant can create only one key per supported signal, but this cap protects the
/// process from an unexpected tenant/key explosion while ClickHouse is unavailable.
pub const MAX_COUNTER_KEYS: usize = 100_000;

type CounterKey = (String, String);
type CounterValue = (u64, u64);
type Snapshot = Vec<(CounterKey, CounterValue)>;

/// Accumulates ingest counts in memory and flushes them to ClickHouse periodically.
/// Uses DashMap for concurrent writes from ingest handlers and restores failed snapshots.
#[derive(Clone)]
pub struct UsageAccumulator {
    /// Key: (tenant_id, signal), Value: (events_count, bytes_count)
    counters: Arc<DashMap<CounterKey, CounterValue>>,
    metrics: Arc<SelfMetrics>,
}

impl UsageAccumulator {
    pub fn new() -> Self {
        Self::with_metrics(Arc::new(SelfMetrics::new()))
    }

    pub fn with_metrics(metrics: Arc<SelfMetrics>) -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
            metrics,
        }
    }

    /// Record an ingest batch. Called from ingest handlers. New key creation is bounded;
    /// an over-cap metering event is dropped without affecting the ingest request itself.
    pub fn record(&self, tenant_id: &str, signal: &str, events: u64, bytes: u64) {
        let key = (tenant_id.to_string(), signal.to_string());
        if let Some(mut entry) = self.counters.get_mut(&key) {
            entry.0 = entry.0.saturating_add(events);
            entry.1 = entry.1.saturating_add(bytes);
            self.metrics.inc_counter(
                "rush_usage_accumulator_records_total",
                &[("outcome", "accepted")],
                1,
            );
            return;
        }

        if self.counters.len() >= MAX_COUNTER_KEYS {
            self.metrics.inc_counter(
                "rush_usage_accumulator_records_total",
                &[("outcome", "dropped")],
                1,
            );
            self.metrics.inc_counter(
                "rush_usage_accumulator_record_drops_total",
                &[("reason", "key_cap")],
                1,
            );
            tracing::warn!(tenant_id = %tenant_id, signal = %signal, "usage accumulator key cap reached; metering event dropped");
            return;
        }

        // Re-check through the entry API so concurrent first writes for the same
        // tenant/signal merge instead of overwriting one another.
        self.counters
            .entry(key)
            .and_modify(|entry| {
                entry.0 = entry.0.saturating_add(events);
                entry.1 = entry.1.saturating_add(bytes);
            })
            .or_insert((events, bytes));
        self.metrics.inc_counter(
            "rush_usage_accumulator_records_total",
            &[("outcome", "accepted")],
            1,
        );
        self.metrics.set_gauge(
            "rush_usage_accumulator_pending_keys",
            &[],
            self.counters.len() as f64,
        );
    }

    /// Record an ingest batch that was dropped because the signal is disabled
    /// for the tenant. Lands in the same per-tenant usage store under the signal
    /// name suffixed `_dropped` (e.g. "logs_dropped").
    pub fn record_dropped(&self, tenant_id: &str, signal: &str, events: u64, bytes: u64) {
        self.record(tenant_id, &format!("{signal}_dropped"), events, bytes);
    }

    /// Remove the current counters atomically with respect to each key. New records that
    /// arrive during the drain create/update the live map and are not included in snapshot.
    fn take_snapshot(&self) -> Snapshot {
        let mut snapshot = Vec::new();
        self.counters.retain(|key, value| {
            snapshot.push((key.clone(), *value));
            false
        });
        self.metrics.set_gauge(
            "rush_usage_accumulator_pending_keys",
            &[],
            self.counters.len() as f64,
        );
        snapshot
    }

    /// Restore a failed snapshot without overwriting newer records from ingest handlers.
    fn restore_snapshot(&self, snapshot: Snapshot) {
        for (key, (events, bytes)) in snapshot {
            self.counters
                .entry(key)
                .and_modify(|current| {
                    current.0 = current.0.saturating_add(events);
                    current.1 = current.1.saturating_add(bytes);
                })
                .or_insert((events, bytes));
        }
        self.metrics
            .inc_counter("rush_usage_accumulator_snapshot_restores_total", &[], 1);
        self.metrics.set_gauge(
            "rush_usage_accumulator_pending_keys",
            &[],
            self.counters.len() as f64,
        );
    }

    /// Spawn the background flush loop. Failed batches remain in the accumulator and are
    /// retried with bounded exponential backoff, so ClickHouse outages do not erase usage.
    pub fn spawn_flusher(&self, ch: Client) {
        let accumulator = self.clone();
        tokio::spawn(async move {
            let mut retrying = false;
            let mut retry_delay = Duration::from_secs(1);
            loop {
                let wait = if retrying {
                    retry_delay
                } else {
                    FLUSH_INTERVAL
                };
                tokio::time::sleep(wait).await;
                let snapshot = accumulator.take_snapshot();
                if snapshot.is_empty() {
                    retrying = false;
                    retry_delay = Duration::from_secs(1);
                    continue;
                }

                let started = Instant::now();
                match flush_snapshot(&ch, &snapshot).await {
                    Ok(()) => {
                        retrying = false;
                        retry_delay = Duration::from_secs(1);
                        accumulator.metrics.inc_counter(
                            "rush_usage_accumulator_flushes_total",
                            &[("outcome", "success")],
                            1,
                        );
                        tracing::debug!(
                            engine = "usage_accumulator",
                            flushed = snapshot.len(),
                            "usage counters flushed"
                        );
                    }
                    Err(error) => {
                        accumulator.restore_snapshot(snapshot);
                        retrying = true;
                        retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                        accumulator.metrics.inc_counter(
                            "rush_usage_accumulator_flushes_total",
                            &[("outcome", "error")],
                            1,
                        );
                        accumulator.metrics.inc_counter(
                            "rush_usage_accumulator_flush_errors_total",
                            &[],
                            1,
                        );
                        tracing::warn!(
                            engine = "usage_accumulator",
                            error = %error,
                            "failed to flush usage counters; snapshot restored for retry"
                        );
                    }
                }
                accumulator.metrics.observe_histogram(
                    "rush_usage_accumulator_flush_duration_ms",
                    &[],
                    started.elapsed().as_millis() as f64,
                );
                accumulator.metrics.set_gauge(
                    "rush_usage_accumulator_pending_keys",
                    &[],
                    accumulator.counters.len() as f64,
                );
            }
        });
    }
}

impl Default for UsageAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

fn build_insert_sql(snapshot: &Snapshot) -> Option<String> {
    if snapshot.is_empty() {
        return None;
    }
    let values = snapshot
        .iter()
        .map(|((tenant_id, signal), (events, bytes))| {
            let escaped_tenant = crate::query_builder::escape_string_literal(tenant_id);
            let escaped_signal = crate::query_builder::escape_string_literal(signal);
            format!("('{escaped_tenant}', '{escaped_signal}', {events}, {bytes})")
        })
        .collect::<Vec<_>>();
    Some(format!(
        "INSERT INTO observability.tenant_usage (tenant_id, signal, events_count, bytes_count) VALUES {}",
        values.join(", ")
    ))
}

async fn flush_snapshot(ch: &Client, snapshot: &Snapshot) -> Result<(), String> {
    let Some(sql) = build_insert_sql(snapshot) else {
        return Ok(());
    };
    ch.query(&sql).execute().await.map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_snapshot_restores_and_merges_newer_counts() {
        let accumulator = UsageAccumulator::new();
        accumulator.record("tenant-a", "logs", 3, 30);
        let snapshot = accumulator.take_snapshot();
        accumulator.record("tenant-a", "logs", 2, 20);
        accumulator.restore_snapshot(snapshot);

        let snapshot = accumulator.take_snapshot();
        assert_eq!(
            snapshot,
            vec![(("tenant-a".to_string(), "logs".to_string()), (5, 50),)]
        );
    }

    #[test]
    fn insert_sql_keeps_tenant_column_explicit() {
        let snapshot = vec![(("tenant-a".to_string(), "metrics".to_string()), (10, 2048))];
        let sql = build_insert_sql(&snapshot).expect("non-empty SQL");
        assert!(sql.contains("(tenant_id, signal, events_count, bytes_count)"));
        assert!(sql.contains("'tenant-a'"));
        assert!(sql.contains("'metrics'"));
    }

    #[test]
    fn empty_snapshot_has_no_insert() {
        assert!(build_insert_sql(&Vec::new()).is_none());
    }
}
