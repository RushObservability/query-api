use clickhouse::Client;
use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::self_metrics::SelfMetrics;

/// Maximum number of events waiting in the producer-to-writer channel.
/// Usage tracking is best-effort at admission, but never allowed to grow without bound.
pub const USAGE_QUEUE_CAPACITY: usize = 4096;
/// Maximum number of events retained by the writer while ClickHouse is unavailable.
pub const USAGE_PENDING_CAPACITY: usize = 16_384;
const USAGE_BATCH_SIZE: usize = 500;
const USAGE_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

/// A single tenant-scoped usage event emitted by query handlers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageEvent {
    pub tenant_id: String,
    pub signal_name: String,
    pub signal_type: String, // "metric", "span", "log"
    pub source: String,      // "explore", "dashboard", "alert", "prom_api"
}

/// Handle for sending usage events without blocking request handlers.
#[derive(Clone)]
pub struct UsageTracker {
    tx: mpsc::Sender<UsageEvent>,
    queue_depth: Arc<AtomicUsize>,
    metrics: Arc<SelfMetrics>,
}

impl UsageTracker {
    /// Track a signal usage event. A full queue increments the drop metric and returns;
    /// request latency must never depend on ClickHouse availability.
    pub fn track(&self, event: UsageEvent) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(event) {
            Ok(()) => {
                self.metrics
                    .inc_counter("rush_signal_usage_events_enqueued_total", &[], 1);
                self.metrics.set_gauge(
                    "rush_signal_usage_queue_depth",
                    &[],
                    self.queue_depth.load(Ordering::Relaxed) as f64,
                );
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.queue_depth.fetch_sub(1, Ordering::Relaxed);
                self.metrics.inc_counter(
                    "rush_signal_usage_events_dropped_total",
                    &[("reason", "queue_full")],
                    1,
                );
                self.metrics.set_gauge(
                    "rush_signal_usage_queue_depth",
                    &[],
                    self.queue_depth.load(Ordering::Relaxed) as f64,
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.queue_depth.fetch_sub(1, Ordering::Relaxed);
                self.metrics.inc_counter(
                    "rush_signal_usage_events_dropped_total",
                    &[("reason", "writer_closed")],
                    1,
                );
            }
        }
    }

    /// Convenience: track multiple signal names for one tenant.
    pub fn track_many(&self, tenant_id: &str, names: Vec<String>, signal_type: &str, source: &str) {
        for name in names {
            self.track(UsageEvent {
                tenant_id: tenant_id.to_string(),
                signal_name: name,
                signal_type: signal_type.to_string(),
                source: source.to_string(),
            });
        }
    }
}

/// Spawn the background usage writer and return the tracker handle.
pub fn spawn(ch: Client, metrics: Arc<SelfMetrics>) -> UsageTracker {
    let (tx, rx) = mpsc::channel(USAGE_QUEUE_CAPACITY);
    let queue_depth = Arc::new(AtomicUsize::new(0));
    tokio::spawn(usage_writer(rx, ch, metrics.clone(), queue_depth.clone()));
    UsageTracker {
        tx,
        queue_depth,
        metrics,
    }
}

/// Background task that batches usage events and retries failed batches.
///
/// A failed batch is put back at the front of `pending`, so ClickHouse outages do not
/// silently erase usage. The pending queue is bounded; once it is full, new usage events
/// are counted as dropped rather than allowing API memory to grow without limit.
async fn usage_writer(
    mut rx: mpsc::Receiver<UsageEvent>,
    ch: Client,
    metrics: Arc<SelfMetrics>,
    queue_depth: Arc<AtomicUsize>,
) {
    let mut pending = VecDeque::new();
    let mut tick = tokio::time::interval(USAGE_FLUSH_INTERVAL);
    let mut next_attempt = Instant::now() + USAGE_FLUSH_INTERVAL;
    let mut retry_delay = Duration::from_secs(1);
    let mut retrying = false;

    loop {
        tokio::select! {
            event = rx.recv() => {
                let Some(event) = event else {
                    // The production sender lives for the process lifetime. If it closes,
                    // stop without issuing a tight loop; the final pending state remains
                    // visible in metrics and is not falsely reported as flushed.
                    break;
                };
                queue_depth.fetch_sub(1, Ordering::Relaxed);
                if pending.len() < USAGE_PENDING_CAPACITY {
                    pending.push_back(event);
                } else {
                    metrics.inc_counter(
                        "rush_signal_usage_events_dropped_total",
                        &[("reason", "pending_full")],
                        1,
                    );
                }
                metrics.set_gauge(
                    "rush_signal_usage_queue_depth",
                    &[],
                    queue_depth.load(Ordering::Relaxed) as f64,
                );
                metrics.set_gauge(
                    "rush_signal_usage_pending_events",
                    &[],
                    pending.len() as f64,
                );
            }
            _ = tick.tick() => {}
        }

        let now = Instant::now();
        let batch_due = !pending.is_empty() && now >= next_attempt
            || (!retrying && pending.len() >= USAGE_BATCH_SIZE);
        if !batch_due {
            continue;
        }

        let batch_len = pending.len().min(USAGE_BATCH_SIZE);
        let batch: Vec<UsageEvent> = pending.drain(..batch_len).collect();
        let started = Instant::now();
        match flush(&ch, &batch).await {
            Ok(unique_entries) => {
                retrying = false;
                retry_delay = Duration::from_secs(1);
                next_attempt = Instant::now() + USAGE_FLUSH_INTERVAL;
                metrics.inc_counter(
                    "rush_signal_usage_flushes_total",
                    &[("outcome", "success")],
                    1,
                );
                metrics.observe_histogram(
                    "rush_signal_usage_flush_duration_ms",
                    &[],
                    started.elapsed().as_millis() as f64,
                );
                tracing::debug!(
                    entries = unique_entries,
                    events = batch.len(),
                    "flushed signal usage"
                );
            }
            Err(error) => {
                // Restore the exact batch before newer events. This preserves usage and
                // ordering across transient ClickHouse failures.
                requeue_front(&mut pending, batch);
                retrying = true;
                next_attempt = Instant::now() + retry_delay;
                retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                metrics.inc_counter(
                    "rush_signal_usage_flushes_total",
                    &[("outcome", "error")],
                    1,
                );
                metrics.inc_counter("rush_signal_usage_flush_errors_total", &[], 1);
                metrics.observe_histogram(
                    "rush_signal_usage_flush_duration_ms",
                    &[],
                    started.elapsed().as_millis() as f64,
                );
                tracing::warn!(error = %error, "failed to flush signal usage; batch retained for retry");
            }
        }
        metrics.set_gauge(
            "rush_signal_usage_pending_events",
            &[],
            pending.len() as f64,
        );
    }
}

fn requeue_front(pending: &mut VecDeque<UsageEvent>, batch: Vec<UsageEvent>) {
    for event in batch.into_iter().rev() {
        pending.push_front(event);
    }
}

/// Build a tenant-correct batch INSERT. Kept separate so the SQL and aggregation can be
/// tested without requiring a live ClickHouse instance.
fn build_insert_sql(events: &[UsageEvent]) -> Option<(String, usize)> {
    let mut counts: HashMap<(String, String, String, String), u64> = HashMap::new();
    for event in events {
        *counts
            .entry((
                event.tenant_id.clone(),
                event.signal_name.clone(),
                event.signal_type.clone(),
                event.source.clone(),
            ))
            .or_insert(0) += 1;
    }

    if counts.is_empty() {
        return None;
    }

    let mut values = Vec::with_capacity(counts.len());
    for ((tenant_id, name, signal_type, source), count) in &counts {
        let escaped_tenant = crate::query_builder::escape_string_literal(tenant_id);
        let escaped_name = crate::query_builder::escape_string_literal(name);
        let escaped_type = crate::query_builder::escape_string_literal(signal_type);
        let escaped_source = crate::query_builder::escape_string_literal(source);
        values.push(format!(
            "('{escaped_tenant}', '{escaped_name}', '{escaped_type}', '{escaped_source}', now64(3), {count})"
        ));
    }

    Some((
        format!(
            "INSERT INTO observability.signal_usage (tenant_id, signal_name, signal_type, source, last_queried_at, query_count) VALUES {}",
            values.join(", ")
        ),
        counts.len(),
    ))
}

/// Flush a batch of usage events to ClickHouse.
async fn flush(ch: &Client, events: &[UsageEvent]) -> Result<usize, String> {
    let Some((sql, unique_entries)) = build_insert_sql(events) else {
        return Ok(0);
    };
    ch.query(&sql)
        .execute()
        .await
        .map(|_| unique_entries)
        .map_err(|e| e.to_string())
}

/// Extract all metric names from a PromQL query string.
/// Parses with promql-parser and walks the AST to find all VectorSelectors.
/// Returns all unique metric names found (works for binary expressions too).
pub fn extract_metrics_from_query(query: &str) -> Vec<String> {
    match promql_parser::parser::parse(query) {
        Ok(expr) => crate::promql::extract_metrics_from_expr(&expr),
        Err(_) => vec![],
    }
}

/// Extract signal names from span/spans query filters.
pub fn extract_span_signals(filters: &[(String, String)]) -> Vec<String> {
    let mut names = Vec::new();
    for (field, value) in filters {
        match field.as_str() {
            "service_name" | "http_path" | "http_method" => {
                names.push(format!("{field}={value}"));
            }
            _ => {}
        }
    }
    if names.is_empty() {
        names.push("spans_query".to_string());
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(tenant_id: &str, name: &str) -> UsageEvent {
        UsageEvent {
            tenant_id: tenant_id.to_string(),
            signal_name: name.to_string(),
            signal_type: "metric".to_string(),
            source: "prom_api".to_string(),
        }
    }

    #[test]
    fn insert_groups_by_tenant_and_never_uses_default_tenant() {
        let events = vec![
            event("tenant-a", "requests_total"),
            event("tenant-b", "requests_total"),
        ];
        let (sql, unique_entries) = build_insert_sql(&events).expect("non-empty SQL");

        assert_eq!(unique_entries, 2);
        assert!(sql.contains("(tenant_id, signal_name"));
        assert!(sql.contains("'tenant-a'"));
        assert!(sql.contains("'tenant-b'"));
        assert!(!sql.contains("DEFAULT"));
    }

    #[test]
    fn failed_batch_is_requeued_before_newer_events() {
        let mut pending = VecDeque::from([event("tenant-b", "new")]);
        requeue_front(
            &mut pending,
            vec![event("tenant-a", "old-1"), event("tenant-a", "old-2")],
        );

        assert_eq!(pending.pop_front().unwrap().signal_name, "old-1");
        assert_eq!(pending.pop_front().unwrap().signal_name, "old-2");
        assert_eq!(pending.pop_front().unwrap().signal_name, "new");
    }

    #[tokio::test]
    async fn tracker_preserves_tenant_on_channel_admission() {
        let (tx, mut rx) = mpsc::channel(2);
        let metrics = Arc::new(SelfMetrics::new());
        let tracker = UsageTracker {
            tx,
            queue_depth: Arc::new(AtomicUsize::new(0)),
            metrics,
        };

        tracker.track_many(
            "tenant-a",
            vec!["latency".to_string()],
            "metric",
            "prom_api",
        );
        assert_eq!(rx.recv().await.unwrap().tenant_id, "tenant-a");
    }
}
