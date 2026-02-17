use clickhouse::Client;
use std::time::Duration;
use tokio::sync::mpsc;

/// A single usage event emitted by query handlers.
#[derive(Debug, Clone)]
pub struct UsageEvent {
    pub signal_name: String,
    pub signal_type: String, // "metric", "span", "log"
    pub source: String,      // "explore", "dashboard", "alert", "prom_api"
}

/// Handle for sending usage events (non-blocking, fire-and-forget).
#[derive(Clone)]
pub struct UsageTracker {
    tx: mpsc::UnboundedSender<UsageEvent>,
}

impl UsageTracker {
    /// Track a signal usage event. Never blocks the caller.
    pub fn track(&self, event: UsageEvent) {
        let _ = self.tx.send(event);
    }

    /// Convenience: track multiple signal names of the same type/source.
    pub fn track_many(&self, names: Vec<String>, signal_type: &str, source: &str) {
        for name in names {
            let _ = self.tx.send(UsageEvent {
                signal_name: name,
                signal_type: signal_type.to_string(),
                source: source.to_string(),
            });
        }
    }
}

/// Spawn the background usage writer and return the tracker handle.
pub fn spawn(ch: Client) -> UsageTracker {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(usage_writer(rx, ch));
    UsageTracker { tx }
}

/// Background task that batches usage events and flushes to ClickHouse every 30s.
async fn usage_writer(mut rx: mpsc::UnboundedReceiver<UsageEvent>, ch: Client) {
    let mut buffer: Vec<UsageEvent> = Vec::new();
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                buffer.push(event);
                // Flush immediately if buffer is large
                if buffer.len() >= 500 {
                    let batch = std::mem::take(&mut buffer);
                    flush(&ch, batch).await;
                }
            }
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    let batch = std::mem::take(&mut buffer);
                    flush(&ch, batch).await;
                }
            }
        }
    }
}

/// Flush a batch of usage events to ClickHouse.
async fn flush(ch: &Client, events: Vec<UsageEvent>) {
    // Deduplicate within the batch: group by (signal_name, signal_type, source)
    use std::collections::HashMap;
    let mut counts: HashMap<(String, String, String), u64> = HashMap::new();
    for e in &events {
        *counts
            .entry((
                e.signal_name.clone(),
                e.signal_type.clone(),
                e.source.clone(),
            ))
            .or_insert(0) += 1;
    }

    // Build a batch INSERT
    let mut values = Vec::new();
    for ((name, sig_type, source), count) in &counts {
        let escaped_name = name.replace('\'', "\\'");
        let escaped_source = source.replace('\'', "\\'");
        values.push(format!(
            "('{escaped_name}', '{sig_type}', '{escaped_source}', now64(3), {count})"
        ));
    }

    if values.is_empty() {
        return;
    }

    let sql = format!(
        "INSERT INTO signal_usage (signal_name, signal_type, source, last_queried_at, query_count) VALUES {}",
        values.join(", ")
    );

    if let Err(e) = ch.query(&sql).execute().await {
        tracing::warn!("Failed to flush signal usage: {e}");
    } else {
        tracing::debug!("Flushed {} usage entries ({} events)", counts.len(), events.len());
    }
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

/// Extract signal names from span/wide_events query filters.
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
        names.push("wide_events_query".to_string());
    }
    names
}
