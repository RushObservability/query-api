use clickhouse::Client;
use dashmap::DashMap;
use std::sync::Arc;

/// Accumulates ingest counts in memory, flushes to ClickHouse every 10 seconds.
/// Uses DashMap for lock-free concurrent writes from ingest handlers.
#[derive(Clone)]
pub struct UsageAccumulator {
    /// Key: (tenant_id, signal), Value: (events_count, bytes_count)
    counters: Arc<DashMap<(String, String), (u64, u64)>>,
}

impl UsageAccumulator {
    pub fn new() -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
        }
    }

    /// Record an ingest batch. Called from ingest handlers.
    pub fn record(&self, tenant_id: &str, signal: &str, events: u64, bytes: u64) {
        self.counters
            .entry((tenant_id.to_string(), signal.to_string()))
            .and_modify(|(e, b)| {
                *e += events;
                *b += bytes;
            })
            .or_insert((events, bytes));
    }

    /// Record an ingest batch that was dropped because the signal is disabled
    /// for the tenant. Lands in the same per-tenant usage store under the signal
    /// name suffixed `_dropped` (e.g. "logs_dropped") so admins can see blocked
    /// volume alongside accepted volume.
    pub fn record_dropped(&self, tenant_id: &str, signal: &str, events: u64, bytes: u64) {
        self.record(tenant_id, &format!("{signal}_dropped"), events, bytes);
    }

    /// Spawn the background flush loop.
    pub fn spawn_flusher(&self, ch: Client) {
        let counters = self.counters.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                // Drain all accumulated counters
                let snapshot: Vec<((String, String), (u64, u64))> = {
                    let mut items = Vec::new();
                    counters.retain(|k, v| {
                        items.push((k.clone(), *v));
                        false // remove after reading
                    });
                    items
                };
                if snapshot.is_empty() {
                    continue;
                }
                // Build a batch INSERT for efficiency
                let mut values = Vec::new();
                for ((tenant_id, signal), (events, bytes)) in &snapshot {
                    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
                    let escaped_signal = crate::query_builder::escape_string_literal(&signal);
                    values.push(format!(
                        "('{escaped_tenant}', '{escaped_signal}', {events}, {bytes})"
                    ));
                }
                let sql = format!(
                    "INSERT INTO observability.tenant_usage (tenant_id, signal, events_count, bytes_count) VALUES {}",
                    values.join(", ")
                );
                if let Err(e) = ch.query(&sql).execute().await {
                    tracing::warn!(
                        engine = "usage_accumulator",
                        error = %e,
                        "failed to flush usage counters"
                    );
                } else {
                    tracing::debug!(
                        engine = "usage_accumulator",
                        flushed = snapshot.len(),
                        "usage counters flushed"
                    );
                }
            }
        });
    }
}
