use crate::self_metrics::{MetricKind, SelfMetrics};
use crate::spool::IngestBuffer;
use clickhouse::Client;
use std::sync::Arc;

#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    count: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct BytesRow {
    total: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct F64Row {
    v: f64,
}

pub fn spawn_stats_engine(ch: Client, buffer: Arc<IngestBuffer>, self_metrics: Arc<SelfMetrics>) {
    tokio::spawn(async move {
        // Emit cadence for rush_stats_* gauges. Default 15s (Prometheus-standard) so
        // rate()/short windows have ≥2 samples; override with RUSH_STATS_INTERVAL_SECS.
        let secs = std::env::var("RUSH_STATS_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&s| s >= 1)
            .unwrap_or(15);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(secs));
        loop {
            interval.tick().await;
            let start = std::time::Instant::now();
            let ok = match collect_and_write(&ch, &buffer, &self_metrics).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::error!("stats engine error: {e}");
                    false
                }
            };
            self_metrics.record_engine("stats_engine", start.elapsed().as_millis() as u64, ok);
        }
    });
}

async fn collect_and_write(
    ch: &Client,
    buffer: &IngestBuffer,
    self_metrics: &SelfMetrics,
) -> anyhow::Result<()> {
    let now = chrono::Utc::now();
    let now_nanos = now.timestamp_nanos_opt().unwrap_or(0);
    let one_hour_ago = (now - chrono::Duration::hours(1))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // All of these queries are independent — run them concurrently so the tick's
    // wall time is the slowest query, not the sum of all of them (~14 round trips).
    let q_spans = format!(
        "SELECT count() as count FROM spans WHERE timestamp >= parseDateTimeBestEffort('{one_hour_ago}') AND timestamp <= parseDateTimeBestEffort('{now_str}')"
    );
    let q_logs = format!(
        "SELECT count() as count FROM logs WHERE Timestamp >= parseDateTimeBestEffort('{one_hour_ago}') AND Timestamp <= parseDateTimeBestEffort('{now_str}')"
    );
    let q_gauge = format!(
        "SELECT count() as count FROM metrics_gauge WHERE TimeUnix >= parseDateTimeBestEffort('{one_hour_ago}') AND TimeUnix <= parseDateTimeBestEffort('{now_str}')"
    );
    let q_sum = format!(
        "SELECT count() as count FROM metrics_sum WHERE TimeUnix >= parseDateTimeBestEffort('{one_hour_ago}') AND TimeUnix <= parseDateTimeBestEffort('{now_str}')"
    );
    let q_hist = format!(
        "SELECT count() as count FROM metrics_histogram WHERE TimeUnix >= parseDateTimeBestEffort('{one_hour_ago}') AND TimeUnix <= parseDateTimeBestEffort('{now_str}')"
    );
    let (
        span_total,
        span_bytes,
        log_total,
        metric_gauge,
        metric_sum,
        metric_hist,
        unique_series,
        storage_bytes,
        storage_rows,
        storage_local_bytes,
        storage_object_store_bytes,
        disk_local_free_bytes,
        disk_local_total_bytes,
        buf_oldest,
    ) = tokio::join!(
        query_count(ch, &q_spans),
        query_bytes(
            ch,
            "SELECT sum(bytes_on_disk) as total FROM system.parts WHERE database = 'observability' AND table = 'spans' AND active"
        ),
        query_count(ch, &q_logs),
        query_count(ch, &q_gauge),
        query_count(ch, &q_sum),
        query_count(ch, &q_hist),
        query_count(
            ch,
            "SELECT uniq(MetricName, Attributes) as count FROM metrics_gauge WHERE TimeUnix >= now() - INTERVAL 1 HOUR"
        ),
        query_bytes(
            ch,
            "SELECT sum(bytes_on_disk) as total FROM system.parts WHERE database = 'observability' AND active"
        ),
        query_count(
            ch,
            "SELECT sum(rows) as count FROM system.parts WHERE database = 'observability' AND active"
        ),
        // Tiered storage breakdown: data bytes on local disk vs object store.
        // Classified by joining each part's disk to system.disks.type, matching
        // the on-demand /stats endpoint. Object store = any non-Local disk.
        query_bytes(
            ch,
            "SELECT sum(p.bytes_on_disk) as total FROM system.parts p \
             LEFT JOIN system.disks d ON p.disk_name = d.name \
             WHERE p.database = 'observability' AND p.active AND d.type = 'Local'"
        ),
        query_bytes(
            ch,
            "SELECT sum(p.bytes_on_disk) as total FROM system.parts p \
             LEFT JOIN system.disks d ON p.disk_name = d.name \
             WHERE p.database = 'observability' AND p.active AND d.type != 'Local'"
        ),
        // Local disk capacity (headroom) from system.disks.
        query_bytes(
            ch,
            "SELECT sum(free_space) as total FROM system.disks WHERE type = 'Local'"
        ),
        query_bytes(
            ch,
            "SELECT sum(total_space) as total FROM system.disks WHERE type = 'Local'"
        ),
        // Ingest buffer (durable spool) replay lag.
        buffer.oldest_age_secs(),
    );
    let metric_total = metric_gauge + metric_sum + metric_hist;
    let buf_oldest = buf_oldest.unwrap_or(0);

    // ── Write all metrics ──
    let metrics: Vec<(&str, f64)> = vec![
        (
            "rush_stats_ingest_buffer_pending_bytes",
            buffer.total_bytes() as f64,
        ),
        (
            "rush_stats_ingest_buffer_pending_count",
            buffer.segment_count() as f64,
        ),
        (
            "rush_stats_ingest_buffer_oldest_age_secs",
            buf_oldest as f64,
        ),
        // Cumulative counter — drain rate = rate(rush_stats_ingest_buffer_committed_total).
        (
            "rush_stats_ingest_buffer_committed_total",
            buffer.committed_total() as f64,
        ),
        ("rush_stats_span_events_total", span_total as f64),
        ("rush_stats_span_events_bytes", span_bytes as f64),
        ("rush_stats_logs_total", log_total as f64),
        ("rush_stats_metrics_total", metric_total as f64),
        ("rush_stats_unique_series", unique_series as f64),
        ("rush_stats_storage_bytes", storage_bytes as f64),
        ("rush_stats_storage_rows", storage_rows as f64),
        // Tiered storage: where the data physically lives.
        ("rush_stats_storage_local_bytes", storage_local_bytes as f64),
        (
            "rush_stats_storage_object_store_bytes",
            storage_object_store_bytes as f64,
        ),
        // Local disk capacity, for headroom / move-pressure monitoring.
        (
            "rush_stats_disk_local_free_bytes",
            disk_local_free_bytes as f64,
        ),
        (
            "rush_stats_disk_local_total_bytes",
            disk_local_total_bytes as f64,
        ),
    ];

    let values: Vec<String> = metrics
        .iter()
        .map(|(name, val)| {
            format!(
                "({{}}, '', '', '', {{}}, 0, '', 'wide-stats-engine', '{name}', '', '', {{}}, \
             {now_nanos}, {now_nanos}, {val}, 0, [], [], [], [], [])"
            )
        })
        .collect();

    let sql = format!(
        "INSERT INTO metrics_gauge \
         (ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, \
          ScopeDroppedAttrCount, ScopeSchemaUrl, ServiceName, MetricName, MetricDescription, \
          MetricUnit, Attributes, StartTimeUnix, TimeUnix, Value, Flags, \
          Exemplars.FilteredAttributes, Exemplars.TimeUnix, Exemplars.Value, \
          Exemplars.SpanId, Exemplars.TraceId) VALUES {}",
        values.join(", ")
    );

    if let Err(e) = ch.query(&sql).execute().await {
        tracing::warn!("stats engine: metric write failed: {e}");
    }

    tracing::debug!("stats engine: wrote {} metrics", metrics.len());

    // ── Ingest spool gauges into SelfMetrics (group B; set from the tick, not the hot path) ──
    self_metrics.set_gauge("rush_ingest_spool_bytes", &[], buffer.total_bytes() as f64);
    self_metrics.set_gauge(
        "rush_ingest_spool_segments",
        &[],
        buffer.segment_count() as f64,
    );
    self_metrics.set_gauge("rush_ingest_spool_oldest_age_secs", &[], buf_oldest as f64);

    // ── ClickHouse health gauges (group D) into both SelfMetrics and metrics_gauge ──
    collect_ch_health(ch, self_metrics).await;

    // ── Self-ingest: write the SelfMetrics snapshot into our own metrics tables so the
    // same rush_http_*/rush_ingest_*/rush_engine_*/rush_ch_* series are queryable via
    // the in-product PromQL API (/prom/api/v1/*). ──
    self_ingest_snapshot(ch, self_metrics, now_nanos).await;

    Ok(())
}

/// Query a curated ClickHouse health set and emit `rush_ch_*` gauges into SelfMetrics.
/// Each metric is fetched independently and skipped gracefully if unavailable on this
/// CH version (the query simply returns 0/None and we set what we got). Sources prefer
/// instantaneous gauges (`system.metrics`, `system.asynchronous_metrics`) over event deltas.
async fn collect_ch_health(ch: &Client, self_metrics: &SelfMetrics) {
    // (metric name, SQL returning a single Float64 column `v`)
    // NOTE: every probe MUST return a single Float64 column named `v`. clickhouse-rs uses
    // RowBinary and reinterprets bytes by declared type, so a UInt64 `count()`/`max()`
    // deserialized into f64 yields garbage — wrap all integer aggregates in toFloat64().
    let probes: [(&str, &str); 9] = [
        // Max parts in any single partition — the classic "too many parts" early warning.
        (
            "rush_ch_max_part_count_for_partition",
            "SELECT toFloat64(max(c)) AS v FROM (SELECT count() AS c FROM system.parts WHERE database='observability' AND active GROUP BY table, partition)",
        ),
        // Active background merges / mutations right now.
        (
            "rush_ch_active_merges",
            "SELECT toFloat64(count()) AS v FROM system.merges",
        ),
        (
            "rush_ch_active_mutations",
            "SELECT toFloat64(count()) AS v FROM system.mutations WHERE is_done = 0",
        ),
        // Longest currently-running merge, seconds.
        (
            "rush_ch_longest_running_merge_secs",
            "SELECT toFloat64(max(elapsed)) AS v FROM system.merges",
        ),
        // Insert pressure — instantaneous current values from system.metrics.
        (
            "rush_ch_delayed_inserts",
            "SELECT toFloat64(value) AS v FROM system.metrics WHERE metric = 'DelayedInserts'",
        ),
        // Cumulative rejected inserts (event counter).
        (
            "rush_ch_rejected_inserts",
            "SELECT toFloat64(value) AS v FROM system.events WHERE event = 'RejectedInserts'",
        ),
        // Server resident memory.
        (
            "rush_ch_memory_resident_bytes",
            "SELECT toFloat64(value) AS v FROM system.asynchronous_metrics WHERE metric = 'MemoryResident'",
        ),
        // Cumulative failed queries (event counter).
        (
            "rush_ch_failed_query_total",
            "SELECT toFloat64(value) AS v FROM system.events WHERE event = 'FailedQuery'",
        ),
        // Background merges+mutations pool occupancy.
        (
            "rush_ch_background_pool_task",
            "SELECT toFloat64(value) AS v FROM system.metrics WHERE metric = 'BackgroundMergesAndMutationsPoolTask'",
        ),
    ];

    for (name, sql) in probes {
        match ch.query(sql).fetch_optional::<F64Row>().await {
            Ok(Some(row)) => self_metrics.set_gauge(name, &[], row.v),
            Ok(None) => {
                // No matching row (metric absent on this CH version) — skip gracefully.
                tracing::debug!(
                    metric = name,
                    "stats engine: ch health metric unavailable (no row)"
                );
            }
            Err(e) => {
                // Query failed (e.g. table/column not present on this CH version) — skip.
                tracing::debug!(metric = name, error = %e, "stats engine: ch health probe failed, skipping");
            }
        }
    }
}

/// Insert the SelfMetrics snapshot into `metrics_gauge` / `metrics_sum` (by kind). Labels
/// become the ClickHouse `Attributes` map. Reuses the same raw-SQL insert path the
/// rush_stats_* gauges use. Best-effort: a failed insert is logged, not fatal to the tick.
async fn self_ingest_snapshot(ch: &Client, self_metrics: &SelfMetrics, now_nanos: i64) {
    let points = self_metrics.snapshot_series();
    if points.is_empty() {
        return;
    }

    let mut gauge_rows: Vec<String> = Vec::new();
    let mut sum_rows: Vec<String> = Vec::new();
    for p in &points {
        let attrs = labels_to_ch_map(&p.labels);
        let name = escape_sql(&p.name);
        // Column order matches the rush_stats_* insert (Attributes filled with the map).
        let row = format!(
            "({{}}, '', '', '', {{}}, 0, '', 'wide-self-metrics', '{name}', '', '', {attrs}, \
             {now_nanos}, {now_nanos}, {val}, 0, [], [], [], [], [])",
            val = p.value,
        );
        match p.kind {
            MetricKind::Gauge => gauge_rows.push(row),
            // metrics_sum has two extra trailing columns: AggregationTemporality, IsMonotonic.
            // Append them to the gauge-shaped row for the sum table.
            MetricKind::Sum => sum_rows.push(format!(
                "({{}}, '', '', '', {{}}, 0, '', 'wide-self-metrics', '{name}', '', '', {attrs}, \
                 {now_nanos}, {now_nanos}, {val}, 0, [], [], [], [], [], 2, 1)",
                val = p.value,
            )),
        }
    }

    if !gauge_rows.is_empty() {
        let sql = format!(
            "INSERT INTO metrics_gauge \
             (ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, \
              ScopeDroppedAttrCount, ScopeSchemaUrl, ServiceName, MetricName, MetricDescription, \
              MetricUnit, Attributes, StartTimeUnix, TimeUnix, Value, Flags, \
              Exemplars.FilteredAttributes, Exemplars.TimeUnix, Exemplars.Value, \
              Exemplars.SpanId, Exemplars.TraceId) VALUES {}",
            gauge_rows.join(", ")
        );
        if let Err(e) = ch.query(&sql).execute().await {
            tracing::warn!("stats engine: self-metrics gauge ingest failed: {e}");
        }
    }

    if !sum_rows.is_empty() {
        let sql = format!(
            "INSERT INTO metrics_sum \
             (ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, \
              ScopeDroppedAttrCount, ScopeSchemaUrl, ServiceName, MetricName, MetricDescription, \
              MetricUnit, Attributes, StartTimeUnix, TimeUnix, Value, Flags, \
              Exemplars.FilteredAttributes, Exemplars.TimeUnix, Exemplars.Value, \
              Exemplars.SpanId, Exemplars.TraceId, AggregationTemporality, IsMonotonic) VALUES {}",
            sum_rows.join(", ")
        );
        if let Err(e) = ch.query(&sql).execute().await {
            tracing::warn!("stats engine: self-metrics sum ingest failed: {e}");
        }
    }
}

/// Render a sorted label set as a ClickHouse Map literal: `{'k':'v','k2':'v2'}` (or `{}`).
fn labels_to_ch_map(labels: &[(&'static str, String)]) -> String {
    if labels.is_empty() {
        return "{}".to_string();
    }
    let inner: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("'{}':'{}'", escape_sql(k), escape_sql(v)))
        .collect();
    format!("{{{}}}", inner.join(","))
}

/// Escape single quotes and backslashes for embedding inside a ClickHouse single-quoted
/// string literal. Self-metric names/labels are internal/finite, but escaping keeps the
/// raw INSERT robust.
fn escape_sql(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

async fn query_count(ch: &Client, sql: &str) -> u64 {
    ch.query(sql)
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0)
}

async fn query_bytes(ch: &Client, sql: &str) -> u64 {
    ch.query(sql)
        .fetch_one::<BytesRow>()
        .await
        .map(|r| r.total)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ch_map_literal_render() {
        assert_eq!(labels_to_ch_map(&[]), "{}");
        let labels: Vec<(&'static str, String)> = vec![("engine", "stats_engine".to_string())];
        assert_eq!(labels_to_ch_map(&labels), "{'engine':'stats_engine'}");
        let multi: Vec<(&'static str, String)> = vec![
            ("method", "POST".to_string()),
            ("route", "/api/v1/query".to_string()),
        ];
        assert_eq!(
            labels_to_ch_map(&multi),
            "{'method':'POST','route':'/api/v1/query'}"
        );
    }

    #[test]
    fn sql_escape() {
        assert_eq!(escape_sql("a'b"), "a\\'b");
        assert_eq!(escape_sql("a\\b"), "a\\\\b");
        assert_eq!(escape_sql("plain"), "plain");
    }
}
