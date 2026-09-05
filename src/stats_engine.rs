use crate::self_metrics::{MetricKind, SelfMetrics};
use crate::spool::IngestBuffer;
use clickhouse::Client;
use futures_util::stream::{self, StreamExt};
use std::sync::Arc;
use std::time::Instant;

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

pub fn spawn_stats_engine(
    ch: Client,
    buffer: Arc<IngestBuffer>,
    self_metrics: Arc<SelfMetrics>,
    instance_id: String,
) {
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
            let ok = match collect_and_write(&ch, &buffer, &self_metrics, &instance_id).await {
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
    instance_id: &str,
) -> anyhow::Result<()> {
    // Object-store buffers are shared by ingest replicas. Reconcile before
    // exporting gauges so a drain performed by another pod is reflected here.
    if let Err(e) = buffer.refresh_counts().await {
        tracing::warn!(error = %e, "stats engine: failed to refresh ingest buffer counters");
    }
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

    let instance_attrs = labels_to_ch_map(&[("instance", instance_id.to_string())]);
    let values: Vec<String> = metrics
        .iter()
        .map(|(name, val)| {
            format!(
                "({{}}, '', '', '', {{}}, 0, '', 'wide-stats-engine', '{name}', '', '', {instance_attrs}, \
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
    let buffer_bytes = buffer.total_bytes();
    let buffer_max_bytes = buffer.max_bytes();
    let buffer_utilization_ratio = spool_utilization_ratio(buffer_bytes, buffer_max_bytes);
    self_metrics.set_gauge("rush_ingest_spool_bytes", &[], buffer_bytes as f64);
    self_metrics.set_gauge("rush_ingest_spool_max_bytes", &[], buffer_max_bytes as f64);
    self_metrics.set_gauge(
        "rush_ingest_spool_utilization_ratio",
        &[],
        buffer_utilization_ratio,
    );
    self_metrics.set_gauge(
        "rush_ingest_spool_segments",
        &[],
        buffer.segment_count() as f64,
    );
    self_metrics.set_gauge("rush_ingest_spool_oldest_age_secs", &[], buf_oldest as f64);

    // Storage capacity gauges are process-global and intentionally have no
    // tenant labels. They power the admin capacity view and keep disk headroom
    // visible even when no recent stats snapshot has been written yet.
    self_metrics.set_gauge(
        "rush_stats_disk_local_free_bytes",
        &[],
        disk_local_free_bytes as f64,
    );
    self_metrics.set_gauge(
        "rush_stats_disk_local_total_bytes",
        &[],
        disk_local_total_bytes as f64,
    );
    self_metrics.set_gauge("rush_stats_storage_bytes", &[], storage_bytes as f64);
    self_metrics.set_gauge("rush_stats_storage_rows", &[], storage_rows as f64);

    // ── ClickHouse health gauges (group D) into both SelfMetrics and metrics_gauge ──
    collect_ch_health(ch, self_metrics).await;

    // ── Self-ingest: write the SelfMetrics snapshot into our own metrics tables so the
    // same rush_http_*/rush_ingest_*/rush_engine_*/rush_ch_* series are queryable via
    // the in-product PromQL API (/prom/api/v1/*). ──
    self_ingest_snapshot(ch, self_metrics, now_nanos, instance_id).await;

    Ok(())
}

/// Query a curated ClickHouse health set and emit `rush_ch_*` gauges into SelfMetrics.
/// Each metric is fetched independently and skipped gracefully if unavailable on this
/// CH version (the query simply returns 0/None and we set what we got). Sources prefer
/// instantaneous gauges (`system.metrics`, `system.asynchronous_metrics`) over event deltas.
/// Fixed, low-cardinality ClickHouse health probes. Every query is best-effort:
/// older ClickHouse versions may not expose a system metric or query-log column,
/// and `run_ch_probe` records that as an unavailable probe rather than failing the
/// stats tick.
const CH_HEALTH_PROBES: &[(&str, &str)] = &[
    // (metric name, SQL returning a single Float64 column `v`)
    // NOTE: every probe MUST return a single Float64 column named `v`. clickhouse-rs uses
    // RowBinary and reinterprets bytes by declared type, so a UInt64 `count()`/`max()`
    // deserialized into f64 yields garbage — wrap all integer aggregates in toFloat64().
    // Max parts in any single partition — the classic "too many parts" warning.
    (
        "rush_ch_max_part_count_for_partition",
        "SELECT toFloat64(max(c)) AS v FROM (SELECT count() AS c FROM system.parts WHERE database='observability' AND active GROUP BY table, partition)",
    ),
    (
        "rush_ch_active_parts",
        "SELECT toFloat64(count()) AS v FROM system.parts WHERE database='observability' AND active",
    ),
    (
        "rush_ch_active_merges",
        "SELECT toFloat64(count()) AS v FROM system.merges WHERE database = 'observability'",
    ),
    (
        "rush_ch_active_mutations",
        "SELECT toFloat64(count()) AS v FROM system.mutations WHERE database = 'observability' AND is_done = 0",
    ),
    (
        "rush_ch_oldest_mutation_secs",
        "SELECT toFloat64(if(count() = 0, 0, dateDiff('second', min(create_time), now()))) AS v FROM system.mutations WHERE database = 'observability' AND is_done = 0",
    ),
    (
        "rush_ch_mutation_parts_to_do",
        "SELECT toFloat64(sum(parts_to_do)) AS v FROM system.mutations WHERE database = 'observability' AND is_done = 0",
    ),
    (
        "rush_ch_failed_mutations",
        "SELECT toFloat64(countIf(latest_fail_reason != '')) AS v FROM system.mutations WHERE database = 'observability' AND is_done = 0",
    ),
    (
        "rush_ch_longest_running_merge_secs",
        "SELECT toFloat64(max(elapsed)) AS v FROM system.merges WHERE database = 'observability'",
    ),
    (
        "rush_ch_delayed_inserts",
        "SELECT toFloat64(value) AS v FROM system.metrics WHERE metric = 'DelayedInserts'",
    ),
    (
        "rush_ch_rejected_inserts_total",
        "SELECT toFloat64(sumIf(value, event = 'RejectedInserts')) AS v FROM system.events",
    ),
    (
        "rush_ch_memory_resident_bytes",
        "SELECT toFloat64(value) AS v FROM system.asynchronous_metrics WHERE metric = 'MemoryResident'",
    ),
    (
        "rush_ch_memory_tracking_bytes",
        "SELECT toFloat64(value) AS v FROM system.metrics WHERE metric = 'MemoryTracking'",
    ),
    (
        "rush_ch_failed_query_total",
        "SELECT toFloat64(value) AS v FROM system.events WHERE event = 'FailedQuery'",
    ),
    (
        "rush_ch_queries_total",
        "SELECT toFloat64(value) AS v FROM system.events WHERE event = 'Query'",
    ),
    (
        "rush_ch_select_queries_total",
        "SELECT toFloat64(value) AS v FROM system.events WHERE event = 'SelectQuery'",
    ),
    (
        "rush_ch_insert_queries_total",
        "SELECT toFloat64(value) AS v FROM system.events WHERE event = 'InsertQuery'",
    ),
    (
        "rush_ch_selected_rows_total",
        "SELECT toFloat64(value) AS v FROM system.events WHERE event = 'SelectedRows'",
    ),
    (
        "rush_ch_selected_bytes_total",
        "SELECT toFloat64(value) AS v FROM system.events WHERE event = 'SelectedBytes'",
    ),
    (
        "rush_ch_inserted_rows_total",
        "SELECT toFloat64(value) AS v FROM system.events WHERE event = 'WrittenRows'",
    ),
    (
        "rush_ch_inserted_bytes_total",
        "SELECT toFloat64(value) AS v FROM system.events WHERE event = 'WrittenBytes'",
    ),
    (
        "rush_ch_background_pool_task",
        "SELECT toFloat64(value) AS v FROM system.metrics WHERE metric = 'BackgroundMergesAndMutationsPoolTask'",
    ),
    (
        "rush_ch_background_pool_size",
        "SELECT toFloat64(value) AS v FROM system.server_settings WHERE name = 'background_pool_size'",
    ),
    (
        "rush_ch_parts_delay_threshold",
        "SELECT toFloat64(value) AS v FROM system.merge_tree_settings WHERE name = 'parts_to_delay_insert'",
    ),
    (
        "rush_ch_parts_throw_threshold",
        "SELECT toFloat64(value) AS v FROM system.merge_tree_settings WHERE name = 'parts_to_throw_insert'",
    ),
    (
        "rush_ch_max_concurrent_select_queries",
        "SELECT toFloat64(value) AS v FROM system.server_settings WHERE name = 'max_concurrent_select_queries'",
    ),
    (
        "rush_ch_active_queries",
        "SELECT toFloat64(count()) AS v FROM system.processes WHERE query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_longest_running_query_secs",
        "SELECT toFloat64(max(elapsed)) AS v FROM system.processes WHERE query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_active_query_memory_bytes",
        "SELECT toFloat64(sum(memory_usage)) AS v FROM system.processes WHERE query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_active_query_read_rows",
        "SELECT toFloat64(sum(read_rows)) AS v FROM system.processes WHERE query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_active_query_read_bytes",
        "SELECT toFloat64(sum(read_bytes)) AS v FROM system.processes WHERE query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_disk_local_used_bytes",
        "SELECT toFloat64(sum(total_space - free_space)) AS v FROM system.disks WHERE type = 'Local'",
    ),
    (
        "rush_ch_max_disk_used_pct",
        "SELECT toFloat64(max(if(total_space > 0, (total_space - free_space) * 100.0 / total_space, 0))) AS v FROM system.disks WHERE type = 'Local'",
    ),
    (
        "rush_ch_memory_capacity_bytes",
        "SELECT toFloat64(if(maxIf(value, metric = 'CGroupMemoryTotal') > 0, maxIf(value, metric = 'CGroupMemoryTotal'), maxIf(value, metric = 'OSMemoryTotal'))) AS v FROM system.asynchronous_metrics WHERE metric IN ('CGroupMemoryTotal', 'OSMemoryTotal')",
    ),
    // Query-log fields are optional: query_log may be disabled or have a shorter
    // retention window, so unavailable probes are intentionally non-fatal.
    (
        "rush_ch_query_log_recent_queries",
        "SELECT toFloat64(countIf(type = 'QueryFinish')) AS v FROM system.query_log WHERE event_time >= now() - INTERVAL 5 MINUTE AND is_initial_query = 1 AND query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_query_log_p50_duration_ms",
        "SELECT toFloat64(quantile(0.50)(query_duration_ms)) AS v FROM system.query_log WHERE event_time >= now() - INTERVAL 5 MINUTE AND type = 'QueryFinish' AND is_initial_query = 1 AND query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_query_log_p95_duration_ms",
        "SELECT toFloat64(quantile(0.95)(query_duration_ms)) AS v FROM system.query_log WHERE event_time >= now() - INTERVAL 5 MINUTE AND type = 'QueryFinish' AND is_initial_query = 1 AND query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_query_log_read_rows",
        "SELECT toFloat64(sum(read_rows)) AS v FROM system.query_log WHERE event_time >= now() - INTERVAL 5 MINUTE AND type = 'QueryFinish' AND is_initial_query = 1 AND query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_query_log_read_bytes",
        "SELECT toFloat64(sum(read_bytes)) AS v FROM system.query_log WHERE event_time >= now() - INTERVAL 5 MINUTE AND type = 'QueryFinish' AND is_initial_query = 1 AND query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_query_log_result_rows",
        "SELECT toFloat64(sum(result_rows)) AS v FROM system.query_log WHERE event_time >= now() - INTERVAL 5 MINUTE AND type = 'QueryFinish' AND is_initial_query = 1 AND query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_query_log_result_bytes",
        "SELECT toFloat64(sum(result_bytes)) AS v FROM system.query_log WHERE event_time >= now() - INTERVAL 5 MINUTE AND type = 'QueryFinish' AND is_initial_query = 1 AND query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_query_log_memory_p95_bytes",
        "SELECT quantile(0.95)(toFloat64(memory_usage)) AS v FROM system.query_log WHERE event_time >= now() - INTERVAL 5 MINUTE AND type = 'QueryFinish' AND is_initial_query = 1 AND query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
    (
        "rush_ch_query_log_recent_errors",
        "SELECT toFloat64(countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing'))) AS v FROM system.query_log WHERE event_time >= now() - INTERVAL 5 MINUTE AND is_initial_query = 1 AND query_kind = 'Select' AND query_id NOT LIKE 'rush-stats-%'",
    ),
];

const CH_HEALTH_PROBE_CONCURRENCY: usize = 4;
const STATS_QUERY_ID_PREFIX: &str = "rush-stats-";

async fn collect_ch_health(ch: &Client, self_metrics: &SelfMetrics) {
    // A small concurrency bound keeps the collector quick without making its own
    // system-table fan-out look like user query pressure.
    stream::iter(CH_HEALTH_PROBES.iter().copied())
        .for_each_concurrent(CH_HEALTH_PROBE_CONCURRENCY, |(name, sql)| {
            run_ch_probe(ch, self_metrics, name, sql)
        })
        .await;
}

async fn run_ch_probe(
    ch: &Client,
    self_metrics: &SelfMetrics,
    name: &'static str,
    sql: &'static str,
) {
    let start = Instant::now();
    let query_id = format!("{STATS_QUERY_ID_PREFIX}{name}");
    match ch
        .query(sql)
        .with_option("query_id", query_id)
        .fetch_optional::<F64Row>()
        .await
    {
        Ok(Some(row)) => {
            self_metrics.set_gauge(name, &[], row.v);
            record_ch_probe(self_metrics, name, start, true);
        }
        Ok(None) => {
            // No matching row (metric absent on this CH version) — skip gracefully.
            tracing::debug!(
                metric = name,
                "stats engine: ch health metric unavailable (no row)"
            );
            record_ch_probe(self_metrics, name, start, false);
        }
        Err(e) => {
            // Query failed (e.g. table/column not present on this CH version) — skip.
            tracing::debug!(metric = name, error = %e, "stats engine: ch health probe failed, skipping");
            record_ch_probe(self_metrics, name, start, false);
        }
    }
}

fn record_ch_probe(self_metrics: &SelfMetrics, probe: &'static str, start: Instant, ok: bool) {
    let outcome = if ok { "ok" } else { "error" };
    self_metrics.inc_counter(
        "rush_ch_health_probes_total",
        &[("probe", probe), ("outcome", outcome)],
        1,
    );
    self_metrics.observe_histogram(
        "rush_ch_health_probe_duration_ms",
        &[("probe", probe)],
        start.elapsed().as_secs_f64() * 1000.0,
    );
}

fn spool_utilization_ratio(bytes: u64, max_bytes: u64) -> f64 {
    if max_bytes == 0 {
        0.0
    } else {
        bytes as f64 / max_bytes as f64
    }
}

/// Insert the SelfMetrics snapshot into `metrics_gauge` / `metrics_sum` (by kind). Labels
/// become the ClickHouse `Attributes` map. Reuses the same raw-SQL insert path the
/// rush_stats_* gauges use. Best-effort: a failed insert is logged, not fatal to the tick.
async fn self_ingest_snapshot(
    ch: &Client,
    self_metrics: &SelfMetrics,
    now_nanos: i64,
    instance_id: &str,
) {
    let points = self_metrics.snapshot_series();
    if points.is_empty() {
        return;
    }

    let mut gauge_rows: Vec<String> = Vec::new();
    let mut sum_rows: Vec<String> = Vec::new();
    for p in &points {
        let attrs = labels_to_ch_map(&labels_with_instance(&p.labels, instance_id));
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

/// Add the deployment's stable identity to every self-ingested series. The in-process
/// `/metrics` registry remains free of deployment-specific labels; this label is added at
/// the ClickHouse boundary so multiple API replicas can be distinguished in product queries.
fn labels_with_instance(
    labels: &[(&'static str, String)],
    instance_id: &str,
) -> Vec<(&'static str, String)> {
    let mut result = labels.to_vec();
    if !result.iter().any(|(key, _)| *key == "instance") {
        result.push(("instance", instance_id.to_string()));
        result.sort_by(|a, b| a.0.cmp(b.0));
    }
    result
}

/// Resolve a stable per-replica identity. Kubernetes deployments should set
/// `RUSH_INSTANCE_ID` from the pod name; `POD_NAME` and `HOSTNAME` keep the default
/// configuration useful with standard Downward API manifests and local containers.
pub fn configured_instance_id() -> String {
    ["RUSH_INSTANCE_ID", "POD_NAME", "HOSTNAME"]
        .iter()
        .find_map(|key| {
            std::env::var(key)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .map(|value| value.chars().take(128).collect())
        .unwrap_or_else(|| "unknown".to_string())
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
        let instance: Vec<(&'static str, String)> = vec![("instance", "api-0".to_string())];
        assert_eq!(labels_to_ch_map(&instance), "{'instance':'api-0'}");
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

    #[test]
    fn instance_label_is_sorted_and_does_not_duplicate() {
        let labels = vec![("signal", "logs".to_string())];
        assert_eq!(
            labels_with_instance(&labels, "api-0"),
            vec![
                ("instance", "api-0".to_string()),
                ("signal", "logs".to_string())
            ]
        );

        let existing = vec![("instance", "already-set".to_string())];
        assert_eq!(labels_with_instance(&existing, "api-0"), existing);
    }

    #[test]
    fn clickhouse_health_probes_are_fixed_and_low_cardinality() {
        assert!(CH_HEALTH_PROBES.len() >= 30);
        assert!(
            CH_HEALTH_PROBES
                .iter()
                .any(|(name, _)| { *name == "rush_ch_query_log_p95_duration_ms" })
        );
        assert!(
            CH_HEALTH_PROBES
                .iter()
                .all(|(name, sql)| { name.starts_with("rush_ch_") && sql.contains(" AS v") })
        );
    }

    #[test]
    fn read_pressure_probes_exclude_the_stats_collector() {
        for name in [
            "rush_ch_active_queries",
            "rush_ch_longest_running_query_secs",
            "rush_ch_query_log_p50_duration_ms",
            "rush_ch_query_log_p95_duration_ms",
        ] {
            let sql = CH_HEALTH_PROBES
                .iter()
                .find_map(|(probe_name, sql)| (*probe_name == name).then_some(*sql))
                .expect("read pressure probe");
            assert!(sql.contains("query_id NOT LIKE 'rush-stats-%'"));
            assert!(sql.contains("query_kind = 'Select'"));
        }
    }

    #[test]
    fn query_latency_uses_the_clickhouse_duration_column() {
        let sql = CH_HEALTH_PROBES
            .iter()
            .find_map(|(name, sql)| (*name == "rush_ch_query_log_p95_duration_ms").then_some(*sql))
            .expect("p95 probe");
        assert!(sql.contains("query_duration_ms"));
        assert!(!sql.contains("QueryDurationMicroseconds"));
        assert!(CH_HEALTH_PROBE_CONCURRENCY < 8);
    }

    #[test]
    fn spool_utilization_ratio_handles_empty_and_full_buffers() {
        assert_eq!(spool_utilization_ratio(0, 0), 0.0);
        assert_eq!(spool_utilization_ratio(0, 100), 0.0);
        assert_eq!(spool_utilization_ratio(100, 100), 1.0);
        assert_eq!(spool_utilization_ratio(125, 100), 1.25);
    }
}
