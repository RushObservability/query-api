use crate::self_metrics::{MetricKind, SelfMetrics};
use crate::spool::IngestBuffer;
use crate::timestamps::TimestampExt;
use clickhouse::Client;
use futures_util::stream::{self, StreamExt};
use std::sync::Arc;
use std::time::Instant;

#[derive(clickhouse::Row, serde::Deserialize, serde::Serialize)]
struct CountRow {
    count: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct BytesRow {
    total: u64,
}

#[derive(clickhouse::Row, serde::Deserialize, serde::Serialize)]
struct F64Row {
    v: f64,
}

pub fn spawn_stats_engine(
    ch: Client,
    buffer: Arc<IngestBuffer>,
    self_metrics: Arc<SelfMetrics>,
    instance_id: String,
    counts_lease: Arc<crate::leader::EngineLease>,
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
        for tick in 0u64.. {
            interval.tick().await;
            let start = std::time::Instant::now();
            let run_counts = counts_due(tick, secs, counts_lease.is_leader());
            let ok = match collect_and_write(&ch, &buffer, &self_metrics, &instance_id, run_counts)
                .await
            {
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

/// The cluster-wide row counts scan the data tables and are the same on every
/// replica, so only the stats lease holder runs them, at most once a minute.
const COUNTS_EVERY_SECS: u64 = 60;

fn counts_due(tick: u64, interval_secs: u64, leader: bool) -> bool {
    leader && tick % COUNTS_EVERY_SECS.div_ceil(interval_secs) == 0
}

/// Last-hour row counts (spans, logs, gauge, sum, histogram) and gauge series.
/// Each query bounds the table's partition and primary-key time columns so it
/// reads only the last hour, not the whole day.
fn count_queries(from: &str, to: &str) -> [String; 6] {
    let range = |col: &str| {
        format!(
            "{col} >= parseDateTimeBestEffort('{from}') AND {col} <= parseDateTimeBestEffort('{to}')"
        )
    };
    let metrics = |table: &str, select: &str| {
        format!(
            "SELECT {select} as count FROM {table} WHERE {}",
            range("TimeUnix")
        )
    };
    [
        format!(
            "SELECT count() as count FROM spans WHERE {}",
            range("timestamp")
        ),
        format!(
            "SELECT count() as count FROM logs WHERE TimestampDate >= toDate(parseDateTimeBestEffort('{from}')) \
             AND {} AND {}",
            range("TimestampTime"),
            range("Timestamp")
        ),
        metrics("metrics_gauge", "count()"),
        metrics("metrics_sum", "count()"),
        metrics("metrics_histogram", "count()"),
        metrics("metrics_gauge", "uniq(MetricName, Attributes)"),
    ]
}

async fn collect_and_write(
    ch: &Client,
    buffer: &IngestBuffer,
    self_metrics: &SelfMetrics,
    instance_id: &str,
    run_counts: bool,
) -> anyhow::Result<()> {
    // Object-store buffers are shared by ingest replicas. Reconcile before
    // exporting gauges so a drain performed by another pod is reflected here.
    if let Err(e) = buffer.refresh_counts().await {
        tracing::warn!(error = %e, "stats engine: failed to refresh ingest buffer counters");
    }
    let now = time::OffsetDateTime::now_utc();
    let now_nanos = now.unix_nanos().unwrap_or(0);
    let one_hour_ago = (now - time::Duration::hours(1)).utc_seconds();
    let now_str = now.utc_seconds();

    // All of these queries are independent — run them concurrently so the tick's
    // wall time is the slowest query, not the sum of all of them (~14 round trips).
    let counts = async {
        if !run_counts {
            return None;
        }
        let [q_spans, q_logs, q_gauge, q_sum, q_hist, q_series] =
            count_queries(&one_hour_ago, &now_str);
        Some(tokio::join!(
            query_count(ch, &q_spans, self_metrics, "spans"),
            query_count(ch, &q_logs, self_metrics, "logs"),
            query_count(ch, &q_gauge, self_metrics, "metric_gauge"),
            query_count(ch, &q_sum, self_metrics, "metric_sum"),
            query_count(ch, &q_hist, self_metrics, "metric_histogram"),
            query_count(ch, &q_series, self_metrics, "unique_series"),
        ))
    };
    let (
        counts,
        span_bytes,
        storage_bytes,
        storage_rows,
        storage_local_bytes,
        storage_object_store_bytes,
        disk_local_free_bytes,
        disk_local_total_bytes,
        buf_oldest,
    ) = tokio::join!(
        counts,
        query_bytes(
            ch,
            "SELECT sum(bytes_on_disk) as total FROM system.parts WHERE database = 'observability' AND table = 'spans' AND active",
            self_metrics,
            "span_bytes"
        ),
        query_bytes(
            ch,
            "SELECT sum(bytes_on_disk) as total FROM system.parts WHERE database = 'observability' AND active",
            self_metrics,
            "storage_bytes"
        ),
        query_count(
            ch,
            "SELECT sum(rows) as count FROM system.parts WHERE database = 'observability' AND active",
            self_metrics,
            "storage_rows"
        ),
        // Tiered storage breakdown: data bytes on local disk vs object store.
        // Classified by joining each part's disk to system.disks.type, matching
        // the on-demand /stats endpoint. Object store = any non-Local disk.
        query_bytes(
            ch,
            "SELECT sum(p.bytes_on_disk) as total FROM system.parts p \
             LEFT JOIN system.disks d ON p.disk_name = d.name \
             WHERE p.database = 'observability' AND p.active AND d.type = 'Local'",
            self_metrics,
            "storage_local_bytes"
        ),
        query_bytes(
            ch,
            "SELECT sum(p.bytes_on_disk) as total FROM system.parts p \
             LEFT JOIN system.disks d ON p.disk_name = d.name \
             WHERE p.database = 'observability' AND p.active AND d.type != 'Local'",
            self_metrics,
            "storage_object_store_bytes"
        ),
        // Local disk capacity (headroom) from system.disks.
        query_bytes(
            ch,
            "SELECT sum(free_space) as total FROM system.disks WHERE type = 'Local'",
            self_metrics,
            "disk_local_free_bytes"
        ),
        query_bytes(
            ch,
            "SELECT sum(total_space) as total FROM system.disks WHERE type = 'Local'",
            self_metrics,
            "disk_local_total_bytes"
        ),
        // Ingest buffer (durable spool) replay lag.
        buffer.oldest_age_secs(),
    );
    // Skipped counts stay None, so their rows are left out of the INSERT below
    // rather than written as zeros.
    let (span_total, log_total, metric_gauge, metric_sum, metric_hist, unique_series) =
        counts.unwrap_or_default();
    let metric_total = metric_gauge
        .zip(metric_sum)
        .zip(metric_hist)
        .map(|((a, b), c)| a + b + c);
    let counts_ok = !run_counts
        || [
            span_total,
            log_total,
            metric_gauge,
            metric_sum,
            metric_hist,
            unique_series,
        ]
        .iter()
        .all(Option::is_some);
    let mut healthy = counts_ok
        && [
            span_bytes,
            storage_bytes,
            storage_rows,
            storage_local_bytes,
            storage_object_store_bytes,
            disk_local_free_bytes,
            disk_local_total_bytes,
        ]
        .iter()
        .all(Option::is_some);
    let buf_oldest = buf_oldest.unwrap_or(0);

    // ── Write all metrics ──
    let metrics: Vec<(&str, f64)> = vec![
        (
            "rush_stats_ingest_buffer_pending_bytes",
            Some(buffer.total_bytes() as f64),
        ),
        (
            "rush_stats_ingest_buffer_pending_count",
            Some(buffer.segment_count() as f64),
        ),
        (
            "rush_stats_ingest_buffer_oldest_age_secs",
            Some(buf_oldest as f64),
        ),
        // Cumulative counter — drain rate = rate(rush_stats_ingest_buffer_committed_total).
        (
            "rush_stats_ingest_buffer_committed_total",
            Some(buffer.committed_total() as f64),
        ),
        ("rush_stats_span_events_total", span_total.map(|v| v as f64)),
        ("rush_stats_span_events_bytes", span_bytes.map(|v| v as f64)),
        ("rush_stats_logs_total", log_total.map(|v| v as f64)),
        ("rush_stats_metrics_total", metric_total.map(|v| v as f64)),
        ("rush_stats_unique_series", unique_series.map(|v| v as f64)),
        ("rush_stats_storage_bytes", storage_bytes.map(|v| v as f64)),
        ("rush_stats_storage_rows", storage_rows.map(|v| v as f64)),
        // Tiered storage: where the data physically lives.
        (
            "rush_stats_storage_local_bytes",
            storage_local_bytes.map(|v| v as f64),
        ),
        (
            "rush_stats_storage_object_store_bytes",
            storage_object_store_bytes.map(|v| v as f64),
        ),
        // Local disk capacity, for headroom / move-pressure monitoring.
        (
            "rush_stats_disk_local_free_bytes",
            disk_local_free_bytes.map(|v| v as f64),
        ),
        (
            "rush_stats_disk_local_total_bytes",
            disk_local_total_bytes.map(|v| v as f64),
        ),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|value| (name, value)))
    .collect();

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

    healthy &= write_self_metrics(ch, self_metrics, &sql, "stats", "metrics_gauge").await;

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
    if let Some(value) = disk_local_free_bytes {
        self_metrics.set_gauge("rush_stats_disk_local_free_bytes", &[], value as f64);
    }
    if let Some(value) = disk_local_total_bytes {
        self_metrics.set_gauge("rush_stats_disk_local_total_bytes", &[], value as f64);
    }
    if let Some(value) = storage_bytes {
        self_metrics.set_gauge("rush_stats_storage_bytes", &[], value as f64);
    }
    if let Some(value) = storage_rows {
        self_metrics.set_gauge("rush_stats_storage_rows", &[], value as f64);
    }

    // ── ClickHouse health gauges (group D) into both SelfMetrics and metrics_gauge ──
    collect_ch_health(ch, self_metrics).await;

    // ── Self-ingest: write the SelfMetrics snapshot into our own metrics tables so the
    // same rush_http_*/rush_ingest_*/rush_engine_*/rush_ch_* series are queryable via
    // the in-product PromQL API (/prom/api/v1/*). ──
    healthy &= self_ingest_snapshot(ch, self_metrics, now_nanos, instance_id).await;
    anyhow::ensure!(
        healthy,
        "stats collection or self-metrics persistence was degraded"
    );
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
        .with_setting("query_id", query_id)
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
    let labels = [("probe", probe)];
    self_metrics.set_gauge(
        "rush_ch_health_probe_valid",
        &labels,
        if ok { 1.0 } else { 0.0 },
    );
    if ok {
        self_metrics.set_gauge(
            "rush_ch_health_probe_last_success_timestamp_seconds",
            &labels,
            time::OffsetDateTime::now_utc().unix_timestamp() as f64,
        );
    }
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
/// rush_stats_* gauges use. Both table writes are attempted; a failed insert degrades the tick.
async fn self_ingest_snapshot(
    ch: &Client,
    self_metrics: &SelfMetrics,
    now_nanos: i64,
    instance_id: &str,
) -> bool {
    let mut healthy = true;
    let points = self_metrics.snapshot_series();
    if points.is_empty() {
        return true;
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
        healthy &= write_self_metrics(ch, self_metrics, &sql, "snapshot", "metrics_gauge").await;
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
        healthy &= write_self_metrics(ch, self_metrics, &sql, "snapshot", "metrics_sum").await;
    }
    healthy
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
    crate::query_builder::escape_string_literal(s)
}

async fn write_self_metrics(
    ch: &Client,
    metrics: &SelfMetrics,
    sql: &str,
    stage: &'static str,
    table: &'static str,
) -> bool {
    let started = Instant::now();
    let result = ch.query(sql).execute().await;
    let outcome = if result.is_ok() { "ok" } else { "error" };
    let labels = [("stage", stage), ("table", table), ("outcome", outcome)];
    metrics.inc_counter("rush_self_metrics_writes_total", &labels, 1);
    metrics.observe_histogram(
        "rush_self_metrics_write_duration_ms",
        &[("stage", stage), ("table", table)],
        started.elapsed().as_secs_f64() * 1000.0,
    );
    if let Err(error) = result {
        tracing::warn!(%error, stage, table, "stats engine: metrics persistence failed");
        false
    } else {
        metrics.set_gauge(
            "rush_self_metrics_last_success_timestamp_seconds",
            &[("stage", stage), ("table", table)],
            time::OffsetDateTime::now_utc().unix_timestamp() as f64,
        );
        true
    }
}

async fn query_count(
    ch: &Client,
    sql: &str,
    metrics: &SelfMetrics,
    probe: &'static str,
) -> Option<u64> {
    let started = Instant::now();
    let value = ch
        .query(sql)
        .fetch_one::<CountRow>()
        .await
        .ok()
        .map(|r| r.count);
    record_ch_probe(metrics, probe, started, value.is_some());
    value
}

async fn query_bytes(
    ch: &Client,
    sql: &str,
    metrics: &SelfMetrics,
    probe: &'static str,
) -> Option<u64> {
    let started = Instant::now();
    let value = ch
        .query(sql)
        .fetch_one::<BytesRow>()
        .await
        .ok()
        .map(|r| r.total);
    record_ch_probe(metrics, probe, started, value.is_some());
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_probe_is_invalid_not_zero_and_preserves_last_valid_health_value() {
        use clickhouse::test::{Mock, handlers};
        let mock = Mock::new();
        let ch = Client::default()
            .with_url(mock.url())
            .with_validation(false);
        let metrics = SelfMetrics::new();
        mock.add(handlers::failure(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        ));
        assert_eq!(
            query_count(&ch, "SELECT count() AS count", &metrics, "logs").await,
            None
        );
        assert!(
            metrics
                .render_prometheus()
                .contains("rush_ch_health_probe_valid{probe=\"logs\"} 0")
        );
        mock.add(handlers::provide([F64Row { v: 42.0 }]));
        run_ch_probe(&ch, &metrics, "rush_ch_active_queries", "SELECT 42.0 AS v").await;
        mock.add(handlers::failure(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        ));
        run_ch_probe(&ch, &metrics, "rush_ch_active_queries", "SELECT 42.0 AS v").await;
        let text = metrics.render_prometheus();
        assert!(text.contains("rush_ch_active_queries 42"));
        assert!(text.contains("rush_ch_health_probe_valid{probe=\"rush_ch_active_queries\"} 0"));
        assert!(text.contains(
            "rush_ch_health_probe_last_success_timestamp_seconds{probe=\"rush_ch_active_queries\"}"
        ));
    }

    #[tokio::test]
    async fn snapshot_reports_partial_persistence_failure_and_still_attempts_other_table() {
        use clickhouse::test::{Mock, handlers};
        let mock = Mock::new();
        let ch = Client::default()
            .with_url(mock.url())
            .with_validation(false);
        let metrics = SelfMetrics::new();
        metrics.set_gauge("test_gauge", &[], 1.0);
        metrics.inc_counter("test_counter", &[], 1);
        mock.add(handlers::failure(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        ));
        let sum_write = mock.add(handlers::record_ddl());
        assert!(!self_ingest_snapshot(&ch, &metrics, 1, "test-replica").await);
        assert!(sum_write.query().await.contains("INSERT INTO metrics_sum"));
        let text = metrics.render_prometheus();
        assert!(text.contains("rush_self_metrics_writes_total{outcome=\"error\",stage=\"snapshot\",table=\"metrics_gauge\"} 1"));
        assert!(text.contains("rush_self_metrics_writes_total{outcome=\"ok\",stage=\"snapshot\",table=\"metrics_sum\"} 1"));
        assert!(!text.contains(
            "last_success_timestamp_seconds{stage=\"snapshot\",table=\"metrics_gauge\"}"
        ));
    }

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
        assert_eq!(escape_sql("a'b"), "a''b");
        assert_eq!(escape_sql("a\\b"), "a\\\\b");
        assert_eq!(escape_sql("plain"), "plain");
        // `?` would be taken as a clickhouse-crate bind placeholder.
        assert_eq!(escape_sql("a?b"), "a\\x3Fb");
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
    fn cluster_counts_run_only_on_the_leader_once_a_minute() {
        assert!(!counts_due(0, 15, false));
        assert!(!counts_due(4, 15, false));
        let leader_ticks: Vec<u64> = (0..9).filter(|&t| counts_due(t, 15, true)).collect();
        assert_eq!(leader_ticks, vec![0, 4, 8]);
        // A slower stats interval never runs the counts more often than it ticks.
        assert!((0..3).all(|t| counts_due(t, 120, true)));
    }

    #[test]
    fn count_queries_bound_the_primary_key_time_columns() {
        let [spans, logs, gauge, sum, hist, series] =
            count_queries("2026-01-02 03:00:00", "2026-01-02 04:00:00");
        assert!(spans.contains("FROM spans WHERE timestamp >= "));
        // logs is partitioned by TimestampDate and keyed by TimestampTime;
        // filtering only on Timestamp read the whole day.
        assert!(
            logs.contains(
                "TimestampDate >= toDate(parseDateTimeBestEffort('2026-01-02 03:00:00'))"
            )
        );
        assert!(logs.contains("TimestampTime >= parseDateTimeBestEffort('2026-01-02 03:00:00')"));
        assert!(logs.contains("TimestampTime <= parseDateTimeBestEffort('2026-01-02 04:00:00')"));
        assert!(logs.contains("Timestamp <= parseDateTimeBestEffort('2026-01-02 04:00:00')"));
        for (sql, table) in [
            (&gauge, "metrics_gauge"),
            (&sum, "metrics_sum"),
            (&hist, "metrics_histogram"),
            (&series, "metrics_gauge"),
        ] {
            assert!(sql.contains(&format!("FROM {table} WHERE TimeUnix >= ")));
            assert!(sql.contains("TimeUnix <= parseDateTimeBestEffort('2026-01-02 04:00:00')"));
        }
        assert!(series.starts_with("SELECT uniq(MetricName, Attributes) as count"));
        assert!(!format!("{spans}{logs}{gauge}{sum}{hist}{series}").contains('?'));
    }

    #[test]
    fn spool_utilization_ratio_handles_empty_and_full_buffers() {
        assert_eq!(spool_utilization_ratio(0, 0), 0.0);
        assert_eq!(spool_utilization_ratio(0, 100), 0.0);
        assert_eq!(spool_utilization_ratio(100, 100), 1.0);
        assert_eq!(spool_utilization_ratio(125, 100), 1.25);
    }
}
