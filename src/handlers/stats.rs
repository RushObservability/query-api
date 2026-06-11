use axum::{Json, extract::State, http::StatusCode, response::IntoResponse, Extension};
use clickhouse::Row;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, Instant};

use crate::AppState;
use crate::TenantContext;

/// Whether any object-storage (S3/MinIO) disk is configured. Disk topology only
/// changes with a ClickHouse config reload + restart, so probe once per process.
static OBJECT_STORE_ENABLED: OnceLock<bool> = OnceLock::new();

/// Short-TTL response cache: stats are dashboard eye-candy, recomputing 12+
/// aggregate scans per tenant per refresh is wasted I/O. Keyed by
/// (tenant_id + requested range) so explicit time ranges never cross-contaminate.
static STATS_CACHE: LazyLock<DashMap<String, (serde_json::Value, Instant)>> =
    LazyLock::new(DashMap::new);
const STATS_CACHE_TTL: Duration = Duration::from_secs(15);
const STATS_CACHE_MAX: usize = 10_000;

#[derive(Debug, Deserialize)]
pub struct StatsRequest {
    pub time_range: Option<TimeRange>,
}

#[derive(Debug, Deserialize)]
pub struct TimeRange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Serialize)]
pub struct UsageSignalStats {
    pub events_count: u64,
    pub bytes_count: u64,
}

#[derive(Debug, Serialize)]
pub struct StatsUsage {
    pub traces: UsageSignalStats,
    pub logs: UsageSignalStats,
    pub metrics: UsageSignalStats,
    pub rum: UsageSignalStats,
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub spans: SignalStats,
    pub logs: SignalStats,
    pub metrics: MetricStats,
    pub storage: Vec<TableStorage>,
    // True when an object-storage (S3/MinIO) disk is configured, so the UI can
    // distinguish "tiering off" from "tiering on but nothing moved to cold yet".
    pub object_store_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<StatsUsage>,
}

#[derive(Debug, Serialize)]
pub struct SignalStats {
    pub total_events: u64,
    pub events_per_sec: f64,
    pub events_today: u64,
}

#[derive(Debug, Serialize)]
pub struct MetricStats {
    pub total_datapoints: u64,
    pub datapoints_per_sec: f64,
    pub datapoints_today: u64,
    pub unique_series: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct TableStorage {
    #[serde(rename = "table_name")]
    pub table_name: String,
    #[serde(rename = "total_rows")]
    pub total_rows: u64,
    #[serde(rename = "bytes_on_disk")]
    pub bytes_on_disk: u64,
    #[serde(rename = "compressed_bytes")]
    pub compressed_bytes: u64,
    #[serde(rename = "uncompressed_bytes")]
    pub uncompressed_bytes: u64,
    // Tiered-storage breakdown: bytes on local disk vs object store (S3/MinIO).
    // Classified by joining system.parts.disk_name → system.disks.type, so it
    // works regardless of disk names and reports 0 for object store when tiering
    // isn't configured.
    #[serde(rename = "bytes_local")]
    pub bytes_local: u64,
    #[serde(rename = "bytes_object_store")]
    pub bytes_object_store: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct CountResult {
    count: u64,
}

/// Combined total + rate in a single query to save a round-trip per signal.
#[derive(Debug, Clone, Deserialize, Row)]
struct TotalRateResult {
    total: u64,
    rate: f64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct UsageRow {
    signal: String,
    events: u64,
    bytes: u64,
}

/// Bucket a timestamp string to 15-second granularity for use in cache keys, so
/// rolling explicit ranges ("last hour", recomputed from now() every refresh)
/// map to the same key within the cache TTL. Unparsable strings pass through
/// unchanged (the key just won't benefit from bucketing).
fn bucket_ts_15s(ts: &str) -> String {
    match chrono::DateTime::parse_from_rfc3339(ts) {
        Ok(dt) => {
            let secs = dt.timestamp();
            (secs - secs.rem_euclid(15)).to_string()
        }
        Err(_) => ts.to_string(),
    }
}

/// Build the `total`/`rate` count SQL for a metric table, automatically reading from a
/// rollup when the window is coarse AND its edges align to the rollup bucket interval.
///
/// Why the alignment guard: `countMerge(cnt_state)` over a rollup sums whole buckets, so
/// it equals the raw `count()` over `[from, to]` ONLY when `from`/`to` fall on bucket
/// boundaries (otherwise an edge bucket would be counted whole instead of partially).
/// When the window is short or unaligned we fall back to raw — the displayed count is
/// never approximated. Tenant scoping (`tenant_id = '<escaped>'`) is applied on every
/// branch, identical to the raw query it replaces.
///
/// `raw_table` is e.g. "metrics_gauge"; the rollup tables are "<raw_table>_1m/_1h".
fn metric_count_source(
    raw_table: &str,
    from: &str,
    to: &str,
    escaped_tenant: &str,
    range_secs: &str,
) -> String {
    // Raw query (default / fallback). Half-open upper bound `< to`: this is what makes a
    // rollup-backed read (which sums whole `[b, b+interval)` buckets up to but excluding
    // the bucket at `to`) able to equal the raw count EXACTLY on an aligned window. The
    // window is thus `[from, to)`. Versus the previous `<= to` this drops only samples
    // whose TimeUnix is exactly `to` (a round boundary instant) — empirically none for
    // metric data — so the displayed count is unchanged in practice while becoming
    // rollup-substitutable. The raw and rollup branches return identical numbers.
    let raw = format!(
        "SELECT count() as total, count() / {range_secs} as rate FROM {raw_table} \
         PREWHERE tenant_id = '{escaped_tenant}' \
           AND TimeUnix >= parseDateTimeBestEffort('{from}') \
           AND TimeUnix < parseDateTimeBestEffort('{to}')"
    );

    // Parse window edges; bail to raw on any parse failure.
    let (Ok(from_dt), Ok(to_dt)) = (
        chrono::DateTime::parse_from_rfc3339(from),
        chrono::DateTime::parse_from_rfc3339(to),
    ) else {
        return raw;
    };
    let from_s = from_dt.timestamp() as f64;
    let to_s = to_dt.timestamp() as f64;

    let source = crate::rollup::select_window_source(from_s, to_s);
    let interval = match source.interval_secs() {
        Some(i) => i,
        None => return raw, // Source::Raw
    };

    // Alignment guard: both edges must sit on a rollup bucket boundary so whole-bucket
    // counts equal the raw windowed count exactly.
    let from_i = from_dt.timestamp();
    let to_i = to_dt.timestamp();
    if from_i % interval != 0 || to_i % interval != 0 {
        return raw;
    }

    // Rollup count: countMerge over the buckets that tile [from, to). Each rollup bucket
    // `b` covers [b, b+interval); with `from`/`to` both on interval boundaries, the
    // half-open predicate `bucket >= from AND bucket < to` selects exactly the buckets
    // whose [b, b+interval) lies inside [from, to). That equals the raw count over
    // [from, to]: the only difference is samples whose TimeUnix is exactly `to` (which
    // would fall in the bucket starting at `to`, excluded here) — empirically none, since
    // metric TimeUnix values never land exactly on a round window boundary. The
    // alignment + half-open bound make whole-bucket sums match the raw windowed count.
    let rollup_table = format!("{raw_table}{}", source.suffix());
    format!(
        "SELECT toUInt64(countMerge(cnt_state)) as total, \
         countMerge(cnt_state) / {range_secs} as rate FROM {rollup_table} \
         PREWHERE tenant_id = '{escaped_tenant}' \
           AND bucket >= parseDateTimeBestEffort('{from}') \
           AND bucket < parseDateTimeBestEffort('{to}')"
    )
}

pub async fn get_stats(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<StatsRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);

    // 15s response cache. The default (no explicit range) path uses a stable key so
    // consecutive dashboard refreshes hit the cache even though from/to are
    // recomputed from `now()` on every call. Explicit ranges are bucketed to 15s
    // granularity in the key so rolling UI ranges (recomputed each refresh) can
    // still hit within the TTL.
    let cache_key = match &req.time_range {
        Some(tr) => format!("{tenant_id}|{}|{}", bucket_ts_15s(&tr.from), bucket_ts_15s(&tr.to)),
        None => format!("{tenant_id}|default"),
    };
    if let Some(entry) = STATS_CACHE.get(&cache_key) {
        if entry.1.elapsed() < STATS_CACHE_TTL {
            return Ok(Json(entry.0.clone()));
        }
    }

    let (from, to) = if let Some(tr) = &req.time_range {
        (tr.from.clone(), tr.to.clone())
    } else {
        let to = chrono::Utc::now();
        let from = to - chrono::Duration::hours(1);
        (from.to_rfc3339(), to.to_rfc3339())
    };

    let today_start = chrono::Utc::now().date_naive().to_string();
    let range_secs = format!(
        "greatest(1, dateDiff('second', parseDateTimeBestEffort('{from}'), parseDateTimeBestEffort('{to}')))"
    );

    // ── Build all query futures ──
    // Combined total + rate in one query per signal to halve span/log round-trips.
    let span_stats_fut = crate::tenant_query(&state.ch, &format!(
        "SELECT count() as total, count() / {range_secs} as rate \
         FROM spans_raw \
         PREWHERE tenant_id = '{escaped_tenant}' \
           AND Timestamp >= parseDateTimeBestEffort('{from}') \
           AND Timestamp <= parseDateTimeBestEffort('{to}')"
    ), tenant_id).fetch_one::<TotalRateResult>();

    let span_today_fut = crate::tenant_query(&state.ch, &format!(
        "SELECT count() as count FROM spans_raw \
         PREWHERE tenant_id = '{escaped_tenant}' AND toDate(Timestamp) = '{today_start}'"
    ), tenant_id).fetch_one::<CountResult>();

    let log_stats_fut = crate::tenant_query(&state.ch, &format!(
        "SELECT count() as total, count() / {range_secs} as rate \
         FROM logs \
         PREWHERE tenant_id = '{escaped_tenant}' \
           AND Timestamp >= parseDateTimeBestEffort('{from}') \
           AND Timestamp <= parseDateTimeBestEffort('{to}')"
    ), tenant_id).fetch_one::<TotalRateResult>();

    let log_today_fut = crate::tenant_query(&state.ch, &format!(
        "SELECT count() as count FROM logs \
         PREWHERE tenant_id = '{escaped_tenant}' AND toDate(Timestamp) = '{today_start}'"
    ), tenant_id).fetch_one::<CountResult>();

    // Combined total + rate per metrics table (was two scans of the same window each).
    //
    // Auto coarse-window source selection: for a long window the per-sample raw scan is
    // replaced by a `countMerge(cnt_state)` over the pre-aggregated rollup, which is
    // EXACTLY the raw count when the window edges align to the rollup interval (verified
    // on live CH). `metric_count_source` returns raw whenever the window is short OR not
    // bucket-aligned, so the count is never approximated. Tenant scoping is preserved on
    // every branch.
    let mg_count = metric_count_source("metrics_gauge", &from, &to, &escaped_tenant, &range_secs);
    let ms_count = metric_count_source("metrics_sum", &from, &to, &escaped_tenant, &range_secs);
    let mg_stats_fut = crate::tenant_query(&state.ch, &mg_count, tenant_id).fetch_one::<TotalRateResult>();
    let ms_stats_fut = crate::tenant_query(&state.ch, &ms_count, tenant_id).fetch_one::<TotalRateResult>();

    let mh_total_fut = crate::tenant_query(&state.ch, &format!(
        "SELECT count() as count FROM metrics_histogram \
         PREWHERE tenant_id = '{escaped_tenant}' \
           AND TimeUnix >= parseDateTimeBestEffort('{from}') \
           AND TimeUnix <= parseDateTimeBestEffort('{to}')"
    ), tenant_id).fetch_one::<CountResult>();

    let mg_today_fut = crate::tenant_query(&state.ch, &format!(
        "SELECT count() as count FROM metrics_gauge \
         PREWHERE tenant_id = '{escaped_tenant}' AND toDate(TimeUnix) = '{today_start}'"
    ), tenant_id).fetch_one::<CountResult>();

    let ms_today_fut = crate::tenant_query(&state.ch, &format!(
        "SELECT count() as count FROM metrics_sum \
         PREWHERE tenant_id = '{escaped_tenant}' AND toDate(TimeUnix) = '{today_start}'"
    ), tenant_id).fetch_one::<CountResult>();

    let unique_series_fut = crate::tenant_query(&state.ch, &format!(
        "SELECT uniq(MetricName, Attributes) as count FROM metrics_gauge \
         PREWHERE tenant_id = '{escaped_tenant}' AND TimeUnix >= now() - INTERVAL 1 HOUR"
    ), tenant_id).fetch_one::<CountResult>();

    let storage_fut = state.ch.query(
        "SELECT \
             p.table as table_name, \
             sum(p.rows) as total_rows, \
             sum(p.bytes_on_disk) as bytes_on_disk, \
             sum(p.data_compressed_bytes) as compressed_bytes, \
             sum(p.data_uncompressed_bytes) as uncompressed_bytes, \
             sumIf(p.bytes_on_disk, d.type = 'Local') as bytes_local, \
             sumIf(p.bytes_on_disk, d.type != 'Local') as bytes_object_store \
         FROM system.parts AS p \
         LEFT JOIN system.disks AS d ON p.disk_name = d.name \
         WHERE p.database = 'observability' AND p.active \
         GROUP BY table_name \
         ORDER BY bytes_on_disk DESC"
    ).fetch_all::<TableStorage>();

    let usage_fut = state.ch.query(&format!(
        "SELECT signal, sum(events_count) AS events, sum(bytes_count) AS bytes \
         FROM observability.tenant_usage \
         WHERE tenant_id = '{escaped_tenant}' AND bucket >= toStartOfDay(now()) \
         GROUP BY signal"
    )).fetch_all::<UsageRow>();

    // ── Fire all 12 queries concurrently ──
    let (
        span_stats_res, span_today_res,
        log_stats_res, log_today_res,
        mg_stats_res, ms_stats_res, mh_total_res,
        mg_today_res, ms_today_res,
        unique_series_res,
        storage_res, usage_rows_res,
    ) = tokio::join!(
        span_stats_fut, span_today_fut,
        log_stats_fut, log_today_fut,
        mg_stats_fut, ms_stats_fut, mh_total_fut,
        mg_today_fut, ms_today_fut,
        unique_series_fut,
        storage_fut, usage_fut,
    );

    // ── Unpack results (failures fall back to zero rather than failing the whole request) ──
    let span_stats = span_stats_res.unwrap_or(TotalRateResult { total: 0, rate: 0.0 });
    let span_today = span_today_res.map(|r| r.count).unwrap_or(0);
    let log_stats = log_stats_res.unwrap_or(TotalRateResult { total: 0, rate: 0.0 });
    let log_today = log_today_res.map(|r| r.count).unwrap_or(0);
    let mg_stats = mg_stats_res.unwrap_or(TotalRateResult { total: 0, rate: 0.0 });
    let ms_stats = ms_stats_res.unwrap_or(TotalRateResult { total: 0, rate: 0.0 });
    let metric_total = mg_stats.total
        + ms_stats.total
        + mh_total_res.map(|r| r.count).unwrap_or(0);
    let metric_rate = mg_stats.rate + ms_stats.rate;
    let metric_today = mg_today_res.map(|r| r.count).unwrap_or(0)
        + ms_today_res.map(|r| r.count).unwrap_or(0);
    let unique_series = unique_series_res.map(|r| r.count).unwrap_or(0);
    let storage: Vec<TableStorage> = storage_res.unwrap_or_default();
    let usage_rows: Vec<UsageRow> = usage_rows_res.unwrap_or_default();

    let mut usage_traces = UsageSignalStats { events_count: 0, bytes_count: 0 };
    let mut usage_logs = UsageSignalStats { events_count: 0, bytes_count: 0 };
    let mut usage_metrics = UsageSignalStats { events_count: 0, bytes_count: 0 };
    let mut usage_rum = UsageSignalStats { events_count: 0, bytes_count: 0 };

    for row in &usage_rows {
        match row.signal.as_str() {
            "traces" => { usage_traces.events_count = row.events; usage_traces.bytes_count = row.bytes; }
            "logs" => { usage_logs.events_count = row.events; usage_logs.bytes_count = row.bytes; }
            "metrics" => { usage_metrics.events_count = row.events; usage_metrics.bytes_count = row.bytes; }
            "rum" => { usage_rum.events_count = row.events; usage_rum.bytes_count = row.bytes; }
            _ => {}
        }
    }

    // Is any object-storage (S3/MinIO) disk configured? Distinguishes "tiering
    // off" from "on but nothing moved to cold yet" (when object-store bytes = 0).
    // Disk topology requires a server restart to change, so resolve once per process.
    // (A racing first call may probe twice; OnceLock keeps the first answer.)
    let object_store_enabled = match OBJECT_STORE_ENABLED.get() {
        Some(v) => *v,
        None => {
            let probed = state.ch.query(
                "SELECT count() AS count FROM system.disks WHERE type != 'Local'"
            ).fetch_one::<CountResult>().await.map(|r| r.count > 0).unwrap_or(false);
            *OBJECT_STORE_ENABLED.get_or_init(|| probed)
        }
    };

    let stats_usage = if usage_rows.is_empty() {
        None
    } else {
        Some(StatsUsage {
            traces: usage_traces,
            logs: usage_logs,
            metrics: usage_metrics,
            rum: usage_rum,
        })
    };

    let response = StatsResponse {
        spans: SignalStats {
            total_events: span_stats.total,
            events_per_sec: span_stats.rate,
            events_today: span_today,
        },
        logs: SignalStats {
            total_events: log_stats.total,
            events_per_sec: log_stats.rate,
            events_today: log_today,
        },
        metrics: MetricStats {
            total_datapoints: metric_total,
            datapoints_per_sec: metric_rate,
            datapoints_today: metric_today,
            unique_series,
        },
        storage,
        object_store_enabled,
        usage: stats_usage,
    };

    // Cache the serialized response (same JSON the client receives).
    let value = serde_json::to_value(&response)
        .map_err(|e| {
            tracing::error!(error = %e, handler = "get_stats", "response serialization failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "serialization failed".into())
        })?;
    if STATS_CACHE.len() > STATS_CACHE_MAX {
        // Evict only expired entries first — clear() would also wipe hot entries.
        STATS_CACHE.retain(|_, v| v.1.elapsed() < STATS_CACHE_TTL);
        if STATS_CACHE.len() > STATS_CACHE_MAX {
            STATS_CACHE.clear(); // backstop: still over cap after pruning
        }
    }
    STATS_CACHE.insert(cache_key, (value.clone(), Instant::now()));
    Ok(Json(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_ts_15s_buckets_rolling_ranges_together() {
        // Timestamps within the same 15s bucket map to the same key fragment.
        let a = bucket_ts_15s("2026-06-10T12:00:01Z");
        let b = bucket_ts_15s("2026-06-10T12:00:14Z");
        assert_eq!(a, b);
        // …and a different bucket maps elsewhere.
        let c = bucket_ts_15s("2026-06-10T12:00:16Z");
        assert_ne!(a, c);
    }

    #[test]
    fn bucket_ts_15s_handles_offsets_and_garbage() {
        // Same instant in a non-UTC offset buckets identically.
        assert_eq!(
            bucket_ts_15s("2026-06-10T12:00:05Z"),
            bucket_ts_15s("2026-06-10T14:00:05+02:00"),
        );
        // Unparsable input passes through unchanged (key still tenant-scoped).
        assert_eq!(bucket_ts_15s("not-a-date"), "not-a-date");
    }

    // ── metric_count_source: auto rollup selection for stats counts ──

    const T: &str = "default"; // escaped tenant
    const R: &str = "1"; // range_secs (irrelevant to source choice)

    #[test]
    fn metric_count_short_window_uses_raw() {
        // 1-hour window → raw (always). Tenant predicate present.
        let sql = metric_count_source(
            "metrics_gauge",
            "2026-06-10T00:00:00Z",
            "2026-06-10T01:00:00Z",
            T,
            R,
        );
        assert!(sql.contains("FROM metrics_gauge "), "{sql}");
        assert!(sql.contains("tenant_id = 'default'"), "{sql}");
        assert!(sql.contains("TimeUnix <"), "{sql}");
    }

    #[test]
    fn metric_count_long_aligned_window_uses_hour_rollup() {
        // 7-day window, both edges on hour boundaries → 1h rollup countMerge.
        let sql = metric_count_source(
            "metrics_sum",
            "2026-06-03T00:00:00Z",
            "2026-06-10T00:00:00Z",
            T,
            R,
        );
        assert!(sql.contains("FROM metrics_sum_1h "), "{sql}");
        assert!(sql.contains("countMerge(cnt_state)"), "{sql}");
        assert!(sql.contains("tenant_id = 'default'"), "{sql}");
        assert!(sql.contains("bucket <"), "{sql}");
    }

    #[test]
    fn metric_count_medium_aligned_window_uses_minute_rollup() {
        // 1-day window, minute-aligned → 1m rollup.
        let sql = metric_count_source(
            "metrics_gauge",
            "2026-06-09T00:00:00Z",
            "2026-06-10T00:00:00Z",
            T,
            R,
        );
        assert!(sql.contains("FROM metrics_gauge_1m "), "{sql}");
        assert!(sql.contains("countMerge(cnt_state)"), "{sql}");
    }

    #[test]
    fn metric_count_long_unaligned_window_falls_back_to_raw() {
        // 7-day span but `to` is NOT on an hour boundary (and not a clean minute either,
        // since 1h rollup needs hour alignment; here we make it minute-misaligned too).
        let sql = metric_count_source(
            "metrics_sum",
            "2026-06-03T00:00:30Z",
            "2026-06-10T00:00:30Z",
            T,
            R,
        );
        // 30s offset: not divisible by the 1h interval the coarse selector picks → raw.
        assert!(sql.contains("FROM metrics_sum "), "{sql}");
        assert!(!sql.contains("countMerge"), "{sql}");
    }

    #[test]
    fn metric_count_unparsable_dates_fall_back_to_raw() {
        let sql = metric_count_source("metrics_gauge", "bad", "also-bad", T, R);
        assert!(sql.contains("FROM metrics_gauge "), "{sql}");
        assert!(!sql.contains("countMerge"), "{sql}");
    }
}
