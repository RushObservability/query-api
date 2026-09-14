use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::AppState;
use crate::TenantContext;
use crate::handlers::usage_metering::resolve_metering_scope;
use crate::handlers::users::{require_admin, require_auth};
use crate::query_builder::escape_string_literal;

#[derive(Debug, Deserialize)]
pub struct UsageQuery {
    /// Filter by signal type: metric, span, log (optional)
    pub signal_type: Option<String>,
    /// Number of days to look back (default 30)
    pub days: Option<u32>,
    /// Limit results (default 100)
    pub limit: Option<u32>,
    /// When true, aggregate usage across all tenants. Admin only.
    pub global: Option<bool>,
    /// Inspect one tenant without changing the application tenant. Admin only
    /// when it differs from the active tenant.
    pub tenant_id: Option<String>,
}

#[derive(Debug, Deserialize, clickhouse::Row)]
struct UsageBaseRow {
    signal_name: String,
    signal_type: String,
    source: String,
    last_queried_at: String,
    query_count: u64,
}

#[derive(Debug, Deserialize, clickhouse::Row)]
struct UsageStatsRow {
    signal_name: String,
    signal_type: String,
    source: String,
    low_duration_ms: f64,
    avg_duration_ms: f64,
    high_duration_ms: f64,
    low_result_rows: f64,
    avg_result_rows: f64,
    high_result_rows: f64,
    query_samples: u64,
}

#[derive(Debug, Serialize)]
pub struct UsageRow {
    pub signal_name: String,
    pub signal_type: String,
    pub source: String,
    pub last_queried_at: String,
    pub query_count: u64,
    pub low_duration_ms: Option<f64>,
    pub avg_duration_ms: Option<f64>,
    pub high_duration_ms: Option<f64>,
    pub low_result_rows: Option<f64>,
    pub avg_result_rows: Option<f64>,
    pub high_result_rows: Option<f64>,
    pub query_samples: u64,
}

#[derive(Debug, Serialize)]
pub struct UsageResponse {
    pub usage: Vec<UsageRow>,
    pub total: u64,
    pub unused: Vec<UnusedMetric>,
    pub cardinality: Vec<CardinalityEntry>,
}

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct UnusedMetric {
    pub metric_name: String,
}

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct CardinalityEntry {
    pub metric_name: String,
    pub series_count: u64,
    pub label_count: u64,
}

/// Get signal usage data — which metrics/spans/logs are being queried.
pub async fn get_usage(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Query(params): Query<UsageQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let scope = resolve_metering_scope(
        &tenant.tenant_id,
        params.global,
        params.tenant_id.as_deref(),
    );
    if scope.requires_admin() {
        require_admin(&state, &headers).await?;
    } else {
        require_auth(&state, &headers).await?;
    }
    let tenant_id = scope.tenant_id();
    let escaped_tenant = escape_string_literal(tenant_id);
    let tenant_filter = if scope.is_global() {
        String::new()
    } else {
        format!("tenant_id = '{escaped_tenant}' AND ")
    };
    let days = params.days.unwrap_or(30);
    let limit = params.limit.unwrap_or(100).min(1000);

    let type_filter = match &params.signal_type {
        Some(t) => format!("AND signal_type = '{}'", escape_string_literal(t)),
        None => String::new(),
    };

    // signal_usage is ReplacingMergeTree(last_queried_at). Instead of FINAL (a full
    // merge-on-read across all parts on every request — materially pricier in 26.x),
    // collapse versions with GROUP BY + argMax on the version column: argMax keeps the
    // query_count from the latest row, max() yields its timestamp. Equivalent result,
    // no merge.
    let sql = if scope.is_global() {
        format!(
            "SELECT signal_name, signal_type, source, \
             toString(toUnixTimestamp64Milli(max(latest_at))) AS last_queried_at, \
             sum(latest_query_count) AS query_count \
             FROM ( \
                 SELECT tenant_id, signal_name, signal_type, source, \
                        max(last_queried_at) AS latest_at, \
                        argMax(query_count, last_queried_at) AS latest_query_count \
                 FROM signal_usage \
                 WHERE last_queried_at >= now() - INTERVAL {days} DAY {type_filter} \
                 GROUP BY tenant_id, signal_name, signal_type, source \
             ) \
             GROUP BY signal_name, signal_type, source \
             ORDER BY max(latest_at) DESC \
             LIMIT {limit}"
        )
    } else {
        format!(
            "SELECT signal_name, signal_type, source, \
             toString(toUnixTimestamp64Milli(max(last_queried_at))) as last_queried_at, \
             argMax(query_count, last_queried_at) as query_count \
             FROM signal_usage \
             WHERE {tenant_filter}last_queried_at >= now() - INTERVAL {days} DAY {type_filter} \
             GROUP BY signal_name, signal_type, source \
             ORDER BY max(last_queried_at) DESC \
             LIMIT {limit}"
        )
    };

    // Query tracking is supplementary to the usage page. A stale deployment
    // may not have the signal_usage table/schema yet, and a ClickHouse read
    // compatibility problem here must not hide the working ingest-metering
    // sections or turn the whole page into a 5xx. Keep the tenant-scoped query
    // and log the failure for operators, but return an empty list until the
    // tracking store is available again.
    let usage_query = if scope.requires_admin() {
        state.admin_ch.query(&sql)
    } else {
        crate::tenant_query(&state.ch, &sql, &tenant.tenant_id)
    };
    let usage_base = match usage_query.fetch_all::<UsageBaseRow>().await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, tenant = %tenant_id, "signal usage query unavailable");
            Vec::new()
        }
    };

    // Query samples are append-only and deduplicated by event_id before aggregation,
    // so a retried background flush cannot skew low/average/high values. Keep this
    // supplementary: older deployments without the table still return usage rows.
    let stats_sql = format!(
        "SELECT signal_name, signal_type, source, \
                toFloat64(min(duration_ms)) AS low_duration_ms, \
                avg(duration_ms) AS avg_duration_ms, \
                toFloat64(max(duration_ms)) AS high_duration_ms, \
                toFloat64(min(result_rows)) AS low_result_rows, \
                avg(result_rows) AS avg_result_rows, \
                toFloat64(max(result_rows)) AS high_result_rows, \
                count() AS query_samples \
         FROM ( \
             SELECT tenant_id, event_id, signal_name, signal_type, source, duration_ms, result_rows \
             FROM signal_query_stats \
             WHERE {tenant_filter}queried_at >= now() - INTERVAL {days} DAY {type_filter} \
             GROUP BY tenant_id, event_id, signal_name, signal_type, source, duration_ms, result_rows \
         ) \
         GROUP BY signal_name, signal_type, source"
    );
    let stats_query = if scope.requires_admin() {
        state.admin_ch.query(&stats_sql)
    } else {
        crate::tenant_query(&state.ch, &stats_sql, &tenant.tenant_id)
    };
    let stats_rows = stats_query
        .fetch_all::<UsageStatsRow>()
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(%error, tenant = %tenant_id, "signal query statistics unavailable");
            Vec::new()
        });
    let mut stats_by_signal: HashMap<(String, String, String), UsageStatsRow> = stats_rows
        .into_iter()
        .map(|row| {
            (
                (
                    row.signal_name.clone(),
                    row.signal_type.clone(),
                    row.source.clone(),
                ),
                row,
            )
        })
        .collect();
    let usage = usage_base
        .into_iter()
        .map(|row| {
            let stats = stats_by_signal.remove(&(
                row.signal_name.clone(),
                row.signal_type.clone(),
                row.source.clone(),
            ));
            UsageRow {
                signal_name: row.signal_name,
                signal_type: row.signal_type,
                source: row.source,
                last_queried_at: row.last_queried_at,
                query_count: row.query_count,
                low_duration_ms: stats.as_ref().map(|value| value.low_duration_ms),
                avg_duration_ms: stats.as_ref().map(|value| value.avg_duration_ms),
                high_duration_ms: stats.as_ref().map(|value| value.high_duration_ms),
                low_result_rows: stats.as_ref().map(|value| value.low_result_rows),
                avg_result_rows: stats.as_ref().map(|value| value.avg_result_rows),
                high_result_rows: stats.as_ref().map(|value| value.high_result_rows),
                query_samples: stats.map_or(0, |value| value.query_samples),
            }
        })
        .collect();

    // Count total tracked signals
    // Distinct tracked signals in window — uniqExact over the dedup key avoids FINAL.
    let count_sql = format!(
        "SELECT uniqExact(signal_name, signal_type, source) as count FROM signal_usage \
         WHERE {tenant_filter}last_queried_at >= now() - INTERVAL {days} DAY {type_filter}"
    );

    #[derive(serde::Deserialize, clickhouse::Row)]
    struct CountRow {
        count: u64,
    }

    let count_query = if scope.requires_admin() {
        state.admin_ch.query(&count_sql)
    } else {
        crate::tenant_query(&state.ch, &count_sql, &tenant.tenant_id)
    };
    let total = count_query
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0);

    // Find unused metrics (exist in metrics_ but not in signal_usage)
    let unused_sql = format!(
        "SELECT metric_name \
         FROM ( \
             SELECT DISTINCT MetricName as metric_name FROM metrics_gauge \
             WHERE {tenant_filter}TimeUnix >= now() - INTERVAL 1 DAY \
             UNION DISTINCT \
             SELECT DISTINCT MetricName as metric_name FROM metrics_sum \
             WHERE {tenant_filter}TimeUnix >= now() - INTERVAL 1 DAY \
         ) AS all_metrics \
         LEFT JOIN ( \
             SELECT DISTINCT signal_name FROM signal_usage \
             WHERE {tenant_filter}signal_type = 'metric' AND last_queried_at >= now() - INTERVAL {days} DAY \
         ) AS used ON all_metrics.metric_name = used.signal_name \
         WHERE used.signal_name IS NULL OR used.signal_name = '' \
         ORDER BY metric_name \
         LIMIT 200"
    );

    let unused_query = if scope.requires_admin() {
        state.admin_ch.query(&unused_sql)
    } else {
        crate::tenant_query(&state.ch, &unused_sql, &tenant.tenant_id)
    };
    let unused = unused_query
        .fetch_all::<UnusedMetric>()
        .await
        .unwrap_or_default();

    // Cardinality explorer — count unique series (label combos) per metric
    let cardinality_sql = format!(
        "SELECT metric_name, sum(series_count) as series_count, max(label_count) as label_count \
         FROM ( \
             SELECT MetricName as metric_name, \
                    uniq(tenant_id, ServiceName, Attributes) as series_count, \
                    max(length(mapKeys(Attributes))) as label_count \
             FROM metrics_gauge \
             WHERE {tenant_filter}TimeUnix >= now() - INTERVAL 1 HOUR \
             GROUP BY metric_name \
             UNION ALL \
             SELECT MetricName as metric_name, \
                    uniq(tenant_id, ServiceName, Attributes) as series_count, \
                    max(length(mapKeys(Attributes))) as label_count \
             FROM metrics_sum \
             WHERE {tenant_filter}TimeUnix >= now() - INTERVAL 1 HOUR \
             GROUP BY metric_name \
         ) \
         GROUP BY metric_name \
         ORDER BY series_count DESC \
         LIMIT 100"
    );

    let cardinality_query = if scope.requires_admin() {
        state.admin_ch.query(&cardinality_sql)
    } else {
        crate::tenant_query(&state.ch, &cardinality_sql, &tenant.tenant_id)
    };
    let cardinality = cardinality_query
        .fetch_all::<CardinalityEntry>()
        .await
        .unwrap_or_default();

    Ok(Json(UsageResponse {
        usage,
        total,
        unused,
        cardinality,
    }))
}

// ── Label cardinality breakdown for a single metric ──

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct LabelCardinality {
    pub label_key: String,
    pub unique_values: u64,
}

#[derive(Debug, Serialize)]
pub struct LabelBreakdownResponse {
    pub metric_name: String,
    pub labels: Vec<LabelCardinality>,
    pub total_series: u64,
}

/// Get label cardinality breakdown for a specific metric.
pub async fn get_label_breakdown(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(metric): Path<String>,
    Query(params): Query<UsageQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let scope = resolve_metering_scope(
        &tenant.tenant_id,
        params.global,
        params.tenant_id.as_deref(),
    );
    if scope.requires_admin() {
        require_admin(&state, &headers).await?;
    } else {
        require_auth(&state, &headers).await?;
    }
    let tenant_id = scope.tenant_id();
    let escaped_tenant = escape_string_literal(tenant_id);
    let tenant_filter = if scope.is_global() {
        String::new()
    } else {
        format!("tenant_id = '{escaped_tenant}' AND ")
    };
    let escaped = escape_string_literal(&metric);

    // Count distinct values per label key across both gauge and sum tables.
    // We union the raw label key/value pairs first, then count distinct per key.
    let sql = format!(
        "SELECT label_key, uniq(label_value) as unique_values FROM ( \
             SELECT 'service_name' as label_key, ServiceName as label_value \
             FROM metrics_gauge \
             WHERE {tenant_filter}MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
             UNION ALL \
             SELECT 'service_name' as label_key, ServiceName as label_value \
             FROM metrics_sum \
             WHERE {tenant_filter}MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
             UNION ALL \
             SELECT k as label_key, v as label_value \
             FROM metrics_gauge \
             ARRAY JOIN mapKeys(Attributes) AS k, mapValues(Attributes) AS v \
             WHERE {tenant_filter}MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
             UNION ALL \
             SELECT k as label_key, v as label_value \
             FROM metrics_sum \
             ARRAY JOIN mapKeys(Attributes) AS k, mapValues(Attributes) AS v \
             WHERE {tenant_filter}MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
         ) \
         GROUP BY label_key \
         ORDER BY unique_values DESC"
    );

    let labels_query = if scope.requires_admin() {
        state.admin_ch.query(&sql)
    } else {
        crate::tenant_query(&state.ch, &sql, &tenant.tenant_id)
    };
    let labels = labels_query
        .fetch_all::<LabelCardinality>()
        .await
        .unwrap_or_default();

    // Total series for this metric
    let total_sql = format!(
        "SELECT 'total' as label_key, sum(sc) as unique_values FROM ( \
             SELECT uniq(tenant_id, ServiceName, Attributes) as sc \
             FROM metrics_gauge \
             WHERE {tenant_filter}MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
             UNION ALL \
             SELECT uniq(tenant_id, ServiceName, Attributes) as sc \
             FROM metrics_sum \
             WHERE {tenant_filter}MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
         )"
    );

    #[derive(serde::Deserialize, clickhouse::Row)]
    #[allow(dead_code)]
    struct TotalRow {
        label_key: String,
        unique_values: u64,
    }

    let total_query = if scope.requires_admin() {
        state.admin_ch.query(&total_sql)
    } else {
        crate::tenant_query(&state.ch, &total_sql, &tenant.tenant_id)
    };
    let total_series = total_query
        .fetch_one::<TotalRow>()
        .await
        .map(|r| r.unique_values)
        .unwrap_or(0);

    Ok(Json(LabelBreakdownResponse {
        metric_name: metric,
        labels,
        total_series,
    }))
}
