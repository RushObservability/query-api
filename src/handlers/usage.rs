use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct UsageQuery {
    /// Filter by signal type: metric, span, log (optional)
    pub signal_type: Option<String>,
    /// Number of days to look back (default 30)
    pub days: Option<u32>,
    /// Limit results (default 100)
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct UsageRow {
    pub signal_name: String,
    pub signal_type: String,
    pub source: String,
    pub last_queried_at: String,
    pub query_count: u64,
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
    Query(params): Query<UsageQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let days = params.days.unwrap_or(30);
    let limit = params.limit.unwrap_or(100).min(1000);

    let type_filter = match &params.signal_type {
        Some(t) => format!("AND signal_type = '{}'", t.replace('\'', "\\'")),
        None => String::new(),
    };

    // Get usage data (FINAL forces ReplacingMergeTree dedup)
    // Use a subquery so toString alias doesn't shadow the column in WHERE
    let sql = format!(
        "SELECT signal_name, signal_type, source, \
         toString(toUnixTimestamp64Milli(last_queried_at)) as last_queried_at, query_count \
         FROM ( \
             SELECT * FROM signal_usage FINAL \
             WHERE last_queried_at >= now() - INTERVAL {days} DAY {type_filter} \
         ) \
         ORDER BY last_queried_at DESC \
         LIMIT {limit}"
    );

    let usage = state
        .ch
        .query(&sql)
        .fetch_all::<UsageRow>()
        .await
        .map_err(|e| {
            tracing::error!("Usage query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    // Count total tracked signals
    let count_sql = format!(
        "SELECT count() as count FROM signal_usage FINAL \
         WHERE last_queried_at >= now() - INTERVAL {days} DAY {type_filter}"
    );

    #[derive(serde::Deserialize, clickhouse::Row)]
    struct CountRow {
        count: u64,
    }

    let total = state
        .ch
        .query(&count_sql)
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0);

    // Find unused metrics (exist in otel_metrics but not in signal_usage)
    let unused_sql = format!(
        "SELECT metric_name \
         FROM ( \
             SELECT DISTINCT MetricName as metric_name FROM otel_metrics_gauge \
             WHERE TimeUnix >= now() - INTERVAL 1 DAY \
             UNION DISTINCT \
             SELECT DISTINCT MetricName as metric_name FROM otel_metrics_sum \
             WHERE TimeUnix >= now() - INTERVAL 1 DAY \
         ) AS all_metrics \
         LEFT JOIN ( \
             SELECT signal_name FROM signal_usage FINAL \
             WHERE signal_type = 'metric' AND last_queried_at >= now() - INTERVAL {days} DAY \
         ) AS used ON all_metrics.metric_name = used.signal_name \
         WHERE used.signal_name IS NULL OR used.signal_name = '' \
         ORDER BY metric_name \
         LIMIT 200"
    );

    let unused = state
        .ch
        .query(&unused_sql)
        .fetch_all::<UnusedMetric>()
        .await
        .unwrap_or_default();

    // Cardinality explorer — count unique series (label combos) per metric
    let cardinality_sql =
        "SELECT metric_name, sum(series_count) as series_count, max(label_count) as label_count \
         FROM ( \
             SELECT MetricName as metric_name, \
                    uniq(ServiceName, Attributes) as series_count, \
                    max(length(mapKeys(Attributes))) as label_count \
             FROM otel_metrics_gauge \
             WHERE TimeUnix >= now() - INTERVAL 1 HOUR \
             GROUP BY metric_name \
             UNION ALL \
             SELECT MetricName as metric_name, \
                    uniq(ServiceName, Attributes) as series_count, \
                    max(length(mapKeys(Attributes))) as label_count \
             FROM otel_metrics_sum \
             WHERE TimeUnix >= now() - INTERVAL 1 HOUR \
             GROUP BY metric_name \
         ) \
         GROUP BY metric_name \
         ORDER BY series_count DESC \
         LIMIT 100";

    let cardinality = state
        .ch
        .query(cardinality_sql)
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
    Path(metric): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let escaped = metric.replace('\'', "\\'");

    // Count distinct values per label key across both gauge and sum tables.
    // We union the raw label key/value pairs first, then count distinct per key.
    let sql = format!(
        "SELECT label_key, uniq(label_value) as unique_values FROM ( \
             SELECT 'service_name' as label_key, ServiceName as label_value \
             FROM otel_metrics_gauge \
             WHERE MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
             UNION ALL \
             SELECT 'service_name' as label_key, ServiceName as label_value \
             FROM otel_metrics_sum \
             WHERE MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
             UNION ALL \
             SELECT k as label_key, v as label_value \
             FROM otel_metrics_gauge \
             ARRAY JOIN mapKeys(Attributes) AS k, mapValues(Attributes) AS v \
             WHERE MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
             UNION ALL \
             SELECT k as label_key, v as label_value \
             FROM otel_metrics_sum \
             ARRAY JOIN mapKeys(Attributes) AS k, mapValues(Attributes) AS v \
             WHERE MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
         ) \
         GROUP BY label_key \
         ORDER BY unique_values DESC"
    );

    let labels = state
        .ch
        .query(&sql)
        .fetch_all::<LabelCardinality>()
        .await
        .unwrap_or_default();

    // Total series for this metric
    let total_sql = format!(
        "SELECT 'total' as label_key, sum(sc) as unique_values FROM ( \
             SELECT uniq(ServiceName, Attributes) as sc \
             FROM otel_metrics_gauge \
             WHERE MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
             UNION ALL \
             SELECT uniq(ServiceName, Attributes) as sc \
             FROM otel_metrics_sum \
             WHERE MetricName = '{escaped}' AND TimeUnix >= now() - INTERVAL 1 HOUR \
         )"
    );

    #[derive(serde::Deserialize, clickhouse::Row)]
    struct TotalRow {
        label_key: String,
        unique_values: u64,
    }

    let total_series = state
        .ch
        .query(&total_sql)
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
