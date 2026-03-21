use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
};

use crate::AppState;
use crate::models::log::LogRecord;
use crate::models::query::{CountBucket, CountQueryRequest, CountRow, Filter, FilterOp, TimeRange};
use crate::query_builder::{format_value, build_log_search_sql};

/// Resolve a log field name to a ClickHouse column expression.
/// Uses materialized columns for common resource attributes (avoids Map lookups).
fn resolve_log_field(field: &str) -> String {
    match field {
        "service_name" | "ServiceName" => "ServiceName".to_string(),
        "severity" | "severity_text" | "SeverityText" => "SeverityText".to_string(),
        "severity_number" | "SeverityNumber" => "SeverityNumber".to_string(),
        "body" | "Body" => "Body".to_string(),
        "trace_id" | "TraceId" => "TraceId".to_string(),
        "span_id" | "SpanId" => "SpanId".to_string(),
        "scope_name" | "ScopeName" => "ScopeName".to_string(),
        _ => {
            if let Some(attr) = field.strip_prefix("resource.") {
                // Use materialized columns for common k8s/deployment attributes
                match attr {
                    "k8s.namespace.name" => "mat_k8s_namespace".to_string(),
                    "k8s.pod.name" => "mat_k8s_pod".to_string(),
                    "k8s.container.name" => "mat_k8s_container".to_string(),
                    "k8s.deployment.name" => "mat_k8s_deployment".to_string(),
                    "deployment.environment" => "mat_environment".to_string(),
                    _ => format!("ResourceAttributes['{attr}']"),
                }
            } else if let Some(attr) = field.strip_prefix("log.") {
                format!("LogAttributes['{attr}']")
            } else {
                // Unqualified key: check both LogAttributes and ResourceAttributes
                let escaped = field.replace('\'', "\\'");
                format!(
                    "if(LogAttributes['{escaped}'] != '', LogAttributes['{escaped}'], ResourceAttributes['{escaped}'])"
                )
            }
        }
    }
}

fn build_log_where(filters: &[Filter], from: &str, to: &str, search: Option<&str>) -> String {
    let mut conditions = vec![
        format!("Timestamp >= parseDateTimeBestEffort('{from}')"),
        format!("Timestamp <= parseDateTimeBestEffort('{to}')"),
    ];

    for filter in filters {
        let field = resolve_log_field(&filter.field);
        let condition = match &filter.op {
            FilterOp::Eq => format!("{field} = {}", format_value(&filter.value)),
            FilterOp::Ne => format!("{field} != {}", format_value(&filter.value)),
            FilterOp::Gt => format!("{field} > {}", format_value(&filter.value)),
            FilterOp::Gte => format!("{field} >= {}", format_value(&filter.value)),
            FilterOp::Lt => format!("{field} < {}", format_value(&filter.value)),
            FilterOp::Lte => format!("{field} <= {}", format_value(&filter.value)),
            FilterOp::Like => format!("{field} LIKE {}", format_value(&filter.value)),
            FilterOp::NotLike => format!("{field} NOT LIKE {}", format_value(&filter.value)),
            FilterOp::In => format!("{field} IN {}", crate::query_builder::format_array_value(&filter.value)),
            FilterOp::NotIn => format!("{field} NOT IN {}", crate::query_builder::format_array_value(&filter.value)),
        };
        conditions.push(condition);
    }

    if let Some(term) = search {
        if let Some(sql) = build_log_search_sql(term) {
            conditions.push(sql);
        }
    }

    conditions.join(" AND ")
}

/// Log query request.
#[derive(Debug, serde::Deserialize)]
pub struct LogQueryRequest {
    pub time_range: TimeRange,
    #[serde(default)]
    pub filters: Vec<Filter>,
    #[serde(default = "default_limit")]
    pub limit: u64,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub search: Option<String>,
}

fn default_limit() -> u64 { 100 }

/// Query logs from otel_logs.
pub async fn query_logs(
    State(state): State<AppState>,
    Json(req): Json<LogQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let where_clause = build_log_where(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref());

    let limit = req.limit.min(1000);
    let select_cols = "Timestamp, TraceId, SpanId, SeverityText, SeverityNumber, \
         ServiceName, Body, ResourceAttributes, ScopeName, LogAttributes";

    // Fast path: when browsing logs (no search), try a narrow recent window first.
    // The table's primary key is (ServiceName, TimestampTime, Timestamp), so a
    // wide time range without ServiceName filter requires a full scan.  Querying
    // just the last hour first is nearly instant and usually returns enough rows.
    let (rows, total) = if req.search.is_none() && req.offset == 0 {
        // Try last 1 hour first
        let narrow_to = &req.time_range.to;
        let narrow_from = {
            let to_dt = chrono::DateTime::parse_from_rfc3339(narrow_to)
                .or_else(|_| chrono::DateTime::parse_from_rfc3339(&format!("{narrow_to}Z")))
                .unwrap_or_else(|_| chrono::Utc::now().into());
            (to_dt - chrono::Duration::hours(1)).to_rfc3339()
        };
        let narrow_where = build_log_where(&req.filters, &narrow_from, narrow_to, None);
        let narrow_sql = format!(
            "SELECT {select_cols} FROM otel_logs WHERE {narrow_where} \
             ORDER BY TimestampTime DESC, Timestamp DESC LIMIT {limit}"
        );
        let narrow_rows = state.ch.query(&narrow_sql)
            .fetch_all::<LogRecord>().await
            .map_err(|e| {
                tracing::error!("Log narrow query failed: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
            })?;

        if (narrow_rows.len() as u64) >= limit {
            // Got enough from the last hour — fast path success
            let total = narrow_rows.len() as u64;
            (narrow_rows, total)
        } else {
            // Not enough recent logs — fall back to full range
            let full_sql = format!(
                "SELECT {select_cols} FROM otel_logs WHERE {where_clause} \
                 ORDER BY TimestampTime DESC, Timestamp DESC LIMIT {limit}"
            );
            let rows = state.ch.query(&full_sql)
                .fetch_all::<LogRecord>().await
                .map_err(|e| {
                    tracing::error!("Log query failed: {e}");
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
                })?;
            let total = rows.len() as u64;
            (rows, total)
        }
    } else {
        // Search or pagination: use full range
        let sql = format!(
            "SELECT {select_cols} FROM otel_logs WHERE {where_clause} \
             ORDER BY TimestampTime DESC, Timestamp DESC LIMIT {limit} OFFSET {}",
            req.offset,
        );
        if req.search.is_some() {
            tracing::info!("Log search SQL: {sql}");
        }
        let rows = state.ch.query(&sql)
            .fetch_all::<LogRecord>().await
            .map_err(|e| {
                tracing::error!("Log query failed: {e} | SQL: {sql}");
                (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
            })?;
        let total = rows.len() as u64;
        (rows, total)
    };

    if req.search.is_some() {
        tracing::info!("Log search returned {} rows, total={}", rows.len(), total);
    }

    // Only track usage if the query returned results
    if total > 0 {
        let filter_pairs: Vec<(String, String)> = req.filters.iter()
            .map(|f| (f.field.clone(), f.value.as_str().unwrap_or_default().to_string()))
            .collect();
        let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
        state.usage.track_many(signals, "log", "explore");
    }

    // P7: Serialize typed structs directly — no intermediate serde_json::Value
    Ok(Json(serde_json::json!({ "rows": rows, "total": total })))
}

/// Count logs bucketed by time interval.
pub async fn count_logs(
    State(state): State<AppState>,
    Json(req): Json<CountQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let where_clause = build_log_where(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref());

    let interval_fn = match req.interval.as_str() {
        "1s" => "toStartOfSecond(Timestamp)",
        "10s" => "toStartOfTenSeconds(Timestamp)",
        "1m" => "toStartOfMinute(Timestamp)",
        "5m" => "toStartOfFiveMinutes(Timestamp)",
        "15m" => "toStartOfFifteenMinutes(Timestamp)",
        "1h" => "toStartOfHour(Timestamp)",
        "1d" => "toStartOfDay(Timestamp)",
        _ => "toStartOfMinute(Timestamp)",
    };

    let sql = format!(
        "SELECT toString({interval_fn}) as bucket, count() as count, \
         countIf(SeverityNumber >= 17) as error_count \
         FROM otel_logs \
         WHERE {where_clause} \
         GROUP BY bucket \
         ORDER BY bucket ASC"
    );

    let buckets = state
        .ch
        .query(&sql)
        .fetch_all::<CountBucket>()
        .await
        .map_err(|e| {
            tracing::error!("Log count query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    Ok(Json(buckets))
}
