use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
};

use crate::AppState;
use crate::models::log::LogRecord;
use crate::models::query::{CountBucket, CountQueryRequest, CountRow, QueryResponse, TimeRange, Filter, FilterOp};
use crate::query_builder::{format_value, build_log_search_sql};

/// Resolve a log field name to a ClickHouse column expression.
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
                format!("ResourceAttributes['{attr}']")
            } else if let Some(attr) = field.strip_prefix("log.") {
                format!("LogAttributes['{attr}']")
            } else {
                // Default: try LogAttributes
                format!("LogAttributes['{field}']")
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

    let sql = format!(
        "SELECT Timestamp, TraceId, SpanId, SeverityText, SeverityNumber, \
         ServiceName, Body, ResourceAttributes, ScopeName, LogAttributes \
         FROM otel_logs WHERE {where_clause} \
         ORDER BY Timestamp DESC LIMIT {} OFFSET {}",
        req.limit.min(1000),
        req.offset,
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<LogRecord>()
        .await
        .map_err(|e| {
            tracing::error!("Log query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    let json_rows: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
        .collect();

    let count_sql = format!("SELECT count() as count FROM otel_logs WHERE {where_clause}");
    let total: u64 = state
        .ch
        .query(&count_sql)
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0);

    // Only track usage if the query returned results
    if total > 0 {
        let filter_pairs: Vec<(String, String)> = req.filters.iter()
            .map(|f| (f.field.clone(), f.value.as_str().unwrap_or_default().to_string()))
            .collect();
        let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
        state.usage.track_many(signals, "log", "explore");
    }

    Ok(Json(QueryResponse {
        rows: json_rows,
        total,
    }))
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
