use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Extension,
};

use crate::AppState;
use crate::TenantContext;
use crate::models::log::LogRecord;
use crate::models::query::{CountBucket, CountQueryRequest, Filter, FilterOp, TimeRange};
use crate::query_builder::{format_value, build_log_search_sql, sanitize_datetime, QueryClauses};

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
                let escaped = crate::query_builder::escape_string_literal(&field);
                format!(
                    "if(LogAttributes['{escaped}'] != '', LogAttributes['{escaped}'], ResourceAttributes['{escaped}'])"
                )
            }
        }
    }
}

/// Build PREWHERE-optimized query clauses for logs.
/// tenant_id + time range go into PREWHERE (evaluated at granule level before decompression);
/// column filters and full-text search go into WHERE.
fn build_log_where(filters: &[Filter], from: &str, to: &str, search: Option<&str>, tenant_id: &str) -> QueryClauses {
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let from = sanitize_datetime(from);
    let to = sanitize_datetime(to);
    let prewhere = format!(
        "tenant_id = '{escaped_tenant}' \
         AND Timestamp >= parseDateTimeBestEffort('{from}') \
         AND Timestamp <= parseDateTimeBestEffort('{to}')"
    );

    let mut conditions = Vec::new();

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

    QueryClauses { prewhere, where_clause: conditions.join(" AND ") }
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

/// Query logs from logs.
pub async fn query_logs(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<LogQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let start = std::time::Instant::now();
    let tenant_id = &tenant.tenant_id;

    if let Some(ref s) = req.search {
        if s.len() > 512 {
            return Err((StatusCode::BAD_REQUEST, "search query too long (max 512 chars)".into()));
        }
    }
    let offset = req.offset.min(100_000);
    let limit = req.limit.min(1000);
    let select_cols = "Timestamp, TraceId, SpanId, SeverityText, SeverityNumber, \
         ServiceName, Body, ResourceAttributes, ScopeName, LogAttributes";

    // Fast path: when browsing logs (no search), try a narrow recent window first.
    // The table's primary key is (ServiceName, TimestampTime, Timestamp), so a
    // wide time range without ServiceName filter requires a full scan.  Querying
    // just the last hour first is nearly instant and usually returns enough rows.
    let clauses = build_log_where(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref(), tenant_id);

    let (rows, total) = if req.search.is_none() && req.offset == 0 {
        // Fast path: try a narrow (last 1h) window first and ONLY run the full-range
        // query when the narrow one doesn't fill the limit. The previous version
        // join!'ed both queries, so the full-range scan always ran to completion even
        // when the narrow result won — pure wasted I/O (up to range/1h × the work)
        // for the common "browsing recent logs" case. The full query is now built and
        // executed lazily, so the fast path never touches the full window. Trade-off:
        // when data is sparse the two queries run sequentially instead of in parallel.
        let narrow_to = &req.time_range.to;
        let narrow_from = {
            let to_dt = chrono::DateTime::parse_from_rfc3339(narrow_to)
                .or_else(|_| chrono::DateTime::parse_from_rfc3339(&format!("{narrow_to}Z")))
                .unwrap_or_else(|_| chrono::Utc::now().into());
            (to_dt - chrono::Duration::hours(1)).to_rfc3339()
        };
        let narrow_clauses = build_log_where(&req.filters, &narrow_from, narrow_to, None, tenant_id);
        let narrow_sql = format!(
            "SELECT {select_cols} FROM logs {} \
             ORDER BY TimestampTime DESC, Timestamp DESC LIMIT {limit}",
            narrow_clauses.to_sql(),
        );
        let narrow_rows = crate::tenant_query(&state.ch, &narrow_sql, tenant_id)
            .fetch_all::<LogRecord>()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, signal = "logs", handler = "query_logs", "narrow query failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
            })?;
        if (narrow_rows.len() as u64) >= limit {
            let total = narrow_rows.len() as u64;
            (narrow_rows, total)
        } else {
            // Sparse case: the recent window didn't fill the page — scan the full range.
            let full_sql = format!(
                "SELECT {select_cols} FROM logs {} \
                 ORDER BY TimestampTime DESC, Timestamp DESC LIMIT {limit}",
                clauses.to_sql(),
            );
            let rows = crate::tenant_query(&state.ch, &full_sql, tenant_id)
                .fetch_all::<LogRecord>()
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, signal = "logs", handler = "query_logs", "full-range query failed");
                    (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
                })?;
            let total = rows.len() as u64;
            (rows, total)
        }
    } else {
        // Search or pagination: use full range
        let sql = format!(
            "SELECT {select_cols} FROM logs {} \
             ORDER BY TimestampTime DESC, Timestamp DESC LIMIT {limit} OFFSET {}",
            clauses.to_sql(),
            offset,
        );
        if req.search.is_some() {
            tracing::debug!(signal = "logs", handler = "query_logs", "log search query executing");
        }
        let rows = crate::tenant_query(&state.ch, &sql, tenant_id)
            .fetch_all::<LogRecord>().await
            .map_err(|e| {
                tracing::error!(error = %e, signal = "logs", handler = "query_logs", "search query failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
            })?;
        let total = rows.len() as u64;
        (rows, total)
    };

    tracing::info!(
        signal = "logs",
        tenant_id = %tenant_id,
        query = "log_search",
        rows = rows.len(),
        total = total,
        duration_ms = start.elapsed().as_millis() as u64,
        "log search completed"
    );

    // Only track usage if the query returned results
    if total > 0 {
        let filter_pairs: Vec<(String, String)> = req.filters.iter()
            .map(|f| (f.field.clone(), f.value.as_str().unwrap_or_default().to_string()))
            .collect();
        let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
        state.usage.track_many(signals, "log", "explore");
    }

    #[derive(serde::Serialize)]
    struct Resp { rows: Vec<LogRecord>, total: u64 }
    Ok(Json(Resp { rows, total }))
}

/// Log export request — same shape as a log query plus output format and an
/// optional human-readable query string for the export's metadata header.
#[derive(Debug, serde::Deserialize)]
pub struct LogExportRequest {
    pub time_range: TimeRange,
    #[serde(default)]
    pub filters: Vec<Filter>,
    #[serde(default)]
    pub limit: u64,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub format: crate::handlers::export::ExportFormat,
    #[serde(default)]
    pub query_text: Option<String>,
}

/// Export logs matching the current query as a CSV or JSON file.
/// Limit is clamped to the admin-configured `export_max_rows` (not the 1000 cap).
pub async fn export_logs(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<LogExportRequest>,
) -> Result<axum::response::Response, (StatusCode, String)> {
    use crate::handlers::export;
    let tenant_id = &tenant.tenant_id;

    if let Some(ref s) = req.search {
        if s.len() > 512 {
            return Err((StatusCode::BAD_REQUEST, "search query too long (max 512 chars)".into()));
        }
    }

    let cap = export::read_export_max_rows(&state).await;
    let limit = export::effective_limit(req.limit, cap);

    let select_cols = "Timestamp, TraceId, SpanId, SeverityText, SeverityNumber, \
         ServiceName, Body, ResourceAttributes, ScopeName, LogAttributes";
    let clauses = build_log_where(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref(), tenant_id);
    let sql = format!(
        "SELECT {select_cols} FROM logs {} \
         ORDER BY TimestampTime DESC, Timestamp DESC LIMIT {limit}",
        clauses.to_sql(),
    );
    let rows = crate::tenant_query(&state.ch, &sql, tenant_id)
        .fetch_all::<LogRecord>().await
        .map_err(|e| {
            tracing::error!(error = %e, signal = "logs", handler = "export_logs", "export query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "export query failed".into())
        })?;

    let unix = chrono::Utc::now().timestamp();
    match req.format {
        export::ExportFormat::Csv => {
            let mut out = export::csv_query_preamble(
                "logs", &req.time_range.from, &req.time_range.to,
                req.search.as_deref(), req.query_text.as_deref(),
            );
            out.push_str("Timestamp,Severity,ServiceName,Body,TraceId\n");
            for r in &rows {
                out.push_str(&format!(
                    "{},{},{},{},{}\n",
                    export::csv_field(&export::ts_rfc3339(r.timestamp)),
                    export::csv_field(&r.severity_text),
                    export::csv_field(&r.service_name),
                    export::csv_field(&r.body),
                    export::csv_field(&r.trace_id),
                ));
            }
            Ok(export::file_response(out, "text/csv; charset=utf-8", &format!("rush-logs-{unix}.csv")))
        }
        export::ExportFormat::Json => {
            let body = serde_json::json!({
                "query": {
                    "signal": "logs",
                    "time_range": { "from": req.time_range.from, "to": req.time_range.to },
                    "search": req.search,
                    "query_text": req.query_text,
                },
                "exported_at": chrono::Utc::now().to_rfc3339(),
                "count": rows.len(),
                "rows": rows,
            });
            let s = serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into());
            Ok(export::file_response(s, "application/json; charset=utf-8", &format!("rush-logs-{unix}.json")))
        }
    }
}

/// Count logs bucketed by time interval.
pub async fn count_logs(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<CountQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let clauses = build_log_where(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref(), tenant_id);

    // The interval is client-supplied: clamp so (range / interval) <= 2000 buckets
    // (a 1s interval over 30d would otherwise be ~2.6M GROUP BY buckets).
    let interval = crate::query_builder::clamp_bucket_interval(
        &req.interval, &req.time_range.from, &req.time_range.to, 2000,
    ).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let interval_fn = match interval {
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
         FROM logs \
         {} \
         GROUP BY bucket \
         ORDER BY bucket ASC",
        clauses.to_sql(),
    );

    let buckets = crate::tenant_query(&state.ch, &sql, tenant_id)
        .fetch_all::<CountBucket>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, signal = "logs", handler = "count_logs", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;

    Ok(Json(buckets))
}
