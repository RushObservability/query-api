use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
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
    // Bound the partition column `TimestampDate` (PARTITION BY TimestampDate) in addition
    // to the precise `Timestamp` filter: a predicate on raw `Timestamp` alone does not
    // reliably drive partition pruning, so add the date range to prune partitions first.
    let time_tenant = format!(
        "tenant_id = '{escaped_tenant}' \
         AND TimestampDate >= toDate(parseDateTimeBestEffort('{from}')) \
         AND TimestampDate <= toDate(parseDateTimeBestEffort('{to}')) \
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

    let mut has_search = false;
    if let Some(term) = search {
        if let Some(sql) = build_log_search_sql(term) {
            conditions.push(sql);
            has_search = true;
        }
    }

    // A free-text term compiles to a `lower(Body) LIKE …` (or TraceId/SpanId match)
    // that relies on a skip index — for Body that's the `idx_body_text` full-text
    // index. An *explicit* PREWHERE on tenant/time defeats that index: ClickHouse
    // reads the entire index (tens of GiB) instead of using it to skip granules,
    // turning a ~150 ms query into multi-second / tens-of-GiB scans. Emitting a
    // single WHERE lets `optimize_move_to_prewhere` re-derive the prewhere while
    // keeping the skip index effective. With no search term there's no Body index
    // in play, so the explicit PREWHERE (efficient granule skipping) is kept.
    if has_search {
        let mut all = Vec::with_capacity(conditions.len() + 1);
        all.push(time_tenant);
        all.extend(conditions);
        QueryClauses { prewhere: String::new(), where_clause: all.join(" AND ") }
    } else {
        QueryClauses { prewhere: time_tenant, where_clause: conditions.join(" AND ") }
    }
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

    let (rows, total) = if req.offset == 0 {
        // Progressive fast path (applies to browse AND free-text search): try a narrow
        // recent window first and ONLY scan the full range when the narrow one doesn't
        // fill the page.
        //
        // Browsing (no search term) already terminates early over wide ranges via
        // read-in-order on the time-first primary key. A free-text term compiles to a
        // `lower(Body) LIKE …` that defeats that early-termination, so a wide search
        // would otherwise scan the entire range (measured ~31s over 48h on a hot,
        // high-volume service). Starting with the last hour returns the newest matches
        // in well under a second in the common "what's happening now" case; the search
        // term is included in the narrow query so it benefits too (previously the narrow
        // probe ran only for browse and passed `None`). When the narrow window doesn't
        // fill the page we fall through to the full requested range, so results are
        // never missed — the miss case just pays one cheap probe before the full scan.
        let narrow_to = &req.time_range.to;
        let to_dt = chrono::DateTime::parse_from_rfc3339(narrow_to)
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(&format!("{narrow_to}Z")))
            .unwrap_or_else(|_| chrono::Utc::now().into());
        let from_dt = chrono::DateTime::parse_from_rfc3339(&req.time_range.from)
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(&format!("{}Z", req.time_range.from)))
            .ok();
        // Only probe when the requested range is wider than the probe window — for an
        // already-narrow range (e.g. a 5-minute zoom) the probe == full range, so skip
        // straight to the single query and avoid a redundant round-trip.
        let probe_from_dt = to_dt - chrono::Duration::hours(1);
        let worth_probing = from_dt.map(|f| f < probe_from_dt).unwrap_or(true);

        let narrow_rows = if worth_probing {
            let narrow_from = probe_from_dt.to_rfc3339();
            let narrow_clauses = build_log_where(&req.filters, &narrow_from, narrow_to, req.search.as_deref(), tenant_id);
            let narrow_sql = format!(
                "SELECT {select_cols} FROM logs {} \
                 ORDER BY TimestampDate DESC, TimestampTime DESC, Timestamp DESC LIMIT {limit}",
                narrow_clauses.to_sql(),
            );
            crate::tenant_query(&state.ch, &narrow_sql, tenant_id)
                .fetch_all::<LogRecord>()
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, signal = "logs", handler = "query_logs", "narrow query failed");
                    (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
                })?
        } else {
            Vec::new()
        };

        if worth_probing && (narrow_rows.len() as u64) >= limit {
            let total = narrow_rows.len() as u64;
            (narrow_rows, total)
        } else {
            // Narrow window didn't fill the page (or the range was already narrow):
            // scan the full requested range.
            let full_sql = format!(
                "SELECT {select_cols} FROM logs {} \
                 ORDER BY TimestampDate DESC, TimestampTime DESC, Timestamp DESC LIMIT {limit}",
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
             ORDER BY TimestampDate DESC, TimestampTime DESC, Timestamp DESC LIMIT {limit} OFFSET {}",
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
    headers: HeaderMap,
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

    // AUDIT: data export. Do NOT log the full search/query text (it may contain
    // sensitive values) — only a has_search boolean and the row cap.
    {
        let (actor_id, actor_name) = match crate::handlers::auth::extract_session_cookie(&headers) {
            Some(tok) => state.config_db.get_session_user(&tok).await
                .map(|c| (c.0, c.1))
                .unwrap_or_default(),
            None => (String::new(), String::new()),
        };
        state.audit.log(
            crate::audit::AuditEvent::new("data.export", if actor_id.is_empty() { "anonymous" } else { "user" })
                .actor(actor_id, actor_name)
                .tenant(tenant.tenant_id.clone())
                .resource("logs", tenant.tenant_id.clone())
                .changes(serde_json::json!({
                    "signal": "logs",
                    "format": match req.format { export::ExportFormat::Csv => "csv", export::ExportFormat::Json => "json" },
                    "limit": limit,
                    "has_search": req.search.as_deref().map(|s| !s.is_empty()).unwrap_or(false)
                }).to_string())
                .description("logs exported")
                .context(crate::audit::actor_context_from_headers(&headers)),
        ).await;
    }

    let select_cols = "Timestamp, TraceId, SpanId, SeverityText, SeverityNumber, \
         ServiceName, Body, ResourceAttributes, ScopeName, LogAttributes";
    let clauses = build_log_where(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref(), tenant_id);
    let sql = format!(
        "SELECT {select_cols} FROM logs {} \
         ORDER BY TimestampDate DESC, TimestampTime DESC, Timestamp DESC LIMIT {limit}",
        clauses.to_sql(),
    );

    let unix = chrono::Utc::now().timestamp();
    match req.format {
        export::ExportFormat::Csv => {
            // Stream rows from the ClickHouse cursor instead of buffering the full
            // result set + the concatenated CSV string in memory. Peak memory is one
            // row at a time, so a million-row export no longer materializes hundreds of
            // MB. Output bytes (preamble, header, per-row escaping) are byte-identical
            // to the previous fetch_all path. The LIMIT in the SQL still enforces the
            // configured row cap. tenant_query carries tenant settings/row-policy.
            let mut prelude = export::csv_query_preamble(
                "logs", &req.time_range.from, &req.time_range.to,
                req.search.as_deref(), req.query_text.as_deref(),
            );
            prelude.push_str("Timestamp,Severity,ServiceName,Body,TraceId\n");

            let cursor = crate::tenant_query(&state.ch, &sql, tenant_id)
                .fetch::<LogRecord>()
                .map_err(|e| {
                    tracing::error!(error = %e, signal = "logs", handler = "export_logs", "export stream init failed");
                    (StatusCode::INTERNAL_SERVER_ERROR, "export query failed".into())
                })?;

            let fmt_row = |r: &LogRecord| -> String {
                format!(
                    "{},{},{},{},{}\n",
                    export::csv_field(&export::ts_rfc3339(r.timestamp)),
                    export::csv_field(&r.severity_text),
                    export::csv_field(&r.service_name),
                    export::csv_field(&r.body),
                    export::csv_field(&r.trace_id),
                )
            };
            Ok(export::stream_csv_response(cursor, prelude, fmt_row, &format!("rush-logs-{unix}.csv")))
        }
        export::ExportFormat::Json => {
            // JSON export stays on the buffered fetch_all path: its output is
            // serde_json::to_string_pretty over the full envelope, which can't be
            // reproduced byte-for-byte while streaming row-by-row. Row count is still
            // capped by LIMIT, so memory is bounded by the cap (default 1000). See report.
            let rows = crate::tenant_query(&state.ch, &sql, tenant_id)
                .fetch_all::<LogRecord>().await
                .map_err(|e| {
                    tracing::error!(error = %e, signal = "logs", handler = "export_logs", "export query failed");
                    (StatusCode::INTERNAL_SERVER_ERROR, "export query failed".into())
                })?;
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

/// Adaptive time-bucketed "match histogram" request — same shape as a log
/// query but only the time range, optional filters, and optional free-text
/// search matter.
#[derive(Debug, serde::Deserialize)]
pub struct LogHistogramRequest {
    pub time_range: TimeRange,
    #[serde(default)]
    pub filters: Vec<Filter>,
    #[serde(default)]
    pub search: Option<String>,
}

/// "Nice" bucket sizes (seconds). The histogram picks the smallest value that
/// is >= the computed bucket so adjacent ranges snap to readable intervals
/// (1s, 5s, 15s, … 6h, 1d) rather than arbitrary widths.
const NICE_BUCKET_SECS: [u64; 11] = [1, 5, 15, 30, 60, 300, 900, 1800, 3600, 21600, 86400];

/// Time-bucketed histogram of matching log lines across the selected range.
/// Buckets adapt to the span (~120 buckets, snapped to a "nice" interval) so
/// the UI can render a compact sparkline and let users zoom into a spike.
pub async fn log_histogram(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<LogHistogramRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;

    if let Some(ref s) = req.search {
        if s.len() > 512 {
            return Err((StatusCode::BAD_REQUEST, "search query too long (max 512 chars)".into()));
        }
    }

    let clauses = build_log_where(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref(), tenant_id);

    // Parse from/to (RFC3339, tolerating a missing 'Z' like query_logs does) to
    // size the bucket. Fall back to a 1s bucket if the range can't be parsed.
    let parse_ts = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s)
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(&format!("{s}Z")))
    };
    let from_dt = parse_ts(&req.time_range.from);
    let to_dt = parse_ts(&req.time_range.to);
    let span_secs = match (&from_dt, &to_dt) {
        (Ok(f), Ok(t)) => (t.timestamp() - f.timestamp()).max(1) as u64,
        _ => 1,
    };
    // Aim for ~120 buckets, then snap up to the smallest "nice" interval.
    let computed = (span_secs / 120).max(1);
    let bucket_secs = NICE_BUCKET_SECS
        .iter()
        .copied()
        .find(|&n| n >= computed)
        .unwrap_or(computed);

    let sql = format!(
        "SELECT toUnixTimestamp(toStartOfInterval(Timestamp, INTERVAL {bucket_secs} SECOND)) AS bucket, \
         count() AS c \
         FROM logs {} \
         GROUP BY bucket \
         ORDER BY bucket",
        clauses.to_sql(),
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct HistoRow { bucket: i64, c: u64 }

    let rows = crate::tenant_query(&state.ch, &sql, tenant_id)
        .fetch_all::<HistoRow>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, signal = "logs", handler = "log_histogram", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;

    #[derive(serde::Serialize)]
    struct Bucket { ts: i64, count: u64 }
    #[derive(serde::Serialize)]
    struct Resp { interval_secs: u64, buckets: Vec<Bucket> }

    let buckets = rows.into_iter().map(|r| Bucket { ts: r.bucket, count: r.c }).collect();
    Ok(Json(Resp { interval_secs: bucket_secs, buckets }))
}

/// Group logs by a single field (e.g. SeverityText) → top-N {field, count}.
/// Mirrors the spans `group_query` response shape so the frontend reuses the
/// same normalization. Backs the dashboard "logs" widget source for bar charts.
pub async fn group_logs(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<crate::models::query::QueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if req.group_by.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "group_by must have at least one field".to_string()));
    }
    let field = &req.group_by[0];
    let col = resolve_log_field(field);
    let tenant_id = &tenant.tenant_id;
    let clauses = build_log_where(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref(), tenant_id);

    let sql = format!(
        "SELECT toString({col}) as group_0, count() as count \
         FROM logs {} \
         GROUP BY group_0 \
         ORDER BY count DESC \
         LIMIT {}",
        clauses.to_sql(),
        req.limit.min(1000),
    );

    #[derive(Debug, serde::Serialize, serde::Deserialize, clickhouse::Row)]
    struct SingleGroupRow { group_0: String, count: u64 }

    let rows = crate::tenant_query(&state.ch, &sql, tenant_id)
        .fetch_all::<SingleGroupRow>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, signal = "logs", handler = "group_logs", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;

    let json_rows: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| serde_json::json!({ field.as_str(): r.group_0, "count": r.count }))
        .collect();

    Ok(Json(serde_json::json!({ "groups": json_rows })))
}
