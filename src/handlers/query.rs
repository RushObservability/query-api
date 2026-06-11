use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Extension,
};

use crate::AppState;
use crate::TenantContext;
use crate::models::query::{
    CountBucket, CountQueryRequest, CountRow, GroupedTimeseriesBucket, QueryRequest,
    TimeseriesBucket, TimeseriesRequest,
};
use crate::models::trace::WideEvent;
use crate::query_builder::{resolve_field, build_where_clause_with_search};

/// Execute a structured query against spans.
pub async fn execute_query(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<QueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let start = std::time::Instant::now();
    let tenant_id = &tenant.tenant_id;

    // Input validation
    if let Some(ref s) = req.search {
        if s.len() > 512 {
            return Err((StatusCode::BAD_REQUEST, "search query too long (max 512 chars)".into()));
        }
    }
    // Deep OFFSET pagination materializes and discards full wide rows server-side, so
    // cap how deep a client can page (50k rows ≈ 500 pages at the default page size).
    let offset = req.offset.min(50_000);
    let limit = req.limit.min(1000);

    let escaped_tenant = crate::query_builder::escape_string_literal(tenant_id);
    let clauses = build_where_clause_with_search(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref())
        .with_prewhere_prefix(&format!("tenant_id = '{escaped_tenant}'"));

    // ── Additive pagination mode ──
    // Keyset (cursor) pagination is opt-in: when the client sends a `cursor`, page via a
    // bound `(timestamp, span_id)` WHERE predicate + `ORDER BY timestamp DESC, span_id
    // DESC` (aligns with the (tenant,timestamp,...,span_id) sort key) instead of OFFSET,
    // so deep pages don't scan+discard rows. A malformed/garbage cursor decodes to None
    // and falls back to the offset path (non-fatal). When `cursor` is absent the SQL is
    // byte-identical to the original offset query — existing callers see no change.
    let keyset = req.cursor.as_deref().and_then(crate::query_builder::KeysetCursor::decode);

    // Slim projection is opt-in via `columns: "list"`: select only the ~10 columns the
    // Explore table renders. Default (absent/other) returns the full wide `SELECT *`.
    let slim = req.columns.as_deref() == Some("list");
    const SLIM_COLS: &str = "timestamp, service_name, span_name, http_method, http_path, \
         http_status_code, duration_ns, status, trace_id, span_id";

    let projection = if slim { SLIM_COLS } else { "*" };
    let sql = if let Some(ref cur) = keyset {
        // Keyset path: no OFFSET; deterministic (timestamp, span_id) ordering.
        format!(
            "SELECT {projection} FROM spans {} AND {} ORDER BY timestamp DESC, span_id DESC LIMIT {limit}",
            clauses.to_sql(),
            cur.before_predicate(),
        )
    } else {
        // Offset path: unchanged from the original behavior (ORDER BY timestamp DESC).
        format!(
            "SELECT {projection} FROM spans {} ORDER BY timestamp DESC LIMIT {limit} OFFSET {offset}",
            clauses.to_sql(),
        )
    };

    // Capped count: an exact count() re-scans the entire lookback window with the same
    // predicate as the data fetch (doubling the work) just to render "N results". Wrap
    // in a subquery with LIMIT so ClickHouse stops reading once the cap is reached.
    // The UI can render this as "10000+". For needle searches (few matches) the cost
    // is dominated by skip-index pruning anyway; for common terms it short-circuits.
    const COUNT_CAP: u64 = 10_000;
    let count_sql = format!(
        "SELECT count() as count FROM (SELECT 1 FROM spans {} LIMIT {COUNT_CAP})",
        clauses.to_sql(),
    );

    // Run data fetch and count in parallel. Wide vs slim deserialize into different row
    // types, but we normalize both into the same JSON `rows` array and compute the same
    // `next_cursor` from the last row's (timestamp, span_id).
    let (rows_json, next_cursor) = if slim {
        let (rows_result, count_result) = tokio::join!(
            crate::tenant_query(&state.ch, &sql, tenant_id).fetch_all::<crate::models::trace::SlimEvent>(),
            crate::tenant_query(&state.ch, &count_sql, tenant_id).fetch_one::<CountRow>(),
        );
        let rows = rows_result.map_err(|e| {
            tracing::error!(error = %e, signal = "traces", handler = "execute_query", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;
        let total = count_result.map(|r| r.count).unwrap_or(0);
        let next = rows.last().map(|r| crate::query_builder::KeysetCursor {
            timestamp: r.timestamp,
            span_id: r.span_id.clone(),
        }.encode());
        emit_usage_and_log(&state, &req, total, rows.len(), start);
        (serde_json::json!({ "rows": rows, "total": total }), next)
    } else {
        let (rows_result, count_result) = tokio::join!(
            crate::tenant_query(&state.ch, &sql, tenant_id).fetch_all::<WideEvent>(),
            crate::tenant_query(&state.ch, &count_sql, tenant_id).fetch_one::<CountRow>(),
        );
        let rows = rows_result.map_err(|e| {
            tracing::error!(error = %e, signal = "traces", handler = "execute_query", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;
        let total = count_result.map(|r| r.count).unwrap_or(0);
        let next = rows.last().map(|r| crate::query_builder::KeysetCursor {
            timestamp: r.timestamp,
            span_id: r.span_id.clone(),
        }.encode());
        emit_usage_and_log(&state, &req, total, rows.len(), start);
        (serde_json::json!({ "rows": rows, "total": total }), next)
    };

    // Merge `next_cursor` additively into the existing `{rows, total}` envelope. Existing
    // callers ignore the extra field; keyset-aware callers use it for the next page.
    let mut resp = rows_json;
    if let (Some(obj), Some(cursor)) = (resp.as_object_mut(), next_cursor) {
        obj.insert("next_cursor".to_string(), serde_json::Value::String(cursor));
    }
    Ok(Json(resp))
}

/// Shared usage-tracking + structured log for the explore query handler (wide & slim).
fn emit_usage_and_log(
    state: &AppState,
    req: &QueryRequest,
    total: u64,
    row_count: usize,
    start: std::time::Instant,
) {
    if total > 0 {
        let filter_pairs: Vec<(String, String)> = req.filters.iter()
            .map(|f| (f.field.clone(), f.value.as_str().unwrap_or_default().to_string()))
            .collect();
        let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
        state.usage.track_many(signals, "span", "explore");
    }
    tracing::info!(
        signal = "traces",
        query = "explore",
        rows = row_count,
        total = total,
        duration_ms = start.elapsed().as_millis() as u64,
        filters = req.filters.len(),
        "query completed"
    );
}

/// Span export request — same shape as a span query plus output format and an
/// optional human-readable query string for the export's metadata header.
#[derive(Debug, serde::Deserialize)]
pub struct SpanExportRequest {
    pub time_range: crate::models::query::TimeRange,
    #[serde(default)]
    pub filters: Vec<crate::models::query::Filter>,
    #[serde(default)]
    pub limit: u64,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub format: crate::handlers::export::ExportFormat,
    #[serde(default)]
    pub query_text: Option<String>,
}

/// Export spans matching the current query as a CSV or JSON file.
/// Limit is clamped to the admin-configured `export_max_rows` (not the 1000 cap).
pub async fn export_query(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<SpanExportRequest>,
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

    let escaped_tenant = crate::query_builder::escape_string_literal(tenant_id);
    let clauses = build_where_clause_with_search(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref())
        .with_prewhere_prefix(&format!("tenant_id = '{escaped_tenant}'"));
    let sql = format!(
        "SELECT * FROM spans {} ORDER BY timestamp DESC LIMIT {limit}",
        clauses.to_sql(),
    );

    let unix = chrono::Utc::now().timestamp();
    match req.format {
        export::ExportFormat::Csv => {
            // Stream span rows from the ClickHouse cursor (see export_logs / export.rs
            // for rationale). Byte-identical CSV output to the prior fetch_all path;
            // peak memory is one row regardless of the configured row cap.
            let mut prelude = export::csv_query_preamble(
                "spans", &req.time_range.from, &req.time_range.to,
                req.search.as_deref(), req.query_text.as_deref(),
            );
            prelude.push_str("Timestamp,Service,Method,Resource,Status,DurationMs,TraceId\n");

            let cursor = crate::tenant_query(&state.ch, &sql, tenant_id)
                .fetch::<WideEvent>()
                .map_err(|e| {
                    tracing::error!(error = %e, signal = "traces", handler = "export_query", "export stream init failed");
                    (StatusCode::INTERNAL_SERVER_ERROR, "export query failed".into())
                })?;

            let fmt_row = |r: &WideEvent| -> String {
                let duration_ms = format!("{:.3}", r.duration_ns as f64 / 1_000_000.0);
                let status = if r.http_status_code > 0 {
                    r.http_status_code.to_string()
                } else {
                    r.status.clone()
                };
                format!(
                    "{},{},{},{},{},{},{}\n",
                    export::csv_field(&export::ts_rfc3339(r.timestamp)),
                    export::csv_field(&r.service_name),
                    export::csv_field(&r.http_method),
                    export::csv_field(&r.http_path),
                    export::csv_field(&status),
                    export::csv_field(&duration_ms),
                    export::csv_field(&r.trace_id),
                )
            };
            Ok(export::stream_csv_response(cursor, prelude, fmt_row, &format!("rush-spans-{unix}.csv")))
        }
        export::ExportFormat::Json => {
            // JSON export stays buffered (to_string_pretty envelope can't stream
            // byte-identically). Row count is capped by LIMIT. See report.
            let rows = crate::tenant_query(&state.ch, &sql, tenant_id)
                .fetch_all::<WideEvent>().await
                .map_err(|e| {
                    tracing::error!(error = %e, signal = "traces", handler = "export_query", "export query failed");
                    (StatusCode::INTERNAL_SERVER_ERROR, "export query failed".into())
                })?;
            let body = serde_json::json!({
                "query": {
                    "signal": "spans",
                    "time_range": { "from": req.time_range.from, "to": req.time_range.to },
                    "search": req.search,
                    "query_text": req.query_text,
                },
                "exported_at": chrono::Utc::now().to_rfc3339(),
                "count": rows.len(),
                "rows": rows,
            });
            let s = serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into());
            Ok(export::file_response(s, "application/json; charset=utf-8", &format!("rush-spans-{unix}.json")))
        }
    }
}

/// Count events bucketed by time interval.
pub async fn count_query(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<CountQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let clauses = build_where_clause_with_search(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref())
        .with_prewhere_prefix(&format!("tenant_id = '{escaped_tenant}'"));

    // The interval is client-supplied: clamp so (range / interval) <= 2000 buckets
    // (a 1s interval over 30d would otherwise be ~2.6M GROUP BY buckets).
    let interval = crate::query_builder::clamp_bucket_interval(
        &req.interval, &req.time_range.from, &req.time_range.to, 2000,
    ).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let interval_fn = match interval {
        "1s" => "toStartOfSecond(timestamp)",
        "10s" => "toStartOfTenSeconds(timestamp)",
        "1m" => "toStartOfMinute(timestamp)",
        "5m" => "toStartOfFiveMinutes(timestamp)",
        "15m" => "toStartOfFifteenMinutes(timestamp)",
        "1h" => "toStartOfHour(timestamp)",
        "1d" => "toStartOfDay(timestamp)",
        _ => "toStartOfMinute(timestamp)",
    };

    let sql = format!(
        "SELECT toString({interval_fn}) as bucket, count() as count, \
         countIf(http_status_code >= 500 OR status = 'ERROR') as error_count \
         FROM spans {} \
         GROUP BY bucket \
         ORDER BY bucket ASC",
        clauses.to_sql(),
    );

    let buckets = crate::tenant_query(&state.ch, &sql, tenant_id)
        .fetch_all::<CountBucket>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, signal = "traces", handler = "count_query", "query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "query failed".into(),
            )
        })?;

    Ok(Json(buckets))
}

/// Group-by query for breakdowns.
pub async fn group_query(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<QueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if req.group_by.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "group_by must have at least one field".to_string(),
        ));
    }
    // Multi-column group_by used to silently return the generated SQL text instead of
    // executing it. Fail loudly instead — the UI only ever sends a single group_by.
    if req.group_by.len() > 1 {
        return Err((
            StatusCode::BAD_REQUEST,
            "multi-column group_by is not supported yet; pass a single group_by field".to_string(),
        ));
    }

    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let clauses = build_where_clause_with_search(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref())
        .with_prewhere_prefix(&format!("tenant_id = '{escaped_tenant}'"));

    let group_cols: Vec<String> = req
        .group_by
        .iter()
        .enumerate()
        .map(|(i, f)| format!("toString({}) as group_{i}", resolve_field(f)))
        .collect();
    let group_select = group_cols.join(", ");
    let group_by_refs: Vec<String> = (0..req.group_by.len())
        .map(|i| format!("group_{i}"))
        .collect();
    let group_by = group_by_refs.join(", ");

    let sql = format!(
        "SELECT {group_select}, count() as count \
         FROM spans {} \
         GROUP BY {group_by} \
         ORDER BY count DESC \
         LIMIT {}",
        clauses.to_sql(),
        req.limit.min(1000),
    );

    #[derive(Debug, serde::Serialize, serde::Deserialize, clickhouse::Row)]
    struct SingleGroupRow {
        group_0: String,
        count: u64,
    }

    let rows = crate::tenant_query(&state.ch, &sql, tenant_id)
        .fetch_all::<SingleGroupRow>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, signal = "traces", handler = "group_query", "query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "query failed".into(),
            )
        })?;

    let json_rows: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                &req.group_by[0]: r.group_0,
                "count": r.count,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({ "groups": json_rows })))
}

/// Timeseries query — returns time-bucketed RED metrics (Rate, Errors, Duration percentiles).
pub async fn timeseries_query(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<TimeseriesRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let clauses = build_where_clause_with_search(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref())
        .with_prewhere_prefix(&format!("tenant_id = '{escaped_tenant}'"));

    // The interval is client-supplied: clamp so (range / interval) <= 2000 buckets
    // (a 1s interval over 30d would otherwise be ~2.6M GROUP BY buckets). Mirrors count_query.
    let interval = crate::query_builder::clamp_bucket_interval(
        &req.interval, &req.time_range.from, &req.time_range.to, 2000,
    ).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let interval_fn = match interval {
        "1s" => "toStartOfSecond(timestamp)",
        "10s" => "toStartOfTenSeconds(timestamp)",
        "1m" => "toStartOfMinute(timestamp)",
        "5m" => "toStartOfFiveMinutes(timestamp)",
        "15m" => "toStartOfFifteenMinutes(timestamp)",
        "1h" => "toStartOfHour(timestamp)",
        "1d" => "toStartOfDay(timestamp)",
        _ => "toStartOfMinute(timestamp)",
    };

    if let Some(ref group_field) = req.group_by {
        let col = resolve_field(group_field);
        let sql = format!(
            "SELECT \
                toString({interval_fn}) as bucket, \
                toString({col}) as group_key, \
                count() as count, \
                countIf(http_status_code >= 500) as error_count, \
                avg(duration_ns) / 1000000.0 as avg_duration_ms, \
                quantile(0.5)(duration_ns) / 1000000.0 as p50_ms, \
                quantile(0.95)(duration_ns) / 1000000.0 as p95_ms, \
                quantile(0.99)(duration_ns) / 1000000.0 as p99_ms \
             FROM spans {} \
             GROUP BY bucket, group_key \
             ORDER BY bucket ASC, count DESC",
            clauses.to_sql(),
        );

        let buckets = crate::tenant_query(&state.ch, &sql, tenant_id)
            .fetch_all::<GroupedTimeseriesBucket>()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, signal = "traces", handler = "timeseries_query", "query failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
            })?;

        // Only track usage if results returned
        if !buckets.is_empty() {
            let filter_pairs: Vec<(String, String)> = req.filters.iter()
                .map(|f| (f.field.clone(), f.value.as_str().unwrap_or_default().to_string()))
                .collect();
            let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
            state.usage.track_many(signals, "span", "explore");
        }

        Ok(Json(serde_json::json!({ "buckets": buckets, "grouped": true })))
    } else {
        let sql = format!(
            "SELECT \
                toString({interval_fn}) as bucket, \
                count() as count, \
                countIf(http_status_code >= 500) as error_count, \
                avg(duration_ns) / 1000000.0 as avg_duration_ms, \
                quantile(0.5)(duration_ns) / 1000000.0 as p50_ms, \
                quantile(0.95)(duration_ns) / 1000000.0 as p95_ms, \
                quantile(0.99)(duration_ns) / 1000000.0 as p99_ms \
             FROM spans {} \
             GROUP BY bucket \
             ORDER BY bucket ASC",
            clauses.to_sql(),
        );

        let buckets = crate::tenant_query(&state.ch, &sql, tenant_id)
            .fetch_all::<TimeseriesBucket>()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, signal = "traces", handler = "timeseries_query", "query failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
            })?;

        // Only track usage if results returned
        if !buckets.is_empty() {
            let filter_pairs: Vec<(String, String)> = req.filters.iter()
                .map(|f| (f.field.clone(), f.value.as_str().unwrap_or_default().to_string()))
                .collect();
            let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
            state.usage.track_many(signals, "span", "explore");
        }

        Ok(Json(serde_json::json!({ "buckets": buckets, "grouped": false })))
    }
}
