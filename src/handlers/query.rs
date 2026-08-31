use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::TenantContext;
use crate::models::query::{
    CountBucket, CountQueryRequest, CountRow, GroupedTimeseriesBucket, QueryRequest,
    TimeseriesBucket, TimeseriesRequest,
};
use crate::models::trace::WideEvent;
use crate::pagination::{CursorPosition, query_scope};
use crate::query_builder::{build_where_clause_with_search, resolve_field};

fn invalid_cursor() -> (StatusCode, String) {
    (
        StatusCode::BAD_REQUEST,
        "invalid or expired pagination cursor".to_string(),
    )
}

fn span_before_predicate(position: &CursorPosition) -> Result<String, (StatusCode, String)> {
    let [span_id] = position.tie.as_slice() else {
        return Err(invalid_cursor());
    };
    if span_id.is_empty()
        || span_id.len() > 64
        || !span_id
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Err(invalid_cursor());
    }
    let span_id = crate::query_builder::escape_string_literal(span_id);
    Ok(format!(
        "(timestamp < fromUnixTimestamp64Nano({timestamp}) OR \
         (timestamp = fromUnixTimestamp64Nano({timestamp}) AND span_id < '{span_id}'))",
        timestamp = position.timestamp_ns,
    ))
}

/// Execute a structured query against spans.
pub async fn execute_query(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<QueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let _query_guard = state.self_metrics.query_guard("explore_spans", "spans");
    let start = std::time::Instant::now();
    let tenant_id = &tenant.tenant_id;

    // Input validation
    if let Some(ref s) = req.search {
        if s.len() > 512 {
            return Err((
                StatusCode::BAD_REQUEST,
                "search query too long (max 512 chars)".into(),
            ));
        }
    }
    // OFFSET remains temporarily available to older clients, but is deliberately
    // shallow. Cursor-aware clients have constant query complexity at any depth.
    let offset = req.offset.min(10_000);
    let limit = req.limit.clamp(1, 1000);

    let escaped_tenant = crate::query_builder::escape_string_literal(tenant_id);
    let clauses = build_where_clause_with_search(
        &req.filters,
        &req.time_range.from,
        &req.time_range.to,
        req.search.as_deref(),
    )
    .with_prewhere_prefix(&format!("tenant_id = '{escaped_tenant}'"));

    // Slim projection is opt-in via `columns: "list"`: select only the ~10 columns the
    // Explore table renders. Default (absent/other) returns the full wide `SELECT *`.
    let slim = req.columns.as_deref() == Some("list");
    const SLIM_COLS: &str = "timestamp, service_name, span_name, http_method, http_path, \
         http_status_code, duration_ns, status, trace_id, span_id";

    let projection = if slim { SLIM_COLS } else { "*" };
    let scope = query_scope(
        tenant_id,
        "spans",
        if slim { "slim" } else { "wide" },
        &req.time_range,
        &req.filters,
        req.search.as_deref(),
    );
    let position = req
        .cursor
        .as_deref()
        .map(|token| crate::pagination::decode(&state.config_db, token, "spans", &scope))
        .transpose()
        .map_err(|_| invalid_cursor())?;
    let sql = if let Some(ref position) = position {
        let paged = clauses.with_where_extra(&span_before_predicate(position)?);
        format!(
            "SELECT {projection} FROM spans {} ORDER BY timestamp DESC, span_id DESC LIMIT {limit}",
            paged.to_sql(),
        )
    } else {
        // First pages also use deterministic ordering so new arrivals cannot move
        // equal-timestamp rows across page boundaries.
        format!(
            "SELECT {projection} FROM spans {} ORDER BY timestamp DESC, span_id DESC LIMIT {limit} OFFSET {offset}",
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
            crate::tenant_query(&state.ch, &sql, tenant_id)
                .fetch_all::<crate::models::trace::SlimEvent>(),
            crate::tenant_query(&state.ch, &count_sql, tenant_id).fetch_one::<CountRow>(),
        );
        let rows = rows_result.map_err(|e| {
            tracing::error!(error = %e, signal = "traces", handler = "execute_query", "query failed");
            state.self_metrics.record_query_and_search("explore_spans", "spans", req.search.as_ref().map(|s| s.chars().count()), 0, start.elapsed().as_millis() as u64, false);
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;
        let total = count_result.map(|r| r.count).unwrap_or(0);
        let next = (rows.len() as u64 == limit).then(|| {
            let row = rows.last().expect("non-empty full page");
            crate::pagination::encode(
                &state.config_db,
                "spans",
                &scope,
                CursorPosition {
                    timestamp_ns: row.timestamp,
                    tie: vec![row.span_id.clone()],
                },
            )
        });
        emit_usage_and_log(&state, tenant_id, &req, total, rows.len(), start);
        (serde_json::json!({ "rows": rows, "total": total }), next)
    } else {
        let (rows_result, count_result) = tokio::join!(
            crate::tenant_query(&state.ch, &sql, tenant_id).fetch_all::<WideEvent>(),
            crate::tenant_query(&state.ch, &count_sql, tenant_id).fetch_one::<CountRow>(),
        );
        let rows = rows_result.map_err(|e| {
            tracing::error!(error = %e, signal = "traces", handler = "execute_query", "query failed");
            state.self_metrics.record_query_and_search("explore_spans", "spans", req.search.as_ref().map(|s| s.chars().count()), 0, start.elapsed().as_millis() as u64, false);
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;
        let total = count_result.map(|r| r.count).unwrap_or(0);
        let next = (rows.len() as u64 == limit).then(|| {
            let row = rows.last().expect("non-empty full page");
            crate::pagination::encode(
                &state.config_db,
                "spans",
                &scope,
                CursorPosition {
                    timestamp_ns: row.timestamp,
                    tie: vec![row.span_id.clone()],
                },
            )
        });
        emit_usage_and_log(&state, tenant_id, &req, total, rows.len(), start);
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
    tenant_id: &str,
    req: &QueryRequest,
    total: u64,
    row_count: usize,
    start: std::time::Instant,
) {
    if total > 0 {
        let filter_pairs: Vec<(String, String)> = req
            .filters
            .iter()
            .map(|f| {
                (
                    f.field.clone(),
                    f.value.as_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
        state
            .usage
            .track_many(tenant_id, signals, "span", "explore");
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

    // Self-metric: span search-quality signals (latency, result count, query length).
    // Low cardinality — labeled only by the fixed `signal="spans"`. `query_len` is None
    // for browse (no free-text term) so the length histogram only reflects real searches;
    // char count (not bytes) matches the handler's 512-char validation.
    state.self_metrics.record_query_and_search(
        "explore_spans",
        "spans",
        req.search.as_ref().map(|s| s.chars().count()),
        row_count as u64,
        start.elapsed().as_millis() as u64,
        true,
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
    headers: HeaderMap,
    Json(req): Json<SpanExportRequest>,
) -> Result<axum::response::Response, (StatusCode, String)> {
    use crate::handlers::export;
    let _query_guard = state.self_metrics.query_guard("explore_spans", "spans");
    let tenant_id = &tenant.tenant_id;

    if let Some(ref s) = req.search {
        if s.len() > 512 {
            return Err((
                StatusCode::BAD_REQUEST,
                "search query too long (max 512 chars)".into(),
            ));
        }
    }

    if req
        .query_text
        .as_ref()
        .is_some_and(|value| value.len() > 8192)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "export query metadata too long (max 8192 bytes)".into(),
        ));
    }
    let cap = export::read_export_max_rows(&state).await;
    let limit = export::effective_limit(req.limit, cap);
    let max_bytes = export::max_export_bytes();

    // AUDIT: data export. Do NOT log the full search/query text — only a
    // has_search boolean and the row cap.
    {
        let (actor_id, actor_name) = match crate::handlers::auth::extract_session_cookie(&headers) {
            Some(tok) => crate::request_auth::resolve_session_user(&state, &tok)
                .await
                .map(|c| (c.0, c.1))
                .unwrap_or_default(),
            None => (String::new(), String::new()),
        };
        state.audit.log(
            crate::audit::AuditEvent::new("data.export", if actor_id.is_empty() { "anonymous" } else { "user" })
                .actor(actor_id, actor_name)
                .tenant(tenant.tenant_id.clone())
                .resource("spans", tenant.tenant_id.clone())
                .changes(serde_json::json!({
                    "signal": "spans",
                    "format": match req.format { export::ExportFormat::Csv => "csv", export::ExportFormat::Json => "json" },
                    "limit": limit,
                    "mode": if export::requires_async(req.limit, limit) { "async" } else { "stream" },
                    "has_search": req.search.as_deref().map(|s| !s.is_empty()).unwrap_or(false)
                }).to_string())
                .description("spans exported")
                .context(crate::audit::actor_context_from_headers(&headers)),
        ).await;
    }

    let escaped_tenant = crate::query_builder::escape_string_literal(tenant_id);
    let clauses = build_where_clause_with_search(
        &req.filters,
        &req.time_range.from,
        &req.time_range.to,
        req.search.as_deref(),
    )
    .with_prewhere_prefix(&format!("tenant_id = '{escaped_tenant}'"));
    let sql = format!(
        "SELECT * FROM spans {} ORDER BY timestamp DESC LIMIT {limit}",
        clauses.to_sql(),
    );

    let unix = chrono::Utc::now().timestamp();
    if export::requires_async(req.limit, limit) {
        let filename = format!(
            "rush-spans-{unix}.{}",
            match req.format {
                export::ExportFormat::Csv => "csv",
                export::ExportFormat::Json => "json",
            }
        );
        let status =
            state
                .export_jobs
                .create(tenant_id, "spans", req.format, filename.clone(), limit);
        let job_id = status.id.clone();
        let (actor_id, actor_name) = match crate::handlers::auth::extract_session_cookie(&headers) {
            Some(token) => crate::request_auth::resolve_session_user(&state, &token)
                .await
                .map(|caller| (caller.0, caller.1))
                .unwrap_or_default(),
            None => (String::new(), String::new()),
        };
        state
            .audit
            .log(
                crate::audit::AuditEvent::new(
                    "export_job.create",
                    if actor_id.is_empty() { "anonymous" } else { "user" },
                )
                .actor(actor_id, actor_name)
                .tenant(tenant_id.clone())
                .resource("export_job", job_id.clone())
                .changes(
                    serde_json::json!({
                        "signal": "spans",
                        "format": match req.format { export::ExportFormat::Csv => "csv", export::ExportFormat::Json => "json" },
                        "requested_rows": limit,
                        "expires_at": status.expires_at,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;

        let background_state = state.clone();
        let background_tenant = tenant_id.clone();
        let background_job_id = job_id.clone();
        let format = req.format;
        let mut csv_prelude = export::csv_query_preamble(
            "spans",
            &req.time_range.from,
            &req.time_range.to,
            req.search.as_deref(),
            req.query_text.as_deref(),
        );
        csv_prelude.push_str("Timestamp,Service,Method,Resource,Status,DurationMs,TraceId\n");
        let json_prelude = export::json_query_preamble(serde_json::json!({
            "signal": "spans",
            "time_range": { "from": req.time_range.from, "to": req.time_range.to },
            "search": req.search,
            "query_text": req.query_text,
        }));
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            let guard = match background_state
                .query_governor
                .admit(
                    crate::query_governor::WorkloadClass::Export,
                    &background_tenant,
                )
                .await
            {
                Ok(guard) => guard,
                Err(_) => {
                    background_state
                        .export_jobs
                        .mark_failed(&background_job_id, "export capacity unavailable")
                        .await;
                    export::audit_job_transition(
                        &background_state,
                        &background_tenant,
                        &background_job_id,
                        "export_job.fail",
                        "failed",
                        "failure",
                    )
                    .await;
                    return;
                }
            };
            let budget = guard.budget().clone();
            let progress = background_state
                .export_jobs
                .progress_callback(&background_job_id);
            let work = async {
                let cursor = crate::tenant_query(&background_state.ch, &sql, &background_tenant)
                    .fetch::<WideEvent>()?;
                let response = match format {
                    export::ExportFormat::Csv => export::stream_csv_response(
                        cursor,
                        csv_prelude,
                        |row: &WideEvent| {
                            let duration_ms =
                                format!("{:.3}", row.duration_ns as f64 / 1_000_000.0);
                            let status = if row.http_status_code > 0 {
                                row.http_status_code.to_string()
                            } else {
                                row.status.clone()
                            };
                            format!(
                                "{},{},{},{},{},{},{}\n",
                                export::csv_field(&export::ts_rfc3339(row.timestamp)),
                                export::csv_field(&row.service_name),
                                export::csv_field(&row.http_method),
                                export::csv_field(&row.http_path),
                                export::csv_field(&status),
                                export::csv_field(&duration_ms),
                                export::csv_field(&row.trace_id),
                            )
                        },
                        &filename,
                        limit,
                        max_bytes,
                        Some(progress),
                    ),
                    export::ExportFormat::Json => export::stream_json_response(
                        cursor,
                        json_prelude,
                        &filename,
                        limit,
                        max_bytes,
                        Some(progress),
                    ),
                };
                background_state
                    .export_jobs
                    .write_response(&background_job_id, response)
                    .await
            };
            let result = crate::query_governor::with_budget(
                budget.clone(),
                tokio::time::timeout(
                    std::time::Duration::from_secs(budget.request_timeout_secs),
                    work,
                ),
            )
            .await;
            drop(guard);
            match result {
                Ok(Ok(())) => {
                    export::audit_job_transition(
                        &background_state,
                        &background_tenant,
                        &background_job_id,
                        "export_job.complete",
                        "completed",
                        "success",
                    )
                    .await;
                }
                _ if background_state
                    .export_jobs
                    .is_cancelled(&background_job_id) => {}
                _ => {
                    background_state
                        .export_jobs
                        .mark_failed(&background_job_id, "export could not be completed")
                        .await;
                    export::audit_job_transition(
                        &background_state,
                        &background_tenant,
                        &background_job_id,
                        "export_job.fail",
                        "failed",
                        "failure",
                    )
                    .await;
                }
            }
        });
        return Ok(export::accepted_job_response(status));
    }
    match req.format {
        export::ExportFormat::Csv => {
            // Stream span rows from the ClickHouse cursor (see export_logs / export.rs
            // for rationale). Byte-identical CSV output to the prior fetch_all path;
            // peak memory is one row regardless of the configured row cap.
            let mut prelude = export::csv_query_preamble(
                "spans",
                &req.time_range.from,
                &req.time_range.to,
                req.search.as_deref(),
                req.query_text.as_deref(),
            );
            prelude.push_str("Timestamp,Service,Method,Resource,Status,DurationMs,TraceId\n");

            let cursor = crate::tenant_query(&state.ch, &sql, tenant_id)
                .fetch::<WideEvent>()
                .map_err(|_error| {
                    tracing::error!(
                        reason = "cursor_init",
                        signal = "traces",
                        handler = "export_query",
                        "export stream init failed"
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "export query failed".into(),
                    )
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
            Ok(export::stream_csv_response(
                cursor,
                prelude,
                fmt_row,
                &format!("rush-spans-{unix}.csv"),
                limit,
                max_bytes,
                None,
            ))
        }
        export::ExportFormat::Json => {
            let cursor = crate::tenant_query(&state.ch, &sql, tenant_id)
                .fetch::<WideEvent>()
                .map_err(|_error| {
                    tracing::error!(
                        reason = "cursor_init",
                        signal = "traces",
                        handler = "export_query",
                        "export stream init failed"
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "export query failed".into(),
                    )
                })?;
            let prelude = export::json_query_preamble(serde_json::json!({
                "signal": "spans",
                "time_range": { "from": req.time_range.from, "to": req.time_range.to },
                "search": req.search,
                "query_text": req.query_text,
            }));
            Ok(export::stream_json_response(
                cursor,
                prelude,
                &format!("rush-spans-{unix}.json"),
                limit,
                max_bytes,
                None,
            ))
        }
    }
}

/// Count events bucketed by time interval.
pub async fn count_query(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<CountQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let _query_guard = state.self_metrics.query_guard("explore_spans", "spans");
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let clauses = build_where_clause_with_search(
        &req.filters,
        &req.time_range.from,
        &req.time_range.to,
        req.search.as_deref(),
    )
    .with_prewhere_prefix(&format!("tenant_id = '{escaped_tenant}'"));

    // The interval is client-supplied: clamp so (range / interval) <= 2000 buckets
    // (a 1s interval over 30d would otherwise be ~2.6M GROUP BY buckets).
    let interval = crate::query_builder::clamp_bucket_interval(
        &req.interval,
        &req.time_range.from,
        &req.time_range.to,
        2000,
    )
    .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
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
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;

    Ok(Json(buckets))
}

/// Group-by query for breakdowns.
pub async fn group_query(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<QueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let _query_guard = state.self_metrics.query_guard("explore_spans", "spans");
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
    let clauses = build_where_clause_with_search(
        &req.filters,
        &req.time_range.from,
        &req.time_range.to,
        req.search.as_deref(),
    )
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
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
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
    let _query_guard = state.self_metrics.query_guard("explore_spans", "spans");
    let start = std::time::Instant::now();
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let clauses = build_where_clause_with_search(
        &req.filters,
        &req.time_range.from,
        &req.time_range.to,
        req.search.as_deref(),
    )
    .with_prewhere_prefix(&format!("tenant_id = '{escaped_tenant}'"));

    // The interval is client-supplied: clamp so (range / interval) <= 2000 buckets
    // (a 1s interval over 30d would otherwise be ~2.6M GROUP BY buckets). Mirrors count_query.
    let interval = crate::query_builder::clamp_bucket_interval(
        &req.interval,
        &req.time_range.from,
        &req.time_range.to,
        2000,
    )
    .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
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
                state.self_metrics.record_query(
                    "explore_spans",
                    "spans",
                    0,
                    start.elapsed().as_millis() as u64,
                    false,
                );
                (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
            })?;

        // Only track usage if results returned
        if !buckets.is_empty() {
            let filter_pairs: Vec<(String, String)> = req
                .filters
                .iter()
                .map(|f| {
                    (
                        f.field.clone(),
                        f.value.as_str().unwrap_or_default().to_string(),
                    )
                })
                .collect();
            let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
            state
                .usage
                .track_many(tenant_id, signals, "span", "explore");
        }
        state.self_metrics.record_query(
            "explore_spans",
            "spans",
            buckets.len() as u64,
            start.elapsed().as_millis() as u64,
            true,
        );

        Ok(Json(
            serde_json::json!({ "buckets": buckets, "grouped": true }),
        ))
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
                state.self_metrics.record_query(
                    "explore_spans",
                    "spans",
                    0,
                    start.elapsed().as_millis() as u64,
                    false,
                );
                (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
            })?;

        // Only track usage if results returned
        if !buckets.is_empty() {
            let filter_pairs: Vec<(String, String)> = req
                .filters
                .iter()
                .map(|f| {
                    (
                        f.field.clone(),
                        f.value.as_str().unwrap_or_default().to_string(),
                    )
                })
                .collect();
            let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
            state
                .usage
                .track_many(tenant_id, signals, "span", "explore");
        }
        state.self_metrics.record_query(
            "explore_spans",
            "spans",
            buckets.len() as u64,
            start.elapsed().as_millis() as u64,
            true,
        );

        Ok(Json(
            serde_json::json!({ "buckets": buckets, "grouped": false }),
        ))
    }
}

#[cfg(test)]
mod pagination_tests {
    use super::*;

    #[test]
    fn span_cursor_orders_duplicate_timestamps_by_span_id() {
        let predicate = span_before_predicate(&CursorPosition {
            timestamp_ns: 1_749_600_000_123_456_789,
            tie: vec!["abcdef0123456789".into()],
        })
        .unwrap();
        assert!(predicate.contains("timestamp < fromUnixTimestamp64Nano(1749600000123456789)"));
        assert!(predicate.contains("timestamp = fromUnixTimestamp64Nano(1749600000123456789)"));
        assert!(predicate.contains("span_id < 'abcdef0123456789'"));
    }

    #[test]
    fn new_arrivals_are_outside_a_continuation_page() {
        let cursor = CursorPosition {
            timestamp_ns: 1_000,
            tie: vec!["ff".into()],
        };
        let predicate = span_before_predicate(&cursor).unwrap();
        assert!(predicate.starts_with("(timestamp <"));
        assert!(!predicate.contains("timestamp >"));
    }
}
