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

/// Execute a structured query against wide_events.
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
    let offset = req.offset.min(100_000);

    let escaped_tenant = crate::query_builder::escape_string_literal(tenant_id);
    let clauses = build_where_clause_with_search(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref())
        .with_prewhere_prefix(&format!("tenant_id = '{escaped_tenant}'"));

    let sql = format!(
        "SELECT * FROM wide_events {} ORDER BY timestamp DESC LIMIT {} OFFSET {}",
        clauses.to_sql(),
        req.limit.min(1000),
        offset,
    );

    // Capped count: an exact count() re-scans the entire lookback window with the same
    // predicate as the data fetch (doubling the work) just to render "N results". Wrap
    // in a subquery with LIMIT so ClickHouse stops reading once the cap is reached.
    // The UI can render this as "10000+". For needle searches (few matches) the cost
    // is dominated by skip-index pruning anyway; for common terms it short-circuits.
    const COUNT_CAP: u64 = 10_000;
    let count_sql = format!(
        "SELECT count() as count FROM (SELECT 1 FROM wide_events {} LIMIT {COUNT_CAP})",
        clauses.to_sql(),
    );

    // P0: Run data fetch and count in parallel
    let (rows_result, count_result) = tokio::join!(
        crate::tenant_query(&state.ch, &sql, tenant_id).fetch_all::<WideEvent>(),
        crate::tenant_query(&state.ch, &count_sql, tenant_id).fetch_one::<CountRow>(),
    );

    let rows = rows_result.map_err(|e| {
        tracing::error!(error = %e, signal = "traces", handler = "execute_query", "query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
    })?;

    let total = count_result.map(|r| r.count).unwrap_or(0);

    // Only track usage if the query returned results
    if total > 0 {
        let filter_pairs: Vec<(String, String)> = req.filters.iter()
            .map(|f| (f.field.clone(), f.value.as_str().unwrap_or_default().to_string()))
            .collect();
        let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
        state.usage.track_many(signals, "span", "explore");
    }

    tracing::info!(
        signal = "traces",
        tenant_id = %tenant_id,
        query = "explore",
        rows = rows.len(),
        total = total,
        duration_ms = start.elapsed().as_millis() as u64,
        filters = req.filters.len(),
        "query completed"
    );

    #[derive(serde::Serialize)]
    struct Resp { rows: Vec<WideEvent>, total: u64 }
    Ok(Json(Resp { rows, total }))
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

    let interval_fn = match req.interval.as_str() {
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
         FROM wide_events {} \
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
         FROM wide_events {} \
         GROUP BY {group_by} \
         ORDER BY count DESC \
         LIMIT {}",
        clauses.to_sql(),
        req.limit.min(1000),
    );

    if req.group_by.len() == 1 {
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
    } else {
        Ok(Json(serde_json::json!({
            "sql": sql,
            "note": "Execute via ClickHouse HTTP interface with FORMAT JSONEachRow for multi-column group-by"
        })))
    }
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

    let interval_fn = match req.interval.as_str() {
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
             FROM wide_events {} \
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
             FROM wide_events {} \
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
