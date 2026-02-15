use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
};

use crate::AppState;
use crate::models::query::{
    CountBucket, CountQueryRequest, CountRow, GroupedTimeseriesBucket, QueryRequest, QueryResponse,
    TimeseriesBucket, TimeseriesRequest,
};
use crate::models::trace::WideEvent;
use crate::query_builder::{resolve_field, build_where_clause_with_search};

/// Execute a structured query against wide_events.
pub async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let where_clause = build_where_clause_with_search(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref());

    let sql = format!(
        "SELECT * FROM wide_events WHERE {where_clause} ORDER BY timestamp DESC LIMIT {} OFFSET {}",
        req.limit.min(1000),
        req.offset,
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<WideEvent>()
        .await
        .map_err(|e| {
            tracing::error!("Query failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("query failed: {e}"),
            )
        })?;

    // Convert to JSON values for flexible response
    let json_rows: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
        .collect();

    // Get total count
    let count_sql = format!("SELECT count() as count FROM wide_events WHERE {where_clause}");
    let total: u64 = state
        .ch
        .query(&count_sql)
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0);

    Ok(Json(QueryResponse {
        rows: json_rows,
        total,
    }))
}

/// Count events bucketed by time interval.
pub async fn count_query(
    State(state): State<AppState>,
    Json(req): Json<CountQueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let where_clause = build_where_clause_with_search(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref());

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
         FROM wide_events \
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
            tracing::error!("Count query failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("query failed: {e}"),
            )
        })?;

    Ok(Json(buckets))
}

/// Group-by query for breakdowns.
pub async fn group_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if req.group_by.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "group_by must have at least one field".to_string(),
        ));
    }

    let where_clause = build_where_clause_with_search(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref());

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
         FROM wide_events \
         WHERE {where_clause} \
         GROUP BY {group_by} \
         ORDER BY count DESC \
         LIMIT {}",
        req.limit.min(1000),
    );

    if req.group_by.len() == 1 {
        #[derive(Debug, serde::Serialize, serde::Deserialize, clickhouse::Row)]
        struct SingleGroupRow {
            group_0: String,
            count: u64,
        }

        let rows = state
            .ch
            .query(&sql)
            .fetch_all::<SingleGroupRow>()
            .await
            .map_err(|e| {
                tracing::error!("Group query failed: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("query failed: {e}"),
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
    Json(req): Json<TimeseriesRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let where_clause = build_where_clause_with_search(&req.filters, &req.time_range.from, &req.time_range.to, req.search.as_deref());

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
             FROM wide_events \
             WHERE {where_clause} \
             GROUP BY bucket, group_key \
             ORDER BY bucket ASC, count DESC"
        );

        let buckets = state
            .ch
            .query(&sql)
            .fetch_all::<GroupedTimeseriesBucket>()
            .await
            .map_err(|e| {
                tracing::error!("Grouped timeseries query failed: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
            })?;

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
             FROM wide_events \
             WHERE {where_clause} \
             GROUP BY bucket \
             ORDER BY bucket ASC"
        );

        let buckets = state
            .ch
            .query(&sql)
            .fetch_all::<TimeseriesBucket>()
            .await
            .map_err(|e| {
                tracing::error!("Timeseries query failed: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
            })?;

        Ok(Json(serde_json::json!({ "buckets": buckets, "grouped": false })))
    }
}

