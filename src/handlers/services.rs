use axum::{
    Extension, Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::AppState;
use crate::TenantContext;

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct ServiceEntry {
    pub service_name: String,
    pub http_path: String,
    pub http_method: String,
    pub last_seen: String,
    pub request_count: u64,
}

#[derive(Debug, Serialize)]
pub struct ServicesResponse {
    pub services: Vec<ServiceEntry>,
}

pub async fn list_services(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let rows = crate::tenant_query(
        &state.ch,
        &format!(
            "SELECT
                    service_name,
                    http_path,
                    http_method,
                    toString(last_seen) as last_seen,
                    request_count
                FROM services
                WHERE tenant_id = '{escaped_tenant}'
                ORDER BY service_name, http_path",
        ),
        tenant_id,
    )
    .fetch_all::<ServiceEntry>()
    .await
    .map_err(|e| {
        tracing::error!(error = %e, handler = "list_services", "query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
    })?;

    tracing::info!(
        tenant_id = %tenant_id,
        services = rows.len(),
        "listed services"
    );

    Ok(Json(ServicesResponse { services: rows }))
}

// ═══ Service Graph ═══

#[derive(Debug, Deserialize)]
pub struct GraphParams {
    #[serde(default = "default_minutes")]
    pub minutes: u64,
}

fn default_minutes() -> u64 {
    60
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct GraphNode {
    pub service_name: String,
    pub request_count: u64,
    pub error_count: u64,
    pub avg_duration_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    pub request_count: u64,
    pub error_count: u64,
    pub avg_duration_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct ServiceGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

pub async fn service_graph(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(params): Query<GraphParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let minutes = params.minutes.min(10080); // max 7d

    // Node metrics: per-service aggregate.
    // PREWHERE on tenant_id + timestamp: ClickHouse reads only those compact columns first,
    // eliminating non-matching granules before loading the wider row data.
    let node_sql = format!(
        "SELECT \
            service_name, \
            count() as request_count, \
            countIf(http_status_code >= 500 OR status = 'ERROR') as error_count, \
            avg(duration_ns) / 1000000.0 as avg_duration_ms, \
            quantile(0.5)(duration_ns) / 1000000.0 as p50_ms, \
            quantile(0.95)(duration_ns) / 1000000.0 as p95_ms, \
            quantile(0.99)(duration_ns) / 1000000.0 as p99_ms \
         FROM spans \
         PREWHERE tenant_id = '{escaped_tenant}' \
            AND timestamp >= now() - INTERVAL {minutes} MINUTE \
         GROUP BY service_name \
         ORDER BY request_count DESC"
    );

    // Edge metrics: join child spans to parent spans across services.
    // Each side of the JOIN uses a subquery with PREWHERE so tenant + time filtering
    // happens before the full row is read, avoiding an unfiltered cross-table scan.
    let edge_sql = format!(
        "SELECT \
            parent_svc as source, \
            child_svc as target, \
            count() as request_count, \
            countIf(child_err) as error_count, \
            avg(child_dur) / 1000000.0 as avg_duration_ms \
         FROM ( \
            SELECT \
                trace_id, span_id, service_name AS child_svc, parent_span_id, \
                (http_status_code >= 500 OR status = 'ERROR') AS child_err, \
                duration_ns AS child_dur \
            FROM spans \
            PREWHERE tenant_id = '{escaped_tenant}' \
                AND timestamp >= now() - INTERVAL {minutes} MINUTE \
         ) child \
         INNER JOIN ( \
            SELECT trace_id, span_id, service_name AS parent_svc \
            FROM spans \
            PREWHERE tenant_id = '{escaped_tenant}' \
                AND timestamp >= now() - INTERVAL {minutes} MINUTE \
         ) parent \
            ON child.trace_id = parent.trace_id \
            AND child.parent_span_id = parent.span_id \
         WHERE parent_svc != child_svc \
         GROUP BY source, target \
         ORDER BY request_count DESC"
    );

    let (nodes_result, edges_result) = tokio::join!(
        crate::tenant_query(&state.ch, &node_sql, tenant_id).fetch_all::<GraphNode>(),
        crate::tenant_query(&state.ch, &edge_sql, tenant_id).fetch_all::<GraphEdge>(),
    );

    let nodes = nodes_result.map_err(|e| {
        tracing::error!(error = %e, handler = "service_graph", "nodes query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
    })?;

    let edges = edges_result.map_err(|e| {
        tracing::error!(error = %e, handler = "service_graph", "edges query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
    })?;

    Ok(Json(ServiceGraph { nodes, edges }))
}

// ═══ Application vs database time ═══
// This is deliberately a trace-level breakdown rather than a sum of all span
// durations. A request's server span is wall-clock time; child spans overlap
// with it and must not be added on top. Database time is capped at the request
// duration so parallel database calls do not make the composition exceed 100%.

#[derive(Debug, Deserialize)]
pub struct TimeBreakdownParams {
    #[serde(default = "default_minutes")]
    pub minutes: u64,
    pub service: String,
    #[serde(default = "default_breakdown_interval")]
    pub interval: String,
}

fn default_breakdown_interval() -> String {
    "1m".to_string()
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct TimeBreakdownTotals {
    pub request_count: u64,
    pub wall_time_ms: f64,
    pub application_time_ms: f64,
    pub database_time_ms: f64,
    pub database_call_time_ms: f64,
    pub database_calls: u64,
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct DatabaseTimeRow {
    pub system: String,
    pub target: String,
    pub calls: u64,
    pub total_ms: f64,
    pub p95_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct ServiceTimeBreakdownResponse {
    pub service_name: String,
    pub minutes: u64,
    pub request_count: u64,
    pub wall_time_ms: f64,
    pub application_time_ms: f64,
    pub database_time_ms: f64,
    /// Raw sum of database child-span durations. This can exceed wall time
    /// when calls run in parallel, so the UI presents it as supporting detail.
    pub database_call_time_ms: f64,
    pub database_calls: u64,
    pub databases: Vec<DatabaseTimeRow>,
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct ServiceTimeBreakdownBucket {
    pub bucket: String,
    pub request_count: u64,
    /// Average wall-clock time per request in this interval.
    pub wall_time_ms: f64,
    /// Average application time per request in this interval.
    pub application_time_ms: f64,
    /// Average database wall-clock impact per request in this interval.
    pub database_time_ms: f64,
    pub database_calls: u64,
}

#[derive(Debug, Deserialize, Row)]
struct ServiceTimeBreakdownServerBucket {
    pub bucket: String,
    pub request_count: u64,
    pub wall_time_ms: f64,
}

#[derive(Debug, Deserialize, Row)]
struct ServiceTimeBreakdownDatabaseBucket {
    pub bucket: String,
    pub database_call_time_ms: f64,
    pub database_calls: u64,
}

#[derive(Debug, Serialize)]
pub struct ServiceTimeBreakdownTimeseriesResponse {
    pub service_name: String,
    pub minutes: u64,
    pub interval: String,
    pub buckets: Vec<ServiceTimeBreakdownBucket>,
}

fn database_span_predicate() -> &'static str {
    "(JSONExtractString(attributes, 'db.system') != '' \
        OR JSONExtractString(attributes, 'db.system.name') != '' \
        OR JSONExtractString(attributes, 'db.operation.name') != '' \
        OR JSONExtractString(attributes, 'db.operation') != '' \
        OR JSONExtractString(attributes, 'db.statement') != '' \
        OR JSONExtractString(attributes, 'db.query.summary') != '')"
}

/// Return a safe ClickHouse bucket expression and the effective interval token.
/// Keep the result under 2,000 buckets even if a caller asks for a very fine
/// interval over the full seven-day API window.
fn breakdown_interval(minutes: u64, requested: &str) -> (&'static str, u64) {
    const INTERVALS: [(&str, u64); 11] = [
        ("1s", 1),
        ("10s", 10),
        ("1m", 60),
        ("2m", 120),
        ("5m", 300),
        ("10m", 600),
        ("15m", 900),
        ("30m", 1800),
        ("1h", 3600),
        ("3h", 10800),
        ("1d", 86400),
    ];
    let requested_secs = INTERVALS
        .iter()
        .find(|(token, _)| *token == requested)
        .map(|(_, secs)| *secs)
        .unwrap_or(60);
    let minimum_secs = (minutes.max(1) * 60).div_ceil(2000);
    let target_secs = requested_secs.max(minimum_secs);
    let selected = INTERVALS
        .iter()
        .find(|(_, secs)| *secs >= target_secs)
        .copied()
        .unwrap_or(INTERVALS[INTERVALS.len() - 1]);
    selected
}

pub async fn service_time_breakdown(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(params): Query<TimeBreakdownParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let escaped_service = crate::query_builder::escape_string_literal(&params.service);
    let minutes = params.minutes.min(10080); // max 7d

    let db_predicate = database_span_predicate();

    // Roll spans up to one row per trace first. `server_ns` is the preferred
    // request wall time; the max span is a useful fallback for instrumentations
    // that omit span kind. `least` caps overlapping DB children to wall time.
    let totals_sql = format!(
        "SELECT \
            count() AS request_count, \
            sum(request_ns) / 1000000.0 AS wall_time_ms, \
            sum(request_ns - least(database_child_ns, request_ns)) / 1000000.0 AS application_time_ms, \
            sum(least(database_child_ns, request_ns)) / 1000000.0 AS database_time_ms, \
            sum(database_child_ns) / 1000000.0 AS database_call_time_ms, \
            sum(database_calls) AS database_calls \
         FROM ( \
            SELECT \
                if(server_ns > 0, server_ns, max_span_ns) AS request_ns, \
                database_child_ns, \
                database_calls \
            FROM ( \
                SELECT \
                    trace_id, \
                    max(duration_ns) AS max_span_ns, \
                    maxIf(duration_ns, kind = 'SPAN_KIND_SERVER') AS server_ns, \
                    sumIf(duration_ns, {db_predicate}) AS database_child_ns, \
                    countIf({db_predicate}) AS database_calls \
                FROM spans \
                PREWHERE tenant_id = '{escaped_tenant}' \
                    AND service_name = '{escaped_service}' \
                    AND timestamp >= now() - INTERVAL {minutes} MINUTE \
                GROUP BY trace_id \
            ) \
         )"
    );

    let databases_sql = format!(
        "SELECT \
            system, \
            target, \
            count() AS calls, \
            sum(duration_ns) / 1000000.0 AS total_ms, \
            quantile(0.95)(duration_ns) / 1000000.0 AS p95_ms \
         FROM ( \
            SELECT \
                if(JSONExtractString(attributes, 'db.system') != '', JSONExtractString(attributes, 'db.system'), \
                    if(JSONExtractString(attributes, 'db.system.name') != '', JSONExtractString(attributes, 'db.system.name'), 'database')) AS system, \
                multiIf( \
                    JSONExtractString(attributes, 'server.address') != '', JSONExtractString(attributes, 'server.address'), \
                    JSONExtractString(attributes, 'db.namespace') != '', JSONExtractString(attributes, 'db.namespace'), \
                    JSONExtractString(attributes, 'db.name') != '', JSONExtractString(attributes, 'db.name'), \
                    JSONExtractString(attributes, 'db.system') != '', JSONExtractString(attributes, 'db.system'), \
                    JSONExtractString(attributes, 'db.system.name') != '', JSONExtractString(attributes, 'db.system.name'), \
                    'database') AS target, \
                duration_ns \
            FROM spans \
            PREWHERE tenant_id = '{escaped_tenant}' \
                AND service_name = '{escaped_service}' \
                AND timestamp >= now() - INTERVAL {minutes} MINUTE \
            WHERE {db_predicate} \
         ) \
         GROUP BY system, target \
         ORDER BY total_ms DESC \
         LIMIT 20"
    );

    let (totals_result, databases_result) = tokio::join!(
        crate::tenant_query(&state.ch, &totals_sql, tenant_id).fetch_one::<TimeBreakdownTotals>(),
        crate::tenant_query(&state.ch, &databases_sql, tenant_id).fetch_all::<DatabaseTimeRow>(),
    );

    let totals = totals_result.map_err(|e| {
        tracing::error!(error = %e, handler = "service_time_breakdown", "totals query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
    })?;
    let databases = databases_result.map_err(|e| {
        tracing::error!(error = %e, handler = "service_time_breakdown", "database query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
    })?;

    Ok(Json(ServiceTimeBreakdownResponse {
        service_name: params.service,
        minutes,
        request_count: totals.request_count,
        wall_time_ms: totals.wall_time_ms,
        application_time_ms: totals.application_time_ms,
        database_time_ms: totals.database_time_ms,
        database_call_time_ms: totals.database_call_time_ms,
        database_calls: totals.database_calls,
        databases,
    }))
}

pub async fn service_time_breakdown_timeseries(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(params): Query<TimeBreakdownParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let escaped_service = crate::query_builder::escape_string_literal(&params.service);
    let minutes = params.minutes.min(10080); // max 7d
    let (interval, bucket_secs) = breakdown_interval(minutes, &params.interval);
    let db_predicate = database_span_predicate();

    let server_sql = format!(
        "SELECT \
            toString(toStartOfInterval(timestamp, INTERVAL {bucket_secs} SECOND)) AS bucket, \
            count() AS request_count, \
            avg(duration_ns) / 1000000.0 AS wall_time_ms \
         FROM spans \
         PREWHERE tenant_id = '{escaped_tenant}' \
            AND service_name = '{escaped_service}' \
            AND timestamp >= now() - INTERVAL {minutes} MINUTE \
         WHERE kind = 'SPAN_KIND_SERVER' \
         GROUP BY bucket \
         ORDER BY bucket"
    );

    let database_sql = format!(
        "SELECT \
            toString(toStartOfInterval(timestamp, INTERVAL {bucket_secs} SECOND)) AS bucket, \
            sum(duration_ns) / 1000000.0 AS database_call_time_ms, \
            count() AS database_calls \
         FROM spans \
         PREWHERE tenant_id = '{escaped_tenant}' \
            AND service_name = '{escaped_service}' \
            AND timestamp >= now() - INTERVAL {minutes} MINUTE \
         WHERE {db_predicate} \
         GROUP BY bucket \
         ORDER BY bucket"
    );

    let (server_result, database_result) = tokio::join!(
        crate::tenant_query(&state.ch, &server_sql, tenant_id)
            .fetch_all::<ServiceTimeBreakdownServerBucket>(),
        crate::tenant_query(&state.ch, &database_sql, tenant_id)
            .fetch_all::<ServiceTimeBreakdownDatabaseBucket>(),
    );

    let server_buckets = server_result.map_err(|e| {
        tracing::error!(error = %e, handler = "service_time_breakdown_timeseries", query = "server", "query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
    })?;
    let database_buckets = database_result.map_err(|e| {
        tracing::error!(error = %e, handler = "service_time_breakdown_timeseries", query = "database", "query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
    })?;

    let database_by_bucket: BTreeMap<_, _> = database_buckets
        .into_iter()
        .map(|bucket| (bucket.bucket.clone(), bucket))
        .collect();

    let buckets = server_buckets
        .into_iter()
        .map(|server| {
            let database = database_by_bucket.get(&server.bucket);
            let database_time_ms = database
                .map(|bucket| bucket.database_call_time_ms.min(server.wall_time_ms))
                .unwrap_or(0.0);
            ServiceTimeBreakdownBucket {
                bucket: server.bucket,
                request_count: server.request_count,
                wall_time_ms: server.wall_time_ms,
                application_time_ms: (server.wall_time_ms - database_time_ms).max(0.0),
                database_time_ms,
                database_calls: database.map(|bucket| bucket.database_calls).unwrap_or(0),
            }
        })
        .collect();

    Ok(Json(ServiceTimeBreakdownTimeseriesResponse {
        service_name: params.service,
        minutes,
        interval: interval.to_string(),
        buckets,
    }))
}

// ═══ Latency Histogram ═══
// Full latency *distribution* for a single service, complementing the
// percentile timeseries (which shows P50/P95/P99 over time but hides the shape
// of the distribution — bimodality, long tails, fast-path/slow-path splits).
// Durations are bucketed by log2(ms) so a single set of exponentially-growing
// buckets covers microseconds to seconds without configuration. The frontend
// maps each exponent `e` to the half-open range [2^e, 2^(e+1)) ms.

#[derive(Debug, Deserialize)]
pub struct LatencyHistParams {
    #[serde(default = "default_minutes")]
    pub minutes: u64,
    pub service: String,
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct LatencyHistBucket {
    /// log2(duration_ms) bucket exponent; range [2^exp, 2^(exp+1)) ms.
    pub exp: i32,
    pub count: u64,
}

#[derive(Debug, Deserialize, Row)]
struct LatencyPercentilesRow {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    total: u64,
}

#[derive(Debug, Serialize)]
pub struct LatencyHistResponse {
    pub buckets: Vec<LatencyHistBucket>,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub total: u64,
}

pub async fn service_latency_histogram(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(params): Query<LatencyHistParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let escaped_service = crate::query_builder::escape_string_literal(&params.service);
    let minutes = params.minutes.min(10080); // max 7d

    // Bucket by log2(ms). greatest(..., 0.001) floors at 1µs so log2 stays finite
    // for zero/near-zero durations; those land in the smallest bucket.
    let bucket_sql = format!(
        "SELECT \
            toInt32(floor(log2(greatest(duration_ns / 1000000.0, 0.001)))) AS exp, \
            count() AS count \
         FROM spans \
         PREWHERE tenant_id = '{escaped_tenant}' \
            AND service_name = '{escaped_service}' \
            AND timestamp >= now() - INTERVAL {minutes} MINUTE \
         GROUP BY exp \
         ORDER BY exp"
    );

    // Exact percentiles over the same window, drawn as markers on the histogram.
    let pct_sql = format!(
        "SELECT \
            quantile(0.5)(duration_ns) / 1000000.0 AS p50_ms, \
            quantile(0.95)(duration_ns) / 1000000.0 AS p95_ms, \
            quantile(0.99)(duration_ns) / 1000000.0 AS p99_ms, \
            count() AS total \
         FROM spans \
         PREWHERE tenant_id = '{escaped_tenant}' \
            AND service_name = '{escaped_service}' \
            AND timestamp >= now() - INTERVAL {minutes} MINUTE"
    );

    let (buckets_res, pct_res) = tokio::join!(
        crate::tenant_query(&state.ch, &bucket_sql, tenant_id).fetch_all::<LatencyHistBucket>(),
        crate::tenant_query(&state.ch, &pct_sql, tenant_id).fetch_one::<LatencyPercentilesRow>(),
    );

    let buckets = buckets_res.map_err(|e| {
        tracing::error!(error = %e, handler = "service_latency_histogram", "buckets query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
    })?;

    // No rows ⇒ no traffic in window; return an empty distribution rather than 500.
    let pct = pct_res.unwrap_or(LatencyPercentilesRow {
        p50_ms: 0.0,
        p95_ms: 0.0,
        p99_ms: 0.0,
        total: 0,
    });

    Ok(Json(LatencyHistResponse {
        buckets,
        p50_ms: pct.p50_ms,
        p95_ms: pct.p95_ms,
        p99_ms: pct.p99_ms,
        total: pct.total,
    }))
}

// ═══ Endpoint / Operation breakdown ═══
// Per-service RED (Rate / Errors / Duration) broken down by endpoint or
// operation, so a single bad route is visible instead of being averaged into
// the service's overall numbers.
//   mode=server (default): the service's own HTTP entry points — SPAN_KIND_SERVER
//     spans grouped by (method, path); path is already templated (/articles/:id).
//   mode=operation: downstream work this service does — non-server spans grouped
//     by span_name (db.select, cache.set, …), excluding framework noise
//     (middleware / request-handler internal spans).

#[derive(Debug, Deserialize)]
pub struct EndpointParams {
    #[serde(default = "default_minutes")]
    pub minutes: u64,
    pub service: String,
    #[serde(default = "default_endpoint_mode")]
    pub mode: String,
}

fn default_endpoint_mode() -> String {
    "server".to_string()
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct EndpointRow {
    pub endpoint: String,
    pub method: String,
    pub path: String,
    pub req: u64,
    pub errors: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct EndpointsResponse {
    pub endpoints: Vec<EndpointRow>,
    pub mode: String,
}

pub async fn service_endpoints(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(params): Query<EndpointParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let escaped_service = crate::query_builder::escape_string_literal(&params.service);
    let minutes = params.minutes.min(10080); // max 7d
    let operation_mode = params.mode == "operation";

    // Common RED aggregates; only the grouping key + filter differ by mode.
    let err_expr = "countIf(http_status_code >= 500 OR status = 'ERROR') AS errors";
    let pct = "quantile(0.5)(duration_ns)/1000000.0 AS p50_ms, \
               quantile(0.95)(duration_ns)/1000000.0 AS p95_ms, \
               quantile(0.99)(duration_ns)/1000000.0 AS p99_ms";

    let sql = if operation_mode {
        format!(
            "SELECT span_name AS endpoint, '' AS method, '' AS path, \
                count() AS req, {err_expr}, {pct} \
             FROM spans \
             PREWHERE tenant_id = '{escaped_tenant}' \
                AND service_name = '{escaped_service}' \
                AND timestamp >= now() - INTERVAL {minutes} MINUTE \
             WHERE kind != 'SPAN_KIND_SERVER' \
                AND span_name NOT LIKE 'middleware%' \
                AND span_name NOT LIKE 'request handler%' \
             GROUP BY span_name \
             ORDER BY req DESC \
             LIMIT 100"
        )
    } else {
        format!(
            "SELECT concat(http_method, ' ', http_path) AS endpoint, http_method AS method, http_path AS path, \
                count() AS req, {err_expr}, {pct} \
             FROM spans \
             PREWHERE tenant_id = '{escaped_tenant}' \
                AND service_name = '{escaped_service}' \
                AND timestamp >= now() - INTERVAL {minutes} MINUTE \
             WHERE kind = 'SPAN_KIND_SERVER' \
             GROUP BY http_method, http_path \
             ORDER BY req DESC \
             LIMIT 100"
        )
    };

    let endpoints = crate::tenant_query(&state.ch, &sql, tenant_id)
        .fetch_all::<EndpointRow>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, handler = "service_endpoints", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;

    Ok(Json(EndpointsResponse {
        endpoints,
        mode: if operation_mode {
            "operation".into()
        } else {
            "server".into()
        },
    }))
}

// ═══ Top Errors (error grouping) ═══
// What's actually failing for a service, grouped two ways:
//   mode=endpoint (default): errored SERVER spans grouped by (status, method, path)
//     — "503 POST /articles ×80". Always available from trace data.
//   mode=message: ERROR/WARN/FATAL logs grouped by a normalized message template
//     (UUIDs → UUID, digits → N) so "DB pool exhausted: 0/20" and "…2/20" collapse
//     into one group — the classic error-grouping / issues view.

#[derive(Debug, Deserialize)]
pub struct ErrorsParams {
    #[serde(default = "default_minutes")]
    pub minutes: u64,
    pub service: String,
    #[serde(default = "default_errors_mode")]
    pub mode: String,
}

fn default_errors_mode() -> String {
    "endpoint".to_string()
}

// One row per error group. Column order/types are identical across both queries
// so a single Row struct deserializes either mode (unused fields filled with
// constants in SQL).
#[derive(Debug, Serialize, Deserialize, Row)]
pub struct ErrorGroup {
    pub key: String,
    pub status_code: u16,
    pub method: String,
    pub path: String,
    pub severity: String,
    pub example: String,
    pub count: u64,
    pub last_seen: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorsResponse {
    pub groups: Vec<ErrorGroup>,
    pub mode: String,
}

pub async fn service_errors(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(params): Query<ErrorsParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let escaped_service = crate::query_builder::escape_string_literal(&params.service);
    let minutes = params.minutes.min(10080); // max 7d
    let message_mode = params.mode == "message";

    let sql = if message_mode {
        // Normalize: collapse UUIDs then runs of digits so message variants group.
        let template = "replaceRegexpAll(replaceRegexpAll(Body, \
            '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', 'UUID'), \
            '[0-9]+', 'N')";
        format!(
            "SELECT {template} AS key, \
                toUInt16(0) AS status_code, '' AS method, '' AS path, \
                argMax(SeverityText, SeverityNumber) AS severity, \
                argMax(Body, Timestamp) AS example, \
                count() AS count, \
                toString(max(Timestamp)) AS last_seen \
             FROM logs \
             PREWHERE tenant_id = '{escaped_tenant}' \
                AND ServiceName = '{escaped_service}' \
                AND Timestamp >= now() - INTERVAL {minutes} MINUTE \
             WHERE SeverityText IN ('ERROR', 'WARN', 'FATAL') \
             GROUP BY key \
             ORDER BY count DESC \
             LIMIT 50"
        )
    } else {
        format!(
            "SELECT concat(toString(http_status_code), ' ', http_method, ' ', http_path) AS key, \
                http_status_code AS status_code, http_method AS method, http_path AS path, \
                '' AS severity, '' AS example, \
                count() AS count, \
                toString(max(timestamp)) AS last_seen \
             FROM spans \
             PREWHERE tenant_id = '{escaped_tenant}' \
                AND service_name = '{escaped_service}' \
                AND timestamp >= now() - INTERVAL {minutes} MINUTE \
             WHERE kind = 'SPAN_KIND_SERVER' \
                AND (status = 'ERROR' OR http_status_code >= 500) \
             GROUP BY http_status_code, http_method, http_path \
             ORDER BY count DESC \
             LIMIT 50"
        )
    };

    let groups = crate::tenant_query(&state.ch, &sql, tenant_id)
        .fetch_all::<ErrorGroup>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, handler = "service_errors", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "query failed".into())
        })?;

    Ok(Json(ErrorsResponse {
        groups,
        mode: if message_mode {
            "message".into()
        } else {
            "endpoint".into()
        },
    }))
}
