use axum::{Json, extract::{Query, State}, http::StatusCode, response::IntoResponse, Extension};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

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
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "query failed".into(),
            )
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
    let pct = pct_res.unwrap_or(LatencyPercentilesRow { p50_ms: 0.0, p95_ms: 0.0, p99_ms: 0.0, total: 0 });

    Ok(Json(LatencyHistResponse {
        buckets,
        p50_ms: pct.p50_ms,
        p95_ms: pct.p95_ms,
        p99_ms: pct.p99_ms,
        total: pct.total,
    }))
}
