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
                FROM service_catalog
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
         FROM wide_events \
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
            FROM wide_events \
            PREWHERE tenant_id = '{escaped_tenant}' \
                AND timestamp >= now() - INTERVAL {minutes} MINUTE \
         ) child \
         INNER JOIN ( \
            SELECT trace_id, span_id, service_name AS parent_svc \
            FROM wide_events \
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
