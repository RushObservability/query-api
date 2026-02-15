use axum::{Json, extract::{Query, State}, http::StatusCode, response::IntoResponse};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::AppState;

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
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let rows = state
        .ch
        .query(
            "SELECT
                service_name,
                http_path,
                http_method,
                toString(last_seen) as last_seen,
                request_count
            FROM service_catalog
            ORDER BY service_name, http_path",
        )
        .fetch_all::<ServiceEntry>()
        .await
        .map_err(|e| {
            tracing::error!("Failed to list services: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("query failed: {e}"),
            )
        })?;

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
    Query(params): Query<GraphParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let minutes = params.minutes.min(10080); // max 7d

    // Node metrics: per-service aggregate
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
         WHERE timestamp >= now() - INTERVAL {minutes} MINUTE \
         GROUP BY service_name \
         ORDER BY request_count DESC"
    );

    // Edge metrics: join child spans to parent spans across services
    let edge_sql = format!(
        "SELECT \
            parent.service_name as source, \
            child.service_name as target, \
            count() as request_count, \
            countIf(child.http_status_code >= 500 OR child.status = 'ERROR') as error_count, \
            avg(child.duration_ns) / 1000000.0 as avg_duration_ms \
         FROM wide_events child \
         INNER JOIN wide_events parent \
            ON child.trace_id = parent.trace_id \
            AND child.parent_span_id = parent.span_id \
         WHERE child.timestamp >= now() - INTERVAL {minutes} MINUTE \
            AND parent.timestamp >= now() - INTERVAL {minutes} MINUTE \
            AND parent.service_name != child.service_name \
         GROUP BY source, target \
         ORDER BY request_count DESC"
    );

    let (nodes_result, edges_result) = tokio::join!(
        state.ch.query(&node_sql).fetch_all::<GraphNode>(),
        state.ch.query(&edge_sql).fetch_all::<GraphEdge>(),
    );

    let nodes = nodes_result.map_err(|e| {
        tracing::error!("Service graph nodes query failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
    })?;

    let edges = edges_result.map_err(|e| {
        tracing::error!("Service graph edges query failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
    })?;

    Ok(Json(ServiceGraph { nodes, edges }))
}
