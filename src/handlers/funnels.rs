use axum::{
    Json,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use crate::{AppState, TenantContext};
use crate::handlers::users::{require_auth, require_write};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FunnelStep {
    pub label: String,
    pub service_name: Option<String>,
    pub http_path_prefix: Option<String>,
    pub min_status_code: Option<u16>,
    pub max_status_code: Option<u16>,
}

#[derive(Debug, Deserialize)]
pub struct CreateFunnelRequest {
    pub name: String,
    pub steps: Vec<FunnelStep>,
}

#[derive(Debug, Deserialize)]
pub struct RunFunnelRequest {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Serialize)]
pub struct FunnelResponse {
    pub id: String,
    pub name: String,
    pub steps: Vec<FunnelStep>,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct FunnelResultStep {
    pub label: String,
    pub count: u64,
    pub pct_of_first: f64,
    pub pct_of_prev: f64,
    pub drop_off: u64,
}

#[derive(Debug, Serialize)]
pub struct FunnelResult {
    pub funnel_id: String,
    pub steps: Vec<FunnelResultStep>,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct TraceCountRow {
    count: u64,
}

fn step_where_clause(step: &FunnelStep, from: &str, to: &str) -> String {
    let mut conditions = vec![
        format!("timestamp >= '{}' AND timestamp <= '{}'", from.replace('\'', ""), to.replace('\'', "")),
    ];
    if let Some(svc) = &step.service_name {
        let safe = svc.replace('\'', "''");
        conditions.push(format!("service_name = '{safe}'"));
    }
    if let Some(prefix) = &step.http_path_prefix {
        let safe = prefix.replace('\'', "''").replace('%', "\\%");
        conditions.push(format!("http_path LIKE '{safe}%'"));
    }
    if let Some(min) = step.min_status_code {
        conditions.push(format!("http_status_code >= {min}"));
    }
    if let Some(max) = step.max_status_code {
        conditions.push(format!("http_status_code <= {max}"));
    }
    format!("WHERE {}", conditions.join(" AND "))
}

pub async fn list_funnels(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let rows = state.config_db.list_funnels(&tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let funnels: Vec<FunnelResponse> = rows.into_iter().filter_map(|(id, name, steps_json, created_at)| {
        let steps: Vec<FunnelStep> = serde_json::from_str(&steps_json).ok()?;
        Some(FunnelResponse { id, name, steps, created_at })
    }).collect();
    Ok(Json(serde_json::json!({ "funnels": funnels })))
}

pub async fn create_funnel(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(req): Json<CreateFunnelRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    if req.name.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name required".to_string()));
    }
    if req.name.len() > 255 {
        return Err((StatusCode::BAD_REQUEST, "name must not exceed 255 characters".to_string()));
    }
    if req.steps.len() < 2 {
        return Err((StatusCode::BAD_REQUEST, "funnel requires at least 2 steps".to_string()));
    }
    if req.steps.len() > 10 {
        return Err((StatusCode::BAD_REQUEST, "funnel supports at most 10 steps".to_string()));
    }
    let steps_json = serde_json::to_string(&req.steps)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let id = uuid::Uuid::new_v4().to_string();
    state.config_db.create_funnel(&id, &req.name, &steps_json, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((StatusCode::CREATED, Json(serde_json::json!({ "id": id, "ok": true }))))
}

pub async fn delete_funnel(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let deleted = state.config_db.delete_funnel(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "funnel not found".to_string()));
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn run_funnel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    Json(req): Json<RunFunnelRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let row = state.config_db.get_funnel(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "funnel not found".to_string()))?;

    let steps: Vec<FunnelStep> = serde_json::from_str(&row.2)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut step_counts: Vec<u64> = Vec::with_capacity(steps.len());

    for step in &steps {
        let where_clause = step_where_clause(step, &req.from, &req.to);
        let sql = format!(
            "SELECT count(DISTINCT trace_id) as count FROM wide_events {where_clause}"
        );
        let count = crate::tenant_query(&state.ch, &sql, &tenant.tenant_id)
            .fetch_one::<TraceCountRow>()
            .await
            .map(|r| r.count)
            .unwrap_or(0);
        step_counts.push(count);
    }

    let first = *step_counts.first().unwrap_or(&0) as f64;
    let mut result_steps: Vec<FunnelResultStep> = Vec::new();
    let mut prev = 0u64;

    for (i, (step, &count)) in steps.iter().zip(step_counts.iter()).enumerate() {
        let pct_of_first = if first > 0.0 { (count as f64 / first) * 100.0 } else { 0.0 };
        let pct_of_prev = if i == 0 { 100.0 } else if prev > 0 { (count as f64 / prev as f64) * 100.0 } else { 0.0 };
        let drop_off = if i == 0 { 0 } else { prev.saturating_sub(count) };
        result_steps.push(FunnelResultStep {
            label: step.label.clone(),
            count,
            pct_of_first,
            pct_of_prev,
            drop_off,
        });
        prev = count;
    }

    Ok(Json(FunnelResult { funnel_id: id, steps: result_steps }))
}
