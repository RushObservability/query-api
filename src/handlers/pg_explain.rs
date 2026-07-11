//! PostgreSQL EXPLAIN job queue.
//!
//! The collector is push-only, so query-plan requests flow through a poll-based
//! queue: the UI submits a job, the collector polls (`/poll`), runs a plain
//! `EXPLAIN (FORMAT JSON)`, and posts the result (`/result`); the UI polls `/{id}`.
//! Tenant is resolved by the global middleware (UI session OR collector Bearer key).
use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::{AppState, TenantContext};

#[derive(Deserialize)]
pub struct SubmitBody {
    pub server: String,
    pub query: String,
}

/// Reject anything that isn't a single, non-EXPLAIN statement.
fn validate_query(q: &str) -> Result<(), String> {
    let t = q.trim();
    if t.is_empty() {
        return Err("query is empty".into());
    }
    if t.len() > 100_000 {
        return Err("query too long".into());
    }
    // Disallow embedded statements (allow a single trailing semicolon).
    if t.trim_end_matches(';').contains(';') {
        return Err("only a single statement is allowed".into());
    }
    if t.to_lowercase().starts_with("explain") {
        return Err("omit EXPLAIN — it is added automatically".into());
    }
    Ok(())
}

/// POST /api/v1/integrations/postgres/explain — UI submits a job.
pub async fn submit(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(body): Json<SubmitBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !crate::license::evaluate().has_entitlement("postgres") {
        return Err((StatusCode::FORBIDDEN, "postgres add-on not licensed".into()));
    }
    if body.server.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "server is required".into()));
    }
    validate_query(&body.query).map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    let id = state
        .config_db
        .create_explain_job(&tenant.tenant_id, body.server.trim(), body.query.trim())
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to create job: {e}"),
            )
        })?;
    Ok(Json(serde_json::json!({ "id": id })))
}

/// GET /api/v1/integrations/postgres/explain/{id} — UI polls for the result.
pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    match state.config_db.get_explain_job(&id).await {
        Ok(Some((status, plan_json, error))) => Ok(Json(serde_json::json!({
            "status": status, "plan_json": plan_json, "error": error,
        }))),
        Ok(None) => Err((StatusCode::NOT_FOUND, "job not found".into())),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))),
    }
}

#[derive(Deserialize)]
pub struct PollParams {
    pub server: String,
}

/// GET /api/v1/integrations/postgres/explain/poll?server= — collector claims a job.
pub async fn poll(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(p): Query<PollParams>,
) -> Response {
    match state
        .config_db
        .claim_pending_explain_job(&tenant.tenant_id, &p.server)
        .await
    {
        Ok(Some((id, query))) => {
            Json(serde_json::json!({ "id": id, "query": query })).into_response()
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}

#[derive(Deserialize)]
pub struct ResultBody {
    #[serde(default)]
    pub plan_json: String,
    #[serde(default)]
    pub error: String,
}

/// POST /api/v1/integrations/postgres/explain/{id}/result — collector posts the plan.
pub async fn post_result(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<ResultBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    state
        .config_db
        .complete_explain_job(&id, &body.plan_json, &body.error)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    Ok(StatusCode::OK)
}
