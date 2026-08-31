//! PostgreSQL EXPLAIN job queue.
//!
//! The collector is push-only, so query-plan requests flow through a poll-based
//! queue: the UI submits a job, the collector polls (`/poll`), runs a plain
//! `EXPLAIN (FORMAT JSON)`, and posts the result (`/result`); the UI polls `/{id}`.
//! Tenant is resolved by the global middleware (UI session OR collector Bearer key).
use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::handlers::users::{require_auth, require_write};
use crate::{AppState, RequestIdentity, TenantContext};

fn licensed() -> Result<(), (StatusCode, String)> {
    if crate::license::evaluate().has_entitlement("postgres") {
        Ok(())
    } else {
        Err((StatusCode::FORBIDDEN, "postgres add-on not licensed".into()))
    }
}

fn require_collector_identity(
    tenant: &TenantContext,
    identity: &RequestIdentity,
) -> Result<(), (StatusCode, String)> {
    if identity.authenticated
        && identity.credential_type == "ingest_key"
        && identity.tenant_id == tenant.tenant_id
    {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "collector ingest key required".into(),
        ))
    }
}

#[derive(Deserialize)]
pub struct SubmitBody {
    pub server: String,
    #[serde(default)]
    pub db: String,
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
    headers: HeaderMap,
    Json(body): Json<SubmitBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    licensed()?;
    if body.server.trim().is_empty() || body.server.len() > 255 {
        return Err((StatusCode::BAD_REQUEST, "server is required".into()));
    }
    if body.db.len() > 255 || body.db.chars().any(char::is_control) {
        return Err((StatusCode::BAD_REQUEST, "invalid database name".into()));
    }
    validate_query(&body.query).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let query = body.query.trim().trim_end_matches(';').trim();

    let id = state
        .config_db
        .create_explain_job(&tenant.tenant_id, body.server.trim(), body.db.trim(), query)
        .await
        .map_err(|e| crate::api_error::internal_legacy("postgres_explain.create", e))?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("postgres_explain.submit", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(tenant.tenant_id.clone())
                .resource("postgres_explain_job", &id)
                .outcome("success")
                .changes(serde_json::json!({ "server": body.server, "db": body.db }).to_string())
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(Json(serde_json::json!({ "id": id })))
}

/// GET /api/v1/integrations/postgres/explain/{id} — UI polls for the result.
pub async fn get_job(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_auth(&state, &headers).await?;
    licensed()?;
    match state
        .config_db
        .get_explain_job(&tenant.tenant_id, &id)
        .await
    {
        Ok(Some((status, db, plan_json, error))) => {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("postgres_explain.read", "user")
                        .actor(caller.0.clone(), caller.1.clone())
                        .tenant(tenant.tenant_id.clone())
                        .resource("postgres_explain_job", &id)
                        .outcome("success")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            Ok(Json(serde_json::json!({
                "status": status, "db": db, "plan_json": plan_json, "error": error,
            })))
        }
        Ok(None) => Err((StatusCode::NOT_FOUND, "job not found".into())),
        Err(e) => Err(crate::api_error::internal_legacy("postgres_explain.get", e)),
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
    Extension(identity): Extension<RequestIdentity>,
    headers: HeaderMap,
    Query(p): Query<PollParams>,
) -> Response {
    if let Err(error) = require_collector_identity(&tenant, &identity) {
        return error.into_response();
    }
    if let Err(error) = licensed() {
        return error.into_response();
    }
    if p.server.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "server is required").into_response();
    }
    if let Ok(requeued) = state
        .config_db
        .requeue_stale_explain_jobs(&tenant.tenant_id, &p.server)
        .await
    {
        if requeued > 0 {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("postgres_explain.requeue", &identity.actor_type)
                        .actor(identity.actor_id.clone(), identity.actor_name.clone())
                        .tenant(tenant.tenant_id.clone())
                        .resource("postgres_explain_queue", &p.server)
                        .outcome("success")
                        .changes(serde_json::json!({ "requeued": requeued }).to_string())
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
        }
    }
    match state
        .config_db
        .claim_pending_explain_job(&tenant.tenant_id, &p.server)
        .await
    {
        Ok(Some((id, db, query))) => {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("postgres_explain.claim", &identity.actor_type)
                        .actor(identity.actor_id.clone(), identity.actor_name.clone())
                        .tenant(tenant.tenant_id.clone())
                        .resource("postgres_explain_job", &id)
                        .outcome("success")
                        .changes(serde_json::json!({ "server": p.server, "db": db }).to_string())
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            Json(serde_json::json!({ "id": id, "db": db, "query": query })).into_response()
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => crate::api_error::internal_legacy("postgres_explain.poll", e).into_response(),
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
    Extension(tenant): Extension<TenantContext>,
    Extension(identity): Extension<RequestIdentity>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<ResultBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_collector_identity(&tenant, &identity)?;
    licensed()?;
    if body.plan_json.len() > 5_000_000 || body.error.len() > 16_384 {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "EXPLAIN result is too large".into(),
        ));
    }
    state
        .config_db
        .complete_explain_job(&tenant.tenant_id, &id, &body.plan_json, &body.error)
        .await
        .map_err(|e| crate::api_error::internal_legacy("postgres_explain.complete", e))?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("postgres_explain.complete", &identity.actor_type)
                .actor(identity.actor_id.clone(), identity.actor_name.clone())
                .tenant(tenant.tenant_id.clone())
                .resource("postgres_explain_job", &id)
                .outcome(if body.error.is_empty() { "success" } else { "failure" })
                .changes(serde_json::json!({ "has_plan": !body.plan_json.is_empty(), "has_error": !body.error.is_empty() }).to_string())
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::{require_collector_identity, validate_query};
    use crate::{RequestIdentity, TenantContext};

    fn identity(tenant: &str, credential_type: &str, authenticated: bool) -> RequestIdentity {
        RequestIdentity {
            tenant_id: tenant.into(),
            authenticated,
            actor_id: "key-id".into(),
            actor_name: "API key".into(),
            actor_type: "api_key".into(),
            credential_type: credential_type.into(),
        }
    }

    #[test]
    fn collector_identity_requires_an_authenticated_ingest_key_for_the_same_tenant() {
        let tenant = TenantContext {
            tenant_id: "tenant-a".into(),
        };
        assert!(
            require_collector_identity(&tenant, &identity("tenant-a", "ingest_key", true)).is_ok()
        );
        assert!(
            require_collector_identity(&tenant, &identity("tenant-a", "query_key", true)).is_err()
        );
        assert!(
            require_collector_identity(&tenant, &identity("tenant-b", "ingest_key", true)).is_err()
        );
        assert!(
            require_collector_identity(&tenant, &identity("tenant-a", "ingest_key", false))
                .is_err()
        );
    }

    #[test]
    fn explain_jobs_reject_multiple_or_nested_explain_statements() {
        assert!(validate_query("select * from orders").is_ok());
        assert!(validate_query("select 1; drop table users").is_err());
        assert!(validate_query("EXPLAIN SELECT 1").is_err());
    }
}
