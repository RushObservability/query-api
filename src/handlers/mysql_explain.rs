//! MySQL EXPLAIN job queue. Plans are produced by the licensed collector.
use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::handlers::users::{require_auth, require_write};
use crate::{AppState, RequestIdentity, TenantContext};

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

fn validate_query(query: &str) -> Result<String, String> {
    let query = query.trim().trim_end_matches(';').trim();
    if query.is_empty() || query.len() > 100_000 {
        return Err("query must contain 1 to 100000 characters".into());
    }
    if query.contains(';') {
        return Err("only one statement is allowed".into());
    }
    let first = query.split_whitespace().next().unwrap_or_default();
    if !first.eq_ignore_ascii_case("select") && !first.eq_ignore_ascii_case("with") {
        return Err("only SELECT or WITH queries can be explained".into());
    }
    let upper = format!(" {} ", query.to_ascii_uppercase());
    if [
        " INSERT ",
        " UPDATE ",
        " DELETE ",
        " REPLACE ",
        " INTO OUTFILE ",
        " INTO DUMPFILE ",
        " FOR UPDATE ",
        " LOCK IN SHARE MODE ",
    ]
    .iter()
    .any(|clause| upper.contains(clause))
    {
        return Err("only non-locking read queries can be explained".into());
    }
    Ok(query.to_string())
}

fn licensed() -> Result<(), (StatusCode, String)> {
    if crate::license::evaluate().has_entitlement("mysql") {
        Ok(())
    } else {
        Err((StatusCode::FORBIDDEN, "MySQL add-on not licensed".into()))
    }
}

pub async fn submit(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(body): Json<SubmitBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    licensed()?;
    if body.server.trim().is_empty() || body.server.len() > 255 {
        return Err((StatusCode::BAD_REQUEST, "invalid server name".into()));
    }
    if body.db.len() > 64 || body.db.chars().any(char::is_control) {
        return Err((StatusCode::BAD_REQUEST, "invalid database name".into()));
    }
    let query = validate_query(&body.query).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let id = state
        .config_db
        .create_mysql_explain_job(
            &tenant.tenant_id,
            body.server.trim(),
            body.db.trim(),
            &query,
        )
        .await
        .map_err(|error| crate::api_error::internal_legacy("mysql_explain.create", error))?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("mysql_explain.submit", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(tenant.tenant_id.clone())
                .resource("mysql_explain_job", &id)
                .outcome("success")
                .changes(serde_json::json!({ "server": body.server, "db": body.db }).to_string())
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(Json(serde_json::json!({ "id": id })))
}

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
        .get_mysql_explain_job(&tenant.tenant_id, &id)
        .await
    {
        Ok(Some((status, db, plan_json, error))) => {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("mysql_explain.read", "user")
                        .actor(caller.0.clone(), caller.1.clone())
                        .tenant(tenant.tenant_id.clone())
                        .resource("mysql_explain_job", &id)
                        .outcome("success")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            Ok(Json(serde_json::json!({
                "status": status, "db": db, "plan_json": plan_json, "error": error,
            })))
        }
        Ok(None) => Err((StatusCode::NOT_FOUND, "job not found".into())),
        Err(error) => Err(crate::api_error::internal_legacy(
            "mysql_explain.get",
            error,
        )),
    }
}

#[derive(Deserialize)]
pub struct PollParams {
    pub server: String,
}

pub async fn poll(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Extension(identity): Extension<RequestIdentity>,
    headers: HeaderMap,
    Query(params): Query<PollParams>,
) -> Response {
    if let Err(error) = require_collector_identity(&tenant, &identity) {
        return error.into_response();
    }
    if licensed().is_err() {
        return (StatusCode::FORBIDDEN, "MySQL add-on not licensed").into_response();
    }
    if params.server.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "server is required").into_response();
    }
    if let Ok(requeued) = state
        .config_db
        .requeue_stale_mysql_explain_jobs(&tenant.tenant_id, &params.server)
        .await
    {
        if requeued > 0 {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("mysql_explain.requeue", &identity.actor_type)
                        .actor(identity.actor_id.clone(), identity.actor_name.clone())
                        .tenant(tenant.tenant_id.clone())
                        .resource("mysql_explain_queue", &params.server)
                        .outcome("success")
                        .changes(serde_json::json!({ "requeued": requeued }).to_string())
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
        }
    }
    match state
        .config_db
        .claim_pending_mysql_explain_job(&tenant.tenant_id, &params.server)
        .await
    {
        Ok(Some((id, db, query))) => {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("mysql_explain.claim", &identity.actor_type)
                        .actor(identity.actor_id.clone(), identity.actor_name.clone())
                        .tenant(tenant.tenant_id.clone())
                        .resource("mysql_explain_job", &id)
                        .outcome("success")
                        .changes(
                            serde_json::json!({ "server": params.server, "db": db }).to_string(),
                        )
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            Json(serde_json::json!({ "id": id, "db": db, "query": query })).into_response()
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => {
            crate::api_error::internal_legacy("mysql_explain.poll", error).into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct ResultBody {
    #[serde(default)]
    pub plan_json: String,
    #[serde(default)]
    pub error: String,
}

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
        .complete_mysql_explain_job(&tenant.tenant_id, &id, &body.plan_json, &body.error)
        .await
        .map_err(|error| crate::api_error::internal_legacy("mysql_explain.complete", error))?;
    state.audit.log(
        crate::audit::AuditEvent::new("mysql_explain.complete", &identity.actor_type)
            .actor(identity.actor_id.clone(), identity.actor_name.clone())
            .tenant(tenant.tenant_id.clone())
            .resource("mysql_explain_job", &id)
            .outcome(if body.error.is_empty() { "success" } else { "failure" })
            .changes(serde_json::json!({ "has_plan": !body.plan_json.is_empty(), "has_error": !body.error.is_empty() }).to_string())
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;
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
    fn only_single_read_statements_enter_the_queue() {
        assert!(validate_query("select * from orders").is_ok());
        assert!(validate_query("WITH recent AS (SELECT 1) SELECT * FROM recent").is_ok());
        assert!(validate_query("delete from orders").is_err());
        assert!(validate_query("select 1; drop table users").is_err());
        assert!(validate_query("WITH recent AS (SELECT 1) DELETE FROM orders").is_err());
        assert!(validate_query("SELECT * FROM orders INTO OUTFILE '/tmp/orders'").is_err());
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
}
