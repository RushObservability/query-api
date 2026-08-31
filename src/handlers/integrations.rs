//! Integration control-plane endpoints.
//!
//! These endpoints manage desired state. Collector execution is handled by the
//! CollectorManager, so an API restart or collector crash does not lose the
//! configured integration target.

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::Deserialize;

use crate::handlers::users::require_admin;
use crate::{
    AppState,
    integrations::{self, IntegrationTargetSecret},
};

#[derive(Debug, Deserialize)]
pub struct TargetBody {
    pub id: Option<String>,
    pub name: String,
    pub dsn: String,
    #[serde(default = "default_environment")]
    pub environment: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_environment() -> String {
    "production".into()
}
fn default_enabled() -> bool {
    true
}

fn check_integration(integration: &str) -> Result<(), (StatusCode, String)> {
    let descriptor = integrations::descriptors()
        .into_iter()
        .find(|d| d.id == integration)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "integration not found".into()))?;
    if !descriptor.compiled {
        return Err((
            StatusCode::NOT_FOUND,
            "integration is not included in this build".into(),
        ));
    }
    if !crate::license::evaluate().has_entitlement(descriptor.entitlement) {
        return Err((
            StatusCode::FORBIDDEN,
            format!("{} add-on not licensed", descriptor.name),
        ));
    }
    Ok(())
}

fn validate_body(integration: &str, body: &TargetBody) -> Result<(), (StatusCode, String)> {
    if body.name.trim().is_empty()
        || body.name.len() > 255
        || body.name.chars().any(char::is_control)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must be between 1 and 255 characters".into(),
        ));
    }
    if body.dsn.trim().is_empty() || body.dsn.len() > 16_384 {
        return Err((
            StatusCode::BAD_REQUEST,
            "dsn must be between 1 and 16384 characters".into(),
        ));
    }
    let parsed = url::Url::parse(body.dsn.trim())
        .map_err(|_| (StatusCode::BAD_REQUEST, "dsn must be a valid URL".into()))?;
    let scheme_ok = match integration {
        integrations::MYSQL_INTEGRATION => parsed.scheme() == "mysql",
        integrations::POSTGRES_INTEGRATION => {
            matches!(parsed.scheme(), "postgres" | "postgresql")
        }
        _ => false,
    };
    if !scheme_ok || parsed.host_str().is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("dsn must be a valid {integration} connection URL with a host"),
        ));
    }
    if body.environment.len() > 128 || body.environment.chars().any(char::is_control) {
        return Err((StatusCode::BAD_REQUEST, "invalid environment".into()));
    }
    if let Some(id) = &body.id {
        if id.trim().is_empty() || id.len() > 128 || id.chars().any(char::is_control) {
            return Err((StatusCode::BAD_REQUEST, "invalid target id".into()));
        }
    }
    Ok(())
}

fn target_from_body(body: TargetBody, id: String) -> IntegrationTargetSecret {
    IntegrationTargetSecret {
        id,
        name: body.name.trim().to_string(),
        dsn: body.dsn.trim().to_string(),
        environment: if body.environment.trim().is_empty() {
            "production".into()
        } else {
            body.environment.trim().into()
        },
        enabled: body.enabled,
    }
}

/// GET /api/v1/integrations/{integration}/targets
pub async fn list_targets(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(integration): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    check_integration(&integration)?;
    let targets = state
        .config_db
        .list_integration_target_secrets(&caller.3, &integration)
        .await
        .map_err(internal_error)?;
    let targets = targets
        .into_iter()
        .map(integrations::target_response)
        .collect::<Vec<_>>();
    Ok(Json(
        serde_json::json!({ "integration": integration, "targets": targets, "manager_enabled": state.collectors.enabled() }),
    ))
}

/// POST /api/v1/integrations/{integration}/targets
pub async fn create_target(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(integration): Path<String>,
    Json(body): Json<TargetBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    check_integration(&integration)?;
    validate_body(&integration, &body)?;
    let id = body
        .id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let target = target_from_body(body, id.clone());
    let encrypted = integrations::encrypt_secret(&target.dsn).map_err(internal_error)?;
    state
        .config_db
        .upsert_integration_target(&caller.3, &integration, &target, &encrypted)
        .await
        .map_err(internal_error)?;
    audit_target(
        &state,
        &caller,
        &headers,
        "integration.target_create",
        &integration,
        &target,
        "success",
    )
    .await;
    if let Err(error) = state.collectors.reconcile(&caller.3).await {
        tracing::warn!(tenant = %caller.3, %error, "target saved but collector reconciliation failed");
    }
    Ok((
        StatusCode::CREATED,
        Json(integrations::target_response(target)),
    ))
}

/// PUT /api/v1/integrations/{integration}/targets/{id}
pub async fn update_target(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((integration, id)): Path<(String, String)>,
    Json(mut body): Json<TargetBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    check_integration(&integration)?;
    body.id = Some(id.clone());
    validate_body(&integration, &body)?;
    let target = target_from_body(body, id.clone());
    let encrypted = integrations::encrypt_secret(&target.dsn).map_err(internal_error)?;
    state
        .config_db
        .upsert_integration_target(&caller.3, &integration, &target, &encrypted)
        .await
        .map_err(internal_error)?;
    audit_target(
        &state,
        &caller,
        &headers,
        "integration.target_update",
        &integration,
        &target,
        "success",
    )
    .await;
    if let Err(error) = state.collectors.reconcile(&caller.3).await {
        tracing::warn!(tenant = %caller.3, %error, "target saved but collector reconciliation failed");
    }
    Ok(Json(integrations::target_response(target)))
}

/// DELETE /api/v1/integrations/{integration}/targets/{id}
pub async fn delete_target(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((integration, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    check_integration(&integration)?;
    state
        .config_db
        .delete_integration_target(&caller.3, &integration, &id)
        .await
        .map_err(internal_error)?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("integration.target_delete", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("integration_target", &id)
                .outcome("success")
                .changes(
                    serde_json::json!({ "integration": integration, "target_id": id }).to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    if let Err(error) = state.collectors.reconcile(&caller.3).await {
        tracing::warn!(tenant = %caller.3, %error, "target deleted but collector reconciliation failed");
    }
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/v1/integrations/registry — public metadata only; no secrets.
pub async fn registry(State(state): State<AppState>) -> impl IntoResponse {
    let license = crate::license::evaluate();
    let integrations = integrations::descriptors()
        .into_iter()
        .map(|descriptor| {
            let licensed = descriptor.compiled && license.has_entitlement(descriptor.entitlement);
            serde_json::json!({
                "id": descriptor.id,
                "name": descriptor.name,
                "entitlement": descriptor.entitlement,
                "compiled": descriptor.compiled,
                "licensed": licensed,
                "available": licensed,
                "manager_enabled": state.collectors.enabled(),
            })
        })
        .collect::<Vec<_>>();
    Json(serde_json::json!({ "integrations": integrations }))
}

async fn audit_target(
    state: &AppState,
    caller: &(String, String, String, String, String),
    headers: &HeaderMap,
    action: &str,
    integration: &str,
    target: &IntegrationTargetSecret,
    outcome: &str,
) {
    // Never include the DSN or any credential-bearing value in audit data.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new(action, "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("integration_target", &target.id)
                .outcome(outcome)
                .changes(
                    serde_json::json!({
                        "integration": integration,
                        "name": target.name,
                        "environment": target.environment,
                        "enabled": target.enabled,
                        "configured": true,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(headers)),
        )
        .await;
}

fn internal_error(error: anyhow::Error) -> (StatusCode, String) {
    tracing::error!(%error, "integration control-plane operation failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "integration operation failed".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(dsn: &str) -> TargetBody {
        TargetBody {
            id: None,
            name: "orders-db".into(),
            dsn: dsn.into(),
            environment: "test".into(),
            enabled: true,
        }
    }

    #[test]
    fn target_dsn_must_match_the_integration() {
        assert!(validate_body("mysql", &body("mysql://monitor:secret@db/app")).is_ok());
        assert!(validate_body("postgresql", &body("postgresql://monitor:secret@db/app")).is_ok());
        assert!(validate_body("mysql", &body("postgresql://monitor:secret@db/app")).is_err());
        assert!(validate_body("mysql", &body("mysql:///app")).is_err());
    }
}
