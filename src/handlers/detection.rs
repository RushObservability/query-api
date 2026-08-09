use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::users::{require_auth, require_write};

/// Validate a detection rule through the same constrained compiler used by
/// previews and scheduled evaluation.
fn validate_detection_sql(query_sql: &str) -> Result<(), (StatusCode, String)> {
    crate::detection_query::validate_template(query_sql).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid detection query: {error}"),
        )
    })
}
use crate::models::detection::*;

#[derive(Debug, serde::Deserialize)]
pub struct ListEventsQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 {
    200
}

/// GET /api/v1/detection/rules
/// List detection rules filtered by the caller's tenant.
pub async fn list_detection_rules(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let rules = state
        .config_db
        .list_detection_rules(Some(&tenant.tenant_id))
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?;
    let responses: Vec<DetectionRuleResponse> =
        rules.into_iter().map(DetectionRuleResponse::from).collect();
    Ok(Json(serde_json::json!({ "rules": responses })))
}

/// POST /api/v1/detection/rules
/// Create a detection rule under the caller's tenant.
pub async fn create_detection_rule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<CreateDetectionRuleRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    if req.name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must not be empty".to_string(),
        ));
    }
    if req.name.len() > 255 {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must not exceed 255 characters".to_string(),
        ));
    }
    if req.description.len() > 1024 {
        return Err((
            StatusCode::BAD_REQUEST,
            "description must not exceed 1024 characters".to_string(),
        ));
    }
    if req.query_sql.len() > 10_000 {
        return Err((
            StatusCode::BAD_REQUEST,
            "query_sql must not exceed 10000 characters".to_string(),
        ));
    }
    let valid_severities = ["critical", "high", "medium", "low", "info"];
    if !valid_severities.contains(&req.severity.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "invalid severity: {} (must be one of: {})",
                req.severity,
                valid_severities.join(", ")
            ),
        ));
    }

    if req.query_sql.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "query_sql cannot be empty".to_string(),
        ));
    }
    validate_detection_sql(&req.query_sql)?;

    let id = uuid::Uuid::new_v4().to_string();
    let channels = serde_json::to_string(&req.channels)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    state
        .config_db
        .create_detection_rule(
            &id,
            &tenant.tenant_id,
            &req.name,
            &req.description,
            &req.query_sql,
            req.interval_secs,
            req.threshold,
            &req.severity,
            req.window_secs,
            req.enabled,
            &channels,
            &caller.1, // created_by: username from session
        )
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?;

    let rule = state
        .config_db
        .get_detection_rule(&id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read created rule".to_string(),
            )
        })?;

    // AUDIT: detection rule created.
    state.audit.log(
        crate::audit::AuditEvent::new("detection_rule.create", "user")
            .actor(caller.0.clone(), caller.1.clone())
            .tenant(tenant.tenant_id.clone())
            .resource("detection_rule", id.clone())
            .changes(serde_json::json!({ "name": req.name, "severity": req.severity, "enabled": req.enabled }).to_string())
            .description("detection rule created")
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;

    Ok((StatusCode::CREATED, Json(DetectionRuleResponse::from(rule))))
}

/// GET /api/v1/detection/rules/{id}
/// Get a single detection rule.
pub async fn get_detection_rule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let rule = state
        .config_db
        .get_detection_rule(&id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "detection rule not found".to_string(),
            )
        })?;

    // Ensure the caller can only see rules in their tenant
    if rule.tenant_id != tenant.tenant_id {
        return Err((
            StatusCode::NOT_FOUND,
            "detection rule not found".to_string(),
        ));
    }

    Ok(Json(DetectionRuleResponse::from(rule)))
}

/// PUT /api/v1/detection/rules/{id}
/// Update a detection rule.
pub async fn update_detection_rule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateDetectionRuleRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    if req.name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must not be empty".to_string(),
        ));
    }
    if req.name.len() > 255 {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must not exceed 255 characters".to_string(),
        ));
    }
    if req.description.len() > 1024 {
        return Err((
            StatusCode::BAD_REQUEST,
            "description must not exceed 1024 characters".to_string(),
        ));
    }
    if req.query_sql.len() > 10_000 {
        return Err((
            StatusCode::BAD_REQUEST,
            "query_sql must not exceed 10000 characters".to_string(),
        ));
    }
    let valid_severities = ["critical", "high", "medium", "low", "info"];
    if !valid_severities.contains(&req.severity.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid severity: {}", req.severity),
        ));
    }

    if req.query_sql.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "query_sql cannot be empty".to_string(),
        ));
    }
    validate_detection_sql(&req.query_sql)?;

    // Verify ownership
    let existing = state
        .config_db
        .get_detection_rule(&id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "detection rule not found".to_string(),
            )
        })?;
    if existing.tenant_id != tenant.tenant_id {
        return Err((
            StatusCode::NOT_FOUND,
            "detection rule not found".to_string(),
        ));
    }

    let channels = serde_json::to_string(&req.channels)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let updated = state
        .config_db
        .update_detection_rule(
            &id,
            &req.name,
            &req.description,
            &req.query_sql,
            req.interval_secs,
            req.threshold,
            &req.severity,
            req.window_secs,
            req.enabled,
            &channels,
        )
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?;
    if !updated {
        return Err((
            StatusCode::NOT_FOUND,
            "detection rule not found".to_string(),
        ));
    }

    let rule = state
        .config_db
        .get_detection_rule(&id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read rule".to_string(),
            )
        })?;

    // AUDIT: detection rule updated.
    state.audit.log(
        crate::audit::AuditEvent::new("detection_rule.update", "user")
            .actor(caller.0.clone(), caller.1.clone())
            .tenant(tenant.tenant_id.clone())
            .resource("detection_rule", id.clone())
            .changes(serde_json::json!({ "name": req.name, "severity": req.severity, "enabled": req.enabled }).to_string())
            .description("detection rule updated")
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;

    Ok(Json(DetectionRuleResponse::from(rule)))
}

/// DELETE /api/v1/detection/rules/{id}
/// Delete a detection rule.
pub async fn delete_detection_rule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    // Verify ownership
    let existing = state
        .config_db
        .get_detection_rule(&id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "detection rule not found".to_string(),
            )
        })?;
    if existing.tenant_id != tenant.tenant_id {
        return Err((
            StatusCode::NOT_FOUND,
            "detection rule not found".to_string(),
        ));
    }

    let deleted = state
        .config_db
        .delete_detection_rule(&id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?;
    if !deleted {
        return Err((
            StatusCode::NOT_FOUND,
            "detection rule not found".to_string(),
        ));
    }

    // AUDIT: detection rule deleted.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("detection_rule.delete", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(tenant.tenant_id.clone())
                .resource("detection_rule", id.clone())
                .changes(serde_json::json!({ "name": existing.name }).to_string())
                .description("detection rule deleted")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(StatusCode::NO_CONTENT)
}

/// POST /api/v1/detection/rules/{id}/test
/// Dry-run a detection rule: execute the query and return results without creating an event.
pub async fn test_detection_rule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let rule = state
        .config_db
        .get_detection_rule(&id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "detection rule not found".to_string(),
            )
        })?;

    if rule.tenant_id != tenant.tenant_id {
        return Err((
            StatusCode::NOT_FOUND,
            "detection rule not found".to_string(),
        ));
    }

    let (row_count, query_executed) = crate::siem_engine::test_detection_query(
        &state.ch,
        &rule.query_sql,
        &rule.tenant_id,
        rule.window_secs,
    )
    .await
    .map_err(|error| {
        tracing::warn!(
            error = %error,
            rule_id = %id,
            tenant_id = %tenant.tenant_id,
            "detection query preview failed"
        );
        match error {
            crate::detection_query::DetectionQueryError::Invalid(message) => (
                StatusCode::BAD_REQUEST,
                format!("invalid detection query: {message}"),
            ),
            crate::detection_query::DetectionQueryError::Execution(_) => (
                StatusCode::BAD_REQUEST,
                "detection query could not be evaluated".to_string(),
            ),
        }
    })?;

    Ok(Json(TestDetectionRuleResponse {
        row_count,
        would_fire: row_count as i64 >= rule.threshold,
        sample_data: serde_json::json!([]),
        query_executed,
    }))
}

/// GET /api/v1/detection/events
/// List recent detection events filtered by the caller's tenant.
pub async fn list_detection_events(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Query(params): Query<ListEventsQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let events = state
        .config_db
        .list_detection_events(&tenant.tenant_id, params.limit)
        .await
        .map_err(|e| crate::api_error::internal_legacy("detection", e))?;
    Ok(Json(serde_json::json!({ "events": events })))
}
