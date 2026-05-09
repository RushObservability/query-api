use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension,
};

use crate::AppState;
use crate::TenantContext;
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
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let rules = state
        .config_db
        .list_detection_rules(Some(&tenant.tenant_id))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let responses: Vec<DetectionRuleResponse> =
        rules.into_iter().map(DetectionRuleResponse::from).collect();
    Ok(Json(serde_json::json!({ "rules": responses })))
}

/// POST /api/v1/detection/rules
/// Create a detection rule under the caller's tenant.
pub async fn create_detection_rule(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<CreateDetectionRuleRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let valid_severities = ["critical", "high", "medium", "low", "info"];
    if !valid_severities.contains(&req.severity.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid severity: {} (must be one of: {})", req.severity, valid_severities.join(", ")),
        ));
    }

    if req.query_sql.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "query_sql cannot be empty".to_string()));
    }

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
            "", // created_by (can be enriched from session later)
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let rule = state
        .config_db
        .get_detection_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read created rule".to_string(),
            )
        })?;

    Ok((StatusCode::CREATED, Json(DetectionRuleResponse::from(rule))))
}

/// GET /api/v1/detection/rules/{id}
/// Get a single detection rule.
pub async fn get_detection_rule(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let rule = state
        .config_db
        .get_detection_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "detection rule not found".to_string()))?;

    // Ensure the caller can only see rules in their tenant
    if rule.tenant_id != tenant.tenant_id {
        return Err((StatusCode::NOT_FOUND, "detection rule not found".to_string()));
    }

    Ok(Json(DetectionRuleResponse::from(rule)))
}

/// PUT /api/v1/detection/rules/{id}
/// Update a detection rule.
pub async fn update_detection_rule(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateDetectionRuleRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let valid_severities = ["critical", "high", "medium", "low", "info"];
    if !valid_severities.contains(&req.severity.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid severity: {}", req.severity),
        ));
    }

    if req.query_sql.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "query_sql cannot be empty".to_string()));
    }

    // Verify ownership
    let existing = state
        .config_db
        .get_detection_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "detection rule not found".to_string()))?;
    if existing.tenant_id != tenant.tenant_id {
        return Err((StatusCode::NOT_FOUND, "detection rule not found".to_string()));
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
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "detection rule not found".to_string()));
    }

    let rule = state
        .config_db
        .get_detection_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read rule".to_string(),
            )
        })?;

    Ok(Json(DetectionRuleResponse::from(rule)))
}

/// DELETE /api/v1/detection/rules/{id}
/// Delete a detection rule.
pub async fn delete_detection_rule(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify ownership
    let existing = state
        .config_db
        .get_detection_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "detection rule not found".to_string()))?;
    if existing.tenant_id != tenant.tenant_id {
        return Err((StatusCode::NOT_FOUND, "detection rule not found".to_string()));
    }

    let deleted = state
        .config_db
        .delete_detection_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "detection rule not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// POST /api/v1/detection/rules/{id}/test
/// Dry-run a detection rule: execute the query and return results without creating an event.
pub async fn test_detection_rule(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let rule = state
        .config_db
        .get_detection_rule(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "detection rule not found".to_string()))?;

    if rule.tenant_id != tenant.tenant_id {
        return Err((StatusCode::NOT_FOUND, "detection rule not found".to_string()));
    }

    let (row_count, query_executed) =
        crate::siem_engine::test_detection_query(
            &state.ch,
            &rule.query_sql,
            &rule.tenant_id,
            rule.window_secs,
        )
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("detection query failed: {e}"),
            )
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
    Query(params): Query<ListEventsQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let events = state
        .config_db
        .list_detection_events(&tenant.tenant_id, params.limit)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "events": events })))
}
