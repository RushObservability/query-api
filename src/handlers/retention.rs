use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::AppState;

#[derive(serde::Deserialize)]
pub struct SetRetentionRequest {
    pub metrics_days: Option<i32>,
    pub traces_days: Option<i32>,
    pub logs_days: Option<i32>,
}

#[derive(serde::Serialize)]
pub struct TenantRetentionResponse {
    pub metrics_days: Option<i32>,
    pub traces_days: Option<i32>,
    pub logs_days: Option<i32>,
}

/// GET /api/v1/tenants/{id}/retention
pub async fn get_tenant_retention(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify tenant exists
    state
        .config_db
        .get_tenant(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    let overrides = state
        .config_db
        .get_tenant_retention(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let mut resp = TenantRetentionResponse {
        metrics_days: None,
        traces_days: None,
        logs_days: None,
    };

    for (signal, days) in overrides {
        match signal.as_str() {
            "metrics" => resp.metrics_days = Some(days),
            "traces" => resp.traces_days = Some(days),
            "logs" => resp.logs_days = Some(days),
            _ => {}
        }
    }

    Ok(Json(resp))
}

/// PUT /api/v1/tenants/{id}/retention
pub async fn set_tenant_retention(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<SetRetentionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify tenant exists
    state
        .config_db
        .get_tenant(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    // Validate: days must be positive if provided
    for (label, val) in [
        ("metrics_days", req.metrics_days),
        ("traces_days", req.traces_days),
        ("logs_days", req.logs_days),
    ] {
        if let Some(d) = val {
            if d < 1 {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("{label} must be >= 1"),
                ));
            }
        }
    }

    // Upsert provided values, delete omitted ones (null = fall back to global)
    let signals = [
        ("metrics", req.metrics_days),
        ("traces", req.traces_days),
        ("logs", req.logs_days),
    ];

    for (signal, maybe_days) in signals {
        match maybe_days {
            Some(days) => {
                state
                    .config_db
                    .set_tenant_retention(&id, signal, days)
                    .map_err(|e| {
                        (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}"))
                    })?;
            }
            None => {
                let _ = state.config_db.delete_tenant_retention(&id, signal);
            }
        }
    }

    // Return current state
    get_tenant_retention(State(state), Path(id)).await
}

/// DELETE /api/v1/tenants/{id}/retention/{signal}
pub async fn delete_tenant_retention(
    State(state): State<AppState>,
    Path((id, signal)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Validate signal name
    if !["metrics", "traces", "logs"].contains(&signal.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid signal: {signal}. Must be one of: metrics, traces, logs"),
        ));
    }

    // Verify tenant exists
    state
        .config_db
        .get_tenant(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    let deleted = state
        .config_db
        .delete_tenant_retention(&id, &signal)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    if !deleted {
        return Err((
            StatusCode::NOT_FOUND,
            "no retention override found for this signal".to_string(),
        ));
    }

    Ok(StatusCode::NO_CONTENT)
}
