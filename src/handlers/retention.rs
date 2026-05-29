use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::handlers::users::require_admin;

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
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    // Verify tenant exists
    state
        .config_db
        .get_tenant(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    let overrides = state
        .config_db
        .get_tenant_retention(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;

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
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<SetRetentionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    // Verify tenant exists
    state
        .config_db
        .get_tenant(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?
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
                    .set_tenant_retention(&id, signal, days).await
                    .map_err(|e| {
                        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
                    })?;
            }
            None => {
                let _ = state.config_db.delete_tenant_retention(&id, signal).await;
            }
        }
    }

    // Return current state (auth already verified above)
    get_tenant_retention(State(state), headers, Path(id)).await
}

/// DELETE /api/v1/tenants/{id}/retention/{signal}
pub async fn delete_tenant_retention(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, signal)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
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
        .get_tenant(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    let deleted = state
        .config_db
        .delete_tenant_retention(&id, &signal).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;

    if !deleted {
        return Err((
            StatusCode::NOT_FOUND,
            "no retention override found for this signal".to_string(),
        ));
    }

    Ok(StatusCode::NO_CONTENT)
}
