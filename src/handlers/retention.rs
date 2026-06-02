use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::clickhouse_config::GlobalRetention;
use crate::handlers::users::require_admin;

#[derive(serde::Serialize)]
pub struct GlobalRetentionResponse {
    pub default_days: i32,
    /// Per-signal caps (0 = inherit default_days).
    pub logs_days: i32,
    pub metrics_days: i32,
    pub apm_days: i32,
    /// Resolved caps actually applied (per-signal value if set, else default).
    pub effective_logs: i32,
    pub effective_metrics: i32,
    pub effective_apm: i32,
}

impl From<GlobalRetention> for GlobalRetentionResponse {
    fn from(g: GlobalRetention) -> Self {
        GlobalRetentionResponse {
            default_days: g.default_days,
            logs_days: g.logs_days,
            metrics_days: g.metrics_days,
            apm_days: g.apm_days,
            effective_logs: g.effective_logs(),
            effective_metrics: g.effective_metrics(),
            effective_apm: g.effective_apm(),
        }
    }
}

#[derive(serde::Deserialize)]
pub struct SetGlobalRetentionRequest {
    pub default_days: i32,
    /// 0 (or null) = inherit default_days.
    #[serde(default)]
    pub logs_days: i32,
    #[serde(default)]
    pub metrics_days: i32,
    #[serde(default)]
    pub apm_days: i32,
}

/// GET /api/v1/retention/global
pub async fn get_global_retention(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let g = state
        .config_db
        .get_global_retention().await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string()))?
        // Sane default if the store hasn't been seeded yet.
        .unwrap_or(GlobalRetention { default_days: 365, logs_days: 0, metrics_days: 0, apm_days: 0 });
    Ok(Json(GlobalRetentionResponse::from(g)))
}

/// PUT /api/v1/retention/global
pub async fn set_global_retention(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SetGlobalRetentionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    if req.default_days < 1 {
        return Err((StatusCode::BAD_REQUEST, "default_days must be >= 1".to_string()));
    }
    for (label, v) in [("logs_days", req.logs_days), ("metrics_days", req.metrics_days), ("apm_days", req.apm_days)] {
        if v < 0 {
            return Err((StatusCode::BAD_REQUEST, format!("{label} must be >= 0 (0 = inherit default)")));
        }
    }
    state
        .config_db
        .set_global_retention(req.default_days, req.logs_days, req.metrics_days, req.apm_days).await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string()))?;
    get_global_retention(State(state), headers).await
}

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

    // Clamp against the global caps — a tenant cannot exceed the global maximum
    // for a signal (the table TTL would already have dropped the data anyway).
    let global = state
        .config_db
        .get_global_retention().await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string()))?
        .unwrap_or(GlobalRetention { default_days: 365, logs_days: 0, metrics_days: 0, apm_days: 0 });
    for (label, val, max) in [
        ("metrics_days", req.metrics_days, global.effective_metrics()),
        ("traces_days", req.traces_days, global.effective_apm()),
        ("logs_days", req.logs_days, global.effective_logs()),
    ] {
        if let Some(d) = val {
            if d > max {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("{label} ({d}d) exceeds the global maximum of {max}d"),
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
