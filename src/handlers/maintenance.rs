use crate::AppState;
use crate::TenantContext;
use crate::handlers::users::{require_auth, require_write};
use crate::models::maintenance::{MaintenanceWindow, Scope, format_time, validate_window};
use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct CreateWindowRequest {
    pub name: String,
    /// `all` (default), `monitor:<id>`, or `tag:<key>:<value>`.
    pub scope: Option<String>,
    /// RFC 3339, any offset.
    pub starts_at: String,
    pub ends_at: String,
}

#[derive(Debug, Serialize)]
pub struct MaintenanceWindowResponse {
    pub id: String,
    pub name: String,
    pub scope: String,
    pub starts_at: String,
    pub ends_at: String,
    pub created_at: String,
    pub created_by: String,
    /// `scheduled`, `active`, or `ended`.
    pub status: &'static str,
}

impl MaintenanceWindowResponse {
    fn from_window(window: MaintenanceWindow, now: chrono::DateTime<chrono::Utc>) -> Self {
        let status = window.status(now);
        Self {
            id: window.id,
            name: window.name,
            scope: window.scope,
            starts_at: window.starts_at,
            ends_at: window.ends_at,
            created_at: window.created_at,
            created_by: window.created_by,
            status,
        }
    }
}

/// GET /api/v1/maintenance-windows — this tenant's windows, newest first.
pub async fn list_windows(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let now = chrono::Utc::now();
    let windows: Vec<MaintenanceWindowResponse> = state
        .config_db
        .list_maintenance_windows(&tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("maintenance", e))?
        .into_iter()
        .map(|window| MaintenanceWindowResponse::from_window(window, now))
        .collect();
    Ok(Json(serde_json::json!({ "windows": windows })))
}

/// POST /api/v1/maintenance-windows — silence monitor notifications for a period.
pub async fn create_window(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<CreateWindowRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let name = req.name.trim();
    if name.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name required".to_string()));
    }
    if name.len() > 255 {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must not exceed 255 characters".to_string(),
        ));
    }
    let now = chrono::Utc::now();
    let (starts_at, ends_at) = validate_window(&req.starts_at, &req.ends_at, now)
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;
    let scope = Scope::parse(req.scope.as_deref().unwrap_or("all"))
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;
    if let Scope::Monitor(monitor_id) = &scope {
        let exists = state
            .config_db
            .get_monitor(monitor_id, &tenant.tenant_id)
            .await
            .map_err(|e| crate::api_error::internal_legacy("maintenance", e))?
            .is_some();
        if !exists {
            return Err((StatusCode::BAD_REQUEST, "monitor not found".to_string()));
        }
    }

    let window = MaintenanceWindow {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: tenant.tenant_id.clone(),
        name: name.to_string(),
        scope: scope.as_stored(),
        starts_at: format_time(starts_at),
        ends_at: format_time(ends_at),
        created_at: format_time(now),
        created_by: caller.1.clone(),
    };
    state
        .config_db
        .create_maintenance_window(&window)
        .await
        .map_err(|e| crate::api_error::internal_legacy("maintenance", e))?;

    // AUDIT: maintenance window created.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("maintenance_window.create", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(tenant.tenant_id.clone())
                .resource("maintenance_window", window.id.clone())
                .changes(
                    serde_json::json!({
                        "name": window.name,
                        "scope": window.scope,
                        "starts_at": window.starts_at,
                        "ends_at": window.ends_at,
                    })
                    .to_string(),
                )
                .description("maintenance window created")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok((
        StatusCode::CREATED,
        Json(MaintenanceWindowResponse::from_window(window, now)),
    ))
}

/// DELETE /api/v1/maintenance-windows/{id} — end or cancel a window.
pub async fn delete_window(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_maintenance_window(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("maintenance", e))?;
    let Some(window) = deleted else {
        return Err((StatusCode::NOT_FOUND, "window not found".to_string()));
    };

    // AUDIT: maintenance window deleted.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("maintenance_window.delete", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(tenant.tenant_id.clone())
                .resource("maintenance_window", window.id.clone())
                .changes(
                    serde_json::json!({
                        "name": window.name,
                        "scope": window.scope,
                        "starts_at": window.starts_at,
                        "ends_at": window.ends_at,
                    })
                    .to_string(),
                )
                .description("maintenance window deleted")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(serde_json::json!({ "ok": true })))
}
