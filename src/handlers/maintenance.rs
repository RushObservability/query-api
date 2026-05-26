use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use crate::AppState;
use crate::handlers::users::require_write;

#[derive(Debug, Deserialize)]
pub struct CreateWindowRequest {
    pub name: String,
    pub scope: Option<String>,
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
}

pub async fn list_windows(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let rows = state.config_db
        .list_maintenance_windows().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let windows: Vec<MaintenanceWindowResponse> = rows.into_iter().map(|r| MaintenanceWindowResponse {
        id: r.0, name: r.1, scope: r.2, starts_at: r.3, ends_at: r.4, created_at: r.5,
    }).collect();
    Ok(Json(serde_json::json!({ "windows": windows })))
}

pub async fn create_window(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateWindowRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    if req.name.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name required".to_string()));
    }
    if req.name.len() > 255 {
        return Err((StatusCode::BAD_REQUEST, "name must not exceed 255 characters".to_string()));
    }
    if req.starts_at.len() < 10 || req.ends_at.len() < 10 {
        return Err((StatusCode::BAD_REQUEST, "invalid starts_at or ends_at".to_string()));
    }
    let id = uuid::Uuid::new_v4().to_string();
    let scope = req.scope.unwrap_or_else(|| "all".to_string());
    state.config_db
        .create_maintenance_window(&id, &req.name, &scope, &req.starts_at, &req.ends_at).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((StatusCode::CREATED, Json(serde_json::json!({ "id": id, "ok": true }))))
}

pub async fn delete_window(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let deleted = state.config_db
        .delete_maintenance_window(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "window not found".to_string()));
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}
