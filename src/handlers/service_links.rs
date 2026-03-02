use axum::{Json, extract::{Path, State}, http::StatusCode, response::IntoResponse};

use crate::AppState;
use crate::models::service_link::CreateServiceLinkRequest;

pub async fn list_service_links(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let links = state.config_db.list_service_links().map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}"))
    })?;
    Ok(Json(serde_json::json!({ "links": links })))
}

pub async fn create_service_link(
    State(state): State<AppState>,
    Json(req): Json<CreateServiceLinkRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    state
        .config_db
        .upsert_service_link(&req.service_name, &req.github_repo, &req.default_branch, &req.root_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let link = state
        .config_db
        .get_service_link(&req.service_name)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "upsert failed".to_string()))?;

    Ok(Json(link))
}

pub async fn delete_service_link(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let deleted = state
        .config_db
        .delete_service_link(&service_name)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}
