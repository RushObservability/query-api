use axum::{Json, extract::{Path, State}, http::{HeaderMap, StatusCode}, response::IntoResponse};

use crate::AppState;
use crate::handlers::users::{require_auth, require_write};
use crate::models::service_link::CreateServiceLinkRequest;

pub async fn list_service_links(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let links = state.config_db.list_service_links().await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;
    Ok(Json(serde_json::json!({ "links": links })))
}

pub async fn create_service_link(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateServiceLinkRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    state
        .config_db
        .upsert_service_link(&req.service_name, &req.github_repo, &req.default_branch, &req.root_path).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;

    let link = state
        .config_db
        .get_service_link(&req.service_name).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "upsert failed".to_string()))?;

    Ok(Json(link))
}

pub async fn delete_service_link(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(service_name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_service_link(&service_name).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}
