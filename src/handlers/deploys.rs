use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::handlers::users::require_write;
use crate::models::deploy::*;

pub async fn create_deploy(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateDeployMarkerRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers)?;
    if req.service_name.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "service_name must not be empty".to_string()));
    }
    if req.service_name.len() > 255 {
        return Err((StatusCode::BAD_REQUEST, "service_name must not exceed 255 characters".to_string()));
    }
    if req.version.len() > 100 {
        return Err((StatusCode::BAD_REQUEST, "version must not exceed 100 characters".to_string()));
    }
    if req.commit_sha.len() > 100 {
        return Err((StatusCode::BAD_REQUEST, "commit_sha must not exceed 100 characters".to_string()));
    }
    if req.description.len() > 1024 {
        return Err((StatusCode::BAD_REQUEST, "description must not exceed 1024 characters".to_string()));
    }
    let id = uuid::Uuid::new_v4().to_string();
    state
        .config_db
        .create_deploy_marker(
            &id,
            &req.service_name,
            &req.version,
            &req.commit_sha,
            &req.description,
            &req.environment,
            &req.deployed_by,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::CREATED, Json(serde_json::json!({ "id": id }))))
}

pub async fn list_deploys(
    State(state): State<AppState>,
    Query(query): Query<DeployMarkerQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let markers = state
        .config_db
        .list_deploy_markers(
            query.service_name.as_deref(),
            query.from.as_deref(),
            query.to.as_deref(),
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "deploys": markers })))
}
