use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::AppState;
use crate::models::slo::*;

const VALID_WINDOWS: [&str; 4] = ["rolling_1h", "rolling_24h", "rolling_7d", "rolling_30d"];

pub async fn list_slos(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let slos = state
        .config_db
        .list_slos()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let responses: Vec<SloResponse> = slos.into_iter().map(SloResponse::from).collect();
    Ok(Json(serde_json::json!({ "slos": responses })))
}

pub async fn create_slo(
    State(state): State<AppState>,
    Json(req): Json<CreateSloRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !VALID_WINDOWS.contains(&req.window_type.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid window_type: {}", req.window_type)));
    }
    if req.target_percentage <= 0.0 || req.target_percentage > 100.0 {
        return Err((StatusCode::BAD_REQUEST, "target_percentage must be between 0 and 100".to_string()));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let good_filters = serde_json::to_string(&req.good_filters)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let total_filters = serde_json::to_string(&req.total_filters)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let channel_ids = serde_json::to_string(&req.notification_channel_ids)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    state
        .config_db
        .create_slo(
            &id,
            &req.name,
            &req.description,
            req.enabled,
            &req.service_name,
            &req.window_type,
            req.target_percentage,
            &good_filters,
            &total_filters,
            req.eval_interval_secs,
            &channel_ids,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let slo = state
        .config_db
        .get_slo(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read created slo".to_string()))?;

    Ok((StatusCode::CREATED, Json(SloResponse::from(slo))))
}

pub async fn get_slo(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let slo = state
        .config_db
        .get_slo(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "slo not found".to_string()))?;
    let events = state
        .config_db
        .list_slo_events(&id, 20)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "slo": SloResponse::from(slo),
        "events": events,
    })))
}

pub async fn update_slo(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateSloRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !VALID_WINDOWS.contains(&req.window_type.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid window_type: {}", req.window_type)));
    }
    if req.target_percentage <= 0.0 || req.target_percentage > 100.0 {
        return Err((StatusCode::BAD_REQUEST, "target_percentage must be between 0 and 100".to_string()));
    }

    let good_filters = serde_json::to_string(&req.good_filters)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let total_filters = serde_json::to_string(&req.total_filters)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let channel_ids = serde_json::to_string(&req.notification_channel_ids)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let updated = state
        .config_db
        .update_slo(
            &id,
            &req.name,
            &req.description,
            req.enabled,
            &req.service_name,
            &req.window_type,
            req.target_percentage,
            &good_filters,
            &total_filters,
            req.eval_interval_secs,
            &channel_ids,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "slo not found".to_string()));
    }

    let slo = state
        .config_db
        .get_slo(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read slo".to_string()))?;

    Ok(Json(SloResponse::from(slo)))
}

pub async fn delete_slo(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let deleted = state
        .config_db
        .delete_slo(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "slo not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_slo_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let events = state
        .config_db
        .list_slo_events(&id, 100)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "events": events })))
}
