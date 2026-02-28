use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::AppState;
use crate::models::alert::*;

pub async fn list_channels(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let channels = state
        .config_db
        .list_channels()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let responses: Vec<NotificationChannelResponse> = channels.into_iter().map(NotificationChannelResponse::from).collect();
    Ok(Json(serde_json::json!({ "channels": responses })))
}

pub async fn create_channel(
    State(state): State<AppState>,
    Json(req): Json<CreateChannelRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let valid_types = ["webhook", "slack"];
    if !valid_types.contains(&req.channel_type.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid channel_type: {}", req.channel_type)));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let config = serde_json::to_string(&req.config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    state
        .config_db
        .create_channel(&id, &req.name, &req.channel_type, &config)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let channel = state
        .config_db
        .get_channel(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read created channel".to_string()))?;

    Ok((StatusCode::CREATED, Json(NotificationChannelResponse::from(channel))))
}

pub async fn delete_channel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let deleted = state
        .config_db
        .delete_channel(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "channel not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn notify_channel(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let channel = state
        .config_db
        .get_channel(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "channel not found".to_string()))?;

    let config: serde_json::Value = serde_json::from_str(&channel.config)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let url = config
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "channel config missing url".to_string()))?;

    let client = reqwest::Client::new();
    client
        .post(url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_alerts(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let alerts = state
        .config_db
        .list_alerts()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let responses: Vec<AlertRuleResponse> = alerts.into_iter().map(AlertRuleResponse::from).collect();
    Ok(Json(serde_json::json!({ "alerts": responses })))
}

pub async fn create_alert(
    State(state): State<AppState>,
    Json(req): Json<CreateAlertRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let valid_ops = [">", ">=", "<", "<=", "=", "!="];
    if !valid_ops.contains(&req.condition_op.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid condition_op: {}", req.condition_op)));
    }
    let valid_signal_types = ["apm", "metrics", "logs"];
    if !valid_signal_types.contains(&req.signal_type.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid signal_type: {}", req.signal_type)));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let query_config = serde_json::to_string(&req.query_config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let channel_ids = serde_json::to_string(&req.notification_channel_ids)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    state
        .config_db
        .create_alert(
            &id,
            &req.name,
            &req.description,
            req.enabled,
            &req.signal_type,
            &query_config,
            &req.condition_op,
            req.condition_threshold,
            req.eval_interval_secs,
            &channel_ids,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let alert = state
        .config_db
        .get_alert(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read created alert".to_string()))?;

    Ok((StatusCode::CREATED, Json(AlertRuleResponse::from(alert))))
}

pub async fn get_alert(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let alert = state
        .config_db
        .get_alert(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "alert not found".to_string()))?;
    let events = state
        .config_db
        .list_alert_events(&id, 20)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "alert": AlertRuleResponse::from(alert),
        "events": events,
    })))
}

pub async fn update_alert(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateAlertRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let valid_ops = [">", ">=", "<", "<=", "=", "!="];
    if !valid_ops.contains(&req.condition_op.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid condition_op: {}", req.condition_op)));
    }
    let valid_signal_types = ["apm", "metrics", "logs"];
    if !valid_signal_types.contains(&req.signal_type.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid signal_type: {}", req.signal_type)));
    }

    let query_config = serde_json::to_string(&req.query_config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let channel_ids = serde_json::to_string(&req.notification_channel_ids)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let updated = state
        .config_db
        .update_alert(
            &id,
            &req.name,
            &req.description,
            req.enabled,
            &req.signal_type,
            &query_config,
            &req.condition_op,
            req.condition_threshold,
            req.eval_interval_secs,
            &channel_ids,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "alert not found".to_string()));
    }

    let alert = state
        .config_db
        .get_alert(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read alert".to_string()))?;

    Ok(Json(AlertRuleResponse::from(alert)))
}

pub async fn delete_alert(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let deleted = state
        .config_db
        .delete_alert(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "alert not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_alert_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let events = state
        .config_db
        .list_alert_events(&id, 100)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "events": events })))
}

pub async fn list_all_alert_events(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let events = state
        .config_db
        .list_all_alert_events(200)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "events": events })))
}
