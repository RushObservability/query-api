use axum::{
    Extension,
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::users::{require_auth, require_write};
use crate::models::alert::*;

pub async fn list_channels(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let channels = state
        .config_db
        .list_channels(&tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let responses: Vec<NotificationChannelResponse> = channels.into_iter().map(NotificationChannelResponse::from).collect();
    Ok(Json(serde_json::json!({ "channels": responses })))
}

pub async fn create_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<CreateChannelRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    if req.name.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name must not be empty".to_string()));
    }
    if req.name.len() > 255 {
        return Err((StatusCode::BAD_REQUEST, "name must not exceed 255 characters".to_string()));
    }
    let valid_types = ["webhook", "slack", "slack_app", "email", "pagerduty", "opsgenie", "discord", "alertmanager"];
    if !valid_types.contains(&req.channel_type.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid channel_type: {}", req.channel_type)));
    }

    // Validate type-specific config
    validate_channel_config(&req.channel_type, &req.config)?;

    let id = uuid::Uuid::new_v4().to_string();
    let config = serde_json::to_string(&req.config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    state
        .config_db
        .create_channel(&id, &tenant.tenant_id, &req.name, &req.channel_type, &config).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let channel = state
        .config_db
        .get_channel(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read created channel".to_string()))?;

    Ok((StatusCode::CREATED, Json(NotificationChannelResponse::from(channel))))
}

pub async fn update_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateChannelRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let config = serde_json::to_string(&req.config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let updated = state
        .config_db
        .update_channel(&id, &tenant.tenant_id, &req.name, &config, req.enabled).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "channel not found".to_string()));
    }

    let channel = state
        .config_db
        .get_channel(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read updated channel".to_string()))?;

    Ok(Json(NotificationChannelResponse::from(channel)))
}

pub async fn delete_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_channel(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "channel not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn test_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let channel = state
        .config_db
        .get_channel(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "channel not found".to_string()))?;

    let test_message = format!(
        "Test notification from Rush Observability for channel '{}'. If you receive this, the channel is configured correctly.",
        channel.name,
    );

    let smtp_config = crate::alert_engine::SmtpConfig {
        host: std::env::var("SMTP_HOST").ok(),
        port: std::env::var("SMTP_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(587),
        user: std::env::var("SMTP_USER").ok(),
        pass: std::env::var("SMTP_PASS").ok(),
        from: std::env::var("SMTP_FROM").unwrap_or_else(|_| "rush@localhost".to_string()),
    };
    let smtp_transport = if smtp_config.host.is_some() {
        // Build a simple transport for the test
        None // We won't build a full transport here; email tests require SMTP to be configured at startup
    } else {
        None
    };

    let http_client = reqwest::Client::new();
    let result = crate::alert_engine::send_channel_notification(
        &channel,
        &test_message,
        "Test Alert",
        "TEST",
        0.0,
        0.0,
        "",
        "",
        "This is a test notification from Rush Observability.",
        "",
        "",
        &http_client,
        &smtp_config,
        &smtp_transport,
    ).await;

    let (status, error_msg) = match &result {
        Ok(()) => ("sent", String::new()),
        Err(e) => ("failed", e.clone()),
    };

    let _ = state.config_db.create_notification_log(
        &id,
        &tenant.tenant_id,
        "test",
        "Test Notification",
        "",
        status,
        &error_msg,
    ).await;

    match result {
        Ok(()) => Ok(Json(serde_json::json!({ "ok": true, "message": "Test notification sent successfully" }))),
        Err(e) => Err((StatusCode::BAD_GATEWAY, e)),
    }
}

pub async fn notify_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let channel = state
        .config_db
        .get_channel(&id, &tenant.tenant_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "channel not found".to_string()))?;

    let config: serde_json::Value = serde_json::from_str(&channel.config)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let url = config
        .get("url")
        .or_else(|| config.get("webhook_url"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "channel config missing url".to_string()))?;

    if !url.starts_with("https://") {
        return Err((StatusCode::BAD_REQUEST, format!("channel URL must use HTTPS (got: {})", url)));
    }

    let client = reqwest::Client::new();
    client
        .post(url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_notification_log(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let entries = state
        .config_db
        .list_notification_log(&tenant.tenant_id, 200).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "entries": entries })))
}

fn validate_channel_config(channel_type: &str, config: &serde_json::Value) -> Result<(), (StatusCode, String)> {
    match channel_type {
        "slack" => {
            let has_url = config.get("webhook_url").and_then(|v| v.as_str()).is_some()
                || config.get("url").and_then(|v| v.as_str()).is_some();
            if !has_url {
                return Err((StatusCode::BAD_REQUEST, "slack channel requires 'webhook_url' in config".to_string()));
            }
        }
        "email" => {
            let has_recipients = config.get("recipients").and_then(|v| v.as_str()).is_some()
                || config.get("to").and_then(|v| v.as_str()).is_some();
            if !has_recipients {
                return Err((StatusCode::BAD_REQUEST, "email channel requires 'recipients' in config".to_string()));
            }
        }
        "webhook" => {
            if config.get("url").and_then(|v| v.as_str()).is_none() {
                return Err((StatusCode::BAD_REQUEST, "webhook channel requires 'url' in config".to_string()));
            }
        }
        "pagerduty" => {
            if config.get("routing_key").and_then(|v| v.as_str()).is_none() {
                return Err((StatusCode::BAD_REQUEST, "pagerduty channel requires 'routing_key' in config".to_string()));
            }
        }
        "opsgenie" => {
            if config.get("api_key").and_then(|v| v.as_str()).is_none() {
                return Err((StatusCode::BAD_REQUEST, "opsgenie channel requires 'api_key' in config".to_string()));
            }
        }
        "slack_app" => {
            if config.get("token").and_then(|v| v.as_str()).is_none() {
                return Err((StatusCode::BAD_REQUEST, "slack_app channel requires 'token' in config".to_string()));
            }
            if config.get("channel").and_then(|v| v.as_str()).is_none() {
                return Err((StatusCode::BAD_REQUEST, "slack_app channel requires 'channel' in config".to_string()));
            }
        }
        "discord" => {
            if config.get("webhook_url").and_then(|v| v.as_str()).is_none() {
                return Err((StatusCode::BAD_REQUEST, "discord channel requires 'webhook_url' in config".to_string()));
            }
        }
        "alertmanager" => {
            if config.get("url").and_then(|v| v.as_str()).is_none() {
                return Err((StatusCode::BAD_REQUEST, "alertmanager channel requires 'url' in config".to_string()));
            }
        }
        _ => {}
    }
    Ok(())
}
