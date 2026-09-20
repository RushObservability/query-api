use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::users::{require_auth, require_write};
use crate::models::alert::*;

async fn normalize_alert_route(
    state: &AppState,
    tenant_id: &str,
    mut req: CreateAlertRouteRequest,
) -> Result<CreateAlertRouteRequest, (StatusCode, String)> {
    req.name = req.name.trim().to_string();
    if req.name.is_empty() || req.name.len() > 255 {
        return Err((
            StatusCode::BAD_REQUEST,
            "route name must be between 1 and 255 characters".to_string(),
        ));
    }

    req.priorities.sort_unstable();
    req.priorities.dedup();
    if req
        .priorities
        .iter()
        .any(|priority| !(1..=5).contains(priority))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "route priorities must be between P1 and P5".to_string(),
        ));
    }

    if req.tag_matchers.len() > 32 {
        return Err((
            StatusCode::BAD_REQUEST,
            "a route can have at most 32 tag matchers".to_string(),
        ));
    }
    let mut tag_matchers = std::collections::BTreeMap::new();
    for (key, value) in req.tag_matchers {
        let key = key.trim().to_string();
        let value = value.trim().to_string();
        if key.is_empty() || value.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "route tag keys and values must not be empty".to_string(),
            ));
        }
        if key.len() > 128 || value.len() > 255 {
            return Err((
                StatusCode::BAD_REQUEST,
                "route tag keys must be at most 128 characters and values at most 255".to_string(),
            ));
        }
        tag_matchers.insert(key, value);
    }
    req.tag_matchers = tag_matchers;

    let mut channel_ids = Vec::new();
    for channel_id in req.channel_ids {
        let channel_id = channel_id.trim().to_string();
        if channel_id.is_empty() || channel_ids.contains(&channel_id) {
            continue;
        }
        if state
            .config_db
            .get_channel(&channel_id, tenant_id)
            .await
            .map_err(|e| crate::api_error::internal_legacy("alert_routes", e))?
            .is_none()
        {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("notification channel does not exist: {channel_id}"),
            ));
        }
        channel_ids.push(channel_id);
    }
    if channel_ids.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "select at least one notification channel".to_string(),
        ));
    }
    req.channel_ids = channel_ids;
    Ok(req)
}

pub async fn list_alert_routes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let routes = state
        .config_db
        .list_alert_routes(&tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alert_routes", e))?;
    Ok(Json(serde_json::json!({ "routes": routes })))
}

pub async fn create_alert_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<CreateAlertRouteRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let req = normalize_alert_route(&state, &tenant.tenant_id, req).await?;
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let route = AlertRoute {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: tenant.tenant_id.clone(),
        name: req.name,
        enabled: req.enabled,
        priorities: req.priorities,
        tag_matchers: req.tag_matchers,
        channel_ids: req.channel_ids,
        created_at: now.clone(),
        updated_at: now,
    };
    state
        .config_db
        .create_alert_route(&route)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alert_routes", e))?;

    state
        .audit
        .log(
            crate::audit::AuditEvent::new("alert_route.create", "user")
                .actor(caller.0, caller.1)
                .tenant(tenant.tenant_id)
                .resource("alert_route", &route.id)
                .changes(
                    serde_json::json!({
                        "name": route.name,
                        "enabled": route.enabled,
                        "priorities": route.priorities,
                        "tag_matchers": route.tag_matchers,
                        "channel_ids": route.channel_ids,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok((StatusCode::CREATED, Json(route)))
}

pub async fn update_alert_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateAlertRouteRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let existing = state
        .config_db
        .get_alert_route(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alert_routes", e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "alert route not found".to_string()))?;
    let req = normalize_alert_route(&state, &tenant.tenant_id, req).await?;
    let route = AlertRoute {
        id: id.clone(),
        tenant_id: tenant.tenant_id.clone(),
        name: req.name,
        enabled: req.enabled,
        priorities: req.priorities,
        tag_matchers: req.tag_matchers,
        channel_ids: req.channel_ids,
        created_at: existing.created_at,
        updated_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
    };
    state
        .config_db
        .update_alert_route(&route)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alert_routes", e))?;

    state
        .audit
        .log(
            crate::audit::AuditEvent::new("alert_route.update", "user")
                .actor(caller.0, caller.1)
                .tenant(tenant.tenant_id)
                .resource("alert_route", &id)
                .changes(
                    serde_json::json!({
                        "name": route.name,
                        "enabled": route.enabled,
                        "priorities": route.priorities,
                        "tag_matchers": route.tag_matchers,
                        "channel_ids": route.channel_ids,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(route))
}

pub async fn delete_alert_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_alert_route(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alert_routes", e))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "alert route not found".to_string()));
    }
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("alert_route.delete", "user")
                .actor(caller.0, caller.1)
                .tenant(tenant.tenant_id)
                .resource("alert_route", &id)
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_channels(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let channels = state
        .config_db
        .list_channels(&tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?;
    let responses: Vec<NotificationChannelResponse> = channels
        .into_iter()
        .map(NotificationChannelResponse::from)
        .collect();
    Ok(Json(serde_json::json!({ "channels": responses })))
}

pub async fn create_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<CreateChannelRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    if req.name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must not be empty".to_string(),
        ));
    }
    if req.name.len() > 255 {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must not exceed 255 characters".to_string(),
        ));
    }
    let valid_types = [
        "webhook",
        "slack",
        "slack_app",
        "email",
        "pagerduty",
        "rootly",
        "discord",
        "alertmanager",
    ];
    if !valid_types.contains(&req.channel_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid channel_type: {}", req.channel_type),
        ));
    }

    // Validate type-specific config
    validate_channel_config(&req.channel_type, &req.config).await?;

    let id = uuid::Uuid::new_v4().to_string();
    let config =
        serde_json::to_string(&req.config).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    state
        .config_db
        .create_channel(
            &id,
            &tenant.tenant_id,
            &req.name,
            &req.channel_type,
            &config,
        )
        .await
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?;

    let channel = state
        .config_db
        .get_channel(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read created channel".to_string(),
            )
        })?;

    state
        .audit
        .log(
            crate::audit::AuditEvent::new("notification_channel.create", "user")
                .actor(caller.0, caller.1)
                .tenant(tenant.tenant_id.clone())
                .resource("notification_channel", id)
                .changes(
                    serde_json::json!({ "name": req.name, "channel_type": req.channel_type })
                        .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok((
        StatusCode::CREATED,
        Json(NotificationChannelResponse::from(channel)),
    ))
}

pub async fn update_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateChannelRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let existing = state
        .config_db
        .get_channel(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "channel not found".to_string()))?;
    let existing_config = serde_json::from_str(&existing.config).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "stored channel config is invalid".to_string(),
        )
    })?;
    let config_value = merge_channel_config(existing_config, req.config);
    validate_channel_config(&existing.channel_type, &config_value).await?;
    let config = serde_json::to_string(&config_value)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let updated = state
        .config_db
        .update_channel(&id, &tenant.tenant_id, &req.name, &config, req.enabled)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "channel not found".to_string()));
    }

    let channel = state
        .config_db
        .get_channel(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read updated channel".to_string(),
            )
        })?;

    state
        .audit
        .log(
            crate::audit::AuditEvent::new("notification_channel.update", "user")
                .actor(caller.0, caller.1)
                .tenant(tenant.tenant_id.clone())
                .resource("notification_channel", id)
                .changes(
                    serde_json::json!({ "name": req.name, "enabled": req.enabled }).to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(NotificationChannelResponse::from(channel)))
}

pub async fn delete_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_channel(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "channel not found".to_string()));
    }
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("notification_channel.delete", "user")
                .actor(caller.0, caller.1)
                .tenant(tenant.tenant_id.clone())
                .resource("notification_channel", id)
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn test_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let channel = state
        .config_db
        .get_channel(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "channel not found".to_string()))?;

    let test_message = format!(
        "Test notification from Rush Observability for channel '{}'. If you receive this, the channel is configured correctly.",
        channel.name,
    );

    let smtp_config = crate::alert_engine::SmtpConfig {
        host: std::env::var("SMTP_HOST").ok(),
        port: std::env::var("SMTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(587),
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
    )
    .await;

    let (status, error_msg) = match &result {
        Ok(()) => ("sent", String::new()),
        Err(e) => ("failed", e.clone()),
    };

    state
        .audit
        .log(
            crate::audit::AuditEvent::new("notification_channel.test", "user")
                .actor(caller.0, caller.1)
                .tenant(tenant.tenant_id.clone())
                .resource("notification_channel", &id)
                .outcome(if result.is_ok() { "success" } else { "failure" })
                .changes(serde_json::json!({"channel_type": channel.channel_type}).to_string())
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    let _ = state
        .config_db
        .create_notification_log(
            &id,
            &tenant.tenant_id,
            "test",
            "Test Notification",
            "",
            status,
            &error_msg,
        )
        .await;

    match result {
        Ok(()) => Ok(Json(
            serde_json::json!({ "ok": true, "message": "Test notification sent successfully" }),
        )),
        Err(e) => Err(crate::api_error::internal_legacy_with_status(
            StatusCode::BAD_GATEWAY,
            "alerts.test_notification",
            e,
        )),
    }
}

pub async fn notify_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(tenant): Extension<TenantContext>,
    Path(id): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let channel = state
        .config_db
        .get_channel(&id, &tenant.tenant_id)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "channel not found".to_string()))?;

    let config: serde_json::Value = serde_json::from_str(&channel.config)
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?;
    if matches!(channel.channel_type.as_str(), "rootly" | "pagerduty") {
        let result = if channel.channel_type == "pagerduty" {
            crate::alert_engine::pagerduty::send(
                &config,
                &crate::alert_engine::pagerduty::manual_payload(&channel, &config, &payload),
            )
            .await
        } else {
            crate::alert_engine::rootly::send(&config, &payload).await
        };
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("notification_channel.notify", "user")
                    .actor(caller.0, caller.1)
                    .tenant(tenant.tenant_id.clone())
                    .resource("notification_channel", &id)
                    .outcome(if result.is_ok() { "success" } else { "failure" })
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        result.map_err(|e| {
            crate::api_error::internal_legacy_with_status(
                StatusCode::BAD_GATEWAY,
                "alerts.notify_channel",
                e,
            )
        })?;
        return Ok(StatusCode::NO_CONTENT);
    }
    let url = config
        .get("url")
        .or_else(|| config.get("webhook_url"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "channel config missing url".to_string(),
            )
        })?;

    crate::outbound::public_https_request(reqwest::Method::POST, url)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?
        .json(&payload)
        .send()
        .await
        .map_err(|e| {
            crate::api_error::internal_legacy_with_status(
                StatusCode::BAD_GATEWAY,
                "alerts.notify_channel",
                e,
            )
        })?;

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
        .list_notification_log(&tenant.tenant_id, 200)
        .await
        .map_err(|e| crate::api_error::internal_legacy("alerts", e))?;
    Ok(Json(serde_json::json!({ "entries": entries })))
}

fn merge_channel_config(
    mut existing: serde_json::Value,
    incoming: serde_json::Value,
) -> serde_json::Value {
    let (Some(existing), Some(incoming)) = (existing.as_object_mut(), incoming.as_object()) else {
        return incoming;
    };
    for (key, value) in incoming {
        let preserve_secret = matches!(
            key.as_str(),
            "url" | "webhook_url" | "token" | "routing_key" | "api_key" | "headers"
        ) && (value.is_null() || value.as_str().is_some_and(str::is_empty));
        if !preserve_secret {
            existing.insert(key.clone(), value.clone());
        }
    }
    existing.clone().into()
}

async fn validate_channel_config(
    channel_type: &str,
    config: &serde_json::Value,
) -> Result<(), (StatusCode, String)> {
    match channel_type {
        "slack" => {
            let has_url = config.get("webhook_url").and_then(|v| v.as_str()).is_some()
                || config.get("url").and_then(|v| v.as_str()).is_some();
            if !has_url {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "slack channel requires 'webhook_url' in config".to_string(),
                ));
            }
        }
        "email" => {
            let has_recipients = config.get("recipients").and_then(|v| v.as_str()).is_some()
                || config.get("to").and_then(|v| v.as_str()).is_some();
            if !has_recipients {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "email channel requires 'recipients' in config".to_string(),
                ));
            }
        }
        "webhook" => {
            if config.get("url").and_then(|v| v.as_str()).is_none() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "webhook channel requires 'url' in config".to_string(),
                ));
            }
        }
        "pagerduty" => crate::alert_engine::pagerduty::validate_config(config)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        "rootly" => crate::alert_engine::rootly::validate_config(config)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        "slack_app" => {
            if config.get("token").and_then(|v| v.as_str()).is_none() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "slack_app channel requires 'token' in config".to_string(),
                ));
            }
            if config.get("channel").and_then(|v| v.as_str()).is_none() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "slack_app channel requires 'channel' in config".to_string(),
                ));
            }
        }
        "discord" => {
            if config.get("webhook_url").and_then(|v| v.as_str()).is_none() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "discord channel requires 'webhook_url' in config".to_string(),
                ));
            }
        }
        "alertmanager" => {
            if config.get("url").and_then(|v| v.as_str()).is_none() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "alertmanager channel requires 'url' in config".to_string(),
                ));
            }
        }
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                "unsupported notification channel type".into(),
            ));
        }
    }
    if matches!(
        channel_type,
        "slack" | "webhook" | "discord" | "alertmanager"
    ) {
        let url = config
            .get("url")
            .or_else(|| config.get("webhook_url"))
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    "channel config missing URL".to_string(),
                )
            })?;
        crate::outbound::validate_notification_url(url)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    }
    Ok(())
}

#[cfg(test)]
mod channel_config_tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn all_channel_configs_validate_without_network_access() {
        let valid = [
            (
                "slack",
                json!({"webhook_url": "https://example.invalid/slack"}),
            ),
            ("slack", json!({"url": "https://example.invalid/legacy"})),
            ("slack_app", json!({"token": "fake-token", "channel": "C1"})),
            (
                "discord",
                json!({"webhook_url": "https://example.invalid/discord"}),
            ),
            (
                "webhook",
                json!({"url": "https://example.invalid/hook", "method": "PUT"}),
            ),
            (
                "alertmanager",
                json!({"url": "https://example.invalid/alertmanager"}),
            ),
            ("email", json!({"recipients": "oncall@example.invalid"})),
            ("email", json!({"to": "legacy@example.invalid"})),
            (
                "rootly",
                json!({"url": crate::alert_engine::rootly::WEBHOOK_URL, "token": "fake-secret"}),
            ),
            ("pagerduty", json!({"routing_key": "fake-key"})),
        ];
        for (kind, config) in valid {
            assert!(
                validate_channel_config(kind, &config).await.is_ok(),
                "{kind}"
            );
            for missing in [json!({}), json!(null), json!([])] {
                assert!(
                    validate_channel_config(kind, &missing).await.is_err(),
                    "{kind}"
                );
            }
        }
        assert!(
            validate_channel_config("unknown", &json!({"url": "https://example.invalid"}))
                .await
                .is_err()
        );
        for config in [json!({"token": "fake-token"}), json!({"channel": "C1"})] {
            assert!(validate_channel_config("slack_app", &config).await.is_err());
        }
        for kind in ["slack", "discord", "webhook", "alertmanager"] {
            for url in [
                "",
                "not-a-url",
                "file:///etc/passwd",
                "https://user:secret@example.invalid",
            ] {
                let config = json!({"url": url, "webhook_url": url});
                let error = validate_channel_config(kind, &config).await.unwrap_err();
                assert_eq!(error.0, StatusCode::BAD_REQUEST);
                assert!(!error.1.contains("user:secret"));
            }
        }
    }

    #[test]
    fn editing_preserves_omitted_empty_and_null_credentials_but_accepts_rotation() {
        for key in [
            "url",
            "webhook_url",
            "token",
            "routing_key",
            "api_key",
            "headers",
        ] {
            let secret = if key == "headers" {
                json!({"Authorization": "Bearer fake-secret"})
            } else {
                json!("fake-secret")
            };
            let existing = json!({key: secret, "channel": "old-channel"});
            for incoming in [json!({}), json!({key: ""}), json!({key: null})] {
                assert_eq!(
                    merge_channel_config(existing.clone(), incoming),
                    existing,
                    "{key}"
                );
            }
            let updated = merge_channel_config(
                existing,
                json!({key: "replacement", "channel": "new-channel"}),
            );
            assert_eq!(updated[key], "replacement");
            assert_eq!(updated["channel"], "new-channel");
        }
    }

    #[tokio::test]
    async fn pagerduty_validation_and_secret_preservation() {
        let saved = json!({"routing_key": "saved-key", "region": "eu", "severity": "error"});
        let merged = merge_channel_config(saved.clone(), json!({"routing_key": ""}));
        assert_eq!(merged, saved);
        assert!(validate_channel_config("pagerduty", &merged).await.is_ok());
        let rotated = merge_channel_config(saved, json!({"routing_key": "new-key"}));
        assert_eq!(rotated["routing_key"], "new-key");
        assert!(
            validate_channel_config("pagerduty", &json!({"routing_key": " "}))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn rootly_validation_and_secret_preservation() {
        let saved =
            json!({"url": crate::alert_engine::rootly::WEBHOOK_URL, "token": "saved-secret"});
        assert!(validate_channel_config("rootly", &saved).await.is_ok());
        let merged = merge_channel_config(saved.clone(), json!({"url": "", "token": ""}));
        assert_eq!(merged, saved);
        assert!(validate_channel_config("rootly", &merged).await.is_ok());
        assert!(
            validate_channel_config(
                "rootly",
                &json!({"url": crate::alert_engine::rootly::WEBHOOK_URL})
            )
            .await
            .is_err()
        );
        let rotated = merge_channel_config(saved, json!({"token": "rotated-secret"}));
        assert_eq!(rotated["token"], "rotated-secret");
    }
}
