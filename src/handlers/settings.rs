use axum::{Json, extract::{Path, State}, http::{HeaderMap, StatusCode}, response::IntoResponse};
use serde::{Deserialize, Serialize};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use rand::Rng;

use crate::AppState;
use crate::handlers::users::require_admin;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Serialize)]
pub struct ApiKeyListEntry {
    pub id: String,
    pub name: String,
    pub prefix: String,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct ApiKeyCreated {
    pub id: String,
    pub name: String,
    pub key: String,
    pub prefix: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
}

fn generate_api_key() -> String {
    let mut rng = rand::rng();
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz0123456789".chars().collect();
    (0..64).map(|_| chars[rng.random_range(0..chars.len())]).collect()
}

/// Hash an API key using HMAC-SHA256 keyed with RUSH_API_KEY_SECRET.
/// Produces a consistent hash for lookups while preventing offline
/// dictionary attacks against a stolen database.
///
/// # Panics in debug builds / warns in release if RUSH_API_KEY_SECRET is absent or weak.
pub fn hash_api_key(key: &str) -> String {
    let secret = std::env::var("RUSH_API_KEY_SECRET").unwrap_or_default();
    if secret.len() < 32 {
        // An empty or short key makes HMAC equivalent to a plain hash, enabling
        // offline dictionary attacks against a stolen api_keys table. Warn ONCE —
        // this runs on every API-key hash (every ingest request), so a per-call
        // warning would flood the logs.
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                "RUSH_API_KEY_SECRET is not set or shorter than 32 bytes; \
                 API key hashing is insecure — set a strong random secret in production"
            );
        });
    }
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts any key length");
    mac.update(key.as_bytes());
    format!("{:x}", mac.finalize().into_bytes())
}

pub async fn list_api_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let rows = state.config_db.list_api_keys().await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;
    let keys: Vec<ApiKeyListEntry> = rows
        .into_iter()
        .map(|(id, name, prefix, created_at)| ApiKeyListEntry { id, name, prefix, created_at })
        .collect();
    Ok(Json(serde_json::json!({ "keys": keys })))
}

pub async fn create_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let id = uuid::Uuid::new_v4().to_string();
    let key = generate_api_key();
    let key_hash = hash_api_key(&key);
    let prefix = key[..8].to_string();

    state.config_db.create_api_key(&id, &req.name, &key_hash, &prefix).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;

    tracing::info!(
        event = "api_key_created",
        key_id = %id,
        key_name = %req.name,
        admin = %caller.1,
        "API key created"
    );

    // Return the full key ONLY on creation
    Ok(Json(ApiKeyCreated {
        id,
        name: req.name,
        key,
        prefix,
        created_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    }))
}

/// GET /api/v1/features — public, no auth required.
/// Returns which optional integrations are enabled so the UI can hide/show nav items.
pub async fn get_features(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let argocd_enabled = std::env::var("ARGOCD_NAMESPACE").is_ok()
        || state
            .config_db
            .get_setting("argocd_enabled").await
            .ok()
            .flatten()
            .map(|v| v == "true")
            .unwrap_or(false);

    let sre_agent_enabled = state
        .config_db
        .get_setting("sre_agent_enabled").await
        .ok()
        .flatten()
        .map(|v| v == "true")
        .unwrap_or(false);

    let export_max_rows = crate::handlers::export::read_export_max_rows(&state).await;

    Json(serde_json::json!({
        "argocd": argocd_enabled,
        "sre_agent": sre_agent_enabled,
        "export_max_rows": export_max_rows,
    }))
}

/// PUT /api/v1/settings/export-max-rows — admin only.
/// Sets the maximum number of rows a user may export from Explore.
pub async fn set_export_max_rows(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (axum::http::StatusCode, String)> {
    crate::handlers::users::require_admin(&state, &headers).await?;

    let value = body.get("value").and_then(|v| v.as_u64()).ok_or_else(|| {
        (axum::http::StatusCode::BAD_REQUEST, "missing or invalid 'value' (expected a positive integer)".to_string())
    })?;
    let value = value.clamp(1, crate::handlers::export::EXPORT_MAX_ROWS_CEILING);

    state.config_db.set_setting("export_max_rows", &value.to_string()).await.map_err(|e| {
        tracing::error!(error = %e, "failed to set export_max_rows");
        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "failed to save setting".to_string())
    })?;

    Ok(Json(serde_json::json!({ "export_max_rows": value })))
}

/// Defaults + clamps for the SRE agent's per-investigation cost budget. Must
/// stay in sync with sre-agent's LoopBudget (which re-clamps defensively).
const SRE_AGENT_DEFAULT_MAX_TOOL_STEPS: u64 = 40;
const SRE_AGENT_DEFAULT_MAX_LLM_CALLS: u64 = 55;

/// GET /api/v1/settings/sre-agent — admin only.
/// Current investigation budget (defaults when unset).
pub async fn get_sre_agent_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let read = |key: &'static str, default: u64| {
        let db = state.config_db.clone();
        async move {
            db.get_setting(key).await.ok().flatten()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(default)
        }
    };
    let max_tool_steps = read("sre_agent_max_tool_steps", SRE_AGENT_DEFAULT_MAX_TOOL_STEPS).await;
    let max_llm_calls = read("sre_agent_max_llm_calls", SRE_AGENT_DEFAULT_MAX_LLM_CALLS).await;
    // Same key /api/v1/features exposes as `sre_agent` — this is the UI switch.
    let enabled = state.config_db.get_setting("sre_agent_enabled").await
        .ok().flatten().map(|v| v == "true").unwrap_or(false);
    Ok(Json(serde_json::json!({
        "enabled": enabled,
        "max_tool_steps": max_tool_steps,
        "max_llm_calls": max_llm_calls,
        "defaults": {
            "max_tool_steps": SRE_AGENT_DEFAULT_MAX_TOOL_STEPS,
            "max_llm_calls": SRE_AGENT_DEFAULT_MAX_LLM_CALLS,
        },
    })))
}

/// PUT /api/v1/settings/sre-agent — admin only.
/// Sets the SRE agent's per-investigation budget: max tool-executing rounds
/// and max total LLM calls (cost control). Values are clamped server-side;
/// the agent clamps again on read.
pub async fn set_sre_agent_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;

    // Optional `enabled` toggle: strictly a JSON bool when present.
    if let Some(enabled_val) = body.get("enabled") {
        let enabled = enabled_val.as_bool().ok_or_else(|| {
            (StatusCode::BAD_REQUEST, "invalid 'enabled' (expected a boolean)".to_string())
        })?;
        state.config_db.set_setting("sre_agent_enabled", if enabled { "true" } else { "false" }).await.map_err(|e| {
            tracing::error!(error = %e, "failed to save sre_agent_enabled");
            (StatusCode::INTERNAL_SERVER_ERROR, "failed to save setting".to_string())
        })?;
        // Toggle-only update: budget fields are optional in this case.
        if body.get("max_tool_steps").is_none() && body.get("max_llm_calls").is_none() {
            return Ok(Json(serde_json::json!({ "enabled": enabled })));
        }
    }

    let steps = body.get("max_tool_steps").and_then(|v| v.as_u64()).ok_or_else(|| {
        (StatusCode::BAD_REQUEST, "missing or invalid 'max_tool_steps' (expected a positive integer)".to_string())
    })?;
    let calls = body.get("max_llm_calls").and_then(|v| v.as_u64()).ok_or_else(|| {
        (StatusCode::BAD_REQUEST, "missing or invalid 'max_llm_calls' (expected a positive integer)".to_string())
    })?;

    let steps = steps.clamp(4, 200);
    // LLM calls must exceed tool steps (retries/critique/summary need slack).
    let calls = calls.clamp(steps + 2, 300);

    for (key, value) in [
        ("sre_agent_max_tool_steps", steps),
        ("sre_agent_max_llm_calls", calls),
    ] {
        state.config_db.set_setting(key, &value.to_string()).await.map_err(|e| {
            tracing::error!(error = %e, key, "failed to save sre-agent setting");
            (StatusCode::INTERNAL_SERVER_ERROR, "failed to save setting".to_string())
        })?;
    }

    Ok(Json(serde_json::json!({ "max_tool_steps": steps, "max_llm_calls": calls })))
}

pub async fn delete_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let deleted = state.config_db.delete_api_key(&id).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    }
    tracing::info!(
        event = "api_key_deleted",
        key_id = %id,
        admin = %caller.1,
        "API key deleted"
    );
    Ok(StatusCode::NO_CONTENT)
}
