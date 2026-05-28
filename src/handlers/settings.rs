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
pub fn hash_api_key(key: &str) -> String {
    let secret = std::env::var("RUSH_API_KEY_SECRET").unwrap_or_default();
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
        (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}"))
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
        (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}"))
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

    Json(serde_json::json!({
        "argocd": argocd_enabled,
        "sre_agent": sre_agent_enabled,
    }))
}

pub async fn delete_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let deleted = state.config_db.delete_api_key(&id).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}"))
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
