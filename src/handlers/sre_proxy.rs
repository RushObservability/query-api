//! Reverse proxy that fronts the SRE-agent service behind query-api.
//!
//! The browser talks only to query-api; query-api authenticates the session,
//! derives the caller's real tenant, and forwards the SRE-agent routes to the
//! agent (`SRE_AGENT_URL`, default `http://localhost:8081`). Forwarding the
//! authenticated tenant server-side means the agent no longer trusts a
//! browser-supplied `tenant_id`/`scopes` (which it would otherwise read
//! verbatim) — closing a tenant-spoofing gap.
//!
//! `POST /investigate` is a long-lived SSE stream and is passed through
//! unbuffered; `/sessions*` and `/investigation-templates` are plain JSON.

use axum::body::{Body, Bytes};
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use std::sync::OnceLock;

use crate::AppState;
use crate::handlers::users::{require_auth, require_write};

/// Shared HTTP client. No total timeout — `/investigate` streams for minutes;
/// a short connect timeout still fails fast when the agent is down.
fn client() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("build sre-agent reqwest client")
    })
}

/// Base URL of the SRE-agent service (no trailing slash).
fn sre_base() -> String {
    std::env::var("SRE_AGENT_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "http://localhost:8081".to_string())
        .trim_end_matches('/')
        .to_string()
}

fn unavailable(e: impl std::fmt::Display) -> (StatusCode, String) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        format!("SRE agent unavailable: {e}"),
    )
}

/// Rebuild a query string, forcing `tenant_id` to the authenticated tenant and
/// dropping any client-supplied value.
fn query_with_tenant(raw: Option<&str>, tenant: &str) -> String {
    let mut pairs: Vec<(String, String)> = Vec::new();
    if let Some(q) = raw {
        for (k, v) in url::form_urlencoded::parse(q.as_bytes()) {
            if k != "tenant_id" {
                pairs.push((k.into_owned(), v.into_owned()));
            }
        }
    }
    pairs.push(("tenant_id".to_string(), tenant.to_string()));
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish()
}

/// `POST /api/v1/investigate` — SSE passthrough with tenant/scope injection.
pub async fn investigate(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let caller = require_auth(&state, &headers).await?;

    // Override caller-supplied tenant/scopes with server-trusted values.
    let mut payload: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")))?;
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("tenant_id".into(), serde_json::json!(caller.3));
        obj.insert("scopes".into(), serde_json::json!(["all"]));
    }

    let url = format!("{}/api/v1/investigate", sre_base());
    let resp = client()
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(unavailable)?;

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("text/event-stream")
        .to_string();

    // Stream the body straight through; disable proxy buffering so SSE events
    // reach the browser as they are produced.
    let body = Body::from_stream(resp.bytes_stream());
    Ok(Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, content_type)
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(body)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .into_response())
}

/// Forward a buffered (JSON) GET to the agent, returning its status + body.
async fn forward_get(url: String) -> Result<Response, (StatusCode, String)> {
    let resp = client().get(&url).send().await.map_err(unavailable)?;
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let bytes = resp.bytes().await.map_err(unavailable)?;
    Ok((status, [(axum::http::header::CONTENT_TYPE, content_type)], bytes).into_response())
}

/// `GET /api/v1/sessions` — list the caller's investigation sessions.
pub async fn list_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, (StatusCode, String)> {
    let caller = require_auth(&state, &headers).await?;
    let query = query_with_tenant(q.as_deref(), &caller.3);
    forward_get(format!("{}/api/v1/sessions?{}", sre_base(), query)).await
}

/// `GET /api/v1/sessions/{id}` — fetch one session.
pub async fn get_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    RawQuery(q): RawQuery,
) -> Result<Response, (StatusCode, String)> {
    let caller = require_auth(&state, &headers).await?;
    let query = query_with_tenant(q.as_deref(), &caller.3);
    forward_get(format!(
        "{}/api/v1/sessions/{}?{}",
        sre_base(),
        urlencoding::encode(&id),
        query
    ))
    .await
}

/// `DELETE /api/v1/sessions/{id}` — delete a session (write access).
pub async fn delete_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, (StatusCode, String)> {
    require_write(&state, &headers).await?;
    let url = format!("{}/api/v1/sessions/{}", sre_base(), urlencoding::encode(&id));
    let resp = client().delete(&url).send().await.map_err(unavailable)?;
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let bytes = resp.bytes().await.map_err(unavailable)?;
    Ok((status, bytes).into_response())
}

/// `GET /api/v1/investigation-templates` — built-in templates (no tenant scope).
pub async fn list_investigation_templates(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    forward_get(format!("{}/api/v1/investigation-templates", sre_base())).await
}
