//! Reverse proxy for operations that the SRE-agent service must execute.
//!
//! The browser talks only to query-api; query-api authenticates the session,
//! derives the caller's real tenant, and forwards the SRE-agent routes to the
//! agent (`SRE_AGENT_URL`, default `http://localhost:8081`). Forwarding the
//! authenticated tenant server-side means the agent no longer trusts a
//! browser-supplied `tenant_id`/`scopes` (which it would otherwise read
//! verbatim) — closing a tenant-spoofing gap.
//!
//! `POST /investigate` is a long-lived SSE stream and is passed through
//! unbuffered. Investigation session history is read directly from query-api's
//! ClickHouse-backed config store in `sre_sessions`.

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use std::{sync::OnceLock, time::Duration};

use crate::AppState;
use crate::handlers::settings::{SreAgentAccessDecision, sre_agent_access_decision};
use crate::handlers::users::{is_api_key_principal, require_agent_principal};

const AGENT_HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const AGENT_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const TEMPLATE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_TEMPLATE_BYTES: usize = 1024 * 1024;
/// An investigation request is a prompt plus a little context.
pub const MAX_INVESTIGATE_BODY_BYTES: usize = 1024 * 1024;

/// SSE can run for minutes, so the client bounds inactivity without imposing
/// a fixed total lifetime on a healthy stream.
fn client() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .read_timeout(AGENT_IDLE_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
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

/// Shared credential for the internal SRE-agent API. Refusing to proxy when it
/// is absent avoids silently falling back to an unauthenticated agent.
fn sre_internal_token() -> Result<HeaderValue, (StatusCode, String)> {
    let token = std::env::var("SRE_AGENT_INTERNAL_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "SRE agent internal authentication is not configured".to_string(),
            )
        })?;
    let mut token = HeaderValue::from_str(&token).map_err(unavailable)?;
    token.set_sensitive(true);
    Ok(token)
}

fn with_internal_token(
    request: reqwest::RequestBuilder,
    token: HeaderValue,
) -> reqwest::RequestBuilder {
    request.header("x-rush-internal-token", token)
}

/// Tool scopes forwarded to the agent. API keys get telemetry tools only:
/// repository contents, Kubernetes/GitOps, and cluster access stay with
/// interactive users. The agent treats "all" as including Kubernetes, so keys
/// get the telemetry signals by name instead.
const API_KEY_SCOPES: [&str; 4] = ["logs", "traces", "metrics", "profiles"];

fn scopes_for_caller(caller: &AuthenticatedCaller) -> serde_json::Value {
    if is_api_key_principal(&caller.0) {
        serde_json::json!(API_KEY_SCOPES)
    } else {
        scopes_for_role(&caller.4)
    }
}

fn scopes_for_role(role: &str) -> serde_json::Value {
    if role == "admin" {
        serde_json::json!(["all", "code", "kube_cluster"])
    } else if role == "write" {
        serde_json::json!(["all", "code"])
    } else {
        serde_json::json!(["all"])
    }
}

fn unavailable(e: impl std::fmt::Display) -> (StatusCode, String) {
    crate::api_error::internal_legacy_with_status(
        StatusCode::SERVICE_UNAVAILABLE,
        "sre_agent.proxy",
        e,
    )
}

type AuthenticatedCaller = (String, String, String, String, String);

async fn enforce_agent_access(
    state: &AppState,
    headers: &HeaderMap,
    caller: &AuthenticatedCaller,
    audit_denial: bool,
) -> Result<(), (StatusCode, String)> {
    let (reason, message) = match sre_agent_access_decision(state, &caller.3).await {
        SreAgentAccessDecision::Allowed => return Ok(()),
        SreAgentAccessDecision::Disabled => ("disabled", "SRE agent is disabled"),
        SreAgentAccessDecision::TenantDenied => (
            "tenant_not_allowed",
            "SRE agent is not enabled for this tenant",
        ),
    };

    if audit_denial {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sre_agent.investigation_denied", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("sre_agent", "investigation")
                    .outcome("failure")
                    .changes(serde_json::json!({ "reason": reason }).to_string())
                    .description("SRE agent investigation denied by access policy")
                    .context(crate::audit::actor_context_from_headers(headers)),
            )
            .await;
    }
    Err((StatusCode::FORBIDDEN, message.to_string()))
}

/// Resolve the investigation caller, auditing an API key turned away for
/// lacking agent access so attempted use of a key is visible to admins.
async fn require_investigation_caller(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AuthenticatedCaller, (StatusCode, String)> {
    let result = require_agent_principal(state, headers).await;
    if let (Err(err), Some(grant)) = (&result, crate::request_auth::api_key_grant()) {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sre_agent.investigation_denied", "api_key")
                    .actor(
                        format!("{}{}", crate::audit::API_KEY_ACTOR_PREFIX, grant.id),
                        grant.name.clone(),
                    )
                    .tenant(grant.tenant_id.clone())
                    .resource("sre_agent", "investigation")
                    .outcome("failure")
                    .changes(serde_json::json!({ "reason": "api_key_not_agent" }).to_string())
                    .description(err.1.clone())
                    .context(crate::audit::actor_context_from_headers(headers)),
            )
            .await;
    }
    result
}

/// `POST /api/v1/investigate` — SSE passthrough with tenant/scope injection.
pub async fn investigate(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let caller = require_investigation_caller(&state, &headers).await?;
    enforce_agent_access(&state, &headers, &caller, true).await?;
    let tenant = caller.3.clone();

    // Override caller-supplied tenant/scopes with server-trusted values.
    let mut obj = parse_investigation_body(&body)?;
    obj.insert("tenant_id".into(), serde_json::json!(tenant));
    // Code is a separate sensitive-read scope. Existing `all` access means
    // all telemetry/infrastructure tools, not repository contents.
    obj.insert("scopes".into(), scopes_for_caller(&caller));
    obj.insert(
        "notebook_access".into(),
        serde_json::json!(crate::notebook_delegation::issue(
            &state.config_db,
            &headers,
            &caller
        )?),
    );

    let url = format!("{}/api/v1/investigate", sre_base());
    let internal_token = sre_internal_token()?;
    let request = with_internal_token(client().post(&url), internal_token).json(&obj);
    let resp = send_with_header_deadline(request, AGENT_HEADER_TIMEOUT).await?;

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("text/event-stream")
        .to_string();

    if status.is_success() {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sre_agent.investigation_start", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("sre_agent", "investigation")
                    .outcome("success")
                    .description("SRE agent investigation started")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }

    // Stream the body straight through; disable proxy buffering so SSE events
    // reach the browser as they are produced.
    let body = Body::from_stream(resp.bytes_stream());
    Ok(Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, content_type)
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(body)
        .map_err(|e| crate::api_error::internal_legacy("sre_proxy", e))?
        .into_response())
}

/// Only a JSON object can carry the server-set tenant and scopes, so anything
/// else would reach the agent without them.
fn parse_investigation_body(
    body: &[u8],
) -> Result<serde_json::Map<String, serde_json::Value>, (StatusCode, String)> {
    match serde_json::from_slice(body) {
        Ok(serde_json::Value::Object(obj)) => Ok(obj),
        Ok(_) => Err((
            StatusCode::BAD_REQUEST,
            "investigation body must be a JSON object".into(),
        )),
        Err(e) => Err((StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}"))),
    }
}

async fn send_with_header_deadline(
    request: reqwest::RequestBuilder,
    deadline: Duration,
) -> Result<reqwest::Response, (StatusCode, String)> {
    let response = tokio::time::timeout(deadline, request.send())
        .await
        .map_err(unavailable)?
        .map_err(unavailable)?;
    if response.status().is_redirection() {
        return Err(unavailable("SRE agent returned a redirect"));
    }
    Ok(response)
}

async fn read_bounded_response(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, (StatusCode, String)> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(unavailable("SRE agent response exceeded its size limit"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(unavailable)? {
        if chunk.len() > limit - body.len() {
            return Err(unavailable("SRE agent response exceeded its size limit"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Forward a buffered (JSON) GET to the agent, returning its status + body.
async fn forward_get(url: String) -> Result<Response, (StatusCode, String)> {
    let internal_token = sre_internal_token()?;
    let request = with_internal_token(client().get(&url), internal_token).timeout(TEMPLATE_TIMEOUT);
    let resp = send_with_header_deadline(request, TEMPLATE_TIMEOUT).await?;
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let bytes = read_bounded_response(resp, MAX_TEMPLATE_BYTES).await?;
    Ok((
        status,
        [(axum::http::header::CONTENT_TYPE, content_type)],
        bytes,
    )
        .into_response())
}

/// `GET /api/v1/investigation-templates` — built-in templates (no tenant scope).
pub async fn list_investigation_templates(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let caller = require_agent_principal(&state, &headers).await?;
    enforce_agent_access(&state, &headers, &caller, false).await?;
    forward_get(format!("{}/api/v1/investigation-templates", sre_base())).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn investigation_body_must_be_a_json_object() {
        assert!(super::parse_investigation_body(br#"{"question":"why"}"#).is_ok());
        for body in [&b"[]"[..], b"\"text\"", b"null", b"not json"] {
            assert_eq!(
                super::parse_investigation_body(body).unwrap_err().0,
                axum::http::StatusCode::BAD_REQUEST
            );
        }
    }

    use super::*;

    #[tokio::test]
    async fn template_body_reader_accepts_exact_limits_and_rejects_larger_bodies() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let router = axum::Router::new().route(
                "/",
                axum::routing::get(|| async {
                    Body::from_stream(futures_util::stream::iter([
                        Ok::<_, std::convert::Infallible>(Bytes::from_static(b"test ")),
                        Ok(Bytes::from_static(b"response")),
                    ]))
                }),
            );
            axum::serve(listener, router).await.unwrap();
        });
        let url = format!("http://{address}");
        let response = client().get(&url).send().await.unwrap();
        assert!(response.content_length().is_none());
        assert_eq!(
            read_bounded_response(response, 13).await.unwrap(),
            b"test response"
        );
        let response = client().get(&url).send().await.unwrap();
        assert_eq!(
            read_bounded_response(response, 12).await.unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        server.abort();
    }

    #[test]
    fn proxy_attaches_the_internal_agent_credential() {
        let request = with_internal_token(
            reqwest::Client::new().post("http://agent.internal/api/v1/investigate"),
            HeaderValue::from_static("test-token"),
        )
        .build()
        .expect("request builds");
        assert_eq!(
            request
                .headers()
                .get("x-rush-internal-token")
                .and_then(|v| v.to_str().ok()),
            Some("test-token")
        );
    }

    #[test]
    fn source_scope_is_limited_to_write_roles() {
        assert_eq!(scopes_for_role("read"), serde_json::json!(["all"]));
        assert_eq!(scopes_for_role("write"), serde_json::json!(["all", "code"]));
        assert_eq!(
            scopes_for_role("admin"),
            serde_json::json!(["all", "code", "kube_cluster"])
        );
    }

    #[test]
    fn api_keys_receive_telemetry_scopes_only() {
        let caller = |id: &str, role: &str| {
            (
                id.to_string(),
                "n".to_string(),
                "n".to_string(),
                "t".to_string(),
                role.to_string(),
            )
        };
        for role in ["read", "write", "admin"] {
            let scopes = scopes_for_caller(&caller("apikey:k1", role));
            assert_eq!(
                scopes,
                serde_json::json!(["logs", "traces", "metrics", "profiles"])
            );
            for denied in ["all", "kubernetes", "code", "kube_cluster"] {
                assert!(
                    !scopes.as_array().unwrap().iter().any(|s| s == denied),
                    "{role} key got {denied}"
                );
            }
        }
        assert_eq!(
            scopes_for_caller(&caller("user-1", "write")),
            serde_json::json!(["all", "code"])
        );
    }
}
