use std::net::SocketAddr;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;

use crate::AppState;

/// A configured token is required from every caller, loopback included: a
/// same-pod proxy (an Istio sidecar forwards from 127.0.0.6) makes outside
/// requests arrive from loopback. Without a token, only loopback may call.
fn shutdown_allowed(
    peer_is_loopback: bool,
    expected: Option<&str>,
    provided: Option<&str>,
) -> bool {
    match expected {
        Some(expected) => provided.is_some_and(|provided| tokens_match(provided, expected)),
        None => peer_is_loopback,
    }
}

/// Compares fixed-length MACs so the time taken does not reveal how much of
/// the token matched.
fn tokens_match(provided: &str, expected: &str) -> bool {
    let tag = |value: &str| {
        let mut mac = Hmac::<Sha256>::new_from_slice(b"rush-shutdown-token")
            .expect("HMAC accepts any key length");
        mac.update(value.as_bytes());
        mac
    };
    tag(provided)
        .verify_slice(&tag(expected).finalize().into_bytes())
        .is_ok()
}

/// POST /shutdown — Kubernetes preStop hook.
///
/// The endpoint is deliberately loopback-only by default: a pod-local exec
/// hook can call it, while an accidentally exposed Service cannot terminate
/// the process. When RUSH_SHUTDOWN_TOKEN is set, every caller, loopback
/// included, must send it in X-Rush-Shutdown-Token.
pub async fn shutdown(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let expected = std::env::var("RUSH_SHUTDOWN_TOKEN")
        .ok()
        .filter(|expected| !expected.is_empty());
    let provided = headers
        .get("x-rush-shutdown-token")
        .and_then(|value| value.to_str().ok());
    if !shutdown_allowed(peer.ip().is_loopback(), expected.as_deref(), provided) {
        let reason = if expected.is_some() {
            "invalid_shutdown_token"
        } else {
            "non_loopback_request"
        };
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("system.shutdown", "system")
                    .resource("query_api", "shutdown")
                    .outcome("failure")
                    .metadata(
                        json!({
                            "reason": reason,
                            "peer": peer.ip().to_string(),
                        })
                        .to_string(),
                    )
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "status": "forbidden",
                "reason": if expected.is_some() {
                    "shutdown requires X-Rush-Shutdown-Token"
                } else {
                    "shutdown is loopback-only"
                },
            })),
        );
    }

    let first_request = state.shutdown.request();
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("system.shutdown", "system")
                .resource("query_api", "shutdown")
                .metadata(
                    json!({
                        "peer": peer.ip().to_string(),
                        "already_requested": !first_request,
                        "trigger": "prestop",
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "status": if first_request { "draining" } else { "already_draining" },
            "ready": false,
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::shutdown_allowed;

    #[test]
    fn loopback_is_trusted_only_when_no_token_is_configured() {
        assert!(shutdown_allowed(true, None, None));
        assert!(!shutdown_allowed(false, None, Some("anything")));
    }

    #[test]
    fn a_configured_token_is_required_even_from_loopback() {
        assert!(!shutdown_allowed(true, Some("s3cret-token"), None));
        assert!(!shutdown_allowed(
            true,
            Some("s3cret-token"),
            Some("s3cret-tokem")
        ));
        assert!(!shutdown_allowed(
            true,
            Some("s3cret-token"),
            Some("s3cret")
        ));
        assert!(shutdown_allowed(
            true,
            Some("s3cret-token"),
            Some("s3cret-token")
        ));
        assert!(shutdown_allowed(
            false,
            Some("s3cret-token"),
            Some("s3cret-token")
        ));
    }
}
