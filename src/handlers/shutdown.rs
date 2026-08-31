use std::net::SocketAddr;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde_json::json;

use crate::AppState;

/// POST /shutdown — Kubernetes preStop hook.
///
/// The endpoint is deliberately loopback-only by default: a pod-local exec
/// hook can call it, while an accidentally exposed Service cannot terminate
/// the process. A configured RUSH_SHUTDOWN_TOKEN additionally permits a
/// sidecar/management plane to call it with X-Rush-Shutdown-Token.
pub async fn shutdown(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let token_ok = std::env::var("RUSH_SHUTDOWN_TOKEN")
        .ok()
        .filter(|expected| !expected.is_empty())
        .is_some_and(|expected| {
            headers
                .get("x-rush-shutdown-token")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|provided| provided == expected)
        });
    if !peer.ip().is_loopback() && !token_ok {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("system.shutdown", "system")
                    .resource("query_api", "shutdown")
                    .outcome("failure")
                    .metadata(
                        json!({
                            "reason": "non_loopback_request",
                            "peer": peer.ip().to_string(),
                        })
                        .to_string(),
                    )
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"status": "forbidden", "reason": "shutdown is loopback-only"})),
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
