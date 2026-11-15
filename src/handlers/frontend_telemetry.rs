//! Session-only operational telemetry for the Rush frontend, never general ingestion.
use super::rum::{RumIngestEvent, RumIngestPayload};
use crate::{AppState, RequestIdentity, TenantContext};
use axum::{
    Extension,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Map, Value};
use std::time::{Duration, Instant};

pub const MAX_BODY_BYTES: usize = 64 * 1024;
const MAX_EVENTS: usize = 1000;
const USER_REQUESTS_PER_MINUTE: u64 = 120;
const TENANT_REQUESTS_PER_MINUTE: u64 = 1200;
const MAX_COUNTER: f64 = 9_007_199_254_740_991.0;
const MAX_DURATION_MS: f64 = 86_400_000.0;
type Rejection = (StatusCode, String);
fn denied(status: StatusCode, message: &str) -> Rejection {
    (status, message.into())
}

pub async fn ingest(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Extension(identity): Extension<RequestIdentity>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, Rejection> {
    if !session_identity(&identity, &tenant.tenant_id) {
        return Err(denied(
            StatusCode::UNAUTHORIZED,
            "frontend telemetry requires a browser session",
        ));
    }
    // Use the same request-scoped session validation as tenant middleware. Neither
    // a query key nor an unverified header may turn into a frontend ingest grant.
    let caller = super::users::require_auth(&state, &headers).await?;
    let session_tenant = state
        .config_db
        .resolve_session_tenant(&caller.0, &caller.4, &caller.3, None)
        .await
        .map_err(|_| {
            denied(
                StatusCode::SERVICE_UNAVAILABLE,
                "session tenant unavailable",
            )
        })?
        .ok_or_else(|| denied(StatusCode::FORBIDDEN, "session tenant unavailable"))?;
    if session_tenant != tenant.tenant_id || caller.0 != identity.actor_id {
        return Err(denied(
            StatusCode::FORBIDDEN,
            "frontend telemetry cannot change the session tenant",
        ));
    }
    if let Err(retry_after) = consume_rate_limits(
        &state.ingest_key_limiter,
        &identity.actor_id,
        &tenant.tenant_id,
        Instant::now(),
    ) {
        return Ok((
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, retry_after.to_string())],
            "frontend telemetry rate limit exceeded",
        )
            .into_response());
    }
    state.ingest_limits.check_body("rum", &body)?;
    state.ingest_limits.check_decompressed("rum", body.len())?;
    if body.len() > MAX_BODY_BYTES {
        return Err(denied(
            StatusCode::PAYLOAD_TOO_LARGE,
            "frontend telemetry payload too large",
        ));
    }
    if !super::rum::rum_enabled(&state).await {
        return Err(denied(StatusCode::FORBIDDEN, "RUM ingestion is disabled"));
    }
    let payload: RumIngestPayload = serde_json::from_slice(&body).map_err(|_| {
        denied(
            StatusCode::BAD_REQUEST,
            "invalid frontend telemetry payload",
        )
    })?;
    state
        .ingest_limits
        .check_entities("rum", payload.events.len())?;
    let payload = sanitize_payload(payload)?;
    if payload.events.is_empty() {
        return Ok(axum::Json(serde_json::json!({"accepted":0})).into_response());
    }
    // Existing tenant signal gate, durable writer/backpressure, usage metering,
    // and finite-label RUM ingest metrics remain the sole write path.
    match super::rum::ingest_events(state, tenant, payload, body.len()).await {
        Ok(response) => Ok(response.into_response()),
        Err((status, message))
            if matches!(
                status,
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
            ) =>
        {
            Ok((status, [(header::RETRY_AFTER, "10")], message).into_response())
        }
        Err(error) => Err(error),
    }
}
fn session_identity(identity: &RequestIdentity, tenant: &str) -> bool {
    identity.authenticated
        && identity.credential_type == "session"
        && identity.actor_type == "user"
        && !identity.actor_id.is_empty()
        && identity.tenant_id == tenant
}
fn consume_rate_limits(
    limiter: &dashmap::DashMap<String, (u64, Instant)>,
    user: &str,
    tenant: &str,
    now: Instant,
) -> Result<(), u64> {
    for (key, limit) in [
        (
            format!("frontend-user:{tenant}:{user}"),
            USER_REQUESTS_PER_MINUTE,
        ),
        (
            format!("frontend-tenant:{tenant}"),
            TENANT_REQUESTS_PER_MINUTE,
        ),
    ] {
        if limiter.len() >= 100_000 && !limiter.contains_key(&key) {
            limiter.retain(|_, (_, started)| {
                now.saturating_duration_since(*started) < Duration::from_secs(60)
            });
            if limiter.len() >= 100_000 {
                return Err(60);
            }
        }
        let mut entry = limiter.entry(key).or_insert((0, now));
        let elapsed = now.saturating_duration_since(entry.1);
        if elapsed >= Duration::from_secs(60) {
            *entry = (0, now);
        }
        if entry.0 >= limit {
            return Err(60 - elapsed.as_secs().min(59));
        }
        entry.0 += 1;
    }
    Ok(())
}
fn safe_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
fn category_path(path: &str) -> String {
    let category = path
        .split(['?', '#'])
        .next()
        .unwrap_or("")
        .split('/')
        .nth(1)
        .unwrap_or("");
    match category {
        "" => "/",
        "apm" | "logs" | "metrics" | "rum" | "dashboards" | "notebooks" | "anomalies" | "slos"
        | "monitors" | "profiles" | "services" | "investigate" | "control"
        | "telemetry-quality" | "noise-waste" | "capacity" => return format!("/{category}"),
        _ => "/other",
    }
    .into()
}
fn sanitize_payload(mut payload: RumIngestPayload) -> Result<RumIngestPayload, Rejection> {
    if payload.events.len() > MAX_EVENTS {
        return Err(denied(
            StatusCode::PAYLOAD_TOO_LARGE,
            "frontend telemetry contains too many events",
        ));
    }
    let meta = &mut payload.meta;
    if !safe_name(&meta.app_name) {
        return Err(denied(
            StatusCode::BAD_REQUEST,
            "invalid frontend application name",
        ));
    }
    if meta.app_name != "rush-ui" {
        meta.app_name = format!("rush-ui:{}", meta.app_name);
    }
    if meta.app_version.is_empty()
        || meta.app_version.len() > 64
        || !meta
            .app_version
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-+".contains(&b))
    {
        meta.app_version = "unknown".into();
    }
    if !matches!(
        meta.environment.as_str(),
        "production" | "development" | "test" | "staging"
    ) {
        meta.environment = "unknown".into();
    }
    meta.page_path = category_path(&meta.page_path);
    meta.session_id.clear();
    meta.user_id.clear();
    meta.page_url.clear();
    meta.referrer.clear();
    meta.view_name.clear();
    // These optional metadata strings are not needed for operational health.
    meta.browser_name.clear();
    meta.browser_version.clear();
    meta.os_name.clear();
    meta.os_version.clear();
    if !matches!(meta.device_type.as_str(), "desktop" | "mobile" | "tablet") {
        meta.device_type = "unknown".into();
    }
    meta.screen_width = meta.screen_width.min(16384);
    meta.screen_height = meta.screen_height.min(16384);
    payload.events = payload
        .events
        .into_iter()
        .filter_map(sanitize_event)
        .collect();
    Ok(payload)
}
fn sanitize_event(mut event: RumIngestEvent) -> Option<RumIngestEvent> {
    match event.event_type.as_str() {
        "custom"
            if matches!(
                event.event_name.as_str(),
                "frontend_started"
                    | "query_request_cache"
                    | "polling_refresh"
                    | "frontend_api_request"
                    | "frontend_api_attempt"
                    | "frontend_api_retry"
                    | "frontend_api_body"
                    | "frontend_api_probe"
                    | "frontend_api_outage"
                    | "frontend_exception"
                    | "frontend_navigation"
                    | "frontend_investigation_stream"
                    | "frontend_investigation_first_response"
                    | "frontend_investigation_discarded"
                    | "frontend_telemetry_dropped"
            ) => {}
        "web_vital"
            if matches!(
                event.vital_name.as_str(),
                "LCP" | "CLS" | "INP" | "FCP" | "TTFB"
            ) =>
        {
            event.event_name.clear();
        }
        "pageview" => {
            event.event_name = "pageview".into();
        }
        "error" | "frustration" | "interaction" | "long_task" | "loaf" => {
            event.event_name.clear();
        }
        _ => return None,
    }
    if event.event_type != "web_vital" {
        event.vital_name.clear();
        event.vital_rating.clear();
        event.vital_value = 0.0;
    }
    event.timestamp = None;
    event.trace_id.clear();
    event.span_id.clear();
    event.duration_ms = event.duration_ms.clamp(0.0, MAX_DURATION_MS);
    event.vital_value = event.vital_value.clamp(0.0, MAX_DURATION_MS);
    if !matches!(
        event.vital_rating.as_str(),
        "good" | "needs-improvement" | "poor"
    ) {
        event.vital_rating.clear();
    }
    if event.event_type == "error" {
        event.error_message = "Client error".into();
        if !matches!(
            event.error_type.as_str(),
            "Error"
                | "TypeError"
                | "RangeError"
                | "ReferenceError"
                | "SyntaxError"
                | "URIError"
                | "EvalError"
                | "UnhandledRejection"
        ) {
            event.error_type = "Error".into();
        }
    } else {
        event.error_message.clear();
        event.error_type.clear();
    }
    event.error_stack.clear();
    let tag = event
        .interaction_target
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    event.interaction_target = match tag.as_str() {
        "button" | "a" | "input" | "textarea" | "select" | "canvas" | "div" | "span" | "label"
        | "form" => tag,
        _ => String::new(),
    };
    if !matches!(
        event.interaction_type.as_str(),
        "click" | "rage_click" | "dead_click" | "error_click"
    ) {
        event.interaction_type.clear();
    }
    event.attributes = sanitize_attributes(&event.attributes);
    Some(event)
}
fn sanitize_attributes(raw: &str) -> String {
    let Ok(Value::Object(attributes)) = serde_json::from_str::<Value>(raw) else {
        return "{}".into();
    };
    let mut safe = Map::new();
    for (key, value) in attributes {
        if numeric_attribute(&key) {
            if let Some(number) = value.as_f64().filter(|n| n.is_finite() && *n >= 0.0) {
                safe.insert(
                    key,
                    if number >= MAX_COUNTER {
                        serde_json::json!(9_007_199_254_740_991_u64)
                    } else {
                        value
                    },
                );
            }
        } else if let Some(allowed) = string_attribute(&key)
            && let Some(value) = value.as_str()
        {
            safe.insert(
                key,
                Value::String(
                    if allowed.contains(&value) {
                        value
                    } else {
                        "other"
                    }
                    .into(),
                ),
            );
        }
    }
    Value::Object(safe).to_string()
}
fn numeric_attribute(key: &str) -> bool {
    matches!(
        key,
        "count"
            | "duration_count"
            | "duration_ms_sum"
            | "duration_ms_max"
            | "startup_duration_ms"
            | "cache_hits"
            | "fresh_hits"
            | "stale_hits"
            | "revalidation_failures"
            | "oversized_bypasses"
            | "cache_entries"
            | "cache_bytes"
            | "in_flight"
            | "in_flight_deduplications"
            | "superseded_or_aborted"
            | "cache_misses"
            | "cache_evictions"
            | "refreshes"
            | "failures"
            | "skipped_overlaps"
            | "backoffs"
            | "average_duration_ms"
            | "maximum_duration_ms"
            | "maximum_backoff_ms"
            | "start"
            | "blockingDuration"
            | "scripts"
    ) || matches!(
        key,
        "duration_ms_le_50"
            | "duration_ms_le_100"
            | "duration_ms_le_250"
            | "duration_ms_le_500"
            | "duration_ms_le_1000"
            | "duration_ms_le_2500"
            | "duration_ms_le_5000"
            | "duration_ms_le_10000"
            | "duration_ms_le_30000"
            | "duration_ms_le_60000"
    )
}
fn string_attribute(key: &str) -> Option<&'static [&'static str]> {
    Some(match key {
        "cancel_reason" => &[
            "caller",
            "none",
            "session_changed",
            "superseded_or_invalidated",
        ],
        "category" => &[
            "alert_list",
            "anomaly",
            "capacity",
            "dashboard",
            "explore_live",
            "integration_activity",
            "notebook",
            "service_graph",
            "slo_detail",
            "slo_list",
        ],
        "kind" => &["evidence", "report"],
        "method" => &[
            "DELETE", "GET", "HEAD", "OPTIONS", "OTHER", "PATCH", "POST", "PUT",
        ],
        "operation" => &[
            "anomalies",
            "anomaly-events",
            "apikeys",
            "auth",
            "bubbleup",
            "dashboards",
            "explore",
            "export",
            "exports",
            "features",
            "frontend",
            "groups",
            "integrations",
            "investigate",
            "license",
            "logs",
            "monitors",
            "notebooks",
            "other",
            "profiles",
            "prometheus",
            "query",
            "retention",
            "roles",
            "rum",
            "services",
            "sessions",
            "settings",
            "slo",
            "slos",
            "stats",
            "suggest",
            "system",
            "tenants",
            "traces",
            "users",
        ],
        "outcome" => &[
            "agent_error",
            "body_error",
            "cancelled",
            "completed",
            "connection_error",
            "decode_error",
            "duplicated",
            "failed",
            "http_5xx",
            "http_error",
            "idle_timeout",
            "interrupted",
            "invalid_response",
            "limit_exceeded",
            "network",
            "network_error",
            "offline",
            "other_error",
            "reachable",
            "redirected",
            "stream_error",
            "success",
            "timeout",
        ],
        "reason" => &[
            "event_limit",
            "field_truncated",
            "http_5xx",
            "http_error",
            "invalid_response",
            "malformed",
            "network",
            "offline",
            "oversized",
            "timeout",
            "unknown_event",
        ],
        "route" => &[
            "alert-channel-add",
            "alerts",
            "anomaly",
            "anomaly-add",
            "anomaly-detail",
            "argocd-detail",
            "audit",
            "capacity",
            "dashboard",
            "dashboards",
            "deploys",
            "explore",
            "fluxcd-detail",
            "funnels",
            "integration-page",
            "integrations",
            "investigate",
            "kubernetes-detail",
            "log-views",
            "login",
            "logs",
            "metric-detail",
            "metrics",
            "monitor-create",
            "monitor-detail",
            "monitor-edit",
            "noise-waste",
            "notebook",
            "notebooks",
            "other",
            "profiles",
            "rum",
            "rum-detail",
            "service-detail",
            "services",
            "settings",
            "slo-detail",
            "slos",
            "sre-agent",
            "sre-agent-session",
            "sso-setup",
            "telemetry-quality",
            "trace",
            "traces",
            "usage",
        ],
        "source" => &["bootstrap", "router", "vue"],
        "stage" => &[
            "event",
            "lazy_chunk",
            "mount",
            "other",
            "render",
            "setup",
            "watcher",
        ],
        "status_class" => &["1xx", "2xx", "3xx", "4xx", "5xx", "none"],
        _ => return None,
    })
}

#[cfg(test)]
#[path = "frontend_telemetry_tests.rs"]
mod tests;
