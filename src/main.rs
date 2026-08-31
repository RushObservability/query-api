// Use jemalloc as the global allocator: lower, flatter RSS than glibc malloc
// under tokio's multi-thread allocation churn (see Cargo.toml note). Declared per
// binary crate root — the anomaly-engine binary has its own identical decl.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::Context;
use axum::http::{HeaderMap, HeaderValue, Method, Uri, header};
use axum::response::IntoResponse;
use axum::{Router, routing::any, routing::delete, routing::get, routing::post, routing::put};
use axum::{
    extract::ConnectInfo, extract::DefaultBodyLimit, extract::Request, middleware::Next,
    response::Response,
};
use clickhouse::Client;
use dashmap::DashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use rush_api::AppState;
use rush_api::RequestIdentity;
use rush_api::TenantContext;
use rush_api::alert_engine;
use rush_api::anomaly_engine;
use rush_api::buffer_topology;
use rush_api::ch_writer::ChWriter;
use rush_api::clickhouse_config::ConfigDb;
use rush_api::config::RushConfig;
use rush_api::cors::{CorsPolicy, parse_web_origin, same_web_origin};
use rush_api::handlers;
use rush_api::migrations;
use rush_api::monitor_engine;
use rush_api::query_governor::{
    AdmissionError, QUERY_LIMITS_SETTING_KEY, QueryGovernor, QueryGovernorConfig, TimeRangeError,
    ValidatedTimeRange, WorkloadClass,
};
use rush_api::retention_enforcer;
use rush_api::siem_engine;
use rush_api::slo_engine;
use rush_api::spool::{IngestBuffer, Spool};
use rush_api::stats_engine;
use rush_api::usage_accumulator::UsageAccumulator;
use rush_api::usage_tracker;

/// Return the request target that is safe to include in logs and traces.
/// Query strings commonly contain OAuth codes, search text, and other secrets.
fn request_log_path(uri: &Uri) -> &str {
    uri.path()
}

/// Result of resolving request credentials to a tenant.
///
/// `authenticated` is deliberately kept separate from the tenant name: open
/// tenants may still be selected without credentials, while locked tenants must
/// reject that same resolution before the request reaches a handler.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TenantResolution {
    tenant_id: String,
    authenticated: bool,
    credential: CredentialKind,
    api_key: Option<rush_api::clickhouse_config::ApiKeyGrant>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CredentialKind {
    Anonymous,
    Session,
    QueryKey,
    IngestKey,
}

impl TenantResolution {
    fn anonymous(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            authenticated: false,
            credential: CredentialKind::Anonymous,
            api_key: None,
        }
    }

    fn session(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            authenticated: true,
            credential: CredentialKind::Session,
            api_key: None,
        }
    }

    fn api_key(grant: rush_api::clickhouse_config::ApiKeyGrant) -> Self {
        let credential = if grant.key_type == "ingest" {
            CredentialKind::IngestKey
        } else {
            // Existing pre-QAPI-SEC-04 keys are deliberately query-only. They
            // must be replaced before collectors can ingest again.
            CredentialKind::QueryKey
        };
        Self {
            tenant_id: grant.tenant_id.clone(),
            authenticated: true,
            credential,
            api_key: Some(grant),
        }
    }
}

/// Middleware that adds security response headers to every response.
async fn security_headers_middleware(req: Request, next: Next) -> Response {
    let mut resp = next.run(req).await;
    let headers = resp.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    headers.insert(
        header::HeaderName::from_static("permissions-policy"),
        HeaderValue::from_static("geolocation=(), microphone=(), camera=()"),
    );
    headers.insert(
        header::HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
             img-src 'self' data:; font-src 'self'; connect-src 'self'; \
             object-src 'none'; frame-ancestors 'none'; base-uri 'self'",
        ),
    );
    resp
}

fn is_state_changing_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

fn trust_forwarded_origin_headers(
    production: bool,
    trust_proxy_headers: bool,
    peer_ip: Option<IpAddr>,
    trusted_proxy_cidrs: &[String],
) -> bool {
    !production
        && trust_proxy_headers
        && peer_ip.is_some_and(|ip| {
            !trusted_proxy_cidrs.is_empty()
                && rush_api::api_key_auth::source_allowed(ip, trusted_proxy_cidrs)
        })
}

/// Validate the browser origin for requests authenticated by an ambient
/// browser credential. Production compares only with the canonical public
/// origin. Local development may use an explicit allowlist, or reconstruct a
/// target origin from the direct Host header. Forwarded host/protocol headers
/// are considered only when the direct peer belongs to a configured trusted
/// proxy network.
fn request_origin_allowed(req: &Request, state: &AppState) -> bool {
    let production = rush_api::api_key_auth::production_mode();
    let configured_base_url = std::env::var("RUSH_BASE_URL").ok();
    let canonical_base_url = configured_base_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let peer_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|connect| connect.0.ip());
    let trust_forwarded = trust_forwarded_origin_headers(
        production,
        rush_api::api_key_auth::env_flag("RUSH_TRUST_PROXY_HEADERS"),
        peer_ip,
        &state.trusted_proxy_cidrs,
    );

    request_origin_allowed_with_policy(
        req.headers(),
        production,
        canonical_base_url,
        Some(state.cors_policy.as_ref()),
        trust_forwarded,
    )
}

fn request_origin_allowed_with_policy(
    headers: &HeaderMap,
    production: bool,
    canonical_base_url: Option<&str>,
    configured_allowlist: Option<&CorsPolicy>,
    trust_forwarded: bool,
) -> bool {
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    if origin == "null" || origin.is_empty() {
        return false;
    }
    let Some(origin_url) = parse_web_origin(origin) else {
        return false;
    };

    if let Some(raw_base_url) = canonical_base_url {
        let Some(base_url) = parse_web_origin(raw_base_url) else {
            return false;
        };
        if same_web_origin(&origin_url, &base_url) {
            return true;
        }
        if production {
            return false;
        }
    } else if production {
        // Startup validation already rejects this configuration; retain a
        // fail-closed request-time guard in case this helper is reused.
        return false;
    }

    if let Some(policy) = configured_allowlist {
        if !policy.is_empty() {
            return policy.allows(&origin_url);
        }
    }

    let host_header = if trust_forwarded {
        headers
            .get("x-forwarded-host")
            .or_else(|| headers.get(header::HOST))
    } else {
        headers.get(header::HOST)
    };
    let host = host_header
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.contains(','));
    let scheme = if trust_forwarded {
        headers
            .get("x-forwarded-proto")
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.contains(',') && matches!(*value, "http" | "https"))
            .unwrap_or("http")
    } else {
        "http"
    };
    let Some(host) = host else { return false };
    let Some(request_url) = parse_web_origin(&format!("{scheme}://{host}")) else {
        return false;
    };

    same_web_origin(&origin_url, &request_url)
}

fn requires_csrf_origin(method: &Method, path: &str, credential: Option<&CredentialKind>) -> bool {
    if matches!(
        path,
        "/api/v1/kubernetes/gateway/authorize" | "/api/v1/kubernetes/gateway/ready"
    ) {
        // Token introspection is a read even though the gateway uses POST.
        return false;
    }
    if !is_state_changing_method(method) {
        return false;
    }
    if credential.is_some_and(|kind| *kind == CredentialKind::Session) {
        return true;
    }

    // These mutations establish or use ambient browser credentials before a
    // normal Rush session exists. They therefore need the same Origin policy
    // even though tenant resolution classifies them as anonymous.
    method == Method::POST
        && matches!(
            path,
            "/api/v1/auth/login"
                | "/api/v1/sso/setup-token/exchange"
                | "/api/v1/sso/setup-session/complete"
                | "/api/v1/sso/providers"
        )
}

/// SameSite=Lax protects normal cross-site form traffic, while this Origin
/// check covers same-site subdomain attacks and login/setup-session mutations
/// that establish or consume ambient browser credentials.
async fn csrf_protection_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let resolution = req.extensions().get::<TenantResolution>().cloned();
    let path = req.uri().path().to_string();
    if requires_csrf_origin(
        req.method(),
        &path,
        resolution.as_ref().map(|value| &value.credential),
    ) && !request_origin_allowed(&req, &state)
    {
        tracing::warn!(
            method = %req.method(),
            path = %path,
            "browser mutation rejected by origin policy"
        );
        state
            .audit
            .log(
                rush_api::audit::AuditEvent::new("security.csrf_rejection", "anonymous")
                    .actor_name("browser request")
                    .tenant(
                        resolution
                            .as_ref()
                            .map(|value| value.tenant_id.as_str())
                            .unwrap_or("default"),
                    )
                    .resource("http_route", path.clone())
                    .outcome("failure")
                    .changes(
                        serde_json::json!({
                            "method": req.method().as_str(),
                            "reason": "origin_not_allowed",
                        })
                        .to_string(),
                    )
                    .description("browser mutation rejected by CSRF origin policy")
                    .context(rush_api::audit::actor_context_from_headers(req.headers())),
            )
            .await;
        return (
            axum::http::StatusCode::FORBIDDEN,
            "request origin is not allowed",
        )
            .into_response();
    }
    next.run(req).await
}

/// Middleware that records API RED self-metrics (`rush_http_*`) for every request.
///
/// Labels are bounded: `route` is the templated `MatchedPath` (a finite set of route
/// patterns, NOT the raw URI), `method` is the HTTP method, and `status_class` is the
/// 2xx/3xx/4xx/5xx family. The raw path and tenant_id are deliberately NOT used as labels.
///
/// `/metrics`, health probes, and `/shutdown` are skipped so control-plane
/// traffic doesn't inflate the request counters.
/// Cost on the hot path: one `MatchedPath` clone, atomic counter increments, one histogram
/// observe (bounded linear scan), and a gauge inc/dec — no locks held across `.await`.
async fn http_metrics_middleware(
    State(state): State<AppState>,
    matched: Option<axum::extract::MatchedPath>,
    req: Request,
    next: Next,
) -> Response {
    // Resolve the templated route up-front (finite cardinality). Fall back to "unmatched".
    let route: String = matched
        .as_ref()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    // Skip self-instrumentation for the scrape + health endpoints.
    if matches!(
        route.as_str(),
        "/metrics" | "/healthz" | "/readyz" | "/shutdown"
    ) {
        return next.run(req).await;
    }

    let method = req.method().as_str().to_string();
    let sm = state.self_metrics.clone();

    // In-flight gauge: inc on entry, dec on exit (even on panic-free early returns).
    sm.add_gauge("rush_http_requests_in_flight", &[], 1.0);
    let start = std::time::Instant::now();

    let resp = next.run(req).await;

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    sm.add_gauge("rush_http_requests_in_flight", &[], -1.0);

    let status = resp.status().as_u16();
    let status_class = match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    };
    sm.inc_counter(
        "rush_http_requests_total",
        &[
            ("route", route.as_str()),
            ("method", method.as_str()),
            ("status_class", status_class),
        ],
        1,
    );
    sm.observe_histogram(
        "rush_http_request_duration_ms",
        &[("route", route.as_str()), ("method", method.as_str())],
        elapsed_ms,
    );

    resp
}

/// Open `GET /metrics` handler: renders the self-metrics registry as Prometheus text
/// exposition (version 0.0.4). No auth — same posture as `/healthz`.
async fn metrics_handler(State(state): State<AppState>) -> Response {
    let audit = state.audit.health();
    state.self_metrics.set_gauge(
        "rush_audit_degraded",
        &[],
        if audit.ready { 0.0 } else { 1.0 },
    );
    state
        .self_metrics
        .set_gauge("rush_audit_outbox_events", &[], audit.pending_events as f64);
    state
        .self_metrics
        .set_gauge("rush_audit_outbox_bytes", &[], audit.pending_bytes as f64);
    state
        .self_metrics
        .set_gauge("rush_audit_outbox_max_bytes", &[], audit.max_bytes as f64);
    state.self_metrics.set_gauge(
        "rush_audit_write_failures_total",
        &[],
        audit.write_failures as f64,
    );
    let body = state.self_metrics.render_prometheus();
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

/// Middleware that resolves the tenant for every request. Methods checked in
/// priority order:
///
/// 1. `Authorization: Bearer <api_key>` — resolves the key to a tenant via
///    the config DB. Secure; the key is the trust boundary.
/// 2. `rush_session` cookie — resolves a session to its user, then uses
///    the user's tenant_id.
/// 3. `X-Rush-Tenant: <tenant_name_or_id>` header OR a `/t/{tenant}/…` URL
///    prefix — selects the tenant by name/id. Query and ingest routes then
///    enforce their independent tenant authentication policies. The
///    URL form lets external tools (e.g. Grafana datasources) carry the tenant
///    in the base URL; the header takes precedence when both are present.
/// 4. Fall back to the `"default"` tenant (backward compatible).
async fn tenant_middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    rush_api::request_auth::scope(tenant_middleware_scoped(state, req, next)).await
}

async fn tenant_middleware_scoped(state: AppState, mut req: Request, next: Next) -> Response {
    // ── URL-based tenant: /t/{tenant}/<rest> ──
    // Strip the prefix so downstream routes match unchanged, and carry the
    // extracted tenant through the same path as the X-Rush-Tenant header.
    let mut url_tenant: Option<String> = None;
    if let Some(rest) = req.uri().path().strip_prefix("/t/") {
        if let Some(slash) = rest.find('/') {
            let tenant = rest[..slash].to_string();
            if !tenant.is_empty() {
                let new_path = &rest[slash..]; // begins with '/'
                let query = req
                    .uri()
                    .query()
                    .map(|q| format!("?{q}"))
                    .unwrap_or_default();
                if let Ok(uri) = format!("{new_path}{query}").parse() {
                    *req.uri_mut() = uri;
                    url_tenant = Some(tenant);
                }
            }
        }
    }
    // Agent-specific path forms keep their route intact but still participate
    // in the same tenant resolution and key/tenant equality checks.
    if url_tenant.is_none() {
        url_tenant = explicit_ingest_tenant(req.uri().path());
    }

    // Extract all header values we need before any await point so the
    // &Request (whose Body is not Send) is not held across awaits.
    let auth_header: Option<String> = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());
    let agent_key: Option<String> = req
        .headers()
        .get("dd-api-key")
        .or_else(|| req.headers().get("x-amz-firehose-access-key"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());
    // Header wins over the URL prefix when both are present.
    let rush_tenant: Option<String> = req
        .headers()
        .get("x-rush-tenant")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
        .or(url_tenant);
    let session_token: Option<String> = handlers::auth::extract_session_cookie(req.headers());
    let session_audit_context = rush_api::audit::actor_context_from_headers(req.headers());
    let request_path = req.uri().path().to_string();

    let resolution = resolve_tenant_from_headers(
        &state,
        auth_header,
        agent_key,
        rush_tenant,
        session_token.clone(),
    )
    .await;
    let session_authenticated = resolution.credential == CredentialKind::Session;
    let resolved_tenant = resolution.tenant_id.clone();
    let request_identity = match (&resolution.credential, resolution.api_key.as_ref()) {
        (CredentialKind::Session, _) => RequestIdentity {
            tenant_id: resolution.tenant_id.clone(),
            authenticated: true,
            actor_id: String::new(),
            actor_name: String::new(),
            actor_type: "user".to_string(),
            credential_type: "session".to_string(),
        },
        (CredentialKind::QueryKey, Some(grant)) => RequestIdentity {
            tenant_id: resolution.tenant_id.clone(),
            authenticated: true,
            actor_id: grant.id.clone(),
            actor_name: "API key".to_string(),
            actor_type: "api_key".to_string(),
            credential_type: "query_key".to_string(),
        },
        (CredentialKind::IngestKey, Some(grant)) => RequestIdentity {
            tenant_id: resolution.tenant_id.clone(),
            authenticated: true,
            actor_id: grant.id.clone(),
            actor_name: "API key".to_string(),
            actor_type: "api_key".to_string(),
            credential_type: "ingest_key".to_string(),
        },
        _ => RequestIdentity {
            tenant_id: resolution.tenant_id.clone(),
            authenticated: false,
            actor_id: String::new(),
            actor_name: String::new(),
            actor_type: "anonymous".to_string(),
            credential_type: "anonymous".to_string(),
        },
    };
    req.extensions_mut().insert(TenantContext {
        tenant_id: resolution.tenant_id.clone(),
    });
    req.extensions_mut().insert(request_identity);
    req.extensions_mut().insert(resolution);
    let mut response = next.run(req).await;

    // Rotate only after a downstream handler successfully validated and used
    // the old session. Login/logout and SSO callback responses manage their own
    // cookies and must never receive a second competing session cookie.
    let manages_own_session_cookie = matches!(
        request_path.as_str(),
        "/api/v1/auth/login" | "/api/v1/auth/logout" | "/auth/sso/callback" | "/auth/sso/acs"
    );
    if session_authenticated
        && response.status().as_u16() < 400
        && !manages_own_session_cookie
        && let Some(token) = session_token
        && should_check_session_rotation(&state, &token)
    {
        let checked_key = state.config_db.session_request_key(&token);
        match state.config_db.rotate_session_if_due(&token).await {
            Ok(Some(rotated)) => {
                state.session_rotation_checks.insert(
                    state.config_db.session_request_key(&rotated.issued.token),
                    Instant::now(),
                );
                let cookie = handlers::auth::session_cookie(
                    &rotated.issued.token,
                    rotated.issued.max_age_seconds,
                );
                if let Ok(value) = HeaderValue::from_str(&cookie) {
                    response.headers_mut().append(header::SET_COOKIE, value);
                    state
                        .audit
                        .log(
                            rush_api::audit::AuditEvent::new("session.rotate", "user")
                                .actor(rotated.user_id, rotated.username)
                                .tenant(rotated.tenant_id)
                                .resource("session", rotated.session_id)
                                .outcome("success")
                                .changes(
                                    serde_json::json!({
                                        "idle_timeout_seconds": rotated.issued.max_age_seconds,
                                    })
                                    .to_string(),
                                )
                                .description("active session bearer rotated")
                                .context(session_audit_context.clone()),
                        )
                        .await;
                }
            }
            Ok(None) => {}
            Err(error) => {
                state.session_rotation_checks.remove(&checked_key);
                tracing::error!(%error, "session renewal failed");
                state
                    .audit
                    .log(
                        rush_api::audit::AuditEvent::new("session.rotate", "anonymous")
                            .actor_name("existing session")
                            .tenant(resolved_tenant)
                            .resource("session", "unknown")
                            .outcome("failure")
                            .changes(
                                serde_json::json!({ "reason": "session_store_unavailable" })
                                    .to_string(),
                            )
                            .description("active session bearer rotation failed")
                            .context(session_audit_context),
                    )
                    .await;
            }
        }
    }
    response
}

fn should_check_session_rotation(state: &AppState, token: &str) -> bool {
    let interval =
        Duration::from_secs(state.config_db.session_activity_interval_seconds().max(1) as u64);
    let key = state.config_db.session_request_key(token);
    if state.session_rotation_checks.len() > 10_000 {
        state
            .session_rotation_checks
            .retain(|_, checked_at| checked_at.elapsed() < interval.saturating_mul(2));
    }
    match state.session_rotation_checks.entry(key) {
        dashmap::mapref::entry::Entry::Occupied(mut entry) => {
            if entry.get().elapsed() < interval {
                false
            } else {
                entry.insert(Instant::now());
                true
            }
        }
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(Instant::now());
            true
        }
    }
}

fn explicit_ingest_tenant(path: &str) -> Option<String> {
    for prefix in ["/api/v2/logs/t/", "/cloudwatch/firehose/t/"] {
        if let Some(tenant) = path.strip_prefix(prefix) {
            if !tenant.is_empty() && !tenant.contains('/') {
                return Some(tenant.to_string());
            }
        }
    }
    None
}

/// Routes that must remain reachable before a tenant-authenticated session
/// exists. All other routes are subject to the resolved tenant's
/// `auth_required` policy.
fn allows_unauthenticated_tenant_request(method: &axum::http::Method, path: &str) -> bool {
    if method == axum::http::Method::OPTIONS {
        // CORS preflight never carries credentials. The actual request is still
        // checked when the browser sends it.
        return true;
    }

    let setup_validation_token = path
        .strip_prefix("/api/v1/sso/setup-token/")
        .and_then(|rest| rest.strip_suffix("/validate"));
    let scoped_setup_session = (method == axum::http::Method::GET
        && path == "/api/v1/sso/setup-session")
        || (method == axum::http::Method::POST
            && matches!(
                path,
                "/api/v1/sso/setup-token/exchange"
                    | "/api/v1/sso/setup-session/complete"
                    | "/api/v1/sso/providers"
            ));

    let unauthenticated_kubernetes_endpoint = (method == axum::http::Method::POST
        && matches!(
            path,
            "/api/v1/kubernetes/access-events/ingest"
                | "/api/v1/kubernetes/session-chunks/ingest"
                | "/api/v1/kubernetes/gateway/ready"
                | "/api/v1/kubernetes/gateway/authorize"
                | "/api/v1/kubernetes/gateway/rbac/reconcile"
                | "/api/v1/kubernetes/login/start"
                | "/api/v1/kubernetes/login/token"
                | "/api/v1/kubernetes/access-events/client"
        ))
        || (method == axum::http::Method::GET && path == "/api/v1/kubernetes/gateway/rbac");

    unauthenticated_kubernetes_endpoint
        || matches!(
            path,
            "/healthz"
                | "/readyz"
                | "/metrics"
                | "/api/v1/security/csp-report"
                | "/shutdown"
                | "/api/v1/auth/login"
                | "/api/v1/auth/logout"
                | "/api/v1/sso/status"
                | "/auth/sso/login"
                | "/auth/sso/callback"
                | "/auth/sso/acs"
                | "/auth/sso/metadata"
        )
        || scoped_setup_session
        || setup_validation_token.is_some_and(|token| !token.is_empty() && !token.contains('/'))
}

/// Natural-language parsing can create metered provider cost, so a tenant's
/// open-query policy must never authorize it. Only an interactive browser
/// session is accepted; query API keys and anonymous tenant selection are not.
fn should_reject_interactive_llm(
    method: &axum::http::Method,
    path: &str,
    credential: &CredentialKind,
) -> bool {
    *method == axum::http::Method::POST
        && matches!(path, "/api/v1/parse-query" | "/api/v1/parse-promql")
        && *credential != CredentialKind::Session
}

/// Once shutdown starts, readiness is already false and this gate prevents new
/// application/ingest work from entering while existing requests drain.
async fn shutdown_gate_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    if state.shutdown.is_requested()
        && !matches!(
            req.uri().path(),
            "/shutdown" | "/healthz" | "/readyz" | "/metrics"
        )
    {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "status": "draining",
                "error": "query-api is shutting down",
            })),
        )
            .into_response();
    }
    next.run(req).await
}

/// Query endpoints participate in workload admission. Ingestion, health,
/// shutdown, authentication/settings mutations, and the long-lived SRE SSE
/// stream keep their purpose-built policies instead of inheriting a generic
/// query timeout.
fn query_workload_for_request(req: &Request) -> Option<WorkloadClass> {
    let path = req.uri().path();
    if ingest_signal_for_route(req.method(), path).is_some()
        || matches!(path, "/healthz" | "/readyz" | "/metrics" | "/shutdown")
        || matches!(
            path,
            "/api/v1/kubernetes/access-events/ingest"
                | "/api/v1/kubernetes/session-chunks/ingest"
                | "/api/v1/kubernetes/gateway/ready"
        )
        || path == "/api/v1/investigate"
    {
        return None;
    }

    if path.starts_with("/api/v1/exports/") {
        return path.ends_with("/download").then_some(WorkloadClass::Export);
    }
    if path.contains("/export") {
        return Some(WorkloadClass::Export);
    }
    if path.starts_with("/jaeger/")
        || path.starts_with("/api/v1/integrations/")
        || path.starts_with("/api/v1/argocd")
        || path.starts_with("/api/v1/flux")
        || path.starts_with("/api/v1/kubernetes")
    {
        return Some(WorkloadClass::Integration);
    }

    let managed = path.starts_with("/api/v1/query")
        || path.starts_with("/api/v1/explore/")
        || path.starts_with("/api/v1/logs")
        || path.starts_with("/api/v1/traces")
        || path.starts_with("/api/v1/services")
        || path.starts_with("/api/v1/bubbleup")
        || path.starts_with("/api/v1/suggest")
        || path.starts_with("/api/v1/stats")
        || path.starts_with("/api/v1/rum/")
        || path.starts_with("/prom/api/v1/query")
        || path.starts_with("/prom/api/v1/series")
        || path.starts_with("/prom/api/v1/labels")
        || path.starts_with("/prom/api/v1/label/")
        || path.starts_with("/api/v1/monitors/") && path.ends_with("/preview")
        || path.starts_with("/api/v1/detection-rules/") && path.ends_with("/test");
    if !managed {
        return None;
    }

    if req
        .headers()
        .get("x-rush-workload")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("dashboard"))
    {
        Some(WorkloadClass::Dashboard)
    } else {
        Some(WorkloadClass::Interactive)
    }
}

fn time_range_error_response(error: TimeRangeError) -> Response {
    let (code, message) = match error {
        TimeRangeError::Malformed => (
            "invalid_time_range",
            "time range must contain valid RFC3339 timestamps or Unix seconds",
        ),
        TimeRangeError::Reversed => (
            "reversed_time_range",
            "time range start must be before or equal to its end",
        ),
        TimeRangeError::TooLarge { .. } => (
            "time_range_too_large",
            "time range exceeds the configured workload limit",
        ),
    };
    rush_api::api_error::ApiError::public(axum::http::StatusCode::BAD_REQUEST, code, message)
        .into_response()
}

fn validate_range_pair(from: &str, to: &str, max_seconds: u64) -> Result<(), Response> {
    ValidatedTimeRange::parse(from, to, max_seconds)
        .map(|_| ())
        .map_err(time_range_error_response)
}

#[derive(serde::Deserialize)]
struct RequestTimeRangeEnvelope<'a> {
    #[serde(borrow)]
    time_range: Option<RequestTimeRange<'a>>,
}

#[derive(serde::Deserialize)]
struct RequestTimeRange<'a> {
    #[serde(borrow)]
    from: Option<&'a str>,
    #[serde(borrow)]
    to: Option<&'a str>,
}

fn json_time_range(
    value: RequestTimeRangeEnvelope<'_>,
) -> Result<Option<(&str, &str)>, TimeRangeError> {
    let Some(range) = value.time_range else {
        return Ok(None);
    };
    Ok(Some((
        range.from.ok_or(TimeRangeError::Malformed)?,
        range.to.ok_or(TimeRangeError::Malformed)?,
    )))
}

async fn validate_request_time_range(req: &mut Request, max_seconds: u64) -> Result<(), Response> {
    if let Some(query) = req.uri().query() {
        let values = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect::<std::collections::HashMap<_, _>>();
        let from_to_present = values.contains_key("from") || values.contains_key("to");
        let start_end_present = values.contains_key("start") || values.contains_key("end");
        let pair = if from_to_present {
            Some(
                values
                    .get("from")
                    .zip(values.get("to"))
                    .ok_or_else(|| time_range_error_response(TimeRangeError::Malformed))?,
            )
        } else if start_end_present {
            Some(
                values
                    .get("start")
                    .zip(values.get("end"))
                    .ok_or_else(|| time_range_error_response(TimeRangeError::Malformed))?,
            )
        } else {
            None
        };
        if let Some((from, to)) = pair {
            validate_range_pair(from, to, max_seconds)?;
        }
    }

    if matches!(*req.method(), Method::POST | Method::PUT) {
        const MAX_QUERY_BODY_BYTES: usize = 512 * 1024;
        let body = std::mem::replace(req.body_mut(), axum::body::Body::empty());
        let bytes = match axum::body::to_bytes(body, MAX_QUERY_BODY_BYTES).await {
            Ok(bytes) => bytes,
            Err(_) => {
                return Err(rush_api::api_error::ApiError::public(
                    axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                    "query_body_too_large",
                    "query request body exceeds the configured limit",
                )
                .into_response());
            }
        };
        let validation = if bytes.is_empty() {
            None
        } else {
            match serde_json::from_slice::<RequestTimeRangeEnvelope<'_>>(&bytes) {
                Ok(value) => json_time_range(value).map_err(time_range_error_response)?,
                // Invalid JSON is still left to the downstream extractor, as
                // before. A valid document with the wrong time_range shape is
                // a stable workload-policy error here.
                Err(error) if error.is_data() => {
                    return Err(time_range_error_response(TimeRangeError::Malformed));
                }
                Err(_) => None,
            }
        };
        if let Some((from, to)) = validation {
            validate_range_pair(from, to, max_seconds)?;
        }
        *req.body_mut() = axum::body::Body::from(bytes);
    }
    Ok(())
}

fn admission_error_response(error: AdmissionError) -> Response {
    let retry_after = error.retry_after_secs();
    let (status, code, message) = match error {
        AdmissionError::TenantBusy { .. } => (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "tenant_query_capacity_exhausted",
            "tenant query capacity is busy; retry shortly",
        ),
        AdmissionError::GlobalBusy { .. } => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "query_capacity_exhausted",
            "query capacity is busy; retry shortly",
        ),
    };
    let mut response = rush_api::api_error::ApiError::public(status, code, message).into_response();
    if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

async fn query_policy_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(class) = query_workload_for_request(&req) else {
        return next.run(req).await;
    };
    let tenant = req
        .extensions()
        .get::<TenantContext>()
        .map(|context| context.tenant_id.clone())
        .unwrap_or_else(|| "default".to_string());
    let guard = match state.query_governor.admit(class, &tenant).await {
        Ok(guard) => guard,
        Err(error) => return admission_error_response(error),
    };
    let budget = guard.budget().clone();
    if let Err(response) = validate_request_time_range(&mut req, budget.max_time_range_secs).await {
        return response;
    }
    let label = class.label();
    let result = rush_api::query_governor::with_budget(
        budget.clone(),
        tokio::time::timeout(
            Duration::from_secs(budget.request_timeout_secs),
            next.run(req),
        ),
    )
    .await;
    match result {
        Ok(response) => {
            state.self_metrics.inc_counter(
                "rush_query_requests_total",
                &[("workload", label), ("outcome", "completed")],
                1,
            );
            if class == WorkloadClass::Export {
                rush_api::query_governor::retain_admission_until_body_end(
                    response,
                    guard,
                    Duration::from_secs(budget.request_timeout_secs),
                )
            } else {
                drop(guard);
                response
            }
        }
        Err(_) => {
            drop(guard);
            state.self_metrics.inc_counter(
                "rush_query_requests_total",
                &[("workload", label), ("outcome", "timeout")],
                1,
            );
            rush_api::api_error::ApiError::public(
                axum::http::StatusCode::GATEWAY_TIMEOUT,
                "query_timeout",
                "query exceeded its configured workload time budget",
            )
            .into_response()
        }
    }
}

fn should_reject_for_tenant_auth(
    method: &axum::http::Method,
    path: &str,
    auth_required: bool,
    authenticated: bool,
) -> bool {
    auth_required && !authenticated && !allows_unauthenticated_tenant_request(method, path)
}

fn ingest_signal_for_route(method: &axum::http::Method, path: &str) -> Option<&'static str> {
    if *method == axum::http::Method::OPTIONS {
        return None;
    }
    if is_explain_collector_route(method, path) {
        return Some("collector");
    }
    if matches!(
        path,
        "/v1/logs" | "/api/v1/ingest/logs" | "/datadog/v1/input" | "/api/v2/logs"
    ) || path.starts_with("/api/v2/logs/t/")
        || path.starts_with("/cloudwatch/firehose/t/")
    {
        return Some("logs");
    }
    if matches!(
        path,
        "/v1/traces" | "/datadog/api/v0.2/traces" | "/datadog/v0.3/traces" | "/datadog/v0.4/traces"
    ) {
        return Some("traces");
    }
    if matches!(
        path,
        "/v1/metrics"
            | "/prom/api/v1/write"
            | "/datadog/api/v1/series"
            | "/datadog/api/v2/series"
            | "/datadog/api/v1/check_run"
    ) {
        return Some("metrics");
    }
    if matches!(path, "/api/v1/rum/ingest" | "/api/v1/rum/replay/ingest") {
        return Some("rum");
    }
    if matches!(
        path,
        "/datadog/api/v0.6/stats"
            | "/datadog/api/v0.2/stats"
            | "/datadog/api/v1/validate"
            | "/datadog/api/v1/metadata"
            | "/datadog/api/v2/host_metadata"
            | "/datadog/api/v2/events"
            | "/datadog/api/v1/collector"
            | "/datadog/intake/"
            | "/datadog/intake"
    ) {
        // Agent handshake/metadata routes do not ingest a telemetry signal,
        // but are part of the authenticated ingest surface.
        return Some("control");
    }
    None
}

fn is_explain_collector_route(method: &axum::http::Method, path: &str) -> bool {
    if *method == axum::http::Method::GET {
        return matches!(
            path,
            "/api/v1/integrations/postgres/explain/poll"
                | "/api/v1/integrations/mysql/explain/poll"
        );
    }
    if *method != axum::http::Method::POST {
        return false;
    }

    [
        "/api/v1/integrations/postgres/explain/",
        "/api/v1/integrations/mysql/explain/",
    ]
    .iter()
    .any(|prefix| {
        path.strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix("/result"))
            .is_some_and(|id| !id.is_empty() && !id.contains('/'))
    })
}

fn effective_route_ingest_auth_required(
    signal: Option<&str>,
    stored_ingest_auth_required: bool,
    default_compatibility: bool,
) -> bool {
    signal == Some("collector") || (stored_ingest_auth_required && !default_compatibility)
}

fn ingest_source_for_route(path: &str) -> &'static str {
    if path == "/prom/api/v1/write" {
        "prometheus"
    } else if path.starts_with("/datadog/") || path.starts_with("/api/v2/logs") {
        "datadog"
    } else if path.starts_with("/cloudwatch/firehose/") {
        "cloudwatch"
    } else if path.starts_with("/api/v1/rum/") {
        "rum"
    } else if matches!(
        path,
        "/v1/logs" | "/v1/traces" | "/v1/metrics" | "/api/v1/ingest/logs"
    ) {
        "otlp"
    } else {
        "other"
    }
}

/// Reject a declared oversized ingest body before axum buffers it for a `Bytes`
/// extractor. `DefaultBodyLimit` below independently enforces the same ceiling
/// for chunked bodies whose final size is not known from Content-Length.
async fn ingest_content_length_limit_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path();
    if ingest_signal_for_route(req.method(), path).is_some()
        && let Some(length) = req
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
        && length > state.ingest_limits.max_compressed_bytes
    {
        state
            .ingest_limits
            .record_rejection(ingest_source_for_route(path), "compressed_bytes");
        return (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            "compressed ingest payload exceeds configured limit",
        )
            .into_response();
    }
    next.run(req).await
}

fn consume_ingest_rate_limit(state: &AppState, key_id: &str, limit: u64) -> bool {
    let now = std::time::Instant::now();
    let mut entry = state
        .ingest_key_limiter
        .entry(key_id.to_string())
        .or_insert((0, now));
    if entry.1.elapsed() >= std::time::Duration::from_secs(60) {
        *entry = (1, now);
        return true;
    }
    if entry.0 >= limit {
        return false;
    }
    entry.0 += 1;
    true
}

fn credential_route_denial(
    credential: &CredentialKind,
    ingest_route: bool,
    ingest_auth_required: bool,
) -> Option<&'static str> {
    if ingest_route {
        match credential {
            CredentialKind::IngestKey => None,
            CredentialKind::Anonymous if !ingest_auth_required => None,
            _ => Some("ingest_key_required"),
        }
    } else if *credential == CredentialKind::IngestKey {
        Some("query_not_allowed")
    } else {
        None
    }
}

async fn audit_api_key_denial(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    resolution: &TenantResolution,
    reason: &str,
    signal: Option<&str>,
) {
    let (action, actor_type) = match resolution.credential {
        CredentialKind::QueryKey | CredentialKind::IngestKey => ("apikey.scope_denied", "api_key"),
        CredentialKind::Session => ("ingest.auth_denied", "user"),
        CredentialKind::Anonymous => ("ingest.auth_denied", "anonymous"),
    };
    let mut event = rush_api::audit::AuditEvent::new(action, actor_type)
        .tenant(resolution.tenant_id.clone())
        .outcome("failure")
        .metadata(
            serde_json::json!({
                "reason": reason,
                "signal": signal,
                "credential_type": format!("{:?}", resolution.credential),
            })
            .to_string(),
        )
        .context(rush_api::audit::actor_context_from_headers(headers));
    if let Some(grant) = &resolution.api_key {
        event = event
            .actor(grant.id.clone(), grant.id.clone())
            .resource("api_key", grant.id.clone());
    } else {
        event = event.resource("tenant", resolution.tenant_id.clone());
    }
    state.audit.log(event).await;
}

async fn same_tenant(state: &AppState, key_tenant: &str, requested: &str) -> bool {
    if key_tenant == requested {
        return true;
    }
    match state.config_db.get_tenant_id_by_name(requested).await {
        Ok(Some(id)) if id == key_tenant => true,
        _ => matches!(
            state.config_db.get_tenant_id_by_name(key_tenant).await,
            Ok(Some(id)) if id == requested
        ),
    }
}

/// Enforce independent query and ingest authentication after tenant resolution
/// but before the handler.
///
/// This is an inner-router middleware so CORS, compression, security headers,
/// request tracing, and metrics still wrap the 401 response. The outer
/// `tenant_middleware` must run first because it also rewrites `/t/{tenant}`
/// paths before routing.
async fn enforce_tenant_auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let Some(tenant_id) = req
        .extensions()
        .get::<TenantContext>()
        .map(|tenant| tenant.tenant_id.clone())
    else {
        tracing::error!("tenant auth middleware ran without TenantContext");
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "tenant resolution unavailable",
        )
            .into_response();
    };
    let resolution = req
        .extensions()
        .get::<TenantResolution>()
        .cloned()
        .unwrap_or_else(|| TenantResolution::anonymous(&tenant_id));
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Login/SSO/bootstrap and operational probes must remain reachable even if
    // the config store is temporarily unavailable.
    if allows_unauthenticated_tenant_request(&method, &path) {
        return next.run(req).await;
    }

    if should_reject_interactive_llm(&method, &path, &resolution.credential) {
        tracing::warn!(
            event = "llm.auth_denied",
            tenant_id = %tenant_id,
            path = %path,
            "LLM request requires an interactive session"
        );
        let mut event = rush_api::audit::AuditEvent::new(
            "llm.auth_denied",
            if resolution.api_key.is_some() {
                "api_key"
            } else {
                "anonymous"
            },
        )
        .tenant(tenant_id.clone())
        .resource("http_route", path.clone())
        .outcome("failure")
        .changes(
            serde_json::json!({
                "reason": "interactive_session_required",
            })
            .to_string(),
        )
        .description("LLM request rejected without an interactive session")
        .context(rush_api::audit::actor_context_from_headers(req.headers()));
        if let Some(grant) = resolution.api_key.as_ref() {
            event = event.actor(grant.id.clone(), "API key");
        }
        state.audit.log(event).await;
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            "interactive authentication required for LLM features",
        )
            .into_response();
    }

    let ingest_signal = ingest_signal_for_route(&method, &path);
    let policy_started = std::time::Instant::now();
    let policy_result = state
        .config_db
        .tenant_auth_required_checked(&tenant_id)
        .await;
    state.self_metrics.record_auth_lookup(
        "tenant_policy",
        policy_started.elapsed().as_secs_f64() * 1_000.0,
        u64::from(matches!(policy_result, Ok(Some(_)))),
        match &policy_result {
            Ok(Some(_)) => "ok",
            Ok(None) => "not_found",
            Err(_) => "error",
        },
    );
    let stored_auth_required = match policy_result {
        Ok(Some(required)) => required,
        Ok(None) => {
            tracing::error!(tenant_id = %tenant_id, "resolved tenant has no policy record");
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "tenant policy unavailable",
            )
                .into_response();
        }
        Err(error) => {
            tracing::error!(tenant_id = %tenant_id, %error, "tenant auth policy lookup failed");
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "tenant policy unavailable",
            )
                .into_response();
        }
    };
    let default_compatibility =
        tenant_id == "default" && rush_api::api_key_auth::allow_anonymous_default();
    let auth_required = stored_auth_required && !default_compatibility;
    // Collector control routes can expose queued SQL and accept execution
    // results, so they are never covered by open telemetry-ingest policy.
    let ingest_auth_required = if ingest_signal.is_some() {
        let ingest_policy_started = std::time::Instant::now();
        let ingest_policy_result = state
            .config_db
            .tenant_ingest_auth_required_checked(&tenant_id)
            .await;
        state.self_metrics.record_auth_lookup(
            "tenant_ingest_policy",
            ingest_policy_started.elapsed().as_secs_f64() * 1_000.0,
            u64::from(matches!(ingest_policy_result, Ok(Some(_)))),
            match &ingest_policy_result {
                Ok(Some(_)) => "ok",
                Ok(None) => "not_found",
                Err(_) => "error",
            },
        );
        match ingest_policy_result {
            Ok(Some(required)) => {
                effective_route_ingest_auth_required(ingest_signal, required, default_compatibility)
            }
            Ok(None) => {
                tracing::error!(tenant_id = %tenant_id, "resolved tenant has no ingest policy record");
                return (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "tenant ingest policy unavailable",
                )
                    .into_response();
            }
            Err(error) => {
                tracing::error!(tenant_id = %tenant_id, %error, "tenant ingest auth policy lookup failed");
                return (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "tenant ingest policy unavailable",
                )
                    .into_response();
            }
        }
    } else {
        true
    };
    let anonymous_ingest = ingest_signal.is_some()
        && resolution.credential == CredentialKind::Anonymous
        && !ingest_auth_required;
    if let Some(reason) = credential_route_denial(
        &resolution.credential,
        ingest_signal.is_some(),
        ingest_auth_required,
    ) {
        audit_api_key_denial(&state, req.headers(), &resolution, reason, ingest_signal).await;
        let message = if reason == "query_not_allowed" {
            "ingest-only API keys cannot access query or configuration routes"
        } else {
            "ingest-only API key required"
        };
        return (axum::http::StatusCode::FORBIDDEN, message).into_response();
    }

    if let Some(signal) = ingest_signal {
        if !anonymous_ingest {
            let Some(grant) = resolution.api_key.as_ref() else {
                return (
                    axum::http::StatusCode::FORBIDDEN,
                    "ingest-only API key required",
                )
                    .into_response();
            };
            if signal != "control" && !grant.signals.iter().any(|allowed| allowed == signal) {
                audit_api_key_denial(
                    &state,
                    req.headers(),
                    &resolution,
                    "signal_not_allowed",
                    Some(signal),
                )
                .await;
                return (
                    axum::http::StatusCode::FORBIDDEN,
                    "signal not allowed for API key",
                )
                    .into_response();
            }
            if let Some(requested_tenant) = explicit_ingest_tenant(&path) {
                if !same_tenant(&state, &grant.tenant_id, &requested_tenant).await {
                    audit_api_key_denial(
                        &state,
                        req.headers(),
                        &resolution,
                        "tenant_not_allowed",
                        Some(signal),
                    )
                    .await;
                    return (
                        axum::http::StatusCode::FORBIDDEN,
                        "tenant not allowed for API key",
                    )
                        .into_response();
                }
            }
            if !grant.source_cidrs.is_empty() {
                let source_ip = req
                    .extensions()
                    .get::<ConnectInfo<SocketAddr>>()
                    .map(|connect| connect.0.ip());
                if !source_ip.is_some_and(|ip| {
                    rush_api::api_key_auth::source_allowed(ip, &grant.source_cidrs)
                }) {
                    audit_api_key_denial(
                        &state,
                        req.headers(),
                        &resolution,
                        "source_not_allowed",
                        Some(signal),
                    )
                    .await;
                    return (
                        axum::http::StatusCode::FORBIDDEN,
                        "source not allowed for API key",
                    )
                        .into_response();
                }
            }
            if !consume_ingest_rate_limit(&state, &grant.id, grant.rate_limit_per_minute) {
                audit_api_key_denial(
                    &state,
                    req.headers(),
                    &resolution,
                    "rate_limit_exceeded",
                    Some(signal),
                )
                .await;
                return (
                    axum::http::StatusCode::TOO_MANY_REQUESTS,
                    "ingest API key rate limit exceeded",
                )
                    .into_response();
            }
        }
    }

    if ingest_signal.is_none()
        && should_reject_for_tenant_auth(&method, &path, auth_required, resolution.authenticated)
    {
        tracing::warn!(
            tenant_id = %tenant_id,
            path = %path,
            "unauthenticated request rejected for locked tenant"
        );
        state
            .audit
            .log(
                rush_api::audit::AuditEvent::new("tenant.auth_denied", "anonymous")
                    .tenant(tenant_id.clone())
                    .resource("tenant", tenant_id.clone())
                    .outcome("failure")
                    .metadata(serde_json::json!({ "path": path }).to_string())
                    .context(rush_api::audit::actor_context_from_headers(req.headers())),
            )
            .await;
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            "authentication required for tenant",
        )
            .into_response();
    }

    next.run(req).await
}

async fn resolve_tenant_from_headers(
    state: &AppState,
    auth_header: Option<String>,
    dd_key: Option<String>,
    rush_tenant: Option<String>,
    session_token: Option<String>,
) -> TenantResolution {
    let resolved =
        resolve_tenant_inner(state, auth_header, dd_key, rush_tenant, session_token).await;
    // FINAL LOCKDOWN SAFETY NET: under no circumstances may the public request
    // path resolve to the reserved `_audit` tenant. Even though every individual
    // resolution branch already excludes it (header branch rejects it; API keys
    // are bound to `default`; sessions resolve to a real user tenant), collapse
    // any `_audit` here to `default` so audit data can never be read or written
    // via the normal telemetry tenant scoping.
    if resolved
        .tenant_id
        .eq_ignore_ascii_case(rush_api::audit::AUDIT_TENANT)
    {
        return TenantResolution::anonymous("default");
    }
    resolved
}

async fn resolve_api_key_credential(state: &AppState, key: &str) -> Option<TenantResolution> {
    if key.is_empty() {
        return None;
    }
    let key_hash = handlers::settings::hash_api_key(key);
    let started = std::time::Instant::now();
    let result = state.config_db.resolve_api_key(&key_hash).await;
    state.self_metrics.record_auth_lookup(
        "api_key_grant",
        started.elapsed().as_secs_f64() * 1_000.0,
        u64::from(matches!(result, Ok(Some(_)))),
        match &result {
            Ok(Some(_)) => "ok",
            Ok(None) => "not_found",
            Err(_) => "error",
        },
    );
    match result {
        Ok(Some(grant)) => Some(TenantResolution::api_key(grant)),
        Ok(None) => None,
        Err(error) => {
            tracing::warn!(%error, "API key resolution failed");
            None
        }
    }
}

async fn resolve_tenant_inner(
    state: &AppState,
    auth_header: Option<String>,
    dd_key: Option<String>,
    rush_tenant: Option<String>,
    session_token: Option<String>,
) -> TenantResolution {
    // ── Priority 1: Bearer token → fixed to the key's tenant ──
    // API keys are scoped to one tenant (for collectors, CI, Grafana).
    if let Some(val) = auth_header {
        if val.len() > 7 && val[..7].eq_ignore_ascii_case("bearer ") {
            let key = val[7..].trim();
            if let Some(resolution) = resolve_api_key_credential(state, key).await {
                return resolution;
            }
            tracing::debug!(method = "api_key", "API key not found");
        }
    }

    // ── Priority 1b: DD-API-KEY header (Datadog agent) ──
    // The Datadog agent sends its API key in this header. Resolve it the
    // same way as a Bearer token so DD agents map to tenants via API keys.
    if let Some(dd_key_val) = dd_key {
        let key = dd_key_val.trim();
        if !key.is_empty() {
            if let Some(resolution) = resolve_api_key_credential(state, key).await {
                return resolution;
            }
            tracing::debug!(method = "agent_api_key", "API key not found");
        }
    }

    // ── Priority 2: X-Rush-Tenant header ──
    // The frontend tenant switcher sends this. It takes priority over the
    // session's default tenant so users can switch between tenants they
    // have access to.
    //
    // If the tenant has auth_required=true (locked), the X-Rush-Tenant header
    // alone is NOT enough — the request must also have been authenticated via
    // Bearer token, DD-API-KEY, or session cookie (priorities 1/1b above).
    // This prevents unauthenticated ingest into locked tenants.
    if let Some(tenant_header) = rush_tenant {
        let tenant = tenant_header.trim().to_string();
        // LOCKDOWN: `_audit` is the reserved tamper-evident-audit tenant. It must
        // NEVER be selectable via the public API (X-Rush-Tenant header or
        // /t/{tenant} URL prefix), for ingest OR query. Treat any attempt to
        // select it as "not allowed" and fall through to the normal resolution
        // chain (which ends at "default"). Case-insensitive. (The tenant is also
        // seeded disabled, so is_tenant_enabled would reject it anyway — this is
        // an explicit belt-and-suspenders guard with a clear audit trail.)
        if tenant.eq_ignore_ascii_case(rush_api::audit::AUDIT_TENANT) {
            tracing::warn!(
                method = "header",
                "attempt to select reserved '_audit' tenant via public API — rejected"
            );
        } else if !tenant.is_empty() {
            let tenant_started = std::time::Instant::now();
            let tenant_enabled = state.config_db.is_tenant_enabled(&tenant).await;
            state.self_metrics.record_auth_lookup(
                "tenant_policy",
                tenant_started.elapsed().as_secs_f64() * 1_000.0,
                u64::from(tenant_enabled),
                if tenant_enabled { "ok" } else { "not_found" },
            );
            if tenant_enabled {
                // If the request carries a session cookie, validate the user has
                // group-based access to the requested tenant.
                if let Some(token) = &session_token {
                    if let Some((user_id, _username, _display_name, _tid, role)) =
                        rush_api::request_auth::resolve_session_user(state, token).await
                    {
                        if role == "admin" {
                            // Admins can access any enabled tenant
                            return TenantResolution::session(tenant);
                        }
                        // Non-admins: resolve accessible tenant IDs and check
                        let permissions_started = std::time::Instant::now();
                        let permissions = state.config_db.resolve_user_permissions(&user_id).await;
                        state.self_metrics.record_auth_lookup(
                            "user_permissions",
                            permissions_started.elapsed().as_secs_f64() * 1_000.0,
                            permissions
                                .as_ref()
                                .map(|(scopes, permissions, tenants)| {
                                    (scopes.len() + permissions.len() + tenants.len()) as u64
                                })
                                .unwrap_or(0),
                            if permissions.is_ok() { "ok" } else { "error" },
                        );
                        if let Ok((_, _, accessible_ids)) = permissions {
                            // accessible_ids are UUIDs; resolve the requested
                            // tenant name to an ID for comparison
                            if let Ok(Some(tenant_id)) =
                                state.config_db.get_tenant_id_by_name(&tenant).await
                            {
                                if accessible_ids.contains(&tenant_id) {
                                    return TenantResolution::session(tenant);
                                }
                            }
                        }
                        tracing::debug!(
                            tenant = %tenant,
                            "X-Rush-Tenant rejected: user lacks group access"
                        );
                        // Fall through to session default tenant
                    }
                } else if !state.config_db.is_tenant_auth_required(&tenant).await
                    || !state
                        .config_db
                        .is_tenant_ingest_auth_required(&tenant)
                        .await
                {
                    // An anonymous header may select a tenant that has either
                    // open queries or open ingestion. The route-specific inner
                    // policy still rejects access to the other surface.
                    return TenantResolution::anonymous(tenant);
                } else {
                    tracing::debug!(
                        tenant_id = %tenant,
                        method = "header",
                        "tenant requires auth — X-Rush-Tenant header rejected without valid session/API key"
                    );
                }
            } else {
                tracing::debug!(tenant_id = %tenant, method = "header", "tenant disabled or missing, falling through");
            }
        }
    }

    // ── Priority 3: Session cookie → user's default tenant ──
    // Fallback when no explicit tenant header is sent (e.g., first page load
    // before the tenant switcher initializes).
    if let Some(token) = session_token {
        if let Some((_user_id, _username, _display_name, tenant_id, _role)) =
            rush_api::request_auth::resolve_session_user(state, &token).await
        {
            return TenantResolution::session(tenant_id);
        }
    }

    // ── Priority 4: default ──
    TenantResolution::anonymous("default")
}

use axum::extract::State;

/// Build the object-store ingest buffer from `RUSH_BUFFER_S3_*` env (reuses the
/// standard S3/MinIO settings). Returns an error if required vars are missing so
/// the caller can fall back to disk.
async fn build_object_store_buffer(max_bytes: u64) -> anyhow::Result<IngestBuffer> {
    let endpoint = std::env::var("RUSH_BUFFER_S3_ENDPOINT").unwrap_or_default();
    let bucket = std::env::var("RUSH_BUFFER_S3_BUCKET").map_err(|_| {
        anyhow::anyhow!("RUSH_BUFFER_S3_BUCKET is required for object_store backend")
    })?;
    let prefix = std::env::var("RUSH_BUFFER_S3_PREFIX").unwrap_or_else(|_| "ingest/".to_string());
    let region = std::env::var("RUSH_BUFFER_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let access = std::env::var("RUSH_BUFFER_S3_ACCESS_KEY")
        .or_else(|_| std::env::var("AWS_ACCESS_KEY_ID"))
        .unwrap_or_default();
    let secret = std::env::var("RUSH_BUFFER_S3_SECRET_KEY")
        .or_else(|_| std::env::var("AWS_SECRET_ACCESS_KEY"))
        .unwrap_or_default();
    let s = rush_api::object_store_spool::ObjectStoreSpool::open_s3(
        &endpoint, &bucket, &prefix, &region, &access, &secret, max_bytes,
    )
    .await?;
    Ok(IngestBuffer::ObjectStore(s))
}

fn clickhouse_boolean_option(name: &str, default: &str) -> anyhow::Result<String> {
    let value = std::env::var(name).unwrap_or_else(|_| default.to_string());
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok("1".to_string()),
        "0" | "false" | "no" | "off" => Ok("0".to_string()),
        _ => anyhow::bail!("{name} must be a boolean (0/1, true/false, yes/no, on/off)"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("rush_api=info,tower_http=info"));

    let log_format = std::env::var("RUSH_LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());
    match log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        "logfmt" => {
            let layer = tracing_logfmt::layer();
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;
            tracing_subscriber::registry()
                .with(filter)
                .with(layer)
                .init();
        }
        _ => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }

    let cors_policy = Arc::new(
        CorsPolicy::from_env()
            .map_err(|error| anyhow::anyhow!("invalid CORS configuration: {error}"))?,
    );
    if cors_policy.is_empty() {
        tracing::info!(
            "RUSH_ALLOWED_ORIGINS is unset or empty; cross-origin browser access is disabled"
        );
    }

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let clickhouse_db =
        std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "observability".to_string());

    let clickhouse_user =
        std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let clickhouse_password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    // Load rush.toml config (defaults if file missing)
    let wide_config_path =
        std::env::var("RUSH_CONFIG").unwrap_or_else(|_| "./rush.toml".to_string());
    let wide_config = RushConfig::load(&wide_config_path)?;

    // Run schema migrations (CREATE TABLE etc.) — blocks until tables exist.
    migrations::run(
        &clickhouse_url,
        &clickhouse_user,
        &clickhouse_password,
        &wide_config,
    )
    .await?;

    // Spawn TTL + storage policy maintenance in the background so the API
    // starts serving immediately instead of blocking on ALTER TABLE mutations.
    migrations::spawn_maintenance(
        clickhouse_url.clone(),
        clickhouse_user.clone(),
        clickhouse_password.clone(),
        wide_config.clone(),
    );

    let async_insert = clickhouse_boolean_option("RUSH_CLICKHOUSE_ASYNC_INSERT", "1")?;
    let wait_for_async_insert =
        clickhouse_boolean_option("RUSH_CLICKHOUSE_WAIT_FOR_ASYNC_INSERT", "0")?;
    let admin_ch = Client::default()
        .with_url(&clickhouse_url)
        .with_database(&clickhouse_db)
        .with_user(&clickhouse_user)
        .with_password(&clickhouse_password)
        .with_option("max_execution_time", "30")
        // Server-side INSERT buffering: ClickHouse batches writes internally,
        // reducing part creation rate at high ingest volume.
        // These options are silently ignored for SELECT queries.
        //
        // DURABILITY TRADEOFF (deliberate): wait_for_async_insert=0 means an
        // insert is acked once buffered, BEFORE the server-side flush — a
        // flush-time error (e.g. disk full) silently drops those rows and the
        // disk spool never sees them, because the insert "succeeded". We accept
        // that window for ingest throughput; the spool covers the common case
        // (CH down/unreachable → insert errors → rows spooled). Operators can
        // select the durability/latency point without rebuilding the service.
        .with_option("async_insert", &async_insert)
        .with_option("wait_for_async_insert", &wait_for_async_insert)
        .with_compression(clickhouse::Compression::Lz4);

    // Tenant reads use a distinct SELECT-only principal. Startup is fail-closed:
    // custom-setting support, every strict policy, and read-principal behavior
    // must verify before HTTP traffic is accepted. Local development may opt in
    // to the visibly insecure compatibility mode explicitly.
    let insecure_tenant_reads = std::env::var("RUSH_ALLOW_INSECURE_TENANT_READS")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes"
            )
        })
        .unwrap_or(false);
    let read_user = std::env::var("CLICKHOUSE_READ_USER")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let read_password = std::env::var("CLICKHOUSE_READ_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty());

    let mut ch = match (&read_user, &read_password) {
        (Some(user), Some(password)) => Client::default()
            .with_url(&clickhouse_url)
            .with_database(&clickhouse_db)
            .with_user(user)
            .with_password(password)
            .with_option("max_execution_time", "30")
            .with_compression(clickhouse::Compression::Lz4),
        _ if insecure_tenant_reads => admin_ch.clone(),
        _ => anyhow::bail!(
            "CLICKHOUSE_READ_USER and CLICKHOUSE_READ_PASSWORD are required; \
             local development may explicitly set RUSH_ALLOW_INSECURE_TENANT_READS=true"
        ),
    };

    let isolation_result = async {
        let user = read_user
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("tenant read principal is not configured"))?;
        if user == clickhouse_user {
            anyhow::bail!("CLICKHOUSE_READ_USER must differ from CLICKHOUSE_USER");
        }
        rush_api::probe_row_policy_support(&admin_ch).await?;
        migrations::apply_row_policies(&admin_ch, user).await?;
        migrations::verify_row_policies(&admin_ch, &ch, user).await?;
        anyhow::Ok(())
    }
    .await;

    match isolation_result {
        Ok(()) => {
            rush_api::mark_row_policy_enforced();
            tracing::info!(
                read_user = read_user.as_deref().unwrap_or(""),
                "ClickHouse tenant row policies verified and enforcing"
            );
        }
        Err(error) if insecure_tenant_reads => {
            ch = admin_ch.clone();
            rush_api::mark_insecure_tenant_read_override();
            tracing::error!(
                error = %error,
                "INSECURE DEVELOPMENT OVERRIDE: tenant row policies are not enforced"
            );
        }
        Err(error) => return Err(error.context("ClickHouse tenant-isolation verification failed")),
    }

    let config_db =
        Arc::new(ConfigDb::open(&clickhouse_url, &clickhouse_user, &clickhouse_password).await?);
    // Shared system-health registry is created before the audit writer so
    // outbox degradation is visible even if no one has scraped `/metrics` yet.
    let self_metrics: std::sync::Arc<rush_api::self_metrics::SelfMetrics> =
        std::sync::Arc::new(rush_api::self_metrics::SelfMetrics::new());
    let query_governor_config = match config_db.get_setting(QUERY_LIMITS_SETTING_KEY).await? {
        Some(raw) => match serde_json::from_str::<QueryGovernorConfig>(&raw) {
            Ok(config) => match config.validate() {
                Ok(()) => config,
                Err(error) => {
                    tracing::error!(%error, "stored query workload limits are invalid; using safe defaults");
                    QueryGovernorConfig::default()
                }
            },
            Err(error) => {
                tracing::error!(%error, "stored query workload limits cannot be decoded; using safe defaults");
                QueryGovernorConfig::default()
            }
        },
        None => QueryGovernorConfig::default(),
    };
    let query_governor = Arc::new(
        QueryGovernor::new(query_governor_config, self_metrics.clone())
            .map_err(anyhow::Error::msg)
            .context("query workload configuration is invalid")?,
    );
    rush_api::query_governor::install_global(query_governor.clone());
    let ingest_limits = rush_api::ingest_limits::IngestLimits::from_env(self_metrics.clone())
        .map_err(anyhow::Error::msg)
        .context("ingest limit configuration is invalid")?;
    let llm_gateway = rush_api::llm_gateway::LlmGateway::from_env(self_metrics.clone())
        .await
        .context("LLM gateway configuration is invalid")?;
    if llm_gateway.is_configured() {
        tracing::info!("bounded LLM gateway configured");
    }
    // Build the audit chain before bootstrap tenant mutation so a newly seeded
    // default tenant is recorded like every other tenant creation.
    let audit = std::sync::Arc::new(
        rush_api::audit::AuditLogger::new(admin_ch.clone(), self_metrics.clone())
            .await
            .context("audit logger initialization failed")?,
    );
    audit.spawn_replayer();
    let export_jobs = Arc::new(handlers::export::ExportJobs::from_env()?);
    export_jobs.spawn_janitor(audit.clone());
    let invalidated_sessions = config_db.invalidate_legacy_session_tokens().await?;
    if invalidated_sessions > 0 {
        audit
            .log(
                rush_api::audit::AuditEvent::new("session.legacy_tokens_invalidate", "system")
                    .tenant("default")
                    .resource("session", "legacy-storage-migration")
                    .changes(
                        serde_json::json!({ "invalidated_count": invalidated_sessions })
                            .to_string(),
                    )
                    .description(
                        "sessions using pre-HMAC token storage were invalidated during startup",
                    ),
            )
            .await;
    }
    let sso_reconciliation = config_db.reconcile_active_sso_provider().await?;
    if sso_reconciliation.changed {
        if !sso_reconciliation.ambiguous_provider_ids.is_empty() {
            tracing::error!(
                provider_count = sso_reconciliation.ambiguous_provider_ids.len(),
                "multiple legacy SSO providers were enabled; SSO was disabled until an administrator selects one"
            );
        }
        audit
            .log(
                rush_api::audit::AuditEvent::new("sso.active_provider_reconcile", "system")
                    .tenant("default")
                    .resource("sso_active_provider", "primary")
                    .outcome("success")
                    .changes(
                        serde_json::json!({
                            "active_provider_id": sso_reconciliation.active_provider_id,
                            "ambiguous_provider_ids": sso_reconciliation.ambiguous_provider_ids,
                        })
                        .to_string(),
                    )
                    .description("legacy SSO enabled-provider state reconciled during startup"),
            )
            .await;
    }
    for provider_id in config_db.legacy_sso_client_secret_ids().await? {
        if !config_db
            .encrypt_legacy_sso_client_secret(&provider_id)
            .await?
        {
            continue;
        }
        audit
            .log(
                rush_api::audit::AuditEvent::new("sso.client_secret_encrypt", "system")
                    .tenant("default")
                    .resource("sso_provider", provider_id)
                    .outcome("success")
                    .changes(serde_json::json!({ "encrypted": true }).to_string())
                    .description("legacy SSO client secret encrypted during startup"),
            )
            .await;
    }
    let default_tenant_created = config_db.ensure_default_tenant().await?;
    if default_tenant_created {
        let auth_required = rush_api::api_key_auth::default_tenant_auth_required(
            rush_api::api_key_auth::allow_anonymous_default(),
        );
        audit
            .log(
                rush_api::audit::AuditEvent::new("tenant.create", "system")
                    .tenant("default")
                    .resource("tenant", "default")
                    .outcome("success")
                    .changes(
                        serde_json::json!({
                            "name": "default",
                            "enabled": true,
                            "auth_required": auth_required,
                            "ingest_auth_required": auth_required,
                        })
                        .to_string(),
                    )
                    .description("default tenant created during bootstrap"),
            )
            .await;
    }
    if let Ok(bootstrap_ingest_key) = std::env::var("RUSH_BOOTSTRAP_INGEST_API_KEY") {
        if bootstrap_ingest_key.len() < 32 || !bootstrap_ingest_key.is_ascii() {
            audit
                .log(
                    rush_api::audit::AuditEvent::new("apikey.create", "system")
                        .actor_name("query-api bootstrap")
                        .tenant("default")
                        .resource("api_key", "bootstrap-ingest-default")
                        .outcome("failure")
                        .changes(
                            serde_json::json!({
                                "name": "Helm-managed ingest",
                                "tenant": "default",
                                "key_type": "ingest",
                                "bootstrap": true,
                                "reason": "invalid_key_format",
                            })
                            .to_string(),
                        )
                        .description("Helm-managed ingest API key bootstrap rejected"),
                )
                .await;
            anyhow::bail!(
                "RUSH_BOOTSTRAP_INGEST_API_KEY must contain at least 32 ASCII bytes when configured"
            );
        }
        let key_hash = handlers::settings::hash_api_key(&bootstrap_ingest_key);
        let prefix = bootstrap_ingest_key.chars().take(12).collect::<String>();
        let bootstrap_key = config_db
            .ensure_bootstrap_ingest_api_key(&key_hash, &prefix)
            .await;
        match bootstrap_key {
            Ok(Some(key_id)) => {
                let signals = rush_api::api_key_auth::INGEST_SIGNALS;
                audit
                    .log(
                        rush_api::audit::AuditEvent::new("apikey.create", "system")
                            .actor_name("query-api bootstrap")
                            .tenant("default")
                            .resource("api_key", &key_id)
                            .outcome("success")
                            .changes(
                                serde_json::json!({
                                    "name": "Helm-managed ingest",
                                    "prefix": prefix,
                                    "tenant": "default",
                                    "key_type": "ingest",
                                    "signals": signals,
                                    "rate_limit_per_minute": 1_000_000,
                                    "source_restricted": false,
                                    "bootstrap": true,
                                })
                                .to_string(),
                            )
                            .description("Helm-managed ingest API key created during bootstrap"),
                    )
                    .await;
                tracing::info!(
                    key_id = %key_id,
                    key_prefix = %prefix,
                    "Helm-managed ingest API key registered"
                );
            }
            Ok(None) => {}
            Err(error) => {
                audit
                    .log(
                        rush_api::audit::AuditEvent::new("apikey.create", "system")
                            .actor_name("query-api bootstrap")
                            .tenant("default")
                            .resource("api_key", "bootstrap-ingest-default")
                            .outcome("failure")
                            .changes(
                                serde_json::json!({
                                    "name": "Helm-managed ingest",
                                    "prefix": prefix,
                                    "tenant": "default",
                                    "key_type": "ingest",
                                    "bootstrap": true,
                                    "reason": "bootstrap_store_unavailable",
                                })
                                .to_string(),
                            )
                            .description("Helm-managed ingest API key bootstrap failed"),
                    )
                    .await;
                return Err(error);
            }
        }
    }
    // Reserve the `_audit` tenant (seeded disabled) so it's never an ingest target.
    config_db.ensure_audit_tenant().await?;
    // Seed the UI/tenant global-retention store from rushConfig.retention.defaults
    // (only on a fresh, unseeded cluster) so new tenants inherit the Helm-configured
    // retention instead of a hardcoded 365. traces → apm_days.
    {
        let rd = &wide_config.retention.defaults;
        let default_days = rd.metrics_days.max(rd.traces_days).max(rd.logs_days) as i32;
        config_db
            .ensure_global_retention(
                default_days,
                rd.logs_days as i32,
                rd.metrics_days as i32,
                rd.traces_days as i32,
            )
            .await?;
    }
    let bootstrap_admin = match config_db.ensure_default_admin().await {
        Ok(admin_id) => admin_id,
        Err(error) => {
            let (reason, policy_code) = error
                .downcast_ref::<rush_api::clickhouse_config::PasswordPolicyError>()
                .map(|policy| ("password_policy", Some(policy.code())))
                .unwrap_or(("bootstrap_store_unavailable", None));
            audit
                .log(
                    rush_api::audit::AuditEvent::new("user.create", "system")
                        .actor_name("query-api bootstrap")
                        .tenant("default")
                        .resource("user", "initial-admin")
                        .outcome("failure")
                        .changes(
                            serde_json::json!({
                                "username": "admin",
                                "bootstrap": true,
                                "reason": reason,
                                "policy_code": policy_code,
                            })
                            .to_string(),
                        )
                        .description("initial administrator creation failed"),
                )
                .await;
            return Err(error);
        }
    };
    if let Some(admin_id) = bootstrap_admin {
        audit
            .log(
                rush_api::audit::AuditEvent::new("user.create", "system")
                    .actor_name("query-api bootstrap")
                    .tenant("default")
                    .resource("user", admin_id)
                    .outcome("success")
                    .changes(
                        serde_json::json!({
                            "username": "admin",
                            "role": "admin",
                            "auth_provider": "local",
                            "bootstrap": true,
                        })
                        .to_string(),
                    )
                    .description("initial administrator created from configured secret"),
            )
            .await;
    }
    config_db.ensure_default_groups().await?;
    handlers::auth::validate_sso_only_config()
        .map_err(|error| anyhow::anyhow!("invalid SSO-only configuration: {error}"))?;
    if handlers::auth::sso_only_mode_enabled() {
        config_db
            .validate_break_glass_account(&handlers::auth::break_glass_username())
            .await
            .map_err(|error| anyhow::anyhow!("invalid SSO-only break-glass account: {error}"))?;
    }
    config_db.ensure_default_templates().await?;
    let tenants = config_db.list_tenants().await?;
    let anonymous_query_tenants = tenants
        .iter()
        .filter(|(_, _, enabled, auth_required, _)| *enabled && !*auth_required)
        .map(|(_, name, _, _, _)| name.clone())
        .collect::<Vec<_>>();
    let mut anonymous_ingest_tenants = Vec::new();
    for (id, name, enabled, _, _) in &tenants {
        if *enabled
            && !config_db
                .tenant_ingest_auth_required_checked(id)
                .await?
                .unwrap_or(true)
        {
            anonymous_ingest_tenants.push(name.clone());
        }
    }
    if !anonymous_query_tenants.is_empty() || !anonymous_ingest_tenants.is_empty() {
        tracing::warn!(
            query_tenants = ?anonymous_query_tenants,
            ingest_tenants = ?anonymous_ingest_tenants,
            "enabled tenants intentionally permit anonymous access; review tenant authentication settings"
        );
    }
    if rush_api::api_key_auth::allow_anonymous_default() {
        tracing::warn!("INSECURE DEVELOPMENT OVERRIDE: RUSH_ALLOW_ANONYMOUS_DEFAULT is enabled");
    }
    tracing::info!("config db opened");

    // SMTP config for email notifications (optional)
    let smtp_config = alert_engine::SmtpConfig {
        host: std::env::var("RUSH_SMTP_HOST").ok(),
        port: std::env::var("RUSH_SMTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(587),
        user: std::env::var("RUSH_SMTP_USER").ok(),
        pass: std::env::var("RUSH_SMTP_PASS").ok(),
        from: std::env::var("RUSH_SMTP_FROM").unwrap_or_else(|_| "wide@localhost".to_string()),
    };

    // Ingest-buffer drain controls (Phase 3):
    //  RUSH_DRAIN_WORKER_ONLY=true → run only the buffer drain (no HTTP, no engines).
    //  RUSH_RUN_REPLAYER=false     → don't drain in this process (API replicas opt out
    //                                so the drain is single-writer in HA / object-store).
    let drain_only = std::env::var("RUSH_DRAIN_WORKER_ONLY")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(false);
    let run_replayer = std::env::var("RUSH_RUN_REPLAYER")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    let expected_query_api_replicas = match std::env::var("RUSH_EXPECTED_QUERY_API_REPLICAS") {
        Ok(raw) => raw.parse::<usize>().map_err(|_| {
            anyhow::anyhow!(
                "RUSH_EXPECTED_QUERY_API_REPLICAS must be a positive integer, got {raw:?}"
            )
        })?,
        Err(_) => 1,
    };
    let require_object_store = std::env::var("RUSH_BUFFER_REQUIRE_OBJECT_STORE")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(false);
    let backend = std::env::var("RUSH_BUFFER_BACKEND").unwrap_or_else(|_| "disk".to_string());
    let shutdown_controller = rush_api::shutdown::ShutdownController::new();

    // Validate the deployment contract before starting engines. For an HA
    // rollout this prevents every API replica from replaying the same shared
    // object before the dedicated drain worker is ready.
    buffer_topology::validate(
        &backend,
        &backend,
        expected_query_api_replicas,
        drain_only,
        run_replayer,
        require_object_store,
    )?;

    // System-health self-metrics registry. Single in-process source of truth for the
    // open `/metrics` Prometheus endpoint AND the self-ingested series the stats engine
    // writes into our own metrics tables. Constructed before engine spawns + middleware
    // so the same Arc is shared everywhere.
    rush_api::process_metrics::sample(&self_metrics);
    rush_api::process_metrics::spawn(self_metrics.clone());
    let instance_id = rush_api::stats_engine::configured_instance_id();
    tracing::info!(instance_id = %instance_id, "self-metrics instance identity configured");

    // Spawn background engines (skipped in drain-worker-only mode)
    //
    // Each rule-evaluation engine runs in-process by default (single-binary /
    // local dev) and can be disabled per-replica with RUSH_RUN_<X>_ENGINE=false
    // so that in HA only one replica evaluates rules — N replicas would mean N×
    // rule-eval load on ClickHouse and N× duplicate notifications (the
    // last_eval_at gate is racy across replicas: ReplacingMergeTree is
    // eventually consistent). Same semantics as RUSH_RUN_ANOMALY_ENGINE below.
    let engine_enabled = |var: &str| -> bool {
        std::env::var(var)
            .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
            .unwrap_or(true)
    };
    if !drain_only {
        // Legacy alert-rules engine retired — Monitors (monitor_engine) is the single
        // alerting system. The alert_engine module is kept only for the shared
        // notification infrastructure (SmtpConfig, send_channel_notification) that
        // Monitors and the anomaly engine use; the rule-evaluation loop no longer runs.
        if engine_enabled("RUSH_RUN_SLO_ENGINE") {
            slo_engine::spawn_slo_engine(
                config_db.clone(),
                ch.clone(),
                admin_ch.clone(),
                self_metrics.clone(),
            );
        } else {
            tracing::info!(
                "in-process slo engine disabled (RUSH_RUN_SLO_ENGINE=false); expecting a dedicated slo-engine deployment"
            );
        }

        // Anomaly detection engine — evaluates anomaly rules, persists events, and
        // sends notifications. Queries Prometheus-source rules against the API's own
        // /prom endpoint (RUSH_PROM_BASE_URL, defaulting to this server).
        //
        // Runs in-process by default (single-binary / local dev). In Kubernetes the
        // chart runs a dedicated `anomaly_engine` Deployment, so it sets
        // RUSH_RUN_ANOMALY_ENGINE=false on the API to avoid double-evaluating rules
        // and sending duplicate notifications.
        let run_anomaly_in_process = std::env::var("RUSH_RUN_ANOMALY_ENGINE")
            .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
            .unwrap_or(true);
        if run_anomaly_in_process {
            let prom_base_url = std::env::var("RUSH_PROM_BASE_URL")
                .unwrap_or_else(|_| "http://localhost:8080".to_string());
            anomaly_engine::spawn_anomaly_engine(
                config_db.clone(),
                ch.clone(),
                admin_ch.clone(),
                smtp_config.clone(),
                prom_base_url,
                self_metrics.clone(),
            );
        } else {
            tracing::info!(
                "in-process anomaly engine disabled (RUSH_RUN_ANOMALY_ENGINE=false); expecting a dedicated anomaly-engine deployment"
            );
        }
        retention_enforcer::spawn_retention_enforcer(
            admin_ch.clone(),
            wide_config.clone(),
            config_db.clone(),
            self_metrics.clone(),
        );
        // stats_engine is spawned after the ingest buffer is built (it emits buffer metrics).

        // Spawn the Datadog-style monitor engine (v2 alerting)
        if engine_enabled("RUSH_RUN_MONITOR_ENGINE") {
            monitor_engine::spawn(
                ch.clone(),
                config_db.clone(),
                smtp_config,
                self_metrics.clone(),
            );
        } else {
            tracing::info!(
                "in-process monitor engine disabled (RUSH_RUN_MONITOR_ENGINE=false); expecting a dedicated monitor-engine deployment"
            );
        }

        // Seed built-in SIEM detection rules and spawn the SIEM detection engine
        config_db.ensure_default_detection_rules().await?;
        if engine_enabled("RUSH_RUN_SIEM_ENGINE") {
            siem_engine::spawn(ch.clone(), config_db.clone(), self_metrics.clone());
        } else {
            tracing::info!(
                "in-process siem engine disabled (RUSH_RUN_SIEM_ENGINE=false); expecting a dedicated siem-engine deployment"
            );
        }
    } // end `if !drain_only` (background engines)

    // ── Durable write path: spool + writer ──
    let spool_dir = std::env::var("RUSH_SPOOL_DIR").unwrap_or_else(|_| "./data/spool".to_string());
    let spool_max_bytes: u64 = std::env::var("RUSH_SPOOL_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_147_483_648); // 2 GiB default

    // Backend selection. Disk is the default and needs no object store. The
    // object-store backend is opt-in via RUSH_BUFFER_BACKEND=object_store; a
    // single-replica deployment may fall back to disk, while HA must fail closed.
    let (buffer, effective_backend) = if backend == "object_store" {
        match build_object_store_buffer(spool_max_bytes).await {
            Ok(b) => {
                tracing::info!("ingest buffer backend: object_store");
                (std::sync::Arc::new(b), "object_store")
            }
            Err(e) => {
                if require_object_store || expected_query_api_replicas > 1 {
                    return Err(anyhow::anyhow!(
                        "object_store buffer backend failed to initialize and disk fallback is unsafe for this deployment: {e}"
                    ));
                }
                tracing::error!(error = %e, "object_store buffer backend failed to init — falling back to disk");
                let spool = Spool::open(&spool_dir, spool_max_bytes)
                    .expect("failed to open spool directory");
                (std::sync::Arc::new(IngestBuffer::Disk(spool)), "disk")
            }
        }
    } else {
        let spool =
            Spool::open(&spool_dir, spool_max_bytes).expect("failed to open spool directory");
        (std::sync::Arc::new(IngestBuffer::Disk(spool)), "disk")
    };
    buffer_topology::validate(
        &backend,
        effective_backend,
        expected_query_api_replicas,
        drain_only,
        run_replayer,
        require_object_store,
    )?;
    let writer = ChWriter::new(admin_ch.clone(), buffer);
    let replayer_handle = if drain_only || run_replayer {
        Some(writer.clone().spawn_replayer(shutdown_controller.clone()))
    } else {
        None
    };
    // Cross-request insert batching: only the HTTP-serving process buffers
    // ingest rows. The drain-only process never calls `write`, so it needs no
    // flusher (and must not buffer — it only replays the spool).
    if !drain_only {
        let bc = writer.batch_config();
        writer.spawn_flusher();
        tracing::info!(
            batch_rows = bc.max_rows,
            batch_ms = bc.max_age.as_millis() as u64,
            "ingest insert batching configured"
        );
    }
    // Stats engine (emits ingest-buffer depth/age/drain metrics). API process only.
    if !drain_only {
        stats_engine::spawn_stats_engine(
            admin_ch.clone(),
            writer.buffer.clone(),
            self_metrics.clone(),
            instance_id,
        );
    }

    // Drain-worker-only: this process exists solely to drain the ingest buffer
    // into ClickHouse. Don't serve HTTP, run engines, or load the firewall.
    if drain_only {
        tracing::info!(
            backend = effective_backend,
            "RUSH_DRAIN_WORKER_ONLY — draining ingest buffer to ClickHouse; not serving HTTP"
        );
        shutdown_signal(shutdown_controller.clone()).await;
        graceful_shutdown_drain(writer, shutdown_controller, replayer_handle).await;
        return Ok(());
    }

    // Metric firewall: load compiled rules now, then refresh periodically so
    // changes (incl. from other replicas) propagate to the ingest hot path.
    if let Ok(fw) = config_db.compiled_metric_firewall().await {
        if let Ok(mut g) = writer.firewall.write() {
            *g = Arc::new(fw);
        }
    }
    {
        let fw_handle = writer.firewall.clone();
        let cdb = config_db.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tick.tick().await;
                if let Ok(fw) = cdb.compiled_metric_firewall().await {
                    if let Ok(mut g) = fw_handle.write() {
                        *g = Arc::new(fw);
                    }
                }
            }
        });
    }

    // Spawn usage tracker (fire-and-forget signal usage tracking)
    let usage = usage_tracker::spawn(admin_ch.clone(), self_metrics.clone());

    // Spawn usage accumulator (per-tenant ingest metering)
    let usage_accumulator = UsageAccumulator::with_metrics(self_metrics.clone());
    usage_accumulator.spawn_flusher(admin_ch.clone());

    handlers::auth::validate_login_rate_limit_secret()
        .map_err(|error| anyhow::anyhow!("invalid login rate-limit configuration: {error}"))?;
    let login_account_limit_per_minute =
        handlers::auth::login_limit_from_env("RUSH_LOGIN_ACCOUNT_LIMIT_PER_MINUTE", 10)
            .map_err(|error| anyhow::anyhow!("invalid login rate-limit configuration: {error}"))?;
    let login_ip_limit_per_minute =
        handlers::auth::login_limit_from_env("RUSH_LOGIN_IP_LIMIT_PER_MINUTE", 50)
            .map_err(|error| anyhow::anyhow!("invalid login rate-limit configuration: {error}"))?;
    let trusted_proxy_cidrs = std::sync::Arc::new(
        handlers::auth::trusted_proxy_cidrs_from_env()
            .map_err(|error| anyhow::anyhow!("invalid trusted-proxy configuration: {error}"))?,
    );
    let login_limiter: std::sync::Arc<dashmap::DashMap<String, (u32, std::time::Instant)>> =
        std::sync::Arc::new(dashmap::DashMap::new());

    // Spawn background task to evict stale rate-limiter entries (M7)
    {
        let limiter_clone = login_limiter.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                // Hard cap: if oversized (e.g. IP flood), evict aggressively.
                if limiter_clone.len() > 100_000 {
                    limiter_clone
                        .retain(|_, (_, ts)| ts.elapsed() < std::time::Duration::from_secs(5));
                } else {
                    limiter_clone
                        .retain(|_, (_, ts)| ts.elapsed() < std::time::Duration::from_secs(60));
                }
            }
        });
    }

    let ingest_key_limiter: std::sync::Arc<dashmap::DashMap<String, (u64, std::time::Instant)>> =
        std::sync::Arc::new(dashmap::DashMap::new());

    {
        let limiter = ingest_key_limiter.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                limiter.retain(|_, (_, started)| {
                    started.elapsed() < std::time::Duration::from_secs(60)
                });
            }
        });
    }

    // Spawn background task to proactively evict stale suggest cache entries.
    // Without this, entries queried once but never again persist forever.
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let cache = handlers::suggest::suggest_cache();
            if cache.len() > 10_000 {
                cache.retain(|_, (_, ts)| ts.elapsed() < std::time::Duration::from_secs(5));
            } else {
                cache.retain(|_, (_, ts)| ts.elapsed() < std::time::Duration::from_secs(30));
            }
        }
    });

    // API-managed integration collector supervisor. It is opt-in and feature
    // gated so the community binary never launches paid collectors.
    let collectors = std::sync::Arc::new(rush_api::integrations::CollectorManager::new(
        config_db.clone(),
    ));
    collectors.spawn_reconciler();

    let state = AppState {
        ch,
        admin_ch,
        writer,
        config_db,
        usage,
        usage_accumulator,
        config: wide_config,
        cors_policy,
        login_limiter,
        login_account_limit_per_minute,
        login_ip_limit_per_minute,
        trusted_proxy_cidrs,
        ingest_key_limiter,
        session_rotation_checks: Arc::new(DashMap::new()),
        audit,
        self_metrics,
        query_governor,
        export_jobs,
        ingest_limits: ingest_limits.clone(),
        llm_gateway,
        collectors,
        shutdown: shutdown_controller.clone(),
    };

    let inner = Router::new()
        // Trace endpoints
        .route("/api/v1/traces/{trace_id}", get(handlers::traces::get_trace))
        // Jaeger-compatible query API (for Grafana's built-in Jaeger data source).
        // Mount behind /t/{tenant}/jaeger; auth via tenant-scoped API key.
        .route("/jaeger/api/services", get(handlers::jaeger::services))
        .route("/jaeger/api/services/{service}/operations", get(handlers::jaeger::operations))
        .route("/jaeger/api/traces", get(handlers::jaeger::search))
        .route("/jaeger/api/traces/{trace_id}", get(handlers::jaeger::get_trace))
        .route("/jaeger/api/dependencies", get(handlers::jaeger::dependencies))
        // Query endpoints
        .route("/api/v1/query", post(handlers::query::execute_query))
        .route("/api/v1/query/count", post(handlers::query::count_query))
        .route("/api/v1/query/group", post(handlers::query::group_query))
        .route("/api/v1/query/timeseries", post(handlers::query::timeseries_query))
        .route("/api/v1/explore/search", post(handlers::explore::search))
        // Export current query + results (CSV/JSON), capped by export_max_rows
        .route("/api/v1/query/export", post(handlers::query::export_query))
        // BubbleUp comparison analysis
        .route("/api/v1/bubbleup", post(handlers::bubbleup::bubbleup))
        // Log endpoints
        .route("/api/v1/logs", post(handlers::logs::query_logs))
        .route("/api/v1/logs/detail", post(handlers::logs::get_log_detail))
        .route("/api/v1/logs/context", post(handlers::logs::get_log_context))
        .route("/api/v1/logs/count", post(handlers::logs::count_logs))
        .route("/api/v1/logs/histogram", post(handlers::logs::log_histogram))
        .route("/api/v1/logs/group", post(handlers::logs::group_logs))
        .route("/api/v1/logs/export", post(handlers::logs::export_logs))
        .route(
            "/api/v1/exports/{id}",
            get(handlers::export::get_export_job).delete(handlers::export::cancel_export_job),
        )
        .route(
            "/api/v1/exports/{id}/download",
            get(handlers::export::download_export_job),
        )
        // Service catalog
        .route("/api/v1/services", get(handlers::services::list_services))
        .route("/api/v1/services/graph", get(handlers::services::service_graph))
        .route("/api/v1/services/time-breakdown", get(handlers::services::service_time_breakdown))
        .route("/api/v1/services/time-breakdown/timeseries", get(handlers::services::service_time_breakdown_timeseries))
        .route("/api/v1/services/latency-histogram", get(handlers::services::service_latency_histogram))
        .route("/api/v1/services/endpoints", get(handlers::services::service_endpoints))
        .route("/api/v1/services/errors", get(handlers::services::service_errors))
        // SRE-agent gateway: query-api fronts the agent (auth + tenant injection),
        // so the browser only talks to query-api. Forwards to SRE_AGENT_URL.
        .route("/api/v1/investigate", post(handlers::sre_proxy::investigate))
        .route("/api/v1/sessions", get(handlers::sre_proxy::list_sessions))
        .route(
            "/api/v1/sessions/{id}",
            get(handlers::sre_proxy::get_session).delete(handlers::sre_proxy::delete_session),
        )
        .route("/api/v1/investigation-templates", get(handlers::sre_proxy::list_investigation_templates))
        // Natural language query parsing (LLM-powered)
        .route("/api/v1/parse-query", post(handlers::parse_query::parse_query))
        .route("/api/v1/parse-promql", post(handlers::parse_promql::parse_promql))
        // Field suggestions
        .route(
            "/api/v1/suggest/{field}",
            get(handlers::suggest::suggest_values),
        )
        // Dashboard endpoints
        .route(
            "/api/v1/dashboards",
            get(handlers::dashboards::list_dashboards).post(handlers::dashboards::create_dashboard),
        )
        .route(
            "/api/v1/dashboards/import",
            post(handlers::dashboards::import_dashboard),
        )
        .route(
            "/api/v1/dashboards/{id}",
            get(handlers::dashboards::get_dashboard)
                .put(handlers::dashboards::update_dashboard)
                .delete(handlers::dashboards::delete_dashboard),
        )
        .route(
            "/api/v1/dashboards/{id}/widgets",
            post(handlers::dashboards::create_widget),
        )
        .route(
            "/api/v1/dashboards/{id}/widgets/{wid}",
            put(handlers::dashboards::update_widget).delete(handlers::dashboards::delete_widget),
        )
        .route(
            "/api/v1/dashboards/{id}/export",
            get(handlers::dashboards::export_dashboard),
        )
        // Dashboard template endpoints
        .route(
            "/api/v1/dashboard-templates",
            get(handlers::dashboards::list_dashboard_templates),
        )
        .route(
            "/api/v1/dashboard-templates/{tid}/create",
            post(handlers::dashboards::create_from_template),
        )
        // Notification channels
        .route(
            "/api/v1/channels",
            get(handlers::alerts::list_channels).post(handlers::alerts::create_channel),
        )
        .route(
            "/api/v1/channels/{id}",
            put(handlers::alerts::update_channel).delete(handlers::alerts::delete_channel),
        )
        .route(
            "/api/v1/channels/{id}/notify",
            post(handlers::alerts::notify_channel),
        )
        .route(
            "/api/v1/channels/{id}/test",
            post(handlers::alerts::test_channel),
        )
        .route(
            "/api/v1/notifications/log",
            get(handlers::alerts::list_notification_log),
        )
        // Legacy alert-rules endpoints removed (system retired in favor of Monitors).
        // Trace Funnels
        .route(
            "/api/v1/funnels",
            get(handlers::funnels::list_funnels).post(handlers::funnels::create_funnel),
        )
        .route(
            "/api/v1/funnels/{id}",
            delete(handlers::funnels::delete_funnel),
        )
        .route(
            "/api/v1/funnels/{id}/run",
            post(handlers::funnels::run_funnel),
        )
        // Maintenance Windows
        .route(
            "/api/v1/maintenance-windows",
            get(handlers::maintenance::list_windows).post(handlers::maintenance::create_window),
        )
        .route(
            "/api/v1/maintenance-windows/{id}",
            delete(handlers::maintenance::delete_window),
        )
        // Monitors (Datadog-style v2 alerting)
        .route(
            "/api/v1/monitors",
            get(handlers::monitors::list_monitors).post(handlers::monitors::create_monitor),
        )
        .route(
            "/api/v1/monitors/autocomplete",
            get(handlers::monitors::autocomplete),
        )
        .route(
            "/api/v1/monitors/suggest",
            post(handlers::monitors::suggest),
        )
        .route(
            "/api/v1/monitors/preview",
            post(handlers::monitors::preview_monitor),
        )
        .route(
            "/api/v1/monitors/{id}",
            get(handlers::monitors::get_monitor)
                .put(handlers::monitors::update_monitor)
                .delete(handlers::monitors::delete_monitor),
        )
        .route(
            "/api/v1/monitors/{id}/events",
            get(handlers::monitors::list_monitor_events),
        )
        .route(
            "/api/v1/monitors/{id}/mute",
            post(handlers::monitors::mute_monitor),
        )
        .route(
            "/api/v1/monitors/{id}/unmute",
            post(handlers::monitors::unmute_monitor),
        )
        // SLOs
        .route(
            "/api/v1/slos",
            get(handlers::slos::list_slos).post(handlers::slos::create_slo),
        )
        .route(
            "/api/v1/slos/{id}",
            get(handlers::slos::get_slo)
                .put(handlers::slos::update_slo)
                .delete(handlers::slos::delete_slo),
        )
        .route(
            "/api/v1/slos/{id}/events",
            get(handlers::slos::list_slo_events),
        )
        // Anomaly rules
        .route(
            "/api/v1/anomaly-rules",
            get(handlers::anomalies::list_anomaly_rules)
                .post(handlers::anomalies::create_anomaly_rule),
        )
        .route(
            "/api/v1/anomaly-rules/{id}",
            get(handlers::anomalies::get_anomaly_rule)
                .put(handlers::anomalies::update_anomaly_rule)
                .delete(handlers::anomalies::delete_anomaly_rule),
        )
        .route(
            "/api/v1/anomaly-events",
            get(handlers::anomalies::list_all_anomaly_events),
        )
        .route(
            "/api/v1/anomaly-events/{event_id}",
            get(handlers::anomalies::get_anomaly_event),
        )
        .route(
            "/api/v1/anomaly-events/{event_id}/correlations",
            get(handlers::anomalies::get_event_correlations),
        )
        .route(
            "/api/v1/anomaly-events/{event_id}/analyze",
            post(handlers::anomalies::analyze_anomaly_event),
        )
        // SIEM Detection rules
        .route(
            "/api/v1/detection/rules",
            get(handlers::detection::list_detection_rules)
                .post(handlers::detection::create_detection_rule),
        )
        .route(
            "/api/v1/detection/rules/{id}",
            get(handlers::detection::get_detection_rule)
                .put(handlers::detection::update_detection_rule)
                .delete(handlers::detection::delete_detection_rule),
        )
        .route(
            "/api/v1/detection/rules/{id}/test",
            post(handlers::detection::test_detection_rule),
        )
        .route(
            "/api/v1/detection/events",
            get(handlers::detection::list_detection_events),
        )
        // Prometheus-compatible metrics API (for Grafana)
        .route(
            "/prom/api/v1/query",
            get(handlers::metrics::prom_query).post(handlers::metrics::prom_query_post),
        )
        .route(
            "/prom/api/v1/query_range",
            get(handlers::metrics::prom_query_range).post(handlers::metrics::prom_query_range_post),
        )
        .route(
            "/prom/api/v1/series",
            get(handlers::metrics::prom_series).post(handlers::metrics::prom_series_post),
        )
        .route(
            "/prom/api/v1/labels",
            get(handlers::metrics::prom_labels).post(handlers::metrics::prom_labels),
        )
        .route(
            "/prom/api/v1/label/{name}/values",
            get(handlers::metrics::prom_label_values),
        )
        // Prometheus remote write
        .route(
            "/prom/api/v1/write",
            post(handlers::remote_write::prom_remote_write),
        )
        // Deploy markers
        .route(
            "/api/v1/deploys",
            get(handlers::deploys::list_deploys).post(handlers::deploys::create_deploy),
        )
        // Service Links (service → GitHub repo mapping)
        .route(
            "/api/v1/service-links",
            get(handlers::service_links::list_service_links)
                .post(handlers::service_links::create_service_link),
        )
        .route(
            "/api/v1/service-links/{service_name}",
            delete(handlers::service_links::delete_service_link),
        )
        // Internal-only audit sink for source-code reads performed by sre-agent.
        .route(
            "/api/v1/internal/repository-access-audit",
            post(handlers::repository_access::audit_repository_access),
        )
        .route(
            "/api/v1/internal/kubernetes-access-events",
            get(handlers::kubernetes_access::list_agent_access_events),
        )
        // Feature flags (public — no auth)
        .route("/api/v1/features", get(handlers::settings::get_features))
        .route("/api/v1/license", get(handlers::license::get_license))
        .route("/api/v1/integrations/registry", get(handlers::integrations::registry))
        .route(
            "/api/v1/integrations/{integration}/targets",
            get(handlers::integrations::list_targets)
                .post(handlers::integrations::create_target),
        )
        .route(
            "/api/v1/integrations/{integration}/targets/{id}",
            put(handlers::integrations::update_target)
                .delete(handlers::integrations::delete_target),
        )
        // Postgres EXPLAIN job queue (UI submit/poll-result; collector poll/post)
        .route("/api/v1/integrations/postgres/explain", post(handlers::pg_explain::submit))
        .route("/api/v1/integrations/postgres/explain/poll", get(handlers::pg_explain::poll))
        .route("/api/v1/integrations/postgres/explain/{id}", get(handlers::pg_explain::get_job))
        .route("/api/v1/integrations/postgres/explain/{id}/result", post(handlers::pg_explain::post_result))
        .route("/api/v1/integrations/mysql/explain", post(handlers::mysql_explain::submit))
        .route("/api/v1/integrations/mysql/explain/poll", get(handlers::mysql_explain::poll))
        .route("/api/v1/integrations/mysql/explain/{id}", get(handlers::mysql_explain::get_job))
        .route("/api/v1/integrations/mysql/explain/{id}/result", post(handlers::mysql_explain::post_result))
        // Export row cap (admin-only setter; value also exposed via /features)
        .route("/api/v1/settings/export-max-rows", put(handlers::settings::set_export_max_rows))
        .route(
            "/api/v1/settings/query-limits",
            get(handlers::settings::get_query_limits).put(handlers::settings::set_query_limits),
        )
        .route("/api/v1/settings/config", get(handlers::settings::get_runtime_config))
        .route(
            "/api/v1/settings/sre-agent",
            get(handlers::settings::get_sre_agent_settings).put(handlers::settings::set_sre_agent_settings),
        )
        .route(
            "/api/v1/settings/sre-agent/models",
            get(handlers::settings::list_sre_agent_models),
        )
        // User-facing model/thinking menu (the admin-defined policy). Any
        // authenticated user can read it to populate the investigation pickers.
        .route(
            "/api/v1/sre-agent/options",
            get(handlers::settings::get_sre_agent_options),
        )
        .route(
            "/api/v1/settings/deploy-markers",
            get(handlers::settings::get_deploy_markers_setting).put(handlers::settings::set_deploy_markers_setting),
        )
        .route(
            "/api/v1/settings/rum",
            get(handlers::settings::get_rum_setting).put(handlers::settings::set_rum_setting),
        )
        .route(
            "/api/v1/settings/cloudwatch",
            get(handlers::settings::get_cloudwatch_setting).put(handlers::settings::set_cloudwatch_setting),
        )
        .route(
            "/api/v1/settings/kubernetes-logging",
            get(handlers::kubernetes_access::get_kubernetes_logging_settings)
                .put(handlers::kubernetes_access::set_kubernetes_logging_settings)
                .delete(handlers::kubernetes_access::revoke_all_kubernetes_clients),
        )
        .route(
            "/api/v1/settings/kubernetes-logging/clients/{id}",
            delete(handlers::kubernetes_access::revoke_kubernetes_client),
        )
        .route(
            "/api/v1/settings/kubernetes-logging/roles",
            post(handlers::kubernetes_access::create_kubernetes_rbac_grant).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_KUBERNETES_RBAC_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/settings/kubernetes-logging/roles/{id}",
            put(handlers::kubernetes_access::update_kubernetes_rbac_grant)
                .delete(handlers::kubernetes_access::delete_kubernetes_rbac_grant)
                .layer(DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_KUBERNETES_RBAC_BODY_BYTES,
                )),
        )
        // API Keys (settings)
        .route(
            "/api/v1/api-keys",
            get(handlers::settings::list_api_keys).post(handlers::settings::create_api_key),
        )
        .route(
            "/api/v1/api-keys/{id}",
            delete(handlers::settings::delete_api_key),
        )
        // Custom skills (user-defined investigation playbooks)
        .route(
            "/api/v1/custom-skills",
            get(handlers::custom_skills::list_custom_skills)
                .post(handlers::custom_skills::create_custom_skill),
        )
        .route(
            "/api/v1/custom-skills/{id}",
            get(handlers::custom_skills::get_custom_skill)
                .put(handlers::custom_skills::update_custom_skill)
                .delete(handlers::custom_skills::delete_custom_skill),
        )
        // Tenants (multi-tenant isolation boundaries)
        .route(
            "/api/v1/tenants",
            get(handlers::tenants::list_tenants)
                .post(handlers::tenants::create_tenant),
        )
        .route(
            "/api/v1/tenants/{id}",
            delete(handlers::tenants::delete_tenant),
        )
        .route(
            "/api/v1/tenants/{id}/toggle",
            put(handlers::tenants::toggle_tenant),
        )
        .route(
            "/api/v1/tenants/{id}/auth",
            put(handlers::tenants::set_auth_required),
        )
        .route(
            "/api/v1/tenants/{id}/ingest-auth",
            put(handlers::tenants::set_ingest_auth_required),
        )
        // Per-tenant ingest signal enable/disable (logs / apm / metrics / rum)
        .route(
            "/api/v1/tenants/{id}/signals",
            get(handlers::tenants::get_tenant_signals)
                .put(handlers::tenants::set_tenant_signals),
        )
        // Global retention caps (default + per-signal maximums)
        .route(
            "/api/v1/retention/global",
            get(handlers::retention::get_global_retention)
                .put(handlers::retention::set_global_retention),
        )
        // Ingest buffer status (durable spool depth + backend)
        .route("/api/v1/ingest/buffer", get(handlers::ingest_buffer::buffer_status))
        // Metric firewall (ingest-time block / drop-label rules)
        .route(
            "/api/v1/metric-firewall",
            get(handlers::metric_firewall::list)
                .post(handlers::metric_firewall::create),
        )
        .route(
            "/api/v1/metric-firewall/{id}",
            put(handlers::metric_firewall::update)
                .delete(handlers::metric_firewall::delete),
        )
        // Tenant retention overrides
        .route(
            "/api/v1/tenants/{id}/retention",
            get(handlers::retention::get_tenant_retention)
                .put(handlers::retention::set_tenant_retention),
        )
        .route(
            "/api/v1/tenants/{id}/retention/{signal}",
            delete(handlers::retention::delete_tenant_retention),
        )
        // Users (user management)
        .route(
            "/api/v1/users",
            get(handlers::users::list_users)
                .post(handlers::users::create_user),
        )
        .route(
            "/api/v1/users/{id}",
            delete(handlers::users::delete_user),
        )
        .route(
            "/api/v1/users/{id}/password",
            put(handlers::users::change_password),
        )
        .route(
            "/api/v1/users/{id}/toggle",
            put(handlers::users::toggle_user),
        )
        // Groups (RBAC group management)
        .route(
            "/api/v1/groups",
            get(handlers::groups::list_groups)
                .post(handlers::groups::create_group),
        )
        .route(
            "/api/v1/groups/{id}",
            put(handlers::groups::update_group)
                .delete(handlers::groups::delete_group),
        )
        .route(
            "/api/v1/groups/{id}/tenants",
            put(handlers::groups::set_group_tenants),
        )
        // User group membership
        .route(
            "/api/v1/users/{user_id}/groups",
            get(handlers::groups::get_user_groups)
                .put(handlers::groups::set_user_groups),
        )
        // RUM (Real User Monitoring)
        .route("/api/v1/rum/ingest", post(handlers::rum::ingest))
        .route("/api/v1/rum/apps", get(handlers::rum::list_apps))
        .route("/api/v1/rum/query", post(handlers::rum::query_events))
        .route("/api/v1/rum/vitals", post(handlers::rum::vitals))
        .route("/api/v1/rum/pages", post(handlers::rum::pages))
        .route("/api/v1/rum/errors", post(handlers::rum::errors))
        .route("/api/v1/rum/sessions", post(handlers::rum::sessions))
        .route("/api/v1/rum/session/{id}", get(handlers::rum::session_detail))
        .route("/api/v1/rum/replay/ingest", post(handlers::rum::ingest_replay))
        .route("/api/v1/rum/replay/available/{app_name}", get(handlers::rum::list_replay_sessions))
        .route("/api/v1/rum/replay/{id}", get(handlers::rum::get_replay))
        // Browser CSP telemetry is intentionally public so login/SSO violations
        // can report before a session exists. Its body has a tighter route cap.
        .route(
            "/api/v1/security/csp-report",
            post(handlers::csp_reports::ingest_csp_report).layer(
                DefaultBodyLimit::max(handlers::csp_reports::MAX_CSP_REPORT_BYTES),
            ),
        )
        // ArgoCD integration
        .route("/api/v1/argocd/applications", get(handlers::argocd::list_applications))
        .route("/api/v1/argocd/applications/{name}", get(handlers::argocd::get_application))
        .route("/api/v1/argocd/applicationsets", get(handlers::argocd::list_applicationsets))
        .route("/api/v1/fluxcd/resources", get(handlers::fluxcd::list_resources))
        .route("/api/v1/fluxcd/sources", get(handlers::fluxcd::list_sources))
        .route("/api/v1/fluxcd/resources/{kind}/{name}", get(handlers::fluxcd::get_resource))
        .route("/api/v1/kubernetes/summary", get(handlers::kubernetes::summary))
        .route("/api/v1/kubernetes/namespaces", get(handlers::kubernetes::list_namespaces))
        .route("/api/v1/kubernetes/resources/{kind}", get(handlers::kubernetes::list_resources))
        .route("/api/v1/kubernetes/resources/{kind}/{namespace}/{name}", get(handlers::kubernetes::get_resource))
        .route(
            "/api/v1/kubernetes/login/start",
            post(handlers::kubernetes_access::start_kubernetes_login).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_KUBERNETES_LOGIN_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/kubernetes/login/approve",
            post(handlers::kubernetes_access::approve_kubernetes_login).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_KUBERNETES_LOGIN_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/kubernetes/login/details",
            post(handlers::kubernetes_access::get_kubernetes_login_details).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_KUBERNETES_LOGIN_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/kubernetes/login/token",
            post(handlers::kubernetes_access::poll_kubernetes_login).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_KUBERNETES_LOGIN_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/kubernetes/access-events/ingest",
            post(handlers::kubernetes_access::ingest_access_event).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_ACCESS_EVENT_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/kubernetes/gateway/authorize",
            post(handlers::kubernetes_access::authorize_gateway_request).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_GATEWAY_AUTHORIZE_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/kubernetes/gateway/ready",
            post(handlers::kubernetes_access::gateway_recording_ready).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_GATEWAY_AUTHORIZE_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/kubernetes/gateway/rbac",
            get(handlers::kubernetes_access::list_gateway_kubernetes_rbac_grants),
        )
        .route(
            "/api/v1/kubernetes/gateway/rbac/reconcile",
            post(handlers::kubernetes_access::record_gateway_kubernetes_rbac_reconcile).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_GATEWAY_AUTHORIZE_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/kubernetes/access-events/client",
            post(handlers::kubernetes_access::ingest_client_event).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_CLIENT_ENRICHMENT_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/kubernetes/session-chunks/ingest",
            post(handlers::kubernetes_access::ingest_session_chunk).layer(
                DefaultBodyLimit::max(
                    handlers::kubernetes_access::MAX_SESSION_CHUNK_BODY_BYTES,
                ),
            ),
        )
        .route(
            "/api/v1/kubernetes/access-events",
            get(handlers::kubernetes_access::list_access_events),
        )
        .route(
            "/api/v1/kubernetes/access-events/{id}",
            get(handlers::kubernetes_access::get_access_event),
        )
        .route(
            "/api/v1/kubernetes/sessions/{id}/chunks",
            get(handlers::kubernetes_access::get_session_chunks),
        )
        .route(
            "/api/v1/kubernetes/access-events/export",
            post(handlers::kubernetes_access::export_access_events),
        )
        // Stats
        .route("/api/v1/stats", post(handlers::stats::get_stats))
        .route("/api/v1/stats/partitions", axum::routing::get(handlers::stats::get_storage_partitions))
        // Signal usage
        .route("/api/v1/usage", get(handlers::usage::get_usage))
        .route("/api/v1/usage/cardinality/{metric}", get(handlers::usage::get_label_breakdown))
        // Usage metering (per-tenant ingest volume)
        .route("/api/v1/usage/summary", get(handlers::usage_metering::usage_summary))
        .route("/api/v1/usage/breakdown", get(handlers::usage_metering::usage_breakdown))
        .route("/api/v1/usage/tenants", get(handlers::usage_metering::usage_tenants))
        // ═══ Datadog Agent Ingestion ═══
        // Logs (agent log forwarder sends to {logs_dd_url}/api/v2/logs)
        .route("/datadog/v1/input", post(handlers::dd_logs::ingest_logs))
        .route("/api/v2/logs", post(handlers::dd_logs::ingest_logs))
        .route("/api/v2/logs/t/{tenant}", post(handlers::dd_logs::ingest_logs_with_tenant))
        // Metrics
        .route("/datadog/api/v1/series", post(handlers::dd_metrics::ingest_v1))
        .route("/datadog/api/v2/series", post(handlers::dd_metrics::ingest_v2))
        .route("/datadog/api/v1/check_run", post(handlers::dd_metrics::check_run))
        // Traces (dd-trace libs use PUT, dd-agent trace writer uses POST)
        .route("/datadog/api/v0.2/traces", any(handlers::dd_traces::ingest_agent))
        .route("/datadog/v0.3/traces", any(handlers::dd_traces::ingest_v03))
        .route("/datadog/v0.4/traces", any(handlers::dd_traces::ingest_v04))
        // ═══ OTLP/HTTP Ingest (OTel Collector) ═══
        .route("/v1/traces",  post(handlers::otlp::ingest_otlp_traces))
        .route("/v1/logs",    post(handlers::otlp::ingest_otlp_logs))
        .route("/v1/metrics", post(handlers::otlp::ingest_otlp_metrics))
        // Vector JSON logs
        .route("/api/v1/ingest/logs", post(handlers::otlp::ingest_vector_logs))
        // ═══ AWS CloudWatch Logs Ingestion (Kinesis Data Firehose HTTP endpoint) ═══
        // Tenant comes from the URL path and must match the scoped ingest key
        // supplied in X-Amz-Firehose-Access-Key or Authorization: Bearer.
        .route("/cloudwatch/firehose/t/{tenant}", post(handlers::cloudwatch::ingest_firehose_with_tenant))
        // Trace stats from agent trace writer
        .route("/datadog/api/v0.6/stats", any(handlers::dd_common::stub_ok))
        .route("/datadog/api/v0.2/stats", any(handlers::dd_common::stub_ok))
        // Validate & metadata stubs
        .route("/datadog/api/v1/validate", post(handlers::dd_common::validate))
        .route("/datadog/api/v1/metadata", any(handlers::dd_common::stub_ok))
        .route("/datadog/api/v2/host_metadata", any(handlers::dd_common::stub_ok))
        .route("/datadog/api/v2/events", any(handlers::dd_common::stub_ok))
        .route("/datadog/api/v1/collector", any(handlers::dd_common::stub_ok))
        .route("/datadog/intake/", any(handlers::dd_common::stub_ok))
        .route("/datadog/intake", any(handlers::dd_common::stub_ok))
        // SSO login flow (OIDC + SAML)
        .route("/auth/sso/login", get(handlers::sso::sso_login))
        .route("/auth/sso/callback", get(handlers::sso::sso_callback))
        .route("/auth/sso/acs", post(handlers::sso::sso_acs))
        .route("/auth/sso/metadata", get(handlers::sso::sso_metadata))
        // SSO config admin endpoints
        .route(
            "/api/v1/sso/providers",
            get(handlers::sso::list_sso_providers).post(handlers::sso::save_sso_provider),
        )
        .route(
            "/api/v1/sso/providers/{id}",
            delete(handlers::sso::delete_sso_provider),
        )
        .route(
            "/api/v1/sso/mappings",
            get(handlers::sso::list_idp_group_mappings).post(handlers::sso::create_idp_group_mapping),
        )
        .route(
            "/api/v1/sso/mappings/{id}",
            put(handlers::sso::update_idp_group_mapping).delete(handlers::sso::delete_idp_group_mapping),
        )
        .route("/api/v1/sso/status", get(handlers::sso::sso_status))
        .route(
            "/api/v1/sso/setup-token",
            post(handlers::sso::create_setup_token),
        )
        .route(
            "/api/v1/sso/setup-token/exchange",
            post(handlers::sso::exchange_setup_token),
        )
        .route(
            "/api/v1/sso/setup-session",
            get(handlers::sso::validate_setup_session),
        )
        .route(
            "/api/v1/sso/setup-session/complete",
            post(handlers::sso::complete_setup_session),
        )
        // Auth
        .route("/api/v1/auth/login", post(handlers::auth::login))
        .route("/api/v1/auth/logout", post(handlers::auth::logout))
        .route("/api/v1/auth/me", get(handlers::auth::me))
        .route(
            "/api/v1/auth/sessions",
            get(handlers::auth::list_sessions),
        )
        .route(
            "/api/v1/auth/sessions/{id}",
            delete(handlers::auth::revoke_session),
        )
        .route(
            "/api/v1/auth/admin/sessions",
            get(handlers::auth::list_all_sessions),
        )
        .route(
            "/api/v1/auth/admin/sessions/{id}",
            delete(handlers::auth::admin_revoke_session),
        )
        // Tamper-evident audit log (admin only)
        .route("/api/v1/audit", get(handlers::audit::list_audit))
        .route("/api/v1/audit/verify", get(handlers::audit::verify_audit))
        // Health
        .route("/healthz", get(handlers::health::healthz))
        .route("/readyz", get(handlers::health::readyz))
        .route("/shutdown", post(handlers::shutdown::shutdown))
        // System-health self-metrics (Prometheus exposition). OPEN — no auth, like /healthz.
        .route("/metrics", get(metrics_handler))
        // Catch-all for unmatched DD agent paths (debug logging)
        .fallback(|req: axum::http::Request<axum::body::Body>| async move {
            tracing::warn!(
                method = %req.method(),
                path = request_log_path(req.uri()),
                content_type = req.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("none"),
                "unmatched request"
            );
            (axum::http::StatusCode::NOT_FOUND, "not found")
        })
        // The outer tenant middleware resolves credentials and rewrites
        // `/t/{tenant}` before routing. Enforce the resulting tenant policy here
        // so the CORS/security/metrics layers below still wrap rejected requests.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            query_policy_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            enforce_tenant_auth_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            csrf_protection_middleware,
        ))
        .layer(state.cors_policy.layer())
        .layer(CompressionLayer::new())
        .layer(axum::middleware::from_fn(security_headers_middleware))
        // API RED self-metrics (rush_http_*). Applied as a router layer so the
        // MatchedPath (templated route) is populated by routing before it runs.
        .layer(axum::middleware::from_fn_with_state(state.clone(), http_metrics_middleware))
        .layer(TraceLayer::new_for_http().make_span_with(
            |request: &axum::http::Request<axum::body::Body>| {
                tracing::info_span!(
                    "http.request",
                    method = %request.method(),
                    path = request_log_path(request.uri()),
                    version = ?request.version(),
                )
            },
        ))
        // Override axum's fixed 2 MiB default with the startup-validated compressed
        // ingest ceiling. Handlers still check the actual body and record the
        // protocol/reason-specific rejection metric before decoding.
        .layer(DefaultBodyLimit::max(ingest_limits.max_compressed_bytes))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            ingest_content_length_limit_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            shutdown_gate_middleware,
        ))
        // Keep a writer handle for the graceful-shutdown flush before `state` is
        // consumed by `with_state`.
        .with_state(state.clone());

    // Wrap the whole router so tenant resolution runs BEFORE routing. This matters
    // for the `/t/{tenant}/…` URL prefix: the middleware strips it and rewrites the
    // path, and routing must then run on the rewritten path. `Router::layer` runs
    // *after* routing, so applying tenant_middleware there can't affect the match.
    let app = Router::new()
        .fallback_service(inner)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            tenant_middleware,
        ))
        .layer(axum::middleware::from_fn(
            rush_api::api_error::public_error_middleware,
        ));
    let shutdown_writer = state.writer.clone();

    let port: u16 = std::env::var("RUSH_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    // FINDING-13: Warn when ClickHouse row policies are not active.
    // Without row policies, tenant isolation is enforced only at the API layer.
    // Configure `custom_settings_prefixes = 'rush_'` in ClickHouse for DB-layer isolation.
    if !rush_api::row_policy_supported() {
        tracing::warn!(
            row_policies = rush_api::row_policy_supported(),
            "ClickHouse row-level security policies are NOT active. \
             Tenant isolation relies solely on API-layer WHERE injection. \
             Set custom_settings_prefixes = 'rush_' in ClickHouse config to enable row policies."
        );
    }
    rush_api::handlers::sso::validate_base_url_config()
        .map_err(|error| anyhow::anyhow!("invalid canonical URL configuration: {error}"))?;

    // R01: Warn when RUSH_API_KEY_SECRET is unset or too short.
    // An empty or short secret means HMAC-SHA256 provides no real keyed-hash protection.
    match std::env::var("RUSH_API_KEY_SECRET") {
        Ok(s) if s.len() >= 32 => {}
        Ok(s) if s.is_empty() => tracing::warn!(
            "RUSH_API_KEY_SECRET is not set. API key hashes are stored with an empty HMAC key. \
             Set RUSH_API_KEY_SECRET to a random 32+ character secret before deployment."
        ),
        Ok(_) => tracing::warn!(
            "RUSH_API_KEY_SECRET is shorter than 32 characters. \
             Use a random secret of at least 32 characters for adequate HMAC security."
        ),
        Err(_) => tracing::warn!(
            "RUSH_API_KEY_SECRET is not set. API key hashes are stored with an empty HMAC key. \
             Set RUSH_API_KEY_SECRET to a random 32+ character secret before deployment."
        ),
    }

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        port = port,
        clickhouse_url = %clickhouse_url,
        row_policies = rush_api::row_policy_supported(),
        "rush-api started"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    // Graceful shutdown: on SIGINT/SIGTERM, stop accepting new connections, let
    // in-flight requests finish, then flush any buffered ingest rows so the
    // cross-request batcher never drops in-memory rows on a clean shutdown.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(shutdown_controller.clone()))
    .await?;
    graceful_shutdown_drain(shutdown_writer, shutdown_controller, replayer_handle).await;

    Ok(())
}

/// Resolve when a shutdown signal (Ctrl-C / SIGTERM) is received.
async fn shutdown_signal(shutdown: rush_api::shutdown::ShutdownController) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = shutdown.wait_for_request() => {},
    }
    shutdown.request();
}

/// Stop admitting work, flush in-memory batches, then keep retrying the durable
/// spool until both layers are empty. If ClickHouse stays unavailable, this
/// intentionally waits until Kubernetes' termination grace period expires
/// instead of claiming a clean shutdown and losing queued telemetry.
async fn graceful_shutdown_drain(
    writer: ChWriter,
    shutdown: rush_api::shutdown::ShutdownController,
    replayer_handle: Option<tokio::task::JoinHandle<()>>,
) {
    tracing::info!("graceful shutdown: flushing buffered ingest batches");
    writer.flush_all().await;
    shutdown.begin_drain();
    let replayer_handle =
        replayer_handle.unwrap_or_else(|| writer.clone().spawn_replayer(shutdown.clone()));

    loop {
        if writer.has_pending_batches().await {
            writer.flush_all().await;
        }
        if !writer.has_pending_batches().await
            && writer.spool_segments() == 0
            && writer.spool_bytes() == 0
        {
            shutdown.finish();
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    if let Err(error) = replayer_handle.await {
        tracing::warn!(%error, "graceful shutdown: replayer task ended with an error");
    }
    tracing::info!("graceful shutdown: durable ingest queue is empty");
}

#[cfg(test)]
mod tenant_auth_tests {
    use super::{
        CredentialKind, TenantResolution, allows_unauthenticated_tenant_request,
        credential_route_denial, effective_route_ingest_auth_required, explicit_ingest_tenant,
        ingest_signal_for_route, is_explain_collector_route, is_state_changing_method,
        query_workload_for_request, request_log_path, request_origin_allowed_with_policy,
        requires_csrf_origin, should_reject_for_tenant_auth, should_reject_interactive_llm,
        trust_forwarded_origin_headers, validate_request_time_range,
    };
    use axum::body::Body;
    use axum::http::{HeaderMap, HeaderValue, Method, Request, header};
    use rush_api::clickhouse_config::ApiKeyGrant;
    use rush_api::cors::CorsPolicy;
    use std::net::{IpAddr, Ipv4Addr};

    const INGEST_ROUTES: &[(&str, &str)] = &[
        ("/v1/logs", "logs"),
        ("/api/v1/ingest/logs", "logs"),
        ("/datadog/v1/input", "logs"),
        ("/api/v2/logs", "logs"),
        ("/api/v2/logs/t/acme", "logs"),
        ("/cloudwatch/firehose/t/acme", "logs"),
        ("/v1/traces", "traces"),
        ("/datadog/api/v0.2/traces", "traces"),
        ("/datadog/v0.3/traces", "traces"),
        ("/datadog/v0.4/traces", "traces"),
        ("/v1/metrics", "metrics"),
        ("/prom/api/v1/write", "metrics"),
        ("/datadog/api/v1/series", "metrics"),
        ("/datadog/api/v2/series", "metrics"),
        ("/datadog/api/v1/check_run", "metrics"),
        ("/api/v1/rum/ingest", "rum"),
        ("/api/v1/rum/replay/ingest", "rum"),
        ("/datadog/api/v0.6/stats", "control"),
        ("/datadog/api/v0.2/stats", "control"),
        ("/datadog/api/v1/validate", "control"),
        ("/datadog/api/v1/metadata", "control"),
        ("/datadog/api/v2/host_metadata", "control"),
        ("/datadog/api/v2/events", "control"),
        ("/datadog/api/v1/collector", "control"),
        ("/datadog/intake/", "control"),
        ("/datadog/intake", "control"),
    ];

    #[test]
    fn request_logs_exclude_query_parameters() {
        let uri: axum::http::Uri = "/auth/sso/callback?code=secret&state=private"
            .parse()
            .unwrap();
        assert_eq!(request_log_path(&uri), "/auth/sso/callback");
        assert!(!request_log_path(&uri).contains("secret"));
    }

    #[test]
    fn explain_collector_routes_are_exact_and_method_scoped() {
        for (method, path) in [
            (Method::GET, "/api/v1/integrations/postgres/explain/poll"),
            (Method::GET, "/api/v1/integrations/mysql/explain/poll"),
            (
                Method::POST,
                "/api/v1/integrations/postgres/explain/job-123/result",
            ),
            (
                Method::POST,
                "/api/v1/integrations/mysql/explain/job-123/result",
            ),
        ] {
            assert!(is_explain_collector_route(&method, path), "{method} {path}");
            assert_eq!(ingest_signal_for_route(&method, path), Some("collector"));
        }

        for (method, path) in [
            (Method::POST, "/api/v1/integrations/postgres/explain"),
            (Method::GET, "/api/v1/integrations/postgres/explain/job-123"),
            (Method::POST, "/api/v1/integrations/postgres/explain/poll"),
            (
                Method::GET,
                "/api/v1/integrations/postgres/explain/job-123/result",
            ),
            (
                Method::POST,
                "/api/v1/integrations/postgres/explain/a/b/result",
            ),
        ] {
            assert!(
                !is_explain_collector_route(&method, path),
                "{method} {path}"
            );
            assert_eq!(ingest_signal_for_route(&method, path), None);
        }
    }

    #[test]
    fn collector_control_routes_ignore_open_ingest_compatibility() {
        assert!(effective_route_ingest_auth_required(
            Some("collector"),
            false,
            true,
        ));
        assert!(!effective_route_ingest_auth_required(
            Some("metrics"),
            false,
            true,
        ));
        assert!(effective_route_ingest_auth_required(
            Some("metrics"),
            true,
            false,
        ));
    }

    #[test]
    fn workload_policy_classifies_queries_and_preserves_explicit_bypasses() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/query")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            query_workload_for_request(&request),
            Some(rush_api::query_governor::WorkloadClass::Interactive)
        );

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/query/timeseries")
            .header("x-rush-workload", "dashboard")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            query_workload_for_request(&request),
            Some(rush_api::query_governor::WorkloadClass::Dashboard)
        );

        let export = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/logs/export")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            query_workload_for_request(&export),
            Some(rush_api::query_governor::WorkloadClass::Export)
        );
        let status = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/exports/job-id")
            .body(Body::empty())
            .unwrap();
        assert_eq!(query_workload_for_request(&status), None);
        let download = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/exports/job-id/download")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            query_workload_for_request(&download),
            Some(rush_api::query_governor::WorkloadClass::Export)
        );

        for (method, path) in [
            (Method::POST, "/v1/traces"),
            (Method::GET, "/healthz"),
            (Method::POST, "/shutdown"),
            (Method::POST, "/api/v1/investigate"),
        ] {
            let request = Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap();
            assert_eq!(query_workload_for_request(&request), None, "{path}");
        }
    }

    #[tokio::test]
    async fn workload_time_validation_rejects_partial_and_oversized_ranges() {
        let mut partial = Request::builder()
            .method(Method::GET)
            .uri("/prom/api/v1/query_range?start=100")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            validate_request_time_range(&mut partial, 60)
                .await
                .unwrap_err()
                .status(),
            axum::http::StatusCode::BAD_REQUEST
        );

        let mut oversized = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"time_range":{"from":"2026-08-10T00:00:00Z","to":"2026-08-10T00:02:00Z"}}"#,
            ))
            .unwrap();
        assert_eq!(
            validate_request_time_range(&mut oversized, 60)
                .await
                .unwrap_err()
                .status(),
            axum::http::StatusCode::BAD_REQUEST
        );

        let mut wrong_shape = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/query")
            .body(Body::from(r#"{"time_range":"last hour"}"#))
            .unwrap();
        assert_eq!(
            validate_request_time_range(&mut wrong_shape, 60)
                .await
                .unwrap_err()
                .status(),
            axum::http::StatusCode::BAD_REQUEST
        );

        let mut invalid_json = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/query")
            .body(Body::from("{"))
            .unwrap();
        assert!(
            validate_request_time_range(&mut invalid_json, 60)
                .await
                .is_ok()
        );
    }

    #[test]
    fn locked_tenant_rejects_unauthenticated_data_requests() {
        assert!(should_reject_for_tenant_auth(
            &Method::POST,
            "/api/v1/query",
            true,
            false,
        ));
        assert!(should_reject_for_tenant_auth(
            &Method::POST,
            "/v1/traces",
            true,
            false,
        ));
        assert!(should_reject_for_tenant_auth(
            &Method::POST,
            "/api/v1/explore/search",
            true,
            false,
        ));
    }

    #[test]
    fn open_tenant_preserves_backwards_compatible_access() {
        assert!(!should_reject_for_tenant_auth(
            &Method::POST,
            "/api/v1/query",
            false,
            false,
        ));
    }

    #[test]
    fn csrf_only_classifies_mutating_methods_as_state_changing() {
        assert!(is_state_changing_method(&Method::POST));
        assert!(is_state_changing_method(&Method::PUT));
        assert!(is_state_changing_method(&Method::PATCH));
        assert!(is_state_changing_method(&Method::DELETE));
        assert!(!is_state_changing_method(&Method::GET));
        assert!(!is_state_changing_method(&Method::OPTIONS));
    }

    #[test]
    fn csrf_covers_login_and_every_setup_session_mutation() {
        for path in [
            "/api/v1/auth/login",
            "/api/v1/sso/setup-token/exchange",
            "/api/v1/sso/setup-session/complete",
            "/api/v1/sso/providers",
        ] {
            assert!(requires_csrf_origin(
                &Method::POST,
                path,
                Some(&CredentialKind::Anonymous),
            ));
        }
        assert!(requires_csrf_origin(
            &Method::DELETE,
            "/api/v1/users/user-1",
            Some(&CredentialKind::Session),
        ));
        assert!(!requires_csrf_origin(
            &Method::POST,
            "/auth/sso/acs",
            Some(&CredentialKind::Anonymous),
        ));
        assert!(!requires_csrf_origin(
            &Method::GET,
            "/api/v1/sso/setup-session",
            Some(&CredentialKind::Anonymous),
        ));
    }

    #[test]
    fn csrf_accepts_exactly_configured_frontend_origins() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://localhost:5173"),
        );
        headers.insert(header::HOST, HeaderValue::from_static("localhost:8080"));

        assert!(!request_origin_allowed_with_policy(
            &headers, false, None, None, false,
        ));
        let policy = CorsPolicy::parse(Some("http://localhost:5173")).unwrap();
        assert!(request_origin_allowed_with_policy(
            &headers,
            false,
            None,
            Some(&policy),
            false,
        ));
    }

    #[test]
    fn csrf_development_allows_local_ui_beside_a_public_sso_base_url() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://localhost:5173"),
        );
        headers.insert(header::HOST, HeaderValue::from_static("localhost:8080"));
        let policy = CorsPolicy::parse(Some("http://localhost:5173")).unwrap();

        assert!(request_origin_allowed_with_policy(
            &headers,
            false,
            Some("https://rush-dev.example.com"),
            Some(&policy),
            false,
        ));
    }

    #[test]
    fn csrf_production_uses_only_the_canonical_origin() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("internal:8080"));
        let attacker_policy = CorsPolicy::parse(Some("https://attacker.example.com")).unwrap();
        headers.insert(
            header::HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("attacker.example.com"),
        );
        headers.insert(
            header::HeaderName::from_static("x-forwarded-proto"),
            HeaderValue::from_static("https"),
        );

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://rush.example.com"),
        );
        assert!(request_origin_allowed_with_policy(
            &headers,
            true,
            Some("https://rush.example.com"),
            Some(&attacker_policy),
            true,
        ));

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://attacker.example.com"),
        );
        assert!(!request_origin_allowed_with_policy(
            &headers,
            true,
            Some("https://rush.example.com"),
            Some(&attacker_policy),
            true,
        ));
        assert!(!request_origin_allowed_with_policy(
            &headers, true, None, None, false,
        ));
    }

    #[test]
    fn csrf_rejects_null_and_untrusted_forwarded_origins() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("rush.internal:8080"));

        headers.insert(header::ORIGIN, HeaderValue::from_static("null"));
        assert!(!request_origin_allowed_with_policy(
            &headers, false, None, None, false,
        ));

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://attacker.example.com"),
        );
        headers.insert(
            header::HeaderName::from_static("x-forwarded-proto"),
            HeaderValue::from_static("https"),
        );
        headers.insert(
            header::HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("attacker.example.com"),
        );
        assert!(!request_origin_allowed_with_policy(
            &headers, false, None, None, false,
        ));

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://edge.example.com"),
        );
        headers.insert(
            header::HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("edge.example.com"),
        );
        assert!(request_origin_allowed_with_policy(
            &headers, false, None, None, true,
        ));
    }

    #[test]
    fn csrf_trusts_forwarded_origin_only_from_configured_development_proxy() {
        let trusted = vec!["10.42.0.0/16".to_string()];
        let trusted_peer = Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 9)));
        let untrusted_peer = Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)));

        assert!(trust_forwarded_origin_headers(
            false,
            true,
            trusted_peer,
            &trusted,
        ));
        assert!(!trust_forwarded_origin_headers(
            false,
            true,
            untrusted_peer,
            &trusted,
        ));
        assert!(!trust_forwarded_origin_headers(
            true,
            true,
            trusted_peer,
            &trusted,
        ));
        assert!(!trust_forwarded_origin_headers(
            false,
            false,
            trusted_peer,
            &trusted,
        ));
    }

    #[test]
    fn valid_credentials_unlock_a_locked_tenant() {
        assert!(!should_reject_for_tenant_auth(
            &Method::POST,
            "/api/v1/query",
            true,
            true,
        ));
    }

    #[test]
    fn bootstrap_and_operational_routes_remain_public() {
        for (method, path) in [
            (Method::POST, "/api/v1/auth/login"),
            (Method::POST, "/api/v1/auth/logout"),
            (Method::GET, "/api/v1/sso/status"),
            (Method::GET, "/auth/sso/login"),
            (Method::GET, "/auth/sso/callback"),
            (Method::POST, "/auth/sso/acs"),
            (Method::GET, "/auth/sso/metadata"),
            (Method::GET, "/api/v1/sso/setup-token/example/validate"),
            (Method::POST, "/api/v1/sso/setup-token/exchange"),
            (Method::GET, "/api/v1/sso/setup-session"),
            (Method::POST, "/api/v1/sso/setup-session/complete"),
            (Method::POST, "/api/v1/sso/providers"),
            (Method::GET, "/healthz"),
            (Method::GET, "/readyz"),
            (Method::GET, "/metrics"),
            (Method::POST, "/api/v1/security/csp-report"),
            (Method::OPTIONS, "/api/v1/query"),
        ] {
            assert!(
                allows_unauthenticated_tenant_request(&method, path),
                "expected {method} {path} to remain public"
            );
            assert!(!should_reject_for_tenant_auth(&method, path, true, false,));
        }
    }

    #[test]
    fn authenticated_only_routes_are_not_accidentally_exempted() {
        for path in [
            "/api/v1/auth/me",
            "/api/v1/sso/providers",
            "/api/v1/sso/setup-token",
            "/api/v1/sso/setup-token/example/complete",
            "/api/v1/sso/setup-token/example/complete/validate",
            "/api/v1/audit",
            "/api/v1/parse-query",
            "/api/v1/parse-promql",
        ] {
            assert!(!allows_unauthenticated_tenant_request(&Method::GET, path));
        }
    }

    #[test]
    fn open_query_tenants_still_require_interactive_auth_for_llm_parsing() {
        for path in ["/api/v1/parse-query", "/api/v1/parse-promql"] {
            assert!(should_reject_interactive_llm(
                &Method::POST,
                path,
                &CredentialKind::Anonymous,
            ));
            assert!(should_reject_interactive_llm(
                &Method::POST,
                path,
                &CredentialKind::QueryKey,
            ));
            assert!(!should_reject_interactive_llm(
                &Method::POST,
                path,
                &CredentialKind::Session,
            ));
        }
    }

    #[test]
    fn every_ingest_family_is_classified_by_signal() {
        for &(path, signal) in INGEST_ROUTES {
            assert_eq!(
                ingest_signal_for_route(&Method::POST, path),
                Some(signal),
                "incorrect ingest signal for {path}"
            );
        }
        assert_eq!(
            ingest_signal_for_route(&Method::POST, "/api/v1/query"),
            None
        );
        assert_eq!(
            ingest_signal_for_route(&Method::GET, "/v1/logs"),
            Some("logs")
        );
        assert_eq!(ingest_signal_for_route(&Method::OPTIONS, "/v1/logs"), None);
    }

    #[test]
    fn tenant_in_ingest_path_is_explicit_and_unambiguous() {
        assert_eq!(
            explicit_ingest_tenant("/api/v2/logs/t/acme"),
            Some("acme".to_string())
        );
        assert_eq!(
            explicit_ingest_tenant("/cloudwatch/firehose/t/acme"),
            Some("acme".to_string())
        );
        assert_eq!(explicit_ingest_tenant("/api/v2/logs/t/acme/extra"), None);
    }

    #[test]
    fn only_explicit_ingest_keys_receive_ingest_credentials() {
        let grant = |key_type: &str| ApiKeyGrant {
            id: "key-id".to_string(),
            tenant_id: "default".to_string(),
            key_type: key_type.to_string(),
            signals: vec!["logs".to_string()],
            rate_limit_per_minute: 100,
            source_cidrs: Vec::new(),
        };
        assert_eq!(
            TenantResolution::api_key(grant("ingest")).credential,
            CredentialKind::IngestKey
        );
        assert_eq!(
            TenantResolution::api_key(grant("query")).credential,
            CredentialKind::QueryKey
        );
        assert_eq!(
            TenantResolution::api_key(grant("legacy")).credential,
            CredentialKind::QueryKey
        );
    }

    #[test]
    fn auth_required_ingestion_rejects_anonymous_and_query_credentials() {
        for &(path, _) in INGEST_ROUTES {
            assert!(ingest_signal_for_route(&Method::POST, path).is_some());
            for credential in [
                CredentialKind::Anonymous,
                CredentialKind::Session,
                CredentialKind::QueryKey,
            ] {
                assert_eq!(
                    credential_route_denial(&credential, true, true),
                    Some("ingest_key_required"),
                    "{credential:?} unexpectedly authorized for locked ingest route {path}"
                );
            }
            assert_eq!(
                credential_route_denial(&CredentialKind::IngestKey, true, true),
                None,
                "ingest key rejected for locked ingest route {path}"
            );
        }
    }

    #[test]
    fn no_auth_ingestion_accepts_anonymous_across_every_ingest_family() {
        for &(path, _) in INGEST_ROUTES {
            assert!(ingest_signal_for_route(&Method::POST, path).is_some());
            assert_eq!(
                credential_route_denial(&CredentialKind::Anonymous, true, false),
                None,
                "anonymous request rejected for open ingest route {path}"
            );
        }
    }

    #[test]
    fn no_auth_ingestion_does_not_grant_ingest_scope_to_other_credentials() {
        for credential in [CredentialKind::Session, CredentialKind::QueryKey] {
            assert_eq!(
                credential_route_denial(&credential, true, false),
                Some("ingest_key_required")
            );
        }
        assert_eq!(
            credential_route_denial(&CredentialKind::IngestKey, true, false),
            None
        );
        assert_eq!(
            credential_route_denial(&CredentialKind::IngestKey, false, false),
            Some("query_not_allowed")
        );
    }

    #[test]
    fn open_ingestion_does_not_open_query_access() {
        assert_eq!(
            credential_route_denial(&CredentialKind::Anonymous, true, false),
            None
        );
        assert!(should_reject_for_tenant_auth(
            &Method::POST,
            "/api/v1/query",
            true,
            false,
        ));
    }

    #[test]
    fn exports_follow_the_same_open_or_locked_query_boundary() {
        for path in ["/api/v1/query/export", "/api/v1/logs/export"] {
            assert!(!should_reject_for_tenant_auth(
                &Method::POST,
                path,
                false,
                false,
            ));
            assert!(should_reject_for_tenant_auth(
                &Method::POST,
                path,
                true,
                false,
            ));
            assert!(!should_reject_for_tenant_auth(
                &Method::POST,
                path,
                true,
                true,
            ));
            assert_eq!(
                credential_route_denial(&CredentialKind::IngestKey, false, false),
                Some("query_not_allowed")
            );
        }
    }

    #[test]
    fn api_key_revocation_has_no_process_local_cache_grace_period() {
        let source = include_str!("main.rs");
        let resolver = source
            .split_once("async fn resolve_api_key_credential")
            .expect("API key resolver must exist")
            .1
            .split("async fn resolve_tenant_inner")
            .next()
            .expect("tenant resolver must follow API key resolver");
        assert!(resolver.contains("config_db.resolve_api_key"));
        assert!(!resolver.contains("api_key_cache"));
        assert!(!include_str!("lib.rs").contains("pub api_key_cache"));
    }

    #[test]
    fn sensitive_administrative_reads_keep_explicit_admin_guards() {
        for (name, source) in [
            ("audit", include_str!("handlers/audit.rs")),
            (
                "SSO provider configuration",
                include_str!("handlers/sso.rs"),
            ),
            ("tenant administration", include_str!("handlers/tenants.rs")),
            (
                "API key administration",
                include_str!("handlers/settings.rs"),
            ),
        ] {
            assert!(
                source.contains("require_admin"),
                "{name} lost its explicit admin authorization guard"
            );
        }
    }

    #[test]
    fn kubernetes_gateway_routes_keep_their_distinct_auth_boundaries() {
        assert!(allows_unauthenticated_tenant_request(
            &Method::POST,
            "/api/v1/kubernetes/access-events/ingest",
        ));
        assert!(allows_unauthenticated_tenant_request(
            &Method::POST,
            "/api/v1/kubernetes/session-chunks/ingest",
        ));
        assert!(allows_unauthenticated_tenant_request(
            &Method::POST,
            "/api/v1/kubernetes/gateway/ready",
        ));
        assert!(allows_unauthenticated_tenant_request(
            &Method::POST,
            "/api/v1/kubernetes/gateway/authorize",
        ));
        assert!(allows_unauthenticated_tenant_request(
            &Method::GET,
            "/api/v1/kubernetes/gateway/rbac",
        ));
        assert!(allows_unauthenticated_tenant_request(
            &Method::POST,
            "/api/v1/kubernetes/gateway/rbac/reconcile",
        ));
        assert!(allows_unauthenticated_tenant_request(
            &Method::POST,
            "/api/v1/kubernetes/login/start",
        ));
        assert!(allows_unauthenticated_tenant_request(
            &Method::POST,
            "/api/v1/kubernetes/login/token",
        ));
        assert!(!allows_unauthenticated_tenant_request(
            &Method::POST,
            "/api/v1/kubernetes/login/approve",
        ));
        assert!(allows_unauthenticated_tenant_request(
            &Method::POST,
            "/api/v1/kubernetes/access-events/client",
        ));
        assert!(!requires_csrf_origin(
            &Method::POST,
            "/api/v1/kubernetes/gateway/authorize",
            Some(&CredentialKind::Session),
        ));
        assert!(!requires_csrf_origin(
            &Method::POST,
            "/api/v1/kubernetes/gateway/ready",
            None,
        ));
    }

    #[test]
    fn kubernetes_recording_routes_are_registered_with_body_limits() {
        let source = include_str!("main.rs");
        for route in [
            "/api/v1/settings/kubernetes-logging",
            "/api/v1/settings/kubernetes-logging/clients/{id}",
            "/api/v1/settings/kubernetes-logging/roles",
            "/api/v1/settings/kubernetes-logging/roles/{id}",
            "/api/v1/kubernetes/login/start",
            "/api/v1/kubernetes/login/approve",
            "/api/v1/kubernetes/login/details",
            "/api/v1/kubernetes/login/token",
            "/api/v1/kubernetes/gateway/authorize",
            "/api/v1/kubernetes/gateway/ready",
            "/api/v1/kubernetes/gateway/rbac",
            "/api/v1/kubernetes/gateway/rbac/reconcile",
            "/api/v1/kubernetes/access-events/ingest",
            "/api/v1/kubernetes/access-events/client",
            "/api/v1/kubernetes/session-chunks/ingest",
            "/api/v1/kubernetes/access-events",
            "/api/v1/kubernetes/access-events/{id}",
            "/api/v1/kubernetes/sessions/{id}/chunks",
            "/api/v1/kubernetes/access-events/export",
        ] {
            assert!(source.contains(route), "missing route {route}");
        }
        assert!(source.contains("MAX_ACCESS_EVENT_BODY_BYTES"));
        assert!(source.contains("MAX_CLIENT_ENRICHMENT_BODY_BYTES"));
        assert!(source.contains("MAX_SESSION_CHUNK_BODY_BYTES"));
        assert!(source.contains("MAX_GATEWAY_AUTHORIZE_BODY_BYTES"));
    }

    #[test]
    fn kubernetes_logging_settings_keep_the_authenticated_tenant_boundary() {
        for (method, path) in [
            (&Method::GET, "/api/v1/settings/kubernetes-logging"),
            (&Method::PUT, "/api/v1/settings/kubernetes-logging"),
            (
                &Method::DELETE,
                "/api/v1/settings/kubernetes-logging/clients/kcs_0123456789abcdef01234567",
            ),
            (&Method::POST, "/api/v1/settings/kubernetes-logging/roles"),
            (
                &Method::PUT,
                "/api/v1/settings/kubernetes-logging/roles/grant-1",
            ),
            (
                &Method::DELETE,
                "/api/v1/settings/kubernetes-logging/roles/grant-1",
            ),
        ] {
            assert!(!allows_unauthenticated_tenant_request(method, path));
        }
    }
}
