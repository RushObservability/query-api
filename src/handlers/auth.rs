use axum::{
    Json,
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::net::{IpAddr, SocketAddr};
use std::time::Instant;

use crate::AppState;

type HmacSha256 = Hmac<Sha256>;

fn public_auth_error(
    status: StatusCode,
    operation: &'static str,
    error: impl std::fmt::Display,
) -> (StatusCode, String) {
    crate::api_error::internal_legacy_with_message(
        status,
        operation,
        error,
        "authentication temporarily unavailable",
    )
}

fn login_rate_limit_secret() -> Result<Vec<u8>, String> {
    let secret = std::env::var("RUSH_LOGIN_RATE_LIMIT_SECRET")
        .or_else(|_| std::env::var("RUSH_SSO_TRANSACTION_SECRET"))
        .or_else(|_| std::env::var("RUSH_API_KEY_SECRET"))
        .map_err(|_| "login rate limiting is not configured".to_string())?;
    if secret.len() < 32 {
        return Err("login rate limiting secret must contain at least 32 bytes".to_string());
    }
    Ok(secret.into_bytes())
}

pub fn validate_login_rate_limit_secret() -> Result<(), String> {
    login_rate_limit_secret().map(|_| ())
}

pub fn sso_only_mode_enabled() -> bool {
    std::env::var("RUSH_SSO_ONLY")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(false)
}

pub fn break_glass_username() -> String {
    std::env::var("RUSH_BREAK_GLASS_USERNAME")
        .unwrap_or_else(|_| "admin".to_string())
        .trim()
        .to_lowercase()
}

pub fn validate_sso_only_config() -> Result<(), String> {
    if sso_only_mode_enabled() && break_glass_username().is_empty() {
        return Err(
            "RUSH_BREAK_GLASS_USERNAME must not be empty when RUSH_SSO_ONLY is enabled".to_string(),
        );
    }
    Ok(())
}

pub(crate) fn is_break_glass_username(username: &str) -> bool {
    username.trim().to_lowercase() == break_glass_username()
}

fn local_login_allowed(
    sso_only: bool,
    sso_enabled: bool,
    username: &str,
    role: &str,
    break_glass: &str,
) -> bool {
    !sso_only
        || !sso_enabled
        || (username.trim().to_lowercase() == break_glass.trim().to_lowercase() && role == "admin")
}

fn keyed_login_identifier(label: &[u8], value: &str, secret: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(b"rush-login-rate-limit-v1\0");
    mac.update(label);
    mac.update(b"\0");
    mac.update(value.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn trusted_proxy_cidrs_from_env() -> Result<Vec<String>, String> {
    let raw = std::env::var("RUSH_TRUSTED_PROXY_CIDRS").unwrap_or_default();
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    let values = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    crate::api_key_auth::normalize_source_cidrs(&values)
}

pub fn login_limit_from_env(name: &str, default: u32) -> Result<u32, String> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u32>()
            .ok()
            .filter(|limit| (1..=10_000).contains(limit))
            .ok_or_else(|| format!("{name} must be an integer between 1 and 10000")),
        Err(_) => Ok(default),
    }
}

fn is_trusted_proxy(address: IpAddr, trusted_proxy_cidrs: &[String]) -> bool {
    !trusted_proxy_cidrs.is_empty()
        && crate::api_key_auth::source_allowed(address, trusted_proxy_cidrs)
}

fn resolve_login_client_ip(
    peer_ip: IpAddr,
    headers: &HeaderMap,
    trusted_proxy_cidrs: &[String],
) -> IpAddr {
    if !is_trusted_proxy(peer_ip, trusted_proxy_cidrs) {
        return peer_ip;
    }

    if let Some(raw) = headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
    {
        let chain = raw
            .split(',')
            .map(|value| value.trim().parse::<IpAddr>())
            .collect::<Result<Vec<_>, _>>();
        let Ok(chain) = chain else {
            return peer_ip;
        };
        let mut client = peer_ip;
        for address in chain.into_iter().rev() {
            if !is_trusted_proxy(client, trusted_proxy_cidrs) {
                break;
            }
            client = address;
        }
        return client;
    }

    headers
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<IpAddr>().ok())
        .unwrap_or(peer_ip)
}

fn consume_local_login_limit(
    limiter: &dashmap::DashMap<String, (u32, Instant)>,
    key: String,
    limit: u32,
    now: Instant,
) -> bool {
    match limiter.entry(key) {
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert((1, now));
            true
        }
        dashmap::mapref::entry::Entry::Occupied(mut entry) => {
            let (count, window_start) = *entry.get();
            let next = if now.duration_since(window_start).as_secs() >= 60 {
                (1, now)
            } else {
                (count.saturating_add(1), window_start)
            };
            entry.insert(next);
            next.0 <= limit
        }
    }
}

fn account_failure_limit_exceeded(
    credentials_valid: bool,
    local_allowed: bool,
    distributed_allowed: bool,
) -> bool {
    // A valid password must never be rejected because an attacker previously
    // targeted the username. Account limits classify failed credentials only;
    // the IP limit still applies before password verification.
    !credentials_valid && !(local_allowed && distributed_allowed)
}

fn login_audit_context(headers: &HeaderMap, client_ip: IpAddr) -> (String, String, String) {
    let (_, user_agent, request_id) = crate::audit::actor_context_from_headers(headers);
    (client_ip.to_string(), user_agent, request_id)
}

#[derive(serde::Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(serde::Serialize)]
pub struct UserInfo {
    pub id: String,
    pub username: String,
    pub display_name: String,
    pub tenant_id: String,
    pub role: String,
}

fn credentials_within_bounds(username: &str, password: &str) -> bool {
    !username.trim().is_empty()
        && username.len() <= crate::clickhouse_config::MAX_USERNAME_BYTES
        && !password.is_empty()
        && password.len() <= crate::clickhouse_config::MAX_PASSWORD_BYTES
}

/// POST /api/v1/auth/login
///
/// Accepts `{ "username": "...", "password": "..." }`.
/// On success, returns user information and keeps the session bearer solely in
/// an HttpOnly cookie; it is never exposed to frontend JavaScript.
pub async fn login(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<LoginRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let client_ip = resolve_login_client_ip(peer.ip(), &headers, &state.trusted_proxy_cidrs);
    if !credentials_within_bounds(&req.username, &req.password) {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("auth.login.failure", "anonymous")
                    .actor_name("invalid login input")
                    .outcome("failure")
                    .changes(serde_json::json!({ "reason": "credential_bounds" }).to_string())
                    .description("login request rejected before credential processing")
                    .context(login_audit_context(&headers, client_ip)),
            )
            .await;
        return Err((StatusCode::BAD_REQUEST, "invalid login request".to_string()));
    }
    let secret = login_rate_limit_secret().map_err(|error| {
        public_auth_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "rate_limit_configuration",
            error,
        )
    })?;
    let account = req.username.trim().to_lowercase();
    let ip_hash = keyed_login_identifier(b"ip", &client_ip.to_string(), &secret);
    let account_hash = keyed_login_identifier(b"account", &account, &secret);
    let local_now = Instant::now();
    let local_ip_allowed = consume_local_login_limit(
        &state.login_limiter,
        format!("ip:{ip_hash}"),
        state.login_ip_limit_per_minute,
        local_now,
    );

    if let Err(error) = state.config_db.record_login_ip_attempt(&ip_hash).await {
        tracing::error!(%error, "failed to persist login rate-limit attempt");
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("auth.login.failure", "anonymous")
                    .actor_name(req.username.clone())
                    .outcome("failure")
                    .changes(
                        serde_json::json!({ "reason": "rate_limit_store_unavailable" }).to_string(),
                    )
                    .description("authentication unavailable")
                    .context(login_audit_context(&headers, client_ip)),
            )
            .await;
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "authentication temporarily unavailable".to_string(),
        ));
    }
    let since = (chrono::Utc::now() - chrono::Duration::seconds(60))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    let ip_attempts = match state
        .config_db
        .login_ip_attempt_count(&ip_hash, &since)
        .await
    {
        Ok(count) => count,
        Err(error) => {
            tracing::error!(%error, "failed to read login rate-limit attempts");
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("auth.login.failure", "anonymous")
                        .actor_name(req.username.clone())
                        .outcome("failure")
                        .changes(
                            serde_json::json!({ "reason": "rate_limit_store_unavailable" })
                                .to_string(),
                        )
                        .description("authentication unavailable")
                        .context(login_audit_context(&headers, client_ip)),
                )
                .await;
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication temporarily unavailable".to_string(),
            ));
        }
    };
    let distributed_ip_allowed = ip_attempts <= u64::from(state.login_ip_limit_per_minute);
    if !(local_ip_allowed && distributed_ip_allowed) {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("auth.login.lockout", "anonymous")
                    .actor_name(req.username.clone())
                    .outcome("failure")
                    .changes(
                        serde_json::json!({
                            "ip_limit_exceeded": true,
                            "account_limit_exceeded": false,
                        })
                        .to_string(),
                    )
                    .description("login rate limit exceeded")
                    .context(login_audit_context(&headers, client_ip)),
            )
            .await;
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "too many login attempts, try again later".to_string(),
        ));
    }

    let mut authenticated = match state
        .config_db
        .authenticate(&req.username, &req.password)
        .await
    {
        Ok(authenticated) => authenticated,
        Err(error) => {
            tracing::error!(operation = "credential_lookup", %error, "authentication request failed");
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("auth.login.failure", "anonymous")
                        .actor_name(req.username.clone())
                        .outcome("failure")
                        .changes(
                            serde_json::json!({ "reason": "identity_store_unavailable" })
                                .to_string(),
                        )
                        .description("authentication unavailable")
                        .context(login_audit_context(&headers, client_ip)),
                )
                .await;
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication temporarily unavailable".to_string(),
            ));
        }
    };

    // Optional enterprise SSO-only policy. It is enforced only while a valid
    // provider is active, and retains one explicitly named, local admin as the
    // break-glass path. Rejections remain indistinguishable from bad credentials.
    if sso_only_mode_enabled() {
        let sso_enabled = match state.config_db.is_sso_enabled().await {
            Ok(enabled) => enabled,
            Err(error) => {
                tracing::error!(operation = "sso_policy_lookup", %error, "authentication request failed");
                state
                    .audit
                    .log(
                        crate::audit::AuditEvent::new("auth.login.failure", "anonymous")
                            .actor_name(req.username.clone())
                            .outcome("failure")
                            .changes(
                                serde_json::json!({ "reason": "sso_policy_unavailable" })
                                    .to_string(),
                            )
                            .description("authentication unavailable")
                            .context(login_audit_context(&headers, client_ip)),
                    )
                    .await;
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "authentication temporarily unavailable".to_string(),
                ));
            }
        };
        if let Some((_, username, _, _, role, _)) = authenticated.as_ref()
            && !local_login_allowed(true, sso_enabled, username, role, &break_glass_username())
        {
            authenticated = None;
        }
    }
    let (user_id, username, display_name, tenant_id, role, user_version) = match authenticated {
        Some(u) => u,
        None => {
            // Only failed credentials consume the account budget. Checking
            // this after password verification ensures targeted failures can
            // never deny a legitimate login with the correct password.
            let local_account_allowed = consume_local_login_limit(
                &state.login_limiter,
                format!("account:{account_hash}"),
                state.login_account_limit_per_minute,
                Instant::now(),
            );
            if let Err(error) = state
                .config_db
                .record_login_account_failure(&account_hash)
                .await
            {
                tracing::error!(%error, "failed to persist account login failure");
                state
                    .audit
                    .log(
                        crate::audit::AuditEvent::new("auth.login.failure", "anonymous")
                            .actor_name(req.username.clone())
                            .outcome("failure")
                            .changes(
                                serde_json::json!({ "reason": "rate_limit_store_unavailable" })
                                    .to_string(),
                            )
                            .description("authentication unavailable")
                            .context(login_audit_context(&headers, client_ip)),
                    )
                    .await;
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "authentication temporarily unavailable".to_string(),
                ));
            }
            let account_failures = match state
                .config_db
                .login_account_failure_count(&account_hash, &since)
                .await
            {
                Ok(count) => count,
                Err(error) => {
                    tracing::error!(%error, "failed to read account login failures");
                    state
                        .audit
                        .log(
                            crate::audit::AuditEvent::new("auth.login.failure", "anonymous")
                                .actor_name(req.username.clone())
                                .outcome("failure")
                                .changes(
                                    serde_json::json!({ "reason": "rate_limit_store_unavailable" })
                                        .to_string(),
                                )
                                .description("authentication unavailable")
                                .context(login_audit_context(&headers, client_ip)),
                        )
                        .await;
                    return Err((
                        StatusCode::SERVICE_UNAVAILABLE,
                        "authentication temporarily unavailable".to_string(),
                    ));
                }
            };
            let distributed_account_allowed =
                account_failures <= u64::from(state.login_account_limit_per_minute);
            if account_failure_limit_exceeded(
                false,
                local_account_allowed,
                distributed_account_allowed,
            ) {
                state
                    .audit
                    .log(
                        crate::audit::AuditEvent::new("auth.login.lockout", "anonymous")
                            .actor_name(req.username.clone())
                            .outcome("failure")
                            .changes(
                                serde_json::json!({
                                    "ip_limit_exceeded": false,
                                    "account_limit_exceeded": true,
                                })
                                .to_string(),
                            )
                            .description("login rate limit exceeded")
                            .context(login_audit_context(&headers, client_ip)),
                    )
                    .await;
                return Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    "too many login attempts, try again later".to_string(),
                ));
            }

            tracing::warn!(
                event = "login_failed",
                username = %req.username,
                reason = "invalid_credentials",
                "authentication failed"
            );
            // AUDIT: failed login. Actor is anonymous (unauthenticated); record
            // the attempted username so the trail shows who was targeted.
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("auth.login.failure", "anonymous")
                        .actor_name(req.username.clone())
                        .outcome("failure")
                        .description("invalid username or password")
                        .context(login_audit_context(&headers, client_ip)),
                )
                .await;
            return Err((
                StatusCode::UNAUTHORIZED,
                "invalid username or password".to_string(),
            ));
        }
    };

    let issued = state
        .config_db
        .create_session_at_version(&user_id, user_version)
        .await
        .map_err(|error| {
            public_auth_error(StatusCode::INTERNAL_SERVER_ERROR, "session_create", error)
        })?;

    tracing::info!(
        event = "login",
        username = %username,
        tenant_id = %tenant_id,
        role = %role,
        method = "local",
        "user authenticated"
    );

    // AUDIT: successful login. `tenant_id` is the user's actual (affected)
    // tenant — the row itself still lives in observability.audit_events.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("auth.login.success", "user")
                .actor(user_id.clone(), username.clone())
                .tenant(tenant_id.clone())
                .outcome("success")
                .description("user authenticated (local)")
                .context(login_audit_context(&headers, client_ip)),
        )
        .await;

    let cookie = session_cookie(&issued.token, issued.max_age_seconds);

    let mut headers = HeaderMap::new();
    headers.insert(header::SET_COOKIE, cookie.parse().unwrap());

    Ok((
        headers,
        Json(serde_json::json!({
            "user": UserInfo {
                id: user_id,
                username,
                display_name,
                tenant_id,
                role,
            },
            "session": {
                "activity_interval_seconds": state.config_db.session_activity_interval_seconds(),
            },
        })),
    ))
}

/// POST /api/v1/auth/logout
///
/// Reads the `rush_session` cookie, deletes that session, and clears the cookie.
pub async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if let Some(token) = extract_session_cookie(&headers) {
        let caller = crate::request_auth::resolve_session_user(&state, &token).await;
        if let Err(error) = state.config_db.delete_session(&token).await {
            tracing::error!(operation = "session_revoke", %error, "logout request failed");
            let event = match caller.as_ref() {
                Some((user_id, username, _, tenant_id, _)) => {
                    crate::audit::AuditEvent::new("auth.logout", "user")
                        .actor(user_id.clone(), username.clone())
                        .tenant(tenant_id.clone())
                }
                None => crate::audit::AuditEvent::new("auth.logout", "anonymous")
                    .actor_name("unknown session"),
            };
            state
                .audit
                .log(
                    event
                        .outcome("failure")
                        .changes(
                            serde_json::json!({ "reason": "session_store_unavailable" })
                                .to_string(),
                        )
                        .description("session revocation failed during logout")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "logout temporarily unavailable; retry the request".to_string(),
            ));
        }

        let event = match caller {
            Some((user_id, username, _, tenant_id, _)) => {
                crate::audit::AuditEvent::new("auth.logout", "user")
                    .actor(user_id, username)
                    .tenant(tenant_id)
            }
            None => crate::audit::AuditEvent::new("auth.logout", "anonymous")
                .actor_name("unknown or expired session"),
        };
        state
            .audit
            .log(
                event
                    .outcome("success")
                    .description("user session ended")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }

    let clear_cookie = session_cookie("", 0);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(header::SET_COOKIE, clear_cookie.parse().unwrap());

    Ok((resp_headers, Json(serde_json::json!({ "ok": true }))))
}

/// GET /api/v1/auth/me
///
/// Returns the current user's info based on the `rush_session` cookie,
/// or 401 if not authenticated.
pub async fn me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let token = extract_session_cookie(&headers)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "not authenticated".to_string()))?;

    let (user_id, username, display_name, tenant_id, role) =
        crate::request_auth::resolve_session_user(&state, &token)
            .await
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    "session expired or invalid".to_string(),
                )
            })?;

    Ok(Json(serde_json::json!({
        "user": UserInfo {
            id: user_id,
            username,
            display_name,
            tenant_id,
            role,
        },
        "session": {
            "activity_interval_seconds": state.config_db.session_activity_interval_seconds(),
        },
    })))
}

/// GET /api/v1/auth/sessions — active sessions for the current user.
pub async fn list_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = crate::handlers::users::require_auth(&state, &headers).await?;
    let token = extract_session_cookie(&headers)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "not authenticated".to_string()))?;
    let sessions = state
        .config_db
        .list_auth_sessions(Some(&caller.0), &token)
        .await
        .map_err(|error| {
            public_auth_error(StatusCode::SERVICE_UNAVAILABLE, "session_inventory", error)
        })?;
    Ok(Json(serde_json::json!({ "sessions": sessions })))
}

/// GET /api/v1/auth/admin/sessions — active sessions across all users.
pub async fn list_all_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = crate::handlers::users::require_admin(&state, &headers).await?;
    let token = extract_session_cookie(&headers)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "not authenticated".to_string()))?;
    let sessions = match state.config_db.list_auth_sessions(None, &token).await {
        Ok(sessions) => sessions,
        Err(error) => {
            tracing::error!(operation = "admin_session_inventory", %error, "authentication request failed");
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("session.inventory_read", "user")
                        .actor(caller.0.clone(), caller.1.clone())
                        .tenant(caller.3.clone())
                        .resource("session_inventory", "all")
                        .outcome("failure")
                        .changes(
                            serde_json::json!({ "reason": "session_store_unavailable" })
                                .to_string(),
                        )
                        .description("administrator session inventory read failed")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "session inventory temporarily unavailable".to_string(),
            ));
        }
    };
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("session.inventory_read", "user")
                .actor(caller.0, caller.1)
                .tenant(caller.3)
                .resource("session_inventory", "all")
                .outcome("success")
                .changes(serde_json::json!({ "session_count": sessions.len() }).to_string())
                .description("administrator read active session inventory")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(Json(serde_json::json!({ "sessions": sessions })))
}

async fn revoke_session_for_scope(
    state: AppState,
    headers: HeaderMap,
    session_id: String,
    admin_scope: bool,
) -> Result<axum::response::Response, (StatusCode, String)> {
    let caller = if admin_scope {
        crate::handlers::users::require_admin(&state, &headers).await?
    } else {
        crate::handlers::users::require_auth(&state, &headers).await?
    };
    if uuid::Uuid::parse_str(&session_id).is_err() {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("session.revoke", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("session", "invalid")
                    .outcome("failure")
                    .changes(serde_json::json!({ "reason": "invalid_session_id" }).to_string())
                    .description("session revocation rejected")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        return Err((StatusCode::BAD_REQUEST, "invalid session id".to_string()));
    }
    let token = extract_session_cookie(&headers)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "not authenticated".to_string()))?;
    let owner = if admin_scope {
        None
    } else {
        Some(caller.0.as_str())
    };
    let revoked = match state
        .config_db
        .revoke_auth_session(&session_id, owner, &token)
        .await
    {
        Ok(Some(session)) => session,
        Ok(None) => {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("session.revoke", "user")
                        .actor(caller.0.clone(), caller.1.clone())
                        .tenant(caller.3.clone())
                        .resource("session", session_id)
                        .outcome("failure")
                        .changes(serde_json::json!({ "reason": "not_found" }).to_string())
                        .description("session revocation target was not found")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            return Err((StatusCode::NOT_FOUND, "session not found".to_string()));
        }
        Err(error) => {
            tracing::error!(operation = "session_revoke", %error, "authentication request failed");
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("session.revoke", "user")
                        .actor(caller.0.clone(), caller.1.clone())
                        .tenant(caller.3.clone())
                        .resource("session", session_id)
                        .outcome("failure")
                        .changes(
                            serde_json::json!({ "reason": "session_store_unavailable" })
                                .to_string(),
                        )
                        .description("session revocation failed")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "session revocation temporarily unavailable".to_string(),
            ));
        }
    };
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("session.revoke", "user")
                .actor(caller.0, caller.1)
                .tenant(revoked.tenant_id.clone())
                .resource("session", session_id)
                .outcome("success")
                .changes(
                    serde_json::json!({
                        "scope": if admin_scope { "administrator" } else { "self" },
                        "target_user_id": revoked.user_id,
                        "auth_method": revoked.auth_method,
                    })
                    .to_string(),
                )
                .description("active session revoked")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    let mut response = Json(serde_json::json!({ "ok": true })).into_response();
    if revoked.current {
        response.headers_mut().insert(
            header::SET_COOKIE,
            session_cookie("", 0)
                .parse()
                .expect("session cookie is a valid header"),
        );
    }
    Ok(response)
}

pub async fn revoke_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<axum::response::Response, (StatusCode, String)> {
    revoke_session_for_scope(state, headers, session_id, false).await
}

pub async fn admin_revoke_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<axum::response::Response, (StatusCode, String)> {
    revoke_session_for_scope(state, headers, session_id, true).await
}

/// Parse the `rush_session` value out of the Cookie header.
/// Build the `Set-Cookie` value for the session.
///
/// Default: a hardened `__Host-rush_session` with `Secure` (HTTPS-only) — the
/// `__Host-` prefix blocks subdomain cookie injection. When `RUSH_INSECURE_COOKIES`
/// is truthy, emit a plain `rush_session` without `__Host-`/`Secure` so the app
/// works over plain HTTP (e.g. `kubectl port-forward`, non-TLS internal access),
/// where browsers refuse to store `Secure`/`__Host-` cookies. `extract_session_cookie`
/// reads both names.
pub fn session_cookie(token: &str, max_age: i64) -> String {
    session_cookie_with_mode(token, max_age, insecure_cookies_enabled())
}

fn insecure_cookies_enabled() -> bool {
    let insecure = std::env::var("RUSH_INSECURE_COOKIES")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    insecure
}

fn session_cookie_with_mode(token: &str, max_age: i64, insecure: bool) -> String {
    if insecure {
        format!("rush_session={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age}")
    } else {
        format!(
            "__Host-rush_session={token}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age={max_age}"
        )
    }
}

pub fn extract_session_cookie(headers: &HeaderMap) -> Option<String> {
    extract_session_cookie_with_mode(headers, insecure_cookies_enabled())
}

fn extract_session_cookie_with_mode(headers: &HeaderMap, insecure: bool) -> Option<String> {
    let cookie_name = if insecure {
        "rush_session="
    } else {
        "__Host-rush_session="
    };
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in cookie_header.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix(cookie_name) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_session_cookie_is_host_only_and_secure() {
        let cookie = session_cookie_with_mode("session-token", 86400, false);
        assert_eq!(
            cookie,
            "__Host-rush_session=session-token; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=86400"
        );
    }

    #[test]
    fn credential_bounds_reject_work_amplification_before_login_processing() {
        assert!(credentials_within_bounds("admin", "password"));
        assert!(!credentials_within_bounds("", "password"));
        assert!(!credentials_within_bounds("admin", ""));
        assert!(!credentials_within_bounds(
            &"u".repeat(crate::clickhouse_config::MAX_USERNAME_BYTES + 1),
            "password"
        ));
        assert!(!credentials_within_bounds(
            "admin",
            &"p".repeat(crate::clickhouse_config::MAX_PASSWORD_BYTES + 1)
        ));
    }

    #[test]
    fn insecure_mode_is_explicitly_http_compatible_but_still_httponly() {
        let cookie = session_cookie_with_mode("session-token", 0, true);
        assert_eq!(
            cookie,
            "rush_session=session-token; HttpOnly; SameSite=Lax; Path=/; Max-Age=0"
        );
    }

    #[test]
    fn logout_fails_closed_and_audits_revocation_errors() {
        let source = include_str!("auth.rs");
        let logout = source
            .split_once("pub async fn logout")
            .map(|(_, method)| method)
            .expect("logout handler must exist")
            .split("pub async fn me")
            .next()
            .expect("session lookup must follow logout");
        assert!(logout.contains("if let Err(error) = state.config_db.delete_session"));
        assert!(logout.contains(".outcome(\"failure\")"));
        assert!(logout.contains("session_store_unavailable"));
        assert!(logout.contains("StatusCode::SERVICE_UNAVAILABLE"));
        assert!(logout.contains(".outcome(\"success\")"));
    }

    #[test]
    fn untrusted_peer_cannot_spoof_forwarded_client_address() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "203.0.113.9".parse().unwrap());
        let peer: IpAddr = "198.51.100.4".parse().unwrap();
        let trusted = vec!["10.0.0.0/8".to_string()];

        assert_eq!(resolve_login_client_ip(peer, &headers, &trusted), peer);
    }

    #[test]
    fn trusted_proxy_chain_selects_first_untrusted_hop_from_the_right() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "203.0.113.9, 10.20.30.40".parse().unwrap(),
        );
        let trusted = vec!["10.0.0.0/8".to_string()];

        assert_eq!(
            resolve_login_client_ip("10.1.2.3".parse().unwrap(), &headers, &trusted),
            "203.0.113.9".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn malformed_forwarded_chain_fails_closed_to_peer_address() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "203.0.113.9, not-an-ip".parse().unwrap());
        let peer: IpAddr = "10.1.2.3".parse().unwrap();
        let trusted = vec!["10.0.0.0/8".to_string()];

        assert_eq!(resolve_login_client_ip(peer, &headers, &trusted), peer);
    }

    #[test]
    fn account_and_ip_local_limits_are_independent_and_atomic() {
        let limiter = dashmap::DashMap::new();
        let now = Instant::now();
        assert!(consume_local_login_limit(
            &limiter,
            "account:a".into(),
            2,
            now
        ));
        assert!(consume_local_login_limit(
            &limiter,
            "account:a".into(),
            2,
            now
        ));
        assert!(!consume_local_login_limit(
            &limiter,
            "account:a".into(),
            2,
            now
        ));
        assert!(consume_local_login_limit(&limiter, "ip:b".into(), 2, now));
    }

    #[test]
    fn valid_credentials_are_not_rejected_by_account_failure_limit() {
        assert!(!account_failure_limit_exceeded(true, false, false));
        assert!(account_failure_limit_exceeded(false, false, true));
        assert!(account_failure_limit_exceeded(false, true, false));
        assert!(!account_failure_limit_exceeded(false, true, true));
    }

    #[test]
    fn sso_only_mode_allows_only_the_named_admin_break_glass_identity() {
        assert!(local_login_allowed(true, true, " Admin ", "admin", "admin"));
        assert!(!local_login_allowed(
            true,
            true,
            "other-admin",
            "admin",
            "admin"
        ));
        assert!(!local_login_allowed(true, true, "admin", "viewer", "admin"));
        assert!(local_login_allowed(
            false,
            true,
            "local-user",
            "viewer",
            "admin"
        ));
        assert!(local_login_allowed(
            true,
            false,
            "local-user",
            "viewer",
            "admin"
        ));
    }

    #[test]
    fn concurrent_local_attempts_cannot_overshoot_the_limit() {
        let limiter = std::sync::Arc::new(dashmap::DashMap::new());
        let start = std::sync::Arc::new(std::sync::Barrier::new(32));
        let handles = (0..32)
            .map(|_| {
                let limiter = limiter.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    start.wait();
                    consume_local_login_limit(
                        &limiter,
                        "account:concurrent".into(),
                        10,
                        Instant::now(),
                    )
                })
            })
            .collect::<Vec<_>>();
        let allowed = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|allowed| *allowed)
            .count();

        assert_eq!(allowed, 10);
    }

    #[test]
    fn rate_limit_identifiers_are_keyed_and_domain_separated() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let account = keyed_login_identifier(b"account", "admin", secret);
        let ip = keyed_login_identifier(b"ip", "admin", secret);
        assert_eq!(account.len(), 64);
        assert_ne!(account, ip);
        assert!(!account.contains("admin"));
    }

    #[test]
    fn public_auth_errors_do_not_expose_internal_details() {
        let (status, message) = public_auth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "test",
            "clickhouse host=db.internal password=secret",
        );
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(message, "authentication temporarily unavailable");
        assert!(!message.contains("clickhouse"));
        assert!(!message.contains("secret"));
    }

    #[test]
    fn secure_cookie_selection_cannot_be_overridden_by_plain_cookie() {
        // Test the selection rule without mutating process-wide environment:
        // secure mode's cookie name is host-prefixed and a plain cookie does
        // not share that prefix.
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            "rush_session=attacker; __Host-rush_session=protected"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            extract_session_cookie_with_mode(&headers, false).as_deref(),
            Some("protected")
        );

        let mut plain_only = HeaderMap::new();
        plain_only.insert(header::COOKIE, "rush_session=attacker".parse().unwrap());
        assert_eq!(extract_session_cookie_with_mode(&plain_only, false), None);
        assert_eq!(
            extract_session_cookie_with_mode(&plain_only, true).as_deref(),
            Some("attacker")
        );
    }
}
