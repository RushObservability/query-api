use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};

use crate::AppState;

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

/// POST /api/v1/auth/login
///
/// Accepts `{ "username": "...", "password": "..." }`.
/// On success, returns the user info + session token in the body and sets
/// a `rush_session` HttpOnly cookie.
pub async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (user_id, username, display_name, tenant_id, role) = state
        .config_db
        .authenticate(&req.username, &req.password).await
        .ok_or_else(|| {
            tracing::warn!(
                event = "login_failed",
                username = %req.username,
                reason = "invalid_credentials",
                "authentication failed"
            );
            (
                StatusCode::UNAUTHORIZED,
                "invalid username or password".to_string(),
            )
        })?;

    let token = state
        .config_db
        .create_session(&user_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("session error: {e}")))?;

    tracing::info!(
        event = "login",
        username = %username,
        tenant_id = %tenant_id,
        role = %role,
        method = "local",
        "user authenticated"
    );

    let cookie = format!(
        "rush_session={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=86400"
    );

    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        cookie.parse().unwrap(),
    );

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
            "token": token,
        })),
    ))
}

/// POST /api/v1/auth/logout
///
/// Reads the `rush_session` cookie, deletes that session, and clears the cookie.
pub async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(token) = extract_session_cookie(&headers) {
        state.config_db.delete_session(&token).await;
    }

    let clear_cookie = "rush_session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0";

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        header::SET_COOKIE,
        clear_cookie.parse().unwrap(),
    );

    (resp_headers, Json(serde_json::json!({ "ok": true })))
}

/// GET /api/v1/auth/me
///
/// Returns the current user's info based on the `rush_session` cookie,
/// or 401 if not authenticated.
pub async fn me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let token = extract_session_cookie(&headers).ok_or_else(|| {
        (StatusCode::UNAUTHORIZED, "not authenticated".to_string())
    })?;

    let (user_id, username, display_name, tenant_id, role) = state
        .config_db
        .get_session_user(&token).await
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
        }
    })))
}

/// Parse the `rush_session` value out of the Cookie header.
pub fn extract_session_cookie(headers: &HeaderMap) -> Option<String> {
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in cookie_header.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix("rush_session=") {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}
