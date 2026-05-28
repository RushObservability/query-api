use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::handlers::auth::extract_session_cookie;

#[derive(serde::Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    pub display_name: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct ChangePasswordRequest {
    pub current_password: Option<String>,
    pub password: String,
}

#[derive(serde::Deserialize)]
pub struct ToggleUserRequest {
    pub enabled: bool,
}

#[derive(serde::Serialize)]
pub struct UserResponse {
    pub id: String,
    pub username: String,
    pub display_name: String,
    pub tenant_id: String,
    pub enabled: bool,
    pub created_at: String,
}

/// Extract the calling user from the session cookie.
/// Returns (user_id, username, display_name, tenant_id, role) or 401.
pub(crate) async fn require_auth(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(String, String, String, String, String), (StatusCode, String)> {
    let token = extract_session_cookie(headers).ok_or_else(|| {
        (StatusCode::UNAUTHORIZED, "not authenticated".to_string())
    })?;
    state
        .config_db
        .get_session_user(&token).await
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "session expired or invalid".to_string(),
            )
        })
}

/// Require that the caller is an admin.
pub(crate) async fn require_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(String, String, String, String, String), (StatusCode, String)> {
    let caller = require_auth(state, headers).await?;
    if caller.4 != "admin" {
        return Err((StatusCode::FORBIDDEN, "admin role required".to_string()));
    }
    Ok(caller)
}

/// Require that the caller has write access (admin or write role).
pub(crate) async fn require_write(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(String, String, String, String, String), (StatusCode, String)> {
    let caller = require_auth(state, headers).await?;
    if caller.4 != "admin" && caller.4 != "write" {
        return Err((StatusCode::FORBIDDEN, "write role required".to_string()));
    }
    Ok(caller)
}

fn user_response(row: (String, String, String, String, bool, String)) -> UserResponse {
    UserResponse {
        id: row.0,
        username: row.1,
        display_name: row.2,
        tenant_id: row.3,
        enabled: row.4,
        created_at: row.5,
    }
}

/// GET /api/v1/users
pub async fn list_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;

    let rows = state
        .config_db
        .list_users().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let users: Vec<UserResponse> = rows.into_iter().map(user_response).collect();

    Ok(Json(serde_json::json!({ "users": users })))
}

/// POST /api/v1/users
pub async fn create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;

    let username = req.username.trim().to_string();
    if username.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "username must not be empty".to_string()));
    }
    if username.len() > 100 {
        return Err((StatusCode::BAD_REQUEST, "username must not exceed 100 characters".to_string()));
    }
    let password = req.password.clone();
    if password.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "password must not be empty".to_string()));
    }
    if password.len() > 1024 {
        return Err((StatusCode::BAD_REQUEST, "password must not exceed 1024 characters".to_string()));
    }

    let display_name = req.display_name.as_deref().unwrap_or("").to_string();
    if display_name.len() > 255 {
        return Err((StatusCode::BAD_REQUEST, "display_name must not exceed 255 characters".to_string()));
    }

    let id = state
        .config_db
        .create_user(&username, &password, &display_name).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    // New users default to the viewers group
    let _ = state
        .config_db
        .set_user_groups(&id, &["viewers".to_string()]);

    let row = state
        .config_db
        .get_user(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "user created but not found".to_string(),
            )
        })?;

    Ok((StatusCode::CREATED, Json(user_response(row))))
}

/// DELETE /api/v1/users/{id}
pub async fn delete_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;

    // Refuse to delete the user named "admin"
    let username = state
        .config_db
        .get_username(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "user not found".to_string()))?;

    if username == "admin" {
        return Err((
            StatusCode::BAD_REQUEST,
            "cannot delete the admin user".to_string(),
        ));
    }

    let deleted = state
        .config_db
        .delete_user(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    if !deleted {
        return Err((StatusCode::NOT_FOUND, "user not found".to_string()));
    }

    Ok(StatusCode::NO_CONTENT)
}

/// PUT /api/v1/users/{id}/password
pub async fn change_password(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<ChangePasswordRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_auth(&state, &headers).await?;

    // Admin can change any user's password; non-admin can only change their own.
    if caller.4 != "admin" && caller.0 != id {
        return Err((
            StatusCode::FORBIDDEN,
            "you can only change your own password".to_string(),
        ));
    }

    if req.password.len() < 12 {
        return Err((StatusCode::BAD_REQUEST, "password must be at least 12 characters".to_string()));
    }

    // Non-admin users must supply their current password to change it.
    if caller.4 != "admin" {
        let current = req.current_password.as_deref().unwrap_or("");
        if current.is_empty() {
            return Err((StatusCode::BAD_REQUEST, "current_password is required".to_string()));
        }
        if state.config_db.authenticate(&caller.1, current).await.is_none() {
            return Err((StatusCode::FORBIDDEN, "current password is incorrect".to_string()));
        }
    }

    let updated = state
        .config_db
        .change_password(&id, &req.password).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    if !updated {
        return Err((StatusCode::NOT_FOUND, "user not found".to_string()));
    }

    Ok(Json(serde_json::json!({ "ok": true })))
}

/// PUT /api/v1/users/{id}/toggle
pub async fn toggle_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<ToggleUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;

    let updated = state
        .config_db
        .set_user_enabled(&id, req.enabled).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    if !updated {
        return Err((StatusCode::NOT_FOUND, "user not found".to_string()));
    }

    let row = state
        .config_db
        .get_user(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "user not found".to_string()))?;

    Ok(Json(user_response(row)))
}
