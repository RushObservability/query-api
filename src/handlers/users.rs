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
    let token = extract_session_cookie(headers)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "not authenticated".to_string()))?;
    crate::request_auth::resolve_session_user(state, &token)
        .await
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
    if !role_allows_admin(&caller.4) {
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
    if !role_allows_write(&caller.4) {
        return Err((StatusCode::FORBIDDEN, "write role required".to_string()));
    }
    Ok(caller)
}

fn role_allows_admin(role: &str) -> bool {
    role == "admin"
}

fn role_allows_write(role: &str) -> bool {
    matches!(role, "admin" | "write")
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

    let rows = state.config_db.list_users().await.map_err(|e| {
        tracing::error!(error = %e, "internal error");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;

    let users: Vec<UserResponse> = rows.into_iter().map(user_response).collect();

    Ok(Json(serde_json::json!({ "users": users })))
}

/// POST /api/v1/users
pub async fn create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;

    let username = req.username.trim().to_string();
    if username.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "username must not be empty".to_string(),
        ));
    }
    if username.len() > crate::clickhouse_config::MAX_USERNAME_BYTES {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "username must not exceed {} bytes",
                crate::clickhouse_config::MAX_USERNAME_BYTES
            ),
        ));
    }
    let password = req.password.clone();
    if let Err(error) = crate::clickhouse_config::validate_password_policy(&password) {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("user.create", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("user", "password-policy-rejection")
                    .outcome("failure")
                    .changes(
                        serde_json::json!({
                            "username": username,
                            "reason": "password_policy",
                            "policy_code": error.code(),
                        })
                        .to_string(),
                    )
                    .description("user creation rejected by password policy")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        return Err((StatusCode::BAD_REQUEST, error.to_string()));
    }

    let display_name = req.display_name.as_deref().unwrap_or("").to_string();
    if display_name.len() > 255 {
        return Err((
            StatusCode::BAD_REQUEST,
            "display_name must not exceed 255 characters".to_string(),
        ));
    }

    let id = match state
        .config_db
        .create_user(&username, &password, &display_name)
        .await
    {
        Ok(id) => id,
        Err(error)
            if error
                .downcast_ref::<crate::clickhouse_config::PasswordPolicyError>()
                .is_some() =>
        {
            let policy = *error
                .downcast_ref::<crate::clickhouse_config::PasswordPolicyError>()
                .expect("guard checked password policy error");
            return Err((StatusCode::BAD_REQUEST, policy.to_string()));
        }
        Err(error)
            if error
                .downcast_ref::<crate::clickhouse_config::UsernameAlreadyExists>()
                .is_some() =>
        {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("user.create", "user")
                        .actor(caller.0.clone(), caller.1.clone())
                        .tenant(caller.3.clone())
                        .resource("user", "canonical-username-conflict")
                        .outcome("failure")
                        .changes(
                            serde_json::json!({
                                "username": username,
                                "reason": "canonical_username_conflict",
                            })
                            .to_string(),
                        )
                        .description("user creation rejected because username is already in use")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            return Err((
                StatusCode::CONFLICT,
                "username is already in use".to_string(),
            ));
        }
        Err(e) => {
            tracing::error!(error = %e, "internal error");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()));
        }
    };

    // New users default to the viewers group
    if let Err(error) = state
        .config_db
        .set_user_groups(&id, &["viewers".to_string()])
        .await
    {
        tracing::error!(user_id = %id, %error, "failed to assign default user group");
    }

    let row = state
        .config_db
        .get_user(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "user created but not found".to_string(),
            )
        })?;

    tracing::info!(
        event = "user_created",
        new_user = %username,
        admin = %caller.1,
        "user created"
    );

    // AUDIT: user creation. Never log the password — only username/display_name/id.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("user.create", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("user", id.clone())
                .changes(
                    serde_json::json!({ "username": username, "display_name": display_name })
                        .to_string(),
                )
                .description("user created")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok((StatusCode::CREATED, Json(user_response(row))))
}

/// DELETE /api/v1/users/{id}
pub async fn delete_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;

    // Refuse to delete the user named "admin"
    let username = state
        .config_db
        .get_username(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "user not found".to_string()))?;

    if username == "admin" {
        return Err((
            StatusCode::BAD_REQUEST,
            "cannot delete the admin user".to_string(),
        ));
    }

    let deleted = state.config_db.delete_user(&id).await.map_err(|e| {
        tracing::error!(error = %e, "internal error");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;

    if !deleted {
        return Err((StatusCode::NOT_FOUND, "user not found".to_string()));
    }

    tracing::info!(
        event = "user_deleted",
        deleted_user = %username,
        admin = %caller.1,
        "user deleted"
    );

    // AUDIT: user deletion.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("user.delete", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("user", id.clone())
                .changes(serde_json::json!({ "username": username }).to_string())
                .description("user deleted")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

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

    if req
        .current_password
        .as_deref()
        .is_some_and(|password| password.len() > crate::clickhouse_config::MAX_PASSWORD_BYTES)
    {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("user.password_change", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("user", id.clone())
                    .outcome("failure")
                    .changes(serde_json::json!({ "reason": "credential_bounds" }).to_string())
                    .description("password change rejected before credential processing")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "current_password must not exceed {} bytes",
                crate::clickhouse_config::MAX_PASSWORD_BYTES
            ),
        ));
    }

    // Admin can change any user's password; non-admin can only change their own.
    if caller.4 != "admin" && caller.0 != id {
        return Err((
            StatusCode::FORBIDDEN,
            "you can only change your own password".to_string(),
        ));
    }

    if let Err(error) = crate::clickhouse_config::validate_password_policy(&req.password) {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("user.password_change", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("user", id.clone())
                    .outcome("failure")
                    .changes(
                        serde_json::json!({
                            "reason": "password_policy",
                            "policy_code": error.code(),
                        })
                        .to_string(),
                    )
                    .description("password change rejected by password policy")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        return Err((StatusCode::BAD_REQUEST, error.to_string()));
    }

    let (target_username, auth_provider) = state
        .config_db
        .get_user_identity_provider(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "user not found".to_string()))?;

    if auth_provider != "local" {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("user.password_change", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("user", id.clone())
                    .outcome("failure")
                    .changes(
                        serde_json::json!({
                            "reason": "sso_managed_identity",
                            "auth_provider": auth_provider,
                        })
                        .to_string(),
                    )
                    .description("password change rejected for SSO-managed user")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        return Err((
            StatusCode::CONFLICT,
            "this user is managed by SSO; change credentials at the identity provider".to_string(),
        ));
    }

    // The break-glass account is deliberately outside SSO administration. It
    // can rotate its own password after step-up verification, but another
    // administrator cannot reset it.
    if crate::handlers::auth::is_break_glass_username(&target_username) && caller.0 != id {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("user.password_change", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("user", id.clone())
                    .outcome("failure")
                    .changes(serde_json::json!({ "reason": "break_glass_protected" }).to_string())
                    .description("password reset rejected for protected break-glass account")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        return Err((
            StatusCode::FORBIDDEN,
            "the break-glass account can only change its own password".to_string(),
        ));
    }

    // Self-service changes always require step-up verification, including for
    // administrators. Admin resets remain available for other local accounts.
    if caller.0 == id || caller.4 != "admin" {
        let current = req.current_password.as_deref().unwrap_or("");
        if current.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "current_password is required".to_string(),
            ));
        }
        match state.config_db.authenticate(&caller.1, current).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                state
                    .audit
                    .log(
                        crate::audit::AuditEvent::new("user.password_change", "user")
                            .actor(caller.0.clone(), caller.1.clone())
                            .tenant(caller.3.clone())
                            .resource("user", id.clone())
                            .outcome("failure")
                            .changes(serde_json::json!({ "reason": "step_up_failed" }).to_string())
                            .description(
                                "password change rejected after failed step-up authentication",
                            )
                            .context(crate::audit::actor_context_from_headers(&headers)),
                    )
                    .await;
                return Err((
                    StatusCode::FORBIDDEN,
                    "current password is incorrect".to_string(),
                ));
            }
            Err(error) => {
                tracing::error!(%error, "password verification failed");
                state
                    .audit
                    .log(
                        crate::audit::AuditEvent::new("user.password_change", "user")
                            .actor(caller.0.clone(), caller.1.clone())
                            .tenant(caller.3.clone())
                            .resource("user", id.clone())
                            .outcome("failure")
                            .changes(
                                serde_json::json!({ "reason": "identity_store_unavailable" })
                                    .to_string(),
                            )
                            .description(
                                "password change unavailable during step-up authentication",
                            )
                            .context(crate::audit::actor_context_from_headers(&headers)),
                    )
                    .await;
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "authentication temporarily unavailable".to_string(),
                ));
            }
        }
    }

    let outcome = match state.config_db.change_password(&id, &req.password).await {
        Ok(outcome) => outcome,
        Err(error)
            if error
                .downcast_ref::<crate::clickhouse_config::PasswordPolicyError>()
                .is_some() =>
        {
            let policy = *error
                .downcast_ref::<crate::clickhouse_config::PasswordPolicyError>()
                .expect("guard checked password policy error");
            return Err((StatusCode::BAD_REQUEST, policy.to_string()));
        }
        Err(error) => {
            tracing::error!(%error, "password change failed");
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("user.password_change", "user")
                        .actor(caller.0.clone(), caller.1.clone())
                        .tenant(caller.3.clone())
                        .resource("user", id.clone())
                        .outcome("failure")
                        .changes(
                            serde_json::json!({ "reason": "password_store_unavailable" })
                                .to_string(),
                        )
                        .description("password change failed before session version advanced")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "password change temporarily unavailable".to_string(),
            ));
        }
    };

    match outcome {
        crate::clickhouse_config::PasswordChangeOutcome::Updated => {}
        crate::clickhouse_config::PasswordChangeOutcome::UserNotFound => {
            return Err((StatusCode::NOT_FOUND, "user not found".to_string()));
        }
        crate::clickhouse_config::PasswordChangeOutcome::SsoManaged { auth_provider } => {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("user.password_change", "user")
                        .actor(caller.0.clone(), caller.1.clone())
                        .tenant(caller.3.clone())
                        .resource("user", id.clone())
                        .outcome("failure")
                        .changes(
                            serde_json::json!({
                                "reason": "sso_managed_identity",
                                "auth_provider": auth_provider,
                            })
                            .to_string(),
                        )
                        .description("password change rejected for SSO-managed user")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            return Err((
                StatusCode::CONFLICT,
                "this user is managed by SSO; change credentials at the identity provider"
                    .to_string(),
            ));
        }
    }

    // AUDIT: password change. NEVER log the password value.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("user.password_change", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("user", id.clone())
                .changes(
                    serde_json::json!({
                        "sessions_revoked": true,
                        "revocation_method": "user_version",
                    })
                    .to_string(),
                )
                .description(if caller.4 == "admin" && caller.0 != id {
                    "password reset by admin"
                } else {
                    "password changed"
                })
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(serde_json::json!({ "ok": true })))
}

/// PUT /api/v1/users/{id}/toggle
pub async fn toggle_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<ToggleUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;

    let updated = state
        .config_db
        .set_user_enabled(&id, req.enabled)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    if !updated {
        return Err((StatusCode::NOT_FOUND, "user not found".to_string()));
    }

    let row = state
        .config_db
        .get_user(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "user not found".to_string()))?;

    // AUDIT: user enable/disable.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("user.update", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("user", id.clone())
                .changes(serde_json::json!({ "enabled": req.enabled }).to_string())
                .description("user enabled state changed")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(user_response(row)))
}

#[cfg(test)]
mod authorization_tests {
    use super::{role_allows_admin, role_allows_write};

    #[test]
    fn session_role_matrix_is_fail_closed() {
        assert!(role_allows_admin("admin"));
        assert!(!role_allows_admin("write"));
        assert!(!role_allows_admin("viewer"));
        assert!(role_allows_write("admin"));
        assert!(role_allows_write("write"));
        assert!(!role_allows_write("viewer"));
        assert!(!role_allows_write("Admin"));
        assert!(!role_allows_write(""));
        assert!(!role_allows_write("unknown"));
    }
}
