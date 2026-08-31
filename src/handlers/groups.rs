use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::handlers::auth::extract_session_cookie;

/// Extract the calling user from the session cookie.
/// Returns (user_id, username, display_name, tenant_id, role) or 401.
async fn require_auth(
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
async fn require_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(String, String, String, String, String), (StatusCode, String)> {
    let caller = require_auth(state, headers).await?;
    if caller.4 != "admin" {
        return Err((StatusCode::FORBIDDEN, "admin role required".to_string()));
    }
    Ok(caller)
}

#[derive(serde::Serialize)]
pub struct GroupResponse {
    pub id: String,
    pub name: String,
    pub description: String,
    pub scopes: Vec<String>,
    pub permissions: Vec<String>,
    pub system: bool,
    pub tenant_ids: Vec<String>,
    pub created_at: String,
}

fn group_response(
    row: (
        String,
        String,
        String,
        String,
        String,
        bool,
        String,
        Vec<String>,
    ),
) -> GroupResponse {
    GroupResponse {
        id: row.0,
        name: row.1,
        description: row.2,
        scopes: serde_json::from_str(&row.3).unwrap_or_default(),
        permissions: serde_json::from_str(&row.4).unwrap_or_default(),
        system: row.5,
        created_at: row.6,
        tenant_ids: row.7,
    }
}

/// GET /api/v1/groups
pub async fn list_groups(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;

    let rows = state.config_db.list_groups().await.map_err(|e| {
        tracing::error!(error = %e, "internal error");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;

    let groups: Vec<GroupResponse> = rows.into_iter().map(group_response).collect();

    Ok(Json(serde_json::json!({ "groups": groups })))
}

#[derive(serde::Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
    pub description: Option<String>,
    pub scopes: Option<Vec<String>>,
    pub permissions: Option<Vec<String>>,
}

/// POST /api/v1/groups
pub async fn create_group(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateGroupRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;

    let name = req.name.trim().to_string();
    if name.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must not be empty".to_string(),
        ));
    }
    if name.len() > 100 {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must not exceed 100 characters".to_string(),
        ));
    }

    let description = req.description.unwrap_or_default();
    let scopes = serde_json::to_string(&req.scopes.unwrap_or_else(|| vec!["all".to_string()]))
        .unwrap_or_else(|_| "[\"all\"]".to_string());
    let permissions =
        serde_json::to_string(&req.permissions.unwrap_or_else(|| vec!["read".to_string()]))
            .unwrap_or_else(|_| "[\"read\"]".to_string());

    let id = state
        .config_db
        .create_group(&name, &description, &scopes, &permissions)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    let row = state
        .config_db
        .get_group(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "group created but not found".to_string(),
            )
        })?;

    // AUDIT: group creation.
    state.audit.log(
        crate::audit::AuditEvent::new("group.create", "user")
            .actor(caller.0.clone(), caller.1.clone())
            .tenant(caller.3.clone())
            .resource("group", id.clone())
            .changes(serde_json::json!({ "name": name, "scopes": scopes, "permissions": permissions }).to_string())
            .description("group created")
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;

    Ok((StatusCode::CREATED, Json(group_response(row))))
}

#[derive(serde::Deserialize)]
pub struct UpdateGroupRequest {
    pub description: Option<String>,
    pub scopes: Option<Vec<String>>,
    pub permissions: Option<Vec<String>>,
}

/// PUT /api/v1/groups/{id}
pub async fn update_group(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<UpdateGroupRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let permissions_changed = req.permissions.is_some();

    // Get current group to use as defaults
    let current = state
        .config_db
        .get_group(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "group not found".to_string()))?;

    let description = req.description.unwrap_or(current.2);
    let scopes = match req.scopes {
        Some(s) => serde_json::to_string(&s).unwrap_or_else(|_| current.3.clone()),
        None => current.3,
    };
    let permissions = match req.permissions {
        Some(p) => serde_json::to_string(&p).unwrap_or_else(|_| current.4.clone()),
        None => current.4,
    };

    let updated = state
        .config_db
        .update_group(&id, &description, &scopes, &permissions)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    if !updated {
        return Err((StatusCode::NOT_FOUND, "group not found".to_string()));
    }

    let row = state
        .config_db
        .get_group(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "group not found".to_string()))?;

    // AUDIT: group update (scopes/permissions/description).
    state.audit.log(
        crate::audit::AuditEvent::new("group.update", "user")
            .actor(caller.0.clone(), caller.1.clone())
            .tenant(caller.3.clone())
            .resource("group", id.clone())
            .changes(serde_json::json!({ "description": description, "scopes": scopes, "permissions": permissions }).to_string())
            .description("group updated")
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;
    if permissions_changed {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("group.permissions_change", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("group", id.clone())
                    .changes(
                        serde_json::json!({ "scopes": scopes, "permissions": permissions })
                            .to_string(),
                    )
                    .description("group permissions changed")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }

    Ok(Json(group_response(row)))
}

/// DELETE /api/v1/groups/{id}
pub async fn delete_group(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;

    match state.config_db.delete_group(&id).await.map_err(|e| {
        tracing::error!(error = %e, "internal error");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })? {
        Ok(true) => {
            // AUDIT: group deletion.
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("group.delete", "user")
                        .actor(caller.0.clone(), caller.1.clone())
                        .tenant(caller.3.clone())
                        .resource("group", id.clone())
                        .description("group deleted")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            Ok(StatusCode::NO_CONTENT)
        }
        Ok(false) => Err((StatusCode::NOT_FOUND, "group not found".to_string())),
        Err(msg) => Err((StatusCode::BAD_REQUEST, msg)),
    }
}

#[derive(serde::Deserialize)]
pub struct SetGroupTenantsRequest {
    pub tenant_ids: Vec<String>,
}

/// PUT /api/v1/groups/{id}/tenants
pub async fn set_group_tenants(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<SetGroupTenantsRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;

    // Verify group exists
    state
        .config_db
        .get_group(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "group not found".to_string()))?;

    state
        .config_db
        .set_group_tenants(&id, &req.tenant_ids)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    let row = state
        .config_db
        .get_group(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "group not found".to_string()))?;

    // AUDIT: group-to-tenant binding change.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("group.tenant_binding_change", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("group", id.clone())
                .changes(serde_json::json!({ "tenant_ids": req.tenant_ids }).to_string())
                .description("group tenant bindings changed")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(group_response(row)))
}

/// GET /api/v1/users/{user_id}/groups
pub async fn get_user_groups(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;

    let group_ids = state
        .config_db
        .get_user_groups(&user_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    Ok(Json(serde_json::json!({ "group_ids": group_ids })))
}

#[derive(serde::Deserialize)]
pub struct SetUserGroupsRequest {
    pub group_ids: Vec<String>,
}

/// PUT /api/v1/users/{user_id}/groups
pub async fn set_user_groups(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
    Json(req): Json<SetUserGroupsRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;

    state
        .config_db
        .set_user_groups(&user_id, &req.group_ids)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    let group_ids = state
        .config_db
        .get_user_groups(&user_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    // AUDIT: user group membership change (effective role/permission change).
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("user.role_change", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("user", user_id.clone())
                .changes(serde_json::json!({ "group_ids": req.group_ids }).to_string())
                .description("user group membership changed")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(serde_json::json!({ "group_ids": group_ids })))
}
