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
fn require_auth(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(String, String, String, String, String), (StatusCode, String)> {
    let token = extract_session_cookie(headers).ok_or_else(|| {
        (StatusCode::UNAUTHORIZED, "not authenticated".to_string())
    })?;
    state
        .config_db
        .get_session_user(&token)
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "session expired or invalid".to_string(),
            )
        })
}

/// Require that the caller is an admin.
fn require_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(String, String, String, String, String), (StatusCode, String)> {
    let caller = require_auth(state, headers)?;
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
    row: (String, String, String, String, String, bool, String, Vec<String>),
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
    require_auth(&state, &headers)?;

    let rows = state
        .config_db
        .list_groups()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

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
    require_admin(&state, &headers)?;

    let name = req.name.trim().to_string();
    if name.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name must not be empty".to_string()));
    }

    let description = req.description.unwrap_or_default();
    let scopes = serde_json::to_string(&req.scopes.unwrap_or_else(|| vec!["all".to_string()]))
        .unwrap_or_else(|_| "[\"all\"]".to_string());
    let permissions = serde_json::to_string(&req.permissions.unwrap_or_else(|| vec!["read".to_string()]))
        .unwrap_or_else(|_| "[\"read\"]".to_string());

    let id = state
        .config_db
        .create_group(&name, &description, &scopes, &permissions)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let row = state
        .config_db
        .get_group(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "group created but not found".to_string(),
            )
        })?;

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
    require_admin(&state, &headers)?;

    // Get current group to use as defaults
    let current = state
        .config_db
        .get_group(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
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
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    if !updated {
        return Err((StatusCode::NOT_FOUND, "group not found".to_string()));
    }

    let row = state
        .config_db
        .get_group(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "group not found".to_string()))?;

    Ok(Json(group_response(row)))
}

/// DELETE /api/v1/groups/{id}
pub async fn delete_group(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;

    match state
        .config_db
        .delete_group(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
    {
        Ok(true) => Ok(StatusCode::NO_CONTENT),
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
    require_admin(&state, &headers)?;

    // Verify group exists
    state
        .config_db
        .get_group(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "group not found".to_string()))?;

    state
        .config_db
        .set_group_tenants(&id, &req.tenant_ids)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let row = state
        .config_db
        .get_group(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "group not found".to_string()))?;

    Ok(Json(group_response(row)))
}

/// GET /api/v1/users/{user_id}/groups
pub async fn get_user_groups(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers)?;

    let group_ids = state
        .config_db
        .get_user_groups(&user_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

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
    require_admin(&state, &headers)?;

    state
        .config_db
        .set_user_groups(&user_id, &req.group_ids)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let group_ids = state
        .config_db
        .get_user_groups(&user_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    Ok(Json(serde_json::json!({ "group_ids": group_ids })))
}
