use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use uuid::Uuid;

use crate::AppState;
use crate::handlers::users::require_admin;

#[derive(serde::Deserialize)]
pub struct CreateTenantRequest {
    pub name: String,
}

#[derive(serde::Deserialize)]
pub struct ToggleTenantRequest {
    pub enabled: bool,
}

#[derive(serde::Deserialize)]
pub struct SetAuthRequiredRequest {
    pub auth_required: bool,
}

#[derive(serde::Serialize)]
pub struct TenantResponse {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub auth_required: bool,
    pub created_at: String,
}

pub async fn list_tenants(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let rows = state
        .config_db
        .list_tenants()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let tenants: Vec<TenantResponse> = rows
        .into_iter()
        .map(|(id, name, enabled, auth_required, created_at)| TenantResponse {
            id,
            name,
            enabled,
            auth_required,
            created_at,
        })
        .collect();

    Ok(Json(serde_json::json!({ "tenants": tenants })))
}

pub async fn create_tenant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateTenantRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let name = req.name.trim().to_string();
    if name.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name must not be empty".to_string()));
    }

    let id = Uuid::new_v4().to_string();

    state
        .config_db
        .create_tenant(&id, &name)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let tenant = state
        .config_db
        .get_tenant(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "tenant created but not found".to_string(),
            )
        })?;

    Ok((
        StatusCode::CREATED,
        Json(TenantResponse {
            id: tenant.0,
            name: tenant.1,
            enabled: tenant.2,
            auth_required: tenant.3, created_at: tenant.4,
        }),
    ))
}

pub async fn toggle_tenant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<ToggleTenantRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let updated = state
        .config_db
        .set_tenant_enabled(&id, req.enabled)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    if !updated {
        return Err((StatusCode::NOT_FOUND, "tenant not found".to_string()));
    }

    let tenant = state
        .config_db
        .get_tenant(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    Ok(Json(TenantResponse {
        id: tenant.0,
        name: tenant.1,
        enabled: tenant.2,
        auth_required: tenant.3, created_at: tenant.4,
    }))
}

pub async fn delete_tenant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    if id == "default" {
        return Err((
            StatusCode::BAD_REQUEST,
            "cannot delete the default tenant".to_string(),
        ));
    }

    let deleted = state
        .config_db
        .delete_tenant(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    if !deleted {
        return Err((StatusCode::NOT_FOUND, "tenant not found".to_string()));
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn set_auth_required(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<SetAuthRequiredRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let updated = state
        .config_db
        .set_tenant_auth_required(&id, req.auth_required)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    if !updated {
        return Err((StatusCode::NOT_FOUND, "tenant not found".to_string()));
    }

    let tenant = state
        .config_db
        .get_tenant(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    Ok(Json(TenantResponse {
        id: tenant.0,
        name: tenant.1,
        enabled: tenant.2,
        auth_required: tenant.3,
        created_at: tenant.4,
    }))
}
