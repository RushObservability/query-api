use axum::{
    Json,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::auth::extract_session_cookie;
use crate::models::dashboard::*;

/// Extract the calling user from the session cookie.
/// Returns (user_id, username, display_name, tenant_id, role).
/// Falls back to anonymous/default context when no session exists (backward compat).
fn resolve_caller(
    state: &AppState,
    headers: &HeaderMap,
    tenant: &TenantContext,
) -> (String, String, String, String, String) {
    if let Some(token) = extract_session_cookie(headers) {
        if let Some(info) = state.config_db.get_session_user(&token) {
            return info;
        }
    }
    // Unauthenticated: treat as anonymous user in the resolved tenant
    ("".to_string(), "".to_string(), "".to_string(), tenant.tenant_id.clone(), "admin".to_string())
}

pub async fn list_dashboards(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (user_id, _, _, _, _) = resolve_caller(&state, &headers, &tenant);
    let dashboards = state
        .config_db
        .list_dashboards(&tenant.tenant_id, &user_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "dashboards": dashboards })))
}

pub async fn create_dashboard(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(req): Json<CreateDashboardRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (user_id, _, _, _, _) = resolve_caller(&state, &headers, &tenant);

    // Validate visibility
    let visibility = match req.visibility.as_str() {
        "private" | "tenant" | "global" => &req.visibility,
        _ => return Err((StatusCode::BAD_REQUEST, format!("invalid visibility: {}", req.visibility))),
    };

    let tags_json = serde_json::to_string(&req.tags)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let id = uuid::Uuid::new_v4().to_string();
    state
        .config_db
        .create_dashboard(&id, &req.name, &req.description, &tenant.tenant_id, &user_id, visibility, &tags_json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let dashboard = state
        .config_db
        .get_dashboard(&id, &tenant.tenant_id, &user_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read created dashboard".to_string()))?;
    Ok((StatusCode::CREATED, Json(dashboard)))
}

pub async fn get_dashboard(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (user_id, _, _, _, _) = resolve_caller(&state, &headers, &tenant);
    let dashboard = state
        .config_db
        .get_dashboard(&id, &tenant.tenant_id, &user_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "dashboard not found".to_string()))?;
    let widgets = state
        .config_db
        .list_widgets(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let widget_responses: Vec<WidgetResponse> = widgets.into_iter().map(WidgetResponse::from).collect();
    Ok(Json(DashboardWithWidgets {
        dashboard,
        widgets: widget_responses,
    }))
}

pub async fn update_dashboard(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<UpdateDashboardRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (user_id, _, _, _, role) = resolve_caller(&state, &headers, &tenant);

    let tags_json = serde_json::to_string(&req.tags)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let updated = state
        .config_db
        .update_dashboard(&id, &req.name, &req.description, &req.visibility, &tags_json, &tenant.tenant_id, &user_id, &role)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "dashboard not found".to_string()));
    }
    let dashboard = state
        .config_db
        .get_dashboard(&id, &tenant.tenant_id, &user_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read dashboard".to_string()))?;
    Ok(Json(dashboard))
}

pub async fn delete_dashboard(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (user_id, _, _, _, role) = resolve_caller(&state, &headers, &tenant);
    let deleted = state
        .config_db
        .delete_dashboard(&id, &tenant.tenant_id, &user_id, &role)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "dashboard not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}

// ── Widget handlers ──

pub async fn create_widget(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(dashboard_id): Path<String>,
    Json(req): Json<CreateWidgetRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (user_id, _, _, _, _) = resolve_caller(&state, &headers, &tenant);

    // Verify dashboard exists and user has visibility
    state
        .config_db
        .get_dashboard(&dashboard_id, &tenant.tenant_id, &user_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "dashboard not found".to_string()))?;

    let valid_types = ["timeseries", "bar", "table", "counter"];
    if !valid_types.contains(&req.widget_type.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid widget_type: {}", req.widget_type)));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let query_config = serde_json::to_string(&req.query_config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let position = serde_json::to_string(&req.position)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let display_config = serde_json::to_string(&req.display_config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    state
        .config_db
        .create_widget(&id, &dashboard_id, &req.title, &req.widget_type, &query_config, &position, &display_config)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Read back the created widget
    let widgets = state
        .config_db
        .list_widgets(&dashboard_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let widget = widgets
        .into_iter()
        .find(|w| w.id == id)
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read created widget".to_string()))?;

    Ok((StatusCode::CREATED, Json(WidgetResponse::from(widget))))
}

pub async fn update_widget(
    State(state): State<AppState>,
    Path((dashboard_id, widget_id)): Path<(String, String)>,
    Json(req): Json<UpdateWidgetRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let valid_types = ["timeseries", "bar", "table", "counter"];
    if !valid_types.contains(&req.widget_type.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid widget_type: {}", req.widget_type)));
    }

    let query_config = serde_json::to_string(&req.query_config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let position = serde_json::to_string(&req.position)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let display_config = serde_json::to_string(&req.display_config)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let updated = state
        .config_db
        .update_widget(&widget_id, &dashboard_id, &req.title, &req.widget_type, &query_config, &position, &display_config)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "widget not found".to_string()));
    }

    let widgets = state
        .config_db
        .list_widgets(&dashboard_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let widget = widgets
        .into_iter()
        .find(|w| w.id == widget_id)
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read widget".to_string()))?;

    Ok(Json(WidgetResponse::from(widget)))
}

pub async fn delete_widget(
    State(state): State<AppState>,
    Path((dashboard_id, widget_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let deleted = state
        .config_db
        .delete_widget(&widget_id, &dashboard_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "widget not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}

// ── Export / Import handlers ──

pub async fn export_dashboard(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (user_id, _, _, _, _) = resolve_caller(&state, &headers, &tenant);
    let export = state
        .config_db
        .export_dashboard(&id, &tenant.tenant_id, &user_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "dashboard not found".to_string()))?;
    Ok(Json(export))
}

pub async fn import_dashboard(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(req): Json<ImportDashboardRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (user_id, _, _, _, role) = resolve_caller(&state, &headers, &tenant);
    let dashboard = state
        .config_db
        .import_dashboard(&req, &tenant.tenant_id, &user_id, &role)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok((StatusCode::CREATED, Json(dashboard)))
}

// ── Template handlers ──

pub async fn list_dashboard_templates(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let templates = state
        .config_db
        .list_dashboard_templates()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "templates": templates })))
}

pub async fn create_from_template(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(template_id): Path<String>,
    Json(req): Json<CreateFromTemplateRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (user_id, _, _, _, _) = resolve_caller(&state, &headers, &tenant);

    let template = state
        .config_db
        .get_dashboard_template(&template_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "template not found".to_string()))?;

    // Parse template_json to get widgets
    let widgets_val = template.template_json.get("widgets")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let widget_exports: Vec<WidgetExport> = widgets_val
        .into_iter()
        .filter_map(|v| serde_json::from_value(v).ok())
        .collect();

    // Create dashboard
    let dash_id = uuid::Uuid::new_v4().to_string();
    let tags = serde_json::to_string(&template.tags)
        .unwrap_or_else(|_| "[]".to_string());
    state
        .config_db
        .create_dashboard(&dash_id, &req.name, &template.description, &tenant.tenant_id, &user_id, "tenant", &tags)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Create widgets from template
    for w in &widget_exports {
        let wid = uuid::Uuid::new_v4().to_string();
        let qc = serde_json::to_string(&w.query_config)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let pos = serde_json::to_string(&w.position)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let dc = serde_json::to_string(&w.display_config)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        state
            .config_db
            .create_widget(&wid, &dash_id, &w.title, &w.widget_type, &qc, &pos, &dc)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    let dashboard = state
        .config_db
        .get_dashboard(&dash_id, &tenant.tenant_id, &user_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read created dashboard".to_string()))?;

    Ok((StatusCode::CREATED, Json(dashboard)))
}
