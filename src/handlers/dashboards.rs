use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::AppState;
use crate::models::dashboard::*;

pub async fn list_dashboards(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let dashboards = state
        .config_db
        .list_dashboards()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "dashboards": dashboards })))
}

pub async fn create_dashboard(
    State(state): State<AppState>,
    Json(req): Json<CreateDashboardRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let id = uuid::Uuid::new_v4().to_string();
    state
        .config_db
        .create_dashboard(&id, &req.name, &req.description)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let dashboard = state
        .config_db
        .get_dashboard(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read created dashboard".to_string()))?;
    Ok((StatusCode::CREATED, Json(dashboard)))
}

pub async fn get_dashboard(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let dashboard = state
        .config_db
        .get_dashboard(&id)
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
    Path(id): Path<String>,
    Json(req): Json<UpdateDashboardRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let updated = state
        .config_db
        .update_dashboard(&id, &req.name, &req.description)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "dashboard not found".to_string()));
    }
    let dashboard = state
        .config_db
        .get_dashboard(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "failed to read dashboard".to_string()))?;
    Ok(Json(dashboard))
}

pub async fn delete_dashboard(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let deleted = state
        .config_db
        .delete_dashboard(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "dashboard not found".to_string()));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn create_widget(
    State(state): State<AppState>,
    Path(dashboard_id): Path<String>,
    Json(req): Json<CreateWidgetRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify dashboard exists
    state
        .config_db
        .get_dashboard(&dashboard_id)
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
