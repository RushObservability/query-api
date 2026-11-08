//! Incident notebooks: notes and query cells about one incident.

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::AppState;
use crate::TenantContext;
use crate::handlers::users::{require_principal, require_principal_write};
use crate::models::notebook::*;

type ApiResult<T> = Result<T, (StatusCode, String)>;

fn bad(message: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, message.into())
}

fn db_err(e: anyhow::Error) -> (StatusCode, String) {
    crate::api_error::internal_legacy("notebooks", e)
}

fn not_found(what: &str) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, format!("{what} not found"))
}

/// Reject a write based on a stale copy: the client sends the `updated_at` it loaded.
fn check_fresh(stored: &str, loaded: &str, what: &str) -> ApiResult<()> {
    if stored != loaded {
        return Err((
            StatusCode::CONFLICT,
            format!("{what} was changed by someone else; reload it"),
        ));
    }
    Ok(())
}

fn parsed(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| json!({}))
}

fn notebook_json(nb: &Notebook) -> Value {
    json!({
        "id": nb.id,
        "name": nb.name,
        "description": nb.description,
        "owner_id": nb.owner_id,
        "owner_name": nb.owner_name,
        "visibility": nb.visibility,
        "status": nb.status,
        "incident_start": nb.incident_start,
        "incident_end": nb.incident_end,
        "tags": serde_json::from_str::<Vec<String>>(&nb.tags).unwrap_or_default(),
        "created_at": nb.created_at,
        "updated_at": nb.updated_at,
    })
}

fn cell_json(cell: &NotebookCell) -> Value {
    json!({
        "id": cell.id,
        "notebook_id": cell.notebook_id,
        "position": cell.position,
        "cell_type": cell.cell_type,
        "title": cell.title,
        "body": cell.body,
        "query_config": parsed(&cell.query_config),
        "time": parsed(&cell.time),
        "display_config": parsed(&cell.display_config),
        "author_id": cell.author_id,
        "author_name": cell.author_name,
        "created_at": cell.created_at,
        "updated_at": cell.updated_at,
    })
}

fn check_name(name: &str) -> ApiResult<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(bad("name must not be empty"));
    }
    if name.len() > 255 {
        return Err(bad("name must not exceed 255 characters"));
    }
    Ok(name.to_string())
}

fn check_description(description: &str) -> ApiResult<()> {
    if description.len() > 1024 {
        return Err(bad("description must not exceed 1024 characters"));
    }
    Ok(())
}

fn check_visibility(visibility: &str) -> ApiResult<()> {
    match visibility {
        "private" | "tenant" => Ok(()),
        other => Err(bad(format!("invalid visibility: {other}"))),
    }
}

fn tags_json(tags: &[String]) -> ApiResult<String> {
    if tags.len() > 20 || tags.iter().any(|t| t.is_empty() || t.len() > 64) {
        return Err(bad("use up to 20 tags of 1 to 64 characters"));
    }
    Ok(serde_json::to_string(tags).expect("strings serialize"))
}

fn check_title(title: &str) -> ApiResult<()> {
    if title.len() > 255 {
        return Err(bad("title must not exceed 255 characters"));
    }
    Ok(())
}

fn check_body(body: &str) -> ApiResult<()> {
    if body.len() > MAX_BODY_BYTES {
        return Err(bad(format!("body must not exceed {MAX_BODY_BYTES} bytes")));
    }
    Ok(())
}

fn time_json(time: CellTime) -> ApiResult<(String, &'static str)> {
    let time = time.validated().map_err(bad)?;
    Ok((
        serde_json::to_string(&time).expect("time serializes"),
        time.mode(),
    ))
}

async fn load_notebook(
    state: &AppState,
    id: &str,
    tenant: &TenantContext,
    user_id: &str,
) -> ApiResult<Notebook> {
    state
        .config_db
        .get_notebook(id, &tenant.tenant_id, user_id)
        .await
        .map_err(db_err)?
        .ok_or_else(|| not_found("notebook"))
}

async fn load_cells(
    state: &AppState,
    notebook_id: &str,
    tenant: &TenantContext,
) -> ApiResult<Vec<NotebookCell>> {
    state
        .config_db
        .list_notebook_cells(notebook_id, &tenant.tenant_id)
        .await
        .map_err(db_err)
}

async fn audit(
    state: &AppState,
    identity: &crate::RequestIdentity,
    headers: &HeaderMap,
    action: &str,
    resource: (&str, &str),
    changes: Value,
) {
    state
        .audit
        .log(
            crate::audit::AuditEvent::for_request(action, identity)
                .resource(resource.0, resource.1)
                .outcome("success")
                .changes(changes.to_string())
                .context(crate::audit::actor_context_from_headers(headers)),
        )
        .await;
}

/// GET /api/v1/notebooks — shared notebooks and the caller's private ones.
pub async fn list_notebooks(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
) -> ApiResult<impl IntoResponse> {
    let caller = require_principal(&state, &headers).await?;
    let notebooks = state
        .config_db
        .list_notebooks(&tenant.tenant_id, &caller.0)
        .await
        .map_err(db_err)?;
    let notebooks: Vec<Value> = notebooks.iter().map(notebook_json).collect();
    Ok(Json(json!({ "notebooks": notebooks })))
}

#[derive(Debug, Deserialize)]
pub struct CreateNotebookRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "tenant_visibility")]
    pub visibility: String,
    pub incident_start: String,
    #[serde(default)]
    pub incident_end: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

fn tenant_visibility() -> String {
    "tenant".into()
}

/// POST /api/v1/notebooks
pub async fn create_notebook(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Extension(identity): Extension<crate::RequestIdentity>,
    headers: HeaderMap,
    Json(req): Json<CreateNotebookRequest>,
) -> ApiResult<impl IntoResponse> {
    let caller = require_principal_write(&state, &headers).await?;
    let name = check_name(&req.name)?;
    check_description(&req.description)?;
    check_visibility(&req.visibility)?;
    let (incident_start, incident_end) =
        validate_incident_window(&req.incident_start, &req.incident_end).map_err(bad)?;
    let now = stamp(chrono::Utc::now());
    let nb = Notebook {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: tenant.tenant_id.clone(),
        name,
        description: req.description,
        owner_id: caller.0.clone(),
        owner_name: caller.1.clone(),
        visibility: req.visibility,
        status: "open".into(),
        incident_start,
        incident_end,
        tags: tags_json(&req.tags)?,
        created_at: now.clone(),
        updated_at: now,
    };
    state
        .config_db
        .write_notebook(&nb, false)
        .await
        .map_err(db_err)?;

    // AUDIT: notebook created.
    audit(
        &state,
        &identity,
        &headers,
        "notebook.create",
        ("notebook", &nb.id),
        json!({
            "name": nb.name,
            "visibility": nb.visibility,
            "incident_start": nb.incident_start,
            "incident_end": nb.incident_end,
        }),
    )
    .await;

    let mut body = notebook_json(&nb);
    body["cells"] = json!([]);
    Ok((StatusCode::CREATED, Json(body)))
}

/// GET /api/v1/notebooks/{id} — the notebook and its cells in order.
pub async fn get_notebook(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<impl IntoResponse> {
    let caller = require_principal(&state, &headers).await?;
    let nb = load_notebook(&state, &id, &tenant, &caller.0).await?;
    let cells = load_cells(&state, &nb.id, &tenant).await?;
    let mut body = notebook_json(&nb);
    body["cells"] = cells.iter().map(cell_json).collect();
    Ok(Json(body))
}

#[derive(Debug, Deserialize)]
pub struct UpdateNotebookRequest {
    /// The `updated_at` the client loaded; a newer stored row is a 409.
    pub updated_at: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub visibility: Option<String>,
    pub status: Option<String>,
    pub incident_start: Option<String>,
    /// Empty string marks the incident ongoing.
    pub incident_end: Option<String>,
    pub tags: Option<Vec<String>>,
}

/// PUT /api/v1/notebooks/{id} — fields left out keep their value.
pub async fn update_notebook(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Extension(identity): Extension<crate::RequestIdentity>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<UpdateNotebookRequest>,
) -> ApiResult<impl IntoResponse> {
    let caller = require_principal_write(&state, &headers).await?;
    let before = load_notebook(&state, &id, &tenant, &caller.0).await?;
    check_fresh(&before.updated_at, &req.updated_at, "notebook")?;

    let mut nb = before.clone();
    if let Some(name) = req.name {
        nb.name = check_name(&name)?;
    }
    if let Some(description) = req.description {
        check_description(&description)?;
        nb.description = description;
    }
    if let Some(visibility) = req.visibility {
        check_visibility(&visibility)?;
        if visibility != nb.visibility && nb.owner_id != caller.0 && caller.4 != "admin" {
            return Err((
                StatusCode::FORBIDDEN,
                "only the owner or an admin can change visibility".into(),
            ));
        }
        nb.visibility = visibility;
    }
    if let Some(status) = req.status {
        if !matches!(status.as_str(), "open" | "resolved") {
            return Err(bad(format!("invalid status: {status}")));
        }
        nb.status = status;
    }
    if req.incident_start.is_some() || req.incident_end.is_some() {
        let start = req.incident_start.unwrap_or(nb.incident_start.clone());
        let end = req.incident_end.unwrap_or(nb.incident_end.clone());
        (nb.incident_start, nb.incident_end) =
            validate_incident_window(&start, &end).map_err(bad)?;
    }
    if let Some(tags) = req.tags {
        nb.tags = tags_json(&tags)?;
    }
    nb.updated_at = stamp(chrono::Utc::now());
    state
        .config_db
        .write_notebook(&nb, false)
        .await
        .map_err(db_err)?;

    // AUDIT: notebook updated (only the fields that changed).
    let mut changed = serde_json::Map::new();
    for (field, old, new) in [
        ("name", &before.name, &nb.name),
        ("visibility", &before.visibility, &nb.visibility),
        ("status", &before.status, &nb.status),
        ("incident_start", &before.incident_start, &nb.incident_start),
        ("incident_end", &before.incident_end, &nb.incident_end),
        ("tags", &before.tags, &nb.tags),
    ] {
        if old != new {
            changed.insert(field.into(), json!({ "before": old, "after": new }));
        }
    }
    if before.description != nb.description {
        changed.insert("description".into(), json!("changed"));
    }
    audit(
        &state,
        &identity,
        &headers,
        "notebook.update",
        ("notebook", &nb.id),
        Value::Object(changed),
    )
    .await;

    Ok(Json(notebook_json(&nb)))
}

/// DELETE /api/v1/notebooks/{id} — owner or admin only.
pub async fn delete_notebook(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Extension(identity): Extension<crate::RequestIdentity>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<impl IntoResponse> {
    let caller = require_principal_write(&state, &headers).await?;
    let nb = load_notebook(&state, &id, &tenant, &caller.0).await?;
    if nb.owner_id != caller.0 && caller.4 != "admin" {
        return Err((
            StatusCode::FORBIDDEN,
            "only the owner or an admin can delete a notebook".into(),
        ));
    }
    state
        .config_db
        .write_notebook(&nb, true)
        .await
        .map_err(db_err)?;

    // AUDIT: notebook deleted.
    audit(
        &state,
        &identity,
        &headers,
        "notebook.delete",
        ("notebook", &nb.id),
        json!({ "name": nb.name, "owner_id": nb.owner_id }),
    )
    .await;

    Ok(Json(json!({ "ok": true })))
}

#[derive(Debug, Deserialize)]
pub struct CreateCellRequest {
    pub cell_type: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub body: String,
    pub query_config: Option<Value>,
    #[serde(default)]
    pub time: CellTime,
    pub display_config: Option<Value>,
    /// Defaults to after the last cell.
    pub position: Option<u32>,
}

/// POST /api/v1/notebooks/{id}/cells
pub async fn create_cell(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Extension(identity): Extension<crate::RequestIdentity>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<CreateCellRequest>,
) -> ApiResult<impl IntoResponse> {
    let caller = require_principal_write(&state, &headers).await?;
    let nb = load_notebook(&state, &id, &tenant, &caller.0).await?;
    if !CELL_TYPES.contains(&req.cell_type.as_str()) {
        return Err(bad(format!("invalid cell_type: {}", req.cell_type)));
    }
    check_title(&req.title)?;
    check_body(&req.body)?;
    let (time, mode) = time_json(req.time)?;
    let query_config = config_json(req.query_config, "query_config").map_err(bad)?;
    let display_config = config_json(req.display_config, "display_config").map_err(bad)?;
    let cells = load_cells(&state, &nb.id, &tenant).await?;
    if cells.len() as u64 >= MAX_CELLS {
        return Err(bad(format!("a notebook holds at most {MAX_CELLS} cells")));
    }
    let position = req.position.unwrap_or_else(|| {
        cells
            .iter()
            .map(|c| c.position)
            .max()
            .unwrap_or(0)
            .saturating_add(POSITION_STEP)
    });
    let now = stamp(chrono::Utc::now());
    let cell = NotebookCell {
        id: uuid::Uuid::new_v4().to_string(),
        notebook_id: nb.id.clone(),
        tenant_id: tenant.tenant_id.clone(),
        position,
        cell_type: req.cell_type,
        title: req.title,
        body: req.body,
        query_config,
        time,
        display_config,
        author_id: caller.0.clone(),
        author_name: caller.1.clone(),
        created_at: now.clone(),
        updated_at: now,
    };
    state
        .config_db
        .write_notebook_cell(&cell, false)
        .await
        .map_err(db_err)?;

    // AUDIT: cell added. Note bodies and query text stay out of the log.
    audit(
        &state,
        &identity,
        &headers,
        "notebook.cell_create",
        ("notebook_cell", &cell.id),
        json!({
            "notebook_id": nb.id,
            "cell_type": cell.cell_type,
            "title": cell.title,
            "time_mode": mode,
        }),
    )
    .await;

    Ok((StatusCode::CREATED, Json(cell_json(&cell))))
}

#[derive(Debug, Deserialize)]
pub struct UpdateCellRequest {
    /// The `updated_at` the client loaded; a newer stored row is a 409.
    pub updated_at: String,
    pub title: Option<String>,
    pub body: Option<String>,
    pub query_config: Option<Value>,
    pub time: Option<CellTime>,
    pub display_config: Option<Value>,
}

/// PUT /api/v1/notebooks/{id}/cells/{cell_id} — fields left out keep their value.
pub async fn update_cell(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Extension(identity): Extension<crate::RequestIdentity>,
    headers: HeaderMap,
    Path((id, cell_id)): Path<(String, String)>,
    Json(req): Json<UpdateCellRequest>,
) -> ApiResult<impl IntoResponse> {
    let caller = require_principal_write(&state, &headers).await?;
    let nb = load_notebook(&state, &id, &tenant, &caller.0).await?;
    let before = load_cells(&state, &nb.id, &tenant)
        .await?
        .into_iter()
        .find(|c| c.id == cell_id)
        .ok_or_else(|| not_found("cell"))?;
    check_fresh(&before.updated_at, &req.updated_at, "cell")?;

    let mut cell = before.clone();
    let mut changed: Vec<&str> = Vec::new();
    if let Some(title) = req.title {
        check_title(&title)?;
        cell.title = title;
    }
    if let Some(body) = req.body {
        check_body(&body)?;
        cell.body = body;
    }
    if req.query_config.is_some() {
        cell.query_config = config_json(req.query_config, "query_config").map_err(bad)?;
    }
    if req.display_config.is_some() {
        cell.display_config = config_json(req.display_config, "display_config").map_err(bad)?;
    }
    let mut time_mode = None;
    if let Some(time) = req.time {
        let (time, mode) = time_json(time)?;
        cell.time = time;
        time_mode = Some(mode);
    }
    for (field, old, new) in [
        ("title", &before.title, &cell.title),
        ("body", &before.body, &cell.body),
        ("query_config", &before.query_config, &cell.query_config),
        (
            "display_config",
            &before.display_config,
            &cell.display_config,
        ),
        ("time", &before.time, &cell.time),
    ] {
        if old != new {
            changed.push(field);
        }
    }
    cell.updated_at = stamp(chrono::Utc::now());
    state
        .config_db
        .write_notebook_cell(&cell, false)
        .await
        .map_err(db_err)?;

    // AUDIT: cell edited. Names the changed fields, never their contents.
    audit(
        &state,
        &identity,
        &headers,
        "notebook.cell_update",
        ("notebook_cell", &cell.id),
        json!({
            "notebook_id": nb.id,
            "cell_type": cell.cell_type,
            "fields": changed,
            "time_mode": time_mode,
        }),
    )
    .await;

    Ok(Json(cell_json(&cell)))
}

/// DELETE /api/v1/notebooks/{id}/cells/{cell_id}
pub async fn delete_cell(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Extension(identity): Extension<crate::RequestIdentity>,
    headers: HeaderMap,
    Path((id, cell_id)): Path<(String, String)>,
) -> ApiResult<impl IntoResponse> {
    let caller = require_principal_write(&state, &headers).await?;
    let nb = load_notebook(&state, &id, &tenant, &caller.0).await?;
    let cell = load_cells(&state, &nb.id, &tenant)
        .await?
        .into_iter()
        .find(|c| c.id == cell_id)
        .ok_or_else(|| not_found("cell"))?;
    state
        .config_db
        .write_notebook_cell(&cell, true)
        .await
        .map_err(db_err)?;

    // AUDIT: cell removed.
    audit(
        &state,
        &identity,
        &headers,
        "notebook.cell_delete",
        ("notebook_cell", &cell.id),
        json!({ "notebook_id": nb.id, "cell_type": cell.cell_type, "title": cell.title }),
    )
    .await;

    Ok(Json(json!({ "ok": true })))
}

#[derive(Debug, Deserialize)]
pub struct ReorderRequest {
    /// Every cell id in the new order.
    pub cell_ids: Vec<String>,
}

/// The new position of each cell whose position changes. `cell_ids` must name
/// every current cell exactly once.
fn reorder_positions(
    cells: &[NotebookCell],
    cell_ids: &[String],
) -> Result<Vec<(usize, u32)>, String> {
    let mut seen = std::collections::HashSet::new();
    if cell_ids.len() != cells.len() || !cell_ids.iter().all(|id| seen.insert(id)) {
        return Err("cell_ids must list every cell exactly once".into());
    }
    let mut moves = Vec::new();
    for (rank, id) in cell_ids.iter().enumerate() {
        let index = cells
            .iter()
            .position(|c| &c.id == id)
            .ok_or("cell_ids must list every cell exactly once")?;
        let position = (rank as u32 + 1) * POSITION_STEP;
        if cells[index].position != position {
            moves.push((index, position));
        }
    }
    Ok(moves)
}

/// POST /api/v1/notebooks/{id}/cells/reorder
pub async fn reorder_cells(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Extension(identity): Extension<crate::RequestIdentity>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<ReorderRequest>,
) -> ApiResult<impl IntoResponse> {
    let caller = require_principal_write(&state, &headers).await?;
    let nb = load_notebook(&state, &id, &tenant, &caller.0).await?;
    let mut cells = load_cells(&state, &nb.id, &tenant).await?;
    let moves = reorder_positions(&cells, &req.cell_ids).map_err(bad)?;
    let now = stamp(chrono::Utc::now());
    for (index, position) in moves {
        let cell = &mut cells[index];
        cell.position = position;
        cell.updated_at = now.clone();
        state
            .config_db
            .write_notebook_cell(cell, false)
            .await
            .map_err(db_err)?;
    }

    // AUDIT: cells reordered.
    audit(
        &state,
        &identity,
        &headers,
        "notebook.cell_reorder",
        ("notebook", &nb.id),
        json!({ "cell_ids": req.cell_ids }),
    )
    .await;

    cells.sort_by_key(|c| c.position);
    let cells: Vec<Value> = cells.iter().map(cell_json).collect();
    Ok(Json(json!({ "cells": cells })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(id: &str, position: u32) -> NotebookCell {
        NotebookCell {
            id: id.into(),
            notebook_id: "nb".into(),
            tenant_id: "default".into(),
            position,
            cell_type: "note".into(),
            title: String::new(),
            body: String::new(),
            query_config: "{}".into(),
            time: "{\"mode\":\"inherit\"}".into(),
            display_config: "{}".into(),
            author_id: String::new(),
            author_name: String::new(),
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    #[test]
    fn reorder_moves_only_cells_whose_position_changes() {
        let cells = [cell("a", 1000), cell("b", 2000), cell("c", 3000)];
        let ids = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            reorder_positions(&cells, &ids(&["a", "b", "c"])),
            Ok(vec![])
        );
        assert_eq!(
            reorder_positions(&cells, &ids(&["c", "a", "b"])),
            Ok(vec![(2, 1000), (0, 2000), (1, 3000)])
        );
    }

    #[test]
    fn reorder_needs_every_cell_exactly_once() {
        let cells = [cell("a", 1000), cell("b", 2000)];
        for bad in [
            vec!["a"],
            vec!["a", "a"],
            vec!["a", "z"],
            vec!["a", "b", "c"],
        ] {
            let bad: Vec<String> = bad.into_iter().map(String::from).collect();
            assert!(reorder_positions(&cells, &bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn stale_writes_conflict() {
        assert!(
            check_fresh(
                "2026-11-07T13:00:00.000001Z",
                "2026-11-07T13:00:00.000001Z",
                "cell"
            )
            .is_ok()
        );
        let err = check_fresh(
            "2026-11-07T13:00:00.000002Z",
            "2026-11-07T13:00:00.000001Z",
            "cell",
        )
        .unwrap_err();
        assert_eq!(err.0, StatusCode::CONFLICT);
    }

    #[test]
    fn cells_serialize_their_json_columns_as_objects() {
        let value = cell_json(&cell("a", 1000));
        assert_eq!(value["time"]["mode"], "inherit");
        assert!(value["query_config"].is_object());
    }
}
