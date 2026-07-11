use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::handlers::users::{require_auth, require_write};
use crate::models::service_link::CreateServiceLinkRequest;

pub async fn list_service_links(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let links = state.config_db.list_service_links().await.map_err(|e| {
        tracing::error!(error = %e, "internal error");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;
    Ok(Json(serde_json::json!({ "links": links })))
}

pub async fn create_service_link(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateServiceLinkRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    state
        .config_db
        .upsert_service_link(
            &req.service_name,
            &req.github_repo,
            &req.default_branch,
            &req.root_path,
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    let link = state
        .config_db
        .get_service_link(&req.service_name)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "upsert failed".to_string(),
            )
        })?;

    // AUDIT: service link created/updated (upsert).
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("service_link.create", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("service_link", req.service_name.clone())
                .changes(
                    serde_json::json!({
                        "service_name": req.service_name,
                        "github_repo": req.github_repo,
                        "default_branch": req.default_branch,
                        "root_path": req.root_path
                    })
                    .to_string(),
                )
                .description("service link upserted")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(link))
}

pub async fn delete_service_link(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(service_name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_write(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_service_link(&service_name)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    }

    // AUDIT: service link deleted.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("service_link.delete", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("service_link", service_name.clone())
                .description("service link deleted")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(StatusCode::NO_CONTENT)
}
