use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;
use crate::handlers::users::{require_auth, require_write};
use crate::models::service_link::CreateServiceLinkRequest;

fn validate_service_link(req: &CreateServiceLinkRequest) -> Result<(), (StatusCode, String)> {
    let service = req.service_name.trim();
    let repository = req
        .github_repo
        .trim()
        .trim_end_matches(".git")
        .strip_prefix("https://github.com/")
        .unwrap_or(req.github_repo.trim().trim_end_matches(".git"));
    let repo_parts: Vec<_> = repository.split('/').collect();
    let valid_repo = repo_parts.len() == 2
        && repo_parts.iter().all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        });
    let valid_root = !req.root_path.starts_with('/')
        && !req.root_path.split('/').any(|part| part == "..")
        && req.root_path.len() <= 1024;
    if service.is_empty()
        || service.len() > 256
        || service != req.service_name
        || req.github_repo.trim() != req.github_repo
        || !valid_repo
        || req.default_branch.trim().is_empty()
        || req.default_branch.trim() != req.default_branch
        || req.default_branch.len() > 200
        || req.default_branch.chars().any(char::is_control)
        || req.root_path.trim() != req.root_path
        || !valid_root
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid service repository link".to_string(),
        ));
    }
    Ok(())
}

pub async fn list_service_links(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_auth(&state, &headers).await?;
    let links = state
        .config_db
        .list_service_links(&caller.3)
        .await
        .map_err(|e| {
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
    validate_service_link(&req)?;
    state
        .config_db
        .upsert_service_link(
            &caller.3,
            &req.service_name,
            &req.github_repo,
            req.github_installation_id,
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
        .get_service_link(&caller.3, &req.service_name)
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
                        "github_installation_id": req.github_installation_id,
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
        .delete_service_link(&caller.3, &service_name)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn request(repository: &str, root_path: &str) -> CreateServiceLinkRequest {
        CreateServiceLinkRequest {
            service_name: "gateway".to_string(),
            github_repo: repository.to_string(),
            github_installation_id: 42,
            default_branch: "main".to_string(),
            root_path: root_path.to_string(),
        }
    }

    #[test]
    fn accepts_owner_repo_and_github_url() {
        assert!(validate_service_link(&request("acme/gateway", "services/gateway")).is_ok());
        assert!(validate_service_link(&request("https://github.com/acme/gateway.git", "")).is_ok());
    }

    #[test]
    fn rejects_hosts_extra_segments_and_traversal() {
        assert!(validate_service_link(&request("https://evil.example/acme/gateway", "")).is_err());
        assert!(validate_service_link(&request("acme/team/gateway", "")).is_err());
        assert!(validate_service_link(&request("acme/gateway", "../secret")).is_err());
    }
}
