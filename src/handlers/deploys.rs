use axum::{
    Json,
    extract::{Extension, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::handlers::users::require_principal_write;
use crate::models::deploy::*;
use crate::{AppState, TenantContext};

pub async fn create_deploy(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(req): Json<CreateDeployMarkerRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_principal_write(&state, &headers).await?;
    if req.service_name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "service_name must not be empty".to_string(),
        ));
    }
    if req.service_name.len() > 255 {
        return Err((
            StatusCode::BAD_REQUEST,
            "service_name must not exceed 255 characters".to_string(),
        ));
    }
    if req.version.len() > 100 {
        return Err((
            StatusCode::BAD_REQUEST,
            "version must not exceed 100 characters".to_string(),
        ));
    }
    if req.commit_sha.len() > 100 {
        return Err((
            StatusCode::BAD_REQUEST,
            "commit_sha must not exceed 100 characters".to_string(),
        ));
    }
    if req.description.len() > 1024 {
        return Err((
            StatusCode::BAD_REQUEST,
            "description must not exceed 1024 characters".to_string(),
        ));
    }
    let id = uuid::Uuid::new_v4().to_string();
    state
        .config_db
        .create_deploy_marker(
            &tenant.tenant_id,
            &id,
            &req.service_name,
            &req.version,
            &req.commit_sha,
            &req.description,
            &req.environment,
            &req.deployed_by,
        )
        .await
        .map_err(|e| crate::api_error::internal_legacy("deploys", e))?;

    state
        .audit
        .log(
            crate::audit::AuditEvent::new("deploy.create", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(tenant.tenant_id.clone())
                .resource("deploy_marker", &id)
                .changes(
                    serde_json::json!({
                        "service_name": req.service_name,
                        "version": req.version,
                        "commit_sha": req.commit_sha,
                        "environment": req.environment,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok((StatusCode::CREATED, Json(serde_json::json!({ "id": id }))))
}

pub async fn list_deploys(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Query(query): Query<DeployMarkerQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let markers = state
        .config_db
        .list_deploy_markers(
            &tenant.tenant_id,
            query.service_name.as_deref(),
            query.from.as_deref(),
            query.to.as_deref(),
        )
        .await
        .map_err(|e| crate::api_error::internal_legacy("deploys", e))?;
    Ok(Json(serde_json::json!({ "deploys": markers })))
}
