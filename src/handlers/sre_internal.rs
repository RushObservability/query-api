use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::Deserialize;

use crate::{AppState, TenantContext};

fn require_internal(headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    crate::internal_auth::sre_agent_token_matches(headers)
        .then_some(())
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "invalid internal credential".into(),
        ))
}

#[derive(Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ContextRequest {
    ListDeploys {
        service: Option<String>,
        from: Option<String>,
        to: Option<String>,
    },
    ListAnomalyRules,
    GetAnomalyRule {
        id: String,
    },
    GetAnomalyEvent {
        id: String,
    },
    ListAnomalyEvents {
        rule_id: String,
        limit: Option<i64>,
    },
    GetSetting {
        key: String,
    },
    ListEnabledCustomSkills,
    GetCustomSkill {
        name: String,
    },
    GetServiceLink {
        service: String,
    },
}

pub async fn context(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(request): Json<ContextRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_internal(&headers)?;
    let data = match request {
        ContextRequest::ListDeploys { service, from, to } => serde_json::to_value(
            state
                .config_db
                .list_deploy_markers(service.as_deref(), from.as_deref(), to.as_deref())
                .await
                .map_err(internal_error)?,
        ),
        ContextRequest::ListAnomalyRules => serde_json::to_value(
            state
                .config_db
                .list_anomaly_rules(&tenant.tenant_id)
                .await
                .map_err(internal_error)?,
        ),
        ContextRequest::GetAnomalyRule { id } => serde_json::to_value(
            state
                .config_db
                .get_anomaly_rule(&id, &tenant.tenant_id)
                .await
                .map_err(internal_error)?,
        ),
        ContextRequest::GetAnomalyEvent { id } => serde_json::to_value(
            state
                .config_db
                .get_anomaly_event(&id, &tenant.tenant_id)
                .await
                .map_err(internal_error)?,
        ),
        ContextRequest::ListAnomalyEvents { rule_id, limit } => serde_json::to_value(
            state
                .config_db
                .list_anomaly_events(
                    &rule_id,
                    &tenant.tenant_id,
                    limit.unwrap_or(10).clamp(1, 100),
                )
                .await
                .map_err(internal_error)?,
        ),
        ContextRequest::GetSetting { key } => {
            const ALLOWED: &[&str] = &[
                "sre_agent_allowed_models",
                "sre_agent_model",
                "sre_agent_max_tool_steps",
                "sre_agent_max_llm_calls",
            ];
            if !ALLOWED.contains(&key.as_str()) {
                return Err((
                    StatusCode::FORBIDDEN,
                    "setting is not available to the SRE agent".into(),
                ));
            }
            serde_json::to_value(
                state
                    .config_db
                    .get_setting(&key)
                    .await
                    .map_err(internal_error)?,
            )
        }
        ContextRequest::ListEnabledCustomSkills => serde_json::to_value(
            state
                .config_db
                .list_custom_skills()
                .await
                .map_err(internal_error)?
                .into_iter()
                .filter(|skill| skill.enabled)
                .collect::<Vec<_>>(),
        ),
        ContextRequest::GetCustomSkill { name } => serde_json::to_value(
            state
                .config_db
                .get_custom_skill_by_name(&name)
                .await
                .map_err(internal_error)?,
        ),
        ContextRequest::GetServiceLink { service } => serde_json::to_value(
            state
                .config_db
                .get_service_link(&tenant.tenant_id, &service)
                .await
                .map_err(internal_error)?,
        ),
    }
    .map_err(|_| internal_error(anyhow::anyhow!("failed to encode SRE context response")))?;
    Ok(Json(serde_json::json!({ "data": data })))
}

#[derive(Debug, Deserialize)]
pub struct CreateSessionRequest {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub created_by: String,
    #[serde(default)]
    pub template_id: String,
}

pub async fn create_session(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(request): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_internal(&headers)?;
    validate_id(&request.id)?;
    if request.title.len() > 512
        || request.created_by.len() > 256
        || request.template_id.len() > 128
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid investigation session".into(),
        ));
    }
    state
        .config_db
        .create_investigation_session(
            &request.id,
            &tenant.tenant_id,
            &request.title,
            &request.created_by,
            &request.template_id,
        )
        .await
        .map_err(internal_error)?;
    state.audit.log(
        crate::audit::AuditEvent::new("investigation_session.create", "system")
            .actor_name("sre-agent")
            .tenant(tenant.tenant_id)
            .resource("investigation_session", &request.id)
            .outcome("success")
            .changes(serde_json::json!({ "template_id": request.template_id, "has_title": !request.title.is_empty() }).to_string())
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "id": request.id })),
    ))
}

#[derive(Debug, Deserialize)]
pub struct SessionListQuery {
    pub limit: Option<u64>,
}

pub async fn list_sessions(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Query(query): Query<SessionListQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_internal(&headers)?;
    let sessions = state
        .config_db
        .list_investigation_sessions(&tenant.tenant_id, query.limit.unwrap_or(50).clamp(1, 200))
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::json!({ "sessions": sessions })))
}

pub async fn get_session(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_internal(&headers)?;
    validate_id(&id)?;
    state
        .config_db
        .get_investigation_session(&id, &tenant.tenant_id)
        .await
        .map_err(internal_error)?
        .map(Json)
        .ok_or((
            StatusCode::NOT_FOUND,
            "investigation session not found".into(),
        ))
}

#[derive(Debug, Deserialize)]
pub struct UpdateSessionRequest {
    pub title: Option<String>,
    pub status: Option<String>,
    pub working_memory: Option<String>,
    #[serde(default)]
    pub prompt_tokens_delta: u64,
    #[serde(default)]
    pub completion_tokens_delta: u64,
    pub llm_model: Option<String>,
}

pub async fn update_session(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpdateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_internal(&headers)?;
    validate_id(&id)?;
    if request
        .title
        .as_ref()
        .is_some_and(|value| value.len() > 512)
        || request
            .working_memory
            .as_ref()
            .is_some_and(|value| value.len() > 512 * 1024)
        || request
            .llm_model
            .as_ref()
            .is_some_and(|value| value.len() > 128)
        || request
            .status
            .as_ref()
            .is_some_and(|value| !matches!(value.as_str(), "active" | "completed" | "archived"))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid investigation session update".into(),
        ));
    }
    let changed = state
        .config_db
        .update_investigation_session(
            &id,
            &tenant.tenant_id,
            request.title.as_deref(),
            request.status.as_deref(),
            request.working_memory.as_deref(),
            request.prompt_tokens_delta,
            request.completion_tokens_delta,
            request.llm_model.as_deref(),
        )
        .await
        .map_err(internal_error)?;
    if !changed {
        return Err((
            StatusCode::NOT_FOUND,
            "investigation session not found".into(),
        ));
    }
    state.audit.log(
        crate::audit::AuditEvent::new("investigation_session.update", "system")
            .actor_name("sre-agent")
            .tenant(tenant.tenant_id)
            .resource("investigation_session", &id)
            .outcome("success")
            .changes(serde_json::json!({
                "title_changed": request.title.is_some(),
                "status": request.status,
                "memory_changed": request.working_memory.is_some(),
                "usage_changed": request.prompt_tokens_delta > 0 || request.completion_tokens_delta > 0,
                "model_changed": request.llm_model.is_some(),
            }).to_string())
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_session(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_internal(&headers)?;
    validate_id(&id)?;
    if !state
        .config_db
        .delete_investigation_session(&id, &tenant.tenant_id)
        .await
        .map_err(internal_error)?
    {
        return Err((
            StatusCode::NOT_FOUND,
            "investigation session not found".into(),
        ));
    }
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("investigation_session.delete", "system")
                .actor_name("sre-agent")
                .tenant(tenant.tenant_id)
                .resource("investigation_session", &id)
                .outcome("success")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
pub struct AddTurnRequest {
    pub id: String,
    pub turn_index: i64,
    pub role: String,
    pub content: String,
    #[serde(default = "empty_array")]
    pub tool_calls: String,
    #[serde(default)]
    pub report_kind: String,
}

fn empty_array() -> String {
    "[]".into()
}

pub async fn add_turn(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(request): Json<AddTurnRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_internal(&headers)?;
    validate_id(&session_id)?;
    validate_id(&request.id)?;
    if request.turn_index < 0
        || !matches!(
            request.role.as_str(),
            "user" | "assistant" | "tool" | "system"
        )
        || request.content.len() > 2 * 1024 * 1024
        || request.tool_calls.len() > 2 * 1024 * 1024
        || request.report_kind.len() > 64
    {
        return Err((StatusCode::BAD_REQUEST, "invalid investigation turn".into()));
    }
    if !state
        .config_db
        .add_investigation_turn(
            &tenant.tenant_id,
            &request.id,
            &session_id,
            request.turn_index,
            &request.role,
            &request.content,
            &request.tool_calls,
            &request.report_kind,
        )
        .await
        .map_err(internal_error)?
    {
        return Err((
            StatusCode::NOT_FOUND,
            "investigation session not found".into(),
        ));
    }
    state.audit.log(
        crate::audit::AuditEvent::new("investigation_turn.create", "system")
            .actor_name("sre-agent")
            .tenant(tenant.tenant_id)
            .resource("investigation_session", &session_id)
            .outcome("success")
            .changes(serde_json::json!({ "turn_index": request.turn_index, "role": request.role, "report_kind": request.report_kind }).to_string())
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;
    Ok(StatusCode::CREATED)
}

#[derive(Debug, Deserialize)]
pub struct TurnListQuery {
    pub limit: Option<u64>,
    #[serde(default)]
    pub count_only: bool,
}

pub async fn get_turns(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<TurnListQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_internal(&headers)?;
    validate_id(&session_id)?;
    if query.count_only {
        let count = state
            .config_db
            .count_investigation_turns(&session_id, &tenant.tenant_id)
            .await
            .map_err(internal_error)?;
        return Ok(Json(serde_json::json!({ "count": count })));
    }
    let turns = state
        .config_db
        .get_investigation_turns(&session_id, &tenant.tenant_id, query.limit)
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::json!({ "turns": turns })))
}

fn validate_id(value: &str) -> Result<(), (StatusCode, String)> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        return Err((StatusCode::BAD_REQUEST, "invalid identifier".into()));
    }
    Ok(())
}

fn internal_error(error: anyhow::Error) -> (StatusCode, String) {
    tracing::error!(%error, "SRE internal store operation failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal store operation failed".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::validate_id;

    #[test]
    fn identifiers_reject_path_syntax() {
        assert!(validate_id("session-123").is_ok());
        assert!(validate_id("../_audit").is_err());
        assert!(validate_id("").is_err());
    }
}
