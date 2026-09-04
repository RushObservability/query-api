//! Public access to stored SRE investigation sessions.
//!
//! Query-api owns the ClickHouse tables for sessions and turns, so these
//! handlers do not depend on a running SRE-agent process. The agent still uses
//! the private `/internal/sre/sessions*` routes to write investigation state.

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::AppState;
use crate::handlers::settings::{SreAgentAccessDecision, sre_agent_access_decision};
use crate::handlers::users::{require_auth, require_write};

type AuthenticatedCaller = (String, String, String, String, String);

#[derive(Debug, Deserialize)]
pub struct SessionListQuery {
    pub limit: Option<u64>,
}

async fn require_agent_access(
    state: &AppState,
    headers: &HeaderMap,
    write: bool,
) -> Result<AuthenticatedCaller, (StatusCode, String)> {
    let caller = if write {
        require_write(state, headers).await?
    } else {
        require_auth(state, headers).await?
    };
    match sre_agent_access_decision(state, &caller.3).await {
        SreAgentAccessDecision::Allowed => Ok(caller),
        SreAgentAccessDecision::Disabled => {
            Err((StatusCode::FORBIDDEN, "SRE agent is disabled".into()))
        }
        SreAgentAccessDecision::TenantDenied => Err((
            StatusCode::FORBIDDEN,
            "SRE agent is not enabled for this tenant".into(),
        )),
    }
}

fn validate_session_id(id: &str) -> Result<(), (StatusCode, String)> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err((StatusCode::BAD_REQUEST, "invalid session id".into()));
    }
    Ok(())
}

/// List investigation sessions for the authenticated tenant.
pub async fn list_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SessionListQuery>,
) -> Result<Response, (StatusCode, String)> {
    let caller = require_agent_access(&state, &headers, false).await?;
    let sessions = state
        .config_db
        .list_investigation_sessions(&caller.3, query.limit.unwrap_or(50).clamp(1, 200))
        .await
        .map_err(|error| crate::api_error::internal_legacy("sre_sessions.list", error))?;
    Ok(Json(serde_json::json!({ "sessions": sessions })).into_response())
}

/// Return one investigation and its stored turns for the authenticated tenant.
pub async fn get_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, (StatusCode, String)> {
    validate_session_id(&id)?;
    let caller = require_agent_access(&state, &headers, false).await?;
    let session = state
        .config_db
        .get_investigation_session(&id, &caller.3)
        .await
        .map_err(|error| crate::api_error::internal_legacy("sre_sessions.get", error))?
        .ok_or((
            StatusCode::NOT_FOUND,
            "investigation session not found".into(),
        ))?;
    let turns = state
        .config_db
        .get_investigation_turns(&id, &caller.3, None)
        .await
        .map_err(|error| crate::api_error::internal_legacy("sre_sessions.turns", error))?;
    Ok(Json(serde_json::json!({ "session": session, "turns": turns })).into_response())
}

/// Archive an investigation session for the authenticated tenant.
pub async fn delete_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, (StatusCode, String)> {
    validate_session_id(&id)?;
    let caller = require_agent_access(&state, &headers, true).await?;
    let deleted = state
        .config_db
        .delete_investigation_session(&id, &caller.3)
        .await
        .map_err(|error| crate::api_error::internal_legacy("sre_sessions.delete", error))?;
    if !deleted {
        return Err((
            StatusCode::NOT_FOUND,
            "investigation session not found".into(),
        ));
    }

    state
        .audit
        .log(
            crate::audit::AuditEvent::new("sre_agent.session_delete", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("sre_agent_session", &id)
                .outcome("success")
                .description("SRE agent investigation session deleted")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[cfg(test)]
mod tests {
    use super::validate_session_id;

    #[test]
    fn session_ids_accept_generated_identifiers() {
        assert!(validate_session_id("019f7c15-9c87-7b42-a506-44927cc1bf03").is_ok());
        assert!(validate_session_id("saved_session-01").is_ok());
    }

    #[test]
    fn session_ids_reject_path_syntax_and_oversized_values() {
        assert!(validate_session_id("../_audit").is_err());
        assert!(validate_session_id("session/id").is_err());
        assert!(validate_session_id(&"a".repeat(129)).is_err());
    }
}
