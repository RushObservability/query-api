use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::AppState;

#[derive(serde::Deserialize)]
pub struct RepositoryAccessAudit {
    tenant_id: String,
    service_name: String,
    repository: String,
    action: String,
    #[serde(default)]
    path: String,
    #[serde(default = "success")]
    outcome: String,
}

fn success() -> String {
    "success".to_string()
}

/// The audit action for an SRE agent repository access. `compare` is a
/// commit comparison between two deploys.
fn audit_action(action: &str) -> Option<&'static str> {
    match action {
        "list" => Some("repository.list"),
        "search" => Some("repository.search"),
        "read" => Some("repository.read"),
        "compare" => Some("repository.compare"),
        _ => None,
    }
}

pub async fn audit_repository_access(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(event): Json<RepositoryAccessAudit>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !crate::internal_auth::sre_agent_token_matches(&headers) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "invalid internal credential".to_string(),
        ));
    }
    let Some(action) = audit_action(&event.action) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid repository action".to_string(),
        ));
    };
    if event.tenant_id.is_empty()
        || event.tenant_id.len() > 128
        || event.service_name.is_empty()
        || event.service_name.len() > 256
        || event.repository.is_empty()
        || event.repository.len() > 256
        || event.path.len() > 1024
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid repository audit event".to_string(),
        ));
    }
    let outcome = if event.outcome == "failure" {
        "failure"
    } else {
        "success"
    };

    state
        .audit
        .log(
            crate::audit::AuditEvent::new(action, "system")
                .actor_name("sre-agent")
                .tenant(event.tenant_id)
                .resource("repository", &event.repository)
                .outcome(outcome)
                .metadata(
                    serde_json::json!({
                        "service_name": event.service_name,
                        "path": event.path
                    })
                    .to_string(),
                )
                .description("SRE agent accessed a read-only repository snapshot")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::audit_action;

    #[test]
    fn action_names_are_stable() {
        assert_eq!(audit_action("list"), Some("repository.list"));
        assert_eq!(audit_action("search"), Some("repository.search"));
        assert_eq!(audit_action("read"), Some("repository.read"));
        assert_eq!(audit_action("compare"), Some("repository.compare"));
        assert_eq!(audit_action("write"), None);
        assert_eq!(audit_action(""), None);
    }
}
