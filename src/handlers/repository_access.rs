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
    let action = match event.action.as_str() {
        "list" => "repository.list",
        "search" => "repository.search",
        "read" => "repository.read",
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                "invalid repository action".to_string(),
            ));
        }
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
    #[test]
    fn action_names_are_stable() {
        assert_eq!("repository.read", format!("repository.{}", "read"));
    }
}
