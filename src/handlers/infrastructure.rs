//! Shared authorization and tenant scoping for Kubernetes-backed integrations.
//!
//! Infrastructure access is deliberately separate from ordinary telemetry
//! `read` access. Namespace grants are operator-owned configuration supplied as
//! JSON in `RUSH_INFRASTRUCTURE_TENANT_NAMESPACES`, for example:
//!
//! ```json
//! {"default":["rush"],"acme":["acme-prod","acme-staging"]}
//! ```
//!
//! A `"*"` tenant entry adds shared namespaces to every tenant. A namespace
//! value of `"*"` is the explicit cluster-wide opt-in.

use std::collections::{BTreeSet, HashMap};

use axum::http::{HeaderMap, StatusCode};

use crate::AppState;
use crate::handlers::users::require_auth;

pub(crate) const INFRASTRUCTURE_READ_PERMISSION: &str = "infrastructure:read";
pub(crate) type Caller = (String, String, String, String, String);

fn has_permission(role: &str, permissions: &[String], required: &str) -> bool {
    role == "admin"
        || permissions
            .iter()
            .any(|permission| permission == "admin" || permission == required)
}

/// Require the dedicated infrastructure permission. Administrators retain
/// access, while ordinary `read`/viewer users are denied.
pub(crate) async fn require_infrastructure_read(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Caller, (StatusCode, String)> {
    let caller = require_auth(state, headers).await?;
    let permissions = state
        .config_db
        .resolve_user_permissions(&caller.0)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to resolve infrastructure permissions");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })?
        .1;

    if !has_permission(&caller.4, &permissions, INFRASTRUCTURE_READ_PERMISSION) {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("infrastructure.read_denied", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("infrastructure", "kubernetes")
                    .outcome("failure")
                    .description("infrastructure:read permission required")
                    .context(crate::audit::actor_context_from_headers(headers)),
            )
            .await;
        return Err((
            StatusCode::FORBIDDEN,
            format!("{INFRASTRUCTURE_READ_PERMISSION} permission required"),
        ));
    }

    Ok(caller)
}

fn parse_namespace_policy(raw: &str, tenant: &str) -> Result<Vec<String>, String> {
    let policy: HashMap<String, Vec<String>> =
        serde_json::from_str(raw).map_err(|_| "invalid namespace policy JSON".to_string())?;
    let mut namespaces = BTreeSet::new();
    for key in ["*", tenant] {
        if let Some(values) = policy.get(key) {
            for namespace in values {
                let namespace = namespace.trim();
                if namespace == "*" || is_dns_namespace(namespace) {
                    namespaces.insert(namespace.to_string());
                } else {
                    return Err(format!("invalid namespace in policy for tenant '{tenant}'"));
                }
            }
        }
    }
    Ok(namespaces.into_iter().collect())
}

fn is_dns_namespace(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value.as_bytes()[value.len() - 1].is_ascii_alphanumeric()
}

/// Resolve the namespaces granted to the selected Rush tenant. The selected
/// tenant is already constrained by group-to-tenant bindings in auth middleware,
/// so this map completes the group -> tenant -> namespace chain.
pub(crate) fn allowed_namespaces(tenant: &str) -> Result<Vec<String>, (StatusCode, String)> {
    let raw = std::env::var("RUSH_INFRASTRUCTURE_TENANT_NAMESPACES").map_err(|_| {
        (
            StatusCode::FORBIDDEN,
            "no infrastructure namespaces configured for this tenant".to_string(),
        )
    })?;
    let namespaces = parse_namespace_policy(&raw, tenant).map_err(|error| {
        tracing::error!(%error, "invalid infrastructure namespace policy");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "infrastructure namespace policy unavailable".to_string(),
        )
    })?;
    if namespaces.is_empty() {
        return Err((
            StatusCode::FORBIDDEN,
            "no infrastructure namespaces configured for this tenant".to_string(),
        ));
    }
    Ok(namespaces)
}

pub(crate) fn namespace_allowed(allowed: &[String], namespace: &str) -> bool {
    allowed.iter().any(|item| item == "*" || item == namespace)
}

pub(crate) fn cluster_wide_allowed(allowed: &[String]) -> bool {
    allowed.iter().any(|item| item == "*")
        && std::env::var("KUBERNETES_CLUSTER_WIDE")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes"
                )
            })
            .unwrap_or(false)
}

pub(crate) async fn audit_read(
    state: &AppState,
    headers: &HeaderMap,
    caller: &Caller,
    integration: &str,
    operation: &str,
    resource_id: &str,
    namespaces: &[String],
) {
    state
        .audit
        .log(
            crate::audit::AuditEvent::new(format!("infrastructure.{integration}.read"), "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("infrastructure", resource_id)
                .metadata(
                    serde_json::json!({
                        "operation": operation,
                        "namespaces": namespaces,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(headers)),
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_read_permission_does_not_grant_infrastructure() {
        assert!(!has_permission(
            "viewer",
            &["read".to_string()],
            INFRASTRUCTURE_READ_PERMISSION
        ));
        assert!(has_permission(
            "viewer",
            &[INFRASTRUCTURE_READ_PERMISSION.to_string()],
            INFRASTRUCTURE_READ_PERMISSION
        ));
        assert!(has_permission("admin", &[], INFRASTRUCTURE_READ_PERMISSION));
    }

    #[test]
    fn namespace_policy_combines_shared_and_tenant_grants() {
        let namespaces = parse_namespace_policy(
            r#"{"* ":[],"*":["shared"],"acme":["acme-prod","acme-staging"],"other":["other-prod"]}"#,
            "acme",
        )
        .unwrap();
        assert_eq!(namespaces, vec!["acme-prod", "acme-staging", "shared"]);
        assert!(!namespace_allowed(&namespaces, "other-prod"));
    }

    #[test]
    fn namespace_policy_is_deny_by_default_and_validates_names() {
        assert!(
            parse_namespace_policy(r#"{"other":["other-prod"]}"#, "acme")
                .unwrap()
                .is_empty()
        );
        assert!(parse_namespace_policy(r#"{"acme":["../secret"]}"#, "acme").is_err());
    }

    #[test]
    fn star_is_explicit_cluster_scope_grant() {
        let namespaces = parse_namespace_policy(r#"{"acme":["*"]}"#, "acme").unwrap();
        assert!(namespace_allowed(&namespaces, "anything"));
    }
}
