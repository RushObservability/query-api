//! `GET /api/v1/whoami`: the identity behind the current credential.
//!
//! Service accounts such as the MCP server call this first to confirm a key
//! works and to learn its tenant and role. Browser sessions get the same shape.

use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde::Serialize;

use crate::audit::API_KEY_ACTOR_PREFIX;
use crate::handlers::users::require_principal;
use crate::{AppState, TenantContext};

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Principal {
    /// `user` or `api_key`.
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// User id, or the API key id (without the `apikey:` actor prefix).
    pub id: String,
    pub name: String,
    pub role: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Capabilities {
    pub read: bool,
    pub write: bool,
    pub admin: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct WhoAmI {
    /// The tenant this request is scoped to. For API keys this is always the
    /// key's own tenant.
    pub tenant_id: String,
    pub principal: Principal,
    pub capabilities: Capabilities,
}

fn describe(tenant_id: String, caller: (String, String, String, String, String)) -> WhoAmI {
    let (id, username, display_name, _, role) = caller;
    let (kind, id) = match id.strip_prefix(API_KEY_ACTOR_PREFIX) {
        Some(key_id) => ("api_key", key_id.to_string()),
        None => ("user", id),
    };
    let name = if display_name.trim().is_empty() {
        username
    } else {
        display_name
    };
    let capabilities = Capabilities {
        read: true,
        write: matches!(role.as_str(), "admin" | "write"),
        admin: role == "admin",
    };
    WhoAmI {
        tenant_id,
        principal: Principal {
            kind,
            id,
            name,
            role,
        },
        capabilities,
    }
}

pub async fn whoami(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
) -> Result<Json<WhoAmI>, (StatusCode, String)> {
    let caller = require_principal(&state, &headers).await?;
    Ok(Json(describe(tenant.tenant_id, caller)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_keys_are_reported_without_the_actor_prefix_and_never_as_admin() {
        let me = describe(
            "prod".into(),
            (
                "apikey:k1".into(),
                "MCP server".into(),
                "MCP server".into(),
                "prod".into(),
                "write".into(),
            ),
        );
        assert_eq!(me.tenant_id, "prod");
        assert_eq!(me.principal.kind, "api_key");
        assert_eq!(me.principal.id, "k1");
        assert_eq!(
            me.capabilities,
            Capabilities {
                read: true,
                write: true,
                admin: false
            }
        );
    }

    #[test]
    fn users_report_their_display_name_and_role() {
        let me = describe(
            "default".into(),
            (
                "u1".into(),
                "alice".into(),
                "".into(),
                "default".into(),
                "viewer".into(),
            ),
        );
        assert_eq!(me.principal.kind, "user");
        assert_eq!(me.principal.name, "alice");
        assert!(!me.capabilities.write);
        let json = serde_json::to_value(&me).unwrap();
        assert_eq!(json["principal"]["type"], "user");
    }
}
