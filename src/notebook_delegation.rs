//! Short-lived notebook access for one authenticated investigation caller.
//! The agent never receives the caller's browser token or API key.

use axum::http::{HeaderMap, Method, StatusCode};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    RequestIdentity,
    clickhouse_config::{ConfigDb, SessionUser},
};

const DOMAIN: &[u8] = b"rush-notebook-delegation-v1\0";
const TTL: i64 = 15 * 60;
#[cfg(test)]
#[path = "notebook_delegation/tests/integration.rs"]
mod integration_tests;
pub const HEADER: &str = "x-rush-notebook-access";
pub const PREFIX: &str = "/api/v1/internal/sre/notebooks";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    tenant: String,
    principal: String,
    session_key: Option<String>,
    key_id: Option<String>,
    key_network_policy: Option<String>,
    expires: i64,
}

pub fn route_allowed(method: &Method, path: &str) -> bool {
    let Some(suffix) = path.strip_prefix(PREFIX) else {
        return false;
    };
    if suffix.is_empty() {
        return matches!(*method, Method::GET | Method::POST);
    }
    let Some(suffix) = suffix.strip_prefix('/') else {
        return false;
    };
    match suffix.split('/').collect::<Vec<_>>().as_slice() {
        [id] => *method == Method::GET && uuid::Uuid::parse_str(id).is_ok(),
        [id, "cells"] => *method == Method::POST && uuid::Uuid::parse_str(id).is_ok(),
        _ => false,
    }
}

pub fn issue(
    db: &ConfigDb,
    headers: &HeaderMap,
    caller: &SessionUser,
) -> Result<String, (StatusCode, String)> {
    let (session_key, key_id, key_network_policy) = if let Some(grant) =
        crate::request_auth::api_key_grant()
    {
        (None, Some(grant.id.clone()), Some(network_policy(&grant)))
    } else {
        let token = crate::handlers::auth::extract_session_cookie(headers).ok_or_else(denied)?;
        (Some(db.session_request_key(&token)), None, None)
    };
    let claims = Claims {
        tenant: caller.3.clone(),
        principal: caller.0.clone(),
        session_key,
        key_id,
        key_network_policy,
        expires: time::OffsetDateTime::now_utc().unix_timestamp() + TTL,
    };
    Ok(encode(db.cursor_hmac_secret(), &claims))
}

fn network_policy(grant: &crate::clickhouse_config::ApiKeyGrant) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(&grant.source_cidrs).expect("CIDR list"),
    ))
}

fn denied() -> (StatusCode, String) {
    (
        StatusCode::FORBIDDEN,
        "notebook access is unavailable or expired; start a new investigation turn".into(),
    )
}

fn encode(secret: &[u8], claims: &Claims) -> String {
    let bytes = serde_json::to_vec(claims).expect("serializable notebook claims");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC key");
    mac.update(DOMAIN);
    mac.update(&bytes);
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(bytes),
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

fn decode(
    secret: &[u8],
    token: &str,
    tenant: &str,
    now: i64,
) -> Result<Claims, (StatusCode, String)> {
    if token.len() > 4096 {
        return Err(denied());
    }
    let (payload, signature) = token.split_once('.').ok_or_else(denied)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).map_err(|_| denied())?;
    let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| denied())?;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC key");
    mac.update(DOMAIN);
    mac.update(&bytes);
    mac.verify_slice(&signature).map_err(|_| denied())?;
    let claims: Claims = serde_json::from_slice(&bytes).map_err(|_| denied())?;
    if claims.tenant != tenant
        || claims.expires <= now
        || claims.expires > now + TTL
        || claims.principal.is_empty()
        || claims.session_key.is_some() == claims.key_id.is_some()
    {
        return Err(denied());
    }
    Ok(claims)
}

pub async fn authorize(
    db: &ConfigDb,
    headers: &HeaderMap,
    tenant: &str,
    write: bool,
) -> Result<(SessionUser, RequestIdentity), (StatusCode, String)> {
    if !crate::internal_auth::sre_agent_token_matches(headers) {
        return Err((
            StatusCode::FORBIDDEN,
            "SRE internal credential required".into(),
        ));
    }
    let token = headers
        .get(HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(denied)?;
    let claims = decode(
        db.cursor_hmac_secret(),
        token,
        tenant,
        time::OffsetDateTime::now_utc().unix_timestamp(),
    )?;
    let (caller, actor_type, actor_id) = if let Some(key_id) = claims.key_id {
        let grant = db
            .resolve_agent_key_by_id(&key_id)
            .await
            .map_err(|_| denied())?
            .ok_or_else(denied)?;
        if !grant.allows_agent()
            || claims.key_network_policy.as_deref() != Some(network_policy(&grant).as_str())
        {
            return Err(denied());
        }
        let caller = crate::handlers::users::api_key_principal(&grant).ok_or_else(denied)?;
        (caller, "api_key", grant.id)
    } else {
        let caller = db
            .delegated_session_user(claims.session_key.as_deref().ok_or_else(denied)?)
            .await
            .ok_or_else(denied)?;
        let id = caller.0.clone();
        (caller, "user", id)
    };
    if caller.0 != claims.principal || caller.3 != tenant {
        return Err(denied());
    }
    if write && !crate::handlers::users::role_allows_write(&caller.4) {
        return Err((StatusCode::FORBIDDEN, "write role required".into()));
    }
    let identity = RequestIdentity {
        tenant_id: tenant.into(),
        authenticated: true,
        actor_id,
        actor_name: caller.1.clone(),
        actor_type: actor_type.into(),
        credential_type: "sre_notebook_delegation".into(),
    };
    Ok((caller, identity))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn claims() -> Claims {
        Claims {
            tenant: "a".into(),
            principal: "user-1".into(),
            session_key: Some("hmac-key".into()),
            key_id: None,
            key_network_policy: None,
            expires: 1100,
        }
    }
    #[test]
    fn grants_are_signed_scoped_expiring_and_unambiguous() {
        let token = encode(b"secret", &claims());
        assert!(decode(b"secret", &token, "a", 1000).is_ok());
        for (secret, tenant, now) in [
            (b"other".as_slice(), "a", 1000),
            (b"secret".as_slice(), "b", 1000),
            (b"secret".as_slice(), "a", 1100),
        ] {
            assert!(decode(secret, &token, tenant, now).is_err());
        }
        let mut bad = claims();
        bad.key_id = Some("key".into());
        assert!(decode(b"secret", &encode(b"secret", &bad), "a", 1000).is_err());
        let mut tampered = token.into_bytes();
        tampered[5] ^= 1;
        assert!(
            decode(
                b"secret",
                std::str::from_utf8(&tampered).unwrap(),
                "a",
                1000
            )
            .is_err()
        );
    }
    #[test]
    fn routes_allow_only_notebook_reads_creation_and_cell_additions() {
        let id = "00000000-0000-0000-0000-000000000001";
        assert!(route_allowed(&Method::POST, PREFIX));
        assert!(route_allowed(&Method::GET, &format!("{PREFIX}/{id}")));
        assert!(route_allowed(
            &Method::POST,
            &format!("{PREFIX}/{id}/cells")
        ));
        for path in [
            format!("{PREFIX}/{id}/cells/reorder"),
            format!("{PREFIX}/../users"),
            format!("{PREFIX}extra"),
        ] {
            assert!(!route_allowed(&Method::POST, &path));
        }
        assert!(!route_allowed(&Method::DELETE, &format!("{PREFIX}/{id}")));
    }
}
