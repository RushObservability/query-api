use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::AppState;
use crate::handlers::users::require_admin;
use crate::saml;

type HmacSha256 = Hmac<Sha256>;
type AuthenticatedCaller = (String, String, String, String, String);

const SSO_TRANSACTION_VERSION: u8 = 1;
const SSO_TRANSACTION_TTL_SECS: i64 = 10 * 60;
const SSO_TRANSACTION_COOKIE_SECURE: &str = "__Host-rush_sso_tx";
const SSO_TRANSACTION_COOKIE_INSECURE: &str = "rush_sso_tx";
const SSO_SETUP_COOKIE_SECURE: &str = "__Host-rush_sso_setup";
const SSO_SETUP_COOKIE_INSECURE: &str = "rush_sso_setup";
const SSO_SETUP_TTL_SECS: i64 = 30 * 60;
const OIDC_IAT_FUTURE_SKEW_SECS: i64 = 60;

fn public_sso_internal_error(
    status: StatusCode,
    operation: &'static str,
    error: impl std::fmt::Display,
    public_message: &'static str,
) -> (StatusCode, String) {
    crate::api_error::internal_legacy_with_message(status, operation, error, public_message)
}

fn public_sso_rejection(
    status: StatusCode,
    operation: &'static str,
    error: impl std::fmt::Display,
    public_message: &'static str,
) -> (StatusCode, String) {
    tracing::warn!(operation, reason = %error, "SSO request rejected");
    (status, public_message.to_string())
}

/// Browser-bound state for one OIDC or SAML authentication transaction.
///
/// The value is authenticated with an application secret and stored only in an
/// HttpOnly cookie. This binds the callback to the browser that initiated it
/// without putting a bearer-capable transaction record in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SsoTransaction {
    version: u8,
    protocol: String,
    provider_id: String,
    state: String,
    nonce: String,
    pkce_verifier: String,
    saml_request_id: String,
    redirect_path: String,
    issued_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SsoSetupSession {
    version: u8,
    provider: String,
    created_by: String,
    session_id: String,
    issued_at: i64,
}

fn random_urlsafe<const N: usize>() -> String {
    let bytes: [u8; N] = rand::rng().random();
    URL_SAFE_NO_PAD.encode(bytes)
}

fn safe_sso_redirect(raw: Option<&str>) -> String {
    let value = raw.unwrap_or("/").trim();
    if !value.starts_with('/')
        || value.starts_with("//")
        || value.contains('\\')
        || value.chars().any(char::is_control)
    {
        return "/".to_string();
    }
    match value.parse::<axum::http::Uri>() {
        Ok(uri) if uri.scheme().is_none() && uri.authority().is_none() => value.to_string(),
        _ => "/".to_string(),
    }
}

fn sso_transaction_secret() -> Result<Vec<u8>, String> {
    let secret = std::env::var("RUSH_SSO_TRANSACTION_SECRET")
        .or_else(|_| std::env::var("RUSH_API_KEY_SECRET"))
        .map_err(|_| {
            "SSO is unavailable: configure RUSH_SSO_TRANSACTION_SECRET with at least 32 bytes"
                .to_string()
        })?;
    if secret.as_bytes().len() < 32 {
        return Err(
            "SSO is unavailable: RUSH_SSO_TRANSACTION_SECRET must be at least 32 bytes".to_string(),
        );
    }
    Ok(secret.into_bytes())
}

fn encode_sso_transaction_with_secret(
    transaction: &SsoTransaction,
    secret: &[u8],
) -> Result<String, String> {
    let payload = serde_json::to_vec(transaction)
        .map_err(|e| format!("failed to encode SSO transaction: {e}"))?;
    let payload = URL_SAFE_NO_PAD.encode(payload);
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| "invalid SSO secret")?;
    mac.update(payload.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{payload}.{signature}"))
}

fn decode_sso_transaction_with_secret(
    encoded: &str,
    secret: &[u8],
    now: i64,
) -> Result<SsoTransaction, String> {
    let (payload, signature) = encoded
        .split_once('.')
        .ok_or_else(|| "invalid SSO transaction cookie".to_string())?;
    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| "invalid SSO transaction signature".to_string())?;
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| "invalid SSO secret")?;
    mac.update(payload.as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| "invalid SSO transaction signature".to_string())?;

    let payload = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| "invalid SSO transaction payload".to_string())?;
    let transaction: SsoTransaction = serde_json::from_slice(&payload)
        .map_err(|_| "invalid SSO transaction payload".to_string())?;
    if transaction.version != SSO_TRANSACTION_VERSION {
        return Err("unsupported SSO transaction version".to_string());
    }
    if transaction.issued_at > now + 60
        || now.saturating_sub(transaction.issued_at) > SSO_TRANSACTION_TTL_SECS
    {
        return Err("SSO transaction expired".to_string());
    }
    Ok(transaction)
}

fn encode_sso_transaction(transaction: &SsoTransaction) -> Result<String, String> {
    encode_sso_transaction_with_secret(transaction, &sso_transaction_secret()?)
}

fn setup_token_hash(token: &str) -> Result<String, String> {
    let mut mac =
        HmacSha256::new_from_slice(&sso_transaction_secret()?).map_err(|_| "invalid SSO secret")?;
    mac.update(b"rush-sso-setup-token-v1\0");
    mac.update(token.as_bytes());
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn encode_setup_session_with_secret(
    session: &SsoSetupSession,
    secret: &[u8],
) -> Result<String, String> {
    let payload = serde_json::to_vec(session)
        .map_err(|error| format!("failed to encode SSO setup session: {error}"))?;
    let payload = URL_SAFE_NO_PAD.encode(payload);
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| "invalid SSO secret")?;
    mac.update(payload.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{payload}.{signature}"))
}

fn decode_setup_session_with_secret(
    encoded: &str,
    secret: &[u8],
    now: i64,
) -> Result<SsoSetupSession, String> {
    let (payload, signature) = encoded
        .split_once('.')
        .ok_or_else(|| "invalid SSO setup session".to_string())?;
    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| "invalid SSO setup session".to_string())?;
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| "invalid SSO secret")?;
    mac.update(payload.as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| "invalid SSO setup session".to_string())?;
    let payload = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| "invalid SSO setup session".to_string())?;
    let session: SsoSetupSession =
        serde_json::from_slice(&payload).map_err(|_| "invalid SSO setup session".to_string())?;
    if session.version != SSO_TRANSACTION_VERSION
        || session.issued_at > now + 60
        || now.saturating_sub(session.issued_at) > SSO_SETUP_TTL_SECS
    {
        return Err("SSO setup session expired".to_string());
    }
    Ok(session)
}

fn setup_session_cookie(value: &str, max_age: i64) -> String {
    if insecure_cookies_enabled() {
        format!(
            "{SSO_SETUP_COOKIE_INSECURE}={value}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age}"
        )
    } else {
        format!(
            "{SSO_SETUP_COOKIE_SECURE}={value}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age={max_age}"
        )
    }
}

fn extract_setup_session(headers: &HeaderMap) -> Result<SsoSetupSession, String> {
    let cookie_name = if insecure_cookies_enabled() {
        SSO_SETUP_COOKIE_INSECURE
    } else {
        SSO_SETUP_COOKIE_SECURE
    };
    let cookie_header = headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "missing SSO setup session".to_string())?;
    let encoded = cookie_header
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix(&format!("{cookie_name}=")))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "missing SSO setup session".to_string())?;
    decode_setup_session_with_secret(
        encoded,
        &sso_transaction_secret()?,
        chrono::Utc::now().timestamp(),
    )
}

fn insecure_cookies_enabled() -> bool {
    std::env::var("RUSH_INSECURE_COOKIES")
        .map(|value| matches!(value.as_str(), "1" | "true"))
        .unwrap_or(false)
}

fn sso_transaction_cookie(value: &str, protocol: &str, max_age: i64) -> Result<String, String> {
    if insecure_cookies_enabled() {
        if protocol == "saml" && max_age > 0 {
            return Err(
                "SAML SSO requires HTTPS so its cross-site callback cookie is secure".into(),
            );
        }
        return Ok(format!(
            "{SSO_TRANSACTION_COOKIE_INSECURE}={value}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age}"
        ));
    }
    let same_site = if protocol == "saml" { "None" } else { "Lax" };
    Ok(format!(
        "{SSO_TRANSACTION_COOKIE_SECURE}={value}; HttpOnly; Secure; SameSite={same_site}; Path=/; Max-Age={max_age}"
    ))
}

fn extract_sso_transaction(headers: &HeaderMap) -> Result<SsoTransaction, String> {
    let cookie_name = if insecure_cookies_enabled() {
        SSO_TRANSACTION_COOKIE_INSECURE
    } else {
        SSO_TRANSACTION_COOKIE_SECURE
    };
    let cookie_header = headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "missing SSO transaction cookie".to_string())?;
    let encoded = cookie_header
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix(&format!("{cookie_name}=")))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "missing SSO transaction cookie".to_string())?;
    decode_sso_transaction_with_secret(
        encoded,
        &sso_transaction_secret()?,
        chrono::Utc::now().timestamp(),
    )
}

fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn validate_oidc_nonce(
    transaction: &SsoTransaction,
    claims: &serde_json::Value,
) -> Result<(), String> {
    let returned_nonce = claims
        .get("nonce")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "id_token missing nonce claim".to_string())?;
    if returned_nonce != transaction.nonce {
        return Err("id_token nonce did not match the login transaction".to_string());
    }
    Ok(())
}

fn external_identity_key(provider_id: &str, issuer: &str, subject: &str) -> String {
    let mut digest = Sha256::new();
    for value in [provider_id, issuer, subject] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    URL_SAFE_NO_PAD.encode(digest.finalize())
}

fn setup_provider_protocol(provider: &str) -> Option<&'static str> {
    match provider {
        "custom-oidc" => Some("oidc"),
        "google" | "okta" | "azure" | "custom-saml" => Some("saml"),
        _ => None,
    }
}

async fn find_namespaced_external_user(
    state: &AppState,
    headers: &HeaderMap,
    provider_id: &str,
    issuer: &str,
    subject: &str,
    auth_provider: &str,
) -> Result<(Option<String>, String), (StatusCode, String)> {
    let identity_key = external_identity_key(provider_id, issuer, subject);
    let existing = state
        .config_db
        .find_user_by_external_id(&identity_key, auth_provider)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_external_identity_lookup",
                error,
                "SSO authentication could not be completed",
            )
        })?;
    if existing.is_some() {
        return Ok((existing, identity_key));
    }

    // One-time compatibility migration for identities created before issuer and
    // provider namespacing was introduced. The migration is performed only
    // after a fully verified assertion from the currently configured provider.
    let legacy = state
        .config_db
        .find_user_by_external_id(subject, auth_provider)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_legacy_identity_lookup",
                error,
                "SSO authentication could not be completed",
            )
        })?;
    if let Some(user_id) = legacy {
        state
            .config_db
            .update_user_external_identity(&user_id, auth_provider, &identity_key)
            .await
            .map_err(|error| {
                public_sso_internal_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "sso_external_identity_migrate",
                    error,
                    "SSO authentication could not be completed",
                )
            })?;
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sso.identity_namespace_migrate", "system")
                    .tenant("default".to_string())
                    .resource("user", user_id.clone())
                    .outcome("success")
                    .changes(
                        serde_json::json!({
                            "auth_provider": auth_provider,
                            "provider_id": provider_id,
                        })
                        .to_string(),
                    )
                    .context(crate::audit::actor_context_from_headers(headers)),
            )
            .await;
        return Ok((Some(user_id), identity_key));
    }
    Ok((None, identity_key))
}

async fn claim_sso_key_once(
    state: &AppState,
    key: String,
    expires_at: i64,
) -> Result<bool, (StatusCode, String)> {
    state
        .config_db
        .claim_sso_key_once(&key, expires_at)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "sso_replay_claim",
                error,
                "SSO authentication is temporarily unavailable",
            )
        })
}

fn provider_session_revocation(
    is_update: bool,
    provider_id: &str,
    enabled: bool,
    active_provider_id: Option<&str>,
) -> Option<(String, &'static str)> {
    if is_update && !enabled {
        return Some((provider_id.to_string(), "sso_provider_disabled"));
    }
    if enabled {
        return active_provider_id
            .filter(|active_id| *active_id != provider_id)
            .map(|active_id| (active_id.to_string(), "sso_provider_replaced"));
    }
    None
}

async fn revoke_sso_provider_sessions(
    state: &AppState,
    headers: &HeaderMap,
    caller: Option<&AuthenticatedCaller>,
    provider_id: &str,
    reason: &'static str,
) -> Result<(), (StatusCode, String)> {
    let actor_type = if caller.is_some() {
        "user"
    } else {
        "anonymous"
    };
    let tenant = caller
        .map(|caller| caller.3.clone())
        .unwrap_or_else(|| "default".to_string());
    let event = |outcome: &'static str| {
        crate::audit::AuditEvent::new("session.revoke", actor_type)
            .tenant(tenant.clone())
            .resource("sso_provider", provider_id)
            .outcome(outcome)
            .changes(
                serde_json::json!({
                    "provider_id": provider_id,
                    "reason": reason,
                })
                .to_string(),
            )
            .description("sessions issued by an SSO provider revoked")
            .context(crate::audit::actor_context_from_headers(headers))
    };

    match state
        .config_db
        .revoke_sso_sessions_for_provider(provider_id)
        .await
    {
        Ok(()) => {
            let event = event("success");
            let event = if let Some(caller) = caller {
                event.actor(caller.0.clone(), caller.1.clone())
            } else {
                event.actor_name("SSO setup link")
            };
            state.audit.log(event).await;
            Ok(())
        }
        Err(error) => {
            let event = event("failure");
            let event = if let Some(caller) = caller {
                event.actor(caller.0.clone(), caller.1.clone())
            } else {
                event.actor_name("SSO setup link")
            };
            state.audit.log(event).await;
            Err(public_sso_internal_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "sso_session_revoke",
                error,
                "SSO provider change is temporarily unavailable",
            ))
        }
    }
}

// ── SSO Types ──

#[derive(Serialize)]
pub struct SsoProviderResponse {
    pub id: String,
    pub name: String,
    pub protocol: String,
    pub enabled: bool,
    pub client_id: String,
    pub issuer_url: String,
    pub oidc_scopes: String,
    pub groups_claim: String,
    pub email_claim: String,
    pub first_name_claim: String,
    pub last_name_claim: String,
    pub jit_provisioning: bool,
    pub default_group_id: String,
    pub created_at: String,
    // SAML-specific fields
    pub saml_idp_metadata_url: String,
    pub saml_idp_sso_url: String,
    pub saml_idp_cert: String,
    pub saml_sp_entity_id: String,
}

#[derive(Deserialize)]
pub struct SaveSsoProviderRequest {
    pub id: Option<String>,
    pub name: String,
    pub protocol: Option<String>,
    pub enabled: Option<bool>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub issuer_url: Option<String>,
    pub oidc_scopes: Option<String>,
    pub groups_claim: Option<String>,
    pub email_claim: Option<String>,
    pub first_name_claim: Option<String>,
    pub last_name_claim: Option<String>,
    pub jit_provisioning: Option<bool>,
    pub default_group_id: Option<String>,
    // SAML-specific fields
    pub saml_idp_metadata_url: Option<String>,
    pub saml_idp_sso_url: Option<String>,
    pub saml_idp_cert: Option<String>,
    pub saml_sp_entity_id: Option<String>,
}

#[derive(Serialize)]
pub struct IdpGroupMappingResponse {
    pub id: String,
    pub idp_group: String,
    pub rush_group_id: String,
    pub provider_id: String,
    pub created_at: String,
}

#[derive(Deserialize)]
pub struct CreateMappingRequest {
    pub idp_group: String,
    pub rush_group_id: String,
    pub provider_id: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateMappingRequest {
    pub idp_group: String,
    pub rush_group_id: String,
}

#[derive(Deserialize)]
pub struct SsoCallbackQuery {
    pub code: String,
    pub state: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct SsoLoginQuery {
    pub redirect: Option<String>,
}

#[derive(Serialize)]
pub struct SsoStatusResponse {
    pub enabled: bool,
    pub provider_name: String,
    pub protocol: String,
    pub local_auth_restricted: bool,
}

// ── OIDC Token Response ──

#[derive(Deserialize)]
struct OidcTokenResponse {
    id_token: Option<String>,
    #[allow(dead_code)]
    access_token: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct OidcProfile {
    external_id: String,
    username: String,
    display_name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct OidcDiscovery {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
}

const OIDC_METADATA_MAX_BYTES: usize = 1024 * 1024;

fn validate_oidc_endpoint(raw: &str, label: &str) -> anyhow::Result<url::Url> {
    let parsed = url::Url::parse(raw).map_err(|_| anyhow::anyhow!("{label} is not a valid URL"))?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        anyhow::bail!("{label} must be an HTTPS URL without credentials or a fragment");
    }
    Ok(parsed)
}

async fn fetch_bounded_oidc_json<T: serde::de::DeserializeOwned>(
    raw_url: &str,
    label: &str,
) -> anyhow::Result<T> {
    validate_oidc_endpoint(raw_url, label)?;
    let response = crate::outbound::strict_public_https_request(reqwest::Method::GET, raw_url)
        .await
        .map_err(|e| anyhow::anyhow!("{label} request rejected: {e}"))?
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("{label} request failed: {e}"))?
        .error_for_status()
        .map_err(|e| anyhow::anyhow!("{label} returned an error: {e}"))?;
    parse_bounded_oidc_response(response, label).await
}

async fn parse_bounded_oidc_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    label: &str,
) -> anyhow::Result<T> {
    if response
        .content_length()
        .is_some_and(|length| length > OIDC_METADATA_MAX_BYTES as u64)
    {
        anyhow::bail!("{label} response is too large");
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("{label} response failed: {e}"))?;
    if bytes.len() > OIDC_METADATA_MAX_BYTES {
        anyhow::bail!("{label} response is too large");
    }
    serde_json::from_slice(&bytes).map_err(|e| anyhow::anyhow!("{label} is invalid JSON: {e}"))
}

async fn fetch_oidc_discovery(issuer_url: &str) -> anyhow::Result<OidcDiscovery> {
    let issuer = validate_oidc_endpoint(issuer_url, "OIDC issuer")?;
    if issuer.query().is_some() {
        anyhow::bail!("OIDC issuer must not include a query");
    }
    let discovery_url = format!(
        "{}/.well-known/openid-configuration",
        issuer_url.trim_end_matches('/')
    );
    let discovery: OidcDiscovery =
        fetch_bounded_oidc_json(&discovery_url, "OIDC discovery document").await?;
    validate_oidc_discovery(&discovery, issuer_url)?;
    Ok(discovery)
}

fn validate_oidc_discovery(discovery: &OidcDiscovery, issuer_url: &str) -> anyhow::Result<()> {
    if discovery.issuer != issuer_url {
        anyhow::bail!("OIDC discovery issuer does not exactly match the configured issuer");
    }
    validate_oidc_endpoint(
        &discovery.authorization_endpoint,
        "OIDC authorization endpoint",
    )?;
    validate_oidc_endpoint(&discovery.token_endpoint, "OIDC token endpoint")?;
    validate_oidc_endpoint(&discovery.jwks_uri, "OIDC JWKS endpoint")?;
    Ok(())
}

fn oidc_string_claim<'a>(claims: &'a serde_json::Value, name: &str) -> Option<&'a str> {
    let name = name.trim();
    if name.is_empty() {
        return None;
    }
    claims
        .get(name)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// Apply the OIDC client-binding rules that `jsonwebtoken` cannot express.
/// In particular, a generic JWT audience check accepts a matching audience
/// even when the token also names another client. Rush has no configured set
/// of trusted co-audiences, so fail closed on every additional audience and
/// verify `azp` whenever the provider supplies it.
fn validate_oidc_client_binding_at(
    claims: &serde_json::Value,
    client_id: &str,
    now: i64,
) -> anyhow::Result<()> {
    let audiences = match claims.get("aud") {
        Some(serde_json::Value::String(audience)) if !audience.is_empty() => {
            vec![audience.as_str()]
        }
        Some(serde_json::Value::Array(audiences)) if !audiences.is_empty() => audiences
            .iter()
            .map(|audience| {
                audience
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow::anyhow!("OIDC audience entries must be strings"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        _ => anyhow::bail!("id_token is missing a valid 'aud' claim"),
    };

    if !audiences.contains(&client_id) {
        anyhow::bail!("id_token audience does not include this OIDC client");
    }
    if audiences.iter().any(|audience| *audience != client_id) {
        anyhow::bail!("id_token contains an untrusted additional audience");
    }

    if let Some(authorized_party) = claims.get("azp") {
        if authorized_party.as_str() != Some(client_id) {
            anyhow::bail!("id_token authorized party does not match this OIDC client");
        }
    } else if audiences.len() > 1 {
        anyhow::bail!("a multi-audience id_token must include 'azp'");
    }

    let issued_at = claims
        .get("iat")
        .and_then(serde_json::Value::as_i64)
        .or_else(|| {
            claims
                .get("iat")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| i64::try_from(value).ok())
        })
        .ok_or_else(|| anyhow::anyhow!("id_token is missing a valid 'iat' claim"))?;
    if issued_at < 0 {
        anyhow::bail!("id_token has an invalid 'iat' claim");
    }
    if issued_at > now.saturating_add(OIDC_IAT_FUTURE_SKEW_SECS) {
        anyhow::bail!("id_token was issued implausibly far in the future");
    }

    Ok(())
}

fn validate_oidc_client_binding(claims: &serde_json::Value, client_id: &str) -> anyhow::Result<()> {
    validate_oidc_client_binding_at(claims, client_id, chrono::Utc::now().timestamp())
}

fn extract_oidc_profile(
    claims: &serde_json::Value,
    email_claim: &str,
    first_name_claim: &str,
    last_name_claim: &str,
) -> anyhow::Result<OidcProfile> {
    let external_id = oidc_string_claim(claims, "sub")
        .ok_or_else(|| anyhow::anyhow!("id_token missing 'sub' claim"))?
        .to_string();

    // An email address is suitable as a username only when the IdP explicitly
    // attests that it is verified. Providers that omit `email_verified` can
    // still authenticate: Rush safely falls back to the stable OIDC subject.
    let verified_email = match oidc_string_claim(claims, email_claim) {
        Some(email) => match claims.get("email_verified") {
            Some(serde_json::Value::Bool(true)) => Some(email.to_string()),
            Some(serde_json::Value::Bool(false)) => {
                anyhow::bail!("id_token email claim is not verified")
            }
            Some(_) => anyhow::bail!("id_token 'email_verified' claim must be a boolean"),
            None => None,
        },
        None => None,
    };

    let first_name = oidc_string_claim(claims, first_name_claim).unwrap_or("");
    let last_name = oidc_string_claim(claims, last_name_claim).unwrap_or("");
    let configured_name = format!("{first_name} {last_name}").trim().to_string();
    let display_name = if !configured_name.is_empty() {
        configured_name
    } else {
        oidc_string_claim(claims, "name")
            .or_else(|| oidc_string_claim(claims, "preferred_username"))
            .map(str::to_string)
            .or_else(|| verified_email.clone())
            .unwrap_or_else(|| external_id.clone())
    };
    let username = verified_email.unwrap_or_else(|| external_id.clone());

    Ok(OidcProfile {
        external_id,
        username,
        display_name,
    })
}

// ── Initiate SSO Login (protocol-aware: OIDC or SAML) ──

/// GET /auth/sso/login -- Redirect to IdP.
/// If protocol is saml, generates SAMLRequest and redirects to IdP SSO URL.
/// If protocol is oidc, redirects to OIDC authorize URL with code/state.
pub async fn sso_login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SsoLoginQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let redirect_path = safe_sso_redirect(query.redirect.as_deref());
    let provider = state
        .config_db
        .get_enabled_sso_provider()
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_login_provider_lookup",
                error,
                "SSO is temporarily unavailable",
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "no SSO provider is enabled".to_string(),
            )
        })?;

    let (
        provider_id,
        _name,
        protocol,
        _enabled,
        client_id,
        _client_secret,
        issuer_url,
        oidc_scopes,
        _groups_claim,
        _email_claim,
        _first_name_claim,
        _last_name_claim,
        _jit,
        _default_group,
        _created_at,
        _saml_meta,
        saml_idp_sso_url,
        _saml_cert,
        saml_sp_entity_id,
    ) = provider;

    match protocol.as_str() {
        "saml" => {
            if saml_idp_sso_url.is_empty()
                || saml_sp_entity_id.is_empty()
                || _saml_cert.trim().is_empty()
                || issuer_url.trim().is_empty()
            {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "SAML provider is incomplete; IdP issuer, SSO URL, SP entity ID, and signing certificate are required"
                        .to_string(),
                ));
            }
            let base_url = resolve_base_url(&headers).map_err(|error| {
                public_sso_internal_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "saml_login_base_url",
                    error,
                    "SSO is temporarily unavailable",
                )
            })?;
            let acs_url = format!("{base_url}/auth/sso/acs");
            let relay_state = redirect_path.as_str();

            let login_request = saml::build_login_redirect_url(
                &saml_sp_entity_id,
                &acs_url,
                &saml_idp_sso_url,
                relay_state,
            );
            let transaction = SsoTransaction {
                version: SSO_TRANSACTION_VERSION,
                protocol: "saml".to_string(),
                provider_id,
                state: String::new(),
                nonce: String::new(),
                pkce_verifier: String::new(),
                saml_request_id: login_request.request_id,
                redirect_path: redirect_path.clone(),
                issued_at: chrono::Utc::now().timestamp(),
            };
            let encoded = encode_sso_transaction(&transaction).map_err(|error| {
                public_sso_internal_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "saml_transaction_encode",
                    error,
                    "SSO is temporarily unavailable",
                )
            })?;
            let cookie = sso_transaction_cookie(&encoded, "saml", SSO_TRANSACTION_TTL_SECS)
                .map_err(|error| {
                    public_sso_internal_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "saml_transaction_cookie",
                        error,
                        "SSO is temporarily unavailable",
                    )
                })?;

            let mut resp_headers = HeaderMap::new();
            resp_headers.insert(
                header::SET_COOKIE,
                cookie.parse().map_err(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to create SSO transaction cookie".to_string(),
                    )
                })?,
            );
            resp_headers.insert(
                header::LOCATION,
                login_request.redirect_url.parse().map_err(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "invalid redirect URL".to_string(),
                    )
                })?,
            );
            Ok((StatusCode::FOUND, resp_headers, "").into_response())
        }
        _ => {
            // OIDC flow
            let csrf_state = random_urlsafe::<32>();
            let nonce = random_urlsafe::<32>();
            let pkce_verifier = random_urlsafe::<32>();
            let base = resolve_base_url(&headers).map_err(|error| {
                public_sso_internal_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "oidc_login_base_url",
                    error,
                    "SSO is temporarily unavailable",
                )
            })?;
            let redirect_uri = format!("{base}/auth/sso/callback");
            let discovery = fetch_oidc_discovery(&issuer_url).await.map_err(|e| {
                tracing::warn!(reason = %e, "OIDC discovery rejected");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "OIDC provider metadata is unavailable or invalid".to_string(),
                )
            })?;
            let mut authorize_url =
                url::Url::parse(&discovery.authorization_endpoint).map_err(|_| {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "OIDC authorization endpoint is invalid".to_string(),
                    )
                })?;
            authorize_url
                .query_pairs_mut()
                .append_pair("client_id", &client_id)
                .append_pair("redirect_uri", &redirect_uri)
                .append_pair("response_type", "code")
                .append_pair("scope", &oidc_scopes)
                .append_pair("state", &csrf_state)
                .append_pair("nonce", &nonce)
                .append_pair("code_challenge", &pkce_challenge(&pkce_verifier))
                .append_pair("code_challenge_method", "S256");

            let transaction = SsoTransaction {
                version: SSO_TRANSACTION_VERSION,
                protocol: "oidc".to_string(),
                provider_id,
                state: csrf_state,
                nonce,
                pkce_verifier,
                saml_request_id: String::new(),
                redirect_path: redirect_path.clone(),
                issued_at: chrono::Utc::now().timestamp(),
            };
            let encoded = encode_sso_transaction(&transaction).map_err(|error| {
                public_sso_internal_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "oidc_transaction_encode",
                    error,
                    "SSO is temporarily unavailable",
                )
            })?;
            let cookie = sso_transaction_cookie(&encoded, "oidc", SSO_TRANSACTION_TTL_SECS)
                .map_err(|error| {
                    public_sso_internal_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "oidc_transaction_cookie",
                        error,
                        "SSO is temporarily unavailable",
                    )
                })?;

            let mut resp_headers = HeaderMap::new();
            resp_headers.insert(
                header::SET_COOKIE,
                cookie.parse().map_err(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to create SSO transaction cookie".to_string(),
                    )
                })?,
            );
            resp_headers.insert(
                header::LOCATION,
                authorize_url.as_str().parse().map_err(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "invalid OIDC redirect URL".to_string(),
                    )
                })?,
            );
            Ok((StatusCode::FOUND, resp_headers, "").into_response())
        }
    }
}

// ── OIDC Callback ──

async fn audit_sso_failure(
    state: &AppState,
    headers: &HeaderMap,
    method: &str,
    status: StatusCode,
) {
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("auth.login.failure", "anonymous")
                .outcome("failure")
                .description(format!("{method} SSO authentication failed"))
                .changes(
                    serde_json::json!({
                        "method": method,
                        "http_status": status.as_u16(),
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(headers)),
        )
        .await;
}

/// GET /auth/sso/callback?code=...&state=... -- Exchange code for tokens, JIT provision user
pub async fn sso_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<SsoCallbackQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let audit_headers = headers.clone();
    let result = sso_callback_inner(state.clone(), headers, params).await;
    if let Err((status, _)) = &result {
        audit_sso_failure(&state, &audit_headers, "oidc", *status).await;
    }
    result
}

async fn sso_callback_inner(
    state: AppState,
    headers: HeaderMap,
    params: SsoCallbackQuery,
) -> Result<(StatusCode, HeaderMap, &'static str), (StatusCode, String)> {
    // 1. Verify the callback is bound to the browser that initiated this OIDC
    // transaction. The signed HttpOnly cookie also carries the nonce and PKCE
    // verifier, so none of these values are accepted from callback parameters.
    let transaction = extract_sso_transaction(&headers).map_err(|error| {
        public_sso_rejection(
            StatusCode::BAD_REQUEST,
            "oidc_transaction_validate",
            error,
            "SSO login transaction is invalid or expired",
        )
    })?;
    if transaction.protocol != "oidc" || transaction.state != params.state {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid or expired state parameter".to_string(),
        ));
    }

    // 2. Load the enabled SSO provider
    let provider = state
        .config_db
        .get_enabled_sso_provider()
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "oidc_callback_provider_lookup",
                error,
                "SSO authentication could not be completed",
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "no SSO provider is enabled".to_string(),
            )
        })?;

    let (
        provider_id,
        _name,
        _protocol,
        _enabled,
        client_id,
        client_secret,
        issuer_url,
        _oidc_scopes,
        groups_claim,
        email_claim,
        first_name_claim,
        last_name_claim,
        jit_provisioning,
        default_group_id,
        _created_at,
        _f13,
        _f14,
        _f15,
        _f16,
    ) = provider;
    if transaction.provider_id != provider_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "SSO provider changed during authentication".to_string(),
        ));
    }

    // 3. Exchange authorization code for tokens
    let discovery = fetch_oidc_discovery(&issuer_url).await.map_err(|e| {
        tracing::warn!(reason = %e, "OIDC discovery rejected during callback");
        (
            StatusCode::BAD_GATEWAY,
            "OIDC provider metadata is unavailable or invalid".to_string(),
        )
    })?;
    let base = resolve_base_url(&headers).map_err(|error| {
        public_sso_internal_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "oidc_callback_base_url",
            error,
            "SSO authentication could not be completed",
        )
    })?;
    let redirect_uri = format!("{base}/auth/sso/callback");

    let token_res = crate::outbound::strict_public_https_request(
        reqwest::Method::POST,
        &discovery.token_endpoint,
    )
    .await
    .map_err(|error| {
        public_sso_internal_error(
            StatusCode::BAD_GATEWAY,
            "oidc_token_endpoint_policy",
            error,
            "SSO provider request failed",
        )
    })?
    .form(&[
        ("grant_type", "authorization_code"),
        ("code", params.code.as_str()),
        ("redirect_uri", redirect_uri.as_str()),
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("code_verifier", transaction.pkce_verifier.as_str()),
    ])
    .send()
    .await
    .map_err(|error| {
        public_sso_internal_error(
            StatusCode::BAD_GATEWAY,
            "oidc_token_exchange",
            error,
            "SSO provider request failed",
        )
    })?;

    if !token_res.status().is_success() {
        tracing::warn!(status = %token_res.status(), "OIDC token exchange failed");
        return Err((
            StatusCode::BAD_GATEWAY,
            "IdP token exchange failed".to_string(),
        ));
    }

    let token_data: OidcTokenResponse =
        parse_bounded_oidc_response(token_res, "OIDC token response")
            .await
            .map_err(|error| {
                tracing::warn!(reason = %error, "OIDC token response rejected");
                (
                    StatusCode::BAD_GATEWAY,
                    "invalid token response".to_string(),
                )
            })?;

    let id_token = token_data.id_token.ok_or_else(|| {
        (
            StatusCode::BAD_GATEWAY,
            "no id_token in response".to_string(),
        )
    })?;

    // 4. Verify the id_token JWT signature against the provider's JWKS and decode claims
    let claims = verify_and_decode_jwt(&id_token, &discovery, &issuer_url, &client_id)
        .await
        .map_err(|error| {
            public_sso_rejection(
                StatusCode::BAD_GATEWAY,
                "oidc_id_token_verify",
                error,
                "SSO identity token is invalid",
            )
        })?;
    validate_oidc_nonce(&transaction, &claims).map_err(|error| {
        public_sso_rejection(
            StatusCode::UNAUTHORIZED,
            "oidc_nonce_validate",
            error,
            "SSO identity token is invalid",
        )
    })?;
    if !claim_sso_key_once(
        &state,
        format!("oidc:{}", transaction.state),
        transaction.issued_at + SSO_TRANSACTION_TTL_SECS,
    )
    .await?
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "OIDC login transaction was already consumed".to_string(),
        ));
    }

    // 5. Extract the configured profile claims. Unverified email addresses are
    // never promoted into Rush usernames.
    let profile = extract_oidc_profile(&claims, &email_claim, &first_name_claim, &last_name_claim)
        .map_err(|error| {
            public_sso_rejection(
                StatusCode::UNAUTHORIZED,
                "oidc_profile_extract",
                error,
                "SSO identity token is invalid",
            )
        })?;
    let external_id = profile.external_id;
    let username = profile.username;
    let display_name = profile.display_name;

    // 6. Extract groups from the configurable groups claim
    let idp_groups: Vec<String> = claims
        .get(&groups_claim)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // 7. Map IdP groups to Rush groups
    let mut mapped_group_ids = state
        .config_db
        .resolve_idp_groups(&idp_groups, &provider_id)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "oidc_group_mapping",
                error,
                "SSO authentication could not be completed",
            )
        })?;

    // If no mappings match, use default_group_id from provider config
    if mapped_group_ids.is_empty() && !default_group_id.is_empty() {
        mapped_group_ids.push(default_group_id);
    }

    // If still nothing, fall back to the built-in viewers group
    if mapped_group_ids.is_empty() {
        mapped_group_ids.push("viewers".to_string());
    }

    // 8. JIT provision: find or create user
    let (existing_user, identity_key) = find_namespaced_external_user(
        &state,
        &headers,
        &provider_id,
        &issuer_url,
        &external_id,
        "oidc",
    )
    .await?;
    let (user_id, jit_created) = match existing_user {
        Some(uid) => (uid, false),
        None => {
            if !jit_provisioning {
                return Err((
                    StatusCode::FORBIDDEN,
                    "JIT provisioning is disabled and user does not exist".to_string(),
                ));
            }
            let id = state
                .config_db
                .create_sso_user(&username, &display_name, &identity_key, "oidc", "default")
                .await
                .map_err(|error| {
                    public_sso_internal_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "oidc_user_create",
                        error,
                        "SSO authentication could not be completed",
                    )
                })?;
            (id, true)
        }
    };

    if jit_created {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("user.create", "system")
                    .tenant("default")
                    .resource("user", user_id.clone())
                    .outcome("success")
                    .changes(
                        serde_json::json!({
                            "username": &username,
                            "auth_provider": "oidc",
                            "jit_provisioned": true,
                        })
                        .to_string(),
                    )
                    .description("SSO user provisioned after verified OIDC authentication")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }

    // 9. Update the user's group memberships with the mapped set
    state
        .config_db
        .update_user_groups_from_idp(&user_id, &mapped_group_ids)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "oidc_group_update",
                error,
                "SSO authentication could not be completed",
            )
        })?;

    // 10. Create a session (same as local auth)
    let issued = state
        .config_db
        .create_sso_session(&user_id, "oidc", &provider_id)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "oidc_session_create",
                error,
                "SSO authentication could not be completed",
            )
        })?;

    // 11. Set the rush_session cookie and redirect to /
    let cookie = crate::handlers::auth::session_cookie(&issued.token, issued.max_age_seconds);
    let clear_transaction = sso_transaction_cookie("", "oidc", 0).map_err(|error| {
        public_sso_internal_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "oidc_transaction_clear",
            error,
            "SSO authentication could not be completed",
        )
    })?;

    // Audit the successful OIDC authentication without logging the code,
    // tokens, transaction cookie, or IdP claims beyond the stable provider id.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("auth.login.success", "user")
                .actor(user_id.clone(), username.clone())
                .tenant("default".to_string())
                .outcome("success")
                .description("user authenticated (SSO/OIDC)")
                .changes(
                    serde_json::json!({
                        "method": "oidc",
                        "provider_id": provider_id,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    let mut headers = HeaderMap::new();
    headers.append(header::SET_COOKIE, cookie.parse().unwrap());
    headers.append(
        header::SET_COOKIE,
        clear_transaction.parse().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to clear SSO transaction cookie".to_string(),
            )
        })?,
    );
    headers.insert(
        header::LOCATION,
        safe_sso_redirect(Some(&transaction.redirect_path))
            .parse()
            .unwrap_or_else(|_| "/".parse().unwrap()),
    );

    Ok((StatusCode::FOUND, headers, ""))
}

fn oidc_jwk_is_eligible(jwk: &jsonwebtoken::jwk::Jwk, algorithm: jsonwebtoken::Algorithm) -> bool {
    use jsonwebtoken::Algorithm;
    use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, KeyAlgorithm, KeyOperations};

    if jwk
        .common
        .public_key_use
        .as_ref()
        .is_some_and(|key_use| key_use != &jsonwebtoken::jwk::PublicKeyUse::Signature)
    {
        return false;
    }
    if jwk
        .common
        .key_operations
        .as_ref()
        .is_some_and(|operations| !operations.contains(&KeyOperations::Verify))
    {
        return false;
    }

    let expected_key_algorithm = match algorithm {
        Algorithm::RS256 => KeyAlgorithm::RS256,
        Algorithm::RS384 => KeyAlgorithm::RS384,
        Algorithm::RS512 => KeyAlgorithm::RS512,
        Algorithm::PS256 => KeyAlgorithm::PS256,
        Algorithm::PS384 => KeyAlgorithm::PS384,
        Algorithm::PS512 => KeyAlgorithm::PS512,
        Algorithm::ES256 => KeyAlgorithm::ES256,
        Algorithm::ES384 => KeyAlgorithm::ES384,
        _ => return false,
    };
    if jwk
        .common
        .key_algorithm
        .is_some_and(|declared| declared != expected_key_algorithm)
    {
        return false;
    }

    match (&jwk.algorithm, algorithm) {
        (
            AlgorithmParameters::RSA(_),
            Algorithm::RS256
            | Algorithm::RS384
            | Algorithm::RS512
            | Algorithm::PS256
            | Algorithm::PS384
            | Algorithm::PS512,
        ) => true,
        (AlgorithmParameters::EllipticCurve(parameters), Algorithm::ES256) => {
            parameters.curve == EllipticCurve::P256
        }
        (AlgorithmParameters::EllipticCurve(parameters), Algorithm::ES384) => {
            parameters.curve == EllipticCurve::P384
        }
        _ => false,
    }
}

fn select_oidc_jwk<'a>(
    jwks: &'a jsonwebtoken::jwk::JwkSet,
    kid: Option<&str>,
    algorithm: jsonwebtoken::Algorithm,
) -> anyhow::Result<&'a jsonwebtoken::jwk::Jwk> {
    if let Some(kid) = kid {
        let matching: Vec<_> = jwks
            .keys
            .iter()
            .filter(|jwk| jwk.common.key_id.as_deref() == Some(kid))
            .collect();
        if matching.len() != 1 {
            anyhow::bail!("OIDC JWKS must contain exactly one key for the token kid");
        }
        let jwk = matching[0];
        if !oidc_jwk_is_eligible(jwk, algorithm) {
            anyhow::bail!("OIDC JWK is not eligible for this token signature");
        }
        return Ok(jwk);
    }

    let compatible: Vec<_> = jwks
        .keys
        .iter()
        .filter(|jwk| oidc_jwk_is_eligible(jwk, algorithm))
        .collect();
    if compatible.len() != 1 {
        anyhow::bail!("an OIDC token without kid requires exactly one compatible signing key");
    }
    Ok(compatible[0])
}

/// Verify an OIDC id_token JWT signature against the provider's JWKS endpoint and return claims.
/// Fetches the OIDC discovery document to resolve the JWKS URI, then verifies the signature.
/// Rejects `alg:none`, ambiguous keys, unsuitable key metadata, and invalid signatures.
async fn verify_and_decode_jwt(
    token: &str,
    discovery: &OidcDiscovery,
    issuer_url: &str,
    client_id: &str,
) -> anyhow::Result<serde_json::Value> {
    use jsonwebtoken::jwk::JwkSet;
    use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};

    // Parse the JWT header to get `kid` and `alg` — does not verify signature
    let header = jsonwebtoken::decode_header(token)
        .map_err(|e| anyhow::anyhow!("invalid JWT header: {e}"))?;

    // Only accept asymmetric algorithms — reject symmetric (HS*) which would require
    // sharing the client_secret as the signing key, an unsafe pattern for OIDC.
    match header.alg {
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512
        | Algorithm::ES256
        | Algorithm::ES384 => {}
        alg => anyhow::bail!("JWT algorithm {alg:?} is not accepted for OIDC"),
    }

    // Fetch the JSON Web Key Set
    let jwks: JwkSet = fetch_bounded_oidc_json(&discovery.jwks_uri, "OIDC JWKS").await?;

    // A missing `kid` is safe only when the set has one unambiguous key that
    // is suitable for this exact signature algorithm. Likewise, a named key
    // must explicitly permit signature verification when metadata is present.
    let jwk = select_oidc_jwk(&jwks, header.kid.as_deref(), header.alg)?;

    let decoding_key = DecodingKey::from_jwk(jwk)
        .map_err(|e| anyhow::anyhow!("failed to build decoding key from JWK: {e}"))?;

    let mut validation = Validation::new(header.alg);
    validation.leeway = OIDC_IAT_FUTURE_SKEW_SECS as u64;
    validation.set_required_spec_claims(&["exp", "iss", "aud", "sub", "iat"]);
    validation.set_issuer(&[issuer_url]);
    // Validate the audience claim against the registered client_id.
    // This ensures tokens issued for other apps at the same IdP are rejected.
    validation.set_audience(&[client_id]);

    let token_data = decode::<serde_json::Value>(token, &decoding_key, &validation)
        .map_err(|e| anyhow::anyhow!("JWT signature verification failed: {e}"))?;

    validate_oidc_client_binding(&token_data.claims, client_id)?;

    Ok(token_data.claims)
}

// ── SSO Config Admin Endpoints ──

/// GET /api/v1/sso/providers -- List all SSO providers
pub async fn list_sso_providers(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let rows = state
        .config_db
        .list_sso_providers()
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_provider_list",
                error,
                "SSO configuration is temporarily unavailable",
            )
        })?;

    let providers: Vec<SsoProviderResponse> = rows
        .into_iter()
        .map(
            |(
                id,
                name,
                protocol,
                enabled,
                client_id,
                _secret,
                issuer_url,
                oidc_scopes,
                groups_claim,
                email_claim,
                first_name_claim,
                last_name_claim,
                jit,
                default_group_id,
                created_at,
                saml_meta,
                saml_sso,
                saml_cert,
                saml_entity,
            )| {
                SsoProviderResponse {
                    id,
                    name,
                    protocol,
                    enabled,
                    client_id,
                    issuer_url,
                    oidc_scopes,
                    groups_claim,
                    email_claim,
                    first_name_claim,
                    last_name_claim,
                    jit_provisioning: jit,
                    default_group_id,
                    created_at,
                    saml_idp_metadata_url: saml_meta,
                    saml_idp_sso_url: saml_sso,
                    saml_idp_cert: saml_cert,
                    saml_sp_entity_id: saml_entity,
                }
            },
        )
        .collect();

    // This response contains security-sensitive identity-provider metadata.
    // Record the read without copying any provider values into the audit log.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("sso.config_read", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("sso_provider", "all")
                .outcome("success")
                .metadata(
                    serde_json::json!({
                        "provider_count": providers.len(),
                    })
                    .to_string(),
                )
                .description("SSO provider configuration listed")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(serde_json::json!({ "providers": providers })))
}

/// POST /api/v1/sso/providers -- Create or update an SSO provider
pub async fn save_sso_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SaveSsoProviderRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let admin_result = require_admin(&state, &headers).await;
    let setup_session = if admin_result.is_err() {
        extract_setup_session(&headers).ok()
    } else {
        None
    };
    if admin_result.is_err() && setup_session.is_none() {
        return Err(admin_result.unwrap_err());
    }
    let is_update = req.id.is_some();
    let id = req.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let protocol = req.protocol.as_deref().unwrap_or("oidc");
    if !matches!(protocol, "oidc" | "saml") {
        return Err((
            StatusCode::BAD_REQUEST,
            "SSO protocol must be 'oidc' or 'saml'".to_string(),
        ));
    }
    let enabled = req.enabled.unwrap_or(false);
    if let Some(session) = &setup_session {
        let expected_protocol = setup_provider_protocol(&session.provider).ok_or_else(|| {
            (
                StatusCode::FORBIDDEN,
                "invalid setup session provider".to_string(),
            )
        })?;
        if is_update || !enabled || protocol != expected_protocol {
            return Err((
                StatusCode::FORBIDDEN,
                "setup links may only create and enable their assigned SSO provider type"
                    .to_string(),
            ));
        }
    }

    // If updating and no new secret provided, keep the existing one
    let client_secret = match &req.client_secret {
        Some(s) if !s.is_empty() => s.clone(),
        _ => {
            // Try to load existing secret
            state
                .config_db
                .get_sso_provider(&id)
                .await
                .map_err(|error| {
                    public_sso_internal_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "sso_provider_secret_lookup",
                        error,
                        "SSO provider could not be saved",
                    )
                })?
                .map(|p| p.5)
                .unwrap_or_default()
        }
    };

    if enabled && protocol == "saml" {
        if req.issuer_url.as_deref().unwrap_or("").trim().is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "SAML IdP issuer/entity ID is required".to_string(),
            ));
        }
        let certificate = req.saml_idp_cert.as_deref().unwrap_or("");
        saml::validate_idp_certificate(certificate).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        let sso_url = req.saml_idp_sso_url.as_deref().unwrap_or("");
        let parsed = url::Url::parse(sso_url).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "SAML IdP SSO URL must be a valid HTTPS URL".to_string(),
            )
        })?;
        if parsed.scheme() != "https" {
            return Err((
                StatusCode::BAD_REQUEST,
                "SAML IdP SSO URL must use HTTPS".to_string(),
            ));
        }
        if req
            .saml_sp_entity_id
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
        {
            return Err((
                StatusCode::BAD_REQUEST,
                "SAML SP entity ID is required".to_string(),
            ));
        }
    }
    if enabled && protocol == "oidc" {
        let issuer = req.issuer_url.as_deref().unwrap_or("");
        let parsed = url::Url::parse(issuer).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "OIDC issuer must be a valid HTTPS URL".to_string(),
            )
        })?;
        if parsed.scheme() != "https" || parsed.query().is_some() || parsed.fragment().is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                "OIDC issuer must be an HTTPS URL without query or fragment".to_string(),
            ));
        }
        if req.client_id.as_deref().unwrap_or("").trim().is_empty()
            || client_secret.trim().is_empty()
        {
            return Err((
                StatusCode::BAD_REQUEST,
                "OIDC client ID and client secret are required".to_string(),
            ));
        }
        fetch_oidc_discovery(issuer).await.map_err(|error| {
            tracing::warn!(reason = %error, "OIDC provider configuration rejected");
            (
                StatusCode::BAD_REQUEST,
                "OIDC discovery metadata or endpoints are invalid".to_string(),
            )
        })?;
    }

    // Do not burn the one-time setup session until the submitted provider has
    // passed all protocol-specific validation. This lets an administrator fix
    // an invalid certificate or discovery URL without requesting another link.
    if let Some(session) = &setup_session {
        let setup_key = format!("sso-setup-session:{}", session.session_id);
        let already_revoked = state
            .config_db
            .is_sso_setup_session_revoked(&setup_key)
            .await
            .map_err(|error| {
                public_sso_internal_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "sso_setup_revocation_lookup",
                    error,
                    "SSO setup is temporarily unavailable",
                )
            })?;
        if already_revoked {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("sso.setup_session_consume", "anonymous")
                        .actor_name("SSO setup link")
                        .outcome("failure")
                        .changes(
                            serde_json::json!({
                                "provider": session.provider,
                                "reason": "already_revoked",
                            })
                            .to_string(),
                        )
                        .description("revoked SSO setup session reuse rejected")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            return Err((
                StatusCode::CONFLICT,
                "setup session was already used".to_string(),
            ));
        }
        if let Err(error) = state
            .config_db
            .revoke_sso_setup_session(&setup_key, session.issued_at + SSO_SETUP_TTL_SECS)
            .await
        {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("sso.setup_session_revoke", "anonymous")
                        .actor_name("SSO setup link")
                        .outcome("failure")
                        .changes(serde_json::json!({ "provider": session.provider }).to_string())
                        .description("SSO setup session durable revocation failed")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            return Err(public_sso_internal_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "sso_setup_revocation_write",
                error,
                "SSO setup is temporarily unavailable",
            ));
        }
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sso.setup_session_revoke", "anonymous")
                    .actor_name("SSO setup link")
                    .outcome("success")
                    .changes(serde_json::json!({ "provider": session.provider }).to_string())
                    .description("SSO setup session durably revoked before use")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        if !claim_sso_key_once(&state, setup_key, session.issued_at + SSO_SETUP_TTL_SECS).await? {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("sso.setup_session_consume", "anonymous")
                        .actor_name("SSO setup link")
                        .outcome("failure")
                        .changes(
                            serde_json::json!({
                                "provider": session.provider,
                                "reason": "concurrent_use",
                            })
                            .to_string(),
                        )
                        .description("concurrent SSO setup session reuse rejected")
                        .context(crate::audit::actor_context_from_headers(&headers)),
                )
                .await;
            return Err((
                StatusCode::CONFLICT,
                "setup session was already used".to_string(),
            ));
        }
    }

    let active_before_change = state
        .config_db
        .active_sso_provider_id()
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "sso_active_provider_lookup",
                error,
                "SSO provider change is temporarily unavailable",
            )
        })?;
    if let Some((provider_id, reason)) =
        provider_session_revocation(is_update, &id, enabled, active_before_change.as_deref())
    {
        revoke_sso_provider_sessions(
            &state,
            &headers,
            admin_result.as_ref().ok(),
            &provider_id,
            reason,
        )
        .await?;
    }

    let previous_active_provider_id = state
        .config_db
        .upsert_sso_provider(
            &id,
            &req.name,
            protocol,
            enabled,
            req.client_id.as_deref().unwrap_or(""),
            &client_secret,
            req.issuer_url.as_deref().unwrap_or(""),
            req.oidc_scopes
                .as_deref()
                .unwrap_or("openid profile email groups"),
            req.groups_claim.as_deref().unwrap_or("groups"),
            req.email_claim.as_deref().unwrap_or("email"),
            req.first_name_claim.as_deref().unwrap_or("given_name"),
            req.last_name_claim.as_deref().unwrap_or("family_name"),
            req.jit_provisioning.unwrap_or(true),
            req.default_group_id.as_deref().unwrap_or(""),
            req.saml_idp_metadata_url.as_deref().unwrap_or(""),
            req.saml_idp_sso_url.as_deref().unwrap_or(""),
            req.saml_idp_cert.as_deref().unwrap_or(""),
            req.saml_sp_entity_id.as_deref().unwrap_or(""),
        )
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_provider_save",
                error,
                "SSO provider could not be saved",
            )
        })?;
    let active_provider_id = if enabled {
        Some(id.clone())
    } else if previous_active_provider_id.as_deref() == Some(id.as_str()) {
        None
    } else {
        previous_active_provider_id.clone()
    };

    tracing::info!(event = "sso_provider_saved", provider_id = %id, "SSO provider saved");

    // AUDIT: SSO provider config update. NEVER log client_secret or SAML cert —
    // only non-sensitive config (name/protocol/issuer/enabled).
    let changes = serde_json::json!({
        "name": req.name,
        "protocol": protocol,
        "enabled": enabled,
        "issuer_url": req.issuer_url.as_deref().unwrap_or(""),
        "client_id": req.client_id.as_deref().unwrap_or(""),
        "client_secret_set": req.client_secret.as_deref().map(|s| !s.is_empty()).unwrap_or(false),
        "previous_active_provider_id": previous_active_provider_id,
        "active_provider_id": active_provider_id,
    })
    .to_string();
    if let Ok(caller) = &admin_result {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sso.config_update", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("sso_provider", id.clone())
                    .changes(changes.clone())
                    .description("sso provider config updated")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    } else {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sso.config_update", "anonymous")
                    .actor_name("SSO setup link")
                    .tenant("default".to_string())
                    .resource("sso_provider", id.clone())
                    .changes(changes.clone())
                    .description("sso provider created through scoped setup session")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }
    // Also emit an explicit enable/disable event reflecting the new state.
    if let Some(enabled) = req.enabled {
        let event = crate::audit::AuditEvent::new(
            if enabled { "sso.enable" } else { "sso.disable" },
            if admin_result.is_ok() {
                "user"
            } else {
                "anonymous"
            },
        )
        .tenant(
            admin_result
                .as_ref()
                .map(|caller| caller.3.clone())
                .unwrap_or_else(|_| "default".to_string()),
        )
        .resource("sso_provider", id.clone())
        .changes(
            serde_json::json!({
                "enabled": enabled,
                "previous_active_provider_id": previous_active_provider_id,
                "active_provider_id": active_provider_id,
            })
            .to_string(),
        )
        .description("sso provider enabled state set")
        .context(crate::audit::actor_context_from_headers(&headers));
        let event = if let Ok(caller) = &admin_result {
            event.actor(caller.0.clone(), caller.1.clone())
        } else {
            event.actor_name("SSO setup link")
        };
        state.audit.log(event).await;
    }

    Ok(Json(serde_json::json!({ "id": id, "ok": true })))
}

/// DELETE /api/v1/sso/providers/{id} -- Delete an SSO provider
pub async fn delete_sso_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let provider = state
        .config_db
        .get_sso_provider(&id)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_provider_lookup_for_delete",
                error,
                "SSO provider could not be deleted",
            )
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "provider not found".to_string()))?;
    let was_active = provider.3;
    revoke_sso_provider_sessions(&state, &headers, Some(&caller), &id, "sso_provider_deleted")
        .await?;
    let deleted = state
        .config_db
        .delete_sso_provider(&id)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_provider_delete",
                error,
                "SSO provider could not be deleted",
            )
        })?;

    if deleted {
        tracing::info!(
            event = "sso_provider_deleted",
            provider_id = %id,
            admin = %caller.1,
            "SSO provider deleted"
        );
        // AUDIT: SSO provider deleted.
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sso.config_update", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("sso_provider", id.clone())
                    .changes(
                        serde_json::json!({
                            "deleted": true,
                            "was_active": was_active,
                            "active_provider_cleared": was_active,
                        })
                        .to_string(),
                    )
                    .description("sso provider deleted")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err((StatusCode::NOT_FOUND, "provider not found".to_string()))
    }
}

/// GET /api/v1/sso/mappings -- List IdP group mappings
pub async fn list_idp_group_mappings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let rows = state
        .config_db
        .list_idp_group_mappings(None)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_group_mapping_list",
                error,
                "SSO configuration is temporarily unavailable",
            )
        })?;

    let mappings: Vec<IdpGroupMappingResponse> = rows
        .into_iter()
        .map(
            |(id, idp_group, rush_group_id, provider_id, created_at)| IdpGroupMappingResponse {
                id,
                idp_group,
                rush_group_id,
                provider_id,
                created_at,
            },
        )
        .collect();

    Ok(Json(serde_json::json!({ "mappings": mappings })))
}

/// POST /api/v1/sso/mappings -- Create a mapping
pub async fn create_idp_group_mapping(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateMappingRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let provider_id = req.provider_id.as_deref().unwrap_or("default");

    let id = state
        .config_db
        .create_idp_group_mapping(&req.idp_group, &req.rush_group_id, provider_id)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_group_mapping_create",
                error,
                "SSO group mapping could not be created",
            )
        })?;

    // AUDIT: IdP→group mapping created.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("sso.group_mapping_change", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("idp_group_mapping", id.clone())
                .changes(
                    serde_json::json!({
                        "action": "create",
                        "idp_group": req.idp_group,
                        "rush_group_id": req.rush_group_id,
                        "provider_id": provider_id
                    })
                    .to_string(),
                )
                .description("idp group mapping created")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(serde_json::json!({ "id": id, "ok": true })))
}

/// PUT /api/v1/sso/mappings/{id} -- Update a mapping
pub async fn update_idp_group_mapping(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<UpdateMappingRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;

    let prev = state
        .config_db
        .update_idp_group_mapping(&id, &req.idp_group, &req.rush_group_id)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_group_mapping_update",
                error,
                "SSO group mapping could not be updated",
            )
        })?;

    let Some((old_idp_group, old_rush_group_id)) = prev else {
        return Err((StatusCode::NOT_FOUND, "mapping not found".to_string()));
    };

    // AUDIT: IdP→group mapping updated.
    state.audit.log(
        crate::audit::AuditEvent::new("sso.group_mapping_change", "user")
            .actor(caller.0.clone(), caller.1.clone())
            .tenant(caller.3.clone())
            .resource("idp_group_mapping", id.clone())
            .changes(serde_json::json!({
                "action": "update",
                "before": { "idp_group": old_idp_group, "rush_group_id": old_rush_group_id },
                "after": { "idp_group": req.idp_group, "rush_group_id": req.rush_group_id }
            }).to_string())
            .description("idp group mapping updated")
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;

    Ok(Json(serde_json::json!({ "ok": true })))
}

/// DELETE /api/v1/sso/mappings/{id} -- Delete a mapping
pub async fn delete_idp_group_mapping(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_idp_group_mapping(&id)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_group_mapping_delete",
                error,
                "SSO group mapping could not be deleted",
            )
        })?;

    if deleted {
        // AUDIT: IdP→group mapping deleted.
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sso.group_mapping_change", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("idp_group_mapping", id.clone())
                    .changes(serde_json::json!({ "action": "delete" }).to_string())
                    .description("idp group mapping deleted")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err((StatusCode::NOT_FOUND, "mapping not found".to_string()))
    }
}

// ── SAML Assertion Consumer Service ──

/// POST /auth/sso/acs -- SAML ACS endpoint.
/// The IdP posts the SAMLResponse here after user authenticates.
pub async fn sso_acs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let audit_headers = headers.clone();
    let result = sso_acs_inner(state.clone(), headers, body).await;
    if let Err((status, _)) = &result {
        audit_sso_failure(&state, &audit_headers, "saml", *status).await;
    }
    result
}

async fn sso_acs_inner(
    state: AppState,
    headers: HeaderMap,
    body: String,
) -> Result<axum::response::Response, (StatusCode, String)> {
    if body.len() > 2 * 1024 * 1024 {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "SAML response is too large".to_string(),
        ));
    }
    let transaction = extract_sso_transaction(&headers).map_err(|error| {
        public_sso_rejection(
            StatusCode::BAD_REQUEST,
            "saml_transaction_validate",
            error,
            "SSO login transaction is invalid or expired",
        )
    })?;
    if transaction.protocol != "saml" || transaction.saml_request_id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid SAML login transaction".to_string(),
        ));
    }
    let params: Vec<(String, String)> = url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect();

    let saml_response = params
        .iter()
        .find(|(k, _)| k == "SAMLResponse")
        .map(|(_, v)| v.as_str())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "missing SAMLResponse in POST body".to_string(),
            )
        })?;

    let provider = state
        .config_db
        .get_enabled_sso_provider()
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "saml_callback_provider_lookup",
                error,
                "SSO authentication could not be completed",
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "no SSO provider configured".to_string(),
            )
        })?;

    let (
        provider_id,
        _name,
        _protocol,
        _enabled,
        _client_id,
        _client_secret,
        issuer_url,
        _oidc_scopes,
        groups_claim,
        _email_claim,
        _first_name_claim,
        _last_name_claim,
        jit_provisioning,
        default_group_id,
        _created_at,
        _saml_meta,
        _saml_sso,
        saml_cert,
        _saml_entity,
    ) = provider;
    if transaction.provider_id != provider_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "SSO provider changed during authentication".to_string(),
        ));
    }

    // Decode the base64 SAMLResponse to raw XML for signature verification
    let xml_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        saml_response.trim(),
    )
    .map_err(|error| {
        public_sso_rejection(
            StatusCode::BAD_REQUEST,
            "saml_response_decode",
            error,
            "invalid SAMLResponse encoding",
        )
    })?;
    let xml = String::from_utf8_lossy(&xml_bytes);

    // Fail closed: a configured signing certificate and a signature covering
    // the exact content we consume are mandatory for every SAML login.
    if saml_cert.trim().is_empty() {
        tracing::error!(provider_id = %provider_id, "enabled SAML provider has no certificate");
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "SSO authentication is temporarily unavailable".to_string(),
        ));
    }
    let signed_xml = saml::verify_signature(&xml, &saml_cert).map_err(|e| {
        tracing::warn!("SAML signature check failed: {e}");
        (
            StatusCode::UNAUTHORIZED,
            "SAML signature verification failed".to_string(),
        )
    })?;

    let base_url = resolve_base_url(&headers).map_err(|error| {
        public_sso_internal_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "saml_acs_base_url",
            error,
            "SSO authentication could not be completed",
        )
    })?;
    let acs_url = format!("{base_url}/auth/sso/acs");
    let assertion = saml::validate_signed_assertion(
        &signed_xml,
        &groups_claim,
        &transaction.saml_request_id,
        &acs_url,
        &_saml_entity,
        &issuer_url,
        chrono::Utc::now().timestamp(),
    )
    .map_err(|e| {
        tracing::warn!(reason = %e, "SAML assertion rejected");
        (
            StatusCode::UNAUTHORIZED,
            "SAML assertion did not satisfy service-provider constraints".to_string(),
        )
    })?;

    let replay_expiry = assertion
        .expires_at
        .min(transaction.issued_at + SSO_TRANSACTION_TTL_SECS);
    for replay_key in [
        format!("saml-request:{}", transaction.saml_request_id),
        format!("saml-assertion:{}", assertion.assertion_id),
    ] {
        if !claim_sso_key_once(&state, replay_key, replay_expiry).await? {
            return Err((
                StatusCode::UNAUTHORIZED,
                "SAML response has already been consumed".to_string(),
            ));
        }
    }
    if let Some(response_id) = &assertion.response_id {
        if !claim_sso_key_once(
            &state,
            format!("saml-response:{response_id}"),
            replay_expiry,
        )
        .await?
        {
            return Err((
                StatusCode::UNAUTHORIZED,
                "SAML response has already been consumed".to_string(),
            ));
        }
    }

    tracing::info!(provider_id = %provider_id, "SAML assertion validated");

    let mut mapped_group_ids = state
        .config_db
        .resolve_idp_groups(&assertion.groups, &provider_id)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "saml_group_mapping",
                error,
                "SSO authentication could not be completed",
            )
        })?;

    if mapped_group_ids.is_empty() {
        if !default_group_id.is_empty() {
            mapped_group_ids.push(default_group_id);
        } else {
            mapped_group_ids.push("viewers".to_string());
        }
    }

    let external_id = &assertion.name_id;
    let auth_provider = "saml";

    let (existing_user, identity_key) = find_namespaced_external_user(
        &state,
        &headers,
        &provider_id,
        &assertion.issuer,
        external_id,
        auth_provider,
    )
    .await?;
    let (user_id, jit_created) = match existing_user {
        Some(uid) => (uid, false),
        None => {
            if !jit_provisioning {
                return Err((
                    StatusCode::FORBIDDEN,
                    "user not found and JIT provisioning is disabled".to_string(),
                ));
            }
            let email = assertion.email.as_deref().unwrap_or(&assertion.name_id);
            let display = assertion.display_name.as_deref().unwrap_or(email);
            let id = state
                .config_db
                .create_sso_user(email, display, &identity_key, auth_provider, "default")
                .await
                .map_err(|error| {
                    public_sso_internal_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "saml_user_create",
                        error,
                        "SSO authentication could not be completed",
                    )
                })?;
            (id, true)
        }
    };

    if jit_created {
        let username = assertion.email.as_deref().unwrap_or(&assertion.name_id);
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("user.create", "system")
                    .tenant("default")
                    .resource("user", user_id.clone())
                    .outcome("success")
                    .changes(
                        serde_json::json!({
                            "username": username,
                            "auth_provider": auth_provider,
                            "jit_provisioned": true,
                        })
                        .to_string(),
                    )
                    .description("SSO user provisioned after verified SAML authentication")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }

    state
        .config_db
        .update_user_groups_from_idp(&user_id, &mapped_group_ids)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "saml_group_update",
                error,
                "SSO authentication could not be completed",
            )
        })?;

    let issued = state
        .config_db
        .create_sso_session(&user_id, "saml", &provider_id)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "saml_session_create",
                error,
                "SSO authentication could not be completed",
            )
        })?;

    // AUDIT: successful SSO login (mirrors the local auth.login.success event).
    // Logs identity + provider/groups only — never the assertion, cert, or token.
    let actor_name = assertion
        .email
        .clone()
        .unwrap_or_else(|| assertion.name_id.clone());
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("auth.login.success", "user")
                .actor(user_id.clone(), actor_name)
                .tenant("default".to_string())
                .outcome("success")
                .description("user authenticated (SSO/SAML)")
                .changes(
                    serde_json::json!({
                        "method": "saml",
                        "provider_id": provider_id,
                        "name_id": assertion.name_id,
                        "groups": mapped_group_ids,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    let cookie = crate::handlers::auth::session_cookie(&issued.token, issued.max_age_seconds);
    let clear_transaction = sso_transaction_cookie("", "saml", 0).map_err(|error| {
        public_sso_internal_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "saml_transaction_clear",
            error,
            "SSO authentication could not be completed",
        )
    })?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.append(header::SET_COOKIE, cookie.parse().unwrap());
    resp_headers.append(
        header::SET_COOKIE,
        clear_transaction.parse().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to clear SSO transaction cookie".to_string(),
            )
        })?,
    );
    resp_headers.insert(
        header::LOCATION,
        safe_sso_redirect(Some(&transaction.redirect_path))
            .parse()
            .unwrap_or_else(|_| "/".parse().unwrap()),
    );

    Ok((StatusCode::FOUND, resp_headers, "").into_response())
}

// ── SAML SP Metadata ──

/// GET /auth/sso/metadata -- Return SP metadata XML.
/// Administrators paste this into their IdP when configuring the SAML app.
pub async fn sso_metadata(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let provider = state
        .config_db
        .get_enabled_sso_provider()
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "saml_metadata_provider_lookup",
                error,
                "SSO metadata is temporarily unavailable",
            )
        })?;

    let base_url = resolve_base_url(&headers).map_err(|error| {
        public_sso_internal_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "saml_metadata_base_url",
            error,
            "SSO metadata is temporarily unavailable",
        )
    })?;
    let acs_url = format!("{base_url}/auth/sso/acs");

    let sp_entity_id = match &provider {
        Some(p) if !p.15.is_empty() => p.15.clone(),
        _ => base_url.clone(),
    };

    let xml = saml::build_sp_metadata(&sp_entity_id, &acs_url);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(header::CONTENT_TYPE, "application/xml".parse().unwrap());

    Ok((resp_headers, xml))
}

/// GET /api/v1/sso/status -- Return whether SSO is enabled (for login page)
pub async fn sso_status(
    State(state): State<AppState>,
) -> Result<Json<SsoStatusResponse>, (StatusCode, String)> {
    match state
        .config_db
        .get_enabled_sso_provider()
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sso_status_read",
                error,
                "SSO status is temporarily unavailable",
            )
        })? {
        Some((
            _id,
            name,
            protocol,
            _enabled,
            _client_id,
            _secret,
            _issuer,
            _scopes,
            _claim,
            _email,
            _first,
            _last,
            _jit,
            _default,
            _created,
            _saml_meta,
            _saml_sso,
            _saml_cert,
            _saml_entity,
        )) => Ok(Json(SsoStatusResponse {
            enabled: true,
            provider_name: name,
            protocol,
            local_auth_restricted: crate::handlers::auth::sso_only_mode_enabled(),
        })),
        None => Ok(Json(SsoStatusResponse {
            enabled: false,
            provider_name: String::new(),
            protocol: String::new(),
            local_auth_restricted: false,
        })),
    }
}

// ── Setup token endpoints ──

#[derive(Deserialize)]
pub struct CreateSetupTokenRequest {
    pub purpose: Option<String>,
    pub provider: Option<String>,
}

#[derive(Deserialize)]
pub struct ExchangeSetupTokenRequest {
    pub token: String,
}

/// POST /api/v1/sso/setup-token -- Create a one-time setup link for security teams
pub async fn create_setup_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateSetupTokenRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let caller = crate::handlers::users::require_admin(&state, &headers).await?;
    let purpose = req.purpose.as_deref().unwrap_or("sso_setup");
    let provider = req.provider.as_deref().unwrap_or("");
    if purpose != "sso_setup" || setup_provider_protocol(provider).is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "unsupported SSO setup link".to_string(),
        ));
    }
    let base = resolve_base_url(&headers).map_err(|error| {
        public_sso_internal_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "setup_token_base_url",
            error,
            "SSO setup is temporarily unavailable",
        )
    })?;
    let token = random_urlsafe::<32>();
    let token_hash = setup_token_hash(&token).map_err(|error| {
        public_sso_internal_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "setup_token_hash",
            error,
            "SSO setup is temporarily unavailable",
        )
    })?;

    state
        .config_db
        .create_setup_token(&token_hash, purpose, &caller.0, provider, &base)
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "setup_token_create",
                error,
                "SSO setup is temporarily unavailable",
            )
        })?;
    // Keep the bearer token in the fragment so browsers do not send it in the
    // request target, Referer header, reverse-proxy logs, or server access logs.
    let url = format!("{base}/setup/sso#token={token}");

    // The one-time token itself is never written to the audit row. Record only
    // its purpose and non-secret setup metadata so token creation is traceable.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("sso.setup_token_create", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("sso_setup_token", "one-time")
                .changes(
                    serde_json::json!({
                        "purpose": purpose,
                        "provider": provider,
                        "expires_in_seconds": SSO_SETUP_TTL_SECS,
                    })
                    .to_string(),
                )
                .description("sso setup token created")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(serde_json::json!({ "token": token, "url": url })))
}

/// POST /api/v1/sso/setup-token/exchange -- consume a URL token and establish
/// a narrowly scoped HttpOnly setup session.
pub async fn exchange_setup_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ExchangeSetupTokenRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if req.token.len() < 32 || req.token.len() > 256 {
        return Err((StatusCode::BAD_REQUEST, "invalid setup token".to_string()));
    }
    let token_hash = setup_token_hash(&req.token).map_err(|error| {
        public_sso_internal_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "setup_token_hash",
            error,
            "SSO setup is temporarily unavailable",
        )
    })?;
    let consumed = state
        .config_db
        .consume_setup_token(&token_hash, "sso_setup")
        .await
        .map_err(|error| {
            public_sso_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "setup_token_consume",
                error,
                "SSO setup is temporarily unavailable",
            )
        })?;
    let (provider, _hostname, created_by) = consumed.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            "setup token is invalid or expired".to_string(),
        )
    })?;
    if !claim_sso_key_once(
        &state,
        format!("sso-setup-link:{token_hash}"),
        chrono::Utc::now().timestamp() + SSO_SETUP_TTL_SECS,
    )
    .await?
    {
        return Err((
            StatusCode::CONFLICT,
            "setup token was already used".to_string(),
        ));
    }
    let session = SsoSetupSession {
        version: SSO_TRANSACTION_VERSION,
        provider: provider.clone(),
        created_by,
        session_id: random_urlsafe::<24>(),
        issued_at: chrono::Utc::now().timestamp(),
    };
    let setup_secret = sso_transaction_secret().map_err(|error| {
        public_sso_internal_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "setup_session_secret",
            error,
            "SSO setup is temporarily unavailable",
        )
    })?;
    let encoded = encode_setup_session_with_secret(&session, &setup_secret).map_err(|error| {
        public_sso_internal_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "setup_session_encode",
            error,
            "SSO setup is temporarily unavailable",
        )
    })?;
    let cookie = setup_session_cookie(&encoded, SSO_SETUP_TTL_SECS);
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("sso.setup_token_exchange", "anonymous")
                .actor_name("SSO setup link")
                .outcome("success")
                .changes(serde_json::json!({ "provider": provider }).to_string())
                .description("SSO setup link exchanged for scoped session")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::SET_COOKIE,
        cookie.parse().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create setup session".to_string(),
            )
        })?,
    );
    Ok((
        response_headers,
        Json(serde_json::json!({ "valid": true, "provider": provider })),
    ))
}

/// GET /api/v1/sso/setup-session -- validate the scoped setup cookie.
pub async fn validate_setup_session(
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let session = extract_setup_session(&headers).map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            "setup session is invalid or expired".to_string(),
        )
    })?;

    Ok(Json(
        serde_json::json!({ "valid": true, "provider": session.provider }),
    ))
}

/// POST /api/v1/sso/setup-session/complete -- end the narrowly scoped setup session.
pub async fn complete_setup_session(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let session = extract_setup_session(&headers).map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            "setup session is invalid or expired".to_string(),
        )
    })?;
    // Clearing a browser cookie is not revocation: copied cookies would remain
    // usable until expiry. Persist a one-way session key in the TTL-backed
    // revocation ledger, then claim it in the atomic local/Keeper store.
    // `false` is an idempotent success because saving a provider consumes the
    // same capability before the UI calls this completion endpoint.
    let setup_key = format!("sso-setup-session:{}", session.session_id);
    if let Err(error) = state
        .config_db
        .revoke_sso_setup_session(&setup_key, session.issued_at + SSO_SETUP_TTL_SECS)
        .await
    {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sso.setup_session_complete", "anonymous")
                    .actor_name("SSO setup link")
                    .outcome("failure")
                    .changes(serde_json::json!({ "provider": session.provider }).to_string())
                    .description("SSO setup session durable revocation failed")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        return Err(public_sso_internal_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "sso_setup_revocation_write",
            error,
            "SSO setup is temporarily unavailable",
        ));
    }
    let revocation_newly_recorded =
        match claim_sso_key_once(&state, setup_key, session.issued_at + SSO_SETUP_TTL_SECS).await {
            Ok(claimed) => claimed,
            Err(error) => {
                state
                    .audit
                    .log(
                        crate::audit::AuditEvent::new("sso.setup_session_complete", "anonymous")
                            .actor_name("SSO setup link")
                            .outcome("failure")
                            .changes(
                                serde_json::json!({ "provider": session.provider }).to_string(),
                            )
                            .description("SSO setup session atomic revocation failed")
                            .context(crate::audit::actor_context_from_headers(&headers)),
                    )
                    .await;
                return Err(error);
            }
        };
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("sso.setup_session_complete", "anonymous")
                .actor_name("SSO setup link")
                .outcome("success")
                .changes(
                    serde_json::json!({
                        "provider": session.provider,
                        "revocation_newly_recorded": revocation_newly_recorded,
                    })
                    .to_string(),
                )
                .description("SSO setup session completed")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::SET_COOKIE,
        setup_session_cookie("", 0).parse().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to clear setup session".to_string(),
            )
        })?,
    );
    Ok((response_headers, Json(serde_json::json!({ "ok": true }))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_sso_errors_do_not_expose_internal_details() {
        let internal = "clickhouse host=db.internal password=secret";
        let (status, message) = public_sso_internal_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "test_database_failure",
            internal,
            "SSO authentication could not be completed",
        );
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(message, "SSO authentication could not be completed");
        assert!(!message.contains("clickhouse"));
        assert!(!message.contains("secret"));

        let (status, message) = public_sso_rejection(
            StatusCode::UNAUTHORIZED,
            "test_jwt_failure",
            "JWT signature verification failed for kid internal-signing-key",
            "SSO identity token is invalid",
        );
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(message, "SSO identity token is invalid");
        assert!(!message.contains("signature"));
        assert!(!message.contains("internal-signing-key"));
    }

    #[test]
    fn sso_configuration_endpoints_do_not_return_raw_storage_errors() {
        let source = include_str!("sso.rs");
        assert!(!source.contains("format!(\"{e}\")"));

        let save = source
            .split_once("\npub async fn save_sso_provider(")
            .map(|(_, endpoint)| endpoint)
            .expect("provider save endpoint must exist")
            .split("\npub async fn delete_sso_provider(")
            .next()
            .expect("provider delete must follow save");
        assert!(save.contains("sso_provider_secret_lookup"));
        assert!(!save.contains(".await\n                .ok()"));

        for operation in [
            "sso_provider_list",
            "sso_group_mapping_list",
            "sso_group_mapping_create",
            "sso_group_mapping_update",
            "sso_group_mapping_delete",
            "setup_token_create",
        ] {
            assert!(
                source.contains(operation),
                "missing sanitized error boundary for {operation}"
            );
        }
    }

    fn transaction() -> SsoTransaction {
        SsoTransaction {
            version: SSO_TRANSACTION_VERSION,
            protocol: "oidc".to_string(),
            provider_id: "provider-1".to_string(),
            state: "state-1".to_string(),
            nonce: "nonce-1".to_string(),
            pkce_verifier: "verifier-1".to_string(),
            saml_request_id: String::new(),
            redirect_path: "/".to_string(),
            issued_at: 1_700_000_000,
        }
    }

    #[test]
    fn signed_transaction_round_trips_and_rejects_tampering() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let encoded = encode_sso_transaction_with_secret(&transaction(), secret).unwrap();
        assert_eq!(
            decode_sso_transaction_with_secret(&encoded, secret, 1_700_000_100).unwrap(),
            transaction()
        );

        let mut tampered = encoded.into_bytes();
        tampered[5] = if tampered[5] == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered).unwrap();
        assert!(decode_sso_transaction_with_secret(&tampered, secret, 1_700_000_100).is_err());
    }

    #[test]
    fn signed_transaction_expires() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let encoded = encode_sso_transaction_with_secret(&transaction(), secret).unwrap();
        assert!(
            decode_sso_transaction_with_secret(
                &encoded,
                secret,
                1_700_000_000 + SSO_TRANSACTION_TTL_SECS + 1,
            )
            .unwrap_err()
            .contains("expired")
        );
    }

    #[test]
    fn pkce_uses_rfc7636_s256_derivation() {
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        assert_eq!(
            pkce_challenge(verifier),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn oidc_nonce_is_required_and_bound_to_transaction() {
        let transaction = transaction();
        assert!(validate_oidc_nonce(&transaction, &serde_json::json!({})).is_err());
        assert!(
            validate_oidc_nonce(&transaction, &serde_json::json!({ "nonce": "attacker" })).is_err()
        );
        assert!(
            validate_oidc_nonce(
                &transaction,
                &serde_json::json!({ "nonce": transaction.nonce })
            )
            .is_ok()
        );
    }

    #[test]
    fn canonical_base_url_is_an_origin_and_https_in_production() {
        assert_eq!(
            normalize_base_url("https://rush.example.com/", true).unwrap(),
            "https://rush.example.com"
        );
        assert!(normalize_base_url("http://rush.example.com", true).is_err());
        assert!(normalize_base_url("https://rush.example.com/callback", true).is_err());
        assert!(normalize_base_url("https://user@rush.example.com", true).is_err());
        assert_eq!(
            normalize_base_url("http://localhost:5173", false).unwrap(),
            "http://localhost:5173"
        );
    }

    #[test]
    fn oidc_metadata_requires_exact_issuer_and_secure_endpoints() {
        let valid = OidcDiscovery {
            issuer: "https://idp.example.com".to_string(),
            authorization_endpoint: "https://idp.example.com/oauth/authorize".to_string(),
            token_endpoint: "https://idp.example.com/oauth/token".to_string(),
            jwks_uri: "https://keys.example.com/jwks.json".to_string(),
        };
        validate_oidc_discovery(&valid, "https://idp.example.com").unwrap();
        assert!(validate_oidc_discovery(&valid, "https://other.example.com").is_err());

        let mut insecure = valid;
        insecure.jwks_uri = "http://169.254.169.254/latest/meta-data".to_string();
        assert!(validate_oidc_discovery(&insecure, "https://idp.example.com").is_err());
    }

    #[test]
    fn oidc_client_binding_rejects_extra_audiences_and_wrong_authorized_party() {
        let valid = serde_json::json!({
            "aud": "rush-client",
            "azp": "rush-client",
            "iat": 1_700_000_000,
        });
        validate_oidc_client_binding(&valid, "rush-client").unwrap();

        let extra_audience = serde_json::json!({
            "aud": ["rush-client", "other-client"],
            "azp": "rush-client",
            "iat": 1_700_000_000,
        });
        assert!(validate_oidc_client_binding(&extra_audience, "rush-client").is_err());

        let wrong_azp = serde_json::json!({
            "aud": "rush-client",
            "azp": "other-client",
            "iat": 1_700_000_000,
        });
        assert!(validate_oidc_client_binding(&wrong_azp, "rush-client").is_err());
    }

    #[test]
    fn oidc_client_binding_requires_issued_at() {
        let claims = serde_json::json!({ "aud": "rush-client" });
        assert!(validate_oidc_client_binding(&claims, "rush-client").is_err());
    }

    #[test]
    fn oidc_client_binding_rejects_implausibly_future_issued_at() {
        let within_skew = serde_json::json!({
            "aud": "rush-client",
            "iat": 1_700_000_000 + OIDC_IAT_FUTURE_SKEW_SECS,
        });
        validate_oidc_client_binding_at(&within_skew, "rush-client", 1_700_000_000).unwrap();

        let future = serde_json::json!({
            "aud": "rush-client",
            "iat": 1_700_000_000 + OIDC_IAT_FUTURE_SKEW_SECS + 1,
        });
        assert!(validate_oidc_client_binding_at(&future, "rush-client", 1_700_000_000).is_err());

        let negative = serde_json::json!({ "aud": "rush-client", "iat": -1 });
        assert!(validate_oidc_client_binding_at(&negative, "rush-client", 1_700_000_000).is_err());
    }

    fn oidc_jwks(keys: serde_json::Value) -> jsonwebtoken::jwk::JwkSet {
        serde_json::from_value(serde_json::json!({ "keys": keys })).unwrap()
    }

    fn rsa_jwk(kid: &str, key_use: &str, key_ops: &[&str], algorithm: &str) -> serde_json::Value {
        serde_json::json!({
            "kty": "RSA",
            "kid": kid,
            "use": key_use,
            "key_ops": key_ops,
            "alg": algorithm,
            "n": "AQAB",
            "e": "AQAB",
        })
    }

    #[test]
    fn oidc_jwk_selection_requires_one_compatible_key_without_kid() {
        use jsonwebtoken::Algorithm;

        let one = oidc_jwks(serde_json::json!([
            rsa_jwk("signing", "sig", &["verify"], "RS256"),
            rsa_jwk("encryption", "enc", &["decrypt"], "RS256"),
        ]));
        assert_eq!(
            select_oidc_jwk(&one, None, Algorithm::RS256)
                .unwrap()
                .common
                .key_id
                .as_deref(),
            Some("signing")
        );

        let ambiguous = oidc_jwks(serde_json::json!([
            rsa_jwk("first", "sig", &["verify"], "RS256"),
            rsa_jwk("second", "sig", &["verify"], "RS256"),
        ]));
        assert!(select_oidc_jwk(&ambiguous, None, Algorithm::RS256).is_err());
    }

    #[test]
    fn oidc_jwk_selection_validates_use_operations_algorithm_and_unique_kid() {
        use jsonwebtoken::Algorithm;

        for invalid in [
            rsa_jwk("key", "enc", &["verify"], "RS256"),
            rsa_jwk("key", "sig", &["sign"], "RS256"),
            rsa_jwk("key", "sig", &["verify"], "RS384"),
        ] {
            let jwks = oidc_jwks(serde_json::json!([invalid]));
            assert!(select_oidc_jwk(&jwks, Some("key"), Algorithm::RS256).is_err());
        }

        let duplicate = oidc_jwks(serde_json::json!([
            rsa_jwk("key", "sig", &["verify"], "RS256"),
            rsa_jwk("key", "sig", &["verify"], "RS256"),
        ]));
        assert!(select_oidc_jwk(&duplicate, Some("key"), Algorithm::RS256).is_err());
    }

    #[test]
    fn oidc_profile_uses_configured_claims_and_verified_email() {
        let claims = serde_json::json!({
            "sub": "subject-1",
            "mail": "person@example.com",
            "email_verified": true,
            "first": "Ada",
            "last": "Lovelace",
        });
        let profile = extract_oidc_profile(&claims, "mail", "first", "last").unwrap();
        assert_eq!(
            profile,
            OidcProfile {
                external_id: "subject-1".to_string(),
                username: "person@example.com".to_string(),
                display_name: "Ada Lovelace".to_string(),
            }
        );
    }

    #[test]
    fn oidc_profile_rejects_explicitly_unverified_email() {
        let claims = serde_json::json!({
            "sub": "subject-1",
            "email": "unverified@example.com",
            "email_verified": false,
        });
        assert!(extract_oidc_profile(&claims, "email", "given_name", "family_name").is_err());
    }

    #[test]
    fn oidc_profile_without_email_attestation_falls_back_to_subject() {
        let claims = serde_json::json!({
            "sub": "subject-1",
            "email": "unattested@example.com",
            "name": "Example User",
        });
        let profile = extract_oidc_profile(&claims, "email", "given_name", "family_name").unwrap();
        assert_eq!(profile.username, "subject-1");
        assert_eq!(profile.display_name, "Example User");
    }

    #[test]
    fn sso_provider_configuration_listing_requires_admin() {
        // Guard the endpoint itself: testing only `require_admin` would not
        // catch a future regression that accidentally calls `require_auth`.
        let source = include_str!("sso.rs");
        let endpoint = source
            .split("pub async fn list_sso_providers")
            .nth(1)
            .expect("list_sso_providers must exist")
            .split("pub async fn save_sso_provider")
            .next()
            .expect("save_sso_provider must follow the list endpoint");
        assert!(endpoint.contains("require_admin(&state, &headers)"));
        assert!(!endpoint.contains("require_auth(&state, &headers)"));
    }

    #[test]
    fn external_identity_is_namespaced_by_provider_and_issuer() {
        let first = external_identity_key("provider-a", "https://idp.example.com", "subject-1");
        assert_ne!(
            first,
            external_identity_key("provider-b", "https://idp.example.com", "subject-1")
        );
        assert_ne!(
            first,
            external_identity_key("provider-a", "https://other.example.com", "subject-1")
        );
        assert_eq!(
            first,
            external_identity_key("provider-a", "https://idp.example.com", "subject-1")
        );
    }

    #[test]
    fn setup_session_is_signed_and_short_lived() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let session = SsoSetupSession {
            version: SSO_TRANSACTION_VERSION,
            provider: "okta".to_string(),
            created_by: "admin-id".to_string(),
            session_id: "session-1".to_string(),
            issued_at: 1_700_000_000,
        };
        let encoded = encode_setup_session_with_secret(&session, secret).unwrap();
        assert_eq!(
            decode_setup_session_with_secret(&encoded, secret, 1_700_000_100).unwrap(),
            session
        );
        assert!(
            decode_setup_session_with_secret(
                &encoded,
                secret,
                1_700_000_000 + SSO_SETUP_TTL_SECS + 1,
            )
            .is_err()
        );
        assert!(
            decode_setup_session_with_secret(
                &encoded,
                b"fedcba9876543210fedcba9876543210",
                1_700_000_100
            )
            .is_err()
        );
    }

    #[test]
    fn provider_session_revocation_covers_disable_and_replacement() {
        assert_eq!(
            provider_session_revocation(true, "provider-a", false, Some("provider-a")),
            Some(("provider-a".to_string(), "sso_provider_disabled"))
        );
        assert_eq!(
            provider_session_revocation(true, "provider-b", false, Some("provider-a")),
            Some(("provider-b".to_string(), "sso_provider_disabled"))
        );
        assert_eq!(
            provider_session_revocation(false, "provider-b", true, Some("provider-a")),
            Some(("provider-a".to_string(), "sso_provider_replaced"))
        );
        assert_eq!(
            provider_session_revocation(true, "provider-a", true, Some("provider-a")),
            None
        );
        assert_eq!(
            provider_session_revocation(false, "provider-a", false, Some("provider-b")),
            None
        );
    }

    #[test]
    fn provider_lifecycle_revokes_sessions_before_the_provider_mutation() {
        let source = include_str!("sso.rs");
        let save = source
            .split_once("\npub async fn save_sso_provider(")
            .map(|(_, endpoint)| endpoint)
            .expect("provider save endpoint must exist")
            .split("\npub async fn delete_sso_provider(")
            .next()
            .expect("provider delete must follow save");
        assert!(
            save.find("revoke_sso_provider_sessions")
                .expect("save must revoke affected sessions")
                < save
                    .find(".upsert_sso_provider(")
                    .expect("save must persist the provider")
        );

        let delete = source
            .split_once("\npub async fn delete_sso_provider(")
            .map(|(_, endpoint)| endpoint)
            .expect("provider delete endpoint must exist")
            .split("\npub async fn list_idp_group_mappings(")
            .next()
            .expect("group mappings must follow provider delete");
        assert!(
            delete
                .find("revoke_sso_provider_sessions")
                .expect("delete must revoke provider sessions")
                < delete
                    .find(".delete_sso_provider(&id)")
                    .expect("delete must persist the provider deletion")
        );
    }

    #[test]
    fn setup_completion_persists_revocation_before_clearing_the_cookie() {
        let source = include_str!("sso.rs");
        let endpoint = source
            .split_once("\npub async fn complete_setup_session(")
            .map(|(_, endpoint)| endpoint)
            .expect("setup completion endpoint must exist")
            .split("\n#[cfg(test)]")
            .next()
            .expect("tests must follow setup completion");
        let durable_revocation = endpoint
            .find("revoke_sso_setup_session")
            .expect("completion must persist durable revocation");
        let claim = endpoint
            .find("claim_sso_key_once")
            .expect("completion must persist a one-time claim");
        let clear_cookie = endpoint
            .find("setup_session_cookie(\"\", 0)")
            .expect("completion must clear the browser cookie");
        assert!(durable_revocation < claim);
        assert!(claim < clear_cookie);
        assert!(endpoint.contains("revocation_newly_recorded"));
        assert!(endpoint.contains(".outcome(\"failure\")"));
    }

    #[test]
    fn setup_link_is_bound_to_one_supported_protocol() {
        assert_eq!(setup_provider_protocol("custom-oidc"), Some("oidc"));
        assert_eq!(setup_provider_protocol("okta"), Some("saml"));
        assert_eq!(setup_provider_protocol("attacker-controlled"), None);
    }

    #[test]
    fn sso_redirects_accept_only_same_origin_paths() {
        assert_eq!(
            safe_sso_redirect(Some("/kubernetes-access/login/ABC123DE45")),
            "/kubernetes-access/login/ABC123DE45"
        );
        assert_eq!(safe_sso_redirect(Some("//evil.example/path")), "/");
        assert_eq!(safe_sso_redirect(Some("https://evil.example/path")), "/");
        assert_eq!(safe_sso_redirect(Some("/\\evil.example/path")), "/");
        assert_eq!(safe_sso_redirect(Some("/path\nset-cookie:x")), "/");
    }
}

// ── Helpers ──

fn normalize_base_url(raw: &str, production: bool) -> Result<String, String> {
    let parsed =
        url::Url::parse(raw).map_err(|_| "RUSH_BASE_URL is not a valid URL".to_string())?;
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !matches!(parsed.path(), "" | "/")
    {
        return Err(
            "RUSH_BASE_URL must be an origin without credentials, path, query, or fragment"
                .to_string(),
        );
    }
    if production && parsed.scheme() != "https" {
        return Err("RUSH_BASE_URL must use HTTPS in production".to_string());
    }
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err("RUSH_BASE_URL must use HTTP or HTTPS".to_string());
    }
    Ok(raw.trim_end_matches('/').to_string())
}

/// Validate the canonical public origin before the API starts accepting
/// traffic. Production is fail-closed; local development may derive an origin
/// from the Host header for convenience.
pub fn validate_base_url_config() -> Result<(), String> {
    let production = crate::api_key_auth::production_mode();
    match std::env::var("RUSH_BASE_URL") {
        Ok(value) if !value.trim().is_empty() => {
            normalize_base_url(value.trim(), production).map(|_| ())
        }
        _ if production => Err("RUSH_BASE_URL is required in production".to_string()),
        _ => Ok(()),
    }
}

fn resolve_base_url(headers: &HeaderMap) -> Result<String, String> {
    let production = crate::api_key_auth::production_mode();
    if let Ok(base) = std::env::var("RUSH_BASE_URL") {
        if !base.trim().is_empty() {
            return normalize_base_url(base.trim(), production);
        }
    }
    if production {
        return Err("SSO is unavailable until RUSH_BASE_URL is configured".to_string());
    }

    let trust_proxy = crate::api_key_auth::env_flag("RUSH_TRUST_PROXY_HEADERS");
    let scheme = if trust_proxy {
        headers
            .get("x-forwarded-proto")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("http")
    } else if insecure_cookies_enabled() {
        "http"
    } else {
        "https"
    };
    if !matches!(scheme, "http" | "https") {
        return Err("request scheme is invalid".to_string());
    }
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost:8080");
    normalize_base_url(&format!("{scheme}://{host}"), false)
}
