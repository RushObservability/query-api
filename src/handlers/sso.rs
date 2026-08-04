use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use dashmap::{DashMap, mapref::entry::Entry};
use hmac::{Hmac, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

use crate::AppState;
use crate::handlers::users::{require_admin, require_auth};
use crate::saml;

type HmacSha256 = Hmac<Sha256>;

const SSO_TRANSACTION_VERSION: u8 = 1;
const SSO_TRANSACTION_TTL_SECS: i64 = 10 * 60;
const SSO_TRANSACTION_COOKIE_SECURE: &str = "__Host-rush_sso_tx";
const SSO_TRANSACTION_COOKIE_INSECURE: &str = "rush_sso_tx";
const SSO_SETUP_COOKIE_SECURE: &str = "__Host-rush_sso_setup";
const SSO_SETUP_COOKIE_INSECURE: &str = "rush_sso_setup";
const SSO_SETUP_TTL_SECS: i64 = 30 * 60;

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
    let mut mac = HmacSha256::new_from_slice(&sso_transaction_secret()?)
        .map_err(|_| "invalid SSO secret")?;
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
    let session: SsoSetupSession = serde_json::from_slice(&payload)
        .map_err(|_| "invalid SSO setup session".to_string())?;
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

fn external_identity_key(
    provider_id: &str,
    issuer: &str,
    subject: &str,
) -> String {
    let mut digest = Sha256::new();
    for value in [provider_id, issuer, subject] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    URL_SAFE_NO_PAD.encode(digest.finalize())
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
            tracing::error!(%error, "external identity lookup failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
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
            tracing::error!(%error, "legacy external identity lookup failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
        })?;
    if let Some(user_id) = legacy {
        state
            .config_db
            .update_user_external_identity(&user_id, auth_provider, &identity_key)
            .await
            .map_err(|error| {
                tracing::error!(%error, "external identity migration failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
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

static CONSUMED_SSO_KEYS: OnceLock<DashMap<String, i64>> = OnceLock::new();

/// Atomically consume a transaction/replay key inside this API process. OIDC
/// authorization codes and the browser-bound transaction cookie provide the
/// cross-replica backstop; this map closes concurrent replays on one replica.
fn consume_sso_key_once(key: String, expires_at: i64) -> bool {
    let now = chrono::Utc::now().timestamp();
    let consumed = CONSUMED_SSO_KEYS.get_or_init(DashMap::new);
    if consumed.len() > 10_000 {
        consumed.retain(|_, expiry| *expiry > now);
    }
    match consumed.entry(key) {
        Entry::Vacant(entry) => {
            entry.insert(expires_at.max(now + 1));
            true
        }
        Entry::Occupied(_) => false,
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

#[derive(Serialize)]
pub struct SsoStatusResponse {
    pub enabled: bool,
    pub provider_name: String,
    pub protocol: String,
}

// ── OIDC Token Response ──

#[derive(Deserialize)]
struct OidcTokenResponse {
    id_token: Option<String>,
    #[allow(dead_code)]
    access_token: Option<String>,
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
    let response = crate::outbound::public_https_request(reqwest::Method::GET, raw_url)
        .await
        .map_err(|e| anyhow::anyhow!("{label} request rejected: {e}"))?
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("{label} request failed: {e}"))?
        .error_for_status()
        .map_err(|e| anyhow::anyhow!("{label} returned an error: {e}"))?;
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
    if discovery.issuer != issuer_url {
        anyhow::bail!("OIDC discovery issuer does not exactly match the configured issuer");
    }
    validate_oidc_endpoint(&discovery.authorization_endpoint, "OIDC authorization endpoint")?;
    validate_oidc_endpoint(&discovery.token_endpoint, "OIDC token endpoint")?;
    validate_oidc_endpoint(&discovery.jwks_uri, "OIDC JWKS endpoint")?;
    Ok(discovery)
}

// ── Initiate SSO Login (protocol-aware: OIDC or SAML) ──

/// GET /auth/sso/login -- Redirect to IdP.
/// If protocol is saml, generates SAMLRequest and redirects to IdP SSO URL.
/// If protocol is oidc, redirects to OIDC authorize URL with code/state.
pub async fn sso_login(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let provider = state
        .config_db
        .get_enabled_sso_provider()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
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
            let base_url = resolve_base_url(&headers)
                .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;
            let acs_url = format!("{base_url}/auth/sso/acs");
            let relay_state = "/";

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
                redirect_path: relay_state.to_string(),
                issued_at: chrono::Utc::now().timestamp(),
            };
            let encoded = encode_sso_transaction(&transaction)
                .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;
            let cookie = sso_transaction_cookie(&encoded, "saml", SSO_TRANSACTION_TTL_SECS)
                .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;

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
            let base = resolve_base_url(&headers)
                .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;
            let redirect_uri = format!("{base}/auth/sso/callback");
            let discovery = fetch_oidc_discovery(&issuer_url).await.map_err(|e| {
                tracing::warn!(reason = %e, "OIDC discovery rejected");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "OIDC provider metadata is unavailable or invalid".to_string(),
                )
            })?;
            let mut authorize_url = url::Url::parse(&discovery.authorization_endpoint).map_err(
                |_| {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "OIDC authorization endpoint is invalid".to_string(),
                    )
                },
            )?;
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
                redirect_path: "/".to_string(),
                issued_at: chrono::Utc::now().timestamp(),
            };
            let encoded = encode_sso_transaction(&transaction)
                .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;
            let cookie = sso_transaction_cookie(&encoded, "oidc", SSO_TRANSACTION_TTL_SECS)
                .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;

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
    let transaction =
        extract_sso_transaction(&headers).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
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
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
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
        _email_claim,
        _first_name_claim,
        _last_name_claim,
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
    let base = resolve_base_url(&headers).map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;
    let redirect_uri = format!("{base}/auth/sso/callback");

    let token_res = crate::outbound::public_https_request(
        reqwest::Method::POST,
        &discovery.token_endpoint,
    )
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("token endpoint rejected: {e}")))?
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
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("token exchange failed: {e}"),
            )
        })?;

    if !token_res.status().is_success() {
        let body = token_res.text().await.unwrap_or_default();
        tracing::warn!("OIDC token exchange failed: {body}");
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("IdP token exchange failed: {body}"),
        ));
    }

    let token_data: OidcTokenResponse = token_res.json().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("invalid token response: {e}"),
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
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("id_token verification failed: {e}"),
            )
        })?;
    validate_oidc_nonce(&transaction, &claims).map_err(|e| (StatusCode::UNAUTHORIZED, e))?;
    if !consume_sso_key_once(
        format!("oidc:{}", transaction.state),
        transaction.issued_at + SSO_TRANSACTION_TTL_SECS,
    ) {
        return Err((
            StatusCode::BAD_REQUEST,
            "OIDC login transaction was already consumed".to_string(),
        ));
    }

    // 5. Extract claims
    let external_id = claims
        .get("sub")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let email = claims
        .get("email")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let display_name = claims
        .get("name")
        .or_else(|| claims.get("preferred_username"))
        .and_then(|v| v.as_str())
        .unwrap_or(&email)
        .to_string();

    // Username: prefer email, fall back to sub
    let username = if email.is_empty() {
        external_id.clone()
    } else {
        email.clone()
    };

    if external_id.is_empty() {
        return Err((
            StatusCode::BAD_GATEWAY,
            "id_token missing 'sub' claim".to_string(),
        ));
    }

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
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("group mapping error: {e}"),
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
    let user_id = match existing_user {
        Some(uid) => uid,
        None => {
            if !jit_provisioning {
                return Err((
                    StatusCode::FORBIDDEN,
                    "JIT provisioning is disabled and user does not exist".to_string(),
                ));
            }
            state
                .config_db
                .create_sso_user(&username, &display_name, &identity_key, "oidc", "default")
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("user creation error: {e}"),
                    )
                })?
        }
    };

    // 9. Update the user's group memberships with the mapped set
    state
        .config_db
        .update_user_groups_from_idp(&user_id, &mapped_group_ids)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("group update error: {e}"),
            )
        })?;

    // 10. Create a session (same as local auth)
    let token = state
        .config_db
        .create_session(&user_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("session error: {e}"),
            )
        })?;

    // 11. Set the rush_session cookie and redirect to /
    let cookie = crate::handlers::auth::session_cookie(&token, 86400);
    let clear_transaction = sso_transaction_cookie("", "oidc", 0)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

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
    headers.insert(header::LOCATION, "/".parse().unwrap());

    Ok((StatusCode::FOUND, headers, ""))
}

/// Verify an OIDC id_token JWT signature against the provider's JWKS endpoint and return claims.
/// Fetches the OIDC discovery document to resolve the JWKS URI, then verifies the signature.
/// Rejects `alg:none` and any token that fails signature validation.
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

    // Select the matching key: prefer by kid, fall back to first key
    let jwk = if let Some(kid) = header.kid.as_deref() {
        jwks.find(kid)
            .ok_or_else(|| anyhow::anyhow!("no JWK found for kid '{kid}'"))?
    } else {
        jwks.keys
            .first()
            .ok_or_else(|| anyhow::anyhow!("JWKS is empty"))?
    };

    let decoding_key = DecodingKey::from_jwk(jwk)
        .map_err(|e| anyhow::anyhow!("failed to build decoding key from JWK: {e}"))?;

    let mut validation = Validation::new(header.alg);
    validation.set_issuer(&[issuer_url]);
    // Validate the audience claim against the registered client_id.
    // This ensures tokens issued for other apps at the same IdP are rejected.
    validation.set_audience(&[client_id]);

    let token_data = decode::<serde_json::Value>(token, &decoding_key, &validation)
        .map_err(|e| anyhow::anyhow!("JWT signature verification failed: {e}"))?;

    Ok(token_data.claims)
}

// ── SSO Config Admin Endpoints ──

/// GET /api/v1/sso/providers -- List all SSO providers
pub async fn list_sso_providers(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let rows = state
        .config_db
        .list_sso_providers()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

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

    Ok(Json(serde_json::json!({ "providers": providers })))
}

/// POST /api/v1/sso/providers -- Create or update an SSO provider
pub async fn save_sso_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SaveSsoProviderRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let id = req.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let protocol = req.protocol.as_deref().unwrap_or("oidc");
    if !matches!(protocol, "oidc" | "saml") {
        return Err((
            StatusCode::BAD_REQUEST,
            "SSO protocol must be 'oidc' or 'saml'".to_string(),
        ));
    }
    let enabled = req.enabled.unwrap_or(false);

    // If updating and no new secret provided, keep the existing one
    let client_secret = match &req.client_secret {
        Some(s) if !s.is_empty() => s.clone(),
        _ => {
            // Try to load existing secret
            state
                .config_db
                .get_sso_provider(&id)
                .await
                .ok()
                .flatten()
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
    }

    state
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
            req.jit_provisioning.unwrap_or(true),
            req.default_group_id.as_deref().unwrap_or(""),
            req.saml_idp_metadata_url.as_deref().unwrap_or(""),
            req.saml_idp_sso_url.as_deref().unwrap_or(""),
            req.saml_idp_cert.as_deref().unwrap_or(""),
            req.saml_sp_entity_id.as_deref().unwrap_or(""),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    tracing::info!(
        event = "sso_provider_saved",
        provider_id = %id,
        provider_name = %req.name,
        admin = %caller.1,
        "SSO provider saved"
    );

    // AUDIT: SSO provider config update. NEVER log client_secret or SAML cert —
    // only non-sensitive config (name/protocol/issuer/enabled).
    state.audit.log(
        crate::audit::AuditEvent::new("sso.config_update", "user")
            .actor(caller.0.clone(), caller.1.clone())
            .tenant(caller.3.clone())
            .resource("sso_provider", id.clone())
            .changes(serde_json::json!({
                "name": req.name,
                "protocol": protocol,
                "enabled": enabled,
                "issuer_url": req.issuer_url.as_deref().unwrap_or(""),
                "client_id": req.client_id.as_deref().unwrap_or(""),
                "client_secret_set": req.client_secret.as_deref().map(|s| !s.is_empty()).unwrap_or(false)
            }).to_string())
            .description("sso provider config updated")
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;
    // Also emit an explicit enable/disable event reflecting the new state.
    if let Some(enabled) = req.enabled {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new(
                    if enabled { "sso.enable" } else { "sso.disable" },
                    "user",
                )
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("sso_provider", id.clone())
                .changes(serde_json::json!({ "enabled": enabled }).to_string())
                .description("sso provider enabled state set")
                .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
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
    let deleted = state
        .config_db
        .delete_sso_provider(&id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

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
                    .changes(serde_json::json!({ "deleted": true }).to_string())
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
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

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
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

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
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

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
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

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
    let transaction =
        extract_sso_transaction(&headers).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
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
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
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
    .map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid base64 in SAMLResponse: {e}"),
        )
    })?;
    let xml = String::from_utf8_lossy(&xml_bytes);

    // Fail closed: a configured signing certificate and a signature covering
    // the exact content we consume are mandatory for every SAML login.
    if saml_cert.trim().is_empty() {
        tracing::error!(provider_id = %provider_id, "enabled SAML provider has no certificate");
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "SAML provider has no signing certificate".to_string(),
        ));
    }
    let signed_xml = saml::verify_signature(&xml, &saml_cert).map_err(|e| {
        tracing::warn!("SAML signature check failed: {e}");
        (
            StatusCode::UNAUTHORIZED,
            "SAML signature verification failed".to_string(),
        )
    })?;

    let base_url = resolve_base_url(&headers)
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;
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
        if !consume_sso_key_once(replay_key, replay_expiry) {
            return Err((
                StatusCode::UNAUTHORIZED,
                "SAML response has already been consumed".to_string(),
            ));
        }
    }
    if let Some(response_id) = &assertion.response_id {
        if !consume_sso_key_once(format!("saml-response:{response_id}"), replay_expiry) {
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
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("group mapping error: {e}"),
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
    let user_id = match existing_user {
        Some(uid) => uid,
        None => {
            if !jit_provisioning {
                return Err((
                    StatusCode::FORBIDDEN,
                    "user not found and JIT provisioning is disabled".to_string(),
                ));
            }
            let email = assertion.email.as_deref().unwrap_or(&assertion.name_id);
            let display = assertion.display_name.as_deref().unwrap_or(email);
            state
                .config_db
                .create_sso_user(email, display, &identity_key, auth_provider, "default")
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("user creation error: {e}"),
                    )
                })?
        }
    };

    state
        .config_db
        .update_user_groups_from_idp(&user_id, &mapped_group_ids)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("group update error: {e}"),
            )
        })?;

    let token = state
        .config_db
        .create_session(&user_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("session error: {e}"),
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

    let cookie = crate::handlers::auth::session_cookie(&token, 86400);
    let clear_transaction = sso_transaction_cookie("", "saml", 0)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

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
        transaction
            .redirect_path
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
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    let base_url = resolve_base_url(&headers)
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;
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
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
    {
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
        })),
        None => Ok(Json(SsoStatusResponse {
            enabled: false,
            provider_name: String::new(),
            protocol: String::new(),
        })),
    }
}

// ── Setup token endpoints ──

#[derive(Deserialize)]
pub struct CreateSetupTokenRequest {
    pub purpose: Option<String>,
    pub created_by: Option<String>,
    pub provider: Option<String>,
    pub hostname: Option<String>,
}

/// POST /api/v1/sso/setup-token -- Create a one-time setup link for security teams
pub async fn create_setup_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateSetupTokenRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let caller = crate::handlers::users::require_admin(&state, &headers).await?;
    let purpose = req.purpose.as_deref().unwrap_or("sso_setup");
    let created_by = req.created_by.as_deref().unwrap_or("admin");
    let provider = req.provider.as_deref().unwrap_or("");
    let hostname = req.hostname.as_deref().unwrap_or("");

    let token = state
        .config_db
        .create_setup_token(purpose, created_by, provider, hostname)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    let base = if hostname.is_empty() {
        String::new()
    } else {
        hostname.to_string()
    };
    let url = format!("{base}/setup/sso?token={token}");

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
                        "hostname_configured": !hostname.is_empty(),
                    })
                    .to_string(),
                )
                .description("sso setup token created")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(serde_json::json!({ "token": token, "url": url })))
}

/// GET /api/v1/sso/setup-token/{token}/validate -- Check if a setup token is still valid
pub async fn validate_setup_token(
    State(state): State<AppState>,
    axum::extract::Path(token): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let (valid, provider) = state
        .config_db
        .validate_setup_token(&token, "sso_setup")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    Ok(Json(
        serde_json::json!({ "valid": valid, "provider": provider }),
    ))
}

/// POST /api/v1/sso/setup-token/{token}/complete -- Mark a setup token as used
pub async fn complete_setup_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(token): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let caller = require_auth(&state, &headers).await?;
    let marked = state
        .config_db
        .mark_setup_token_used(&token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    if marked {
        // Do not log the setup token or any token-derived value.
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("sso.setup_token_complete", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("sso_setup_token", "one-time")
                    .changes(serde_json::json!({ "used": true }).to_string())
                    .description("sso setup token completed")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            "token not found or already used".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn replay_key_is_consumed_only_once() {
        let key = format!("test:{}", uuid::Uuid::new_v4());
        let expiry = chrono::Utc::now().timestamp() + 60;
        assert!(consume_sso_key_once(key.clone(), expiry));
        assert!(!consume_sso_key_once(key, expiry));
    }
}

// ── Helpers ──

fn normalize_base_url(raw: &str, production: bool) -> Result<String, String> {
    let parsed = url::Url::parse(raw).map_err(|_| "RUSH_BASE_URL is not a valid URL".to_string())?;
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !matches!(parsed.path(), "" | "/")
    {
        return Err("RUSH_BASE_URL must be an origin without credentials, path, query, or fragment".to_string());
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
        Ok(value) if !value.trim().is_empty() => normalize_base_url(value.trim(), production).map(|_| ()),
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
