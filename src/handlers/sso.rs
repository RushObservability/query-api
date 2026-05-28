use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect},
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::handlers::users::{require_admin, require_auth};
use crate::saml;

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
        .get_enabled_sso_provider().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "no SSO provider is enabled".to_string(),
            )
        })?;

    let (
        _id, _name, protocol, _enabled,
        client_id, _client_secret, issuer_url, oidc_scopes,
        _groups_claim, _email_claim, _first_name_claim, _last_name_claim, _jit, _default_group, _created_at,
        _saml_meta, saml_idp_sso_url, _saml_cert, saml_sp_entity_id,
    ) = provider;

    match protocol.as_str() {
        "saml" => {
            let base_url = resolve_base_url(&headers);
            let acs_url = format!("{base_url}/auth/sso/acs");
            let relay_state = "/";

            let redirect_url = saml::build_login_redirect_url(
                &saml_sp_entity_id,
                &acs_url,
                &saml_idp_sso_url,
                relay_state,
            );

            let mut resp_headers = HeaderMap::new();
            resp_headers.insert(
                header::LOCATION,
                redirect_url.parse().map_err(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "invalid redirect URL".to_string())
                })?,
            );
            Ok((StatusCode::FOUND, resp_headers, "").into_response())
        }
        _ => {
            // OIDC flow
            let csrf_state: String = {
                use rand::Rng;
                let mut rng = rand::rng();
                let bytes: [u8; 16] = rng.random();
                bytes.iter().map(|b| format!("{b:02x}")).collect()
            };

            state
                .config_db
                .store_sso_state(&csrf_state).await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("state error: {e}")))?;

            let scopes_encoded = oidc_scopes
                .split_whitespace()
                .collect::<Vec<&str>>()
                .join("+");

            let authorize_url = format!(
                "{issuer_url}/authorize?client_id={client_id}&redirect_uri={redirect}&response_type=code&scope={scopes}&state={csrf_state}",
                issuer_url = issuer_url.trim_end_matches('/'),
                client_id = urlencoding::encode(&client_id),
                redirect = urlencoding::encode("/auth/sso/callback"),
                scopes = scopes_encoded,
            );

            Ok(Redirect::temporary(&authorize_url).into_response())
        }
    }
}

// ── OIDC Callback ──

/// GET /auth/sso/callback?code=...&state=... -- Exchange code for tokens, JIT provision user
pub async fn sso_callback(
    State(state): State<AppState>,
    Query(params): Query<SsoCallbackQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // 1. Verify CSRF state
    let valid = state
        .config_db
        .validate_sso_state(&params.state).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("state error: {e}")))?;

    if !valid {
        return Err((StatusCode::BAD_REQUEST, "invalid or expired state parameter".to_string()));
    }

    // 2. Load the enabled SSO provider
    let provider = state
        .config_db
        .get_enabled_sso_provider().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "no SSO provider is enabled".to_string(),
            )
        })?;

    let (
        provider_id, _name, _protocol, _enabled,
        client_id, client_secret, issuer_url, _oidc_scopes,
        groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, _created_at,
        _f13, _f14, _f15, _f16,
    ) = provider;

    // 3. Exchange authorization code for tokens
    let token_url = format!("{}/token", issuer_url.trim_end_matches('/'));

    let client = reqwest::Client::new();
    let token_res = client
        .post(&token_url)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", &params.code),
            ("redirect_uri", "/auth/sso/callback"),
            ("client_id", &client_id),
            ("client_secret", &client_secret),
        ])
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("token exchange failed: {e}")))?;

    if !token_res.status().is_success() {
        let body = token_res.text().await.unwrap_or_default();
        tracing::warn!("OIDC token exchange failed: {body}");
        return Err((StatusCode::BAD_GATEWAY, format!("IdP token exchange failed: {body}")));
    }

    let token_data: OidcTokenResponse = token_res
        .json()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("invalid token response: {e}")))?;

    let id_token = token_data.id_token.ok_or_else(|| {
        (StatusCode::BAD_GATEWAY, "no id_token in response".to_string())
    })?;

    // 4. Verify the id_token JWT signature against the provider's JWKS and decode claims
    let claims = verify_and_decode_jwt(&client, &id_token, &issuer_url).await.map_err(|e| {
        (StatusCode::BAD_GATEWAY, format!("id_token verification failed: {e}"))
    })?;

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
        .resolve_idp_groups(&idp_groups, &provider_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("group mapping error: {e}")))?;

    // If no mappings match, use default_group_id from provider config
    if mapped_group_ids.is_empty() && !default_group_id.is_empty() {
        mapped_group_ids.push(default_group_id);
    }

    // If still nothing, fall back to the built-in viewers group
    if mapped_group_ids.is_empty() {
        mapped_group_ids.push("viewers".to_string());
    }

    // 8. JIT provision: find or create user
    let user_id = match state
        .config_db
        .find_user_by_external_id(&external_id, "oidc").await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("user lookup error: {e}")))?
    {
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
                .create_sso_user(&username, &display_name, &external_id, "oidc", "default").await
                .map_err(|e| {
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("user creation error: {e}"))
                })?
        }
    };

    // 9. Update the user's group memberships with the mapped set
    state
        .config_db
        .update_user_groups_from_idp(&user_id, &mapped_group_ids).await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("group update error: {e}"))
        })?;

    // 10. Create a session (same as local auth)
    let token = state
        .config_db
        .create_session(&user_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("session error: {e}")))?;

    // 11. Set the rush_session cookie and redirect to /
    let cookie = format!(
        "rush_session={token}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=86400"
    );

    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        cookie.parse().unwrap(),
    );
    headers.insert(
        header::LOCATION,
        "/".parse().unwrap(),
    );

    Ok((StatusCode::FOUND, headers, ""))
}

/// Verify an OIDC id_token JWT signature against the provider's JWKS endpoint and return claims.
/// Fetches the OIDC discovery document to resolve the JWKS URI, then verifies the signature.
/// Rejects `alg:none` and any token that fails signature validation.
async fn verify_and_decode_jwt(
    http_client: &reqwest::Client,
    token: &str,
    issuer_url: &str,
) -> anyhow::Result<serde_json::Value> {
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
    use jsonwebtoken::jwk::JwkSet;

    // Parse the JWT header to get `kid` and `alg` — does not verify signature
    let header = jsonwebtoken::decode_header(token)
        .map_err(|e| anyhow::anyhow!("invalid JWT header: {e}"))?;

    // Only accept asymmetric algorithms — reject symmetric (HS*) which would require
    // sharing the client_secret as the signing key, an unsafe pattern for OIDC.
    match header.alg {
        Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512
        | Algorithm::PS256 | Algorithm::PS384 | Algorithm::PS512
        | Algorithm::ES256 | Algorithm::ES384 => {}
        alg => anyhow::bail!("JWT algorithm {alg:?} is not accepted for OIDC"),
    }

    // Fetch the OIDC discovery document to get the JWKS URI
    let discovery_url = format!(
        "{}/.well-known/openid-configuration",
        issuer_url.trim_end_matches('/')
    );
    let discovery: serde_json::Value = http_client
        .get(&discovery_url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("OIDC discovery request failed: {e}"))?
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("OIDC discovery parse error: {e}"))?;

    let jwks_uri = discovery["jwks_uri"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("OIDC discovery document missing jwks_uri"))?;

    // Fetch the JSON Web Key Set
    let jwks: JwkSet = http_client
        .get(jwks_uri)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("JWKS fetch failed: {e}"))?
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("JWKS parse error: {e}"))?;

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
    // Skip audience validation — client_id varies by provider
    validation.validate_aud = false;

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
        .list_sso_providers().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    let providers: Vec<SsoProviderResponse> = rows
        .into_iter()
        .map(|(
            id, name, protocol, enabled,
            client_id, _secret, issuer_url, oidc_scopes,
            groups_claim, email_claim, first_name_claim, last_name_claim,
            jit, default_group_id, created_at,
            saml_meta, saml_sso, saml_cert, saml_entity,
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
        })
        .collect();

    Ok(Json(serde_json::json!({ "providers": providers })))
}

/// POST /api/v1/sso/providers -- Create or update an SSO provider
pub async fn save_sso_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SaveSsoProviderRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let id = req.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // If updating and no new secret provided, keep the existing one
    let client_secret = match &req.client_secret {
        Some(s) if !s.is_empty() => s.clone(),
        _ => {
            // Try to load existing secret
            state
                .config_db
                .get_sso_provider(&id).await
                .ok()
                .flatten()
                .map(|p| p.5)
                .unwrap_or_default()
        }
    };

    state
        .config_db
        .upsert_sso_provider(
            &id,
            &req.name,
            req.protocol.as_deref().unwrap_or("oidc"),
            req.enabled.unwrap_or(false),
            req.client_id.as_deref().unwrap_or(""),
            &client_secret,
            req.issuer_url.as_deref().unwrap_or(""),
            req.oidc_scopes.as_deref().unwrap_or("openid profile email groups"),
            req.groups_claim.as_deref().unwrap_or("groups"),
            req.jit_provisioning.unwrap_or(true),
            req.default_group_id.as_deref().unwrap_or(""),
            req.saml_idp_metadata_url.as_deref().unwrap_or(""),
            req.saml_idp_sso_url.as_deref().unwrap_or(""),
            req.saml_idp_cert.as_deref().unwrap_or(""),
            req.saml_sp_entity_id.as_deref().unwrap_or("")
        ).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    Ok(Json(serde_json::json!({ "id": id, "ok": true })))
}

/// DELETE /api/v1/sso/providers/{id} -- Delete an SSO provider
pub async fn delete_sso_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_sso_provider(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    if deleted {
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
        .list_idp_group_mappings(None).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    let mappings: Vec<IdpGroupMappingResponse> = rows
        .into_iter()
        .map(|(id, idp_group, rush_group_id, provider_id, created_at)| {
            IdpGroupMappingResponse {
                id,
                idp_group,
                rush_group_id,
                provider_id,
                created_at,
            }
        })
        .collect();

    Ok(Json(serde_json::json!({ "mappings": mappings })))
}

/// POST /api/v1/sso/mappings -- Create a mapping
pub async fn create_idp_group_mapping(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateMappingRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let provider_id = req.provider_id.as_deref().unwrap_or("default");

    let id = state
        .config_db
        .create_idp_group_mapping(&req.idp_group, &req.rush_group_id, provider_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    Ok(Json(serde_json::json!({ "id": id, "ok": true })))
}

/// DELETE /api/v1/sso/mappings/{id} -- Delete a mapping
pub async fn delete_idp_group_mapping(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_idp_group_mapping(&id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    if deleted {
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
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let params: Vec<(String, String)> = url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect();

    let saml_response = params
        .iter()
        .find(|(k, _)| k == "SAMLResponse")
        .map(|(_, v)| v.as_str())
        .ok_or_else(|| {
            (StatusCode::BAD_REQUEST, "missing SAMLResponse in POST body".to_string())
        })?;

    let relay_state_raw = params
        .iter()
        .find(|(k, _)| k == "RelayState")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| "/".to_string());
    // Reject absolute URLs and protocol-relative URLs to prevent open redirect
    let relay_state = if relay_state_raw.starts_with('/') && !relay_state_raw.starts_with("//") {
        relay_state_raw
    } else {
        "/".to_string()
    };

    let provider = state
        .config_db
        .get_enabled_sso_provider().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
        .ok_or_else(|| {
            (StatusCode::BAD_REQUEST, "no SSO provider configured".to_string())
        })?;

    let (
        provider_id, _name, _protocol, _enabled,
        _client_id, _client_secret, _issuer_url, _oidc_scopes,
        groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, _created_at,
        _saml_meta, _saml_sso, saml_cert, _saml_entity,
    ) = provider;

    // Decode the base64 SAMLResponse to raw XML for signature verification
    let xml_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        saml_response.trim(),
    )
    .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid base64 in SAMLResponse: {e}")))?;
    let xml = String::from_utf8_lossy(&xml_bytes);

    // If the provider has a certificate configured, verify the XML signature
    if !saml_cert.is_empty() {
        match saml::verify_signature(&xml, &saml_cert) {
            Ok(true) => tracing::info!("SAML signature verified"),
            Ok(false) => {
                tracing::warn!("SAML signature verification failed");
                return Err((
                    StatusCode::UNAUTHORIZED,
                    "SAML signature verification failed".to_string(),
                ));
            }
            Err(e) => {
                tracing::warn!("SAML signature check error: {e}");
                return Err((
                    StatusCode::UNAUTHORIZED,
                    format!("SAML signature error: {e}"),
                ));
            }
        }
    }

    let assertion = saml::parse_saml_response(saml_response, &groups_claim).map_err(|e| {
        tracing::warn!("SAML response parse error: {e}");
        (StatusCode::BAD_REQUEST, format!("invalid SAML response: {e}"))
    })?;

    tracing::info!(
        name_id = %assertion.name_id,
        email = ?assertion.email,
        groups = ?assertion.groups,
        "SAML assertion parsed"
    );

    let mut mapped_group_ids = state
        .config_db
        .resolve_idp_groups(&assertion.groups, &provider_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("group mapping error: {e}")))?;

    if mapped_group_ids.is_empty() {
        if !default_group_id.is_empty() {
            mapped_group_ids.push(default_group_id);
        } else {
            mapped_group_ids.push("viewers".to_string());
        }
    }

    let external_id = &assertion.name_id;
    let auth_provider = "saml";

    let user_id = match state
        .config_db
        .find_user_by_external_id(external_id, auth_provider).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?
    {
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
                .create_sso_user(email, display, external_id, auth_provider, "default").await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("user creation error: {e}")))?
        }
    };

    state
        .config_db
        .update_user_groups_from_idp(&user_id, &mapped_group_ids).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("group update error: {e}")))?;

    let token = state.config_db.create_session(&user_id).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("session error: {e}"))
    })?;

    let cookie = format!("rush_session={token}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=86400");

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(header::SET_COOKIE, cookie.parse().unwrap());
    resp_headers.insert(
        header::LOCATION,
        relay_state.parse().unwrap_or_else(|_| "/".parse().unwrap()),
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
        .get_enabled_sso_provider().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let base_url = resolve_base_url(&headers);
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
        .get_enabled_sso_provider().await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
    {
        Some((
            _id, name, protocol, _enabled,
            _client_id, _secret, _issuer, _scopes,
            _claim, _email, _first, _last, _jit, _default, _created,
            _saml_meta, _saml_sso, _saml_cert, _saml_entity,
        )) => {
            Ok(Json(SsoStatusResponse {
                enabled: true,
                provider_name: name,
                protocol,
            }))
        }
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
    crate::handlers::users::require_admin(&state, &headers).await?;
    let purpose = req.purpose.as_deref().unwrap_or("sso_setup");
    let created_by = req.created_by.as_deref().unwrap_or("admin");
    let provider = req.provider.as_deref().unwrap_or("");
    let hostname = req.hostname.as_deref().unwrap_or("");

    let token = state
        .config_db
        .create_setup_token(purpose, created_by, provider, hostname).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    let base = if hostname.is_empty() {
        String::new()
    } else {
        hostname.to_string()
    };
    let url = format!("{base}/setup/sso?token={token}");

    Ok(Json(serde_json::json!({ "token": token, "url": url })))
}

/// GET /api/v1/sso/setup-token/{token}/validate -- Check if a setup token is still valid
pub async fn validate_setup_token(
    State(state): State<AppState>,
    axum::extract::Path(token): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let (valid, provider) = state
        .config_db
        .validate_setup_token(&token, "sso_setup").await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    Ok(Json(serde_json::json!({ "valid": valid, "provider": provider })))
}

/// POST /api/v1/sso/setup-token/{token}/complete -- Mark a setup token as used
pub async fn complete_setup_token(
    State(state): State<AppState>,
    axum::extract::Path(token): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let marked = state
        .config_db
        .mark_setup_token_used(&token).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    if marked {
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err((StatusCode::NOT_FOUND, "token not found or already used".to_string()))
    }
}

// ── Helpers ──

/// Resolve the base URL from request headers.
fn resolve_base_url(headers: &HeaderMap) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");

    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost:8080");

    format!("{scheme}://{host}")
}
