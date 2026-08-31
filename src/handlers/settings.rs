use axum::{
    Json,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use hmac::{Hmac, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::handlers::users::{require_admin, require_auth};
use crate::query_governor::{QUERY_LIMITS_SETTING_KEY, QueryGovernorConfig};
use crate::{AppState, TenantContext};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Serialize)]
pub struct ApiKeyListEntry {
    pub id: String,
    pub name: String,
    pub prefix: String,
    pub tenant_id: String,
    pub key_type: String,
    pub signals: Vec<String>,
    pub rate_limit_per_minute: u64,
    pub source_cidrs: Vec<String>,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct ApiKeyCreated {
    pub id: String,
    pub name: String,
    pub key: String,
    pub prefix: String,
    pub tenant_id: String,
    pub key_type: String,
    pub signals: Vec<String>,
    pub rate_limit_per_minute: u64,
    pub source_cidrs: Vec<String>,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
    #[serde(default = "default_api_key_type")]
    pub key_type: String,
    #[serde(default)]
    pub signals: Vec<String>,
    #[serde(default)]
    pub rate_limit_per_minute: u64,
    #[serde(default)]
    pub source_cidrs: Vec<String>,
}

fn default_api_key_type() -> String {
    "query".to_string()
}

fn generate_api_key(key_type: &str) -> String {
    let mut rng = rand::rng();
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz0123456789".chars().collect();
    let random: String = (0..64)
        .map(|_| chars[rng.random_range(0..chars.len())])
        .collect();
    format!(
        "rush_{}_{random}",
        if key_type == "ingest" { "ing" } else { "qry" }
    )
}

/// Hash an API key using HMAC-SHA256 keyed with RUSH_API_KEY_SECRET.
/// Produces a consistent hash for lookups while preventing offline
/// dictionary attacks against a stolen database.
///
/// # Panics in debug builds / warns in release if RUSH_API_KEY_SECRET is absent or weak.
pub fn hash_api_key(key: &str) -> String {
    let secret = std::env::var("RUSH_API_KEY_SECRET").unwrap_or_default();
    if secret.len() < 32 {
        // An empty or short key makes HMAC equivalent to a plain hash, enabling
        // offline dictionary attacks against a stolen api_keys table. Warn ONCE —
        // this runs on every API-key hash (every ingest request), so a per-call
        // warning would flood the logs.
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                "RUSH_API_KEY_SECRET is not set or shorter than 32 bytes; \
                 API key hashing is insecure — set a strong random secret in production"
            );
        });
    }
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(key.as_bytes());
    format!("{:x}", mac.finalize().into_bytes())
}

pub async fn list_api_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let rows = state.config_db.list_api_keys().await.map_err(|e| {
        tracing::error!(error = %e, "internal error");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;
    let keys: Vec<ApiKeyListEntry> = rows
        .into_iter()
        .map(
            |(
                id,
                name,
                prefix,
                tenant_id,
                key_type,
                signals,
                rate_limit_per_minute,
                source_cidrs,
                created_at,
            )| ApiKeyListEntry {
                id,
                name,
                prefix,
                tenant_id,
                key_type,
                signals,
                rate_limit_per_minute,
                source_cidrs,
                created_at,
            },
        )
        .collect();
    Ok(Json(serde_json::json!({ "keys": keys })))
}

pub async fn create_api_key(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let name = req.name.trim();
    if name.is_empty() || name.len() > 100 {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must be 1-100 characters".to_string(),
        ));
    }
    let key_type = req.key_type.trim().to_ascii_lowercase();
    if !matches!(key_type.as_str(), "query" | "ingest") {
        return Err((
            StatusCode::BAD_REQUEST,
            "key_type must be 'query' or 'ingest'".to_string(),
        ));
    }
    let signals = if key_type == "ingest" {
        crate::api_key_auth::normalize_signals(&req.signals)
            .map_err(|message| (StatusCode::BAD_REQUEST, message))?
    } else {
        if !req.signals.is_empty() || req.rate_limit_per_minute != 0 || !req.source_cidrs.is_empty()
        {
            return Err((
                StatusCode::BAD_REQUEST,
                "signal, rate, and source restrictions apply only to ingest keys".to_string(),
            ));
        }
        Vec::new()
    };
    let rate_limit_per_minute = if key_type == "ingest" {
        if !(1..=1_000_000).contains(&req.rate_limit_per_minute) {
            return Err((
                StatusCode::BAD_REQUEST,
                "ingest rate_limit_per_minute must be between 1 and 1000000".to_string(),
            ));
        }
        req.rate_limit_per_minute
    } else {
        0
    };
    let source_cidrs = crate::api_key_auth::normalize_source_cidrs(&req.source_cidrs)
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;
    let id = uuid::Uuid::new_v4().to_string();
    let key = generate_api_key(&key_type);
    let key_hash = hash_api_key(&key);
    let prefix = key[..12].to_string();

    state
        .config_db
        .create_api_key(
            &id,
            name,
            &key_hash,
            &prefix,
            &tenant.tenant_id,
            &key_type,
            &signals,
            rate_limit_per_minute,
            &source_cidrs,
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    tracing::info!(
        event = "api_key_created",
        key_id = %id,
        key_name = %name,
        admin = %caller.1,
        "API key created"
    );

    // AUDIT: API key created. NEVER log the key value or its hash — only the
    // name, the public prefix, and the tenant.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("apikey.create", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(tenant.tenant_id.clone())
                .resource("api_key", id.clone())
                .changes(
                    serde_json::json!({
                        "name": name,
                        "prefix": prefix,
                        "tenant": tenant.tenant_id,
                        "key_type": key_type,
                        "signals": signals,
                        "rate_limit_per_minute": rate_limit_per_minute,
                        "source_restricted": !source_cidrs.is_empty(),
                    })
                    .to_string(),
                )
                .description("api key created")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    // Return the full key ONLY on creation
    Ok(Json(ApiKeyCreated {
        id,
        name: name.to_string(),
        key,
        prefix,
        tenant_id: tenant.tenant_id,
        key_type,
        signals,
        rate_limit_per_minute,
        source_cidrs,
        created_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    }))
}

/// GET /api/v1/features — public, no auth required.
/// Returns which optional integrations are enabled so the UI can hide/show nav items.
pub async fn get_features(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let argocd_enabled = std::env::var("ARGOCD_NAMESPACE").is_ok()
        || state
            .config_db
            .get_setting("argocd_enabled")
            .await
            .ok()
            .flatten()
            .map(|v| v == "true")
            .unwrap_or(false);

    let fluxcd_enabled = std::env::var("FLUXCD_NAMESPACE").is_ok()
        || state
            .config_db
            .get_setting("fluxcd_enabled")
            .await
            .ok()
            .flatten()
            .map(|v| v == "true")
            .unwrap_or(false);

    let kubernetes_enabled = std::env::var("KUBERNETES_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
        || state
            .config_db
            .get_setting("kubernetes_enabled")
            .await
            .ok()
            .flatten()
            .map(|v| v == "true")
            .unwrap_or(false);
    let kubernetes_logging_enabled = crate::handlers::kubernetes_access::available();

    let cloudwatch_enabled = std::env::var("CLOUDWATCH_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
        || state
            .config_db
            .get_setting("cloudwatch_enabled")
            .await
            .ok()
            .flatten()
            .map(|v| v == "true")
            .unwrap_or(false);

    // This route remains public because it is only a UI hint. The authenticated
    // SRE proxy independently enforces the same policy using the server-trusted
    // caller tenant, so spoofing this header cannot grant access.
    let feature_tenant = headers
        .get("x-rush-tenant")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("default");
    let sre_agent_enabled = matches!(
        sre_agent_access_decision(&state, feature_tenant).await,
        SreAgentAccessDecision::Allowed
    );

    let export_max_rows = crate::handlers::export::read_export_max_rows(&state).await;

    // Deploy markers on charts — display enhancement over existing deploy data.
    // Defaults ON (unset → true); only an explicit "false" disables it.
    let deploy_markers_enabled = state
        .config_db
        .get_setting("deploy_markers_enabled")
        .await
        .ok()
        .flatten()
        .map(|v| v != "false")
        .unwrap_or(true);

    // Real User Monitoring — gates the RUM UI and ingest. Defaults ON
    // (unset → true); only an explicit "false" disables it.
    let rum_enabled = state
        .config_db
        .get_setting("rum_enabled")
        .await
        .ok()
        .flatten()
        .map(|v| v != "false")
        .unwrap_or(true);

    Json(serde_json::json!({
        "argocd": argocd_enabled,
        "fluxcd": fluxcd_enabled,
        "kubernetes": kubernetes_enabled,
        "kubernetes_logging": kubernetes_logging_enabled,
        "cloudwatch": cloudwatch_enabled,
        "sre_agent": sre_agent_enabled,
        "export_max_rows": export_max_rows,
        "deploy_markers": deploy_markers_enabled,
        "rum": rum_enabled,
    }))
}

/// GET /api/v1/settings/config — admin-only, redacted runtime configuration.
///
/// This is intentionally an allowlist rather than a dump of the process
/// environment. Secret values never cross the API boundary; the UI only gets
/// a configured/not-configured state for them.
pub async fn get_runtime_config(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("settings.config_view", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("settings", "runtime-config")
                .outcome("success")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    let license = crate::license::evaluate();
    let manager_enabled = state.collectors.enabled();
    let descriptors = crate::integrations::descriptors();
    let mut target_counts = std::collections::HashMap::new();
    for descriptor in &descriptors {
        let count = state
            .config_db
            .list_integration_target_secrets(&caller.3, descriptor.id)
            .await
            .map(|targets| targets.into_iter().filter(|target| target.enabled).count())
            .unwrap_or(0);
        target_counts.insert(descriptor.id, count);
    }

    let integrations = descriptors
        .into_iter()
        .map(|descriptor| {
            let licensed = descriptor.compiled && license.has_entitlement(descriptor.entitlement);
            serde_json::json!({
                "id": descriptor.id,
                "name": descriptor.name,
                "entitlement": descriptor.entitlement,
                "compiled": descriptor.compiled,
                "licensed": licensed,
                "loaded": licensed,
                "manager_enabled": manager_enabled,
                "configured_targets": target_counts.get(descriptor.id).copied().unwrap_or(0),
            })
        })
        .collect::<Vec<_>>();

    let mut runtime = vec![
        config_entry("RUSH_PORT", Some("8080"), false, false),
        config_entry("CLICKHOUSE_URL", Some("http://localhost:8123"), false, true),
        config_entry("CLICKHOUSE_DATABASE", Some("observability"), false, false),
        config_entry("RUSH_CONFIG", Some("./rush.toml"), false, false),
        config_entry("RUSH_ALLOWED_ORIGINS", Some("same-origin"), false, false),
        config_entry("SRE_AGENT_URL", Some("http://localhost:8081"), false, true),
        config_entry(
            "RUSH_COLLECTOR_OTLP_ENDPOINT",
            Some("http://localhost:8080"),
            false,
            true,
        ),
        config_entry(
            "RUSH_COLLECTOR_MANAGER_ENABLED",
            Some("false"),
            false,
            false,
        ),
        config_entry("RUSH_POSTGRES_COLLECTOR_BIN", None, false, false),
        config_entry("RUSH_POSTGRES_COLLECTOR_CONFIG", None, false, false),
        config_entry("RUSH_MYSQL_COLLECTOR_BIN", None, false, false),
        config_entry("RUSH_MYSQL_COLLECTOR_CONFIG", None, false, false),
        config_entry("RUSH_COLLECTOR_TENANT", Some("default"), false, false),
        config_entry("RUSH_SPOOL_DIR", Some("./data/spool"), false, false),
        config_entry("RUSH_BUFFER_BACKEND", Some("disk"), false, false),
        config_entry("RUSH_SESSION_IDLE_TIMEOUT_SECS", Some("1800"), false, false),
        config_entry(
            "RUSH_SESSION_ABSOLUTE_TIMEOUT_SECS",
            Some("86400"),
            false,
            false,
        ),
        config_entry(
            "RUSH_SESSION_RENEWAL_INTERVAL_SECS",
            Some("300"),
            false,
            false,
        ),
        config_entry("RUSH_LOG_FORMAT", Some("pretty"), false, false),
    ];
    runtime.extend([
        config_entry("RUSH_LICENSE_KEY", None, true, false),
        config_entry("RUSH_API_KEY_SECRET", None, true, false),
        config_entry("RUSH_SSO_TRANSACTION_SECRET", None, true, false),
        config_entry(
            "RUSH_INTEGRATION_ENCRYPTION_KEY_ID",
            Some("primary"),
            false,
            false,
        ),
        config_entry("RUSH_INTEGRATION_ENCRYPTION_KEY", None, true, false),
        config_entry(
            "RUSH_INTEGRATION_ENCRYPTION_PREVIOUS_KEYS",
            None,
            true,
            false,
        ),
        config_entry("RUSH_AUDIT_HMAC_SECRET", None, true, false),
        config_entry("RUSH_COLLECTOR_API_KEY", None, true, false),
        config_entry("RUSH_SRE_AGENT_INTERNAL_TOKEN", None, true, false),
        config_entry("RUSH_SMTP_PASS", None, true, false),
        config_entry("RUSH_BUFFER_S3_SECRET_KEY", None, true, false),
    ]);

    Ok(Json(serde_json::json!({
        "tenant": caller.3,
        "runtime": runtime,
        "license": license,
        "integrations": integrations,
    })))
}

fn config_entry(
    key: &str,
    default: Option<&str>,
    sensitive: bool,
    endpoint: bool,
) -> serde_json::Value {
    let raw = std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty());
    let configured = raw.is_some();
    let value = if sensitive {
        None
    } else {
        raw.as_deref()
            .map(|value| {
                if endpoint {
                    safe_endpoint(value)
                } else {
                    value.to_string()
                }
            })
            .or_else(|| default.map(str::to_string))
    };
    serde_json::json!({
        "key": key,
        "value": value,
        "configured": configured,
        "sensitive": sensitive,
        "source": if configured { "environment" } else { "default" },
    })
}

fn safe_endpoint(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(parsed) => {
            let host = parsed.host_str().unwrap_or("configured");
            match parsed.port() {
                Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
                None => format!("{}://{host}", parsed.scheme()),
            }
        }
        Err(_) => "configured".into(),
    }
}

/// GET /api/v1/settings/rum — admin only. Returns { enabled }.
pub async fn get_rum_setting(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let enabled = state
        .config_db
        .get_setting("rum_enabled")
        .await
        .ok()
        .flatten()
        .map(|v| v != "false")
        .unwrap_or(true);
    Ok(Json(serde_json::json!({ "enabled": enabled })))
}

/// PUT /api/v1/settings/rum — admin only. Body: { enabled: bool }.
/// Toggles Real User Monitoring: when off, the RUM UI is hidden and RUM
/// ingest endpoints reject data.
pub async fn set_rum_setting(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let enabled = body
        .get("enabled")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "invalid 'enabled' (expected a boolean)".to_string(),
            )
        })?;
    state
        .config_db
        .set_setting("rum_enabled", if enabled { "true" } else { "false" })
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to save rum_enabled");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to save setting".to_string(),
            )
        })?;

    // AUDIT: RUM setting change.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("settings.update", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("setting", "rum_enabled")
                .changes(serde_json::json!({ "key": "rum_enabled", "value": enabled }).to_string())
                .description("rum setting updated")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(serde_json::json!({ "enabled": enabled })))
}

/// GET /api/v1/settings/cloudwatch — admin only. Returns { enabled, default_tenant }.
/// `enabled` reflects the env override OR the stored `cloudwatch_enabled` setting.
/// `default_tenant` is purely a UI hint (the tenant shown in setup instructions);
/// it does NOT gate ingest.
pub async fn get_cloudwatch_setting(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let enabled = std::env::var("CLOUDWATCH_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
        || state
            .config_db
            .get_setting("cloudwatch_enabled")
            .await
            .ok()
            .flatten()
            .map(|v| v == "true")
            .unwrap_or(false);
    let default_tenant = state
        .config_db
        .get_setting("cloudwatch_default_tenant")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    Ok(Json(
        serde_json::json!({ "enabled": enabled, "default_tenant": default_tenant }),
    ))
}

/// PUT /api/v1/settings/cloudwatch — admin only. Body: { enabled: bool, default_tenant?: string }.
/// Toggles CloudWatch Logs ingest (Kinesis Data Firehose). `default_tenant` is an
/// optional UI hint for the setup instructions and does NOT gate ingest.
pub async fn set_cloudwatch_setting(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let enabled = body
        .get("enabled")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "invalid 'enabled' (expected a boolean)".to_string(),
            )
        })?;

    // Validate the optional UI hint before mutating either setting. It is not
    // a credential, but it is still a persisted setting and must be audited
    // independently when it changes.
    let requested_default_tenant = body
        .get("default_tenant")
        .map(|value| {
            let dt = value.as_str().ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    "invalid 'default_tenant' (expected a string)".to_string(),
                )
            })?;
            let dt = dt.trim();
            if dt.len() > 128 {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "default_tenant too long".to_string(),
                ));
            }
            Ok(dt.to_string())
        })
        .transpose()?;
    let previous_default_tenant = state
        .config_db
        .get_setting("cloudwatch_default_tenant")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    state
        .config_db
        .set_setting("cloudwatch_enabled", if enabled { "true" } else { "false" })
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to save cloudwatch_enabled");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to save setting".to_string(),
            )
        })?;

    // AUDIT: CloudWatch integration toggle.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("integration.update", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("integration", "cloudwatch")
                .changes(
                    serde_json::json!({ "key": "cloudwatch_enabled", "enabled": enabled })
                        .to_string(),
                )
                .description("cloudwatch integration toggled")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    // Optional default_tenant (UI hint only). Present → persist (empty clears it).
    if let Some(dt) = requested_default_tenant {
        state
            .config_db
            .set_setting("cloudwatch_default_tenant", &dt)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to save cloudwatch_default_tenant");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to save setting".to_string(),
                )
            })?;

        state
            .audit
            .log(
                crate::audit::AuditEvent::new("settings.update", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("setting", "cloudwatch_default_tenant")
                    .changes(
                        serde_json::json!({
                            "key": "cloudwatch_default_tenant",
                            "before": previous_default_tenant,
                            "after": dt,
                        })
                        .to_string(),
                    )
                    .description("cloudwatch default tenant updated")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }
    let default_tenant = state
        .config_db
        .get_setting("cloudwatch_default_tenant")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    Ok(Json(
        serde_json::json!({ "enabled": enabled, "default_tenant": default_tenant }),
    ))
}

/// GET /api/v1/settings/deploy-markers — admin only. Returns { enabled }.
pub async fn get_deploy_markers_setting(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let enabled = state
        .config_db
        .get_setting("deploy_markers_enabled")
        .await
        .ok()
        .flatten()
        .map(|v| v != "false")
        .unwrap_or(true);
    Ok(Json(serde_json::json!({ "enabled": enabled })))
}

/// PUT /api/v1/settings/deploy-markers — admin only. Body: { enabled: bool }.
/// Toggles whether deploy markers are drawn on service charts.
pub async fn set_deploy_markers_setting(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let enabled = body
        .get("enabled")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "invalid 'enabled' (expected a boolean)".to_string(),
            )
        })?;
    state
        .config_db
        .set_setting(
            "deploy_markers_enabled",
            if enabled { "true" } else { "false" },
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to save deploy_markers_enabled");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to save setting".to_string(),
            )
        })?;

    // AUDIT: deploy-markers setting change.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("settings.update", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("setting", "deploy_markers_enabled")
                .changes(
                    serde_json::json!({ "key": "deploy_markers_enabled", "value": enabled })
                        .to_string(),
                )
                .description("deploy markers setting updated")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(serde_json::json!({ "enabled": enabled })))
}

/// PUT /api/v1/settings/export-max-rows — admin only.
/// Sets the maximum number of rows a user may export from Explore.
pub async fn set_export_max_rows(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (axum::http::StatusCode, String)> {
    let caller = crate::handlers::users::require_admin(&state, &headers).await?;

    let value = body.get("value").and_then(|v| v.as_u64()).ok_or_else(|| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            "missing or invalid 'value' (expected a positive integer)".to_string(),
        )
    })?;
    let value = value.clamp(1, crate::handlers::export::EXPORT_MAX_ROWS_CEILING);

    state
        .config_db
        .set_setting("export_max_rows", &value.to_string())
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to set export_max_rows");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "failed to save setting".to_string(),
            )
        })?;

    // AUDIT: export-max-rows setting change.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("settings.update", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("setting", "export_max_rows")
                .changes(
                    serde_json::json!({ "key": "export_max_rows", "value": value }).to_string(),
                )
                .description("export max rows setting updated")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(serde_json::json!({ "export_max_rows": value })))
}

/// GET /api/v1/settings/query-limits — admin only.
/// Returns the effective live workload policy and factory defaults so the UI
/// can offer a safe reset without duplicating defaults in TypeScript.
pub async fn get_query_limits(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    Ok(Json(serde_json::json!({
        "limits": state.query_governor.config(),
        "defaults": QueryGovernorConfig::default(),
    })))
}

/// PUT /api/v1/settings/query-limits — admin only.
/// Persists and activates the complete policy atomically for new work. Queries
/// that already hold permits finish against the previous admission pool.
pub async fn set_query_limits(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(config): Json<QueryGovernorConfig>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    config
        .validate()
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;

    let before = state.query_governor.config();
    let encoded = serde_json::to_string(&config).map_err(|error| {
        tracing::error!(%error, "failed to encode query workload limits");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to save setting".to_string(),
        )
    })?;
    state
        .config_db
        .set_setting(QUERY_LIMITS_SETTING_KEY, &encoded)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to persist query workload limits");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to save setting".to_string(),
            )
        })?;
    state
        .query_governor
        .reconfigure(config.clone())
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;

    // AUDIT: query limit mutation. These are operational limits only; no
    // credentials or secrets are present in either snapshot.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("settings.update", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("setting", QUERY_LIMITS_SETTING_KEY)
                .outcome("success")
                .changes(
                    serde_json::json!({
                        "key": QUERY_LIMITS_SETTING_KEY,
                        "before": before,
                        "after": config,
                    })
                    .to_string(),
                )
                .description("query workload limits updated")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(
        serde_json::json!({ "limits": state.query_governor.config() }),
    ))
}

/// Defaults + clamps for the SRE agent's per-investigation cost budget. Must
/// stay in sync with sre-agent's LoopBudget (which re-clamps defensively).
const SRE_AGENT_DEFAULT_MAX_TOOL_STEPS: u64 = 40;
const SRE_AGENT_DEFAULT_MAX_LLM_CALLS: u64 = 55;
const SRE_AGENT_TENANT_MODE_ALL: &str = "all";
const SRE_AGENT_TENANT_MODE_SELECTED: &str = "selected";
/// Common OpenAI models offered as a combo-box suggestion list in the UI. The field is
/// free-text, so any model name (including non-OpenAI-compatible providers) still works.
const SRE_AGENT_MODEL_SUGGESTIONS: &[&str] = &[
    "gpt-5",
    "gpt-5-mini",
    "gpt-4.1",
    "gpt-4.1-mini",
    "gpt-4o",
    "gpt-4o-mini",
    "o4-mini",
];
/// Reasoning-effort levels for thinking models (OpenAI gpt-5 / o-series).
const SRE_AGENT_REASONING_LEVELS: &[&str] = &["minimal", "low", "medium", "high"];

/// True for models that accept `reasoning_effort` (gpt-5 / o-series). Mirrors the agent's
/// `is_reasoning_model` so the UI can decide whether to show the reasoning control.
fn is_reasoning_model(model: &str) -> bool {
    let m = model.trim().to_ascii_lowercase();
    m.starts_with("gpt-5") || m.starts_with("o1") || m.starts_with("o3") || m.starts_with("o4")
}

/// One admin-allowed model: its id and the reasoning ("thinking") levels a user
/// may pick for it. `reasoning` is empty for non-reasoning models.
#[derive(Debug, Clone, Serialize)]
struct AllowedModel {
    id: String,
    reasoning: Vec<String>,
}

/// Parse the `sre_agent_allowed_models` JSON setting into a validated list.
/// Tolerant: bad JSON / missing setting → empty list. Reasoning levels are
/// filtered against SRE_AGENT_REASONING_LEVELS and dropped for non-reasoning ids.
fn parse_allowed_models(raw: &str) -> Vec<AllowedModel> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Vec::new();
    }
    let arr: Vec<serde_json::Value> = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    arr.into_iter()
        .filter_map(|item| {
            let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").trim();
            if id.is_empty() {
                return None;
            }
            let reasoning = if is_reasoning_model(id) {
                item.get("reasoning")
                    .and_then(|v| v.as_array())
                    .map(|levels| {
                        levels
                            .iter()
                            .filter_map(|l| l.as_str())
                            .map(|s| s.trim())
                            .filter(|s| SRE_AGENT_REASONING_LEVELS.contains(s))
                            .map(|s| s.to_string())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            Some(AllowedModel {
                id: id.to_string(),
                reasoning,
            })
        })
        .collect()
}

/// Resolve the default model from the stored `sre_agent_model` default + the
/// allowed list: the default if it's allowed, else the allowed list's first
/// entry, else the stored default verbatim (which may be empty → agent env).
fn resolve_default_model(default: &str, allowed: &[AllowedModel]) -> String {
    let default = default.trim();
    if !default.is_empty() && allowed.iter().any(|m| m.id == default) {
        return default.to_string();
    }
    if let Some(first) = allowed.first() {
        return first.id.clone();
    }
    default.to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SreAgentAccessDecision {
    Allowed,
    Disabled,
    TenantDenied,
}

fn parse_allowed_tenants(raw: &str) -> Vec<String> {
    let mut tenants = serde_json::from_str::<Vec<String>>(raw).unwrap_or_default();
    tenants = tenants
        .into_iter()
        .map(|tenant| tenant.trim().to_string())
        .filter(|tenant| !tenant.is_empty())
        .collect();
    tenants.sort();
    tenants.dedup();
    tenants
}

fn parse_sre_agent_tenant_policy(raw: &str) -> (String, Vec<String>) {
    let value = serde_json::from_str::<serde_json::Value>(raw).unwrap_or_default();
    let mode = value
        .get("mode")
        .and_then(|mode| mode.as_str())
        .filter(|mode| *mode == SRE_AGENT_TENANT_MODE_SELECTED)
        .unwrap_or(SRE_AGENT_TENANT_MODE_ALL)
        .to_string();
    let allowed = value
        .get("allowed_tenants")
        .map(|allowed| parse_allowed_tenants(&allowed.to_string()))
        .unwrap_or_default();
    (mode, allowed)
}

fn evaluate_sre_agent_access(
    enabled: bool,
    tenant_mode: &str,
    allowed_tenants: &[String],
    tenant: &str,
) -> SreAgentAccessDecision {
    if !enabled {
        return SreAgentAccessDecision::Disabled;
    }
    if tenant_mode != SRE_AGENT_TENANT_MODE_SELECTED {
        return SreAgentAccessDecision::Allowed;
    }
    if allowed_tenants.iter().any(|allowed| allowed == tenant) {
        SreAgentAccessDecision::Allowed
    } else {
        SreAgentAccessDecision::TenantDenied
    }
}

/// Resolve the effective runtime policy. Any settings read failure fails closed.
pub async fn sre_agent_access_decision(state: &AppState, tenant: &str) -> SreAgentAccessDecision {
    let enabled = match state.config_db.get_setting("sre_agent_enabled").await {
        Ok(value) => value.map(|value| value == "true").unwrap_or(false),
        Err(error) => {
            tracing::error!(%error, "failed to read sre_agent_enabled");
            return SreAgentAccessDecision::Disabled;
        }
    };
    let (tenant_mode, allowed_tenants) =
        match state.config_db.get_setting("sre_agent_tenant_access").await {
            Ok(value) => parse_sre_agent_tenant_policy(value.as_deref().unwrap_or("{}")),
            Err(error) => {
                tracing::error!(%error, "failed to read sre_agent_tenant_access");
                return SreAgentAccessDecision::Disabled;
            }
        };
    evaluate_sre_agent_access(enabled, &tenant_mode, &allowed_tenants, tenant)
}

/// GET /api/v1/settings/sre-agent — admin only.
/// Current investigation budget (defaults when unset).
pub async fn get_sre_agent_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let read = |key: &'static str, default: u64| {
        let db = state.config_db.clone();
        async move {
            db.get_setting(key)
                .await
                .ok()
                .flatten()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(default)
        }
    };
    let max_tool_steps = read("sre_agent_max_tool_steps", SRE_AGENT_DEFAULT_MAX_TOOL_STEPS).await;
    let max_llm_calls = read("sre_agent_max_llm_calls", SRE_AGENT_DEFAULT_MAX_LLM_CALLS).await;
    // Same key /api/v1/features exposes as `sre_agent` — this is the UI switch.
    let enabled = state
        .config_db
        .get_setting("sre_agent_enabled")
        .await
        .ok()
        .flatten()
        .map(|v| v == "true")
        .unwrap_or(false);
    // Operator-chosen model (empty = use the agent's built-in default).
    let model = state
        .config_db
        .get_setting("sre_agent_model")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let reasoning_effort = state
        .config_db
        .get_setting("sre_agent_reasoning_effort")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    // Admin-defined policy: which models users may pick + per-model thinking levels.
    let allowed_raw = state
        .config_db
        .get_setting("sre_agent_allowed_models")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let allowed_models = parse_allowed_models(&allowed_raw);
    let (tenant_mode, allowed_tenants) = state
        .config_db
        .get_setting("sre_agent_tenant_access")
        .await
        .ok()
        .flatten()
        .map(|raw| parse_sre_agent_tenant_policy(&raw))
        .unwrap_or_else(|| (SRE_AGENT_TENANT_MODE_ALL.to_string(), Vec::new()));
    Ok(Json(serde_json::json!({
        "enabled": enabled,
        "tenant_mode": tenant_mode,
        "allowed_tenants": allowed_tenants,
        "model": model,
        "allowed_models": allowed_models,
        "model_suggestions": SRE_AGENT_MODEL_SUGGESTIONS,
        "reasoning_effort": reasoning_effort,
        "reasoning_levels": SRE_AGENT_REASONING_LEVELS,
        "model_is_reasoning": is_reasoning_model(&model),
        "max_tool_steps": max_tool_steps,
        "max_llm_calls": max_llm_calls,
        "defaults": {
            "max_tool_steps": SRE_AGENT_DEFAULT_MAX_TOOL_STEPS,
            "max_llm_calls": SRE_AGENT_DEFAULT_MAX_LLM_CALLS,
        },
    })))
}

/// PUT /api/v1/settings/sre-agent — admin only.
/// Sets the SRE agent's per-investigation budget: max tool-executing rounds
/// and max total LLM calls (cost control). Values are clamped server-side;
/// the agent clamps again on read.
pub async fn set_sre_agent_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;

    // Helper to emit a settings.update audit event for an sre-agent setting.
    // No secrets are involved here, but we still log only key + value.
    let audit_setting = |key: &'static str, value: serde_json::Value| {
        let state = state.clone();
        let caller = caller.clone();
        let ctx = crate::audit::actor_context_from_headers(&headers);
        async move {
            state
                .audit
                .log(
                    crate::audit::AuditEvent::new("settings.update", "user")
                        .actor(caller.0.clone(), caller.1.clone())
                        .tenant(caller.3.clone())
                        .resource("setting", key)
                        .changes(serde_json::json!({ "key": key, "value": value }).to_string())
                        .description("sre-agent setting updated")
                        .context(ctx),
                )
                .await;
        }
    };

    // Optional `model` (free text; empty clears it → agent falls back to its built-in default).
    // Saved first so it persists even on a toggle-only update.
    if let Some(model_val) = body.get("model") {
        let model = model_val.as_str().unwrap_or("").trim();
        if model.len() > 100 {
            return Err((StatusCode::BAD_REQUEST, "model name too long".to_string()));
        }
        state
            .config_db
            .set_setting("sre_agent_model", model)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to save sre_agent_model");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to save setting".to_string(),
                )
            })?;
        audit_setting("sre_agent_model", serde_json::json!(model)).await;
    }

    // Optional `reasoning_effort` (minimal/low/medium/high, or empty to clear).
    if let Some(re_val) = body.get("reasoning_effort") {
        let re = re_val.as_str().unwrap_or("").trim();
        if !re.is_empty() && !SRE_AGENT_REASONING_LEVELS.contains(&re) {
            return Err((
                StatusCode::BAD_REQUEST,
                "invalid 'reasoning_effort' (expected minimal|low|medium|high)".to_string(),
            ));
        }
        state
            .config_db
            .set_setting("sre_agent_reasoning_effort", re)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to save sre_agent_reasoning_effort");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to save setting".to_string(),
                )
            })?;
        audit_setting("sre_agent_reasoning_effort", serde_json::json!(re)).await;
    }

    // Optional `allowed_models` policy: which models users may pick + per-model
    // thinking levels. Each item is {id: string, reasoning: string[]}. Validate
    // server-side (non-empty id ≤100 chars; levels ∈ reasoning_levels; reasoning
    // dropped for non-reasoning ids) before re-serializing to the JSON setting.
    if let Some(am_val) = body.get("allowed_models") {
        let arr = am_val.as_array().ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "invalid 'allowed_models' (expected an array)".to_string(),
            )
        })?;
        let mut normalized: Vec<serde_json::Value> = Vec::with_capacity(arr.len());
        for item in arr {
            let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").trim();
            if id.is_empty() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "allowed_models: each model needs a non-empty 'id'".to_string(),
                ));
            }
            if id.len() > 100 {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "allowed_models: model id too long".to_string(),
                ));
            }
            let reasoning: Vec<String> = if is_reasoning_model(id) {
                let mut levels = Vec::new();
                if let Some(arr) = item.get("reasoning").and_then(|v| v.as_array()) {
                    for l in arr {
                        let lvl = l.as_str().unwrap_or("").trim();
                        if lvl.is_empty() {
                            continue;
                        }
                        if !SRE_AGENT_REASONING_LEVELS.contains(&lvl) {
                            return Err((StatusCode::BAD_REQUEST,
                                "allowed_models: invalid reasoning level (expected minimal|low|medium|high)".to_string()));
                        }
                        if !levels.iter().any(|x: &String| x == lvl) {
                            levels.push(lvl.to_string());
                        }
                    }
                }
                levels
            } else {
                Vec::new()
            };
            normalized.push(serde_json::json!({ "id": id, "reasoning": reasoning }));
        }
        let serialized = serde_json::to_string(&normalized).unwrap_or_else(|_| "[]".to_string());
        state
            .config_db
            .set_setting("sre_agent_allowed_models", &serialized)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to save sre_agent_allowed_models");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to save setting".to_string(),
                )
            })?;
        let model_ids: Vec<&str> = normalized
            .iter()
            .filter_map(|m| m.get("id").and_then(|v| v.as_str()))
            .collect();
        audit_setting("sre_agent_allowed_models", serde_json::json!(model_ids)).await;
    }

    // Optional tenant access policy. Both fields are required together so an
    // interrupted/partial client request cannot silently broaden access.
    let tenant_mode_value = body.get("tenant_mode");
    let allowed_tenants_value = body.get("allowed_tenants");
    if tenant_mode_value.is_some() || allowed_tenants_value.is_some() {
        let previous_policy = state
            .config_db
            .get_setting("sre_agent_tenant_access")
            .await
            .map_err(|error| {
                tracing::error!(%error, "failed to read existing sre-agent tenant policy");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to save tenant policy".to_string(),
                )
            })?
            .map(|raw| parse_sre_agent_tenant_policy(&raw))
            .unwrap_or_else(|| (SRE_AGENT_TENANT_MODE_ALL.to_string(), Vec::new()));
        let tenant_mode = tenant_mode_value
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    "invalid 'tenant_mode' (expected all|selected)".to_string(),
                )
            })?;
        if tenant_mode != SRE_AGENT_TENANT_MODE_ALL && tenant_mode != SRE_AGENT_TENANT_MODE_SELECTED
        {
            return Err((
                StatusCode::BAD_REQUEST,
                "invalid 'tenant_mode' (expected all|selected)".to_string(),
            ));
        }
        let allowed_values = allowed_tenants_value
            .and_then(|value| value.as_array())
            .ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    "invalid 'allowed_tenants' (expected an array)".to_string(),
                )
            })?;
        if allowed_values.len() > 500 {
            return Err((
                StatusCode::BAD_REQUEST,
                "allowed_tenants exceeds the 500 tenant limit".to_string(),
            ));
        }
        let mut allowed_tenants = Vec::with_capacity(allowed_values.len());
        for value in allowed_values {
            let tenant = value.as_str().unwrap_or("").trim();
            if tenant.is_empty() || tenant.len() > 128 {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "allowed_tenants contains an invalid tenant name".to_string(),
                ));
            }
            if !allowed_tenants
                .iter()
                .any(|existing: &String| existing == tenant)
            {
                allowed_tenants.push(tenant.to_string());
            }
        }
        let known_tenants = state.config_db.list_tenants().await.map_err(|error| {
            tracing::error!(%error, "failed to validate sre-agent tenant policy");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to validate tenant policy".to_string(),
            )
        })?;
        let unknown: Vec<&String> = allowed_tenants
            .iter()
            .filter(|tenant| {
                !known_tenants
                    .iter()
                    .any(|(_, name, _, _, _)| name == tenant.as_str())
            })
            .collect();
        if !unknown.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "allowed_tenants contains an unknown tenant".to_string(),
            ));
        }
        allowed_tenants.sort();
        let serialized = serde_json::to_string(&serde_json::json!({
            "mode": tenant_mode,
            "allowed_tenants": allowed_tenants,
        }))
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to save tenant policy".to_string(),
            )
        })?;
        state
            .config_db
            .set_setting("sre_agent_tenant_access", &serialized)
            .await
            .map_err(|error| {
                tracing::error!(%error, "failed to save sre_agent_tenant_access");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to save tenant policy".to_string(),
                )
            })?;
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("settings.update", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("setting", "sre_agent_tenant_access")
                    .changes(
                        serde_json::json!({
                            "key": "sre_agent_tenant_access",
                            "before": {
                                "tenant_mode": previous_policy.0,
                                "allowed_tenants": previous_policy.1,
                            },
                            "after": {
                                "tenant_mode": tenant_mode,
                                "allowed_tenants": allowed_tenants,
                            },
                        })
                        .to_string(),
                    )
                    .description("sre-agent tenant access policy updated")
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }

    // model/reasoning/policy-only update (no toggle, no budget) — done.
    if (body.get("model").is_some()
        || body.get("reasoning_effort").is_some()
        || body.get("allowed_models").is_some()
        || body.get("tenant_mode").is_some()
        || body.get("allowed_tenants").is_some())
        && body.get("enabled").is_none()
        && body.get("max_tool_steps").is_none()
        && body.get("max_llm_calls").is_none()
    {
        if let (Some(tenant_mode), Some(allowed_tenants)) =
            (body.get("tenant_mode"), body.get("allowed_tenants"))
        {
            return Ok(Json(serde_json::json!({
                "ok": true,
                "tenant_mode": tenant_mode,
                "allowed_tenants": allowed_tenants,
            })));
        }
        return Ok(Json(serde_json::json!({ "ok": true })));
    }

    // Optional `enabled` toggle: strictly a JSON bool when present.
    if let Some(enabled_val) = body.get("enabled") {
        let enabled = enabled_val.as_bool().ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "invalid 'enabled' (expected a boolean)".to_string(),
            )
        })?;
        state
            .config_db
            .set_setting("sre_agent_enabled", if enabled { "true" } else { "false" })
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to save sre_agent_enabled");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to save setting".to_string(),
                )
            })?;
        audit_setting("sre_agent_enabled", serde_json::json!(enabled)).await;
        // Toggle-only update: budget fields are optional in this case.
        if body.get("max_tool_steps").is_none() && body.get("max_llm_calls").is_none() {
            return Ok(Json(serde_json::json!({ "enabled": enabled })));
        }
    }

    let steps = body
        .get("max_tool_steps")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "missing or invalid 'max_tool_steps' (expected a positive integer)".to_string(),
            )
        })?;
    let calls = body
        .get("max_llm_calls")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "missing or invalid 'max_llm_calls' (expected a positive integer)".to_string(),
            )
        })?;

    let steps = steps.clamp(4, 200);
    // LLM calls must exceed tool steps (retries/critique/summary need slack).
    let calls = calls.clamp(steps + 2, 300);

    for (key, value) in [
        ("sre_agent_max_tool_steps", steps),
        ("sre_agent_max_llm_calls", calls),
    ] {
        state
            .config_db
            .set_setting(key, &value.to_string())
            .await
            .map_err(|e| {
                tracing::error!(error = %e, key, "failed to save sre-agent setting");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to save setting".to_string(),
                )
            })?;
    }

    // AUDIT: sre-agent budget update.
    state.audit.log(
        crate::audit::AuditEvent::new("settings.update", "user")
            .actor(caller.0.clone(), caller.1.clone())
            .tenant(caller.3.clone())
            .resource("setting", "sre_agent_budget")
            .changes(serde_json::json!({ "key": "sre_agent_budget", "max_tool_steps": steps, "max_llm_calls": calls }).to_string())
            .description("sre-agent budget updated")
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;

    Ok(Json(
        serde_json::json!({ "max_tool_steps": steps, "max_llm_calls": calls }),
    ))
}

/// GET /api/v1/sre-agent/options — any authenticated user (NOT admin-only).
/// Surfaces the admin-defined model/thinking policy to the investigation page so
/// a user can pick a model + thinking level from the allowed menu without admin
/// rights. Returns `{ models: [{id, reasoning}], default_model }`.
pub async fn get_sre_agent_options(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_auth(&state, &headers).await?;
    match sre_agent_access_decision(&state, &caller.3).await {
        SreAgentAccessDecision::Allowed => {}
        SreAgentAccessDecision::Disabled => {
            return Err((StatusCode::FORBIDDEN, "SRE agent is disabled".to_string()));
        }
        SreAgentAccessDecision::TenantDenied => {
            return Err((
                StatusCode::FORBIDDEN,
                "SRE agent is not enabled for this tenant".to_string(),
            ));
        }
    }
    let allowed_raw = state
        .config_db
        .get_setting("sre_agent_allowed_models")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let allowed_models = parse_allowed_models(&allowed_raw);
    let default = state
        .config_db
        .get_setting("sre_agent_model")
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let default_model = resolve_default_model(&default, &allowed_models);
    Ok(Json(serde_json::json!({
        "models": allowed_models,
        "default_model": default_model,
    })))
}

/// GET /api/v1/settings/sre-agent/models — admin. Pulls the live model list from the
/// LLM provider (OpenAI-compatible `/v1/models`) using the configured key, filtered to
/// chat-capable models. Falls back to the static suggestion list when no key is set or
/// the provider call fails.
pub async fn list_sre_agent_models(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let fallback = || {
        Json(serde_json::json!({ "models": SRE_AGENT_MODEL_SUGGESTIONS, "source": "suggestions" }))
    };
    if !state.llm_gateway.is_configured() {
        return Ok(fallback());
    }
    let mut models = match state
        .llm_gateway
        .list_models(&crate::llm_gateway::LlmCaller::new(caller.0, caller.3))
        .await
    {
        Ok(models) => models
            .into_iter()
            .filter(|id| is_chat_model(id))
            .collect::<Vec<_>>(),
        Err(_) => return Ok(fallback()),
    };
    models.sort();
    models.dedup();
    if models.is_empty() {
        return Ok(fallback());
    }
    Ok(Json(
        serde_json::json!({ "models": models, "source": "provider" }),
    ))
}

/// Filter `/v1/models` ids down to chat-completions-capable models.
fn is_chat_model(id: &str) -> bool {
    let m = id.to_ascii_lowercase();
    let chatty = m.starts_with("gpt-")
        || m.starts_with("chatgpt")
        || m.starts_with("o1")
        || m.starts_with("o3")
        || m.starts_with("o4");
    let excluded = [
        "embedding",
        "audio",
        "realtime",
        "transcribe",
        "tts",
        "whisper",
        "image",
        "moderation",
        "instruct",
        "search",
    ]
    .iter()
    .any(|x| m.contains(x));
    chatty && !excluded
}

#[cfg(test)]
mod sre_agent_access_tests {
    use super::{
        SreAgentAccessDecision, evaluate_sre_agent_access, parse_allowed_tenants,
        parse_sre_agent_tenant_policy,
    };

    #[test]
    fn disabled_agent_denies_every_tenant() {
        assert_eq!(
            evaluate_sre_agent_access(false, "all", &[], "default"),
            SreAgentAccessDecision::Disabled
        );
    }

    #[test]
    fn all_mode_preserves_existing_behavior() {
        assert_eq!(
            evaluate_sre_agent_access(true, "all", &[], "any-tenant"),
            SreAgentAccessDecision::Allowed
        );
    }

    #[test]
    fn selected_mode_only_allows_exact_tenant_names() {
        let allowed = vec!["default".to_string(), "production".to_string()];
        assert_eq!(
            evaluate_sre_agent_access(true, "selected", &allowed, "production"),
            SreAgentAccessDecision::Allowed
        );
        assert_eq!(
            evaluate_sre_agent_access(true, "selected", &allowed, "staging"),
            SreAgentAccessDecision::TenantDenied
        );
    }

    #[test]
    fn tenant_parser_trims_and_deduplicates() {
        assert_eq!(
            parse_allowed_tenants(r#"[" production ","default","production",""]"#),
            vec!["default".to_string(), "production".to_string()]
        );
    }

    #[test]
    fn missing_or_invalid_policy_defaults_to_all_tenants() {
        assert_eq!(
            parse_sre_agent_tenant_policy("{}"),
            ("all".to_string(), Vec::new())
        );
        assert_eq!(
            parse_sre_agent_tenant_policy(r#"{"mode":"bogus","allowed_tenants":["a"]}"#),
            ("all".to_string(), vec!["a".to_string()])
        );
    }
}

pub async fn delete_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let deleted = state.config_db.delete_api_key(&id).await.map_err(|e| {
        tracing::error!(error = %e, "internal error");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    }
    state.ingest_key_limiter.remove(&id);
    tracing::info!(
        event = "api_key_deleted",
        key_id = %id,
        admin = %caller.1,
        "API key deleted"
    );

    // AUDIT: API key revoked/deleted.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("apikey.revoke", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("api_key", id.clone())
                .description("api key revoked")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(StatusCode::NO_CONTENT)
}
