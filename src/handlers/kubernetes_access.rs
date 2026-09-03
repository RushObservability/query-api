use std::net::{IpAddr, SocketAddr};

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::handlers::infrastructure::require_infrastructure_read;
use crate::handlers::users::require_admin;
use crate::models::kubernetes_access::{
    KubernetesAccessEvent, KubernetesAccessEventView, KubernetesAccessFilter,
    KubernetesSessionChunk, KubernetesSessionChunkView,
};
use crate::{AppState, RequestIdentity};

pub const MAX_ACCESS_EVENT_BODY_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_SESSION_CHUNK_BODY_BYTES: usize = 512 * 1024;
pub const MAX_GATEWAY_AUTHORIZE_BODY_BYTES: usize = 8 * 1024;
pub const MAX_KUBERNETES_LOGIN_BODY_BYTES: usize = 4 * 1024;
pub const MAX_CLIENT_ENRICHMENT_BODY_BYTES: usize = 64 * 1024;
pub const MAX_KUBERNETES_RBAC_BODY_BYTES: usize = 64 * 1024;
pub const MIN_KUBERNETES_SESSION_SECONDS: i64 = 300;
pub const MAX_KUBERNETES_SESSION_SECONDS: i64 = 43_200;
pub const DEFAULT_KUBERNETES_SESSION_SECONDS: i64 = 3_600;
const KUBERNETES_SESSION_SECONDS_SETTING: &str = "kubernetes_access_max_session_seconds";
const DEFAULT_MAX_RESULT_BYTES: usize = 256 * 1024;
const HARD_MAX_RESULT_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_SESSION_BYTES: u64 = 64 * 1024 * 1024;
const HARD_MAX_SESSION_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_TEXT_BYTES: usize = 1024;
const MAX_JSON_METADATA_BYTES: usize = 64 * 1024;
const MAX_LIST_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_EXPORT_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_SESSION_REPLAY_PAGE: u64 = 512;
const MAX_KUBERNETES_RBAC_GRANTS: usize = 100;
const MAX_KUBERNETES_RBAC_RULES: usize = 32;
const MAX_KUBERNETES_AUTHORIZATION_GROUPS: usize = 32;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InternalAccessEventInput {
    pub tenant_id: String,
    #[serde(default)]
    pub id: String,
    pub cluster_id: String,
    #[serde(default)]
    pub gateway_id: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub actor_user_id: String,
    #[serde(default)]
    pub actor_name: String,
    pub actor_type: String,
    #[serde(default)]
    pub kube_username: String,
    #[serde(default)]
    pub kube_groups: Vec<String>,
    pub source_kind: String,
    #[serde(default)]
    pub client_reported: serde_json::Value,
    #[serde(default)]
    pub observed_network: serde_json::Value,
    #[serde(default)]
    pub http_method: String,
    pub verb: String,
    #[serde(default)]
    pub api_group: String,
    #[serde(default)]
    pub api_version: String,
    #[serde(default)]
    pub resource: String,
    #[serde(default)]
    pub subresource: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub request_query: serde_json::Value,
    #[serde(default)]
    pub user_agent: String,
    #[serde(default)]
    pub status_code: u16,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub request_bytes: u64,
    #[serde(default)]
    pub response_bytes: u64,
    #[serde(default)]
    pub result_summary: serde_json::Value,
    #[serde(default)]
    pub result_truncated: bool,
    #[serde(default)]
    pub recording_state: String,
    #[serde(default)]
    pub created_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientReportedInput {
    #[serde(default)]
    pub argv: Vec<String>,
    #[serde(default)]
    pub cli_version: String,
    #[serde(default)]
    pub os: String,
    #[serde(default)]
    pub arch: String,
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub private_ips: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientAccessEventInput {
    pub cluster_id: String,
    pub client_reported: ClientReportedInput,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionChunkInput {
    #[serde(default)]
    pub id: String,
    pub tenant_id: String,
    pub session_id: String,
    pub event_id: String,
    #[serde(default)]
    pub gateway_id: String,
    pub sequence: u64,
    pub stream: String,
    #[serde(default)]
    pub encoding: String,
    #[serde(default)]
    pub offset_ms: u64,
    pub data: String,
    #[serde(default)]
    pub byte_count: u64,
    #[serde(default)]
    pub recording_state: String,
    #[serde(default)]
    pub created_at: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct SessionChunkQuery {
    pub after_sequence: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct SessionChunkListResponse {
    pub chunks: Vec<KubernetesSessionChunkView>,
    pub has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_sequence: Option<u64>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct AccessEventQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub actor: Option<String>,
    pub cluster: Option<String>,
    pub namespace: Option<String>,
    pub verb: Option<String>,
    pub resource: Option<String>,
    pub status: Option<String>,
    pub source_kind: Option<String>,
    pub recording_state: Option<String>,
    pub q: Option<String>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Serialize)]
struct AccessEventListResponse {
    events: Vec<KubernetesAccessEventView>,
    total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct AgentAccessEventQuery {
    pub tenant_id: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub actor: Option<String>,
    pub cluster: Option<String>,
    pub namespace: Option<String>,
    pub verb: Option<String>,
    pub resource: Option<String>,
    pub status: Option<String>,
    pub limit: Option<u64>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct AgentAccessEventView {
    id: String,
    created_at: String,
    actor_name: String,
    actor_type: String,
    kube_username: String,
    cluster_id: String,
    namespace: String,
    verb: String,
    resource: String,
    subresource: String,
    name: String,
    status_code: u16,
    duration_ms: u64,
    likely_kubectl_command: String,
    source_kind: String,
    session_id: String,
    recording_state: String,
}

#[derive(Debug, Serialize)]
struct AgentAccessEventListResponse {
    events: Vec<AgentAccessEventView>,
    total: u64,
    evidence_note: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GatewayAuthorization {
    pub actor_user_id: String,
    pub actor_name: String,
    pub actor_type: String,
    pub tenant_id: String,
    pub cluster_id: String,
    pub role: String,
    pub kube_username: String,
    pub kube_groups: Vec<String>,
    pub client_reported: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayAuthorizeInput {
    pub cluster_id: String,
    pub audience: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayReadyInput {
    pub cluster_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesLoginStartInput {
    pub cluster_id: String,
}

#[derive(Debug, Serialize)]
pub struct KubernetesLoginStartResponse {
    pub device_code: String,
    pub user_code: String,
    pub expires_in: i64,
    pub interval: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesLoginApproveInput {
    pub user_code: String,
}

#[derive(Debug, Serialize)]
pub struct KubernetesLoginDetailsResponse {
    pub status: String,
    pub cluster_id: String,
    pub approval_expires_at: String,
    pub credential_ttl_seconds: i64,
}

#[derive(Debug, Serialize)]
pub struct KubernetesLoginApproveResponse {
    pub status: &'static str,
    pub cluster_id: String,
    pub credential_expires_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesLoginTokenInput {
    pub device_code: String,
}

#[derive(Debug, Serialize)]
pub struct KubernetesLoginTokenResponse {
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    pub interval: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesLoggingSettingsInput {
    pub max_session_seconds: i64,
}

#[derive(Debug, Serialize)]
pub struct KubernetesClientSessionView {
    pub session_id: String,
    pub username: String,
    pub cluster_id: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub cli_version: String,
    pub approved_at: String,
    pub expires_at: String,
}

#[derive(Debug, Serialize)]
pub struct KubernetesLoggingSettingsResponse {
    pub max_session_seconds: i64,
    pub min_session_seconds: i64,
    pub max_allowed_session_seconds: i64,
    pub active_clients: Vec<KubernetesClientSessionView>,
    pub rbac_grants: Vec<KubernetesRbacGrantView>,
    pub gateways: Vec<KubernetesGatewayClusterView>,
    pub available_clusters: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KubernetesGatewayClusterView {
    pub gateway_id: String,
    pub cluster_id: String,
    pub configured: bool,
    pub last_activity: String,
    pub recorded_requests: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KubernetesRbacRule {
    #[serde(default)]
    pub api_groups: Vec<String>,
    pub resources: Vec<String>,
    pub verbs: Vec<String>,
}

fn default_kubernetes_cluster_match() -> String {
    "single".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesRbacGrantInput {
    pub group_id: String,
    #[serde(default)]
    pub cluster_id: String,
    #[serde(default = "default_kubernetes_cluster_match")]
    pub cluster_match: String,
    #[serde(default)]
    pub cluster_pattern: String,
    pub name: String,
    pub role_kind: String,
    #[serde(default)]
    pub role_name: String,
    pub scope: String,
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default)]
    pub rules: Vec<KubernetesRbacRule>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KubernetesRbacGrantView {
    pub id: String,
    pub tenant_id: String,
    pub group_id: String,
    pub kubernetes_group: String,
    pub cluster_id: String,
    pub cluster_match: String,
    pub cluster_pattern: String,
    pub name: String,
    pub role_kind: String,
    pub role_name: String,
    pub scope: String,
    pub namespaces: Vec<String>,
    pub rules: Vec<KubernetesRbacRule>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct GatewayRbacResponse {
    pub cluster_id: String,
    pub grants: Vec<KubernetesRbacGrantView>,
}

#[derive(Debug, Deserialize)]
pub struct GatewayRbacQuery {
    pub cluster_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayRbacReconcileInput {
    pub cluster_id: String,
    pub revision: String,
    pub grant_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayBinding {
    gateway_id: String,
    tenant_ids: Vec<String>,
    cluster_id: String,
}

fn enabled() -> bool {
    std::env::var("KUBERNETES_ACCESS_ENABLED")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(false)
}

pub fn available() -> bool {
    enabled() && crate::license::evaluate().has_entitlement("kubernetes_access")
}

fn require_enabled() -> Result<(), (StatusCode, String)> {
    if !enabled() {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    }
    if !crate::license::evaluate().has_entitlement("kubernetes_access") {
        return Err((
            StatusCode::FORBIDDEN,
            "Kubernetes access recording add-on is not licensed".to_string(),
        ));
    }
    Ok(())
}

fn parse_bounded_env(name: &str, default: usize, hard_max: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1024..=hard_max).contains(value))
        .unwrap_or(default)
}

fn max_result_bytes() -> usize {
    parse_bounded_env(
        "KUBERNETES_ACCESS_MAX_RESULT_BYTES",
        DEFAULT_MAX_RESULT_BYTES,
        HARD_MAX_RESULT_BYTES,
    )
}

fn max_session_bytes() -> u64 {
    parse_bounded_env(
        "KUBERNETES_ACCESS_MAX_SESSION_BYTES",
        DEFAULT_MAX_SESSION_BYTES as usize,
        HARD_MAX_SESSION_BYTES as usize,
    ) as u64
}

fn kubernetes_session_seconds_from_env() -> i64 {
    std::env::var("KUBERNETES_ACCESS_CREDENTIAL_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| {
            (MIN_KUBERNETES_SESSION_SECONDS..=MAX_KUBERNETES_SESSION_SECONDS).contains(value)
        })
        .unwrap_or(DEFAULT_KUBERNETES_SESSION_SECONDS)
}

async fn kubernetes_session_seconds(state: &AppState) -> Result<i64, (StatusCode, String)> {
    let stored = state
        .config_db
        .get_setting(KUBERNETES_SESSION_SECONDS_SETTING)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to read Kubernetes session limit");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Kubernetes logging settings are unavailable".to_string(),
            )
        })?;
    Ok(stored
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| {
            (MIN_KUBERNETES_SESSION_SECONDS..=MAX_KUBERNETES_SESSION_SECONDS).contains(value)
        })
        .unwrap_or_else(kubernetes_session_seconds_from_env))
}

fn temporary_credential_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn temporary_device_credential() -> String {
    format!(
        "rkt1_{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

fn temporary_user_code() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..16].to_ascii_uppercase()
}

fn client_session_id(device_code_hash: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"kubernetes-client-session\0");
    digest.update(device_code_hash.as_bytes());
    format!("kcs_{}", &hex::encode(digest.finalize())[..24])
}

fn client_session_view(
    request: &crate::clickhouse_config::KubernetesLoginRequest,
) -> KubernetesClientSessionView {
    let reported = serde_json::from_str::<serde_json::Value>(&request.client_reported)
        .unwrap_or_else(|_| serde_json::json!({}));
    let field = |name: &str| {
        reported
            .get(name)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    KubernetesClientSessionView {
        session_id: client_session_id(&request.device_code_hash),
        username: request.username.clone(),
        cluster_id: request.cluster_id.clone(),
        hostname: field("hostname"),
        os: field("os"),
        arch: field("arch"),
        cli_version: field("cli_version"),
        approved_at: request.approved_at.clone(),
        expires_at: request.credential_expires_at.clone(),
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn internal_secret() -> Result<String, (StatusCode, String)> {
    std::env::var("KUBERNETES_ACCESS_INTERNAL_TOKEN")
        .ok()
        .filter(|value| value.len() >= 32)
        .ok_or_else(|| {
            tracing::error!("KUBERNETES_ACCESS_INTERNAL_TOKEN is missing or shorter than 32 bytes");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes access recording is not configured".to_string(),
            )
        })
}

fn constant_time_eq(actual: &[u8], expected: &[u8]) -> bool {
    if actual.len() != expected.len() {
        return false;
    }
    actual
        .iter()
        .zip(expected)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn require_internal(headers: &HeaderMap) -> Result<String, (StatusCode, String)> {
    let expected = internal_secret()?;
    validate_internal_header(headers, &expected)?;
    Ok(expected)
}

fn validate_internal_header(
    headers: &HeaderMap,
    expected: &str,
) -> Result<(), (StatusCode, String)> {
    let actual = headers
        .get("x-rush-internal-token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "invalid internal credential".to_string(),
        ));
    }
    Ok(())
}

fn gateway_binding() -> Result<GatewayBinding, (StatusCode, String)> {
    let read = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                tracing::error!(setting = name, "Kubernetes gateway binding is missing");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Kubernetes access gateway is not configured".to_string(),
                )
            })
    };
    let tenant_ids = read("KUBERNETES_ACCESS_GATEWAY_TENANT_IDS")?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if tenant_ids.is_empty() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Kubernetes access gateway is not configured".to_string(),
        ));
    }
    Ok(GatewayBinding {
        gateway_id: read("KUBERNETES_ACCESS_GATEWAY_ID")?,
        tenant_ids,
        cluster_id: read("KUBERNETES_ACCESS_GATEWAY_CLUSTER_ID")?,
    })
}

fn validate_gateway_binding(
    binding: &GatewayBinding,
    gateway_id: &str,
    tenant_id: &str,
    cluster_id: Option<&str>,
) -> Result<(), (StatusCode, String)> {
    if gateway_id != binding.gateway_id
        || !binding
            .tenant_ids
            .iter()
            .any(|allowed| allowed == tenant_id)
        || cluster_id.is_some_and(|cluster| cluster != binding.cluster_id)
    {
        return Err((
            StatusCode::FORBIDDEN,
            "gateway is not authorized for this tenant or cluster".to_string(),
        ));
    }
    Ok(())
}

fn validate_authorizing_gateway(
    binding: &GatewayBinding,
    headers: &HeaderMap,
    tenant_id: &str,
    cluster_id: &str,
) -> Result<(), (StatusCode, String)> {
    validate_gateway_instance(binding, headers, cluster_id)?;
    if !binding
        .tenant_ids
        .iter()
        .any(|allowed| allowed == tenant_id)
    {
        return Err((
            StatusCode::FORBIDDEN,
            "gateway is not authorized for this tenant or cluster".to_string(),
        ));
    }
    Ok(())
}

fn validate_gateway_instance(
    binding: &GatewayBinding,
    headers: &HeaderMap,
    cluster_id: &str,
) -> Result<(), (StatusCode, String)> {
    let gateway_id = headers
        .get("x-rush-gateway-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if gateway_id != binding.gateway_id || cluster_id != binding.cluster_id {
        return Err((
            StatusCode::FORBIDDEN,
            "gateway is not authorized for this cluster".to_string(),
        ));
    }
    Ok(())
}

fn tenant_cluster_allowed(
    raw_policy: &str,
    tenant_id: &str,
    cluster_id: &str,
) -> Result<bool, (StatusCode, String)> {
    let policy = parse_tenant_cluster_policy(raw_policy)?;
    Ok(policy
        .get(tenant_id)
        .is_some_and(|clusters| clusters.iter().any(|allowed| allowed == cluster_id)))
}

fn parse_tenant_cluster_policy(
    raw_policy: &str,
) -> Result<std::collections::HashMap<String, Vec<String>>, (StatusCode, String)> {
    serde_json::from_str(raw_policy).map_err(|_| {
        tracing::error!("KUBERNETES_ACCESS_TENANT_CLUSTERS is invalid JSON");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Kubernetes access policy is unavailable".to_string(),
        )
    })
}

fn validate_text(name: &str, value: &str, max_bytes: usize) -> Result<(), (StatusCode, String)> {
    if value.len() > max_bytes {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("{name} must not exceed {max_bytes} bytes"),
        ));
    }
    Ok(())
}

fn require_text(name: &str, value: &str, max_bytes: usize) -> Result<(), (StatusCode, String)> {
    if value.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, format!("{name} is required")));
    }
    validate_text(name, value, max_bytes)
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "authorization",
        "cookie",
        "password",
        "passwd",
        "secret",
        "token",
        "client_certificate",
        "private_key",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

fn redact_value(value: &mut serde_json::Value) -> u32 {
    match value {
        serde_json::Value::Object(fields) => {
            let kubernetes_secret = fields
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|kind| kind.eq_ignore_ascii_case("Secret"));
            fields
                .iter_mut()
                .map(|(key, value)| {
                    if is_sensitive_key(key)
                        || (kubernetes_secret && matches!(key.as_str(), "data" | "stringData"))
                    {
                        *value = serde_json::Value::String("[REDACTED]".to_string());
                        1
                    } else {
                        redact_value(value)
                    }
                })
                .sum()
        }
        serde_json::Value::Array(values) => values.iter_mut().map(redact_value).sum(),
        serde_json::Value::String(text) => {
            let lower = text.to_ascii_lowercase();
            if lower.contains("bearer ")
                || lower.contains("-----begin private key-----")
                || lower.contains("-----begin rsa private key-----")
            {
                *text = "[REDACTED]".to_string();
                1
            } else {
                0
            }
        }
        _ => 0,
    }
}

fn parse_embedded_json(value: &mut serde_json::Value, depth: u8) {
    if depth >= 4 {
        return;
    }
    match value {
        serde_json::Value::Object(fields) => {
            for value in fields.values_mut() {
                parse_embedded_json(value, depth + 1);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                parse_embedded_json(value, depth + 1);
            }
        }
        serde_json::Value::String(raw)
            if matches!(raw.trim_start().as_bytes().first(), Some(b'{') | Some(b'[')) =>
        {
            if let Ok(mut parsed) = serde_json::from_str::<serde_json::Value>(raw) {
                parse_embedded_json(&mut parsed, depth + 1);
                *value = parsed;
            }
        }
        _ => {}
    }
}

fn bounded_json(
    mut value: serde_json::Value,
    max_bytes: usize,
) -> Result<(String, bool, u32), (StatusCode, String)> {
    parse_embedded_json(&mut value, 0);
    let redactions = redact_value(&mut value);
    let serialized = serde_json::to_string(&value)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid JSON metadata".to_string()))?;
    if serialized.len() <= max_bytes {
        return Ok((serialized, false, redactions));
    }

    let mut preview_bytes = max_bytes / 2;
    loop {
        let mut end = preview_bytes.min(serialized.len());
        while end > 0 && !serialized.is_char_boundary(end) {
            end -= 1;
        }
        let replacement = serde_json::json!({
            "truncated": true,
            "original_bytes": serialized.len(),
            "preview": &serialized[..end],
        });
        let bounded = serde_json::to_string(&replacement).unwrap_or_else(|_| {
            format!(
                r#"{{"truncated":true,"original_bytes":{}}}"#,
                serialized.len()
            )
        });
        if bounded.len() <= max_bytes || preview_bytes == 0 {
            return Ok((bounded, true, redactions));
        }
        preview_bytes /= 2;
    }
}

fn with_provenance(value: serde_json::Value, provenance: &str) -> serde_json::Value {
    let mut value = match value {
        serde_json::Value::Object(fields) => serde_json::Value::Object(fields),
        _ => serde_json::json!({}),
    };
    if let Some(fields) = value.as_object_mut() {
        fields.insert(
            "provenance".to_string(),
            serde_json::Value::String(provenance.to_string()),
        );
    }
    value
}

fn redact_client_argv(value: &mut serde_json::Value) -> u32 {
    let Some(argv) = value
        .get_mut("argv")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return 0;
    };
    let mut redact_next = false;
    let mut redactions = 0_u32;
    for argument in argv {
        let Some(text) = argument.as_str() else {
            redact_next = false;
            continue;
        };
        if redact_next {
            *argument = serde_json::Value::String("[REDACTED]".to_string());
            redactions = redactions.saturating_add(1);
            redact_next = false;
            continue;
        }
        let lower = text.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "--token" | "--password" | "--client-key" | "--client-certificate"
        ) {
            redact_next = true;
        } else if [
            "--token=",
            "--password=",
            "--client-key=",
            "--client-certificate=",
        ]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
        {
            let flag = text.split_once('=').map(|(flag, _)| flag).unwrap_or(text);
            *argument = serde_json::Value::String(format!("{flag}=[REDACTED]"));
            redactions = redactions.saturating_add(1);
        }
    }
    redactions
}

fn sanitize_gateway_network(value: serde_json::Value, secret: &str) -> serde_json::Value {
    sanitize_gateway_network_with_retention(
        value,
        secret,
        crate::api_key_auth::env_flag("KUBERNETES_ACCESS_RETAIN_RAW_IP"),
    )
}

fn sanitize_gateway_network_with_retention(
    value: serde_json::Value,
    secret: &str,
    retain_raw_ip: bool,
) -> serde_json::Value {
    let gateway_provenance = value.get("provenance").cloned();
    let mut value = with_provenance(value, "gateway_observed");
    if retain_raw_ip {
        return value;
    }
    let Some(fields) = value.as_object_mut() else {
        return serde_json::json!({"provenance": "gateway_observed"});
    };
    if let Some(gateway_provenance) = gateway_provenance {
        fields.insert(
            "gateway_reported_provenance".to_string(),
            gateway_provenance,
        );
    }
    let parse_ip = |value: serde_json::Value| {
        value.as_str().and_then(|raw| {
            let first = raw.split(',').next().unwrap_or(raw).trim();
            first
                .parse::<IpAddr>()
                .ok()
                .or_else(|| first.parse::<SocketAddr>().ok().map(|peer| peer.ip()))
        })
    };
    let mut source_ip = None;
    for key in [
        "observed_source_ip",
        "source_ip",
        "public_ip",
        "ip",
        "trusted_forwarded_for",
        "forwarded_for",
        "socket_peer",
    ] {
        if let Some(value) = fields.remove(key)
            && source_ip.is_none()
        {
            source_ip = parse_ip(value);
        }
    }
    if let Some(ip) = source_ip {
        let evidence = network_evidence(ip, secret);
        if let Some(ip_hash) = evidence.get("ip_hash") {
            fields.insert("ip_hash".to_string(), ip_hash.clone());
        }
        if let Some(ip_prefix) = evidence.get("ip_prefix") {
            fields.insert("ip_prefix".to_string(), ip_prefix.clone());
        }
    }
    if let Some(proxy_chain) = fields.remove("proxy_chain") {
        let hop_count = proxy_chain.as_array().map(Vec::len).unwrap_or_default();
        fields.insert("proxy_hop_count".to_string(), serde_json::json!(hop_count));
    }
    fields.insert("raw_ip_retained".to_string(), serde_json::json!(false));
    value
}

fn normalize_timestamp(raw: &str) -> String {
    if raw.is_empty() {
        return chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|value| {
            value
                .with_timezone(&chrono::Utc)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|_| chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string())
}

fn normalize_filter_timestamp(name: &str, raw: &str) -> Result<String, (StatusCode, String)> {
    if raw.is_empty() {
        return Ok(String::new());
    }
    if let Ok(value) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Ok(value
            .with_timezone(&chrono::Utc)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string());
    }
    chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
        .map(|value| value.format("%Y-%m-%d %H:%M:%S").to_string())
        .map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                format!("{name} must be an RFC3339 timestamp"),
            )
        })
}

fn validate_internal_input(input: &InternalAccessEventInput) -> Result<(), (StatusCode, String)> {
    require_text("tenant_id", &input.tenant_id, 128)?;
    require_text("cluster_id", &input.cluster_id, 256)?;
    require_text("verb", &input.verb, 64)?;
    if !matches!(
        input.source_kind.as_str(),
        "gateway" | "kubernetes_audit_webhook"
    ) {
        return Err((
            StatusCode::BAD_REQUEST,
            "source_kind must be gateway or kubernetes_audit_webhook".to_string(),
        ));
    }
    if !matches!(input.actor_type.as_str(), "user" | "api_key" | "system") {
        return Err((
            StatusCode::BAD_REQUEST,
            "actor_type must be user, api_key, or system".to_string(),
        ));
    }
    if input.source_kind == "gateway" && input.actor_type == "system" {
        return Err((
            StatusCode::BAD_REQUEST,
            "gateway actor_type must be user or api_key".to_string(),
        ));
    }
    if !input.recording_state.is_empty()
        && !matches!(
            input.recording_state.as_str(),
            "complete" | "partial" | "partial_protocol_capture" | "failed" | "not_recorded"
        )
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid recording_state".to_string(),
        ));
    }
    for (name, value, max) in [
        ("id", input.id.as_str(), 128),
        ("gateway_id", input.gateway_id.as_str(), 256),
        ("session_id", input.session_id.as_str(), 128),
        ("actor_user_id", input.actor_user_id.as_str(), 256),
        ("actor_name", input.actor_name.as_str(), 256),
        ("kube_username", input.kube_username.as_str(), 256),
        ("http_method", input.http_method.as_str(), 32),
        ("api_group", input.api_group.as_str(), 128),
        ("api_version", input.api_version.as_str(), 128),
        ("resource", input.resource.as_str(), 128),
        ("subresource", input.subresource.as_str(), 128),
        ("namespace", input.namespace.as_str(), 253),
        ("name", input.name.as_str(), 253),
        ("user_agent", input.user_agent.as_str(), MAX_TEXT_BYTES),
        ("created_at", input.created_at.as_str(), 64),
    ] {
        validate_text(name, value, max)?;
    }
    if input.kube_groups.len() > 32 || input.kube_groups.iter().any(|group| group.len() > 128) {
        return Err((
            StatusCode::BAD_REQUEST,
            "kube_groups exceeds its limit".to_string(),
        ));
    }
    Ok(())
}

fn internal_event(
    input: InternalAccessEventInput,
    secret: &str,
) -> Result<KubernetesAccessEvent, (StatusCode, String)> {
    validate_internal_input(&input)?;
    let mut client_reported_value = with_provenance(input.client_reported, "client_reported");
    let argv_redactions = redact_client_argv(&mut client_reported_value);
    let (client_reported, _, client_redactions) =
        bounded_json(client_reported_value, MAX_JSON_METADATA_BYTES)?;
    let (observed_network, _, network_redactions) = bounded_json(
        sanitize_gateway_network(input.observed_network, secret),
        MAX_JSON_METADATA_BYTES,
    )?;
    let (request_query, _, query_redactions) =
        bounded_json(input.request_query, MAX_JSON_METADATA_BYTES)?;
    let (result_summary, result_truncated, result_redactions) =
        bounded_json(input.result_summary, max_result_bytes())?;
    let recording_state = if input.recording_state.is_empty() {
        "complete".to_string()
    } else {
        input.recording_state
    };

    Ok(KubernetesAccessEvent {
        id: if input.id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            input.id
        },
        tenant_id: input.tenant_id,
        cluster_id: input.cluster_id,
        gateway_id: input.gateway_id,
        session_id: input.session_id,
        actor_user_id: input.actor_user_id,
        actor_name: input.actor_name,
        actor_type: input.actor_type,
        kube_username: input.kube_username,
        kube_groups: serde_json::to_string(&input.kube_groups).unwrap_or_else(|_| "[]".to_string()),
        source_kind: input.source_kind,
        client_reported,
        observed_network,
        http_method: input.http_method,
        verb: input.verb,
        api_group: input.api_group,
        api_version: input.api_version,
        resource: input.resource,
        subresource: input.subresource,
        namespace: input.namespace,
        name: input.name,
        request_query,
        user_agent: input.user_agent,
        status_code: input.status_code,
        duration_ms: input.duration_ms,
        request_bytes: input.request_bytes,
        response_bytes: input.response_bytes,
        result_summary,
        result_truncated: u8::from(input.result_truncated || result_truncated),
        redaction_count: client_redactions
            .saturating_add(argv_redactions)
            .saturating_add(network_redactions)
            .saturating_add(query_redactions)
            .saturating_add(result_redactions),
        recording_state,
        created_at: normalize_timestamp(&input.created_at),
    })
}

fn network_evidence(ip: IpAddr, secret: &str) -> serde_json::Value {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(b"kubernetes-access-source-ip\0");
    mac.update(ip.to_string().as_bytes());
    let ip_hash = hex::encode(mac.finalize().into_bytes());
    let prefix = match ip {
        IpAddr::V4(value) => {
            let [a, b, c, _] = value.octets();
            format!("{a}.{b}.{c}.0/24")
        }
        IpAddr::V6(value) => {
            let segments = value.segments();
            format!(
                "{:x}:{:x}:{:x}:{:x}::/64",
                segments[0], segments[1], segments[2], segments[3]
            )
        }
    };
    serde_json::json!({
        "provenance": "query_api_observed",
        "ip_hash": ip_hash,
        "ip_prefix": prefix,
    })
}

fn collect_private_ips(input: &[String]) -> Vec<String> {
    if !std::env::var("KUBERNETES_ACCESS_COLLECT_PRIVATE_IP")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(false)
    {
        return Vec::new();
    }
    input
        .iter()
        .take(8)
        .filter_map(|value| value.parse::<IpAddr>().ok())
        .filter(|address| match address {
            IpAddr::V4(value) => value.is_private() || value.is_link_local(),
            IpAddr::V6(value) => value.is_unique_local() || value.is_unicast_link_local(),
        })
        .map(|value| value.to_string())
        .collect()
}

fn prepare_session_chunk(
    stream: &str,
    requested_encoding: &str,
    input: String,
) -> Result<(String, String, u64, u32, String), (StatusCode, String)> {
    if !matches!(
        stream,
        "stdin"
            | "stdout"
            | "stderr"
            | "error"
            | "resize"
            | "session"
            | "raw_upgrade_input"
            | "raw_upgrade_output"
    ) {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid Kubernetes session stream".to_string(),
        ));
    }
    let raw_protocol = matches!(stream, "raw_upgrade_input" | "raw_upgrade_output");
    let encoding = if requested_encoding.is_empty() {
        if raw_protocol { "base64" } else { "utf8" }.to_string()
    } else {
        requested_encoding.to_string()
    };
    let (data, byte_count, redactions) = match encoding.as_str() {
        "base64" => {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(input.as_bytes())
                .map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        "session chunk data is not valid base64".to_string(),
                    )
                })?;
            (input, decoded.len() as u64, 0)
        }
        "utf8" if !raw_protocol => {
            let mut data = serde_json::Value::String(input);
            let redactions = redact_value(&mut data);
            let data = data.as_str().unwrap_or("[REDACTED]").to_string();
            let byte_count = data.len() as u64;
            (data, byte_count, redactions)
        }
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                "session chunk encoding must be base64, or utf8 for decoded streams".to_string(),
            ));
        }
    };
    let direction = match stream {
        "stdin" | "resize" | "raw_upgrade_input" => "client_to_cluster",
        "stdout" | "stderr" | "error" | "raw_upgrade_output" => "cluster_to_client",
        _ => "gateway",
    };
    let provenance = serde_json::json!({
        "capture": if raw_protocol { "gateway_raw_protocol" } else { "gateway_decoded_stream" },
        "direction": direction,
        "decoded_channels": !raw_protocol,
        "terminal_text": matches!(stream, "stdin" | "stdout" | "stderr" | "error"),
        "sensitive_input": stream == "stdin",
    });
    let max_chunk = MAX_SESSION_CHUNK_BODY_BYTES / 2;
    if byte_count > max_chunk as u64 || data.len() > MAX_SESSION_CHUNK_BODY_BYTES {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("session chunk must not exceed {max_chunk} bytes"),
        ));
    }
    Ok((
        data,
        encoding,
        byte_count,
        redactions,
        serde_json::to_string(&provenance).unwrap_or_else(|_| "{}".to_string()),
    ))
}

fn prepare_client_reported(mut input: ClientReportedInput) -> Result<String, (StatusCode, String)> {
    if input.argv.len() > 128 || input.argv.iter().any(|argument| argument.len() > 4096) {
        return Err((
            StatusCode::BAD_REQUEST,
            "client_reported.argv exceeds its limit".to_string(),
        ));
    }
    for (name, value, max) in [
        ("cli_version", input.cli_version.as_str(), 128),
        ("os", input.os.as_str(), 128),
        ("arch", input.arch.as_str(), 128),
        ("hostname", input.hostname.as_str(), 256),
    ] {
        validate_text(name, value, max)?;
    }

    let private_ips = collect_private_ips(&input.private_ips);
    input.private_ips = private_ips;
    let private_ips_collected = !input.private_ips.is_empty();
    let mut client_reported_value = serde_json::json!({
        "provenance": "client_reported",
        "argv": input.argv,
        "cli_version": input.cli_version,
        "os": input.os,
        "arch": input.arch,
        "hostname": input.hostname,
        "private_ips": input.private_ips,
        "private_ips_collected": private_ips_collected,
    });
    let argv_redactions = redact_client_argv(&mut client_reported_value);
    if let Some(fields) = client_reported_value.as_object_mut() {
        fields.insert(
            "redaction_count".to_string(),
            serde_json::Value::from(argv_redactions),
        );
    }
    let (client_reported, _, _) = bounded_json(client_reported_value, MAX_JSON_METADATA_BYTES)?;
    Ok(client_reported)
}

fn map_query(
    tenant_id: &str,
    query: &AccessEventQuery,
    export: bool,
) -> Result<KubernetesAccessFilter, (StatusCode, String)> {
    let response_budget = if export {
        MAX_EXPORT_RESPONSE_BYTES
    } else {
        MAX_LIST_RESPONSE_BYTES
    };
    let estimated_row_bytes = max_result_bytes()
        .saturating_add(4 * MAX_JSON_METADATA_BYTES)
        .saturating_add(4096);
    let max_rows = (response_budget / estimated_row_bytes).max(1) as u64;
    let limit = query
        .limit
        .unwrap_or(max_rows.min(if export { 1000 } else { 100 }))
        .clamp(1, max_rows);
    let values = [
        ("from", query.from.as_deref().unwrap_or_default(), 64),
        ("to", query.to.as_deref().unwrap_or_default(), 64),
        ("actor", query.actor.as_deref().unwrap_or_default(), 256),
        ("cluster", query.cluster.as_deref().unwrap_or_default(), 256),
        (
            "namespace",
            query.namespace.as_deref().unwrap_or_default(),
            253,
        ),
        ("verb", query.verb.as_deref().unwrap_or_default(), 64),
        (
            "resource",
            query.resource.as_deref().unwrap_or_default(),
            128,
        ),
        (
            "source_kind",
            query.source_kind.as_deref().unwrap_or_default(),
            64,
        ),
        (
            "recording_state",
            query.recording_state.as_deref().unwrap_or_default(),
            64,
        ),
        ("q", query.q.as_deref().unwrap_or_default(), 256),
    ];
    for (name, value, max) in values {
        validate_text(name, value, max)?;
    }
    let (status_min, status_max) = match query.status.as_deref().unwrap_or_default() {
        "" => (0, 0),
        "2xx" => (200, 299),
        "4xx" => (400, 499),
        "5xx" => (500, 599),
        value => {
            let exact = value.parse::<u16>().map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "status must be an HTTP code, 2xx, 4xx, or 5xx".to_string(),
                )
            })?;
            (exact, exact)
        }
    };
    let from = normalize_filter_timestamp("from", query.from.as_deref().unwrap_or_default())?;
    let to = normalize_filter_timestamp("to", query.to.as_deref().unwrap_or_default())?;
    Ok(KubernetesAccessFilter {
        tenant_id: tenant_id.to_string(),
        from,
        to,
        actor: query.actor.clone().unwrap_or_default(),
        cluster: query.cluster.clone().unwrap_or_default(),
        namespace: query.namespace.clone().unwrap_or_default(),
        verb: query.verb.clone().unwrap_or_default(),
        resource: query.resource.clone().unwrap_or_default(),
        status_min,
        status_max,
        source_kind: query.source_kind.clone().unwrap_or_default(),
        recording_state: query.recording_state.clone().unwrap_or_default(),
        q: query.q.clone().unwrap_or_default(),
        limit,
        offset: query.offset.unwrap_or(0),
    })
}

fn access_audit_event(
    action: &str,
    actor_type: &str,
    actor_id: &str,
    actor_name: &str,
    tenant_id: &str,
    resource_id: &str,
    headers: &HeaderMap,
) -> crate::audit::AuditEvent {
    crate::audit::AuditEvent::new(action, actor_type)
        .actor(actor_id, actor_name)
        .tenant(tenant_id)
        .resource("kubernetes_access", resource_id)
        .context(crate::audit::actor_context_from_headers(headers))
}

async fn audit_access_denial(
    state: &AppState,
    headers: &HeaderMap,
    identity: &RequestIdentity,
    action: &str,
    resource_id: &str,
) {
    state
        .audit
        .log(
            access_audit_event(
                action,
                &identity.actor_type,
                &identity.actor_id,
                &identity.actor_name,
                &identity.tenant_id,
                resource_id,
                headers,
            )
            .outcome("failure"),
        )
        .await;
}

async fn audit_login_approval_denial(
    state: &AppState,
    headers: &HeaderMap,
    caller: &(String, String, String, String, String),
    cluster_id: &str,
    reason: &str,
) {
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_access.login_approve", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("kubernetes_cluster", cluster_id)
                .outcome("failure")
                .changes(serde_json::json!({ "reason": reason }).to_string())
                .context(crate::audit::actor_context_from_headers(headers)),
        )
        .await;
}

fn views_within_budget(
    rows: Vec<KubernetesAccessEvent>,
    max_bytes: usize,
) -> Vec<KubernetesAccessEventView> {
    let mut used = 0_usize;
    let mut views = Vec::new();
    for row in rows {
        let view = KubernetesAccessEventView::from(row);
        let bytes = serde_json::to_vec(&view)
            .map(|encoded| encoded.len())
            .unwrap_or(max_bytes);
        if !views.is_empty() && used.saturating_add(bytes) > max_bytes {
            break;
        }
        used = used.saturating_add(bytes);
        views.push(view);
    }
    views
}

fn shell_argument(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_@%+=:,./-".contains(&byte))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn shell_command(parts: impl IntoIterator<Item = String>) -> String {
    parts
        .into_iter()
        .filter(|part| !part.is_empty())
        .map(|part| shell_argument(&part))
        .collect::<Vec<_>>()
        .join(" ")
}

fn reconstructed_kubectl_command(event: &KubernetesAccessEventView) -> String {
    let verb = event.verb.to_ascii_lowercase();
    let subresource = event.subresource.to_ascii_lowercase();
    let mut namespace = Vec::new();
    if !event.namespace.is_empty() {
        namespace.extend(["-n".to_string(), event.namespace.clone()]);
    }

    if matches!(subresource.as_str(), "exec" | "attach") {
        let mut parts = vec!["kubectl".to_string(), subresource];
        parts.extend(namespace);
        if !event.name.is_empty() {
            parts.push(event.name.clone());
        }
        return shell_command(parts);
    }
    if matches!(subresource.as_str(), "log" | "logs") {
        let mut parts = vec!["kubectl".to_string(), "logs".to_string()];
        parts.extend(namespace);
        if !event.name.is_empty() {
            parts.push(event.name.clone());
        }
        return shell_command(parts);
    }
    if subresource == "portforward" {
        let mut parts = vec!["kubectl".to_string(), "port-forward".to_string()];
        parts.extend(namespace);
        if !event.name.is_empty() {
            parts.push(format!("{}/{}", event.resource, event.name));
        }
        return shell_command(parts);
    }
    if event.resource.is_empty() {
        return "kubectl api-resources".to_string();
    }

    let action = match verb.as_str() {
        "get" | "list" | "watch" => "get".to_string(),
        "deletecollection" => "delete".to_string(),
        "update" => "replace".to_string(),
        other if !other.is_empty() => other.to_string(),
        _ => event.http_method.to_ascii_lowercase(),
    };
    let mut parts = vec!["kubectl".to_string(), action, event.resource.clone()];
    if !event.name.is_empty() && verb != "deletecollection" {
        parts.push(event.name.clone());
    }
    parts.extend(namespace);
    if verb == "watch" {
        parts.push("--watch".to_string());
    } else if verb == "deletecollection" {
        parts.push("--all".to_string());
    }
    shell_command(parts)
}

impl From<KubernetesAccessEventView> for AgentAccessEventView {
    fn from(event: KubernetesAccessEventView) -> Self {
        let likely_kubectl_command = reconstructed_kubectl_command(&event);
        Self {
            id: event.id,
            created_at: event.created_at,
            actor_name: event.actor_name,
            actor_type: event.actor_type,
            kube_username: event.kube_username,
            cluster_id: event.cluster_id,
            namespace: event.namespace,
            verb: event.verb,
            resource: event.resource,
            subresource: event.subresource,
            name: event.name,
            status_code: event.status_code,
            duration_ms: event.duration_ms,
            likely_kubectl_command,
            source_kind: event.source_kind,
            session_id: event.session_id,
            recording_state: event.recording_state,
        }
    }
}

fn kubernetes_group_for_rush_group(group_id: &str) -> String {
    format!("rush:group:{group_id}")
}

fn kubernetes_authorization_groups(
    tenant_id: &str,
    role: &str,
    rbac_group_ids: &[String],
) -> Result<Vec<String>, (StatusCode, String)> {
    let mut groups = vec![
        "rush:authenticated".to_string(),
        format!("rush:tenant:{tenant_id}:role:{role}"),
    ];
    groups.extend(
        rbac_group_ids
            .iter()
            .map(|group_id| kubernetes_group_for_rush_group(group_id)),
    );
    groups.sort();
    groups.dedup();
    if groups.len() > MAX_KUBERNETES_AUTHORIZATION_GROUPS {
        return Err((
            StatusCode::FORBIDDEN,
            "Kubernetes access matches too many Rush groups".to_string(),
        ));
    }
    Ok(groups)
}

fn valid_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn valid_dns_subdomain(value: &str) -> bool {
    !value.is_empty() && value.len() <= 253 && value.split('.').all(valid_dns_label)
}

fn normalize_rbac_values(values: Vec<String>) -> Vec<String> {
    let mut normalized = values
        .into_iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn valid_rbac_resource(value: &str) -> bool {
    value == "*"
        || value.len() <= 128
            && value
                .split('/')
                .all(|part| part == "*" || valid_dns_label(part))
}

fn validate_kubernetes_rbac_grant(
    mut input: KubernetesRbacGrantInput,
) -> Result<KubernetesRbacGrantInput, (StatusCode, String)> {
    input.group_id = input.group_id.trim().to_string();
    input.cluster_id = input.cluster_id.trim().to_string();
    input.cluster_match = input.cluster_match.trim().to_ascii_lowercase();
    input.cluster_pattern = input.cluster_pattern.trim().to_string();
    input.name = input.name.trim().to_string();
    input.role_kind = input.role_kind.trim().to_ascii_lowercase();
    input.role_name = input.role_name.trim().to_ascii_lowercase();
    input.scope = input.scope.trim().to_ascii_lowercase();

    require_text("group_id", &input.group_id, 128)?;
    if !input
        .group_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err((StatusCode::BAD_REQUEST, "invalid Rush group id".to_string()));
    }
    if !matches!(input.cluster_match.as_str(), "single" | "all" | "pattern") {
        return Err((
            StatusCode::BAD_REQUEST,
            "cluster_match must be single, all, or pattern".to_string(),
        ));
    }
    match input.cluster_match.as_str() {
        "single" => {
            require_text("cluster_id", &input.cluster_id, 256)?;
            if input.cluster_id.contains('/')
                || input
                    .cluster_id
                    .chars()
                    .any(|character| character.is_control())
            {
                return Err((StatusCode::BAD_REQUEST, "invalid cluster id".to_string()));
            }
            input.cluster_pattern.clear();
        }
        "all" => {
            input.cluster_id.clear();
            input.cluster_pattern.clear();
        }
        "pattern" => {
            require_text("cluster_pattern", &input.cluster_pattern, 256)?;
            if !input.cluster_pattern.contains(['*', '?']) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "cluster pattern must contain * or ?".to_string(),
                ));
            }
            if input.cluster_pattern.contains('/')
                || input
                    .cluster_pattern
                    .chars()
                    .any(|character| character.is_control())
            {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "invalid cluster pattern".to_string(),
                ));
            }
            input.cluster_id.clear();
        }
        _ => unreachable!(),
    }
    require_text("name", &input.name, 100)?;
    if !matches!(
        input.role_kind.as_str(),
        "view" | "edit" | "admin" | "existing" | "custom"
    ) {
        return Err((
            StatusCode::BAD_REQUEST,
            "role_kind must be view, edit, admin, existing, or custom".to_string(),
        ));
    }
    if !matches!(input.scope.as_str(), "cluster" | "namespaces") {
        return Err((
            StatusCode::BAD_REQUEST,
            "scope must be cluster or namespaces".to_string(),
        ));
    }

    if matches!(input.role_kind.as_str(), "view" | "edit" | "admin") {
        input.role_name.clone_from(&input.role_kind);
        input.rules.clear();
    } else if input.role_kind == "existing" {
        if !valid_dns_subdomain(&input.role_name) {
            return Err((
                StatusCode::BAD_REQUEST,
                "existing ClusterRole name is invalid".to_string(),
            ));
        }
        input.rules.clear();
    } else {
        input.role_name.clear();
        if input.rules.is_empty() || input.rules.len() > MAX_KUBERNETES_RBAC_RULES {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("custom roles require 1 to {MAX_KUBERNETES_RBAC_RULES} rules"),
            ));
        }
        for rule in &mut input.rules {
            rule.api_groups = normalize_rbac_values(std::mem::take(&mut rule.api_groups));
            if rule.api_groups.is_empty() {
                rule.api_groups.push(String::new());
            }
            rule.resources = normalize_rbac_values(std::mem::take(&mut rule.resources));
            rule.verbs = normalize_rbac_values(std::mem::take(&mut rule.verbs));
            if rule.api_groups.len() > 16
                || rule.resources.is_empty()
                || rule.resources.len() > 32
                || rule.verbs.is_empty()
                || rule.verbs.len() > 16
            {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "RBAC rule exceeds its limit".to_string(),
                ));
            }
            if rule.api_groups.iter().any(|group| {
                group == "*"
                    || (!group.is_empty() && !valid_dns_subdomain(group))
                    || group == "rbac.authorization.k8s.io"
            }) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "custom rules cannot grant Kubernetes RBAC administration".to_string(),
                ));
            }
            if rule
                .resources
                .iter()
                .any(|resource| !valid_rbac_resource(resource))
            {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "invalid Kubernetes resource".to_string(),
                ));
            }
            if rule.verbs.iter().any(|verb| {
                !matches!(
                    verb.as_str(),
                    "get"
                        | "list"
                        | "watch"
                        | "create"
                        | "update"
                        | "patch"
                        | "delete"
                        | "deletecollection"
                        | "*"
                )
            }) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "invalid Kubernetes verb".to_string(),
                ));
            }
        }
    }

    input.namespaces = normalize_rbac_values(input.namespaces);
    if input.scope == "cluster" {
        input.namespaces.clear();
    } else if input.namespaces.is_empty()
        || input.namespaces.len() > 64
        || input
            .namespaces
            .iter()
            .any(|namespace| !valid_dns_label(namespace))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "namespace roles require 1 to 64 valid namespaces".to_string(),
        ));
    }
    Ok(input)
}

fn kubernetes_rbac_grant_view(
    row: crate::clickhouse_config::KubernetesRbacGrantRow,
) -> KubernetesRbacGrantView {
    KubernetesRbacGrantView {
        kubernetes_group: kubernetes_group_for_rush_group(&row.group_id),
        id: row.id,
        tenant_id: row.tenant_id,
        group_id: row.group_id,
        cluster_id: row.cluster_id,
        cluster_match: row.cluster_match,
        cluster_pattern: row.cluster_pattern,
        name: row.name,
        role_kind: row.role_kind,
        role_name: row.role_name,
        scope: row.scope,
        namespaces: serde_json::from_str(&row.namespaces).unwrap_or_default(),
        rules: serde_json::from_str(&row.rules).unwrap_or_default(),
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

async fn kubernetes_logging_settings_response(
    state: &AppState,
    tenant_id: &str,
) -> Result<KubernetesLoggingSettingsResponse, (StatusCode, String)> {
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let active_clients = state
        .config_db
        .list_active_kubernetes_login_requests(tenant_id, &now)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to list active Kubernetes clients");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Kubernetes clients are unavailable".to_string(),
            )
        })?
        .iter()
        .map(client_session_view)
        .collect();
    let rbac_grants = state
        .config_db
        .list_kubernetes_rbac_grants(tenant_id, None)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to list Kubernetes RBAC grants");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Kubernetes roles are unavailable".to_string(),
            )
        })?
        .into_iter()
        .map(kubernetes_rbac_grant_view)
        .collect();
    let configured_binding = gateway_binding()
        .ok()
        .filter(|binding| binding.tenant_ids.iter().any(|tenant| tenant == tenant_id));
    let mut gateways = state
        .config_db
        .list_kubernetes_gateway_activity(tenant_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to list Kubernetes gateway activity");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Kubernetes gateways are unavailable".to_string(),
            )
        })?
        .into_iter()
        .map(|row| KubernetesGatewayClusterView {
            configured: configured_binding.as_ref().is_some_and(|binding| {
                binding.gateway_id == row.gateway_id && binding.cluster_id == row.cluster_id
            }),
            gateway_id: row.gateway_id,
            cluster_id: row.cluster_id,
            last_activity: row.last_activity,
            recorded_requests: row.recorded_requests,
        })
        .collect::<Vec<_>>();
    if let Some(binding) = configured_binding {
        if !gateways.iter().any(|gateway| {
            gateway.gateway_id == binding.gateway_id && gateway.cluster_id == binding.cluster_id
        }) {
            gateways.push(KubernetesGatewayClusterView {
                gateway_id: binding.gateway_id,
                cluster_id: binding.cluster_id,
                configured: true,
                last_activity: String::new(),
                recorded_requests: 0,
            });
        }
    }
    gateways.sort_by(|left, right| {
        right
            .configured
            .cmp(&left.configured)
            .then_with(|| right.last_activity.cmp(&left.last_activity))
            .then_with(|| left.gateway_id.cmp(&right.gateway_id))
    });
    let mut available_clusters = gateways
        .iter()
        .map(|gateway| gateway.cluster_id.clone())
        .collect::<Vec<_>>();
    available_clusters.sort();
    available_clusters.dedup();
    Ok(KubernetesLoggingSettingsResponse {
        max_session_seconds: kubernetes_session_seconds(state).await?,
        min_session_seconds: MIN_KUBERNETES_SESSION_SECONDS,
        max_allowed_session_seconds: MAX_KUBERNETES_SESSION_SECONDS,
        active_clients,
        rbac_grants,
        gateways,
        available_clusters,
    })
}

pub async fn get_kubernetes_logging_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = require_admin(&state, &headers).await?;
    let response = kubernetes_logging_settings_response(&state, &caller.3).await?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_access.client_session_list", "user")
                .actor(caller.0, caller.1)
                .tenant(caller.3.clone())
                .resource("kubernetes_client_sessions", caller.3)
                .outcome("success")
                .changes(
                    serde_json::json!({
                        "active_client_count": response.active_clients.len(),
                        "gateway_count": response.gateways.len(),
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(Json(response))
}

pub async fn set_kubernetes_logging_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<KubernetesLoggingSettingsInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = require_admin(&state, &headers).await?;
    if !(MIN_KUBERNETES_SESSION_SECONDS..=MAX_KUBERNETES_SESSION_SECONDS)
        .contains(&input.max_session_seconds)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "max_session_seconds must be between {MIN_KUBERNETES_SESSION_SECONDS} and {MAX_KUBERNETES_SESSION_SECONDS}"
            ),
        ));
    }
    let before = kubernetes_session_seconds(&state).await?;
    state
        .config_db
        .set_setting(
            KUBERNETES_SESSION_SECONDS_SETTING,
            &input.max_session_seconds.to_string(),
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to save Kubernetes session limit");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to save Kubernetes logging settings".to_string(),
            )
        })?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("settings.update", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(caller.3.clone())
                .resource("setting", KUBERNETES_SESSION_SECONDS_SETTING)
                .outcome("success")
                .changes(
                    serde_json::json!({
                        "key": KUBERNETES_SESSION_SECONDS_SETTING,
                        "before": before,
                        "after": input.max_session_seconds,
                    })
                    .to_string(),
                )
                .description("Kubernetes client session limit updated")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(Json(
        kubernetes_logging_settings_response(&state, &caller.3).await?,
    ))
}

async fn validate_rbac_grant_context(
    state: &AppState,
    tenant_id: &str,
    input: KubernetesRbacGrantInput,
) -> Result<KubernetesRbacGrantInput, (StatusCode, String)> {
    let input = validate_kubernetes_rbac_grant(input)?;
    let group = state
        .config_db
        .get_group(&input.group_id)
        .await
        .map_err(internal_error)?;
    let Some(group) = group else {
        return Err((StatusCode::BAD_REQUEST, "Rush group not found".to_string()));
    };
    if !group.7.iter().any(|group_tenant| group_tenant == tenant_id) {
        return Err((
            StatusCode::FORBIDDEN,
            "Rush group is not assigned to this tenant".to_string(),
        ));
    }
    let policy = std::env::var("KUBERNETES_ACCESS_TENANT_CLUSTERS").map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Kubernetes access policy is unavailable".to_string(),
        )
    })?;
    let authorized = if input.cluster_match == "single" {
        tenant_cluster_allowed(&policy, tenant_id, &input.cluster_id)?
    } else {
        parse_tenant_cluster_policy(&policy)?
            .get(tenant_id)
            .is_some_and(|clusters| !clusters.is_empty())
    };
    if !authorized {
        return Err((
            StatusCode::FORBIDDEN,
            "tenant has no authorized Kubernetes clusters".to_string(),
        ));
    }
    Ok(input)
}

pub async fn create_kubernetes_rbac_grant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<KubernetesRbacGrantInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = require_admin(&state, &headers).await?;
    let input = validate_rbac_grant_context(&state, &caller.3, input).await?;
    if state
        .config_db
        .list_kubernetes_rbac_grants(&caller.3, None)
        .await
        .map_err(internal_error)?
        .len()
        >= MAX_KUBERNETES_RBAC_GRANTS
    {
        return Err((
            StatusCode::CONFLICT,
            format!("a tenant may define at most {MAX_KUBERNETES_RBAC_GRANTS} Kubernetes roles"),
        ));
    }
    let namespaces =
        serde_json::to_string(&input.namespaces).map_err(|error| internal_error(error.into()))?;
    let rules =
        serde_json::to_string(&input.rules).map_err(|error| internal_error(error.into()))?;
    let view = kubernetes_rbac_grant_view(
        state
            .config_db
            .create_kubernetes_rbac_grant(
                &caller.3,
                &input.group_id,
                &input.cluster_id,
                &input.cluster_match,
                &input.cluster_pattern,
                &input.name,
                &input.role_kind,
                &input.role_name,
                &input.scope,
                &namespaces,
                &rules,
            )
            .await
            .map_err(internal_error)?,
    );
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_rbac.grant_create", "user")
                .actor(caller.0, caller.1)
                .tenant(caller.3)
                .resource("kubernetes_rbac_grant", view.id.clone())
                .outcome("success")
                .changes(serde_json::to_string(&view).unwrap_or_else(|_| "{}".to_string()))
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok((StatusCode::CREATED, Json(view)))
}

pub async fn update_kubernetes_rbac_grant(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<KubernetesRbacGrantInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = require_admin(&state, &headers).await?;
    let before = state
        .config_db
        .get_kubernetes_rbac_grant(&caller.3, &id)
        .await
        .map_err(internal_error)?
        .map(kubernetes_rbac_grant_view)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "Kubernetes role not found".to_string(),
            )
        })?;
    let input = validate_rbac_grant_context(&state, &caller.3, input).await?;
    let namespaces =
        serde_json::to_string(&input.namespaces).map_err(|error| internal_error(error.into()))?;
    let rules =
        serde_json::to_string(&input.rules).map_err(|error| internal_error(error.into()))?;
    let view = state
        .config_db
        .update_kubernetes_rbac_grant(
            &caller.3,
            &id,
            &input.group_id,
            &input.cluster_id,
            &input.cluster_match,
            &input.cluster_pattern,
            &input.name,
            &input.role_kind,
            &input.role_name,
            &input.scope,
            &namespaces,
            &rules,
        )
        .await
        .map_err(internal_error)?
        .map(kubernetes_rbac_grant_view)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "Kubernetes role not found".to_string(),
            )
        })?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_rbac.grant_update", "user")
                .actor(caller.0, caller.1)
                .tenant(caller.3)
                .resource("kubernetes_rbac_grant", id)
                .outcome("success")
                .changes(serde_json::json!({ "before": before, "after": view }).to_string())
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(Json(view))
}

pub async fn delete_kubernetes_rbac_grant(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = require_admin(&state, &headers).await?;
    let deleted = state
        .config_db
        .delete_kubernetes_rbac_grant(&caller.3, &id)
        .await
        .map_err(internal_error)?
        .map(kubernetes_rbac_grant_view)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "Kubernetes role not found".to_string(),
            )
        })?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_rbac.grant_delete", "user")
                .actor(caller.0, caller.1)
                .tenant(caller.3)
                .resource("kubernetes_rbac_grant", id)
                .outcome("success")
                .changes(serde_json::to_string(&deleted).unwrap_or_else(|_| "{}".to_string()))
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_gateway_kubernetes_rbac_grants(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<GatewayRbacQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    require_internal(&headers)?;
    require_text("cluster_id", &query.cluster_id, 256)?;
    let binding = gateway_binding()?;
    validate_gateway_instance(&binding, &headers, &query.cluster_id)?;
    let grants = state
        .config_db
        .list_gateway_kubernetes_rbac_grants(&query.cluster_id, &binding.tenant_ids)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to list gateway Kubernetes RBAC grants");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes roles are unavailable".to_string(),
            )
        })?
        .into_iter()
        .map(kubernetes_rbac_grant_view)
        .collect();
    Ok(Json(GatewayRbacResponse {
        cluster_id: query.cluster_id,
        grants,
    }))
}

pub async fn record_gateway_kubernetes_rbac_reconcile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<GatewayRbacReconcileInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    require_internal(&headers)?;
    require_text("cluster_id", &input.cluster_id, 256)?;
    if input.revision.len() != 32
        || !input.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
        || input.grant_count > MAX_KUBERNETES_RBAC_GRANTS
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid Kubernetes RBAC reconciliation result".to_string(),
        ));
    }
    let binding = gateway_binding()?;
    validate_gateway_instance(&binding, &headers, &input.cluster_id)?;
    for tenant_id in &binding.tenant_ids {
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("kubernetes_rbac.reconcile", "system")
                    .actor(binding.gateway_id.clone(), "Kubernetes access gateway")
                    .tenant(tenant_id.clone())
                    .resource("kubernetes_cluster", input.cluster_id.clone())
                    .outcome("success")
                    .changes(
                        serde_json::json!({
                            "revision": input.revision,
                            "grant_count": input.grant_count,
                        })
                        .to_string(),
                    )
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn revoke_kubernetes_client(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = require_admin(&state, &headers).await?;
    if !session_id.starts_with("kcs_")
        || session_id.len() != 28
        || !session_id[4..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid client session id".to_string(),
        ));
    }
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let active = state
        .config_db
        .list_active_kubernetes_login_requests(&caller.3, &now)
        .await
        .map_err(internal_error)?;
    let request = active
        .iter()
        .find(|request| client_session_id(&request.device_code_hash) == session_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "active client not found".to_string()))?;
    state
        .config_db
        .revoke_kubernetes_login_request(request, &now)
        .await
        .map_err(internal_error)?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_access.credential_revoke", "user")
                .actor(caller.0, caller.1)
                .tenant(caller.3)
                .resource("kubernetes_client_session", session_id.clone())
                .outcome("success")
                .changes(
                    serde_json::json!({
                        "session_id": session_id,
                        "username": request.username,
                        "cluster_id": request.cluster_id,
                        "bulk": false,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn revoke_all_kubernetes_clients(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = require_admin(&state, &headers).await?;
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let active = state
        .config_db
        .list_active_kubernetes_login_requests(&caller.3, &now)
        .await
        .map_err(internal_error)?;
    for request in &active {
        state
            .config_db
            .revoke_kubernetes_login_request(request, &now)
            .await
            .map_err(internal_error)?;
        let session_id = client_session_id(&request.device_code_hash);
        state
            .audit
            .log(
                crate::audit::AuditEvent::new("kubernetes_access.credential_revoke", "user")
                    .actor(caller.0.clone(), caller.1.clone())
                    .tenant(caller.3.clone())
                    .resource("kubernetes_client_session", session_id.clone())
                    .outcome("success")
                    .changes(
                        serde_json::json!({
                            "session_id": session_id,
                            "username": request.username,
                            "cluster_id": request.cluster_id,
                            "bulk": true,
                        })
                        .to_string(),
                    )
                    .context(crate::audit::actor_context_from_headers(&headers)),
            )
            .await;
    }
    Ok(Json(serde_json::json!({ "revoked": active.len() })))
}

pub async fn start_kubernetes_login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<KubernetesLoginStartInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    require_text("cluster_id", &input.cluster_id, 256)?;
    let binding = gateway_binding()?;
    if binding.cluster_id != input.cluster_id {
        return Err((StatusCode::NOT_FOUND, "cluster not found".to_string()));
    }

    let device_code = temporary_device_credential();
    let device_code_hash = temporary_credential_hash(&device_code);
    let user_code = temporary_user_code();
    let now = chrono::Utc::now();
    let expires_in = 600_i64;
    let created_at = now.format("%Y-%m-%d %H:%M:%S").to_string();
    let expires_at = (now + chrono::Duration::seconds(expires_in))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    state
        .config_db
        .create_kubernetes_login_request(
            &device_code_hash,
            &user_code,
            &input.cluster_id,
            &created_at,
            &expires_at,
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to start Kubernetes browser login");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Kubernetes login could not be started".to_string(),
            )
        })?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_access.login_start", "anonymous")
                .tenant("default")
                .resource("kubernetes_cluster", input.cluster_id.clone())
                .outcome("success")
                .changes(
                    serde_json::json!({
                        "approval_expires_in_seconds": expires_in,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(KubernetesLoginStartResponse {
        device_code,
        user_code,
        expires_in,
        interval: 2,
    }))
}

pub async fn approve_kubernetes_login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<KubernetesLoginApproveInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = require_infrastructure_read(&state, &headers).await?;
    let user_code = input.user_code.trim().to_ascii_uppercase();
    if user_code.len() != 16 || !user_code.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        audit_login_approval_denial(&state, &headers, &caller, "unknown", "invalid_code").await;
        return Err((StatusCode::BAD_REQUEST, "invalid login code".to_string()));
    }

    let request = state
        .config_db
        .get_kubernetes_login_by_user_code(&user_code)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to read Kubernetes browser login");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Kubernetes login could not be approved".to_string(),
            )
        })?;
    let Some(request) = request else {
        audit_login_approval_denial(&state, &headers, &caller, "unknown", "not_found").await;
        return Err((StatusCode::NOT_FOUND, "login request not found".to_string()));
    };
    let now = chrono::Utc::now();
    let now_text = now.format("%Y-%m-%d %H:%M:%S").to_string();
    if request.state != "pending" || request.expires_at <= now_text {
        audit_login_approval_denial(
            &state,
            &headers,
            &caller,
            &request.cluster_id,
            "expired_or_used",
        )
        .await;
        return Err((StatusCode::GONE, "login request expired".to_string()));
    }

    let binding = gateway_binding()?;
    if binding.cluster_id != request.cluster_id
        || !binding.tenant_ids.iter().any(|tenant| tenant == &caller.3)
    {
        audit_login_approval_denial(
            &state,
            &headers,
            &caller,
            &request.cluster_id,
            "gateway_binding_denied",
        )
        .await;
        return Err((
            StatusCode::FORBIDDEN,
            "cluster is not authorized for this tenant".to_string(),
        ));
    }
    let policy = std::env::var("KUBERNETES_ACCESS_TENANT_CLUSTERS").map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Kubernetes access policy is unavailable".to_string(),
        )
    })?;
    if !tenant_cluster_allowed(&policy, &caller.3, &request.cluster_id)? {
        audit_login_approval_denial(
            &state,
            &headers,
            &caller,
            &request.cluster_id,
            "tenant_policy_denied",
        )
        .await;
        return Err((
            StatusCode::FORBIDDEN,
            "cluster is not authorized for this tenant".to_string(),
        ));
    }

    let approval_claim_expires =
        chrono::NaiveDateTime::parse_from_str(&request.expires_at, "%Y-%m-%d %H:%M:%S")
            .map_err(|error| {
                tracing::error!(%error, "Kubernetes login request has an invalid expiry");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Kubernetes login could not be approved".to_string(),
                )
            })?
            .and_utc()
            .timestamp();
    let approval_claim = format!("kubernetes-login:{}", request.device_code_hash);
    if !state
        .config_db
        .claim_sso_key_once(&approval_claim, approval_claim_expires)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to claim Kubernetes login approval");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes login could not be approved".to_string(),
            )
        })?
    {
        audit_login_approval_denial(
            &state,
            &headers,
            &caller,
            &request.cluster_id,
            "already_approved",
        )
        .await;
        return Err((
            StatusCode::GONE,
            "login request was already approved".to_string(),
        ));
    }

    let credential_expires_at = (now
        + chrono::Duration::seconds(kubernetes_session_seconds(&state).await?))
    .format("%Y-%m-%d %H:%M:%S")
    .to_string();
    let approved = state
        .config_db
        .approve_kubernetes_login_request(
            &request,
            &caller.0,
            &caller.1,
            &caller.3,
            &caller.4,
            &now_text,
            &credential_expires_at,
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to approve Kubernetes browser login");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Kubernetes login could not be approved".to_string(),
            )
        })?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_access.login_approve", "user")
                .actor(caller.0, caller.1)
                .tenant(caller.3)
                .resource("kubernetes_cluster", approved.cluster_id.clone())
                .outcome("success")
                .changes(
                    serde_json::json!({
                        "credential_expires_at": approved.credential_expires_at,
                    })
                    .to_string(),
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(KubernetesLoginApproveResponse {
        status: "approved",
        cluster_id: approved.cluster_id,
        credential_expires_at: approved.credential_expires_at,
    }))
}

pub async fn get_kubernetes_login_details(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<KubernetesLoginApproveInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = require_infrastructure_read(&state, &headers).await?;
    let user_code = input.user_code.trim().to_ascii_uppercase();
    if user_code.len() != 16 || !user_code.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err((StatusCode::BAD_REQUEST, "invalid login code".to_string()));
    }
    let request = state
        .config_db
        .get_kubernetes_login_by_user_code(&user_code)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to read Kubernetes browser login details");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Kubernetes login details are unavailable".to_string(),
            )
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "login request not found".to_string()))?;
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    if (request.state == "pending" && request.expires_at <= now)
        || (request.state == "approved" && request.credential_expires_at <= now)
    {
        return Err((StatusCode::GONE, "login request expired".to_string()));
    }
    let binding = gateway_binding()?;
    let policy = std::env::var("KUBERNETES_ACCESS_TENANT_CLUSTERS").map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Kubernetes access policy is unavailable".to_string(),
        )
    })?;
    if binding.cluster_id != request.cluster_id
        || !binding.tenant_ids.iter().any(|tenant| tenant == &caller.3)
        || !tenant_cluster_allowed(&policy, &caller.3, &request.cluster_id)?
    {
        return Err((
            StatusCode::FORBIDDEN,
            "cluster is not authorized for this tenant".to_string(),
        ));
    }
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_access.login_preview", "user")
                .actor(caller.0, caller.1)
                .tenant(caller.3)
                .resource("kubernetes_cluster", request.cluster_id.clone())
                .outcome("success")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    if state
        .config_db
        .is_kubernetes_login_revoked(&request.device_code_hash)
        .await
        .map_err(internal_error)?
    {
        return Err((StatusCode::GONE, "login credential was revoked".to_string()));
    }
    Ok(Json(KubernetesLoginDetailsResponse {
        status: request.state,
        cluster_id: request.cluster_id,
        approval_expires_at: request.expires_at,
        credential_ttl_seconds: kubernetes_session_seconds(&state).await?,
    }))
}

pub async fn poll_kubernetes_login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<KubernetesLoginTokenInput>,
) -> Result<(StatusCode, Json<KubernetesLoginTokenResponse>), (StatusCode, String)> {
    require_enabled()?;
    if !input.device_code.starts_with("rkt1_") || input.device_code.len() != 69 {
        return Err((
            StatusCode::UNAUTHORIZED,
            "invalid login credential".to_string(),
        ));
    }
    let request = state
        .config_db
        .get_kubernetes_login_by_device_hash(&temporary_credential_hash(&input.device_code))
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to poll Kubernetes browser login");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Kubernetes login is temporarily unavailable".to_string(),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "invalid login credential".to_string(),
            )
        })?;
    if state
        .config_db
        .is_kubernetes_login_revoked(&request.device_code_hash)
        .await
        .map_err(internal_error)?
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            "login credential was revoked".to_string(),
        ));
    }
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    if request.state == "pending" && request.expires_at > now {
        return Ok((
            StatusCode::ACCEPTED,
            Json(KubernetesLoginTokenResponse {
                status: "pending",
                access_token: None,
                expires_at: None,
                interval: 2,
            }),
        ));
    }
    if request.state != "approved" || request.credential_expires_at <= now {
        return Err((
            StatusCode::UNAUTHORIZED,
            "login credential expired".to_string(),
        ));
    }

    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_access.credential_issue", "user")
                .actor(request.user_id.clone(), request.username.clone())
                .tenant(request.tenant_id.clone())
                .resource("kubernetes_cluster", request.cluster_id)
                .outcome("success")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok((
        StatusCode::OK,
        Json(KubernetesLoginTokenResponse {
            status: "approved",
            access_token: Some(input.device_code),
            expires_at: Some(request.credential_expires_at),
            interval: 2,
        }),
    ))
}

pub async fn authorize_gateway_request(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    headers: HeaderMap,
    Json(input): Json<GatewayAuthorizeInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    if let Err(error) = require_internal(&headers) {
        audit_access_denial(
            &state,
            &headers,
            &identity,
            "kubernetes_access.authorize_denied",
            "gateway",
        )
        .await;
        return Err(error);
    }
    require_text("cluster_id", &input.cluster_id, 256)?;
    if input.audience != "kubernetes-access-gateway" {
        audit_access_denial(
            &state,
            &headers,
            &identity,
            "kubernetes_access.authorize_denied",
            &input.cluster_id,
        )
        .await;
        return Err((StatusCode::FORBIDDEN, "invalid token audience".to_string()));
    }
    let binding = gateway_binding()?;
    if let Err(error) = validate_gateway_instance(&binding, &headers, &input.cluster_id) {
        audit_access_denial(
            &state,
            &headers,
            &identity,
            "kubernetes_access.authorize_denied",
            &input.cluster_id,
        )
        .await;
        return Err(error);
    }
    let Some(token) =
        bearer_token(&headers).filter(|value| value.starts_with("rkt1_") && value.len() == 69)
    else {
        audit_access_denial(
            &state,
            &headers,
            &identity,
            "kubernetes_access.authorize_denied",
            &input.cluster_id,
        )
        .await;
        return Err((
            StatusCode::UNAUTHORIZED,
            "temporary Rush login credential required".to_string(),
        ));
    };
    let login = state
        .config_db
        .get_kubernetes_login_by_device_hash(&temporary_credential_hash(token))
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to validate Kubernetes credential");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes credential validation is unavailable".to_string(),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "invalid Kubernetes credential".to_string(),
            )
        })?;
    if state
        .config_db
        .is_kubernetes_login_revoked(&login.device_code_hash)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to check Kubernetes credential revocation");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes credential validation is unavailable".to_string(),
            )
        })?
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Kubernetes credential was revoked".to_string(),
        ));
    }
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    if login.state != "approved"
        || login.cluster_id != input.cluster_id
        || login.credential_expires_at <= now
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            "invalid or expired Kubernetes credential".to_string(),
        ));
    }
    let caller = state
        .config_db
        .get_active_kubernetes_user(&login.user_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to resolve Kubernetes credential user");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes credential validation is unavailable".to_string(),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "Kubernetes credential user is disabled".to_string(),
            )
        })?;
    if caller.3 != login.tenant_id {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Kubernetes credential is no longer valid".to_string(),
        ));
    }
    let permissions = state
        .config_db
        .resolve_user_permissions(&caller.0)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to resolve Kubernetes credential permissions");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes credential validation is unavailable".to_string(),
            )
        })?
        .1;
    if caller.4 != "admin"
        && !permissions
            .iter()
            .any(|permission| matches!(permission.as_str(), "admin" | "infrastructure:read"))
    {
        return Err((
            StatusCode::FORBIDDEN,
            "Kubernetes access permission was revoked".to_string(),
        ));
    }
    if let Err(error) =
        validate_authorizing_gateway(&binding, &headers, &caller.3, &input.cluster_id)
    {
        return Err(error);
    }
    let policy = std::env::var("KUBERNETES_ACCESS_TENANT_CLUSTERS").map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Kubernetes access policy is unavailable".to_string(),
        )
    })?;
    if !tenant_cluster_allowed(&policy, &caller.3, &input.cluster_id)? {
        audit_access_denial(
            &state,
            &headers,
            &identity,
            "kubernetes_access.authorize_denied",
            &input.cluster_id,
        )
        .await;
        return Err((
            StatusCode::FORBIDDEN,
            "tenant is not authorized for this cluster".to_string(),
        ));
    }
    let rbac_group_ids = state
        .config_db
        .kubernetes_rbac_group_ids_for_user(&caller.3, &input.cluster_id, &caller.0)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to resolve Kubernetes RBAC groups");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes credential validation is unavailable".to_string(),
            )
        })?;
    let kube_groups = kubernetes_authorization_groups(&caller.3, &caller.4, &rbac_group_ids)?;
    let authorization = GatewayAuthorization {
        actor_user_id: caller.0,
        actor_name: caller.1.clone(),
        actor_type: "user".to_string(),
        tenant_id: caller.3.clone(),
        cluster_id: input.cluster_id.clone(),
        role: caller.4.clone(),
        kube_username: format!("rush:user:{}", caller.1),
        kube_groups,
        client_reported: serde_json::from_str(&login.client_reported)
            .unwrap_or_else(|_| serde_json::json!({})),
    };
    state
        .audit
        .log(access_audit_event(
            "kubernetes_access.gateway_authorize",
            &authorization.actor_type,
            &authorization.actor_user_id,
            &authorization.actor_name,
            &authorization.tenant_id,
            "gateway",
            &headers,
        ))
        .await;
    Ok(Json(authorization))
}

fn recorder_storage_ready(result: anyhow::Result<()>) -> Result<(), (StatusCode, String)> {
    result.map_err(|error| {
        tracing::warn!(%error, "Kubernetes access recorder storage is unavailable");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Kubernetes access recorder storage is unavailable".to_string(),
        )
    })
}

pub async fn gateway_recording_ready(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<GatewayReadyInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    require_internal(&headers)?;
    require_text("cluster_id", &input.cluster_id, 256)?;
    let binding = gateway_binding()?;
    validate_gateway_instance(&binding, &headers, &input.cluster_id)?;
    recorder_storage_ready(state.config_db.kubernetes_access_storage_ready().await)?;

    Ok(Json(serde_json::json!({
        "status": "ready",
        "gateway_id": binding.gateway_id,
        "cluster_id": binding.cluster_id,
    })))
}

pub async fn ingest_access_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<InternalAccessEventInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let secret = require_internal(&headers)?;
    let binding = gateway_binding()?;
    validate_gateway_binding(
        &binding,
        &input.gateway_id,
        &input.tenant_id,
        Some(&input.cluster_id),
    )?;
    let event = internal_event(input, &secret)?;
    state
        .config_db
        .insert_kubernetes_access_event(&event)
        .await
        .map_err(internal_error)?;
    state
        .audit
        .log(access_audit_event(
            "kubernetes_access.event_ingest",
            "system",
            &event.gateway_id,
            "Kubernetes access gateway",
            &event.tenant_id,
            &event.id,
            &headers,
        ))
        .await;
    Ok((
        StatusCode::CREATED,
        Json(KubernetesAccessEventView::from(event)),
    ))
}

pub async fn ingest_client_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<ClientAccessEventInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    require_text("cluster_id", &input.cluster_id, 256)?;
    let Some(token) =
        bearer_token(&headers).filter(|value| value.starts_with("rkt1_") && value.len() == 69)
    else {
        return Err((
            StatusCode::UNAUTHORIZED,
            "temporary Rush login credential required".to_string(),
        ));
    };
    let login = state
        .config_db
        .get_kubernetes_login_by_device_hash(&temporary_credential_hash(token))
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "invalid Kubernetes credential".to_string(),
            )
        })?;
    if state
        .config_db
        .is_kubernetes_login_revoked(&login.device_code_hash)
        .await
        .map_err(internal_error)?
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Kubernetes credential was revoked".to_string(),
        ));
    }
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    if login.state != "approved"
        || login.cluster_id != input.cluster_id
        || login.credential_expires_at <= now
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            "invalid or expired Kubernetes credential".to_string(),
        ));
    }
    let caller = state
        .config_db
        .get_active_kubernetes_user(&login.user_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "Kubernetes credential user is disabled".to_string(),
            )
        })?;
    if caller.3 != login.tenant_id {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Kubernetes credential is no longer valid".to_string(),
        ));
    }
    let permissions = state
        .config_db
        .resolve_user_permissions(&caller.0)
        .await
        .map_err(internal_error)?
        .1;
    if caller.4 != "admin"
        && !permissions
            .iter()
            .any(|permission| matches!(permission.as_str(), "admin" | "infrastructure:read"))
    {
        return Err((
            StatusCode::FORBIDDEN,
            "Kubernetes access permission was revoked".to_string(),
        ));
    }

    let reported_fields = serde_json::json!({
        "argv": !input.client_reported.argv.is_empty(),
        "cli_version": !input.client_reported.cli_version.is_empty(),
        "os": !input.client_reported.os.is_empty(),
        "arch": !input.client_reported.arch.is_empty(),
        "hostname": !input.client_reported.hostname.is_empty(),
        "private_ips": !input.client_reported.private_ips.is_empty(),
    });
    let client_reported = prepare_client_reported(input.client_reported)?;
    state
        .config_db
        .attach_kubernetes_login_enrichment(&login, &client_reported)
        .await
        .map_err(internal_error)?;
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_access.client_enrichment_update", "user")
                .actor(caller.0, caller.1)
                .tenant(caller.3)
                .resource("kubernetes_cluster", input.cluster_id)
                .outcome("success")
                .changes(reported_fields.to_string())
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn ingest_session_chunk(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<SessionChunkInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    require_internal(&headers)?;
    require_text("tenant_id", &input.tenant_id, 128)?;
    require_text("session_id", &input.session_id, 128)?;
    require_text("event_id", &input.event_id, 128)?;
    require_text("gateway_id", &input.gateway_id, 256)?;
    let binding = gateway_binding()?;
    validate_gateway_binding(&binding, &input.gateway_id, &input.tenant_id, None)?;
    let gateway_id = input.gateway_id.clone();
    if !input.recording_state.is_empty()
        && !matches!(
            input.recording_state.as_str(),
            "recording" | "complete" | "partial" | "partial_protocol_capture" | "failed"
        )
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid recording_state".to_string(),
        ));
    }
    let (data, encoding, byte_count, redactions, provenance) =
        prepare_session_chunk(&input.stream, &input.encoding, input.data)?;
    let summary = state
        .config_db
        .kubernetes_session_summary(&input.tenant_id, &input.session_id)
        .await
        .map_err(internal_error)?;
    if summary.total_bytes.saturating_add(byte_count) > max_session_bytes() {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "session exceeds KUBERNETES_ACCESS_MAX_SESSION_BYTES".to_string(),
        ));
    }
    let chunk = KubernetesSessionChunk {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: input.tenant_id,
        session_id: input.session_id,
        event_id: input.event_id,
        gateway_id: gateway_id.clone(),
        sequence: input.sequence,
        stream: input.stream,
        encoding,
        provenance,
        recording_state: if input.recording_state.is_empty() {
            "partial".to_string()
        } else {
            input.recording_state
        },
        offset_ms: input.offset_ms,
        byte_count,
        data,
        redaction_count: redactions,
        created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
    };
    state
        .config_db
        .insert_kubernetes_session_chunk(&chunk)
        .await
        .map_err(internal_error)?;
    state
        .audit
        .log(access_audit_event(
            "kubernetes_access.session_chunk_ingest",
            "system",
            &gateway_id,
            "Kubernetes access gateway",
            &chunk.tenant_id,
            &chunk.session_id,
            &headers,
        ))
        .await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_agent_access_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AgentAccessEventQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !crate::internal_auth::sre_agent_token_matches(&headers) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "invalid internal credential".to_string(),
        ));
    }
    require_text("tenant_id", &query.tenant_id, 128)?;
    require_enabled()?;

    let filters = AccessEventQuery {
        from: query.from,
        to: query.to,
        actor: query.actor,
        cluster: query.cluster,
        namespace: query.namespace,
        verb: query.verb,
        resource: query.resource,
        status: query.status,
        source_kind: None,
        recording_state: None,
        q: None,
        limit: Some(query.limit.unwrap_or(25).clamp(1, 50)),
        offset: Some(0),
    };
    let filter = map_query(&query.tenant_id, &filters, false)?;
    let (rows, total) = state
        .config_db
        .list_kubernetes_access_events(&filter, false)
        .await
        .map_err(internal_error)?;
    let events = views_within_budget(rows, MAX_LIST_RESPONSE_BYTES)
        .into_iter()
        .map(AgentAccessEventView::from)
        .collect::<Vec<_>>();

    state
        .audit
        .log(
            crate::audit::AuditEvent::new("kubernetes_access.agent_search", "system")
                .actor_name("sre-agent")
                .tenant(query.tenant_id)
                .resource("kubernetes_access", "agent-search")
                .changes(
                    serde_json::json!({
                        "from": filter.from,
                        "to": filter.to,
                        "cluster": filter.cluster,
                        "namespace": filter.namespace,
                        "verb": filter.verb,
                        "resource": filter.resource,
                        "actor_filter_applied": !filter.actor.is_empty(),
                        "returned": events.len(),
                        "total": total,
                    })
                    .to_string(),
                )
                .description(
                    "SRE agent searched Kubernetes access metadata during an investigation",
                )
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(AgentAccessEventListResponse {
        events,
        total,
        evidence_note: "Commands are reconstructed from Kubernetes API request metadata. Recorded command arguments, terminal output, device details, and network evidence are not included.",
    }))
}

pub async fn list_access_events(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    headers: HeaderMap,
    Query(query): Query<AccessEventQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = match require_admin(&state, &headers).await {
        Ok(caller) => caller,
        Err(error) => {
            audit_access_denial(
                &state,
                &headers,
                &identity,
                "kubernetes_access.search_denied",
                "search",
            )
            .await;
            return Err(error);
        }
    };
    let filter = map_query(&caller.3, &query, false)?;
    let (rows, total) = state
        .config_db
        .list_kubernetes_access_events(&filter, false)
        .await
        .map_err(internal_error)?;
    let events = views_within_budget(rows, MAX_LIST_RESPONSE_BYTES);
    let count = events.len() as u64;
    let next_offset = filter.offset.saturating_add(count);
    state
        .audit
        .log(access_audit_event(
            "kubernetes_access.search",
            "user",
            &caller.0,
            &caller.1,
            &caller.3,
            "search",
            &headers,
        ))
        .await;
    Ok(Json(AccessEventListResponse {
        events,
        total,
        next_cursor: (next_offset < total).then(|| next_offset.to_string()),
    }))
}

pub async fn get_access_event(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = match require_admin(&state, &headers).await {
        Ok(caller) => caller,
        Err(error) => {
            audit_access_denial(
                &state,
                &headers,
                &identity,
                "kubernetes_access.event_read_denied",
                &id,
            )
            .await;
            return Err(error);
        }
    };
    validate_text("id", &id, 128)?;
    let event = state
        .config_db
        .get_kubernetes_access_event(&caller.3, &id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "access event not found".to_string()))?;
    let session = if event.session_id.is_empty() {
        None
    } else {
        Some(
            state
                .config_db
                .kubernetes_session_summary(&caller.3, &event.session_id)
                .await
                .map_err(internal_error)?,
        )
    };
    state
        .audit
        .log(access_audit_event(
            "kubernetes_access.event_read",
            "user",
            &caller.0,
            &caller.1,
            &caller.3,
            &id,
            &headers,
        ))
        .await;
    Ok(Json(serde_json::json!({
        "event": KubernetesAccessEventView::from(event),
        "session": session,
    })))
}

pub async fn get_session_chunks(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<SessionChunkQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = match require_admin(&state, &headers).await {
        Ok(caller) => caller,
        Err(error) => {
            audit_access_denial(
                &state,
                &headers,
                &identity,
                "kubernetes_access.session_read_denied",
                &session_id,
            )
            .await;
            return Err(error);
        }
    };
    require_text("session_id", &session_id, 128)?;
    let after_sequence = query.after_sequence.unwrap_or(0);
    let limit = query.limit.unwrap_or(256).clamp(1, MAX_SESSION_REPLAY_PAGE);
    let rows = state
        .config_db
        .list_kubernetes_session_chunks(
            &caller.3,
            &session_id,
            after_sequence,
            limit.saturating_add(1),
        )
        .await
        .map_err(internal_error)?;

    let mut has_more = rows.len() as u64 > limit;
    let mut used = 0_usize;
    let mut chunks = Vec::new();
    for row in rows.into_iter().take(limit as usize) {
        let view = KubernetesSessionChunkView::from(row);
        let encoded_bytes = serde_json::to_vec(&view)
            .map(|encoded| encoded.len())
            .unwrap_or(MAX_LIST_RESPONSE_BYTES);
        if !chunks.is_empty() && used.saturating_add(encoded_bytes) > MAX_LIST_RESPONSE_BYTES {
            has_more = true;
            break;
        }
        used = used.saturating_add(encoded_bytes);
        chunks.push(view);
    }
    let next_sequence = has_more
        .then(|| chunks.last().map(|chunk| chunk.sequence))
        .flatten();
    state
        .audit
        .log(access_audit_event(
            "kubernetes_access.session_read",
            "user",
            &caller.0,
            &caller.1,
            &caller.3,
            &session_id,
            &headers,
        ))
        .await;
    Ok(Json(SessionChunkListResponse {
        chunks,
        has_more,
        next_sequence,
    }))
}

pub async fn export_access_events(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    headers: HeaderMap,
    Query(query): Query<AccessEventQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    let caller = match require_admin(&state, &headers).await {
        Ok(caller) => caller,
        Err(error) => {
            audit_access_denial(
                &state,
                &headers,
                &identity,
                "kubernetes_access.export_denied",
                "export",
            )
            .await;
            return Err(error);
        }
    };
    let filter = map_query(&caller.3, &query, true)?;
    let (rows, total) = state
        .config_db
        .list_kubernetes_access_events(&filter, true)
        .await
        .map_err(internal_error)?;
    let events = views_within_budget(rows, MAX_EXPORT_RESPONSE_BYTES);
    let exported = events.len();
    state
        .audit
        .log(access_audit_event(
            "kubernetes_access.export",
            "user",
            &caller.0,
            &caller.1,
            &caller.3,
            "export",
            &headers,
        ))
        .await;
    Ok(Json(serde_json::json!({
        "events": events,
        "total": total,
        "exported": exported,
    })))
}

fn internal_error(error: anyhow::Error) -> (StatusCode, String) {
    tracing::error!(%error, "Kubernetes access storage failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal error".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_auth_uses_exact_constant_time_comparison() {
        assert!(constant_time_eq(b"same-value", b"same-value"));
        assert!(!constant_time_eq(b"same-value", b"same-valuE"));
        assert!(!constant_time_eq(b"short", b"longer"));

        let mut headers = HeaderMap::new();
        assert!(validate_internal_header(&headers, "same-value").is_err());
        headers.insert("x-rush-internal-token", "same-valuE".parse().unwrap());
        assert!(validate_internal_header(&headers, "same-value").is_err());
        headers.insert("x-rush-internal-token", "same-value".parse().unwrap());
        assert!(validate_internal_header(&headers, "same-value").is_ok());
    }

    #[test]
    fn gateway_ready_contract_is_strict_and_fail_closed() {
        assert!(
            serde_json::from_value::<GatewayReadyInput>(serde_json::json!({
                "cluster_id": "prod",
                "tenant_id": "tenant-a"
            }))
            .is_err()
        );

        let binding = GatewayBinding {
            gateway_id: "gateway-1".to_string(),
            tenant_ids: vec!["tenant-a".to_string()],
            cluster_id: "prod".to_string(),
        };
        let mut headers = HeaderMap::new();
        assert!(validate_gateway_instance(&binding, &headers, "prod").is_err());
        headers.insert("x-rush-gateway-id", "gateway-1".parse().unwrap());
        assert!(validate_gateway_instance(&binding, &headers, "staging").is_err());
        assert!(validate_gateway_instance(&binding, &headers, "prod").is_ok());

        assert!(recorder_storage_ready(Ok(())).is_ok());
        assert!(recorder_storage_ready(Err(anyhow::anyhow!("offline"))).is_err());
    }

    #[test]
    fn client_body_rejects_authoritative_provenance_fields() {
        let body = serde_json::json!({
            "tenant_id": "victim",
            "actor_user_id": "admin",
            "source_kind": "gateway",
            "observed_network": {"ip": "203.0.113.10"},
            "cluster_id": "prod",
            "client_reported": {"argv": ["get", "pods"]}
        });
        assert!(serde_json::from_value::<ClientAccessEventInput>(body).is_err());
    }

    #[test]
    fn temporary_credential_enrichment_is_bounded_and_redacted() {
        let encoded = prepare_client_reported(ClientReportedInput {
            argv: vec![
                "kubectl".to_string(),
                "get".to_string(),
                "pods".to_string(),
                "--token".to_string(),
                "secret-value".to_string(),
            ],
            cli_version: "1.2.3".to_string(),
            os: "macos".to_string(),
            arch: "aarch64".to_string(),
            hostname: "workstation".to_string(),
            private_ips: vec![],
        })
        .unwrap();
        let reported: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(reported["os"], "macos");
        assert_eq!(reported["arch"], "aarch64");
        assert_eq!(reported["cli_version"], "1.2.3");
        assert_eq!(reported["argv"][4], "[REDACTED]");
        assert_eq!(reported["redaction_count"], 1);
    }

    #[test]
    fn raw_gateway_addresses_are_hashed_unless_retention_is_enabled() {
        let sanitized = sanitize_gateway_network_with_retention(
            serde_json::json!({
                "provenance": "trusted_proxy_chain",
                "socket_peer": "10.0.0.8:443",
                "trusted_forwarded_for": "203.0.113.9",
                "country": "JP"
            }),
            "01234567890123456789012345678901",
            false,
        );
        assert!(sanitized.get("socket_peer").is_none());
        assert!(sanitized.get("trusted_forwarded_for").is_none());
        assert_eq!(sanitized["ip_prefix"], "203.0.113.0/24");
        assert_eq!(sanitized["country"], "JP");
        assert_eq!(
            sanitized["gateway_reported_provenance"],
            "trusted_proxy_chain"
        );
        assert_eq!(sanitized["raw_ip_retained"], false);
    }

    #[test]
    fn result_is_redacted_before_it_is_bounded() {
        let value = serde_json::json!({
            "authorization": "Bearer should-not-survive",
            "rows": "x".repeat(4096),
        });
        let (stored, truncated, redactions) = bounded_json(value, 1024).unwrap();
        assert!(truncated);
        assert!(redactions > 0);
        assert!(stored.len() <= 1024);
        assert!(!stored.contains("should-not-survive"));
    }

    #[test]
    fn stdin_chunks_are_preserved_as_bounded_binary_replay_data() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"echo hello\r");
        let (data, encoding, byte_count, redactions, provenance) =
            prepare_session_chunk("stdin", "base64", encoded.clone()).unwrap();
        assert_eq!(data, encoded);
        assert_eq!(encoding, "base64");
        assert_eq!(byte_count, 11);
        assert_eq!(redactions, 0);
        let provenance: serde_json::Value = serde_json::from_str(&provenance).unwrap();
        assert_eq!(provenance["decoded_channels"], true);
        assert_eq!(provenance["sensitive_input"], true);
    }

    #[test]
    fn raw_upgrade_chunks_keep_binary_provenance_without_becoming_terminal_text() {
        let encoded = base64::engine::general_purpose::STANDARD.encode([0_u8, 1, 2, 255]);
        let (data, encoding, byte_count, redactions, provenance) =
            prepare_session_chunk("raw_upgrade_output", "base64", encoded.clone()).unwrap();
        assert_eq!(data, encoded);
        assert_eq!(encoding, "base64");
        assert_eq!(byte_count, 4);
        assert_eq!(redactions, 0);
        let provenance: serde_json::Value = serde_json::from_str(&provenance).unwrap();
        assert_eq!(provenance["terminal_text"], false);
        assert_eq!(provenance["decoded_channels"], false);
    }

    #[test]
    fn malformed_binary_session_chunks_are_rejected() {
        assert!(prepare_session_chunk("stdout", "base64", "%%%".to_string()).is_err());
    }

    #[test]
    fn query_filter_is_pinned_to_callers_tenant() {
        let filter = map_query(
            "tenant-a",
            &AccessEventQuery {
                limit: Some(50000),
                ..Default::default()
            },
            false,
        )
        .unwrap();
        assert_eq!(filter.tenant_id, "tenant-a");
        assert!(filter.limit > 0);
        assert!(filter.limit < 1000);
    }

    #[test]
    fn status_filter_accepts_buckets_and_exact_codes() {
        let bucket = map_query(
            "tenant-a",
            &AccessEventQuery {
                status: Some("4xx".to_string()),
                ..Default::default()
            },
            false,
        )
        .unwrap();
        assert_eq!((bucket.status_min, bucket.status_max), (400, 499));

        let exact = map_query(
            "tenant-a",
            &AccessEventQuery {
                status: Some("404".to_string()),
                ..Default::default()
            },
            false,
        )
        .unwrap();
        assert_eq!((exact.status_min, exact.status_max), (404, 404));
    }

    #[test]
    fn agent_access_view_returns_causal_metadata_without_captured_evidence() {
        let input: InternalAccessEventInput = serde_json::from_value(serde_json::json!({
            "id": "event-1",
            "tenant_id": "tenant-a",
            "cluster_id": "prod",
            "gateway_id": "gateway-1",
            "session_id": "session-1",
            "actor_user_id": "user-1",
            "actor_name": "operator",
            "actor_type": "user",
            "kube_username": "rush:user:operator",
            "source_kind": "gateway",
            "client_reported": {
                "argv": ["kubectl", "exec", "api-7f8c", "--", "sh", "-lc", "sensitive command"],
                "hostname": "workstation"
            },
            "observed_network": {"country": "JP", "source_ip": "203.0.113.10"},
            "http_method": "POST",
            "verb": "create",
            "api_version": "v1",
            "resource": "pods",
            "subresource": "exec",
            "namespace": "payments",
            "name": "api-7f8c",
            "request_query": {"command": ["sh", "-lc", "sensitive command"]},
            "status_code": 201,
            "duration_ms": 42,
            "result_summary": {"stdout": "sensitive output"},
            "recording_state": "complete",
            "created_at": "2026-08-23T10:00:00Z"
        }))
        .unwrap();
        let event = internal_event(input, "01234567890123456789012345678901").unwrap();
        let view = AgentAccessEventView::from(KubernetesAccessEventView::from(event));
        let encoded = serde_json::to_value(view).unwrap();

        assert_eq!(
            encoded["likely_kubectl_command"],
            "kubectl exec -n payments api-7f8c"
        );
        assert_eq!(encoded["actor_name"], "operator");
        assert_eq!(encoded["session_id"], "session-1");
        for forbidden in [
            "client_reported",
            "observed_network",
            "request_query",
            "result_summary",
            "user_agent",
            "kube_groups",
        ] {
            assert!(encoded.get(forbidden).is_none(), "unexpected {forbidden}");
        }
        assert!(!encoded.to_string().contains("sensitive"));
        assert!(!encoded.to_string().contains("workstation"));
        assert!(!encoded.to_string().contains("203.0.113.10"));
    }

    #[test]
    fn rfc3339_filters_match_the_stored_timestamp_format() {
        let filter = map_query(
            "tenant-a",
            &AccessEventQuery {
                from: Some("2026-08-21T13:45:00Z".to_string()),
                to: Some("2026-08-21T14:45:00+00:00".to_string()),
                ..Default::default()
            },
            false,
        )
        .unwrap();
        assert_eq!(filter.from, "2026-08-21 13:45:00");
        assert_eq!(filter.to, "2026-08-21 14:45:00");
    }

    #[test]
    fn json_encoded_results_are_parsed_before_secret_redaction() {
        let raw = serde_json::Value::String(
            r#"{"kind":"Secret","data":{"token":"dG9wLXNlY3JldA=="}}"#.to_string(),
        );
        let (stored, _, redactions) = bounded_json(raw, 4096).unwrap();
        assert_eq!(redactions, 1);
        assert!(!stored.contains("dG9wLXNlY3JldA=="));
        assert!(stored.contains("[REDACTED]"));
    }

    #[test]
    fn gateway_body_json_is_parsed_before_secret_redaction() {
        let summary = serde_json::json!({
            "body": r#"{"kind":"Secret","data":{"token":"dG9wLXNlY3JldA=="}}"#,
        });
        let (stored, _, redactions) = bounded_json(summary, 4096).unwrap();
        let stored: serde_json::Value = serde_json::from_str(&stored).unwrap();

        assert_eq!(redactions, 1);
        assert_eq!(stored["body"]["data"], "[REDACTED]");
    }

    #[test]
    fn gateway_event_contract_persists_validated_actor_type() {
        let mut input: InternalAccessEventInput = serde_json::from_value(serde_json::json!({
            "id": "kar-1",
            "tenant_id": "tenant-a",
            "cluster_id": "prod",
            "gateway_id": "gateway-1",
            "actor_user_id": "key-1",
            "actor_name": "deployment key",
            "actor_type": "api_key",
            "kube_username": "rush:api-key:key-1",
            "kube_groups": ["rush:tenant:tenant-a:role:write"],
            "source_kind": "gateway",
            "verb": "list",
            "api_version": "v1",
            "resource": "pods",
            "status_code": 200,
            "recording_state": "complete",
            "result_summary": {"body": {"kind": "PodList"}},
            "created_at": "2026-08-21T12:00:00Z"
        }))
        .unwrap();

        let event = internal_event(input, "01234567890123456789012345678901").unwrap();
        assert_eq!(event.actor_type, "api_key");

        input = serde_json::from_value(serde_json::json!({
            "tenant_id": "tenant-a",
            "cluster_id": "prod",
            "gateway_id": "gateway-1",
            "actor_type": "human",
            "source_kind": "gateway",
            "verb": "list"
        }))
        .unwrap();
        assert!(validate_internal_input(&input).is_err());
    }

    #[test]
    fn internal_ingest_rejects_client_reported_source_kind() {
        let input = InternalAccessEventInput {
            tenant_id: "tenant-a".to_string(),
            id: String::new(),
            cluster_id: "prod".to_string(),
            gateway_id: "gateway-1".to_string(),
            session_id: String::new(),
            actor_user_id: "user-1".to_string(),
            actor_name: "operator".to_string(),
            actor_type: "user".to_string(),
            kube_username: "operator".to_string(),
            kube_groups: vec![],
            source_kind: "rush_cli".to_string(),
            client_reported: serde_json::json!({}),
            observed_network: serde_json::json!({}),
            http_method: "GET".to_string(),
            verb: "get".to_string(),
            api_group: String::new(),
            api_version: "v1".to_string(),
            resource: "pods".to_string(),
            subresource: String::new(),
            namespace: "default".to_string(),
            name: String::new(),
            request_query: serde_json::json!({}),
            user_agent: String::new(),
            status_code: 200,
            duration_ms: 1,
            request_bytes: 0,
            response_bytes: 0,
            result_summary: serde_json::Value::Null,
            result_truncated: false,
            recording_state: "complete".to_string(),
            created_at: String::new(),
        };
        assert!(validate_internal_input(&input).is_err());
    }

    #[test]
    fn gateway_binding_pins_identity_tenant_and_cluster() {
        let binding = GatewayBinding {
            gateway_id: "gateway-1".to_string(),
            tenant_ids: vec!["tenant-a".to_string()],
            cluster_id: "prod".to_string(),
        };
        assert!(validate_gateway_binding(&binding, "gateway-1", "tenant-a", Some("prod")).is_ok());
        assert!(validate_gateway_binding(&binding, "gateway-2", "tenant-a", Some("prod")).is_err());
        assert!(validate_gateway_binding(&binding, "gateway-1", "tenant-b", Some("prod")).is_err());
        assert!(
            validate_gateway_binding(&binding, "gateway-1", "tenant-a", Some("staging")).is_err()
        );
    }

    #[test]
    fn gateway_authorize_requires_the_bound_gateway_header() {
        let binding = GatewayBinding {
            gateway_id: "gateway-1".to_string(),
            tenant_ids: vec!["tenant-a".to_string()],
            cluster_id: "prod".to_string(),
        };
        let mut headers = HeaderMap::new();
        assert!(validate_authorizing_gateway(&binding, &headers, "tenant-a", "prod").is_err());

        headers.insert("x-rush-gateway-id", "gateway-2".parse().unwrap());
        assert!(validate_authorizing_gateway(&binding, &headers, "tenant-a", "prod").is_err());

        headers.insert("x-rush-gateway-id", "gateway-1".parse().unwrap());
        assert!(validate_authorizing_gateway(&binding, &headers, "tenant-a", "prod").is_ok());
    }

    #[test]
    fn tenant_cluster_policy_is_deny_by_default() {
        let policy = r#"{"tenant-a":["prod"]}"#;
        assert!(tenant_cluster_allowed(policy, "tenant-a", "prod").unwrap());
        assert!(!tenant_cluster_allowed(policy, "tenant-a", "staging").unwrap());
        assert!(!tenant_cluster_allowed(policy, "tenant-b", "prod").unwrap());
    }

    #[test]
    fn gateway_chunk_contract_accepts_pre_event_protocol_capture() {
        let chunk: SessionChunkInput = serde_json::from_value(serde_json::json!({
            "id": "chunk-1",
            "tenant_id": "tenant-a",
            "session_id": "session-1",
            "event_id": "event-written-later",
            "gateway_id": "gateway-1",
            "sequence": 0,
            "stream": "raw_upgrade_output",
            "offset_ms": 10,
            "data": "AAEC/w==",
            "encoding": "base64",
            "byte_count": 4,
            "recording_state": "partial_protocol_capture",
            "created_at": "2026-08-21T13:45:00Z"
        }))
        .unwrap();
        assert_eq!(chunk.gateway_id, "gateway-1");
        assert_eq!(chunk.stream, "raw_upgrade_output");
        assert_eq!(chunk.recording_state, "partial_protocol_capture");

        let handler = include_str!("kubernetes_access.rs")
            .split_once("pub async fn ingest_session_chunk")
            .unwrap()
            .1
            .split("pub async fn list_access_events")
            .next()
            .unwrap();
        assert!(!handler.contains("get_kubernetes_access_event"));
    }

    #[test]
    fn sensitive_reads_and_exports_have_stable_audit_actions() {
        let headers = HeaderMap::new();
        for action in [
            "kubernetes_access.search",
            "kubernetes_access.event_read",
            "kubernetes_access.export",
        ] {
            let event = access_audit_event(
                action,
                "user",
                "user-1",
                "admin",
                "tenant-a",
                "resource-1",
                &headers,
            );
            assert_eq!(event.action, action);
            assert_eq!(event.tenant_id, "tenant-a");
            assert_eq!(event.actor_id, "user-1");
        }
        assert!(include_str!("kubernetes_access.rs").contains("kubernetes_access.agent_search"));
    }

    #[test]
    fn temporary_credentials_are_high_entropy_and_stored_as_hashes() {
        let credential = temporary_device_credential();
        assert!(credential.starts_with("rkt1_"));
        assert_eq!(credential.len(), 69);
        let digest = temporary_credential_hash(&credential);
        assert_eq!(digest.len(), 64);
        assert!(!digest.contains(&credential));
        assert_eq!(digest, temporary_credential_hash(&credential));
        let user_code = temporary_user_code();
        assert_eq!(user_code.len(), 16);
        assert!(user_code.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[test]
    fn client_session_ids_are_stable_opaque_handles() {
        let first = client_session_id(&"a".repeat(64));
        let second = client_session_id(&"b".repeat(64));

        assert_eq!(first, client_session_id(&"a".repeat(64)));
        assert_ne!(first, second);
        assert!(first.starts_with("kcs_"));
        assert_eq!(first.len(), 28);
        assert!(!first.contains(&"a".repeat(16)));
    }

    #[test]
    fn kubernetes_client_settings_are_bounded_audited_and_revocation_checked() {
        assert_eq!(MIN_KUBERNETES_SESSION_SECONDS, 300);
        assert_eq!(MAX_KUBERNETES_SESSION_SECONDS, 43_200);
        let source = include_str!("kubernetes_access.rs");
        assert!(source.contains(KUBERNETES_SESSION_SECONDS_SETTING));
        assert!(source.contains("kubernetes_access.client_session_list"));
        assert!(source.contains("kubernetes_access.credential_revoke"));
        assert!(source.matches("is_kubernetes_login_revoked").count() >= 4);
    }

    #[test]
    fn kubernetes_role_groups_are_tenant_qualified() {
        let tenant_a =
            kubernetes_authorization_groups("tenant-a", "write", &["platform".to_string()])
                .unwrap();
        let tenant_b =
            kubernetes_authorization_groups("tenant-b", "write", &["developers".to_string()])
                .unwrap();

        assert_ne!(tenant_a, tenant_b);
        assert!(tenant_a.contains(&"rush:tenant:tenant-a:role:write".to_string()));
        assert!(tenant_b.contains(&"rush:tenant:tenant-b:role:write".to_string()));
        assert!(tenant_a.contains(&"rush:group:platform".to_string()));
        assert!(tenant_b.contains(&"rush:group:developers".to_string()));
        assert!(tenant_a.iter().all(|group| group != "rush:role:write"));
        assert!(tenant_b.iter().all(|group| group != "rush:role:write"));
    }

    #[test]
    fn custom_kubernetes_roles_support_crds_but_not_rbac_administration() {
        let crd = KubernetesRbacGrantInput {
            group_id: "platform".to_string(),
            cluster_id: "prod".to_string(),
            cluster_match: "single".to_string(),
            cluster_pattern: String::new(),
            name: "Read Argo CD applications".to_string(),
            role_kind: "custom".to_string(),
            role_name: String::new(),
            scope: "namespaces".to_string(),
            namespaces: vec!["argocd".to_string()],
            rules: vec![KubernetesRbacRule {
                api_groups: vec!["argoproj.io".to_string()],
                resources: vec![
                    "applications".to_string(),
                    "applications/status".to_string(),
                ],
                verbs: vec!["get".to_string(), "list".to_string(), "watch".to_string()],
            }],
        };
        assert!(validate_kubernetes_rbac_grant(crd).is_ok());

        let rbac_admin = KubernetesRbacGrantInput {
            group_id: "platform".to_string(),
            cluster_id: "prod".to_string(),
            cluster_match: "single".to_string(),
            cluster_pattern: String::new(),
            name: "RBAC admin".to_string(),
            role_kind: "custom".to_string(),
            role_name: String::new(),
            scope: "cluster".to_string(),
            namespaces: vec![],
            rules: vec![KubernetesRbacRule {
                api_groups: vec!["rbac.authorization.k8s.io".to_string()],
                resources: vec!["clusterroles".to_string()],
                verbs: vec!["*".to_string()],
            }],
        };
        assert!(validate_kubernetes_rbac_grant(rbac_admin).is_err());
    }

    #[test]
    fn kubernetes_roles_accept_single_all_and_wildcard_cluster_targets() {
        let base = KubernetesRbacGrantInput {
            group_id: "platform".to_string(),
            cluster_id: "west-production".to_string(),
            cluster_match: "single".to_string(),
            cluster_pattern: String::new(),
            name: "Production readers".to_string(),
            role_kind: "view".to_string(),
            role_name: String::new(),
            scope: "cluster".to_string(),
            namespaces: vec![],
            rules: vec![],
        };

        let single = validate_kubernetes_rbac_grant(base.clone()).unwrap();
        assert_eq!(single.cluster_id, "west-production");

        let all = validate_kubernetes_rbac_grant(KubernetesRbacGrantInput {
            cluster_match: "all".to_string(),
            ..base.clone()
        })
        .unwrap();
        assert!(all.cluster_id.is_empty());

        let pattern = validate_kubernetes_rbac_grant(KubernetesRbacGrantInput {
            cluster_match: "pattern".to_string(),
            cluster_pattern: "*-production".to_string(),
            ..base.clone()
        })
        .unwrap();
        assert!(pattern.cluster_id.is_empty());

        assert!(
            validate_kubernetes_rbac_grant(KubernetesRbacGrantInput {
                cluster_match: "pattern".to_string(),
                cluster_pattern: "production".to_string(),
                ..base
            })
            .is_err()
        );
    }

    #[test]
    fn gateway_authorization_has_no_api_key_allowlist_path() {
        let source = include_str!("kubernetes_access.rs");
        assert!(!source.contains(&["KUBERNETES_ACCESS_API_KEY_", "IDS"].concat()));
        assert!(!source.contains(&["KUBERNETES_ACCESS_API_KEY_", "ROLES"].concat()));
        assert!(source.contains("temporary Rush login credential required"));
    }
}
