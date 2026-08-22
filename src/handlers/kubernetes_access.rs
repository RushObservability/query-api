use std::net::{IpAddr, SocketAddr};

use axum::{
    Extension, Json,
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::handlers::infrastructure::require_infrastructure_read;
use crate::handlers::users::{require_admin, require_auth};
use crate::models::kubernetes_access::{
    KubernetesAccessEvent, KubernetesAccessEventView, KubernetesAccessFilter,
    KubernetesSessionChunk,
};
use crate::{AppState, RequestIdentity};

pub const MAX_ACCESS_EVENT_BODY_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_SESSION_CHUNK_BODY_BYTES: usize = 512 * 1024;
pub const MAX_GATEWAY_AUTHORIZE_BODY_BYTES: usize = 8 * 1024;
const DEFAULT_MAX_RESULT_BYTES: usize = 256 * 1024;
const HARD_MAX_RESULT_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_SESSION_BYTES: u64 = 64 * 1024 * 1024;
const HARD_MAX_SESSION_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_TEXT_BYTES: usize = 1024;
const MAX_JSON_METADATA_BYTES: usize = 64 * 1024;
const MAX_LIST_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_EXPORT_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

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

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientExecutionInput {
    #[serde(default)]
    pub started_at: String,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub exit_code: i32,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientCaptureInput {
    #[serde(default)]
    pub stdout_preview: String,
    #[serde(default)]
    pub stderr_preview: String,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub redaction_count: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientAccessEventInput {
    pub cluster_id: String,
    pub client_reported: ClientReportedInput,
    #[serde(default)]
    pub execution: Option<ClientExecutionInput>,
    #[serde(default)]
    pub capture: Option<ClientCaptureInput>,
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
    let policy = serde_json::from_str::<std::collections::HashMap<String, Vec<String>>>(raw_policy)
        .map_err(|_| {
            tracing::error!("KUBERNETES_ACCESS_TENANT_CLUSTERS is invalid JSON");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes access policy is unavailable".to_string(),
            )
        })?;
    Ok(policy
        .get(tenant_id)
        .is_some_and(|clusters| clusters.iter().any(|allowed| allowed == cluster_id)))
}

fn api_key_id_allowed(actor_id: &str) -> bool {
    std::env::var("KUBERNETES_ACCESS_API_KEY_IDS")
        .ok()
        .is_some_and(|raw| raw.split(',').map(str::trim).any(|id| id == actor_id))
}

fn api_key_role_from_policy(
    raw_policy: Option<&str>,
    actor_id: &str,
) -> Result<String, (StatusCode, String)> {
    let Some(raw_policy) = raw_policy.filter(|value| !value.trim().is_empty()) else {
        return Ok("read".to_string());
    };
    let policy = serde_json::from_str::<std::collections::HashMap<String, String>>(raw_policy)
        .map_err(|_| {
            tracing::error!("KUBERNETES_ACCESS_API_KEY_ROLES is invalid JSON");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Kubernetes access API key policy is unavailable".to_string(),
            )
        })?;
    if policy.iter().any(|(key_id, role)| {
        key_id.trim().is_empty()
            || key_id.len() > 256
            || !matches!(role.as_str(), "read" | "write" | "admin")
    }) {
        tracing::error!("KUBERNETES_ACCESS_API_KEY_ROLES contains an invalid key ID or role");
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Kubernetes access API key policy is unavailable".to_string(),
        ));
    }
    Ok(policy
        .get(actor_id)
        .cloned()
        .unwrap_or_else(|| "read".to_string()))
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

fn resolved_client_ip(peer: IpAddr, headers: &HeaderMap, trusted_proxy_cidrs: &[String]) -> IpAddr {
    let trusted = |address| {
        !trusted_proxy_cidrs.is_empty()
            && crate::api_key_auth::source_allowed(address, trusted_proxy_cidrs)
    };
    if !trusted(peer) {
        return peer;
    }
    if let Some(raw) = headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
    {
        let Ok(chain) = raw
            .split(',')
            .map(|value| value.trim().parse::<IpAddr>())
            .collect::<Result<Vec<_>, _>>()
        else {
            return peer;
        };
        let mut client = peer;
        for address in chain.into_iter().rev() {
            if !trusted(client) {
                break;
            }
            client = address;
        }
        return client;
    }
    headers
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<IpAddr>().ok())
        .unwrap_or(peer)
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

fn kubectl_parts(argv: &[String]) -> (String, String, String) {
    const VERBS: &[&str] = &[
        "get",
        "list",
        "describe",
        "logs",
        "apply",
        "create",
        "delete",
        "patch",
        "edit",
        "exec",
        "attach",
        "debug",
        "run",
        "port-forward",
        "scale",
        "rollout",
        "cordon",
        "uncordon",
        "drain",
    ];
    let verb_index = argv.iter().position(|arg| VERBS.contains(&arg.as_str()));
    let verb = verb_index
        .and_then(|index| argv.get(index))
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    let resource = verb_index
        .and_then(|index| argv.get(index + 1))
        .filter(|value| !value.starts_with('-'))
        .cloned()
        .unwrap_or_default();
    let namespace = argv
        .iter()
        .enumerate()
        .find_map(|(index, arg)| match arg.as_str() {
            "-n" | "--namespace" => argv.get(index + 1).cloned(),
            _ => arg.strip_prefix("--namespace=").map(str::to_string),
        })
        .unwrap_or_default();
    (verb, resource, namespace)
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
        "stdout" | "stderr" | "resize" | "raw_upgrade_output"
    ) {
        return Err((
            StatusCode::BAD_REQUEST,
            "stream must be stdout, stderr, resize, or raw_upgrade_output; stdin is not recorded"
                .to_string(),
        ));
    }
    let encoding = if requested_encoding.is_empty() {
        "utf8".to_string()
    } else {
        requested_encoding.to_string()
    };
    let (data, byte_count, redactions, provenance) = if stream == "raw_upgrade_output" {
        if encoding != "base64" {
            return Err((
                StatusCode::BAD_REQUEST,
                "raw_upgrade_output requires base64 encoding".to_string(),
            ));
        }
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(input.as_bytes())
            .map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "raw_upgrade_output data is not valid base64".to_string(),
                )
            })?;
        (
            input,
            decoded.len() as u64,
            0,
            serde_json::json!({
                "capture": "gateway_upstream_to_client",
                "decoded_channels": false,
                "terminal_text": false,
            }),
        )
    } else {
        if encoding != "utf8" {
            return Err((
                StatusCode::BAD_REQUEST,
                "decoded session streams require utf8 encoding".to_string(),
            ));
        }
        let mut data = serde_json::Value::String(input);
        let redactions = redact_value(&mut data);
        let data = data.as_str().unwrap_or("[REDACTED]").to_string();
        let byte_count = data.len() as u64;
        (
            data,
            byte_count,
            redactions,
            serde_json::json!({
                "capture": "gateway_decoded_stream",
                "decoded_channels": true,
                "terminal_text": stream != "resize",
            }),
        )
    };
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

fn client_event(
    input: ClientAccessEventInput,
    identity: &RequestIdentity,
    actor_id: String,
    actor_name: String,
    actor_type: String,
    user_agent: String,
    peer_ip: IpAddr,
    secret: &str,
) -> Result<KubernetesAccessEvent, (StatusCode, String)> {
    require_text("cluster_id", &input.cluster_id, 256)?;
    if input.client_reported.argv.len() > 128
        || input
            .client_reported
            .argv
            .iter()
            .any(|argument| argument.len() > 4096)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "client_reported.argv exceeds its limit".to_string(),
        ));
    }
    for (name, value, max) in [
        (
            "cli_version",
            input.client_reported.cli_version.as_str(),
            128,
        ),
        ("os", input.client_reported.os.as_str(), 128),
        ("arch", input.client_reported.arch.as_str(), 128),
        ("hostname", input.client_reported.hostname.as_str(), 256),
    ] {
        validate_text(name, value, max)?;
    }

    let execution = input.execution.unwrap_or(ClientExecutionInput {
        started_at: String::new(),
        duration_ms: 0,
        exit_code: 0,
    });
    let capture = input.capture.unwrap_or(ClientCaptureInput {
        stdout_preview: String::new(),
        stderr_preview: String::new(),
        truncated: false,
        redaction_count: 0,
    });
    let private_ips = collect_private_ips(&input.client_reported.private_ips);
    let (verb, resource, namespace) = kubectl_parts(&input.client_reported.argv);
    let client_reported_value = serde_json::json!({
        "provenance": "client_reported",
        "argv": input.client_reported.argv,
        "cli_version": input.client_reported.cli_version,
        "os": input.client_reported.os,
        "arch": input.client_reported.arch,
        "hostname": input.client_reported.hostname,
        "private_ips": private_ips,
        "private_ips_collected": !private_ips.is_empty(),
        "execution": execution,
    });
    let result_value = serde_json::json!({
        "stdout": capture.stdout_preview,
        "stderr": capture.stderr_preview,
        "client_reported_truncated": capture.truncated,
    });
    let (client_reported, _, client_redactions) =
        bounded_json(client_reported_value, MAX_JSON_METADATA_BYTES)?;
    let (result_summary, bounded, result_redactions) =
        bounded_json(result_value, max_result_bytes())?;
    let truncated = bounded || capture.truncated;

    Ok(KubernetesAccessEvent {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: identity.tenant_id.clone(),
        cluster_id: input.cluster_id,
        gateway_id: String::new(),
        session_id: String::new(),
        actor_user_id: actor_id,
        actor_name,
        actor_type,
        kube_username: String::new(),
        kube_groups: "[]".to_string(),
        source_kind: "rush_cli".to_string(),
        client_reported,
        observed_network: serde_json::to_string(&network_evidence(peer_ip, secret))
            .unwrap_or_else(|_| "{}".to_string()),
        http_method: "KUBECTL".to_string(),
        verb,
        api_group: String::new(),
        api_version: String::new(),
        resource,
        subresource: String::new(),
        namespace,
        name: String::new(),
        request_query: "{}".to_string(),
        user_agent,
        status_code: if execution.exit_code == 0 { 200 } else { 500 },
        duration_ms: execution.duration_ms,
        request_bytes: 0,
        response_bytes: result_summary.len() as u64,
        result_summary,
        result_truncated: u8::from(truncated),
        redaction_count: client_redactions
            .saturating_add(result_redactions)
            .saturating_add(capture.redaction_count),
        recording_state: if truncated { "partial" } else { "complete" }.to_string(),
        created_at: normalize_timestamp(&execution.started_at),
    })
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

fn kubernetes_authorization_groups(tenant_id: &str, role: &str) -> Vec<String> {
    vec![
        "rush:authenticated".to_string(),
        format!("rush:tenant:{tenant_id}:role:{role}"),
    ]
}

fn api_key_authorization(
    identity: &RequestIdentity,
    cluster_id: &str,
    explicitly_allowed: bool,
    role: &str,
) -> Result<GatewayAuthorization, (StatusCode, String)> {
    if !identity.authenticated
        || identity.credential_type != "query_key"
        || !explicitly_allowed
        || !matches!(role, "read" | "write" | "admin")
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            "valid Rush query credentials required".to_string(),
        ));
    }
    Ok(GatewayAuthorization {
        actor_user_id: identity.actor_id.clone(),
        actor_name: identity.actor_name.clone(),
        actor_type: "api_key".to_string(),
        tenant_id: identity.tenant_id.clone(),
        cluster_id: cluster_id.to_string(),
        role: role.to_string(),
        kube_username: format!("rush:api-key:{}", identity.actor_id),
        kube_groups: kubernetes_authorization_groups(&identity.tenant_id, role),
    })
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
    if let Err(error) =
        validate_authorizing_gateway(&binding, &headers, &identity.tenant_id, &input.cluster_id)
    {
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
    let policy = std::env::var("KUBERNETES_ACCESS_TENANT_CLUSTERS").map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Kubernetes access policy is unavailable".to_string(),
        )
    })?;
    if !tenant_cluster_allowed(&policy, &identity.tenant_id, &input.cluster_id)? {
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
    let authorization = if identity.credential_type == "session" {
        let caller = require_infrastructure_read(&state, &headers).await?;
        if caller.3 != identity.tenant_id {
            return Err((StatusCode::FORBIDDEN, "tenant mismatch".to_string()));
        }
        GatewayAuthorization {
            actor_user_id: caller.0,
            actor_name: caller.1.clone(),
            actor_type: "user".to_string(),
            tenant_id: caller.3.clone(),
            cluster_id: input.cluster_id.clone(),
            role: caller.4.clone(),
            kube_username: format!("rush:user:{}", caller.1),
            kube_groups: kubernetes_authorization_groups(&caller.3, &caller.4),
        }
    } else {
        let role_policy = std::env::var("KUBERNETES_ACCESS_API_KEY_ROLES").ok();
        let role = match api_key_role_from_policy(role_policy.as_deref(), &identity.actor_id) {
            Ok(role) => role,
            Err(error) => {
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
        };
        match api_key_authorization(
            &identity,
            &input.cluster_id,
            api_key_id_allowed(&identity.actor_id),
            &role,
        ) {
            Ok(authorization) => authorization,
            Err(error) => {
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
        }
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
    Extension(identity): Extension<RequestIdentity>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(input): Json<ClientAccessEventInput>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_enabled()?;
    if !crate::api_key_auth::env_flag("KUBERNETES_ACCESS_CLIENT_ENRICHMENT_ENABLED") {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    }
    if !identity.authenticated || identity.credential_type == "ingest_key" {
        return Err((
            StatusCode::UNAUTHORIZED,
            "authentication required".to_string(),
        ));
    }
    let secret = internal_secret()?;
    let (actor_id, actor_name, actor_type) = if identity.credential_type == "session" {
        let caller = require_auth(&state, &headers).await?;
        if caller.3 != identity.tenant_id {
            return Err((StatusCode::FORBIDDEN, "tenant mismatch".to_string()));
        }
        (caller.0, caller.1, "user")
    } else {
        (
            identity.actor_id.clone(),
            identity.actor_name.clone(),
            "api_key",
        )
    };
    let client_ip = resolved_client_ip(peer.ip(), &headers, &state.trusted_proxy_cidrs);
    let user_agent = headers
        .get("user-agent")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .chars()
        .take(MAX_TEXT_BYTES)
        .collect();
    let event = client_event(
        input,
        &identity,
        actor_id.clone(),
        actor_name.clone(),
        actor_type.to_string(),
        user_agent,
        client_ip,
        &secret,
    )?;
    state
        .config_db
        .insert_kubernetes_access_event(&event)
        .await
        .map_err(internal_error)?;
    state
        .audit
        .log(access_audit_event(
            "kubernetes_access.client_enrichment_create",
            actor_type,
            &actor_id,
            &actor_name,
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
            "complete" | "partial" | "partial_protocol_capture" | "failed"
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

    fn identity(tenant: &str) -> RequestIdentity {
        RequestIdentity {
            tenant_id: tenant.to_string(),
            authenticated: true,
            actor_id: "key-1".to_string(),
            actor_name: "API key".to_string(),
            actor_type: "api_key".to_string(),
            credential_type: "query_key".to_string(),
        }
    }

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
    fn tenant_comes_only_from_authenticated_identity() {
        let event = client_event(
            ClientAccessEventInput {
                cluster_id: "prod".to_string(),
                client_reported: ClientReportedInput {
                    argv: vec!["get".to_string(), "pods".to_string()],
                    cli_version: String::new(),
                    os: String::new(),
                    arch: String::new(),
                    hostname: String::new(),
                    private_ips: vec![],
                },
                execution: None,
                capture: None,
            },
            &identity("tenant-a"),
            "key-1".to_string(),
            "API key".to_string(),
            "api_key".to_string(),
            String::new(),
            "198.51.100.25".parse().unwrap(),
            "01234567890123456789012345678901",
        )
        .unwrap();
        assert_eq!(event.tenant_id, "tenant-a");
        assert_eq!(event.actor_user_id, "key-1");
        assert_eq!(event.source_kind, "rush_cli");
    }

    #[test]
    fn untrusted_forwarded_header_cannot_spoof_observed_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "203.0.113.9".parse().unwrap());
        let peer: IpAddr = "198.51.100.25".parse().unwrap();
        assert_eq!(resolved_client_ip(peer, &headers, &[]), peer);

        let trusted = vec!["198.51.100.0/24".to_string()];
        assert_eq!(
            resolved_client_ip(peer, &headers, &trusted),
            "203.0.113.9".parse::<IpAddr>().unwrap()
        );
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
    fn stdin_chunks_are_rejected_by_the_stream_policy() {
        assert!(prepare_session_chunk("stdin", "utf8", "secret".to_string()).is_err());
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
    }

    #[test]
    fn gateway_authorization_rejects_unresolved_or_ingest_credentials() {
        let mut anonymous = identity("tenant-a");
        anonymous.authenticated = false;
        anonymous.credential_type = "anonymous".to_string();
        assert!(api_key_authorization(&anonymous, "prod", true, "read").is_err());

        let mut ingest = identity("tenant-a");
        ingest.credential_type = "ingest_key".to_string();
        assert!(api_key_authorization(&ingest, "prod", true, "read").is_err());

        assert!(api_key_authorization(&identity("tenant-a"), "prod", false, "read").is_err());
        assert!(api_key_authorization(&identity("tenant-a"), "prod", true, "operator").is_err());
    }

    #[test]
    fn gateway_authorization_derives_actor_and_tenant_from_api_key_identity() {
        let authorization =
            api_key_authorization(&identity("tenant-a"), "prod", true, "write").unwrap();
        assert_eq!(authorization.actor_user_id, "key-1");
        assert_eq!(authorization.tenant_id, "tenant-a");
        assert_eq!(authorization.role, "write");
        assert_eq!(authorization.kube_username, "rush:api-key:key-1");
        assert_eq!(authorization.cluster_id, "prod");
        assert!(
            authorization
                .kube_groups
                .contains(&"rush:tenant:tenant-a:role:write".to_string())
        );
        assert!(
            !authorization
                .kube_groups
                .contains(&"rush:role:write".to_string())
        );
    }

    #[test]
    fn kubernetes_role_groups_are_tenant_qualified() {
        let tenant_a = kubernetes_authorization_groups("tenant-a", "write");
        let tenant_b = kubernetes_authorization_groups("tenant-b", "write");

        assert_ne!(tenant_a, tenant_b);
        assert_eq!(tenant_a[1], "rush:tenant:tenant-a:role:write");
        assert_eq!(tenant_b[1], "rush:tenant:tenant-b:role:write");
        assert!(tenant_a.iter().all(|group| group != "rush:role:write"));
        assert!(tenant_b.iter().all(|group| group != "rush:role:write"));
    }

    #[test]
    fn api_key_role_policy_is_explicit_and_fail_closed() {
        let policy = r#"{"key-1":"write","key-2":"admin"}"#;
        assert_eq!(
            api_key_role_from_policy(Some(policy), "key-1").unwrap(),
            "write"
        );
        assert_eq!(
            api_key_role_from_policy(Some(policy), "unmapped").unwrap(),
            "read"
        );
        assert!(api_key_role_from_policy(Some(r#"{"key-1":"operator"}"#), "key-1").is_err());
        assert!(api_key_role_from_policy(Some("[]"), "key-1").is_err());
    }
}
