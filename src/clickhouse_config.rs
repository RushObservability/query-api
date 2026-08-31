use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use clickhouse::Client;
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use openssl::symm::Cipher;
use rand::Rng;
use sha2::{Digest, Sha256};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

/// (user_id, username, display_name, tenant_id, role) — the require_auth tuple.
pub type SessionUser = (String, String, String, String, String);
/// (id, name, enabled, auth_required) for a tenant; None = tenant not found.
type TenantFlags = Option<(String, String, bool, bool)>;
type ApiKeyLookupResult = Result<Option<ApiKeyGrant>, String>;
type ApiKeyLookupCell = Arc<tokio::sync::OnceCell<ApiKeyLookupResult>>;

/// Credential bounds are enforced both at the HTTP boundary and immediately
/// before database/Argon2 work. They are byte limits, which is what controls
/// memory and hashing cost for UTF-8 input.
pub const MAX_USERNAME_BYTES: usize = 100;
pub const MAX_PASSWORD_BYTES: usize = 1024;
pub const MIN_PASSWORD_CHARS: usize = 12;

/// Stable, client-safe password-policy failures. These messages describe the
/// proposed password only and never reveal whether an account exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PasswordPolicyError {
    #[error("password must be at least {MIN_PASSWORD_CHARS} characters")]
    TooShort,
    #[error("password must not exceed {MAX_PASSWORD_BYTES} bytes")]
    TooLong,
    #[error("password must contain at least one non-whitespace character")]
    WhitespaceOnly,
    #[error("password is too common; choose a less predictable passphrase")]
    Common,
}

impl PasswordPolicyError {
    pub fn code(self) -> &'static str {
        match self {
            Self::TooShort => "too_short",
            Self::TooLong => "too_long",
            Self::WhitespaceOnly => "whitespace_only",
            Self::Common => "common_password",
        }
    }
}

/// Apply one policy to every new password while leaving verification of
/// existing legacy hashes compatible. The byte cap is checked first, before
/// character counting or Argon2 work, and spaces remain valid in passphrases.
pub fn validate_password_policy(password: &str) -> Result<(), PasswordPolicyError> {
    if password.len() > MAX_PASSWORD_BYTES {
        return Err(PasswordPolicyError::TooLong);
    }
    if password.chars().count() < MIN_PASSWORD_CHARS {
        return Err(PasswordPolicyError::TooShort);
    }
    if password.chars().all(char::is_whitespace) {
        return Err(PasswordPolicyError::WhitespaceOnly);
    }

    const COMMON_PASSWORDS: &[&str] = &[
        "123456789012",
        "administrator",
        "changeme12345",
        "letmeinletmein",
        "password1234",
        "password12345",
        "qwertyuiop12",
        "welcome12345",
    ];
    let normalized = password.to_lowercase();
    if COMMON_PASSWORDS.contains(&normalized.as_str()) {
        return Err(PasswordPolicyError::Common);
    }
    Ok(())
}

const SESSION_IDLE_TIMEOUT_ENV: &str = "RUSH_SESSION_IDLE_TIMEOUT_SECS";
const SESSION_ABSOLUTE_TIMEOUT_ENV: &str = "RUSH_SESSION_ABSOLUTE_TIMEOUT_SECS";
const SESSION_RENEWAL_INTERVAL_ENV: &str = "RUSH_SESSION_RENEWAL_INTERVAL_SECS";
const DEFAULT_SESSION_IDLE_TIMEOUT_SECS: i64 = 30 * 60;
const DEFAULT_SESSION_ABSOLUTE_TIMEOUT_SECS: i64 = 24 * 60 * 60;
const DEFAULT_SESSION_RENEWAL_INTERVAL_SECS: i64 = 5 * 60;
const SESSION_ROTATION_GRACE_SECS: i64 = 60;
const MAX_SESSION_ABSOLUTE_TIMEOUT_SECS: i64 = 31 * 24 * 60 * 60;
const DEFAULT_KUBERNETES_ACCESS_RETENTION_DAYS: u16 = 30;
const MAX_KUBERNETES_ACCESS_RETENTION_DAYS: u16 = 3650;
const KUBERNETES_ACCESS_FULL_COLUMNS: &str = "id, tenant_id, cluster_id, gateway_id, session_id, actor_user_id, actor_name, actor_type, kube_username, kube_groups, source_kind, client_reported, observed_network, http_method, verb, api_group, api_version, resource, subresource, namespace, name, request_query, user_agent, status_code, duration_ms, request_bytes, response_bytes, result_summary, result_truncated, redaction_count, recording_state, created_at";
const KUBERNETES_ACCESS_COMPACT_COLUMNS: &str = "id, tenant_id, cluster_id, gateway_id, session_id, actor_user_id, actor_name, actor_type, kube_username, '[]' AS kube_groups, source_kind, '{}' AS client_reported, '{}' AS observed_network, http_method, verb, api_group, api_version, resource, subresource, namespace, name, '{}' AS request_query, '' AS user_agent, status_code, duration_ms, request_bytes, response_bytes, 'null' AS result_summary, result_truncated, redaction_count, recording_state, created_at";

fn kubernetes_access_columns(include_evidence: bool) -> &'static str {
    if include_evidence {
        KUBERNETES_ACCESS_FULL_COLUMNS
    } else {
        KUBERNETES_ACCESS_COMPACT_COLUMNS
    }
}

fn kubernetes_access_retention_days_from(raw: Option<&str>) -> u16 {
    raw.and_then(|value| value.trim().parse::<u16>().ok())
        .filter(|days| (1..=MAX_KUBERNETES_ACCESS_RETENTION_DAYS).contains(days))
        .unwrap_or(DEFAULT_KUBERNETES_ACCESS_RETENTION_DAYS)
}

fn kubernetes_access_retention_days() -> u16 {
    let raw = std::env::var("KUBERNETES_ACCESS_RETENTION_DAYS").ok();
    let days = kubernetes_access_retention_days_from(raw.as_deref());
    if raw.as_deref().is_some_and(|value| {
        value
            .trim()
            .parse::<u16>()
            .map_or(true, |configured| configured != days)
    }) {
        tracing::warn!(
            configured = raw.as_deref().unwrap_or_default(),
            fallback_days = days,
            "invalid KUBERNETES_ACCESS_RETENTION_DAYS; using the safe default"
        );
    }
    days
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionPolicy {
    pub idle_timeout_secs: i64,
    pub absolute_timeout_secs: i64,
    pub renewal_interval_secs: i64,
}

#[derive(Debug, Clone, clickhouse::Row, serde::Deserialize)]
pub struct KubernetesRbacGrantRow {
    pub id: String,
    pub tenant_id: String,
    pub group_id: String,
    pub cluster_id: String,
    pub cluster_match: String,
    pub cluster_pattern: String,
    pub name: String,
    pub role_kind: String,
    pub role_name: String,
    pub scope: String,
    pub namespaces: String,
    pub rules: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, clickhouse::Row, serde::Deserialize)]
pub struct KubernetesGatewayActivityRow {
    pub gateway_id: String,
    pub cluster_id: String,
    pub last_activity: String,
    pub recorded_requests: u64,
}

const KUBERNETES_RBAC_GRANT_COLUMNS: &str = "id, tenant_id, group_id, cluster_id, cluster_match, cluster_pattern, name, role_kind, role_name, scope, namespaces, rules, created_at, updated_at";

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut pattern_index, mut value_index) = (0, 0);
    let (mut last_star, mut star_value_index) = (None, 0);

    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            last_star = Some(pattern_index);
            pattern_index += 1;
            star_value_index = value_index;
        } else if let Some(star_index) = last_star {
            pattern_index = star_index + 1;
            star_value_index += 1;
            value_index = star_value_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

pub fn kubernetes_cluster_selector_matches(
    cluster_match: &str,
    cluster_id: &str,
    cluster_pattern: &str,
    candidate: &str,
) -> bool {
    match cluster_match {
        "single" | "" => cluster_id == candidate,
        "all" => true,
        "pattern" => wildcard_matches(cluster_pattern, candidate),
        _ => false,
    }
}

impl SessionPolicy {
    fn new(
        idle_timeout_secs: i64,
        absolute_timeout_secs: i64,
        renewal_interval_secs: i64,
    ) -> anyhow::Result<Self> {
        if !(60..=MAX_SESSION_ABSOLUTE_TIMEOUT_SECS).contains(&idle_timeout_secs) {
            anyhow::bail!(
                "{SESSION_IDLE_TIMEOUT_ENV} must be between 60 and {MAX_SESSION_ABSOLUTE_TIMEOUT_SECS} seconds"
            );
        }
        if !(idle_timeout_secs..=MAX_SESSION_ABSOLUTE_TIMEOUT_SECS).contains(&absolute_timeout_secs)
        {
            anyhow::bail!(
                "{SESSION_ABSOLUTE_TIMEOUT_ENV} must be at least the idle timeout and no more than {MAX_SESSION_ABSOLUTE_TIMEOUT_SECS} seconds"
            );
        }
        if !(30..idle_timeout_secs).contains(&renewal_interval_secs) {
            anyhow::bail!(
                "{SESSION_RENEWAL_INTERVAL_ENV} must be at least 30 seconds and less than the idle timeout"
            );
        }
        Ok(Self {
            idle_timeout_secs,
            absolute_timeout_secs,
            renewal_interval_secs,
        })
    }

    pub fn from_env() -> anyhow::Result<Self> {
        fn parse(name: &str, default: i64) -> anyhow::Result<i64> {
            std::env::var(name)
                .ok()
                .map(|value| {
                    value
                        .parse::<i64>()
                        .map_err(|_| anyhow::anyhow!("{name} must be an integer number of seconds"))
                })
                .transpose()
                .map(|value| value.unwrap_or(default))
        }

        Self::new(
            parse(SESSION_IDLE_TIMEOUT_ENV, DEFAULT_SESSION_IDLE_TIMEOUT_SECS)?,
            parse(
                SESSION_ABSOLUTE_TIMEOUT_ENV,
                DEFAULT_SESSION_ABSOLUTE_TIMEOUT_SECS,
            )?,
            parse(
                SESSION_RENEWAL_INTERVAL_ENV,
                DEFAULT_SESSION_RENEWAL_INTERVAL_SECS,
            )?,
        )
    }

    fn activity_touch_interval_secs(self) -> i64 {
        self.renewal_interval_secs
            .min((self.idle_timeout_secs / 2).max(30))
    }
}

#[derive(Debug, Clone)]
pub struct IssuedSession {
    pub token: String,
    pub max_age_seconds: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AuthSessionInfo {
    pub session_id: String,
    pub user_id: String,
    pub username: String,
    pub tenant_id: String,
    pub auth_method: String,
    pub provider_id: String,
    pub created_at: String,
    pub last_seen_at: String,
    pub idle_expires_at: String,
    pub absolute_expires_at: String,
    pub current: bool,
}

#[derive(Debug, Clone)]
pub struct RotatedSession {
    pub issued: IssuedSession,
    pub session_id: String,
    pub user_id: String,
    pub username: String,
    pub tenant_id: String,
}

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct AuthSessionRow {
    session_id: String,
    user_id: String,
    username: String,
    tenant_id: String,
    auth_method: String,
    provider_id: String,
    created_at: String,
    last_seen_at: String,
    expires_at: String,
    absolute_expires_at: String,
    token: String,
}

impl AuthSessionRow {
    fn into_info(self, session_hmac_secret: &[u8], current_token: &str) -> AuthSessionInfo {
        fn utc_api_time(value: String) -> String {
            if value.ends_with('Z') {
                value
            } else {
                format!("{}Z", value.replace(' ', "T"))
            }
        }
        AuthSessionInfo {
            session_id: self.session_id,
            user_id: self.user_id,
            username: self.username,
            tenant_id: self.tenant_id,
            auth_method: self.auth_method,
            provider_id: self.provider_id,
            created_at: utc_api_time(self.created_at),
            last_seen_at: utc_api_time(self.last_seen_at),
            idle_expires_at: utc_api_time(self.expires_at),
            absolute_expires_at: utc_api_time(self.absolute_expires_at),
            current: openssl::memcmp::eq(
                self.token.as_bytes(),
                session_storage_key(session_hmac_secret, current_token).as_bytes(),
            ),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("username is already in use")]
pub struct UsernameAlreadyExists;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PasswordChangeOutcome {
    Updated,
    UserNotFound,
    SsoManaged { auth_provider: String },
}

/// Server-owned authorization attached to a hashed API key. The plaintext key
/// is returned only once at creation and never stored here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiKeyGrant {
    pub id: String,
    pub tenant_id: String,
    /// `query`, `ingest`, or `legacy` for keys created before QAPI-SEC-04.
    pub key_type: String,
    pub signals: Vec<String>,
    pub rate_limit_per_minute: u64,
    pub source_cidrs: Vec<String>,
}

/// TTL for the config-plane read caches below. Mutations through this process
/// clear the caches immediately; mutations from another replica are visible
/// after at most this long. Keep it short — these exist to absorb the
/// per-request auth fan-out, not to be a long-lived cache.
const CONFIG_CACHE_TTL: Duration = Duration::from_secs(30);
/// Keep attacker-controlled tenant names and user ids from growing the
/// process-local config caches without bound. Entries are still refreshed on
/// demand when a cache is full, so this limit changes memory use, not auth
/// semantics.
const MAX_CONFIG_CACHE_ENTRIES: usize = 8_192;
const CONFIG_CACHE_MAINTENANCE_EVERY: u64 = 1_024;

const SSO_CLAIM_STORE_ENV: &str = "RUSH_SSO_REPLAY_STORE";
const QUERY_API_REPLICAS_ENV: &str = "RUSH_QUERY_API_REPLICAS";
const SSO_CLAIM_KEEPER_TABLE: &str = "config_sso_one_time_claims";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SsoClaimStoreMode {
    Local,
    Keeper,
}

fn sso_claim_store_mode(raw: Option<&str>, replicas: usize) -> anyhow::Result<SsoClaimStoreMode> {
    let mode = raw.unwrap_or("auto").trim().to_ascii_lowercase();
    match mode.as_str() {
        "auto" if replicas > 1 => Ok(SsoClaimStoreMode::Keeper),
        "auto" => Ok(SsoClaimStoreMode::Local),
        "local" if replicas > 1 => anyhow::bail!(
            "{SSO_CLAIM_STORE_ENV}=local is unsafe with {QUERY_API_REPLICAS_ENV}={replicas}; use keeper"
        ),
        "local" => Ok(SsoClaimStoreMode::Local),
        "keeper" => Ok(SsoClaimStoreMode::Keeper),
        _ => anyhow::bail!("{SSO_CLAIM_STORE_ENV} must be one of: auto, local, keeper"),
    }
}

fn configured_sso_claim_store_mode() -> anyhow::Result<SsoClaimStoreMode> {
    let replicas = std::env::var(QUERY_API_REPLICAS_ENV)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .ok()
                .filter(|count| *count > 0)
                .ok_or_else(|| {
                    anyhow::anyhow!("{QUERY_API_REPLICAS_ENV} must be a positive integer")
                })
        })
        .transpose()?
        .unwrap_or(1);
    sso_claim_store_mode(std::env::var(SSO_CLAIM_STORE_ENV).ok().as_deref(), replicas)
}

fn sso_claim_storage_key(key: &str) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(key.as_bytes())))
}

fn consume_local_sso_claim(
    claims: &DashMap<String, i64>,
    claim_key: String,
    expires_at: i64,
    now: i64,
) -> bool {
    if expires_at <= now {
        return false;
    }
    if claims.len() > 10_000 {
        claims.retain(|_, expiry| *expiry > now);
    }
    match claims.entry(claim_key) {
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(expires_at);
            true
        }
        dashmap::mapref::entry::Entry::Occupied(_) => false,
    }
}

/// SLO history is an incident timeline, not an evaluation log. Keep one breach
/// followed by one recovery and discard no-data or duplicate state rows. Input
/// is newest-first (the order returned by ClickHouse); output preserves that
/// order and is capped to the requested API limit.
fn normalize_slo_incident_events(
    events: Vec<crate::models::slo::SloEvent>,
    limit: usize,
) -> Vec<crate::models::slo::SloEvent> {
    let mut incident_open = false;
    let mut normalized = Vec::new();

    for event in events.into_iter().rev() {
        match event.state.as_str() {
            "breaching" if !incident_open => {
                incident_open = true;
                normalized.push(event);
            }
            "compliant" if incident_open => {
                incident_open = false;
                normalized.push(event);
            }
            _ => {}
        }
    }

    normalized.into_iter().rev().take(limit).collect()
}

#[cfg(test)]
mod auth_storage_tests {
    use super::*;

    #[test]
    fn kubernetes_access_retention_is_always_bounded() {
        assert_eq!(kubernetes_access_retention_days_from(None), 30);
        assert_eq!(kubernetes_access_retention_days_from(Some("1")), 1);
        assert_eq!(kubernetes_access_retention_days_from(Some("3650")), 3650);
        assert_eq!(kubernetes_access_retention_days_from(Some("0")), 30);
        assert_eq!(kubernetes_access_retention_days_from(Some("3651")), 30);
        assert_eq!(kubernetes_access_retention_days_from(Some("invalid")), 30);
    }

    #[test]
    fn kubernetes_cluster_selectors_match_exact_all_and_wildcards() {
        assert!(kubernetes_cluster_selector_matches(
            "single",
            "west-production",
            "",
            "west-production"
        ));
        assert!(!kubernetes_cluster_selector_matches(
            "single",
            "east-production",
            "",
            "west-production"
        ));
        assert!(kubernetes_cluster_selector_matches(
            "all",
            "",
            "",
            "any-cluster"
        ));
        assert!(kubernetes_cluster_selector_matches(
            "pattern",
            "",
            "*-production",
            "west-production"
        ));
        assert!(!kubernetes_cluster_selector_matches(
            "pattern",
            "",
            "*-production",
            "production-west"
        ));
        assert!(kubernetes_cluster_selector_matches(
            "pattern", "", "prod-?", "prod-a"
        ));
        assert!(!kubernetes_cluster_selector_matches(
            "unknown",
            "west-production",
            "*",
            "west-production"
        ));
    }

    #[test]
    fn kubernetes_access_tables_create_and_migrate_retention_and_actor_type() {
        let source = include_str!("clickhouse_config.rs");
        assert!(source.contains(
            "TTL parseDateTimeBestEffort(created_at) + INTERVAL {access_retention_days} DAY DELETE"
        ));
        assert!(source.contains(
            "ALTER TABLE config_kubernetes_access_events MODIFY TTL parseDateTimeBestEffort(created_at)"
        ));
        assert!(source.contains(
            "ALTER TABLE config_kubernetes_session_chunks MODIFY TTL parseDateTimeBestEffort(created_at)"
        ));
        assert!(source.contains(
            "ADD COLUMN IF NOT EXISTS actor_type LowCardinality(String) DEFAULT 'unknown'"
        ));
        assert!(source.contains("CREATE TABLE IF NOT EXISTS config_kubernetes_login_requests"));
        assert!(source.contains("CREATE TABLE IF NOT EXISTS config_kubernetes_login_revocations"));
        assert!(source.contains("device_code_hash     String"));
        assert!(source.contains("ADD COLUMN IF NOT EXISTS client_reported String DEFAULT '{}'"));
        assert!(!source.contains(&["device_code", "          String"].concat()));
    }

    #[test]
    fn kubernetes_access_list_projection_omits_captured_evidence() {
        let compact = kubernetes_access_columns(false);
        assert!(compact.contains("'{}' AS client_reported"));
        assert!(compact.contains("'{}' AS observed_network"));
        assert!(compact.contains("'{}' AS request_query"));
        assert!(compact.contains("'null' AS result_summary"));

        let full = kubernetes_access_columns(true);
        assert!(full.contains("client_reported"));
        assert!(full.contains("observed_network"));
        assert!(full.contains("request_query"));
        assert!(full.contains("result_summary"));
        assert!(!full.contains(" AS result_summary"));
    }

    #[test]
    fn kubernetes_access_free_text_search_includes_event_id() {
        let source = include_str!("clickhouse_config.rs");
        assert!(source.contains("positionCaseInsensitive(id, ?) > 0"));
    }

    #[test]
    fn kubernetes_gateway_activity_is_tenant_scoped() {
        let source = include_str!("clickhouse_config.rs");
        assert!(source.contains(
            "WHERE tenant_id = ? AND gateway_id != '' AND cluster_id != '' GROUP BY gateway_id, cluster_id"
        ));
    }

    #[test]
    fn api_key_auth_uses_hash_ordered_lookup_with_tombstones() {
        let source = include_str!("clickhouse_config.rs");
        assert!(source.contains("CREATE TABLE IF NOT EXISTS config_api_keys_by_hash"));
        assert!(source.contains("ORDER BY (key_hash)"));
        assert!(
            source.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS config_api_keys_by_hash_mv")
        );
        assert!(source.contains("FROM config_api_keys_by_hash FINAL WHERE key_hash = ? LIMIT 1"));
        assert!(source.contains("Ok(row) if row.is_deleted == 0"));
    }

    #[test]
    fn session_auth_coalesces_user_role_and_sso_policy_into_one_query() {
        let source = include_str!("clickhouse_config.rs");
        assert!(
            source.contains("FROM config_sessions s\n                   JOIN config_users u FINAL")
        );
        assert!(source.contains("LEFT JOIN (\n                       SELECT ug.user_id"));
        assert!(source.contains("FROM config_sso_active_provider FINAL"));
        assert!(source.contains("ifNull(roles.role, 'viewer') AS role"));
    }

    #[test]
    fn config_plane_caches_have_expiry_maintenance_and_a_hard_insert_cap() {
        let source = include_str!("clickhouse_config.rs");
        assert!(source.contains("const MAX_CONFIG_CACHE_ENTRIES: usize = 8_192"));
        assert!(source.contains("fn maintain_config_caches(&self)"));
        assert!(source.contains(".retain(|_, (_, cached_at)| Self::cache_fresh(*cached_at))"));
        assert!(source.contains("self.signal_cache.len() < MAX_CONFIG_CACHE_ENTRIES"));
    }

    #[test]
    fn session_policy_enforces_idle_absolute_and_rotation_relationships() {
        assert_eq!(
            SessionPolicy::new(1_800, 86_400, 300).unwrap(),
            SessionPolicy {
                idle_timeout_secs: 1_800,
                absolute_timeout_secs: 86_400,
                renewal_interval_secs: 300,
            }
        );
        assert!(SessionPolicy::new(59, 86_400, 30).is_err());
        assert!(SessionPolicy::new(1_800, 1_799, 300).is_err());
        assert!(SessionPolicy::new(1_800, 86_400, 1_800).is_err());
        assert_eq!(
            SessionPolicy::new(1_800, 86_400, 300)
                .unwrap()
                .activity_touch_interval_secs(),
            300
        );
        assert_eq!(
            SessionPolicy::new(60, 3_600, 30)
                .unwrap()
                .activity_touch_interval_secs(),
            30
        );
    }

    #[test]
    fn password_policy_covers_boundaries_unicode_whitespace_and_common_values() {
        let exact_minimum = "a".repeat(MIN_PASSWORD_CHARS);
        assert_eq!(validate_password_policy(&exact_minimum), Ok(()));
        assert_eq!(
            validate_password_policy(&"a".repeat(MIN_PASSWORD_CHARS - 1)),
            Err(PasswordPolicyError::TooShort)
        );

        let unicode = "🔐".repeat(MIN_PASSWORD_CHARS);
        assert_eq!(unicode.chars().count(), MIN_PASSWORD_CHARS);
        assert_eq!(validate_password_policy(&unicode), Ok(()));

        assert_eq!(
            validate_password_policy(&" ".repeat(MIN_PASSWORD_CHARS)),
            Err(PasswordPolicyError::WhitespaceOnly)
        );
        assert_eq!(
            validate_password_policy("  a long passphrase is valid  "),
            Ok(())
        );
        assert_eq!(
            validate_password_policy("Password12345"),
            Err(PasswordPolicyError::Common)
        );

        assert_eq!(
            validate_password_policy(&"z".repeat(MAX_PASSWORD_BYTES)),
            Ok(())
        );
        let oversized = "x".repeat(MAX_PASSWORD_BYTES + 1);
        assert_eq!(
            validate_password_policy(&oversized),
            Err(PasswordPolicyError::TooLong)
        );
        assert!(!verify_password(&oversized, dummy_password_hash()));
    }

    #[test]
    fn password_hashing_uses_the_shared_policy_but_legacy_hashes_still_verify() {
        let accepted = "new secure passphrase";
        let hash = hash_password(accepted).unwrap();
        assert!(verify_password(accepted, &hash));

        let salt = SaltString::encode_b64(b"rush-legacy-password-salt")
            .expect("static legacy salt must be valid");
        let legacy_hash = Argon2::default()
            .hash_password(b"short", &salt)
            .unwrap()
            .to_string();
        assert!(verify_password("short", &legacy_hash));
        assert_eq!(
            validate_password_policy("short"),
            Err(PasswordPolicyError::TooShort)
        );
    }

    #[test]
    fn every_password_setting_path_reaches_the_shared_policy() {
        let source = include_str!("clickhouse_config.rs");
        let hasher = source
            .split_once("\nfn hash_password(password: &str)")
            .map(|(_, body)| body)
            .expect("password hasher must exist")
            .split("fn canonical_username")
            .next()
            .unwrap();
        assert!(hasher.contains("validate_password_policy(password)?"));

        for function in [
            "pub async fn ensure_default_admin",
            "pub async fn create_user",
            "pub async fn change_password",
        ] {
            let marker = format!("\n    {function}");
            let body = source
                .split_once(&marker)
                .map(|(_, body)| body)
                .expect("password-setting function must exist")
                .split("\n    pub async fn")
                .next()
                .unwrap();
            assert!(
                body.contains("hash_password("),
                "{function} bypasses policy"
            );
        }
    }

    #[test]
    fn session_storage_tracks_both_deadlines_and_rotates_without_exposing_bearers() {
        let source = include_str!("clickhouse_config.rs");
        let migration = source
            .rsplit_once("CREATE TABLE IF NOT EXISTS config_sessions")
            .map(|(_, migration)| migration)
            .expect("session schema must exist")
            .split("CREATE TABLE IF NOT EXISTS config_login_attempts")
            .next()
            .expect("login-attempt schema must follow sessions");
        assert!(migration.contains("session_id"));
        assert!(migration.contains("last_seen_at"));
        assert!(migration.contains("absolute_expires_at"));
        assert!(migration.contains("DELETE WHERE session_id = ''"));
        assert!(migration.contains("CREATE TABLE IF NOT EXISTS config_session_revocations"));
        assert!(migration.contains("CREATE TABLE IF NOT EXISTS config_session_rotation_grace"));

        let rotation = source
            .rsplit_once("pub async fn rotate_session_if_due")
            .map(|(_, method)| method)
            .expect("session rotation must exist")
            .split("pub async fn delete_session")
            .next()
            .expect("logout deletion must follow rotation");
        let insert_position = rotation
            .find("INSERT INTO config_sessions")
            .expect("rotation must write the replacement bearer");
        let grace_position = rotation
            .find("INSERT INTO config_session_rotation_grace")
            .expect("rotation must supersede the old bearer after a grace period");
        assert!(insert_position < grace_position);
        assert!(rotation.contains("session_storage_key(&self.session_hmac_secret, token)"));
        assert!(rotation.contains("SESSION_ROTATION_GRACE_SECS"));
        assert!(!rotation.contains("DELETE FROM config_sessions WHERE token = ?"));
        assert!(!rotation.contains(".bind(token)"));

        let logout = source
            .rsplit_once("pub async fn delete_session(&self")
            .map(|(_, method)| method)
            .expect("logout session deletion must exist")
            .split("pub async fn record_login_ip_attempt")
            .next()
            .expect("login attempt storage must follow logout deletion");
        let tombstone = logout
            .find("INSERT INTO config_session_revocations")
            .expect("logout must persist a session tombstone");
        let delete = logout
            .find("DELETE FROM config_sessions WHERE session_id = ?")
            .expect("logout must delete every bearer for the public session id");
        assert!(tombstone < delete);
    }

    #[test]
    fn legacy_sso_provider_reconciliation_adopts_only_one_enabled_provider() {
        let resolution = resolve_legacy_active_sso_provider(vec!["provider-a".to_string()]);
        assert_eq!(resolution.active_provider_id.as_deref(), Some("provider-a"));
        assert!(resolution.ambiguous_provider_ids.is_empty());
        assert!(resolution.changed);
    }

    #[test]
    fn legacy_sso_provider_reconciliation_fails_closed_on_ambiguity() {
        let resolution = resolve_legacy_active_sso_provider(vec![
            "provider-b".to_string(),
            "provider-a".to_string(),
            "provider-b".to_string(),
        ]);
        assert_eq!(resolution.active_provider_id, None);
        assert_eq!(
            resolution.ambiguous_provider_ids,
            vec!["provider-a".to_string(), "provider-b".to_string()]
        );
        assert!(resolution.changed);
    }

    #[test]
    fn enabled_sso_provider_lookup_uses_the_singleton_pointer() {
        let source = include_str!("clickhouse_config.rs");
        let lookup = source
            .rsplit_once("pub async fn get_enabled_sso_provider")
            .map(|(_, lookup)| lookup)
            .expect("enabled-provider lookup must exist")
            .split("/// Validate the configured encryption key")
            .next()
            .expect("secret migration must follow enabled-provider lookup");
        assert!(lookup.contains("effective_active_sso_provider_id"));
        assert!(!lookup.contains("WHERE enabled = 1 AND is_deleted = 0 LIMIT 1"));
    }

    #[test]
    fn sso_provider_mutations_maintain_the_singleton_pointer() {
        let source = include_str!("clickhouse_config.rs");
        let upsert = source
            .rsplit_once("pub async fn upsert_sso_provider")
            .map(|(_, upsert)| upsert)
            .expect("SSO provider upsert must exist")
            .split("pub async fn delete_sso_provider")
            .next()
            .expect("SSO provider delete must follow upsert");
        assert!(upsert.contains("if enabled"));
        assert!(upsert.contains("set_active_sso_provider_id(id)"));
        assert!(upsert.contains("set_active_sso_provider_id(\"\")"));

        let delete = source
            .rsplit_once("pub async fn delete_sso_provider")
            .map(|(_, delete)| delete)
            .expect("SSO provider delete must exist")
            .split("// ── IdP group mapping operations")
            .next()
            .expect("group mapping operations must follow provider delete");
        assert!(delete.contains("if was_active"));
        assert!(delete.contains("set_active_sso_provider_id(\"\")"));
    }

    #[test]
    fn session_storage_uses_a_one_way_key_not_the_bearer_value() {
        let raw = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let secret = b"01234567890123456789012345678901";
        let stored = session_storage_key(secret, raw);
        assert!(stored.starts_with(SESSION_HMAC_PREFIX));
        assert_eq!(stored, session_storage_key(secret, raw));
        assert_ne!(stored, raw);
        assert!(!stored.contains(raw));

        let other_secret = b"abcdefghijklmnopqrstuvwxyzABCDEF";
        assert_ne!(stored, session_storage_key(other_secret, raw));

        let source = include_str!("clickhouse_config.rs");
        let migration = source
            .rsplit_once("pub async fn invalidate_legacy_session_tokens")
            .map(|(_, method)| method)
            .expect("legacy session migration must exist")
            .split("async fn insert_session")
            .next()
            .expect("session insertion must follow migration");
        assert!(migration.contains("NOT startsWith(token, ?)"));
        assert!(migration.contains("lightweight_deletes_sync"));
    }

    #[test]
    fn session_lookup_and_deletion_never_send_the_raw_bearer_to_storage() {
        let source = include_str!("clickhouse_config.rs");
        let validation = source
            .split_once("\n    pub async fn get_session_user(")
            .map(|(_, method)| method)
            .expect("session validation must exist")
            .split("\n    pub async fn delete_session(")
            .next()
            .expect("session deletion must follow validation");
        assert!(validation.contains("session_storage_key(&self.session_hmac_secret, token)"));
        assert!(validation.contains(".bind(&stored_token)"));
        assert!(!validation.contains(".bind(token)"));
        assert!(!validation.contains("Rolling-upgrade compatibility"));

        let deletion = source
            .split_once("\n    pub async fn delete_session(")
            .map(|(_, method)| method)
            .expect("session deletion must exist")
            .split("\n    pub async fn record_login_ip_attempt(")
            .next()
            .expect("login attempt storage must follow session deletion");
        assert!(deletion.contains("session_storage_key(&self.session_hmac_secret, token)"));
        assert!(deletion.contains("WHERE token = ?"));
        assert!(deletion.contains(".bind(&stored_token)"));
        assert!(!deletion.contains("OR token"));
        assert!(!deletion.contains(".bind(token)"));
    }

    #[test]
    fn sso_claim_store_requires_shared_coordination_for_multiple_replicas() {
        assert_eq!(
            sso_claim_store_mode(Some("auto"), 1).unwrap(),
            SsoClaimStoreMode::Local
        );
        assert_eq!(
            sso_claim_store_mode(Some("auto"), 2).unwrap(),
            SsoClaimStoreMode::Keeper
        );
        assert!(sso_claim_store_mode(Some("local"), 2).is_err());
        assert_eq!(
            sso_claim_store_mode(Some("keeper"), 1).unwrap(),
            SsoClaimStoreMode::Keeper
        );
    }

    #[test]
    fn sso_claim_keys_are_one_way_and_domain_separated_by_the_caller() {
        let raw = "saml-assertion:assertion-secret-id";
        let stored = sso_claim_storage_key(raw);
        assert!(stored.starts_with("sha256:"));
        assert_eq!(stored, sso_claim_storage_key(raw));
        assert_ne!(stored, raw);
        assert!(!stored.contains("assertion-secret-id"));
        assert_ne!(
            stored,
            sso_claim_storage_key("saml-response:assertion-secret-id")
        );
    }

    #[test]
    fn setup_session_revocations_are_persistent_and_store_only_one_way_keys() {
        let source = include_str!("clickhouse_config.rs");
        assert!(source.contains("CREATE TABLE IF NOT EXISTS config_sso_setup_revocations"));
        assert!(source.contains("TTL toDateTime(expires_at)"));

        let write = source
            .split_once("\n    pub async fn revoke_sso_setup_session(")
            .map(|(_, method)| method)
            .expect("durable setup revocation write must exist")
            .split("\n    pub async fn is_sso_setup_session_revoked(")
            .next()
            .expect("revocation lookup must follow the write");
        assert!(write.contains("INSERT INTO config_sso_setup_revocations"));
        assert!(write.contains(".bind(sso_claim_storage_key(key))"));
        assert!(!write.contains(".bind(key)"));

        let lookup = source
            .split_once("\n    pub async fn is_sso_setup_session_revoked(")
            .map(|(_, method)| method)
            .expect("durable setup revocation lookup must exist")
            .split("\n    async fn ensure_username_available(")
            .next()
            .expect("username validation must follow revocation lookup");
        assert!(lookup.contains("expires_at > ?"));
        assert!(lookup.contains(".bind(sso_claim_storage_key(key))"));
        assert!(!lookup.contains(".bind(key)"));
    }

    #[test]
    fn local_sso_claims_are_consumed_once_and_expired_input_fails_closed() {
        let claims = DashMap::new();
        assert!(consume_local_sso_claim(
            &claims,
            "claim-a".to_string(),
            1_100,
            1_000,
        ));
        assert!(!consume_local_sso_claim(
            &claims,
            "claim-a".to_string(),
            1_100,
            1_001,
        ));
        assert!(!consume_local_sso_claim(
            &claims,
            "claim-expired".to_string(),
            999,
            1_000,
        ));
    }

    #[test]
    fn bootstrap_logging_never_interpolates_the_initial_password() {
        let source = include_str!("clickhouse_config.rs");
        let bootstrap = source
            .rsplit_once("pub async fn ensure_default_admin")
            .map(|(_, method)| method)
            .expect("default-admin bootstrap method must exist")
            .split("pub async fn authenticate")
            .next()
            .expect("authentication must follow bootstrap");
        assert!(bootstrap.contains("INITIAL_ADMIN_PASSWORD is required"));
        let logging = bootstrap
            .split_once("tracing::warn!")
            .map(|(_, logging)| logging)
            .expect("bootstrap creation must leave an operational log");
        assert!(
            !logging.contains("initial_password"),
            "bootstrap logging must never reference the password variable"
        );
    }

    #[test]
    fn usernames_use_one_trimmed_case_insensitive_identity() {
        assert_eq!(
            canonical_username("  Alice@Example.COM  "),
            "alice@example.com"
        );
        assert_eq!(canonical_username("ADMIN"), canonical_username("admin"));
    }

    #[test]
    fn missing_identity_dummy_hash_performs_real_argon2_verification() {
        let hash = dummy_password_hash();
        assert!(PasswordHash::new(hash).is_ok());
        assert!(verify_password("not-a-real-rush-password", hash));
        assert!(!verify_password("attacker-input", hash));
    }

    #[test]
    fn authentication_is_canonical_and_fails_closed_on_collisions() {
        let source = include_str!("clickhouse_config.rs");
        let authentication = source
            .rsplit_once("pub async fn authenticate")
            .map(|(_, authentication)| authentication)
            .expect("authentication method must exist")
            .split("async fn derive_user_role")
            .next()
            .expect("role derivation must follow authentication");
        assert!(authentication.contains("lowerUTF8(trimBoth(username)) = lowerUTF8(trimBoth(?))"));
        assert!(authentication.contains("LIMIT 2"));
        assert!(authentication.contains("dummy_password_hash()"));
        assert!(authentication.contains("if rows.len() > 1"));
    }

    #[test]
    fn user_rewrites_preserve_sso_identity_and_password_changes_reject_it() {
        let source = include_str!("clickhouse_config.rs");
        let password_change = source
            .rsplit_once("pub async fn change_password")
            .map(|(_, method)| method)
            .expect("password change method must exist")
            .split("pub async fn delete_sessions_for_user")
            .next()
            .expect("session deletion must follow password change");
        assert!(password_change.contains("if row.auth_provider != \"local\""));
        assert!(password_change.contains("PasswordChangeOutcome::SsoManaged"));
        assert!(password_change.contains(".bind(&row.auth_provider)"));
        assert!(password_change.contains(".bind(&row.external_id)"));
        assert!(password_change.contains("sessions are already invalid"));
        assert!(password_change.contains("if let Err(error)"));

        let enabled_change = source
            .rsplit_once("pub async fn set_user_enabled")
            .map(|(_, method)| method)
            .expect("enabled mutation must exist")
            .split("pub async fn get_username")
            .next()
            .expect("username lookup must follow enabled mutation");
        assert!(enabled_change.contains(".bind(&row.auth_provider)"));
        assert!(enabled_change.contains(".bind(&row.external_id)"));
    }

    #[test]
    fn sessions_are_bound_to_the_authenticated_user_version() {
        let source = include_str!("clickhouse_config.rs");
        let session_create = source
            .rsplit_once("async fn insert_session")
            .map(|(_, method)| method)
            .expect("session insertion method must exist")
            .split("pub async fn get_session_user")
            .next()
            .expect("session validation must follow session insertion");
        assert!(session_create.contains("user_version"));
        assert!(session_create.contains("create_session_at_version"));

        let session_validation = source
            .rsplit_once("pub async fn get_session_user")
            .map(|(_, method)| method)
            .expect("session validation method must exist")
            .split("pub async fn delete_session")
            .next()
            .expect("session deletion must follow validation");
        assert!(session_validation.contains("s.user_version = u.version"));

        let migration = source
            .rsplit_once("async fn run_migrations")
            .map(|(_, method)| method)
            .expect("config migrations must exist")
            .split("// ── Helpers")
            .next()
            .expect("migration list must precede helpers");
        assert!(migration.contains("ADD COLUMN IF NOT EXISTS user_version"));
        assert!(migration.contains("DELETE WHERE user_version = 0"));
    }

    #[test]
    fn sessions_are_bound_to_their_authentication_provider() {
        let source = include_str!("clickhouse_config.rs");
        let session_create = source
            .rsplit_once("async fn insert_session")
            .map(|(_, method)| method)
            .expect("session insertion method must exist")
            .split("pub async fn get_session_user")
            .next()
            .expect("session validation must follow insertion");
        assert!(session_create.contains("auth_method"));
        assert!(session_create.contains("provider_id"));
        assert!(session_create.contains("create_sso_session"));
        assert!(session_create.contains("\"local\", \"\""));

        let validation = source
            .rsplit_once("pub async fn get_session_user")
            .map(|(_, method)| method)
            .expect("session validation must exist")
            .split("pub async fn delete_session")
            .next()
            .expect("session deletion must follow validation");
        assert!(validation.contains("effective_active_sso_provider_id"));
        assert!(validation.contains("active_provider_id.as_deref()"));
        assert!(validation.contains("_ => return None"));

        let migration = source
            .rsplit_once("async fn run_migrations")
            .map(|(_, method)| method)
            .expect("config migrations must exist")
            .split("// ── Helpers")
            .next()
            .expect("migration list must precede helpers");
        assert!(migration.contains("ADD COLUMN IF NOT EXISTS auth_method"));
        assert!(migration.contains("ADD COLUMN IF NOT EXISTS provider_id"));
    }

    #[test]
    fn sso_secret_encryption_round_trips_and_uses_random_nonces() {
        let key = config_encryption_key_from_secret(
            "0123456789abcdef0123456789abcdef-extra-key-material",
        )
        .unwrap();
        let first = encrypt_sso_secret_with_key("client-secret", &key).unwrap();
        let second = encrypt_sso_secret_with_key("client-secret", &key).unwrap();
        assert!(first.starts_with(ENCRYPTED_SECRET_PREFIX));
        assert_ne!(first, second);
        assert_eq!(
            decrypt_sso_secret_with_key(&first, &key).unwrap(),
            "client-secret"
        );
    }

    #[test]
    fn sso_secret_envelope_rejects_tampering_and_wrong_keys() {
        let key = config_encryption_key_from_secret(
            "0123456789abcdef0123456789abcdef-extra-key-material",
        )
        .unwrap();
        let wrong_key = config_encryption_key_from_secret(
            "fedcba9876543210fedcba9876543210-extra-key-material",
        )
        .unwrap();
        let encrypted = encrypt_sso_secret_with_key("client-secret", &key).unwrap();
        let mut tampered = encrypted.clone().into_bytes();
        let tag_start = encrypted.rfind('.').unwrap() + 1;
        tampered[tag_start] = if tampered[tag_start] == b'A' {
            b'B'
        } else {
            b'A'
        };
        let tampered = String::from_utf8(tampered).unwrap();

        assert!(decrypt_sso_secret_with_key(&tampered, &key).is_err());
        assert!(decrypt_sso_secret_with_key(&encrypted, &wrong_key).is_err());
    }

    #[test]
    fn legacy_plaintext_secret_is_read_only_for_startup_migration() {
        let key = config_encryption_key_from_secret(
            "0123456789abcdef0123456789abcdef-extra-key-material",
        )
        .unwrap();
        assert_eq!(
            decrypt_sso_secret_with_key("legacy-secret", &key).unwrap(),
            "legacy-secret"
        );
    }
}

#[cfg(test)]
mod slo_event_tests {
    use super::normalize_slo_incident_events;
    use crate::models::slo::SloEvent;

    fn event(id: &str, state: &str, created_at: &str) -> SloEvent {
        SloEvent {
            id: id.to_string(),
            slo_id: "slo-1".to_string(),
            tenant_id: "default".to_string(),
            state: state.to_string(),
            error_count: 0,
            total_count: 100,
            error_budget_remaining: 1.0,
            message: state.to_string(),
            created_at: created_at.to_string(),
        }
    }

    #[test]
    fn normalizes_legacy_history_into_breach_recovery_pairs() {
        // Input matches ClickHouse: newest event first.
        let events = vec![
            event("c3", "compliant", "2026-01-06"),
            event("c2", "compliant", "2026-01-05"),
            event("n2", "no_data", "2026-01-04"),
            event("b2", "breaching", "2026-01-03"),
            event("c1", "compliant", "2026-01-02"),
            event("b1", "breaching", "2026-01-01"),
        ];

        let normalized = normalize_slo_incident_events(events, 100);
        let states: Vec<_> = normalized
            .iter()
            .map(|event| (event.id.as_str(), event.state.as_str()))
            .collect();

        assert_eq!(
            states,
            vec![
                ("c2", "compliant"),
                ("b2", "breaching"),
                ("c1", "compliant"),
                ("b1", "breaching"),
            ]
        );
    }

    #[test]
    fn drops_orphan_compliant_and_no_data_events() {
        let events = vec![
            event("c2", "compliant", "2026-01-03"),
            event("n1", "no_data", "2026-01-02"),
            event("c1", "compliant", "2026-01-01"),
        ];

        assert!(normalize_slo_incident_events(events, 100).is_empty());
    }
}

fn hash_password(password: &str) -> anyhow::Result<String> {
    validate_password_policy(password)?;
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| anyhow::anyhow!("argon2 hash failed: {e}"))
}

fn canonical_username(username: &str) -> String {
    username.trim().to_lowercase()
}

/// A process-wide, precomputed Argon2 hash used when an account is absent or
/// cannot use local authentication. Verifying it keeps the expensive portion
/// of the login path equivalent without creating a fresh hash on every miss.
fn dummy_password_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        let salt = SaltString::encode_b64(b"rush-login-dummy-salt")
            .expect("static dummy password salt must be valid");
        Argon2::default()
            .hash_password(b"not-a-real-rush-password", &salt)
            .expect("static dummy password hash must be constructible")
            .to_string()
    })
}

fn verify_password(password: &str, hash: &str) -> bool {
    if password.is_empty() || password.len() > MAX_PASSWORD_BYTES {
        return false;
    }
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

const ENCRYPTED_SECRET_PREFIX: &str = "enc:v1:";
const CONFIG_ENCRYPTION_CONTEXT: &[u8] = b"rush-config-encryption-v1\0";
const SSO_SECRET_AAD: &[u8] = b"rush-sso-client-secret-v1";

const SESSION_HMAC_PREFIX: &str = "hmac-sha256:v1:";

fn session_hmac_secret_from_env() -> anyhow::Result<Vec<u8>> {
    let secret = std::env::var("RUSH_SESSION_HMAC_SECRET")
        .or_else(|_| std::env::var("RUSH_API_KEY_SECRET"))
        .or_else(|_| std::env::var("RUSH_AUDIT_HMAC_SECRET"))
        .map_err(|_| {
            anyhow::anyhow!(
                "RUSH_SESSION_HMAC_SECRET is required (API-key or audit HMAC secrets are accepted as compatibility fallbacks)"
            )
        })?;
    if secret.len() < 32 {
        anyhow::bail!("RUSH_SESSION_HMAC_SECRET must contain at least 32 bytes");
    }
    Ok(secret.into_bytes())
}

fn session_storage_key(secret: &[u8], token: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(token.as_bytes());
    format!(
        "{SESSION_HMAC_PREFIX}{}",
        hex::encode(mac.finalize().into_bytes())
    )
}

fn config_encryption_key_from_secret(secret: &str) -> anyhow::Result<[u8; 32]> {
    if secret.len() < 32 {
        anyhow::bail!("RUSH_CONFIG_ENCRYPTION_KEY must contain at least 32 bytes");
    }
    let mut digest = Sha256::new();
    digest.update(CONFIG_ENCRYPTION_CONTEXT);
    digest.update(secret.as_bytes());
    Ok(digest.finalize().into())
}

fn config_encryption_key() -> anyhow::Result<[u8; 32]> {
    let secret = std::env::var("RUSH_CONFIG_ENCRYPTION_KEY").map_err(|_| {
        anyhow::anyhow!(
            "RUSH_CONFIG_ENCRYPTION_KEY is required to store or read SSO client secrets"
        )
    })?;
    config_encryption_key_from_secret(&secret)
}

fn encrypt_sso_secret_with_key(plaintext: &str, key: &[u8; 32]) -> anyhow::Result<String> {
    if plaintext.is_empty() {
        return Ok(String::new());
    }
    let nonce: [u8; 12] = rand::rng().random();
    let mut tag = [0u8; 16];
    let ciphertext = openssl::symm::encrypt_aead(
        Cipher::aes_256_gcm(),
        key,
        Some(&nonce),
        SSO_SECRET_AAD,
        plaintext.as_bytes(),
        &mut tag,
    )?;
    Ok(format!(
        "{ENCRYPTED_SECRET_PREFIX}{}.{}.{}",
        URL_SAFE_NO_PAD.encode(nonce),
        URL_SAFE_NO_PAD.encode(ciphertext),
        URL_SAFE_NO_PAD.encode(tag)
    ))
}

fn encrypt_sso_secret(plaintext: &str) -> anyhow::Result<String> {
    if plaintext.is_empty() {
        return Ok(String::new());
    }
    encrypt_sso_secret_with_key(plaintext, &config_encryption_key()?)
}

fn decrypt_sso_secret_with_key(stored: &str, key: &[u8; 32]) -> anyhow::Result<String> {
    if stored.is_empty() || !stored.starts_with(ENCRYPTED_SECRET_PREFIX) {
        return Ok(stored.to_string());
    }
    let encoded = stored
        .strip_prefix(ENCRYPTED_SECRET_PREFIX)
        .ok_or_else(|| anyhow::anyhow!("invalid encrypted SSO secret"))?;
    let mut parts = encoded.split('.');
    let nonce = parts
        .next()
        .and_then(|value| URL_SAFE_NO_PAD.decode(value).ok())
        .ok_or_else(|| anyhow::anyhow!("invalid encrypted SSO secret"))?;
    let ciphertext = parts
        .next()
        .and_then(|value| URL_SAFE_NO_PAD.decode(value).ok())
        .ok_or_else(|| anyhow::anyhow!("invalid encrypted SSO secret"))?;
    let tag = parts
        .next()
        .and_then(|value| URL_SAFE_NO_PAD.decode(value).ok())
        .ok_or_else(|| anyhow::anyhow!("invalid encrypted SSO secret"))?;
    if parts.next().is_some() || nonce.len() != 12 || tag.len() != 16 {
        anyhow::bail!("invalid encrypted SSO secret");
    }
    let plaintext = openssl::symm::decrypt_aead(
        Cipher::aes_256_gcm(),
        key,
        Some(&nonce),
        SSO_SECRET_AAD,
        &ciphertext,
        &tag,
    )
    .map_err(|_| anyhow::anyhow!("SSO client secret could not be decrypted"))?;
    String::from_utf8(plaintext).map_err(|_| anyhow::anyhow!("SSO client secret is invalid UTF-8"))
}

fn decrypt_sso_secret(stored: &str) -> anyhow::Result<String> {
    if stored.is_empty() || !stored.starts_with(ENCRYPTED_SECRET_PREFIX) {
        return Ok(stored.to_string());
    }
    decrypt_sso_secret_with_key(stored, &config_encryption_key()?)
}

// ── Module-level row types used by helper methods ─────────────────────────────

pub type SsoProviderRow = (
    String,
    String,
    String,
    bool,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    bool,
    String,
    String,
    String,
    String,
    String,
    String,
);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SsoActiveProviderReconciliation {
    pub active_provider_id: Option<String>,
    pub ambiguous_provider_ids: Vec<String>,
    pub changed: bool,
}

fn resolve_legacy_active_sso_provider(
    mut provider_ids: Vec<String>,
) -> SsoActiveProviderReconciliation {
    provider_ids.sort();
    provider_ids.dedup();
    match provider_ids.as_slice() {
        [] => SsoActiveProviderReconciliation {
            active_provider_id: None,
            ambiguous_provider_ids: Vec::new(),
            changed: false,
        },
        [provider_id] => SsoActiveProviderReconciliation {
            active_provider_id: Some(provider_id.clone()),
            ambiguous_provider_ids: Vec::new(),
            changed: true,
        },
        _ => SsoActiveProviderReconciliation {
            active_provider_id: None,
            ambiguous_provider_ids: provider_ids,
            changed: true,
        },
    }
}

#[derive(clickhouse::Row, serde::Deserialize)]
pub struct AlertRuleRow {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: u8,
    pub signal_type: String,
    pub query_config: String,
    pub condition_op: String,
    pub condition_threshold: f64,
    pub eval_interval_secs: i64,
    pub notification_channel_ids: String,
    pub runbook_url: String,
    pub state: String,
    pub last_eval_at: String,
    pub last_triggered_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct ExplainClaimRow {
    id: String,
    db: String,
    query: String,
}
#[derive(clickhouse::Row, serde::Deserialize)]
struct ExplainStatusRow {
    status: String,
    db: String,
    plan_json: String,
    error: String,
}

#[derive(clickhouse::Row, serde::Deserialize)]
pub struct SloRow {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub description: String,
    pub enabled: u8,
    pub slo_type: String,
    pub indicator_type: String,
    pub service_name: String,
    pub metric_name: String,
    pub window_type: String,
    pub target_percentage: f64,
    pub threshold_ms: Option<f64>,
    pub threshold_value: Option<f64>,
    pub threshold_op: String,
    pub error_filters: String,
    pub total_filters: String,
    pub eval_interval_secs: i64,
    pub notification_channel_ids: String,
    pub state: String,
    pub error_budget_remaining: Option<f64>,
    pub error_count: Option<i64>,
    pub total_count: Option<i64>,
    pub last_eval_at: String,
    pub last_breached_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(clickhouse::Row, serde::Deserialize)]
pub struct AnomalyRuleRow {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub description: String,
    pub enabled: u8,
    pub source: String,
    pub pattern: String,
    pub query: String,
    pub service_name: String,
    pub apm_metric: String,
    pub sensitivity: f64,
    pub alpha: f64,
    pub eval_interval_secs: i64,
    pub window_secs: i64,
    pub split_labels: String,
    pub notification_channel_ids: String,
    pub state: String,
    pub last_eval_at: String,
    pub last_triggered_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(clickhouse::Row, serde::Deserialize)]
pub struct MonitorRow {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub monitor_type: String,
    pub query_config: String,
    pub critical: Option<f64>,
    pub critical_recovery: Option<f64>,
    pub warning: Option<f64>,
    pub warning_recovery: Option<f64>,
    pub comparator: String,
    pub eval_window_secs: i64,
    pub eval_interval_secs: i64,
    pub group_by: String,
    pub state: String,
    pub group_states: String,
    pub no_data_action: String,
    pub no_data_timeframe: i64,
    pub auto_resolve_hours: Option<i64>,
    pub message: String,
    pub notification_channels: String,
    pub renotify_interval: Option<i64>,
    pub tags: String,
    pub priority: Option<i64>,
    pub enabled: u8,
    pub composite_formula: String,
    pub composite_monitor_ids: String,
    pub last_eval_at: String,
    pub last_triggered_at: String,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
}

pub struct ConfigDb {
    pub client: Client,
    /// Tenant name-or-id → flags. Negative results are cached too so unknown
    /// tenants on the ingest path don't hammer ClickHouse.
    tenant_cache: DashMap<String, (TenantFlags, Instant)>,
    /// Tenant name-or-id → whether ingest requires a scoped key. Tenants with
    /// no explicit row inherit their legacy `auth_required` value.
    ingest_auth_cache: DashMap<String, (bool, Instant)>,
    /// user_id → (scopes, permissions, tenant_ids).
    perms_cache: DashMap<String, ((Vec<String>, Vec<String>, Vec<String>), Instant)>,
    /// (tenant_id_or_name, signal) → (enabled, cached_at). Hit on every ingest
    /// request to decide drop-vs-write, so it mirrors the tenant_flags TTL cache.
    /// Defaults (no stored row) are cached too so all-enabled tenants stay cheap.
    signal_cache: DashMap<(String, String), (bool, Instant)>,
    /// Identical API-key hashes being validated at the same instant share one
    /// ClickHouse lookup. Completed results are removed immediately, so key
    /// revocation remains visible on the next request without a cache TTL.
    api_key_inflight: DashMap<String, ApiKeyLookupCell>,
    /// Serializes canonical username checks and inserts inside one replica.
    /// Authentication also fails closed if separately racing replicas ever
    /// produce a collision.
    username_mutation_lock: tokio::sync::Mutex<()>,
    /// One-time SAML/setup claims. Keeper mode gives every API replica the same
    /// linearizable keyspace; local mode is accepted only for a single replica.
    sso_claim_store_mode: SsoClaimStoreMode,
    local_sso_claims: DashMap<String, i64>,
    sso_claim_cleanup_counter: AtomicU64,
    config_cache_maintenance_counter: AtomicU64,
    session_policy: SessionPolicy,
    /// Dedicated key for one-way session bearer storage. The raw bearer exists
    /// only in the issuing response/cookie and is never sent to ClickHouse.
    session_hmac_secret: Vec<u8>,
}

#[derive(Debug, Clone, clickhouse::Row, serde::Deserialize)]
pub struct KubernetesLoginRequest {
    pub device_code_hash: String,
    pub user_code: String,
    pub cluster_id: String,
    pub state: String,
    pub tenant_id: String,
    pub user_id: String,
    pub username: String,
    pub role: String,
    pub client_reported: String,
    pub created_at: String,
    pub expires_at: String,
    pub approved_at: String,
    pub credential_expires_at: String,
    pub version: u64,
    pub is_deleted: u8,
}

/// A metric firewall rule (storage + API shape). `enabled`/`*_regex` are 0/1.
/// `action` is "allow", "block" or "drop_label".
#[derive(Debug, Clone, clickhouse::Row, serde::Deserialize, serde::Serialize)]
pub struct MetricFirewallRule {
    pub id: String,
    pub name: String,
    pub enabled: u8,
    pub action: String,
    pub metric_pattern: String,
    pub metric_regex: u8,
    pub match_label_key: String,
    pub match_label_value: String,
    pub match_label_value_regex: u8,
    pub drop_label_pattern: String,
    pub drop_label_regex: u8,
    pub created_at: String,
}

/// Global retention caps. Per-signal values of 0 mean "inherit `default_days`".
/// These are the maximum retention per signal (logs / metrics / apm), used as
/// the table-level TTL and as the ceiling for tenant overrides. `apm` covers
/// traces (spans) and RUM.
#[derive(Debug, Clone, Copy, clickhouse::Row, serde::Deserialize, serde::Serialize)]
pub struct GlobalRetention {
    pub default_days: i32,
    pub logs_days: i32,
    pub metrics_days: i32,
    pub apm_days: i32,
}

impl GlobalRetention {
    /// Effective days for a signal, clamped to a safe floor of 1 so a stray 0
    /// can never become an `INTERVAL 0 DAY` (delete-everything) TTL.
    fn eff(value: i32, default_days: i32) -> i32 {
        let v = if value > 0 { value } else { default_days };
        v.max(1)
    }
    pub fn effective_logs(&self) -> i32 {
        Self::eff(self.logs_days, self.default_days)
    }
    pub fn effective_metrics(&self) -> i32 {
        Self::eff(self.metrics_days, self.default_days)
    }
    pub fn effective_apm(&self) -> i32 {
        Self::eff(self.apm_days, self.default_days)
    }

    /// Effective cap for a tenant-retention signal name ("logs"/"metrics"/"traces").
    pub fn effective_for_signal(&self, signal: &str) -> Option<i32> {
        match signal {
            "logs" => Some(self.effective_logs()),
            "metrics" => Some(self.effective_metrics()),
            "traces" => Some(self.effective_apm()),
            _ => None,
        }
    }
}

impl ConfigDb {
    /// Internal key material for domain-separated query cursor signatures.
    /// The secret never leaves the process or appears in cursor payloads/logs.
    pub(crate) fn cursor_hmac_secret(&self) -> &[u8] {
        &self.session_hmac_secret
    }

    pub fn session_activity_interval_seconds(&self) -> i64 {
        self.session_policy.activity_touch_interval_secs()
    }

    /// One-way, process-stable identity for a session bearer. Request-scoped
    /// authorization reuse keys by this value so the raw cookie never enters a
    /// cache, metric, log, or trace.
    pub fn session_request_key(&self, token: &str) -> String {
        session_storage_key(&self.session_hmac_secret, token)
    }

    pub async fn open(url: &str, user: &str, password: &str) -> anyhow::Result<Self> {
        let sso_claim_store_mode = configured_sso_claim_store_mode()?;
        let session_policy = SessionPolicy::from_env()?;
        let session_hmac_secret = session_hmac_secret_from_env()?;
        let client = Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(password);
        let db = Self {
            client,
            tenant_cache: DashMap::new(),
            ingest_auth_cache: DashMap::new(),
            perms_cache: DashMap::new(),
            signal_cache: DashMap::new(),
            api_key_inflight: DashMap::new(),
            username_mutation_lock: tokio::sync::Mutex::new(()),
            sso_claim_store_mode,
            local_sso_claims: DashMap::new(),
            sso_claim_cleanup_counter: AtomicU64::new(0),
            config_cache_maintenance_counter: AtomicU64::new(0),
            session_policy,
            session_hmac_secret,
        };
        db.run_migrations().await?;
        db.initialize_sso_claim_store().await?;
        // Do this at startup so the first unknown-user login has no one-time
        // initialization cost that could become a timing signal.
        let _ = dummy_password_hash();
        db.validate_unique_usernames().await?;
        Ok(db)
    }

    async fn initialize_sso_claim_store(&self) -> anyhow::Result<()> {
        if self.sso_claim_store_mode == SsoClaimStoreMode::Local {
            tracing::info!("SSO replay protection uses the single-replica in-process claim store");
            return Ok(());
        }

        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {SSO_CLAIM_KEEPER_TABLE} (\
                 claim_key String,\
                 expires_at Int64,\
                 claimed_at Int64\
             ) ENGINE = KeeperMap('sso_one_time_claims') PRIMARY KEY claim_key"
        );
        self.client.query(&ddl).execute().await.map_err(|error| {
            anyhow::anyhow!(
                "shared SSO replay store initialization failed; configure ClickHouse Keeper and keeper_map_path_prefix, or run exactly one query-api replica: {error}"
            )
        })?;
        tracing::info!(
            table = SSO_CLAIM_KEEPER_TABLE,
            "SSO replay protection uses the shared ClickHouse Keeper claim store"
        );
        Ok(())
    }

    /// Drop all config-plane caches. Called by every tenant/user/group mutation:
    /// coarse but cheap (the caches rebuild on next request), and it guarantees
    /// permission changes made through this process take effect immediately.
    fn invalidate_config_caches(&self) {
        self.tenant_cache.clear();
        self.ingest_auth_cache.clear();
        self.perms_cache.clear();
        self.signal_cache.clear();
    }

    fn cache_fresh(at: Instant) -> bool {
        at.elapsed() < CONFIG_CACHE_TTL
    }

    /// Opportunistically discard expired entries. The fast path is one relaxed
    /// atomic increment; a full scan happens infrequently or whenever a cache
    /// reaches its cap.
    fn maintain_config_caches(&self) {
        let maintenance_due = self
            .config_cache_maintenance_counter
            .fetch_add(1, Ordering::Relaxed)
            % CONFIG_CACHE_MAINTENANCE_EVERY
            == 0;
        let cache_full = self.tenant_cache.len() >= MAX_CONFIG_CACHE_ENTRIES
            || self.ingest_auth_cache.len() >= MAX_CONFIG_CACHE_ENTRIES
            || self.perms_cache.len() >= MAX_CONFIG_CACHE_ENTRIES
            || self.signal_cache.len() >= MAX_CONFIG_CACHE_ENTRIES;
        if !maintenance_due && !cache_full {
            return;
        }

        self.tenant_cache
            .retain(|_, (_, cached_at)| Self::cache_fresh(*cached_at));
        self.ingest_auth_cache
            .retain(|_, (_, cached_at)| Self::cache_fresh(*cached_at));
        self.perms_cache
            .retain(|_, (_, cached_at)| Self::cache_fresh(*cached_at));
        self.signal_cache
            .retain(|_, (_, cached_at)| Self::cache_fresh(*cached_at));
    }

    async fn run_migrations(&self) -> anyhow::Result<()> {
        let access_retention_days = kubernetes_access_retention_days();
        let access_event_ddl = format!(
            "CREATE TABLE IF NOT EXISTS config_kubernetes_access_events (
                id                String,
                tenant_id         String,
                cluster_id        String,
                gateway_id        String,
                session_id        String,
                actor_user_id     String,
                actor_name        String,
                actor_type        LowCardinality(String),
                kube_username     String,
                kube_groups       String DEFAULT '[]',
                source_kind       LowCardinality(String),
                client_reported   String DEFAULT '{{}}',
                observed_network  String DEFAULT '{{}}',
                http_method       LowCardinality(String),
                verb              LowCardinality(String),
                api_group         LowCardinality(String),
                api_version       LowCardinality(String),
                resource          LowCardinality(String),
                subresource       LowCardinality(String),
                namespace         String,
                name              String,
                request_query     String DEFAULT '{{}}',
                user_agent        String,
                status_code       UInt16,
                duration_ms       UInt64,
                request_bytes     UInt64,
                response_bytes    UInt64,
                result_summary    String DEFAULT 'null',
                result_truncated  UInt8 DEFAULT 0,
                redaction_count   UInt32 DEFAULT 0,
                recording_state   LowCardinality(String),
                created_at        String
            ) ENGINE = MergeTree
            ORDER BY (tenant_id, created_at, id)
            TTL parseDateTimeBestEffort(created_at) + INTERVAL {access_retention_days} DAY DELETE"
        );
        let access_chunk_ddl = format!(
            "CREATE TABLE IF NOT EXISTS config_kubernetes_session_chunks (
                id                String,
                tenant_id         String,
                session_id        String,
                event_id          String,
                gateway_id        String,
                sequence          UInt64,
                stream            LowCardinality(String),
                encoding          LowCardinality(String) DEFAULT 'utf8',
                provenance        String DEFAULT '{{}}',
                recording_state   LowCardinality(String) DEFAULT 'partial',
                offset_ms         UInt64,
                data              String,
                byte_count        UInt64,
                redaction_count   UInt32 DEFAULT 0,
                created_at        String
            ) ENGINE = MergeTree
            ORDER BY (tenant_id, session_id, sequence, id)
            TTL parseDateTimeBestEffort(created_at) + INTERVAL {access_retention_days} DAY DELETE"
        );
        let access_event_ttl_migration = format!(
            "ALTER TABLE config_kubernetes_access_events MODIFY TTL parseDateTimeBestEffort(created_at) + INTERVAL {access_retention_days} DAY DELETE"
        );
        let access_chunk_ttl_migration = format!(
            "ALTER TABLE config_kubernetes_session_chunks MODIFY TTL parseDateTimeBestEffort(created_at) + INTERVAL {access_retention_days} DAY DELETE"
        );
        let ddls = vec![
            // ── Tenants ──────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_tenants (
                id           String,
                name         String,
                enabled      UInt8 DEFAULT 1,
                auth_required UInt8 DEFAULT 1,
                created_at   String DEFAULT toString(now()),
                version      UInt64,
                is_deleted   UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // Kept separate from config_tenants so existing rows can migrate
            // without being rewritten: no row means inherit auth_required.
            "CREATE TABLE IF NOT EXISTS config_tenant_ingest_auth (
                tenant_id           String,
                ingest_auth_required UInt8 DEFAULT 1,
                updated_at          String DEFAULT toString(now()),
                version             UInt64,
                is_deleted          UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (tenant_id)",
            // ── Groups ────────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_groups (
                id          String,
                name        String,
                description String DEFAULT '',
                scopes      String DEFAULT '[\"all\"]',
                permissions String DEFAULT '[\"read\"]',
                system      UInt8 DEFAULT 0,
                created_at  String DEFAULT toString(now()),
                version     UInt64,
                is_deleted  UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Users ─────────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_users (
                id            String,
                username      String,
                password_hash String,
                display_name  String DEFAULT '',
                tenant_id     String DEFAULT 'default',
                role          String DEFAULT 'admin',
                enabled       UInt8 DEFAULT 1,
                auth_provider String DEFAULT 'local',
                external_id   String DEFAULT '',
                created_at    String DEFAULT toString(now()),
                version       UInt64,
                is_deleted    UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Sessions ──────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_sessions (
                token      String,
                session_id String,
                user_id    String,
                user_version UInt64,
                auth_method String DEFAULT '',
                provider_id String DEFAULT '',
                created_at String DEFAULT toString(now()),
                last_seen_at String,
                expires_at String,
                absolute_expires_at String
            ) ENGINE = MergeTree()
            ORDER BY (token)
            TTL parseDateTimeBestEffort(expires_at) + INTERVAL 0 SECOND",
            // Bind every session to the exact user row that issued it. Existing
            // unversioned sessions are revoked during the rolling upgrade;
            // subsequent password/user rewrites invalidate sessions atomically.
            "ALTER TABLE config_sessions ADD COLUMN IF NOT EXISTS user_version UInt64 DEFAULT 0 AFTER user_id",
            // Session provenance is authorization data, not just metadata. Empty
            // defaults deliberately fail closed for sessions minted before this
            // migration; current local and SSO login paths always set both fields.
            "ALTER TABLE config_sessions ADD COLUMN IF NOT EXISTS auth_method String DEFAULT '' AFTER user_version",
            "ALTER TABLE config_sessions ADD COLUMN IF NOT EXISTS provider_id String DEFAULT '' AFTER auth_method",
            "ALTER TABLE config_sessions ADD COLUMN IF NOT EXISTS session_id String DEFAULT '' AFTER token",
            "ALTER TABLE config_sessions ADD COLUMN IF NOT EXISTS last_seen_at String DEFAULT created_at AFTER created_at",
            "ALTER TABLE config_sessions ADD COLUMN IF NOT EXISTS absolute_expires_at String DEFAULT expires_at AFTER expires_at",
            "ALTER TABLE config_sessions DELETE WHERE user_version = 0 SETTINGS mutations_sync = 2",
            // Legacy rows have no stable public identifier or absolute deadline.
            // Revoke them once during migration rather than silently treating
            // their old 24-hour expiry as the new policy.
            "ALTER TABLE config_sessions DELETE WHERE session_id = '' SETTINGS mutations_sync = 2",
            // Durable tombstones make logout/admin revocation win over a token
            // rotation that was already in flight on another API replica.
            "CREATE TABLE IF NOT EXISTS config_session_revocations (
                session_id String,
                expires_at String,
                revoked_at String
            ) ENGINE = MergeTree()
            ORDER BY (session_id)
            TTL parseDateTimeBestEffort(expires_at) + INTERVAL 0 SECOND",
            // Keep a superseded bearer valid briefly so requests already in
            // flight can finish while the browser applies the replacement
            // cookie. The raw bearer never enters this table.
            "CREATE TABLE IF NOT EXISTS config_session_rotation_grace (
                token            String,
                grace_expires_at String,
                expires_at       String,
                created_at       String
            ) ENGINE = MergeTree()
            ORDER BY (token)
            TTL parseDateTimeBestEffort(expires_at) + INTERVAL 0 SECOND",
            // Browser-approved kubectl credentials. Only a SHA-256 digest of
            // the bearer is stored; the raw device credential stays with the
            // CLI that initiated the login. ReplacingMergeTree makes approval
            // visible to every query-api replica without process-local state.
            "CREATE TABLE IF NOT EXISTS config_kubernetes_login_requests (
                device_code_hash     String,
                user_code            String,
                cluster_id           String,
                state                LowCardinality(String) DEFAULT 'pending',
                tenant_id            String DEFAULT '',
                user_id              String DEFAULT '',
                username             String DEFAULT '',
                role                 String DEFAULT '',
                client_reported      String DEFAULT '{}',
                created_at           String,
                expires_at           String,
                approved_at          String DEFAULT '',
                credential_expires_at String DEFAULT '',
                version              UInt64,
                is_deleted           UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (device_code_hash)
            TTL parseDateTimeBestEffort(expires_at) + INTERVAL 1 DAY",
            "ALTER TABLE config_kubernetes_login_requests ADD COLUMN IF NOT EXISTS client_reported String DEFAULT '{}' AFTER role",
            // Revocations live separately from mutable client enrichment. This
            // makes de-auth fail closed even if an enrichment write races with
            // an administrator revoking the same credential.
            "CREATE TABLE IF NOT EXISTS config_kubernetes_login_revocations (
                device_code_hash String,
                tenant_id        String,
                revoked_at       String,
                version          UInt64
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (device_code_hash)
            TTL parseDateTimeBestEffort(revoked_at) + INTERVAL 2 DAY",
            // Rush group mappings to native Kubernetes RBAC. The gateway reads
            // these rows through its internal API and reconciles ClusterRoles
            // and bindings in the target cluster.
            "CREATE TABLE IF NOT EXISTS config_kubernetes_rbac_grants (
                id          String,
                tenant_id   String,
                group_id    String,
                cluster_id  String,
                cluster_match LowCardinality(String) DEFAULT 'single',
                cluster_pattern String DEFAULT '',
                name        String,
                role_kind   LowCardinality(String),
                role_name   String DEFAULT '',
                scope       LowCardinality(String),
                namespaces  String DEFAULT '[]',
                rules       String DEFAULT '[]',
                created_at  String,
                updated_at  String,
                version     UInt64,
                is_deleted  UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (tenant_id, id)",
            "ALTER TABLE config_kubernetes_rbac_grants ADD COLUMN IF NOT EXISTS cluster_match LowCardinality(String) DEFAULT 'single' AFTER cluster_id",
            "ALTER TABLE config_kubernetes_rbac_grants ADD COLUMN IF NOT EXISTS cluster_pattern String DEFAULT '' AFTER cluster_match",
            // Shared login-attempt ledger. Identifiers are keyed hashes, never
            // raw usernames or addresses, and expire after one day. IP rows
            // count every request; account rows count failed credentials only.
            // Keeping those dimensions separate prevents an attacker from
            // locking out a valid account while retaining cross-replica limits.
            "CREATE TABLE IF NOT EXISTS config_login_attempts (
                attempted_at String,
                ip_hash      String,
                account_hash String
            ) ENGINE = MergeTree()
            ORDER BY (account_hash, attempted_at, ip_hash)
            TTL parseDateTimeBestEffort(attempted_at) + INTERVAL 1 DAY",
            // ── Group tenants ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_group_tenants (
                group_id  String,
                tenant_id String,
                version   UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (group_id, tenant_id)",
            // ── User groups ───────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_user_groups (
                user_id   String,
                group_id  String,
                version   UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (user_id, group_id)",
            // ── SSO providers ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_sso_providers (
                id                    String,
                name                  String,
                protocol              String,
                enabled               UInt8 DEFAULT 0,
                client_id             String DEFAULT '',
                client_secret         String DEFAULT '',
                issuer_url            String DEFAULT '',
                oidc_scopes           String DEFAULT 'openid profile email groups',
                groups_claim          String DEFAULT 'groups',
                email_claim           String DEFAULT 'email',
                first_name_claim      String DEFAULT 'given_name',
                last_name_claim       String DEFAULT 'family_name',
                jit_provisioning      UInt8 DEFAULT 1,
                default_group_id      String DEFAULT '',
                saml_idp_metadata_url String DEFAULT '',
                saml_idp_sso_url      String DEFAULT '',
                saml_idp_cert         String DEFAULT '',
                saml_sp_entity_id     String DEFAULT '',
                created_at            String DEFAULT toString(now()),
                version               UInt64,
                is_deleted            UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // One authoritative pointer prevents multiple provider rows from
            // making login selection ambiguous. Provider rows retain their
            // legacy `enabled` column for migration compatibility, but all
            // runtime reads use this singleton after startup reconciliation.
            "CREATE TABLE IF NOT EXISTS config_sso_active_provider (
                slot        String,
                provider_id String DEFAULT '',
                version     UInt64
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (slot)",
            // Persistent setup-session revocations survive a single-replica API
            // restart. The atomic local/Keeper claim store still arbitrates
            // concurrent use; this ledger closes the restart window.
            "CREATE TABLE IF NOT EXISTS config_sso_setup_revocations (
                claim_key  String,
                expires_at Int64,
                revoked_at Int64
            ) ENGINE = MergeTree()
            ORDER BY (claim_key)
            TTL toDateTime(expires_at)",
            // ── IdP group mappings ────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_idp_group_mappings (
                id            String,
                idp_group     String,
                rush_group_id String,
                provider_id   String DEFAULT 'default',
                created_at    String DEFAULT toString(now()),
                version       UInt64,
                is_deleted    UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── SSO state ─────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_sso_state (
                state      String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (state)
            TTL created_at + INTERVAL 10 MINUTE",
            // ── Setup tokens ──────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_setup_tokens (
                token      String,
                purpose    String,
                created_by String,
                expires_at String,
                used       UInt8 DEFAULT 0,
                provider   String DEFAULT '',
                hostname   String DEFAULT '',
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (token)",
            // ── API keys ──────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_api_keys (
                id         String,
                name       String,
                key_hash   String,
                prefix     String,
                tenant_id  String DEFAULT 'default',
                created_at String DEFAULT toString(now()),
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            "ALTER TABLE config_api_keys ADD COLUMN IF NOT EXISTS key_type String DEFAULT 'legacy' AFTER tenant_id",
            "ALTER TABLE config_api_keys ADD COLUMN IF NOT EXISTS signals String DEFAULT '[]' AFTER key_type",
            "ALTER TABLE config_api_keys ADD COLUMN IF NOT EXISTS rate_limit_per_minute UInt64 DEFAULT 0 AFTER signals",
            "ALTER TABLE config_api_keys ADD COLUMN IF NOT EXISTS source_cidrs String DEFAULT '[]' AFTER rate_limit_per_minute",
            // Authentication reads by key hash, while the source table stays
            // keyed by id for the admin API. A materialized view keeps this
            // lookup table current for new and old API replicas during rolling
            // upgrades, including revocation tombstones.
            "CREATE TABLE IF NOT EXISTS config_api_keys_by_hash (
                key_hash              String,
                id                    String,
                tenant_id             String,
                key_type              String,
                signals               String,
                rate_limit_per_minute UInt64,
                source_cidrs          String,
                version               UInt64,
                is_deleted            UInt8
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (key_hash)",
            "CREATE MATERIALIZED VIEW IF NOT EXISTS config_api_keys_by_hash_mv
             TO config_api_keys_by_hash AS
             SELECT key_hash, id, tenant_id, key_type, signals,
                    rate_limit_per_minute, source_cidrs, version, is_deleted
             FROM config_api_keys",
            "INSERT INTO config_api_keys_by_hash
             SELECT key_hash, id, tenant_id, key_type, signals,
                    rate_limit_per_minute, source_cidrs, version, is_deleted
             FROM config_api_keys FINAL
             WHERE key_hash NOT IN (SELECT key_hash FROM config_api_keys_by_hash FINAL)",
            // ── Settings ──────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_settings (
                key        String,
                value      String,
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (key)",
            // ── API-managed integration targets ────────────────────────────────
            // DSNs are encrypted by query-api before they are written here.
            "CREATE TABLE IF NOT EXISTS config_integration_targets (
                id             String,
                tenant_id      String,
                integration    String,
                name           String,
                dsn_encrypted  String,
                environment    String DEFAULT 'production',
                enabled        UInt8 DEFAULT 1,
                created_at     String DEFAULT toString(now()),
                updated_at     String DEFAULT toString(now()),
                version        UInt64,
                is_deleted     UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (tenant_id, integration, id)",
            // ── Custom skills ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_custom_skills (
                id            String,
                name          String,
                title         String,
                description   String,
                content       String,
                allowed_tools String DEFAULT '[]',
                enabled       UInt8 DEFAULT 1,
                created_by    String DEFAULT '',
                created_at    String DEFAULT toString(now()),
                updated_at    String DEFAULT toString(now()),
                version       UInt64,
                is_deleted    UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Investigation sessions (owned by sre-agent; mutable → ReplacingMergeTree) ──
            "CREATE TABLE IF NOT EXISTS config_investigation_sessions (
                id                String,
                tenant_id         String DEFAULT 'default',
                title             String DEFAULT '',
                status            String DEFAULT 'active',
                template_id       String DEFAULT '',
                created_by        String DEFAULT '',
                created_at        String DEFAULT toString(now()),
                updated_at        String DEFAULT toString(now()),
                working_memory    String DEFAULT '{}',
                prompt_tokens     Int64 DEFAULT 0,
                completion_tokens Int64 DEFAULT 0,
                llm_model         String DEFAULT '',
                version           UInt64,
                is_deleted        UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Investigation turns (owned by sre-agent; append-only) ──
            "CREATE TABLE IF NOT EXISTS config_investigation_turns (
                id          String,
                session_id  String,
                turn_index  Int64,
                role        String,
                content     String,
                tool_calls  String DEFAULT '[]',
                report_kind String DEFAULT '',
                created_at  String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (session_id, turn_index)",
            // ── Service links ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_service_links (
                service_name   String,
                github_repo    String,
                default_branch String DEFAULT 'main',
                root_path      String DEFAULT '',
                updated_at     String DEFAULT toString(now()),
                version        UInt64,
                is_deleted     UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (service_name)",
            // Tenant-safe replacement for the original service-links table. The
            // old table keyed only by service_name, so ClickHouse merges could
            // collapse identically named services belonging to different tenants.
            "CREATE TABLE IF NOT EXISTS config_service_links_v2 (
                tenant_id             String DEFAULT 'default',
                service_name          String,
                github_repo           String,
                github_installation_id UInt64 DEFAULT 0,
                github_repository_id  UInt64 DEFAULT 0,
                default_branch        String DEFAULT 'main',
                root_path             String DEFAULT '',
                updated_at            String DEFAULT toString(now()),
                version               UInt64,
                is_deleted            UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (tenant_id, service_name)",
            "ALTER TABLE config_service_links_v2 ADD COLUMN IF NOT EXISTS github_repository_id UInt64 DEFAULT 0 AFTER github_installation_id",
            // Preserve pre-tenancy links for the default tenant. The anti-join
            // makes this idempotent across process restarts.
            "INSERT INTO config_service_links_v2
                 (tenant_id, service_name, github_repo, github_installation_id, github_repository_id,
                  default_branch, root_path, updated_at, version, is_deleted)
             SELECT 'default', service_name, github_repo, 0, 0, default_branch, root_path,
                    updated_at, version, is_deleted
             FROM config_service_links FINAL
             WHERE service_name NOT IN (
                 SELECT service_name FROM config_service_links_v2 FINAL WHERE tenant_id = 'default'
             )",
            // ── Dashboards ────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_dashboards (
                id          String,
                name        String,
                description String DEFAULT '',
                tenant_id   String DEFAULT 'default',
                owner_id    String DEFAULT '',
                visibility  String DEFAULT 'tenant',
                tags        String DEFAULT '[]',
                variables   String DEFAULT '[]',
                created_at  String DEFAULT toString(now()),
                updated_at  String DEFAULT toString(now()),
                version     UInt64,
                is_deleted  UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // Backfill `variables` on dashboards created before template-variable support.
            "ALTER TABLE config_dashboards ADD COLUMN IF NOT EXISTS variables String DEFAULT '[]'",
            // ── Widgets ───────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_widgets (
                id             String,
                dashboard_id   String,
                title          String,
                widget_type    String,
                query_config   String,
                position       String,
                display_config String DEFAULT '{}',
                created_at     String DEFAULT toString(now()),
                updated_at     String DEFAULT toString(now()),
                version        UInt64,
                is_deleted     UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Dashboard templates ───────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_dashboard_templates (
                id            String,
                name          String,
                description   String DEFAULT '',
                category      String DEFAULT 'general',
                is_builtin    UInt8 DEFAULT 0,
                template_json String,
                tags          String DEFAULT '[]',
                created_at    String DEFAULT toString(now()),
                version       UInt64,
                is_deleted    UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Notification channels ─────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_notification_channels (
                id           String,
                tenant_id    String DEFAULT 'default',
                name         String,
                channel_type String,
                config       String DEFAULT '{}',
                enabled      UInt8 DEFAULT 1,
                created_at   String DEFAULT toString(now()),
                version      UInt64,
                is_deleted   UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Notification log ──────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_notification_log (
                id         String,
                channel_id String,
                tenant_id  String,
                alert_type String,
                alert_name String,
                severity   String DEFAULT '',
                status     String,
                error      String DEFAULT '',
                created_at String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (tenant_id, created_at)",
            // ── Alert rules ───────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_alert_rules (
                id                       String,
                name                     String,
                description              String DEFAULT '',
                enabled                  UInt8 DEFAULT 1,
                signal_type              String DEFAULT 'apm',
                query_config             String,
                condition_op             String,
                condition_threshold      Float64,
                eval_interval_secs       Int64 DEFAULT 60,
                notification_channel_ids String DEFAULT '[]',
                runbook_url              String DEFAULT '',
                state                    String DEFAULT 'ok',
                last_eval_at             String DEFAULT '',
                last_triggered_at        String DEFAULT '',
                created_at               String DEFAULT toString(now()),
                updated_at               String DEFAULT toString(now()),
                version                  UInt64,
                is_deleted               UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Alert events ──────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_alert_events (
                id         String,
                rule_id    String,
                state      String,
                value      Float64,
                threshold  Float64,
                message    String,
                created_at String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (rule_id, created_at)",
            // ── Anomaly rules ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_anomaly_rules (
                id                       String,
                tenant_id                String DEFAULT 'default',
                name                     String,
                description              String DEFAULT '',
                enabled                  UInt8 DEFAULT 1,
                source                   String,
                pattern                  String DEFAULT '',
                query                    String DEFAULT '',
                service_name             String DEFAULT '',
                apm_metric               String DEFAULT '',
                sensitivity              Float64 DEFAULT 3.0,
                alpha                    Float64 DEFAULT 0.25,
                eval_interval_secs       Int64 DEFAULT 300,
                window_secs              Int64 DEFAULT 3600,
                notification_channel_ids String DEFAULT '[]',
                split_labels             String DEFAULT '[]',
                state                    String DEFAULT 'normal',
                last_eval_at             String DEFAULT '',
                last_triggered_at        String DEFAULT '',
                created_at               String DEFAULT toString(now()),
                updated_at               String DEFAULT toString(now()),
                version                  UInt64,
                is_deleted               UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Anomaly events ────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_anomaly_events (
                id         String,
                rule_id    String,
                tenant_id  String DEFAULT 'default',
                state      String,
                metric     String DEFAULT '',
                value      Float64,
                expected   Float64,
                deviation  Float64 DEFAULT 0.0,
                message    String,
                created_at String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (rule_id, created_at)",
            // Tenant-scope existing anomaly tables (deployments created before
            // anomaly rules/events carried a tenant). Idempotent; existing rows
            // backfill to 'default' via the column DEFAULT.
            "ALTER TABLE config_anomaly_rules ADD COLUMN IF NOT EXISTS tenant_id String DEFAULT 'default'",
            "ALTER TABLE config_anomaly_events ADD COLUMN IF NOT EXISTS tenant_id String DEFAULT 'default'",
            // ── Monitors ──────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_monitors (
                id                    String,
                tenant_id             String DEFAULT 'default',
                name                  String,
                monitor_type          String,
                query_config          String,
                critical              Nullable(Float64),
                critical_recovery     Nullable(Float64),
                warning               Nullable(Float64),
                warning_recovery      Nullable(Float64),
                comparator            String DEFAULT 'above',
                eval_window_secs      Int64 DEFAULT 300,
                eval_interval_secs    Int64 DEFAULT 60,
                group_by              String DEFAULT '[]',
                state                 String DEFAULT 'ok',
                group_states          String DEFAULT '{}',
                no_data_action        String DEFAULT 'show',
                no_data_timeframe     Int64 DEFAULT 600,
                auto_resolve_hours    Nullable(Int64),
                message               String DEFAULT '',
                notification_channels String DEFAULT '[]',
                renotify_interval     Nullable(Int64),
                tags                  String DEFAULT '[]',
                priority              Nullable(Int64),
                enabled               UInt8 DEFAULT 1,
                composite_formula     String DEFAULT '',
                composite_monitor_ids String DEFAULT '[]',
                last_eval_at          String DEFAULT '',
                last_triggered_at     String DEFAULT '',
                created_by            String DEFAULT '',
                created_at            String DEFAULT toString(now()),
                updated_at            String DEFAULT toString(now()),
                version               UInt64,
                is_deleted            UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Monitor events ────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_monitor_events (
                id         String,
                monitor_id String,
                tenant_id  String,
                group_key  String DEFAULT '',
                prev_state String,
                new_state  String,
                value      Nullable(Float64),
                threshold  Nullable(Float64),
                message    String DEFAULT '',
                created_at String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (monitor_id, created_at)",
            // ── SLOs ──────────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_slos (
                id                       String,
                name                     String,
                description              String DEFAULT '',
                enabled                  UInt8 DEFAULT 1,
                tenant_id                String DEFAULT 'default',
                slo_type                 String DEFAULT 'trace',
                service_name             String,
                metric_name              String DEFAULT '',
                window_type              String,
                target_percentage        Float64,
                threshold_ms             Nullable(Float64),
                threshold_value          Nullable(Float64),
                threshold_op             String DEFAULT '',
                error_filters            String,
                total_filters            String,
                eval_interval_secs       Int64 DEFAULT 60,
                notification_channel_ids String DEFAULT '[]',
                indicator_type           String DEFAULT 'availability',
                state                    String DEFAULT 'compliant',
                error_budget_remaining   Nullable(Float64),
                error_count              Nullable(Int64),
                total_count              Nullable(Int64),
                last_eval_at             String DEFAULT '',
                last_breached_at         String DEFAULT '',
                created_at               String DEFAULT toString(now()),
                updated_at               String DEFAULT toString(now()),
                version                  UInt64,
                is_deleted               UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Postgres EXPLAIN jobs (collector-run plan queue) ───────────────────
            "CREATE TABLE IF NOT EXISTS config_pg_explain_jobs (
                id           String,
                tenant_id    String DEFAULT 'default',
                server_name  String,
                db           String DEFAULT '',
                query        String,
                status       String DEFAULT 'pending',
                plan_json    String DEFAULT '',
                error        String DEFAULT '',
                created_at   String DEFAULT '',
                updated_at   String DEFAULT '',
                version      UInt64,
                is_deleted   UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── MySQL EXPLAIN jobs (collector-run plan queue) ─────────────────────
            "CREATE TABLE IF NOT EXISTS config_mysql_explain_jobs (
                id           String,
                tenant_id    String DEFAULT 'default',
                server_name  String,
                db           String DEFAULT '',
                query        String,
                status       String DEFAULT 'pending',
                plan_json    String DEFAULT '',
                error        String DEFAULT '',
                created_at   String DEFAULT '',
                updated_at   String DEFAULT '',
                version      UInt64,
                is_deleted   UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── SLO events ────────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_slo_events (
                id                     String,
                slo_id                 String,
                tenant_id              String DEFAULT 'default',
                state                  String,
                error_count            Int64,
                total_count            Int64,
                error_budget_remaining Float64,
                message                String,
                created_at             String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (slo_id, created_at)",
            // Tenant-scope pre-existing SLO tables for deployments created before
            // SLOs carried a tenant. Placed AFTER the config_slos / config_slo_events
            // CREATEs above so a fresh install (tables don't exist yet) doesn't ALTER
            // a missing table and abort migrations. The CREATEs already include
            // tenant_id, so on fresh installs these are no-ops; on older deployments
            // they retrofit the column. Idempotent (ADD COLUMN IF NOT EXISTS).
            "ALTER TABLE config_slos ADD COLUMN IF NOT EXISTS tenant_id String DEFAULT 'default'",
            "ALTER TABLE config_slo_events ADD COLUMN IF NOT EXISTS tenant_id String DEFAULT 'default'",
            // ── Deploy markers ────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_deploy_markers (
                id           String,
                service_name String,
                version      String DEFAULT '',
                commit_sha   String DEFAULT '',
                description  String DEFAULT '',
                environment  String DEFAULT '',
                deployed_by  String DEFAULT '',
                deployed_at  String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (service_name, deployed_at)",
            // ── Detection rules ───────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_detection_rules (
                id                String,
                tenant_id         String DEFAULT 'default',
                name              String,
                description       String DEFAULT '',
                query_sql         String,
                interval_secs     Int64 DEFAULT 300,
                threshold         Int64 DEFAULT 1,
                severity          String DEFAULT 'medium',
                window_secs       Int64 DEFAULT 300,
                enabled           UInt8 DEFAULT 1,
                channels          String DEFAULT '[]',
                created_by        String DEFAULT '',
                last_eval_at      String DEFAULT '',
                last_triggered_at String DEFAULT '',
                created_at        String DEFAULT toString(now()),
                updated_at        String DEFAULT toString(now()),
                version           UInt64,
                is_deleted        UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Detection events ──────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_detection_events (
                id          String,
                rule_id     String,
                tenant_id   String,
                severity    String,
                match_count Int64 DEFAULT 0,
                sample_data String DEFAULT '[]',
                created_at  String DEFAULT toString(now())
            ) ENGINE = MergeTree()
            ORDER BY (tenant_id, created_at)",
            // ── Tenant retention ──────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_tenant_retention (
                tenant_id   String,
                signal      String,
                retain_days Int32,
                version     UInt64,
                is_deleted  UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (tenant_id, signal)",
            // ── Tenant ingest signal enable/disable ───────────────────────────────
            // Per (tenant, signal) on/off switch for ingest. Missing row = enabled,
            // so tenants without explicit config keep ingesting every signal.
            // signal ∈ {logs, apm, metrics, rum}.
            "CREATE TABLE IF NOT EXISTS config_tenant_signals (
                tenant_id  String,
                signal     String,
                enabled    UInt8,
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (tenant_id, signal)",
            // ── Global retention (singleton, id='global') ─────────────────────────
            // default_days applies to any signal whose per-signal value is 0 (inherit).
            // These are the MAXIMUM retention per signal — tenant overrides are clamped
            // to them, and they double as the table-level TTL. apm covers traces + RUM.
            "CREATE TABLE IF NOT EXISTS config_global_retention (
                id           String,
                default_days Int32 DEFAULT 365,
                logs_days    Int32 DEFAULT 0,
                metrics_days Int32 DEFAULT 0,
                apm_days     Int32 DEFAULT 0,
                version      UInt64,
                is_deleted   UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Maintenance windows ───────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_maintenance_windows (
                id         String,
                name       String,
                scope      String DEFAULT 'all',
                starts_at  String,
                ends_at    String,
                created_at String DEFAULT toString(now()),
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Metric firewall (ingest-time block / drop-label rules) ────────────
            "CREATE TABLE IF NOT EXISTS config_metric_firewall (
                id                      String,
                name                    String,
                enabled                 UInt8 DEFAULT 1,
                action                  String DEFAULT 'block',
                metric_pattern          String DEFAULT '',
                metric_regex            UInt8 DEFAULT 0,
                match_label_key         String DEFAULT '',
                match_label_value       String DEFAULT '',
                match_label_value_regex UInt8 DEFAULT 0,
                drop_label_pattern      String DEFAULT '',
                drop_label_regex        UInt8 DEFAULT 0,
                created_at              String DEFAULT toString(now()),
                version                 UInt64,
                is_deleted              UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            // ── Trace funnels ─────────────────────────────────────────────────────
            "CREATE TABLE IF NOT EXISTS config_trace_funnels (
                id         String,
                name       String,
                steps_json String DEFAULT '[]',
                tenant_id  String DEFAULT 'default',
                created_at String DEFAULT toString(now()),
                version    UInt64,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (id)",
            access_event_ddl.as_str(),
            access_chunk_ddl.as_str(),
            "ALTER TABLE config_kubernetes_access_events ADD COLUMN IF NOT EXISTS actor_type LowCardinality(String) DEFAULT 'unknown' AFTER actor_name",
            access_event_ttl_migration.as_str(),
            access_chunk_ttl_migration.as_str(),
        ];

        for ddl in ddls {
            self.client
                .query(ddl)
                .execute()
                .await
                .map_err(|e| anyhow::anyhow!("DDL failed: {e}\nSQL: {ddl}"))?;
        }
        Ok(())
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn now_str() -> String {
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
    }

    fn next_version() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
    }

    /// Atomically claim a one-time SSO key until `expires_at` (Unix seconds).
    ///
    /// Keeper mode relies on KeeperMap's strict insert semantics: creating an
    /// existing primary key fails atomically across every query-api replica.
    /// An ambiguous insert failure is checked by key and always fails closed.
    /// Local mode uses the same contract inside the sole permitted replica.
    pub async fn claim_sso_key_once(&self, key: &str, expires_at: i64) -> anyhow::Result<bool> {
        let now = chrono::Utc::now().timestamp();
        if expires_at <= now {
            return Ok(false);
        }
        let claim_key = sso_claim_storage_key(key);

        if self.sso_claim_store_mode == SsoClaimStoreMode::Local {
            return Ok(consume_local_sso_claim(
                &self.local_sso_claims,
                claim_key,
                expires_at,
                now,
            ));
        }

        // KeeperMap has no TTL clause. Amortize deletion so expired claim keys
        // do not accumulate indefinitely without putting a scan on every login.
        if self
            .sso_claim_cleanup_counter
            .fetch_add(1, Ordering::Relaxed)
            % 256
            == 0
        {
            let cleanup = format!("DELETE FROM {SSO_CLAIM_KEEPER_TABLE} WHERE expires_at <= ?");
            if let Err(error) = self
                .client
                .query(&cleanup)
                .with_option("keeper_map_strict_mode", "1")
                .bind(now)
                .execute()
                .await
            {
                // Cleanup does not affect the atomic insertion below. Keep the
                // request available and surface the operational issue in logs.
                tracing::warn!(%error, "failed to remove expired SSO replay claims");
            }
        }

        let insert = format!(
            "INSERT INTO {SSO_CLAIM_KEEPER_TABLE} (claim_key, expires_at, claimed_at) \
             VALUES (?, ?, ?)"
        );
        match self
            .client
            .query(&insert)
            .with_option("keeper_map_strict_mode", "1")
            .bind(&claim_key)
            .bind(expires_at)
            .bind(now)
            .execute()
            .await
        {
            Ok(()) => Ok(true),
            Err(insert_error) => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct ClaimCount {
                    count: u64,
                }
                let lookup = format!(
                    "SELECT count() AS count FROM {SSO_CLAIM_KEEPER_TABLE} WHERE claim_key = ?"
                );
                match self
                    .client
                    .query(&lookup)
                    .bind(&claim_key)
                    .fetch_one::<ClaimCount>()
                    .await
                {
                    Ok(row) if row.count > 0 => Ok(false),
                    Ok(_) => Err(insert_error.into()),
                    Err(lookup_error) => Err(anyhow::anyhow!(
                        "SSO replay claim failed and its outcome could not be verified: insert={insert_error}; lookup={lookup_error}"
                    )),
                }
            }
        }
    }

    /// Persistently revoke a scoped SSO setup capability. Keys use the same
    /// one-way representation as replay claims, so session identifiers are not
    /// recoverable from ClickHouse or its query logs.
    pub async fn revoke_sso_setup_session(&self, key: &str, expires_at: i64) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        if expires_at <= now {
            anyhow::bail!("cannot revoke an expired SSO setup session");
        }
        self.client
            .query(
                "INSERT INTO config_sso_setup_revocations (claim_key, expires_at, revoked_at) VALUES (?, ?, ?)",
            )
            .bind(sso_claim_storage_key(key))
            .bind(expires_at)
            .bind(now)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn is_sso_setup_session_revoked(&self, key: &str) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ClaimCount {
            count: u64,
        }

        let row = self
            .client
            .query(
                "SELECT count() AS count FROM config_sso_setup_revocations WHERE claim_key = ? AND expires_at > ?",
            )
            .bind(sso_claim_storage_key(key))
            .bind(chrono::Utc::now().timestamp())
            .fetch_one::<ClaimCount>()
            .await?;
        Ok(row.count > 0)
    }

    async fn ensure_username_available(&self, username: &str) -> anyhow::Result<()> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count {
            n: u64,
        }

        if canonical_username(username).is_empty() {
            anyhow::bail!("username must not be empty");
        }
        let row = self
            .client
            .query("SELECT countDistinct(id) AS n FROM config_users FINAL WHERE lowerUTF8(trimBoth(username)) = lowerUTF8(trimBoth(?)) AND is_deleted = 0")
            .bind(username)
            .fetch_one::<Count>()
            .await?;
        if row.n != 0 {
            return Err(UsernameAlreadyExists.into());
        }
        Ok(())
    }

    /// Refuse to start with ambiguous legacy identities. All authentication
    /// lookups use this same canonical form and independently fail closed if a
    /// collision appears after startup (for example, through another replica).
    async fn validate_unique_usernames(&self) -> anyhow::Result<()> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Collision {
            canonical: String,
            n: u64,
        }

        let collision = self
            .client
            .query("SELECT lowerUTF8(trimBoth(username)) AS canonical, countDistinct(id) AS n FROM config_users FINAL WHERE is_deleted = 0 GROUP BY canonical HAVING n > 1 ORDER BY canonical LIMIT 1")
            .fetch_one::<Collision>()
            .await;
        match collision {
            Ok(row) => anyhow::bail!(
                "canonical username collision for {:?} across {} active users; resolve the duplicate before starting query-api",
                row.canonical,
                row.n
            ),
            Err(clickhouse::error::Error::RowNotFound) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    // ── Tenant operations ─────────────────────────────────────────────────────

    pub async fn ensure_default_tenant(&self) -> anyhow::Result<bool> {
        let existing = self.get_tenant("default").await?;
        if existing.is_none() {
            let ver = Self::next_version();
            let now = Self::now_str();
            // Secure by default. Anonymous access is available only through an
            // explicit development compatibility switch. Existing tenant rows
            // are never silently rewritten; startup/readiness reports an open
            // tenant so operators can plan the migration.
            let allow_anonymous = std::env::var("RUSH_ALLOW_ANONYMOUS_DEFAULT")
                .map(|value| {
                    matches!(
                        value.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes"
                    )
                })
                .unwrap_or(false);
            self.client
                .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, 1, ?, ?, ?, 0)")
                .bind("default")
                .bind("default")
                .bind(u8::from(crate::api_key_auth::default_tenant_auth_required(
                    allow_anonymous,
                )))
                .bind(&now)
                .bind(ver)
                .execute()
                .await?;
            self.set_tenant_ingest_auth_required(
                "default",
                crate::api_key_auth::default_tenant_auth_required(allow_anonymous),
            )
            .await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Seed the reserved `_audit` tenant row, DISABLED (enabled=0).
    ///
    /// This makes `_audit` a known/reserved tenant id+name so it can never be
    /// accidentally created as a normal tenant, while `enabled=0` guarantees
    /// `is_tenant_enabled("_audit")` is false — so the tenant middleware will
    /// never resolve `_audit` as an ingest/query target. Belt-and-suspenders
    /// with the explicit reject in `resolve_tenant_from_headers`. `auth_required=1`
    /// for good measure. Mirrors `ensure_default_tenant`.
    pub async fn ensure_audit_tenant(&self) -> anyhow::Result<()> {
        let existing = self.get_tenant(crate::audit::AUDIT_TENANT).await?;
        if existing.is_none() {
            let ver = Self::next_version();
            let now = Self::now_str();
            self.client
                .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, 0, 1, ?, ?, 0)")
                .bind(crate::audit::AUDIT_TENANT)
                .bind(crate::audit::AUDIT_TENANT)
                .bind(&now)
                .bind(ver)
                .execute()
                .await?;
        }
        Ok(())
    }

    pub async fn resolve_api_key(&self, key_hash: &str) -> anyhow::Result<Option<ApiKeyGrant>> {
        let cell = self
            .api_key_inflight
            .entry(key_hash.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
            .clone();
        let result = cell
            .get_or_init(|| async {
                self.resolve_api_key_uncached(key_hash)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
            .clone();
        self.api_key_inflight
            .remove_if(key_hash, |_, current| Arc::ptr_eq(current, &cell));
        result.map_err(anyhow::Error::msg)
    }

    async fn resolve_api_key_uncached(
        &self,
        key_hash: &str,
    ) -> anyhow::Result<Option<ApiKeyGrant>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            tenant_id: String,
            key_type: String,
            signals: String,
            rate_limit_per_minute: u64,
            source_cidrs: String,
            is_deleted: u8,
        }
        let result = self.client
            .query("SELECT id, tenant_id, key_type, signals, rate_limit_per_minute, source_cidrs, is_deleted FROM config_api_keys_by_hash FINAL WHERE key_hash = ? LIMIT 1")
            .bind(key_hash)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(row) if row.is_deleted == 0 => Ok(Some(ApiKeyGrant {
                id: row.id,
                tenant_id: row.tenant_id,
                key_type: row.key_type,
                signals: serde_json::from_str(&row.signals).unwrap_or_default(),
                rate_limit_per_minute: row.rate_limit_per_minute,
                source_cidrs: serde_json::from_str(&row.source_cidrs).unwrap_or_default(),
            })),
            Ok(_) => Ok(None),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn resolve_tenant_for_api_key(
        &self,
        key_hash: &str,
    ) -> anyhow::Result<Option<String>> {
        Ok(self
            .resolve_api_key(key_hash)
            .await?
            .map(|grant| grant.tenant_id))
    }

    pub async fn list_tenants(&self) -> anyhow::Result<Vec<(String, String, bool, bool, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            enabled: u8,
            auth_required: u8,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, name, enabled, auth_required, created_at FROM config_tenants FINAL WHERE is_deleted = 0 ORDER BY created_at ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.id,
                    r.name,
                    r.enabled != 0,
                    r.auth_required != 0,
                    r.created_at,
                )
            })
            .collect())
    }

    pub async fn create_tenant(
        &self,
        id: &str,
        name: &str,
        auth_required: bool,
        ingest_auth_required: bool,
    ) -> anyhow::Result<()> {
        self.invalidate_config_caches();
        let ver = Self::next_version();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, 1, ?, ?, ?, 0)")
            .bind(id)
            .bind(name)
            .bind(u8::from(auth_required))
            .bind(&now)
            .bind(ver)
            .execute()
            .await?;
        self.set_tenant_ingest_auth_required(id, ingest_auth_required)
            .await?;
        Ok(())
    }

    pub async fn get_tenant(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<(String, String, bool, bool, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            enabled: u8,
            auth_required: u8,
            created_at: String,
        }
        let result = self.client
            .query("SELECT id, name, enabled, auth_required, created_at FROM config_tenants FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some((
                r.id,
                r.name,
                r.enabled != 0,
                r.auth_required != 0,
                r.created_at,
            ))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Cached tenant flag lookup by name OR id. One query feeds
    /// `is_tenant_enabled`, `is_tenant_auth_required` and `get_tenant_id_by_name`,
    /// which the tenant middleware may call several times per request.
    /// Negative results (unknown tenant) are cached too; storage errors are
    /// returned and never cached so security callers can fail closed.
    async fn tenant_flags(&self, name_or_id: &str) -> anyhow::Result<TenantFlags> {
        if let Some(entry) = self.tenant_cache.get(name_or_id) {
            let (flags, at) = entry.value();
            if Self::cache_fresh(*at) {
                return Ok(flags.clone());
            }
        }
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            enabled: u8,
            auth_required: u8,
        }
        let result = self.client
            .query("SELECT id, name, enabled, auth_required FROM config_tenants FINAL WHERE (id = ? OR name = ?) AND is_deleted = 0 LIMIT 1")
            .bind(name_or_id)
            .bind(name_or_id)
            .fetch_one::<Row>()
            .await;
        let flags: TenantFlags = match result {
            Ok(r) => Some((r.id, r.name, r.enabled != 0, r.auth_required != 0)),
            Err(clickhouse::error::Error::RowNotFound) => None,
            Err(error) => return Err(error.into()),
        };
        self.maintain_config_caches();
        if self.tenant_cache.len() < MAX_CONFIG_CACHE_ENTRIES
            || self.tenant_cache.contains_key(name_or_id)
        {
            self.tenant_cache
                .insert(name_or_id.to_string(), (flags.clone(), Instant::now()));
        }
        Ok(flags)
    }

    pub async fn get_tenant_id_by_name(&self, name: &str) -> anyhow::Result<Option<String>> {
        // Preserves prior semantics: only enabled tenants resolve by name.
        Ok(self
            .tenant_flags(name)
            .await?
            .filter(|(_, n, enabled, _)| *enabled && n == name)
            .map(|(id, ..)| id))
    }

    pub async fn set_tenant_enabled(&self, id: &str, enabled: bool) -> anyhow::Result<bool> {
        self.invalidate_config_caches();
        let existing = self.get_tenant(id).await?;
        let (_, name, _, auth_required, created_at) = match existing {
            Some(t) => t,
            None => return Ok(false),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 0)")
            .bind(id)
            .bind(&name)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(if auth_required { 1u8 } else { 0u8 })
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn is_tenant_enabled(&self, name_or_id: &str) -> bool {
        self.tenant_flags(name_or_id)
            .await
            .ok()
            .flatten()
            .map(|(_, _, enabled, _)| enabled)
            .unwrap_or(false)
    }

    pub async fn is_tenant_auth_required(&self, name_or_id: &str) -> bool {
        self.tenant_flags(name_or_id)
            .await
            .ok()
            .flatten()
            .map(|(_, _, _, auth_required)| auth_required)
            .unwrap_or(false)
    }

    /// Resolve a tenant's authentication policy without collapsing storage
    /// errors into an unlocked tenant. Security-boundary middleware uses this
    /// checked variant so policy lookup failures can fail closed.
    pub async fn tenant_auth_required_checked(
        &self,
        name_or_id: &str,
    ) -> anyhow::Result<Option<bool>> {
        Ok(self
            .tenant_flags(name_or_id)
            .await?
            .map(|(_, _, _, auth_required)| auth_required))
    }

    /// Resolve the independent ingest-auth policy. Existing tenants without an
    /// explicit row inherit `auth_required`, preserving intentionally open
    /// ingestion during migration. Storage errors fail closed at callers.
    pub async fn tenant_ingest_auth_required_checked(
        &self,
        name_or_id: &str,
    ) -> anyhow::Result<Option<bool>> {
        if let Some(entry) = self.ingest_auth_cache.get(name_or_id) {
            let (required, at) = entry.value();
            if Self::cache_fresh(*at) {
                return Ok(Some(*required));
            }
        }

        let Some((tenant_id, tenant_name, _, legacy_auth_required)) =
            self.tenant_flags(name_or_id).await?
        else {
            return Ok(None);
        };

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            ingest_auth_required: u8,
        }
        let explicit = match self
            .client
            .query("SELECT ingest_auth_required FROM config_tenant_ingest_auth FINAL WHERE tenant_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(&tenant_id)
            .fetch_one::<Row>()
            .await
        {
            Ok(row) => Some(row.ingest_auth_required != 0),
            Err(clickhouse::error::Error::RowNotFound) => None,
            Err(error) => return Err(error.into()),
        };
        let required =
            crate::api_key_auth::effective_ingest_auth_required(explicit, legacy_auth_required);
        let now = Instant::now();
        self.maintain_config_caches();
        for cache_key in [tenant_id, tenant_name] {
            if self.ingest_auth_cache.len() < MAX_CONFIG_CACHE_ENTRIES
                || self.ingest_auth_cache.contains_key(&cache_key)
            {
                self.ingest_auth_cache.insert(cache_key, (required, now));
            }
        }
        Ok(Some(required))
    }

    pub async fn is_tenant_ingest_auth_required(&self, name_or_id: &str) -> bool {
        self.tenant_ingest_auth_required_checked(name_or_id)
            .await
            .ok()
            .flatten()
            .unwrap_or(true)
    }

    pub async fn set_tenant_ingest_auth_required(
        &self,
        id: &str,
        ingest_auth_required: bool,
    ) -> anyhow::Result<bool> {
        self.invalidate_config_caches();
        if self.get_tenant(id).await?.is_none() {
            return Ok(false);
        }
        self.client
            .query("INSERT INTO config_tenant_ingest_auth (tenant_id, ingest_auth_required, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, 0)")
            .bind(id)
            .bind(u8::from(ingest_auth_required))
            .bind(Self::now_str())
            .bind(Self::next_version())
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn set_tenant_auth_required(
        &self,
        id: &str,
        auth_required: bool,
    ) -> anyhow::Result<bool> {
        self.invalidate_config_caches();
        let existing = self.get_tenant(id).await?;
        let (_, name, enabled, _, created_at) = match existing {
            Some(t) => t,
            None => return Ok(false),
        };
        // Materialize the current inherited ingest policy before changing the
        // query policy so the two controls remain independent from this point.
        let ingest_auth_required = self
            .tenant_ingest_auth_required_checked(id)
            .await?
            .unwrap_or(true);
        self.set_tenant_ingest_auth_required(id, ingest_auth_required)
            .await?;
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 0)")
            .bind(id)
            .bind(&name)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(if auth_required { 1u8 } else { 0u8 })
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn delete_tenant(&self, id: &str) -> anyhow::Result<bool> {
        self.invalidate_config_caches();
        let existing = self.get_tenant(id).await?;
        let (_, name, enabled, auth_required, created_at) = match existing {
            Some(t) => t,
            None => return Ok(false),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenants (id, name, enabled, auth_required, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 1)")
            .bind(id)
            .bind(&name)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(if auth_required { 1u8 } else { 0u8 })
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    // ── Tenant retention operations ───────────────────────────────────────────

    pub async fn get_tenant_retention(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<(String, i32)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            signal: String,
            retain_days: i32,
        }
        let rows = self.client
            .query("SELECT signal, retain_days FROM config_tenant_retention FINAL WHERE tenant_id = ? AND is_deleted = 0")
            .bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.signal, r.retain_days))
            .collect())
    }

    pub async fn set_tenant_retention(
        &self,
        tenant_id: &str,
        signal: &str,
        days: i32,
    ) -> anyhow::Result<()> {
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenant_retention (tenant_id, signal, retain_days, version, is_deleted) VALUES (?, ?, ?, ?, 0)")
            .bind(tenant_id)
            .bind(signal)
            .bind(days)
            .bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn delete_tenant_retention(
        &self,
        tenant_id: &str,
        signal: &str,
    ) -> anyhow::Result<bool> {
        let existing = self.get_tenant_retention(tenant_id).await?;
        let found = existing.iter().find(|(s, _)| s == signal);
        if found.is_none() {
            return Ok(false);
        }
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenant_retention (tenant_id, signal, retain_days, version, is_deleted) VALUES (?, ?, 0, ?, 1)")
            .bind(tenant_id)
            .bind(signal)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn list_all_tenant_retention(&self) -> anyhow::Result<Vec<(String, String, i32)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            tenant_id: String,
            signal: String,
            retain_days: i32,
        }
        let rows = self.client
            .query("SELECT tenant_id, signal, retain_days FROM config_tenant_retention FINAL WHERE is_deleted = 0 ORDER BY tenant_id, signal")
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.tenant_id, r.signal, r.retain_days))
            .collect())
    }

    // ── Tenant ingest-signal operations ────────────────────────────────────────

    /// Whether `signal` ingest is enabled for `tenant_id_or_name`. Accepts a
    /// tenant id OR name (cloudwatch/dd URL paths pass a name), resolving the id
    /// the same way `tenant_flags` does. Defaults to TRUE when no explicit row
    /// exists, so existing tenants keep ingesting everything. Cached per
    /// (tenant, signal) with CONFIG_CACHE_TTL since this is hit on every ingest.
    pub async fn tenant_signal_enabled(&self, tenant_id_or_name: &str, signal: &str) -> bool {
        let key = (tenant_id_or_name.to_string(), signal.to_string());
        if let Some(entry) = self.signal_cache.get(&key) {
            let (enabled, at) = entry.value();
            if Self::cache_fresh(*at) {
                return *enabled;
            }
        }
        // Resolve to a canonical id (name-or-id → id). Unknown tenant → keep the
        // passed value as the key; default-enabled still applies.
        let resolved = self
            .tenant_flags(tenant_id_or_name)
            .await
            .ok()
            .flatten()
            .map(|(id, ..)| id)
            .unwrap_or_else(|| tenant_id_or_name.to_string());

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            enabled: u8,
        }
        let result = self.client
            .query("SELECT enabled FROM config_tenant_signals FINAL WHERE tenant_id = ? AND signal = ? AND is_deleted = 0 LIMIT 1")
            .bind(&resolved)
            .bind(signal)
            .fetch_one::<Row>()
            .await;
        let enabled = match result {
            Ok(r) => r.enabled != 0,
            // No row (or any read error) → default enabled (backward compatible).
            Err(_) => true,
        };
        self.maintain_config_caches();
        if self.signal_cache.len() < MAX_CONFIG_CACHE_ENTRIES
            || self.signal_cache.contains_key(&key)
        {
            self.signal_cache.insert(key, (enabled, Instant::now()));
        }
        enabled
    }

    /// Explicitly stored signal flags for a tenant (no defaults filled in).
    pub async fn get_tenant_signals(&self, tenant_id: &str) -> anyhow::Result<Vec<(String, bool)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            signal: String,
            enabled: u8,
        }
        let rows = self.client
            .query("SELECT signal, enabled FROM config_tenant_signals FINAL WHERE tenant_id = ? AND is_deleted = 0")
            .bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.signal, r.enabled != 0))
            .collect())
    }

    /// Upsert a tenant signal flag. Versioned (microseconds) like retention.
    pub async fn set_tenant_signal(
        &self,
        tenant_id: &str,
        signal: &str,
        enabled: bool,
    ) -> anyhow::Result<()> {
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_tenant_signals (tenant_id, signal, enabled, version, is_deleted) VALUES (?, ?, ?, ?, 0)")
            .bind(tenant_id)
            .bind(signal)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(ver)
            .execute()
            .await?;
        self.invalidate_config_caches();
        Ok(())
    }

    // ── Global retention operations ────────────────────────────────────────────

    /// Seed the singleton global-retention row if absent: 365d default, all
    /// signals inheriting it (per-signal value 0 = inherit default).
    /// Seed the UI-editable global-retention store ONCE (when empty) from the
    /// caller-supplied defaults — normally `rushConfig.retention.defaults`, so a
    /// fresh install's tenant/UI retention matches Helm instead of a hardcoded 365.
    /// Existing clusters keep whatever's already stored (edit via the UI/API).
    /// Per-signal 0 = inherit `default_days`.
    pub async fn ensure_global_retention(
        &self,
        default_days: i32,
        logs_days: i32,
        metrics_days: i32,
        apm_days: i32,
    ) -> anyhow::Result<()> {
        if self.get_global_retention().await?.is_none() {
            self.set_global_retention(default_days, logs_days, metrics_days, apm_days)
                .await?;
        }
        Ok(())
    }

    /// Raw stored global retention (per-signal 0 = inherit `default_days`).
    /// Returns None if unset.
    pub async fn get_global_retention(&self) -> anyhow::Result<Option<GlobalRetention>> {
        let result = self.client
            .query("SELECT default_days, logs_days, metrics_days, apm_days FROM config_global_retention FINAL WHERE id = 'global' AND is_deleted = 0 LIMIT 1")
            .fetch_one::<GlobalRetention>()
            .await;
        match result {
            Ok(r) => Ok(Some(r)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn set_global_retention(
        &self,
        default_days: i32,
        logs_days: i32,
        metrics_days: i32,
        apm_days: i32,
    ) -> anyhow::Result<()> {
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_global_retention (id, default_days, logs_days, metrics_days, apm_days, version, is_deleted) VALUES ('global', ?, ?, ?, ?, ?, 0)")
            .bind(default_days)
            .bind(logs_days)
            .bind(metrics_days)
            .bind(apm_days)
            .bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    // ── Metric firewall operations ─────────────────────────────────────────────

    pub async fn list_metric_firewall(&self) -> anyhow::Result<Vec<MetricFirewallRule>> {
        let rows = self.client
            .query("SELECT id, name, enabled, action, metric_pattern, metric_regex, match_label_key, match_label_value, match_label_value_regex, drop_label_pattern, drop_label_regex, created_at FROM config_metric_firewall FINAL WHERE is_deleted = 0 ORDER BY created_at")
            .fetch_all::<MetricFirewallRule>()
            .await?;
        Ok(rows)
    }

    /// Insert/replace a rule (ReplacingMergeTree keyed by id + version).
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_metric_firewall(&self, r: &MetricFirewallRule) -> anyhow::Result<()> {
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_metric_firewall (id, name, enabled, action, metric_pattern, metric_regex, match_label_key, match_label_value, match_label_value_regex, drop_label_pattern, drop_label_regex, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(&r.id).bind(&r.name).bind(r.enabled).bind(&r.action)
            .bind(&r.metric_pattern).bind(r.metric_regex)
            .bind(&r.match_label_key).bind(&r.match_label_value).bind(r.match_label_value_regex)
            .bind(&r.drop_label_pattern).bind(r.drop_label_regex)
            .bind(&r.created_at).bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn delete_metric_firewall(&self, id: &str) -> anyhow::Result<bool> {
        let existing = self.list_metric_firewall().await?;
        let Some(r) = existing.into_iter().find(|r| r.id == id) else {
            return Ok(false);
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_metric_firewall (id, name, enabled, action, metric_pattern, metric_regex, match_label_key, match_label_value, match_label_value_regex, drop_label_pattern, drop_label_regex, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(&r.id).bind(&r.name).bind(r.enabled).bind(&r.action)
            .bind(&r.metric_pattern).bind(r.metric_regex)
            .bind(&r.match_label_key).bind(&r.match_label_value).bind(r.match_label_value_regex)
            .bind(&r.drop_label_pattern).bind(r.drop_label_regex)
            .bind(&r.created_at).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    /// Load + compile the firewall rules for the ingest hot path.
    pub async fn compiled_metric_firewall(
        &self,
    ) -> anyhow::Result<crate::metric_firewall::MetricFirewall> {
        let rows = self.list_metric_firewall().await?;
        let raw: Vec<crate::metric_firewall::RawRule> = rows
            .iter()
            .map(|r| crate::metric_firewall::RawRule {
                enabled: r.enabled != 0,
                action: r.action.clone(),
                metric_pattern: r.metric_pattern.clone(),
                metric_regex: r.metric_regex != 0,
                match_label_key: r.match_label_key.clone(),
                match_label_value: r.match_label_value.clone(),
                match_label_value_regex: r.match_label_value_regex != 0,
                drop_label_pattern: r.drop_label_pattern.clone(),
                drop_label_regex: r.drop_label_regex != 0,
            })
            .collect();
        Ok(crate::metric_firewall::MetricFirewall::compile(&raw))
    }

    // ── User & session operations ──────────────────────────────────────────────

    pub async fn ensure_default_admin(&self) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count {
            n: u64,
        }
        let row = self
            .client
            .query("SELECT count() AS n FROM config_users FINAL WHERE is_deleted = 0")
            .fetch_one::<Count>()
            .await?;
        if row.n > 0 {
            return Ok(None);
        }

        let initial_password = std::env::var("INITIAL_ADMIN_PASSWORD")
            .ok()
            .filter(|password| !password.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "INITIAL_ADMIN_PASSWORD is required when creating the initial admin; provide it through a secret"
                )
            })?;

        let id = uuid::Uuid::new_v4().to_string();
        let password_hash = hash_password(&initial_password)?;
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 1, 'local', '', ?, ?, 0)")
            .bind(&id)
            .bind("admin")
            .bind(&password_hash)
            .bind("Admin")
            .bind("default")
            .bind("admin")
            .bind(&now)
            .bind(ver)
            .execute()
            .await?;

        tracing::warn!(
            username = "admin",
            "initial administrator created; retrieve the password from the configured secret and change it after first login"
        );
        Ok(Some(id))
    }

    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> anyhow::Result<Option<(String, String, String, String, String, u64)>> {
        if username.trim().is_empty()
            || username.len() > MAX_USERNAME_BYTES
            || password.is_empty()
            || password.len() > MAX_PASSWORD_BYTES
        {
            return Ok(None);
        }
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            username: String,
            password_hash: String,
            display_name: String,
            tenant_id: String,
            auth_provider: String,
            version: u64,
        }

        let mut rows = self.client
            .query("SELECT id, username, password_hash, display_name, tenant_id, auth_provider, version FROM config_users FINAL WHERE lowerUTF8(trimBoth(username)) = lowerUTF8(trimBoth(?)) AND enabled = 1 AND is_deleted = 0 ORDER BY id LIMIT 2")
            .bind(username)
            .fetch_all::<Row>()
            .await?;

        // Only local identities have a password. Unknown, disabled, SSO, and
        // ambiguous identities all verify the same dummy Argon2 hash so their
        // response timing does not reveal account state or provider type.
        let usable_local_identity = rows.len() == 1 && rows[0].auth_provider == "local";
        let password_hash = if usable_local_identity {
            rows[0].password_hash.as_str()
        } else {
            dummy_password_hash()
        };
        let credentials_valid = verify_password(password, password_hash);

        if rows.len() > 1 {
            anyhow::bail!(
                "canonical username collision detected during authentication; refusing ambiguous identity"
            );
        }
        if !usable_local_identity || !credentials_valid {
            return Ok(None);
        }
        let row = rows.pop().expect("one local identity was checked above");
        // Derive role from group membership
        let role = self
            .derive_user_role(&row.id)
            .await
            .unwrap_or_else(|_| "viewer".to_string());
        Ok(Some((
            row.id,
            row.username,
            row.display_name,
            row.tenant_id,
            role,
            row.version,
        )))
    }

    async fn derive_user_role(&self, user_id: &str) -> anyhow::Result<String> {
        self.derive_user_role_with_rows(user_id)
            .await
            .map(|(role, _)| role)
    }

    async fn derive_user_role_with_rows(&self, user_id: &str) -> anyhow::Result<(String, u64)> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            permissions: String,
        }
        let rows = self.client
            .query("SELECT g.permissions FROM config_user_groups ug FINAL JOIN config_groups g FINAL ON ug.group_id = g.id WHERE ug.user_id = ? AND ug.is_deleted = 0 AND g.is_deleted = 0")
            .bind(user_id)
            .fetch_all::<Row>()
            .await?;
        for row in &rows {
            if let Ok(perms) = serde_json::from_str::<Vec<String>>(&row.permissions) {
                if perms.contains(&"admin".to_string()) {
                    return Ok(("admin".to_string(), rows.len() as u64));
                }
            }
        }
        for row in &rows {
            if let Ok(perms) = serde_json::from_str::<Vec<String>>(&row.permissions) {
                if perms.contains(&"write".to_string()) {
                    return Ok(("write".to_string(), rows.len() as u64));
                }
            }
        }
        Ok(("viewer".to_string(), rows.len() as u64))
    }

    /// Invalidate session rows created before keyed HMAC storage was enabled.
    /// Their raw bearers are intentionally unavailable, so conversion is not
    /// possible. Startup calls this before serving HTTP and audits the count.
    pub async fn invalidate_legacy_session_tokens(&self) -> anyhow::Result<u64> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct CountRow {
            count: u64,
        }

        let row = self
            .client
            .query("SELECT count() AS count FROM config_sessions WHERE NOT startsWith(token, ?)")
            .bind(SESSION_HMAC_PREFIX)
            .fetch_one::<CountRow>()
            .await?;
        if row.count == 0 {
            return Ok(0);
        }

        self.client
            .query("DELETE FROM config_sessions WHERE NOT startsWith(token, ?)")
            .with_option("lightweight_deletes_sync", "1")
            .bind(SESSION_HMAC_PREFIX)
            .execute()
            .await?;
        Ok(row.count)
    }

    async fn insert_session(
        &self,
        user_id: &str,
        user_version: u64,
        auth_method: &str,
        provider_id: &str,
    ) -> anyhow::Result<IssuedSession> {
        let token: String = {
            use rand::Rng;
            let mut rng = rand::rng();
            let bytes: [u8; 32] = rng.random();
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        };

        let session_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now();
        let created_at = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let absolute_expires =
            now + chrono::Duration::seconds(self.session_policy.absolute_timeout_secs);
        let idle_expires = now + chrono::Duration::seconds(self.session_policy.idle_timeout_secs);
        let expires_at = idle_expires
            .min(absolute_expires)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let absolute_expires_at = absolute_expires.format("%Y-%m-%d %H:%M:%S").to_string();
        let stored_token = session_storage_key(&self.session_hmac_secret, &token);
        self.client
            .query("INSERT INTO config_sessions (token, session_id, user_id, user_version, auth_method, provider_id, created_at, last_seen_at, expires_at, absolute_expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&stored_token)
            .bind(&session_id)
            .bind(user_id)
            .bind(user_version)
            .bind(auth_method)
            .bind(provider_id)
            .bind(&created_at)
            .bind(&created_at)
            .bind(&expires_at)
            .bind(&absolute_expires_at)
            .execute()
            .await?;
        Ok(IssuedSession {
            token,
            max_age_seconds: self.session_policy.idle_timeout_secs,
        })
    }

    /// Create a session for an SSO-authenticated identity using its latest user
    /// row. Local password authentication uses `create_session_at_version` so a
    /// concurrent password change cannot mint a session from stale credentials.
    pub async fn create_sso_session(
        &self,
        user_id: &str,
        auth_method: &str,
        provider_id: &str,
    ) -> anyhow::Result<IssuedSession> {
        if !matches!(auth_method, "oidc" | "saml") || provider_id.is_empty() {
            anyhow::bail!("invalid SSO session provenance");
        }
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            version: u64,
        }
        let row = self
            .client
            .query("SELECT version FROM config_users FINAL WHERE id = ? AND enabled = 1 AND is_deleted = 0 LIMIT 1")
            .bind(user_id)
            .fetch_one::<Row>()
            .await?;
        self.insert_session(user_id, row.version, auth_method, provider_id)
            .await
    }

    pub async fn create_session_at_version(
        &self,
        user_id: &str,
        authenticated_user_version: u64,
    ) -> anyhow::Result<IssuedSession> {
        self.insert_session(user_id, authenticated_user_version, "local", "")
            .await
    }

    pub async fn get_session_user(
        &self,
        token: &str,
    ) -> Option<(String, String, String, String, String)> {
        self.get_session_user_inner(token, None).await
    }

    /// Session validation with bounded self-metrics for the constituent user
    /// and role lookups. The authorization result is unchanged.
    pub async fn get_session_user_observed(
        &self,
        token: &str,
        metrics: &crate::self_metrics::SelfMetrics,
    ) -> Option<SessionUser> {
        self.get_session_user_inner(token, Some(metrics)).await
    }

    async fn get_session_user_inner(
        &self,
        token: &str,
        metrics: Option<&crate::self_metrics::SelfMetrics>,
    ) -> Option<SessionUser> {
        let stored_token = session_storage_key(&self.session_hmac_secret, token);
        // Session authorization is deliberately not cached. Password changes,
        // user disables, and logout must become visible to every API replica
        // without a per-process cache grace period.
        #[derive(clickhouse::Row, serde::Deserialize)]
        #[allow(dead_code)]
        struct Row {
            id: String,
            username: String,
            display_name: String,
            tenant_id: String,
            expires_at: String,
            user_id: String,
            auth_method: String,
            provider_id: String,
            role: String,
            active_provider_id: String,
            active_provider_present: u8,
        }
        let now = Self::now_str();
        let sql = "SELECT u.id, u.username, u.display_name, u.tenant_id,
                          s.expires_at, s.user_id, s.auth_method, s.provider_id,
                          ifNull(roles.role, 'viewer') AS role,
                          ifNull(active.provider_id, '') AS active_provider_id,
                          ifNull(active.present, 0) AS active_provider_present
                   FROM config_sessions s
                   JOIN config_users u FINAL ON s.user_id = u.id
                   LEFT JOIN (
                       SELECT ug.user_id,
                              multiIf(
                                  countIf(has(JSONExtract(g.permissions, 'Array(String)'), 'admin')) > 0, 'admin',
                                  countIf(has(JSONExtract(g.permissions, 'Array(String)'), 'write')) > 0, 'write',
                                  'viewer'
                              ) AS role
                       FROM config_user_groups ug FINAL
                       JOIN config_groups g FINAL ON ug.group_id = g.id
                       WHERE ug.is_deleted = 0 AND g.is_deleted = 0
                       GROUP BY ug.user_id
                   ) roles ON roles.user_id = u.id
                   LEFT JOIN (
                       SELECT provider_id, toUInt8(1) AS present
                       FROM config_sso_active_provider FINAL
                       WHERE slot = 'primary'
                       LIMIT 1
                   ) active ON 1
                   WHERE s.token = ?
                     AND s.user_version = u.version
                     AND u.enabled = 1
                     AND u.is_deleted = 0
                     AND s.expires_at > ?
                     AND s.absolute_expires_at > ?
                     AND s.session_id NOT IN (
                         SELECT session_id FROM config_session_revocations WHERE expires_at > ?
                     )
                     AND s.token NOT IN (
                         SELECT token FROM config_session_rotation_grace WHERE grace_expires_at <= ?
                     )
                   LIMIT 1";
        let user_started = Instant::now();
        let result = self
            .client
            .query(sql)
            .bind(&stored_token)
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .fetch_one::<Row>()
            .await;
        if let Some(metrics) = metrics {
            metrics.record_auth_lookup(
                "user",
                user_started.elapsed().as_secs_f64() * 1_000.0,
                u64::from(result.is_ok()),
                match &result {
                    Ok(_) => "ok",
                    Err(clickhouse::error::Error::RowNotFound) => "not_found",
                    Err(_) => "error",
                },
            );
        }
        let row = result.ok()?;
        match row.auth_method.as_str() {
            "local" if row.provider_id.is_empty() => {}
            "oidc" | "saml" => {
                let active_provider_id = if row.active_provider_present != 0 {
                    (!row.active_provider_id.is_empty()).then_some(row.active_provider_id.clone())
                } else {
                    // Startup reconciliation normally makes the joined row
                    // authoritative. Keep the legacy path only for an upgrade
                    // window where the singleton row does not exist yet.
                    self.effective_active_sso_provider_id().await.ok()?
                };
                if active_provider_id.as_deref() != Some(row.provider_id.as_str()) {
                    return None;
                }
            }
            // Sessions issued before provenance binding fail closed.
            _ => return None,
        }
        let user: SessionUser = (
            row.id,
            row.username,
            row.display_name,
            row.tenant_id,
            row.role,
        );
        Some(user)
    }

    pub async fn list_auth_sessions(
        &self,
        user_id: Option<&str>,
        current_token: &str,
    ) -> anyhow::Result<Vec<AuthSessionInfo>> {
        let now = Self::now_str();
        let base = "SELECT s.session_id, s.user_id, u.username, u.tenant_id, s.auth_method, s.provider_id, s.created_at, s.last_seen_at, s.expires_at, s.absolute_expires_at, s.token FROM config_sessions s JOIN config_users u FINAL ON s.user_id = u.id WHERE s.session_id != '' AND s.user_version = u.version AND u.enabled = 1 AND u.is_deleted = 0 AND s.expires_at > ? AND s.absolute_expires_at > ? AND s.session_id NOT IN (SELECT session_id FROM config_session_revocations WHERE expires_at > ?) AND s.token NOT IN (SELECT token FROM config_session_rotation_grace WHERE grace_expires_at <= ?)";
        let rows = if let Some(user_id) = user_id {
            self.client
                .query(&format!(
                    "{base} AND s.user_id = ? ORDER BY s.last_seen_at DESC LIMIT 100"
                ))
                .bind(&now)
                .bind(&now)
                .bind(&now)
                .bind(&now)
                .bind(user_id)
                .fetch_all::<AuthSessionRow>()
                .await?
        } else {
            self.client
                .query(&format!("{base} ORDER BY s.last_seen_at DESC LIMIT 1000"))
                .bind(&now)
                .bind(&now)
                .bind(&now)
                .bind(&now)
                .fetch_all::<AuthSessionRow>()
                .await?
        };
        let mut seen = std::collections::HashSet::new();
        Ok(rows
            .into_iter()
            .filter(|row| seen.insert(row.session_id.clone()))
            .map(|row| row.into_info(&self.session_hmac_secret, current_token))
            .collect())
    }

    /// Revoke one public session id. When `user_id` is provided, ownership is
    /// part of the lookup and delete so a user can never target another account.
    pub async fn revoke_auth_session(
        &self,
        session_id: &str,
        user_id: Option<&str>,
        current_token: &str,
    ) -> anyhow::Result<Option<AuthSessionInfo>> {
        uuid::Uuid::parse_str(session_id)
            .map_err(|_| anyhow::anyhow!("invalid session identifier"))?;
        let now = Self::now_str();
        let base = "SELECT s.session_id, s.user_id, u.username, u.tenant_id, s.auth_method, s.provider_id, s.created_at, s.last_seen_at, s.expires_at, s.absolute_expires_at, s.token FROM config_sessions s JOIN config_users u FINAL ON s.user_id = u.id WHERE s.session_id = ? AND s.expires_at > ? AND s.absolute_expires_at > ?";
        let row = if let Some(user_id) = user_id {
            self.client
                .query(&format!("{base} AND s.user_id = ? LIMIT 1"))
                .bind(session_id)
                .bind(&now)
                .bind(&now)
                .bind(user_id)
                .fetch_optional::<AuthSessionRow>()
                .await?
        } else {
            self.client
                .query(&format!("{base} LIMIT 1"))
                .bind(session_id)
                .bind(&now)
                .bind(&now)
                .fetch_optional::<AuthSessionRow>()
                .await?
        };
        let Some(row) = row else {
            return Ok(None);
        };
        let absolute_expires_at = row.absolute_expires_at.clone();
        let info = row.into_info(&self.session_hmac_secret, current_token);
        self.client
            .query("INSERT INTO config_session_revocations (session_id, expires_at, revoked_at) VALUES (?, ?, ?)")
            .bind(session_id)
            .bind(&absolute_expires_at)
            .bind(Self::now_str())
            .execute()
            .await?;
        let mut delete = self
            .client
            .query(if user_id.is_some() {
                "DELETE FROM config_sessions WHERE session_id = ? AND user_id = ?"
            } else {
                "DELETE FROM config_sessions WHERE session_id = ?"
            })
            .with_option("lightweight_deletes_sync", "1")
            .bind(session_id);
        if let Some(user_id) = user_id {
            delete = delete.bind(user_id);
        }
        delete.execute().await?;
        Ok(Some(info))
    }

    /// Renew an active session by replacing its bearer. The absolute deadline
    /// and public session id are preserved, so renewal cannot create an
    /// immortal session or make inventory entries jump around.
    pub async fn rotate_session_if_due(
        &self,
        token: &str,
    ) -> anyhow::Result<Option<RotatedSession>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            session_id: String,
            user_id: String,
            user_version: u64,
            username: String,
            tenant_id: String,
            auth_method: String,
            provider_id: String,
            created_at: String,
            last_seen_at: String,
            last_seen_unix: i64,
            expires_unix: i64,
            absolute_expires_unix: i64,
            absolute_expires_at: String,
        }

        let now = chrono::Utc::now();
        let now_unix = now.timestamp();
        let now_string = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let stored_token = session_storage_key(&self.session_hmac_secret, token);
        let row = self
            .client
            .query("SELECT s.session_id, s.user_id, s.user_version, u.username, u.tenant_id, s.auth_method, s.provider_id, s.created_at, s.last_seen_at, toInt64(toUnixTimestamp(parseDateTimeBestEffort(s.last_seen_at))) AS last_seen_unix, toInt64(toUnixTimestamp(parseDateTimeBestEffort(s.expires_at))) AS expires_unix, toInt64(toUnixTimestamp(parseDateTimeBestEffort(s.absolute_expires_at))) AS absolute_expires_unix, s.absolute_expires_at FROM config_sessions s JOIN config_users u FINAL ON s.user_id = u.id WHERE s.token = ? AND s.session_id != '' AND s.user_version = u.version AND u.enabled = 1 AND u.is_deleted = 0 AND s.expires_at > ? AND s.absolute_expires_at > ? AND s.session_id NOT IN (SELECT session_id FROM config_session_revocations WHERE expires_at > ?) LIMIT 1")
            .bind(&stored_token)
            .bind(&now_string)
            .bind(&now_string)
            .bind(&now_string)
            .fetch_optional::<Row>()
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        if now_unix.saturating_sub(row.last_seen_unix) < self.session_policy.renewal_interval_secs {
            return Ok(None);
        }
        match row.auth_method.as_str() {
            "local" if row.provider_id.is_empty() => {}
            "oidc" | "saml" => {
                let active_provider_id = self.effective_active_sso_provider_id().await?;
                if active_provider_id.as_deref() != Some(row.provider_id.as_str()) {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        }

        let claim_expiry = row.expires_unix.min(row.absolute_expires_unix);
        if !self
            .claim_sso_key_once(&format!("session-rotation:{stored_token}"), claim_expiry)
            .await?
        {
            return Ok(None);
        }

        let remaining_absolute = row.absolute_expires_unix.saturating_sub(now_unix);
        if remaining_absolute <= 0 {
            return Ok(None);
        }
        let max_age_seconds = self
            .session_policy
            .idle_timeout_secs
            .min(remaining_absolute);
        let expires_at = (now + chrono::Duration::seconds(max_age_seconds))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let grace_expires_unix = now_unix
            .saturating_add(SESSION_ROTATION_GRACE_SECS)
            .min(row.absolute_expires_unix);
        let grace_expires_at = chrono::DateTime::from_timestamp(grace_expires_unix, 0)
            .ok_or_else(|| anyhow::anyhow!("invalid session rotation grace deadline"))?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let grace_record_expires_unix = row.expires_unix.max(grace_expires_unix);
        let grace_record_expires_at =
            chrono::DateTime::from_timestamp(grace_record_expires_unix, 0)
                .ok_or_else(|| anyhow::anyhow!("invalid session rotation cleanup deadline"))?
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();
        let new_token: String = {
            use rand::Rng;
            let bytes: [u8; 32] = rand::rng().random();
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        };
        let new_stored_token = session_storage_key(&self.session_hmac_secret, &new_token);

        // Write the replacement before superseding the old bearer. Requests
        // that were already in flight may keep using the old cookie during a
        // short shared grace period, which avoids a logout race across API pods.
        self.client
            .query("INSERT INTO config_sessions (token, session_id, user_id, user_version, auth_method, provider_id, created_at, last_seen_at, expires_at, absolute_expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&new_stored_token)
            .bind(&row.session_id)
            .bind(&row.user_id)
            .bind(row.user_version)
            .bind(&row.auth_method)
            .bind(&row.provider_id)
            .bind(&row.created_at)
            .bind(&now_string)
            .bind(&expires_at)
            .bind(&row.absolute_expires_at)
            .execute()
            .await?;

        self.client
            .query("INSERT INTO config_session_rotation_grace (token, grace_expires_at, expires_at, created_at) VALUES (?, ?, ?, ?)")
            .bind(&stored_token)
            .bind(&grace_expires_at)
            .bind(&grace_record_expires_at)
            .bind(&now_string)
            .execute()
            .await?;

        // Extend a bearer that rotated very close to its idle deadline through
        // the full grace period. The grace table rejects every copy afterward.
        if let Err(error) = self.client
            .query("INSERT INTO config_sessions (token, session_id, user_id, user_version, auth_method, provider_id, created_at, last_seen_at, expires_at, absolute_expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&stored_token)
            .bind(&row.session_id)
            .bind(&row.user_id)
            .bind(row.user_version)
            .bind(&row.auth_method)
            .bind(&row.provider_id)
            .bind(&row.created_at)
            .bind(&row.last_seen_at)
            .bind(&grace_expires_at)
            .bind(&row.absolute_expires_at)
            .execute()
            .await
        {
            tracing::warn!(%error, session_id = %row.session_id, "failed to extend rotated session through grace period");
        }

        Ok(Some(RotatedSession {
            issued: IssuedSession {
                token: new_token,
                max_age_seconds,
            },
            session_id: row.session_id,
            user_id: row.user_id,
            username: row.username,
            tenant_id: row.tenant_id,
        }))
    }

    pub async fn delete_session(&self, token: &str) -> anyhow::Result<()> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            session_id: String,
            absolute_expires_at: String,
        }
        let stored_token = session_storage_key(&self.session_hmac_secret, token);
        let row = self
            .client
            .query("SELECT session_id, absolute_expires_at FROM config_sessions WHERE token = ? AND session_id != '' LIMIT 1")
            .bind(&stored_token)
            .fetch_optional::<Row>()
            .await?;
        if let Some(row) = row {
            // Persist the tombstone before deleting the bearer. Any concurrent
            // rotation that inserts afterward remains invalid on every lookup.
            self.client
                .query("INSERT INTO config_session_revocations (session_id, expires_at, revoked_at) VALUES (?, ?, ?)")
                .bind(&row.session_id)
                .bind(&row.absolute_expires_at)
                .bind(Self::now_str())
                .execute()
                .await?;
            self.client
                .query("DELETE FROM config_sessions WHERE session_id = ?")
                .with_option("lightweight_deletes_sync", "1")
                .bind(&row.session_id)
                .execute()
                .await?;
            return Ok(());
        }
        // Lightweight DELETE instead of a heavyweight ALTER ... DELETE mutation:
        // marks rows via a mask column instead of rewriting parts. Waiting for
        // completion ensures a successful logout response means the bearer is
        // no longer accepted by any subsequent request.
        self.client
            .query("DELETE FROM config_sessions WHERE token = ?")
            .with_option("lightweight_deletes_sync", "1")
            .bind(&stored_token)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn record_login_ip_attempt(&self, ip_hash: &str) -> anyhow::Result<()> {
        self.client
            .query("INSERT INTO config_login_attempts (attempted_at, ip_hash, account_hash) VALUES (?, ?, ?)")
            .bind(Self::now_str())
            .bind(ip_hash)
            .bind("")
            .execute()
            .await?;
        Ok(())
    }

    pub async fn record_login_account_failure(&self, account_hash: &str) -> anyhow::Result<()> {
        self.client
            .query("INSERT INTO config_login_attempts (attempted_at, ip_hash, account_hash) VALUES (?, ?, ?)")
            .bind(Self::now_str())
            .bind("")
            .bind(account_hash)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn login_ip_attempt_count(&self, ip_hash: &str, since: &str) -> anyhow::Result<u64> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count {
            attempts: u64,
        }
        let count = self
            .client
            .query("SELECT count() AS attempts FROM config_login_attempts WHERE attempted_at >= ? AND ip_hash = ?")
            .bind(since)
            .bind(ip_hash)
            .fetch_one::<Count>()
            .await?;
        Ok(count.attempts)
    }

    pub async fn login_account_failure_count(
        &self,
        account_hash: &str,
        since: &str,
    ) -> anyhow::Result<u64> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count {
            attempts: u64,
        }
        let count = self
            .client
            .query("SELECT count() AS attempts FROM config_login_attempts WHERE attempted_at >= ? AND account_hash = ?")
            .bind(since)
            .bind(account_hash)
            .fetch_one::<Count>()
            .await?;
        Ok(count.attempts)
    }

    pub async fn list_users(
        &self,
    ) -> anyhow::Result<Vec<(String, String, String, String, bool, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            username: String,
            display_name: String,
            tenant_id: String,
            enabled: u8,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, username, display_name, tenant_id, enabled, created_at FROM config_users FINAL WHERE is_deleted = 0 ORDER BY created_at ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.id,
                    r.username,
                    r.display_name,
                    r.tenant_id,
                    r.enabled != 0,
                    r.created_at,
                )
            })
            .collect())
    }

    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        display_name: &str,
    ) -> anyhow::Result<String> {
        self.invalidate_config_caches();
        let _username_guard = self.username_mutation_lock.lock().await;
        self.ensure_username_available(username).await?;
        let id = uuid::Uuid::new_v4().to_string();
        let password_hash = hash_password(password)?;
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, 'default', 'viewer', 1, 'local', '', ?, ?, 0)")
            .bind(&id)
            .bind(username)
            .bind(&password_hash)
            .bind(display_name)
            .bind(&now)
            .bind(ver)
            .execute()
            .await?;
        Ok(id)
    }

    pub async fn delete_user(&self, id: &str) -> anyhow::Result<bool> {
        self.invalidate_config_caches();
        let existing = self.get_user(id).await?;
        if existing.is_none() {
            return Ok(false);
        }
        // Remove sessions
        let _ = self
            .client
            .query("ALTER TABLE config_sessions DELETE WHERE user_id = ?")
            .bind(id)
            .execute()
            .await;
        // Soft-delete user
        let (_, username, display_name, tenant_id, _, created_at) = existing.unwrap();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, '!deleted', ?, ?, 'viewer', 0, 'local', '', ?, ?, 1)")
            .bind(id)
            .bind(&username)
            .bind(&display_name)
            .bind(&tenant_id)
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn get_user(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<(String, String, String, String, bool, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            username: String,
            display_name: String,
            tenant_id: String,
            enabled: u8,
            created_at: String,
        }
        let result = self.client
            .query("SELECT id, username, display_name, tenant_id, enabled, created_at FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some((
                r.id,
                r.username,
                r.display_name,
                r.tenant_id,
                r.enabled != 0,
                r.created_at,
            ))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Resolve the current enabled user and role for a short-lived Kubernetes
    /// credential. Looking the user up on every gateway authorization means a
    /// disabled or deleted account stops creating new Kubernetes requests even
    /// when the credential itself has not reached its expiry time yet.
    pub async fn get_active_kubernetes_user(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<SessionUser>> {
        let Some((user_id, username, display_name, tenant_id, enabled, _)) =
            self.get_user(id).await?
        else {
            return Ok(None);
        };
        if !enabled {
            return Ok(None);
        }
        let role = self.derive_user_role(&user_id).await?;
        Ok(Some((user_id, username, display_name, tenant_id, role)))
    }

    pub async fn change_password(
        &self,
        user_id: &str,
        new_password: &str,
    ) -> anyhow::Result<PasswordChangeOutcome> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            username: String,
            display_name: String,
            tenant_id: String,
            role: String,
            enabled: u8,
            auth_provider: String,
            external_id: String,
            created_at: String,
        }

        let existing = self
            .client
            .query("SELECT username, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(user_id)
            .fetch_one::<Row>()
            .await;
        let row = match existing {
            Ok(row) => row,
            Err(clickhouse::error::Error::RowNotFound) => {
                return Ok(PasswordChangeOutcome::UserNotFound);
            }
            Err(error) => return Err(error.into()),
        };
        if row.auth_provider != "local" {
            return Ok(PasswordChangeOutcome::SsoManaged {
                auth_provider: row.auth_provider,
            });
        }

        let password_hash = hash_password(new_password)?;
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(user_id)
            .bind(&row.username)
            .bind(&password_hash)
            .bind(&row.display_name)
            .bind(&row.tenant_id)
            .bind(&row.role)
            .bind(row.enabled)
            .bind(&row.auth_provider)
            .bind(&row.external_id)
            .bind(&row.created_at)
            .bind(ver)
            .execute()
            .await?;
        // The new user `version` is stored in the same row as the password
        // hash, so all older sessions are already invalid at this point. Row
        // deletion is only storage cleanup and cannot make this operation
        // partially succeed from an authorization perspective.
        if let Err(error) = self.delete_sessions_for_user(user_id).await {
            tracing::warn!(user_id, %error, "failed to clean up sessions invalidated by password change");
        }
        Ok(PasswordChangeOutcome::Updated)
    }

    pub async fn delete_sessions_for_user(&self, user_id: &str) -> anyhow::Result<()> {
        self.client
            .query("DELETE FROM config_sessions WHERE user_id = ?")
            .bind(user_id)
            .execute()
            .await?;
        Ok(())
    }

    /// Revoke every session issued by an SSO provider. The second predicate
    /// removes pre-provenance SSO sessions during rolling upgrades without
    /// affecting legacy local users.
    pub async fn revoke_sso_sessions_for_provider(&self, provider_id: &str) -> anyhow::Result<()> {
        self.client
            .query(
                "DELETE FROM config_sessions WHERE provider_id = ? OR (provider_id = '' AND user_id IN (SELECT id FROM config_users FINAL WHERE auth_provider IN ('oidc', 'saml') AND is_deleted = 0))",
            )
            .with_option("lightweight_deletes_sync", "1")
            .bind(provider_id)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn set_user_enabled(&self, user_id: &str, enabled: bool) -> anyhow::Result<bool> {
        self.invalidate_config_caches();
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            username: String,
            password_hash: String,
            display_name: String,
            tenant_id: String,
            role: String,
            auth_provider: String,
            external_id: String,
            created_at: String,
        }
        let row = match self
            .client
            .query("SELECT username, password_hash, display_name, tenant_id, role, auth_provider, external_id, created_at FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(user_id)
            .fetch_one::<Row>()
            .await
        {
            Ok(row) => row,
            Err(clickhouse::error::Error::RowNotFound) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(user_id)
            .bind(&row.username)
            .bind(&row.password_hash)
            .bind(&row.display_name)
            .bind(&row.tenant_id)
            .bind(&row.role)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(&row.auth_provider)
            .bind(&row.external_id)
            .bind(&row.created_at)
            .bind(ver)
            .execute()
            .await?;
        // Invalidate all sessions when disabling a user
        if !enabled {
            let _ = self.delete_sessions_for_user(user_id).await;
        }
        Ok(true)
    }

    pub async fn get_username(&self, user_id: &str) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            username: String,
        }
        let result = self
            .client
            .query(
                "SELECT username FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1",
            )
            .bind(user_id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(r.username)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_user_identity_provider(
        &self,
        user_id: &str,
    ) -> anyhow::Result<Option<(String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            username: String,
            auth_provider: String,
        }
        let result = self
            .client
            .query("SELECT username, auth_provider FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(user_id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(row) => Ok(Some((row.username, row.auth_provider))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn validate_break_glass_account(&self, username: &str) -> anyhow::Result<()> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            auth_provider: String,
            enabled: u8,
        }

        let rows = self
            .client
            .query("SELECT id, auth_provider, enabled FROM config_users FINAL WHERE lowerUTF8(trimBoth(username)) = lowerUTF8(trimBoth(?)) AND is_deleted = 0 ORDER BY id LIMIT 2")
            .bind(username)
            .fetch_all::<Row>()
            .await?;
        if rows.len() != 1 {
            anyhow::bail!("RUSH_BREAK_GLASS_USERNAME must identify exactly one active user");
        }
        let row = &rows[0];
        if row.auth_provider != "local" || row.enabled == 0 {
            anyhow::bail!("RUSH_BREAK_GLASS_USERNAME must identify an enabled local user");
        }
        if self.derive_user_role(&row.id).await? != "admin" {
            anyhow::bail!("RUSH_BREAK_GLASS_USERNAME must identify an administrator");
        }
        Ok(())
    }

    // ── Group operations ───────────────────────────────────────────────────────

    pub async fn ensure_default_groups(&self) -> anyhow::Result<()> {
        // Upsert admins group
        let ver = Self::next_version();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_groups (id, name, description, scopes, permissions, system, created_at, version, is_deleted) VALUES ('admins', 'admins', 'Full access administrators', '[\"all\"]', '[\"read\",\"write\",\"admin\",\"infrastructure:read\"]', 1, ?, ?, 0)")
            .bind(&now)
            .bind(ver)
            .execute()
            .await?;

        let ver2 = Self::next_version();
        self.client
            .query("INSERT INTO config_groups (id, name, description, scopes, permissions, system, created_at, version, is_deleted) VALUES ('viewers', 'viewers', 'Read-only viewers', '[\"all\"]', '[\"read\"]', 1, ?, ?, 0)")
            .bind(&now)
            .bind(ver2)
            .execute()
            .await?;

        // Bind admins to all tenants
        let tenants = self.list_tenants().await?;
        for (tid, _, _, _, _) in &tenants {
            let ver3 = Self::next_version();
            self.client
                .query("INSERT INTO config_group_tenants (group_id, tenant_id, version, is_deleted) VALUES ('admins', ?, ?, 0)")
                .bind(tid)
                .bind(ver3)
                .execute()
                .await?;
        }

        // Assign default groups to users with no existing groups
        let users = self.list_users().await?;
        for (uid, _, _, _, _, _) in &users {
            #[derive(clickhouse::Row, serde::Deserialize)]
            struct Count {
                n: u64,
            }
            let count = self.client
                .query("SELECT count() AS n FROM config_user_groups FINAL WHERE user_id = ? AND is_deleted = 0")
                .bind(uid)
                .fetch_one::<Count>()
                .await
                .map(|r| r.n)
                .unwrap_or(0);
            if count == 0 {
                // Fetch role from users table
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct RoleRow {
                    role: String,
                }
                let role = self.client
                    .query("SELECT role FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
                    .bind(uid)
                    .fetch_one::<RoleRow>()
                    .await
                    .map(|r| r.role)
                    .unwrap_or_else(|_| "viewer".to_string());
                let group_id = if role == "admin" { "admins" } else { "viewers" };
                let ver4 = Self::next_version();
                self.client
                    .query("INSERT INTO config_user_groups (user_id, group_id, version, is_deleted) VALUES (?, ?, ?, 0)")
                    .bind(uid)
                    .bind(group_id)
                    .bind(ver4)
                    .execute()
                    .await?;
            }
        }
        tracing::info!("default groups ensured (admins, viewers)");
        Ok(())
    }

    pub async fn list_groups(
        &self,
    ) -> anyhow::Result<
        Vec<(
            String,
            String,
            String,
            String,
            String,
            bool,
            String,
            Vec<String>,
        )>,
    > {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct GRow {
            id: String,
            name: String,
            description: String,
            scopes: String,
            permissions: String,
            system: u8,
            created_at: String,
        }
        let groups = self.client
            .query("SELECT id, name, description, scopes, permissions, system, created_at FROM config_groups FINAL WHERE is_deleted = 0 ORDER BY created_at ASC")
            .fetch_all::<GRow>()
            .await?;

        let mut result = Vec::new();
        for g in groups {
            let tids = self.get_group_tenant_ids(&g.id).await?;
            result.push((
                g.id,
                g.name,
                g.description,
                g.scopes,
                g.permissions,
                g.system != 0,
                g.created_at,
                tids,
            ));
        }
        Ok(result)
    }

    async fn get_group_tenant_ids(&self, group_id: &str) -> anyhow::Result<Vec<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            tenant_id: String,
        }
        let rows = self.client
            .query("SELECT tenant_id FROM config_group_tenants FINAL WHERE group_id = ? AND is_deleted = 0")
            .bind(group_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| r.tenant_id).collect())
    }

    pub async fn get_group(
        &self,
        id: &str,
    ) -> anyhow::Result<
        Option<(
            String,
            String,
            String,
            String,
            String,
            bool,
            String,
            Vec<String>,
        )>,
    > {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct GRow {
            id: String,
            name: String,
            description: String,
            scopes: String,
            permissions: String,
            system: u8,
            created_at: String,
        }
        let result = self.client
            .query("SELECT id, name, description, scopes, permissions, system, created_at FROM config_groups FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<GRow>()
            .await;
        match result {
            Ok(g) => {
                let tids = self.get_group_tenant_ids(&g.id).await?;
                Ok(Some((
                    g.id,
                    g.name,
                    g.description,
                    g.scopes,
                    g.permissions,
                    g.system != 0,
                    g.created_at,
                    tids,
                )))
            }
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_group(
        &self,
        name: &str,
        description: &str,
        scopes: &str,
        permissions: &str,
    ) -> anyhow::Result<String> {
        self.invalidate_config_caches();
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_groups (id, name, description, scopes, permissions, system, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 0, ?, ?, 0)")
            .bind(&id)
            .bind(name)
            .bind(description)
            .bind(scopes)
            .bind(permissions)
            .bind(&now)
            .bind(ver)
            .execute()
            .await?;
        Ok(id)
    }

    pub async fn update_group(
        &self,
        id: &str,
        description: &str,
        scopes: &str,
        permissions: &str,
    ) -> anyhow::Result<bool> {
        self.invalidate_config_caches();
        let existing = self.get_group(id).await?;
        let (_, name, _, _, _, system, created_at, _) = match existing {
            Some(g) => g,
            None => return Ok(false),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_groups (id, name, description, scopes, permissions, system, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id)
            .bind(&name)
            .bind(description)
            .bind(scopes)
            .bind(permissions)
            .bind(if system { 1u8 } else { 0u8 })
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn delete_group(&self, id: &str) -> anyhow::Result<Result<bool, String>> {
        self.invalidate_config_caches();
        let existing = self.get_group(id).await?;
        let (_, name, description, scopes, permissions, system, created_at, _) = match existing {
            Some(g) => g,
            None => return Ok(Ok(false)),
        };
        if system {
            return Ok(Err("cannot delete a system group".to_string()));
        }
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_groups (id, name, description, scopes, permissions, system, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 0, ?, ?, 1)")
            .bind(id)
            .bind(&name)
            .bind(&description)
            .bind(&scopes)
            .bind(&permissions)
            .bind(&created_at)
            .bind(ver)
            .execute()
            .await?;
        Ok(Ok(true))
    }

    pub async fn set_group_tenants(
        &self,
        group_id: &str,
        tenant_ids: &[String],
    ) -> anyhow::Result<()> {
        self.invalidate_config_caches();
        // Soft-delete existing bindings
        let existing_tids = self.get_group_tenant_ids(group_id).await?;
        for tid in &existing_tids {
            let ver = Self::next_version();
            self.client
                .query("INSERT INTO config_group_tenants (group_id, tenant_id, version, is_deleted) VALUES (?, ?, ?, 1)")
                .bind(group_id)
                .bind(tid)
                .bind(ver)
                .execute()
                .await?;
        }
        // Insert new bindings
        for tid in tenant_ids {
            let ver = Self::next_version();
            self.client
                .query("INSERT INTO config_group_tenants (group_id, tenant_id, version, is_deleted) VALUES (?, ?, ?, 0)")
                .bind(group_id)
                .bind(tid)
                .bind(ver)
                .execute()
                .await?;
        }
        Ok(())
    }

    pub async fn get_user_groups(&self, user_id: &str) -> anyhow::Result<Vec<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            group_id: String,
        }
        let rows = self.client
            .query("SELECT group_id FROM config_user_groups FINAL WHERE user_id = ? AND is_deleted = 0")
            .bind(user_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().map(|r| r.group_id).collect())
    }

    pub async fn set_user_groups(&self, user_id: &str, group_ids: &[String]) -> anyhow::Result<()> {
        self.invalidate_config_caches();
        let existing = self.get_user_groups(user_id).await?;
        for gid in &existing {
            let ver = Self::next_version();
            self.client
                .query("INSERT INTO config_user_groups (user_id, group_id, version, is_deleted) VALUES (?, ?, ?, 1)")
                .bind(user_id)
                .bind(gid)
                .bind(ver)
                .execute()
                .await?;
        }
        for gid in group_ids {
            let ver = Self::next_version();
            self.client
                .query("INSERT INTO config_user_groups (user_id, group_id, version, is_deleted) VALUES (?, ?, ?, 0)")
                .bind(user_id)
                .bind(gid)
                .bind(ver)
                .execute()
                .await?;
        }
        Ok(())
    }

    pub async fn resolve_user_permissions(
        &self,
        user_id: &str,
    ) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>)> {
        if let Some(entry) = self.perms_cache.get(user_id) {
            let (perms, at) = entry.value();
            if Self::cache_fresh(*at) {
                return Ok(perms.clone());
            }
        }

        let mut all_scopes = std::collections::HashSet::new();
        let mut all_permissions = std::collections::HashSet::new();
        let mut all_tenant_ids = std::collections::HashSet::new();

        // Two fixed queries regardless of group count (previously 1 + 2·N).
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct GRow {
            scopes: String,
            permissions: String,
        }
        let groups = self.client
            .query("SELECT g.scopes, g.permissions FROM config_user_groups ug FINAL JOIN config_groups g FINAL ON ug.group_id = g.id WHERE ug.user_id = ? AND ug.is_deleted = 0 AND g.is_deleted = 0")
            .bind(user_id)
            .fetch_all::<GRow>()
            .await?;
        for g in &groups {
            if let Ok(s) = serde_json::from_str::<Vec<String>>(&g.scopes) {
                all_scopes.extend(s);
            }
            if let Ok(p) = serde_json::from_str::<Vec<String>>(&g.permissions) {
                all_permissions.extend(p);
            }
        }

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct TRow {
            tenant_id: String,
        }
        let tids = self.client
            .query("SELECT gt.tenant_id FROM config_user_groups ug FINAL JOIN config_group_tenants gt FINAL ON ug.group_id = gt.group_id WHERE ug.user_id = ? AND ug.is_deleted = 0 AND gt.is_deleted = 0")
            .bind(user_id)
            .fetch_all::<TRow>()
            .await?;
        all_tenant_ids.extend(tids.into_iter().map(|r| r.tenant_id));

        if all_scopes.contains("all") {
            all_scopes = std::collections::HashSet::from(["all".to_string()]);
        }
        if all_permissions.contains("admin") {
            all_permissions.insert("read".to_string());
            all_permissions.insert("write".to_string());
        }

        let result = (
            all_scopes.into_iter().collect::<Vec<_>>(),
            all_permissions.into_iter().collect::<Vec<_>>(),
            all_tenant_ids.into_iter().collect::<Vec<_>>(),
        );
        self.maintain_config_caches();
        if self.perms_cache.len() < MAX_CONFIG_CACHE_ENTRIES
            || self.perms_cache.contains_key(user_id)
        {
            self.perms_cache
                .insert(user_id.to_string(), (result.clone(), Instant::now()));
        }
        Ok(result)
    }

    // ── SSO provider operations ────────────────────────────────────────────────

    async fn fetch_sso_provider_row(
        &self,
        sql: &str,
        bind_id: Option<&str>,
    ) -> anyhow::Result<Option<SsoProviderRow>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            protocol: String,
            enabled: u8,
            client_id: String,
            client_secret: String,
            issuer_url: String,
            oidc_scopes: String,
            groups_claim: String,
            email_claim: String,
            first_name_claim: String,
            last_name_claim: String,
            jit_provisioning: u8,
            default_group_id: String,
            created_at: String,
            saml_idp_metadata_url: String,
            saml_idp_sso_url: String,
            saml_idp_cert: String,
            saml_sp_entity_id: String,
        }
        let result = match bind_id {
            Some(id) => self.client.query(sql).bind(id).fetch_one::<Row>().await,
            None => self.client.query(sql).fetch_one::<Row>().await,
        };
        match result {
            Ok(r) => {
                let client_secret = decrypt_sso_secret(&r.client_secret)?;
                Ok(Some((
                    r.id,
                    r.name,
                    r.protocol,
                    r.enabled != 0,
                    r.client_id,
                    client_secret,
                    r.issuer_url,
                    r.oidc_scopes,
                    r.groups_claim,
                    r.email_claim,
                    r.first_name_claim,
                    r.last_name_claim,
                    r.jit_provisioning != 0,
                    r.default_group_id,
                    r.created_at,
                    r.saml_idp_metadata_url,
                    r.saml_idp_sso_url,
                    r.saml_idp_cert,
                    r.saml_sp_entity_id,
                )))
            }
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn stored_active_sso_provider_id(&self) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            provider_id: String,
        }

        match self
            .client
            .query(
                "SELECT provider_id FROM config_sso_active_provider FINAL WHERE slot = 'primary' LIMIT 1",
            )
            .fetch_one::<Row>()
            .await
        {
            Ok(row) => Ok(Some(row.provider_id)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn legacy_enabled_sso_provider_ids(&self) -> anyhow::Result<Vec<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
        }

        Ok(self
            .client
            .query(
                "SELECT id FROM config_sso_providers FINAL WHERE enabled = 1 AND is_deleted = 0 ORDER BY created_at ASC, id ASC",
            )
            .fetch_all::<Row>()
            .await?
            .into_iter()
            .map(|row| row.id)
            .collect())
    }

    async fn set_active_sso_provider_id(&self, provider_id: &str) -> anyhow::Result<()> {
        self.client
            .query(
                "INSERT INTO config_sso_active_provider (slot, provider_id, version) VALUES ('primary', ?, ?)",
            )
            .bind(provider_id)
            .bind(Self::next_version())
            .execute()
            .await?;
        Ok(())
    }

    async fn effective_active_sso_provider_id(&self) -> anyhow::Result<Option<String>> {
        if let Some(provider_id) = self.stored_active_sso_provider_id().await? {
            return Ok((!provider_id.is_empty()).then_some(provider_id));
        }

        let resolution =
            resolve_legacy_active_sso_provider(self.legacy_enabled_sso_provider_ids().await?);
        if !resolution.ambiguous_provider_ids.is_empty() {
            anyhow::bail!(
                "multiple legacy SSO providers are enabled; startup reconciliation is required"
            );
        }
        Ok(resolution.active_provider_id)
    }

    /// Return the active provider identifier without loading or decrypting any
    /// provider configuration secrets.
    pub async fn active_sso_provider_id(&self) -> anyhow::Result<Option<String>> {
        self.effective_active_sso_provider_id().await
    }

    /// Lightweight SSO policy check that does not decrypt provider secrets.
    /// Break-glass local authentication must remain available even when an SSO
    /// encryption key is temporarily unavailable or being recovered.
    pub async fn is_sso_enabled(&self) -> anyhow::Result<bool> {
        Ok(self.effective_active_sso_provider_id().await?.is_some())
    }

    /// Establish the singleton active-provider pointer for installations that
    /// predate it. A single legacy enabled provider is adopted. Multiple legacy
    /// enabled rows fail closed by activating none; the caller records the
    /// mandatory system audit event so an administrator can choose explicitly.
    pub async fn reconcile_active_sso_provider(
        &self,
    ) -> anyhow::Result<SsoActiveProviderReconciliation> {
        if let Some(provider_id) = self.stored_active_sso_provider_id().await? {
            if provider_id.is_empty() {
                return Ok(SsoActiveProviderReconciliation {
                    active_provider_id: None,
                    ambiguous_provider_ids: Vec::new(),
                    changed: false,
                });
            }
            let provider = self
                .fetch_sso_provider_row(
                    "SELECT id, name, protocol, enabled, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, created_at, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id FROM config_sso_providers FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1",
                    Some(&provider_id),
                )
                .await?;
            if provider.is_some() {
                return Ok(SsoActiveProviderReconciliation {
                    active_provider_id: Some(provider_id),
                    ambiguous_provider_ids: Vec::new(),
                    changed: false,
                });
            }

            self.set_active_sso_provider_id("").await?;
            return Ok(SsoActiveProviderReconciliation {
                active_provider_id: None,
                ambiguous_provider_ids: Vec::new(),
                changed: true,
            });
        }

        let resolution =
            resolve_legacy_active_sso_provider(self.legacy_enabled_sso_provider_ids().await?);
        if resolution.changed {
            self.set_active_sso_provider_id(resolution.active_provider_id.as_deref().unwrap_or(""))
                .await?;
        }
        Ok(resolution)
    }

    pub async fn get_sso_provider(&self, id: &str) -> anyhow::Result<Option<SsoProviderRow>> {
        let mut provider = self.fetch_sso_provider_row(
            "SELECT id, name, protocol, enabled, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, created_at, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id FROM config_sso_providers FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1",
            Some(id),
        ).await?;
        let active_provider_id = self.effective_active_sso_provider_id().await?;
        if let Some(provider) = &mut provider {
            provider.3 = active_provider_id.as_deref() == Some(provider.0.as_str());
        }
        Ok(provider)
    }

    pub async fn list_sso_providers(&self) -> anyhow::Result<Vec<SsoProviderRow>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            protocol: String,
            client_id: String,
            client_secret: String,
            issuer_url: String,
            oidc_scopes: String,
            groups_claim: String,
            email_claim: String,
            first_name_claim: String,
            last_name_claim: String,
            jit_provisioning: u8,
            default_group_id: String,
            created_at: String,
            saml_idp_metadata_url: String,
            saml_idp_sso_url: String,
            saml_idp_cert: String,
            saml_sp_entity_id: String,
        }
        let rows = self.client
            .query("SELECT id, name, protocol, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, created_at, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id FROM config_sso_providers FINAL WHERE is_deleted = 0 ORDER BY created_at ASC")
            .fetch_all::<Row>()
            .await?;
        let active_provider_id = self.effective_active_sso_provider_id().await?;
        rows.into_iter()
            .map(|r| {
                let client_secret = decrypt_sso_secret(&r.client_secret)?;
                let enabled = active_provider_id.as_deref() == Some(r.id.as_str());
                Ok((
                    r.id,
                    r.name,
                    r.protocol,
                    enabled,
                    r.client_id,
                    client_secret,
                    r.issuer_url,
                    r.oidc_scopes,
                    r.groups_claim,
                    r.email_claim,
                    r.first_name_claim,
                    r.last_name_claim,
                    r.jit_provisioning != 0,
                    r.default_group_id,
                    r.created_at,
                    r.saml_idp_metadata_url,
                    r.saml_idp_sso_url,
                    r.saml_idp_cert,
                    r.saml_sp_entity_id,
                ))
            })
            .collect()
    }

    pub async fn get_enabled_sso_provider(&self) -> anyhow::Result<Option<SsoProviderRow>> {
        let Some(provider_id) = self.effective_active_sso_provider_id().await? else {
            return Ok(None);
        };
        let mut provider = self
            .fetch_sso_provider_row(
                "SELECT id, name, protocol, enabled, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, created_at, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id FROM config_sso_providers FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1",
                Some(&provider_id),
            )
            .await?;
        let Some(provider) = &mut provider else {
            anyhow::bail!("active SSO provider does not exist");
        };
        provider.3 = true;
        Ok(Some(provider.clone()))
    }

    /// Validate the configured encryption key against stored envelopes and
    /// return providers that still need the plaintext-to-envelope migration.
    pub async fn legacy_sso_client_secret_ids(&self) -> anyhow::Result<Vec<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct SecretRow {
            id: String,
            client_secret: String,
        }
        let stored = self
            .client
            .query("SELECT id, client_secret FROM config_sso_providers FINAL WHERE is_deleted = 0 AND client_secret != ''")
            .fetch_all::<SecretRow>()
            .await?;
        if stored.is_empty() {
            return Ok(Vec::new());
        }
        // Validate the configured key and every existing encrypted envelope
        // before rewriting any row. A missing, rotated, or malformed key must
        // fail startup instead of silently making SSO unusable later.
        let key = config_encryption_key()?;
        let mut legacy = Vec::new();
        for row in stored {
            if row.client_secret.starts_with(ENCRYPTED_SECRET_PREFIX) {
                decrypt_sso_secret_with_key(&row.client_secret, &key)?;
            } else {
                legacy.push(row.id);
            }
        }
        Ok(legacy)
    }

    /// Encrypt exactly one legacy provider. Startup calls this one row at a
    /// time and immediately appends the mandatory audit event afterward.
    pub async fn encrypt_legacy_sso_client_secret(
        &self,
        provider_id: &str,
    ) -> anyhow::Result<bool> {
        let Some(provider) = self.get_sso_provider(provider_id).await? else {
            return Ok(false);
        };
        self.upsert_sso_provider(
            &provider.0,
            &provider.1,
            &provider.2,
            provider.3,
            &provider.4,
            &provider.5,
            &provider.6,
            &provider.7,
            &provider.8,
            &provider.9,
            &provider.10,
            &provider.11,
            provider.12,
            &provider.13,
            &provider.15,
            &provider.16,
            &provider.17,
            &provider.18,
        )
        .await?;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_sso_provider(
        &self,
        id: &str,
        name: &str,
        protocol: &str,
        enabled: bool,
        client_id: &str,
        client_secret: &str,
        issuer_url: &str,
        oidc_scopes: &str,
        groups_claim: &str,
        email_claim: &str,
        first_name_claim: &str,
        last_name_claim: &str,
        jit_provisioning: bool,
        default_group_id: &str,
        saml_idp_metadata_url: &str,
        saml_idp_sso_url: &str,
        saml_idp_cert: &str,
        saml_sp_entity_id: &str,
    ) -> anyhow::Result<Option<String>> {
        let previous_active_provider_id = self.effective_active_sso_provider_id().await?;
        let now = Self::now_str();
        let ver = Self::next_version();
        let encrypted_client_secret = encrypt_sso_secret(client_secret)?;
        self.client
            .query("INSERT INTO config_sso_providers (id, name, protocol, enabled, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(protocol)
            .bind(if enabled { 1u8 } else { 0u8 })
            .bind(client_id).bind(&encrypted_client_secret).bind(issuer_url)
            .bind(oidc_scopes).bind(groups_claim)
            .bind(email_claim).bind(first_name_claim).bind(last_name_claim)
            .bind(if jit_provisioning { 1u8 } else { 0u8 })
            .bind(default_group_id)
            .bind(saml_idp_metadata_url).bind(saml_idp_sso_url)
            .bind(saml_idp_cert).bind(saml_sp_entity_id)
            .bind(&now).bind(ver)
            .execute()
            .await?;
        if enabled {
            self.set_active_sso_provider_id(id).await?;
        } else if previous_active_provider_id.as_deref() == Some(id) {
            self.set_active_sso_provider_id("").await?;
        }
        Ok(previous_active_provider_id)
    }

    pub async fn delete_sso_provider(&self, id: &str) -> anyhow::Result<bool> {
        let existing = self.get_sso_provider(id).await?;
        if existing.is_none() {
            return Ok(false);
        }
        let was_active = existing.as_ref().is_some_and(|provider| provider.3);
        let ver = Self::next_version();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_sso_providers (id, name, protocol, enabled, client_id, client_secret, issuer_url, oidc_scopes, groups_claim, email_claim, first_name_claim, last_name_claim, jit_provisioning, default_group_id, saml_idp_metadata_url, saml_idp_sso_url, saml_idp_cert, saml_sp_entity_id, created_at, version, is_deleted) VALUES (?, '', '', 0, '', '', '', '', '', '', '', '', 0, '', '', '', '', '', ?, ?, 1)")
            .bind(id).bind(&now).bind(ver)
            .execute()
            .await?;
        if was_active {
            self.set_active_sso_provider_id("").await?;
        }
        Ok(true)
    }

    // ── IdP group mapping operations ───────────────────────────────────────────

    pub async fn list_idp_group_mappings(
        &self,
        provider_id: Option<&str>,
    ) -> anyhow::Result<Vec<(String, String, String, String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            idp_group: String,
            rush_group_id: String,
            provider_id: String,
            created_at: String,
        }
        let rows = match provider_id {
            Some(pid) => self.client
                .query("SELECT id, idp_group, rush_group_id, provider_id, created_at FROM config_idp_group_mappings FINAL WHERE provider_id = ? AND is_deleted = 0 ORDER BY created_at ASC")
                .bind(pid)
                .fetch_all::<Row>()
                .await?,
            None => self.client
                .query("SELECT id, idp_group, rush_group_id, provider_id, created_at FROM config_idp_group_mappings FINAL WHERE is_deleted = 0 ORDER BY created_at ASC")
                .fetch_all::<Row>()
                .await?,
        };
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.id,
                    r.idp_group,
                    r.rush_group_id,
                    r.provider_id,
                    r.created_at,
                )
            })
            .collect())
    }

    pub async fn create_idp_group_mapping(
        &self,
        idp_group: &str,
        rush_group_id: &str,
        provider_id: &str,
    ) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_idp_group_mappings (id, idp_group, rush_group_id, provider_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 0)")
            .bind(&id).bind(idp_group).bind(rush_group_id).bind(provider_id).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(id)
    }

    /// Update an existing mapping's idp_group / rush_group_id, preserving its id,
    /// provider_id, and created_at. Returns the prior (idp_group, rush_group_id) on
    /// success so callers can audit the before/after, or None if the id is unknown.
    pub async fn update_idp_group_mapping(
        &self,
        id: &str,
        idp_group: &str,
        rush_group_id: &str,
    ) -> anyhow::Result<Option<(String, String)>> {
        let mappings = self.list_idp_group_mappings(None).await?;
        let found = mappings.iter().find(|(mid, _, _, _, _)| mid == id);
        if found.is_none() {
            return Ok(None);
        }
        let (_, old_idp_group, old_rush_group_id, provider_id, created_at) = found.unwrap().clone();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_idp_group_mappings (id, idp_group, rush_group_id, provider_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(idp_group).bind(rush_group_id).bind(&provider_id).bind(&created_at).bind(ver)
            .execute()
            .await?;
        Ok(Some((old_idp_group, old_rush_group_id)))
    }

    pub async fn delete_idp_group_mapping(&self, id: &str) -> anyhow::Result<bool> {
        let mappings = self.list_idp_group_mappings(None).await?;
        let found = mappings.iter().find(|(mid, _, _, _, _)| mid == id);
        if found.is_none() {
            return Ok(false);
        }
        let ver = Self::next_version();
        let (_, idp_group, rush_group_id, provider_id, created_at) = found.unwrap().clone();
        self.client
            .query("INSERT INTO config_idp_group_mappings (id, idp_group, rush_group_id, provider_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&idp_group).bind(&rush_group_id).bind(&provider_id).bind(&created_at).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn resolve_idp_groups(
        &self,
        idp_groups: &[String],
        provider_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        let mut result = std::collections::HashSet::new();
        for idp_group in idp_groups {
            #[derive(clickhouse::Row, serde::Deserialize)]
            struct Row {
                rush_group_id: String,
            }
            let rows = self.client
                .query("SELECT rush_group_id FROM config_idp_group_mappings FINAL WHERE idp_group = ? AND provider_id = ? AND is_deleted = 0")
                .bind(idp_group)
                .bind(provider_id)
                .fetch_all::<Row>()
                .await?;
            for r in rows {
                result.insert(r.rush_group_id);
            }
        }
        Ok(result.into_iter().collect())
    }

    // ── SSO user operations ────────────────────────────────────────────────────

    pub async fn find_user_by_external_id(
        &self,
        external_id: &str,
        auth_provider: &str,
    ) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
        }
        let result = self.client
            .query("SELECT id FROM config_users FINAL WHERE external_id = ? AND auth_provider = ? AND is_deleted = 0 LIMIT 1")
            .bind(external_id)
            .bind(auth_provider)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(r.id)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_sso_user(
        &self,
        username: &str,
        display_name: &str,
        external_id: &str,
        auth_provider: &str,
        tenant_id: &str,
    ) -> anyhow::Result<String> {
        let _username_guard = self.username_mutation_lock.lock().await;
        self.ensure_username_available(username).await?;
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, '!sso-no-password', ?, ?, 'viewer', 1, ?, ?, ?, ?, 0)")
            .bind(&id).bind(username).bind(display_name).bind(tenant_id)
            .bind(auth_provider).bind(external_id)
            .bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(id)
    }

    pub async fn update_user_external_identity(
        &self,
        user_id: &str,
        auth_provider: &str,
        external_id: &str,
    ) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            username: String,
            password_hash: String,
            display_name: String,
            tenant_id: String,
            role: String,
            enabled: u8,
            created_at: String,
        }
        let row = match self
            .client
            .query("SELECT username, password_hash, display_name, tenant_id, role, enabled, created_at FROM config_users FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(user_id)
            .fetch_one::<Row>()
            .await
        {
            Ok(row) => row,
            Err(clickhouse::error::Error::RowNotFound) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        self.client
            .query("INSERT INTO config_users (id, username, password_hash, display_name, tenant_id, role, enabled, auth_provider, external_id, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(user_id)
            .bind(&row.username)
            .bind(&row.password_hash)
            .bind(&row.display_name)
            .bind(&row.tenant_id)
            .bind(&row.role)
            .bind(row.enabled)
            .bind(auth_provider)
            .bind(external_id)
            .bind(&row.created_at)
            .bind(Self::next_version())
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn update_user_groups_from_idp(
        &self,
        user_id: &str,
        mapped_group_ids: &[String],
    ) -> anyhow::Result<()> {
        self.set_user_groups(user_id, mapped_group_ids).await
    }

    // ── SSO CSRF state operations ──────────────────────────────────────────────

    pub async fn store_sso_state(&self, state: &str) -> anyhow::Result<()> {
        self.client
            .query("INSERT INTO config_sso_state (state, created_at) VALUES (?, now())")
            .bind(state)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn validate_sso_state(&self, state: &str) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        #[allow(dead_code)]
        struct Row {
            state: String,
        }
        // ClickHouse TTL handles expiry; just check existence and delete
        let result = self.client
            .query("SELECT state FROM config_sso_state WHERE state = ? AND created_at > now() - INTERVAL 10 MINUTE LIMIT 1")
            .bind(state)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(_) => {
                // Delete the consumed state via lightweight delete
                let _ = self
                    .client
                    .query("ALTER TABLE config_sso_state DELETE WHERE state = ?")
                    .bind(state)
                    .execute()
                    .await;
                Ok(true)
            }
            Err(clickhouse::error::Error::RowNotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    // ── API key operations ─────────────────────────────────────────────────────

    pub async fn list_api_keys(
        &self,
    ) -> anyhow::Result<
        Vec<(
            String,
            String,
            String,
            String,
            String,
            Vec<String>,
            u64,
            Vec<String>,
            String,
        )>,
    > {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            prefix: String,
            tenant_id: String,
            key_type: String,
            signals: String,
            rate_limit_per_minute: u64,
            source_cidrs: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, name, prefix, tenant_id, key_type, signals, rate_limit_per_minute, source_cidrs, created_at FROM config_api_keys FINAL WHERE is_deleted = 0 ORDER BY created_at DESC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.id,
                    row.name,
                    row.prefix,
                    row.tenant_id,
                    row.key_type,
                    serde_json::from_str(&row.signals).unwrap_or_default(),
                    row.rate_limit_per_minute,
                    serde_json::from_str(&row.source_cidrs).unwrap_or_default(),
                    row.created_at,
                )
            })
            .collect())
    }

    pub async fn create_api_key(
        &self,
        id: &str,
        name: &str,
        key_hash: &str,
        prefix: &str,
        tenant_id: &str,
        key_type: &str,
        signals: &[String],
        rate_limit_per_minute: u64,
        source_cidrs: &[String],
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        let signals = serde_json::to_string(signals)?;
        let source_cidrs = serde_json::to_string(source_cidrs)?;
        self.client
            .query("INSERT INTO config_api_keys (id, name, key_hash, prefix, tenant_id, key_type, signals, rate_limit_per_minute, source_cidrs, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(key_hash).bind(prefix).bind(tenant_id)
            .bind(key_type).bind(&signals).bind(rate_limit_per_minute)
            .bind(&source_cidrs).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    /// Register the Helm-managed default-tenant ingest key exactly once.
    ///
    /// The fixed row id makes concurrent startup by multiple API replicas
    /// converge on one ReplacingMergeTree row. Looking up the hash first also
    /// leaves an already registered, externally managed key untouched.
    pub async fn ensure_bootstrap_ingest_api_key(
        &self,
        key_hash: &str,
        prefix: &str,
    ) -> anyhow::Result<Option<String>> {
        if self.resolve_api_key(key_hash).await?.is_some() {
            return Ok(None);
        }

        const ID: &str = "bootstrap-ingest-default";
        let signals = crate::api_key_auth::INGEST_SIGNALS
            .iter()
            .map(|signal| (*signal).to_string())
            .collect::<Vec<_>>();
        self.create_api_key(
            ID,
            "Helm-managed ingest",
            key_hash,
            prefix,
            "default",
            "ingest",
            &signals,
            1_000_000,
            &[],
        )
        .await?;
        Ok(Some(ID.to_string()))
    }

    pub async fn delete_api_key(&self, id: &str) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            name: String,
            key_hash: String,
            prefix: String,
            tenant_id: String,
            key_type: String,
            signals: String,
            rate_limit_per_minute: u64,
            source_cidrs: String,
            created_at: String,
        }
        let result = self.client
            .query("SELECT name, key_hash, prefix, tenant_id, key_type, signals, rate_limit_per_minute, source_cidrs, created_at FROM config_api_keys FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        let row = match result {
            Ok(r) => r,
            Err(clickhouse::error::Error::RowNotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_api_keys (id, name, key_hash, prefix, tenant_id, key_type, signals, rate_limit_per_minute, source_cidrs, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&row.name).bind(&row.key_hash).bind(&row.prefix)
            .bind(&row.tenant_id).bind(&row.key_type).bind(&row.signals)
            .bind(row.rate_limit_per_minute).bind(&row.source_cidrs)
            .bind(&row.created_at).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    // ── Settings operations ────────────────────────────────────────────────────

    pub async fn get_setting(&self, key: &str) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            value: String,
        }
        let result = self
            .client
            .query(
                "SELECT value FROM config_settings FINAL WHERE key = ? AND is_deleted = 0 LIMIT 1",
            )
            .bind(key)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(r.value)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn set_setting(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let ver = Self::next_version();
        self.client
            .query(
                "INSERT INTO config_settings (key, value, version, is_deleted) VALUES (?, ?, ?, 0)",
            )
            .bind(key)
            .bind(value)
            .bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    // ── API-managed integration targets ───────────────────────────────────────

    /// Return configured integration targets, decrypting DSNs only inside the
    /// API process. Callers must never serialize this result directly to users.
    pub async fn list_integration_target_secrets(
        &self,
        tenant_id: &str,
        integration: &str,
    ) -> anyhow::Result<Vec<crate::integrations::IntegrationTargetSecret>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            dsn_encrypted: String,
            environment: String,
            enabled: u8,
        }
        let rows = self
            .client
            .query(
                "SELECT id, name, dsn_encrypted, environment, enabled
                 FROM config_integration_targets FINAL
                 WHERE tenant_id = ? AND integration = ? AND is_deleted = 0
                 ORDER BY id",
            )
            .bind(tenant_id)
            .bind(integration)
            .fetch_all::<Row>()
            .await?;

        rows.into_iter()
            .map(|row| {
                Ok(crate::integrations::IntegrationTargetSecret {
                    id: row.id,
                    name: row.name,
                    dsn: crate::integrations::decrypt_secret(&row.dsn_encrypted)?,
                    environment: row.environment,
                    enabled: row.enabled != 0,
                })
            })
            .collect()
    }

    pub async fn upsert_integration_target(
        &self,
        tenant_id: &str,
        integration: &str,
        target: &crate::integrations::IntegrationTargetSecret,
        encrypted_dsn: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query(
                "INSERT INTO config_integration_targets
                 (id, tenant_id, integration, name, dsn_encrypted, environment, enabled,
                  created_at, updated_at, version, is_deleted)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
            )
            .bind(&target.id)
            .bind(tenant_id)
            .bind(integration)
            .bind(&target.name)
            .bind(encrypted_dsn)
            .bind(&target.environment)
            .bind(target.enabled)
            .bind(&now)
            .bind(&now)
            .bind(Self::next_version())
            .execute()
            .await?;
        Ok(())
    }

    pub async fn delete_integration_target(
        &self,
        tenant_id: &str,
        integration: &str,
        target_id: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query(
                "INSERT INTO config_integration_targets
                 (id, tenant_id, integration, name, dsn_encrypted, environment, enabled,
                  created_at, updated_at, version, is_deleted)
                 VALUES (?, ?, ?, '', '', '', 0, ?, ?, ?, 1)",
            )
            .bind(target_id)
            .bind(tenant_id)
            .bind(integration)
            .bind(&now)
            .bind(&now)
            .bind(Self::next_version())
            .execute()
            .await?;
        Ok(())
    }

    // ── Postgres EXPLAIN job queue ─────────────────────────────────────────────
    /// Create a pending EXPLAIN job; returns its id.
    pub async fn create_explain_job(
        &self,
        tenant_id: &str,
        server: &str,
        db: &str,
        query: &str,
    ) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_pg_explain_jobs (id, tenant_id, server_name, db, query, status, plan_json, error, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 'pending', '', '', ?, ?, ?, 0)")
            .bind(&id).bind(tenant_id).bind(server).bind(db).bind(query).bind(&now).bind(&now).bind(Self::next_version())
            .execute().await?;
        Ok(id)
    }

    /// Claim the oldest pending job for a tenant+server, flipping it to `running`.
    pub async fn claim_pending_explain_job(
        &self,
        tenant_id: &str,
        server: &str,
    ) -> anyhow::Result<Option<(String, String, String)>> {
        let row = self.client
            .query("SELECT id, db, query FROM config_pg_explain_jobs FINAL WHERE tenant_id = ? AND server_name = ? AND status = 'pending' AND is_deleted = 0 ORDER BY created_at ASC LIMIT 1")
            .bind(tenant_id).bind(server)
            .fetch_all::<ExplainClaimRow>().await?
            .into_iter().next();
        if let Some(r) = &row {
            self.client
                .query("INSERT INTO config_pg_explain_jobs (id, tenant_id, server_name, db, query, status, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 'running', ?, ?, 0)")
                .bind(&r.id).bind(tenant_id).bind(server).bind(&r.db).bind(&r.query).bind(Self::now_str()).bind(Self::next_version())
                .execute().await?;
        }
        Ok(row.map(|r| (r.id, r.db, r.query)))
    }

    /// Requeue jobs whose collector lease expired. Collector-side EXPLAIN is
    /// bounded, so two minutes is long enough for a slow plan while preventing
    /// a dead collector from leaving the UI in a permanent running state.
    pub async fn requeue_stale_explain_jobs(
        &self,
        tenant_id: &str,
        server: &str,
    ) -> anyhow::Result<u64> {
        let rows = self
            .client
            .query("SELECT id, db, query FROM config_pg_explain_jobs FINAL WHERE tenant_id = ? AND server_name = ? AND status = 'running' AND is_deleted = 0 AND updated_at < toString(now() - INTERVAL 2 MINUTE) LIMIT 20")
            .bind(tenant_id)
            .bind(server)
            .fetch_all::<ExplainClaimRow>()
            .await?;
        let mut count = 0;
        for row in rows {
            self.client
                .query("INSERT INTO config_pg_explain_jobs (id, tenant_id, server_name, db, query, status, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, 0)")
                .bind(&row.id)
                .bind(tenant_id)
                .bind(server)
                .bind(&row.db)
                .bind(&row.query)
                .bind(Self::now_str())
                .bind(Self::next_version())
                .execute()
                .await?;
            count += 1;
        }
        Ok(count)
    }

    /// Complete a job with a plan or an error (sets status done/error).
    pub async fn complete_explain_job(
        &self,
        tenant_id: &str,
        id: &str,
        plan_json: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        let status = if error.is_empty() { "done" } else { "error" };
        self.client
            .query("INSERT INTO config_pg_explain_jobs (id, tenant_id, status, plan_json, error, updated_at, version, is_deleted) SELECT id, tenant_id, ?, ?, ?, ?, ?, 0 FROM config_pg_explain_jobs FINAL WHERE id = ? AND tenant_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(status).bind(plan_json).bind(error).bind(Self::now_str()).bind(Self::next_version()).bind(id).bind(tenant_id)
            .execute().await?;
        Ok(())
    }

    /// Fetch a job's status/result for the UI.
    pub async fn get_explain_job(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> anyhow::Result<Option<(String, String, String, String)>> {
        Ok(self.client
            .query("SELECT status, db, plan_json, error FROM config_pg_explain_jobs FINAL WHERE id = ? AND tenant_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id).bind(tenant_id)
            .fetch_all::<ExplainStatusRow>().await?
            .into_iter().next()
            .map(|r| (r.status, r.db, r.plan_json, r.error)))
    }

    // ── MySQL EXPLAIN job queue ───────────────────────────────────────────────
    pub async fn create_mysql_explain_job(
        &self,
        tenant_id: &str,
        server: &str,
        db: &str,
        query: &str,
    ) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_mysql_explain_jobs (id, tenant_id, server_name, db, query, status, plan_json, error, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 'pending', '', '', ?, ?, ?, 0)")
            .bind(&id).bind(tenant_id).bind(server).bind(db).bind(query).bind(&now).bind(&now).bind(Self::next_version())
            .execute().await?;
        Ok(id)
    }

    pub async fn claim_pending_mysql_explain_job(
        &self,
        tenant_id: &str,
        server: &str,
    ) -> anyhow::Result<Option<(String, String, String)>> {
        let row = self.client
            .query("SELECT id, db, query FROM config_mysql_explain_jobs FINAL WHERE tenant_id = ? AND server_name = ? AND status = 'pending' AND is_deleted = 0 ORDER BY created_at ASC LIMIT 1")
            .bind(tenant_id).bind(server)
            .fetch_all::<ExplainClaimRow>().await?
            .into_iter().next();
        if let Some(row) = &row {
            self.client
                .query("INSERT INTO config_mysql_explain_jobs (id, tenant_id, server_name, db, query, status, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 'running', ?, ?, 0)")
                .bind(&row.id).bind(tenant_id).bind(server).bind(&row.db).bind(&row.query).bind(Self::now_str()).bind(Self::next_version())
                .execute().await?;
        }
        Ok(row.map(|row| (row.id, row.db, row.query)))
    }

    pub async fn requeue_stale_mysql_explain_jobs(
        &self,
        tenant_id: &str,
        server: &str,
    ) -> anyhow::Result<u64> {
        let rows = self.client
            .query("SELECT id, db, query FROM config_mysql_explain_jobs FINAL WHERE tenant_id = ? AND server_name = ? AND status = 'running' AND is_deleted = 0 AND updated_at < toString(now() - INTERVAL 2 MINUTE) LIMIT 20")
            .bind(tenant_id).bind(server)
            .fetch_all::<ExplainClaimRow>().await?;
        let mut count = 0;
        for row in rows {
            self.client
                .query("INSERT INTO config_mysql_explain_jobs (id, tenant_id, server_name, db, query, status, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, 0)")
                .bind(&row.id).bind(tenant_id).bind(server).bind(&row.db).bind(&row.query).bind(Self::now_str()).bind(Self::next_version())
                .execute().await?;
            count += 1;
        }
        Ok(count)
    }

    pub async fn complete_mysql_explain_job(
        &self,
        tenant_id: &str,
        id: &str,
        plan_json: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        let status = if error.is_empty() { "done" } else { "error" };
        self.client
            .query("INSERT INTO config_mysql_explain_jobs (id, tenant_id, status, plan_json, error, updated_at, version, is_deleted) SELECT id, tenant_id, ?, ?, ?, ?, ?, 0 FROM config_mysql_explain_jobs FINAL WHERE id = ? AND tenant_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(status).bind(plan_json).bind(error).bind(Self::now_str()).bind(Self::next_version()).bind(id).bind(tenant_id)
            .execute().await?;
        Ok(())
    }

    pub async fn get_mysql_explain_job(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> anyhow::Result<Option<(String, String, String, String)>> {
        Ok(self.client
            .query("SELECT status, db, plan_json, error FROM config_mysql_explain_jobs FINAL WHERE id = ? AND tenant_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id).bind(tenant_id)
            .fetch_all::<ExplainStatusRow>().await?
            .into_iter().next()
            .map(|row| (row.status, row.db, row.plan_json, row.error)))
    }

    // ── Setup token operations ─────────────────────────────────────────────────

    pub async fn create_setup_token(
        &self,
        token_hash: &str,
        purpose: &str,
        created_by: &str,
        provider: &str,
        hostname: &str,
    ) -> anyhow::Result<()> {
        let expires_at = (chrono::Utc::now() + chrono::Duration::minutes(30))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_setup_tokens (token, purpose, created_by, expires_at, used, provider, hostname, version, is_deleted) VALUES (?, ?, ?, ?, 0, ?, ?, ?, 0)")
            .bind(token_hash).bind(purpose).bind(created_by).bind(&expires_at)
            .bind(provider).bind(hostname).bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn consume_setup_token(
        &self,
        token_hash: &str,
        purpose: &str,
    ) -> anyhow::Result<Option<(String, String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            provider: String,
            hostname: String,
            created_by: String,
            expires_at: String,
        }
        let now = Self::now_str();
        let row = match self.client
            .query("SELECT provider, hostname, created_by, expires_at FROM config_setup_tokens FINAL WHERE token = ? AND purpose = ? AND used = 0 AND expires_at > ? AND is_deleted = 0 LIMIT 1")
            .bind(token_hash).bind(purpose).bind(&now)
            .fetch_one::<Row>()
            .await {
            Ok(row) => row,
            Err(clickhouse::error::Error::RowNotFound) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_setup_tokens (token, purpose, created_by, expires_at, used, provider, hostname, version, is_deleted) VALUES (?, ?, ?, ?, 1, ?, ?, ?, 0)")
            .bind(token_hash).bind(purpose).bind(&row.created_by).bind(&row.expires_at)
            .bind(&row.provider).bind(&row.hostname).bind(ver)
            .execute()
            .await?;
        Ok(Some((row.provider, row.hostname, row.created_by)))
    }

    // ── Dashboard operations ───────────────────────────────────────────────────

    pub async fn list_dashboards(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Vec<crate::models::dashboard::Dashboard>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            description: String,
            tenant_id: String,
            owner_id: String,
            visibility: String,
            tags: String,
            variables: String,
            created_at: String,
            updated_at: String,
        }
        let rows = self.client
            .query("SELECT id, name, description, tenant_id, owner_id, visibility, tags, variables, created_at, updated_at FROM config_dashboards FINAL WHERE is_deleted = 0 AND ((visibility = 'private' AND owner_id = ?) OR (visibility = 'tenant' AND tenant_id = ?) OR (visibility = 'global')) ORDER BY updated_at DESC")
            .bind(user_id).bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::dashboard::Dashboard {
                id: r.id,
                name: r.name,
                description: r.description,
                tenant_id: r.tenant_id,
                owner_id: r.owner_id,
                visibility: r.visibility,
                tags: serde_json::from_str(&r.tags).unwrap_or(serde_json::json!([])),
                variables: serde_json::from_str(&r.variables).unwrap_or(serde_json::json!([])),
                created_at: r.created_at,
                updated_at: r.updated_at,
            })
            .collect())
    }

    pub async fn get_dashboard(
        &self,
        id: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Option<crate::models::dashboard::Dashboard>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            description: String,
            tenant_id: String,
            owner_id: String,
            visibility: String,
            tags: String,
            variables: String,
            created_at: String,
            updated_at: String,
        }
        let result = self.client
            .query("SELECT id, name, description, tenant_id, owner_id, visibility, tags, variables, created_at, updated_at FROM config_dashboards FINAL WHERE id = ? AND is_deleted = 0 AND ((visibility = 'private' AND owner_id = ?) OR (visibility = 'tenant' AND tenant_id = ?) OR (visibility = 'global')) LIMIT 1")
            .bind(id).bind(user_id).bind(tenant_id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::dashboard::Dashboard {
                id: r.id,
                name: r.name,
                description: r.description,
                tenant_id: r.tenant_id,
                owner_id: r.owner_id,
                visibility: r.visibility,
                tags: serde_json::from_str(&r.tags).unwrap_or(serde_json::json!([])),
                variables: serde_json::from_str(&r.variables).unwrap_or(serde_json::json!([])),
                created_at: r.created_at,
                updated_at: r.updated_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_dashboard_unchecked(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::dashboard::Dashboard>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            description: String,
            tenant_id: String,
            owner_id: String,
            visibility: String,
            tags: String,
            variables: String,
            created_at: String,
            updated_at: String,
        }
        let result = self.client
            .query("SELECT id, name, description, tenant_id, owner_id, visibility, tags, variables, created_at, updated_at FROM config_dashboards FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::dashboard::Dashboard {
                id: r.id,
                name: r.name,
                description: r.description,
                tenant_id: r.tenant_id,
                owner_id: r.owner_id,
                visibility: r.visibility,
                tags: serde_json::from_str(&r.tags).unwrap_or(serde_json::json!([])),
                variables: serde_json::from_str(&r.variables).unwrap_or(serde_json::json!([])),
                created_at: r.created_at,
                updated_at: r.updated_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_dashboard(
        &self,
        id: &str,
        name: &str,
        description: &str,
        tenant_id: &str,
        owner_id: &str,
        visibility: &str,
        tags: &str,
        variables: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_dashboards (id, name, description, tenant_id, owner_id, visibility, tags, variables, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(tenant_id)
            .bind(owner_id).bind(visibility).bind(tags).bind(variables)
            .bind(&now).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn update_dashboard(
        &self,
        id: &str,
        name: &str,
        description: &str,
        visibility: &str,
        tags: &str,
        variables: &str,
        tenant_id: &str,
        user_id: &str,
        user_role: &str,
    ) -> anyhow::Result<bool> {
        let dash = match self.get_dashboard(id, tenant_id, user_id).await? {
            Some(d) => d,
            None => return Ok(false),
        };
        let can_edit = dash.owner_id == user_id
            || (dash.visibility == "tenant"
                && dash.tenant_id == tenant_id
                && (user_role == "admin" || user_role == "editor"))
            || (dash.visibility == "global" && user_role == "admin")
            || dash.owner_id.is_empty();
        if !can_edit {
            return Ok(false);
        }
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_dashboards (id, name, description, tenant_id, owner_id, visibility, tags, variables, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(&dash.tenant_id)
            .bind(&dash.owner_id).bind(visibility).bind(tags).bind(variables)
            .bind(&dash.created_at).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn delete_dashboard(
        &self,
        id: &str,
        tenant_id: &str,
        user_id: &str,
        user_role: &str,
    ) -> anyhow::Result<bool> {
        let dash = match self.get_dashboard(id, tenant_id, user_id).await? {
            Some(d) => d,
            None => return Ok(false),
        };
        let can_delete =
            dash.owner_id == user_id || user_role == "admin" || dash.owner_id.is_empty();
        if !can_delete {
            return Ok(false);
        }
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_dashboards (id, name, description, tenant_id, owner_id, visibility, tags, variables, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&dash.name).bind(&dash.description).bind(&dash.tenant_id)
            .bind(&dash.owner_id).bind(&dash.visibility)
            .bind(serde_json::to_string(&dash.tags).unwrap_or_else(|_| "[]".to_string()))
            .bind(serde_json::to_string(&dash.variables).unwrap_or_else(|_| "[]".to_string()))
            .bind(&dash.created_at).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn export_dashboard(
        &self,
        id: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let dash = match self.get_dashboard(id, tenant_id, user_id).await? {
            Some(d) => d,
            None => return Ok(None),
        };
        let widgets = self.list_widgets(id).await?;
        let widget_exports: Vec<serde_json::Value> = widgets.into_iter().map(|w| serde_json::json!({
            "title": w.title,
            "widget_type": w.widget_type,
            "query_config": serde_json::from_str::<serde_json::Value>(&w.query_config).unwrap_or_default(),
            "position": serde_json::from_str::<serde_json::Value>(&w.position).unwrap_or_default(),
            "display_config": serde_json::from_str::<serde_json::Value>(&w.display_config).unwrap_or_default(),
        })).collect();
        Ok(Some(serde_json::json!({
            "format_version": "v1",
            "exported_at": Self::now_str(),
            "dashboard": {"name": dash.name, "description": dash.description, "visibility": dash.visibility, "tags": dash.tags, "variables": dash.variables},
            "widgets": widget_exports,
        })))
    }

    pub async fn import_dashboard(
        &self,
        import: &crate::models::dashboard::ImportDashboardRequest,
        tenant_id: &str,
        owner_id: &str,
        user_role: &str,
    ) -> anyhow::Result<crate::models::dashboard::Dashboard> {
        if import.format_version != "v1" {
            anyhow::bail!("unsupported format_version: {}", import.format_version);
        }
        let visibility = if import.dashboard.visibility == "global" && user_role != "admin" {
            "tenant"
        } else {
            &import.dashboard.visibility
        };
        let tags_str = serde_json::to_string(&import.dashboard.tags)?;
        let vars_str =
            serde_json::to_string(&import.dashboard.variables).unwrap_or_else(|_| "[]".to_string());
        let dash_id = uuid::Uuid::new_v4().to_string();
        self.create_dashboard(
            &dash_id,
            &import.dashboard.name,
            &import.dashboard.description,
            tenant_id,
            owner_id,
            visibility,
            &tags_str,
            &vars_str,
        )
        .await?;
        for w in &import.widgets {
            let wid = uuid::Uuid::new_v4().to_string();
            self.create_widget(
                &wid,
                &dash_id,
                &w.title,
                &w.widget_type,
                &serde_json::to_string(&w.query_config)?,
                &serde_json::to_string(&w.position)?,
                &serde_json::to_string(&w.display_config)?,
            )
            .await?;
        }
        self.get_dashboard_unchecked(&dash_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("failed to read imported dashboard"))
    }

    // ── Widget operations ──────────────────────────────────────────────────────

    pub async fn list_widgets(
        &self,
        dashboard_id: &str,
    ) -> anyhow::Result<Vec<crate::models::dashboard::Widget>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            dashboard_id: String,
            title: String,
            widget_type: String,
            query_config: String,
            position: String,
            display_config: String,
            created_at: String,
            updated_at: String,
        }
        let rows = self.client
            .query("SELECT id, dashboard_id, title, widget_type, query_config, position, display_config, created_at, updated_at FROM config_widgets FINAL WHERE dashboard_id = ? AND is_deleted = 0 ORDER BY created_at ASC")
            .bind(dashboard_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::dashboard::Widget {
                id: r.id,
                dashboard_id: r.dashboard_id,
                title: r.title,
                widget_type: r.widget_type,
                query_config: r.query_config,
                position: r.position,
                display_config: r.display_config,
                created_at: r.created_at,
                updated_at: r.updated_at,
            })
            .collect())
    }

    pub async fn create_widget(
        &self,
        id: &str,
        dashboard_id: &str,
        title: &str,
        widget_type: &str,
        query_config: &str,
        position: &str,
        display_config: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_widgets (id, dashboard_id, title, widget_type, query_config, position, display_config, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(dashboard_id).bind(title).bind(widget_type)
            .bind(query_config).bind(position).bind(display_config)
            .bind(&now).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn update_widget(
        &self,
        id: &str,
        dashboard_id: &str,
        title: &str,
        widget_type: &str,
        query_config: &str,
        position: &str,
        display_config: &str,
    ) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            created_at: String,
        }
        let result = self.client
            .query("SELECT created_at FROM config_widgets FINAL WHERE id = ? AND dashboard_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id).bind(dashboard_id)
            .fetch_one::<Row>()
            .await;
        let row = match result {
            Ok(r) => r,
            Err(clickhouse::error::Error::RowNotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_widgets (id, dashboard_id, title, widget_type, query_config, position, display_config, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(dashboard_id).bind(title).bind(widget_type)
            .bind(query_config).bind(position).bind(display_config)
            .bind(&row.created_at).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    pub async fn delete_widget(&self, id: &str, dashboard_id: &str) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            title: String,
            widget_type: String,
            query_config: String,
            position: String,
            display_config: String,
            created_at: String,
        }
        let result = self.client
            .query("SELECT title, widget_type, query_config, position, display_config, created_at FROM config_widgets FINAL WHERE id = ? AND dashboard_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id).bind(dashboard_id)
            .fetch_one::<Row>()
            .await;
        let row = match result {
            Ok(r) => r,
            Err(clickhouse::error::Error::RowNotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_widgets (id, dashboard_id, title, widget_type, query_config, position, display_config, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(dashboard_id).bind(&row.title).bind(&row.widget_type)
            .bind(&row.query_config).bind(&row.position).bind(&row.display_config)
            .bind(&row.created_at).bind(&now).bind(ver)
            .execute()
            .await?;
        Ok(true)
    }

    // ── Dashboard template operations ─────────────────────────────────────────

    pub async fn list_dashboard_templates(
        &self,
    ) -> anyhow::Result<Vec<crate::models::dashboard::DashboardTemplate>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            description: String,
            category: String,
            is_builtin: u8,
            template_json: String,
            tags: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, name, description, category, is_builtin, template_json, tags, created_at FROM config_dashboard_templates FINAL WHERE is_deleted = 0 ORDER BY is_builtin DESC, name ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::dashboard::DashboardTemplate {
                id: r.id,
                name: r.name,
                description: r.description,
                category: r.category,
                is_builtin: r.is_builtin != 0,
                template_json: serde_json::from_str(&r.template_json).unwrap_or_default(),
                tags: serde_json::from_str(&r.tags).unwrap_or(serde_json::json!([])),
                created_at: r.created_at,
            })
            .collect())
    }

    pub async fn get_dashboard_template(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::dashboard::DashboardTemplate>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            description: String,
            category: String,
            is_builtin: u8,
            template_json: String,
            tags: String,
            created_at: String,
        }
        let result = self.client
            .query("SELECT id, name, description, category, is_builtin, template_json, tags, created_at FROM config_dashboard_templates FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::dashboard::DashboardTemplate {
                id: r.id,
                name: r.name,
                description: r.description,
                category: r.category,
                is_builtin: r.is_builtin != 0,
                template_json: serde_json::from_str(&r.template_json).unwrap_or_default(),
                tags: serde_json::from_str(&r.tags).unwrap_or(serde_json::json!([])),
                created_at: r.created_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn ensure_default_templates(&self) -> anyhow::Result<()> {
        // Upsert on every startup (ReplacingMergeTree dedupes by id + version), so
        // corrected built-in templates roll out to existing installs — not just
        // fresh ones. User-created templates have different ids and are untouched.

        fn w(
            title: &str,
            wt: &str,
            qc: serde_json::Value,
            pos: (i32, i32, i32, i32),
            dc: serde_json::Value,
        ) -> serde_json::Value {
            // Positions are authored 0-indexed; the grid (and the rest of the app) is
            // 1-indexed, so shift col/row by 1. Without this, col/row 0 computes to CSS
            // `auto` and widgets auto-pack into the wrong cells.
            serde_json::json!({"title":title,"widget_type":wt,"query_config":qc,"position":{"col":pos.0+1,"row":pos.1+1,"col_span":pos.2,"row_span":pos.3},"display_config":dc})
        }
        fn qc_svc(
            agg: &str,
            interval: Option<&str>,
            extra: Vec<serde_json::Value>,
            group_by: Option<Vec<&str>>,
            limit: Option<i32>,
        ) -> serde_json::Value {
            let mut filters =
                vec![serde_json::json!({"field":"service_name","op":"=","value":"$service"})];
            filters.extend(extra);
            let mut v =
                serde_json::json!({"time_range_minutes":60,"filters":filters,"aggregation":agg});
            if let Some(i) = interval {
                v["interval"] = serde_json::json!(i);
            }
            if let Some(g) = group_by {
                v["group_by"] = serde_json::json!(g);
            }
            if let Some(l) = limit {
                v["limit"] = serde_json::json!(l);
            }
            v
        }
        fn qc(
            agg: &str,
            interval: Option<&str>,
            filters: Vec<serde_json::Value>,
            group_by: Option<Vec<&str>>,
            limit: Option<i32>,
        ) -> serde_json::Value {
            let mut v =
                serde_json::json!({"time_range_minutes":60,"filters":filters,"aggregation":agg});
            if let Some(i) = interval {
                v["interval"] = serde_json::json!(i);
            }
            if let Some(g) = group_by {
                v["group_by"] = serde_json::json!(g);
            }
            if let Some(l) = limit {
                v["limit"] = serde_json::json!(l);
            }
            v
        }
        // Like qc() but tagged source:"logs" so widgets query the logs table.
        fn qc_logs(
            agg: &str,
            interval: Option<&str>,
            filters: Vec<serde_json::Value>,
            group_by: Option<Vec<&str>>,
            limit: Option<i32>,
        ) -> serde_json::Value {
            let mut v = qc(agg, interval, filters, group_by, limit);
            v["source"] = serde_json::json!("logs");
            v
        }
        // Metrics widget: PromQL against the metrics tables (source:"metrics").
        fn qc_metrics(promql: &str) -> serde_json::Value {
            serde_json::json!({"time_range_minutes":60,"source":"metrics","promql":promql,"filters":[]})
        }
        fn color(c: &str) -> serde_json::Value {
            serde_json::json!({"color":c})
        }
        fn empty() -> serde_json::Value {
            serde_json::json!({})
        }
        let ef = || vec![serde_json::json!({"field":"http_status_code","op":">=","value":"500"})];
        // A `$service` template variable, pre-wired so service-scoped templates load
        // with a dropdown instead of an unsubstituted placeholder.
        let svc_var = || serde_json::json!([{"name":"service","label":"service","type":"query","field":"service_name","include_all":false}]);

        let templates: Vec<(&str, &str, &str, &str, serde_json::Value)> = vec![
            (
                "tpl-service-overview",
                "Service Overview",
                "Golden signals for a single service: request rate, error rate, and latency percentiles.",
                "apm",
                serde_json::json!({"widgets":[w("Request Rate","timeseries",qc_svc("count",Some("1m"),vec![],None,None),(0,0,6,4),color("#3b82f6")),w("Error Rate","timeseries",qc_svc("count",Some("1m"),ef(),None,None),(6,0,6,4),color("#ef4444")),w("P50 Latency","timeseries",qc_svc("p50",Some("1m"),vec![],None,None),(0,4,4,4),color("#22c55e")),w("P99 Latency","timeseries",qc_svc("p99",Some("1m"),vec![],None,None),(4,4,4,4),color("#f59e0b")),w("Top Endpoints","table",qc_svc("count",None,vec![],Some(vec!["span_name"]),Some(10)),(8,4,4,4),empty())],"variables":svc_var()}),
            ),
            (
                "tpl-error-analysis",
                "Error Analysis",
                "Error count by service, top error messages, and error rate timeline.",
                "apm",
                serde_json::json!({"widgets":[w("Error Count","counter",qc("count",None,ef(),None,None),(0,0,3,3),color("#ef4444")),w("Error Rate Over Time","timeseries",qc("count",Some("5m"),ef(),None,None),(3,0,9,3),color("#ef4444")),w("Errors by Service","bar",qc("count",None,ef(),Some(vec!["service_name"]),Some(10)),(0,3,6,4),empty()),w("Top Error Messages","table",qc("count",None,ef(),Some(vec!["span_name"]),Some(20)),(6,3,6,4),empty())]}),
            ),
            (
                "tpl-latency-deep-dive",
                "Latency Deep-Dive",
                "P50/P99/P999 latency, latency by endpoint, and slow traces.",
                "apm",
                serde_json::json!({"widgets":[w("P50 / P99 Latency","timeseries",qc_svc("p50",Some("1m"),vec![],None,None),(0,0,12,4),color("#8b5cf6")),w("Latency by Endpoint","bar",qc_svc("p99",None,vec![],Some(vec!["span_name"]),Some(10)),(0,4,6,4),empty()),w("Slowest Traces","table",qc_svc("max",None,vec![],None,Some(20)),(6,4,6,4),empty())],"variables":svc_var()}),
            ),
            (
                "tpl-infra-overview",
                "Infrastructure Overview",
                "CPU, memory, pod count, and restart count for infrastructure monitoring.",
                "infrastructure",
                serde_json::json!({"widgets":[w("Pod Count","counter",qc("count",None,vec![],None,None),(0,0,3,3),color("#06b6d4")),w("CPU Utilization","timeseries",qc("avg",Some("1m"),vec![],None,None),(3,0,9,3),color("#3b82f6")),w("Memory Usage","timeseries",qc("avg",Some("1m"),vec![],None,None),(0,3,6,4),color("#22c55e")),w("Disk I/O","timeseries",qc("avg",Some("1m"),vec![],None,None),(6,3,6,4),color("#f59e0b"))]}),
            ),
            (
                "tpl-log-volume",
                "Log Volume",
                "Log count by severity, by service, and timeline for understanding ingestion patterns.",
                "security",
                serde_json::json!({"widgets":[w("Error/Fatal Count","counter",qc_logs("count",None,vec![serde_json::json!({"field":"severity_text","op":"IN","value":"ERROR,FATAL"})],None,None),(0,0,3,3),color("#ef4444")),w("Log Volume Over Time","timeseries",qc_logs("count",Some("5m"),vec![],None,None),(3,0,9,3),color("#6366f1")),w("Logs by Severity","bar",qc_logs("count",None,vec![],Some(vec!["severity_text"]),Some(10)),(0,3,6,4),empty()),w("Top Services by Log Count","table",qc_logs("count",None,vec![],Some(vec!["service_name"]),Some(20)),(6,3,6,4),empty())]}),
            ),
            (
                "tpl-postgresql-overview",
                "PostgreSQL",
                "PostgreSQL control room: collector freshness, connection pressure, query workload, waits, storage, replication, maintenance, and database health.",
                "database",
                serde_json::json!({"widgets":[
                    // ── Connections & throughput ──
                    w("Connections by state","timeseries",qc_metrics("sum by (state) (postgresql_connection_count)"),(0,0,6,4),empty()),
                    w("Transactions / s","timeseries",qc_metrics("sum(rate(postgresql_commits[5m]))"),(6,0,6,4),color("#22c55e")),
                    w("Rollbacks / s","timeseries",qc_metrics("sum(rate(postgresql_rollbacks[5m]))"),(0,4,6,4),color("#ef4444")),
                    w("Rows / s by operation","timeseries",qc_metrics("sum by (operation) (rate(postgresql_rows[5m]))"),(6,4,6,4),empty()),
                    // ── Cache & I/O ──
                    w("Cache hit ratio %","timeseries",qc_metrics("100 * sum(rate(postgresql_blocks_read{source=\"hit\"}[5m])) / (sum(rate(postgresql_blocks_read{source=\"hit\"}[5m])) + sum(rate(postgresql_blocks_read{source=\"read\"}[5m])))"),(0,8,6,4),color("#f59e0b")),
                    w("Block reads / s (hit vs read)","timeseries",qc_metrics("sum by (source) (rate(postgresql_blocks_read[5m]))"),(6,8,6,4),empty()),
                    // ── Locks, deadlocks & waits ──
                    w("Locks by mode","timeseries",qc_metrics("sum by (mode) (postgresql_database_locks)"),(0,12,6,4),empty()),
                    w("Deadlocks / s","timeseries",qc_metrics("sum(rate(postgresql_deadlocks[5m]))"),(6,12,6,4),color("#ef4444")),
                    w("Wait events","timeseries",qc_metrics("sum by (wait_event_type) (postgresql_wait_events)"),(0,16,6,4),empty()),
                    w("Temp bytes / s","timeseries",qc_metrics("sum(rate(postgresql_temp_bytes[5m]))"),(6,16,6,4),color("#a855f7")),
                    // ── Storage & replication ──
                    w("Database size","timeseries",qc_metrics("sum by (db) (postgresql_db_size)"),(0,20,6,4),color("#8b5cf6")),
                    w("Replication delay (bytes)","timeseries",qc_metrics("max(postgresql_replication_data_delay)"),(6,20,6,4),color("#06b6d4")),
                    // ── Access patterns ──
                    w("Sequential scans / s","timeseries",qc_metrics("sum(rate(postgresql_table_seq_scans[5m]))"),(0,24,6,4),color("#f59e0b")),
                    w("Index scans / s","timeseries",qc_metrics("sum(rate(postgresql_table_idx_scans[5m]))"),(6,24,6,4),color("#22c55e")),
                    // ── Maintenance & queries ──
                    w("XID wraparound %","timeseries",qc_metrics("100 * max(postgresql_database_xid_age) / 2100000000"),(0,28,6,4),color("#ef4444")),
                    w("Slowest query mean latency (ms)","timeseries",qc_metrics("max(postgresql_query_mean_time)"),(6,28,6,4),color("#3b82f6")),
                    // ── WAL & checkpoints ──
                    w("WAL generated / s","timeseries",qc_metrics("rate(postgresql_wal_lsn[5m])"),(0,32,6,4),color("#06b6d4")),
                    w("Checkpoints / s by kind","timeseries",qc_metrics("sum by (kind) (rate(postgresql_checkpoints[5m]))"),(6,32,6,4),empty()),
                    w("Checkpoint write time / s (ms)","timeseries",qc_metrics("rate(postgresql_checkpoint_write_time[5m])"),(0,36,6,4),color("#f59e0b")),
                    w("Checkpoint buffers written / s","timeseries",qc_metrics("rate(postgresql_checkpoint_buffers_written[5m])"),(6,36,6,4),color("#a855f7")),
                    w("Buffers allocated / s","timeseries",qc_metrics("sum(rate(postgresql_bgwriter_buffers_alloc[5m]))"),(0,40,6,4),color("#8b5cf6")),
                    w("Replication slot lag (bytes)","timeseries",qc_metrics("max by (slot) (postgresql_replication_slot_lag)"),(6,40,6,4),color("#06b6d4")),
                    // ── Saturation & efficiency ──
                    w("Connections % of max","timeseries",qc_metrics("100 * sum(postgresql_backends) / max(postgresql_max_connections)"),(0,44,6,4),color("#3b82f6")),
                    w("Commit ratio %","timeseries",qc_metrics("100 * sum(rate(postgresql_commits[5m])) / (sum(rate(postgresql_commits[5m])) + sum(rate(postgresql_rollbacks[5m])))"),(6,44,6,4),color("#22c55e")),
                    // ── Collector and diagnosis ──
                    w("Collector signal age","timeseries",qc_metrics("max by (signal) (postgresql_collector_signal_age)"),(0,48,6,4),color("#64748b")),
                    w("Oldest transaction","timeseries",qc_metrics("max(postgresql_oldest_transaction_age)"),(6,48,6,4),color("#ef4444")),
                    w("Query DB time","timeseries",qc_metrics("sum by (queryid) (postgresql_query_total_time)"),(0,52,6,4),color("#3b82f6")),
                    w("Dead row ratio %","timeseries",qc_metrics("max(postgresql_table_dead_ratio)"),(6,52,6,4),color("#f59e0b"))
                ]}),
            ),
            // Rush platform self-usage: how operators exercise the system. All series come
            // from the API's self-ingested `rush_*` metrics (source:"metrics" / PromQL).
            // Search query rate/timing split by `signal` (logs, spans/apm, metrics/PromQL);
            // all three signals share the same rush_search_* self-metrics, so latency /
            // result-size / empty / error widgets are apples-to-apples across them.
            (
                "tpl-rush-usage",
                "Rush Usage & Performance",
                "Full self-observability for the Rush platform: query rate by signal (APM/logs/metrics), search latency p50/p95/p99, result sizes, empty/error rates, API request load & latency, ingest throughput/spool backpressure, background-engine health, and ClickHouse storage health. Sourced entirely from the platform's own self-metrics.",
                "platform",
                serde_json::json!({"widgets":[
                    // ── Query rate by signal ──
                    w("Search queries / s by signal","timeseries",qc_metrics("sum by (signal) (rate(rush_search_queries_total[5m]))"),(0,0,6,4),empty()),
                    w("Metrics (PromQL) queries / s","timeseries",qc_metrics("sum(rate(rush_search_queries_total{signal=\"metrics\"}[5m]))"),(6,0,6,4),color("#a855f7")),
                    // ── Search latency percentiles (ms) ──
                    w("Search p95 latency by signal (ms)","timeseries",qc_metrics("rush_search_duration_ms_p95"),(0,4,6,4),color("#f59e0b")),
                    w("Search p99 latency by signal (ms)","timeseries",qc_metrics("rush_search_duration_ms_p99"),(6,4,6,4),color("#ef4444")),
                    w("Search avg latency by signal (ms)","timeseries",qc_metrics("sum by (signal) (rate(rush_search_duration_ms_sum[5m])) / sum by (signal) (rate(rush_search_duration_ms_count[5m]))"),(0,8,6,4),color("#22c55e")),
                    w("Search p50 latency by signal (ms)","timeseries",qc_metrics("rush_search_duration_ms_p50"),(6,8,6,4),color("#3b82f6")),
                    // ── Result sizes & query shape ──
                    w("Avg result rows by signal","timeseries",qc_metrics("sum by (signal) (rate(rush_search_result_rows_sum[5m])) / sum by (signal) (rate(rush_search_result_rows_count[5m]))"),(0,12,6,4),color("#06b6d4")),
                    w("Avg search query length (chars)","timeseries",qc_metrics("sum by (signal) (rate(rush_search_query_length_chars_sum[5m])) / sum by (signal) (rate(rush_search_query_length_chars_count[5m]))"),(6,12,6,4),color("#8b5cf6")),
                    // ── Quality signals: empty & error rate ──
                    w("Empty-result searches / s by signal","timeseries",qc_metrics("sum by (signal) (rate(rush_search_empty_total[5m]))"),(0,16,6,4),color("#f59e0b")),
                    w("Search error rate % by signal","timeseries",qc_metrics("100 * sum by (signal) (rate(rush_search_queries_total{outcome=\"error\"}[5m])) / sum by (signal) (rate(rush_search_queries_total[5m]))"),(6,16,6,4),color("#ef4444")),
                    // ── API request load (system-usage context) ──
                    w("API requests / s by route","timeseries",qc_metrics("sum by (route) (rate(rush_http_requests_total[5m]))"),(0,20,6,4),empty()),
                    w("API request p95 latency (ms)","timeseries",qc_metrics("rush_http_request_duration_ms_p95"),(6,20,6,4),color("#f59e0b")),
                    w("In-flight API requests","timeseries",qc_metrics("rush_http_requests_in_flight"),(0,24,6,4),color("#3b82f6")),
                    w("API 5xx / s by route","timeseries",qc_metrics("sum by (route) (rate(rush_http_requests_total{status_class=\"5xx\"}[5m]))"),(6,24,6,4),color("#ef4444")),
                    w("API request p99 latency (ms)","timeseries",qc_metrics("rush_http_request_duration_ms_p99"),(0,28,6,4),color("#ef4444")),
                    w("API request avg latency (ms)","timeseries",qc_metrics("sum(rate(rush_http_request_duration_ms_sum[5m])) / sum(rate(rush_http_request_duration_ms_count[5m]))"),(6,28,6,4),color("#22c55e")),
                    // ── Ingest throughput (self-ingested rush_ingest_* counters, split by signal) ──
                    w("Ingested events / s by signal","timeseries",qc_metrics("sum by (signal) (rate(rush_ingest_events_total[5m]))"),(0,32,6,4),empty()),
                    w("Ingested bytes / s by signal","timeseries",qc_metrics("sum by (signal) (rate(rush_ingest_bytes_total[5m]))"),(6,32,6,4),color("#06b6d4")),
                    w("Rejected events / s by signal","timeseries",qc_metrics("sum by (signal) (rate(rush_ingest_events_total{outcome=\"rejected\"}[5m]))"),(0,36,6,4),color("#ef4444")),
                    w("Avg ingested event size (bytes)","timeseries",qc_metrics("sum(rate(rush_ingest_bytes_total[5m])) / sum(rate(rush_ingest_events_total[5m]))"),(6,36,6,4),color("#8b5cf6")),
                    // ── Ingest spool (disk-backed buffer; rising = backpressure / downstream stalls) ──
                    w("Spool buffered (bytes)","timeseries",qc_metrics("rush_ingest_spool_bytes"),(0,40,6,4),color("#f59e0b")),
                    w("Spool segments on disk","timeseries",qc_metrics("rush_ingest_spool_segments"),(6,40,6,4),color("#a855f7")),
                    w("Oldest spooled segment age (s)","timeseries",qc_metrics("rush_ingest_spool_oldest_age_secs"),(0,44,6,4),color("#ef4444")),
                    // ── Background engines (anomaly / monitor / siem / slo / stats) ──
                    w("Engine runs / s by engine","timeseries",qc_metrics("sum by (engine) (rate(rush_engine_runs_total[5m]))"),(6,44,6,4),empty()),
                    w("Engine run p95 duration (ms)","timeseries",qc_metrics("rush_engine_run_duration_ms_p95"),(0,48,6,4),color("#f59e0b")),
                    w("Engine run avg duration (ms)","timeseries",qc_metrics("sum by (engine) (rate(rush_engine_run_duration_ms_sum[5m])) / sum by (engine) (rate(rush_engine_run_duration_ms_count[5m]))"),(6,48,6,4),color("#22c55e")),
                    w("Seconds since last engine run","timeseries",qc_metrics("time() - max by (engine) (rush_engine_last_run_timestamp)"),(0,52,6,4),color("#ef4444")),
                    // ── ClickHouse storage backend health (rush_ch_* gauges) ──
                    w("CH resident memory (bytes)","timeseries",qc_metrics("rush_ch_memory_resident_bytes"),(6,52,6,4),color("#3b82f6")),
                    w("CH active merges","timeseries",qc_metrics("rush_ch_active_merges"),(0,56,6,4),color("#06b6d4")),
                    w("CH active mutations","timeseries",qc_metrics("rush_ch_active_mutations"),(6,56,6,4),color("#a855f7")),
                    w("CH longest running merge (s)","timeseries",qc_metrics("rush_ch_longest_running_merge_secs"),(0,60,6,4),color("#f59e0b")),
                    w("CH max parts per partition","timeseries",qc_metrics("rush_ch_max_part_count_for_partition"),(6,60,6,4),color("#ef4444")),
                    w("CH delayed inserts","timeseries",qc_metrics("rush_ch_delayed_inserts"),(0,64,6,4),color("#ef4444")),
                    w("CH background pool tasks","timeseries",qc_metrics("rush_ch_background_pool_task"),(6,64,6,4),color("#8b5cf6")),
                    w("CH failed queries (cumulative)","timeseries",qc_metrics("rush_ch_failed_query_total"),(0,68,6,4),color("#ef4444"))
                ]}),
            ),
        ];

        for (id, name, desc, category, json_val) in &templates {
            let json_str = serde_json::to_string(json_val)?;
            let now = Self::now_str();
            let ver = Self::next_version();
            self.client
                .query("INSERT INTO config_dashboard_templates (id, name, description, category, is_builtin, template_json, tags, created_at, version, is_deleted) VALUES (?, ?, ?, ?, 1, ?, '[]', ?, ?, 0)")
                .bind(*id).bind(*name).bind(*desc).bind(*category)
                .bind(&json_str).bind(&now).bind(ver)
                .execute()
                .await?;
        }
        Ok(())
    }

    // ── Notification channel operations ───────────────────────────────────────

    pub async fn list_channels(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<crate::models::alert::NotificationChannel>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            tenant_id: String,
            name: String,
            channel_type: String,
            config: String,
            enabled: u8,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, tenant_id, name, channel_type, config, enabled, created_at FROM config_notification_channels FINAL WHERE tenant_id = ? AND is_deleted = 0 ORDER BY created_at DESC")
            .bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::alert::NotificationChannel {
                id: r.id,
                tenant_id: r.tenant_id,
                name: r.name,
                channel_type: r.channel_type,
                config: r.config,
                enabled: r.enabled != 0,
                created_at: r.created_at,
            })
            .collect())
    }

    pub async fn get_channel(
        &self,
        id: &str,
        tenant_id: &str,
    ) -> anyhow::Result<Option<crate::models::alert::NotificationChannel>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            tenant_id: String,
            name: String,
            channel_type: String,
            config: String,
            enabled: u8,
            created_at: String,
        }
        let result = self.client
            .query("SELECT id, tenant_id, name, channel_type, config, enabled, created_at FROM config_notification_channels FINAL WHERE id = ? AND tenant_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id).bind(tenant_id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::alert::NotificationChannel {
                id: r.id,
                tenant_id: r.tenant_id,
                name: r.name,
                channel_type: r.channel_type,
                config: r.config,
                enabled: r.enabled != 0,
                created_at: r.created_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_channel_by_id(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::alert::NotificationChannel>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            tenant_id: String,
            name: String,
            channel_type: String,
            config: String,
            enabled: u8,
            created_at: String,
        }
        let result = self.client
            .query("SELECT id, tenant_id, name, channel_type, config, enabled, created_at FROM config_notification_channels FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::alert::NotificationChannel {
                id: r.id,
                tenant_id: r.tenant_id,
                name: r.name,
                channel_type: r.channel_type,
                config: r.config,
                enabled: r.enabled != 0,
                created_at: r.created_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_channel(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        channel_type: &str,
        config: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_notification_channels (id, tenant_id, name, channel_type, config, enabled, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, 1, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(channel_type).bind(config).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn update_channel(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        config: &str,
        enabled: bool,
    ) -> anyhow::Result<bool> {
        let existing = self.get_channel(id, tenant_id).await?;
        let row = match existing {
            Some(r) => r,
            None => return Ok(false),
        };
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_notification_channels (id, tenant_id, name, channel_type, config, enabled, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(&row.channel_type).bind(config)
            .bind(if enabled { 1u8 } else { 0u8 }).bind(&row.created_at).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_channel(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        let existing = self.get_channel(id, tenant_id).await?;
        let row = match existing {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_notification_channels (id, tenant_id, name, channel_type, config, enabled, created_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(tenant_id).bind(&row.name).bind(&row.channel_type).bind(&row.config)
            .bind(if row.enabled { 1u8 } else { 0u8 }).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    // ── Notification log operations ────────────────────────────────────────────

    pub async fn create_notification_log(
        &self,
        channel_id: &str,
        tenant_id: &str,
        alert_type: &str,
        alert_name: &str,
        severity: &str,
        status: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_notification_log (id, channel_id, tenant_id, alert_type, alert_name, severity, status, error, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&id).bind(channel_id).bind(tenant_id).bind(alert_type).bind(alert_name).bind(severity).bind(status).bind(error).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_notification_log(
        &self,
        tenant_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::alert::NotificationLogEntry>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            channel_id: String,
            tenant_id: String,
            alert_type: String,
            alert_name: String,
            severity: String,
            status: String,
            error: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, channel_id, tenant_id, alert_type, alert_name, severity, status, error, created_at FROM config_notification_log WHERE tenant_id = ? ORDER BY created_at DESC LIMIT ?")
            .bind(tenant_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::alert::NotificationLogEntry {
                id: r.id,
                channel_id: r.channel_id,
                tenant_id: r.tenant_id,
                alert_type: r.alert_type,
                alert_name: r.alert_name,
                severity: r.severity,
                status: r.status,
                error: r.error,
                created_at: r.created_at,
            })
            .collect())
    }

    // ── Alert rule operations ──────────────────────────────────────────────────

    fn map_alert_row(r: AlertRuleRow) -> crate::models::alert::AlertRule {
        crate::models::alert::AlertRule {
            id: r.id,
            name: r.name,
            description: r.description,
            enabled: r.enabled != 0,
            signal_type: r.signal_type,
            query_config: r.query_config,
            condition_op: r.condition_op,
            condition_threshold: r.condition_threshold,
            eval_interval_secs: r.eval_interval_secs,
            notification_channel_ids: r.notification_channel_ids,
            runbook_url: r.runbook_url,
            state: r.state,
            last_eval_at: if r.last_eval_at.is_empty() {
                None
            } else {
                Some(r.last_eval_at)
            },
            last_triggered_at: if r.last_triggered_at.is_empty() {
                None
            } else {
                Some(r.last_triggered_at)
            },
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }

    pub async fn list_alerts(&self) -> anyhow::Result<Vec<crate::models::alert::AlertRule>> {
        let rows = self.client
            .query("SELECT id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_alert_rules FINAL WHERE is_deleted = 0 ORDER BY created_at DESC")
            .fetch_all::<AlertRuleRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_alert_row).collect())
    }

    pub async fn get_alert(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::alert::AlertRule>> {
        let result = self.client
            .query("SELECT id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_alert_rules FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .fetch_one::<AlertRuleRow>()
            .await;
        match result {
            Ok(r) => Ok(Some(Self::map_alert_row(r))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_alert(
        &self,
        id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        signal_type: &str,
        query_config: &str,
        condition_op: &str,
        condition_threshold: f64,
        eval_interval_secs: i64,
        notification_channel_ids: &str,
        runbook_url: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_alert_rules (id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ok', '', '', ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(signal_type).bind(query_config).bind(condition_op)
            .bind(condition_threshold).bind(eval_interval_secs).bind(notification_channel_ids)
            .bind(runbook_url).bind(&now).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn update_alert(
        &self,
        id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        signal_type: &str,
        query_config: &str,
        condition_op: &str,
        condition_threshold: f64,
        eval_interval_secs: i64,
        notification_channel_ids: &str,
        runbook_url: &str,
    ) -> anyhow::Result<bool> {
        let existing = match self.get_alert(id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_alert_rules (id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(signal_type).bind(query_config).bind(condition_op)
            .bind(condition_threshold).bind(eval_interval_secs).bind(notification_channel_ids)
            .bind(runbook_url).bind(&existing.state)
            .bind(existing.last_eval_at.unwrap_or_default())
            .bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_alert(&self, id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_alert(id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_alert_rules (id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&existing.name).bind(&existing.description)
            .bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.signal_type).bind(&existing.query_config).bind(&existing.condition_op)
            .bind(existing.condition_threshold).bind(existing.eval_interval_secs)
            .bind(&existing.notification_channel_ids).bind(&existing.runbook_url).bind(&existing.state)
            .bind(existing.last_eval_at.unwrap_or_default())
            .bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn update_alert_state(
        &self,
        id: &str,
        state: &str,
        last_eval_at: &str,
        last_triggered_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let existing = match self.get_alert(id).await? {
            Some(r) => r,
            None => return Ok(()),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        let lta = last_triggered_at
            .map(|s| s.to_string())
            .unwrap_or_else(|| existing.last_triggered_at.clone().unwrap_or_default());
        self.client
            .query("INSERT INTO config_alert_rules (id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.name).bind(&existing.description)
            .bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.signal_type).bind(&existing.query_config).bind(&existing.condition_op)
            .bind(existing.condition_threshold).bind(existing.eval_interval_secs)
            .bind(&existing.notification_channel_ids).bind(&existing.runbook_url)
            .bind(state).bind(last_eval_at).bind(&lta)
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    /// Narrow `last_eval_at` flush for the alert engine: re-inserts the rule row the
    /// engine already fetched this tick (state unchanged), avoiding the SELECT…FINAL
    /// read-modify-write of `update_alert_state`. Only call when no state transition
    /// occurred — transitions must go through `update_alert_state`.
    pub async fn persist_alert_rule_eval(
        &self,
        rule: &crate::models::alert::AlertRule,
        last_eval_at: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_alert_rules (id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(&rule.id).bind(&rule.name).bind(&rule.description)
            .bind(if rule.enabled { 1u8 } else { 0u8 })
            .bind(&rule.signal_type).bind(&rule.query_config).bind(&rule.condition_op)
            .bind(rule.condition_threshold).bind(rule.eval_interval_secs)
            .bind(&rule.notification_channel_ids).bind(&rule.runbook_url)
            .bind(&rule.state).bind(last_eval_at)
            .bind(rule.last_triggered_at.clone().unwrap_or_default())
            .bind(&rule.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn get_due_alerts(
        &self,
        now: &str,
    ) -> anyhow::Result<Vec<crate::models::alert::AlertRule>> {
        let rows = self.client
            .query("SELECT id, name, description, enabled, signal_type, query_config, condition_op, condition_threshold, eval_interval_secs, notification_channel_ids, runbook_url, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_alert_rules FINAL WHERE enabled = 1 AND is_deleted = 0 AND (last_eval_at = '' OR toUnixTimestamp(parseDateTimeBestEffort(?)) - toUnixTimestamp(parseDateTimeBestEffort(last_eval_at)) >= eval_interval_secs)")
            .bind(now)
            .fetch_all::<AlertRuleRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_alert_row).collect())
    }

    // ── Alert event operations ─────────────────────────────────────────────────

    pub async fn create_alert_event(
        &self,
        id: &str,
        rule_id: &str,
        state: &str,
        value: f64,
        threshold: f64,
        message: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_alert_events (id, rule_id, state, value, threshold, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(rule_id).bind(state).bind(value).bind(threshold).bind(message).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_alert_events(
        &self,
        rule_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::alert::AlertEvent>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            rule_id: String,
            state: String,
            value: f64,
            threshold: f64,
            message: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, rule_id, state, value, threshold, message, created_at FROM config_alert_events WHERE rule_id = ? ORDER BY created_at DESC LIMIT ?")
            .bind(rule_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::alert::AlertEvent {
                id: r.id,
                rule_id: r.rule_id,
                state: r.state,
                value: r.value,
                threshold: r.threshold,
                message: r.message,
                created_at: r.created_at,
            })
            .collect())
    }

    pub async fn list_all_alert_events(
        &self,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::alert::AlertEventWithRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            rule_id: String,
            rule_name: String,
            state: String,
            value: f64,
            threshold: f64,
            message: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT e.id, e.rule_id, coalesce(r.name, 'deleted rule') AS rule_name, e.state, e.value, e.threshold, e.message, e.created_at FROM config_alert_events e LEFT JOIN (SELECT id, name FROM config_alert_rules FINAL WHERE is_deleted = 0) r ON e.rule_id = r.id ORDER BY e.created_at DESC LIMIT ?")
            .bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::alert::AlertEventWithRule {
                id: r.id,
                rule_id: r.rule_id,
                rule_name: r.rule_name,
                state: r.state,
                value: r.value,
                threshold: r.threshold,
                message: r.message,
                created_at: r.created_at,
            })
            .collect())
    }

    // ── Deploy marker operations ───────────────────────────────────────────────

    pub async fn create_deploy_marker(
        &self,
        id: &str,
        service_name: &str,
        version: &str,
        commit_sha: &str,
        description: &str,
        environment: &str,
        deployed_by: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_deploy_markers (id, service_name, version, commit_sha, description, environment, deployed_by, deployed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(service_name).bind(version).bind(commit_sha).bind(description).bind(environment).bind(deployed_by).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_deploy_markers(
        &self,
        service_name: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
    ) -> anyhow::Result<Vec<crate::models::deploy::DeployMarker>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            service_name: String,
            version: String,
            commit_sha: String,
            description: String,
            environment: String,
            deployed_by: String,
            deployed_at: String,
        }
        // Build query dynamically; ClickHouse doesn't support optional parameters so we build different SQL
        let sql = {
            let mut s = "SELECT id, service_name, version, commit_sha, description, environment, deployed_by, deployed_at FROM config_deploy_markers WHERE 1=1".to_string();
            if service_name.is_some() {
                s.push_str(" AND service_name = ?");
            }
            // deployed_at is a String column stored space-separated ("YYYY-MM-DD HH:MM:SS")
            // while callers pass ISO ("...T...Z"); a raw string compare mismatches on the
            // 'T' vs ' ' separator (space < 'T'), silently dropping in-window markers.
            // Parse both sides so the window filter is timestamp-format-agnostic.
            if from.is_some() {
                s.push_str(
                    " AND parseDateTimeBestEffort(deployed_at) >= parseDateTimeBestEffort(?)",
                );
            }
            if to.is_some() {
                s.push_str(
                    " AND parseDateTimeBestEffort(deployed_at) <= parseDateTimeBestEffort(?)",
                );
            }
            s.push_str(" ORDER BY deployed_at DESC LIMIT 100");
            s
        };
        let mut q = self.client.query(&sql);
        if let Some(sn) = service_name {
            q = q.bind(sn);
        }
        if let Some(f) = from {
            q = q.bind(f);
        }
        if let Some(t) = to {
            q = q.bind(t);
        }
        let rows = q.fetch_all::<Row>().await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::deploy::DeployMarker {
                id: r.id,
                service_name: r.service_name,
                version: r.version,
                commit_sha: r.commit_sha,
                description: r.description,
                environment: r.environment,
                deployed_by: r.deployed_by,
                deployed_at: r.deployed_at,
            })
            .collect())
    }

    // ── SLO operations ─────────────────────────────────────────────────────────

    fn map_slo_row(r: SloRow) -> crate::models::slo::Slo {
        crate::models::slo::Slo {
            id: r.id,
            tenant_id: r.tenant_id,
            name: r.name,
            description: r.description,
            enabled: r.enabled != 0,
            slo_type: r.slo_type,
            indicator_type: r.indicator_type,
            service_name: r.service_name,
            metric_name: r.metric_name,
            window_type: r.window_type,
            target_percentage: r.target_percentage,
            threshold_ms: r.threshold_ms,
            threshold_value: r.threshold_value,
            threshold_op: if r.threshold_op.is_empty() {
                None
            } else {
                Some(r.threshold_op)
            },
            error_filters: r.error_filters,
            total_filters: r.total_filters,
            eval_interval_secs: r.eval_interval_secs,
            notification_channel_ids: r.notification_channel_ids,
            state: r.state,
            error_budget_remaining: r.error_budget_remaining,
            error_count: r.error_count,
            total_count: r.total_count,
            last_eval_at: if r.last_eval_at.is_empty() {
                None
            } else {
                Some(r.last_eval_at)
            },
            last_breached_at: if r.last_breached_at.is_empty() {
                None
            } else {
                Some(r.last_breached_at)
            },
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }

    pub async fn list_slos(&self, tenant_id: &str) -> anyhow::Result<Vec<crate::models::slo::Slo>> {
        let rows = self.client
            .query("SELECT id, tenant_id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at FROM config_slos FINAL WHERE tenant_id = ? AND is_deleted = 0 ORDER BY created_at DESC")
            .bind(tenant_id)
            .fetch_all::<SloRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_slo_row).collect())
    }

    pub async fn get_slo(
        &self,
        id: &str,
        tenant_id: &str,
    ) -> anyhow::Result<Option<crate::models::slo::Slo>> {
        let result = self.client
            .query("SELECT id, tenant_id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at FROM config_slos FINAL WHERE id = ? AND tenant_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .bind(tenant_id)
            .fetch_one::<SloRow>()
            .await;
        match result {
            Ok(r) => Ok(Some(Self::map_slo_row(r))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_slo(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        slo_type: &str,
        indicator_type: &str,
        service_name: &str,
        metric_name: &str,
        window_type: &str,
        target_percentage: f64,
        threshold_ms: Option<f64>,
        threshold_value: Option<f64>,
        threshold_op: Option<&str>,
        error_filters: &str,
        total_filters: &str,
        eval_interval_secs: i64,
        notification_channel_ids: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_slos (id, tenant_id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'compliant', NULL, NULL, NULL, '', '', ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(slo_type).bind(indicator_type).bind(service_name).bind(metric_name)
            .bind(window_type).bind(target_percentage).bind(threshold_ms).bind(threshold_value)
            .bind(threshold_op.unwrap_or("")).bind(error_filters).bind(total_filters)
            .bind(eval_interval_secs).bind(notification_channel_ids)
            .bind(&now).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_slo(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        slo_type: &str,
        indicator_type: &str,
        service_name: &str,
        metric_name: &str,
        window_type: &str,
        target_percentage: f64,
        threshold_ms: Option<f64>,
        threshold_value: Option<f64>,
        threshold_op: Option<&str>,
        error_filters: &str,
        total_filters: &str,
        eval_interval_secs: i64,
        notification_channel_ids: &str,
    ) -> anyhow::Result<bool> {
        let existing = match self.get_slo(id, tenant_id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_slos (id, tenant_id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(slo_type).bind(indicator_type).bind(service_name).bind(metric_name)
            .bind(window_type).bind(target_percentage).bind(threshold_ms).bind(threshold_value)
            .bind(threshold_op.unwrap_or("")).bind(error_filters).bind(total_filters)
            .bind(eval_interval_secs).bind(notification_channel_ids)
            .bind(&existing.state).bind(existing.error_budget_remaining).bind(existing.error_count).bind(existing.total_count)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(existing.last_breached_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_slo(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_slo(id, tenant_id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_slos (id, tenant_id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.description).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.slo_type).bind(&existing.indicator_type).bind(&existing.service_name).bind(&existing.metric_name)
            .bind(&existing.window_type).bind(existing.target_percentage).bind(existing.threshold_ms).bind(existing.threshold_value)
            .bind(existing.threshold_op.unwrap_or_default()).bind(&existing.error_filters).bind(&existing.total_filters)
            .bind(existing.eval_interval_secs).bind(&existing.notification_channel_ids)
            .bind(&existing.state).bind(existing.error_budget_remaining).bind(existing.error_count).bind(existing.total_count)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(existing.last_breached_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn get_due_slos(&self, now: &str) -> anyhow::Result<Vec<crate::models::slo::Slo>> {
        let rows = self.client
            .query("SELECT id, tenant_id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at FROM config_slos FINAL WHERE enabled = 1 AND is_deleted = 0 AND (last_eval_at = '' OR toUnixTimestamp(parseDateTimeBestEffort(?)) - toUnixTimestamp(parseDateTimeBestEffort(last_eval_at)) >= eval_interval_secs)")
            .bind(now)
            .fetch_all::<SloRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_slo_row).collect())
    }

    pub async fn update_slo_state(
        &self,
        id: &str,
        tenant_id: &str,
        state: &str,
        error_budget_remaining: f64,
        error_count: i64,
        total_count: i64,
        last_eval_at: &str,
        last_breached_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let existing = match self.get_slo(id, tenant_id).await? {
            Some(r) => r,
            None => return Ok(()),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        let lba = last_breached_at
            .map(|s| s.to_string())
            .unwrap_or_else(|| existing.last_breached_at.clone().unwrap_or_default());
        self.client
            .query("INSERT INTO config_slos (id, tenant_id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.description).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.slo_type).bind(&existing.indicator_type).bind(&existing.service_name).bind(&existing.metric_name)
            .bind(&existing.window_type).bind(existing.target_percentage).bind(existing.threshold_ms).bind(existing.threshold_value)
            .bind(existing.threshold_op.unwrap_or_default()).bind(&existing.error_filters).bind(&existing.total_filters)
            .bind(existing.eval_interval_secs).bind(&existing.notification_channel_ids)
            .bind(state).bind(error_budget_remaining).bind(error_count).bind(total_count)
            .bind(last_eval_at).bind(&lba)
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    /// Narrow eval flush for the SLO engine: re-inserts the SLO row the engine already
    /// fetched this tick with the freshly computed budget/count values, avoiding the
    /// SELECT…FINAL read-modify-write of `update_slo_state`. Only call when no state
    /// transition occurred — transitions must go through `update_slo_state`.
    #[allow(clippy::too_many_arguments)]
    pub async fn persist_slo_eval(
        &self,
        slo: &crate::models::slo::Slo,
        state: &str,
        error_budget_remaining: f64,
        error_count: i64,
        total_count: i64,
        last_eval_at: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_slos (id, tenant_id, name, description, enabled, slo_type, indicator_type, service_name, metric_name, window_type, target_percentage, threshold_ms, threshold_value, threshold_op, error_filters, total_filters, eval_interval_secs, notification_channel_ids, state, error_budget_remaining, error_count, total_count, last_eval_at, last_breached_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(&slo.id).bind(&slo.tenant_id).bind(&slo.name).bind(&slo.description).bind(if slo.enabled { 1u8 } else { 0u8 })
            .bind(&slo.slo_type).bind(&slo.indicator_type).bind(&slo.service_name).bind(&slo.metric_name)
            .bind(&slo.window_type).bind(slo.target_percentage).bind(slo.threshold_ms).bind(slo.threshold_value)
            .bind(slo.threshold_op.clone().unwrap_or_default()).bind(&slo.error_filters).bind(&slo.total_filters)
            .bind(slo.eval_interval_secs).bind(&slo.notification_channel_ids)
            .bind(state).bind(error_budget_remaining).bind(error_count).bind(total_count)
            .bind(last_eval_at).bind(slo.last_breached_at.clone().unwrap_or_default())
            .bind(&slo.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn create_slo_event(
        &self,
        id: &str,
        slo_id: &str,
        tenant_id: &str,
        state: &str,
        error_count: i64,
        total_count: i64,
        error_budget_remaining: f64,
        message: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_slo_events (id, slo_id, tenant_id, state, error_count, total_count, error_budget_remaining, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(slo_id).bind(tenant_id).bind(state).bind(error_count).bind(total_count).bind(error_budget_remaining).bind(message).bind(&now)
            .execute().await?;
        Ok(())
    }

    /// Latest incident lifecycle state. `no_data` is deliberately excluded:
    /// it describes evaluation availability, not whether an incident opened or
    /// recovered.
    pub async fn latest_slo_event_state(
        &self,
        slo_id: &str,
        tenant_id: &str,
    ) -> anyhow::Result<Option<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            state: String,
        }

        let result = self
            .client
            .query("SELECT state FROM config_slo_events WHERE slo_id = ? AND tenant_id = ? AND state IN ('breaching', 'compliant') ORDER BY created_at DESC LIMIT 1")
            .bind(slo_id)
            .bind(tenant_id)
            .fetch_one::<Row>()
            .await;

        match result {
            Ok(row) => Ok(Some(row.state)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list_slo_events(
        &self,
        slo_id: &str,
        tenant_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::slo::SloEvent>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            slo_id: String,
            tenant_id: String,
            state: String,
            error_count: i64,
            total_count: i64,
            error_budget_remaining: f64,
            message: String,
            created_at: String,
        }
        // Read extra legacy rows so the normalizer can remove repeated
        // compliant/no-data entries without prematurely exhausting the API
        // limit. New writes already follow the strict incident lifecycle.
        let raw_limit = limit.max(1).saturating_mul(10) as u64;
        let rows = self.client
            .query("SELECT id, slo_id, tenant_id, state, error_count, total_count, error_budget_remaining, message, created_at FROM config_slo_events WHERE slo_id = ? AND tenant_id = ? ORDER BY created_at DESC LIMIT ?")
            .bind(slo_id).bind(tenant_id).bind(raw_limit)
            .fetch_all::<Row>()
            .await?;
        let events = rows
            .into_iter()
            .map(|r| crate::models::slo::SloEvent {
                id: r.id,
                slo_id: r.slo_id,
                tenant_id: r.tenant_id,
                state: r.state,
                error_count: r.error_count,
                total_count: r.total_count,
                error_budget_remaining: r.error_budget_remaining,
                message: r.message,
                created_at: r.created_at,
            })
            .collect();
        Ok(normalize_slo_incident_events(events, limit.max(0) as usize))
    }

    // ── Anomaly rule operations ────────────────────────────────────────────────

    fn map_anomaly_rule(r: AnomalyRuleRow) -> crate::models::anomaly::AnomalyRule {
        crate::models::anomaly::AnomalyRule {
            id: r.id,
            tenant_id: r.tenant_id,
            name: r.name,
            description: r.description,
            enabled: r.enabled != 0,
            source: r.source,
            pattern: r.pattern,
            query: r.query,
            service_name: r.service_name,
            apm_metric: r.apm_metric,
            sensitivity: r.sensitivity,
            alpha: r.alpha,
            eval_interval_secs: r.eval_interval_secs,
            window_secs: r.window_secs,
            split_labels: r.split_labels,
            notification_channel_ids: r.notification_channel_ids,
            state: r.state,
            last_eval_at: if r.last_eval_at.is_empty() {
                None
            } else {
                Some(r.last_eval_at)
            },
            last_triggered_at: if r.last_triggered_at.is_empty() {
                None
            } else {
                Some(r.last_triggered_at)
            },
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }

    pub async fn list_anomaly_rules(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyRule>> {
        let rows = self.client
            .query("SELECT id, tenant_id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_anomaly_rules FINAL WHERE tenant_id = ? AND is_deleted = 0 ORDER BY created_at DESC")
            .bind(tenant_id)
            .fetch_all::<AnomalyRuleRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_anomaly_rule).collect())
    }

    pub async fn get_anomaly_rule(
        &self,
        id: &str,
        tenant_id: &str,
    ) -> anyhow::Result<Option<crate::models::anomaly::AnomalyRule>> {
        let result = self.client
            .query("SELECT id, tenant_id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_anomaly_rules FINAL WHERE id = ? AND tenant_id = ? AND is_deleted = 0 LIMIT 1")
            .bind(id)
            .bind(tenant_id)
            .fetch_one::<AnomalyRuleRow>()
            .await;
        match result {
            Ok(r) => Ok(Some(Self::map_anomaly_rule(r))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_anomaly_rule(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        source: &str,
        pattern: &str,
        query: &str,
        service_name: &str,
        apm_metric: &str,
        sensitivity: f64,
        alpha: f64,
        eval_interval_secs: i64,
        window_secs: i64,
        split_labels: &str,
        notification_channel_ids: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_anomaly_rules (id, tenant_id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'normal', '', '', ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(source).bind(pattern).bind(query).bind(service_name).bind(apm_metric)
            .bind(sensitivity).bind(alpha).bind(eval_interval_secs).bind(window_secs)
            .bind(split_labels).bind(notification_channel_ids)
            .bind(&now).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_anomaly_rule(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        description: &str,
        enabled: bool,
        source: &str,
        pattern: &str,
        query: &str,
        service_name: &str,
        apm_metric: &str,
        sensitivity: f64,
        alpha: f64,
        eval_interval_secs: i64,
        window_secs: i64,
        split_labels: &str,
        notification_channel_ids: &str,
    ) -> anyhow::Result<bool> {
        let existing = match self.get_anomaly_rule(id, tenant_id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_anomaly_rules (id, tenant_id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(name).bind(description).bind(if enabled { 1u8 } else { 0u8 })
            .bind(source).bind(pattern).bind(query).bind(service_name).bind(apm_metric)
            .bind(sensitivity).bind(alpha).bind(eval_interval_secs).bind(window_secs)
            .bind(split_labels).bind(notification_channel_ids).bind(&existing.state)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_anomaly_rule(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_anomaly_rule(id, tenant_id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_anomaly_rules (id, tenant_id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.description).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.source).bind(&existing.pattern).bind(&existing.query)
            .bind(&existing.service_name).bind(&existing.apm_metric)
            .bind(existing.sensitivity).bind(existing.alpha)
            .bind(existing.eval_interval_secs).bind(existing.window_secs)
            .bind(&existing.split_labels).bind(&existing.notification_channel_ids)
            .bind(&existing.state).bind(existing.last_eval_at.unwrap_or_default())
            .bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn get_due_anomaly_rules(
        &self,
        now: &str,
    ) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyRule>> {
        // Engine lister: returns due rules across ALL tenants (deliberately NOT
        // tenant-filtered). Each rule carries its own tenant_id so the engine can
        // scope the telemetry queries and stamp anomaly events per tenant.
        let rows = self.client
            .query("SELECT id, tenant_id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at FROM config_anomaly_rules FINAL WHERE enabled = 1 AND is_deleted = 0 AND (last_eval_at = '' OR toUnixTimestamp(parseDateTimeBestEffort(?)) - toUnixTimestamp(parseDateTimeBestEffort(last_eval_at)) >= eval_interval_secs)")
            .bind(now)
            .fetch_all::<AnomalyRuleRow>()
            .await?;
        Ok(rows.into_iter().map(Self::map_anomaly_rule).collect())
    }

    pub async fn update_anomaly_state(
        &self,
        id: &str,
        tenant_id: &str,
        state: &str,
        last_eval_at: &str,
        last_triggered_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let existing = match self.get_anomaly_rule(id, tenant_id).await? {
            Some(r) => r,
            None => return Ok(()),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        let lta = last_triggered_at
            .map(|s| s.to_string())
            .unwrap_or_else(|| existing.last_triggered_at.clone().unwrap_or_default());
        self.client
            .query("INSERT INTO config_anomaly_rules (id, tenant_id, name, description, enabled, source, pattern, query, service_name, apm_metric, sensitivity, alpha, eval_interval_secs, window_secs, split_labels, notification_channel_ids, state, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.description).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.source).bind(&existing.pattern).bind(&existing.query)
            .bind(&existing.service_name).bind(&existing.apm_metric)
            .bind(existing.sensitivity).bind(existing.alpha)
            .bind(existing.eval_interval_secs).bind(existing.window_secs)
            .bind(&existing.split_labels).bind(&existing.notification_channel_ids)
            .bind(state).bind(last_eval_at).bind(&lta)
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    // ── Anomaly event operations ───────────────────────────────────────────────

    pub async fn get_anomaly_event(
        &self,
        id: &str,
        tenant_id: &str,
    ) -> anyhow::Result<Option<crate::models::anomaly::AnomalyEvent>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            rule_id: String,
            tenant_id: String,
            state: String,
            metric: String,
            value: f64,
            expected: f64,
            deviation: f64,
            message: String,
            created_at: String,
        }
        let result = self.client
            .query("SELECT id, rule_id, tenant_id, state, metric, value, expected, deviation, message, created_at FROM config_anomaly_events WHERE id = ? AND tenant_id = ? LIMIT 1")
            .bind(id)
            .bind(tenant_id)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::anomaly::AnomalyEvent {
                id: r.id,
                rule_id: r.rule_id,
                tenant_id: r.tenant_id,
                state: r.state,
                metric: r.metric,
                value: r.value,
                expected: r.expected,
                deviation: r.deviation,
                message: r.message,
                created_at: r.created_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_anomaly_event(
        &self,
        id: &str,
        rule_id: &str,
        tenant_id: &str,
        state: &str,
        metric: &str,
        value: f64,
        expected: f64,
        deviation: f64,
        message: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_anomaly_events (id, rule_id, tenant_id, state, metric, value, expected, deviation, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(rule_id).bind(tenant_id).bind(state).bind(metric).bind(value).bind(expected).bind(deviation).bind(message).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_anomaly_events(
        &self,
        rule_id: &str,
        tenant_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyEvent>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            rule_id: String,
            tenant_id: String,
            state: String,
            metric: String,
            value: f64,
            expected: f64,
            deviation: f64,
            message: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, rule_id, tenant_id, state, metric, value, expected, deviation, message, created_at FROM config_anomaly_events WHERE rule_id = ? AND tenant_id = ? ORDER BY created_at DESC LIMIT ?")
            .bind(rule_id).bind(tenant_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::anomaly::AnomalyEvent {
                id: r.id,
                rule_id: r.rule_id,
                tenant_id: r.tenant_id,
                state: r.state,
                metric: r.metric,
                value: r.value,
                expected: r.expected,
                deviation: r.deviation,
                message: r.message,
                created_at: r.created_at,
            })
            .collect())
    }

    pub async fn list_all_anomaly_events(
        &self,
        tenant_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::anomaly::AnomalyEventWithRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            rule_id: String,
            tenant_id: String,
            rule_name: String,
            state: String,
            metric: String,
            value: f64,
            expected: f64,
            deviation: f64,
            message: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT e.id, e.rule_id, e.tenant_id, coalesce(r.name, 'deleted rule') AS rule_name, e.state, e.metric, e.value, e.expected, e.deviation, e.message, e.created_at FROM config_anomaly_events e LEFT JOIN (SELECT id, name FROM config_anomaly_rules FINAL WHERE is_deleted = 0) r ON e.rule_id = r.id WHERE e.tenant_id = ? ORDER BY e.created_at DESC LIMIT ?")
            .bind(tenant_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::anomaly::AnomalyEventWithRule {
                id: r.id,
                rule_id: r.rule_id,
                tenant_id: r.tenant_id,
                rule_name: r.rule_name,
                state: r.state,
                metric: r.metric,
                value: r.value,
                expected: r.expected,
                deviation: r.deviation,
                message: r.message,
                created_at: r.created_at,
            })
            .collect())
    }

    // ── Custom skills operations ───────────────────────────────────────────────

    async fn fetch_custom_skill_row(
        &self,
        sql: &str,
        bind_val: Option<&str>,
    ) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            title: String,
            description: String,
            content: String,
            allowed_tools: String,
            enabled: u8,
            created_by: String,
            created_at: String,
            updated_at: String,
        }
        let result = match bind_val {
            Some(v) => self.client.query(sql).bind(v).fetch_one::<Row>().await,
            None => self.client.query(sql).fetch_one::<Row>().await,
        };
        match result {
            Ok(r) => Ok(Some(crate::models::custom_skills::CustomSkill {
                id: r.id,
                name: r.name,
                title: r.title,
                description: r.description,
                content: r.content,
                allowed_tools: serde_json::from_str(&r.allowed_tools).unwrap_or_default(),
                enabled: r.enabled != 0,
                created_by: r.created_by,
                created_at: r.created_at,
                updated_at: r.updated_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list_custom_skills(
        &self,
    ) -> anyhow::Result<Vec<crate::models::custom_skills::CustomSkill>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            title: String,
            description: String,
            content: String,
            allowed_tools: String,
            enabled: u8,
            created_by: String,
            created_at: String,
            updated_at: String,
        }
        let rows = self.client
            .query("SELECT id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at FROM config_custom_skills FINAL WHERE is_deleted = 0 ORDER BY name ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::custom_skills::CustomSkill {
                id: r.id,
                name: r.name,
                title: r.title,
                description: r.description,
                content: r.content,
                allowed_tools: serde_json::from_str(&r.allowed_tools).unwrap_or_default(),
                enabled: r.enabled != 0,
                created_by: r.created_by,
                created_at: r.created_at,
                updated_at: r.updated_at,
            })
            .collect())
    }

    pub async fn get_custom_skill(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        self.fetch_custom_skill_row("SELECT id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at FROM config_custom_skills FINAL WHERE id = ? AND is_deleted = 0 LIMIT 1", Some(id)).await
    }

    pub async fn get_custom_skill_by_name(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        self.fetch_custom_skill_row("SELECT id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at FROM config_custom_skills FINAL WHERE name = ? AND is_deleted = 0 LIMIT 1", Some(name)).await
    }

    pub async fn create_custom_skill(
        &self,
        req: &crate::models::custom_skills::CreateCustomSkillRequest,
        created_by: &str,
    ) -> anyhow::Result<crate::models::custom_skills::CustomSkill> {
        let id = uuid::Uuid::new_v4().to_string();
        let allowed_tools_json = serde_json::to_string(&req.allowed_tools)?;
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_custom_skills (id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(&id).bind(&req.name).bind(&req.title).bind(&req.description).bind(&req.content)
            .bind(&allowed_tools_json).bind(if req.enabled { 1u8 } else { 0u8 })
            .bind(created_by).bind(&now).bind(&now).bind(ver)
            .execute().await?;
        self.get_custom_skill(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("failed to fetch newly created custom skill"))
    }

    pub async fn update_custom_skill(
        &self,
        id: &str,
        req: &crate::models::custom_skills::UpdateCustomSkillRequest,
    ) -> anyhow::Result<Option<crate::models::custom_skills::CustomSkill>> {
        let existing = match self.get_custom_skill(id).await? {
            Some(r) => r,
            None => return Ok(None),
        };
        let allowed_tools_json = serde_json::to_string(&req.allowed_tools)?;
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_custom_skills (id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.name).bind(&req.title).bind(&req.description).bind(&req.content)
            .bind(&allowed_tools_json).bind(if req.enabled { 1u8 } else { 0u8 })
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        self.get_custom_skill(id).await
    }

    pub async fn delete_custom_skill(&self, id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_custom_skill(id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let allowed_tools_json = serde_json::to_string(&existing.allowed_tools)?;
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_custom_skills (id, name, title, description, content, allowed_tools, enabled, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&existing.name).bind(&existing.title).bind(&existing.description).bind(&existing.content)
            .bind(&allowed_tools_json).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    // ── Service link operations ────────────────────────────────────────────────

    pub async fn list_service_links(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<crate::models::service_link::ServiceLink>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            tenant_id: String,
            service_name: String,
            github_repo: String,
            github_installation_id: u64,
            github_repository_id: u64,
            default_branch: String,
            root_path: String,
            updated_at: String,
        }
        let rows = self.client
            .query("SELECT tenant_id, service_name, github_repo, github_installation_id, github_repository_id, default_branch, root_path, updated_at FROM config_service_links_v2 FINAL WHERE tenant_id = ? AND is_deleted = 0 ORDER BY service_name ASC")
            .bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::service_link::ServiceLink {
                tenant_id: r.tenant_id,
                service_name: r.service_name,
                github_repo: r.github_repo,
                github_installation_id: r.github_installation_id,
                github_repository_id: r.github_repository_id,
                default_branch: r.default_branch,
                root_path: r.root_path,
                updated_at: r.updated_at,
            })
            .collect())
    }

    pub async fn get_service_link(
        &self,
        tenant_id: &str,
        service_name: &str,
    ) -> anyhow::Result<Option<crate::models::service_link::ServiceLink>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            tenant_id: String,
            service_name: String,
            github_repo: String,
            github_installation_id: u64,
            github_repository_id: u64,
            default_branch: String,
            root_path: String,
            updated_at: String,
        }
        let result = self.client
            .query("SELECT tenant_id, service_name, github_repo, github_installation_id, github_repository_id, default_branch, root_path, updated_at FROM config_service_links_v2 FINAL WHERE tenant_id = ? AND service_name = ? AND is_deleted = 0 LIMIT 1")
            .bind(tenant_id)
            .bind(service_name)
            .fetch_one::<Row>()
            .await;
        match result {
            Ok(r) => Ok(Some(crate::models::service_link::ServiceLink {
                tenant_id: r.tenant_id,
                service_name: r.service_name,
                github_repo: r.github_repo,
                github_installation_id: r.github_installation_id,
                github_repository_id: r.github_repository_id,
                default_branch: r.default_branch,
                root_path: r.root_path,
                updated_at: r.updated_at,
            })),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn upsert_service_link(
        &self,
        tenant_id: &str,
        service_name: &str,
        github_repo: &str,
        github_installation_id: u64,
        github_repository_id: u64,
        default_branch: &str,
        root_path: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_service_links_v2 (tenant_id, service_name, github_repo, github_installation_id, github_repository_id, default_branch, root_path, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(tenant_id).bind(service_name).bind(github_repo).bind(github_installation_id).bind(github_repository_id).bind(default_branch).bind(root_path).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn delete_service_link(
        &self,
        tenant_id: &str,
        service_name: &str,
    ) -> anyhow::Result<bool> {
        let existing = match self.get_service_link(tenant_id, service_name).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_service_links_v2 (tenant_id, service_name, github_repo, github_installation_id, github_repository_id, default_branch, root_path, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(tenant_id).bind(service_name).bind(&existing.github_repo).bind(existing.github_installation_id).bind(existing.github_repository_id).bind(&existing.default_branch).bind(&existing.root_path).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    // ── Monitor operations ─────────────────────────────────────────────────────

    async fn fetch_monitors(
        &self,
        sql: &str,
        bind_vals: &[&str],
    ) -> anyhow::Result<Vec<crate::models::monitor::Monitor>> {
        let mut q = self.client.query(sql);
        for v in bind_vals {
            q = q.bind(*v);
        }
        let rows = q.fetch_all::<MonitorRow>().await?;
        Ok(rows.into_iter().map(Self::map_monitor_row).collect())
    }

    fn map_monitor_row(r: MonitorRow) -> crate::models::monitor::Monitor {
        crate::models::monitor::Monitor {
            id: r.id,
            tenant_id: r.tenant_id,
            name: r.name,
            monitor_type: r.monitor_type,
            query_config: r.query_config,
            critical: r.critical,
            critical_recovery: r.critical_recovery,
            warning: r.warning,
            warning_recovery: r.warning_recovery,
            comparator: r.comparator,
            eval_window_secs: r.eval_window_secs,
            eval_interval_secs: r.eval_interval_secs,
            group_by: r.group_by,
            state: r.state,
            group_states: r.group_states,
            no_data_action: r.no_data_action,
            no_data_timeframe: r.no_data_timeframe,
            auto_resolve_hours: r.auto_resolve_hours,
            message: r.message,
            notification_channels: r.notification_channels,
            renotify_interval: r.renotify_interval,
            tags: r.tags,
            priority: r.priority,
            enabled: r.enabled != 0,
            composite_formula: r.composite_formula,
            composite_monitor_ids: r.composite_monitor_ids,
            last_eval_at: if r.last_eval_at.is_empty() {
                None
            } else {
                Some(r.last_eval_at)
            },
            last_triggered_at: if r.last_triggered_at.is_empty() {
                None
            } else {
                Some(r.last_triggered_at)
            },
            created_by: r.created_by,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }

    const MONITOR_SELECT: &'static str = "SELECT id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at FROM config_monitors FINAL WHERE is_deleted = 0";

    pub async fn list_monitors(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<crate::models::monitor::Monitor>> {
        self.fetch_monitors(
            &format!(
                "{} AND tenant_id = ? ORDER BY created_at DESC",
                Self::MONITOR_SELECT
            ),
            &[tenant_id],
        )
        .await
    }

    pub async fn get_monitor(
        &self,
        id: &str,
        tenant_id: &str,
    ) -> anyhow::Result<Option<crate::models::monitor::Monitor>> {
        let q = self
            .client
            .query(&format!(
                "{} AND id = ? AND tenant_id = ? LIMIT 1",
                Self::MONITOR_SELECT
            ))
            .bind(id)
            .bind(tenant_id);
        let result = q.fetch_one::<MonitorRow>().await;
        match result {
            Ok(r) => Ok(Some(Self::map_monitor_row(r))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_monitor_by_id(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::monitor::Monitor>> {
        let result = self
            .client
            .query(&format!("{} AND id = ? LIMIT 1", Self::MONITOR_SELECT))
            .bind(id)
            .fetch_one::<MonitorRow>()
            .await;
        match result {
            Ok(r) => Ok(Some(Self::map_monitor_row(r))),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_monitor(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        monitor_type: &str,
        query_config: &str,
        critical: Option<f64>,
        critical_recovery: Option<f64>,
        warning: Option<f64>,
        warning_recovery: Option<f64>,
        comparator: &str,
        eval_window_secs: i64,
        eval_interval_secs: i64,
        group_by: &str,
        no_data_action: &str,
        no_data_timeframe: i64,
        auto_resolve_hours: Option<i64>,
        message: &str,
        notification_channels: &str,
        renotify_interval: Option<i64>,
        tags: &str,
        priority: Option<i64>,
        enabled: bool,
        composite_formula: &str,
        composite_monitor_ids: &str,
        created_by: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ok', '{}', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(monitor_type).bind(query_config)
            .bind(critical).bind(critical_recovery).bind(warning).bind(warning_recovery)
            .bind(comparator).bind(eval_window_secs).bind(eval_interval_secs).bind(group_by)
            .bind(no_data_action).bind(no_data_timeframe).bind(auto_resolve_hours)
            .bind(message).bind(notification_channels).bind(renotify_interval)
            .bind(tags).bind(priority).bind(if enabled { 1u8 } else { 0u8 })
            .bind(composite_formula).bind(composite_monitor_ids)
            .bind(created_by).bind(&now).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_monitor(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        monitor_type: &str,
        query_config: &str,
        critical: Option<f64>,
        critical_recovery: Option<f64>,
        warning: Option<f64>,
        warning_recovery: Option<f64>,
        comparator: &str,
        eval_window_secs: i64,
        eval_interval_secs: i64,
        group_by: &str,
        no_data_action: &str,
        no_data_timeframe: i64,
        auto_resolve_hours: Option<i64>,
        message: &str,
        notification_channels: &str,
        renotify_interval: Option<i64>,
        tags: &str,
        priority: Option<i64>,
        enabled: bool,
        composite_formula: &str,
        composite_monitor_ids: &str,
    ) -> anyhow::Result<bool> {
        let existing = match self.get_monitor(id, tenant_id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(monitor_type).bind(query_config)
            .bind(critical).bind(critical_recovery).bind(warning).bind(warning_recovery)
            .bind(comparator).bind(eval_window_secs).bind(eval_interval_secs).bind(group_by)
            .bind(&existing.state).bind(&existing.group_states)
            .bind(no_data_action).bind(no_data_timeframe).bind(auto_resolve_hours)
            .bind(message).bind(notification_channels).bind(renotify_interval)
            .bind(tags).bind(priority).bind(if enabled { 1u8 } else { 0u8 })
            .bind(composite_formula).bind(composite_monitor_ids)
            .bind(existing.last_eval_at.unwrap_or_default())
            .bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_monitor(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_monitor(id, tenant_id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(tenant_id).bind(&existing.name).bind(&existing.monitor_type).bind(&existing.query_config)
            .bind(existing.critical).bind(existing.critical_recovery).bind(existing.warning).bind(existing.warning_recovery)
            .bind(&existing.comparator).bind(existing.eval_window_secs).bind(existing.eval_interval_secs).bind(&existing.group_by)
            .bind(&existing.state).bind(&existing.group_states)
            .bind(&existing.no_data_action).bind(existing.no_data_timeframe).bind(existing.auto_resolve_hours)
            .bind(&existing.message).bind(&existing.notification_channels).bind(existing.renotify_interval)
            .bind(&existing.tags).bind(existing.priority).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.composite_formula).bind(&existing.composite_monitor_ids)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn list_enabled_monitors(
        &self,
    ) -> anyhow::Result<Vec<crate::models::monitor::Monitor>> {
        self.fetch_monitors(&format!("{} AND enabled = 1", Self::MONITOR_SELECT), &[])
            .await
    }

    pub async fn update_monitor_state(
        &self,
        id: &str,
        state: &str,
        group_states: &str,
        last_eval_at: &str,
    ) -> anyhow::Result<()> {
        let existing = match self.get_monitor_by_id(id).await? {
            Some(r) => r,
            None => return Ok(()),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.monitor_type).bind(&existing.query_config)
            .bind(existing.critical).bind(existing.critical_recovery).bind(existing.warning).bind(existing.warning_recovery)
            .bind(&existing.comparator).bind(existing.eval_window_secs).bind(existing.eval_interval_secs).bind(&existing.group_by)
            .bind(state).bind(group_states)
            .bind(&existing.no_data_action).bind(existing.no_data_timeframe).bind(existing.auto_resolve_hours)
            .bind(&existing.message).bind(&existing.notification_channels).bind(existing.renotify_interval)
            .bind(&existing.tags).bind(existing.priority).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.composite_formula).bind(&existing.composite_monitor_ids)
            .bind(last_eval_at).bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    /// Narrow `last_eval_at` flush for the monitor engine: re-inserts the monitor row
    /// the engine already fetched this tick (state/group_states unchanged), avoiding
    /// the SELECT…FINAL read-modify-write of `update_monitor_state`. Only call when no
    /// state transition occurred — transitions must go through `update_monitor_state`.
    pub async fn persist_monitor_eval(
        &self,
        monitor: &crate::models::monitor::Monitor,
        last_eval_at: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(&monitor.id).bind(&monitor.tenant_id).bind(&monitor.name).bind(&monitor.monitor_type).bind(&monitor.query_config)
            .bind(monitor.critical).bind(monitor.critical_recovery).bind(monitor.warning).bind(monitor.warning_recovery)
            .bind(&monitor.comparator).bind(monitor.eval_window_secs).bind(monitor.eval_interval_secs).bind(&monitor.group_by)
            .bind(&monitor.state).bind(&monitor.group_states)
            .bind(&monitor.no_data_action).bind(monitor.no_data_timeframe).bind(monitor.auto_resolve_hours)
            .bind(&monitor.message).bind(&monitor.notification_channels).bind(monitor.renotify_interval)
            .bind(&monitor.tags).bind(monitor.priority).bind(if monitor.enabled { 1u8 } else { 0u8 })
            .bind(&monitor.composite_formula).bind(&monitor.composite_monitor_ids)
            .bind(last_eval_at).bind(monitor.last_triggered_at.clone().unwrap_or_default())
            .bind(&monitor.created_by).bind(&monitor.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn update_monitor_triggered(
        &self,
        id: &str,
        last_triggered_at: &str,
    ) -> anyhow::Result<()> {
        let existing = match self.get_monitor_by_id(id).await? {
            Some(r) => r,
            None => return Ok(()),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.monitor_type).bind(&existing.query_config)
            .bind(existing.critical).bind(existing.critical_recovery).bind(existing.warning).bind(existing.warning_recovery)
            .bind(&existing.comparator).bind(existing.eval_window_secs).bind(existing.eval_interval_secs).bind(&existing.group_by)
            .bind(&existing.state).bind(&existing.group_states)
            .bind(&existing.no_data_action).bind(existing.no_data_timeframe).bind(existing.auto_resolve_hours)
            .bind(&existing.message).bind(&existing.notification_channels).bind(existing.renotify_interval)
            .bind(&existing.tags).bind(existing.priority).bind(if existing.enabled { 1u8 } else { 0u8 })
            .bind(&existing.composite_formula).bind(&existing.composite_monitor_ids)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(last_triggered_at)
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn create_monitor_event(
        &self,
        id: &str,
        monitor_id: &str,
        tenant_id: &str,
        group_key: &str,
        prev_state: &str,
        new_state: &str,
        value: Option<f64>,
        threshold: Option<f64>,
        message: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_monitor_events (id, monitor_id, tenant_id, group_key, prev_state, new_state, value, threshold, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(monitor_id).bind(tenant_id).bind(group_key).bind(prev_state).bind(new_state).bind(value).bind(threshold).bind(message).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_monitor_events(
        &self,
        monitor_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::monitor::MonitorEvent>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            monitor_id: String,
            tenant_id: String,
            group_key: String,
            prev_state: String,
            new_state: String,
            value: Option<f64>,
            threshold: Option<f64>,
            message: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, monitor_id, tenant_id, group_key, prev_state, new_state, value, threshold, message, created_at FROM config_monitor_events WHERE monitor_id = ? ORDER BY created_at DESC LIMIT ?")
            .bind(monitor_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::monitor::MonitorEvent {
                id: r.id,
                monitor_id: r.monitor_id,
                tenant_id: r.tenant_id,
                group_key: r.group_key,
                prev_state: r.prev_state,
                new_state: r.new_state,
                value: r.value,
                threshold: r.threshold,
                message: r.message,
                created_at: r.created_at,
            })
            .collect())
    }

    pub async fn count_monitors(&self, tenant_id: &str) -> anyhow::Result<i64> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count {
            n: u64,
        }
        let row = self.client
            .query("SELECT count() AS n FROM config_monitors FINAL WHERE tenant_id = ? AND is_deleted = 0")
            .bind(tenant_id)
            .fetch_one::<Count>()
            .await?;
        Ok(row.n as i64)
    }

    pub async fn set_monitor_enabled(
        &self,
        id: &str,
        tenant_id: &str,
        enabled: bool,
    ) -> anyhow::Result<bool> {
        let existing = match self.get_monitor(id, tenant_id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_monitors (id, tenant_id, name, monitor_type, query_config, critical, critical_recovery, warning, warning_recovery, comparator, eval_window_secs, eval_interval_secs, group_by, state, group_states, no_data_action, no_data_timeframe, auto_resolve_hours, message, notification_channels, renotify_interval, tags, priority, enabled, composite_formula, composite_monitor_ids, last_eval_at, last_triggered_at, created_by, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(&existing.name).bind(&existing.monitor_type).bind(&existing.query_config)
            .bind(existing.critical).bind(existing.critical_recovery).bind(existing.warning).bind(existing.warning_recovery)
            .bind(&existing.comparator).bind(existing.eval_window_secs).bind(existing.eval_interval_secs).bind(&existing.group_by)
            .bind(&existing.state).bind(&existing.group_states)
            .bind(&existing.no_data_action).bind(existing.no_data_timeframe).bind(existing.auto_resolve_hours)
            .bind(&existing.message).bind(&existing.notification_channels).bind(existing.renotify_interval)
            .bind(&existing.tags).bind(existing.priority).bind(if enabled { 1u8 } else { 0u8 })
            .bind(&existing.composite_formula).bind(&existing.composite_monitor_ids)
            .bind(existing.last_eval_at.unwrap_or_default()).bind(existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_by).bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    // ── SIEM Detection Rule operations ──

    pub async fn list_detection_rules(
        &self,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<crate::models::detection::DetectionRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            tenant_id: String,
            name: String,
            description: String,
            query_sql: String,
            interval_secs: i64,
            threshold: i64,
            severity: String,
            window_secs: i64,
            enabled: u8,
            channels: String,
            created_by: String,
            last_eval_at: String,
            last_triggered_at: String,
            created_at: String,
            updated_at: String,
        }
        let map_row = |r: Row| crate::models::detection::DetectionRule {
            id: r.id,
            tenant_id: r.tenant_id,
            name: r.name,
            description: r.description,
            query_sql: r.query_sql,
            interval_secs: r.interval_secs,
            threshold: r.threshold,
            severity: r.severity,
            window_secs: r.window_secs,
            enabled: r.enabled != 0,
            channels: r.channels,
            created_by: r.created_by,
            last_eval_at: if r.last_eval_at.is_empty() {
                None
            } else {
                Some(r.last_eval_at)
            },
            last_triggered_at: if r.last_triggered_at.is_empty() {
                None
            } else {
                Some(r.last_triggered_at)
            },
            created_at: r.created_at,
            updated_at: r.updated_at,
        };
        let rows = if let Some(tid) = tenant_id {
            self.client
                .query("SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at FROM config_detection_rules FINAL WHERE tenant_id = ? AND is_deleted = 0 ORDER BY created_at DESC")
                .bind(tid)
                .fetch_all::<Row>()
                .await?
        } else {
            self.client
                .query("SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at FROM config_detection_rules FINAL WHERE is_deleted = 0 ORDER BY created_at DESC")
                .fetch_all::<Row>()
                .await?
        };
        Ok(rows.into_iter().map(map_row).collect())
    }

    pub async fn get_detection_rule(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::detection::DetectionRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            tenant_id: String,
            name: String,
            description: String,
            query_sql: String,
            interval_secs: i64,
            threshold: i64,
            severity: String,
            window_secs: i64,
            enabled: u8,
            channels: String,
            created_by: String,
            last_eval_at: String,
            last_triggered_at: String,
            created_at: String,
            updated_at: String,
        }
        let rows = self.client
            .query("SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at FROM config_detection_rules FINAL WHERE id = ? AND is_deleted = 0")
            .bind(id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .next()
            .map(|r| crate::models::detection::DetectionRule {
                id: r.id,
                tenant_id: r.tenant_id,
                name: r.name,
                description: r.description,
                query_sql: r.query_sql,
                interval_secs: r.interval_secs,
                threshold: r.threshold,
                severity: r.severity,
                window_secs: r.window_secs,
                enabled: r.enabled != 0,
                channels: r.channels,
                created_by: r.created_by,
                last_eval_at: if r.last_eval_at.is_empty() {
                    None
                } else {
                    Some(r.last_eval_at)
                },
                last_triggered_at: if r.last_triggered_at.is_empty() {
                    None
                } else {
                    Some(r.last_triggered_at)
                },
                created_at: r.created_at,
                updated_at: r.updated_at,
            }))
    }

    pub async fn create_detection_rule(
        &self,
        id: &str,
        tenant_id: &str,
        name: &str,
        description: &str,
        query_sql: &str,
        interval_secs: i64,
        threshold: i64,
        severity: &str,
        window_secs: i64,
        enabled: bool,
        channels: &str,
        created_by: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_detection_rules (id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, 0)")
            .bind(id).bind(tenant_id).bind(name).bind(description).bind(query_sql)
            .bind(interval_secs).bind(threshold).bind(severity).bind(window_secs)
            .bind(if enabled { 1u8 } else { 0u8 }).bind(channels).bind(created_by)
            .bind(&now).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn update_detection_rule(
        &self,
        id: &str,
        name: &str,
        description: &str,
        query_sql: &str,
        interval_secs: i64,
        threshold: i64,
        severity: &str,
        window_secs: i64,
        enabled: bool,
        channels: &str,
    ) -> anyhow::Result<bool> {
        let existing = match self.get_detection_rule(id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_detection_rules (id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(name).bind(description).bind(query_sql)
            .bind(interval_secs).bind(threshold).bind(severity).bind(window_secs)
            .bind(if enabled { 1u8 } else { 0u8 }).bind(channels).bind(&existing.created_by)
            .bind(&existing.last_eval_at.unwrap_or_default())
            .bind(&existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn delete_detection_rule(&self, id: &str) -> anyhow::Result<bool> {
        let existing = match self.get_detection_rule(id).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        self.client
            .query("INSERT INTO config_detection_rules (id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.description)
            .bind(&existing.query_sql).bind(existing.interval_secs).bind(existing.threshold)
            .bind(&existing.severity).bind(existing.window_secs)
            .bind(if existing.enabled { 1u8 } else { 0u8 }).bind(&existing.channels)
            .bind(&existing.created_by)
            .bind(&existing.last_eval_at.unwrap_or_default())
            .bind(&existing.last_triggered_at.unwrap_or_default())
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(true)
    }

    pub async fn list_enabled_detection_rules(
        &self,
    ) -> anyhow::Result<Vec<crate::models::detection::DetectionRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            tenant_id: String,
            name: String,
            description: String,
            query_sql: String,
            interval_secs: i64,
            threshold: i64,
            severity: String,
            window_secs: i64,
            enabled: u8,
            channels: String,
            created_by: String,
            last_eval_at: String,
            last_triggered_at: String,
            created_at: String,
            updated_at: String,
        }
        let rows = self.client
            .query("SELECT id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at FROM config_detection_rules FINAL WHERE enabled = 1 AND is_deleted = 0 ORDER BY created_at ASC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| crate::models::detection::DetectionRule {
                id: r.id,
                tenant_id: r.tenant_id,
                name: r.name,
                description: r.description,
                query_sql: r.query_sql,
                interval_secs: r.interval_secs,
                threshold: r.threshold,
                severity: r.severity,
                window_secs: r.window_secs,
                enabled: r.enabled != 0,
                channels: r.channels,
                created_by: r.created_by,
                last_eval_at: if r.last_eval_at.is_empty() {
                    None
                } else {
                    Some(r.last_eval_at)
                },
                last_triggered_at: if r.last_triggered_at.is_empty() {
                    None
                } else {
                    Some(r.last_triggered_at)
                },
                created_at: r.created_at,
                updated_at: r.updated_at,
            })
            .collect())
    }

    pub async fn update_detection_rule_eval(
        &self,
        id: &str,
        last_eval_at: &str,
        last_triggered_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let existing = match self.get_detection_rule(id).await? {
            Some(r) => r,
            None => return Ok(()),
        };
        let now = Self::now_str();
        let ver = Self::next_version();
        let triggered = last_triggered_at
            .unwrap_or_else(|| existing.last_triggered_at.as_deref().unwrap_or(""));
        self.client
            .query("INSERT INTO config_detection_rules (id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id).bind(&existing.tenant_id).bind(&existing.name).bind(&existing.description)
            .bind(&existing.query_sql).bind(existing.interval_secs).bind(existing.threshold)
            .bind(&existing.severity).bind(existing.window_secs)
            .bind(if existing.enabled { 1u8 } else { 0u8 }).bind(&existing.channels)
            .bind(&existing.created_by).bind(last_eval_at).bind(triggered)
            .bind(&existing.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    /// Narrow eval persist for the SIEM engine: re-inserts the detection rule row the
    /// engine already fetched this tick, avoiding the SELECT…FINAL read-modify-write
    /// of `update_detection_rule_eval`. Used both for the coarse `last_eval_at` flush
    /// and on fire (with `last_triggered_at = Some(now)`).
    pub async fn persist_detection_rule_eval(
        &self,
        rule: &crate::models::detection::DetectionRule,
        last_eval_at: &str,
        last_triggered_at: Option<&str>,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        let ver = Self::next_version();
        let triggered =
            last_triggered_at.unwrap_or_else(|| rule.last_triggered_at.as_deref().unwrap_or(""));
        self.client
            .query("INSERT INTO config_detection_rules (id, tenant_id, name, description, query_sql, interval_secs, threshold, severity, window_secs, enabled, channels, created_by, last_eval_at, last_triggered_at, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(&rule.id).bind(&rule.tenant_id).bind(&rule.name).bind(&rule.description)
            .bind(&rule.query_sql).bind(rule.interval_secs).bind(rule.threshold)
            .bind(&rule.severity).bind(rule.window_secs)
            .bind(if rule.enabled { 1u8 } else { 0u8 }).bind(&rule.channels)
            .bind(&rule.created_by).bind(last_eval_at).bind(triggered)
            .bind(&rule.created_at).bind(&now).bind(ver)
            .execute().await?;
        Ok(())
    }

    pub async fn count_detection_rules(&self) -> anyhow::Result<i64> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count {
            n: u64,
        }
        let row = self
            .client
            .query("SELECT count() AS n FROM config_detection_rules FINAL WHERE is_deleted = 0")
            .fetch_one::<Count>()
            .await?;
        Ok(row.n as i64)
    }

    /// Fetch a built-in (`system`) default detection rule by name: its id and
    /// current query_sql. Returns None when the rule isn't present. Used by the
    /// seeder to decide whether to create, refresh, or skip a built-in rule.
    async fn get_default_detection_rule(
        &self,
        name: &str,
        tenant_id: &str,
    ) -> anyhow::Result<Option<(String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            query_sql: String,
        }
        let rows = self.client
            .query("SELECT id, query_sql FROM config_detection_rules FINAL WHERE name = ? AND tenant_id = ? AND created_by = 'system' AND is_deleted = 0 LIMIT 1")
            .bind(name).bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows.into_iter().next().map(|r| (r.id, r.query_sql)))
    }

    pub async fn ensure_default_detection_rules(&self) -> anyhow::Result<()> {
        tracing::info!("SIEM: checking default detection rules");

        // (name, description, query_sql, severity, interval_secs, window_secs)
        let defaults: Vec<(&str, &str, &str, &str, i64, i64)> = vec![
            (
                "Failed login brute force",
                "Detects IPs with 10+ failed login attempts within the detection window.",
                "SELECT mat_source_ip, count() AS attempt_count \
                 FROM logs \
                 WHERE Timestamp BETWEEN @window_start AND @window_end \
                   AND mat_action = 'login_failed' \
                 GROUP BY mat_source_ip \
                 HAVING attempt_count >= 10",
                "high",
                300,
                300,
            ),
            (
                "Error rate spike per service",
                "Fires when any service has an error rate above 5% with at least 100 spans.",
                "SELECT service_name, \
                   countIf(status = 'ERROR') AS errors, \
                   count() AS total, \
                   errors / total AS error_rate \
                 FROM spans \
                 WHERE timestamp BETWEEN @window_start AND @window_end \
                 GROUP BY service_name \
                 HAVING error_rate > 0.05 AND total > 100",
                "high",
                300,
                300,
            ),
            (
                "P99 latency regression",
                "Detects server spans where p99 latency exceeds 500ms with sufficient traffic.",
                "SELECT service_name, span_name, \
                   quantile(0.99)(duration_ns) / 1000000 AS p99_ms, \
                   count() AS total \
                 FROM spans \
                 WHERE timestamp BETWEEN @window_start AND @window_end \
                   AND kind = 'SPAN_KIND_SERVER' \
                 GROUP BY service_name, span_name \
                 HAVING p99_ms > 500 AND total > 50",
                "high",
                300,
                300,
            ),
            (
                "CPU saturation",
                "Alerts when average CPU utilization exceeds 90% for any host.",
                "SELECT ServiceName, \
                   Attributes['host.name'] AS host, \
                   avg(Value) AS avg_cpu \
                 FROM metrics_gauge \
                 WHERE TimeUnix BETWEEN @window_start AND @window_end \
                   AND MetricName = 'system.cpu.utilization' \
                 GROUP BY ServiceName, host \
                 HAVING avg_cpu > 0.9",
                "critical",
                300,
                300,
            ),
            (
                "Request rate drop",
                "Detects services whose request volume falls below 50 requests in the current window.",
                "SELECT ServiceName, sum(Value) AS current_requests \
                 FROM metrics_sum \
                 WHERE TimeUnix BETWEEN @window_start AND @window_end \
                   AND MetricName = 'http.server.request.count' \
                 GROUP BY ServiceName \
                 HAVING current_requests < 50",
                "high",
                300,
                300,
            ),
            (
                "Error + latency correlation",
                "Detects services with both elevated error rates and high p99 latency.",
                "SELECT service_name, \
                   countIf(status = 'ERROR') AS errors, \
                   count() AS total, \
                   quantile(0.99)(duration_ns) / 1000000 AS p99_ms \
                 FROM spans \
                 WHERE timestamp BETWEEN @window_start AND @window_end \
                   AND kind = 'SPAN_KIND_SERVER' \
                 GROUP BY service_name \
                 HAVING errors / total > 0.05 AND p99_ms > 500 AND total > 50",
                "critical",
                300,
                300,
            ),
            (
                "High severity log volume",
                "Fires when ERROR/FATAL log volume exceeds 100 entries in the window.",
                "SELECT ServiceName, SeverityText, count() AS log_count \
                 FROM logs \
                 WHERE Timestamp BETWEEN @window_start AND @window_end \
                   AND SeverityText IN ('ERROR', 'FATAL') \
                 GROUP BY ServiceName, SeverityText \
                 HAVING log_count >= 100",
                "medium",
                300,
                300,
            ),
            (
                "Log errors + trace failures correlation",
                "Detects services with 5+ ERROR/FATAL logs carrying trace context, providing a safe single-signal correlation point.",
                "SELECT ServiceName, count() AS correlated_errors \
                 FROM logs \
                 WHERE Timestamp BETWEEN @window_start AND @window_end \
                   AND SeverityText IN ('ERROR', 'FATAL') \
                   AND TraceId != '' \
                 GROUP BY ServiceName \
                 HAVING correlated_errors >= 5",
                "critical",
                300,
                300,
            ),
            (
                "Latency spike + memory pressure",
                "Detects services with sustained memory utilization above 85%; investigate alongside latency telemetry.",
                "SELECT ServiceName, max(Value) AS max_memory \
                 FROM metrics_gauge \
                 WHERE TimeUnix BETWEEN @window_start AND @window_end \
                   AND MetricName IN ('process.runtime.jvm.memory.usage', \
                                      'container.memory.usage', \
                                      'process.memory.usage') \
                 GROUP BY ServiceName \
                 HAVING max_memory > 0.85",
                "high",
                300,
                300,
            ),
            (
                "Post-deploy error rate increase",
                "Detects services that report a deploy span and an elevated 5xx rate in the same evaluation window.",
                "SELECT service_name, \
                   countIf(span_name LIKE '%deploy%') AS deploy_spans, \
                   countIf(http_status_code >= 500) AS errors, \
                   count() AS total \
                 FROM spans \
                 WHERE timestamp BETWEEN @window_start AND @window_end \
                 GROUP BY service_name \
                 HAVING deploy_spans > 0 AND total > 20 AND errors / total > 0.05",
                "high",
                300,
                600,
            ),
            (
                "New error patterns (unseen in past 7 days)",
                "Identifies repeated ERROR/FATAL message patterns appearing 3+ times in the current window.",
                "SELECT ServiceName, Body, count() AS occurrences \
                 FROM logs \
                 WHERE Timestamp BETWEEN @window_start AND @window_end \
                   AND SeverityText IN ('ERROR', 'FATAL') \
                 GROUP BY ServiceName, Body \
                 HAVING occurrences >= 3",
                "medium",
                300,
                300,
            ),
            (
                "Cascading service failures (3+ services)",
                "Detects errors across 3 or more distinct services in the same window, which can indicate a shared dependency incident.",
                "SELECT uniqIf(service_name, status = 'ERROR' OR http_status_code >= 500) AS failing_services \
                 FROM spans \
                 WHERE timestamp BETWEEN @window_start AND @window_end \
                 HAVING failing_services >= 3",
                "critical",
                300,
                300,
            ),
        ];

        let mut seeded = 0u32;
        let mut refreshed = 0u32;
        for (name, description, query_sql, severity, interval, window) in &defaults {
            crate::detection_query::validate_template(query_sql).map_err(|error| {
                anyhow::anyhow!("invalid built-in detection rule '{name}': {error}")
            })?;
            match self.get_default_detection_rule(name, "default").await? {
                Some((id, existing_sql)) => {
                    // Built-in rule already present. Refresh it in place if its
                    // definition drifted from the current canonical SQL — older
                    // seeds referenced since-renamed tables (otel_traces → spans,
                    // otel_logs → logs, otel_metrics_gauge → metrics_gauge,
                    // wide_events → spans) and a malformed any(subquery), which
                    // made every eval fail. This makes the built-ins self-heal on
                    // upgrade without clobbering user-edited rules (created_by != system).
                    if existing_sql.trim() != query_sql.trim() {
                        self.update_detection_rule(
                            &id,
                            name,
                            description,
                            query_sql,
                            *interval,
                            1,
                            severity,
                            *window,
                            true,
                            "[]",
                        )
                        .await?;
                        refreshed += 1;
                    }
                }
                None => {
                    let id = uuid::Uuid::new_v4().to_string();
                    self.create_detection_rule(
                        &id,
                        "default",
                        name,
                        description,
                        query_sql,
                        *interval,
                        1,
                        severity,
                        *window,
                        true,
                        "[]",
                        "system",
                    )
                    .await?;
                    seeded += 1;
                }
            }
        }

        if seeded > 0 || refreshed > 0 {
            tracing::info!(
                "SIEM: seeded {seeded} new + refreshed {refreshed} stale built-in detection rules ({} total built-in)",
                defaults.len()
            );
        } else {
            tracing::debug!(
                "SIEM: all {} default detection rules already up to date",
                defaults.len()
            );
        }
        Ok(())
    }

    // ── SIEM Detection Event operations ──

    pub async fn create_detection_event(
        &self,
        id: &str,
        rule_id: &str,
        tenant_id: &str,
        severity: &str,
        match_count: i64,
        sample_data: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_detection_events (id, rule_id, tenant_id, severity, match_count, sample_data, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(rule_id).bind(tenant_id).bind(severity).bind(match_count).bind(sample_data).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_detection_events(
        &self,
        tenant_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<crate::models::detection::DetectionEventWithRule>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            rule_id: String,
            rule_name: String,
            tenant_id: String,
            severity: String,
            match_count: i64,
            sample_data: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT e.id, e.rule_id, coalesce(r.name, 'deleted rule') AS rule_name, e.tenant_id, e.severity, e.match_count, e.sample_data, e.created_at FROM config_detection_events e LEFT JOIN (SELECT id, name FROM config_detection_rules FINAL WHERE is_deleted = 0) r ON e.rule_id = r.id WHERE e.tenant_id = ? ORDER BY e.created_at DESC LIMIT ?")
            .bind(tenant_id).bind(limit as u64)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let sample_data_json: serde_json::Value =
                    serde_json::from_str(&r.sample_data).unwrap_or(serde_json::json!([]));
                crate::models::detection::DetectionEventWithRule {
                    id: r.id,
                    rule_id: r.rule_id,
                    rule_name: r.rule_name,
                    tenant_id: r.tenant_id,
                    severity: r.severity,
                    match_count: r.match_count,
                    sample_data: sample_data_json,
                    created_at: r.created_at,
                }
            })
            .collect())
    }

    // ── Alert Maintenance Windows ──────────────────────────────────────────────

    pub async fn create_maintenance_window(
        &self,
        id: &str,
        name: &str,
        scope: &str,
        starts_at: &str,
        ends_at: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_maintenance_windows (id, name, scope, starts_at, ends_at, created_at) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(id).bind(name).bind(scope).bind(starts_at).bind(ends_at).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_maintenance_windows(
        &self,
    ) -> anyhow::Result<Vec<(String, String, String, String, String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            scope: String,
            starts_at: String,
            ends_at: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, name, scope, starts_at, ends_at, created_at FROM config_maintenance_windows ORDER BY starts_at DESC")
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.id, r.name, r.scope, r.starts_at, r.ends_at, r.created_at))
            .collect())
    }

    pub async fn delete_maintenance_window(&self, id: &str) -> anyhow::Result<bool> {
        self.client
            .query("ALTER TABLE config_maintenance_windows DELETE WHERE id = ?")
            .bind(id)
            .execute()
            .await?;
        Ok(true)
    }

    /// Returns true if `now_str` (ISO 8601) falls within any active maintenance window
    /// that covers this alert_id (or all alerts if scope = 'all').
    pub async fn is_in_maintenance(&self, now_str: &str, alert_id: Option<&str>) -> bool {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Count {
            n: u64,
        }
        let alert_scope = alert_id.map(|id| format!("alert:{id}")).unwrap_or_default();
        let result = self.client
            .query("SELECT count() AS n FROM config_maintenance_windows WHERE starts_at <= ? AND ends_at >= ? AND (scope = 'all' OR scope = ?)")
            .bind(now_str).bind(now_str).bind(&alert_scope)
            .fetch_one::<Count>()
            .await;
        result.map(|r| r.n > 0).unwrap_or(false)
    }

    // ── Trace Funnels ──────────────────────────────────────────────────────────

    pub async fn create_funnel(
        &self,
        id: &str,
        name: &str,
        steps_json: &str,
        tenant_id: &str,
    ) -> anyhow::Result<()> {
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_trace_funnels (id, name, steps_json, tenant_id, created_at) VALUES (?, ?, ?, ?, ?)")
            .bind(id).bind(name).bind(steps_json).bind(tenant_id).bind(&now)
            .execute().await?;
        Ok(())
    }

    pub async fn list_funnels(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<(String, String, String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            steps_json: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, name, steps_json, created_at FROM config_trace_funnels WHERE tenant_id = ? ORDER BY created_at DESC")
            .bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.id, r.name, r.steps_json, r.created_at))
            .collect())
    }

    pub async fn get_funnel(
        &self,
        id: &str,
        tenant_id: &str,
    ) -> anyhow::Result<Option<(String, String, String, String)>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            id: String,
            name: String,
            steps_json: String,
            created_at: String,
        }
        let rows = self.client
            .query("SELECT id, name, steps_json, created_at FROM config_trace_funnels WHERE id = ? AND tenant_id = ?")
            .bind(id).bind(tenant_id)
            .fetch_all::<Row>()
            .await?;
        Ok(rows
            .into_iter()
            .next()
            .map(|r| (r.id, r.name, r.steps_json, r.created_at)))
    }

    pub async fn delete_funnel(&self, id: &str, tenant_id: &str) -> anyhow::Result<bool> {
        self.client
            .query("ALTER TABLE config_trace_funnels DELETE WHERE id = ? AND tenant_id = ?")
            .bind(id)
            .bind(tenant_id)
            .execute()
            .await?;
        Ok(true)
    }

    // ── Kubernetes access recording ───────────────────────────────────────────────

    async fn insert_kubernetes_login_request(
        &self,
        request: &KubernetesLoginRequest,
    ) -> anyhow::Result<()> {
        self.client
            .query("INSERT INTO config_kubernetes_login_requests (device_code_hash, user_code, cluster_id, state, tenant_id, user_id, username, role, client_reported, created_at, expires_at, approved_at, credential_expires_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&request.device_code_hash)
            .bind(&request.user_code)
            .bind(&request.cluster_id)
            .bind(&request.state)
            .bind(&request.tenant_id)
            .bind(&request.user_id)
            .bind(&request.username)
            .bind(&request.role)
            .bind(&request.client_reported)
            .bind(&request.created_at)
            .bind(&request.expires_at)
            .bind(&request.approved_at)
            .bind(&request.credential_expires_at)
            .bind(request.version)
            .bind(request.is_deleted)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn list_kubernetes_rbac_grants(
        &self,
        tenant_id: &str,
        cluster_id: Option<&str>,
    ) -> anyhow::Result<Vec<KubernetesRbacGrantRow>> {
        let rows = self
            .client
            .query(&format!(
                "SELECT {KUBERNETES_RBAC_GRANT_COLUMNS} FROM config_kubernetes_rbac_grants FINAL WHERE tenant_id = ? AND is_deleted = 0 ORDER BY cluster_match, cluster_id, cluster_pattern, name, id"
            ))
            .bind(tenant_id)
            .fetch_all::<KubernetesRbacGrantRow>()
            .await?;
        Ok(match cluster_id {
            Some(cluster_id) => rows
                .into_iter()
                .filter(|row| {
                    kubernetes_cluster_selector_matches(
                        &row.cluster_match,
                        &row.cluster_id,
                        &row.cluster_pattern,
                        cluster_id,
                    )
                })
                .collect(),
            None => rows,
        })
    }

    pub async fn list_gateway_kubernetes_rbac_grants(
        &self,
        cluster_id: &str,
        tenant_ids: &[String],
    ) -> anyhow::Result<Vec<KubernetesRbacGrantRow>> {
        let allowed = tenant_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        let rows = self
            .client
            .query(&format!(
                "SELECT {KUBERNETES_RBAC_GRANT_COLUMNS} FROM config_kubernetes_rbac_grants FINAL WHERE is_deleted = 0 ORDER BY tenant_id, name, id"
            ))
            .fetch_all::<KubernetesRbacGrantRow>()
            .await?;
        Ok(rows
            .into_iter()
            .filter(|row| {
                allowed.contains(row.tenant_id.as_str())
                    && kubernetes_cluster_selector_matches(
                        &row.cluster_match,
                        &row.cluster_id,
                        &row.cluster_pattern,
                        cluster_id,
                    )
            })
            .collect())
    }

    pub async fn get_kubernetes_rbac_grant(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> anyhow::Result<Option<KubernetesRbacGrantRow>> {
        let result = self
            .client
            .query(&format!(
                "SELECT {KUBERNETES_RBAC_GRANT_COLUMNS} FROM config_kubernetes_rbac_grants FINAL WHERE tenant_id = ? AND id = ? AND is_deleted = 0 LIMIT 1"
            ))
            .bind(tenant_id)
            .bind(id)
            .fetch_one::<KubernetesRbacGrantRow>()
            .await;
        match result {
            Ok(row) => Ok(Some(row)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_kubernetes_rbac_grant(
        &self,
        tenant_id: &str,
        group_id: &str,
        cluster_id: &str,
        cluster_match: &str,
        cluster_pattern: &str,
        name: &str,
        role_kind: &str,
        role_name: &str,
        scope: &str,
        namespaces: &str,
        rules: &str,
    ) -> anyhow::Result<KubernetesRbacGrantRow> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_kubernetes_rbac_grants (id, tenant_id, group_id, cluster_id, cluster_match, cluster_pattern, name, role_kind, role_name, scope, namespaces, rules, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(&id)
            .bind(tenant_id)
            .bind(group_id)
            .bind(cluster_id)
            .bind(cluster_match)
            .bind(cluster_pattern)
            .bind(name)
            .bind(role_kind)
            .bind(role_name)
            .bind(scope)
            .bind(namespaces)
            .bind(rules)
            .bind(&now)
            .bind(&now)
            .bind(Self::next_version())
            .execute()
            .await?;
        self.get_kubernetes_rbac_grant(tenant_id, &id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Kubernetes RBAC grant was not visible after creation"))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_kubernetes_rbac_grant(
        &self,
        tenant_id: &str,
        id: &str,
        group_id: &str,
        cluster_id: &str,
        cluster_match: &str,
        cluster_pattern: &str,
        name: &str,
        role_kind: &str,
        role_name: &str,
        scope: &str,
        namespaces: &str,
        rules: &str,
    ) -> anyhow::Result<Option<KubernetesRbacGrantRow>> {
        let Some(existing) = self.get_kubernetes_rbac_grant(tenant_id, id).await? else {
            return Ok(None);
        };
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_kubernetes_rbac_grants (id, tenant_id, group_id, cluster_id, cluster_match, cluster_pattern, name, role_kind, role_name, scope, namespaces, rules, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(id)
            .bind(tenant_id)
            .bind(group_id)
            .bind(cluster_id)
            .bind(cluster_match)
            .bind(cluster_pattern)
            .bind(name)
            .bind(role_kind)
            .bind(role_name)
            .bind(scope)
            .bind(namespaces)
            .bind(rules)
            .bind(&existing.created_at)
            .bind(&now)
            .bind(Self::next_version())
            .execute()
            .await?;
        self.get_kubernetes_rbac_grant(tenant_id, id).await
    }

    pub async fn delete_kubernetes_rbac_grant(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> anyhow::Result<Option<KubernetesRbacGrantRow>> {
        let Some(existing) = self.get_kubernetes_rbac_grant(tenant_id, id).await? else {
            return Ok(None);
        };
        let now = Self::now_str();
        self.client
            .query("INSERT INTO config_kubernetes_rbac_grants (id, tenant_id, group_id, cluster_id, cluster_match, cluster_pattern, name, role_kind, role_name, scope, namespaces, rules, created_at, updated_at, version, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(&existing.id)
            .bind(&existing.tenant_id)
            .bind(&existing.group_id)
            .bind(&existing.cluster_id)
            .bind(&existing.cluster_match)
            .bind(&existing.cluster_pattern)
            .bind(&existing.name)
            .bind(&existing.role_kind)
            .bind(&existing.role_name)
            .bind(&existing.scope)
            .bind(&existing.namespaces)
            .bind(&existing.rules)
            .bind(&existing.created_at)
            .bind(&now)
            .bind(Self::next_version())
            .execute()
            .await?;
        Ok(Some(existing))
    }

    pub async fn kubernetes_rbac_group_ids_for_user(
        &self,
        tenant_id: &str,
        cluster_id: &str,
        user_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            group_id: String,
            cluster_id: String,
            cluster_match: String,
            cluster_pattern: String,
        }
        let mut group_ids = self
            .client
            .query("SELECT DISTINCT gr.group_id AS group_id, gr.cluster_id AS cluster_id, gr.cluster_match AS cluster_match, gr.cluster_pattern AS cluster_pattern FROM config_kubernetes_rbac_grants gr FINAL JOIN config_user_groups ug FINAL ON gr.group_id = ug.group_id JOIN config_group_tenants gt FINAL ON gr.group_id = gt.group_id JOIN config_groups cg FINAL ON gr.group_id = cg.id WHERE gr.tenant_id = ? AND gr.is_deleted = 0 AND ug.user_id = ? AND ug.is_deleted = 0 AND gt.tenant_id = ? AND gt.is_deleted = 0 AND cg.is_deleted = 0")
            .bind(tenant_id)
            .bind(user_id)
            .bind(tenant_id)
            .fetch_all::<Row>()
            .await?
            .into_iter()
            .filter(|row| {
                kubernetes_cluster_selector_matches(
                    &row.cluster_match,
                    &row.cluster_id,
                    &row.cluster_pattern,
                    cluster_id,
                )
            })
            .map(|row| row.group_id)
            .collect::<Vec<_>>();
        group_ids.sort();
        group_ids.dedup();
        Ok(group_ids)
    }

    pub async fn create_kubernetes_login_request(
        &self,
        device_code_hash: &str,
        user_code: &str,
        cluster_id: &str,
        created_at: &str,
        expires_at: &str,
    ) -> anyhow::Result<()> {
        self.insert_kubernetes_login_request(&KubernetesLoginRequest {
            device_code_hash: device_code_hash.to_string(),
            user_code: user_code.to_string(),
            cluster_id: cluster_id.to_string(),
            state: "pending".to_string(),
            tenant_id: String::new(),
            user_id: String::new(),
            username: String::new(),
            role: String::new(),
            client_reported: "{}".to_string(),
            created_at: created_at.to_string(),
            expires_at: expires_at.to_string(),
            approved_at: String::new(),
            credential_expires_at: String::new(),
            version: Self::next_version(),
            is_deleted: 0,
        })
        .await
    }

    pub async fn get_kubernetes_login_by_user_code(
        &self,
        user_code: &str,
    ) -> anyhow::Result<Option<KubernetesLoginRequest>> {
        let result = self.client
            .query("SELECT device_code_hash, user_code, cluster_id, state, tenant_id, user_id, username, role, client_reported, created_at, expires_at, approved_at, credential_expires_at, version, is_deleted FROM config_kubernetes_login_requests FINAL WHERE user_code = ? AND is_deleted = 0 ORDER BY version DESC LIMIT 1")
            .bind(user_code)
            .fetch_one::<KubernetesLoginRequest>()
            .await;
        match result {
            Ok(request) => Ok(Some(request)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn get_kubernetes_login_by_device_hash(
        &self,
        device_code_hash: &str,
    ) -> anyhow::Result<Option<KubernetesLoginRequest>> {
        let result = self.client
            .query("SELECT device_code_hash, user_code, cluster_id, state, tenant_id, user_id, username, role, client_reported, created_at, expires_at, approved_at, credential_expires_at, version, is_deleted FROM config_kubernetes_login_requests FINAL WHERE device_code_hash = ? AND is_deleted = 0 LIMIT 1")
            .bind(device_code_hash)
            .fetch_one::<KubernetesLoginRequest>()
            .await;
        match result {
            Ok(request) => Ok(Some(request)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn approve_kubernetes_login_request(
        &self,
        request: &KubernetesLoginRequest,
        user_id: &str,
        username: &str,
        tenant_id: &str,
        role: &str,
        approved_at: &str,
        credential_expires_at: &str,
    ) -> anyhow::Result<KubernetesLoginRequest> {
        let mut approved = request.clone();
        approved.state = "approved".to_string();
        approved.user_id = user_id.to_string();
        approved.username = username.to_string();
        approved.tenant_id = tenant_id.to_string();
        approved.role = role.to_string();
        approved.approved_at = approved_at.to_string();
        approved.credential_expires_at = credential_expires_at.to_string();
        approved.version = Self::next_version();
        self.insert_kubernetes_login_request(&approved).await?;
        Ok(approved)
    }

    pub async fn attach_kubernetes_login_enrichment(
        &self,
        request: &KubernetesLoginRequest,
        client_reported: &str,
    ) -> anyhow::Result<KubernetesLoginRequest> {
        let mut enriched = request.clone();
        enriched.client_reported = client_reported.to_string();
        enriched.version = Self::next_version();
        self.insert_kubernetes_login_request(&enriched).await?;
        Ok(enriched)
    }

    pub async fn list_active_kubernetes_login_requests(
        &self,
        tenant_id: &str,
        now: &str,
    ) -> anyhow::Result<Vec<KubernetesLoginRequest>> {
        self.client
            .query("SELECT device_code_hash, user_code, cluster_id, state, tenant_id, user_id, username, role, client_reported, created_at, expires_at, approved_at, credential_expires_at, version, is_deleted FROM config_kubernetes_login_requests FINAL WHERE tenant_id = ? AND state = 'approved' AND credential_expires_at > ? AND is_deleted = 0 AND device_code_hash NOT IN (SELECT device_code_hash FROM config_kubernetes_login_revocations FINAL) ORDER BY credential_expires_at ASC")
            .bind(tenant_id)
            .bind(now)
            .fetch_all::<KubernetesLoginRequest>()
            .await
            .map_err(Into::into)
    }

    pub async fn is_kubernetes_login_revoked(
        &self,
        device_code_hash: &str,
    ) -> anyhow::Result<bool> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            count: u64,
        }
        let row = self
            .client
            .query("SELECT count() AS count FROM config_kubernetes_login_revocations FINAL WHERE device_code_hash = ?")
            .bind(device_code_hash)
            .fetch_one::<Row>()
            .await?;
        Ok(row.count > 0)
    }

    pub async fn revoke_kubernetes_login_request(
        &self,
        request: &KubernetesLoginRequest,
        revoked_at: &str,
    ) -> anyhow::Result<()> {
        self.client
            .query("INSERT INTO config_kubernetes_login_revocations (device_code_hash, tenant_id, revoked_at, version) VALUES (?, ?, ?, ?)")
            .bind(&request.device_code_hash)
            .bind(&request.tenant_id)
            .bind(revoked_at)
            .bind(Self::next_version())
            .execute()
            .await?;
        Ok(())
    }

    pub async fn kubernetes_access_storage_ready(&self) -> anyhow::Result<()> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ReadyRow {
            count: u64,
        }

        let events = self
            .client
            .query("SELECT count() AS count FROM config_kubernetes_access_events WHERE 0")
            .fetch_one::<ReadyRow>()
            .await?;
        let chunks = self
            .client
            .query("SELECT count() AS count FROM config_kubernetes_session_chunks WHERE 0")
            .fetch_one::<ReadyRow>()
            .await?;
        let _ = events.count.saturating_add(chunks.count);
        Ok(())
    }

    pub async fn list_kubernetes_gateway_activity(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<KubernetesGatewayActivityRow>> {
        self.client
            .query("SELECT gateway_id, cluster_id, max(created_at) AS last_activity, count() AS recorded_requests FROM config_kubernetes_access_events WHERE tenant_id = ? AND gateway_id != '' AND cluster_id != '' GROUP BY gateway_id, cluster_id ORDER BY last_activity DESC, gateway_id ASC")
            .bind(tenant_id)
            .fetch_all::<KubernetesGatewayActivityRow>()
            .await
            .map_err(Into::into)
    }

    pub async fn insert_kubernetes_access_event(
        &self,
        event: &crate::models::kubernetes_access::KubernetesAccessEvent,
    ) -> anyhow::Result<()> {
        self.client
            .query(
                "INSERT INTO config_kubernetes_access_events (id, tenant_id, cluster_id, gateway_id, session_id, actor_user_id, actor_name, actor_type, kube_username, kube_groups, source_kind, client_reported, observed_network, http_method, verb, api_group, api_version, resource, subresource, namespace, name, request_query, user_agent, status_code, duration_ms, request_bytes, response_bytes, result_summary, result_truncated, redaction_count, recording_state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&event.id)
            .bind(&event.tenant_id)
            .bind(&event.cluster_id)
            .bind(&event.gateway_id)
            .bind(&event.session_id)
            .bind(&event.actor_user_id)
            .bind(&event.actor_name)
            .bind(&event.actor_type)
            .bind(&event.kube_username)
            .bind(&event.kube_groups)
            .bind(&event.source_kind)
            .bind(&event.client_reported)
            .bind(&event.observed_network)
            .bind(&event.http_method)
            .bind(&event.verb)
            .bind(&event.api_group)
            .bind(&event.api_version)
            .bind(&event.resource)
            .bind(&event.subresource)
            .bind(&event.namespace)
            .bind(&event.name)
            .bind(&event.request_query)
            .bind(&event.user_agent)
            .bind(event.status_code)
            .bind(event.duration_ms)
            .bind(event.request_bytes)
            .bind(event.response_bytes)
            .bind(&event.result_summary)
            .bind(event.result_truncated)
            .bind(event.redaction_count)
            .bind(&event.recording_state)
            .bind(&event.created_at)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn list_kubernetes_access_events(
        &self,
        filter: &crate::models::kubernetes_access::KubernetesAccessFilter,
        include_evidence: bool,
    ) -> anyhow::Result<(
        Vec<crate::models::kubernetes_access::KubernetesAccessEvent>,
        u64,
    )> {
        const WHERE_SQL: &str = "tenant_id = ? \
            AND (? = '' OR created_at >= ?) \
            AND (? = '' OR created_at <= ?) \
            AND (? = '' OR actor_user_id = ? OR actor_name = ?) \
            AND (? = '' OR cluster_id = ?) \
            AND (? = '' OR namespace = ?) \
            AND (? = '' OR verb = ?) \
            AND (? = '' OR resource = ?) \
            AND (? = 0 OR (status_code >= ? AND status_code <= ?)) \
            AND (? = '' OR source_kind = ?) \
            AND (? = '' OR recording_state = ?) \
            AND (? = '' OR positionCaseInsensitive(id, ?) > 0 OR positionCaseInsensitive(actor_name, ?) > 0 OR positionCaseInsensitive(kube_username, ?) > 0 OR positionCaseInsensitive(name, ?) > 0 OR positionCaseInsensitive(session_id, ?) > 0 OR positionCaseInsensitive(cluster_id, ?) > 0 OR positionCaseInsensitive(namespace, ?) > 0 OR positionCaseInsensitive(resource, ?) > 0 OR positionCaseInsensitive(client_reported, ?) > 0 OR positionCaseInsensitive(result_summary, ?) > 0)";
        macro_rules! bind_filter {
            ($query:expr) => {
                $query
                    .bind(&filter.tenant_id)
                    .bind(&filter.from)
                    .bind(&filter.from)
                    .bind(&filter.to)
                    .bind(&filter.to)
                    .bind(&filter.actor)
                    .bind(&filter.actor)
                    .bind(&filter.actor)
                    .bind(&filter.cluster)
                    .bind(&filter.cluster)
                    .bind(&filter.namespace)
                    .bind(&filter.namespace)
                    .bind(&filter.verb)
                    .bind(&filter.verb)
                    .bind(&filter.resource)
                    .bind(&filter.resource)
                    .bind(filter.status_min)
                    .bind(filter.status_min)
                    .bind(filter.status_max)
                    .bind(&filter.source_kind)
                    .bind(&filter.source_kind)
                    .bind(&filter.recording_state)
                    .bind(&filter.recording_state)
                    .bind(&filter.q)
                    .bind(&filter.q)
                    .bind(&filter.q)
                    .bind(&filter.q)
                    .bind(&filter.q)
                    .bind(&filter.q)
                    .bind(&filter.q)
                    .bind(&filter.q)
                    .bind(&filter.q)
                    .bind(&filter.q)
                    .bind(&filter.q)
            };
        }

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct CountRow {
            total: u64,
        }

        let count_sql = format!(
            "SELECT count() AS total FROM config_kubernetes_access_events WHERE {WHERE_SQL}"
        );
        let total = bind_filter!(self.client.query(&count_sql))
            .fetch_one::<CountRow>()
            .await?
            .total;

        let columns = kubernetes_access_columns(include_evidence);
        let list_sql = format!(
            "SELECT {columns} FROM config_kubernetes_access_events WHERE {WHERE_SQL} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
        );
        let rows = bind_filter!(self.client.query(&list_sql))
            .bind(filter.limit)
            .bind(filter.offset)
            .fetch_all::<crate::models::kubernetes_access::KubernetesAccessEvent>()
            .await?;
        Ok((rows, total))
    }

    pub async fn get_kubernetes_access_event(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> anyhow::Result<Option<crate::models::kubernetes_access::KubernetesAccessEvent>> {
        let result = self
            .client
            .query("SELECT id, tenant_id, cluster_id, gateway_id, session_id, actor_user_id, actor_name, actor_type, kube_username, kube_groups, source_kind, client_reported, observed_network, http_method, verb, api_group, api_version, resource, subresource, namespace, name, request_query, user_agent, status_code, duration_ms, request_bytes, response_bytes, result_summary, result_truncated, redaction_count, recording_state, created_at FROM config_kubernetes_access_events WHERE tenant_id = ? AND id = ? LIMIT 1")
            .bind(tenant_id)
            .bind(id)
            .fetch_one::<crate::models::kubernetes_access::KubernetesAccessEvent>()
            .await;
        match result {
            Ok(event) => Ok(Some(event)),
            Err(clickhouse::error::Error::RowNotFound) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn insert_kubernetes_session_chunk(
        &self,
        chunk: &crate::models::kubernetes_access::KubernetesSessionChunk,
    ) -> anyhow::Result<()> {
        self.client
            .query("INSERT INTO config_kubernetes_session_chunks (id, tenant_id, session_id, event_id, gateway_id, sequence, stream, encoding, provenance, recording_state, offset_ms, data, byte_count, redaction_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&chunk.id)
            .bind(&chunk.tenant_id)
            .bind(&chunk.session_id)
            .bind(&chunk.event_id)
            .bind(&chunk.gateway_id)
            .bind(chunk.sequence)
            .bind(&chunk.stream)
            .bind(&chunk.encoding)
            .bind(&chunk.provenance)
            .bind(&chunk.recording_state)
            .bind(chunk.offset_ms)
            .bind(&chunk.data)
            .bind(chunk.byte_count)
            .bind(chunk.redaction_count)
            .bind(&chunk.created_at)
            .execute()
            .await?;
        Ok(())
    }

    pub async fn list_kubernetes_session_chunks(
        &self,
        tenant_id: &str,
        session_id: &str,
        after_sequence: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<crate::models::kubernetes_access::KubernetesSessionChunk>> {
        self.client
            .query("SELECT id, tenant_id, session_id, event_id, gateway_id, sequence, stream, encoding, provenance, recording_state, offset_ms, data, byte_count, redaction_count, created_at FROM config_kubernetes_session_chunks WHERE tenant_id = ? AND session_id = ? AND sequence > ? ORDER BY sequence ASC, id ASC LIMIT ?")
            .bind(tenant_id)
            .bind(session_id)
            .bind(after_sequence)
            .bind(limit)
            .fetch_all::<crate::models::kubernetes_access::KubernetesSessionChunk>()
            .await
            .map_err(Into::into)
    }

    pub async fn kubernetes_session_summary(
        &self,
        tenant_id: &str,
        session_id: &str,
    ) -> anyhow::Result<crate::models::kubernetes_access::KubernetesSessionSummary> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct SummaryRow {
            chunk_count: u64,
            total_bytes: u64,
            redaction_count: u64,
        }
        let row = self
            .client
            .query("SELECT count() AS chunk_count, sum(byte_count) AS total_bytes, sum(toUInt64(redaction_count)) AS redaction_count FROM config_kubernetes_session_chunks WHERE tenant_id = ? AND session_id = ?")
            .bind(tenant_id)
            .bind(session_id)
            .fetch_one::<SummaryRow>()
            .await?;
        Ok(crate::models::kubernetes_access::KubernetesSessionSummary {
            session_id: session_id.to_string(),
            chunk_count: row.chunk_count,
            total_bytes: row.total_bytes,
            redaction_count: row.redaction_count,
        })
    }
}
