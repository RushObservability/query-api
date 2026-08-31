//! API-managed integration metadata, target secrets, and collector lifecycle.
//!
//! Integrations are compiled into a distribution, then enabled at runtime only
//! when the customer license contains the matching entitlement. PostgreSQL and
//! MySQL collectors run behind a process boundary so a collector cannot take
//! down the query API.

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use openssl::rand::rand_bytes;
use openssl::sha::sha256;
use openssl::symm::{Cipher, decrypt_aead, encrypt_aead};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::clickhouse_config::ConfigDb;

pub const POSTGRES_INTEGRATION: &str = "postgresql";
pub const POSTGRES_ENTITLEMENT: &str = "postgres";
pub const MYSQL_INTEGRATION: &str = "mysql";
pub const MYSQL_ENTITLEMENT: &str = "mysql";

const INTEGRATION_CIPHERTEXT_PREFIX: &str = "v2:";
const INTEGRATION_LEGACY_KEY_ID: &str = "legacy";
const INTEGRATION_LEGACY_AAD: &[u8] = b"rush-integration-secret-v1";

#[derive(Debug)]
struct IntegrationEncryptionKeys {
    current_key_id: String,
    keys: HashMap<String, [u8; 32]>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IntegrationDescriptor {
    pub id: &'static str,
    pub name: &'static str,
    pub entitlement: &'static str,
    pub compiled: bool,
}

pub fn descriptors() -> Vec<IntegrationDescriptor> {
    vec![
        IntegrationDescriptor {
            id: POSTGRES_INTEGRATION,
            name: "PostgreSQL",
            entitlement: POSTGRES_ENTITLEMENT,
            compiled: cfg!(feature = "postgres-collector"),
        },
        IntegrationDescriptor {
            id: MYSQL_INTEGRATION,
            name: "MySQL",
            entitlement: MYSQL_ENTITLEMENT,
            compiled: cfg!(feature = "mysql-collector"),
        },
    ]
}

struct CollectorRuntime {
    integration: &'static str,
    entitlement: &'static str,
    name: &'static str,
    compiled: bool,
    bootstrap_env: &'static str,
    binary_env: &'static str,
    binary_default: &'static str,
    config_env: &'static str,
}

fn runtimes() -> [CollectorRuntime; 2] {
    [
        CollectorRuntime {
            integration: POSTGRES_INTEGRATION,
            entitlement: POSTGRES_ENTITLEMENT,
            name: "PostgreSQL",
            compiled: cfg!(feature = "postgres-collector"),
            bootstrap_env: "RUSH_POSTGRES_COLLECTOR_CONFIG",
            binary_env: "RUSH_POSTGRES_COLLECTOR_BIN",
            binary_default: "../postgres-collector/target/debug/postgres-collector",
            config_env: "PG_COLLECTOR_CONFIG",
        },
        CollectorRuntime {
            integration: MYSQL_INTEGRATION,
            entitlement: MYSQL_ENTITLEMENT,
            name: "MySQL",
            compiled: cfg!(feature = "mysql-collector"),
            bootstrap_env: "RUSH_MYSQL_COLLECTOR_CONFIG",
            binary_env: "RUSH_MYSQL_COLLECTOR_BIN",
            binary_default: "../mysql-collector/target/debug/mysql-collector",
            config_env: "MYSQL_COLLECTOR_CONFIG",
        },
    ]
}

#[derive(Debug, Clone, Deserialize)]
pub struct IntegrationTargetInput {
    pub id: Option<String>,
    pub name: String,
    pub dsn: String,
    #[serde(default = "default_environment")]
    pub environment: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct IntegrationTargetResponse {
    pub id: String,
    pub name: String,
    pub environment: String,
    pub enabled: bool,
    pub configured: bool,
}

#[derive(Debug, Clone)]
pub struct IntegrationTargetSecret {
    pub id: String,
    pub name: String,
    pub dsn: String,
    pub environment: String,
    pub enabled: bool,
}

fn default_environment() -> String {
    "production".to_string()
}

fn default_enabled() -> bool {
    true
}

pub fn target_response(target: IntegrationTargetSecret) -> IntegrationTargetResponse {
    IntegrationTargetResponse {
        id: target.id,
        name: target.name,
        environment: target.environment,
        enabled: target.enabled,
        configured: !target.dsn.is_empty(),
    }
}

/// Encrypt integration credentials before they enter ClickHouse.
///
/// Ciphertexts carry the non-secret key ID so prior keys can remain available
/// during a planned rotation without trying every configured secret.
pub fn encrypt_secret(plaintext: &str) -> Result<String> {
    encrypt_secret_with_keys(plaintext, &load_encryption_keys()?)
}

fn encrypt_secret_with_keys(plaintext: &str, keys: &IntegrationEncryptionKeys) -> Result<String> {
    let key = keys
        .keys
        .get(&keys.current_key_id)
        .ok_or_else(|| anyhow!("current integration encryption key is unavailable"))?;
    let aad = integration_aad(&keys.current_key_id);
    let mut iv = [0u8; 12];
    rand_bytes(&mut iv).context("generate integration secret nonce")?;
    let mut tag = [0u8; 16];
    let ciphertext = encrypt_aead(
        Cipher::aes_256_gcm(),
        key,
        Some(&iv),
        &aad,
        plaintext.as_bytes(),
        &mut tag,
    )
    .context("encrypt integration secret")?;

    let mut packed = Vec::with_capacity(iv.len() + tag.len() + ciphertext.len());
    packed.extend_from_slice(&iv);
    packed.extend_from_slice(&tag);
    packed.extend_from_slice(&ciphertext);
    Ok(format!(
        "{INTEGRATION_CIPHERTEXT_PREFIX}{}:{}",
        keys.current_key_id,
        base64::engine::general_purpose::STANDARD.encode(packed)
    ))
}

pub fn decrypt_secret(encoded: &str) -> Result<String> {
    decrypt_secret_with_keys(encoded, &load_encryption_keys()?)
}

fn decrypt_secret_with_keys(encoded: &str, keys: &IntegrationEncryptionKeys) -> Result<String> {
    let (payload, key, aad) =
        if let Some(envelope) = encoded.strip_prefix(INTEGRATION_CIPHERTEXT_PREFIX) {
            let (key_id, payload) = envelope
                .split_once(':')
                .ok_or_else(|| anyhow!("integration secret envelope is malformed"))?;
            if !valid_integration_key_id(key_id) {
                bail!("integration secret envelope has an invalid key id");
            }
            let key = keys
                .keys
                .get(key_id)
                .ok_or_else(|| anyhow!("integration encryption key '{key_id}' is unavailable"))?;
            (payload, key, integration_aad(key_id))
        } else {
            // Rows written before key IDs were introduced used the v1 AAD and no
            // envelope. During the first rotation, retain that secret under the
            // reserved `legacy` ID. Before rotation, the current key also works.
            let key = keys
                .keys
                .get(INTEGRATION_LEGACY_KEY_ID)
                .or_else(|| keys.keys.get(&keys.current_key_id))
                .ok_or_else(|| anyhow!("legacy integration encryption key is unavailable"))?;
            (encoded, key, INTEGRATION_LEGACY_AAD.to_vec())
        };

    let packed = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .context("decode integration secret")?;
    if packed.len() < 28 {
        bail!("integration secret is truncated");
    }
    let plaintext = decrypt_aead(
        Cipher::aes_256_gcm(),
        key,
        Some(&packed[..12]),
        &aad,
        &packed[28..],
        &packed[12..28],
    )
    .context("decrypt integration secret")?;
    String::from_utf8(plaintext).context("integration secret is not valid UTF-8")
}

fn integration_aad(key_id: &str) -> Vec<u8> {
    format!("rush-integration-secret-v2:{key_id}").into_bytes()
}

fn valid_integration_key_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn derive_integration_key(variable: &str, secret: &str) -> Result<[u8; 32]> {
    if secret.len() < 32 {
        bail!("{variable} must be at least 32 bytes");
    }
    Ok(sha256(secret.as_bytes()))
}

fn build_encryption_keys(
    current_key_id: &str,
    current_secret: &str,
    previous_keys_json: Option<&str>,
) -> Result<IntegrationEncryptionKeys> {
    if !valid_integration_key_id(current_key_id) || current_key_id == INTEGRATION_LEGACY_KEY_ID {
        bail!(
            "RUSH_INTEGRATION_ENCRYPTION_KEY_ID must be 1-64 ASCII letters, digits, '.', '_' or '-' and may not be 'legacy'"
        );
    }

    let mut keys = HashMap::new();
    if let Some(raw) = previous_keys_json.filter(|value| !value.trim().is_empty()) {
        let previous: HashMap<String, String> = serde_json::from_str(raw).context(
            "RUSH_INTEGRATION_ENCRYPTION_PREVIOUS_KEYS must be a JSON object of key-id to secret",
        )?;
        for (key_id, secret) in previous {
            if !valid_integration_key_id(&key_id) {
                bail!("invalid previous integration encryption key id '{key_id}'");
            }
            let key = derive_integration_key(
                &format!("previous integration encryption key '{key_id}'"),
                &secret,
            )?;
            keys.insert(key_id, key);
        }
    }
    if keys.contains_key(current_key_id) {
        bail!("current integration encryption key id is duplicated in previous keys");
    }
    keys.insert(
        current_key_id.to_string(),
        derive_integration_key("RUSH_INTEGRATION_ENCRYPTION_KEY", current_secret)?,
    );
    Ok(IntegrationEncryptionKeys {
        current_key_id: current_key_id.to_string(),
        keys,
    })
}

fn load_encryption_keys() -> Result<IntegrationEncryptionKeys> {
    let current_key_id = std::env::var("RUSH_INTEGRATION_ENCRYPTION_KEY_ID")
        .unwrap_or_else(|_| "primary".to_string());
    let current_secret = std::env::var("RUSH_INTEGRATION_ENCRYPTION_KEY")
        .map_err(|_| anyhow!("RUSH_INTEGRATION_ENCRYPTION_KEY is required"))?;
    let previous_keys = std::env::var("RUSH_INTEGRATION_ENCRYPTION_PREVIOUS_KEYS").ok();
    build_encryption_keys(&current_key_id, &current_secret, previous_keys.as_deref())
}

#[derive(Debug)]
struct ManagedProcess {
    child: Child,
    fingerprint: String,
    config_path: PathBuf,
    cleanup_config: bool,
}

/// Supervises locally spawned collectors. A remote/Kubernetes runner can use
/// the same ConfigDb target methods without needing this process supervisor.
pub struct CollectorManager {
    config_db: Arc<ConfigDb>,
    processes: Mutex<HashMap<String, ManagedProcess>>,
    enabled: bool,
}

impl CollectorManager {
    pub fn new(config_db: Arc<ConfigDb>) -> Self {
        let enabled = std::env::var("RUSH_COLLECTOR_MANAGER_ENABLED")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        Self {
            config_db,
            processes: Mutex::new(HashMap::new()),
            enabled,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Reconcile every configured, licensed collector for a tenant.
    pub async fn reconcile(&self, tenant_id: &str) -> Result<()> {
        let mut first_error = None;
        for runtime in runtimes() {
            if let Err(error) = self.reconcile_one(tenant_id, &runtime).await {
                tracing::warn!(tenant = %tenant_id, integration = runtime.integration, %error, "collector reconciliation failed");
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn reconcile_one(&self, tenant_id: &str, runtime: &CollectorRuntime) -> Result<()> {
        let key = format!("{}:{tenant_id}", runtime.integration);
        if !self.enabled || !runtime.compiled {
            return self.stop(&key).await;
        }

        let license = crate::license::evaluate();
        if !license.has_entitlement(runtime.entitlement) {
            tracing::info!(tenant = %tenant_id, integration = runtime.integration, status = %license.status, "collector not licensed; keeping it stopped");
            return self.stop(&key).await;
        }

        let targets = self
            .config_db
            .list_integration_target_secrets(tenant_id, runtime.integration)
            .await?;
        let targets: Vec<_> = targets.into_iter().filter(|t| t.enabled).collect();
        let bootstrap_config = if targets.is_empty() {
            std::env::var(runtime.bootstrap_env)
                .ok()
                .filter(|path| !path.trim().is_empty())
                .map(PathBuf::from)
        } else {
            None
        };
        if targets.is_empty() && bootstrap_config.is_none() {
            return self.stop(&key).await;
        }
        if let Some(path) = &bootstrap_config {
            if !path.is_file() {
                self.stop(&key).await?;
                bail!(
                    "{} collector config not found at {}; set {}",
                    runtime.name,
                    path.display(),
                    runtime.bootstrap_env
                );
            }
        }

        let fingerprint = if let Some(path) = &bootstrap_config {
            static_config_fingerprint(path)?
        } else {
            serde_json::to_string(
                &targets
                    .iter()
                    .map(|t| (&t.id, &t.name, &t.dsn, &t.environment))
                    .collect::<Vec<_>>(),
            )?
        };
        let mut processes = self.processes.lock().await;
        if let Some(process) = processes.get_mut(&key) {
            if process.fingerprint == fingerprint && process.child.try_wait()?.is_none() {
                return Ok(());
            }
            let _ = process.child.kill().await;
            let _ = process.child.wait().await;
            if process.cleanup_config {
                let _ = std::fs::remove_file(&process.config_path);
            }
            processes.remove(&key);
        }

        let (config_path, cleanup_config) = if targets.is_empty() {
            // Local development can point at a checked-out collector config.
            // API-managed targets take precedence whenever one is configured.
            (
                bootstrap_config.expect("bootstrap config checked above"),
                false,
            )
        } else {
            (
                write_collector_config(runtime.integration, tenant_id, &targets)?,
                true,
            )
        };
        let binary =
            std::env::var(runtime.binary_env).unwrap_or_else(|_| runtime.binary_default.into());
        if !Path::new(&binary).exists() {
            if cleanup_config {
                let _ = std::fs::remove_file(&config_path);
            }
            bail!(
                "{} collector binary not found at {binary}; set {}",
                runtime.name,
                runtime.binary_env
            );
        }

        let endpoint = std::env::var("RUSH_COLLECTOR_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:8080".into());
        let mut command = Command::new(&binary);
        command
            .env(runtime.config_env, &config_path)
            .env("RUSH_OTLP_ENDPOINT", endpoint)
            .env("RUSH_COLLECTOR_TENANT", tenant_id)
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
            )
            .kill_on_drop(true);
        if let Ok(key) = std::env::var("RUSH_COLLECTOR_API_KEY") {
            command.env("RUSH_API_KEY", key);
        }
        let child = command
            .spawn()
            .with_context(|| format!("starting {} collector binary {binary}", runtime.name))?;
        tracing::info!(tenant = %tenant_id, integration = runtime.integration, pid = ?child.id(), "started managed collector");
        processes.insert(
            key,
            ManagedProcess {
                child,
                fingerprint,
                config_path,
                cleanup_config,
            },
        );
        Ok(())
    }

    pub async fn stop(&self, key: &str) -> Result<()> {
        let mut processes = self.processes.lock().await;
        if let Some(mut process) = processes.remove(key) {
            let _ = process.child.kill().await;
            let _ = process.child.wait().await;
            if process.cleanup_config {
                let _ = std::fs::remove_file(process.config_path);
            }
            tracing::info!(collector = %key, "stopped managed collector");
        }
        Ok(())
    }

    /// Start a lightweight reconciliation loop for the configured local tenant.
    pub fn spawn_reconciler(self: &Arc<Self>) {
        if !self.enabled {
            return;
        }
        let manager = Arc::clone(self);
        let tenant = std::env::var("RUSH_COLLECTOR_TENANT").unwrap_or_else(|_| "default".into());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tick.tick().await;
                if let Err(error) = manager.reconcile(&tenant).await {
                    tracing::warn!(tenant = %tenant, %error, "collector reconciliation failed");
                }
            }
        });
    }
}

fn static_config_fingerprint(path: &Path) -> Result<String> {
    let contents =
        std::fs::read(path).with_context(|| format!("read collector config {}", path.display()))?;
    Ok(format!(
        "static:{}:{}",
        path.display(),
        hex::encode(sha256(&contents))
    ))
}

fn write_collector_config(
    integration: &str,
    tenant_id: &str,
    targets: &[IntegrationTargetSecret],
) -> Result<PathBuf> {
    #[derive(Serialize)]
    struct FileTarget<'a> {
        dsn: &'a str,
        environment: &'a str,
    }
    #[derive(Serialize)]
    struct FileConfig<'a> {
        targets: HashMap<&'a str, FileTarget<'a>>,
    }

    let dir = std::env::var("RUSH_COLLECTOR_CONFIG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data/collector-config"));
    std::fs::create_dir_all(&dir).context("create collector config directory")?;
    let mut named = HashMap::new();
    for target in targets {
        named.insert(
            target.name.as_str(),
            FileTarget {
                dsn: &target.dsn,
                environment: &target.environment,
            },
        );
    }
    // JSON is a YAML 1.2 subset and is accepted by each collector's
    // YAML parser. Emitting it with serde_json avoids the unmaintained
    // serde_yaml dependency while preserving the existing config contract.
    let contents = serde_json::to_string_pretty(&FileConfig { targets: named })?;
    let path = dir.join(format!("{integration}-{tenant_id}.yaml"));
    write_private_file(&path, contents.as_bytes())?;
    Ok(path)
}

fn write_private_file(path: &Path, contents: &[u8]) -> Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;
    #[cfg(unix)]
    std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    use std::io::Write;
    file.write_all(contents)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRIMARY_SECRET: &str = "primary-integration-secret-32-bytes-minimum";
    const ROTATED_SECRET: &str = "rotated-integration-secret-32-bytes-minimum";

    #[test]
    fn descriptors_report_collectors_as_independently_feature_gated() {
        let postgres = descriptors()
            .into_iter()
            .find(|d| d.id == POSTGRES_INTEGRATION)
            .unwrap();
        assert_eq!(postgres.entitlement, POSTGRES_ENTITLEMENT);
        assert_eq!(postgres.compiled, cfg!(feature = "postgres-collector"));
        let mysql = descriptors()
            .into_iter()
            .find(|d| d.id == MYSQL_INTEGRATION)
            .unwrap();
        assert_eq!(mysql.entitlement, MYSQL_ENTITLEMENT);
        assert_eq!(mysql.compiled, cfg!(feature = "mysql-collector"));
    }

    #[test]
    fn integration_ciphertext_roundtrips_with_an_authenticated_key_id() {
        let keys = build_encryption_keys("2026-08", PRIMARY_SECRET, None).unwrap();
        let encrypted = encrypt_secret_with_keys("mysql://user:pass@db/app", &keys).unwrap();
        assert!(encrypted.starts_with("v2:2026-08:"));
        assert_eq!(
            decrypt_secret_with_keys(&encrypted, &keys).unwrap(),
            "mysql://user:pass@db/app"
        );
    }

    #[test]
    fn integration_key_rotation_keeps_prior_ciphertexts_readable() {
        let old_keys = build_encryption_keys("old", PRIMARY_SECRET, None).unwrap();
        let encrypted = encrypt_secret_with_keys("postgres://old", &old_keys).unwrap();
        let previous = serde_json::json!({ "old": PRIMARY_SECRET }).to_string();
        let rotated = build_encryption_keys("current", ROTATED_SECRET, Some(&previous)).unwrap();

        assert_eq!(
            decrypt_secret_with_keys(&encrypted, &rotated).unwrap(),
            "postgres://old"
        );
        assert!(
            encrypt_secret_with_keys("postgres://new", &rotated)
                .unwrap()
                .starts_with("v2:current:")
        );
    }

    #[test]
    fn integration_key_config_rejects_weak_or_ambiguous_keys() {
        assert!(build_encryption_keys("primary", "too-short", None).is_err());
        assert!(build_encryption_keys("legacy", PRIMARY_SECRET, None).is_err());
        assert!(build_encryption_keys("bad:key", PRIMARY_SECRET, None).is_err());
        let duplicate = serde_json::json!({ "primary": ROTATED_SECRET }).to_string();
        assert!(build_encryption_keys("primary", PRIMARY_SECRET, Some(&duplicate)).is_err());
    }

    #[test]
    fn integration_decryption_rejects_unknown_key_ids() {
        let keys = build_encryption_keys("primary", PRIMARY_SECRET, None).unwrap();
        let encrypted = encrypt_secret_with_keys("secret", &keys).unwrap();
        let unknown = encrypted.replacen("v2:primary:", "v2:missing:", 1);
        let error = decrypt_secret_with_keys(&unknown, &keys).unwrap_err();
        assert!(error.to_string().contains("unavailable"));
    }

    #[test]
    fn legacy_ciphertexts_use_the_explicit_legacy_rotation_key() {
        let legacy_key = derive_integration_key("legacy", PRIMARY_SECRET).unwrap();
        let mut iv = [0u8; 12];
        rand_bytes(&mut iv).unwrap();
        let mut tag = [0u8; 16];
        let ciphertext = encrypt_aead(
            Cipher::aes_256_gcm(),
            &legacy_key,
            Some(&iv),
            INTEGRATION_LEGACY_AAD,
            b"legacy-secret",
            &mut tag,
        )
        .unwrap();
        let mut packed = Vec::new();
        packed.extend_from_slice(&iv);
        packed.extend_from_slice(&tag);
        packed.extend_from_slice(&ciphertext);
        let encoded = base64::engine::general_purpose::STANDARD.encode(packed);

        let previous = serde_json::json!({ "legacy": PRIMARY_SECRET }).to_string();
        let keys = build_encryption_keys("current", ROTATED_SECRET, Some(&previous)).unwrap();
        assert_eq!(
            decrypt_secret_with_keys(&encoded, &keys).unwrap(),
            "legacy-secret"
        );
    }
}
