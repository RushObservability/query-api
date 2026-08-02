//! API-managed integration metadata, target secrets, and collector lifecycle.
//!
//! Integrations are compiled into a distribution, then enabled at runtime only
//! when the customer license contains the matching entitlement. The first
//! managed collector is PostgreSQL; the manager intentionally uses a process
//! boundary so a collector cannot take down the query API.

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

#[derive(Debug, Clone, Serialize)]
pub struct IntegrationDescriptor {
    pub id: &'static str,
    pub name: &'static str,
    pub entitlement: &'static str,
    pub compiled: bool,
}

pub fn descriptors() -> Vec<IntegrationDescriptor> {
    vec![IntegrationDescriptor {
        id: POSTGRES_INTEGRATION,
        name: "PostgreSQL",
        entitlement: POSTGRES_ENTITLEMENT,
        compiled: cfg!(feature = "postgres-collector"),
    }]
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
/// `RUSH_INTEGRATION_ENCRYPTION_KEY` is preferred. `RUSH_API_KEY_SECRET` is a
/// backwards-compatible local-dev fallback. Production deployments should set
/// the dedicated key and rotate it through their secret manager.
pub fn encrypt_secret(plaintext: &str) -> Result<String> {
    let key = encryption_key()?;
    let mut iv = [0u8; 12];
    rand_bytes(&mut iv).context("generate integration secret nonce")?;
    let mut tag = [0u8; 16];
    let ciphertext = encrypt_aead(
        Cipher::aes_256_gcm(),
        &key,
        Some(&iv),
        b"rush-integration-secret-v1",
        plaintext.as_bytes(),
        &mut tag,
    )
    .context("encrypt integration secret")?;

    let mut packed = Vec::with_capacity(iv.len() + tag.len() + ciphertext.len());
    packed.extend_from_slice(&iv);
    packed.extend_from_slice(&tag);
    packed.extend_from_slice(&ciphertext);
    Ok(base64::engine::general_purpose::STANDARD.encode(packed))
}

pub fn decrypt_secret(encoded: &str) -> Result<String> {
    let key = encryption_key()?;
    let packed = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .context("decode integration secret")?;
    if packed.len() < 28 {
        bail!("integration secret is truncated");
    }
    let plaintext = decrypt_aead(
        Cipher::aes_256_gcm(),
        &key,
        Some(&packed[..12]),
        b"rush-integration-secret-v1",
        &packed[28..],
        &packed[12..28],
    )
    .context("decrypt integration secret")?;
    String::from_utf8(plaintext).context("integration secret is not valid UTF-8")
}

fn encryption_key() -> Result<[u8; 32]> {
    let raw = std::env::var("RUSH_INTEGRATION_ENCRYPTION_KEY")
        .or_else(|_| std::env::var("RUSH_API_KEY_SECRET"))
        .map_err(|_| anyhow!("RUSH_INTEGRATION_ENCRYPTION_KEY is required"))?;
    if raw.trim().len() < 16 {
        bail!("RUSH_INTEGRATION_ENCRYPTION_KEY must be at least 16 characters");
    }
    Ok(sha256(raw.as_bytes()))
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

    /// Reconcile the configured tenant's PostgreSQL process. The manager is
    /// deliberately opt-in so the community API never launches binaries by
    /// accident, and the feature check keeps OSS builds collector-free.
    pub async fn reconcile(&self, tenant_id: &str) -> Result<()> {
        let key = format!("{POSTGRES_INTEGRATION}:{tenant_id}");
        if !self.enabled || !cfg!(feature = "postgres-collector") {
            return self.stop(&key).await;
        }

        let license = crate::license::evaluate();
        if !license.has_entitlement(POSTGRES_ENTITLEMENT) {
            tracing::info!(tenant = %tenant_id, status = %license.status, "PostgreSQL collector not licensed; keeping it stopped");
            return self.stop(&key).await;
        }

        let targets = self
            .config_db
            .list_integration_target_secrets(tenant_id, POSTGRES_INTEGRATION)
            .await?;
        let targets: Vec<_> = targets.into_iter().filter(|t| t.enabled).collect();
        let bootstrap_config = if targets.is_empty() {
            std::env::var("RUSH_POSTGRES_COLLECTOR_CONFIG")
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
                    "PostgreSQL collector config not found at {}; set RUSH_POSTGRES_COLLECTOR_CONFIG",
                    path.display()
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
            (write_collector_config(tenant_id, &targets)?, true)
        };
        let binary = std::env::var("RUSH_POSTGRES_COLLECTOR_BIN")
            .unwrap_or_else(|_| "../postgres-collector/target/debug/postgres-collector".into());
        if !Path::new(&binary).exists() {
            if cleanup_config {
                let _ = std::fs::remove_file(&config_path);
            }
            bail!(
                "PostgreSQL collector binary not found at {binary}; set RUSH_POSTGRES_COLLECTOR_BIN"
            );
        }

        let endpoint = std::env::var("RUSH_COLLECTOR_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:8080".into());
        let mut command = Command::new(&binary);
        command
            .env("PG_COLLECTOR_CONFIG", &config_path)
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
            .with_context(|| format!("starting PostgreSQL collector binary {binary}"))?;
        tracing::info!(tenant = %tenant_id, pid = ?child.id(), "started managed PostgreSQL collector");
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
    let contents = std::fs::read(path)
        .with_context(|| format!("read PostgreSQL collector config {}", path.display()))?;
    Ok(format!(
        "static:{}:{}",
        path.display(),
        hex::encode(sha256(&contents))
    ))
}

fn write_collector_config(tenant_id: &str, targets: &[IntegrationTargetSecret]) -> Result<PathBuf> {
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
    // JSON is a YAML 1.2 subset and is accepted by the PostgreSQL collector's
    // YAML parser. Emitting it with serde_json avoids the unmaintained
    // serde_yaml dependency while preserving the existing config contract.
    let contents = serde_json::to_string_pretty(&FileConfig { targets: named })?;
    let path = dir.join(format!("postgres-{tenant_id}.yaml"));
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

    #[test]
    fn descriptors_report_postgres_as_feature_gated() {
        let postgres = descriptors()
            .into_iter()
            .find(|d| d.id == POSTGRES_INTEGRATION)
            .unwrap();
        assert_eq!(postgres.entitlement, POSTGRES_ENTITLEMENT);
        assert_eq!(postgres.compiled, cfg!(feature = "postgres-collector"));
    }
}
