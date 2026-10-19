//! Tamper-evident audit log.
//!
//! Audit events are written to the append-only `observability.audit_events`
//! table as hash-chained sequences: each process owns a unique `chain_id`.
//! Within that chain each row carries a monotonic `seq`, the
//! `prev_hash` of the row before it, and its own `hash`, where
//! `hash = HMAC_SHA256(secret, canonical(seq, prev_hash, fields))`.
//!
//! Because each `hash` covers the previous row's `hash`, any insertion,
//! deletion, reordering, or field mutation anywhere in the chain breaks every
//! `hash` from that point forward — making tampering detectable by
//! [`AuditLogger`]-side verification (see `verify` in `handlers::audit`).
//!
//! Writes within a process are serialized through an async mutex. An advisory
//! file lock also coordinates processes sharing an outbox directory. Before
//! delivery, every row is fsynced to
//! a local ordered outbox. ClickHouse outages therefore retain events for later
//! replay while readiness and metrics expose the degraded state.

use clickhouse::Client;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::sync::Mutex;

use axum::http::HeaderMap;

use crate::self_metrics::SelfMetrics;

type HmacSha256 = Hmac<Sha256>;

/// The reserved tenant id/name that owns audit rows. Locked down everywhere
/// (see `resolve_tenant_from_headers` and `ensure_audit_tenant`): it can never
/// be selected as an ingest/query target via the public API.
pub const AUDIT_TENANT: &str = "_audit";

/// In-memory tail of the hash chain, guarded by a mutex so each `log` call
/// reads + advances it atomically.
#[derive(Default)]
struct ChainState {
    last_seq: u64,
    last_hash: String,
    key_id: String,
    segment_id: String,
}

/// Serialized, hash-chaining audit writer. Construct once at startup and share
/// via `Arc` (it lives on `AppState`).
pub struct AuditLogger {
    ch: Client,
    /// Unique per process lifetime. Restarts replay old chains before writing
    /// a new one; no two replicas allocate sequences in the same chain.
    chain_id: String,
    current_key_id: String,
    keys: HashMap<String, Vec<u8>>,
    spool_dir: PathBuf,
    spool_max_bytes: u64,
    degraded: AtomicBool,
    pending_events: AtomicU64,
    pending_bytes: AtomicU64,
    write_failures: AtomicU64,
    metrics: Arc<SelfMetrics>,
    state: Mutex<ChainState>,
}

#[derive(Debug, Clone, Copy)]
pub struct AuditHealth {
    pub ready: bool,
    pub pending_events: u64,
    pub pending_bytes: u64,
    pub max_bytes: u64,
    pub write_failures: u64,
}

/// Actor-id prefix for API keys acting as principals (`apikey:<key id>`).
/// [`AuditEvent::actor`] records such actors as `api_key`, so handlers that
/// build events with `"user"` stay correct when a service account calls them.
pub const API_KEY_ACTOR_PREFIX: &str = "apikey:";

/// One audit event to record. All fields are `String`; use `Default` +
/// struct-update syntax or [`AuditEvent::new`] + setters to build one.
///
/// `tenant_id` is the AFFECTED tenant (e.g. the user's tenant on login), not
/// where the row is stored — every row lives in `observability.audit_events`.
#[derive(Debug, Clone, Default)]
pub struct AuditEvent {
    pub tenant_id: String,
    pub actor_id: String,
    pub actor_name: String,
    /// One of: `user` | `system` | `api_key` | `anonymous`.
    pub actor_type: String,
    /// Dotted action name, e.g. `auth.login.failure`, `user.create`.
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    /// `success` | `failure`.
    pub outcome: String,
    pub ip_address: String,
    pub user_agent: String,
    pub request_id: String,
    /// JSON before/after diff (secrets MUST be redacted before this point).
    pub changes: String,
    pub description: String,
    /// Free-form JSON metadata.
    pub metadata: String,
}

impl AuditEvent {
    /// Start a new event with the required `action` + `actor_type`. Defaults
    /// `tenant_id` to the reserved audit tenant and `outcome` to `success`.
    pub fn new(action: impl Into<String>, actor_type: impl Into<String>) -> Self {
        AuditEvent {
            action: action.into(),
            actor_type: actor_type.into(),
            tenant_id: AUDIT_TENANT.to_string(),
            outcome: "success".to_string(),
            ..Default::default()
        }
    }

    pub fn tenant(mut self, v: impl Into<String>) -> Self {
        self.tenant_id = v.into();
        self
    }
    pub fn actor(mut self, id: impl Into<String>, name: impl Into<String>) -> Self {
        self.actor_id = id.into();
        self.actor_name = name.into();
        if self.actor_type == "user" && self.actor_id.starts_with(API_KEY_ACTOR_PREFIX) {
            self.actor_type = "api_key".to_string();
        }
        self
    }
    pub fn actor_name(mut self, v: impl Into<String>) -> Self {
        self.actor_name = v.into();
        self
    }
    pub fn resource(mut self, rtype: impl Into<String>, rid: impl Into<String>) -> Self {
        self.resource_type = rtype.into();
        self.resource_id = rid.into();
        self
    }
    pub fn outcome(mut self, v: impl Into<String>) -> Self {
        self.outcome = v.into();
        self
    }
    pub fn description(mut self, v: impl Into<String>) -> Self {
        self.description = v.into();
        self
    }
    pub fn changes(mut self, v: impl Into<String>) -> Self {
        self.changes = v.into();
        self
    }
    pub fn metadata(mut self, v: impl Into<String>) -> Self {
        self.metadata = v.into();
        self
    }
    /// Apply ip / user-agent / request-id extracted from request headers.
    pub fn context(mut self, ctx: (String, String, String)) -> Self {
        self.ip_address = ctx.0;
        self.user_agent = ctx.1;
        self.request_id = ctx.2;
        self
    }
}

/// A fully materialized audit row, ready to hash + insert. Built once so the
/// `id`/`timestamp` that go into the canonical string are the SAME values
/// written to the table (otherwise verification would never reproduce them).
#[derive(Debug, Clone, clickhouse::Row, serde::Serialize, serde::Deserialize)]
pub struct AuditRow {
    pub id: String,
    pub seq: u64,
    /// Empty for the historical global chain. Default also reads old outboxes.
    #[serde(default)]
    pub chain_id: String,
    /// Nanosecond unix timestamp (DateTime64(9) is encoded as i64 ns by the driver).
    pub timestamp: i64,
    pub tenant_id: String,
    pub actor_id: String,
    pub actor_name: String,
    pub actor_type: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    pub outcome: String,
    pub ip_address: String,
    pub user_agent: String,
    pub request_id: String,
    pub changes: String,
    pub description: String,
    pub metadata: String,
    /// Selects the HMAC verification key without exposing key material.
    pub key_id: String,
    /// Stable for one signing-key segment; changes on planned key rotation.
    pub segment_id: String,
    pub prev_hash: String,
    pub hash: String,
}

/// Canonical, deterministic serialization of an audit row for hashing.
///
/// FORMAT (stable — do not change without a chain migration): newline-joined
/// `key=value` pairs in this FIXED order, each value percent-style escaped so a
/// literal newline or backslash in a field cannot forge a field boundary:
///
/// ```text
/// seq=<u64>\n
/// prev_hash=<hex>\n
/// id=<uuid>\n
/// timestamp=<i64 ns>\n
/// tenant_id=<v>\n
/// actor_id=<v>\n
/// actor_name=<v>\n
/// actor_type=<v>\n
/// action=<v>\n
/// resource_type=<v>\n
/// resource_id=<v>\n
/// outcome=<v>\n
/// ip_address=<v>\n
/// user_agent=<v>\n
/// request_id=<v>\n
/// changes=<v>\n
/// description=<v>\n
/// metadata=<v>
/// ```
///
/// The trailing field has no newline. `hash` and `prev_hash`-as-its-own-field
/// are excluded from `<v>` escaping concerns because `prev_hash` is hex and
/// `hash` is the output, never an input.
fn canonical(row: &AuditRow) -> String {
    fn esc(s: &str) -> String {
        // Escape backslash first, then newline, so the inverse is unambiguous.
        s.replace('\\', "\\\\").replace('\n', "\\n")
    }
    let mut out = String::with_capacity(512);
    out.push_str(&format!("seq={}\n", row.seq));
    out.push_str(&format!("prev_hash={}\n", row.prev_hash));
    out.push_str(&format!("id={}\n", esc(&row.id)));
    out.push_str(&format!("timestamp={}\n", row.timestamp));
    out.push_str(&format!("tenant_id={}\n", esc(&row.tenant_id)));
    out.push_str(&format!("actor_id={}\n", esc(&row.actor_id)));
    out.push_str(&format!("actor_name={}\n", esc(&row.actor_name)));
    out.push_str(&format!("actor_type={}\n", esc(&row.actor_type)));
    out.push_str(&format!("action={}\n", esc(&row.action)));
    out.push_str(&format!("resource_type={}\n", esc(&row.resource_type)));
    out.push_str(&format!("resource_id={}\n", esc(&row.resource_id)));
    out.push_str(&format!("outcome={}\n", esc(&row.outcome)));
    out.push_str(&format!("ip_address={}\n", esc(&row.ip_address)));
    out.push_str(&format!("user_agent={}\n", esc(&row.user_agent)));
    out.push_str(&format!("request_id={}\n", esc(&row.request_id)));
    out.push_str(&format!("changes={}\n", esc(&row.changes)));
    out.push_str(&format!("description={}\n", esc(&row.description)));
    out.push_str(&format!("metadata={}", esc(&row.metadata)));
    // Rows written before QAPI-SEC-11 have empty key/segment columns after the
    // additive schema migration. Preserve their original canonical format so
    // verification remains valid. New segments bind both identifiers.
    if !row.key_id.is_empty() || !row.segment_id.is_empty() {
        out.push_str(&format!("\nkey_id={}", esc(&row.key_id)));
        out.push_str(&format!("\nsegment_id={}", esc(&row.segment_id)));
    }
    // Keep legacy hashes byte-for-byte stable. New rows bind their writer's
    // identity so they cannot be moved between chains without detection.
    if !row.chain_id.is_empty() {
        out.push_str(&format!("\nchain_id={}", esc(&row.chain_id)));
    }
    out
}

/// Compute the chain hash for a row given the secret. Pure function shared by
/// the writer ([`AuditLogger::log`]) and the verifier (`handlers::audit::verify_audit`).
pub fn compute_hash(secret: &[u8], row: &AuditRow) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(canonical(row).as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn is_production_environment(value: Option<&str>) -> bool {
    !matches!(
        value
            .map(|item| item.trim().to_ascii_lowercase())
            .as_deref(),
        Some("development" | "dev" | "local" | "test")
    )
}

fn validate_current_secret(secret: &str, production: bool) -> anyhow::Result<()> {
    if secret.len() < 32 && production {
        anyhow::bail!("RUSH_AUDIT_HMAC_SECRET must contain at least 32 bytes in production");
    }
    Ok(())
}

fn valid_key_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn load_key_config() -> anyhow::Result<(String, HashMap<String, Vec<u8>>)> {
    let environment = std::env::var("RUSH_ENVIRONMENT").ok();
    let production = is_production_environment(environment.as_deref());
    let current_key_id =
        std::env::var("RUSH_AUDIT_HMAC_KEY_ID").unwrap_or_else(|_| "primary".to_string());
    if !valid_key_id(&current_key_id) || current_key_id == "legacy" {
        anyhow::bail!(
            "RUSH_AUDIT_HMAC_KEY_ID must be 1-64 ASCII letters, digits, '.', '_' or '-' and may not be 'legacy'"
        );
    }

    let current_secret = std::env::var("RUSH_AUDIT_HMAC_SECRET").unwrap_or_default();
    validate_current_secret(&current_secret, production)?;
    if current_secret.len() < 32 {
        tracing::warn!(
            "RUSH_AUDIT_HMAC_SECRET is absent or weak; audit integrity is development-only"
        );
    }

    let mut keys: HashMap<String, Vec<u8>> = match std::env::var("RUSH_AUDIT_HMAC_PREVIOUS_KEYS") {
        Ok(value) if !value.trim().is_empty() => {
            let parsed: HashMap<String, String> = serde_json::from_str(&value).map_err(|_| {
                anyhow::anyhow!(
                    "RUSH_AUDIT_HMAC_PREVIOUS_KEYS must be a JSON object of key-id to secret"
                )
            })?;
            let mut result = HashMap::new();
            for (key_id, secret) in parsed {
                if !valid_key_id(&key_id) {
                    anyhow::bail!("invalid previous audit HMAC key id '{key_id}'");
                }
                if secret.len() < 32 {
                    anyhow::bail!("previous audit HMAC key '{key_id}' is shorter than 32 bytes");
                }
                result.insert(key_id, secret.into_bytes());
            }
            result
        }
        _ => HashMap::new(),
    };
    if keys.contains_key(&current_key_id) {
        anyhow::bail!("current audit key id must not also appear in previous keys");
    }
    keys.insert(current_key_id.clone(), current_secret.into_bytes());
    Ok((current_key_id, keys))
}

fn key_for_id<'a>(
    keys: &'a HashMap<String, Vec<u8>>,
    current_key_id: &str,
    row_key_id: &str,
) -> Option<&'a [u8]> {
    if row_key_id.is_empty() {
        keys.get("legacy")
            .or_else(|| keys.get(current_key_id))
            .map(Vec::as_slice)
    } else {
        keys.get(row_key_id).map(Vec::as_slice)
    }
}

fn create_private_directory(path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                anyhow::bail!("audit spool path must be a regular directory");
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir_all(path)?,
        Err(error) => return Err(error.into()),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn pending_files(path: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let candidate = entry.path();
        if candidate.extension().and_then(|ext| ext.to_str()) == Some("json") {
            let file_type = entry.file_type()?;
            if file_type.is_symlink() || !file_type.is_file() {
                anyhow::bail!("audit spool entry must be a regular file");
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&candidate, fs::Permissions::from_mode(0o600))?;
            }
            files.push(candidate);
        }
    }
    files.sort();
    Ok(files)
}

fn read_spool_row(path: &Path) -> anyhow::Result<(AuditRow, u64)> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        anyhow::bail!("audit spool entry must be a regular file");
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut payload = Vec::with_capacity(metadata.len() as usize);
    options.open(path)?.read_to_end(&mut payload)?;
    let row = serde_json::from_slice::<AuditRow>(&payload).map_err(|error| {
        anyhow::anyhow!("invalid audit outbox file {}: {error}", path.display())
    })?;
    Ok((row, payload.len() as u64))
}

fn spool_usage(path: &Path) -> anyhow::Result<(u64, u64)> {
    let files = pending_files(path)?;
    let mut bytes = 0_u64;
    for file in &files {
        bytes = bytes.saturating_add(fs::metadata(file)?.len());
    }
    Ok((files.len() as u64, bytes))
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

/// Each call opens a separate descriptor, so flock also excludes other logger
/// instances in this process. Closing the descriptor releases it on error or
/// cancellation. Never unlink the lock file, which would create a second lock.
async fn lock_spool_directory(path: &Path) -> anyhow::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::{fs::OpenOptionsExt, io::AsRawFd};
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        let file = options.open(path.join(".lock"))?;
        loop {
            // SAFETY: file owns a live descriptor for the duration of the call.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                return Ok(file);
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::WouldBlock {
                return Err(error.into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    #[cfg(not(unix))]
    anyhow::bail!("audit outbox locking requires a Unix platform")
}

impl AuditLogger {
    /// Validate existing signing keys and recover the durable outbox before
    /// starting this process's independent chain. Recovery errors fail startup;
    /// an unreadable previous tail is never treated as an empty chain.
    pub async fn new(ch: Client, metrics: Arc<SelfMetrics>) -> anyhow::Result<Self> {
        let (current_key_id, keys) = load_key_config()?;
        let spool_dir = PathBuf::from(
            std::env::var("RUSH_AUDIT_SPOOL_DIR")
                .unwrap_or_else(|_| "./data/audit-spool".to_string()),
        );
        let spool_max_bytes = std::env::var("RUSH_AUDIT_SPOOL_MAX_BYTES")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| anyhow::anyhow!("RUSH_AUDIT_SPOOL_MAX_BYTES must be an integer"))?
            .unwrap_or(256 * 1024 * 1024);
        if spool_max_bytes < 1024 * 1024 {
            anyhow::bail!("RUSH_AUDIT_SPOOL_MAX_BYTES must be at least 1048576");
        }
        Self::open(
            ch,
            metrics,
            current_key_id,
            keys,
            spool_dir,
            spool_max_bytes,
        )
        .await
    }

    async fn open(
        ch: Client,
        metrics: Arc<SelfMetrics>,
        current_key_id: String,
        keys: HashMap<String, Vec<u8>>,
        spool_dir: PathBuf,
        spool_max_bytes: u64,
    ) -> anyhow::Result<Self> {
        create_private_directory(&spool_dir)?;
        let _spool_lock = lock_spool_directory(&spool_dir).await?;
        let migrated = crate::migrations::upgrade_audit_writer_chains(&ch).await?;
        let required_key_ids = Self::load_required_key_ids(&ch).await?;
        for key_id in &required_key_ids {
            if key_for_id(&keys, &current_key_id, key_id).is_none() {
                anyhow::bail!(
                    "audit verification key '{}' is unavailable; add it to RUSH_AUDIT_HMAC_PREVIOUS_KEYS before rotating keys",
                    if key_id.is_empty() { "legacy" } else { &key_id }
                );
            }
        }
        Self::validate_configured_keys(&ch, &keys, &current_key_id, &required_key_ids).await?;

        let (pending_events, pending_bytes) = spool_usage(&spool_dir)?;
        let logger = AuditLogger {
            ch,
            chain_id: uuid::Uuid::new_v4().to_string(),
            current_key_id,
            keys,
            spool_dir,
            spool_max_bytes,
            degraded: AtomicBool::new(pending_events > 0),
            pending_events: AtomicU64::new(pending_events),
            pending_bytes: AtomicU64::new(pending_bytes),
            write_failures: AtomicU64::new(0),
            metrics,
            state: Mutex::new(ChainState::default()),
        };
        logger.publish_metrics();
        logger.recover_spool().await?;
        if let Err(error) = logger.flush_spool_locked().await {
            tracing::error!(%error, "audit outbox replay deferred; readiness is degraded");
        }
        drop(_spool_lock);
        if migrated {
            logger
                .log(
                    AuditEvent::new("schema.migrate", "system")
                        .resource("table", "audit_events")
                        .changes(
                            serde_json::json!({
                                "migration": "audit_writer_chains",
                                "added_column": "chain_id",
                                "legacy_default": ""
                            })
                            .to_string(),
                        ),
                )
                .await;
        }
        Ok(logger)
    }

    async fn load_chain_tail(ch: &Client, chain_id: &str) -> anyhow::Result<ChainState> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Tail {
            seq: u64,
            hash: String,
            key_id: String,
            segment_id: String,
        }
        match ch
            .query(
                "SELECT seq, hash, key_id, segment_id FROM audit_events WHERE chain_id = ? ORDER BY seq DESC LIMIT 1",
            )
            .bind(chain_id)
            .fetch_one::<Tail>()
            .await
        {
            Ok(t) => Ok(ChainState {
                last_seq: t.seq,
                last_hash: t.hash,
                key_id: t.key_id,
                segment_id: t.segment_id,
            }),
            Err(clickhouse::error::Error::RowNotFound) => Ok(ChainState {
                last_seq: 0,
                last_hash: String::new(),
                key_id: String::new(),
                segment_id: String::new(),
            }),
            Err(error) => Err(anyhow::anyhow!(
                "audit chain tail could not be read; refusing to restart the chain: {error}"
            )),
        }
    }

    async fn load_required_key_ids(ch: &Client) -> anyhow::Result<Vec<String>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct KeyRow {
            key_id: String,
        }
        ch.query("SELECT DISTINCT key_id FROM audit_events")
            .fetch_all::<KeyRow>()
            .await
            .map(|rows| rows.into_iter().map(|row| row.key_id).collect())
            .map_err(|error| {
                anyhow::anyhow!(
                    "audit verification key inventory could not be read; refusing startup: {error}"
                )
            })
    }

    async fn validate_configured_keys(
        ch: &Client,
        keys: &HashMap<String, Vec<u8>>,
        current_key_id: &str,
        required_key_ids: &[String],
    ) -> anyhow::Result<()> {
        const SELECT_ROW: &str = "SELECT id, seq, chain_id, ts AS timestamp, tenant_id, actor_id, actor_name, actor_type, action, resource_type, resource_id, outcome, ip_address, user_agent, request_id, changes, description, metadata, key_id, segment_id, prev_hash, hash FROM (SELECT *, toUnixTimestamp64Nano(timestamp) AS ts FROM audit_events WHERE key_id = ?) ORDER BY seq DESC LIMIT 1";
        for key_id in required_key_ids {
            let row = ch
                .query(SELECT_ROW)
                .bind(key_id)
                .fetch_one::<AuditRow>()
                .await
                .map_err(|error| {
                    anyhow::anyhow!("audit key validation row could not be read: {error}")
                })?;
            let secret = key_for_id(keys, current_key_id, key_id)
                .expect("required audit keys were checked before validation");
            if compute_hash(secret, &row) != row.hash {
                anyhow::bail!(
                    "audit HMAC key '{}' does not verify existing rows",
                    if key_id.is_empty() { "legacy" } else { key_id }
                );
            }
        }
        Ok(())
    }

    async fn recover_spool(&self) -> anyhow::Result<()> {
        let files = pending_files(&self.spool_dir)?;
        let mut tails = HashMap::<String, ChainState>::new();
        for path in files {
            let (row, bytes) = read_spool_row(&path)?;
            if !tails.contains_key(&row.chain_id) {
                tails.insert(
                    row.chain_id.clone(),
                    Self::load_chain_tail(&self.ch, &row.chain_id).await?,
                );
            }
            let state = tails.get_mut(&row.chain_id).expect("chain tail loaded");
            if row.seq <= state.last_seq {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct Existing {
                    hash: String,
                }
                let existing = self
                    .ch
                    .query("SELECT hash FROM audit_events WHERE chain_id = ? AND seq = ? LIMIT 1")
                    .bind(&row.chain_id)
                    .bind(row.seq)
                    .fetch_optional::<Existing>()
                    .await?;
                match existing {
                    Some(existing) if existing.hash == row.hash => {
                        self.remove_spool_file(&path, bytes)?;
                        continue;
                    }
                    _ => anyhow::bail!(
                        "audit outbox row {} conflicts with the persisted chain",
                        row.seq
                    ),
                }
            }
            if row.seq != state.last_seq + 1 || row.prev_hash != state.last_hash {
                anyhow::bail!("audit outbox ordering/link failure at sequence {}", row.seq);
            }
            let secret = self
                .secret_for_key(&row.key_id)
                .ok_or_else(|| anyhow::anyhow!("missing audit key '{}'", row.key_id))?;
            if compute_hash(secret, &row) != row.hash {
                anyhow::bail!("audit outbox integrity failure at sequence {}", row.seq);
            }
            state.last_seq = row.seq;
            state.last_hash = row.hash;
            state.key_id = row.key_id;
            state.segment_id = row.segment_id;
        }
        Ok(())
    }

    /// Record an audit event. Serialized via the chain mutex.
    ///
    /// NON-BLOCKING CONTRACT: returns `()`. On any DB/serialization error it
    /// logs at `error` level and does NOT propagate — callers must never fail
    /// their operation because the audit write failed.
    pub async fn log(&self, ev: AuditEvent) {
        let mut state = self.state.lock().await;
        let _spool_lock = match lock_spool_directory(&self.spool_dir).await {
            Ok(lock) => lock,
            Err(error) => {
                self.mark_failure();
                tracing::error!(%error, "audit outbox lock failed");
                return;
            }
        };
        let seq = state.last_seq + 1;
        let prev_hash = state.last_hash.clone();
        if state.segment_id.is_empty() || state.key_id != self.current_key_id {
            state.segment_id = uuid::Uuid::new_v4().to_string();
        }

        // Build the row first (id + timestamp fixed) so the canonical string and
        // the inserted row use identical values — required for verification.
        let mut row = AuditRow {
            id: uuid::Uuid::new_v4().to_string(),
            seq,
            chain_id: self.chain_id.clone(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            tenant_id: ev.tenant_id,
            actor_id: ev.actor_id,
            actor_name: ev.actor_name,
            actor_type: ev.actor_type,
            action: ev.action,
            resource_type: ev.resource_type,
            resource_id: ev.resource_id,
            outcome: ev.outcome,
            ip_address: ev.ip_address,
            user_agent: ev.user_agent,
            request_id: ev.request_id,
            changes: ev.changes,
            description: ev.description,
            metadata: ev.metadata,
            key_id: self.current_key_id.clone(),
            segment_id: state.segment_id.clone(),
            prev_hash,
            hash: String::new(),
        };
        let secret = self
            .secret_for_key(&self.current_key_id)
            .expect("current audit key is always configured");
        row.hash = compute_hash(secret, &row);

        if let Err(error) = self.persist_spool_row(&row) {
            self.mark_failure();
            tracing::error!(%error, seq, "audit outbox persistence failed; event was not delivered");
            return;
        }
        // The chain advances after durable local persistence, not after remote
        // delivery. Every later event therefore remains ordered during a CH outage.
        state.last_seq = seq;
        state.last_hash = row.hash.clone();
        state.key_id = self.current_key_id.clone();

        if let Err(error) = self.flush_spool_locked().await {
            self.mark_failure();
            tracing::error!(%error, "audit delivery failed; durable outbox retained for replay");
        }
    }

    fn persist_spool_row(&self, row: &AuditRow) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(row)?;
        // A different process may have delivered files in a shared directory.
        let (pending_events, pending_bytes) = spool_usage(&self.spool_dir)?;
        let projected = pending_bytes.saturating_add(payload.len() as u64);
        if projected > self.spool_max_bytes {
            anyhow::bail!(
                "audit outbox capacity exhausted ({projected} > {})",
                self.spool_max_bytes
            );
        }
        let name = format!("{:020}-{}.json", row.seq, row.id);
        let final_path = self.spool_dir.join(&name);
        let temp_path = self.spool_dir.join(format!(".{name}.tmp"));
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options.open(&temp_path)?;
        file.write_all(&payload)?;
        file.sync_all()?;
        fs::rename(&temp_path, &final_path)?;
        sync_directory(&self.spool_dir)?;
        self.pending_events
            .store(pending_events + 1, Ordering::Relaxed);
        self.pending_bytes.store(projected, Ordering::Relaxed);
        self.degraded.store(true, Ordering::Relaxed);
        self.publish_metrics();
        Ok(())
    }

    async fn flush_spool_locked(&self) -> anyhow::Result<()> {
        for path in pending_files(&self.spool_dir)? {
            let (row, bytes) = read_spool_row(&path)?;
            let secret = self
                .secret_for_key(&row.key_id)
                .ok_or_else(|| anyhow::anyhow!("missing audit key '{}'", row.key_id))?;
            if compute_hash(secret, &row) != row.hash {
                anyhow::bail!("audit outbox integrity failure at sequence {}", row.seq);
            }
            #[derive(clickhouse::Row, serde::Deserialize)]
            struct Existing {
                hash: String,
            }
            if let Some(existing) = self
                .ch
                .query("SELECT hash FROM audit_events WHERE chain_id = ? AND seq = ? LIMIT 1")
                .bind(&row.chain_id)
                .bind(row.seq)
                .fetch_optional::<Existing>()
                .await?
            {
                if existing.hash != row.hash {
                    anyhow::bail!("audit sequence {} already has a different hash", row.seq);
                }
                self.remove_spool_file(&path, bytes)?;
                continue;
            }

            // Never inherit fire-and-forget ingestion settings. end() must mean
            // durable storage before we remove the only local copy.
            let durable = self
                .ch
                .clone()
                .with_option("async_insert", "0")
                .with_option("wait_for_async_insert", "1");
            let mut insert = durable.insert("audit_events")?;
            insert.write(&row).await?;
            insert.end().await?;
            self.remove_spool_file(&path, bytes)?;
        }
        // Another process can drain a shared outbox between our attempts.
        self.pending_events.store(0, Ordering::Relaxed);
        self.pending_bytes.store(0, Ordering::Relaxed);
        self.degraded.store(false, Ordering::Relaxed);
        self.publish_metrics();
        Ok(())
    }

    fn remove_spool_file(&self, path: &Path, _bytes: u64) -> anyhow::Result<()> {
        fs::remove_file(path)?;
        sync_directory(&self.spool_dir)?;
        let (events, bytes) = spool_usage(&self.spool_dir)?;
        self.pending_events.store(events, Ordering::Relaxed);
        self.pending_bytes.store(bytes, Ordering::Relaxed);
        self.publish_metrics();
        Ok(())
    }

    async fn retry_pending(&self) -> anyhow::Result<()> {
        let _state = self.state.lock().await;
        let _spool_lock = lock_spool_directory(&self.spool_dir).await?;
        self.flush_spool_locked().await
    }

    fn mark_failure(&self) {
        self.degraded.store(true, Ordering::Relaxed);
        self.write_failures.fetch_add(1, Ordering::Relaxed);
        self.publish_metrics();
    }

    fn publish_metrics(&self) {
        let health = self.health();
        self.metrics.set_gauge(
            "rush_audit_degraded",
            &[],
            if health.ready { 0.0 } else { 1.0 },
        );
        self.metrics.set_gauge(
            "rush_audit_outbox_events",
            &[],
            health.pending_events as f64,
        );
        self.metrics
            .set_gauge("rush_audit_outbox_bytes", &[], health.pending_bytes as f64);
        self.metrics
            .set_gauge("rush_audit_outbox_max_bytes", &[], health.max_bytes as f64);
        self.metrics.set_gauge(
            "rush_audit_write_failures_total",
            &[],
            health.write_failures as f64,
        );
    }

    pub fn health(&self) -> AuditHealth {
        AuditHealth {
            ready: !self.degraded.load(Ordering::Relaxed),
            pending_events: self.pending_events.load(Ordering::Relaxed),
            pending_bytes: self.pending_bytes.load(Ordering::Relaxed),
            max_bytes: self.spool_max_bytes,
            write_failures: self.write_failures.load(Ordering::Relaxed),
        }
    }

    pub fn spawn_replayer(self: &Arc<Self>) {
        let logger = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if logger.pending_events.load(Ordering::Relaxed) == 0 {
                    continue;
                }
                if let Err(error) = logger.retry_pending().await {
                    logger.mark_failure();
                    tracing::warn!(%error, "audit outbox replay remains degraded");
                }
            }
        });
    }

    /// Spawn `log` on the tokio runtime so hot call sites don't await the write.
    /// Prefer awaiting `log` directly for low-volume paths; use this only where
    /// the request path is latency-sensitive.
    pub fn log_detached(self: &Arc<Self>, ev: AuditEvent) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            me.log(ev).await;
        });
    }

    pub fn secret_for_key(&self, key_id: &str) -> Option<&[u8]> {
        key_for_id(&self.keys, &self.current_key_id, key_id)
    }
}

/// Extract `(ip, user_agent, request_id)` from request headers for audit
/// context. Reusable across all instrumented call sites (phase 2).
///
/// - ip: first hop of `X-Forwarded-For`, else `X-Real-IP`, else `""`.
/// - user_agent: `User-Agent`, else `""`.
/// - request_id: `X-Request-Id`, else `""`.
pub fn actor_context_from_headers(headers: &HeaderMap) -> (String, String, String) {
    let ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_default();
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let request_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    (ip, user_agent, request_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_principals_are_recorded_as_api_key_actors() {
        let event = AuditEvent::new("monitor.mute", "user").actor("apikey:k1", "MCP server");
        assert_eq!(event.actor_type, "api_key");
        assert_eq!(event.actor_id, "apikey:k1");
        let user = AuditEvent::new("monitor.mute", "user").actor("u1", "alice");
        assert_eq!(user.actor_type, "user");
        // Explicit non-user actor types are never rewritten.
        let system = AuditEvent::new("x", "system").actor("apikey:k1", "n");
        assert_eq!(system.actor_type, "system");
    }

    fn sample_row(seq: u64, prev: &str) -> AuditRow {
        AuditRow {
            id: "id-1".into(),
            seq,
            chain_id: String::new(),
            timestamp: 1_700_000_000_000_000_000,
            tenant_id: "default".into(),
            actor_id: "u1".into(),
            actor_name: "alice".into(),
            actor_type: "user".into(),
            action: "auth.login.success".into(),
            resource_type: String::new(),
            resource_id: String::new(),
            outcome: "success".into(),
            ip_address: "1.2.3.4".into(),
            user_agent: "curl".into(),
            request_id: String::new(),
            changes: String::new(),
            description: String::new(),
            metadata: String::new(),
            key_id: "primary".into(),
            segment_id: "segment-1".into(),
            prev_hash: prev.into(),
            hash: String::new(),
        }
    }

    #[test]
    fn hash_is_deterministic() {
        let secret = b"0123456789012345678901234567890123456789";
        let row = sample_row(1, "");
        assert_eq!(compute_hash(secret, &row), compute_hash(secret, &row));
    }

    #[test]
    fn hash_changes_when_any_field_changes() {
        let secret = b"0123456789012345678901234567890123456789";
        let row = sample_row(1, "");
        let base = compute_hash(secret, &row);

        let mut r2 = row.clone();
        r2.action = "auth.login.failure".into();
        assert_ne!(base, compute_hash(secret, &r2));

        let mut r3 = row.clone();
        r3.seq = 2;
        assert_ne!(base, compute_hash(secret, &r3));

        let mut r4 = row.clone();
        r4.prev_hash = "deadbeef".into();
        assert_ne!(base, compute_hash(secret, &r4));
    }

    #[test]
    fn chain_links_via_prev_hash() {
        let secret = b"0123456789012345678901234567890123456789";
        let mut r1 = sample_row(1, "");
        r1.hash = compute_hash(secret, &r1);
        let mut r2 = sample_row(2, &r1.hash);
        r2.id = "id-2".into();
        r2.hash = compute_hash(secret, &r2);
        // r2's hash depends on r1's hash; mutating r1 would break r2's link.
        let mut r1_tampered = r1.clone();
        r1_tampered.actor_name = "mallory".into();
        let new_r1_hash = compute_hash(secret, &r1_tampered);
        assert_ne!(new_r1_hash, r1.hash);
        // The verifier would recompute r1's hash (mismatch) -> first_broken=1.
    }

    #[test]
    fn field_boundary_escaping_prevents_forgery() {
        let secret = b"0123456789012345678901234567890123456789";
        let mut a = sample_row(1, "");
        a.actor_name = "alice".into();
        a.actor_type = "user".into();
        let mut b = sample_row(1, "");
        // Without escaping, an injected newline could shift the value across the
        // key=value boundary and collide. Escaping must keep these distinct.
        b.actor_name = "alice\nactor_type=user".into();
        b.actor_type = String::new();
        assert_ne!(compute_hash(secret, &a), compute_hash(secret, &b));
    }

    #[test]
    fn key_and_segment_identifiers_are_bound_for_new_rows() {
        let secret = b"0123456789012345678901234567890123456789";
        let row = sample_row(1, "");
        let base = compute_hash(secret, &row);

        let mut changed_key = row.clone();
        changed_key.key_id = "rotated".into();
        assert_ne!(base, compute_hash(secret, &changed_key));

        let mut changed_segment = row;
        changed_segment.segment_id = "segment-2".into();
        assert_ne!(base, compute_hash(secret, &changed_segment));
    }

    #[test]
    fn legacy_rows_keep_the_pre_rotation_canonical_format() {
        let secret = b"0123456789012345678901234567890123456789";
        let mut row = sample_row(1, "");
        row.key_id.clear();
        row.segment_id.clear();
        let legacy = compute_hash(secret, &row);
        row.key_id = "primary".into();
        row.segment_id = "segment-1".into();
        assert_ne!(legacy, compute_hash(secret, &row));
    }

    #[test]
    fn audit_outbox_is_fsynced_before_delivery() {
        let path = std::env::temp_dir().join(format!("rush-audit-test-{}", uuid::Uuid::new_v4()));
        create_private_directory(&path).unwrap();
        let secret = b"0123456789012345678901234567890123456789".to_vec();
        let mut keys = HashMap::new();
        keys.insert("primary".to_string(), secret.clone());
        let logger = AuditLogger {
            chain_id: uuid::Uuid::new_v4().to_string(),
            ch: Client::default(),
            current_key_id: "primary".into(),
            keys,
            spool_dir: path.clone(),
            spool_max_bytes: 1024 * 1024,
            degraded: AtomicBool::new(false),
            pending_events: AtomicU64::new(0),
            pending_bytes: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            metrics: Arc::new(SelfMetrics::new()),
            state: Mutex::new(ChainState {
                last_seq: 0,
                last_hash: String::new(),
                key_id: String::new(),
                segment_id: String::new(),
            }),
        };
        let mut row = sample_row(1, "");
        row.hash = compute_hash(&secret, &row);

        logger.persist_spool_row(&row).unwrap();
        let files = pending_files(&path).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(read_spool_row(&files[0]).unwrap().0.hash, row.hash);
        assert_eq!(logger.health().pending_events, 1);
        assert!(!logger.health().ready);

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn audit_spool_rejects_symlink_directories_and_entries() {
        use std::os::unix::fs::symlink;

        let root =
            std::env::temp_dir().join(format!("rush-audit-symlink-test-{}", uuid::Uuid::new_v4()));
        let real = root.join("real");
        fs::create_dir_all(&real).unwrap();
        let linked = root.join("linked");
        symlink(&real, &linked).unwrap();
        assert!(create_private_directory(&linked).is_err());

        let target = root.join("target");
        fs::write(&target, b"{}").unwrap();
        symlink(&target, real.join("00000000000000000001-test.json")).unwrap();
        assert!(pending_files(&real).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn clickhouse_outage_retains_ordered_event_and_degrades_readiness() {
        let path = std::env::temp_dir().join(format!("rush-audit-test-{}", uuid::Uuid::new_v4()));
        create_private_directory(&path).unwrap();
        let secret = b"0123456789012345678901234567890123456789".to_vec();
        let mut keys = HashMap::new();
        keys.insert("primary".to_string(), secret);
        let logger = AuditLogger {
            chain_id: uuid::Uuid::new_v4().to_string(),
            ch: Client::default().with_url("http://127.0.0.1:9"),
            current_key_id: "primary".into(),
            keys,
            spool_dir: path.clone(),
            spool_max_bytes: 1024 * 1024,
            degraded: AtomicBool::new(false),
            pending_events: AtomicU64::new(0),
            pending_bytes: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            metrics: Arc::new(SelfMetrics::new()),
            state: Mutex::new(ChainState {
                last_seq: 0,
                last_hash: String::new(),
                key_id: String::new(),
                segment_id: String::new(),
            }),
        };

        logger.log(AuditEvent::new("test.mutation", "system")).await;

        let files = pending_files(&path).unwrap();
        assert_eq!(files.len(), 1);
        let row = read_spool_row(&files[0]).unwrap().0;
        assert_eq!(row.seq, 1);
        assert_eq!(row.action, "test.mutation");
        assert_eq!(logger.health().pending_events, 1);
        assert!(!logger.health().ready);
        assert_eq!(logger.health().write_failures, 1);

        fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn exhausted_outbox_is_fail_open_but_visible() {
        let path = std::env::temp_dir().join(format!("rush-audit-test-{}", uuid::Uuid::new_v4()));
        create_private_directory(&path).unwrap();
        let secret = b"0123456789012345678901234567890123456789".to_vec();
        let mut keys = HashMap::new();
        keys.insert("primary".to_string(), secret);
        let logger = AuditLogger {
            chain_id: uuid::Uuid::new_v4().to_string(),
            ch: Client::default(),
            current_key_id: "primary".into(),
            keys,
            spool_dir: path.clone(),
            spool_max_bytes: 1,
            degraded: AtomicBool::new(false),
            pending_events: AtomicU64::new(0),
            pending_bytes: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            metrics: Arc::new(SelfMetrics::new()),
            state: Mutex::new(ChainState {
                last_seq: 0,
                last_hash: String::new(),
                key_id: String::new(),
                segment_id: String::new(),
            }),
        };

        logger.log(AuditEvent::new("test.mutation", "system")).await;

        assert!(pending_files(&path).unwrap().is_empty());
        assert!(!logger.health().ready);
        assert_eq!(logger.health().write_failures, 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn production_is_secure_by_default_and_rejects_weak_audit_keys() {
        assert!(is_production_environment(None));
        assert!(is_production_environment(Some("production")));
        assert!(!is_production_environment(Some("development")));
        assert!(validate_current_secret("short", true).is_err());
        assert!(validate_current_secret("short", false).is_ok());
        assert!(validate_current_secret("01234567890123456789012345678901", true).is_ok());
    }
    const TEST_SECRET: &[u8] = b"0123456789012345678901234567890123456789";

    struct TestDirectory(PathBuf);
    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("rush-audit-regression-{}", uuid::Uuid::new_v4()));
            create_private_directory(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn test_logger(ch: Client, spool_dir: PathBuf) -> AuditLogger {
        create_private_directory(&spool_dir).unwrap();
        AuditLogger {
            ch,
            chain_id: uuid::Uuid::new_v4().to_string(),
            current_key_id: "primary".into(),
            keys: HashMap::from([("primary".into(), TEST_SECRET.to_vec())]),
            spool_dir,
            spool_max_bytes: 1024 * 1024,
            degraded: AtomicBool::new(false),
            pending_events: AtomicU64::new(0),
            pending_bytes: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            metrics: Arc::new(SelfMetrics::new()),
            state: Mutex::new(ChainState::default()),
        }
    }

    async fn open_test_logger(ch: Client, path: PathBuf) -> anyhow::Result<AuditLogger> {
        AuditLogger::open(
            ch,
            Arc::new(SelfMetrics::new()),
            "primary".into(),
            HashMap::from([("primary".into(), TEST_SECRET.to_vec())]),
            path,
            1024 * 1024,
        )
        .await
    }

    #[test]
    fn writer_identity_is_signed_and_old_outboxes_still_deserialize() {
        let mut row = sample_row(1, "");
        let legacy_hash = compute_hash(TEST_SECRET, &row);
        let mut json = serde_json::to_value(&row).unwrap();
        json.as_object_mut().unwrap().remove("chain_id");
        let old: AuditRow = serde_json::from_value(json).unwrap();
        assert_eq!(compute_hash(TEST_SECRET, &old), legacy_hash);
        row.chain_id = "writer-a".into();
        let first = compute_hash(TEST_SECRET, &row);
        assert_ne!(first, legacy_hash);
        row.chain_id = "writer-b".into();
        assert_ne!(first, compute_hash(TEST_SECRET, &row));
    }

    #[tokio::test]
    async fn concurrent_writers_use_independent_verifiable_chains() {
        let dir = TestDirectory::new();
        let client = Client::default().with_url("http://127.0.0.1:9");
        let a = test_logger(client.clone(), dir.0.join("a"));
        let b = test_logger(client, dir.0.join("b"));
        for _ in 0..3 {
            tokio::join!(
                a.log(AuditEvent::new("a.update", "system")),
                b.log(AuditEvent::new("b.update", "system"))
            );
        }
        let mut rows = Vec::new();
        for path in [&a.spool_dir, &b.spool_dir] {
            rows.extend(
                pending_files(path)
                    .unwrap()
                    .into_iter()
                    .map(|file| read_spool_row(&file).unwrap().0),
            );
        }
        assert_ne!(a.chain_id, b.chain_id);
        let result = crate::handlers::audit::verify_rows(&rows, |_| Some(TEST_SECRET));
        assert_eq!(result["intact"], true);
        assert_eq!(result["checked"], 6);
        rows[1].actor_name = "changed".into();
        let result = crate::handlers::audit::verify_rows(&rows, |_| Some(TEST_SECRET));
        assert_eq!(result["intact"], false);
        assert_eq!(result["first_broken_chain_id"], a.chain_id);
    }

    #[tokio::test]
    async fn durable_insert_failure_keeps_outbox_until_success() {
        use axum::{
            Router,
            extract::State,
            http::{StatusCode, Uri},
            routing::any,
        };
        #[derive(Default)]
        struct Fixture {
            queries: Mutex<Vec<String>>,
            fail_insert: AtomicBool,
        }
        async fn respond(State(f): State<Arc<Fixture>>, uri: Uri) -> (StatusCode, &'static str) {
            let query = uri.query().unwrap_or_default().to_string();
            let is_insert = query.contains("INSERT");
            f.queries.lock().await.push(query);
            if is_insert && f.fail_insert.load(Ordering::Relaxed) {
                (StatusCode::SERVICE_UNAVAILABLE, "disk unavailable")
            } else {
                (StatusCode::OK, "")
            }
        }
        let fixture = Arc::new(Fixture::default());
        fixture.fail_insert.store(true, Ordering::Relaxed);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new()
            .route("/", any(respond))
            .with_state(fixture.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let dir = TestDirectory::new();
        let logger = test_logger(
            Client::default()
                .with_url(url)
                .with_option("async_insert", "1")
                .with_option("wait_for_async_insert", "0"),
            dir.0.clone(),
        );
        logger.log(AuditEvent::new("settings.update", "user")).await;
        assert_eq!(pending_files(&dir.0).unwrap().len(), 1);
        assert!(!logger.health().ready);
        fixture.fail_insert.store(false, Ordering::Relaxed);
        logger.retry_pending().await.unwrap();
        assert!(pending_files(&dir.0).unwrap().is_empty());
        assert!(logger.health().ready);
        let queries = fixture.queries.lock().await;
        let inserts: Vec<_> = queries.iter().filter(|q| q.contains("INSERT")).collect();
        assert_eq!(inserts.len(), 2);
        for insert in inserts {
            let params: HashMap<_, _> = url::form_urlencoded::parse(insert.as_bytes())
                .into_owned()
                .collect();
            assert_eq!(params.get("async_insert").map(String::as_str), Some("0"));
            assert_eq!(
                params.get("wait_for_async_insert").map(String::as_str),
                Some("1")
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn shared_outbox_lock_excludes_other_writers_and_releases_on_drop() {
        let dir = TestDirectory::new();
        let first = lock_spool_directory(&dir.0).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), lock_spool_directory(&dir.0))
                .await
                .is_err()
        );
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), lock_spool_directory(&dir.0))
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires isolated ClickHouse at RUSH_TEST_CLICKHOUSE_URL"]
    async fn audit_migration_concurrency_and_restart_recovery() -> anyhow::Result<()> {
        let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
        let admin = Client::default().with_url(&url);
        let database = format!("audit_test_{}", uuid::Uuid::new_v4().simple());
        admin
            .query(&format!("CREATE DATABASE {database}"))
            .execute()
            .await?;
        let ch = admin
            .clone()
            .with_database(&database)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");
        let result: anyhow::Result<()> = async {
            crate::migrations::create_audit_table(&ch).await?;
            let mut legacy = sample_row(1, "");
            legacy.timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap();
            legacy.hash = compute_hash(TEST_SECRET, &legacy);
            let durable = ch.clone().with_option("async_insert", "0");
            let mut insert = durable.insert("audit_events")?;
            insert.write(&legacy).await?;
            insert.end().await?;
            ch.query("ALTER TABLE audit_events DROP COLUMN chain_id").execute().await?;
            // Migration upgrades an existing table without rewriting its hashes.
            crate::migrations::create_audit_table(&ch).await?;
            crate::migrations::create_audit_table(&ch).await?;
            let root = TestDirectory::new();
            let a = open_test_logger(ch.clone(), root.0.join("a")).await?;
            let b = open_test_logger(ch.clone(), root.0.join("b")).await?;
            for _ in 0..5 {
                tokio::join!(a.log(AuditEvent::new("a.update", "user")), b.log(AuditEvent::new("b.update", "user")));
            }
            assert!(a.health().ready && b.health().ready);
            // Exercise the shared-volume overlap used by rolling deployments.
            let shared = root.0.join("shared");
            let c = open_test_logger(ch.clone(), shared.clone()).await?;
            let d = open_test_logger(ch.clone(), shared).await?;
            for _ in 0..3 {
                tokio::join!(c.log(AuditEvent::new("c.update", "user")), d.log(AuditEvent::new("d.update", "user")));
            }
            c.retry_pending().await?;
            assert!(c.health().ready && d.health().ready);
            // Replay a chain left by a process that could not reach ClickHouse.
            let offline = test_logger(Client::default().with_url("http://127.0.0.1:9"), root.0.join("offline"));
            offline.log(AuditEvent::new("offline.update", "user")).await;
            offline.log(AuditEvent::new("offline.update", "user")).await;
            let restarted = open_test_logger(ch.clone(), offline.spool_dir.clone()).await?;
            assert_ne!(offline.chain_id, restarted.chain_id);
            assert!(restarted.health().ready);
            restarted.log(AuditEvent::new("restarted.update", "user")).await;
            let rows = ch.query("SELECT id, seq, chain_id, ts AS timestamp, tenant_id, actor_id, actor_name, actor_type, action, resource_type, resource_id, outcome, ip_address, user_agent, request_id, changes, description, metadata, key_id, segment_id, prev_hash, hash FROM (SELECT *, toUnixTimestamp64Nano(timestamp) AS ts FROM audit_events) ORDER BY chain_id, seq")
                .fetch_all::<AuditRow>().await?;
            assert_eq!(rows.len(), 21);
            assert_eq!(rows.iter().filter(|row| row.action == "schema.migrate").count(), 1);
            assert_eq!(crate::handlers::audit::verify_rows(&rows, |_| Some(TEST_SECRET))["intact"], true);
            // Match the list endpoint's cross-chain chronological order SQL.
            let newest = ch.query("SELECT id, seq, chain_id, ts AS timestamp, tenant_id, actor_id, actor_name, actor_type, action, resource_type, resource_id, outcome, ip_address, user_agent, request_id, changes, description, metadata, key_id, segment_id, prev_hash, hash FROM (SELECT *, toUnixTimestamp64Nano(timestamp) AS ts FROM audit_events) ORDER BY ts DESC, id DESC LIMIT 1")
                .fetch_one::<AuditRow>().await?;
            assert_eq!(newest.action, "restarted.update");
            Ok(())
        }.await;
        admin
            .query(&format!("DROP DATABASE {database}"))
            .execute()
            .await?;
        result
    }
}
