//! Tamper-evident audit log.
//!
//! Audit events are written to the append-only `observability.audit_events`
//! table as a hash-chained sequence: each row carries a monotonic `seq`, the
//! `prev_hash` of the row before it, and its own `hash`, where
//! `hash = HMAC_SHA256(secret, canonical(seq, prev_hash, fields))`.
//!
//! Because each `hash` covers the previous row's `hash`, any insertion,
//! deletion, reordering, or field mutation anywhere in the chain breaks every
//! `hash` from that point forward — making tampering detectable by
//! [`AuditLogger`]-side verification (see `verify` in `handlers::audit`).
//!
//! Writes are **serialized** through a single async mutex so `seq` and the
//! chain link are assigned atomically. The volume is low (security-relevant
//! events, not telemetry), so serialization is acceptable. Writes are also
//! **non-blocking on failure**: a DB error is logged and swallowed so a failed
//! audit write can never fail the caller's underlying operation.

use clickhouse::Client;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use tokio::sync::Mutex;

use axum::http::HeaderMap;

type HmacSha256 = Hmac<Sha256>;

/// The reserved tenant id/name that owns audit rows. Locked down everywhere
/// (see `resolve_tenant_from_headers` and `ensure_audit_tenant`): it can never
/// be selected as an ingest/query target via the public API.
pub const AUDIT_TENANT: &str = "_audit";

/// In-memory tail of the hash chain, guarded by a mutex so each `log` call
/// reads + advances it atomically.
struct ChainState {
    last_seq: u64,
    last_hash: String,
}

/// Serialized, hash-chaining audit writer. Construct once at startup and share
/// via `Arc` (it lives on `AppState`).
pub struct AuditLogger {
    ch: Client,
    secret: Vec<u8>,
    state: Mutex<ChainState>,
}

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
    out
}

/// Compute the chain hash for a row given the secret. Pure function shared by
/// the writer ([`AuditLogger::log`]) and the verifier (`handlers::audit::verify_audit`).
pub fn compute_hash(secret: &[u8], row: &AuditRow) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(canonical(row).as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

impl AuditLogger {
    /// Build the logger: read the HMAC secret from `RUSH_AUDIT_HMAC_SECRET`
    /// (warns once if empty/short, mirroring `hash_api_key`) and load the chain
    /// tail (`last_seq`, `last_hash`) from the table so a restart continues the
    /// same chain.
    pub async fn new(ch: Client) -> Self {
        let secret = std::env::var("RUSH_AUDIT_HMAC_SECRET").unwrap_or_default();
        if secret.len() < 32 {
            // An empty/short secret makes the HMAC chain forgeable by anyone who
            // can guess the (empty) key. Warn once — this is a startup path so a
            // single warning suffices.
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                tracing::warn!(
                    "RUSH_AUDIT_HMAC_SECRET is not set or shorter than 32 bytes; \
                     the audit hash chain is not tamper-evident — set a strong random \
                     secret in production"
                );
            });
        }

        let (last_seq, last_hash) = Self::load_chain_tail(&ch).await;

        AuditLogger {
            ch,
            secret: secret.into_bytes(),
            state: Mutex::new(ChainState {
                last_seq,
                last_hash,
            }),
        }
    }

    /// Load the highest `seq` and its `hash` from the table. Returns `(0, "")`
    /// when the table is empty (fresh chain) or on any read error (so startup
    /// never blocks on a transient CH issue — the chain simply restarts from 0,
    /// which verification will treat as a new segment).
    async fn load_chain_tail(ch: &Client) -> (u64, String) {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Tail {
            seq: u64,
            hash: String,
        }
        match ch
            .query("SELECT seq, hash FROM audit_events ORDER BY seq DESC LIMIT 1")
            .fetch_one::<Tail>()
            .await
        {
            Ok(t) => (t.seq, t.hash),
            Err(clickhouse::error::Error::RowNotFound) => (0, String::new()),
            Err(e) => {
                tracing::warn!(error = %e, "audit: failed to load chain tail at startup, starting from seq 0");
                (0, String::new())
            }
        }
    }

    /// Record an audit event. Serialized via the chain mutex.
    ///
    /// NON-BLOCKING CONTRACT: returns `()`. On any DB/serialization error it
    /// logs at `error` level and does NOT propagate — callers must never fail
    /// their operation because the audit write failed.
    pub async fn log(&self, ev: AuditEvent) {
        let mut state = self.state.lock().await;
        let seq = state.last_seq + 1;
        let prev_hash = state.last_hash.clone();

        // Build the row first (id + timestamp fixed) so the canonical string and
        // the inserted row use identical values — required for verification.
        let mut row = AuditRow {
            id: uuid::Uuid::new_v4().to_string(),
            seq,
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
            prev_hash,
            hash: String::new(),
        };
        row.hash = compute_hash(&self.secret, &row);

        // Insert via the typed row API (matches the schema column order).
        let mut insert = match self.ch.insert("audit_events") {
            Ok(i) => i,
            Err(e) => {
                tracing::error!(error = %e, "audit log write failed (insert init)");
                return;
            }
        };
        if let Err(e) = insert.write(&row).await {
            tracing::error!(error = %e, "audit log write failed (row write)");
            return;
        }
        if let Err(e) = insert.end().await {
            tracing::error!(error = %e, "audit log write failed (commit)");
            return;
        }

        // Only advance the in-memory chain tail after a successful write so a
        // failed write doesn't orphan the next row's prev_hash.
        state.last_seq = seq;
        state.last_hash = row.hash;
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

    /// Borrow the HMAC secret (used by the verify endpoint to recompute hashes).
    pub fn secret(&self) -> &[u8] {
        &self.secret
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

    fn sample_row(seq: u64, prev: &str) -> AuditRow {
        AuditRow {
            id: "id-1".into(),
            seq,
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
}
