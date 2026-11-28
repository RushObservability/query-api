//! Admin-only read + verification API for the tamper-evident audit log.
//!
//! - `GET /api/v1/audit`        → filtered, paginated user-activity event list.
//! - `GET /api/v1/audit/verify` → recompute the whole hash chain and report
//!   whether it is intact (and the first broken seq if not).
//!
//! Both endpoints require the caller to be an admin (`require_admin`). All
//! user-supplied filter values are escaped via `escape_string_literal` before
//! being inlined into SQL.

use crate::timestamps::TimestampExt;
use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::Deserialize;

use crate::AppState;
use crate::audit::{AuditRow, compute_hash};
use crate::handlers::users::require_admin;
use crate::query_builder::escape_string_literal;

#[derive(Debug, Deserialize, Default)]
pub struct AuditQuery {
    /// Inclusive lower bound on `timestamp` (ISO-8601 or `YYYY-MM-DD HH:MM:SS`).
    pub from: Option<String>,
    /// Inclusive upper bound on `timestamp`.
    pub to: Option<String>,
    /// Match `actor_id` OR `actor_name` exactly.
    pub actor: Option<String>,
    /// `action` filter — prefix match if it ends with `.`/`*`, else exact.
    pub action: Option<String>,
    pub resource_type: Option<String>,
    pub outcome: Option<String>,
    pub tenant_id: Option<String>,
    /// Free-text substring over action / description / actor_name.
    pub q: Option<String>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

// Filter before pagination, not in the UI. Historical ingestion failures and
// system activity remain in the append-only chain but are not user activity.
const USER_ACTIVITY_FILTER: &str = "actor_type != 'system' \
    AND NOT startsWith(action, 'ingest.') \
    AND NOT (action IN ('apikey.scope_denied', 'sre_agent.scope_denied') \
        AND JSONExtractString(metadata, 'signal') NOT IN ('', 'collector'))";

/// GET /api/v1/audit — list audit events (admin only), newest first.
pub async fn list_audit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<AuditQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;

    let where_clause = audit_where_clause(&params);
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let offset = params.offset.unwrap_or(0);

    // Convert the DateTime64 `timestamp` to Int64 nanos for the driver, via a
    // subquery so the alias never collides with the source column of the same
    // name (ClickHouse 26.1 rejects `toUnixTimestamp64Nano(timestamp) AS timestamp`
    // with an AMBIGUOUS_COLUMN_NAME / block-structure-mismatch error).
    let sql = format!(
        "SELECT id, seq, chain_id, ts AS timestamp, tenant_id, \
         actor_id, actor_name, actor_type, action, resource_type, resource_id, outcome, \
         ip_address, user_agent, request_id, changes, description, metadata, key_id, segment_id, prev_hash, hash \
         FROM (SELECT *, toUnixTimestamp64Nano(timestamp) AS ts FROM audit_events {where_clause}) \
         ORDER BY ts DESC, id DESC LIMIT {limit} OFFSET {offset}"
    );

    let rows = state
        .admin_ch
        .query(&sql)
        .fetch_all::<AuditRow>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "audit list query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })?;

    let events: Vec<serde_json::Value> = rows.iter().map(audit_row_json).collect();

    Ok(Json(serde_json::json!({
        "events": events,
        "limit": limit,
        "offset": offset,
    })))
}

fn audit_where_clause(params: &AuditQuery) -> String {
    let mut conds: Vec<String> = vec![USER_ACTIVITY_FILTER.to_string()];

    if let Some(from) = params.from.as_deref().filter(|s| !s.is_empty()) {
        conds.push(format!(
            "timestamp >= parseDateTime64BestEffort('{}')",
            escape_string_literal(from)
        ));
    }
    if let Some(to) = params.to.as_deref().filter(|s| !s.is_empty()) {
        conds.push(format!(
            "timestamp <= parseDateTime64BestEffort('{}')",
            escape_string_literal(to)
        ));
    }
    if let Some(actor) = params.actor.as_deref().filter(|s| !s.is_empty()) {
        let e = escape_string_literal(actor);
        conds.push(format!("(actor_id = '{e}' OR actor_name = '{e}')"));
    }
    if let Some(action) = params.action.as_deref().filter(|s| !s.is_empty()) {
        // Treat a trailing '.' or '*' as a prefix match; otherwise exact.
        if let Some(prefix) = action.strip_suffix('*').or_else(|| {
            if action.ends_with('.') {
                Some(action)
            } else {
                None
            }
        }) {
            conds.push(format!(
                "startsWith(action, '{}')",
                escape_string_literal(prefix)
            ));
        } else {
            conds.push(format!("action = '{}'", escape_string_literal(action)));
        }
    }
    if let Some(rt) = params.resource_type.as_deref().filter(|s| !s.is_empty()) {
        conds.push(format!("resource_type = '{}'", escape_string_literal(rt)));
    }
    if let Some(outcome) = params.outcome.as_deref().filter(|s| !s.is_empty()) {
        conds.push(format!("outcome = '{}'", escape_string_literal(outcome)));
    }
    if let Some(tid) = params.tenant_id.as_deref().filter(|s| !s.is_empty()) {
        conds.push(format!("tenant_id = '{}'", escape_string_literal(tid)));
    }
    if let Some(q) = params.q.as_deref().filter(|s| !s.is_empty()) {
        let e = escape_string_literal(&q.to_lowercase());
        conds.push(format!(
            "(positionCaseInsensitive(action, '{e}') > 0 \
              OR positionCaseInsensitive(description, '{e}') > 0 \
              OR positionCaseInsensitive(actor_name, '{e}') > 0)"
        ));
    }

    format!("WHERE {}", conds.join(" AND "))
}

/// GET /api/v1/audit/verify — recompute the full hash chain (admin only).
///
/// Walks each writer chain in `seq ASC` order and recomputes each row's hash using the
/// SAME canonical function as the writer. Reports `intact`, how many rows were
/// `checked`, every broken chain in `broken_chains` (and the first one in
/// `first_broken_seq`/`first_broken_chain_id`) if any link fails to reproduce —
/// either because a row's own hash was tampered, or because its `prev_hash`
/// does not match the previous row's recomputed hash (insert/delete/reorder).
pub async fn verify_audit(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;

    let result = verify_table(&state.admin_ch, |key_id| state.audit.secret_for_key(key_id))
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "audit verify query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })?;
    Ok(Json(result))
}

/// Stream every row through [`ChainVerifier`] so memory stays bounded by the
/// number of chains, not the size of the audit table.
pub(crate) async fn verify_table<'a>(
    ch: &clickhouse::Client,
    secret_for_key: impl Fn(&str) -> Option<&'a [u8]>,
) -> clickhouse::error::Result<serde_json::Value> {
    let sql = "SELECT id, seq, chain_id, ts AS timestamp, tenant_id, \
         actor_id, actor_name, actor_type, action, resource_type, resource_id, outcome, \
         ip_address, user_agent, request_id, changes, description, metadata, key_id, segment_id, prev_hash, hash \
         FROM (SELECT *, toUnixTimestamp64Nano(timestamp) AS ts FROM audit_events) ORDER BY chain_id ASC, seq ASC";
    let mut cursor = ch.query(sql).fetch::<AuditRow>()?;
    let mut verifier = ChainVerifier::new(ttl_horizon_nanos());
    while let Some(row) = cursor.next().await? {
        verifier.push(&row, &secret_for_key);
    }
    Ok(verifier.result())
}

/// A chain whose oldest surviving row is older than this may have lost its
/// head to the `audit_events` TTL. The slack covers the gap between that row
/// and the expired one before it, and TTL merges running late.
fn ttl_horizon_nanos() -> i64 {
    const SLACK_DAYS: i64 = 7;
    // Same setting and default as `migrations::create_audit_table`.
    let retention_days: i64 = std::env::var("RUSH_AUDIT_RETENTION_DAYS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(730);
    let horizon = time::OffsetDateTime::now_utc()
        - time::Duration::days(retention_days.saturating_sub(SLACK_DAYS).max(0));
    horizon.unix_nanos().unwrap_or(i64::MIN)
}

/// Verifies rows one at a time, keeping only each chain's tail. Every chain is
/// checked to the end; a broken chain is reported once and then skipped.
pub(crate) struct ChainVerifier {
    checked: u64,
    /// `None` once the chain is broken.
    tails: std::collections::HashMap<String, Option<(u64, String)>>,
    broken: Vec<(String, u64)>,
    ttl_horizon: i64,
    expired_heads: u64,
}

impl ChainVerifier {
    pub(crate) fn new(ttl_horizon: i64) -> Self {
        Self {
            checked: 0,
            tails: std::collections::HashMap::new(),
            broken: Vec::new(),
            ttl_horizon,
            expired_heads: 0,
        }
    }

    /// Check the next row of its chain.
    pub(crate) fn push<'a>(
        &mut self,
        row: &AuditRow,
        secret_for_key: impl Fn(&str) -> Option<&'a [u8]>,
    ) {
        self.checked += 1;
        let signed =
            || secret_for_key(&row.key_id).is_some_and(|key| compute_hash(key, row) == row.hash);
        let valid = match self.tails.get(&row.chain_id) {
            Some(None) => return,
            Some(Some((seq, hash))) => row.seq == seq + 1 && row.prev_hash == *hash && signed(),
            None if row.seq == 1 => row.prev_hash.is_empty() && signed(),
            // Older rows of this chain expired by TTL; the rest must still link.
            None => {
                let expired = row.timestamp < self.ttl_horizon && signed();
                self.expired_heads += u64::from(expired);
                expired
            }
        };
        if !valid {
            self.broken.push((row.chain_id.clone(), row.seq));
            self.tails.insert(row.chain_id.clone(), None);
            return;
        }
        match self.tails.get_mut(&row.chain_id) {
            Some(Some(tail)) => {
                tail.0 = row.seq;
                tail.1.clone_from(&row.hash);
            }
            _ => {
                self.tails
                    .insert(row.chain_id.clone(), Some((row.seq, row.hash.clone())));
            }
        }
    }

    pub(crate) fn result(&self) -> serde_json::Value {
        let first = self.broken.first();
        serde_json::json!({
            "intact": self.broken.is_empty(),
            "checked": self.checked,
            "first_broken_seq": first.map(|(_, seq)| seq),
            "first_broken_chain_id": first.map(|(chain, _)| chain),
            "broken_chains": self
                .broken
                .iter()
                .map(|(chain, seq)| serde_json::json!({ "chain_id": chain, "seq": seq }))
                .collect::<Vec<_>>(),
            "ttl_expired_chain_heads": self.expired_heads,
        })
    }
}

/// Verify each writer independently, including the legacy chain with an empty
/// id. Input must be ordered by sequence within each chain.
#[cfg(test)]
pub(crate) fn verify_rows<'a>(
    rows: &[AuditRow],
    secret_for_key: impl Fn(&str) -> Option<&'a [u8]>,
) -> serde_json::Value {
    let mut verifier = ChainVerifier::new(ttl_horizon_nanos());
    for row in rows {
        verifier.push(row, &secret_for_key);
    }
    verifier.result()
}

/// Render an `AuditRow` as JSON with the timestamp as an RFC3339 string instead
/// of raw nanoseconds (and without re-exposing the chain internals confusingly).
fn audit_row_json(row: &AuditRow) -> serde_json::Value {
    let ts = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(row.timestamp))
        .expect("i64 nanoseconds are in range");
    serde_json::json!({
        "id": row.id,
        "seq": row.seq,
        "chain_id": row.chain_id,
        "timestamp": ts.to_rfc3339(),
        "tenant_id": row.tenant_id,
        "actor_id": row.actor_id,
        "actor_name": row.actor_name,
        "actor_type": row.actor_type,
        "action": row.action,
        "resource_type": row.resource_type,
        "resource_id": row.resource_id,
        "outcome": row.outcome,
        "ip_address": row.ip_address,
        "user_agent": row.user_agent,
        "request_id": row.request_id,
        "changes": row.changes,
        "description": row.description,
        "metadata": row.metadata,
        "key_id": row.key_id,
        "segment_id": row.segment_id,
        "prev_hash": row.prev_hash,
        "hash": row.hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_activity_filter_is_always_applied_before_pagination() {
        let clause = audit_where_clause(&AuditQuery::default());
        assert_eq!(clause, format!("WHERE {USER_ACTIVITY_FILTER}"));
        let filtered = audit_where_clause(&AuditQuery {
            action: Some("apikey.*".into()),
            outcome: Some("failure".into()),
            offset: Some(100),
            ..Default::default()
        });
        assert!(filtered.starts_with(&clause));
        assert!(filtered.contains("startsWith(action, 'apikey.')"));
        assert!(filtered.contains("outcome = 'failure'"));
        assert!(!filtered.contains("OFFSET"));
    }

    #[test]
    fn user_filters_remain_escaped() {
        let clause = audit_where_clause(&AuditQuery {
            actor: Some("alice' OR 1=1 --".into()),
            ..Default::default()
        });
        assert!(clause.starts_with(&format!("WHERE {USER_ACTIVITY_FILTER} AND ")));
        assert!(clause.contains(&escape_string_literal("alice' OR 1=1 --")));
    }

    const SECRET: &[u8] = b"0123456789012345678901234567890123456789";

    fn chain(chain_id: &str, n: u64) -> Vec<AuditRow> {
        let mut out: Vec<AuditRow> = Vec::new();
        for seq in 1..=n {
            let mut row = AuditRow {
                id: format!("{chain_id}-{seq}"),
                seq,
                chain_id: chain_id.into(),
                timestamp: 1_700_000_000_000_000_000 + seq as i64,
                tenant_id: "_audit".into(),
                actor_id: "u1".into(),
                actor_name: "admin".into(),
                actor_type: "user".into(),
                action: "settings.update".into(),
                resource_type: String::new(),
                resource_id: String::new(),
                outcome: "success".into(),
                ip_address: String::new(),
                user_agent: String::new(),
                request_id: String::new(),
                changes: String::new(),
                description: String::new(),
                metadata: String::new(),
                key_id: "primary".into(),
                segment_id: "s".into(),
                prev_hash: out.last().map(|r| r.hash.clone()).unwrap_or_default(),
                hash: String::new(),
            };
            row.hash = compute_hash(SECRET, &row);
            out.push(row);
        }
        out
    }

    #[tokio::test]
    async fn verify_streams_rows_from_clickhouse() {
        let mut rows = chain("writer-a", 3);
        rows.extend(chain("writer-b", 2));
        rows[3].actor_name = "changed".into();
        let mock = clickhouse::test::Mock::new();
        mock.add(clickhouse::test::handlers::provide(rows));
        let ch = clickhouse::Client::default()
            .with_url(mock.url())
            .with_validation(false);
        let result = verify_table(&ch, |_| Some(SECRET)).await.unwrap();
        assert_eq!(result["intact"], false);
        // Every row is read, including those after the break.
        assert_eq!(result["checked"], 5);
        assert_eq!(result["first_broken_chain_id"], "writer-b");
        assert_eq!(result["first_broken_seq"], 1);
    }

    #[test]
    fn ttl_expiry_is_tolerated_and_every_broken_chain_is_reported() {
        let now = time::OffsetDateTime::now_utc().unix_nanos().unwrap();
        let mut rows = chain("a-expired", 5);
        // TTL removed the oldest rows of a chain written before the horizon.
        rows.drain(0..2);
        let mut tampered = chain("b-tampered", 3);
        tampered[1].description = "changed".into();
        rows.extend(tampered);
        // The same gap in a recent chain cannot be TTL and is still reported.
        let mut recent = chain("c-recent", 4);
        for row in &mut recent {
            row.timestamp = now - 1_000;
            row.prev_hash.clear();
        }
        for i in 0..recent.len() {
            if i > 0 {
                recent[i].prev_hash = recent[i - 1].hash.clone();
            }
            recent[i].hash = compute_hash(SECRET, &recent[i]);
        }
        rows.extend(recent.drain(1..));
        rows.extend(chain("d-intact", 2));

        let result = verify_rows(&rows, |_| Some(SECRET));
        assert_eq!(result["intact"], false);
        assert_eq!(result["ttl_expired_chain_heads"], 1);
        assert_eq!(result["first_broken_chain_id"], "b-tampered");
        assert_eq!(result["first_broken_seq"], 2);
        assert_eq!(
            result["broken_chains"],
            serde_json::json!([
                { "chain_id": "b-tampered", "seq": 2 },
                { "chain_id": "c-recent", "seq": 2 },
            ])
        );

        // An expired head alone is not tampering.
        let mut rows = chain("a-expired", 5);
        rows.drain(0..2);
        assert_eq!(verify_rows(&rows, |_| Some(SECRET))["intact"], true);
    }

    #[tokio::test]
    #[ignore = "requires a local ClickHouse at RUSH_TEST_CLICKHOUSE_URL or localhost:8123"]
    async fn user_activity_filter_matches_clickhouse_events() {
        let client = clickhouse::Client::default()
            .with_url(
                std::env::var("RUSH_TEST_CLICKHOUSE_URL")
                    .unwrap_or_else(|_| "http://127.0.0.1:8123".into()),
            )
            .with_user(
                std::env::var("RUSH_TEST_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
            )
            .with_password(std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default());
        // Constant rows only: this test never reads or changes stored audit data.
        for (actor, action, metadata, expected) in [
            ("user", "settings.update", "", 1),
            ("user", "user.delete", "", 1),
            ("user", "group.delete", "", 1),
            ("user", "tenant.ingest_auth_required_change", "", 1),
            ("user", "apikey.create", "", 1),
            ("user", "auth.login.success", "", 1),
            ("anonymous", "auth.login.failure", "", 1),
            ("api_key", "settings.update", "", 1),
            ("system", "schema.migrate", "", 0),
            ("system", "user.create", "", 0),
            ("anonymous", "ingest.auth_denied", "", 0),
            ("user", "ingest.auth_denied", "", 0),
            ("api_key", "apikey.scope_denied", r#"{"signal":"logs"}"#, 0),
            (
                "api_key",
                "apikey.scope_denied",
                r#"{"signal":"metrics","reason":"rate_limit_exceeded"}"#,
                0,
            ),
            (
                "api_key",
                "apikey.scope_denied",
                r#"{"signal":"control"}"#,
                0,
            ),
            (
                "api_key",
                "apikey.scope_denied",
                r#"{"signal":"collector"}"#,
                1,
            ),
            ("api_key", "apikey.scope_denied", r#"{"signal":null}"#, 1),
            ("api_key", "apikey.scope_denied", "", 1),
        ] {
            let sql = format!(
                "SELECT count() FROM (SELECT '{}' AS actor_type, '{}' AS action, '{}' AS metadata) WHERE {USER_ACTIVITY_FILTER}",
                escape_string_literal(actor),
                escape_string_literal(action),
                escape_string_literal(metadata),
            );
            let count = client.query(&sql).fetch_one::<u64>().await.unwrap();
            assert_eq!(count, expected, "{actor}: {action} {metadata}");
        }
    }
}
