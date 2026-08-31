//! Admin-only read + verification API for the tamper-evident audit log.
//!
//! - `GET /api/v1/audit`        → filtered, paginated, newest-first event list.
//! - `GET /api/v1/audit/verify` → recompute the whole hash chain and report
//!   whether it is intact (and the first broken seq if not).
//!
//! Both endpoints require the caller to be an admin (`require_admin`). All
//! user-supplied filter values are escaped via `escape_string_literal` before
//! being inlined into SQL.

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

/// GET /api/v1/audit — list audit events (admin only), newest first.
pub async fn list_audit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<AuditQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;

    let mut conds: Vec<String> = Vec::new();

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

    let where_clause = if conds.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conds.join(" AND "))
    };

    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let offset = params.offset.unwrap_or(0);

    // Convert the DateTime64 `timestamp` to Int64 nanos for the driver, via a
    // subquery so the alias never collides with the source column of the same
    // name (ClickHouse 26.1 rejects `toUnixTimestamp64Nano(timestamp) AS timestamp`
    // with an AMBIGUOUS_COLUMN_NAME / block-structure-mismatch error).
    let sql = format!(
        "SELECT id, seq, ts AS timestamp, tenant_id, \
         actor_id, actor_name, actor_type, action, resource_type, resource_id, outcome, \
         ip_address, user_agent, request_id, changes, description, metadata, key_id, segment_id, prev_hash, hash \
         FROM (SELECT *, toUnixTimestamp64Nano(timestamp) AS ts FROM audit_events {where_clause}) \
         ORDER BY seq DESC LIMIT {limit} OFFSET {offset}"
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

/// GET /api/v1/audit/verify — recompute the full hash chain (admin only).
///
/// Walks the chain in `seq ASC` order and recomputes each row's hash using the
/// SAME canonical function as the writer. Reports `intact`, how many rows were
/// `checked`, and the `first_broken_seq` if any link fails to reproduce —
/// either because a row's own hash was tampered, or because its `prev_hash`
/// does not match the previous row's recomputed hash (insert/delete/reorder).
pub async fn verify_audit(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;

    let sql = "SELECT id, seq, ts AS timestamp, tenant_id, \
         actor_id, actor_name, actor_type, action, resource_type, resource_id, outcome, \
         ip_address, user_agent, request_id, changes, description, metadata, key_id, segment_id, prev_hash, hash \
         FROM (SELECT *, toUnixTimestamp64Nano(timestamp) AS ts FROM audit_events) ORDER BY seq ASC";

    let rows = state
        .admin_ch
        .query(sql)
        .fetch_all::<AuditRow>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "audit verify query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })?;

    let mut checked: u64 = 0;
    let mut first_broken_seq: Option<u64> = None;
    let mut expected_prev = String::new();

    for row in &rows {
        checked += 1;
        // Link check: this row's prev_hash must equal the previous row's hash.
        if row.prev_hash != expected_prev {
            first_broken_seq = Some(row.seq);
            break;
        }
        // Integrity check: recompute this row's hash from its fields.
        let Some(secret) = state.audit.secret_for_key(&row.key_id) else {
            first_broken_seq = Some(row.seq);
            break;
        };
        let recomputed = compute_hash(secret, row);
        if recomputed != row.hash {
            first_broken_seq = Some(row.seq);
            break;
        }
        expected_prev = row.hash.clone();
    }

    Ok(Json(serde_json::json!({
        "intact": first_broken_seq.is_none(),
        "checked": checked,
        "first_broken_seq": first_broken_seq,
    })))
}

/// Render an `AuditRow` as JSON with the timestamp as an RFC3339 string instead
/// of raw nanoseconds (and without re-exposing the chain internals confusingly).
fn audit_row_json(row: &AuditRow) -> serde_json::Value {
    let ts = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(row.timestamp);
    serde_json::json!({
        "id": row.id,
        "seq": row.seq,
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
