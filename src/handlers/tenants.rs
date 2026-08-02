use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use uuid::Uuid;

use crate::AppState;
use crate::handlers::users::{require_admin, require_auth};

/// Per-signal on/off flags. Each field defaults to `true` (enabled) when
/// omitted, so a tenant with no explicit config ingests every signal.
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy)]
pub struct SignalFlags {
    #[serde(default = "default_true")]
    pub logs: bool,
    #[serde(default = "default_true")]
    pub apm: bool,
    #[serde(default = "default_true")]
    pub metrics: bool,
    #[serde(default = "default_true")]
    pub rum: bool,
}

fn default_true() -> bool {
    true
}

impl Default for SignalFlags {
    fn default() -> Self {
        SignalFlags {
            logs: true,
            apm: true,
            metrics: true,
            rum: true,
        }
    }
}

/// Resolve the effective signal flags for a tenant id (defaulting each to true).
async fn resolve_signal_flags(state: &AppState, tenant_id: &str) -> SignalFlags {
    SignalFlags {
        logs: state
            .config_db
            .tenant_signal_enabled(tenant_id, "logs")
            .await,
        apm: state
            .config_db
            .tenant_signal_enabled(tenant_id, "apm")
            .await,
        metrics: state
            .config_db
            .tenant_signal_enabled(tenant_id, "metrics")
            .await,
        rum: state
            .config_db
            .tenant_signal_enabled(tenant_id, "rum")
            .await,
    }
}

#[derive(serde::Deserialize)]
pub struct CreateTenantRequest {
    pub name: String,
    #[serde(default = "default_true")]
    pub auth_required: bool,
    #[serde(default = "default_true")]
    pub ingest_auth_required: bool,
    /// Optional per-signal enable flags; each defaults to enabled when omitted.
    #[serde(default)]
    pub signals: Option<SignalFlags>,
}

#[derive(serde::Deserialize)]
pub struct ToggleTenantRequest {
    pub enabled: bool,
}

#[derive(serde::Deserialize)]
pub struct SetAuthRequiredRequest {
    pub auth_required: bool,
}

#[derive(serde::Deserialize)]
pub struct SetIngestAuthRequiredRequest {
    pub ingest_auth_required: bool,
}

#[derive(serde::Serialize)]
pub struct TenantResponse {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub auth_required: bool,
    pub ingest_auth_required: bool,
    pub created_at: String,
    /// Per-signal ingest enable flags (each defaults true when no row exists).
    pub signals: SignalFlags,
}

impl TenantResponse {
    /// Build a response, resolving the tenant's signal flags from config.
    async fn build(state: &AppState, row: (String, String, bool, bool, String)) -> TenantResponse {
        let signals = resolve_signal_flags(state, &row.0).await;
        let ingest_auth_required = state
            .config_db
            .tenant_ingest_auth_required_checked(&row.0)
            .await
            .ok()
            .flatten()
            .unwrap_or(true);
        TenantResponse {
            id: row.0,
            name: row.1,
            enabled: row.2,
            auth_required: row.3,
            ingest_auth_required,
            created_at: row.4,
            signals,
        }
    }
}

pub async fn list_tenants(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_auth(&state, &headers).await?;

    let rows: Vec<(String, String, bool, bool, String)> = state
        .config_db
        .list_tenants()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .into_iter()
        // The reserved `_audit` tenant is not a user-facing tenant — never show
        // it in the switcher (it's disabled and locked down anyway).
        .filter(|(id, name, ..)| {
            !id.eq_ignore_ascii_case(crate::audit::AUDIT_TENANT)
                && !name.eq_ignore_ascii_case(crate::audit::AUDIT_TENANT)
        })
        .collect();

    let visible: Vec<(String, String, bool, bool, String)> = if caller.4 == "admin" {
        // Admins see all tenants
        rows
    } else {
        // Non-admins see only tenants accessible via their groups
        let (_, _, accessible_ids) = state
            .config_db
            .resolve_user_permissions(&caller.0)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
            })?;

        tracing::info!(user_id = %caller.0, username = %caller.1, accessible_tenant_ids = ?accessible_ids, "list_tenants: non-admin user");

        rows.into_iter()
            .filter(|(id, _, enabled, _, _)| *enabled && accessible_ids.contains(id))
            .collect()
    };

    let mut tenants: Vec<TenantResponse> = Vec::with_capacity(visible.len());
    for row in visible {
        tenants.push(TenantResponse::build(&state, row).await);
    }

    Ok(Json(serde_json::json!({ "tenants": tenants })))
}

pub async fn create_tenant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateTenantRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let name = req.name.trim().to_string();
    if name.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "name must not be empty".to_string(),
        ));
    }
    // `_audit` is reserved for the tamper-evident audit log — refuse to let an
    // admin create a normal tenant under that name/id (case-insensitive).
    if name.eq_ignore_ascii_case(crate::audit::AUDIT_TENANT) {
        return Err((
            StatusCode::BAD_REQUEST,
            "tenant name '_audit' is reserved".to_string(),
        ));
    }

    // Tenant names must be unique: telemetry rows and the X-Rush-Tenant header
    // are keyed by NAME, so duplicate names would silently merge/split data.
    // Case-insensitive to avoid "Test" vs "test" confusion. ClickHouse has no
    // transactions, so a concurrent create could still race past this check —
    // acceptable for an admin-only config endpoint.
    let existing = state.config_db.list_tenants().await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal error".to_string(),
        )
    })?;
    if existing
        .iter()
        .any(|(_, n, ..)| n.eq_ignore_ascii_case(&name))
    {
        return Err((
            StatusCode::CONFLICT,
            format!("a tenant named \"{name}\" already exists"),
        ));
    }

    let id = Uuid::new_v4().to_string();

    state
        .config_db
        .create_tenant(&id, &name, req.auth_required, req.ingest_auth_required)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    // Persist any explicitly-disabled signals. Default is enabled, so we only
    // need to write the ones turned off (writing enabled rows is harmless too).
    if let Some(flags) = req.signals {
        for (signal, enabled) in [
            ("logs", flags.logs),
            ("apm", flags.apm),
            ("metrics", flags.metrics),
            ("rum", flags.rum),
        ] {
            if !enabled {
                state
                    .config_db
                    .set_tenant_signal(&id, signal, false)
                    .await
                    .map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal error".to_string(),
                        )
                    })?;
            }
        }
    }

    let tenant = state
        .config_db
        .get_tenant(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "tenant created but not found".to_string(),
            )
        })?;

    // AUDIT: tenant creation. Target tenant is the affected resource.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("tenant.create", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(id.clone())
                .resource("tenant", id.clone())
                .changes(
                    serde_json::json!({
                        "name": name,
                        "auth_required": req.auth_required,
                        "ingest_auth_required": req.ingest_auth_required,
                    })
                    .to_string(),
                )
                .description("tenant created")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok((
        StatusCode::CREATED,
        Json(TenantResponse::build(&state, tenant).await),
    ))
}

pub async fn toggle_tenant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<ToggleTenantRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let updated = state
        .config_db
        .set_tenant_enabled(&id, req.enabled)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    if !updated {
        return Err((StatusCode::NOT_FOUND, "tenant not found".to_string()));
    }

    let tenant = state
        .config_db
        .get_tenant(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    // AUDIT: tenant enable/disable.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("tenant.toggle", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(id.clone())
                .resource("tenant", id.clone())
                .changes(serde_json::json!({ "enabled": req.enabled }).to_string())
                .description("tenant enabled state changed")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(TenantResponse::build(&state, tenant).await))
}

pub async fn delete_tenant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    if id == "default" {
        return Err((
            StatusCode::BAD_REQUEST,
            "cannot delete the default tenant".to_string(),
        ));
    }

    let deleted = state.config_db.delete_tenant(&id).await.map_err(|e| {
        tracing::error!(error = %e, "internal error");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    })?;

    if !deleted {
        return Err((StatusCode::NOT_FOUND, "tenant not found".to_string()));
    }

    // AUDIT: tenant deletion.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("tenant.delete", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(id.clone())
                .resource("tenant", id.clone())
                .description("tenant deleted")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn set_auth_required(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<SetAuthRequiredRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let updated = state
        .config_db
        .set_tenant_auth_required(&id, req.auth_required)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;

    if !updated {
        return Err((StatusCode::NOT_FOUND, "tenant not found".to_string()));
    }

    let tenant = state
        .config_db
        .get_tenant(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "internal error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    // AUDIT: tenant auth-required setting change.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("tenant.auth_required_change", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(id.clone())
                .resource("tenant", id.clone())
                .changes(serde_json::json!({ "auth_required": req.auth_required }).to_string())
                .description("tenant auth_required changed")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(TenantResponse::build(&state, tenant).await))
}

pub async fn set_ingest_auth_required(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<SetIngestAuthRequiredRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let before = state
        .config_db
        .tenant_ingest_auth_required_checked(&id)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to read tenant ingest auth policy");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;
    let updated = state
        .config_db
        .set_tenant_ingest_auth_required(&id, req.ingest_auth_required)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to update tenant ingest auth policy");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?;
    if !updated {
        return Err((StatusCode::NOT_FOUND, "tenant not found".to_string()));
    }
    let tenant = state
        .config_db
        .get_tenant(&id)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to load updated tenant");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    state
        .audit
        .log(
            crate::audit::AuditEvent::new("tenant.ingest_auth_required_change", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(id.clone())
                .resource("tenant", id.clone())
                .changes(
                    serde_json::json!({
                        "before": before,
                        "after": req.ingest_auth_required,
                    })
                    .to_string(),
                )
                .description("tenant ingest authentication requirement changed")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    Ok(Json(TenantResponse::build(&state, tenant).await))
}

// ── Per-tenant ingest signal enable/disable ────────────────────────────────

/// Dropped-event counts per signal (blocked ingest volume).
#[derive(serde::Serialize, Default)]
pub struct DroppedCounts {
    pub logs: u64,
    pub apm: u64,
    pub metrics: u64,
    pub rum: u64,
}

#[derive(serde::Serialize)]
pub struct TenantSignalsResponse {
    pub signals: SignalFlags,
    /// Events dropped per signal over the last 24h because the signal is
    /// disabled for this tenant.
    pub dropped: DroppedCounts,
}

/// PUT body for signal flags — any omitted field is left unchanged.
#[derive(serde::Deserialize)]
pub struct SetSignalsRequest {
    pub logs: Option<bool>,
    pub apm: Option<bool>,
    pub metrics: Option<bool>,
    pub rum: Option<bool>,
}

/// Best-effort dropped-event counts (last 24h) from the usage store. Returns
/// zeros if the query fails — visibility is non-critical.
async fn dropped_counts(state: &AppState, tenant_id: &str) -> DroppedCounts {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct Row {
        signal: String,
        events: u64,
    }
    let escaped = crate::query_builder::escape_string_literal(tenant_id);
    let sql = format!(
        "SELECT signal, sum(events_count) AS events \
         FROM observability.tenant_usage \
         WHERE tenant_id = '{escaped}' \
           AND signal IN ('logs_dropped','apm_dropped','metrics_dropped','rum_dropped') \
           AND bucket >= now() - INTERVAL 24 HOUR \
         GROUP BY signal"
    );
    let mut out = DroppedCounts::default();
    match crate::tenant_query(&state.ch, &sql, tenant_id)
        .fetch_all::<Row>()
        .await
    {
        Ok(rows) => {
            for r in rows {
                match r.signal.as_str() {
                    "logs_dropped" => out.logs = r.events,
                    "apm_dropped" => out.apm = r.events,
                    "metrics_dropped" => out.metrics = r.events,
                    "rum_dropped" => out.rum = r.events,
                    _ => {}
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, tenant_id = %tenant_id, "dropped_counts query failed");
        }
    }
    out
}

/// GET /api/v1/tenants/{id}/signals
pub async fn get_tenant_signals(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    // Verify tenant exists.
    state
        .config_db
        .get_tenant(&id)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    let signals = resolve_signal_flags(&state, &id).await;
    let dropped = dropped_counts(&state, &id).await;
    Ok(Json(TenantSignalsResponse { signals, dropped }))
}

/// PUT /api/v1/tenants/{id}/signals
pub async fn set_tenant_signals(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<SetSignalsRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    // Verify tenant exists.
    state
        .config_db
        .get_tenant(&id)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tenant not found".to_string()))?;

    for (signal, maybe_enabled) in [
        ("logs", req.logs),
        ("apm", req.apm),
        ("metrics", req.metrics),
        ("rum", req.rum),
    ] {
        if let Some(enabled) = maybe_enabled {
            state
                .config_db
                .set_tenant_signal(&id, signal, enabled)
                .await
                .map_err(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal error".to_string(),
                    )
                })?;
        }
    }

    // AUDIT: per-tenant signal enable/disable change.
    state
        .audit
        .log(
            crate::audit::AuditEvent::new("tenant.signals_change", "user")
                .actor(caller.0.clone(), caller.1.clone())
                .tenant(id.clone())
                .resource("tenant", id.clone())
                .changes(
                    serde_json::json!({
                        "logs": req.logs, "apm": req.apm, "metrics": req.metrics, "rum": req.rum
                    })
                    .to_string(),
                )
                .description("tenant signal flags changed")
                .context(crate::audit::actor_context_from_headers(&headers)),
        )
        .await;

    let signals = resolve_signal_flags(&state, &id).await;
    let dropped = dropped_counts(&state, &id).await;
    Ok(Json(TenantSignalsResponse { signals, dropped }))
}
