use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Extension, Json,
};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::AppState;
use crate::TenantContext;
use crate::handlers::users::{require_admin, require_auth};

// ── Query params ──

#[derive(Debug, Deserialize)]
pub struct SummaryParams {
    pub from: Option<String>,
    pub to: Option<String>,
    /// When true, returns usage across ALL tenants (ignores X-Rush-Tenant header).
    pub global: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct BreakdownParams {
    pub from: Option<String>,
    pub to: Option<String>,
    pub interval: Option<String>, // "hour" or "day"
    pub signal: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TenantsParams {
    pub from: Option<String>,
    pub to: Option<String>,
    pub sort_by: Option<String>, // "bytes" or "events"
    pub limit: Option<u64>,
}

// ── ClickHouse row types ──

#[derive(Debug, Clone, Deserialize, Row)]
struct SignalUsageRow {
    signal: String,
    events: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct BreakdownRow {
    ts: String,
    signal: String,
    events: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct TenantSignalRow {
    tenant_id: String,
    signal: String,
    events: u64,
    bytes: u64,
}

// ── Response types ──

#[derive(Debug, Serialize)]
pub struct SignalCounts {
    pub events_count: u64,
    pub bytes_count: u64,
}

#[derive(Debug, Serialize)]
pub struct UsageSummaryResponse {
    pub tenant_id: String,
    pub from: String,
    pub to: String,
    pub signals: HashMap<String, SignalCounts>,
    pub totals: SignalCounts,
}

#[derive(Debug, Serialize)]
pub struct BreakdownBucket {
    pub timestamp: String,
    pub signals: HashMap<String, SignalCounts>,
}

#[derive(Debug, Serialize)]
pub struct UsageBreakdownResponse {
    pub tenant_id: String,
    pub interval: String,
    pub buckets: Vec<BreakdownBucket>,
}

#[derive(Debug, Serialize)]
pub struct TenantUsageEntry {
    pub tenant_id: String,
    pub events_count: u64,
    pub bytes_count: u64,
    pub signals: HashMap<String, SignalCounts>,
}

#[derive(Debug, Serialize)]
pub struct UsageTenantsResponse {
    pub tenants: Vec<TenantUsageEntry>,
}

// ── Handlers ──

/// GET /api/v1/usage/summary — Per-tenant totals for all signals in a time range.
pub async fn usage_summary(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Query(params): Query<SummaryParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = tenant_id.replace('\'', "\\'");
    let is_global = params.global.unwrap_or(false);

    let now = chrono::Utc::now();
    let from = params.from.unwrap_or_else(|| (now - chrono::Duration::hours(24)).to_rfc3339());
    let to = params.to.unwrap_or_else(|| now.to_rfc3339());

    let tenant_filter = if is_global {
        String::new()
    } else {
        format!("tenant_id = '{escaped_tenant}' AND ")
    };

    let sql = format!(
        "SELECT \
            signal, \
            sum(events_count) AS events, \
            sum(bytes_count) AS bytes \
         FROM observability.tenant_usage \
         WHERE {tenant_filter}bucket >= parseDateTimeBestEffort('{from}') \
           AND bucket <= parseDateTimeBestEffort('{to}') \
         GROUP BY signal"
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<SignalUsageRow>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, handler = "usage_summary", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    let mut signals = HashMap::new();
    let mut total_events = 0u64;
    let mut total_bytes = 0u64;

    for row in rows {
        total_events += row.events;
        total_bytes += row.bytes;
        signals.insert(
            row.signal,
            SignalCounts {
                events_count: row.events,
                bytes_count: row.bytes,
            },
        );
    }

    Ok(Json(UsageSummaryResponse {
        tenant_id: tenant_id.clone(),
        from,
        to,
        signals,
        totals: SignalCounts {
            events_count: total_events,
            bytes_count: total_bytes,
        },
    }))
}

/// GET /api/v1/usage/breakdown — Time-series breakdown for a tenant.
pub async fn usage_breakdown(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Query(params): Query<BreakdownParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_auth(&state, &headers).await?;
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = tenant_id.replace('\'', "\\'");

    let now = chrono::Utc::now();
    let from = params.from.unwrap_or_else(|| (now - chrono::Duration::days(7)).to_rfc3339());
    let to = params.to.unwrap_or_else(|| now.to_rfc3339());
    let interval = params.interval.as_deref().unwrap_or("hour");

    let ts_expr = match interval {
        "day" => "toStartOfDay(bucket)",
        _ => "toStartOfHour(bucket)",
    };

    let mut sql = format!(
        "SELECT \
            toString({ts_expr}) AS ts, \
            signal, \
            sum(events_count) AS events, \
            sum(bytes_count) AS bytes \
         FROM observability.tenant_usage \
         WHERE tenant_id = '{escaped_tenant}' \
           AND bucket >= parseDateTimeBestEffort('{from}') \
           AND bucket <= parseDateTimeBestEffort('{to}')"
    );

    if let Some(ref sig) = params.signal {
        let escaped_signal = sig.replace('\'', "\\'");
        sql.push_str(&format!(" AND signal = '{escaped_signal}'"));
    }

    sql.push_str(&format!(" GROUP BY ts, signal ORDER BY ts"));

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<BreakdownRow>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, handler = "usage_breakdown", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    // Group by timestamp
    let mut buckets_map: HashMap<String, HashMap<String, SignalCounts>> = HashMap::new();
    for row in rows {
        buckets_map
            .entry(row.ts.clone())
            .or_default()
            .insert(
                row.signal,
                SignalCounts {
                    events_count: row.events,
                    bytes_count: row.bytes,
                },
            );
    }

    let mut buckets: Vec<BreakdownBucket> = buckets_map
        .into_iter()
        .map(|(ts, signals)| BreakdownBucket {
            timestamp: ts,
            signals,
        })
        .collect();
    buckets.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    Ok(Json(UsageBreakdownResponse {
        tenant_id: tenant_id.clone(),
        interval: interval.to_string(),
        buckets,
    }))
}

/// GET /api/v1/usage/tenants — Ranked list of all tenants by volume (admin only).
pub async fn usage_tenants(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<TenantsParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let now = chrono::Utc::now();
    let from = params.from.unwrap_or_else(|| (now - chrono::Duration::hours(24)).to_rfc3339());
    let to = params.to.unwrap_or_else(|| now.to_rfc3339());
    let limit = params.limit.unwrap_or(50).min(500);

    let sql = format!(
        "SELECT \
            tenant_id, \
            signal, \
            sum(events_count) AS events, \
            sum(bytes_count) AS bytes \
         FROM observability.tenant_usage \
         WHERE bucket >= parseDateTimeBestEffort('{from}') \
           AND bucket <= parseDateTimeBestEffort('{to}') \
         GROUP BY tenant_id, signal"
    );

    let rows = state
        .ch
        .query(&sql)
        .fetch_all::<TenantSignalRow>()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, handler = "usage_tenants", "query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
        })?;

    // Pivot: (tenant_id, signal) rows -> nested tenant entries
    let mut tenant_map: HashMap<String, TenantUsageEntry> = HashMap::new();
    for row in rows {
        let entry = tenant_map.entry(row.tenant_id.clone()).or_insert_with(|| {
            TenantUsageEntry {
                tenant_id: row.tenant_id.clone(),
                events_count: 0,
                bytes_count: 0,
                signals: HashMap::new(),
            }
        });
        entry.events_count += row.events;
        entry.bytes_count += row.bytes;
        entry.signals.insert(
            row.signal,
            SignalCounts {
                events_count: row.events,
                bytes_count: row.bytes,
            },
        );
    }

    let sort_by = params.sort_by.as_deref().unwrap_or("bytes");
    let mut tenants: Vec<TenantUsageEntry> = tenant_map.into_values().collect();
    match sort_by {
        "events" => tenants.sort_by(|a, b| b.events_count.cmp(&a.events_count)),
        _ => tenants.sort_by(|a, b| b.bytes_count.cmp(&a.bytes_count)),
    }
    tenants.truncate(limit as usize);

    Ok(Json(UsageTenantsResponse { tenants }))
}
