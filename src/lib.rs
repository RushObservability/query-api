pub mod alert_engine;
pub mod anomaly_engine;
pub mod ch_writer;
pub mod clickhouse_config;
pub mod config;
pub mod eval_state;
pub mod handlers;
pub mod metric_firewall;
pub mod migrations;
pub mod models;
pub mod monitor_engine;
pub mod object_store_spool;
pub mod promql;
pub mod query_builder;
pub mod retention_enforcer;
pub mod rollup;
pub mod saml;
pub mod siem_engine;
pub mod slo_engine;
pub mod spool;
pub mod stats_engine;
pub mod usage_accumulator;
pub mod usage_tracker;

use clickhouse::Client;
use clickhouse::query::Query;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use ch_writer::ChWriter;
use config::RushConfig;
use clickhouse_config::ConfigDb;
use usage_accumulator::UsageAccumulator;
use usage_tracker::UsageTracker;

/// Tenant context extracted from the authenticated request by middleware.
/// Every handler that queries ClickHouse must use this to scope data access.
#[derive(Clone, Debug)]
pub struct TenantContext {
    pub tenant_id: String,
}

/// Tri-state flag for whether ClickHouse accepts the `rush_tenant_id` custom setting.
/// 0 = untested, 1 = supported, 2 = not supported (graceful fallback).
static ROW_POLICY_SUPPORTED: AtomicU8 = AtomicU8::new(0);

/// Probe ClickHouse once at startup to see if custom_settings_prefixes includes 'rush_'.
/// If not, we skip injecting the per-query setting (row policies stay permissive).
pub async fn probe_row_policy_support(ch: &Client) {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct Probe { n: u8 }
    let result = ch
        .query("SELECT 1 AS n")
        .with_option("rush_tenant_id", "probe")
        .fetch_one::<Probe>()
        .await;
    match result {
        Ok(_) => {
            tracing::info!("ClickHouse accepts rush_tenant_id custom setting — row policies enforcing");
            ROW_POLICY_SUPPORTED.store(1, Ordering::Relaxed);
        }
        Err(_) => {
            tracing::warn!(
                "ClickHouse does not accept rush_tenant_id custom setting — row policies permissive. \
                 To enable, add custom_settings_prefixes='rush_' to your ClickHouse server config."
            );
            ROW_POLICY_SUPPORTED.store(2, Ordering::Relaxed);
        }
    }
}

/// Returns true if ClickHouse supports the rush_tenant_id custom setting.
pub fn row_policy_supported() -> bool {
    ROW_POLICY_SUPPORTED.load(Ordering::Relaxed) == 1
}

/// Create a ClickHouse query, optionally with the `rush_tenant_id` setting for row policy
/// enforcement. If ClickHouse doesn't support the custom setting (no `custom_settings_prefixes`
/// configured), the query runs without it — the API-layer WHERE clause is still the primary
/// tenant isolation mechanism.
pub fn tenant_query(ch: &Client, sql: &str, tenant_id: &str) -> Query {
    // Read guardrails: cap result sets so a single pathological query (PromQL over a
    // huge range, export with a wide window, etc.) cannot stream unbounded rows into
    // this process. `break` truncates silently at the cap instead of erroring, which
    // is acceptable for the read path. Note: deliberately NOT setting readonly=2 here
    // because this client is shared with paths that set their own settings.
    let q = ch
        .query(sql)
        .with_option("max_result_rows", "500000")
        .with_option("result_overflow_mode", "break");
    if ROW_POLICY_SUPPORTED.load(Ordering::Relaxed) == 1 {
        q.with_option("rush_tenant_id", tenant_id)
    } else {
        q
    }
}

#[derive(Clone)]
pub struct AppState {
    pub ch: Client,
    /// Durable write path: inserts go through ChWriter which spools to disk on CH failure.
    pub writer: ChWriter,
    pub config_db: Arc<ConfigDb>,
    pub usage: UsageTracker,
    pub usage_accumulator: UsageAccumulator,
    pub config: RushConfig,
    /// Per-IP login attempt counter for rate limiting: (attempts, window_start).
    pub login_limiter: Arc<DashMap<String, (u32, Instant)>>,
    /// API key resolution cache: key_hash → (tenant_id, cached_at). TTL 60s.
    pub api_key_cache: Arc<DashMap<String, (String, Instant)>>,
}
