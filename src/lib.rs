pub mod alert_engine;
pub mod anomaly_engine;
pub mod audit;
pub mod ch_writer;
pub mod clickhouse_config;
pub mod config;
pub mod eval_state;
pub mod handlers;
pub mod license;
pub mod metric_firewall;
pub mod migrations;
pub mod models;
pub mod monitor_engine;
pub mod object_store_spool;
pub mod outbound;
pub mod promql;
pub mod query_builder;
pub mod retention_enforcer;
pub mod rollup;
pub mod saml;
pub mod self_metrics;
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
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use ch_writer::ChWriter;
use clickhouse_config::ConfigDb;
use config::RushConfig;
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

/// Per-query ClickHouse memory guardrails, read once from the environment. These
/// are ClickHouse *server* settings attached to every read via [`tenant_query`]:
/// they bound how much memory a single query can consume server-side and let
/// large aggregations/sorts spill to disk instead of failing. `max_result_rows`
/// (set separately in `tenant_query`) bounds rows streamed back to this process;
/// these bound CH-side working memory, a different failure mode.
struct QueryGuards {
    /// `max_memory_usage` — per-query byte ceiling.
    max_memory_usage: String,
    /// Threshold (bytes) applied to both `max_bytes_before_external_group_by`
    /// and `max_bytes_before_external_sort`, so heavy GROUP BY / ORDER BY spill
    /// to disk rather than erroring (graceful degradation).
    max_bytes_external: String,
    /// `max_threads` — optional per-query CPU fan-out cap; only emitted when
    /// `RUSH_CH_MAX_THREADS` is set (None → ClickHouse default).
    max_threads: Option<String>,
}

static QUERY_GUARDS: OnceLock<QueryGuards> = OnceLock::new();

/// Read the read-path memory guardrails from the environment once and cache them.
/// Mirrors the env-parse idiom in `ch_writer::BatchConfig::from_env`.
fn query_guards() -> &'static QueryGuards {
    QUERY_GUARDS.get_or_init(|| {
        // 4 GiB default per-query ceiling.
        let max_memory: u64 = std::env::var("RUSH_CH_MAX_MEMORY_USAGE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4_000_000_000u64);
        // Spill threshold defaults to half the memory ceiling so a query starts
        // spilling well before it hits the hard cap.
        let max_external: u64 = std::env::var("RUSH_CH_MAX_BYTES_EXTERNAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(max_memory / 2);
        let max_threads = std::env::var("RUSH_CH_MAX_THREADS")
            .ok()
            .filter(|s| s.parse::<u64>().is_ok());
        QueryGuards {
            max_memory_usage: max_memory.to_string(),
            max_bytes_external: max_external.to_string(),
            max_threads,
        }
    })
}

/// Probe ClickHouse once at startup to see if custom_settings_prefixes includes 'rush_'.
/// If not, we skip injecting the per-query setting (row policies stay permissive).
pub async fn probe_row_policy_support(ch: &Client) {
    #[derive(clickhouse::Row, serde::Deserialize)]
    #[allow(dead_code)]
    struct Probe {
        n: u8,
    }
    let result = ch
        .query("SELECT 1 AS n")
        .with_option("rush_tenant_id", "probe")
        .fetch_one::<Probe>()
        .await;
    match result {
        Ok(_) => {
            tracing::info!(
                "ClickHouse accepts rush_tenant_id custom setting — row policies enforcing"
            );
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
    // Memory guardrails: cap CH-side working memory for a single query and let
    // heavy GROUP BY / ORDER BY spill to disk instead of OOMing the server. These
    // complement the row cap below (which bounds rows streamed back to us).
    let guards = query_guards();
    let q = ch
        .query(sql)
        .with_option("max_result_rows", "500000")
        .with_option("result_overflow_mode", "break")
        .with_option("max_memory_usage", guards.max_memory_usage.as_str())
        .with_option(
            "max_bytes_before_external_group_by",
            guards.max_bytes_external.as_str(),
        )
        .with_option(
            "max_bytes_before_external_sort",
            guards.max_bytes_external.as_str(),
        )
        // ClickHouse 26.2 query condition cache: caches the per-granule match bitset
        // for a WHERE predicate so repeated identical predicates (dashboard refreshes,
        // the count+list+histogram+timeseries siblings of one Explore search, monitor/
        // detection eval re-runs, service-map polls) skip re-evaluating skip indexes and
        // re-reading granules. Safe on these MergeTree reads (no FINAL on the read path).
        .with_option("use_query_condition_cache", "1");
    let q = match &guards.max_threads {
        Some(n) => q.with_option("max_threads", n.as_str()),
        None => q,
    };
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
    /// Tamper-evident audit log writer (hash-chained, serialized). Shared.
    pub audit: Arc<audit::AuditLogger>,
    /// In-process system-health self-metrics registry. Updated on the HTTP hot path,
    /// the ingest path, and engine loops; rendered at the open `GET /metrics` endpoint
    /// and self-ingested into our own metrics tables by the stats engine each tick.
    pub self_metrics: Arc<self_metrics::SelfMetrics>,
}
