pub mod alert_engine;
pub mod anomaly_engine;
pub mod api_key_auth;
pub mod audit;
pub mod ch_writer;
pub mod clickhouse_config;
pub mod config;
pub mod detection_query;
pub mod eval_state;
pub mod github_repository_policy;
pub mod handlers;
pub mod integrations;
pub mod license;
pub mod metric_firewall;
pub mod migrations;
pub mod models;
pub mod monitor_engine;
pub mod object_store_spool;
pub mod outbound;
pub mod process_metrics;
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

/// Process-wide tenant-isolation state.
///
/// 0 = not initialized (fail closed), 1 = verified/enforcing,
/// 2 = explicit insecure development override.
static TENANT_ISOLATION_STATE: AtomicU8 = AtomicU8::new(0);

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

/// Verify that ClickHouse accepts the custom setting used by the row policies.
pub async fn probe_row_policy_support(ch: &Client) -> anyhow::Result<()> {
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
    result.map(|_| ()).map_err(|error| {
        anyhow::anyhow!(
            "ClickHouse rejected rush_tenant_id; configure custom_settings_prefixes='rush_': {error}"
        )
    })
}

/// Mark row-policy enforcement verified after both policy inspection and a
/// tenant-scoped read-principal probe succeed.
pub fn mark_row_policy_enforced() {
    TENANT_ISOLATION_STATE.store(1, Ordering::SeqCst);
}

/// Enable the explicit development-only compatibility mode. Production Helm
/// values never set this state.
pub fn mark_insecure_tenant_read_override() {
    TENANT_ISOLATION_STATE.store(2, Ordering::SeqCst);
}

/// Returns true only after row-policy behavior was verified at startup.
pub fn row_policy_supported() -> bool {
    TENANT_ISOLATION_STATE.load(Ordering::SeqCst) == 1
}

pub fn tenant_isolation_ready() -> bool {
    matches!(TENANT_ISOLATION_STATE.load(Ordering::SeqCst), 1 | 2)
}

pub fn tenant_isolation_status() -> &'static str {
    match TENANT_ISOLATION_STATE.load(Ordering::SeqCst) {
        1 => "enforced",
        2 => "insecure_development_override",
        _ => "uninitialized",
    }
}

/// Create a ClickHouse query with the tenant setting used by ClickHouse row
/// policies. The setting is omitted only in the explicit development override.
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
    if TENANT_ISOLATION_STATE.load(Ordering::SeqCst) == 2 {
        q
    } else {
        // Uninitialized is intentionally fail closed: ClickHouse will reject
        // this setting when support is absent instead of silently running an
        // unscoped application query.
        q.with_option("rush_tenant_id", tenant_id)
    }
}

#[derive(Clone)]
pub struct AppState {
    /// Tenant-scoped, SELECT-only ClickHouse client. All telemetry reads use it.
    pub ch: Client,
    /// Privileged migration/write client. Never use this for tenant telemetry reads.
    pub admin_ch: Client,
    /// Durable write path: inserts go through ChWriter which spools to disk on CH failure.
    pub writer: ChWriter,
    pub config_db: Arc<ConfigDb>,
    pub usage: UsageTracker,
    pub usage_accumulator: UsageAccumulator,
    pub config: RushConfig,
    /// Per-IP login attempt counter for rate limiting: (attempts, window_start).
    pub login_limiter: Arc<DashMap<String, (u32, Instant)>>,
    /// API key resolution cache: key_hash → (tenant_id, cached_at). TTL 60s.
    pub api_key_cache: Arc<DashMap<String, (clickhouse_config::ApiKeyGrant, Instant)>>,
    /// Per ingest-key fixed-window request limiter: key id -> (count, window start).
    pub ingest_key_limiter: Arc<DashMap<String, (u64, Instant)>>,
    /// Tamper-evident audit log writer (hash-chained, serialized). Shared.
    pub audit: Arc<audit::AuditLogger>,
    /// In-process system-health self-metrics registry. Updated on the HTTP hot path,
    /// the ingest path, and engine loops; rendered at the open `GET /metrics` endpoint
    /// and self-ingested into our own metrics tables by the stats engine each tick.
    pub self_metrics: Arc<self_metrics::SelfMetrics>,
    /// API-managed integration collector supervisor. The community build keeps
    /// this disabled unless a collector feature and manager setting are present.
    pub collectors: Arc<integrations::CollectorManager>,
}
