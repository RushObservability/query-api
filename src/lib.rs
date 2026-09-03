pub mod alert_engine;
pub mod anomaly_engine;
pub mod api_error;
pub mod api_key_auth;
pub mod audit;
pub mod buffer_topology;
pub mod ch_writer;
pub mod clickhouse_config;
pub mod config;
pub mod cors;
pub mod detection_query;
pub mod eval_state;
pub mod github_repository_policy;
pub mod handlers;
pub mod ingest_limits;
pub mod integrations;
pub mod internal_auth;
pub mod license;
pub mod llm_gateway;
pub mod llm_providers;
pub mod metric_firewall;
pub mod migrations;
pub mod models;
pub mod monitor_engine;
pub mod object_store_spool;
pub mod outbound;
pub mod pagination;
pub mod process_metrics;
pub mod promql;
pub mod query_builder;
pub mod query_governor;
pub mod request_auth;
pub mod retention_enforcer;
pub mod rollup;
pub mod saml;
pub mod self_metrics;
pub mod shutdown;
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

/// Authenticated identity resolved by tenant middleware. Handlers use this for
/// API-key callers because browser-session lookup is not available for keys.
#[derive(Clone, Debug)]
pub struct RequestIdentity {
    pub tenant_id: String,
    pub authenticated: bool,
    pub actor_id: String,
    pub actor_name: String,
    pub actor_type: String,
    pub credential_type: String,
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
    let active_budget = query_governor::active_budget()
        .or_else(|| query_governor::global().map(|governor| governor.config().background));
    let guards = query_guards();
    let max_result_rows = active_budget
        .as_ref()
        .map(|budget| budget.max_result_rows.to_string())
        .unwrap_or_else(|| "500000".to_string());
    let max_memory_usage = active_budget
        .as_ref()
        .map(|budget| budget.max_memory_usage.to_string())
        .unwrap_or_else(|| guards.max_memory_usage.clone());
    let spill_threshold = active_budget
        .as_ref()
        .map(|budget| budget.spill_threshold_bytes.to_string())
        .unwrap_or_else(|| guards.max_bytes_external.clone());
    let mut q = ch
        .query(sql)
        .with_option("max_result_rows", max_result_rows.as_str())
        .with_option("result_overflow_mode", "throw")
        .with_option("max_memory_usage", max_memory_usage.as_str())
        .with_option(
            "max_bytes_before_external_group_by",
            spill_threshold.as_str(),
        )
        .with_option("max_bytes_before_external_sort", spill_threshold.as_str())
        // Dropping the HTTP handler future (client disconnect, navigation, or
        // request timeout) closes the ClickHouse response and cancels its
        // read-only query rather than leaving server work detached.
        .with_option("cancel_http_readonly_queries_on_client_close", "1")
        // ClickHouse 26.2 query condition cache: caches the per-granule match bitset
        // for a WHERE predicate so repeated identical predicates (dashboard refreshes,
        // the count+list+histogram+timeseries siblings of one Explore search, monitor/
        // detection eval re-runs, service-map polls) skip re-evaluating skip indexes and
        // re-reading granules. Safe on these MergeTree reads (no FINAL on the read path).
        .with_option("use_query_condition_cache", "1");
    if let Some(budget) = &active_budget {
        let max_execution_time = budget.max_execution_time_secs.to_string();
        let max_rows_to_read = budget.max_rows_to_read.to_string();
        let max_bytes_to_read = budget.max_bytes_to_read.to_string();
        let max_threads = budget.max_threads.to_string();
        q = q
            .with_option("max_execution_time", max_execution_time.as_str())
            .with_option("max_rows_to_read", max_rows_to_read.as_str())
            .with_option("read_overflow_mode", "throw")
            .with_option("max_bytes_to_read", max_bytes_to_read.as_str())
            .with_option("max_threads", max_threads.as_str());
    } else if let Some(n) = &guards.max_threads {
        q = q.with_option("max_threads", n.as_str());
    }
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
    /// Startup-validated exact cross-origin browser allowlist.
    pub cors_policy: Arc<cors::CorsPolicy>,
    /// Per-axis login attempt counter for rate limiting: (attempts, window_start).
    /// Keys contain only keyed hashes, never raw usernames or client addresses.
    pub login_limiter: Arc<DashMap<String, (u32, Instant)>>,
    pub login_account_limit_per_minute: u32,
    pub login_ip_limit_per_minute: u32,
    /// Only direct peers in these networks may supply client forwarding headers.
    pub trusted_proxy_cidrs: Arc<Vec<String>>,
    /// Per ingest-key fixed-window request limiter: key id -> (count, window start).
    pub ingest_key_limiter: Arc<DashMap<String, (u64, Instant)>>,
    /// HMAC session key → last rotation eligibility check. This gates only the
    /// extra renewal query; authorization itself is still checked every request.
    pub session_rotation_checks: Arc<DashMap<String, Instant>>,
    /// Tamper-evident audit log writer (hash-chained, serialized). Shared.
    pub audit: Arc<audit::AuditLogger>,
    /// In-process system-health self-metrics registry. Updated on the HTTP hot path,
    /// the ingest path, and engine loops; rendered at the open `GET /metrics` endpoint
    /// and self-ingested into our own metrics tables by the stats engine each tick.
    pub self_metrics: Arc<self_metrics::SelfMetrics>,
    /// Live-reconfigurable workload admission and ClickHouse query budgets.
    pub query_governor: Arc<query_governor::QueryGovernor>,
    /// Expiring tenant-scoped asynchronous export objects and progress state.
    pub export_jobs: Arc<handlers::export::ExportJobs>,
    /// Startup-validated byte/entity limits and bounded blocking decode admission.
    pub ingest_limits: ingest_limits::IngestLimits,
    /// Startup-validated, rate-limited outbound LLM client. Handlers never
    /// receive provider credentials or construct their own LLM HTTP clients.
    pub llm_gateway: llm_gateway::LlmGateway,
    /// API-managed integration collector supervisor. The community build keeps
    /// this disabled unless a collector feature and manager setting are present.
    pub collectors: Arc<integrations::CollectorManager>,
    /// Coordinates readiness, HTTP admission, and durable ingest draining.
    pub shutdown: shutdown::ShutdownController,
}
