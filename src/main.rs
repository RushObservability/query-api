use axum::{Router, routing::any, routing::delete, routing::get, routing::post, routing::put};
use axum::{extract::Request, middleware::Next, response::Response};
use axum::http::{HeaderValue, header};
use clickhouse::Client;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use rush_api::alert_engine;
use rush_api::anomaly_engine;
use rush_api::config::RushConfig;
use rush_api::clickhouse_config::ConfigDb;
use rush_api::handlers;
use rush_api::migrations;
use rush_api::monitor_engine;
use rush_api::retention_enforcer;
use rush_api::siem_engine;
use rush_api::slo_engine;
use rush_api::stats_engine;
use rush_api::usage_accumulator::UsageAccumulator;
use rush_api::usage_tracker;
use rush_api::ch_writer::ChWriter;
use rush_api::spool::{IngestBuffer, Spool};
use rush_api::AppState;
use rush_api::TenantContext;

/// Middleware that adds security response headers to every response.
async fn security_headers_middleware(req: Request, next: Next) -> Response {
    let mut resp = next.run(req).await;
    let headers = resp.headers_mut();
    headers.insert(header::X_CONTENT_TYPE_OPTIONS,     HeaderValue::from_static("nosniff"));
    headers.insert(header::X_FRAME_OPTIONS,            HeaderValue::from_static("DENY"));
    headers.insert(header::REFERRER_POLICY,            HeaderValue::from_static("strict-origin-when-cross-origin"));
    headers.insert(
        header::HeaderName::from_static("permissions-policy"),
        HeaderValue::from_static("geolocation=(), microphone=(), camera=()"),
    );
    headers.insert(
        header::HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
             img-src 'self' data:; font-src 'self'; connect-src 'self'; \
             object-src 'none'; frame-ancestors 'none'; base-uri 'self'"
        ),
    );
    resp
}

/// Middleware that resolves the tenant for every request. Four methods,
/// checked in priority order:
///
/// 1. `Authorization: Bearer <api_key>` — resolves the key to a tenant via
///    the config DB. Secure; the key is the trust boundary.
/// 2. `rush_session` cookie — resolves a session to its user, then uses
///    the user's tenant_id.
/// 3. `X-Rush-Tenant: <tenant_name_or_id>` — use the header value directly.
///    No auth required. Intended for simple / dev / single-org deployments
///    where teams trust each other and don't want to manage API keys.
/// 4. Fall back to the `"default"` tenant (backward compatible, no headers
///    needed at all).
async fn tenant_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    // Extract all header values we need before any await point so the
    // &Request (whose Body is not Send) is not held across awaits.
    let auth_header: Option<String> = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());
    let dd_key: Option<String> = req
        .headers()
        .get("dd-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());
    let rush_tenant: Option<String> = req
        .headers()
        .get("x-rush-tenant")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());
    let session_token: Option<String> = handlers::auth::extract_session_cookie(req.headers());

    let tenant_id = resolve_tenant_from_headers(
        &state, auth_header, dd_key, rush_tenant, session_token,
    ).await;
    req.extensions_mut().insert(TenantContext { tenant_id });
    next.run(req).await
}

async fn resolve_tenant_from_headers(
    state: &AppState,
    auth_header: Option<String>,
    dd_key: Option<String>,
    rush_tenant: Option<String>,
    session_token: Option<String>,
) -> String {
    // ── Priority 1: Bearer token → fixed to the key's tenant ──
    // API keys are scoped to one tenant (for collectors, CI, Grafana).
    if let Some(val) = auth_header {
        if val.len() > 7 && val[..7].eq_ignore_ascii_case("bearer ") {
            let key = val[7..].trim();
            let key_hash = handlers::settings::hash_api_key(key);

            // Fast path: check in-memory cache (TTL 60s) before hitting ClickHouse
            if let Some(entry) = state.api_key_cache.get(&key_hash) {
                let (tid, ts) = entry.value();
                if ts.elapsed() < std::time::Duration::from_secs(60) {
                    return tid.clone();
                }
            }

            match state.config_db.resolve_tenant_for_api_key(&key_hash).await {
                Ok(Some(tid)) => {
                    state.api_key_cache.insert(key_hash, (tid.clone(), std::time::Instant::now()));
                    return tid;
                }
                Ok(None) => {
                    tracing::debug!(method = "api_key", "tenant resolution: key not found, falling through");
                }
                Err(e) => {
                    tracing::warn!(error = %e, method = "api_key", "tenant resolution failed");
                }
            }
        }
    }

    // ── Priority 1b: DD-API-KEY header (Datadog agent) ──
    // The Datadog agent sends its API key in this header. Resolve it the
    // same way as a Bearer token so DD agents map to tenants via API keys.
    if let Some(dd_key_val) = dd_key {
        let key = dd_key_val.trim();
        if !key.is_empty() {
            let key_hash = handlers::settings::hash_api_key(key);

            // Fast path: check in-memory cache (TTL 60s) before hitting ClickHouse
            if let Some(entry) = state.api_key_cache.get(&key_hash) {
                let (tid, ts) = entry.value();
                if ts.elapsed() < std::time::Duration::from_secs(60) {
                    return tid.clone();
                }
            }

            match state.config_db.resolve_tenant_for_api_key(&key_hash).await {
                Ok(Some(tid)) => {
                    state.api_key_cache.insert(key_hash, (tid.clone(), std::time::Instant::now()));
                    return tid;
                }
                Ok(None) => {
                    tracing::debug!(method = "dd_api_key", "tenant resolution: DD key not found, falling through");
                }
                Err(e) => {
                    tracing::warn!(error = %e, method = "dd_api_key", "tenant resolution failed");
                }
            }
        }
    }

    // ── Priority 2: X-Rush-Tenant header ──
    // The frontend tenant switcher sends this. It takes priority over the
    // session's default tenant so users can switch between tenants they
    // have access to.
    //
    // If the tenant has auth_required=true (locked), the X-Rush-Tenant header
    // alone is NOT enough — the request must also have been authenticated via
    // Bearer token, DD-API-KEY, or session cookie (priorities 1/1b above).
    // This prevents unauthenticated ingest into locked tenants.
    if let Some(tenant_header) = rush_tenant {
        let tenant = tenant_header.trim().to_string();
        if !tenant.is_empty() {
            if state.config_db.is_tenant_enabled(&tenant).await {
                // If the request carries a session cookie, validate the user has
                // group-based access to the requested tenant.
                if let Some(token) = &session_token {
                    if let Some((user_id, _username, _display_name, _tid, role)) =
                        state.config_db.get_session_user(token).await
                    {
                        if role == "admin" {
                            // Admins can access any enabled tenant
                            return tenant;
                        }
                        // Non-admins: resolve accessible tenant IDs and check
                        if let Ok((_, _, accessible_ids)) =
                            state.config_db.resolve_user_permissions(&user_id).await
                        {
                            // accessible_ids are UUIDs; resolve the requested
                            // tenant name to an ID for comparison
                            if let Ok(Some(tenant_id)) =
                                state.config_db.get_tenant_id_by_name(&tenant).await
                            {
                                if accessible_ids.contains(&tenant_id) {
                                    return tenant;
                                }
                            }
                        }
                        tracing::debug!(
                            tenant = %tenant,
                            "X-Rush-Tenant rejected: user lacks group access"
                        );
                        // Fall through to session default tenant
                    }
                } else if !state.config_db.is_tenant_auth_required(&tenant).await {
                    // No session + open tenant: header is enough (for collectors)
                    return tenant;
                } else {
                    tracing::debug!(
                        tenant_id = %tenant,
                        method = "header",
                        "tenant requires auth — X-Rush-Tenant header rejected without valid session/API key"
                    );
                }
            } else {
                tracing::debug!(tenant_id = %tenant, method = "header", "tenant disabled or missing, falling through");
            }
        }
    }

    // ── Priority 3: Session cookie → user's default tenant ──
    // Fallback when no explicit tenant header is sent (e.g., first page load
    // before the tenant switcher initializes).
    if let Some(token) = session_token {
        if let Some((_user_id, _username, _display_name, tenant_id, _role)) =
            state.config_db.get_session_user(&token).await
        {
            return tenant_id;
        }
    }

    // ── Priority 4: default ──
    "default".to_string()
}

use axum::extract::State;

/// Build the object-store ingest buffer from `RUSH_BUFFER_S3_*` env (reuses the
/// standard S3/MinIO settings). Returns an error if required vars are missing so
/// the caller can fall back to disk.
async fn build_object_store_buffer(max_bytes: u64) -> anyhow::Result<IngestBuffer> {
    let endpoint = std::env::var("RUSH_BUFFER_S3_ENDPOINT").unwrap_or_default();
    let bucket = std::env::var("RUSH_BUFFER_S3_BUCKET")
        .map_err(|_| anyhow::anyhow!("RUSH_BUFFER_S3_BUCKET is required for object_store backend"))?;
    let prefix = std::env::var("RUSH_BUFFER_S3_PREFIX").unwrap_or_else(|_| "ingest/".to_string());
    let region = std::env::var("RUSH_BUFFER_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let access = std::env::var("RUSH_BUFFER_S3_ACCESS_KEY")
        .or_else(|_| std::env::var("AWS_ACCESS_KEY_ID"))
        .unwrap_or_default();
    let secret = std::env::var("RUSH_BUFFER_S3_SECRET_KEY")
        .or_else(|_| std::env::var("AWS_SECRET_ACCESS_KEY"))
        .unwrap_or_default();
    let s = rush_api::object_store_spool::ObjectStoreSpool::open_s3(
        &endpoint, &bucket, &prefix, &region, &access, &secret, max_bytes,
    )
    .await?;
    Ok(IngestBuffer::ObjectStore(s))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("rush_api=info,tower_http=info"));

    let log_format = std::env::var("RUSH_LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());
    match log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        "logfmt" => {
            let layer = tracing_logfmt::layer();
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;
            tracing_subscriber::registry()
                .with(filter)
                .with(layer)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .init();
        }
    }

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let clickhouse_db =
        std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "observability".to_string());

    let clickhouse_user =
        std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let clickhouse_password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    // Load rush.toml config (defaults if file missing)
    let wide_config_path =
        std::env::var("RUSH_CONFIG").unwrap_or_else(|_| "./rush.toml".to_string());
    let wide_config = RushConfig::load(&wide_config_path)?;

    // Run schema migrations (CREATE TABLE etc.) — blocks until tables exist.
    migrations::run(&clickhouse_url, &clickhouse_user, &clickhouse_password, &wide_config).await?;

    // Spawn TTL + storage policy maintenance in the background so the API
    // starts serving immediately instead of blocking on ALTER TABLE mutations.
    migrations::spawn_maintenance(
        clickhouse_url.clone(),
        clickhouse_user.clone(),
        clickhouse_password.clone(),
        wide_config.clone(),
    );

    let ch = Client::default()
        .with_url(&clickhouse_url)
        .with_database(&clickhouse_db)
        .with_user(&clickhouse_user)
        .with_password(&clickhouse_password)
        .with_option("max_execution_time", "30")
        // Server-side INSERT buffering: ClickHouse batches writes internally,
        // reducing part creation rate at high ingest volume.
        // These options are silently ignored for SELECT queries.
        //
        // DURABILITY TRADEOFF (deliberate): wait_for_async_insert=0 means an
        // insert is acked once buffered, BEFORE the server-side flush — a
        // flush-time error (e.g. disk full) silently drops those rows and the
        // disk spool never sees them, because the insert "succeeded". We accept
        // that window for ingest throughput; the spool covers the common case
        // (CH down/unreachable → insert errors → rows spooled). Set this to "1"
        // if at-least-once mattering more than latency.
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0")
        .with_compression(clickhouse::Compression::Lz4);

    // Check if ClickHouse supports the rush_tenant_id custom setting for row policy enforcement.
    // If not (no custom_settings_prefixes configured), row policies are NOT created (they would
    // break all queries). The API-layer WHERE clause is still the primary enforcement.
    rush_api::probe_row_policy_support(&ch).await;
    if rush_api::row_policy_supported() {
        migrations::apply_row_policies(&ch).await;
    }

    let config_db = Arc::new(
        ConfigDb::open(&clickhouse_url, &clickhouse_user, &clickhouse_password).await?
    );
    config_db.ensure_default_tenant().await?;
    config_db.ensure_global_retention().await?;
    config_db.ensure_default_admin().await?;
    config_db.ensure_default_groups().await?;
    config_db.ensure_default_templates().await?;
    tracing::info!("config db opened");

    // SMTP config for email notifications (optional)
    let smtp_config = alert_engine::SmtpConfig {
        host: std::env::var("RUSH_SMTP_HOST").ok(),
        port: std::env::var("RUSH_SMTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(587),
        user: std::env::var("RUSH_SMTP_USER").ok(),
        pass: std::env::var("RUSH_SMTP_PASS").ok(),
        from: std::env::var("RUSH_SMTP_FROM")
            .unwrap_or_else(|_| "wide@localhost".to_string()),
    };

    // Ingest-buffer drain controls (Phase 3):
    //  RUSH_DRAIN_WORKER_ONLY=true → run only the buffer drain (no HTTP, no engines).
    //  RUSH_RUN_REPLAYER=false     → don't drain in this process (API replicas opt out
    //                                so the drain is single-writer in HA / object-store).
    let drain_only = std::env::var("RUSH_DRAIN_WORKER_ONLY")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(false);
    let run_replayer = std::env::var("RUSH_RUN_REPLAYER")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);

    // Spawn background engines (skipped in drain-worker-only mode)
    if !drain_only {
    alert_engine::spawn_alert_engine(config_db.clone(), ch.clone(), smtp_config.clone());
    slo_engine::spawn_slo_engine(config_db.clone(), ch.clone());

    // Anomaly detection engine — evaluates anomaly rules, persists events, and
    // sends notifications. Queries Prometheus-source rules against the API's own
    // /prom endpoint (RUSH_PROM_BASE_URL, defaulting to this server).
    //
    // Runs in-process by default (single-binary / local dev). In Kubernetes the
    // chart runs a dedicated `anomaly_engine` Deployment, so it sets
    // RUSH_RUN_ANOMALY_ENGINE=false on the API to avoid double-evaluating rules
    // and sending duplicate notifications.
    let run_anomaly_in_process = std::env::var("RUSH_RUN_ANOMALY_ENGINE")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    if run_anomaly_in_process {
        let prom_base_url = std::env::var("RUSH_PROM_BASE_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        anomaly_engine::spawn_anomaly_engine(config_db.clone(), ch.clone(), smtp_config.clone(), prom_base_url);
    } else {
        tracing::info!("in-process anomaly engine disabled (RUSH_RUN_ANOMALY_ENGINE=false); expecting a dedicated anomaly-engine deployment");
    }
    retention_enforcer::spawn_retention_enforcer(ch.clone(), wide_config.clone(), config_db.clone());
    // stats_engine is spawned after the ingest buffer is built (it emits buffer metrics).

    // Spawn the Datadog-style monitor engine (v2 alerting)
    monitor_engine::spawn(ch.clone(), config_db.clone(), smtp_config);

    // Seed built-in SIEM detection rules and spawn the SIEM detection engine
    config_db.ensure_default_detection_rules().await?;
    siem_engine::spawn(ch.clone(), config_db.clone());
    } // end `if !drain_only` (background engines)

    // ── Durable write path: spool + writer ──
    let spool_dir = std::env::var("RUSH_SPOOL_DIR")
        .unwrap_or_else(|_| "./data/spool".to_string());
    let spool_max_bytes: u64 = std::env::var("RUSH_SPOOL_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_147_483_648); // 2 GiB default

    // Backend selection. Disk is the default and needs no object store. The
    // object-store backend is opt-in via RUSH_BUFFER_BACKEND=object_store; if its
    // config is missing/invalid we log and fall back to disk so ingestion always works.
    let backend = std::env::var("RUSH_BUFFER_BACKEND").unwrap_or_else(|_| "disk".to_string());
    let buffer = if backend == "object_store" {
        match build_object_store_buffer(spool_max_bytes).await {
            Ok(b) => {
                tracing::info!("ingest buffer backend: object_store");
                std::sync::Arc::new(b)
            }
            Err(e) => {
                tracing::error!(error = %e, "object_store buffer backend failed to init — falling back to disk");
                let spool = Spool::open(&spool_dir, spool_max_bytes).expect("failed to open spool directory");
                std::sync::Arc::new(IngestBuffer::Disk(spool))
            }
        }
    } else {
        let spool = Spool::open(&spool_dir, spool_max_bytes).expect("failed to open spool directory");
        std::sync::Arc::new(IngestBuffer::Disk(spool))
    };
    let writer = ChWriter::new(ch.clone(), buffer);
    if drain_only || run_replayer {
        writer.clone().spawn_replayer();
    }
    // Stats engine (emits ingest-buffer depth/age/drain metrics). API process only.
    if !drain_only {
        stats_engine::spawn_stats_engine(ch.clone(), writer.buffer.clone());
    }

    // Drain-worker-only: this process exists solely to drain the ingest buffer
    // into ClickHouse. Don't serve HTTP, run engines, or load the firewall.
    if drain_only {
        tracing::info!(
            backend = %backend,
            "RUSH_DRAIN_WORKER_ONLY — draining ingest buffer to ClickHouse; not serving HTTP"
        );
        std::future::pending::<()>().await;
    }

    // Metric firewall: load compiled rules now, then refresh periodically so
    // changes (incl. from other replicas) propagate to the ingest hot path.
    if let Ok(fw) = config_db.compiled_metric_firewall().await {
        if let Ok(mut g) = writer.firewall.write() { *g = Arc::new(fw); }
    }
    {
        let fw_handle = writer.firewall.clone();
        let cdb = config_db.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tick.tick().await;
                if let Ok(fw) = cdb.compiled_metric_firewall().await {
                    if let Ok(mut g) = fw_handle.write() { *g = Arc::new(fw); }
                }
            }
        });
    }

    // Spawn usage tracker (fire-and-forget signal usage tracking)
    let usage = usage_tracker::spawn(ch.clone());

    // Spawn usage accumulator (per-tenant ingest metering)
    let usage_accumulator = UsageAccumulator::new();
    usage_accumulator.spawn_flusher(ch.clone());

    let login_limiter: std::sync::Arc<dashmap::DashMap<String, (u32, std::time::Instant)>> =
        std::sync::Arc::new(dashmap::DashMap::new());

    // Spawn background task to evict stale rate-limiter entries (M7)
    {
        let limiter_clone = login_limiter.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                // Hard cap: if oversized (e.g. IP flood), evict aggressively.
                if limiter_clone.len() > 100_000 {
                    limiter_clone.retain(|_, (_, ts)| ts.elapsed() < std::time::Duration::from_secs(5));
                } else {
                    limiter_clone.retain(|_, (_, ts)| ts.elapsed() < std::time::Duration::from_secs(60));
                }
            }
        });
    }

    let api_key_cache: std::sync::Arc<dashmap::DashMap<String, (String, std::time::Instant)>> =
        std::sync::Arc::new(dashmap::DashMap::new());

    // Spawn background task to evict expired API key cache entries (TTL 60s)
    {
        let cache_clone = api_key_cache.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                // Hard cap: more keys than any real deployment should have.
                if cache_clone.len() > 50_000 {
                    cache_clone.retain(|_, (_, ts)| ts.elapsed() < std::time::Duration::from_secs(10));
                } else {
                    cache_clone.retain(|_, (_, ts)| ts.elapsed() < std::time::Duration::from_secs(60));
                }
            }
        });
    }

    // Spawn background task to proactively evict stale suggest cache entries.
    // Without this, entries queried once but never again persist forever.
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let cache = handlers::suggest::suggest_cache();
            if cache.len() > 10_000 {
                cache.retain(|_, (_, ts)| ts.elapsed() < std::time::Duration::from_secs(5));
            } else {
                cache.retain(|_, (_, ts)| ts.elapsed() < std::time::Duration::from_secs(30));
            }
        }
    });

    let state = AppState {
        ch,
        writer,
        config_db,
        usage,
        usage_accumulator,
        config: wide_config,
        login_limiter,
        api_key_cache,
    };

    let app = Router::new()
        // Trace endpoints
        .route("/api/v1/traces/{trace_id}", get(handlers::traces::get_trace))
        // Query endpoints
        .route("/api/v1/query", post(handlers::query::execute_query))
        .route("/api/v1/query/count", post(handlers::query::count_query))
        .route("/api/v1/query/group", post(handlers::query::group_query))
        .route("/api/v1/query/timeseries", post(handlers::query::timeseries_query))
        // Export current query + results (CSV/JSON), capped by export_max_rows
        .route("/api/v1/query/export", post(handlers::query::export_query))
        // BubbleUp comparison analysis
        .route("/api/v1/bubbleup", post(handlers::bubbleup::bubbleup))
        // Log endpoints
        .route("/api/v1/logs", post(handlers::logs::query_logs))
        .route("/api/v1/logs/count", post(handlers::logs::count_logs))
        .route("/api/v1/logs/export", post(handlers::logs::export_logs))
        // Service catalog
        .route("/api/v1/services", get(handlers::services::list_services))
        .route("/api/v1/services/graph", get(handlers::services::service_graph))
        // Natural language query parsing (LLM-powered)
        .route("/api/v1/parse-query", post(handlers::parse_query::parse_query))
        .route("/api/v1/parse-promql", post(handlers::parse_promql::parse_promql))
        // Field suggestions
        .route(
            "/api/v1/suggest/{field}",
            get(handlers::suggest::suggest_values),
        )
        // Dashboard endpoints
        .route(
            "/api/v1/dashboards",
            get(handlers::dashboards::list_dashboards).post(handlers::dashboards::create_dashboard),
        )
        .route(
            "/api/v1/dashboards/import",
            post(handlers::dashboards::import_dashboard),
        )
        .route(
            "/api/v1/dashboards/{id}",
            get(handlers::dashboards::get_dashboard)
                .put(handlers::dashboards::update_dashboard)
                .delete(handlers::dashboards::delete_dashboard),
        )
        .route(
            "/api/v1/dashboards/{id}/widgets",
            post(handlers::dashboards::create_widget),
        )
        .route(
            "/api/v1/dashboards/{id}/widgets/{wid}",
            put(handlers::dashboards::update_widget).delete(handlers::dashboards::delete_widget),
        )
        .route(
            "/api/v1/dashboards/{id}/export",
            get(handlers::dashboards::export_dashboard),
        )
        // Dashboard template endpoints
        .route(
            "/api/v1/dashboard-templates",
            get(handlers::dashboards::list_dashboard_templates),
        )
        .route(
            "/api/v1/dashboard-templates/{tid}/create",
            post(handlers::dashboards::create_from_template),
        )
        // Notification channels
        .route(
            "/api/v1/channels",
            get(handlers::alerts::list_channels).post(handlers::alerts::create_channel),
        )
        .route(
            "/api/v1/channels/{id}",
            put(handlers::alerts::update_channel).delete(handlers::alerts::delete_channel),
        )
        .route(
            "/api/v1/channels/{id}/notify",
            post(handlers::alerts::notify_channel),
        )
        .route(
            "/api/v1/channels/{id}/test",
            post(handlers::alerts::test_channel),
        )
        .route(
            "/api/v1/notifications/log",
            get(handlers::alerts::list_notification_log),
        )
        // Alert events (all rules)
        .route(
            "/api/v1/alert-events",
            get(handlers::alerts::list_all_alert_events),
        )
        // Alert rules
        .route(
            "/api/v1/alerts",
            get(handlers::alerts::list_alerts).post(handlers::alerts::create_alert),
        )
        .route(
            "/api/v1/alerts/{id}",
            get(handlers::alerts::get_alert)
                .put(handlers::alerts::update_alert)
                .delete(handlers::alerts::delete_alert),
        )
        .route(
            "/api/v1/alerts/{id}/events",
            get(handlers::alerts::list_alert_events),
        )
        // Trace Funnels
        .route(
            "/api/v1/funnels",
            get(handlers::funnels::list_funnels).post(handlers::funnels::create_funnel),
        )
        .route(
            "/api/v1/funnels/{id}",
            delete(handlers::funnels::delete_funnel),
        )
        .route(
            "/api/v1/funnels/{id}/run",
            post(handlers::funnels::run_funnel),
        )
        // Maintenance Windows
        .route(
            "/api/v1/maintenance-windows",
            get(handlers::maintenance::list_windows).post(handlers::maintenance::create_window),
        )
        .route(
            "/api/v1/maintenance-windows/{id}",
            delete(handlers::maintenance::delete_window),
        )
        // Monitors (Datadog-style v2 alerting)
        .route(
            "/api/v1/monitors",
            get(handlers::monitors::list_monitors).post(handlers::monitors::create_monitor),
        )
        .route(
            "/api/v1/monitors/autocomplete",
            get(handlers::monitors::autocomplete),
        )
        .route(
            "/api/v1/monitors/suggest",
            post(handlers::monitors::suggest),
        )
        .route(
            "/api/v1/monitors/preview",
            post(handlers::monitors::preview_monitor),
        )
        .route(
            "/api/v1/monitors/{id}",
            get(handlers::monitors::get_monitor)
                .put(handlers::monitors::update_monitor)
                .delete(handlers::monitors::delete_monitor),
        )
        .route(
            "/api/v1/monitors/{id}/events",
            get(handlers::monitors::list_monitor_events),
        )
        .route(
            "/api/v1/monitors/{id}/mute",
            post(handlers::monitors::mute_monitor),
        )
        .route(
            "/api/v1/monitors/{id}/unmute",
            post(handlers::monitors::unmute_monitor),
        )
        // SLOs
        .route(
            "/api/v1/slos",
            get(handlers::slos::list_slos).post(handlers::slos::create_slo),
        )
        .route(
            "/api/v1/slos/{id}",
            get(handlers::slos::get_slo)
                .put(handlers::slos::update_slo)
                .delete(handlers::slos::delete_slo),
        )
        .route(
            "/api/v1/slos/{id}/events",
            get(handlers::slos::list_slo_events),
        )
        // Anomaly rules
        .route(
            "/api/v1/anomaly-rules",
            get(handlers::anomalies::list_anomaly_rules)
                .post(handlers::anomalies::create_anomaly_rule),
        )
        .route(
            "/api/v1/anomaly-rules/{id}",
            get(handlers::anomalies::get_anomaly_rule)
                .put(handlers::anomalies::update_anomaly_rule)
                .delete(handlers::anomalies::delete_anomaly_rule),
        )
        .route(
            "/api/v1/anomaly-events",
            get(handlers::anomalies::list_all_anomaly_events),
        )
        .route(
            "/api/v1/anomaly-events/{event_id}",
            get(handlers::anomalies::get_anomaly_event),
        )
        .route(
            "/api/v1/anomaly-events/{event_id}/correlations",
            get(handlers::anomalies::get_event_correlations),
        )
        .route(
            "/api/v1/anomaly-events/{event_id}/analyze",
            post(handlers::anomalies::analyze_anomaly_event),
        )
        // SIEM Detection rules
        .route(
            "/api/v1/detection/rules",
            get(handlers::detection::list_detection_rules)
                .post(handlers::detection::create_detection_rule),
        )
        .route(
            "/api/v1/detection/rules/{id}",
            get(handlers::detection::get_detection_rule)
                .put(handlers::detection::update_detection_rule)
                .delete(handlers::detection::delete_detection_rule),
        )
        .route(
            "/api/v1/detection/rules/{id}/test",
            post(handlers::detection::test_detection_rule),
        )
        .route(
            "/api/v1/detection/events",
            get(handlers::detection::list_detection_events),
        )
        // Prometheus-compatible metrics API (for Grafana)
        .route(
            "/prom/api/v1/query",
            get(handlers::metrics::prom_query).post(handlers::metrics::prom_query_post),
        )
        .route(
            "/prom/api/v1/query_range",
            get(handlers::metrics::prom_query_range).post(handlers::metrics::prom_query_range_post),
        )
        .route(
            "/prom/api/v1/series",
            get(handlers::metrics::prom_series).post(handlers::metrics::prom_series_post),
        )
        .route(
            "/prom/api/v1/labels",
            get(handlers::metrics::prom_labels).post(handlers::metrics::prom_labels),
        )
        .route(
            "/prom/api/v1/label/{name}/values",
            get(handlers::metrics::prom_label_values),
        )
        // Prometheus remote write
        .route(
            "/prom/api/v1/write",
            post(handlers::remote_write::prom_remote_write),
        )
        // Deploy markers
        .route(
            "/api/v1/deploys",
            get(handlers::deploys::list_deploys).post(handlers::deploys::create_deploy),
        )
        // Service Links (service → GitHub repo mapping)
        .route(
            "/api/v1/service-links",
            get(handlers::service_links::list_service_links)
                .post(handlers::service_links::create_service_link),
        )
        .route(
            "/api/v1/service-links/{service_name}",
            delete(handlers::service_links::delete_service_link),
        )
        // Feature flags (public — no auth)
        .route("/api/v1/features", get(handlers::settings::get_features))
        // Export row cap (admin-only setter; value also exposed via /features)
        .route("/api/v1/settings/export-max-rows", put(handlers::settings::set_export_max_rows))
        .route(
            "/api/v1/settings/sre-agent",
            get(handlers::settings::get_sre_agent_settings).put(handlers::settings::set_sre_agent_settings),
        )
        // API Keys (settings)
        .route(
            "/api/v1/api-keys",
            get(handlers::settings::list_api_keys).post(handlers::settings::create_api_key),
        )
        .route(
            "/api/v1/api-keys/{id}",
            delete(handlers::settings::delete_api_key),
        )
        // Custom skills (user-defined investigation playbooks)
        .route(
            "/api/v1/custom-skills",
            get(handlers::custom_skills::list_custom_skills)
                .post(handlers::custom_skills::create_custom_skill),
        )
        .route(
            "/api/v1/custom-skills/{id}",
            get(handlers::custom_skills::get_custom_skill)
                .put(handlers::custom_skills::update_custom_skill)
                .delete(handlers::custom_skills::delete_custom_skill),
        )
        // Tenants (multi-tenant isolation boundaries)
        .route(
            "/api/v1/tenants",
            get(handlers::tenants::list_tenants)
                .post(handlers::tenants::create_tenant),
        )
        .route(
            "/api/v1/tenants/{id}",
            delete(handlers::tenants::delete_tenant),
        )
        .route(
            "/api/v1/tenants/{id}/toggle",
            put(handlers::tenants::toggle_tenant),
        )
        .route(
            "/api/v1/tenants/{id}/auth",
            put(handlers::tenants::set_auth_required),
        )
        // Global retention caps (default + per-signal maximums)
        .route(
            "/api/v1/retention/global",
            get(handlers::retention::get_global_retention)
                .put(handlers::retention::set_global_retention),
        )
        // Ingest buffer status (durable spool depth + backend)
        .route("/api/v1/ingest/buffer", get(handlers::ingest_buffer::buffer_status))
        // Metric firewall (ingest-time block / drop-label rules)
        .route(
            "/api/v1/metric-firewall",
            get(handlers::metric_firewall::list)
                .post(handlers::metric_firewall::create),
        )
        .route(
            "/api/v1/metric-firewall/{id}",
            put(handlers::metric_firewall::update)
                .delete(handlers::metric_firewall::delete),
        )
        // Tenant retention overrides
        .route(
            "/api/v1/tenants/{id}/retention",
            get(handlers::retention::get_tenant_retention)
                .put(handlers::retention::set_tenant_retention),
        )
        .route(
            "/api/v1/tenants/{id}/retention/{signal}",
            delete(handlers::retention::delete_tenant_retention),
        )
        // Users (user management)
        .route(
            "/api/v1/users",
            get(handlers::users::list_users)
                .post(handlers::users::create_user),
        )
        .route(
            "/api/v1/users/{id}",
            delete(handlers::users::delete_user),
        )
        .route(
            "/api/v1/users/{id}/password",
            put(handlers::users::change_password),
        )
        .route(
            "/api/v1/users/{id}/toggle",
            put(handlers::users::toggle_user),
        )
        // Groups (RBAC group management)
        .route(
            "/api/v1/groups",
            get(handlers::groups::list_groups)
                .post(handlers::groups::create_group),
        )
        .route(
            "/api/v1/groups/{id}",
            put(handlers::groups::update_group)
                .delete(handlers::groups::delete_group),
        )
        .route(
            "/api/v1/groups/{id}/tenants",
            put(handlers::groups::set_group_tenants),
        )
        // User group membership
        .route(
            "/api/v1/users/{user_id}/groups",
            get(handlers::groups::get_user_groups)
                .put(handlers::groups::set_user_groups),
        )
        // RUM (Real User Monitoring)
        .route("/api/v1/rum/ingest", post(handlers::rum::ingest))
        .route("/api/v1/rum/apps", get(handlers::rum::list_apps))
        .route("/api/v1/rum/query", post(handlers::rum::query_events))
        .route("/api/v1/rum/vitals", post(handlers::rum::vitals))
        .route("/api/v1/rum/pages", post(handlers::rum::pages))
        .route("/api/v1/rum/errors", post(handlers::rum::errors))
        .route("/api/v1/rum/sessions", post(handlers::rum::sessions))
        .route("/api/v1/rum/session/{id}", get(handlers::rum::session_detail))
        .route("/api/v1/rum/replay/ingest", post(handlers::rum::ingest_replay))
        .route("/api/v1/rum/replay/available/{app_name}", get(handlers::rum::list_replay_sessions))
        .route("/api/v1/rum/replay/{id}", get(handlers::rum::get_replay))
        // ArgoCD integration
        .route("/api/v1/argocd/applications", get(handlers::argocd::list_applications))
        .route("/api/v1/argocd/applications/{name}", get(handlers::argocd::get_application))
        .route("/api/v1/argocd/applicationsets", get(handlers::argocd::list_applicationsets))
        // Stats
        .route("/api/v1/stats", post(handlers::stats::get_stats))
        // Signal usage
        .route("/api/v1/usage", get(handlers::usage::get_usage))
        .route("/api/v1/usage/cardinality/{metric}", get(handlers::usage::get_label_breakdown))
        // Usage metering (per-tenant ingest volume)
        .route("/api/v1/usage/summary", get(handlers::usage_metering::usage_summary))
        .route("/api/v1/usage/breakdown", get(handlers::usage_metering::usage_breakdown))
        .route("/api/v1/usage/tenants", get(handlers::usage_metering::usage_tenants))
        // ═══ Datadog Agent Ingestion ═══
        // Logs (agent log forwarder sends to {logs_dd_url}/api/v2/logs)
        .route("/datadog/v1/input", post(handlers::dd_logs::ingest_logs))
        .route("/api/v2/logs", post(handlers::dd_logs::ingest_logs))
        .route("/api/v2/logs/t/{tenant}", post(handlers::dd_logs::ingest_logs_with_tenant))
        // Metrics
        .route("/datadog/api/v1/series", post(handlers::dd_metrics::ingest_v1))
        .route("/datadog/api/v2/series", post(handlers::dd_metrics::ingest_v2))
        .route("/datadog/api/v1/check_run", post(handlers::dd_metrics::check_run))
        // Traces (dd-trace libs use PUT, dd-agent trace writer uses POST)
        .route("/datadog/api/v0.2/traces", any(handlers::dd_traces::ingest_agent))
        .route("/datadog/v0.3/traces", any(handlers::dd_traces::ingest_v03))
        .route("/datadog/v0.4/traces", any(handlers::dd_traces::ingest_v04))
        // ═══ OTLP/HTTP Ingest (OTel Collector) ═══
        .route("/v1/traces",  post(handlers::otlp::ingest_otlp_traces))
        .route("/v1/logs",    post(handlers::otlp::ingest_otlp_logs))
        .route("/v1/metrics", post(handlers::otlp::ingest_otlp_metrics))
        // Vector JSON logs
        .route("/api/v1/ingest/logs", post(handlers::otlp::ingest_vector_logs))
        // Trace stats from agent trace writer
        .route("/datadog/api/v0.6/stats", any(handlers::dd_common::stub_ok))
        .route("/datadog/api/v0.2/stats", any(handlers::dd_common::stub_ok))
        // Validate & metadata stubs
        .route("/datadog/api/v1/validate", post(handlers::dd_common::validate))
        .route("/datadog/api/v1/metadata", any(handlers::dd_common::stub_ok))
        .route("/datadog/api/v2/host_metadata", any(handlers::dd_common::stub_ok))
        .route("/datadog/api/v2/events", any(handlers::dd_common::stub_ok))
        .route("/datadog/api/v1/collector", any(handlers::dd_common::stub_ok))
        .route("/datadog/intake/", any(handlers::dd_common::stub_ok))
        .route("/datadog/intake", any(handlers::dd_common::stub_ok))
        // SSO login flow (OIDC + SAML)
        .route("/auth/sso/login", get(handlers::sso::sso_login))
        .route("/auth/sso/callback", get(handlers::sso::sso_callback))
        .route("/auth/sso/acs", post(handlers::sso::sso_acs))
        .route("/auth/sso/metadata", get(handlers::sso::sso_metadata))
        // SSO config admin endpoints
        .route(
            "/api/v1/sso/providers",
            get(handlers::sso::list_sso_providers).post(handlers::sso::save_sso_provider),
        )
        .route(
            "/api/v1/sso/providers/{id}",
            delete(handlers::sso::delete_sso_provider),
        )
        .route(
            "/api/v1/sso/mappings",
            get(handlers::sso::list_idp_group_mappings).post(handlers::sso::create_idp_group_mapping),
        )
        .route(
            "/api/v1/sso/mappings/{id}",
            delete(handlers::sso::delete_idp_group_mapping),
        )
        .route("/api/v1/sso/status", get(handlers::sso::sso_status))
        .route(
            "/api/v1/sso/setup-token",
            post(handlers::sso::create_setup_token),
        )
        .route(
            "/api/v1/sso/setup-token/{token}/validate",
            get(handlers::sso::validate_setup_token),
        )
        .route(
            "/api/v1/sso/setup-token/{token}/complete",
            post(handlers::sso::complete_setup_token),
        )
        // Auth
        .route("/api/v1/auth/login", post(handlers::auth::login))
        .route("/api/v1/auth/logout", post(handlers::auth::logout))
        .route("/api/v1/auth/me", get(handlers::auth::me))
        // Health
        .route("/healthz", get(handlers::health::healthz))
        // Catch-all for unmatched DD agent paths (debug logging)
        .fallback(|req: axum::http::Request<axum::body::Body>| async move {
            tracing::warn!(
                method = %req.method(),
                uri = %req.uri(),
                content_type = req.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("none"),
                "unmatched request"
            );
            (axum::http::StatusCode::NOT_FOUND, "not found")
        })
        .layer({
            let origins = std::env::var("RUSH_ALLOWED_ORIGINS")
                .ok()
                .filter(|s| !s.is_empty())
                .map(|s| {
                    s.split(',')
                        .filter_map(|o| o.trim().parse::<HeaderValue>().ok())
                        .collect::<Vec<_>>()
                });
            match origins {
                Some(list) => CorsLayer::new()
                    .allow_origin(AllowOrigin::list(list))
                    .allow_methods([
                        axum::http::Method::GET,
                        axum::http::Method::POST,
                        axum::http::Method::PUT,
                        axum::http::Method::DELETE,
                        axum::http::Method::PATCH,
                        axum::http::Method::OPTIONS,
                    ])
                    .allow_headers([
                        header::CONTENT_TYPE,
                        header::AUTHORIZATION,
                        header::HeaderName::from_static("x-rush-tenant"),
                        header::HeaderName::from_static("dd-api-key"),
                    ])
                    .allow_credentials(true),
                None => {
                    // No RUSH_ALLOWED_ORIGINS set — restrict to same-origin only.
                    // Set RUSH_ALLOWED_ORIGINS=http://localhost:5173 for local dev.
                    tracing::warn!(
                        "RUSH_ALLOWED_ORIGINS not set; CORS restricted to same-origin. \
                         Set this variable for cross-origin access."
                    );
                    CorsLayer::new()
                        .allow_origin(AllowOrigin::exact(HeaderValue::from_static("null")))
                        .allow_methods([
                            axum::http::Method::GET,
                            axum::http::Method::POST,
                            axum::http::Method::PUT,
                            axum::http::Method::DELETE,
                            axum::http::Method::PATCH,
                            axum::http::Method::OPTIONS,
                        ])
                        .allow_headers([
                            header::CONTENT_TYPE,
                            header::AUTHORIZATION,
                            header::HeaderName::from_static("x-rush-tenant"),
                            header::HeaderName::from_static("dd-api-key"),
                        ])
                }
            }
        })
        .layer(CompressionLayer::new())
        .layer(axum::middleware::from_fn(security_headers_middleware))
        .layer(TraceLayer::new_for_http())
        .layer(axum::middleware::from_fn_with_state(state.clone(), tenant_middleware))
        .with_state(state);

    let port: u16 = std::env::var("RUSH_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    // FINDING-13: Warn when ClickHouse row policies are not active.
    // Without row policies, tenant isolation is enforced only at the API layer.
    // Configure `custom_settings_prefixes = 'rush_'` in ClickHouse for DB-layer isolation.
    if !rush_api::row_policy_supported() {
        tracing::warn!(
            row_policies = rush_api::row_policy_supported(),
            "ClickHouse row-level security policies are NOT active. \
             Tenant isolation relies solely on API-layer WHERE injection. \
             Set custom_settings_prefixes = 'rush_' in ClickHouse config to enable row policies."
        );
    }
    // L3: Warn when RUSH_BASE_URL is unset — SAML/OIDC redirect URIs derived from Host header.
    if std::env::var("RUSH_BASE_URL").map(|s| s.is_empty()).unwrap_or(true) {
        tracing::warn!(
            "RUSH_BASE_URL is not set. SAML ACS and OIDC redirect URIs will be derived from \
             the Host request header, which can be spoofed. Set RUSH_BASE_URL to your \
             public hostname for production deployments."
        );
    }

    // R01: Warn when RUSH_API_KEY_SECRET is unset or too short.
    // An empty or short secret means HMAC-SHA256 provides no real keyed-hash protection.
    match std::env::var("RUSH_API_KEY_SECRET") {
        Ok(s) if s.len() >= 32 => {}
        Ok(s) if s.is_empty() => tracing::warn!(
            "RUSH_API_KEY_SECRET is not set. API key hashes are stored with an empty HMAC key. \
             Set RUSH_API_KEY_SECRET to a random 32+ character secret before deployment."
        ),
        Ok(_) => tracing::warn!(
            "RUSH_API_KEY_SECRET is shorter than 32 characters. \
             Use a random secret of at least 32 characters for adequate HMAC security."
        ),
        Err(_) => tracing::warn!(
            "RUSH_API_KEY_SECRET is not set. API key hashes are stored with an empty HMAC key. \
             Set RUSH_API_KEY_SECRET to a random 32+ character secret before deployment."
        ),
    }

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        port = port,
        clickhouse_url = %clickhouse_url,
        row_policies = rush_api::row_policy_supported(),
        "rush-api started"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
