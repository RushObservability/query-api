use axum::{Router, routing::delete, routing::get, routing::post, routing::put};
use clickhouse::Client;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use wide_query_api::alert_engine;
use wide_query_api::config_db::ConfigDb;
use wide_query_api::handlers;
use wide_query_api::migrations;
use wide_query_api::slo_engine;
use wide_query_api::usage_tracker;
use wide_query_api::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("wide_query_api=debug,tower_http=debug")
        }))
        .init();

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let clickhouse_db =
        std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "observability".to_string());

    let clickhouse_user =
        std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let clickhouse_password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    // Run migrations before creating the database-scoped client.
    // This ensures the database and all tables exist on every startup.
    migrations::run(&clickhouse_url, &clickhouse_user, &clickhouse_password).await?;

    let ch = Client::default()
        .with_url(&clickhouse_url)
        .with_database(&clickhouse_db)
        .with_user(&clickhouse_user)
        .with_password(&clickhouse_password);

    let config_db_path =
        std::env::var("WIDE_CONFIG_DB").unwrap_or_else(|_| "./wide_config.db".to_string());
    let config_db = Arc::new(ConfigDb::open(&config_db_path)?);
    tracing::info!("config db opened at {config_db_path}");

    // SMTP config for email notifications (optional)
    let smtp_config = alert_engine::SmtpConfig {
        host: std::env::var("WIDE_SMTP_HOST").ok(),
        port: std::env::var("WIDE_SMTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(587),
        user: std::env::var("WIDE_SMTP_USER").ok(),
        pass: std::env::var("WIDE_SMTP_PASS").ok(),
        from: std::env::var("WIDE_SMTP_FROM")
            .unwrap_or_else(|_| "wide@localhost".to_string()),
    };

    // Spawn background engines
    alert_engine::spawn_alert_engine(config_db.clone(), ch.clone(), smtp_config);
    slo_engine::spawn_slo_engine(config_db.clone(), ch.clone());

    // Spawn usage tracker (fire-and-forget signal usage tracking)
    let usage = usage_tracker::spawn(ch.clone());

    let state = AppState {
        ch,
        config_db,
        usage,
    };

    let app = Router::new()
        // Trace endpoints
        .route("/api/v1/traces/{trace_id}", get(handlers::traces::get_trace))
        // Query endpoints
        .route("/api/v1/query", post(handlers::query::execute_query))
        .route("/api/v1/query/count", post(handlers::query::count_query))
        .route("/api/v1/query/group", post(handlers::query::group_query))
        .route("/api/v1/query/timeseries", post(handlers::query::timeseries_query))
        // Log endpoints
        .route("/api/v1/logs", post(handlers::logs::query_logs))
        .route("/api/v1/logs/count", post(handlers::logs::count_logs))
        // Service catalog
        .route("/api/v1/services", get(handlers::services::list_services))
        .route("/api/v1/services/graph", get(handlers::services::service_graph))
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
        // Notification channels
        .route(
            "/api/v1/channels",
            get(handlers::alerts::list_channels).post(handlers::alerts::create_channel),
        )
        .route(
            "/api/v1/channels/{id}",
            delete(handlers::alerts::delete_channel),
        )
        .route(
            "/api/v1/channels/{id}/notify",
            post(handlers::alerts::notify_channel),
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
        // Deploy markers
        .route(
            "/api/v1/deploys",
            get(handlers::deploys::list_deploys).post(handlers::deploys::create_deploy),
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
        // Stats
        .route("/api/v1/stats", post(handlers::stats::get_stats))
        // Signal usage
        .route("/api/v1/usage", get(handlers::usage::get_usage))
        .route("/api/v1/usage/cardinality/{metric}", get(handlers::usage::get_label_breakdown))
        // Health
        .route("/healthz", get(handlers::health::healthz))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!("wide-query-api listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
