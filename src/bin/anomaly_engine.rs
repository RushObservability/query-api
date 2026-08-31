// Use jemalloc as the global allocator (see src/main.rs / Cargo.toml). Declared
// per binary crate root, so this mirrors the decl in the main rush-api binary.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use rush_api::alert_engine::SmtpConfig;
use rush_api::anomaly_engine;
use rush_api::clickhouse_config::ConfigDb;
use rush_api::config::RushConfig;
use rush_api::migrations;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("rush_api=debug")),
        )
        .init();

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let clickhouse_db =
        std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "observability".to_string());
    let clickhouse_user =
        std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let clickhouse_password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    let wide_config_path =
        std::env::var("RUSH_CONFIG").unwrap_or_else(|_| "./rush.toml".to_string());
    let wide_config = RushConfig::load(&wide_config_path)?;

    migrations::run(
        &clickhouse_url,
        &clickhouse_user,
        &clickhouse_password,
        &wide_config,
    )
    .await?;

    let write_ch = clickhouse::Client::default()
        .with_url(&clickhouse_url)
        .with_database(&clickhouse_db)
        .with_user(&clickhouse_user)
        .with_password(&clickhouse_password);

    let read_user = std::env::var("CLICKHOUSE_READ_USER")?;
    let read_password = std::env::var("CLICKHOUSE_READ_PASSWORD")?;
    if read_user == clickhouse_user {
        anyhow::bail!("CLICKHOUSE_READ_USER must differ from CLICKHOUSE_USER");
    }
    let read_ch = clickhouse::Client::default()
        .with_url(&clickhouse_url)
        .with_database(&clickhouse_db)
        .with_user(&read_user)
        .with_password(&read_password);

    rush_api::probe_row_policy_support(&write_ch).await?;
    migrations::apply_row_policies(&write_ch, &read_user).await?;
    migrations::verify_row_policies(&write_ch, &read_ch, &read_user).await?;
    rush_api::mark_row_policy_enforced();

    let config_db =
        Arc::new(ConfigDb::open(&clickhouse_url, &clickhouse_user, &clickhouse_password).await?);
    tracing::info!("config db opened");

    let smtp_config = SmtpConfig {
        host: std::env::var("RUSH_SMTP_HOST").ok(),
        port: std::env::var("RUSH_SMTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(587),
        user: std::env::var("RUSH_SMTP_USER").ok(),
        pass: std::env::var("RUSH_SMTP_PASS").ok(),
        from: std::env::var("RUSH_SMTP_FROM").unwrap_or_else(|_| "wide@localhost".to_string()),
    };

    let prom_base_url =
        std::env::var("RUSH_PROM_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());

    tracing::info!("wide-anomaly-engine starting");
    // The standalone engine has no /metrics endpoint; give it a private registry so the
    // engine-loop instrumentation still works (record_engine just updates in-memory atomics).
    let self_metrics = Arc::new(rush_api::self_metrics::SelfMetrics::new());
    let query_limits = config_db
        .get_setting(rush_api::query_governor::QUERY_LIMITS_SETTING_KEY)
        .await?
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default();
    let query_governor = Arc::new(
        rush_api::query_governor::QueryGovernor::new(query_limits, self_metrics.clone())
            .map_err(anyhow::Error::msg)?,
    );
    rush_api::query_governor::install_global(query_governor);
    anomaly_engine::run_anomaly_engine(
        config_db,
        read_ch,
        write_ch,
        smtp_config,
        prom_base_url,
        self_metrics,
    )
    .await;

    Ok(())
}
