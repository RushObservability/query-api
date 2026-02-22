use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use wide_query_api::alert_engine::SmtpConfig;
use wide_query_api::anomaly_engine;
use wide_query_api::config_db::ConfigDb;
use wide_query_api::migrations;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("wide_query_api=debug")
        }))
        .init();

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let clickhouse_db =
        std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "observability".to_string());
    let clickhouse_user =
        std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let clickhouse_password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    migrations::run(&clickhouse_url, &clickhouse_user, &clickhouse_password).await?;

    let ch = clickhouse::Client::default()
        .with_url(&clickhouse_url)
        .with_database(&clickhouse_db)
        .with_user(&clickhouse_user)
        .with_password(&clickhouse_password);

    let config_db_path =
        std::env::var("WIDE_CONFIG_DB").unwrap_or_else(|_| "./wide_config.db".to_string());
    let config_db = Arc::new(ConfigDb::open(&config_db_path)?);
    tracing::info!("config db opened at {config_db_path}");

    let smtp_config = SmtpConfig {
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

    let prom_base_url = std::env::var("WIDE_PROM_BASE_URL")
        .unwrap_or_else(|_| "http://localhost:8080".to_string());

    tracing::info!("wide-anomaly-engine starting");
    anomaly_engine::run_anomaly_engine(config_db, ch, smtp_config, prom_base_url).await;

    Ok(())
}
