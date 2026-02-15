use std::sync::Arc;
use crate::config_db::ConfigDb;
use crate::models::query::Filter;
use crate::query_builder::build_where_clause;
use clickhouse::Client;
use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

#[derive(Debug, Clone)]
pub struct SmtpConfig {
    pub host: Option<String>,
    pub port: u16,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub from: String,
}

#[derive(Debug, serde::Deserialize)]
struct AlertQueryConfig {
    #[serde(default = "default_time_range")]
    time_range_minutes: i64,
    #[serde(default)]
    filters: Vec<Filter>,
}

fn default_time_range() -> i64 {
    5
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    count: u64,
}

fn build_smtp_transport(cfg: &SmtpConfig) -> Option<AsyncSmtpTransport<Tokio1Executor>> {
    let host = cfg.host.as_deref()?;
    let mut builder = AsyncSmtpTransport::<Tokio1Executor>::relay(host).ok()?;
    builder = builder.port(cfg.port);
    if let (Some(user), Some(pass)) = (&cfg.user, &cfg.pass) {
        builder = builder.credentials(Credentials::new(user.clone(), pass.clone()));
    }
    Some(builder.build())
}

pub fn spawn_alert_engine(config_db: Arc<ConfigDb>, ch: Client, smtp_config: SmtpConfig) {
    tokio::spawn(async move {
        let http_client = reqwest::Client::new();
        let smtp_transport = build_smtp_transport(&smtp_config);
        if smtp_transport.is_some() {
            tracing::info!("alert engine: SMTP configured for email notifications");
        }
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            if let Err(e) = eval_alerts(&config_db, &ch, &http_client, &smtp_config, &smtp_transport).await {
                tracing::error!("alert engine error: {e}");
            }
        }
    });
}

async fn eval_alerts(
    config_db: &ConfigDb,
    ch: &Client,
    http_client: &reqwest::Client,
    smtp_config: &SmtpConfig,
    smtp_transport: &Option<AsyncSmtpTransport<Tokio1Executor>>,
) -> anyhow::Result<()> {
    let now = chrono::Utc::now();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let due_alerts = config_db.get_due_alerts(&now_str)?;

    for rule in due_alerts {
        let query_config: AlertQueryConfig = match serde_json::from_str(&rule.query_config) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("alert {}: bad query_config: {e}", rule.id);
                continue;
            }
        };

        let from = (now - chrono::Duration::minutes(query_config.time_range_minutes))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        let where_clause = build_where_clause(&query_config.filters, &from, &now_str);
        let sql = format!("SELECT count() as count FROM wide_events WHERE {where_clause}");

        let value = match ch.query(&sql).fetch_one::<CountRow>().await {
            Ok(row) => row.count as f64,
            Err(e) => {
                tracing::warn!("alert {}: query failed: {e}", rule.id);
                config_db.update_alert_state(&rule.id, "no_data", &now_str, None)?;
                continue;
            }
        };

        let threshold = rule.condition_threshold;
        let triggered = match rule.condition_op.as_str() {
            ">" => value > threshold,
            ">=" => value >= threshold,
            "<" => value < threshold,
            "<=" => value <= threshold,
            "=" => (value - threshold).abs() < f64::EPSILON,
            "!=" => (value - threshold).abs() >= f64::EPSILON,
            _ => false,
        };

        let new_state = if triggered { "alerting" } else { "ok" };
        let old_state = rule.state.as_str();

        if new_state != old_state {
            // State changed — record event and notify
            let event_id = uuid::Uuid::new_v4().to_string();
            let message = format!(
                "Alert '{}': {} (value={}, threshold={} {})",
                rule.name,
                if triggered { "FIRING" } else { "RESOLVED" },
                value,
                threshold,
                rule.condition_op,
            );

            config_db.create_alert_event(
                &event_id,
                &rule.id,
                new_state,
                value,
                threshold,
                &message,
            )?;

            let triggered_at = if triggered { Some(now_str.as_str()) } else { None };
            config_db.update_alert_state(&rule.id, new_state, &now_str, triggered_at)?;

            // Send notifications
            let channel_ids: Vec<String> = serde_json::from_str(&rule.notification_channel_ids)
                .unwrap_or_default();
            for channel_id in &channel_ids {
                if let Ok(Some(channel)) = config_db.get_channel(channel_id) {
                    let config: serde_json::Value = serde_json::from_str(&channel.config)
                        .unwrap_or(serde_json::json!({}));

                    match channel.channel_type.as_str() {
                        "email" => {
                            if let Some(to_addr) = config.get("to").and_then(|t| t.as_str()) {
                                if let Some(transport) = smtp_transport {
                                    let subject = format!(
                                        "[Wide Alert] {} - {}",
                                        rule.name,
                                        if triggered { "FIRING" } else { "RESOLVED" }
                                    );
                                    match Message::builder()
                                        .from(smtp_config.from.parse().unwrap_or_else(|_| "wide@localhost".parse().unwrap()))
                                        .to(to_addr.parse().unwrap_or_else(|_| "noreply@localhost".parse().unwrap()))
                                        .subject(subject)
                                        .header(ContentType::TEXT_PLAIN)
                                        .body(message.clone())
                                    {
                                        Ok(email) => {
                                            if let Err(e) = transport.send(email).await {
                                                tracing::warn!("alert {}: email to {} failed: {e}", rule.id, to_addr);
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("alert {}: failed to build email: {e}", rule.id);
                                        }
                                    }
                                } else {
                                    tracing::warn!("alert {}: email channel configured but SMTP not set up", rule.id);
                                }
                            }
                        }
                        "slack" => {
                            if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                                let payload = serde_json::json!({ "text": message });
                                if let Err(e) = http_client.post(url).json(&payload).send().await {
                                    tracing::warn!("alert {}: notification to {} failed: {e}", rule.id, channel.name);
                                }
                            }
                        }
                        _ => {
                            // webhook
                            if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                                let payload = serde_json::json!({
                                    "alert": rule.name,
                                    "state": new_state,
                                    "value": value,
                                    "threshold": threshold,
                                    "message": message,
                                });
                                if let Err(e) = http_client.post(url).json(&payload).send().await {
                                    tracing::warn!("alert {}: notification to {} failed: {e}", rule.id, channel.name);
                                }
                            }
                        }
                    }
                }
            }

            tracing::info!("alert '{}' state: {} -> {}", rule.name, old_state, new_state);
        } else {
            config_db.update_alert_state(&rule.id, new_state, &now_str, None)?;
        }
    }

    Ok(())
}
