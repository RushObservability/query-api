use std::sync::Arc;
use crate::clickhouse_config::ConfigDb;
use crate::models::query::Filter;
use crate::query_builder::{build_where_clause, build_metrics_where_clause, build_logs_where_clause};
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

/// Build a rich Slack payload with colored attachment, metadata fields, and action buttons.
/// Follows the Grafana/Datadog pattern: legacy attachment title/text/fields for the card,
/// plus a Block Kit `actions` block inside the attachment for clickable buttons.
fn build_slack_payload(
    alert_name: &str,
    alert_state: &str,
    value: f64,
    threshold: f64,
    signal_type: &str,
    condition_op: &str,
    description: &str,
    alert_id: &str,
    runbook_url: &str,
) -> serde_json::Value {
    let is_firing = !matches!(alert_state, "RESOLVED" | "ok" | "TEST");
    let is_test   = alert_state == "TEST";

    let (color, status_emoji, status_label) = if is_test {
        ("#888888", "🔔", "TEST")
    } else if is_firing {
        ("#E53E3E", "🚨", "FIRING")
    } else {
        ("#38A169", "✅", "RESOLVED")
    };

    let signal_label = match signal_type {
        "metrics"  => "Metrics",
        "logs"     => "Logs",
        "apm"      => "APM / Traces",
        "monitors" => "Monitor",
        s if s.is_empty() => "—",
        s => s,
    };

    let fmt_num = |n: f64| -> String {
        if n.fract() == 0.0 { format!("{}", n as i64) } else { format!("{n:.2}") }
    };
    let value_str     = fmt_num(value);
    let threshold_str = fmt_num(threshold);

    let condition_str = if condition_op.is_empty() {
        "—".to_string()
    } else {
        format!("{condition_op} {threshold_str}")
    };

    let ts = chrono::Utc::now().timestamp();
    let fallback = format!("[{status_label}] {alert_name} — {value_str} {condition_op} {threshold_str}");

    // Resolve base URL for deep links; fall back gracefully if unset
    let base_url = std::env::var("RUSH_BASE_URL")
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_string();

    // Build action buttons
    let view_query_path = match signal_type {
        "metrics"  => "/?mode=metrics",
        "logs"     => "/?mode=logs",
        "apm"      => "/?mode=traces",
        "monitors" => "/monitors",
        _          => "/",
    };
    let mut buttons: Vec<serde_json::Value> = Vec::new();
    if !alert_id.is_empty() && !base_url.is_empty() {
        buttons.push(serde_json::json!({
            "type": "button",
            "text": { "type": "plain_text", "text": "View Alert", "emoji": true },
            "url": format!("{base_url}/alerts/{alert_id}"),
            "style": "primary",
        }));
        buttons.push(serde_json::json!({
            "type": "button",
            "text": { "type": "plain_text", "text": "View Query", "emoji": true },
            "url": format!("{base_url}{view_query_path}"),
        }));
    }
    if !runbook_url.is_empty() {
        buttons.push(serde_json::json!({
            "type": "button",
            "text": { "type": "plain_text", "text": "📖 Runbook", "emoji": true },
            "url": runbook_url,
        }));
    }

    // Metadata fields (hidden for test notifications)
    let fields: Vec<serde_json::Value> = if is_test {
        vec![]
    } else {
        vec![
            serde_json::json!({ "title": "Signal",    "value": signal_label,  "short": true }),
            serde_json::json!({ "title": "Value",     "value": value_str,     "short": true }),
            serde_json::json!({ "title": "Condition", "value": condition_str, "short": true }),
            serde_json::json!({ "title": "Status",    "value": format!("{status_emoji} {status_label}"), "short": true }),
        ]
    };

    // Compose the attachment: legacy fields for the card + optional actions block
    let mut attachment = serde_json::json!({
        "color":    color,
        "fallback": fallback,
        "title":    format!("{status_emoji} [{status_label}] {alert_name}"),
        "text":     description,
        "fields":   fields,
        "footer":   "Rush Observability",
        "ts":       ts,
    });
    if !buttons.is_empty() {
        attachment["actions"] = serde_json::json!(buttons);
    }

    serde_json::json!({ "attachments": [attachment] })
}

/// Send a notification to a channel and log the result.
/// Returns Ok(()) on success or Err with the error message.
pub async fn send_channel_notification(
    channel: &crate::models::alert::NotificationChannel,
    message: &str,
    alert_name: &str,
    alert_state: &str,
    value: f64,
    threshold: f64,
    signal_type: &str,
    condition_op: &str,
    description: &str,
    alert_id: &str,
    runbook_url: &str,
    http_client: &reqwest::Client,
    smtp_config: &SmtpConfig,
    smtp_transport: &Option<AsyncSmtpTransport<Tokio1Executor>>,
) -> Result<(), String> {
    let config: serde_json::Value = serde_json::from_str(&channel.config)
        .unwrap_or(serde_json::json!({}));

    match channel.channel_type.as_str() {
        "email" => {
            let recipients = config.get("recipients")
                .and_then(|r| r.as_str())
                .or_else(|| config.get("to").and_then(|t| t.as_str()))
                .ok_or_else(|| "email channel config missing recipients".to_string())?;

            let transport = smtp_transport.as_ref()
                .ok_or_else(|| "email channel configured but SMTP not set up".to_string())?;

            let subject = format!(
                "[Rush Alert] {} - {}",
                alert_name,
                alert_state,
            );

            // Send to each recipient
            for to_addr in recipients.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                match Message::builder()
                    .from(smtp_config.from.parse().unwrap_or_else(|_| "wide@localhost".parse().unwrap()))
                    .to(to_addr.parse().unwrap_or_else(|_| "noreply@localhost".parse().unwrap()))
                    .subject(&subject)
                    .header(ContentType::TEXT_PLAIN)
                    .body(message.to_string())
                {
                    Ok(email) => {
                        if let Err(e) = transport.send(email).await {
                            return Err(format!("email to {to_addr} failed: {e}"));
                        }
                    }
                    Err(e) => {
                        return Err(format!("failed to build email: {e}"));
                    }
                }
            }
            Ok(())
        }
        "slack" => {
            let url = config.get("webhook_url")
                .or_else(|| config.get("url"))
                .and_then(|u| u.as_str())
                .ok_or_else(|| "slack channel config missing webhook_url".to_string())?;

            if !url.starts_with("https://") {
                return Err(format!("channel URL must use HTTPS (got: {url})"));
            }

            let payload = build_slack_payload(alert_name, alert_state, value, threshold, signal_type, condition_op, description, alert_id, runbook_url);
            http_client.post(url).json(&payload).send().await
                .map_err(|e| format!("slack notification failed: {e}"))?;
            Ok(())
        }
        "webhook" => {
            let url = config.get("url")
                .and_then(|u| u.as_str())
                .ok_or_else(|| "webhook channel config missing url".to_string())?;

            if !url.starts_with("https://") {
                return Err(format!("channel URL must use HTTPS (got: {url})"));
            }

            let method = config.get("method")
                .and_then(|m| m.as_str())
                .unwrap_or("POST");

            let payload = serde_json::json!({
                "alert": alert_name,
                "state": alert_state,
                "value": value,
                "threshold": threshold,
                "message": message,
            });

            let mut req_builder = match method.to_uppercase().as_str() {
                "PUT" => http_client.put(url),
                _ => http_client.post(url),
            };

            // Apply custom headers
            if let Some(headers) = config.get("headers").and_then(|h| h.as_object()) {
                for (k, v) in headers {
                    if let Some(vs) = v.as_str() {
                        req_builder = req_builder.header(k, vs);
                    }
                }
            }

            req_builder.json(&payload).send().await
                .map_err(|e| format!("webhook notification failed: {e}"))?;
            Ok(())
        }
        "pagerduty" => {
            let routing_key = config.get("routing_key")
                .and_then(|r| r.as_str())
                .ok_or_else(|| "pagerduty channel config missing routing_key".to_string())?;

            let event_action = if alert_state == "ok" || alert_state == "RESOLVED" {
                "resolve"
            } else {
                "trigger"
            };

            let pd_severity = config.get("severity_mapping")
                .and_then(|m| m.get("critical"))
                .and_then(|s| s.as_str())
                .unwrap_or("critical");

            let payload = serde_json::json!({
                "routing_key": routing_key,
                "event_action": event_action,
                "dedup_key": format!("rush-alert-{}", alert_name.replace(' ', "-").to_lowercase()),
                "payload": {
                    "summary": message,
                    "severity": pd_severity,
                    "source": "rush-observability",
                    "custom_details": {
                        "value": value,
                        "threshold": threshold,
                    }
                }
            });

            http_client
                .post("https://events.pagerduty.com/v2/enqueue")
                .json(&payload)
                .send()
                .await
                .map_err(|e| format!("pagerduty notification failed: {e}"))?;
            Ok(())
        }
        "opsgenie" => {
            let api_key = config.get("api_key")
                .and_then(|k| k.as_str())
                .ok_or_else(|| "opsgenie channel config missing api_key".to_string())?;

            if alert_state == "ok" || alert_state == "RESOLVED" {
                // Close the alert
                let alias = format!("rush-alert-{}", alert_name.replace(' ', "-").to_lowercase());
                let close_url = format!("https://api.opsgenie.com/v2/alerts/{}/close", alias);
                let payload = serde_json::json!({
                    "source": "rush-observability",
                    "note": message,
                });
                http_client
                    .post(&close_url)
                    .header("Authorization", format!("GenieKey {api_key}"))
                    .query(&[("identifierType", "alias")])
                    .json(&payload)
                    .send()
                    .await
                    .map_err(|e| format!("opsgenie close failed: {e}"))?;
            } else {
                let priority = config.get("priority_mapping")
                    .and_then(|m| m.get("critical"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("P1");

                let mut og_payload = serde_json::json!({
                    "message": message,
                    "alias": format!("rush-alert-{}", alert_name.replace(' ', "-").to_lowercase()),
                    "priority": priority,
                    "source": "rush-observability",
                    "details": {
                        "value": value,
                        "threshold": threshold,
                    }
                });

                if let Some(responders) = config.get("responders") {
                    og_payload["responders"] = responders.clone();
                }
                if let Some(tags) = config.get("tags") {
                    og_payload["tags"] = tags.clone();
                }

                http_client
                    .post("https://api.opsgenie.com/v2/alerts")
                    .header("Authorization", format!("GenieKey {api_key}"))
                    .json(&og_payload)
                    .send()
                    .await
                    .map_err(|e| format!("opsgenie notification failed: {e}"))?;
            }
            Ok(())
        }
        "slack_app" => {
            let token = config.get("token")
                .and_then(|t| t.as_str())
                .ok_or_else(|| "slack_app channel config missing token".to_string())?;
            let channel = config.get("channel")
                .and_then(|c| c.as_str())
                .ok_or_else(|| "slack_app channel config missing channel".to_string())?;
            let mut payload = build_slack_payload(alert_name, alert_state, value, threshold, signal_type, condition_op, description, alert_id, runbook_url);
            payload["channel"] = serde_json::json!(channel);
            payload["username"] = serde_json::json!(config.get("username").and_then(|u| u.as_str()).unwrap_or("Rush Alerts"));
            http_client
                .post("https://slack.com/api/chat.postMessage")
                .header("Authorization", format!("Bearer {token}"))
                .json(&payload)
                .send()
                .await
                .map_err(|e| format!("slack_app notification failed: {e}"))?;
            Ok(())
        }
        "discord" => {
            let url = config.get("webhook_url")
                .and_then(|u| u.as_str())
                .ok_or_else(|| "discord channel config missing webhook_url".to_string())?;
            if !url.starts_with("https://") {
                return Err(format!("channel URL must use HTTPS (got: {url})"));
            }
            let color: u32 = if alert_state == "RESOLVED" || alert_state == "ok" { 0x57F287 } else { 0xED4245 };
            let payload = serde_json::json!({
                "embeds": [{
                    "title": format!("[{}] {}", alert_state, alert_name),
                    "description": message,
                    "color": color,
                    "fields": [
                        { "name": "Value", "value": value.to_string(), "inline": true },
                        { "name": "Threshold", "value": threshold.to_string(), "inline": true },
                    ],
                }]
            });
            http_client.post(url).json(&payload).send().await
                .map_err(|e| format!("discord notification failed: {e}"))?;
            Ok(())
        }
        "alertmanager" => {
            let base_url = config.get("url")
                .and_then(|u| u.as_str())
                .ok_or_else(|| "alertmanager channel config missing url".to_string())?;
            if !base_url.starts_with("https://") {
                return Err(format!("channel URL must use HTTPS (got: {base_url})"));
            }
            let api_url = format!("{}/api/v2/alerts", base_url.trim_end_matches('/'));
            let status = if alert_state == "RESOLVED" || alert_state == "ok" { "resolved" } else { "firing" };
            let extra_labels = config.get("labels").cloned().unwrap_or_else(|| serde_json::json!({}));
            let mut labels = serde_json::json!({ "alertname": alert_name, "severity": "critical" });
            if let (Some(lobj), Some(eobj)) = (labels.as_object_mut(), extra_labels.as_object()) {
                for (k, v) in eobj { lobj.insert(k.clone(), v.clone()); }
            }
            let payload = serde_json::json!([{
                "labels": labels,
                "annotations": { "summary": message, "value": value.to_string() },
                "status": status,
            }]);
            http_client.post(&api_url).json(&payload).send().await
                .map_err(|e| format!("alertmanager notification failed: {e}"))?;
            Ok(())
        }
        other => {
            Err(format!("unsupported channel type: {other}"))
        }
    }
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
    let due_alerts = config_db.get_due_alerts(&now_str).await?;

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

        let sql = match rule.signal_type.as_str() {
            "metrics" => {
                let mc = build_metrics_where_clause(&query_config.filters, &from, &now_str);
                let s = mc.to_sql();
                format!(
                    "SELECT count() as count FROM (\
                     SELECT TimeUnix FROM observability.otel_metrics_gauge {s} \
                     UNION ALL SELECT TimeUnix FROM observability.otel_metrics_sum {s} \
                     UNION ALL SELECT TimeUnix FROM observability.otel_metrics_histogram {s} \
                     UNION ALL SELECT TimeUnix FROM observability.otel_metrics_exponential_histogram {s} \
                     UNION ALL SELECT TimeUnix FROM observability.otel_metrics_summary {s})"
                )
            }
            "logs" => {
                let lc = build_logs_where_clause(&query_config.filters, &from, &now_str);
                format!("SELECT count() as count FROM observability.otel_logs {}", lc.to_sql())
            }
            _ => {
                // "apm" (default) — query wide_events
                let wc = build_where_clause(&query_config.filters, &from, &now_str);
                format!("SELECT count() as count FROM wide_events {}", wc.to_sql())
            }
        };

        let value = match ch.query(&sql).fetch_one::<CountRow>().await {
            Ok(row) => row.count as f64,
            Err(e) => {
                tracing::warn!("alert {}: query failed: {e}", rule.id);
                config_db.update_alert_state(&rule.id, "no_data", &now_str, None).await?;
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
            ).await?;

            let triggered_at = if triggered { Some(now_str.as_str()) } else { None };
            config_db.update_alert_state(&rule.id, new_state, &now_str, triggered_at).await?;

            // Skip notifications during active maintenance windows
            if config_db.is_in_maintenance(&now_str, Some(&rule.id)).await {
                tracing::debug!("alert '{}': skipping notification — maintenance window active", rule.id);
                continue;
            }

            // Send notifications
            let channel_ids: Vec<String> = serde_json::from_str(&rule.notification_channel_ids)
                .unwrap_or_default();
            let alert_state_str = if triggered { "FIRING" } else { "RESOLVED" };
            for channel_id in &channel_ids {
                if let Ok(Some(channel)) = config_db.get_channel_by_id(channel_id).await {
                    if !channel.enabled {
                        continue;
                    }
                    let result = send_channel_notification(
                        &channel,
                        &message,
                        &rule.name,
                        alert_state_str,
                        value,
                        threshold,
                        &rule.signal_type,
                        &rule.condition_op,
                        &rule.description,
                        &rule.id,
                        &rule.runbook_url,
                        http_client,
                        smtp_config,
                        smtp_transport,
                    ).await;

                    let (status, error_msg) = match &result {
                        Ok(()) => ("sent", String::new()),
                        Err(e) => {
                            tracing::warn!("alert {}: notification to {} failed: {e}", rule.id, channel.name);
                            ("failed", e.clone())
                        }
                    };

                    let _ = config_db.create_notification_log(
                        channel_id,
                        &channel.tenant_id,
                        "alert_rule",
                        &rule.name,
                        "",
                        status,
                        &error_msg,
                    ).await;
                }
            }

            tracing::info!("alert '{}' state: {} -> {}", rule.name, old_state, new_state);
        } else {
            config_db.update_alert_state(&rule.id, new_state, &now_str, None).await?;
        }
    }

    Ok(())
}
