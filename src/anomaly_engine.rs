use std::sync::Arc;
use crate::alert_engine::SmtpConfig;
use crate::config_db::ConfigDb;
use crate::models::anomaly::AnomalyRule;
use clickhouse::Client;
use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

#[derive(clickhouse::Row, serde::Deserialize)]
struct ApmBucket {
    bucket: u32,
    count: u64,
    error_count: u64,
    p50: f64,
    p95: f64,
    p99: f64,
}

#[derive(Debug, serde::Deserialize)]
struct PromResponse {
    data: Option<PromData>,
}

#[derive(Debug, serde::Deserialize)]
struct PromData {
    result: Vec<PromSeries>,
}

#[derive(Debug, serde::Deserialize)]
struct PromSeries {
    metric: std::collections::BTreeMap<String, String>,
    values: Vec<(f64, String)>,
}

struct EwmaResult {
    mean: f64,
    anomalous: bool,
    deviation: f64,
}

fn ewma_eval(data: &[f64], alpha: f64, sensitivity: f64) -> EwmaResult {
    let warmup = 12.min(data.len());
    if warmup == 0 {
        return EwmaResult { mean: 0.0, anomalous: false, deviation: 0.0 };
    }

    let mut sum = 0.0;
    for v in &data[..warmup] {
        sum += v;
    }
    let mut mean = sum / warmup as f64;
    let mut var_sum = 0.0;
    for v in &data[..warmup] {
        var_sum += (v - mean).powi(2);
    }
    let mut variance = var_sum / warmup as f64;
    let min_var = variance * 0.3;

    for (i, &val) in data.iter().enumerate() {
        let std = variance.sqrt();
        let upper = mean + sensitivity * std;
        let lower = (mean - sensitivity * std).max(0.0);

        let is_anomaly = i >= warmup && (val > upper || val < lower);

        if i > 0 && !is_anomaly {
            let diff = val - mean;
            mean = alpha * val + (1.0 - alpha) * mean;
            let va = alpha * 0.25;
            variance = (va * diff * diff + (1.0 - va) * variance).max(min_var);
        }

        // Return result for the last data point
        if i == data.len() - 1 {
            let dev = if std > 0.0 { (val - mean).abs() / std } else { 0.0 };
            return EwmaResult { mean, anomalous: is_anomaly, deviation: dev };
        }
    }

    EwmaResult { mean: 0.0, anomalous: false, deviation: 0.0 }
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

/// Run the anomaly engine loop forever. Call this directly from the standalone binary.
pub async fn run_anomaly_engine(
    config_db: Arc<ConfigDb>,
    ch: Client,
    smtp_config: SmtpConfig,
    prom_base_url: String,
) {
    let http_client = reqwest::Client::new();
    let smtp_transport = build_smtp_transport(&smtp_config);
    if smtp_transport.is_some() {
        tracing::info!("anomaly engine: SMTP configured for email notifications");
    }
    tracing::info!("anomaly engine: started (30s tick, prom_base_url={prom_base_url})");

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    loop {
        interval.tick().await;
        if let Err(e) = eval_anomaly_rules(&config_db, &ch, &http_client, &smtp_config, &smtp_transport, &prom_base_url).await {
            tracing::error!("anomaly engine error: {e}");
        }
    }
}

async fn eval_anomaly_rules(
    config_db: &ConfigDb,
    ch: &Client,
    http_client: &reqwest::Client,
    smtp_config: &SmtpConfig,
    smtp_transport: &Option<AsyncSmtpTransport<Tokio1Executor>>,
    prom_base_url: &str,
) -> anyhow::Result<()> {
    let now = chrono::Utc::now();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let due_rules = config_db.get_due_anomaly_rules(&now_str)?;

    if due_rules.is_empty() {
        tracing::debug!("anomaly engine: tick — no rules due");
    } else {
        tracing::info!("anomaly engine: tick — evaluating {} rule(s)", due_rules.len());
    }

    for rule in due_rules {
        tracing::info!(
            "anomaly engine: evaluating '{}' (source={}, state={})",
            rule.name, rule.source, rule.state
        );

        let all_series = match rule.source.as_str() {
            "apm" => fetch_apm_data(ch, &rule, &now).await,
            "prometheus" => fetch_prom_data(http_client, prom_base_url, &rule, &now).await,
            _ => {
                tracing::warn!("anomaly rule {}: unknown source '{}'", rule.id, rule.source);
                continue;
            }
        };

        let series_list = match all_series {
            Ok(s) if !s.is_empty() => s,
            Ok(_) => {
                tracing::info!("anomaly engine: '{}' — no data points returned", rule.name);
                config_db.update_anomaly_state(&rule.id, "no_data", &now_str, None)?;
                continue;
            }
            Err(e) => {
                tracing::warn!("anomaly rule {}: data fetch failed: {e}", rule.id);
                config_db.update_anomaly_state(&rule.id, "no_data", &now_str, None)?;
                continue;
            }
        };

        tracing::info!(
            "anomaly engine: '{}' — {} series to evaluate",
            rule.name, series_list.len()
        );

        // Evaluate each series independently; if ANY is anomalous the rule triggers
        let mut any_anomalous = false;
        let mut worst_deviation = 0.0_f64;
        let mut worst_metric = String::new();
        let mut worst_val = 0.0_f64;
        let mut worst_expected = 0.0_f64;

        for (values, metric_label) in &series_list {
            if values.is_empty() { continue; }
            let latest_val = *values.last().unwrap_or(&0.0);
            let result = ewma_eval(values, rule.alpha, rule.sensitivity);

            let series_state = if result.anomalous { "anomalous" } else { "normal" };

            tracing::info!(
                "anomaly engine: '{}' [{}] — {} pts, latest={:.2}, ewma={:.2}, dev={:.1}σ, anomalous={}",
                rule.name, metric_label, values.len(), latest_val, result.mean, result.deviation, result.anomalous
            );

            // Write a log per series evaluation to ClickHouse
            if let Err(e) = insert_anomaly_log(ch, &now, &rule, series_state, metric_label, latest_val, result.mean, result.deviation).await {
                tracing::warn!("anomaly '{}': failed to write eval log: {e}", rule.name);
            }

            if result.anomalous {
                any_anomalous = true;
            }
            if result.deviation > worst_deviation {
                worst_deviation = result.deviation;
                worst_metric = metric_label.clone();
                worst_val = latest_val;
                worst_expected = result.mean;
            }
        }

        let new_state = if any_anomalous { "anomalous" } else { "normal" };
        let old_state = rule.state.as_str();

        if new_state != old_state {
            let event_id = uuid::Uuid::new_v4().to_string();
            let message = format!(
                "Anomaly '{}': {} (metric={}, value={:.2}, expected={:.2}, deviation={:.1}σ)",
                rule.name,
                if any_anomalous { "ANOMALOUS" } else { "RESOLVED" },
                worst_metric,
                worst_val,
                worst_expected,
                worst_deviation,
            );

            config_db.create_anomaly_event(
                &event_id,
                &rule.id,
                new_state,
                &worst_metric,
                worst_val,
                worst_expected,
                worst_deviation,
                &message,
            )?;

            let triggered_at = if any_anomalous { Some(now_str.as_str()) } else { None };
            config_db.update_anomaly_state(&rule.id, new_state, &now_str, triggered_at)?;

            // Send notifications
            send_notifications(config_db, http_client, smtp_config, smtp_transport, &rule, &message, any_anomalous).await;

            tracing::info!("anomaly '{}' state: {} -> {}", rule.name, old_state, new_state);
        } else {
            config_db.update_anomaly_state(&rule.id, new_state, &now_str, None)?;
        }
    }

    Ok(())
}

async fn fetch_apm_data(
    ch: &Client,
    rule: &AnomalyRule,
    now: &chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Vec<(Vec<f64>, String)>> {
    let from = (*now - chrono::Duration::seconds(rule.window_secs))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let sql = format!(
        "SELECT toUnixTimestamp(toStartOfInterval(timestamp, INTERVAL '1' MINUTE)) as bucket, \
         count() as count, \
         countIf(status = 'error') as error_count, \
         quantile(0.50)(duration_ms) as p50, \
         quantile(0.95)(duration_ms) as p95, \
         quantile(0.99)(duration_ms) as p99 \
         FROM wide_events \
         WHERE service_name = '{}' AND timestamp >= '{}' AND timestamp <= '{}' \
         GROUP BY bucket ORDER BY bucket",
        rule.service_name.replace('\'', "''"),
        from,
        now_str,
    );

    let rows = ch.query(&sql).fetch_all::<ApmBucket>().await?;
    if rows.is_empty() {
        return Ok(vec![]);
    }

    let metric_label = format!("{}:{}", rule.service_name, rule.apm_metric);
    let values: Vec<f64> = rows.iter().map(|r| {
        match rule.apm_metric.as_str() {
            "error_rate" => r.error_count as f64,
            "p50" => r.p50,
            "p95" => r.p95,
            "p99" => r.p99,
            _ => r.count as f64, // request_rate
        }
    }).collect();

    Ok(vec![(values, metric_label)])
}

async fn fetch_prom_data(
    http_client: &reqwest::Client,
    prom_base_url: &str,
    rule: &AnomalyRule,
    now: &chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Vec<(Vec<f64>, String)>> {
    let end = now.timestamp();
    let start = end - rule.window_secs;
    let step = if rule.window_secs <= 3600 { 15 } else if rule.window_secs <= 21600 { 60 } else { 300 };

    let split_labels: Vec<String> = serde_json::from_str(&rule.split_labels).unwrap_or_default();

    let query = if !rule.query.is_empty() {
        rule.query.clone()
    } else {
        // Auto-build from pattern
        let suffix_is_counter = ["_total", "_count", "_sum", "_bucket", "_created"]
            .iter()
            .any(|s| rule.pattern.ends_with(s));
        let by_clause = if split_labels.is_empty() {
            String::new()
        } else {
            format!(" by ({})", split_labels.join(", "))
        };
        if suffix_is_counter {
            format!("sum{by_clause}(rate({}[5m]))", rule.pattern)
        } else {
            format!("sum{by_clause}({})", rule.pattern)
        }
    };

    let url = format!(
        "{}/prom/api/v1/query_range?query={}&start={}&end={}&step={}",
        prom_base_url,
        urlencoding::encode(&query),
        start,
        end,
        step,
    );

    let resp: PromResponse = http_client.get(&url).send().await?.json().await?;

    let all_series = resp.data
        .map(|d| d.result)
        .unwrap_or_default();

    if all_series.is_empty() {
        return Ok(vec![]);
    }

    let results: Vec<(Vec<f64>, String)> = all_series.into_iter().map(|s| {
        let values: Vec<f64> = s.values.iter()
            .map(|(_, v)| v.parse::<f64>().unwrap_or(0.0))
            .collect();
        // Build a human-readable label from the metric map
        let label = if s.metric.is_empty() {
            rule.pattern.clone()
        } else {
            let parts: Vec<String> = s.metric.iter()
                .filter(|(k, _)| k.as_str() != "__name__")
                .map(|(k, v)| format!("{}=\"{}\"", k, v))
                .collect();
            if parts.is_empty() {
                rule.pattern.clone()
            } else {
                format!("{}{{{}}}", rule.pattern, parts.join(", "))
            }
        };
        (values, label)
    }).collect();

    Ok(results)
}

async fn insert_anomaly_log(
    ch: &Client,
    now: &chrono::DateTime<chrono::Utc>,
    rule: &AnomalyRule,
    state: &str,
    metric: &str,
    value: f64,
    expected: f64,
    deviation: f64,
) -> anyhow::Result<()> {
    let ts_nanos = now.timestamp_nanos_opt().unwrap_or(now.timestamp() * 1_000_000_000);
    let severity_text = if state == "anomalous" { "WARN" } else { "INFO" };
    let severity_number: u8 = if state == "anomalous" { 13 } else { 9 }; // WARN=13, INFO=9

    let body = format!(
        "[wide-anomaly] rule={} state={} metric={} value={:.2} expected={:.2} deviation={:.1}σ",
        rule.name, state, metric, value, expected, deviation,
    );

    let sql = format!(
        "INSERT INTO otel_logs (Timestamp, SeverityText, SeverityNumber, ServiceName, Body, LogAttributes) VALUES \
         ({ts_nanos}, '{severity_text}', {severity_number}, 'wide-anomaly-engine', '{body}', \
         {{'anomaly.rule_id': '{rule_id}', 'anomaly.rule_name': '{rule_name}', 'anomaly.state': '{state}', \
         'anomaly.metric': '{metric}', 'anomaly.value': '{value:.2}', 'anomaly.expected': '{expected:.2}', \
         'anomaly.deviation': '{deviation:.1}'}})",
        ts_nanos = ts_nanos,
        severity_text = severity_text,
        severity_number = severity_number,
        body = body.replace('\'', "\\'"),
        rule_id = rule.id,
        rule_name = rule.name.replace('\'', "\\'"),
        state = state,
        metric = metric.replace('\'', "\\'"),
        value = value,
        expected = expected,
        deviation = deviation,
    );

    ch.query(&sql).execute().await?;
    Ok(())
}

async fn send_notifications(
    config_db: &ConfigDb,
    http_client: &reqwest::Client,
    smtp_config: &SmtpConfig,
    smtp_transport: &Option<AsyncSmtpTransport<Tokio1Executor>>,
    rule: &AnomalyRule,
    message: &str,
    is_anomalous: bool,
) {
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
                                "[Wide Anomaly] {} - {}",
                                rule.name,
                                if is_anomalous { "ANOMALOUS" } else { "RESOLVED" }
                            );
                            match Message::builder()
                                .from(smtp_config.from.parse().unwrap_or_else(|_| "wide@localhost".parse().unwrap()))
                                .to(to_addr.parse().unwrap_or_else(|_| "noreply@localhost".parse().unwrap()))
                                .subject(subject)
                                .header(ContentType::TEXT_PLAIN)
                                .body(message.to_string())
                            {
                                Ok(email) => {
                                    if let Err(e) = transport.send(email).await {
                                        tracing::warn!("anomaly {}: email to {} failed: {e}", rule.id, to_addr);
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("anomaly {}: failed to build email: {e}", rule.id);
                                }
                            }
                        }
                    }
                }
                "slack" => {
                    if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                        let payload = serde_json::json!({ "text": message });
                        if let Err(e) = http_client.post(url).json(&payload).send().await {
                            tracing::warn!("anomaly {}: slack notification failed: {e}", rule.id);
                        }
                    }
                }
                _ => {
                    // webhook
                    if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                        let payload = serde_json::json!({
                            "anomaly": rule.name,
                            "state": if is_anomalous { "anomalous" } else { "normal" },
                            "message": message,
                        });
                        if let Err(e) = http_client.post(url).json(&payload).send().await {
                            tracing::warn!("anomaly {}: webhook notification failed: {e}", rule.id);
                        }
                    }
                }
            }
        }
    }
}
