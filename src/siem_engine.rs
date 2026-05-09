use std::sync::Arc;
use crate::config_db::ConfigDb;
use crate::models::detection::DetectionRule;
use clickhouse::Client;

#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    #[serde(rename = "_siem_count")]
    count: u64,
}

/// Spawn the SIEM detection engine as a background task.
/// Runs every 60 seconds, evaluating all enabled detection rules that are due.
pub fn spawn(ch: Client, config_db: Arc<ConfigDb>) {
    tokio::spawn(async move {
        let http_client = reqwest::Client::new();
        tracing::info!(engine = "siem", interval_secs = 60, "detection engine started");

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Err(e) = run_detection_cycle(&ch, &config_db, &http_client).await {
                tracing::error!(error = %e, engine = "siem", "detection cycle failed");
            }
        }
    });
}

async fn run_detection_cycle(
    ch: &Client,
    config_db: &ConfigDb,
    http_client: &reqwest::Client,
) -> anyhow::Result<()> {
    let now = chrono::Utc::now();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let rules = config_db.list_enabled_detection_rules()?;
    if rules.is_empty() {
        tracing::debug!(engine = "siem", "tick -- no enabled detection rules");
        return Ok(());
    }

    let mut evaluated = 0u32;
    let mut fired = 0u32;

    for rule in &rules {
        // Check if the rule is due based on its interval and last_eval_at
        if !is_rule_due(rule, &now) {
            continue;
        }

        evaluated += 1;
        match evaluate_rule(ch, config_db, http_client, rule, &now, &now_str).await {
            Ok(did_fire) => {
                if did_fire {
                    fired += 1;
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    engine = "siem",
                    rule_name = %rule.name,
                    rule_id = %rule.id,
                    "rule evaluation failed"
                );
                // Still update last_eval_at so we don't retry every tick
                let _ = config_db.update_detection_rule_eval(&rule.id, &now_str, None);
            }
        }
    }

    if evaluated > 0 {
        tracing::info!(
            engine = "siem",
            rules_evaluated = evaluated,
            rules_triggered = fired,
            "detection cycle completed"
        );
    } else {
        tracing::debug!(engine = "siem", "tick -- no rules due");
    }

    Ok(())
}

fn is_rule_due(rule: &DetectionRule, now: &chrono::DateTime<chrono::Utc>) -> bool {
    match &rule.last_eval_at {
        None => true, // never evaluated
        Some(last_eval) => {
            let parsed = chrono::NaiveDateTime::parse_from_str(last_eval, "%Y-%m-%dT%H:%M:%SZ")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(last_eval, "%Y-%m-%dT%H:%M:%S%.fZ"));
            match parsed {
                Ok(dt) => {
                    let last_eval_utc = dt.and_utc();
                    let elapsed = (*now - last_eval_utc).num_seconds();
                    elapsed >= rule.interval_secs
                }
                Err(_) => true, // can't parse, run it
            }
        }
    }
}

async fn evaluate_rule(
    ch: &Client,
    config_db: &ConfigDb,
    http_client: &reqwest::Client,
    rule: &DetectionRule,
    now: &chrono::DateTime<chrono::Utc>,
    now_str: &str,
) -> anyhow::Result<bool> {
    let window_end = *now;
    let window_start = window_end - chrono::Duration::seconds(rule.window_secs);

    let window_start_str = window_start.format("%Y-%m-%d %H:%M:%S").to_string();
    let window_end_str = window_end.format("%Y-%m-%d %H:%M:%S").to_string();

    // Build the scoped query: replace placeholders and inject tenant_id
    let scoped_sql = build_scoped_query(
        &rule.query_sql,
        &rule.tenant_id,
        &window_start_str,
        &window_end_str,
    );

    tracing::debug!(
        engine = "siem",
        rule_name = %rule.name,
        tenant_id = %rule.tenant_id,
        window_secs = rule.window_secs,
        "evaluating rule"
    );

    // Count the number of rows returned by the detection query.
    // This avoids needing to know the schema of the result set.
    let count_sql = format!(
        "SELECT count() AS _siem_count FROM ({scoped_sql}) AS _siem_sub \
         SETTINGS max_execution_time = 10"
    );

    let row = crate::tenant_query(ch, &count_sql, &rule.tenant_id).fetch_one::<CountRow>().await?;
    let match_count = row.count as i64;
    let did_fire = match_count >= rule.threshold;

    if did_fire {
        fire_detection(config_db, http_client, rule, match_count, "[]", now_str).await;
        config_db.update_detection_rule_eval(&rule.id, now_str, Some(now_str))?;
    } else {
        config_db.update_detection_rule_eval(&rule.id, now_str, None)?;
    }

    Ok(did_fire)
}

/// Build the final SQL with tenant_id injection and window placeholder replacement.
fn build_scoped_query(
    query_sql: &str,
    tenant_id: &str,
    window_start: &str,
    window_end: &str,
) -> String {
    // Replace @window_start and @window_end placeholders
    let sql = query_sql
        .replace("@window_start", &format!("'{window_start}'"))
        .replace("@window_end", &format!("'{window_end}'"));

    // Inject tenant_id into every WHERE clause.
    inject_tenant_filter(&sql, tenant_id)
}

fn inject_tenant_filter(sql: &str, tenant_id: &str) -> String {
    let escaped_tenant = tenant_id.replace('\'', "''");

    // Find all WHERE positions (case-insensitive) and inject tenant_id after each one.
    let mut result = String::with_capacity(sql.len() + 100);
    let lower = sql.to_lowercase();
    let mut last_pos = 0;

    for (idx, _) in lower.match_indices("where ") {
        // Copy everything up to and including "WHERE "
        result.push_str(&sql[last_pos..idx + 6]);
        // Inject tenant_id filter
        result.push_str(&format!("tenant_id = '{escaped_tenant}' AND "));
        last_pos = idx + 6;
    }
    // Copy the remainder
    result.push_str(&sql[last_pos..]);

    result
}

/// Fire a detection: create an event and send notifications.
async fn fire_detection(
    config_db: &ConfigDb,
    http_client: &reqwest::Client,
    rule: &DetectionRule,
    match_count: i64,
    sample_data: &str,
    now_str: &str,
) {
    let event_id = uuid::Uuid::new_v4().to_string();

    tracing::info!(
        engine = "siem",
        event = "rule_fired",
        rule_name = %rule.name,
        tenant_id = %rule.tenant_id,
        severity = %rule.severity,
        match_count = match_count,
        "detection rule fired"
    );

    if let Err(e) = config_db.create_detection_event(
        &event_id,
        &rule.id,
        &rule.tenant_id,
        &rule.severity,
        match_count,
        sample_data,
    ) {
        tracing::error!(error = %e, engine = "siem", rule_name = %rule.name, "failed to create detection event");
    }

    // Send notifications through configured channels
    let channel_ids: Vec<String> = serde_json::from_str(&rule.channels).unwrap_or_default();
    if channel_ids.is_empty() {
        return;
    }

    let message = format!(
        "[SIEM Detection] Rule '{}' fired (severity={}, match_count={}, tenant={})",
        rule.name, rule.severity, match_count, rule.tenant_id,
    );

    for channel_id in &channel_ids {
        if let Ok(Some(channel)) = config_db.get_channel_by_id(channel_id) {
            let config: serde_json::Value =
                serde_json::from_str(&channel.config).unwrap_or(serde_json::json!({}));

            match channel.channel_type.as_str() {
                "slack" => {
                    if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                        let payload = serde_json::json!({ "text": message });
                        if let Err(e) = http_client.post(url).json(&payload).send().await {
                            tracing::warn!(
                                error = %e,
                                engine = "siem",
                                rule_name = %rule.name,
                                channel = "slack",
                                "notification failed"
                            );
                        }
                    }
                }
                _ => {
                    // webhook (default)
                    if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                        let payload = serde_json::json!({
                            "detection_rule": rule.name,
                            "severity": rule.severity,
                            "tenant_id": rule.tenant_id,
                            "match_count": match_count,
                            "message": message,
                            "event_id": event_id,
                            "fired_at": now_str,
                        });
                        if let Err(e) = http_client.post(url).json(&payload).send().await {
                            tracing::warn!(
                                error = %e,
                                engine = "siem",
                                rule_name = %rule.name,
                                channel = "webhook",
                                "notification failed"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Execute a detection rule query for dry-run / test purposes.
/// Returns (row_count, query_executed).
pub async fn test_detection_query(
    ch: &Client,
    query_sql: &str,
    tenant_id: &str,
    window_secs: i64,
) -> anyhow::Result<(u64, String)> {
    let now = chrono::Utc::now();
    let window_start = (now - chrono::Duration::seconds(window_secs))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    let window_end = now.format("%Y-%m-%d %H:%M:%S").to_string();

    let scoped_sql = build_scoped_query(query_sql, tenant_id, &window_start, &window_end);
    let count_sql = format!(
        "SELECT count() AS _siem_count FROM ({scoped_sql}) AS _siem_sub \
         SETTINGS max_execution_time = 10"
    );

    let row = crate::tenant_query(ch, &count_sql, tenant_id).fetch_one::<CountRow>().await?;
    Ok((row.count, count_sql))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inject_tenant_filter_simple() {
        let sql = "SELECT count() FROM otel_logs WHERE Timestamp > '2024-01-01'";
        let result = inject_tenant_filter(sql, "security");
        assert!(result.contains("tenant_id = 'security' AND"));
        assert!(result.contains("Timestamp > '2024-01-01'"));
    }

    #[test]
    fn test_inject_tenant_filter_multiple_where() {
        let sql = "WITH sub AS (SELECT * FROM otel_logs WHERE Timestamp > '2024-01-01') \
                   SELECT * FROM otel_traces WHERE Timestamp > '2024-01-01'";
        let result = inject_tenant_filter(sql, "eng");
        // Both WHERE clauses should have tenant_id injected
        let count = result.matches("tenant_id = 'eng'").count();
        assert_eq!(count, 2, "Expected 2 tenant injections, got: {result}");
    }

    #[test]
    fn test_inject_tenant_filter_escapes_quotes() {
        let sql = "SELECT * FROM otel_logs WHERE Body LIKE '%test%'";
        let result = inject_tenant_filter(sql, "tenant'evil");
        assert!(result.contains("tenant_id = 'tenant''evil'"));
    }

    #[test]
    fn test_build_scoped_query_replaces_placeholders() {
        let sql = "SELECT count() FROM otel_logs WHERE Timestamp BETWEEN @window_start AND @window_end";
        let result = build_scoped_query(sql, "default", "2024-01-01 00:00:00", "2024-01-01 00:05:00");
        assert!(result.contains("'2024-01-01 00:00:00'"));
        assert!(result.contains("'2024-01-01 00:05:00'"));
        assert!(result.contains("tenant_id = 'default'"));
        assert!(!result.contains("@window_start"));
        assert!(!result.contains("@window_end"));
    }
}
