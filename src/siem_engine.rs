use std::sync::Arc;
use crate::clickhouse_config::ConfigDb;
use crate::models::detection::DetectionRule;
use clickhouse::Client;

#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    #[serde(rename = "_siem_count")]
    count: u64,
}

/// Max rules evaluated concurrently per cycle (bounds parallel CH data queries).
const ENGINE_CONCURRENCY: usize = 6;
/// Flush `last_eval_at` to the config table once per this many evals per rule.
const EVAL_FLUSH_EVERY: u32 = 10;

/// Spawn the SIEM detection engine as a background task.
/// Runs every 60 seconds, evaluating all enabled detection rules that are due.
pub fn spawn(ch: Client, config_db: Arc<ConfigDb>) {
    tokio::spawn(async move {
        let http_client = reqwest::Client::new();
        tracing::info!(engine = "siem", interval_secs = 60, "detection engine started");

        let mut eval_state = crate::eval_state::EvalState::new(EVAL_FLUSH_EVERY);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Err(e) = run_detection_cycle(&ch, &config_db, &http_client, &mut eval_state).await {
                tracing::error!(error = %e, engine = "siem", "detection cycle failed");
            }
        }
    });
}

async fn run_detection_cycle(
    ch: &Client,
    config_db: &ConfigDb,
    http_client: &reqwest::Client,
    eval_state: &mut crate::eval_state::EvalState,
) -> anyhow::Result<()> {
    use futures_util::StreamExt;

    let now = chrono::Utc::now();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let rules = config_db.list_enabled_detection_rules().await?;
    if rules.is_empty() {
        tracing::debug!(engine = "siem", "tick -- no enabled detection rules");
        return Ok(());
    }

    // Due = DB-side last_eval_at check (coarse: flushed 1-in-N) AND in-memory
    // check ⇒ max(db, mem) + interval <= now semantics. The in-memory state
    // also paces retries after evaluation errors, replacing the old
    // write-last_eval_at-on-error round-trip.
    let jobs: Vec<(DetectionRule, bool)> = rules
        .into_iter()
        .filter(|r| is_rule_due(r, &now) && eval_state.is_due(&r.id, now, r.interval_secs))
        .map(|r| {
            let flush = eval_state.should_flush(&r.id);
            (r, flush)
        })
        .collect();

    let evaluated = jobs.len() as u32;
    let now_str_ref = now_str.as_str();

    let outcomes: Vec<(String, bool, bool)> = futures_util::stream::iter(jobs.into_iter().map(|(rule, should_flush)| async move {
        match evaluate_rule(ch, config_db, http_client, &rule, &now, now_str_ref, should_flush).await {
            Ok((did_fire, persisted)) => (rule.id, did_fire, persisted),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    engine = "siem",
                    rule_name = %rule.name,
                    rule_id = %rule.id,
                    "rule evaluation failed"
                );
                (rule.id, false, false)
            }
        }
    }))
    .buffer_unordered(ENGINE_CONCURRENCY)
    .collect()
    .await;

    let mut fired = 0u32;
    for (id, did_fire, persisted) in outcomes {
        if did_fire {
            fired += 1;
        }
        eval_state.record(id, now, persisted);
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

/// Evaluate one detection rule. Returns (did_fire, persisted_to_db).
async fn evaluate_rule(
    ch: &Client,
    config_db: &ConfigDb,
    http_client: &reqwest::Client,
    rule: &DetectionRule,
    now: &chrono::DateTime<chrono::Utc>,
    now_str: &str,
    should_flush: bool,
) -> anyhow::Result<(bool, bool)> {
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
        // Fires persist last_triggered_at immediately, from the rule row we
        // already hold (no SELECT…FINAL re-read).
        fire_detection(config_db, http_client, rule, match_count, "[]", now_str).await;
        config_db.persist_detection_rule_eval(rule, now_str, Some(now_str)).await?;
        return Ok((true, true));
    }

    // No fire: only flush last_eval_at on the coarse cadence.
    if should_flush {
        config_db.persist_detection_rule_eval(rule, now_str, None).await?;
        return Ok((false, true));
    }

    Ok((false, false))
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
    let tenant_condition = format!("tenant_id = '{escaped_tenant}'");
    let lower = sql.to_lowercase();

    // Walk the SQL character-by-character, tracking whether we are inside a string
    // literal so that occurrences of "WHERE" inside quoted strings are ignored.
    // This avoids injecting a spurious tenant filter into query text like:
    //   WHERE Body LIKE 'show me where errors occurred'
    let chars: Vec<(usize, char)> = sql.char_indices().collect();
    let n = chars.len();
    let mut result = String::with_capacity(sql.len() + 100);
    let mut ci = 0usize;
    let mut in_string = false;
    let mut injections = 0usize;

    while ci < n {
        let (byte_pos, ch) = chars[ci];

        if in_string {
            result.push(ch);
            ci += 1;
            if ch == '\'' {
                // '' is an escaped single-quote inside a string literal — stay in string
                if ci < n && chars[ci].1 == '\'' {
                    result.push('\'');
                    ci += 1;
                } else {
                    in_string = false;
                }
            }
        } else if ch == '\'' {
            in_string = true;
            result.push(ch);
            ci += 1;
        } else {
            // "where " is 6 ASCII bytes; check lower-cased view outside string literals
            let rest = &lower[byte_pos..];
            if (rest.starts_with("where ") || rest.starts_with("where\n") || rest.starts_with("where\t"))
                && ci + 6 <= n
            {
                // Push "WHERE " from original SQL preserving the caller's case
                result.push_str(&sql[byte_pos..byte_pos + 6]);
                result.push_str(&tenant_condition);
                result.push_str(" AND ");
                ci += 6;
                injections += 1;
            } else {
                result.push(ch);
                ci += 1;
            }
        }
    }

    // Safety net: if the query had no WHERE clause, append one so tenant isolation
    // is always enforced even for bare "SELECT * FROM logs" queries.
    if injections == 0 {
        result.push_str(&format!(" WHERE {tenant_condition}"));
    }

    result
}

/// Validate that a notification URL is safe to send HTTP requests to.
/// Requires HTTPS scheme to prevent SSRF via plaintext channels.
fn is_safe_notification_url(url: &str) -> bool {
    url.starts_with("https://")
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
    ).await {
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
        if let Ok(Some(channel)) = config_db.get_channel_by_id(channel_id).await {
            let config: serde_json::Value =
                serde_json::from_str(&channel.config).unwrap_or(serde_json::json!({}));

            match channel.channel_type.as_str() {
                "slack" => {
                    if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                        if !is_safe_notification_url(url) {
                            tracing::warn!(
                                engine = "siem",
                                rule_name = %rule.name,
                                channel = "slack",
                                url = %url,
                                "notification URL rejected: must use HTTPS"
                            );
                        } else {
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
                }
                _ => {
                    // webhook (default)
                    if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                        if !is_safe_notification_url(url) {
                            tracing::warn!(
                                engine = "siem",
                                rule_name = %rule.name,
                                channel = "webhook",
                                url = %url,
                                "notification URL rejected: must use HTTPS"
                            );
                        } else {
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
        let sql = "SELECT count() FROM logs WHERE Timestamp > '2024-01-01'";
        let result = inject_tenant_filter(sql, "security");
        assert!(result.contains("tenant_id = 'security' AND"));
        assert!(result.contains("Timestamp > '2024-01-01'"));
    }

    #[test]
    fn test_inject_tenant_filter_multiple_where() {
        let sql = "WITH sub AS (SELECT * FROM logs WHERE Timestamp > '2024-01-01') \
                   SELECT * FROM spans_raw WHERE Timestamp > '2024-01-01'";
        let result = inject_tenant_filter(sql, "eng");
        // Both WHERE clauses should have tenant_id injected
        let count = result.matches("tenant_id = 'eng'").count();
        assert_eq!(count, 2, "Expected 2 tenant injections, got: {result}");
    }

    #[test]
    fn test_inject_tenant_filter_escapes_quotes() {
        let sql = "SELECT * FROM logs WHERE Body LIKE '%test%'";
        let result = inject_tenant_filter(sql, "tenant'evil");
        assert!(result.contains("tenant_id = 'tenant''evil'"));
    }

    #[test]
    fn test_build_scoped_query_replaces_placeholders() {
        let sql = "SELECT count() FROM logs WHERE Timestamp BETWEEN @window_start AND @window_end";
        let result = build_scoped_query(sql, "default", "2024-01-01 00:00:00", "2024-01-01 00:05:00");
        assert!(result.contains("'2024-01-01 00:00:00'"));
        assert!(result.contains("'2024-01-01 00:05:00'"));
        assert!(result.contains("tenant_id = 'default'"));
        assert!(!result.contains("@window_start"));
        assert!(!result.contains("@window_end"));
    }

    #[test]
    fn test_inject_tenant_filter_ignores_where_in_string_literal() {
        // "where" inside a quoted string should NOT get a tenant filter injected
        let sql = "SELECT count() FROM logs WHERE Body LIKE 'show where errors occurred'";
        let result = inject_tenant_filter(sql, "sec");
        // Should inject exactly once (the real WHERE), not twice
        assert_eq!(result.matches("tenant_id = 'sec'").count(), 1, "got: {result}");
        assert!(result.contains("show where errors occurred"), "string literal mangled: {result}");
    }

    #[test]
    fn test_inject_tenant_filter_no_where_clause() {
        // Queries without WHERE must still get a tenant filter appended
        let sql = "SELECT count() FROM logs";
        let result = inject_tenant_filter(sql, "acme");
        assert!(result.contains("WHERE tenant_id = 'acme'"), "missing tenant filter: {result}");
    }
}
