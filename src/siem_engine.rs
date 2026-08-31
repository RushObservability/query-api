use crate::clickhouse_config::ConfigDb;
use crate::models::detection::DetectionRule;
use clickhouse::Client;
use std::sync::Arc;

/// Max rules evaluated concurrently per cycle (bounds parallel CH data queries).
const ENGINE_CONCURRENCY: usize = 6;
/// Flush `last_eval_at` to the config table once per this many evals per rule.
const EVAL_FLUSH_EVERY: u32 = 10;

/// Spawn the SIEM detection engine as a background task.
/// Runs every 60 seconds, evaluating all enabled detection rules that are due.
pub fn spawn(
    ch: Client,
    config_db: Arc<ConfigDb>,
    self_metrics: Arc<crate::self_metrics::SelfMetrics>,
) {
    tokio::spawn(async move {
        let http_client = reqwest::Client::new();
        tracing::info!(
            engine = "siem",
            interval_secs = 60,
            "detection engine started"
        );

        let mut eval_state = crate::eval_state::EvalState::new(EVAL_FLUSH_EVERY);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let start = std::time::Instant::now();
            let ok = match run_detection_cycle(&ch, &config_db, &http_client, &mut eval_state).await
            {
                Ok(()) => true,
                Err(e) => {
                    tracing::error!(error = %e, engine = "siem", "detection cycle failed");
                    false
                }
            };
            self_metrics.record_engine("siem_engine", start.elapsed().as_millis() as u64, ok);
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

    let outcomes: Vec<(String, bool, bool)> =
        futures_util::stream::iter(jobs.into_iter().map(|(rule, should_flush)| async move {
            let evaluated = crate::query_governor::run_background(
                &rule.tenant_id,
                evaluate_rule(
                    ch,
                    config_db,
                    http_client,
                    &rule,
                    &now,
                    now_str_ref,
                    should_flush,
                ),
            )
            .await;
            match evaluated {
                Ok(Ok((did_fire, persisted))) => (rule.id, did_fire, persisted),
                Ok(Err(e)) => {
                    tracing::warn!(
                        error = %e,
                        engine = "siem",
                        rule_name = %rule.name,
                        rule_id = %rule.id,
                        "rule evaluation failed"
                    );
                    (rule.id, false, false)
                }
                Err(e) => {
                    tracing::warn!(
                        error = ?e,
                        engine = "siem",
                        rule_name = %rule.name,
                        rule_id = %rule.id,
                        "rule evaluation admission rejected"
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
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(last_eval, "%Y-%m-%dT%H:%M:%S%.fZ")
                });
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

    let compiled = crate::detection_query::compile_count_query(
        &rule.query_sql,
        &rule.tenant_id,
        &window_start_str,
        &window_end_str,
    )?;

    tracing::debug!(
        engine = "siem",
        rule_name = %rule.name,
        tenant_id = %rule.tenant_id,
        window_secs = rule.window_secs,
        "evaluating rule"
    );

    let match_count =
        crate::detection_query::execute_count(ch, &compiled, &rule.tenant_id).await? as i64;
    let did_fire = match_count >= rule.threshold;

    if did_fire {
        // Fires persist last_triggered_at immediately, from the rule row we
        // already hold (no SELECT…FINAL re-read).
        fire_detection(config_db, http_client, rule, match_count, "[]", now_str).await;
        config_db
            .persist_detection_rule_eval(rule, now_str, Some(now_str))
            .await?;
        return Ok((true, true));
    }

    // No fire: only flush last_eval_at on the coarse cadence.
    if should_flush {
        config_db
            .persist_detection_rule_eval(rule, now_str, None)
            .await?;
        return Ok((false, true));
    }

    Ok((false, false))
}

/// Fire a detection: create an event and send notifications.
async fn fire_detection(
    config_db: &ConfigDb,
    _http_client: &reqwest::Client,
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

    if let Err(e) = config_db
        .create_detection_event(
            &event_id,
            &rule.id,
            &rule.tenant_id,
            &rule.severity,
            match_count,
            sample_data,
        )
        .await
    {
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
                    if let Some(url) = config
                        .get("url")
                        .or_else(|| config.get("webhook_url"))
                        .and_then(|u| u.as_str())
                    {
                        let payload = serde_json::json!({ "text": message });
                        if let Err(e) = crate::outbound::post_json(url, &payload).await {
                            tracing::warn!(error = %e, engine = "siem", rule_name = %rule.name, channel = "slack", "notification failed");
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
                        if let Err(e) = crate::outbound::post_json(url, &payload).await {
                            tracing::warn!(error = %e, engine = "siem", rule_name = %rule.name, channel = "webhook", "notification failed");
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
) -> Result<(u64, String), crate::detection_query::DetectionQueryError> {
    let now = chrono::Utc::now();
    let window_start = (now - chrono::Duration::seconds(window_secs))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    let window_end = now.format("%Y-%m-%d %H:%M:%S").to_string();

    let compiled = crate::detection_query::compile_count_query(
        query_sql,
        tenant_id,
        &window_start,
        &window_end,
    )?;
    let row_count = crate::detection_query::execute_count(ch, &compiled, tenant_id).await?;
    Ok((row_count, compiled.count_sql))
}
