use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use clickhouse::Client;

use crate::alert_engine;
use crate::clickhouse_config::ConfigDb;
use crate::models::monitor::{ApmQueryConfig, LogQueryConfig, MetricQueryConfig, Monitor};
use crate::promql;

/// A single value row returned by ClickHouse aggregation queries.
#[derive(clickhouse::Row, serde::Deserialize)]
struct ValueRow {
    value: f64,
}

/// A grouped value row (group_key + aggregated value).
#[derive(clickhouse::Row, serde::Deserialize)]
struct GroupedRow {
    group_key: String,
    value: f64,
}

/// Max monitors evaluated concurrently per cycle (bounds parallel CH data queries).
const ENGINE_CONCURRENCY: usize = 6;
/// Flush `last_eval_at` to the config table once per this many evals per monitor.
const EVAL_FLUSH_EVERY: u32 = 10;

/// Spawn the monitor evaluation engine. Runs every 60 seconds.
pub fn spawn(
    ch: Client,
    config_db: Arc<ConfigDb>,
    smtp_config: alert_engine::SmtpConfig,
) {
    tokio::spawn(async move {
        let http_client = reqwest::Client::new();
        let smtp_transport = build_smtp_transport(&smtp_config);
        let mut eval_state = crate::eval_state::EvalState::new(EVAL_FLUSH_EVERY);

        loop {
            let start = Instant::now();
            let (evaluated, state_changes) =
                match run_evaluation_cycle(&ch, &config_db, &http_client, &smtp_config, &smtp_transport, &mut eval_state)
                    .await
                {
                    Ok(stats) => stats,
                    Err(e) => {
                        tracing::error!(engine = "monitors", error = %e, "evaluation cycle failed");
                        (0, 0)
                    }
                };
            let elapsed_ms = start.elapsed().as_millis() as u64;

            if evaluated > 0 {
                tracing::info!(
                    engine = "monitors",
                    monitors_evaluated = evaluated,
                    state_changes = state_changes,
                    cycle_ms = elapsed_ms,
                    "monitor evaluation cycle completed"
                );
            }

            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    });
}

fn build_smtp_transport(
    cfg: &alert_engine::SmtpConfig,
) -> Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>> {
    use lettre::transport::smtp::authentication::Credentials;
    let host = cfg.host.as_deref()?;
    let mut builder = lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::relay(host).ok()?;
    builder = builder.port(cfg.port);
    if let (Some(user), Some(pass)) = (&cfg.user, &cfg.pass) {
        builder = builder.credentials(Credentials::new(user.clone(), pass.clone()));
    }
    Some(builder.build())
}

/// Run one evaluation cycle across all enabled monitors. Returns (evaluated, state_changes).
async fn run_evaluation_cycle(
    ch: &Client,
    config_db: &ConfigDb,
    http_client: &reqwest::Client,
    smtp_config: &alert_engine::SmtpConfig,
    smtp_transport: &Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>,
    eval_state: &mut crate::eval_state::EvalState,
) -> anyhow::Result<(u64, u64)> {
    use futures_util::StreamExt;

    let now = chrono::Utc::now();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let monitors = config_db.list_enabled_monitors().await?;

    // States of every enabled monitor as fetched this cycle — composite monitors
    // resolve their members from this map instead of one SELECT…FINAL per member.
    // (Members may be evaluated concurrently this same cycle; composites see the
    // cycle-start states, which matches the prior racy read-at-eval behavior to
    // within one cycle.)
    let monitor_states: HashMap<String, String> = monitors
        .iter()
        .map(|m| (m.id.clone(), m.state.clone()))
        .collect();

    // Due = DB-side last_eval_at check (coarse: flushed 1-in-N) AND in-memory
    // check ⇒ max(db, mem) + interval <= now semantics.
    let jobs: Vec<(Monitor, bool)> = monitors
        .into_iter()
        .filter(|monitor| {
            if let Some(ref last_eval) = monitor.last_eval_at {
                if let Ok(last) = chrono::NaiveDateTime::parse_from_str(last_eval, "%Y-%m-%dT%H:%M:%SZ") {
                    let last_utc = last.and_utc();
                    let elapsed = (now - last_utc).num_seconds();
                    if elapsed < monitor.eval_interval_secs {
                        return false;
                    }
                }
            }
            eval_state.is_due(&monitor.id, now, monitor.eval_interval_secs)
        })
        .map(|m| {
            let flush = eval_state.should_flush(&m.id);
            (m, flush)
        })
        .collect();

    let evaluated = jobs.len() as u64;
    let now_str_ref = now_str.as_str();
    let monitor_states_ref = &monitor_states;

    let outcomes: Vec<(String, u64, bool)> = futures_util::stream::iter(jobs.into_iter().map(|(monitor, should_flush)| async move {
        let result = evaluate_monitor(
            ch,
            config_db,
            &monitor,
            now_str_ref,
            http_client,
            smtp_config,
            smtp_transport,
            monitor_states_ref,
            should_flush,
        )
        .await;
        let (changes, persisted) = match result {
            Ok(cp) => cp,
            Err(e) => {
                tracing::warn!(
                    engine = "monitors",
                    monitor_id = %monitor.id,
                    monitor_name = %monitor.name,
                    error = %e,
                    "monitor evaluation failed"
                );
                // On query failure, check no_data handling
                handle_no_data(config_db, &monitor, now_str_ref, http_client, smtp_config, smtp_transport, should_flush).await
            }
        };
        (monitor.id, changes, persisted)
    }))
    .buffer_unordered(ENGINE_CONCURRENCY)
    .collect()
    .await;

    let mut state_changes: u64 = 0;
    for (id, changes, persisted) in outcomes {
        state_changes += changes;
        eval_state.record(id, now, persisted);
    }

    Ok((evaluated, state_changes))
}

/// Evaluate a single monitor. Returns (state_changes, persisted_to_db).
#[allow(clippy::too_many_arguments)]
async fn evaluate_monitor(
    ch: &Client,
    config_db: &ConfigDb,
    monitor: &Monitor,
    now_str: &str,
    http_client: &reqwest::Client,
    smtp_config: &alert_engine::SmtpConfig,
    smtp_transport: &Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>,
    monitor_states: &HashMap<String, String>,
    should_flush: bool,
) -> anyhow::Result<(u64, bool)> {
    let group_by: Vec<String> = serde_json::from_str(&monitor.group_by).unwrap_or_default();
    let has_groups = !group_by.is_empty();

    // Build and execute query
    let results = match monitor.monitor_type.as_str() {
        "metric" => query_metric(ch, monitor, has_groups).await?,
        "log" => query_log(ch, monitor, has_groups).await?,
        "apm" => query_apm(ch, monitor, has_groups).await?,
        "composite" => {
            // Composite monitors combine other monitor states, not queries
            return evaluate_composite(config_db, monitor, now_str, http_client, smtp_config, smtp_transport, monitor_states, should_flush).await;
        }
        other => {
            tracing::warn!(engine = "monitors", monitor_id = %monitor.id, "unknown monitor type: {other}");
            return Ok((0, false));
        }
    };

    if results.is_empty() {
        // No data returned
        return Ok(handle_no_data(config_db, monitor, now_str, http_client, smtp_config, smtp_transport, should_flush).await);
    }

    // Evaluate thresholds for each group result
    let mut group_states: HashMap<String, String> =
        serde_json::from_str(&monitor.group_states).unwrap_or_default();
    let mut changes: u64 = 0;

    for (group_key, value) in &results {
        let current_state = group_states
            .get(group_key)
            .map(|s| s.as_str())
            .unwrap_or(&monitor.state);

        let new_state = evaluate_threshold(
            current_state,
            *value,
            monitor.critical,
            monitor.critical_recovery,
            monitor.warning,
            monitor.warning_recovery,
            &monitor.comparator,
        );

        if new_state != current_state {
            changes += 1;

            let threshold = match new_state {
                "alert" => monitor.critical,
                "warn" => monitor.warning,
                _ => monitor.critical_recovery.or(monitor.critical),
            };

            let event_msg = format!(
                "Monitor '{}'{}: {} -> {} (value={:.4}, threshold={:.4})",
                monitor.name,
                if group_key.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", group_key)
                },
                current_state,
                new_state,
                value,
                threshold.unwrap_or(0.0),
            );

            let event_id = uuid::Uuid::new_v4().to_string();
            let _ = config_db.create_monitor_event(
                &event_id,
                &monitor.id,
                &monitor.tenant_id,
                group_key,
                current_state,
                new_state,
                Some(*value),
                threshold,
                &event_msg,
            ).await;

            // Fire notifications
            fire_notifications(
                config_db,
                monitor,
                &event_msg,
                new_state,
                *value,
                threshold.unwrap_or(0.0),
                http_client,
                smtp_config,
                smtp_transport,
            )
            .await;

            if new_state == "alert" || new_state == "warn" {
                let _ = config_db.update_monitor_triggered(&monitor.id, now_str).await;
            }

            group_states.insert(group_key.clone(), new_state.to_string());
        }
    }

    // Determine overall monitor state (worst across groups)
    let overall = if has_groups {
        worst_state(group_states.values().map(|s| s.as_str()))
    } else {
        results
            .first()
            .map(|(gk, _)| {
                group_states
                    .get(gk)
                    .map(|s| s.as_str())
                    .unwrap_or("ok")
            })
            .unwrap_or("ok")
    };

    // Persist only on a real transition (a group changed or the overall state
    // moved) — that path is identical to before. Otherwise just flush
    // last_eval_at on the coarse cadence from the row we already hold.
    if changes > 0 || overall != monitor.state.as_str() {
        let group_states_json = serde_json::to_string(&group_states).unwrap_or_else(|_| "{}".to_string());
        config_db.update_monitor_state(&monitor.id, overall, &group_states_json, now_str).await?;
        Ok((changes, true))
    } else if should_flush {
        config_db.persist_monitor_eval(monitor, now_str).await?;
        Ok((changes, true))
    } else {
        Ok((changes, false))
    }
}

/// Determine the worst state from an iterator of state strings.
fn worst_state<'a>(states: impl Iterator<Item = &'a str>) -> &'a str {
    let mut worst = "ok";
    for s in states {
        match s {
            "alert" => return "alert",
            "warn" if worst != "alert" => worst = "warn",
            "no_data" if worst == "ok" => worst = "no_data",
            _ => {}
        }
    }
    worst
}

/// Hysteresis-based threshold evaluation. Returns the new state string.
fn evaluate_threshold(
    current_state: &str,
    value: f64,
    critical: Option<f64>,
    critical_recovery: Option<f64>,
    warning: Option<f64>,
    warning_recovery: Option<f64>,
    comparator: &str,
) -> &'static str {
    let exceeds = |val: f64, threshold: f64| -> bool {
        match comparator {
            "below" => val < threshold,
            _ => val >= threshold, // "above" (default)
        }
    };

    let below = |val: f64, threshold: f64| -> bool {
        match comparator {
            "below" => val >= threshold, // recovery means value went back above
            _ => val < threshold,        // recovery means value went back below
        }
    };

    match current_state {
        "ok" | "no_data" => {
            if let Some(crit) = critical {
                if exceeds(value, crit) {
                    return "alert";
                }
            }
            if let Some(warn) = warning {
                if exceeds(value, warn) {
                    return "warn";
                }
            }
            "ok"
        }
        "warn" => {
            if let Some(crit) = critical {
                if exceeds(value, crit) {
                    return "alert";
                }
            }
            // Recovery from warning
            let recovery_threshold = warning_recovery.or(warning);
            if let Some(thresh) = recovery_threshold {
                if below(value, thresh) {
                    return "ok";
                }
            }
            "warn"
        }
        "alert" => {
            // Recovery from alert
            let recovery_threshold = critical_recovery.or(critical);
            if let Some(thresh) = recovery_threshold {
                if below(value, thresh) {
                    // Check if we should drop to warn or ok
                    if let Some(warn) = warning {
                        if exceeds(value, warn) {
                            return "warn";
                        }
                    }
                    return "ok";
                }
            }
            "alert"
        }
        _ => "ok",
    }
}

/// Handle the no-data condition for a monitor. Returns (state_changes, persisted_to_db).
#[allow(clippy::too_many_arguments)]
async fn handle_no_data(
    config_db: &ConfigDb,
    monitor: &Monitor,
    now_str: &str,
    http_client: &reqwest::Client,
    smtp_config: &alert_engine::SmtpConfig,
    smtp_transport: &Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>,
    should_flush: bool,
) -> (u64, bool) {
    let old_state = &monitor.state;
    let action = monitor.no_data_action.as_str();

    let new_state = match action {
        "notify" => "no_data",
        "resolve" => "ok",
        _ => "no_data", // "show" also sets no_data, but does not notify
    };

    if new_state != old_state.as_str() {
        let event_id = uuid::Uuid::new_v4().to_string();
        let event_msg = format!(
            "Monitor '{}': {} -> {} (no data received)",
            monitor.name, old_state, new_state,
        );
        let _ = config_db.create_monitor_event(
            &event_id,
            &monitor.id,
            &monitor.tenant_id,
            "",
            old_state,
            new_state,
            None,
            None,
            &event_msg,
        ).await;

        if action == "notify" {
            fire_notifications(
                config_db,
                monitor,
                &event_msg,
                new_state,
                0.0,
                0.0,
                http_client,
                smtp_config,
                smtp_transport,
            )
            .await;
        }

        // Transition persists immediately, exactly as before.
        let _ = config_db.update_monitor_state(&monitor.id, new_state, &monitor.group_states, now_str).await;
        (1, true)
    } else if should_flush {
        let _ = config_db.persist_monitor_eval(monitor, now_str).await;
        (0, true)
    } else {
        (0, false)
    }
}

/// Fire notifications to all configured channels for a monitor.
async fn fire_notifications(
    config_db: &ConfigDb,
    monitor: &Monitor,
    message: &str,
    alert_state: &str,
    value: f64,
    threshold: f64,
    http_client: &reqwest::Client,
    smtp_config: &alert_engine::SmtpConfig,
    smtp_transport: &Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>,
) {
    let channel_ids: Vec<String> =
        serde_json::from_str(&monitor.notification_channels).unwrap_or_default();

    for channel_id in &channel_ids {
        if let Ok(Some(channel)) = config_db.get_channel_by_id(channel_id).await {
            if !channel.enabled {
                continue;
            }
            let result = alert_engine::send_channel_notification(
                &channel,
                message,
                &monitor.name,
                alert_state,
                value,
                threshold,
                "monitors",
                &monitor.comparator,
                "",
                &monitor.id,
                "",
                http_client,
                smtp_config,
                smtp_transport,
            )
            .await;

            let (status, error_msg) = match &result {
                Ok(()) => ("sent", String::new()),
                Err(e) => {
                    tracing::warn!(
                        engine = "monitors",
                        monitor_id = %monitor.id,
                        channel_id = %channel_id,
                        error = %e,
                        "notification failed"
                    );
                    ("failed", e.clone())
                }
            };

            let _ = config_db.create_notification_log(
                channel_id,
                &monitor.tenant_id,
                "monitor",
                &monitor.name,
                alert_state,
                status,
                &error_msg,
            ).await;
        }
    }
}

/// Evaluate a composite monitor by examining the states of its component monitors.
/// Returns (state_changes, persisted_to_db).
#[allow(clippy::too_many_arguments)]
async fn evaluate_composite(
    config_db: &ConfigDb,
    monitor: &Monitor,
    now_str: &str,
    http_client: &reqwest::Client,
    smtp_config: &alert_engine::SmtpConfig,
    smtp_transport: &Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>,
    monitor_states: &HashMap<String, String>,
    should_flush: bool,
) -> anyhow::Result<(u64, bool)> {
    let monitor_ids: Vec<String> =
        serde_json::from_str(&monitor.composite_monitor_ids).unwrap_or_default();
    let formula = &monitor.composite_formula;

    if monitor_ids.is_empty() || formula.is_empty() {
        if monitor.state != "no_data" {
            let _ = config_db.update_monitor_state(&monitor.id, "no_data", "{}", now_str).await;
            return Ok((0, true));
        }
        if should_flush {
            let _ = config_db.persist_monitor_eval(monitor, now_str).await;
            return Ok((0, true));
        }
        return Ok((0, false));
    }

    // Build a map: letter label (A, B, C...) -> is_alerting (bool).
    // Member states come from the monitors already fetched at cycle start;
    // only members missing there (e.g. disabled) fall back to a point read.
    let mut letter_states: HashMap<char, bool> = HashMap::new();
    for (i, mid) in monitor_ids.iter().enumerate() {
        let letter = (b'A' + i as u8) as char;
        let is_alerting = match monitor_states.get(mid) {
            Some(state) => state == "alert" || state == "warn",
            None => match config_db.get_monitor_by_id(mid).await {
                Ok(Some(m)) => m.state == "alert" || m.state == "warn",
                _ => false,
            },
        };
        letter_states.insert(letter, is_alerting);
    }

    // Evaluate the boolean formula (simple parser for A && B && !C patterns)
    let composite_result = eval_boolean_formula(formula, &letter_states);
    let new_state = if composite_result { "alert" } else { "ok" };

    let mut changes: u64 = 0;
    if new_state != monitor.state.as_str() {
        changes = 1;
        let event_id = uuid::Uuid::new_v4().to_string();
        let event_msg = format!(
            "Composite monitor '{}': {} -> {} (formula: {})",
            monitor.name, monitor.state, new_state, formula,
        );
        let _ = config_db.create_monitor_event(
            &event_id,
            &monitor.id,
            &monitor.tenant_id,
            "",
            &monitor.state,
            new_state,
            None,
            None,
            &event_msg,
        ).await;

        fire_notifications(
            config_db,
            monitor,
            &event_msg,
            new_state,
            0.0,
            0.0,
            http_client,
            smtp_config,
            smtp_transport,
        )
        .await;

        if new_state == "alert" {
            let _ = config_db.update_monitor_triggered(&monitor.id, now_str).await;
        }
    }

    if changes > 0 {
        // Transition persists immediately, exactly as before.
        let _ = config_db.update_monitor_state(&monitor.id, new_state, "{}", now_str).await;
        Ok((changes, true))
    } else if should_flush {
        let _ = config_db.persist_monitor_eval(monitor, now_str).await;
        Ok((changes, true))
    } else {
        Ok((changes, false))
    }
}

/// Evaluate a simple boolean formula like "A && B && !C" or "A || B".
fn eval_boolean_formula(formula: &str, states: &HashMap<char, bool>) -> bool {
    // Split by || first (lower precedence), then by && (higher precedence)
    let or_parts: Vec<&str> = formula.split("||").collect();
    for or_part in or_parts {
        let and_parts: Vec<&str> = or_part.split("&&").collect();
        let and_result = and_parts.iter().all(|part| {
            let part = part.trim();
            if let Some(rest) = part.strip_prefix('!') {
                let letter = rest.trim().chars().next().unwrap_or('A');
                !states.get(&letter).copied().unwrap_or(false)
            } else {
                let letter = part.chars().next().unwrap_or('A');
                states.get(&letter).copied().unwrap_or(false)
            }
        });
        if and_result {
            return true;
        }
    }
    false
}

// ── ClickHouse query builders ──

/// Escape a string value for safe use in a ClickHouse SQL literal.
fn escape_ch(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

async fn query_metric(
    ch: &Client,
    monitor: &Monitor,
    has_groups: bool,
) -> anyhow::Result<Vec<(String, f64)>> {
    // Check if this is a PromQL-style expression
    let config_value: serde_json::Value = serde_json::from_str(&monitor.query_config)?;
    let is_promql = config_value
        .get("type")
        .and_then(|v| v.as_str())
        .map(|t| t == "promql")
        .unwrap_or(false);

    // Also treat it as PromQL if the "expression" field is present and non-empty
    let has_expression = config_value
        .get("expression")
        .and_then(|v| v.as_str())
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);

    if is_promql || has_expression {
        return query_metric_promql(ch, monitor, &config_value).await;
    }

    let cfg: MetricQueryConfig = serde_json::from_str(&monitor.query_config)?;

    let agg = match cfg.aggregation.as_str() {
        "sum" => "sum(Value)",
        "max" => "max(Value)",
        "min" => "min(Value)",
        "count" => "count()",
        _ => "avg(Value)", // default: avg
    };

    let mut conditions = vec![
        format!(
            "tenant_id = '{}'",
            escape_ch(&monitor.tenant_id)
        ),
        format!(
            "MetricName = '{}'",
            escape_ch(&cfg.metric_name)
        ),
        format!(
            "TimeUnix >= now() - INTERVAL {} SECOND",
            monitor.eval_window_secs
        ),
    ];

    for f in &cfg.filters {
        conditions.push(format!(
            "ResourceAttributes['{}'] = '{}'",
            escape_ch(&f.key),
            escape_ch(&f.value),
        ));
    }

    let where_clause = conditions.join(" AND ");

    if has_groups {
        let group_by_cols: Vec<String> = cfg
            .group_by
            .iter()
            .map(|g| format!("ResourceAttributes['{}']", escape_ch(g)))
            .collect();
        let group_expr = if group_by_cols.is_empty() {
            "'*'".to_string()
        } else {
            group_by_cols.join(" || ':' || ")
        };

        let sql = format!(
            "SELECT ({group_expr}) AS group_key, {agg} AS value \
             FROM metrics_gauge WHERE {where_clause} \
             GROUP BY group_key"
        );
        let rows = ch.query(&sql).with_option("max_execution_time", "30").fetch_all::<GroupedRow>().await?;
        Ok(rows.into_iter().map(|r| (r.group_key, r.value)).collect())
    } else {
        let sql = format!(
            "SELECT {agg} AS value FROM metrics_gauge WHERE {where_clause}"
        );
        let row = ch.query(&sql).with_option("max_execution_time", "30").fetch_one::<ValueRow>().await?;
        Ok(vec![("".to_string(), row.value)])
    }
}

/// Evaluate a PromQL expression for a metric monitor.
/// Uses the existing promql::evaluate_instant_query engine and maps the result
/// to the (group_key, value) pairs that the monitor threshold evaluator expects.
async fn query_metric_promql(
    ch: &Client,
    monitor: &Monitor,
    config: &serde_json::Value,
) -> anyhow::Result<Vec<(String, f64)>> {
    let expr = config
        .get("expr")
        .or_else(|| config.get("expression"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("promql query_config missing 'expr' or 'expression'"))?;

    let now = chrono::Utc::now().timestamp() as f64;
    let lookback = monitor.eval_window_secs as f64;

    let series = promql::evaluate_instant_query(ch, expr, now, lookback, &monitor.tenant_id)
        .await
        .map_err(|e| anyhow::anyhow!("promql evaluation failed: {}", e))?;

    let mut results: Vec<(String, f64)> = Vec::new();
    for ts in &series {
        // The last sample value is the "current" value for threshold evaluation
        let value = ts
            .samples
            .last()
            .map(|(_t, v)| *v)
            .unwrap_or(f64::NAN);

        if value.is_nan() {
            continue;
        }

        // Build the group key from labels (excluding __name__)
        let group_key: String = ts
            .labels
            .iter()
            .filter(|(k, _)| k.as_str() != "__name__")
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");

        results.push((group_key, value));
    }

    // If no series returned meaningful results, return empty (triggers no_data handling)
    if results.is_empty() {
        return Ok(vec![]);
    }

    // If there is only one group and its key is empty, treat as ungrouped
    if results.len() == 1 && results[0].0.is_empty() {
        results[0].0 = String::new();
    }

    Ok(results)
}

async fn query_log(
    ch: &Client,
    monitor: &Monitor,
    has_groups: bool,
) -> anyhow::Result<Vec<(String, f64)>> {
    let cfg: LogQueryConfig = serde_json::from_str(&monitor.query_config)?;

    let mut conditions = vec![
        format!(
            "tenant_id = '{}'",
            escape_ch(&monitor.tenant_id)
        ),
        format!(
            "Timestamp >= now() - INTERVAL {} SECOND",
            monitor.eval_window_secs
        ),
    ];

    // Full-text search using hasToken on lowered Body
    if !cfg.search.is_empty() {
        for term in cfg.search.split_whitespace() {
            let escaped = escape_ch(&term.to_lowercase());
            conditions.push(format!("hasToken(lower(Body), '{escaped}')"));
        }
    }

    for f in &cfg.filters {
        let field = escape_ch(&f.field);
        let value = escape_ch(&f.value);
        match f.op.as_str() {
            "!=" => conditions.push(format!("{field} != '{value}'")),
            "LIKE" | "like" => conditions.push(format!("{field} LIKE '%{value}%'")),
            _ => conditions.push(format!("{field} = '{value}'")),
        }
    }

    let where_clause = conditions.join(" AND ");

    if has_groups {
        let group_by_cols: Vec<String> = cfg
            .group_by
            .iter()
            .map(|g| escape_ch(g))
            .collect();
        let group_expr = if group_by_cols.is_empty() {
            "'*'".to_string()
        } else {
            group_by_cols.join(" || ':' || ")
        };

        let sql = format!(
            "SELECT ({group_expr}) AS group_key, count() AS value \
             FROM logs WHERE {where_clause} \
             GROUP BY group_key"
        );
        let rows = ch.query(&sql).with_option("max_execution_time", "30").fetch_all::<GroupedRow>().await?;
        Ok(rows.into_iter().map(|r| (r.group_key, r.value)).collect())
    } else {
        let sql = format!(
            "SELECT count() AS value FROM logs WHERE {where_clause}"
        );
        let row = ch.query(&sql).with_option("max_execution_time", "30").fetch_one::<ValueRow>().await?;
        Ok(vec![("".to_string(), row.value)])
    }
}

async fn query_apm(
    ch: &Client,
    monitor: &Monitor,
    has_groups: bool,
) -> anyhow::Result<Vec<(String, f64)>> {
    let cfg: ApmQueryConfig = serde_json::from_str(&monitor.query_config)?;

    let mut conditions = vec![
        format!(
            "tenant_id = '{}'",
            escape_ch(&monitor.tenant_id)
        ),
        format!(
            "service_name = '{}'",
            escape_ch(&cfg.service)
        ),
        format!(
            "timestamp >= now() - INTERVAL {} SECOND",
            monitor.eval_window_secs
        ),
    ];

    if let Some(ref ep) = cfg.endpoint_filter {
        if !ep.is_empty() {
            conditions.push(format!(
                "http_path = '{}'",
                escape_ch(ep),
            ));
        }
    }

    let where_clause = conditions.join(" AND ");

    let agg_expr = match cfg.metric.as_str() {
        "error_rate" => {
            "countIf(status = 'ERROR' OR JSONExtractInt(attributes, 'http_status_code') >= 500) \
             * 100.0 / count()"
        }
        "error_count" => {
            "countIf(status = 'ERROR' OR JSONExtractInt(attributes, 'http_status_code') >= 500)"
        }
        "request_rate" => {
            // requests per second over the evaluation window
            &format!("count() / {}", monitor.eval_window_secs)
        }
        "p50_latency" | "p50" => "quantile(0.50)(duration_ns) / 1000000",
        "p75_latency" | "p75" => "quantile(0.75)(duration_ns) / 1000000",
        "p90_latency" | "p90" => "quantile(0.90)(duration_ns) / 1000000",
        "p95_latency" | "p95" => "quantile(0.95)(duration_ns) / 1000000",
        "p99_latency" | "p99" => "quantile(0.99)(duration_ns) / 1000000",
        _ => "count()",
    };

    // Handle request_rate specially since it uses a runtime-computed string
    let agg_str;
    let agg = if cfg.metric == "request_rate" {
        agg_str = format!("count() * 1.0 / {}", monitor.eval_window_secs);
        &agg_str
    } else {
        agg_expr
    };

    if has_groups {
        let group_by_cols: Vec<String> = cfg
            .group_by
            .iter()
            .map(|g| {
                if g == "endpoint" || g == "http_path" {
                    "http_path".to_string()
                } else {
                    escape_ch(g)
                }
            })
            .collect();
        let group_expr = if group_by_cols.is_empty() {
            "'*'".to_string()
        } else {
            group_by_cols.join(" || ':' || ")
        };

        let sql = format!(
            "SELECT ({group_expr}) AS group_key, {agg} AS value \
             FROM spans WHERE {where_clause} \
             GROUP BY group_key"
        );
        let rows = ch.query(&sql).with_option("max_execution_time", "30").fetch_all::<GroupedRow>().await?;
        Ok(rows.into_iter().map(|r| (r.group_key, r.value)).collect())
    } else {
        let sql = format!(
            "SELECT {agg} AS value FROM spans WHERE {where_clause}"
        );
        let row = ch.query(&sql).with_option("max_execution_time", "30").fetch_one::<ValueRow>().await?;
        Ok(vec![("".to_string(), row.value)])
    }
}

/// Execute a monitor query and return current value + time series for preview.
/// This is used by the /monitors/preview endpoint in the creation wizard.
pub async fn preview_query(
    ch: &Client,
    tenant_id: &str,
    monitor_type: &str,
    query_config: &serde_json::Value,
    eval_window_secs: i64,
    group_by: &[String],
) -> anyhow::Result<PreviewResult> {
    // Build a temporary Monitor struct for the query functions
    let temp_monitor = Monitor {
        id: String::new(),
        tenant_id: tenant_id.to_string(),
        name: String::new(),
        monitor_type: monitor_type.to_string(),
        query_config: serde_json::to_string(query_config)?,
        critical: None,
        critical_recovery: None,
        warning: None,
        warning_recovery: None,
        comparator: "above".to_string(),
        eval_window_secs,
        eval_interval_secs: 60,
        group_by: serde_json::to_string(group_by)?,
        state: "ok".to_string(),
        group_states: "{}".to_string(),
        no_data_action: "show".to_string(),
        no_data_timeframe: 600,
        auto_resolve_hours: None,
        message: String::new(),
        notification_channels: "[]".to_string(),
        renotify_interval: None,
        tags: "[]".to_string(),
        priority: None,
        enabled: true,
        composite_formula: String::new(),
        composite_monitor_ids: "[]".to_string(),
        last_eval_at: None,
        last_triggered_at: None,
        created_by: String::new(),
        created_at: String::new(),
        updated_at: String::new(),
    };

    let has_groups = !group_by.is_empty();
    let results = match monitor_type {
        "metric" => query_metric(ch, &temp_monitor, has_groups).await?,
        "log" => query_log(ch, &temp_monitor, has_groups).await?,
        "apm" => query_apm(ch, &temp_monitor, has_groups).await?,
        _ => vec![],
    };

    let current_value = results.first().map(|(_, v)| *v);

    // Build a simple time series by querying multiple sub-windows
    let timeseries = build_preview_timeseries(ch, &temp_monitor, eval_window_secs).await;

    Ok(PreviewResult {
        current_value,
        groups: results,
        timeseries,
    })
}

#[derive(Debug, serde::Serialize)]
pub struct PreviewResult {
    pub current_value: Option<f64>,
    pub groups: Vec<(String, f64)>,
    pub timeseries: Vec<TimeseriesPoint>,
}

#[derive(Debug, serde::Serialize)]
pub struct TimeseriesPoint {
    pub timestamp: String,
    pub value: f64,
}

/// Build a simple timeseries for preview by splitting the window into ~10 buckets.
async fn build_preview_timeseries(
    ch: &Client,
    monitor: &Monitor,
    window_secs: i64,
) -> Vec<TimeseriesPoint> {
    let bucket_count = 10i64;
    let bucket_secs = (window_secs / bucket_count).max(1);

    // Check if this is an expression-based metric (PromQL)
    if monitor.monitor_type == "metric" {
        let config_value: serde_json::Value = match serde_json::from_str(&monitor.query_config) {
            Ok(v) => v,
            Err(_) => return vec![],
        };
        let expr = config_value
            .get("expr")
            .or_else(|| config_value.get("expression"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !expr.trim().is_empty() {
            return build_preview_timeseries_promql(ch, expr, &monitor.tenant_id, window_secs)
                .await;
        }
    }

    // Build a ClickHouse query that groups by time bucket
    let (table, agg_expr, extra_conditions) = match monitor.monitor_type.as_str() {
        "metric" => {
            let cfg: MetricQueryConfig = match serde_json::from_str(&monitor.query_config) {
                Ok(c) => c,
                Err(_) => return vec![],
            };
            let agg = match cfg.aggregation.as_str() {
                "sum" => "sum(Value)",
                "max" => "max(Value)",
                "min" => "min(Value)",
                "count" => "count()",
                _ => "avg(Value)",
            };
            let mut conds = vec![
                format!("tenant_id = '{}'", escape_ch(&monitor.tenant_id)),
                format!("MetricName = '{}'", escape_ch(&cfg.metric_name)),
            ];
            for f in &cfg.filters {
                conds.push(format!(
                    "ResourceAttributes['{}'] = '{}'",
                    escape_ch(&f.key),
                    escape_ch(&f.value),
                ));
            }
            ("metrics_gauge".to_string(), agg.to_string(), conds.join(" AND "))
        }
        "log" => {
            let cfg: LogQueryConfig = match serde_json::from_str(&monitor.query_config) {
                Ok(c) => c,
                Err(_) => return vec![],
            };
            let mut conds = vec![format!(
                "tenant_id = '{}'",
                escape_ch(&monitor.tenant_id)
            )];
            if !cfg.search.is_empty() {
                for term in cfg.search.split_whitespace() {
                    conds.push(format!(
                        "hasToken(lower(Body), '{}')",
                        escape_ch(&term.to_lowercase())
                    ));
                }
            }
            for f in &cfg.filters {
                conds.push(format!("{} = '{}'", escape_ch(&f.field), escape_ch(&f.value)));
            }
            ("logs".to_string(), "count()".to_string(), conds.join(" AND "))
        }
        "apm" => {
            let cfg: ApmQueryConfig = match serde_json::from_str(&monitor.query_config) {
                Ok(c) => c,
                Err(_) => return vec![],
            };
            let mut conds = vec![
                format!("tenant_id = '{}'", escape_ch(&monitor.tenant_id)),
                format!("service_name = '{}'", escape_ch(&cfg.service)),
            ];
            if let Some(ref ep) = cfg.endpoint_filter {
                if !ep.is_empty() {
                    conds.push(format!("http_path = '{}'", escape_ch(ep)));
                }
            }
            let agg = match cfg.metric.as_str() {
                "error_rate" => "countIf(status = 'ERROR' OR JSONExtractInt(attributes, 'http_status_code') >= 500) * 100.0 / count()".to_string(),
                "error_count" => "countIf(status = 'ERROR' OR JSONExtractInt(attributes, 'http_status_code') >= 500)".to_string(),
                "request_rate" => format!("count() * 1.0 / {}", bucket_secs),
                "p50_latency" | "p50" => "quantile(0.50)(duration_ns) / 1000000".to_string(),
                "p75_latency" | "p75" => "quantile(0.75)(duration_ns) / 1000000".to_string(),
                "p90_latency" | "p90" => "quantile(0.90)(duration_ns) / 1000000".to_string(),
                "p95_latency" | "p95" => "quantile(0.95)(duration_ns) / 1000000".to_string(),
                "p99_latency" | "p99" => "quantile(0.99)(duration_ns) / 1000000".to_string(),
                _ => "count()".to_string(),
            };
            ("spans".to_string(), agg, conds.join(" AND "))
        }
        _ => return vec![],
    };

    let time_col = match table.as_str() {
        "metrics_gauge" => "TimeUnix",
        "logs" => "Timestamp",
        _ => "timestamp",
    };

    let sql = format!(
        "SELECT toString(toStartOfInterval({time_col}, INTERVAL {bucket_secs} SECOND)) AS ts, \
         {agg_expr} AS value \
         FROM {table} \
         WHERE {extra_conditions} AND {time_col} >= now() - INTERVAL {window_secs} SECOND \
         GROUP BY ts ORDER BY ts"
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TsRow {
        ts: String,
        value: f64,
    }

    match ch.query(&sql).fetch_all::<TsRow>().await {
        Ok(rows) => rows
            .into_iter()
            .map(|r| TimeseriesPoint {
                timestamp: r.ts,
                value: r.value,
            })
            .collect(),
        Err(e) => {
            tracing::debug!(engine = "monitors", error = %e, "preview timeseries query failed");
            vec![]
        }
    }
}

/// Build a timeseries for preview using the PromQL range query evaluator.
async fn build_preview_timeseries_promql(
    ch: &Client,
    expr: &str,
    tenant_id: &str,
    window_secs: i64,
) -> Vec<TimeseriesPoint> {
    let now = chrono::Utc::now().timestamp() as f64;
    let start = now - window_secs as f64;
    let step = (window_secs as f64 / 10.0).max(1.0);

    match promql::evaluate_range_query(ch, expr, start, now, step, tenant_id).await {
        Ok(series) => {
            // Take the first series and convert its samples to TimeseriesPoints
            if let Some(ts) = series.first() {
                ts.samples
                    .iter()
                    .map(|(t, v)| {
                        let dt = chrono::DateTime::from_timestamp(*t as i64, 0)
                            .unwrap_or_default();
                        TimeseriesPoint {
                            timestamp: dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                            value: *v,
                        }
                    })
                    .collect()
            } else {
                vec![]
            }
        }
        Err(e) => {
            tracing::debug!(engine = "monitors", error = %e, "preview promql timeseries failed");
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_threshold_basic() {
        // OK -> alert when value exceeds critical
        assert_eq!(
            evaluate_threshold("ok", 100.0, Some(50.0), None, None, None, "above"),
            "alert"
        );

        // OK -> warn when value exceeds warning but not critical
        assert_eq!(
            evaluate_threshold("ok", 40.0, Some(50.0), None, Some(30.0), None, "above"),
            "warn"
        );

        // OK stays OK when below all thresholds
        assert_eq!(
            evaluate_threshold("ok", 10.0, Some(50.0), None, Some(30.0), None, "above"),
            "ok"
        );
    }

    #[test]
    fn test_evaluate_threshold_hysteresis() {
        // Alert stays alert even when value drops below critical (no recovery threshold set)
        // because without recovery threshold, the critical value IS the recovery threshold
        assert_eq!(
            evaluate_threshold("alert", 49.0, Some(50.0), None, None, None, "above"),
            "ok"
        );

        // Alert stays alert when value is above recovery threshold
        assert_eq!(
            evaluate_threshold("alert", 45.0, Some(50.0), Some(40.0), None, None, "above"),
            "alert"
        );

        // Alert recovers when value drops below recovery threshold
        assert_eq!(
            evaluate_threshold("alert", 35.0, Some(50.0), Some(40.0), None, None, "above"),
            "ok"
        );

        // Alert drops to warn when below critical recovery but above warning
        assert_eq!(
            evaluate_threshold("alert", 35.0, Some(50.0), Some(40.0), Some(30.0), None, "above"),
            "warn"
        );
    }

    #[test]
    fn test_evaluate_threshold_below() {
        // "below" comparator: alert when value < critical
        assert_eq!(
            evaluate_threshold("ok", 10.0, Some(20.0), None, None, None, "below"),
            "alert"
        );

        // value above threshold is OK for "below" comparator
        assert_eq!(
            evaluate_threshold("ok", 30.0, Some(20.0), None, None, None, "below"),
            "ok"
        );
    }

    #[test]
    fn test_eval_boolean_formula() {
        let mut states = HashMap::new();
        states.insert('A', true);
        states.insert('B', true);
        states.insert('C', false);

        assert!(eval_boolean_formula("A && B", &states));
        assert!(eval_boolean_formula("A && B && !C", &states));
        assert!(!eval_boolean_formula("A && B && C", &states));
        assert!(eval_boolean_formula("A || C", &states));
        assert!(!eval_boolean_formula("C", &states));
    }

    #[test]
    fn test_worst_state() {
        assert_eq!(worst_state(["ok", "ok"].iter().copied()), "ok");
        assert_eq!(worst_state(["ok", "warn"].iter().copied()), "warn");
        assert_eq!(worst_state(["ok", "alert"].iter().copied()), "alert");
        assert_eq!(worst_state(["warn", "alert", "ok"].iter().copied()), "alert");
        assert_eq!(worst_state(["ok", "no_data"].iter().copied()), "no_data");
    }
}
