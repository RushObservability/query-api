use std::collections::{BTreeMap, HashMap};
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
    self_metrics: Arc<crate::self_metrics::SelfMetrics>,
) {
    tokio::spawn(async move {
        let http_client = reqwest::Client::new();
        let smtp_transport = build_smtp_transport(&smtp_config);
        let mut eval_state = crate::eval_state::EvalState::new(EVAL_FLUSH_EVERY);

        loop {
            let start = Instant::now();
            let mut ok = true;
            let (evaluated, state_changes) = match run_evaluation_cycle(
                &ch,
                &config_db,
                &http_client,
                &smtp_config,
                &smtp_transport,
                &mut eval_state,
            )
            .await
            {
                Ok(stats) => stats,
                Err(e) => {
                    tracing::error!(engine = "monitors", error = %e, "evaluation cycle failed");
                    ok = false;
                    (0, 0)
                }
            };
            let elapsed_ms = start.elapsed().as_millis() as u64;
            self_metrics.record_engine("monitor_engine", elapsed_ms, ok);

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
                if let Ok(last) =
                    chrono::NaiveDateTime::parse_from_str(last_eval, "%Y-%m-%dT%H:%M:%SZ")
                {
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

    let outcomes: Vec<(String, u64, bool)> =
        futures_util::stream::iter(jobs.into_iter().map(|(monitor, should_flush)| async move {
            let result = match crate::query_governor::run_background(
                &monitor.tenant_id,
                evaluate_monitor(
                    ch,
                    config_db,
                    &monitor,
                    now_str_ref,
                    http_client,
                    smtp_config,
                    smtp_transport,
                    monitor_states_ref,
                    should_flush,
                ),
            )
            .await
            {
                Ok(result) => result,
                Err(error) => Err(anyhow::anyhow!("background admission rejected: {error:?}")),
            };
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
                    handle_no_data(
                        config_db,
                        &monitor,
                        now_str_ref,
                        http_client,
                        smtp_config,
                        smtp_transport,
                        should_flush,
                    )
                    .await
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
        "metric" => query_metric(ch, monitor, &group_by).await?,
        "log" => query_log(ch, monitor, &group_by).await?,
        "apm" => query_apm(ch, monitor, &group_by).await?,
        "composite" => {
            // Composite monitors combine other monitor states, not queries
            return evaluate_composite(
                config_db,
                monitor,
                now_str,
                http_client,
                smtp_config,
                smtp_transport,
                monitor_states,
                should_flush,
            )
            .await;
        }
        other => {
            tracing::warn!(engine = "monitors", monitor_id = %monitor.id, "unknown monitor type: {other}");
            return Ok((0, false));
        }
    };

    if results.is_empty() {
        // No data returned
        return Ok(handle_no_data(
            config_db,
            monitor,
            now_str,
            http_client,
            smtp_config,
            smtp_transport,
            should_flush,
        )
        .await);
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

            let alert_name = render_monitor_template(
                &monitor.name,
                &group_by,
                group_key,
                new_state,
                *value,
                threshold.unwrap_or(0.0),
            );

            let event_msg = format!(
                "Monitor '{}'{}: {} -> {} (value={:.4}, threshold={:.4})",
                alert_name,
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
            let notification_message = if monitor.message.trim().is_empty() {
                event_msg.clone()
            } else {
                render_monitor_template(
                    &monitor.message,
                    &group_by,
                    group_key,
                    new_state,
                    *value,
                    threshold.unwrap_or(0.0),
                )
            };

            let event_id = uuid::Uuid::new_v4().to_string();
            let _ = config_db
                .create_monitor_event(
                    &event_id,
                    &monitor.id,
                    &monitor.tenant_id,
                    group_key,
                    current_state,
                    new_state,
                    Some(*value),
                    threshold,
                    &event_msg,
                )
                .await;

            // Fire notifications
            fire_notifications(
                config_db,
                monitor,
                &alert_name,
                &notification_message,
                new_state,
                *value,
                threshold.unwrap_or(0.0),
                http_client,
                smtp_config,
                smtp_transport,
            )
            .await;

            if new_state == "alert" || new_state == "warn" {
                let _ = config_db
                    .update_monitor_triggered(&monitor.id, now_str)
                    .await;
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
            .map(|(gk, _)| group_states.get(gk).map(|s| s.as_str()).unwrap_or("ok"))
            .unwrap_or("ok")
    };

    // Persist only on a real transition (a group changed or the overall state
    // moved) — that path is identical to before. Otherwise just flush
    // last_eval_at on the coarse cadence from the row we already hold.
    if changes > 0 || overall != monitor.state.as_str() {
        let group_states_json =
            serde_json::to_string(&group_states).unwrap_or_else(|_| "{}".to_string());
        config_db
            .update_monitor_state(&monitor.id, overall, &group_states_json, now_str)
            .await?;
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

fn threshold_matches(value: f64, threshold: f64, comparator: &str) -> bool {
    match comparator {
        "above" => value > threshold,
        "above_or_equal" => value >= threshold,
        "equal" => value == threshold,
        "below_or_equal" => value <= threshold,
        "below" => value < threshold,
        _ => value > threshold,
    }
}

fn threshold_has_recovered(
    value: f64,
    trigger_threshold: f64,
    recovery_threshold: Option<f64>,
    comparator: &str,
) -> bool {
    let boundary = recovery_threshold.unwrap_or(trigger_threshold);
    match comparator {
        "above" => value <= boundary,
        "above_or_equal" => value < boundary,
        "equal" if recovery_threshold.is_some() => value == boundary,
        "equal" => value != trigger_threshold,
        "below_or_equal" => value > boundary,
        "below" => value >= boundary,
        _ => value <= boundary,
    }
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
    match current_state {
        "ok" | "no_data" => {
            if let Some(crit) = critical {
                if threshold_matches(value, crit, comparator) {
                    return "alert";
                }
            }
            if let Some(warn) = warning {
                if threshold_matches(value, warn, comparator) {
                    return "warn";
                }
            }
            "ok"
        }
        "warn" => {
            if let Some(crit) = critical {
                if threshold_matches(value, crit, comparator) {
                    return "alert";
                }
            }
            if let Some(warn) = warning {
                if threshold_has_recovered(value, warn, warning_recovery, comparator) {
                    return "ok";
                }
            }
            "warn"
        }
        "alert" => {
            if let Some(crit) = critical {
                if threshold_has_recovered(value, crit, critical_recovery, comparator) {
                    // Check if we should drop to warn or ok
                    if let Some(warn) = warning {
                        if threshold_matches(value, warn, comparator) {
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
        let _ = config_db
            .create_monitor_event(
                &event_id,
                &monitor.id,
                &monitor.tenant_id,
                "",
                old_state,
                new_state,
                None,
                None,
                &event_msg,
            )
            .await;

        if action == "notify" {
            fire_notifications(
                config_db,
                monitor,
                &monitor.name,
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
        let _ = config_db
            .update_monitor_state(&monitor.id, new_state, &monitor.group_states, now_str)
            .await;
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
    alert_name: &str,
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
                alert_name,
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

            let _ = config_db
                .create_notification_log(
                    channel_id,
                    &monitor.tenant_id,
                    "monitor",
                    alert_name,
                    alert_state,
                    status,
                    &error_msg,
                )
                .await;
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
            let _ = config_db
                .update_monitor_state(&monitor.id, "no_data", "{}", now_str)
                .await;
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
        let _ = config_db
            .create_monitor_event(
                &event_id,
                &monitor.id,
                &monitor.tenant_id,
                "",
                &monitor.state,
                new_state,
                None,
                None,
                &event_msg,
            )
            .await;

        fire_notifications(
            config_db,
            monitor,
            &monitor.name,
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
            let _ = config_db
                .update_monitor_triggered(&monitor.id, now_str)
                .await;
        }
    }

    if changes > 0 {
        // Transition persists immediately, exactly as before.
        let _ = config_db
            .update_monitor_state(&monitor.id, new_state, "{}", now_str)
            .await;
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

pub(crate) fn apm_match_condition(column: &str, value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value == "*" {
        return None;
    }

    let escaped = escape_ch(value);
    if value.contains('*') {
        let pattern = escaped
            .replace('%', "\\%")
            .replace('_', "\\_")
            .replace('*', "%");
        Some(format!("{column} LIKE '{pattern}'"))
    } else {
        Some(format!("{column} = '{escaped}'"))
    }
}

fn render_monitor_template(
    template: &str,
    group_by: &[String],
    group_key: &str,
    state: &str,
    value: f64,
    threshold: f64,
) -> String {
    let mut rendered = template
        .replace("{{group}}", group_key)
        .replace("{{state}}", state)
        .replace("{{value}}", &format_template_number(value))
        .replace("{{threshold}}", &format_template_number(threshold));

    if !group_by.is_empty() {
        for (field, field_value) in group_by.iter().zip(group_key.splitn(group_by.len(), ':')) {
            let aliases: &[&str] = match field.as_str() {
                "service" | "service_name" => &["service", "service_name"],
                "endpoint" | "http_path" => &["endpoint", "http_path"],
                "method" | "http_method" => &["method", "http_method"],
                "status_code" | "http_status_code" => &["status_code", "http_status_code"],
                _ => &[],
            };
            rendered = rendered.replace(&format!("{{{{{field}}}}}"), field_value);
            for alias in aliases {
                rendered = rendered.replace(&format!("{{{{{alias}}}}}"), field_value);
            }
        }
    }

    rendered
}

fn format_template_number(value: f64) -> String {
    let formatted = format!("{value:.4}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

const MAX_GROUP_BY_FIELDS: usize = 5;

/// Resolve a public log field name to a fixed ClickHouse expression.
///
/// Monitor fields are API input, so they must never be copied into SQL as identifiers.
/// Keep this mapping in sync with the static `log_field` autocomplete response.
fn log_field_expr(field: &str) -> Option<&'static str> {
    match field {
        "service_name" | "ServiceName" => Some("ServiceName"),
        "severity" | "severity_text" | "SeverityText" => Some("SeverityText"),
        "severity_number" | "SeverityNumber" => Some("SeverityNumber"),
        "body" | "Body" => Some("Body"),
        "trace_id" | "TraceId" => Some("TraceId"),
        "span_id" | "SpanId" => Some("SpanId"),
        "scope_name" | "ScopeName" => Some("ScopeName"),
        "mat_k8s_namespace" => Some("mat_k8s_namespace"),
        "mat_k8s_pod" => Some("mat_k8s_pod"),
        "mat_k8s_container" => Some("mat_k8s_container"),
        "mat_k8s_deployment" => Some("mat_k8s_deployment"),
        "mat_k8s_node" => Some("mat_k8s_node"),
        "mat_level" => Some("mat_level"),
        "mat_component" => Some("mat_component"),
        "mat_environment" => Some("mat_environment"),
        "mat_source_ip" => Some("mat_source_ip"),
        "mat_action" => Some("mat_action"),
        _ => None,
    }
}

/// Resolve a public APM grouping name to a fixed ClickHouse expression.
fn apm_group_expr(field: &str) -> Option<&'static str> {
    match field {
        "service" | "service_name" => Some("service_name"),
        "endpoint" | "http_path" => Some("http_path"),
        "method" | "http_method" => Some("http_method"),
        "status_code" | "http_status_code" => Some("http_status_code"),
        "status" => Some("status"),
        "span_name" => Some("span_name"),
        "kind" | "span_kind" => Some("kind"),
        _ => None,
    }
}

fn is_valid_metric_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 128
        && label
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'/'))
}

fn group_expression(
    fields: &[String],
    resolver: fn(&str) -> Option<&'static str>,
) -> anyhow::Result<String> {
    let expressions = fields
        .iter()
        .map(|field| {
            resolver(field)
                .map(|expr| format!("toString({expr})"))
                .ok_or_else(|| anyhow::anyhow!("unsupported group_by field '{field}'"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(if expressions.is_empty() {
        "'*'".to_string()
    } else {
        expressions.join(" || ':' || ")
    })
}

/// Validate all query fields that can influence SQL construction.
/// Called by create, update, and preview handlers; the query functions also resolve
/// fields independently so malformed legacy rows fail closed during evaluation.
pub(crate) fn validate_query_fields(
    monitor_type: &str,
    config: &serde_json::Value,
    group_by: &[String],
) -> Result<(), String> {
    if group_by.len() > MAX_GROUP_BY_FIELDS {
        return Err(format!(
            "group_by supports at most {MAX_GROUP_BY_FIELDS} fields"
        ));
    }

    match monitor_type {
        "metric" => {
            let is_expression = config
                .get("expr")
                .or_else(|| config.get("expression"))
                .and_then(|value| value.as_str())
                .is_some_and(|value| !value.trim().is_empty());
            if !is_expression {
                let cfg: MetricQueryConfig = serde_json::from_value(config.clone())
                    .map_err(|error| format!("invalid metric query_config: {error}"))?;
                let valid_aggregations = ["avg", "sum", "max", "min", "count"];
                if !valid_aggregations.contains(&cfg.aggregation.as_str()) {
                    return Err(format!("invalid metric aggregation '{}'", cfg.aggregation));
                }
                for filter in &cfg.filters {
                    if !is_valid_metric_label(&filter.key) {
                        return Err(format!("invalid metric filter label '{}'", filter.key));
                    }
                }
            }
            for field in group_by {
                if !is_valid_metric_label(field) {
                    return Err(format!("invalid metric group_by label '{field}'"));
                }
            }
        }
        "log" => {
            let cfg: LogQueryConfig = serde_json::from_value(config.clone())
                .map_err(|error| format!("invalid log query_config: {error}"))?;
            for filter in &cfg.filters {
                if log_field_expr(&filter.field).is_none() {
                    return Err(format!("unsupported log filter field '{}'", filter.field));
                }
                if !matches!(filter.op.as_str(), "=" | "!=" | "LIKE" | "like") {
                    return Err(format!("unsupported log filter operator '{}'", filter.op));
                }
            }
            for field in group_by {
                if log_field_expr(field).is_none() {
                    return Err(format!("unsupported log group_by field '{field}'"));
                }
            }
        }
        "apm" => {
            serde_json::from_value::<ApmQueryConfig>(config.clone())
                .map_err(|error| format!("invalid apm query_config: {error}"))?;
            for field in group_by {
                if apm_group_expr(field).is_none() {
                    return Err(format!("unsupported apm group_by field '{field}'"));
                }
            }
        }
        "composite" => {}
        _ => return Err(format!("invalid monitor type: {monitor_type}")),
    }

    Ok(())
}

async fn query_metric(
    ch: &Client,
    monitor: &Monitor,
    group_by: &[String],
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
        format!("tenant_id = '{}'", escape_ch(&monitor.tenant_id)),
        format!("MetricName = '{}'", escape_ch(&cfg.metric_name)),
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

    if !group_by.is_empty() {
        let group_by_cols: Vec<String> = group_by
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
        let rows = crate::tenant_query(ch, &sql, &monitor.tenant_id)
            .with_option("max_execution_time", "30")
            .fetch_all::<GroupedRow>()
            .await?;
        Ok(rows.into_iter().map(|r| (r.group_key, r.value)).collect())
    } else {
        let sql = format!("SELECT {agg} AS value FROM metrics_gauge WHERE {where_clause}");
        let row = crate::tenant_query(ch, &sql, &monitor.tenant_id)
            .with_option("max_execution_time", "30")
            .fetch_one::<ValueRow>()
            .await?;
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
        let value = ts.samples.last().map(|(_t, v)| *v).unwrap_or(f64::NAN);

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
    group_by: &[String],
) -> anyhow::Result<Vec<(String, f64)>> {
    let cfg: LogQueryConfig = serde_json::from_str(&monitor.query_config)?;

    let mut conditions = vec![
        format!("tenant_id = '{}'", escape_ch(&monitor.tenant_id)),
        format!(
            "Timestamp >= now() - INTERVAL {} SECOND",
            monitor.eval_window_secs
        ),
    ];

    // Full-text search via `lower(Body) LIKE` — matches the `idx_body_text`
    // text(ngrams(4)) index char-for-char. (hasToken uses token semantics the
    // ngram index can't serve, forcing a full Body scan every eval interval.)
    if !cfg.search.is_empty() {
        for term in cfg.search.split_whitespace() {
            let escaped = escape_ch(&term.to_lowercase());
            let inner = escaped.replace('%', "\\%").replace('_', "\\_");
            conditions.push(format!("lower(Body) LIKE '%{inner}%'"));
        }
    }

    if !cfg.service.is_empty() {
        conditions.push(format!("ServiceName = '{}'", escape_ch(&cfg.service)));
    }
    if !cfg.severities.is_empty() {
        let severities = cfg
            .severities
            .iter()
            .map(|severity| format!("'{}'", escape_ch(severity)))
            .collect::<Vec<_>>()
            .join(", ");
        conditions.push(format!("SeverityText IN ({severities})"));
    }

    for f in &cfg.filters {
        let field = log_field_expr(&f.field)
            .ok_or_else(|| anyhow::anyhow!("unsupported log filter field '{}'", f.field))?;
        let value = escape_ch(&f.value);
        match f.op.as_str() {
            "!=" => conditions.push(format!("{field} != '{value}'")),
            "LIKE" | "like" => conditions.push(format!("{field} LIKE '%{value}%'")),
            "=" => conditions.push(format!("{field} = '{value}'")),
            _ => {
                return Err(anyhow::anyhow!(
                    "unsupported log filter operator '{}'",
                    f.op
                ));
            }
        }
    }

    let where_clause = conditions.join(" AND ");

    if !group_by.is_empty() {
        let group_expr = group_expression(group_by, log_field_expr)?;

        let sql = format!(
            "SELECT ({group_expr}) AS group_key, count() AS value \
             FROM logs WHERE {where_clause} \
             GROUP BY group_key"
        );
        let rows = crate::tenant_query(ch, &sql, &monitor.tenant_id)
            .with_option("max_execution_time", "30")
            .fetch_all::<GroupedRow>()
            .await?;
        Ok(rows.into_iter().map(|r| (r.group_key, r.value)).collect())
    } else {
        let sql = format!("SELECT count() AS value FROM logs WHERE {where_clause}");
        let row = crate::tenant_query(ch, &sql, &monitor.tenant_id)
            .with_option("max_execution_time", "30")
            .fetch_one::<ValueRow>()
            .await?;
        Ok(vec![("".to_string(), row.value)])
    }
}

async fn query_apm(
    ch: &Client,
    monitor: &Monitor,
    group_by: &[String],
) -> anyhow::Result<Vec<(String, f64)>> {
    let cfg: ApmQueryConfig = serde_json::from_str(&monitor.query_config)?;

    let mut conditions = vec![
        format!("tenant_id = '{}'", escape_ch(&monitor.tenant_id)),
        format!(
            "timestamp >= now() - INTERVAL {} SECOND",
            monitor.eval_window_secs
        ),
    ];

    if let Some(condition) = apm_match_condition("service_name", &cfg.service) {
        conditions.push(condition);
    }

    if let Some(ref ep) = cfg.endpoint_filter {
        if let Some(condition) = apm_match_condition("http_path", ep) {
            conditions.push(condition);
        }
    }

    let where_clause = conditions.join(" AND ");

    let agg = apm_aggregation(&cfg.metric, monitor.eval_window_secs);

    if !group_by.is_empty() {
        let group_expr = group_expression(group_by, apm_group_expr)?;

        let sql = format!(
            "SELECT ({group_expr}) AS group_key, {agg} AS value \
             FROM spans WHERE {where_clause} \
             GROUP BY group_key"
        );
        let rows = crate::tenant_query(ch, &sql, &monitor.tenant_id)
            .with_option("max_execution_time", "30")
            .fetch_all::<GroupedRow>()
            .await?;
        Ok(rows.into_iter().map(|r| (r.group_key, r.value)).collect())
    } else {
        let sql = format!("SELECT {agg} AS value FROM spans WHERE {where_clause}");
        let row = crate::tenant_query(ch, &sql, &monitor.tenant_id)
            .with_option("max_execution_time", "30")
            .fetch_one::<ValueRow>()
            .await?;
        Ok(vec![("".to_string(), row.value)])
    }
}

const APM_ERROR_CONDITION: &str =
    "status IN ('ERROR', 'STATUS_CODE_ERROR') OR http_status_code >= 500";

fn apm_aggregation(metric: &str, rate_window_secs: i64) -> String {
    match metric {
        "error_rate" => {
            format!("if(count() = 0, 0.0, countIf({APM_ERROR_CONDITION}) * 100.0 / count())")
        }
        "error_count" => format!("countIf({APM_ERROR_CONDITION})"),
        "request_rate" => format!("count() * 1.0 / {rate_window_secs}"),
        "p50_latency" | "p50" => "quantile(0.50)(duration_ns) / 1000000".to_string(),
        "p75_latency" | "p75" => "quantile(0.75)(duration_ns) / 1000000".to_string(),
        "p90_latency" | "p90" => "quantile(0.90)(duration_ns) / 1000000".to_string(),
        "p95_latency" | "p95" => "quantile(0.95)(duration_ns) / 1000000".to_string(),
        "p99_latency" | "p99" => "quantile(0.99)(duration_ns) / 1000000".to_string(),
        _ => "count()".to_string(),
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
    lookback_secs: i64,
    group_by: &[String],
    conditions: PreviewConditions,
) -> anyhow::Result<PreviewResult> {
    // Build a temporary Monitor struct for the query functions
    let temp_monitor = Monitor {
        id: String::new(),
        tenant_id: tenant_id.to_string(),
        name: String::new(),
        monitor_type: monitor_type.to_string(),
        query_config: serde_json::to_string(query_config)?,
        critical: conditions.critical,
        critical_recovery: conditions.critical_recovery,
        warning: conditions.warning,
        warning_recovery: conditions.warning_recovery,
        comparator: conditions.comparator.clone(),
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

    let results = match monitor_type {
        "metric" => query_metric(ch, &temp_monitor, group_by).await?,
        "log" => query_log(ch, &temp_monitor, group_by).await?,
        "apm" => query_apm(ch, &temp_monitor, group_by).await?,
        _ => vec![],
    };

    let current_value = results.first().map(|(_, v)| *v);

    let (series, bucket_secs) =
        build_preview_series(ch, &temp_monitor, lookback_secs, group_by).await;
    let timeseries = series
        .first()
        .map(|item| item.points.clone())
        .unwrap_or_default();
    let simulated_events = simulate_preview_events(&series, &conditions);

    Ok(PreviewResult {
        current_value,
        groups: results,
        timeseries,
        series,
        simulated_events,
        bucket_secs,
    })
}

#[derive(Debug, Clone)]
pub struct PreviewConditions {
    pub critical: Option<f64>,
    pub critical_recovery: Option<f64>,
    pub warning: Option<f64>,
    pub warning_recovery: Option<f64>,
    pub comparator: String,
}

#[derive(Debug, serde::Serialize)]
pub struct PreviewResult {
    pub current_value: Option<f64>,
    pub groups: Vec<(String, f64)>,
    pub timeseries: Vec<TimeseriesPoint>,
    pub series: Vec<PreviewSeries>,
    pub simulated_events: Vec<SimulatedMonitorEvent>,
    pub bucket_secs: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TimeseriesPoint {
    pub timestamp: String,
    pub value: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PreviewSeries {
    pub group_key: String,
    pub points: Vec<TimeseriesPoint>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct SimulatedMonitorEvent {
    pub timestamp: String,
    pub group_key: String,
    pub previous_state: String,
    pub state: String,
    pub value: f64,
    pub threshold: Option<f64>,
}

const MAX_PREVIEW_POINTS_PER_SERIES: i64 = 720;
const MAX_PREVIEW_SERIES: usize = 50;
const MAX_SIMULATED_EVENTS: usize = 1_000;

fn preview_bucket_secs(eval_window_secs: i64, lookback_secs: i64) -> i64 {
    let capped_point_width =
        (lookback_secs + MAX_PREVIEW_POINTS_PER_SERIES - 1) / MAX_PREVIEW_POINTS_PER_SERIES;
    eval_window_secs.max(capped_point_width).max(60)
}

fn simulate_preview_events(
    series: &[PreviewSeries],
    conditions: &PreviewConditions,
) -> Vec<SimulatedMonitorEvent> {
    if conditions.critical.is_none() && conditions.warning.is_none() {
        return vec![];
    }

    let mut events = Vec::new();
    for item in series {
        let mut state = "ok";
        for point in &item.points {
            let next_state = evaluate_threshold(
                state,
                point.value,
                conditions.critical,
                conditions.critical_recovery,
                conditions.warning,
                conditions.warning_recovery,
                &conditions.comparator,
            );
            if next_state == state {
                continue;
            }

            let threshold = match next_state {
                "alert" => conditions.critical,
                "warn" => conditions.warning,
                "ok" if state == "alert" => conditions.critical_recovery.or(conditions.critical),
                "ok" => conditions.warning_recovery.or(conditions.warning),
                _ => None,
            };
            events.push(SimulatedMonitorEvent {
                timestamp: point.timestamp.clone(),
                group_key: item.group_key.clone(),
                previous_state: state.to_string(),
                state: next_state.to_string(),
                value: point.value,
                threshold,
            });
            state = next_state;
        }
    }
    events.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
    events.truncate(MAX_SIMULATED_EVENTS);
    events
}

/// Build historical evaluation buckets for the chart and alert backtest.
async fn build_preview_series(
    ch: &Client,
    monitor: &Monitor,
    lookback_secs: i64,
    group_by: &[String],
) -> (Vec<PreviewSeries>, i64) {
    let bucket_secs = preview_bucket_secs(monitor.eval_window_secs, lookback_secs);

    // Check if this is an expression-based metric (PromQL)
    if monitor.monitor_type == "metric" {
        let config_value: serde_json::Value = match serde_json::from_str(&monitor.query_config) {
            Ok(v) => v,
            Err(_) => return (vec![], bucket_secs),
        };
        let expr = config_value
            .get("expr")
            .or_else(|| config_value.get("expression"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !expr.trim().is_empty() {
            return (
                build_preview_series_promql(
                    ch,
                    expr,
                    &monitor.tenant_id,
                    lookback_secs,
                    bucket_secs,
                )
                .await,
                bucket_secs,
            );
        }
    }

    // Build a ClickHouse query that groups by time bucket
    let (table, agg_expr, extra_conditions) = match monitor.monitor_type.as_str() {
        "metric" => {
            let cfg: MetricQueryConfig = match serde_json::from_str(&monitor.query_config) {
                Ok(c) => c,
                Err(_) => return (vec![], bucket_secs),
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
            (
                "metrics_gauge".to_string(),
                agg.to_string(),
                conds.join(" AND "),
            )
        }
        "log" => {
            let cfg: LogQueryConfig = match serde_json::from_str(&monitor.query_config) {
                Ok(c) => c,
                Err(_) => return (vec![], bucket_secs),
            };
            let mut conds = vec![format!("tenant_id = '{}'", escape_ch(&monitor.tenant_id))];
            if !cfg.search.is_empty() {
                for term in cfg.search.split_whitespace() {
                    // lower(Body) LIKE matches the idx_body_text ngrams(4) index;
                    // hasToken would force a full Body scan in the preview path.
                    let escaped = escape_ch(&term.to_lowercase());
                    let inner = escaped.replace('%', "\\%").replace('_', "\\_");
                    conds.push(format!("lower(Body) LIKE '%{inner}%'"));
                }
            }
            if !cfg.service.is_empty() {
                conds.push(format!("ServiceName = '{}'", escape_ch(&cfg.service)));
            }
            if !cfg.severities.is_empty() {
                let severities = cfg
                    .severities
                    .iter()
                    .map(|severity| format!("'{}'", escape_ch(severity)))
                    .collect::<Vec<_>>()
                    .join(", ");
                conds.push(format!("SeverityText IN ({severities})"));
            }
            for f in &cfg.filters {
                let Some(field) = log_field_expr(&f.field) else {
                    tracing::warn!(
                        engine = "monitors",
                        field = %f.field,
                        "rejecting unsupported log filter field in preview"
                    );
                    return (vec![], bucket_secs);
                };
                let value = escape_ch(&f.value);
                match f.op.as_str() {
                    "=" => conds.push(format!("{field} = '{value}'")),
                    "!=" => conds.push(format!("{field} != '{value}'")),
                    "LIKE" | "like" => conds.push(format!("{field} LIKE '%{value}%'")),
                    _ => {
                        tracing::warn!(
                            engine = "monitors",
                            operator = %f.op,
                            "rejecting unsupported log filter operator in preview"
                        );
                        return (vec![], bucket_secs);
                    }
                }
            }
            (
                "logs".to_string(),
                "count()".to_string(),
                conds.join(" AND "),
            )
        }
        "apm" => {
            let cfg: ApmQueryConfig = match serde_json::from_str(&monitor.query_config) {
                Ok(c) => c,
                Err(_) => return (vec![], bucket_secs),
            };
            let mut conds = vec![format!("tenant_id = '{}'", escape_ch(&monitor.tenant_id))];
            if let Some(condition) = apm_match_condition("service_name", &cfg.service) {
                conds.push(condition);
            }
            if let Some(ref ep) = cfg.endpoint_filter {
                if let Some(condition) = apm_match_condition("http_path", ep) {
                    conds.push(condition);
                }
            }
            let agg = apm_aggregation(&cfg.metric, bucket_secs);
            ("spans".to_string(), agg, conds.join(" AND "))
        }
        _ => return (vec![], bucket_secs),
    };

    let time_col = match table.as_str() {
        "metrics_gauge" => "TimeUnix",
        "logs" => "Timestamp",
        _ => "timestamp",
    };

    let group_expr = match monitor.monitor_type.as_str() {
        "metric" => {
            if group_by.iter().any(|field| !is_valid_metric_label(field)) {
                return (vec![], bucket_secs);
            }
            let expressions = group_by
                .iter()
                .map(|field| format!("toString(ResourceAttributes['{}'])", escape_ch(field)))
                .collect::<Vec<_>>();
            if expressions.is_empty() {
                "''".to_string()
            } else {
                expressions.join(" || ':' || ")
            }
        }
        "log" => match group_expression(group_by, log_field_expr) {
            Ok(expression) => expression,
            Err(_) => return (vec![], bucket_secs),
        },
        "apm" => match group_expression(group_by, apm_group_expr) {
            Ok(expression) => expression,
            Err(_) => return (vec![], bucket_secs),
        },
        _ => "''".to_string(),
    };

    let sql = format!(
        "SELECT toString(toStartOfInterval({time_col}, INTERVAL {bucket_secs} SECOND)) AS ts, \
         ({group_expr}) AS group_key, {agg_expr} AS value \
         FROM {table} \
         WHERE {extra_conditions} AND {time_col} >= now() - INTERVAL {lookback_secs} SECOND \
         GROUP BY ts, group_key ORDER BY ts, group_key LIMIT 10000"
    );

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct TsRow {
        ts: String,
        group_key: String,
        value: f64,
    }

    match crate::tenant_query(ch, &sql, &monitor.tenant_id)
        .with_option("max_execution_time", "30")
        .fetch_all::<TsRow>()
        .await
    {
        Ok(rows) => {
            let mut grouped = BTreeMap::<String, Vec<TimeseriesPoint>>::new();
            for row in rows {
                if !grouped.contains_key(&row.group_key) && grouped.len() >= MAX_PREVIEW_SERIES {
                    continue;
                }
                grouped
                    .entry(row.group_key)
                    .or_default()
                    .push(TimeseriesPoint {
                        timestamp: row.ts,
                        value: row.value,
                    });
            }
            (
                grouped
                    .into_iter()
                    .map(|(group_key, points)| PreviewSeries { group_key, points })
                    .collect(),
                bucket_secs,
            )
        }
        Err(e) => {
            tracing::debug!(engine = "monitors", error = %e, "preview timeseries query failed");
            (vec![], bucket_secs)
        }
    }
}

/// Build preview series using the PromQL range query evaluator.
async fn build_preview_series_promql(
    ch: &Client,
    expr: &str,
    tenant_id: &str,
    lookback_secs: i64,
    bucket_secs: i64,
) -> Vec<PreviewSeries> {
    let now = chrono::Utc::now().timestamp() as f64;
    let start = now - lookback_secs as f64;
    let step = bucket_secs as f64;

    match promql::evaluate_range_query(ch, expr, start, now, step, tenant_id).await {
        Ok(series) => series
            .into_iter()
            .take(MAX_PREVIEW_SERIES)
            .map(|item| {
                let group_key = item
                    .labels
                    .iter()
                    .filter(|(key, _)| key.as_str() != "__name__")
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let points = item
                    .samples
                    .into_iter()
                    .map(|(timestamp, value)| {
                        let datetime = chrono::DateTime::from_timestamp(timestamp as i64, 0)
                            .unwrap_or_default();
                        TimeseriesPoint {
                            timestamp: datetime.format("%Y-%m-%d %H:%M:%S").to_string(),
                            value,
                        }
                    })
                    .collect();
                PreviewSeries { group_key, points }
            })
            .collect(),
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
            evaluate_threshold(
                "alert",
                35.0,
                Some(50.0),
                Some(40.0),
                Some(30.0),
                None,
                "above"
            ),
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
    fn test_evaluate_threshold_strict_inclusive_and_equal_boundaries() {
        assert_eq!(
            evaluate_threshold("ok", 1.0, Some(1.0), None, None, None, "above"),
            "ok"
        );
        assert_eq!(
            evaluate_threshold("ok", 1.0, Some(1.0), None, None, None, "above_or_equal"),
            "alert"
        );
        assert_eq!(
            evaluate_threshold("ok", 1.0, Some(1.0), None, None, None, "below"),
            "ok"
        );
        assert_eq!(
            evaluate_threshold("ok", 1.0, Some(1.0), None, None, None, "below_or_equal"),
            "alert"
        );
        assert_eq!(
            evaluate_threshold("ok", 0.0, Some(0.0), None, None, None, "equal"),
            "alert"
        );
        assert_eq!(
            evaluate_threshold("ok", 1.0, Some(0.0), None, None, None, "equal"),
            "ok"
        );
        assert_eq!(
            evaluate_threshold("alert", 1.0, Some(0.0), None, None, None, "equal"),
            "ok"
        );
        assert_eq!(
            evaluate_threshold("alert", 1.0, Some(0.0), Some(1.0), None, None, "equal"),
            "ok"
        );
        assert_eq!(
            evaluate_threshold("alert", 2.0, Some(0.0), Some(1.0), None, None, "equal"),
            "alert"
        );
    }

    #[test]
    fn preview_backtest_records_alert_warning_and_recovery_transitions() {
        let series = vec![PreviewSeries {
            group_key: "gateway:/checkout".to_string(),
            points: vec![
                TimeseriesPoint {
                    timestamp: "2026-09-04 10:00:00".to_string(),
                    value: 100.0,
                },
                TimeseriesPoint {
                    timestamp: "2026-09-04 10:05:00".to_string(),
                    value: 350.0,
                },
                TimeseriesPoint {
                    timestamp: "2026-09-04 10:10:00".to_string(),
                    value: 550.0,
                },
                TimeseriesPoint {
                    timestamp: "2026-09-04 10:15:00".to_string(),
                    value: 350.0,
                },
                TimeseriesPoint {
                    timestamp: "2026-09-04 10:20:00".to_string(),
                    value: 150.0,
                },
            ],
        }];
        let conditions = PreviewConditions {
            critical: Some(500.0),
            critical_recovery: Some(400.0),
            warning: Some(300.0),
            warning_recovery: Some(200.0),
            comparator: "above".to_string(),
        };

        let events = simulate_preview_events(&series, &conditions);

        assert_eq!(events.len(), 4);
        assert_eq!(
            (events[0].previous_state.as_str(), events[0].state.as_str()),
            ("warn", "ok")
        );
        assert_eq!(
            (events[1].previous_state.as_str(), events[1].state.as_str()),
            ("alert", "warn")
        );
        assert_eq!(
            (events[2].previous_state.as_str(), events[2].state.as_str()),
            ("warn", "alert")
        );
        assert_eq!(
            (events[3].previous_state.as_str(), events[3].state.as_str()),
            ("ok", "warn")
        );
        assert_eq!(events[0].threshold, Some(200.0));
        assert_eq!(events[0].group_key, "gateway:/checkout");
        assert!(events[0].timestamp > events[1].timestamp);
    }

    #[test]
    fn preview_backtest_supports_below_thresholds() {
        let series = vec![PreviewSeries {
            group_key: String::new(),
            points: vec![
                TimeseriesPoint {
                    timestamp: "2026-09-04 10:00:00".to_string(),
                    value: 99.0,
                },
                TimeseriesPoint {
                    timestamp: "2026-09-04 10:05:00".to_string(),
                    value: 10.0,
                },
                TimeseriesPoint {
                    timestamp: "2026-09-04 10:10:00".to_string(),
                    value: 30.0,
                },
            ],
        }];
        let conditions = PreviewConditions {
            critical: Some(20.0),
            critical_recovery: Some(25.0),
            warning: None,
            warning_recovery: None,
            comparator: "below".to_string(),
        };

        let events = simulate_preview_events(&series, &conditions);

        assert_eq!(
            events
                .iter()
                .map(|event| event.state.as_str())
                .collect::<Vec<_>>(),
            vec!["ok", "alert"]
        );
    }

    #[test]
    fn preview_buckets_follow_the_evaluation_window_and_cap_long_ranges() {
        assert_eq!(preview_bucket_secs(300, 43_200), 300);
        assert_eq!(preview_bucket_secs(60, 604_800), 840);
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
        assert_eq!(
            worst_state(["warn", "alert", "ok"].iter().copied()),
            "alert"
        );
        assert_eq!(worst_state(["ok", "no_data"].iter().copied()), "no_data");
    }

    #[test]
    fn monitor_log_fields_resolve_to_fixed_expressions() {
        assert_eq!(log_field_expr("service_name"), Some("ServiceName"));
        assert_eq!(log_field_expr("SeverityText"), Some("SeverityText"));
        assert_eq!(log_field_expr("trace_id"), Some("TraceId"));
        assert_eq!(log_field_expr("mat_source_ip"), Some("mat_source_ip"));
        assert_eq!(log_field_expr("mat_action"), Some("mat_action"));
        assert_eq!(log_field_expr("Body) OR 1 = 1 --"), None);

        let groups = vec!["ServiceName".to_string(), "SeverityText".to_string()];
        assert_eq!(
            group_expression(&groups, log_field_expr).unwrap(),
            "toString(ServiceName) || ':' || toString(SeverityText)"
        );
    }

    #[test]
    fn monitor_apm_groups_use_an_allowlist() {
        assert_eq!(apm_group_expr("endpoint"), Some("http_path"));
        assert_eq!(apm_group_expr("http_status_code"), Some("http_status_code"));
        assert_eq!(apm_group_expr("tenant_id"), None);
        assert_eq!(apm_group_expr("http_path, sleep(10)"), None);
    }

    #[test]
    fn apm_error_aggregations_use_the_typed_span_fields() {
        let rate = apm_aggregation("error_rate", 300);
        let count = apm_aggregation("error_count", 300);

        for expression in [&rate, &count] {
            assert!(expression.contains("http_status_code >= 500"));
            assert!(expression.contains("STATUS_CODE_ERROR"));
            assert!(!expression.contains("JSONExtract"));
        }
        assert!(rate.contains("* 100.0 / count()"));
    }

    #[test]
    fn apm_wildcards_build_safe_match_conditions() {
        assert_eq!(apm_match_condition("service_name", "*"), None);
        assert_eq!(
            apm_match_condition("service_name", "checkout-*"),
            Some("service_name LIKE 'checkout-%'".to_string())
        );
        assert_eq!(
            apm_match_condition("service_name", "api_gateway"),
            Some("service_name = 'api_gateway'".to_string())
        );
    }

    #[test]
    fn monitor_templates_include_group_values() {
        let fields = vec!["service_name".to_string(), "endpoint".to_string()];
        let rendered = render_monitor_template(
            "{{service}} {{endpoint}} is {{state}} at {{value}} over {{threshold}}",
            &fields,
            "checkout:/api/orders",
            "alert",
            512.25,
            500.0,
        );

        assert_eq!(rendered, "checkout /api/orders is alert at 512.25 over 500");
    }

    #[test]
    fn monitor_query_field_validation_rejects_sql_expressions() {
        let log_config = serde_json::json!({
            "search": "timeout",
            "filters": [{
                "field": "Body) OR 1 = 1 --",
                "op": "=",
                "value": "ignored"
            }]
        });
        assert!(
            validate_query_fields("log", &log_config, &[])
                .unwrap_err()
                .contains("unsupported log filter field")
        );

        let apm_config = serde_json::json!({
            "service": "gateway",
            "metric": "error_rate"
        });
        assert!(
            validate_query_fields(
                "apm",
                &apm_config,
                &["http_path) UNION ALL SELECT tenant_id".to_string()]
            )
            .unwrap_err()
            .contains("unsupported apm group_by field")
        );
    }

    #[test]
    fn monitor_query_field_validation_accepts_supported_ui_fields() {
        let log_config = serde_json::json!({
            "service": "gateway",
            "severities": ["ERROR"],
            "filters": [{"field": "TraceId", "op": "!=", "value": ""}]
        });
        assert!(
            validate_query_fields(
                "log",
                &log_config,
                &["ServiceName".to_string(), "SeverityText".to_string()]
            )
            .is_ok()
        );

        let apm_config = serde_json::json!({
            "service": "gateway",
            "metric": "p95_latency"
        });
        assert!(
            validate_query_fields(
                "apm",
                &apm_config,
                &["endpoint".to_string(), "http_method".to_string()]
            )
            .is_ok()
        );
    }
}
