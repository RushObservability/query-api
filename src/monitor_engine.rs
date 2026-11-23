use crate::engine_metrics::{EvaluationObservation, EvaluationOutcome};
use crate::monitor_forecast;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use clickhouse::Client;

use crate::alert_engine;
use crate::clickhouse_config::ConfigDb;
use crate::models::alert::AlertRoute;
use crate::models::monitor::{
    ApmQueryConfig, LogQueryConfig, MetricQueryConfig, Monitor, MonitorEvent, MonitorEvidence,
    MonitorEvidenceQuery, MonitorExploreScope,
};
use crate::models::query::{Filter, FilterOp};
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
    lease: Arc<crate::leader::EngineLease>,
) {
    tokio::spawn(async move {
        let http_client = reqwest::Client::new();
        let smtp_transport = build_smtp_transport(&smtp_config);
        let mut eval_state = crate::eval_state::EvalState::new(EVAL_FLUSH_EVERY);

        loop {
            if !lease.is_leader() {
                tokio::time::sleep(crate::leader::FOLLOWER_POLL).await;
                continue;
            }
            let start = Instant::now();
            let mut ok = true;
            let cycle = run_evaluation_cycle(
                &ch,
                &config_db,
                &http_client,
                &smtp_config,
                &smtp_transport,
                &mut eval_state,
            );
            let (evaluated, state_changes) = match crate::leader::fenced(lease.clone(), cycle).await
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

/// What to do about notifications for one monitor group this cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Notify {
    /// Nothing to send or record.
    No,
    /// The state differs from the last notification: send it.
    Change,
    /// Still in the same problem state past the renotify interval: send again.
    Renotify,
    /// No notification on record yet: record the current state without sending,
    /// so renotify timing and maintenance catch-up have a starting point.
    Baseline,
}

/// Decide whether a group notifies. `last` is the (state, epoch ms) of its last
/// notification, if any.
fn decide_notification(
    prev_state: &str,
    new_state: &str,
    last: Option<(&str, i64)>,
    now_ms: i64,
    silenced: bool,
    renotify_secs: Option<i64>,
    notifies: impl Fn(&str) -> bool,
) -> Notify {
    let baseline = if last.is_none() {
        Notify::Baseline
    } else {
        Notify::No
    };
    let (last_state, last_at) = last.unwrap_or((prev_state, now_ms));
    if silenced {
        return baseline;
    }
    if new_state != last_state {
        return if notifies(new_state) {
            Notify::Change
        } else {
            baseline
        };
    }
    let problem = matches!(new_state, "alert" | "warn" | "no_data");
    let due = renotify_secs
        .filter(|secs| *secs > 0)
        .is_some_and(|secs| now_ms.saturating_sub(last_at) >= secs.saturating_mul(1_000));
    if problem && notifies(new_state) && due {
        Notify::Renotify
    } else {
        baseline
    }
}

/// (tenant, monitor, group) → (state, notified at ms).
type LastNotifications = HashMap<(String, String, String), (String, i64)>;

/// Maintenance windows and last notifications, loaded once per cycle and
/// shared by every monitor evaluated in it.
struct NotifyPolicy {
    now_ms: i64,
    /// Active windows, with their parsed scope.
    windows: Vec<(
        crate::models::maintenance::MaintenanceWindow,
        crate::models::maintenance::Scope,
    )>,
    /// `None` when it couldn't be loaded: then only state changes notify, as before.
    last: Option<LastNotifications>,
}

impl NotifyPolicy {
    async fn load(config_db: &ConfigDb, now: chrono::DateTime<chrono::Utc>) -> Self {
        let now_text = crate::models::maintenance::format_time(now);
        let windows = match config_db
            .list_unfinished_maintenance_windows(&now_text)
            .await
        {
            Ok(windows) => windows
                .into_iter()
                .filter(|window| window.is_active(now))
                .filter_map(|window| {
                    let scope = crate::models::maintenance::Scope::parse(&window.scope).ok()?;
                    Some((window, scope))
                })
                .collect(),
            Err(error) => {
                // Fail open: an unreadable window table must not silence alerts.
                tracing::warn!(engine = "monitors", %error, "failed to load maintenance windows; notifying normally");
                Vec::new()
            }
        };
        let last = match config_db.list_monitor_notifications().await {
            Ok(rows) => Some(
                rows.into_iter()
                    .map(|row| {
                        (
                            (row.tenant_id, row.monitor_id, row.group_key),
                            (row.state, row.notified_at_ms),
                        )
                    })
                    .collect(),
            ),
            Err(error) => {
                tracing::warn!(engine = "monitors", %error, "failed to load monitor notification state; renotify skipped this cycle");
                None
            }
        };
        Self {
            now_ms: now.timestamp_millis(),
            windows,
            last,
        }
    }

    /// Name of the active window silencing this monitor, if any.
    fn silencing_window(&self, monitor: &Monitor) -> Option<&str> {
        if self.windows.is_empty() {
            return None;
        }
        let tags = monitor_tag_map(&monitor.tags);
        self.windows
            .iter()
            .find(|(window, scope)| {
                window.tenant_id == monitor.tenant_id && scope.covers(&monitor.id, &tags)
            })
            .map(|(window, _)| window.name.as_str())
    }

    fn decide(
        &self,
        monitor: &Monitor,
        group_key: &str,
        prev_state: &str,
        new_state: &str,
    ) -> Notify {
        let notifies = |state: &str| state != "no_data" || monitor.no_data_action == "notify";
        let silenced = self.silencing_window(monitor).is_some();
        let Some(last) = &self.last else {
            // Without notification state, behave as before: notify on change.
            return if !silenced && new_state != prev_state && notifies(new_state) {
                Notify::Change
            } else {
                Notify::No
            };
        };
        let key = (
            monitor.tenant_id.clone(),
            monitor.id.clone(),
            group_key.to_string(),
        );
        decide_notification(
            prev_state,
            new_state,
            last.get(&key).map(|(state, at)| (state.as_str(), *at)),
            self.now_ms,
            silenced,
            monitor.renotify_interval,
            notifies,
        )
    }

    /// Record what was sent, or the baseline. Only called when state is tracked.
    async fn record(
        &self,
        config_db: &ConfigDb,
        monitor: &Monitor,
        group_key: &str,
        decision: Notify,
        prev_state: &str,
        new_state: &str,
    ) {
        if self.last.is_none() || decision == Notify::No {
            return;
        }
        let state = if decision == Notify::Baseline {
            prev_state
        } else {
            new_state
        };
        let record = crate::models::maintenance::MonitorNotification {
            tenant_id: monitor.tenant_id.clone(),
            monitor_id: monitor.id.clone(),
            group_key: group_key.to_string(),
            state: state.to_string(),
            notified_at_ms: self.now_ms,
        };
        if let Err(error) = config_db.record_monitor_notification(&record).await {
            tracing::warn!(engine = "monitors", monitor_id = %monitor.id, %error, "failed to record monitor notification");
        }
    }
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
    let policy = NotifyPolicy::load(config_db, now).await;
    let policy_ref = &policy;

    // States of every enabled monitor as fetched this cycle — composite monitors
    // resolve their members from this map instead of one SELECT…FINAL per member.
    // (Members may be evaluated concurrently this same cycle; composites see the
    // cycle-start states, which matches the prior racy read-at-eval behavior to
    // within one cycle.)
    let monitor_states: HashMap<String, (String, String)> = monitors
        .iter()
        .map(|m| (m.id.clone(), (m.tenant_id.clone(), m.state.clone())))
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

    crate::engine_metrics::due(config_db.self_metrics(), "monitor_engine", jobs.len());
    let outcomes: Vec<(String, u64, bool, bool)> =
        futures_util::stream::iter(jobs.into_iter().map(|(monitor, should_flush)| async move {
            let mut observation =
                EvaluationObservation::new(config_db.self_metrics(), "monitor_engine");
            let window = EvaluationWindow::new(
                chrono::Utc::now().timestamp_millis(),
                monitor.eval_window_secs,
            );
            let result = match crate::query_governor::run_background(
                &monitor.tenant_id,
                evaluate_monitor(
                    ch,
                    config_db,
                    &monitor,
                    now_str_ref,
                    window,
                    http_client,
                    smtp_config,
                    smtp_transport,
                    monitor_states_ref,
                    should_flush,
                    policy_ref,
                ),
            )
            .await
            {
                Ok(result) => {
                    observation.finish(if result.is_ok() {
                        EvaluationOutcome::Completed
                    } else {
                        EvaluationOutcome::Error
                    });
                    result
                }
                Err(error) => {
                    observation.finish(EvaluationOutcome::AdmissionRejected);
                    Err(anyhow::anyhow!("background admission rejected: {error:?}"))
                }
            };
            let failed = result.is_err();
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
                        window,
                        http_client,
                        smtp_config,
                        smtp_transport,
                        should_flush,
                        policy_ref,
                    )
                    .await
                }
            };
            (monitor.id, changes, persisted, failed)
        }))
        .buffer_unordered(ENGINE_CONCURRENCY)
        .collect()
        .await;

    let mut state_changes: u64 = 0;
    let mut failures = 0;
    for (id, changes, persisted, failed) in outcomes {
        failures += usize::from(failed);
        state_changes += changes;
        eval_state.record(id, now, persisted);
    }

    anyhow::ensure!(failures == 0, "{failures} monitor evaluation job(s) failed");
    Ok((evaluated, state_changes))
}

/// Evaluate a single monitor. Returns (state_changes, persisted_to_db).
#[allow(clippy::too_many_arguments)]
async fn evaluate_monitor(
    ch: &Client,
    config_db: &ConfigDb,
    monitor: &Monitor,
    now_str: &str,
    window: EvaluationWindow,
    http_client: &reqwest::Client,
    smtp_config: &alert_engine::SmtpConfig,
    smtp_transport: &Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>,
    monitor_states: &HashMap<String, (String, String)>,
    should_flush: bool,
    policy: &NotifyPolicy,
) -> anyhow::Result<(u64, bool)> {
    let group_by: Vec<String> = serde_json::from_str(&monitor.group_by).unwrap_or_default();
    let has_groups = !group_by.is_empty();
    if monitor.monitor_type == "composite" {
        return evaluate_composite(
            config_db,
            monitor,
            now_str,
            http_client,
            smtp_config,
            smtp_transport,
            monitor_states,
            should_flush,
            policy,
        )
        .await;
    }
    let query = parse_monitor_query(monitor)?;
    let results = run_monitor_query(ch, monitor, &query, &group_by, window).await?;

    if results.is_empty() {
        // No data returned
        return Ok(handle_no_data(
            config_db,
            monitor,
            now_str,
            window,
            http_client,
            smtp_config,
            smtp_transport,
            should_flush,
            policy,
        )
        .await);
    }

    // Evaluate thresholds for each group result
    let mut group_states: HashMap<String, String> =
        serde_json::from_str(&monitor.group_states).unwrap_or_default();
    let mut changes: u64 = 0;
    let mut triggered = false;

    for MonitorReading {
        group_key,
        value,
        series,
        crossing_at,
    } in &results
    {
        let current_state = group_states
            .get(group_key)
            .map(|s| s.as_str())
            .unwrap_or(&monitor.state)
            .to_string();
        let current_state = current_state.as_str();

        let new_state = evaluate_threshold(
            current_state,
            *value,
            monitor.critical,
            monitor.critical_recovery,
            monitor.warning,
            monitor.warning_recovery,
            &monitor.comparator,
        );
        let decision = policy.decide(monitor, group_key, current_state, new_state);
        let changed = new_state != current_state;
        let sends = matches!(decision, Notify::Change | Notify::Renotify);

        if changed || sends {
            let threshold = match new_state {
                "alert" => monitor.critical,
                "warn" => monitor.warning,
                _ if current_state == "warn" => monitor.warning_recovery.or(monitor.warning),
                _ if current_state == "alert" => monitor.critical_recovery.or(monitor.critical),
                _ => None,
            };

            let alert_name = render_monitor_template(
                &monitor.name,
                &group_by,
                group_key,
                new_state,
                *value,
                threshold.unwrap_or(0.0),
            );

            let group_label = if group_key.is_empty() {
                String::new()
            } else {
                format!(" [{group_key}]")
            };
            let reading = match &query {
                MonitorEvidenceQuery::Forecast { .. } => match crossing_at {
                    Some(at) => format!("(forecast peak {value:.1}%; threshold crossing at {at})"),
                    None => format!("(forecast peak {value:.1}%)"),
                },
                _ => format!(
                    "(value={:.4}, threshold={:.4})",
                    value,
                    threshold.unwrap_or(0.0)
                ),
            };
            let event_msg = format!(
                "Monitor '{alert_name}'{group_label}: {current_state} -> {new_state} {reading}"
            );

            if changed {
                changes += 1;
                let event_msg = match policy.silencing_window(monitor) {
                    Some(window) => format!(
                        "{event_msg}; notifications silenced by maintenance window '{window}'"
                    ),
                    None => event_msg.clone(),
                };
                let evidence_query = match &query {
                    MonitorEvidenceQuery::Promql { expression, .. } => {
                        MonitorEvidenceQuery::Promql {
                            expression: expression.clone(),
                            series: series.clone(),
                        }
                    }
                    MonitorEvidenceQuery::Forecast { config, .. } => {
                        MonitorEvidenceQuery::Forecast {
                            config: config.clone(),
                            crossing_at: crossing_at.clone(),
                        }
                    }
                    _ => query.clone(),
                };
                record_transition(
                    config_db,
                    monitor,
                    group_key,
                    current_state,
                    new_state,
                    Some(*value),
                    threshold,
                    event_msg,
                    Some(window.evidence(monitor, group_by.clone(), evidence_query)),
                )
                .await;
                triggered |= matches!(new_state, "alert" | "warn");
            }

            if sends {
                let renotify = decision == Notify::Renotify;
                let message = if !monitor.message.trim().is_empty() {
                    let rendered = render_monitor_template(
                        &monitor.message,
                        &group_by,
                        group_key,
                        new_state,
                        *value,
                        threshold.unwrap_or(0.0),
                    );
                    if renotify {
                        format!("[Renotify] {rendered}")
                    } else {
                        rendered
                    }
                } else if changed {
                    event_msg.clone()
                } else if renotify {
                    format!(
                        "[Renotify] Monitor '{alert_name}'{group_label}: still {new_state} {reading}"
                    )
                } else {
                    // Catching up, e.g. after a maintenance window: report where it is now.
                    format!("Monitor '{alert_name}'{group_label}: {new_state} {reading}")
                };
                fire_notifications(
                    config_db,
                    monitor,
                    group_key,
                    &alert_name,
                    &message,
                    new_state,
                    *value,
                    threshold.unwrap_or(0.0),
                    http_client,
                    smtp_config,
                    smtp_transport,
                )
                .await;
            }

            group_states.insert(group_key.clone(), new_state.to_string());
        }
        policy
            .record(
                config_db,
                monitor,
                group_key,
                decision,
                current_state,
                new_state,
            )
            .await;
    }

    // Determine overall monitor state (worst across groups)
    let overall = if has_groups {
        worst_state(group_states.values().map(|s| s.as_str()))
    } else {
        results
            .first()
            .map(|row| {
                group_states
                    .get(&row.group_key)
                    .map(|s| s.as_str())
                    .unwrap_or("ok")
            })
            .unwrap_or("ok")
    };

    // Persist only on a real transition (a group changed or the overall state
    // moved) — that path is identical to before. Otherwise just flush
    // last_eval_at on the coarse cadence from the row we already hold.
    if changes > 0 || overall != monitor.state.as_str() {
        let group_states_json =
            serde_json::to_string(&group_states).unwrap_or_else(|_| "{}".to_string());
        config_db
            .update_monitor_state(monitor, overall, &group_states_json, now_str, triggered)
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
#[allow(clippy::too_many_arguments)]
async fn handle_no_data(
    config_db: &ConfigDb,
    monitor: &Monitor,
    now_str: &str,
    window: EvaluationWindow,
    http_client: &reqwest::Client,
    smtp_config: &alert_engine::SmtpConfig,
    smtp_transport: &Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>,
    should_flush: bool,
    policy: &NotifyPolicy,
) -> (u64, bool) {
    let old_state = monitor.state.as_str();
    let action = monitor.no_data_action.as_str();

    let new_state = match action {
        "notify" => "no_data",
        "resolve" => "ok",
        _ => "no_data", // "show" also sets no_data, but does not notify
    };
    let decision = policy.decide(monitor, "", old_state, new_state);
    let changed = new_state != old_state;
    let event_msg = format!(
        "Monitor '{}': {} -> {} (no data received)",
        monitor.name, old_state, new_state,
    );

    if changed {
        let event_msg = match policy.silencing_window(monitor) {
            Some(window) => {
                format!("{event_msg}; notifications silenced by maintenance window '{window}'")
            }
            None => event_msg.clone(),
        };
        let evidence = parse_monitor_query(monitor)
            .ok()
            .map(|query| window.evidence(monitor, Vec::new(), query));
        record_transition(
            config_db, monitor, "", old_state, new_state, None, None, event_msg, evidence,
        )
        .await;
    }

    if matches!(decision, Notify::Change | Notify::Renotify) {
        let message = if decision == Notify::Renotify {
            format!("[Renotify] Monitor '{}' still has no data", monitor.name)
        } else {
            event_msg.clone()
        };
        fire_notifications(
            config_db,
            monitor,
            "",
            &monitor.name,
            &message,
            new_state,
            0.0,
            0.0,
            http_client,
            smtp_config,
            smtp_transport,
        )
        .await;
    }
    policy
        .record(config_db, monitor, "", decision, old_state, new_state)
        .await;

    if changed {
        // A forecast has one series. Its old reading must not mask the
        // no-data state when samples return and the engine records recovery.
        let group_states = if monitor.monitor_type == "forecast" {
            "{}"
        } else {
            &monitor.group_states
        };
        let _ = config_db
            .update_monitor_state(monitor, new_state, group_states, now_str, false)
            .await;
        (1, true)
    } else if should_flush {
        let _ = config_db.persist_monitor_eval(monitor, now_str).await;
        (0, true)
    } else {
        (0, false)
    }
}

fn monitor_tag_map(tags_json: &str) -> BTreeMap<String, String> {
    let tags: Vec<String> = serde_json::from_str(tags_json).unwrap_or_default();
    tags.into_iter()
        .filter_map(|tag| {
            let pair = tag.split_once(':').or_else(|| tag.split_once('='))?;
            let key = pair.0.trim();
            let value = pair.1.trim();
            (!key.is_empty() && !value.is_empty()).then(|| (key.to_string(), value.to_string()))
        })
        .collect()
}

fn alert_route_matches(
    priority: Option<i64>,
    tags: &BTreeMap<String, String>,
    route: &AlertRoute,
) -> bool {
    if !route.enabled {
        return false;
    }
    if !route.priorities.is_empty()
        && !priority.is_some_and(|priority| route.priorities.contains(&priority))
    {
        return false;
    }
    route
        .tag_matchers
        .iter()
        .all(|(key, value)| tags.get(key) == Some(value))
}

fn resolve_notification_channel_ids(
    direct_channels_json: &str,
    priority: Option<i64>,
    tags_json: &str,
    routes: &[AlertRoute],
) -> Vec<String> {
    let direct: Vec<String> = serde_json::from_str(direct_channels_json).unwrap_or_default();
    let tags = monitor_tag_map(tags_json);
    let mut seen = HashSet::new();
    direct
        .into_iter()
        .chain(
            routes
                .iter()
                .filter(|route| alert_route_matches(priority, &tags, route))
                .flat_map(|route| route.channel_ids.iter().cloned()),
        )
        .filter(|channel_id| seen.insert(channel_id.clone()))
        .collect()
}

/// Fire notifications to direct channels and every matching alert route.
async fn fire_notifications(
    config_db: &ConfigDb,
    monitor: &Monitor,
    group_key: &str,
    alert_name: &str,
    message: &str,
    alert_state: &str,
    value: f64,
    threshold: f64,
    http_client: &reqwest::Client,
    smtp_config: &alert_engine::SmtpConfig,
    smtp_transport: &Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>,
) {
    let routes = match config_db.list_alert_routes(&monitor.tenant_id).await {
        Ok(routes) => routes,
        Err(error) => {
            tracing::warn!(
                engine = "monitors",
                monitor_id = %monitor.id,
                error = %error,
                "failed to load alert routes; sending to direct channels only"
            );
            Vec::new()
        }
    };
    let channel_ids = resolve_notification_channel_ids(
        &monitor.notification_channels,
        monitor.priority,
        &monitor.tags,
        &routes,
    );

    alert_engine::notify_channels(
        config_db,
        &monitor.tenant_id,
        &channel_ids,
        &alert_engine::ChannelNotification {
            source: "monitor",
            signal_type: "monitors",
            alert_name,
            message,
            alert_state,
            value,
            threshold,
            condition_op: &monitor.comparator,
            description: "",
            alert_id: &monitor.id,
            // A recovery for one monitor group must not resolve another group's alert.
            incident_key: &alert_engine::rootly::monitor_alert_id(&monitor.id, group_key),
            runbook_url: "",
            webhook_fields: None,
        },
        http_client,
        smtp_config,
        smtp_transport,
    )
    .await;
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
    monitor_states: &HashMap<String, (String, String)>,
    should_flush: bool,
    policy: &NotifyPolicy,
) -> anyhow::Result<(u64, bool)> {
    let monitor_ids: Vec<String> =
        serde_json::from_str(&monitor.composite_monitor_ids).unwrap_or_default();
    let formula = &monitor.composite_formula;

    if monitor_ids.is_empty() || formula.is_empty() {
        if monitor.state != "no_data" {
            let _ = config_db
                .update_monitor_state(monitor, "no_data", "{}", now_str, false)
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
            Some((tenant, state)) => {
                tenant == &monitor.tenant_id && (state == "alert" || state == "warn")
            }
            None => match config_db.get_monitor(mid, &monitor.tenant_id).await {
                Ok(Some(m)) => m.state == "alert" || m.state == "warn",
                _ => false,
            },
        };
        letter_states.insert(letter, is_alerting);
    }

    // Evaluate the boolean formula (simple parser for A && B && !C patterns)
    let composite_result = eval_boolean_formula(formula, &letter_states);
    let new_state = if composite_result { "alert" } else { "ok" };
    let old_state = monitor.state.as_str();
    let decision = policy.decide(monitor, "", old_state, new_state);
    let event_msg = format!(
        "Composite monitor '{}': {} -> {} (formula: {})",
        monitor.name, old_state, new_state, formula,
    );

    let mut changes: u64 = 0;
    if new_state != old_state {
        changes = 1;
        let event_msg = match policy.silencing_window(monitor) {
            Some(window) => {
                format!("{event_msg}; notifications silenced by maintenance window '{window}'")
            }
            None => event_msg.clone(),
        };
        let evidence = MonitorEvidence {
            rule_version: monitor.rule_version,
            rule_name: monitor.name.clone(),
            comparator: monitor.comparator.clone(),
            window_start: None,
            evaluated_at: now_str.to_string(),
            group_by: Vec::new(),
            query: MonitorEvidenceQuery::Composite {
                formula: formula.clone(),
                members: letter_states.into_iter().collect(),
            },
        };
        record_transition(
            config_db,
            monitor,
            "",
            old_state,
            new_state,
            None,
            None,
            event_msg,
            Some(evidence),
        )
        .await;
    }

    if matches!(decision, Notify::Change | Notify::Renotify) {
        let message = if decision == Notify::Renotify {
            format!(
                "[Renotify] Composite monitor '{}' is still alerting (formula: {formula})",
                monitor.name
            )
        } else {
            event_msg.clone()
        };
        fire_notifications(
            config_db,
            monitor,
            "",
            &monitor.name,
            &message,
            new_state,
            0.0,
            0.0,
            http_client,
            smtp_config,
            smtp_transport,
        )
        .await;
    }
    policy
        .record(config_db, monitor, "", decision, old_state, new_state)
        .await;

    if changes > 0 {
        // Transition persists immediately, exactly as before.
        let _ = config_db
            .update_monitor_state(monitor, new_state, "{}", now_str, new_state == "alert")
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

/// Escape a string value for safe use in a ClickHouse SQL literal (delegates to the
/// shared escaper, which also encodes `?` so the clickhouse crate can't mistake it
/// for a bind placeholder).
fn escape_ch(s: &str) -> String {
    crate::query_builder::escape_string_literal(s)
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
        "forecast" => {
            if !group_by.is_empty() {
                return Err("Capacity forecasts require a single resource series.".into());
            }
            serde_json::from_value::<monitor_forecast::ForecastConfig>(config.clone())
                .map_err(|error| format!("invalid forecast query_config: {error}"))?
                .validate()?;
        }
        "composite" => {}
        _ => return Err(format!("invalid monitor type: {monitor_type}")),
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct EvaluationWindow {
    from: i64,
    to: i64,
}

impl EvaluationWindow {
    fn new(to: i64, seconds: i64) -> Self {
        Self {
            from: to.saturating_sub(seconds.saturating_mul(1000)),
            to,
        }
    }

    fn condition(self, column: &str) -> String {
        format!(
            "{column} >= fromUnixTimestamp64Milli({}, 'UTC') AND {column} <= fromUnixTimestamp64Milli({}, 'UTC')",
            self.from, self.to
        )
    }

    fn evidence(
        self,
        monitor: &Monitor,
        group_by: Vec<String>,
        query: MonitorEvidenceQuery,
    ) -> MonitorEvidence {
        let time = |ms| {
            chrono::DateTime::from_timestamp_millis(ms)
                .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        };
        MonitorEvidence {
            rule_version: monitor.rule_version,
            rule_name: monitor.name.clone(),
            comparator: monitor.comparator.clone(),
            window_start: time(self.from),
            evaluated_at: time(self.to).expect("current evaluation time is representable"),
            group_by,
            query,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn record_transition(
    config_db: &ConfigDb,
    monitor: &Monitor,
    group_key: &str,
    prev_state: &str,
    new_state: &str,
    value: Option<f64>,
    threshold: Option<f64>,
    message: String,
    evidence: Option<MonitorEvidence>,
) {
    let event = MonitorEvent {
        id: uuid::Uuid::new_v4().to_string(),
        monitor_id: monitor.id.clone(),
        tenant_id: monitor.tenant_id.clone(),
        group_key: group_key.to_string(),
        prev_state: prev_state.to_string(),
        new_state: new_state.to_string(),
        value: value.filter(|value| value.is_finite()),
        threshold,
        message,
        created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        evidence,
    };
    if let Err(error) = config_db.create_monitor_event(&event).await {
        tracing::warn!(engine = "monitors", monitor_id = %monitor.id, %error, "failed to record monitor transition");
    }
}

fn parse_monitor_query(monitor: &Monitor) -> anyhow::Result<MonitorEvidenceQuery> {
    let config = &monitor.query_config;
    match monitor.monitor_type.as_str() {
        "metric" => {
            let config: serde_json::Value = serde_json::from_str(config)?;
            let is_promql = config["type"] == "promql"
                || config["expression"]
                    .as_str()
                    .is_some_and(|expr| !expr.trim().is_empty());
            if is_promql {
                let expression = config
                    .get("expr")
                    .or_else(|| config.get("expression"))
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        anyhow::anyhow!("promql query_config missing 'expr' or 'expression'")
                    })?;
                Ok(MonitorEvidenceQuery::Promql {
                    expression: expression.to_string(),
                    series: None,
                })
            } else {
                Ok(MonitorEvidenceQuery::Metric(serde_json::from_value(
                    config,
                )?))
            }
        }
        "log" => Ok(MonitorEvidenceQuery::Log(serde_json::from_str(config)?)),
        "apm" => Ok(MonitorEvidenceQuery::Apm(serde_json::from_str(config)?)),
        "forecast" => Ok(MonitorEvidenceQuery::Forecast {
            config: serde_json::from_str(config)?,
            crossing_at: None,
        }),
        other => anyhow::bail!("unsupported monitor type '{other}'"),
    }
}

fn log_conditions(cfg: &LogQueryConfig) -> anyhow::Result<Vec<String>> {
    let mut conditions = Vec::new();
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

    Ok(conditions)
}

fn apm_conditions(cfg: &ApmQueryConfig) -> Vec<String> {
    let mut conditions = Vec::new();
    if let Some(condition) = apm_match_condition("service_name", &cfg.service) {
        conditions.push(condition);
    }

    if let Some(condition) = cfg
        .endpoint_filter
        .as_deref()
        .and_then(|endpoint| apm_match_condition("http_path", endpoint))
    {
        conditions.push(condition);
    }

    conditions
}

/// Replay the original monitor predicates, including its actual grouping
/// expression. A group key is opaque and can contain the grouping separator.
pub(crate) fn alert_scope_condition(filter: &Filter, logs: bool) -> anyhow::Result<String> {
    anyhow::ensure!(
        matches!(filter.op, FilterOp::Eq),
        "invalid alert scope operator"
    );
    let scope: MonitorExploreScope = serde_json::from_str(
        filter
            .value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid alert scope"))?,
    )?;
    let (mut predicates, group_resolver) = match scope.query {
        MonitorEvidenceQuery::Log(cfg) if logs => (
            log_conditions(&cfg)?,
            log_field_expr as fn(&str) -> Option<&'static str>,
        ),
        MonitorEvidenceQuery::Apm(cfg) if !logs => (
            apm_conditions(&cfg),
            apm_group_expr as fn(&str) -> Option<&'static str>,
        ),
        _ => anyhow::bail!("alert scope does not match the selected signal"),
    };
    if let Some(group_key) = scope.group_key.filter(|_| !scope.group_by.is_empty()) {
        let group = group_expression(&scope.group_by, group_resolver)?;
        predicates.push(format!("({group}) = '{}'", escape_ch(&group_key)));
    }
    if predicates.is_empty() {
        return Ok("1".into());
    }
    Ok(format!("({})", predicates.join(" AND ")))
}

struct MonitorReading {
    group_key: String,
    value: f64,
    series: Option<BTreeMap<String, String>>,
    crossing_at: Option<String>,
}

async fn run_monitor_query(
    ch: &Client,
    monitor: &Monitor,
    query: &MonitorEvidenceQuery,
    group_by: &[String],
    window: EvaluationWindow,
) -> anyhow::Result<Vec<MonitorReading>> {
    let tenant_id = &monitor.tenant_id;
    let (table, time_column, aggregation, group, mut conditions) = match query {
        MonitorEvidenceQuery::Metric(cfg) => {
            let agg = match cfg.aggregation.as_str() {
                "sum" => "sum(Value)",
                "max" => "max(Value)",
                "min" => "min(Value)",
                "count" => "count()",
                _ => "avg(Value)",
            };
            let group = group_by
                .iter()
                .map(|label| format!("ResourceAttributes['{}']", escape_ch(label)))
                .collect::<Vec<_>>()
                .join(" || ':' || ");
            let mut conditions = vec![format!("MetricName = '{}'", escape_ch(&cfg.metric_name))];
            conditions.extend(cfg.filters.iter().map(|f| {
                format!(
                    "ResourceAttributes['{}'] = '{}'",
                    escape_ch(&f.key),
                    escape_ch(&f.value),
                )
            }));
            (
                "metrics_gauge",
                "TimeUnix",
                agg.to_string(),
                group,
                conditions,
            )
        }
        MonitorEvidenceQuery::Log(cfg) => (
            "logs",
            "Timestamp",
            "count()".to_string(),
            group_expression(group_by, log_field_expr)?,
            log_conditions(cfg)?,
        ),
        MonitorEvidenceQuery::Apm(cfg) => (
            "spans",
            "timestamp",
            apm_aggregation(&cfg.metric, monitor.eval_window_secs),
            group_expression(group_by, apm_group_expr)?,
            apm_conditions(cfg),
        ),
        MonitorEvidenceQuery::Promql { expression, .. } => {
            let series = promql::evaluate_instant_query(
                ch,
                expression,
                window.to as f64 / 1000.0,
                monitor.eval_window_secs as f64,
                tenant_id,
            )
            .await
            .map_err(|error| anyhow::anyhow!("promql evaluation failed: {error}"))?;
            return Ok(series
                .into_iter()
                .filter_map(|series| {
                    let value = series.samples.last()?.1;
                    if value.is_nan() {
                        return None;
                    }
                    let group_key = series
                        .labels
                        .iter()
                        .filter(|(key, _)| key.as_str() != "__name__")
                        .map(|(key, value)| format!("{key}={value}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    Some(MonitorReading {
                        group_key,
                        value,
                        series: Some(series.labels),
                        crossing_at: None,
                    })
                })
                .collect());
        }
        MonitorEvidenceQuery::Forecast { config, .. } => {
            let threshold = monitor
                .critical
                .ok_or_else(|| anyhow::anyhow!("forecast threshold missing"))?;
            let forecast =
                monitor_forecast::evaluate(ch, tenant_id, config, threshold, window.to).await?;
            return Ok(forecast
                .reading()
                .map(|value| MonitorReading {
                    group_key: String::new(),
                    value,
                    series: None,
                    crossing_at: forecast.crossing_at,
                })
                .into_iter()
                .collect());
        }
        MonitorEvidenceQuery::Composite { .. } => {
            anyhow::bail!("composite monitors do not query telemetry")
        }
    };

    conditions.insert(0, window.condition(time_column));
    conditions.insert(0, format!("tenant_id = '{}'", escape_ch(tenant_id)));
    let where_clause = conditions.join(" AND ");
    if group_by.is_empty() {
        let sql =
            format!("SELECT toFloat64({aggregation}) AS value FROM {table} WHERE {where_clause}");
        let row = crate::tenant_query(ch, &sql, tenant_id)
            .with_setting("max_execution_time", "30")
            .fetch_one::<ValueRow>()
            .await?;
        Ok(vec![MonitorReading {
            group_key: String::new(),
            value: row.value,
            series: None,
            crossing_at: None,
        }])
    } else {
        let sql = format!(
            "SELECT ({group}) AS group_key, toFloat64({aggregation}) AS value \
             FROM {table} WHERE {where_clause} GROUP BY group_key"
        );
        let rows = crate::tenant_query(ch, &sql, tenant_id)
            .with_setting("max_execution_time", "30")
            .fetch_all::<GroupedRow>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| MonitorReading {
                group_key: row.group_key,
                value: row.value,
                series: None,
                crossing_at: None,
            })
            .collect())
    }
}

const APM_ERROR_CONDITION: &str = crate::query_builder::ERROR_SPAN;

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
    if monitor_type == "forecast" {
        let config = serde_json::from_value(query_config.clone())?;
        let threshold = conditions
            .critical
            .ok_or_else(|| anyhow::anyhow!("forecast threshold missing"))?;
        let forecast = monitor_forecast::evaluate(
            ch,
            tenant_id,
            &config,
            threshold,
            chrono::Utc::now().timestamp_millis(),
        )
        .await?;
        return Ok(PreviewResult {
            current_value: forecast.reading(),
            groups: vec![],
            timeseries: vec![],
            series: vec![],
            simulated_events: vec![],
            bucket_secs: 3600,
            forecast: Some(forecast),
        });
    }

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
        rule_version: 0,
    };

    let results: Vec<_> = if matches!(monitor_type, "metric" | "log" | "apm") {
        let query = parse_monitor_query(&temp_monitor)?;
        let window = EvaluationWindow::new(chrono::Utc::now().timestamp_millis(), eval_window_secs);
        run_monitor_query(ch, &temp_monitor, &query, group_by, window)
            .await?
            .into_iter()
            .map(|row| (row.group_key, row.value))
            .collect()
    } else {
        Vec::new()
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
        forecast: None,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forecast: Option<monitor_forecast::ForecastPreview>,
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
            let Ok(mut conds) = log_conditions(&cfg) else {
                return (vec![], bucket_secs);
            };
            conds.insert(
                0,
                format!("tenant_id = '{}'", escape_ch(&monitor.tenant_id)),
            );
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
            let mut conds = apm_conditions(&cfg);
            conds.insert(
                0,
                format!("tenant_id = '{}'", escape_ch(&monitor.tenant_id)),
            );
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
         ({group_expr}) AS group_key, toFloat64({agg_expr}) AS value \
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
        .with_setting("max_execution_time", "30")
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

    fn route(
        id: &str,
        priorities: Vec<i64>,
        tag_matchers: &[(&str, &str)],
        channel_ids: &[&str],
    ) -> AlertRoute {
        AlertRoute {
            id: id.to_string(),
            tenant_id: "default".to_string(),
            name: id.to_string(),
            enabled: true,
            priorities,
            tag_matchers: tag_matchers
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            channel_ids: channel_ids.iter().map(|id| id.to_string()).collect(),
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    #[test]
    fn alert_routes_are_additive_and_match_priority_with_tags() {
        let routes = vec![
            route("all-p1", vec![1], &[], &["slack-one"]),
            route(
                "devops-p1-p2",
                vec![1, 2],
                &[("team", "devops")],
                &["slack-two"],
            ),
        ];

        assert_eq!(
            resolve_notification_channel_ids("[]", Some(1), "[\"team:devops\"]", &routes),
            vec!["slack-one", "slack-two"]
        );
        assert_eq!(
            resolve_notification_channel_ids("[]", Some(2), "[\"team=devops\"]", &routes),
            vec!["slack-two"]
        );
        assert_eq!(
            resolve_notification_channel_ids("[]", Some(1), "[\"team:platform\"]", &routes),
            vec!["slack-one"]
        );
    }

    #[test]
    fn alert_routes_keep_direct_channels_and_deduplicate_destinations() {
        let mut route = route("all-p1", vec![1], &[], &["shared", "routed"]);
        assert_eq!(
            resolve_notification_channel_ids(
                "[\"direct\",\"shared\"]",
                Some(1),
                "[]",
                &[route.clone()],
            ),
            vec!["direct", "shared", "routed"]
        );

        route.enabled = false;
        assert_eq!(
            resolve_notification_channel_ids("[\"direct\"]", Some(1), "[]", &[route]),
            vec!["direct"]
        );
    }

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
    fn explore_replays_alert_predicates_group_and_precise_bounds() {
        let scope = Filter {
            field: crate::models::monitor::MONITOR_SCOPE_FIELD.into(),
            op: FilterOp::Eq,
            value: serde_json::json!(
                serde_json::json!({
                    "query": {
                        "kind": "log", "search": "Timeout Gone", "service": "checkout worker",
                        "severities": ["ERROR", "WARN"],
                        "filters": [{"field": "body", "op": "LIKE", "value": "retry required"}],
                    },
                    "group_by": ["service_name", "scope_name"],
                    "group_key": "checkout worker:scope:one",
                })
                .to_string()
            ),
        };
        let from = "2026-11-22T09:09:00.124Z";
        let to = "2026-11-22T09:14:00.124Z";
        let sql = crate::handlers::logs::build_log_where(
            std::slice::from_ref(&scope),
            from,
            to,
            None,
            "default",
        )
        .unwrap()
        .to_sql();
        assert!(sql.contains(
            "Timestamp >= parseDateTime64BestEffort('2026-11-22T09:09:00.124Z', 3, 'UTC')"
        ));
        assert!(sql.contains(
            "Timestamp <= parseDateTime64BestEffort('2026-11-22T09:14:00.124Z', 3, 'UTC')"
        ));
        assert!(sql.contains("lower(Body) LIKE '%timeout%' AND lower(Body) LIKE '%gone%'"));
        assert!(sql.contains("SeverityText IN ('ERROR', 'WARN')"));
        assert!(sql.contains("Body LIKE '%retry required%'"));
        assert!(sql.contains(
            "(toString(ServiceName) || ':' || toString(ScopeName)) = 'checkout worker:scope:one'"
        ));
        assert!(alert_scope_condition(&scope, false).is_err());

        let apm = Filter {
            value: serde_json::json!(serde_json::json!({
                "query": {"kind": "apm", "service": "checkout-*", "endpoint_filter": "/checkout/*", "metric": "error_rate"},
                "group_by": ["endpoint"],
                "group_key": "/checkout/confirm",
            }).to_string()),
            ..scope
        };
        let sql = crate::query_builder::build_where_clause(&[apm], from, to).to_sql();
        assert!(sql.contains("service_name LIKE 'checkout-%' AND http_path LIKE '/checkout/%'"));
        assert!(sql.contains("(toString(http_path)) = '/checkout/confirm'"));
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

    // count()/countIf() are UInt64; decoding them straight into the f64 value
    // read 10 as ~5e-323, so integer monitors never crossed their threshold.
    #[tokio::test]
    #[ignore = "requires isolated ClickHouse at RUSH_TEST_CLICKHOUSE_URL with rush_ custom settings"]
    async fn integer_aggregates_evaluate_as_their_counts() -> anyhow::Result<()> {
        let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
        let database = format!("monitor_test_{}", uuid::Uuid::new_v4().simple());
        let admin = Client::default().with_url(&url);
        admin
            .query(&format!("CREATE DATABASE {database}"))
            .execute()
            .await?;
        let ch = admin
            .clone()
            .with_database(&database)
            .with_validation(false);
        let result: anyhow::Result<()> = async {
            for table in ["logs", "spans", "metrics_gauge"] {
                let ddl = crate::migrations::MIGRATIONS
                    .iter()
                    .find(|sql| {
                        sql.starts_with(&format!("CREATE TABLE IF NOT EXISTS observability.{table}\n"))
                    })
                    .unwrap()
                    .replace("observability.", &format!("{database}."));
                admin.query(&ddl).execute().await?;
            }
            for insert in [
                "INSERT INTO logs (tenant_id, Timestamp, ServiceName, SeverityText, Body) \
                 SELECT 'default', now64(9), 'svc', 'ERROR', concat('boom ', toString(number)) FROM numbers(10)",
                "INSERT INTO spans (tenant_id, timestamp, service_name, status) \
                 SELECT 'default', now64(9), 'svc', if(number < 3, 'STATUS_CODE_ERROR', 'STATUS_CODE_OK') FROM numbers(5)",
                "INSERT INTO metrics_gauge (tenant_id, TimeUnix, MetricName, Value) \
                 SELECT 'default', now64(9), 'm', 1.5 FROM numbers(4)",
            ] {
                ch.query(insert).execute().await?;
            }
            let cases = [
                ("log", serde_json::json!({"service": "svc", "search": "boom"}), "service_name", 10.0),
                ("apm", serde_json::json!({"service": "svc", "metric": "error_count"}), "service_name", 3.0),
                ("apm", serde_json::json!({"service": "svc", "metric": "count"}), "service_name", 5.0),
                ("metric", serde_json::json!({"metric_name": "m", "aggregation": "count"}), "", 4.0),
            ];
            for (monitor_type, config, group, expected) in cases {
                let groups: Vec<String> = [group].into_iter().filter(|g| !g.is_empty()).map(String::from).collect();
                for group_by in [Vec::new(), groups] {
                    let preview = preview_query(
                        &ch,
                        "default",
                        monitor_type,
                        &config,
                        300,
                        600,
                        &group_by,
                        PreviewConditions {
                            critical: Some(1.0),
                            critical_recovery: None,
                            warning: None,
                            warning_recovery: None,
                            comparator: "above".into(),
                        },
                    )
                    .await?;
                    let case = format!("{monitor_type} {config} grouped by {group_by:?}");
                    assert_eq!(preview.current_value, Some(expected), "{case}");
                    let total: f64 = preview.timeseries.iter().map(|point| point.value).sum();
                    assert_eq!(total, expected, "{case} timeseries");
                }
            }
            Ok(())
        }
        .await;
        admin
            .query(&format!("DROP DATABASE {database}"))
            .execute()
            .await?;
        result
    }
}

#[cfg(test)]
mod notify_policy_tests {
    use super::{Notify, decide_notification};

    const NOW: i64 = 10_000_000;
    fn all(_: &str) -> bool {
        true
    }

    #[test]
    fn notifies_state_changes() {
        assert_eq!(
            decide_notification("ok", "alert", Some(("ok", 0)), NOW, false, None, all),
            Notify::Change
        );
        assert_eq!(
            decide_notification("alert", "ok", Some(("alert", 0)), NOW, false, None, all),
            Notify::Change
        );
        assert_eq!(
            decide_notification("ok", "ok", Some(("ok", 0)), NOW, false, None, all),
            Notify::No
        );
    }

    #[test]
    fn first_sight_records_a_baseline_or_notifies_a_change() {
        assert_eq!(
            decide_notification("alert", "alert", None, NOW, false, Some(60), all),
            Notify::Baseline
        );
        assert_eq!(
            decide_notification("ok", "alert", None, NOW, false, None, all),
            Notify::Change
        );
    }

    #[test]
    fn maintenance_silences_and_catches_up_afterwards() {
        // During the window: nothing sent, whatever happens.
        assert_eq!(
            decide_notification("ok", "alert", Some(("ok", 0)), NOW, true, None, all),
            Notify::No
        );
        assert_eq!(
            decide_notification("ok", "alert", None, NOW, true, None, all),
            Notify::Baseline
        );
        // After it: still alerting, last notification said ok, so notify once.
        assert_eq!(
            decide_notification("alert", "alert", Some(("ok", 0)), NOW, false, None, all),
            Notify::Change
        );
        // Alerted before the window, recovered inside it: send the recovery.
        assert_eq!(
            decide_notification("ok", "ok", Some(("alert", 0)), NOW, false, None, all),
            Notify::Change
        );
        // Flapped inside the window and ended where it started: nothing.
        assert_eq!(
            decide_notification("ok", "ok", Some(("ok", 0)), NOW, false, None, all),
            Notify::No
        );
    }

    #[test]
    fn renotifies_on_schedule_while_in_a_problem_state() {
        let last = Some(("alert", NOW - 3_600_000));
        assert_eq!(
            decide_notification("alert", "alert", last, NOW, false, Some(3_600), all),
            Notify::Renotify
        );
        assert_eq!(
            decide_notification("alert", "alert", last, NOW, false, Some(7_200), all),
            Notify::No
        );
        assert_eq!(
            decide_notification("alert", "alert", last, NOW, false, None, all),
            Notify::No
        );
        assert_eq!(
            decide_notification("alert", "alert", last, NOW, false, Some(0), all),
            Notify::No
        );
        assert_eq!(
            decide_notification("warn", "warn", Some(("warn", 0)), NOW, false, Some(60), all),
            Notify::Renotify
        );
        assert_eq!(
            decide_notification("ok", "ok", Some(("ok", 0)), NOW, false, Some(60), all),
            Notify::No
        );
        assert_eq!(
            decide_notification("alert", "alert", last, NOW, true, Some(60), all),
            Notify::No,
            "maintenance wins"
        );
    }

    #[test]
    fn respects_no_data_notify_setting() {
        let quiet = |state: &str| state != "no_data";
        assert_eq!(
            decide_notification("ok", "no_data", Some(("ok", 0)), NOW, false, None, quiet),
            Notify::No
        );
        assert_eq!(
            decide_notification("ok", "no_data", Some(("ok", 0)), NOW, false, None, all),
            Notify::Change
        );
        assert_eq!(
            decide_notification(
                "no_data",
                "no_data",
                Some(("no_data", 0)),
                NOW,
                false,
                Some(60),
                quiet
            ),
            Notify::No
        );
        // Back from an un-notified no_data to the last notified state: nothing new.
        assert_eq!(
            decide_notification("no_data", "ok", Some(("ok", 0)), NOW, false, None, quiet),
            Notify::No
        );
    }
}
