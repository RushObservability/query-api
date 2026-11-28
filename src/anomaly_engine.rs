use crate::alert_engine::SmtpConfig;
use crate::clickhouse_config::ConfigDb;
use crate::engine_metrics::{EvaluationObservation, EvaluationOutcome};
use crate::models::anomaly::AnomalyRule;
use crate::query_builder::ERROR_SPAN;
use crate::timestamps::TimestampExt;
use clickhouse::Client;
use futures_util::StreamExt;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, Tokio1Executor};
use std::sync::Arc;

#[derive(clickhouse::Row, serde::Deserialize)]
#[allow(dead_code)]
struct ApmBucket {
    bucket: u32,
    count: u64,
    error_count: u64,
    p50: f64,
    p95: f64,
    p99: f64,
}

struct EwmaResult {
    mean: f64,
    anomalous: bool,
    deviation: f64,
}

fn ewma_eval(data: &[f64], alpha: f64, sensitivity: f64) -> EwmaResult {
    let warmup = 12.min(data.len());
    if warmup == 0 {
        return EwmaResult {
            mean: 0.0,
            anomalous: false,
            deviation: 0.0,
        };
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
            let dev = if std > 0.0 {
                (val - mean).abs() / std
            } else {
                0.0
            };
            return EwmaResult {
                mean,
                anomalous: is_anomaly,
                deviation: dev,
            };
        }
    }

    EwmaResult {
        mean: 0.0,
        anomalous: false,
        deviation: 0.0,
    }
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

/// Spawn the anomaly engine as a background task on the API server (mirrors
/// `spawn_alert_engine` / `spawn_slo_engine`). Without this, anomaly rules are
/// never evaluated unless the standalone `anomaly_engine` binary is run, so no
/// anomaly events are ever persisted.
pub fn spawn_anomaly_engine(
    config_db: Arc<ConfigDb>,
    read_ch: Client,
    write_ch: Client,
    smtp_config: SmtpConfig,
    self_metrics: Arc<crate::self_metrics::SelfMetrics>,
    lease: Arc<crate::leader::EngineLease>,
) {
    tokio::spawn(run_anomaly_engine(
        config_db,
        read_ch,
        write_ch,
        smtp_config,
        self_metrics,
        lease,
    ));
}

/// Run the anomaly engine loop forever. Call this directly from the standalone binary.
pub async fn run_anomaly_engine(
    config_db: Arc<ConfigDb>,
    read_ch: Client,
    write_ch: Client,
    smtp_config: SmtpConfig,
    self_metrics: Arc<crate::self_metrics::SelfMetrics>,
    lease: Arc<crate::leader::EngineLease>,
) {
    let http_client = reqwest::Client::new();
    let smtp_transport = build_smtp_transport(&smtp_config);
    if smtp_transport.is_some() {
        tracing::info!(
            engine = "anomaly",
            "SMTP configured for email notifications"
        );
    }
    tracing::info!(
        engine = "anomaly",
        interval_secs = 30,
        "anomaly engine started"
    );

    let mut eval_state = crate::eval_state::EvalState::new(EVAL_FLUSH_EVERY);
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    loop {
        interval.tick().await;
        if !lease.is_leader() {
            continue;
        }
        let start = std::time::Instant::now();
        let cycle = eval_anomaly_rules(
            &config_db,
            &read_ch,
            &write_ch,
            &http_client,
            &smtp_config,
            &smtp_transport,
            &mut eval_state,
        );
        let ok = match crate::leader::fenced(lease.clone(), cycle).await {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(error = %e, engine = "anomaly", "anomaly engine error");
                false
            }
        };
        self_metrics.record_engine("anomaly_engine", start.elapsed().as_millis() as u64, ok);
    }
}

/// Write an unchanged rule's `last_eval_at` once per this many evaluations.
const EVAL_FLUSH_EVERY: u32 = 10;

async fn eval_anomaly_rules(
    config_db: &ConfigDb,
    read_ch: &Client,
    write_ch: &Client,
    http_client: &reqwest::Client,
    smtp_config: &SmtpConfig,
    smtp_transport: &Option<AsyncSmtpTransport<Tokio1Executor>>,
    eval_state: &mut crate::eval_state::EvalState,
) -> anyhow::Result<()> {
    let now = time::OffsetDateTime::now_utc();
    let now_str = now.utc_seconds();
    // DB-side due filter (coarse: last_eval_at is only flushed 1-in-N) combined
    // with the in-memory check ⇒ max(db, mem) + interval <= now semantics.
    let jobs: Vec<(AnomalyRule, bool)> = config_db
        .get_due_anomaly_rules(&now_str)
        .await?
        .into_iter()
        .filter(|rule| eval_state.is_due(&rule.id, now, rule.eval_interval_secs))
        .map(|rule| {
            let flush = eval_state.should_flush(&rule.id);
            (rule, flush)
        })
        .collect();
    crate::engine_metrics::due(config_db.self_metrics(), "anomaly_engine", jobs.len());

    if jobs.is_empty() {
        tracing::debug!(engine = "anomaly", "tick -- no rules due");
    } else {
        tracing::debug!(
            engine = "anomaly",
            rules_due = jobs.len(),
            "tick -- evaluating rules"
        );
    }

    // Each rule is fetched under a background permit and evaluated as soon as
    // its data arrives, so only in-flight rules' series are held in memory.
    // Tenants take turns; half the background pool is left for the monitor
    // and SLO engines that share it.
    let budget = crate::query_governor::global()
        .map(|governor| governor.config())
        .unwrap_or_default()
        .background;
    let (tenants_at_once, per_tenant) = crate::monitor_engine::engine_concurrency(&budget);
    let now_str_ref = now_str.as_str();
    let outcomes = run_tenant_fair(
        jobs,
        |(rule, _)| rule.tenant_id.clone(),
        (tenants_at_once / 2).max(1),
        per_tenant,
        |(rule, should_flush)| async move {
            let mut log_rows = Vec::new();
            let result = evaluate_rule(
                config_db,
                read_ch,
                http_client,
                smtp_config,
                smtp_transport,
                &rule,
                now,
                now_str_ref,
                should_flush,
                &mut log_rows,
            )
            .await;
            (rule.id, result, log_rows)
        },
    )
    .await;

    // Per-series eval logs are accumulated here and written as ONE batch insert
    // at the end of the tick (previously one INSERT per series per rule).
    let mut eval_log_values: Vec<String> = Vec::new();
    let mut failures = 0;
    for (id, result, log_rows) in outcomes {
        eval_log_values.extend(log_rows);
        match result {
            // Not admitted: keep the rule due for the next tick.
            Ok(None) => {}
            Ok(Some(persisted)) => eval_state.record(id, now, persisted),
            Err(error) => {
                failures += 1;
                tracing::warn!(engine = "anomaly", rule_id = %id, %error, "anomaly evaluation failed");
                eval_state.record(id, now, false);
            }
        }
    }

    // Flush all per-series eval logs in one INSERT (previously one per series).
    if !eval_log_values.is_empty() && crate::leader::ensure_leader().is_ok() {
        let sql = format!(
            "INSERT INTO logs (tenant_id, Timestamp, SeverityText, SeverityNumber, ServiceName, Body, LogAttributes) VALUES {}",
            eval_log_values.join(", ")
        );
        if let Err(e) = write_ch.query(&sql).execute().await {
            failures += 1;
            if let Some(metrics) = config_db.self_metrics() {
                metrics.inc_counter(
                    "rush_engine_persistence_failures_total",
                    &[("engine", "anomaly_engine"), ("operation", "eval_log")],
                    1,
                );
            }
            tracing::warn!(error = %e, engine = "anomaly", rows = eval_log_values.len(), "failed to write eval log batch");
        }
    }

    anyhow::ensure!(
        failures == 0,
        "{failures} anomaly evaluation operation(s) failed"
    );
    Ok(())
}

/// Fetch and evaluate one rule, pushing its per-series eval log rows. Returns
/// `None` when the background pool refused the fetch, otherwise whether the
/// rule row was written. Errors end only this rule.
#[allow(clippy::too_many_arguments)]
async fn evaluate_rule(
    config_db: &ConfigDb,
    read_ch: &Client,
    http_client: &reqwest::Client,
    smtp_config: &SmtpConfig,
    smtp_transport: &Option<AsyncSmtpTransport<Tokio1Executor>>,
    rule: &AnomalyRule,
    now: time::OffsetDateTime,
    now_str: &str,
    should_flush: bool,
    log_rows: &mut Vec<String>,
) -> anyhow::Result<Option<bool>> {
    let mut observation = EvaluationObservation::new(config_db.self_metrics(), "anomaly_engine");
    let fetched = crate::query_governor::run_background(&rule.tenant_id, async {
        match rule.source.as_str() {
            "apm" => Some(fetch_apm_data(read_ch, rule, &now).await),
            "prometheus" => Some(fetch_prom_data(read_ch, rule, &now).await),
            _ => None,
        }
    })
    .await;
    let prefetched = match fetched {
        Ok(result) => result,
        Err(error) => {
            observation.finish(EvaluationOutcome::AdmissionRejected);
            tracing::debug!(engine = "anomaly", rule_id = %rule.id, ?error, "background pool busy; rule retried next tick");
            return Ok(None);
        }
    };
    observation.finish(EvaluationOutcome::Error);
    tracing::debug!(
        engine = "anomaly",
        rule_name = %rule.name,
        source = %rule.source,
        state = %rule.state,
        "evaluating rule"
    );

    let Some(all_series) = prefetched else {
        observation.finish(EvaluationOutcome::InvalidConfig);
        anyhow::bail!("unknown data source {:?}", rule.source);
    };

    let series_list = match all_series {
        Ok(s) if !s.is_empty() => s,
        Ok(_) => {
            tracing::debug!(engine = "anomaly", rule_name = %rule.name, "no data points returned");
            let persisted = persist_no_data(config_db, rule, now_str, should_flush).await?;
            observation.finish(EvaluationOutcome::NoData);
            return Ok(Some(persisted));
        }
        Err(e) => {
            persist_no_data(config_db, rule, now_str, should_flush).await?;
            return Err(e.context("data fetch failed"));
        }
    };

    tracing::debug!(
        engine = "anomaly",
        rule_name = %rule.name,
        series_count = series_list.len(),
        "series to evaluate"
    );

    // Evaluate each series independently; if ANY is anomalous the rule triggers
    let mut any_anomalous = false;
    let mut worst_deviation = 0.0_f64;
    let mut worst_metric = String::new();
    let mut worst_val = 0.0_f64;
    let mut worst_expected = 0.0_f64;

    for (values, metric_label) in &series_list {
        if values.is_empty() {
            continue;
        }
        let latest_val = *values.last().unwrap_or(&0.0);
        let result = ewma_eval(values, rule.alpha, rule.sensitivity);

        let series_state = if result.anomalous {
            "anomalous"
        } else {
            "normal"
        };

        tracing::debug!(
            engine = "anomaly",
            rule_name = %rule.name,
            metric = %metric_label,
            data_points = values.len(),
            latest_value = format_args!("{:.2}", latest_val),
            ewma_mean = format_args!("{:.2}", result.mean),
            deviation_sigma = format_args!("{:.1}", result.deviation),
            anomalous = result.anomalous,
            "series evaluated"
        );

        // Collect the eval log row; flushed in one batch insert per tick.
        log_rows.push(anomaly_log_tuple(
            &now,
            rule,
            series_state,
            metric_label,
            latest_val,
            result.mean,
            result.deviation,
        ));

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

    let persisted = if new_state != old_state {
        let event_id = uuid::Uuid::new_v4().to_string();
        let message = format!(
            "Anomaly '{}': {} (metric={}, value={:.2}, expected={:.2}, deviation={:.1}σ)",
            rule.name,
            if any_anomalous {
                "ANOMALOUS"
            } else {
                "RESOLVED"
            },
            worst_metric,
            worst_val,
            worst_expected,
            worst_deviation,
        );

        config_db
            .create_anomaly_event(
                &event_id,
                &rule.id,
                &rule.tenant_id,
                new_state,
                &worst_metric,
                worst_val,
                worst_expected,
                worst_deviation,
                &message,
            )
            .await?;

        let triggered_at = if any_anomalous { Some(now_str) } else { None };
        config_db
            .update_anomaly_state(&rule.id, &rule.tenant_id, new_state, now_str, triggered_at)
            .await?;

        // Send notifications
        send_notifications(
            config_db,
            http_client,
            smtp_config,
            smtp_transport,
            rule,
            &message,
            any_anomalous,
        )
        .await;

        tracing::info!(
            engine = "anomaly",
            rule_name = %rule.name,
            old_state = %old_state,
            new_state = %new_state,
            metric = %worst_metric,
            deviation_sigma = format_args!("{:.1}", worst_deviation),
            "anomaly state changed"
        );
        true
    } else if should_flush {
        // Unchanged: only refresh last_eval_at on the flush cadence.
        config_db
            .update_anomaly_state(&rule.id, &rule.tenant_id, new_state, now_str, None)
            .await?;
        true
    } else {
        false
    };
    observation.finish(EvaluationOutcome::Completed);
    Ok(Some(persisted))
}

/// Set the rule to `no_data`, writing only on a change or the flush cadence.
async fn persist_no_data(
    config_db: &ConfigDb,
    rule: &AnomalyRule,
    now_str: &str,
    should_flush: bool,
) -> anyhow::Result<bool> {
    if rule.state == "no_data" && !should_flush {
        return Ok(false);
    }
    config_db
        .update_anomaly_state(&rule.id, &rule.tenant_id, "no_data", now_str, None)
        .await?;
    Ok(true)
}

/// Runs `job` for every item, at most `per_tenant` at a time within a tenant
/// and `tenants` tenants at a time, returning results in completion order.
async fn run_tenant_fair<I, T, Fut>(
    items: Vec<I>,
    tenant_of: impl Fn(&I) -> String,
    tenants: usize,
    per_tenant: usize,
    job: impl Fn(I) -> Fut,
) -> Vec<T>
where
    Fut: std::future::Future<Output = T>,
{
    let mut by_tenant: std::collections::BTreeMap<String, Vec<I>> = Default::default();
    for item in items {
        by_tenant.entry(tenant_of(&item)).or_default().push(item);
    }
    // Build the futures up front; a lazily mapped stream trips the `Send`
    // check on the spawned engine task.
    let lanes: Vec<_> = by_tenant
        .into_values()
        .map(|items| {
            let jobs: Vec<Fut> = items.into_iter().map(&job).collect();
            futures_util::stream::iter(jobs).buffer_unordered(per_tenant.max(1))
        })
        .collect();
    futures_util::stream::iter(lanes)
        .flatten_unordered(tenants.max(1))
        .collect()
        .await
}

/// Spans reach ClickHouse a few seconds after they end (collector and ingest
/// batching), so a minute is only scored once it has been closed this long.
const LATE_SPAN_GRACE_SECS: i64 = 30;

async fn fetch_apm_data(
    ch: &Client,
    rule: &AnomalyRule,
    now: &time::OffsetDateTime,
) -> anyhow::Result<Vec<(Vec<f64>, String)>> {
    // Only whole minutes that ended at least LATE_SPAN_GRACE_SECS ago. Scoring the
    // minute still filling up read as a sudden drop (and fed that low value
    // into the EWMA baseline), so steady traffic raised false anomalies.
    let cutoff = now.unix_timestamp() - LATE_SPAN_GRACE_SECS;
    let complete_before = time::OffsetDateTime::from_unix_timestamp(cutoff.div_euclid(60) * 60)?;
    let from = (complete_before - time::Duration::seconds(rule.window_secs)).utc_seconds();
    let until = complete_before.utc_seconds();

    let escaped_tenant = crate::query_builder::escape_string_literal(&rule.tenant_id);
    let sql = format!(
        "SELECT toUnixTimestamp(toStartOfInterval(timestamp, INTERVAL '1' MINUTE)) as bucket, \
         count() as count, \
         countIf({ERROR_SPAN}) as error_count, \
         quantile(0.50)(duration_ns) as p50, \
         quantile(0.95)(duration_ns) as p95, \
         quantile(0.99)(duration_ns) as p99 \
         FROM spans \
         WHERE tenant_id = '{}' AND service_name = '{}' AND timestamp >= parseDateTimeBestEffort('{}') AND timestamp < parseDateTimeBestEffort('{}') \
         GROUP BY bucket ORDER BY bucket",
        escaped_tenant,
        crate::query_builder::escape_string_literal(&rule.service_name),
        from,
        until,
    );

    let rows = crate::tenant_query(ch, &sql, &rule.tenant_id)
        .fetch_all::<ApmBucket>()
        .await?;
    if rows.is_empty() {
        return Ok(vec![]);
    }

    let metric_label = format!("{}:{}", rule.service_name, rule.apm_metric);
    let values: Vec<f64> = rows
        .iter()
        .map(|r| {
            match rule.apm_metric.as_str() {
                "error_rate" => r.error_count as f64,
                "p50" => r.p50,
                "p95" => r.p95,
                "p99" => r.p99,
                _ => r.count as f64, // request_rate
            }
        })
        .collect();

    Ok(vec![(values, metric_label)])
}

async fn fetch_prom_data(
    ch: &Client,
    rule: &AnomalyRule,
    now: &time::OffsetDateTime,
) -> anyhow::Result<Vec<(Vec<f64>, String)>> {
    let end = now.unix_timestamp();
    let start = end - rule.window_secs;
    let step = if rule.window_secs <= 3600 {
        15
    } else if rule.window_secs <= 21600 {
        60
    } else {
        300
    };

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

    // Evaluated in-process, scoped to the rule's tenant, under the background
    // permit this fetch already holds. Calling our own /prom over HTTP sent no
    // credential, so auth-required tenants refused it.
    let all_series = crate::promql::evaluate_range_query(
        ch,
        &query,
        start as f64,
        end as f64,
        step as f64,
        &rule.tenant_id,
    )
    .await
    .map_err(anyhow::Error::msg)?;

    if all_series.is_empty() {
        return Ok(vec![]);
    }

    let results: Vec<(Vec<f64>, String)> = all_series
        .into_iter()
        .map(|s| {
            let values: Vec<f64> = s.samples.iter().map(|(_, v)| *v).collect();
            // Build a human-readable label from the metric map
            let label = if s.labels.is_empty() {
                rule.pattern.clone()
            } else {
                let parts: Vec<String> = s
                    .labels
                    .iter()
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
        })
        .collect();

    Ok(results)
}

/// Build one VALUES tuple for a per-series eval log row. Rows are accumulated
/// across the whole tick and flushed in a single batch INSERT.
fn anomaly_log_tuple(
    now: &time::OffsetDateTime,
    rule: &AnomalyRule,
    state: &str,
    metric: &str,
    value: f64,
    expected: f64,
    deviation: f64,
) -> String {
    let ts_nanos = now
        .unix_nanos()
        .unwrap_or(now.unix_timestamp() * 1_000_000_000);
    let severity_text = if state == "anomalous" { "WARN" } else { "INFO" };
    let severity_number: u8 = if state == "anomalous" { 13 } else { 9 }; // WARN=13, INFO=9

    let body = format!(
        "[wide-anomaly] rule={} state={} metric={} value={:.2} expected={:.2} deviation={:.1}σ",
        rule.name, state, metric, value, expected, deviation,
    );

    format!(
        "('{tenant}', {ts_nanos}, '{severity_text}', {severity_number}, 'wide-anomaly-engine', '{body}', \
         {{'anomaly.rule_id': '{rule_id}', 'anomaly.rule_name': '{rule_name}', 'anomaly.state': '{state}', \
         'anomaly.metric': '{metric}', 'anomaly.value': '{value:.2}', 'anomaly.expected': '{expected:.2}', \
         'anomaly.deviation': '{deviation:.1}'}})",
        tenant = crate::query_builder::escape_string_literal(&rule.tenant_id),
        ts_nanos = ts_nanos,
        severity_text = severity_text,
        severity_number = severity_number,
        body = crate::query_builder::escape_string_literal(&body),
        rule_id = rule.id,
        rule_name = crate::query_builder::escape_string_literal(&rule.name),
        state = state,
        metric = crate::query_builder::escape_string_literal(metric),
        value = value,
        expected = expected,
        deviation = deviation,
    )
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
    if let Err(error) = crate::leader::ensure_leader() {
        tracing::info!(engine = "anomaly", rule_id = %rule.id, %error, "notifications skipped");
        return;
    }
    let channel_ids: Vec<String> =
        serde_json::from_str(&rule.notification_channel_ids).unwrap_or_default();
    // Webhook receivers written for the old anomaly payload keep their fields,
    // including `state` as anomalous/normal.
    let webhook_fields = serde_json::json!({
        "anomaly": rule.name,
        "state": if is_anomalous { "anomalous" } else { "normal" },
    });
    crate::alert_engine::notify_channels(
        config_db,
        &rule.tenant_id,
        &channel_ids,
        &crate::alert_engine::ChannelNotification {
            source: "anomaly",
            signal_type: "anomaly",
            alert_name: &rule.name,
            message,
            alert_state: if is_anomalous { "alert" } else { "ok" },
            value: 0.0,
            threshold: 0.0,
            condition_op: "",
            description: "",
            alert_id: &rule.id,
            incident_key: &format!("anomaly:{}", rule.id),
            runbook_url: "",
            webhook_fields: Some(&webhook_fields),
        },
        http_client,
        smtp_config,
        smtp_transport,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_rule(id: &str, source: &str) -> AnomalyRule {
        AnomalyRule {
            id: id.into(),
            tenant_id: "acme".into(),
            name: id.into(),
            description: String::new(),
            enabled: true,
            source: source.into(),
            pattern: "up".into(),
            query: String::new(),
            service_name: String::new(),
            apm_metric: String::new(),
            sensitivity: 3.0,
            alpha: 0.25,
            eval_interval_secs: 60,
            window_secs: 3600,
            split_labels: "[]".into(),
            notification_channel_ids: "[]".into(),
            state: "normal".into(),
            last_eval_at: None,
            last_triggered_at: None,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    /// Fetches each rule while holding a background permit for 50 ms, and
    /// returns how many were refused admission.
    async fn rejected_fetches(rules: Vec<AnomalyRule>, tenants: usize, per_tenant: usize) -> usize {
        use crate::query_governor::{QueryGovernor, QueryGovernorConfig, WorkloadClass};
        let mut config = QueryGovernorConfig::default();
        config.background.queue_timeout_ms = 10;
        let governor =
            QueryGovernor::new(config, Arc::new(crate::self_metrics::SelfMetrics::new())).unwrap();
        let governor = &governor;
        let results = run_tenant_fair(
            rules,
            |rule| rule.tenant_id.clone(),
            tenants,
            per_tenant,
            |rule| async move {
                let guard = governor
                    .admit(WorkloadClass::Background, &rule.tenant_id)
                    .await?;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                drop(guard);
                Ok::<_, crate::query_governor::AdmissionError>(rule.id)
            },
        )
        .await;
        results.iter().filter(|r| r.is_err()).count()
    }

    #[tokio::test]
    async fn fetches_stay_within_the_background_pool() {
        let budget = crate::query_governor::QueryGovernorConfig::default().background;
        let (tenants, per_tenant) = crate::monitor_engine::engine_concurrency(&budget);
        let one_tenant = || (0..6).map(|i| test_rule(&i.to_string(), "apm")).collect();
        // Starting every rule at once (the old join_all) overflowed the pool.
        assert!(rejected_fetches(one_tenant(), 1, 6).await > 0);
        assert_eq!(rejected_fetches(one_tenant(), tenants, per_tenant).await, 0);
        let many_tenants = (0..12)
            .map(|i| {
                let mut rule = test_rule(&i.to_string(), "apm");
                rule.tenant_id = format!("t{}", i % 4);
                rule
            })
            .collect();
        assert_eq!(
            rejected_fetches(many_tenants, (tenants / 2).max(1), per_tenant).await,
            0
        );
    }

    #[tokio::test]
    async fn a_tenant_with_many_rules_does_not_go_first() {
        // Rules used to be fetched in list order, two at a time across all
        // tenants, so a tenant listed after a large one waited for all of it.
        let mut rules: Vec<AnomalyRule> =
            (0..6).map(|i| test_rule(&i.to_string(), "apm")).collect();
        let mut other = test_rule("other", "apm");
        other.tenant_id = "zeta".into();
        rules.push(other);
        let order = run_tenant_fair(
            rules,
            |rule| rule.tenant_id.clone(),
            2,
            2,
            |rule| async move {
                if rule.tenant_id == "acme" {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                rule.id
            },
        )
        .await;
        assert_eq!(order[0], "other", "{order:?}");
        assert_eq!(order.len(), 7);
    }

    #[tokio::test]
    async fn unchanged_rules_skip_the_config_write_until_the_flush() {
        // No handlers: any config read or write fails.
        let config_mock = clickhouse::test::Mock::new();
        let config_db = crate::clickhouse_config::session_tenant_regressions::config(&config_mock);
        // Nothing listens here, so the rule has no data.
        let ch = Client::default().with_url("http://127.0.0.1:9");
        let mut rule = test_rule("r", "prometheus");
        rule.state = "no_data".into();
        let evaluate = |should_flush| {
            let (config_db, ch, rule) = (&config_db, &ch, &rule);
            async move {
                let mut rows = Vec::new();
                evaluate_rule(
                    config_db,
                    ch,
                    &reqwest::Client::new(),
                    &SmtpConfig {
                        host: None,
                        port: 25,
                        user: None,
                        pass: None,
                        from: String::new(),
                    },
                    &None,
                    rule,
                    time::OffsetDateTime::now_utc(),
                    "2026-01-01 00:00:00",
                    should_flush,
                    &mut rows,
                )
                .await
            }
        };
        // Every evaluation used to do a SELECT FINAL and a full-row INSERT.
        assert_eq!(evaluate(false).await.unwrap(), Some(false));
        // The flush writes, and a failed write ends only this rule.
        config_mock.add(clickhouse::test::handlers::failure(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        ));
        assert!(evaluate(true).await.is_err());
    }

    #[tokio::test]
    async fn prometheus_rules_do_not_need_an_http_prom_endpoint() {
        // Nothing listens here. The old self-call to /prom failed (as it did on
        // auth-required tenants) and set the rule to no_data; in-process PromQL
        // treats an unreadable table as no samples, like the /prom handler.
        let ch = Client::default().with_url("http://127.0.0.1:9");
        let series = fetch_prom_data(
            &ch,
            &test_rule("r", "prometheus"),
            &time::OffsetDateTime::now_utc(),
        )
        .await
        .unwrap();
        assert!(series.is_empty());
    }

    #[tokio::test]
    #[ignore = "requires isolated ClickHouse at RUSH_TEST_CLICKHOUSE_URL with rush_ custom settings"]
    async fn apm_series_scores_only_finished_minutes() -> anyhow::Result<()> {
        let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
        let database = format!("anomaly_test_{}", uuid::Uuid::new_v4().simple());
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
            let ddl = crate::migrations::MIGRATIONS
                .iter()
                .find(|sql| sql.starts_with("CREATE TABLE IF NOT EXISTS observability.spans\n"))
                .unwrap()
                .replace("observability.", &format!("{database}."));
            admin.query(&ddl).execute().await?;
            // 10s into minute M: M-1 closed under LATE_SPAN_GRACE_SECS ago, M is filling.
            let seconds = time::OffsetDateTime::now_utc().unix_timestamp();
            let minute = time::OffsetDateTime::from_unix_timestamp(seconds.div_euclid(60) * 60)?;
            let now = minute + time::Duration::seconds(10);
            let m = minute.clickhouse_seconds();
            for (offset_minutes, spans) in [(-1, 20), (0, 20)] {
                ch.query(&format!(
                    "INSERT INTO spans (tenant_id, timestamp, service_name, status) \
                     SELECT 'default', toDateTime64('{m}', 9) + INTERVAL {offset_minutes} MINUTE + INTERVAL 5 SECOND, 'svc', 'STATUS_CODE_OK' \
                     FROM numbers({spans})"
                ))
                .execute()
                .await?;
            }
            // 80 spans in each finished minute M-16 ..= M-2.
            ch.query(&format!(
                "INSERT INTO spans (tenant_id, timestamp, service_name, status) \
                 SELECT 'default', toDateTime64('{m}', 9) - INTERVAL (number % 15 + 2) MINUTE + INTERVAL 5 SECOND, 'svc', 'STATUS_CODE_OK' \
                 FROM numbers(1200)"
            ))
            .execute()
            .await?;
            let rule = AnomalyRule {
                id: "r".into(),
                tenant_id: "default".into(),
                name: "r".into(),
                description: String::new(),
                enabled: true,
                source: "apm".into(),
                pattern: String::new(),
                query: String::new(),
                service_name: "svc".into(),
                apm_metric: "request_rate".into(),
                sensitivity: 3.0,
                alpha: 0.25,
                eval_interval_secs: 60,
                window_secs: 3600,
                split_labels: "[]".into(),
                notification_channel_ids: "[]".into(),
                state: "normal".into(),
                last_eval_at: None,
                last_triggered_at: None,
                created_at: String::new(),
                updated_at: String::new(),
            };
            let series = fetch_apm_data(&ch, &rule, &now).await?;
            let (values, _) = &series[0];
            assert_eq!(values, &vec![80.0; 15]);
            assert!(!ewma_eval(values, rule.alpha, rule.sensitivity).anomalous);
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
