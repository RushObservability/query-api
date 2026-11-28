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

async fn eval_anomaly_rules(
    config_db: &ConfigDb,
    read_ch: &Client,
    write_ch: &Client,
    http_client: &reqwest::Client,
    smtp_config: &SmtpConfig,
    smtp_transport: &Option<AsyncSmtpTransport<Tokio1Executor>>,
) -> anyhow::Result<()> {
    let now = time::OffsetDateTime::now_utc();
    let now_str = now.utc_seconds();
    let due_rules = config_db.get_due_anomaly_rules(&now_str).await?;
    crate::engine_metrics::due(config_db.self_metrics(), "anomaly_engine", due_rules.len());

    if due_rules.is_empty() {
        tracing::debug!(engine = "anomaly", "tick -- no rules due");
    } else {
        tracing::debug!(
            engine = "anomaly",
            rules_due = due_rules.len(),
            "tick -- evaluating rules"
        );
    }

    // Prefetch due rules' data concurrently, but no more at once than one tenant
    // may hold background permits: starting them all at once left the rest
    // waiting out the admission queue and failing.
    let concurrency = crate::query_governor::global().map_or(2, |governor| {
        governor.config().background.per_tenant_concurrency as usize
    });
    let fetched = prefetch(&due_rules, concurrency, |rule| async move {
        let mut observation =
            EvaluationObservation::new(config_db.self_metrics(), "anomaly_engine");
        let fetched = crate::query_governor::run_background(&rule.tenant_id, async {
            match rule.source.as_str() {
                "apm" => Some(fetch_apm_data(read_ch, rule, &now).await),
                "prometheus" => Some(fetch_prom_data(read_ch, rule, &now).await),
                _ => None,
            }
        })
        .await;
        match fetched {
            Ok(result) => Some((result, observation)),
            Err(error) => {
                observation.finish(EvaluationOutcome::AdmissionRejected);
                tracing::debug!(engine = "anomaly", rule_id = %rule.id, ?error, "background pool busy; rule retried next tick");
                None
            }
        }
    })
    .await;

    // Per-series eval logs are accumulated here and written as ONE batch insert
    // at the end of the tick (previously one INSERT per series per rule).
    let mut eval_log_values: Vec<String> = Vec::new();

    let mut failures = 0;
    for (rule, fetched) in due_rules.into_iter().zip(fetched) {
        // Not admitted: keep the rule's state; it is still due next tick.
        let Some((prefetched, mut observation)) = fetched else {
            continue;
        };
        observation.finish(EvaluationOutcome::Error);
        tracing::debug!(
            engine = "anomaly",
            rule_name = %rule.name,
            source = %rule.source,
            state = %rule.state,
            "evaluating rule"
        );

        let all_series = match prefetched {
            Some(r) => r,
            None => {
                observation.finish(EvaluationOutcome::InvalidConfig);
                failures += 1;
                tracing::warn!(engine = "anomaly", rule_id = %rule.id, source = %rule.source, "unknown data source");
                continue;
            }
        };

        let series_list = match all_series {
            Ok(s) if !s.is_empty() => s,
            Ok(_) => {
                tracing::debug!(engine = "anomaly", rule_name = %rule.name, "no data points returned");
                config_db
                    .update_anomaly_state(&rule.id, &rule.tenant_id, "no_data", &now_str, None)
                    .await?;
                observation.finish(EvaluationOutcome::NoData);
                continue;
            }
            Err(e) => {
                failures += 1;
                tracing::warn!(error = %e, engine = "anomaly", rule_id = %rule.id, "data fetch failed");
                config_db
                    .update_anomaly_state(&rule.id, &rule.tenant_id, "no_data", &now_str, None)
                    .await?;
                continue;
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
            eval_log_values.push(anomaly_log_tuple(
                &now,
                &rule,
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

        if new_state != old_state {
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

            let triggered_at = if any_anomalous {
                Some(now_str.as_str())
            } else {
                None
            };
            config_db
                .update_anomaly_state(&rule.id, &rule.tenant_id, new_state, &now_str, triggered_at)
                .await?;

            // Send notifications
            send_notifications(
                config_db,
                http_client,
                smtp_config,
                smtp_transport,
                &rule,
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
        } else {
            config_db
                .update_anomaly_state(&rule.id, &rule.tenant_id, new_state, &now_str, None)
                .await?;
        }
        observation.finish(EvaluationOutcome::Completed);
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

/// Runs `fetch` for each rule, at most `concurrency` at a time, in rule order.
async fn prefetch<'a, T, Fut>(
    rules: &'a [AnomalyRule],
    concurrency: usize,
    fetch: impl FnMut(&'a AnomalyRule) -> Fut,
) -> Vec<T>
where
    Fut: std::future::Future<Output = T>,
{
    // Build the futures up front; a lazily mapped stream trips the `Send`
    // check on the spawned engine task.
    let fetches: Vec<Fut> = rules.iter().map(fetch).collect();
    futures_util::stream::iter(fetches)
        .buffered(concurrency.max(1))
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

    /// Fetches each rule while holding a background permit for `hold`, and
    /// returns how many were refused admission.
    async fn rejected_fetches(concurrency: usize) -> usize {
        use crate::query_governor::{QueryGovernor, QueryGovernorConfig, WorkloadClass};
        let mut config = QueryGovernorConfig::default();
        config.background.queue_timeout_ms = 10;
        let governor =
            QueryGovernor::new(config, Arc::new(crate::self_metrics::SelfMetrics::new())).unwrap();
        let rules: Vec<AnomalyRule> = (0..6).map(|i| test_rule(&i.to_string(), "apm")).collect();
        let results = prefetch(&rules, concurrency, |rule| {
            let governor = &governor;
            async move {
                let guard = governor
                    .admit(WorkloadClass::Background, &rule.tenant_id)
                    .await?;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                drop(guard);
                Ok::<_, crate::query_governor::AdmissionError>(rule.id.clone())
            }
        })
        .await;
        let admitted: Vec<&String> = results.iter().flatten().collect();
        if admitted.len() == rules.len() {
            assert_eq!(admitted, rules.iter().map(|r| &r.id).collect::<Vec<_>>());
        }
        results.iter().filter(|r| r.is_err()).count()
    }

    #[test]
    fn eval_log_rows_belong_to_the_rule_tenant() {
        // Without tenant_id the column default put every tenant's rows in `default`.
        let row = anomaly_log_tuple(
            &time::OffsetDateTime::now_utc(),
            &test_rule("r", "prometheus"),
            "anomalous",
            "up",
            1.0,
            2.0,
            3.0,
        );
        assert!(row.starts_with("('acme', "), "{row}");
    }

    #[tokio::test]
    async fn prefetch_stays_within_the_tenant_background_pool() {
        let per_tenant = crate::query_governor::QueryGovernorConfig::default()
            .background
            .per_tenant_concurrency as usize;
        // Starting every rule at once (the old join_all) overflowed the pool.
        assert!(rejected_fetches(6).await > 0);
        assert_eq!(rejected_fetches(per_tenant).await, 0);
    }

    #[tokio::test]
    async fn prometheus_rules_do_not_need_an_http_prom_endpoint() {
        // Only ClickHouse is reachable, no /prom endpoint. The old self-call to
        // /prom failed (as it did on auth-required tenants) and set the rule to
        // no_data; in-process PromQL reads the metric tables directly.
        let mock = clickhouse::test::Mock::new();
        mock.add(clickhouse::test::handlers::record_ddl());
        mock.add(clickhouse::test::handlers::record_ddl());
        let ch = Client::default().with_url(mock.url());
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
