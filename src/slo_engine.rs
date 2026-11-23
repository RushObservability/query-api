use crate::clickhouse_config::ConfigDb;
use crate::engine_metrics::{EvaluationObservation, EvaluationOutcome};
use crate::models::query::{Filter, FilterOp};
use crate::models::slo::{Slo, SloEvaluation};
use crate::models::slo_maintenance::{
    MaintenanceRange, SloMaintenanceEvaluation, exclusion_ranges,
};
use crate::query_builder::{QueryClauses, build_metrics_where_clause, build_where_clause};
use clickhouse::Client;
use std::sync::Arc;

/// Max SLOs evaluated concurrently per tick (bounds parallel CH data queries).
const ENGINE_CONCURRENCY: usize = 6;
/// Flush `last_eval_at` to the config table once per this many evals per SLO.
const EVAL_FLUSH_EVERY: u32 = 10;
/// Cap on each SLO data scan (rolling-30d windows can be large).
const MAX_EXECUTION_TIME: &str = "30";

#[derive(clickhouse::Row, serde::Deserialize)]
struct BudgetCounts {
    bad: f64,
    total: f64,
    excluded_bad: f64,
    excluded_total: f64,
}

struct EvaluationWindow<'a> {
    from: &'a str,
    to: &'a str,
    exclusions: &'a [MaintenanceRange],
}

impl EvaluationWindow<'_> {
    fn trace(&self, filters: &[Filter]) -> QueryClauses {
        build_where_clause(filters, self.from, self.to)
    }

    fn metric(&self, filters: &[Filter]) -> QueryClauses {
        build_metrics_where_clause(filters, self.from, self.to)
    }

    fn exclusion_predicate(&self, column: &str) -> String {
        if self.exclusions.is_empty() {
            return "0".to_string();
        }
        self.exclusions
            .iter()
            .map(|range| format!(
                "({column} >= fromUnixTimestamp64Milli({}, 'UTC') AND {column} < fromUnixTimestamp64Milli({}, 'UTC'))",
                range.start * 1000,
                range.end * 1000
            ))
            .collect::<Vec<_>>()
            .join(" OR ")
    }
}

/// Render QueryClauses as one predicate inside countIf/sumIf.
fn clauses_predicate(c: &QueryClauses) -> String {
    match (c.prewhere.is_empty(), c.where_clause.is_empty()) {
        (true, true) => "1".to_string(),
        (false, true) => format!("({})", c.prewhere),
        (true, false) => format!("({})", c.where_clause),
        (false, false) => format!("(({}) AND ({}))", c.prewhere, c.where_clause),
    }
}

async fn query_budget_counts(ch: &Client, tenant: &str, sql: &str) -> anyhow::Result<BudgetCounts> {
    Ok(crate::tenant_query(ch, sql, tenant)
        .with_setting("max_execution_time", MAX_EXECUTION_TIME)
        .fetch_one::<BudgetCounts>()
        .await?)
}

/// The same scan compares good/bad traffic, with and without maintenance.
/// Evaluating a single predicate for both sets includes each row at most once.
async fn eval_trace_availability(
    ch: &Client,
    tenant_id: &str,
    common_filters: &[Filter],
    error_filters: &[Filter],
    total_filters: &[Filter],
    window: &EvaluationWindow<'_>,
) -> anyhow::Result<BudgetCounts> {
    let excluded = window.exclusion_predicate("timestamp");
    let bad = clauses_predicate(&window.trace(error_filters));
    let total = clauses_predicate(&window.trace(total_filters));
    let base = window.trace(common_filters).to_sql();
    let sql = format!(
        "WITH ({excluded}) AS in_maintenance
         SELECT toFloat64(countIf({bad} AND NOT in_maintenance)) AS bad,
                toFloat64(countIf({total} AND NOT in_maintenance)) AS total,
                toFloat64(countIf({bad} AND in_maintenance)) AS excluded_bad,
                toFloat64(countIf({total} AND in_maintenance)) AS excluded_total
         FROM spans {base}"
    );
    query_budget_counts(ch, tenant_id, &sql).await
}

async fn eval_trace_latency(
    ch: &Client,
    tenant_id: &str,
    total_filters: &[Filter],
    threshold_ns: i64,
    window: &EvaluationWindow<'_>,
) -> anyhow::Result<BudgetCounts> {
    let excluded = window.exclusion_predicate("timestamp");
    let base = window.trace(total_filters).to_sql();
    let sql = format!(
        "WITH ({excluded}) AS in_maintenance
         SELECT toFloat64(countIf(duration_ns > {threshold_ns} AND NOT in_maintenance)) AS bad,
                toFloat64(countIf(NOT in_maintenance)) AS total,
                toFloat64(countIf(duration_ns > {threshold_ns} AND in_maintenance)) AS excluded_bad,
                toFloat64(countIf(in_maintenance)) AS excluded_total
         FROM spans {base}"
    );
    query_budget_counts(ch, tenant_id, &sql).await
}

// Metric budgets use raw samples. The minute/hour rollups cannot reconstruct a
// windowed SUM(Value), per-row histogram buckets, or gauge sample counts.
async fn eval_metric_availability(
    ch: &Client,
    tenant_id: &str,
    common_filters: &[Filter],
    error_filters: &[Filter],
    total_filters: &[Filter],
    window: &EvaluationWindow<'_>,
) -> anyhow::Result<BudgetCounts> {
    let excluded = window.exclusion_predicate("TimeUnix");
    let bad = clauses_predicate(&window.metric(error_filters));
    let total = clauses_predicate(&window.metric(total_filters));
    let base = window.metric(common_filters).to_sql();
    let sql = format!(
        "WITH ({excluded}) AS in_maintenance
         SELECT sumIf(Value, {bad} AND NOT in_maintenance) AS bad,
                sumIf(Value, {total} AND NOT in_maintenance) AS total,
                sumIf(Value, {bad} AND in_maintenance) AS excluded_bad,
                sumIf(Value, {total} AND in_maintenance) AS excluded_total
         FROM metrics_sum {base}"
    );
    query_budget_counts(ch, tenant_id, &sql).await
}

async fn eval_metric_latency(
    ch: &Client,
    tenant_id: &str,
    total_filters: &[Filter],
    threshold_ms: f64,
    window: &EvaluationWindow<'_>,
) -> anyhow::Result<BudgetCounts> {
    let excluded = window.exclusion_predicate("TimeUnix");
    let base = window.metric(total_filters).to_sql();
    // Preserve the original histogram bucket interpretation. Subtract in
    // Float64, so an inconsistent histogram can't underflow an unsigned sum.
    let sql = format!(
        "WITH ({excluded}) AS in_maintenance,
              arraySum(arrayMap(eb -> BucketCounts[indexOf(ExplicitBounds, eb)],
                  arrayFilter(eb -> eb <= {threshold_ms}, ExplicitBounds))) AS fast_count
         SELECT greatest(0., toFloat64(sumIf(Count, NOT in_maintenance)) -
                             toFloat64(sumIf(fast_count, NOT in_maintenance))) AS bad,
                toFloat64(sumIf(Count, NOT in_maintenance)) AS total,
                greatest(0., toFloat64(sumIf(Count, in_maintenance)) -
                             toFloat64(sumIf(fast_count, in_maintenance))) AS excluded_bad,
                toFloat64(sumIf(Count, in_maintenance)) AS excluded_total
         FROM metrics_histogram {base}"
    );
    query_budget_counts(ch, tenant_id, &sql).await
}

async fn eval_metric_threshold(
    ch: &Client,
    tenant_id: &str,
    total_filters: &[Filter],
    threshold_value: f64,
    threshold_op: &str,
    window: &EvaluationWindow<'_>,
) -> anyhow::Result<BudgetCounts> {
    let excluded = window.exclusion_predicate("TimeUnix");
    let base = window.metric(total_filters).to_sql();
    // threshold_op defines what counts as good.
    let violating_op = match threshold_op {
        "lte" => ">",
        "gt" => "<=",
        "gte" => "<",
        _ => ">=",
    };
    let sql = format!(
        "WITH ({excluded}) AS in_maintenance,
              (Value {violating_op} {threshold_value}) AS violating
         SELECT toFloat64(countIf(violating AND NOT in_maintenance)) AS bad,
                toFloat64(countIf(NOT in_maintenance)) AS total,
                toFloat64(countIf(violating AND in_maintenance)) AS excluded_bad,
                toFloat64(countIf(in_maintenance)) AS excluded_total
         FROM metrics_gauge {base}"
    );
    query_budget_counts(ch, tenant_id, &sql).await
}

async fn eval_metric_promql_pair(
    ch: &Client,
    tenant_id: &str,
    error_promql: &str,
    total_promql: &str,
    now: &chrono::DateTime<chrono::Utc>,
    lookback_secs: f64,
) -> anyhow::Result<BudgetCounts> {
    let eval_time = now.timestamp_millis() as f64 / 1_000.0;
    let (error_result, total_result) = tokio::join!(
        crate::promql::evaluate_instant_query(
            ch,
            error_promql,
            eval_time,
            lookback_secs,
            tenant_id,
        ),
        crate::promql::evaluate_instant_query(
            ch,
            total_promql,
            eval_time,
            lookback_secs,
            tenant_id,
        ),
    );
    let error_series =
        error_result.map_err(|error| anyhow::anyhow!("error PromQL failed: {error}"))?;
    let total_series =
        total_result.map_err(|error| anyhow::anyhow!("total PromQL failed: {error}"))?;
    let error_value = sum_latest(&error_series);
    let total_value = sum_latest(&total_series);

    if !error_value.is_finite() || !total_value.is_finite() {
        anyhow::bail!("PromQL returned a non-finite value");
    }
    if error_value < 0.0 || total_value < 0.0 {
        anyhow::bail!("PromQL SLO values must be non-negative");
    }
    if error_value > total_value {
        anyhow::bail!(
            "error PromQL value ({error_value}) exceeds total PromQL value ({total_value})"
        );
    }

    Ok(BudgetCounts {
        bad: error_value,
        total: total_value,
        excluded_bad: 0.0,
        excluded_total: 0.0,
    })
}

fn sum_latest(series: &[crate::promql::types::TimeSeries]) -> f64 {
    series
        .iter()
        .filter_map(|series| series.samples.last().map(|(_, value)| *value))
        .sum()
}

/// Write SLO gauge metrics to ClickHouse so they can be graphed over time.
/// Emits: rush_slo_current, rush_slo_error_budget_remaining, rush_slo_error_count,
///        rush_slo_total_count, rush_slo_compliant
async fn write_slo_metrics(
    ch: &Client,
    slo_id: &str,
    slo_name: &str,
    current_pct: f64,
    error_budget_remaining: f64,
    error_count: f64,
    total_count: f64,
    compliant: bool,
    now_nanos: i64,
) {
    if crate::leader::ensure_leader().is_err() {
        return;
    }
    let escaped_name = crate::query_builder::escape_string_literal(&slo_name);
    let attrs = format!("{{'slo.id': '{slo_id}', 'slo.name': '{escaped_name}'}}");
    let metrics = [
        ("rush_slo_current", current_pct),
        (
            "rush_slo_error_budget_remaining",
            error_budget_remaining * 100.0,
        ),
        ("rush_slo_error_count", error_count),
        ("rush_slo_total_count", total_count),
        ("rush_slo_compliant", if compliant { 1.0 } else { 0.0 }),
    ];
    let values: Vec<String> = metrics
        .iter()
        .map(|(name, val)| {
            format!(
                "({{}}, '', '', '', {{}}, 0, '', 'wide-slo-engine', '{name}', '', '', {attrs}, \
             {now_nanos}, {now_nanos}, {val}, 0, [], [], [], [], [])"
            )
        })
        .collect();
    let sql = format!(
        "INSERT INTO metrics_gauge \
         (ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, \
          ScopeDroppedAttrCount, ScopeSchemaUrl, ServiceName, MetricName, MetricDescription, \
          MetricUnit, Attributes, StartTimeUnix, TimeUnix, Value, Flags, \
          Exemplars.FilteredAttributes, Exemplars.TimeUnix, Exemplars.Value, \
          Exemplars.SpanId, Exemplars.TraceId) VALUES {}",
        values.join(", ")
    );
    if let Err(e) = ch.query(&sql).execute().await {
        tracing::warn!("slo metrics write failed: {e}");
    }
}

pub fn spawn_slo_engine(
    config_db: Arc<ConfigDb>,
    read_ch: Client,
    write_ch: Client,
    self_metrics: Arc<crate::self_metrics::SelfMetrics>,
    lease: Arc<crate::leader::EngineLease>,
    smtp_config: crate::alert_engine::SmtpConfig,
) {
    tokio::spawn(async move {
        let notifier = crate::alert_engine::Notifier::new(smtp_config);
        let mut eval_state = crate::eval_state::EvalState::new(EVAL_FLUSH_EVERY);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            if !lease.is_leader() {
                continue;
            }
            let start = std::time::Instant::now();
            let cycle = eval_slos(&config_db, &read_ch, &write_ch, &notifier, &mut eval_state);
            let ok = match crate::leader::fenced(lease.clone(), cycle).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::error!("slo engine error: {e}");
                    false
                }
            };
            self_metrics.record_engine("slo_engine", start.elapsed().as_millis() as u64, ok);
        }
    });
}

fn window_minutes(window_type: &str) -> i64 {
    match window_type {
        "rolling_1h" => 60,
        "rolling_24h" => 1440,
        "rolling_7d" => 10080,
        "rolling_30d" => 43200,
        _ => 60,
    }
}

/// Events describe an incident lifecycle, not every evaluation-state change.
/// A breach opens an incident and a compliant result closes it. `no_data`
/// remains a valid live SLO state but never opens or closes an incident.
fn should_record_incident_event(previous_event_state: Option<&str>, new_state: &str) -> bool {
    match new_state {
        "breaching" => previous_event_state != Some("breaching"),
        "compliant" => previous_event_state == Some("breaching"),
        _ => false,
    }
}

async fn eval_slos(
    config_db: &ConfigDb,
    read_ch: &Client,
    write_ch: &Client,
    notifier: &crate::alert_engine::Notifier,
    eval_state: &mut crate::eval_state::EvalState,
) -> anyhow::Result<()> {
    use futures_util::StreamExt;

    let now = chrono::Utc::now();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // DB-side due filter (coarse: last_eval_at is only flushed 1-in-N) combined
    // with the in-memory check ⇒ max(db, mem) + interval <= now semantics.
    let due_slos = config_db.get_due_slos(&now_str).await?;
    let jobs: Vec<(Slo, bool)> = due_slos
        .into_iter()
        .filter(|s| eval_state.is_due(&s.id, now, s.eval_interval_secs))
        .map(|s| {
            let flush = eval_state.should_flush(&s.id);
            (s, flush)
        })
        .collect();

    crate::engine_metrics::due(config_db.self_metrics(), "slo_engine", jobs.len());
    let now_str_ref = now_str.as_str();
    let outcomes: Vec<(String, bool, bool)> =
        futures_util::stream::iter(jobs.into_iter().map(|(slo, should_flush)| async move {
            let mut observation =
                EvaluationObservation::new(config_db.self_metrics(), "slo_engine");
            let evaluated = crate::query_governor::run_background(
                &slo.tenant_id,
                eval_one_slo(
                    config_db,
                    read_ch,
                    write_ch,
                    notifier,
                    &slo,
                    now,
                    now_str_ref,
                    should_flush,
                ),
            )
            .await;
            let (persisted, succeeded) = match evaluated {
                Ok(Ok(p)) => {
                    observation.finish(EvaluationOutcome::Completed);
                    (p, true)
                }
                Ok(Err(e)) => {
                    observation.finish(EvaluationOutcome::Error);
                    tracing::warn!("slo {}: evaluation error: {e}", slo.id);
                    (false, false)
                }
                Err(e) => {
                    observation.finish(EvaluationOutcome::AdmissionRejected);
                    tracing::warn!("slo {}: background admission rejected: {e:?}", slo.id);
                    (false, false)
                }
            };
            (slo.id, persisted, succeeded)
        }))
        .buffer_unordered(ENGINE_CONCURRENCY)
        .collect()
        .await;

    let mut failures = 0usize;
    for (id, persisted, succeeded) in outcomes {
        eval_state.record(id, now, persisted);
        if !succeeded {
            failures += 1;
        }
    }
    if failures > 0 {
        anyhow::bail!("{failures} SLO evaluation job(s) failed");
    }

    Ok(())
}

/// Persist the "no_data" outcome: immediately on a transition into no_data
/// (exactly as before), otherwise only on the coarse flush cadence.
async fn persist_no_data(
    config_db: &ConfigDb,
    slo: &Slo,
    now_str: &str,
    should_flush: bool,
) -> anyhow::Result<bool> {
    if slo.state != "no_data" || should_flush {
        return config_db
            .persist_slo_eval(
                slo,
                &SloEvaluation {
                    state: "no_data",
                    error_budget_remaining: 0.0,
                    error_count: 0.0,
                    total_count: 0.0,
                    maintenance: None,
                },
                now_str,
            )
            .await;
    }
    Ok(false)
}

/// Evaluate one SLO. Returns Ok(true) iff the SLO row was persisted to the
/// config table (state transition or coarse `last_eval_at` flush).
async fn eval_one_slo(
    config_db: &ConfigDb,
    read_ch: &Client,
    write_ch: &Client,
    notifier: &crate::alert_engine::Notifier,
    slo: &Slo,
    now: chrono::DateTime<chrono::Utc>,
    now_str: &str,
    should_flush: bool,
) -> anyhow::Result<bool> {
    let minutes = window_minutes(&slo.window_type);
    let start = now - chrono::Duration::minutes(minutes);
    let from = crate::models::maintenance::format_time(start);
    let ranges = exclusion_ranges(&slo.maintenance_windows, start.timestamp(), now.timestamp());
    let window = EvaluationWindow {
        from: &from,
        to: now_str,
        exclusions: &ranges,
    };
    // Publish the latest breakdown with its budget on each opt-in evaluation.
    // When a schedule is cleared, publish once before resuming coarse flushes.
    let schedule_pending = slo
        .maintenance_evaluation
        .as_ref()
        .map_or(0, |m| m.revision)
        != slo.maintenance_revision;
    let should_flush = should_flush || !slo.maintenance_windows.is_empty() || schedule_pending;

    let metric_promql_pair = if slo.slo_type == "metric"
        && matches!(slo.indicator_type.as_str(), "availability" | "latency")
    {
        match (
            crate::models::slo::stored_promql(&slo.error_filters),
            crate::models::slo::stored_promql(&slo.total_filters),
        ) {
            (Some(error), Some(total)) => Some((error, total)),
            _ => None,
        }
    } else {
        None
    };

    // PromQL-backed metric SLOs store query objects in these legacy columns so
    // existing ClickHouse deployments do not need a config-table rewrite.
    let mut error_filters: Vec<Filter> = if metric_promql_pair.is_some() {
        Vec::new()
    } else {
        match serde_json::from_str(&slo.error_filters) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("slo {}: bad error_filters: {e}", slo.id);
                return Ok(false);
            }
        }
    };
    let mut total_filters: Vec<Filter> = if metric_promql_pair.is_some() {
        Vec::new()
    } else {
        match serde_json::from_str(&slo.total_filters) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("slo {}: bad total_filters: {e}", slo.id);
                return Ok(false);
            }
        }
    };

    // tenant_id scopes EVERY evaluation query: telemetry tables (spans / metrics_*)
    // all carry a tenant_id column, and an SLO must only ever see its own tenant's
    // signals. Injected first into every filter list so it lands in both the pruning
    // base scan and each countIf/sumIf predicate. `tenant_id` is a safe column name
    // and the String value is escaped by format_value (build_where_clause), yielding
    // `tenant_id = '<escaped>'` — no unscoped query can run.
    let tenant_filter = Filter {
        field: "tenant_id".to_string(),
        op: FilterOp::Eq,
        value: serde_json::Value::String(slo.tenant_id.clone()),
    };
    let mut common_filters: Vec<Filter> = vec![tenant_filter.clone()];
    error_filters.insert(0, tenant_filter.clone());
    total_filters.insert(0, tenant_filter);

    // service_name scopes every evaluation query. The availability evaluators take
    // it separately (it prunes the single combined scan); the single-filter-set
    // evaluators get it injected into their filter list as before.
    if !slo.service_name.is_empty() {
        let sn_filter = Filter {
            field: "service_name".to_string(),
            op: FilterOp::Eq,
            value: serde_json::Value::String(slo.service_name.clone()),
        };
        common_filters.push(sn_filter.clone());
        error_filters.insert(0, sn_filter.clone());
        total_filters.insert(0, sn_filter);
    }

    // Evaluate based on (slo_type, indicator_type) — each is a single scan.
    let eval_result = match (slo.slo_type.as_str(), slo.indicator_type.as_str()) {
        ("trace", "availability") => {
            eval_trace_availability(
                read_ch,
                &slo.tenant_id,
                &common_filters,
                &error_filters,
                &total_filters,
                &window,
            )
            .await
        }
        ("trace", "latency") => {
            let threshold_ns = (slo.threshold_ms.unwrap_or(0.0) * 1_000_000.0) as i64;
            eval_trace_latency(
                read_ch,
                &slo.tenant_id,
                &total_filters,
                threshold_ns,
                &window,
            )
            .await
        }
        ("metric", "availability") => {
            if let Some((error_promql, total_promql)) = &metric_promql_pair {
                eval_metric_promql_pair(
                    read_ch,
                    &slo.tenant_id,
                    error_promql,
                    total_promql,
                    &now,
                    minutes as f64 * 60.0,
                )
                .await
            } else {
                eval_metric_availability(
                    read_ch,
                    &slo.tenant_id,
                    &common_filters,
                    &error_filters,
                    &total_filters,
                    &window,
                )
                .await
            }
        }
        ("metric", "latency") => {
            if let Some((error_promql, total_promql)) = &metric_promql_pair {
                eval_metric_promql_pair(
                    read_ch,
                    &slo.tenant_id,
                    error_promql,
                    total_promql,
                    &now,
                    minutes as f64 * 60.0,
                )
                .await
            } else {
                let threshold_ms = slo.threshold_ms.unwrap_or(0.0);
                eval_metric_latency(
                    read_ch,
                    &slo.tenant_id,
                    &total_filters,
                    threshold_ms,
                    &window,
                )
                .await
            }
        }
        ("metric", "threshold") => {
            if !slo.metric_name.is_empty() {
                total_filters.push(Filter {
                    field: "metric_name".into(),
                    op: FilterOp::Eq,
                    value: serde_json::Value::String(slo.metric_name.clone()),
                });
            }
            let threshold_value = slo.threshold_value.unwrap_or(0.0);
            let threshold_op = slo.threshold_op.as_deref().unwrap_or("lt");
            eval_metric_threshold(
                read_ch,
                &slo.tenant_id,
                &total_filters,
                threshold_value,
                threshold_op,
                &window,
            )
            .await
        }
        _ => {
            tracing::warn!(
                "slo {}: unsupported type/indicator: {}/{}",
                slo.id,
                slo.slo_type,
                slo.indicator_type
            );
            return persist_no_data(config_db, slo, now_str, should_flush).await;
        }
    };

    let counts = match eval_result {
        Ok(counts) => counts,
        Err(e) => {
            tracing::warn!("slo {}: evaluation failed: {e}", slo.id);
            return persist_no_data(config_db, slo, now_str, should_flush).await;
        }
    };

    let error_count = counts.bad;
    let total_count = counts.total;
    let maintenance = (slo.maintenance_revision != 0).then(|| {
        SloMaintenanceEvaluation::new(
            slo.maintenance_revision,
            &ranges,
            now.timestamp(),
            counts.excluded_total,
            counts.excluded_bad,
        )
    });

    // Calculate error budget
    // error_budget = 1 - target/100 (allowed error rate)
    // consumed = error_count / total_count (actual error rate)
    // remaining = error_budget - consumed
    let (new_state, error_budget_remaining) = if total_count <= 0.0 {
        ("no_data", 0.0_f64)
    } else {
        let error_budget = 1.0 - slo.target_percentage / 100.0;
        let consumed = error_count / total_count;
        // Express remaining as fraction of the allowed budget (1.0 = 100% remaining, 0 = exhausted).
        let remaining = if error_budget <= 0.0 {
            0.0
        } else {
            (error_budget - consumed) / error_budget
        };
        let state = if remaining > 0.0 {
            "compliant"
        } else {
            "breaching"
        };
        (state, remaining)
    };

    let old_state = slo.state.as_str();
    let evaluation = SloEvaluation {
        state: new_state,
        error_budget_remaining,
        error_count,
        total_count,
        maintenance,
    };
    let persisted;

    if new_state != old_state {
        // Persist every live state transition, but only write history for the
        // incident lifecycle: breach opened, then recovered. This prevents
        // intermittent no-data windows from producing repeated compliant rows.
        let previous_event_state = if matches!(new_state, "breaching" | "compliant") {
            config_db
                .latest_slo_event_state(&slo.id, &slo.tenant_id)
                .await?
        } else {
            None
        };
        let record_event = should_record_incident_event(previous_event_state.as_deref(), new_state);

        let message = record_event.then(|| {
            format!(
                "SLO '{}': {} (errors={}, total={}, budget_remaining={:.4}%)",
                slo.name,
                if new_state == "breaching" {
                    "BREACHING"
                } else {
                    "COMPLIANT"
                },
                error_count,
                total_count,
                error_budget_remaining * 100.0,
            )
        });

        if let Some(message) = &message {
            if !config_db
                .get_slo(&slo.id, &slo.tenant_id)
                .await?
                .is_some_and(|current| current.version == slo.version)
            {
                return Ok(false);
            }
            config_db
                .create_slo_event(
                    &uuid::Uuid::new_v4().to_string(),
                    &slo.id,
                    &slo.tenant_id,
                    new_state,
                    error_count,
                    total_count,
                    error_budget_remaining,
                    message,
                )
                .await?;
        }

        if !config_db
            .persist_slo_eval(slo, &evaluation, now_str)
            .await?
        {
            return Ok(false);
        }

        // Notifications follow the same incident lifecycle as history.
        if let Some(message) = &message {
            let channel_ids: Vec<String> =
                serde_json::from_str(&slo.notification_channel_ids).unwrap_or_default();
            // Webhook receivers written for the old SLO payload keep their fields,
            // including `state` as breaching/compliant.
            let webhook_fields = serde_json::json!({
                "slo": slo.name,
                "state": new_state,
                "error_count": error_count,
                "total_count": total_count,
                "error_budget_remaining": error_budget_remaining,
            });
            notifier
                .notify(
                    config_db,
                    &slo.tenant_id,
                    &channel_ids,
                    &crate::alert_engine::ChannelNotification {
                        source: "slo",
                        signal_type: "slo",
                        alert_name: &slo.name,
                        message,
                        alert_state: if new_state == "breaching" {
                            "alert"
                        } else {
                            "ok"
                        },
                        value: error_budget_remaining * 100.0,
                        threshold: 0.0,
                        condition_op: "",
                        description: "",
                        alert_id: &slo.id,
                        incident_key: &format!("slo:{}", slo.id),
                        runbook_url: "",
                        webhook_fields: Some(&webhook_fields),
                    },
                )
                .await;
        }

        tracing::info!("slo '{}' state: {} -> {}", slo.name, old_state, new_state);
        persisted = true;
    } else if should_flush {
        // Publish the result and exclusion breakdown together.
        persisted = config_db
            .persist_slo_eval(slo, &evaluation, now_str)
            .await?;
    } else {
        persisted = false;
    }

    // Write SLO gauge metrics for graphing
    let current_pct = if total_count > 0.0 {
        ((total_count - error_count) / total_count) * 100.0
    } else {
        0.0
    };
    let now_nanos = now.timestamp_nanos_opt().unwrap_or(0);
    write_slo_metrics(
        write_ch,
        &slo.id,
        &slo.name,
        current_pct,
        error_budget_remaining,
        error_count,
        total_count,
        new_state == "compliant",
        now_nanos,
    )
    .await;

    Ok(persisted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn incident_events_only_open_on_breach_and_close_on_recovery() {
        assert!(should_record_incident_event(None, "breaching"));
        assert!(!should_record_incident_event(
            Some("breaching"),
            "breaching"
        ));
        assert!(should_record_incident_event(Some("breaching"), "compliant"));
        assert!(!should_record_incident_event(None, "compliant"));
        assert!(!should_record_incident_event(
            Some("compliant"),
            "compliant"
        ));
        assert!(!should_record_incident_event(Some("breaching"), "no_data"));
    }

    fn f(field: &str, val: &str) -> Filter {
        Filter {
            field: field.to_string(),
            op: FilterOp::Eq,
            value: serde_json::Value::String(val.to_string()),
        }
    }

    #[tokio::test]
    #[ignore = "requires isolated ClickHouse at RUSH_TEST_CLICKHOUSE_URL with rush_ custom settings"]
    async fn maintenance_removes_overlapping_requests_once_from_both_counts() -> anyhow::Result<()>
    {
        let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
        let database = format!("slo_maintenance_{}", uuid::Uuid::new_v4().simple());
        let client = Client::default().with_url(url).with_validation(false);
        client
            .query(&format!("CREATE DATABASE {database}"))
            .execute()
            .await?;
        let ch = client.clone().with_database(&database);
        let result = async {
            ch.query("CREATE TABLE spans (
                tenant_id String, timestamp DateTime64(9, 'UTC'), service_name String,
                status String, http_status_code UInt16
            ) ENGINE = MergeTree ORDER BY (tenant_id, timestamp)").execute().await?;
            ch.query("INSERT INTO spans (tenant_id, timestamp, service_name, status) VALUES
                ('default', '2026-11-22 09:01:29.999', 'checkout', 'STATUS_CODE_OK'),
                ('default', '2026-11-22 09:01:30.000', 'checkout', 'STATUS_CODE_ERROR'),
                ('default', '2026-11-22 09:01:45.000', 'checkout', 'STATUS_CODE_ERROR'),
                ('default', '2026-11-22 09:03:29.999', 'checkout', 'STATUS_CODE_OK'),
                ('default', '2026-11-22 09:03:30.000', 'checkout', 'STATUS_CODE_ERROR'),
                ('default', '2026-11-22 09:04:01.000', 'checkout', 'STATUS_CODE_OK'),
                ('other', '2026-11-22 09:02:00.000', 'checkout', 'STATUS_CODE_ERROR')
            ").execute().await?;
            let from = "2026-11-22T09:00:00Z";
            let to = "2026-11-22T09:05:00Z";
            let windows = serde_json::from_value::<Vec<_>>(serde_json::json!([
                {"name":"First","starts_at":"2026-11-22T09:01:30Z","ends_at":"2026-11-22T09:02:20Z"},
                {"name":"Second","starts_at":"2026-11-22T09:01:40Z","ends_at":"2026-11-22T09:03:30Z"}
            ]))?;
            let parse = |utc| crate::models::maintenance::normalize_time(utc).map(|t| t.timestamp()).unwrap();
            let ranges = exclusion_ranges(&windows, parse(from), parse(to));
            let window = EvaluationWindow { from, to, exclusions: &ranges };
            let total = [f("tenant_id", "default"), f("service_name", "checkout")];
            let mut errors = total.to_vec();
            errors.push(f("status", "STATUS_CODE_ERROR"));
            let counts = eval_trace_availability(&ch, "default", &total, &errors, &total, &window).await?;
            assert_eq!((counts.bad, counts.total), (1.0, 3.0));
            assert_eq!((counts.excluded_bad, counts.excluded_total), (2.0, 3.0));
            anyhow::Ok(())
        }.await;
        client
            .query(&format!("DROP DATABASE {database}"))
            .execute()
            .await?;
        result
    }

    #[test]
    fn clauses_predicate_combines_prewhere_and_where() {
        let c = QueryClauses {
            prewhere: "a = 1".into(),
            where_clause: "b = 2".into(),
        };
        assert_eq!(clauses_predicate(&c), "((a = 1) AND (b = 2))");
        let p = QueryClauses {
            prewhere: "a = 1".into(),
            where_clause: String::new(),
        };
        assert_eq!(clauses_predicate(&p), "(a = 1)");
        let w = QueryClauses {
            prewhere: String::new(),
            where_clause: "b = 2".into(),
        };
        assert_eq!(clauses_predicate(&w), "(b = 2)");
        let e = QueryClauses {
            prewhere: String::new(),
            where_clause: String::new(),
        };
        assert_eq!(clauses_predicate(&e), "1");
    }

    // Regression for R3-Q4: the availability predicates must fully restate each
    // side's conditions so countIf counts are identical to the former standalone
    // scans, while the base scan carries the shared service/time pruning.
    #[test]
    fn availability_predicates_restate_each_side() {
        let common = vec![f("service_name", "payments")];
        let err = vec![f("service_name", "payments"), f("status", "ERROR")];
        let base = build_where_clause(&common, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z");
        let err_clauses = build_where_clause(&err, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z");
        let pred = clauses_predicate(&err_clauses);
        assert!(
            pred.contains("'ERROR'"),
            "error predicate must include error condition: {pred}"
        );
        assert!(
            pred.contains("payments"),
            "error predicate restates common filter: {pred}"
        );
        assert!(
            base.to_sql().contains("payments"),
            "base scan pruned by service: {}",
            base.to_sql()
        );
    }

    #[test]
    fn promql_pair_sums_latest_value_from_each_series() {
        let series = vec![
            crate::promql::types::TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(1.0, 2.5), (2.0, 3.5)],
            },
            crate::promql::types::TimeSeries {
                labels: BTreeMap::new(),
                samples: vec![(2.0, 1.25)],
            },
        ];
        assert_eq!(sum_latest(&series), 4.75);
    }
}
