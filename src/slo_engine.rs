use std::sync::Arc;
use crate::clickhouse_config::ConfigDb;
use crate::models::query::{Filter, FilterOp};
use crate::query_builder::{build_where_clause, build_metrics_where_clause, QueryClauses};
use clickhouse::Client;

/// Max SLOs evaluated concurrently per tick (bounds parallel CH data queries).
const ENGINE_CONCURRENCY: usize = 6;
/// Flush `last_eval_at` to the config table once per this many evals per SLO.
const EVAL_FLUSH_EVERY: u32 = 10;
/// Cap on each SLO data scan (rolling-30d windows can be large).
const MAX_EXECUTION_TIME: &str = "30";

#[derive(clickhouse::Row, serde::Deserialize)]
struct BadTotalRow {
    bad: u64,
    total: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct SumBadTotalRow {
    bad: f64,
    total: f64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct HistTotalFastRow {
    total: f64,
    fast: f64,
}

/// Render QueryClauses as a single boolean predicate usable inside countIf/sumIf.
fn clauses_predicate(c: &QueryClauses) -> String {
    match (c.prewhere.is_empty(), c.where_clause.is_empty()) {
        (true, true) => "1".to_string(),
        (false, true) => format!("({})", c.prewhere),
        (true, false) => format!("({})", c.where_clause),
        (false, false) => format!("(({}) AND ({}))", c.prewhere, c.where_clause),
    }
}

/// trace + availability: COUNT errors / COUNT total on spans — ONE scan.
///
/// The scan is pruned by the conditions common to both sides (time range + the
/// injected service_name filter); the error/total filter sets become countIf
/// predicates. Each predicate re-states the time range (redundant inside the
/// pruned scan, but keeps each count exactly equal to its former standalone query).
async fn eval_trace_availability(
    ch: &Client,
    common_filters: &[Filter],
    error_filters: &[Filter],
    total_filters: &[Filter],
    from: &str,
    to: &str,
) -> anyhow::Result<(i64, i64)> {
    let base = build_where_clause(common_filters, from, to);
    let error_clauses = build_where_clause(error_filters, from, to);
    let total_clauses = build_where_clause(total_filters, from, to);
    let sql = format!(
        "SELECT countIf({}) as bad, countIf({}) as total FROM spans {}",
        clauses_predicate(&error_clauses),
        clauses_predicate(&total_clauses),
        base.to_sql(),
    );
    let row = ch.query(&sql)
        .with_option("max_execution_time", MAX_EXECUTION_TIME)
        .fetch_one::<BadTotalRow>().await?;
    Ok((row.bad as i64, row.total as i64))
}

/// trace + latency: COUNT(duration_ns > threshold) / COUNT total on spans — ONE scan.
/// The slow set is a subset of the total set by construction, so a single
/// countIf over the total WHERE is exactly the former two counts.
async fn eval_trace_latency(
    ch: &Client,
    total_filters: &[Filter],
    threshold_ns: i64,
    from: &str,
    to: &str,
) -> anyhow::Result<(i64, i64)> {
    let total_clauses = build_where_clause(total_filters, from, to);
    let sql = format!(
        "SELECT countIf(duration_ns > {threshold_ns}) as bad, count() as total FROM spans {}",
        total_clauses.to_sql(),
    );
    let row = ch.query(&sql)
        .with_option("max_execution_time", MAX_EXECUTION_TIME)
        .fetch_one::<BadTotalRow>().await?;
    Ok((row.bad as i64, row.total as i64))
}

// ─────────────────────────────────────────────────────────────────────────────
// Metric SLO evaluators: ALL stay on RAW (no rollup), deliberately.
//
// The 1m/1h metric rollups store per-bucket avg/min/max/last/count (gauge) and
// last/min/max/count (sum). The three metric SLO evaluators below each need something
// the rollups cannot reproduce without approximation, so per the correctness mandate
// they are NOT rolled up:
//
//   * eval_metric_availability — `sumIf(Value, pred)` over metrics_sum: the arithmetic
//     SUM of all sample Values in the window. The sum rollup intentionally stores
//     last/min/max/count (a counter's instant value), NOT sum-of-values, so a windowed
//     sumIf is unrecoverable. (Storing sum-of-values for a monotonic counter would also
//     be meaningless.)
//   * eval_metric_latency — histogram-bucket math on metrics_histogram. Histograms are
//     not rolled up at all (only gauge + sum are), and per-bucket counts can't be
//     derived from scalar aggregates.
//   * eval_metric_threshold — `countIf(Value <op> threshold)` over metrics_gauge: the
//     number of individual samples crossing a threshold. avg/min/max/last/count per
//     bucket cannot tell you how many of the underlying samples exceeded a value
//     (that's a per-sample predicate), so this is unrecoverable from the rollup.
//
// These are long rolling-window scans (up to 30d), so they are the queries that would
// benefit most — but a wrong availability ratio is far worse than a slow one. They read
// raw, scoped by tenant + service_name + the SLO filters, with max_execution_time capped.
// ─────────────────────────────────────────────────────────────────────────────

/// metric + availability: SUM(Value) error / SUM(Value) total on metrics_sum — ONE scan,
/// same predicate-pushdown shape as trace availability but with sumIf.
///
/// Stays on RAW (see the block comment above): a windowed sumIf(Value) is not derivable
/// from the sum rollup's last/min/max/count aggregates.
async fn eval_metric_availability(
    ch: &Client,
    common_filters: &[Filter],
    error_filters: &[Filter],
    total_filters: &[Filter],
    from: &str,
    to: &str,
) -> anyhow::Result<(i64, i64)> {
    let base = build_metrics_where_clause(common_filters, from, to);
    let error_clauses = build_metrics_where_clause(error_filters, from, to);
    let total_clauses = build_metrics_where_clause(total_filters, from, to);
    let sql = format!(
        "SELECT sumIf(Value, {}) as bad, sumIf(Value, {}) as total FROM metrics_sum {}",
        clauses_predicate(&error_clauses),
        clauses_predicate(&total_clauses),
        base.to_sql(),
    );
    let row = ch.query(&sql)
        .with_option("max_execution_time", MAX_EXECUTION_TIME)
        .fetch_one::<SumBadTotalRow>().await?;
    Ok((row.bad as i64, row.total as i64))
}

/// metric + latency: histogram bucket query on metrics_histogram — ONE scan.
///
/// The former second scan (`… ARRAY JOIN ExplicitBounds AS eb WHERE eb <= T`)
/// is replaced by an arrayFilter/arrayMap expression evaluated per row in the
/// same scan: for each bound element <= threshold, add BucketCounts at that
/// bound's (first) index — element-for-element identical to the ARRAY JOIN
/// + indexOf evaluation, including the duplicate-bound edge case.
async fn eval_metric_latency(
    ch: &Client,
    total_filters: &[Filter],
    threshold_ms: f64,
    from: &str,
    to: &str,
) -> anyhow::Result<(i64, i64)> {
    let clauses = build_metrics_where_clause(total_filters, from, to);

    // Histogram ExplicitBounds are in the metric's unit; we pass threshold_ms directly.
    // toFloat64 keeps the wire types stable (Count/BucketCounts are UInt64).
    let sql = format!(
        "SELECT toFloat64(sum(Count)) as total, \
         toFloat64(sum(arraySum(arrayMap(eb -> BucketCounts[indexOf(ExplicitBounds, eb)], \
         arrayFilter(eb -> eb <= {threshold_ms}, ExplicitBounds))))) as fast \
         FROM metrics_histogram {}",
        clauses.to_sql(),
    );
    let row = ch.query(&sql)
        .with_option("max_execution_time", MAX_EXECUTION_TIME)
        .fetch_one::<HistTotalFastRow>().await?;

    let total_count = row.total as i64;
    let fast_count = row.fast as i64;

    // slow_count = total - fast (error count for budget)
    let slow_count = total_count - fast_count;
    Ok((slow_count.max(0), total_count))
}

/// metric + threshold: COUNT violating / COUNT total on metrics_gauge — ONE scan.
/// Both former scans used the exact same clauses, so countIf over them is identical.
/// threshold_op defines what "good" means, so violating = NOT good.
async fn eval_metric_threshold(
    ch: &Client,
    total_filters: &[Filter],
    threshold_value: f64,
    threshold_op: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<(i64, i64)> {
    let clauses = build_metrics_where_clause(total_filters, from, to);

    // "good" condition based on threshold_op (what good means)
    // Violating = NOT good
    let violating_op = match threshold_op {
        "lt" => format!("Value >= {threshold_value}"),   // good = Value < threshold
        "lte" => format!("Value > {threshold_value}"),   // good = Value <= threshold
        "gt" => format!("Value <= {threshold_value}"),   // good = Value > threshold
        "gte" => format!("Value < {threshold_value}"),   // good = Value >= threshold
        _ => format!("Value >= {threshold_value}"),
    };

    let sql = format!(
        "SELECT countIf({violating_op}) as bad, count() as total FROM metrics_gauge {}",
        clauses.to_sql(),
    );
    let row = ch.query(&sql)
        .with_option("max_execution_time", MAX_EXECUTION_TIME)
        .fetch_one::<BadTotalRow>().await?;
    Ok((row.bad as i64, row.total as i64))
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
    error_count: i64,
    total_count: i64,
    compliant: bool,
    now_nanos: i64,
) {
    let escaped_name = crate::query_builder::escape_string_literal(&slo_name);
    let attrs = format!(
        "{{'slo.id': '{slo_id}', 'slo.name': '{escaped_name}'}}"
    );
    let metrics = [
        ("rush_slo_current", current_pct),
        ("rush_slo_error_budget_remaining", error_budget_remaining * 100.0),
        ("rush_slo_error_count", error_count as f64),
        ("rush_slo_total_count", total_count as f64),
        ("rush_slo_compliant", if compliant { 1.0 } else { 0.0 }),
    ];
    let values: Vec<String> = metrics.iter().map(|(name, val)| {
        format!(
            "({{}}, '', '', '', {{}}, 0, '', 'wide-slo-engine', '{name}', '', '', {attrs}, \
             {now_nanos}, {now_nanos}, {val}, 0, [], [], [], [], [])"
        )
    }).collect();
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

pub fn spawn_slo_engine(config_db: Arc<ConfigDb>, ch: Client) {
    tokio::spawn(async move {
        let http_client = reqwest::Client::new();
        let mut eval_state = crate::eval_state::EvalState::new(EVAL_FLUSH_EVERY);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            if let Err(e) = eval_slos(&config_db, &ch, &http_client, &mut eval_state).await {
                tracing::error!("slo engine error: {e}");
            }
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

async fn eval_slos(
    config_db: &ConfigDb,
    ch: &Client,
    http_client: &reqwest::Client,
    eval_state: &mut crate::eval_state::EvalState,
) -> anyhow::Result<()> {
    use futures_util::StreamExt;

    let now = chrono::Utc::now();
    let now_str = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // DB-side due filter (coarse: last_eval_at is only flushed 1-in-N) combined
    // with the in-memory check ⇒ max(db, mem) + interval <= now semantics.
    let due_slos = config_db.get_due_slos(&now_str).await?;
    let jobs: Vec<(crate::models::slo::Slo, bool)> = due_slos
        .into_iter()
        .filter(|s| eval_state.is_due(&s.id, now, s.eval_interval_secs))
        .map(|s| {
            let flush = eval_state.should_flush(&s.id);
            (s, flush)
        })
        .collect();

    let now_str_ref = now_str.as_str();
    let outcomes: Vec<(String, bool)> = futures_util::stream::iter(jobs.into_iter().map(|(slo, should_flush)| async move {
        let persisted = match eval_one_slo(config_db, ch, http_client, &slo, now, now_str_ref, should_flush).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("slo {}: evaluation error: {e}", slo.id);
                false
            }
        };
        (slo.id, persisted)
    }))
    .buffer_unordered(ENGINE_CONCURRENCY)
    .collect()
    .await;

    for (id, persisted) in outcomes {
        eval_state.record(id, now, persisted);
    }

    Ok(())
}

/// Persist the "no_data" outcome: immediately on a transition into no_data
/// (exactly as before), otherwise only on the coarse flush cadence.
async fn persist_no_data(
    config_db: &ConfigDb,
    slo: &crate::models::slo::Slo,
    now_str: &str,
    should_flush: bool,
) -> anyhow::Result<bool> {
    if slo.state != "no_data" {
        config_db.update_slo_state(&slo.id, "no_data", 0.0, 0, 0, now_str, None).await?;
        return Ok(true);
    }
    if should_flush {
        config_db.persist_slo_eval(slo, "no_data", 0.0, 0, 0, now_str).await?;
        return Ok(true);
    }
    Ok(false)
}

/// Evaluate one SLO. Returns Ok(true) iff the SLO row was persisted to the
/// config table (state transition or coarse `last_eval_at` flush).
async fn eval_one_slo(
    config_db: &ConfigDb,
    ch: &Client,
    http_client: &reqwest::Client,
    slo: &crate::models::slo::Slo,
    now: chrono::DateTime<chrono::Utc>,
    now_str: &str,
    should_flush: bool,
) -> anyhow::Result<bool> {
    let minutes = window_minutes(&slo.window_type);
    let from = (now - chrono::Duration::minutes(minutes))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

    // Parse filters
    let mut error_filters: Vec<Filter> = match serde_json::from_str(&slo.error_filters) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("slo {}: bad error_filters: {e}", slo.id);
            return Ok(false);
        }
    };
    let mut total_filters: Vec<Filter> = match serde_json::from_str(&slo.total_filters) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("slo {}: bad total_filters: {e}", slo.id);
            return Ok(false);
        }
    };

    // service_name scopes every evaluation query. The availability evaluators take
    // it separately (it prunes the single combined scan); the single-filter-set
    // evaluators get it injected into their filter list as before.
    let mut common_filters: Vec<Filter> = Vec::new();
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
            eval_trace_availability(ch, &common_filters, &error_filters, &total_filters, &from, now_str).await
        }
        ("trace", "latency") => {
            let threshold_ns = (slo.threshold_ms.unwrap_or(0.0) * 1_000_000.0) as i64;
            eval_trace_latency(ch, &total_filters, threshold_ns, &from, now_str).await
        }
        ("metric", "availability") => {
            eval_metric_availability(ch, &common_filters, &error_filters, &total_filters, &from, now_str).await
        }
        ("metric", "latency") => {
            let threshold_ms = slo.threshold_ms.unwrap_or(0.0);
            eval_metric_latency(ch, &total_filters, threshold_ms, &from, now_str).await
        }
        ("metric", "threshold") => {
            let threshold_value = slo.threshold_value.unwrap_or(0.0);
            let threshold_op = slo.threshold_op.as_deref().unwrap_or("lt");
            eval_metric_threshold(ch, &total_filters, threshold_value, threshold_op, &from, now_str).await
        }
        _ => {
            tracing::warn!("slo {}: unsupported type/indicator: {}/{}", slo.id, slo.slo_type, slo.indicator_type);
            return persist_no_data(config_db, slo, now_str, should_flush).await;
        }
    };

    let (error_count, total_count) = match eval_result {
        Ok(counts) => counts,
        Err(e) => {
            tracing::warn!("slo {}: evaluation failed: {e}", slo.id);
            return persist_no_data(config_db, slo, now_str, should_flush).await;
        }
    };

    // Calculate error budget
    // error_budget = 1 - target/100 (allowed error rate)
    // consumed = error_count / total_count (actual error rate)
    // remaining = error_budget - consumed
    let (new_state, error_budget_remaining) = if total_count == 0 {
        ("no_data", 0.0_f64)
    } else {
        let error_budget = 1.0 - slo.target_percentage / 100.0;
        let consumed = error_count as f64 / total_count as f64;
        // Express remaining as fraction of the allowed budget (1.0 = 100% remaining, 0 = exhausted).
        let remaining = if error_budget <= 0.0 {
            0.0
        } else {
            (error_budget - consumed) / error_budget
        };
        let state = if remaining > 0.0 { "compliant" } else { "breaching" };
        (state, remaining)
    };

    let old_state = slo.state.as_str();
    let persisted;

    if new_state != old_state {
        // State changed — record event, persist immediately, and notify.
        let event_id = uuid::Uuid::new_v4().to_string();
        let message = format!(
            "SLO '{}': {} (errors={}, total={}, budget_remaining={:.4}%)",
            slo.name,
            match new_state {
                "breaching" => "BREACHING",
                "compliant" => "COMPLIANT",
                _ => "NO_DATA",
            },
            error_count,
            total_count,
            error_budget_remaining * 100.0,
        );

        config_db.create_slo_event(
            &event_id,
            &slo.id,
            new_state,
            error_count,
            total_count,
            error_budget_remaining,
            &message,
        ).await?;

        let breached_at = if new_state == "breaching" { Some(now_str) } else { None };
        config_db.update_slo_state(
            &slo.id, new_state, error_budget_remaining,
            error_count, total_count, now_str, breached_at,
        ).await?;

        // Send notifications
        let channel_ids: Vec<String> = serde_json::from_str(&slo.notification_channel_ids)
            .unwrap_or_default();
        for channel_id in &channel_ids {
            if let Ok(Some(channel)) = config_db.get_channel_by_id(channel_id).await {
                let config: serde_json::Value = serde_json::from_str(&channel.config)
                    .unwrap_or(serde_json::json!({}));
                if let Some(url) = config.get("url").and_then(|u| u.as_str()) {
                    let payload = match channel.channel_type.as_str() {
                        "slack" => serde_json::json!({ "text": message }),
                        _ => serde_json::json!({
                            "slo": slo.name,
                            "state": new_state,
                            "error_count": error_count,
                            "total_count": total_count,
                            "error_budget_remaining": error_budget_remaining,
                            "message": message,
                        }),
                    };
                    if let Err(e) = http_client.post(url).json(&payload).send().await {
                        tracing::warn!("slo {}: notification to {} failed: {e}", slo.id, channel.name);
                    }
                }
            }
        }

        tracing::info!("slo '{}' state: {} -> {}", slo.name, old_state, new_state);
        persisted = true;
    } else if should_flush {
        // No transition: coarse flush of last_eval_at + freshest budget numbers
        // from the row we already hold (no SELECT…FINAL re-read). Between
        // flushes the persisted budget/counts lag by up to EVAL_FLUSH_EVERY
        // evals; the gauge metrics below are still written every eval, so
        // graphs stay fresh.
        config_db.persist_slo_eval(
            slo, new_state, error_budget_remaining,
            error_count, total_count, now_str,
        ).await?;
        persisted = true;
    } else {
        persisted = false;
    }

    // Write SLO gauge metrics for graphing
    let current_pct = if total_count > 0 {
        ((total_count - error_count) as f64 / total_count as f64) * 100.0
    } else {
        0.0
    };
    let now_nanos = now.timestamp_nanos_opt().unwrap_or(0);
    write_slo_metrics(
        ch, &slo.id, &slo.name,
        current_pct, error_budget_remaining,
        error_count, total_count,
        new_state == "compliant",
        now_nanos,
    ).await;

    Ok(persisted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(field: &str, val: &str) -> Filter {
        Filter {
            field: field.to_string(),
            op: FilterOp::Eq,
            value: serde_json::Value::String(val.to_string()),
        }
    }

    #[test]
    fn clauses_predicate_combines_prewhere_and_where() {
        let c = QueryClauses { prewhere: "a = 1".into(), where_clause: "b = 2".into() };
        assert_eq!(clauses_predicate(&c), "((a = 1) AND (b = 2))");
        let p = QueryClauses { prewhere: "a = 1".into(), where_clause: String::new() };
        assert_eq!(clauses_predicate(&p), "(a = 1)");
        let w = QueryClauses { prewhere: String::new(), where_clause: "b = 2".into() };
        assert_eq!(clauses_predicate(&w), "(b = 2)");
        let e = QueryClauses { prewhere: String::new(), where_clause: String::new() };
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
        assert!(pred.contains("'ERROR'"), "error predicate must include error condition: {pred}");
        assert!(pred.contains("payments"), "error predicate restates common filter: {pred}");
        assert!(base.to_sql().contains("payments"), "base scan pruned by service: {}", base.to_sql());
    }
}
