use std::collections::BTreeMap;
use std::pin::Pin;
use std::future::Future;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use clickhouse::Client;
use dashmap::DashMap;
use promql_parser::parser::{self, Expr};

use super::types::TimeSeries;
use super::{aggregate, binary, compute, scalar, sql, translate, types};
use crate::models::metrics::MetricSample;

/// Which metrics table(s) a metric name lives in. A metric is either a gauge or a
/// sum in practice, so half the per-selector scans always return 0 rows.
#[derive(Debug, Clone, Copy, PartialEq)]
enum MetricTable {
    Gauge,
    Sum,
    Both,
}

/// (tenant_id, metric_name) → which table(s) returned rows last time. Tenant-scoped:
/// different tenants may ship the same metric name with different types. 5-minute TTL
/// so a metric that starts emitting to the other table is picked up quickly.
static METRIC_TABLE_CACHE: LazyLock<DashMap<(String, String), (MetricTable, Instant)>> =
    LazyLock::new(DashMap::new);
const METRIC_TABLE_TTL: Duration = Duration::from_secs(300);
const METRIC_TABLE_CACHE_MAX: usize = 10_000;

// ═══════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════

/// Evaluate an instant query (single point in time).
pub async fn evaluate_instant_query(
    ch: &Client,
    query: &str,
    eval_time: f64,
    lookback: f64,
    tenant_id: &str,
) -> Result<Vec<TimeSeries>, String> {
    let expr = parser::parse(query).map_err(|e| format!("{e}"))?;
    let step_timestamps = vec![eval_time];
    evaluate(&expr, ch, eval_time - lookback, eval_time, &step_timestamps, tenant_id).await
}

/// Evaluate a range query (multiple points across a time range).
pub async fn evaluate_range_query(
    ch: &Client,
    query: &str,
    start: f64,
    end: f64,
    step: f64,
    tenant_id: &str,
) -> Result<Vec<TimeSeries>, String> {
    let expr = parser::parse(query).map_err(|e| format!("{e}"))?;
    let lookback = extract_lookback(&expr);
    let step_timestamps = generate_steps(start, end, step);
    evaluate(&expr, ch, start - lookback, end, &step_timestamps, tenant_id).await
}

// ═══════════════════════════════════════════════════════════════════
// Recursive evaluator
// ═══════════════════════════════════════════════════════════════════

/// Recursively evaluate a promql-parser Expr tree.
fn evaluate<'a>(
    expr: &'a Expr,
    ch: &'a Client,
    query_start: f64,
    query_end: f64,
    step_timestamps: &'a [f64],
    tenant_id: &'a str,
) -> Pin<Box<dyn Future<Output = Result<Vec<TimeSeries>, String>> + Send + 'a>> {
    Box::pin(async move {
    match expr {
        Expr::VectorSelector(vs) => {
            query_clickhouse(ch, vs, query_start, query_end, step_timestamps, true, tenant_id).await
        }

        Expr::MatrixSelector(ms) => {
            // MatrixSelector wraps a VectorSelector with a range duration.
            // We query the full range needed and keep ALL raw samples
            // so that range functions (rate, increase, etc.) have enough data.
            let range_secs = ms.range.as_secs_f64();
            let adjusted_start = query_start - range_secs;
            query_clickhouse(ch, &ms.vs, adjusted_start, query_end, step_timestamps, false, tenant_id).await
        }

        Expr::Call(call) => {
            let func_name = call.func.name;

            // Check if it's a range function
            if let Some(range_func) = translate::to_range_func(func_name) {
                return evaluate_range_call(
                    &call.args.args, range_func, func_name, ch, query_start, query_end, step_timestamps, tenant_id,
                )
                .await;
            }

            // Check if it's a scalar function
            if let Some(scalar_func) = translate::to_scalar_func(func_name) {
                return evaluate_scalar_call(
                    &call.args.args, scalar_func, func_name, ch, query_start, query_end, step_timestamps, tenant_id,
                )
                .await;
            }

            Err(format!("unsupported function: {func_name}"))
        }

        Expr::Aggregate(agg) => {
            let op = translate::to_agg_op(agg.op)?;
            let (by_labels, without) = translate::extract_label_modifier(&agg.modifier);

            let inner = evaluate(&agg.expr, ch, query_start, query_end, step_timestamps, tenant_id).await?;

            // Extract param (e.g., for quantile, topk, bottomk)
            let param = match &agg.param {
                Some(p) => extract_number_literal(p),
                None => None,
            };

            Ok(aggregate::aggregate_series(
                inner,
                op,
                &by_labels,
                without,
                step_timestamps,
                param,
            ))
        }

        Expr::Binary(bin) => {
            // Evaluate both sides in parallel
            let (lhs_result, rhs_result) = tokio::join!(
                evaluate(&bin.lhs, ch, query_start, query_end, step_timestamps, tenant_id),
                evaluate(&bin.rhs, ch, query_start, query_end, step_timestamps, tenant_id),
            );

            let lhs = lhs_result?;
            let rhs = rhs_result?;

            Ok(binary::apply_binary_op(
                bin.op,
                lhs,
                rhs,
                &bin.modifier,
                step_timestamps,
            ))
        }

        Expr::Unary(unary) => {
            let mut inner = evaluate(&unary.expr, ch, query_start, query_end, step_timestamps, tenant_id).await?;
            // Negate all values
            for ts in &mut inner {
                for sample in &mut ts.samples {
                    sample.1 = -sample.1;
                }
            }
            Ok(inner)
        }

        Expr::Paren(paren) => {
            evaluate(&paren.expr, ch, query_start, query_end, step_timestamps, tenant_id).await
        }

        Expr::NumberLiteral(num) => {
            // Return a scalar series with the literal value at each step
            let samples: Vec<(f64, f64)> = step_timestamps
                .iter()
                .map(|&t| (t, num.val))
                .collect();
            Ok(vec![TimeSeries {
                labels: BTreeMap::new(),
                samples,
            }])
        }

        Expr::StringLiteral(_) => {
            Err("string literals are not supported in evaluation".to_string())
        }

        Expr::Subquery(_) => {
            Err("subqueries are not yet supported".to_string())
        }

        Expr::Extension(_) => {
            Err("extension expressions are not supported".to_string())
        }
    }
    }) // Box::pin
}

/// Evaluate a Call node that wraps a range function (rate, increase, *_over_time, etc.).
async fn evaluate_range_call(
    args: &[Box<Expr>],
    range_func: types::RangeFunc,
    func_name: &str,
    ch: &Client,
    query_start: f64,
    query_end: f64,
    step_timestamps: &[f64],
    tenant_id: &str,
) -> Result<Vec<TimeSeries>, String> {
    // Range functions expect their first arg to be a MatrixSelector (or nested expr).
    // Some have an additional numeric parameter (quantile_over_time, predict_linear).

    let (matrix_arg_idx, param) = match func_name {
        "quantile_over_time" => {
            // quantile_over_time(scalar, matrix)
            let p = args.first().and_then(|a| extract_number_literal(a));
            (1, p)
        }
        "predict_linear" => {
            // predict_linear(matrix, scalar)
            let p = args.get(1).and_then(|a| extract_number_literal(a));
            (0, p)
        }
        _ => (0, None),
    };

    let matrix_arg = args.get(matrix_arg_idx).ok_or_else(|| {
        format!("{func_name} requires a matrix argument")
    })?;

    // Extract the range duration from the matrix selector
    let range_secs = match matrix_arg.as_ref() {
        Expr::MatrixSelector(ms) => ms.range.as_secs_f64(),
        _ => 300.0, // default 5m
    };

    // Evaluate the inner expression to get raw series
    let raw_series = evaluate(
        matrix_arg,
        ch,
        query_start - range_secs,
        query_end,
        step_timestamps,
        tenant_id,
    )
    .await?;

    // Apply range function at each step
    Ok(evaluate_range_at_steps(
        &raw_series,
        range_func,
        range_secs,
        step_timestamps,
        param,
    ))
}

/// Evaluate a Call node that wraps a scalar function (abs, ceil, histogram_quantile, etc.).
async fn evaluate_scalar_call(
    args: &[Box<Expr>],
    scalar_func: types::ScalarFunc,
    func_name: &str,
    ch: &Client,
    query_start: f64,
    query_end: f64,
    step_timestamps: &[f64],
    tenant_id: &str,
) -> Result<Vec<TimeSeries>, String> {
    // Classify based on function argument patterns
    let (inner_expr, extra_args): (&Expr, Vec<f64>) = match func_name {
        "histogram_quantile" => {
            // histogram_quantile(scalar, vector_expr)
            if args.len() != 2 {
                return Err("histogram_quantile requires 2 arguments".to_string());
            }
            let phi = extract_number_literal(&args[0]).unwrap_or(0.5);
            (&args[1], vec![phi])
        }
        "clamp_min" => {
            if args.len() != 2 {
                return Err("clamp_min requires 2 arguments".to_string());
            }
            let min_val = extract_number_literal(&args[1]).unwrap_or(0.0);
            (&args[0], vec![min_val])
        }
        "clamp_max" => {
            if args.len() != 2 {
                return Err("clamp_max requires 2 arguments".to_string());
            }
            let max_val = extract_number_literal(&args[1]).unwrap_or(0.0);
            (&args[0], vec![max_val])
        }
        "clamp" => {
            if args.len() != 3 {
                return Err("clamp requires 3 arguments".to_string());
            }
            let min_val = extract_number_literal(&args[1]).unwrap_or(0.0);
            let max_val = extract_number_literal(&args[2]).unwrap_or(0.0);
            (&args[0], vec![min_val, max_val])
        }
        "round" => {
            if args.is_empty() || args.len() > 2 {
                return Err("round requires 1 or 2 arguments".to_string());
            }
            let extra = if args.len() == 2 {
                vec![extract_number_literal(&args[1]).unwrap_or(1.0)]
            } else {
                vec![]
            };
            (&args[0], extra)
        }
        _ => {
            // Unary scalar function: func(expr)
            if args.is_empty() {
                return Err(format!("{func_name} requires at least 1 argument"));
            }
            (&args[0], vec![])
        }
    };

    let inner_series = evaluate(inner_expr, ch, query_start, query_end, step_timestamps, tenant_id).await?;
    Ok(scalar::apply_scalar_func(inner_series, scalar_func, &extra_args))
}

// ═══════════════════════════════════════════════════════════════════
// ClickHouse query
// ═══════════════════════════════════════════════════════════════════

/// Query ClickHouse for a VectorSelector, returning TimeSeries.
/// When `align` is true, step-align samples to step_timestamps (for instant vectors).
/// When false, return all raw samples (for range vectors used by rate/increase/etc).
async fn query_clickhouse(
    ch: &Client,
    vs: &promql_parser::parser::VectorSelector,
    start_secs: f64,
    end_secs: f64,
    step_timestamps: &[f64],
    align: bool,
    tenant_id: &str,
) -> Result<Vec<TimeSeries>, String> {
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let mut where_parts = vec![
        format!("tenant_id = '{escaped_tenant}'"),
        format!("TimeUnix >= toDateTime64({}, 9)", start_secs as i64),
        format!("TimeUnix <= toDateTime64({}, 9)", end_secs as i64),
    ];

    // Extract metric name from matchers
    if let Some(name) = &vs.name {
        if !name.is_empty() {
            where_parts.push(format!(
                "MetricName = '{}'",
                crate::query_builder::escape_string_literal(&name)
            ));
        }
    }

    // Also check for __name__ matcher
    for m in &vs.matchers.matchers {
        if m.name == "__name__" {
            // Already handled by vs.name for Equal matches,
            // but handle regex and other ops here
            where_parts.extend(sql::matchers_to_sql(&[m.clone()]));
            continue;
        }
        where_parts.extend(sql::matchers_to_sql(&[m.clone()]));
    }

    let where_clause = where_parts.join(" AND ");

    // NOTE on SQL-side bucketing: we deliberately still pull raw samples here rather
    // than pushing step-bucketing into SQL (GROUP BY toStartOfInterval + argMax/avg),
    // because rate()/increase() counter-reset detection requires adjacent raw samples;
    // pre-bucketing would silently change their semantics. Revisit by splitting the
    // align=true (instant vector) path, which CAN be bucketed server-side safely.
    //
    // ORDER BY series key first, then time: rows arrive grouped per series so the
    // label set is built once per series (key-change detection) instead of once per
    // sample row, and per-series samples arrive already time-sorted.
    let make_sql = |table: &str| {
        format!(
            "SELECT MetricName, ServiceName, Attributes, \
             toInt64(toUnixTimestamp64Milli(TimeUnix)) AS ts_ms, Value \
             FROM {table} \
             WHERE {where_clause} \
             ORDER BY MetricName, ServiceName, Attributes, TimeUnix"
        )
    };

    // Table-routing cache (tenant, metric) → gauge/sum/both: skip the table that
    // never has this metric. Only usable when the selector names a metric.
    let cache_key: Option<(String, String)> = vs
        .name
        .as_ref()
        .filter(|n| !n.is_empty())
        .map(|n| (tenant_id.to_string(), n.clone()));
    let cached_choice = cache_key.as_ref().and_then(|k| {
        METRIC_TABLE_CACHE
            .get(k)
            .filter(|e| e.1.elapsed() < METRIC_TABLE_TTL)
            .map(|e| e.0)
    });

    let (gauge_rows, sum_rows) = match cached_choice {
        Some(MetricTable::Gauge) => {
            let rows = crate::tenant_query(ch, &make_sql("metrics_gauge"), tenant_id)
                .fetch_all::<MetricSample>()
                .await
                .unwrap_or_default();
            (rows, Vec::new())
        }
        Some(MetricTable::Sum) => {
            let rows = crate::tenant_query(ch, &make_sql("metrics_sum"), tenant_id)
                .fetch_all::<MetricSample>()
                .await
                .unwrap_or_default();
            (Vec::new(), rows)
        }
        _ => {
            // Cache miss (or 'both'): query both tables in parallel.
            let (gauge_res, sum_res) = tokio::join!(
                crate::tenant_query(ch, &make_sql("metrics_gauge"), tenant_id)
                    .fetch_all::<MetricSample>(),
                crate::tenant_query(ch, &make_sql("metrics_sum"), tenant_id)
                    .fetch_all::<MetricSample>(),
            );
            let gauge_rows = gauge_res.unwrap_or_default();
            let sum_rows = sum_res.unwrap_or_default();

            // Record which table(s) actually had data (only on a true miss, and only
            // when at least one table returned rows — an empty result tells us nothing).
            if cached_choice.is_none() {
                if let Some(key) = cache_key {
                    let observed = match (!gauge_rows.is_empty(), !sum_rows.is_empty()) {
                        (true, false) => Some(MetricTable::Gauge),
                        (false, true) => Some(MetricTable::Sum),
                        (true, true) => Some(MetricTable::Both),
                        (false, false) => None,
                    };
                    if let Some(observed) = observed {
                        if METRIC_TABLE_CACHE.len() > METRIC_TABLE_CACHE_MAX {
                            // Evict only expired entries; clear() would also wipe hot ones.
                            METRIC_TABLE_CACHE.retain(|_, v| v.1.elapsed() < METRIC_TABLE_TTL);
                            if METRIC_TABLE_CACHE.len() > METRIC_TABLE_CACHE_MAX {
                                METRIC_TABLE_CACHE.clear(); // backstop: still over cap after pruning
                            }
                        }
                        METRIC_TABLE_CACHE.insert(key, (observed, Instant::now()));
                    }
                }
            }
            (gauge_rows, sum_rows)
        }
    };

    let series = rows_to_series(&gauge_rows, &sum_rows);

    if align {
        // Step-align for VectorSelector (instant vectors)
        let lookback = end_secs - start_secs;
        Ok(step_align_series(series, step_timestamps, lookback))
    } else {
        // Return all raw samples for MatrixSelector (range vectors)
        Ok(series)
    }
}

/// Convert raw sample rows into TimeSeries. Rows must arrive ordered by
/// (MetricName, ServiceName, Attributes, TimeUnix) — the SQL guarantees this — so a
/// label BTreeMap is allocated once per distinct series (key-change detection) instead
/// of once per sample row, as group_into_series used to require.
///
/// Output matches the old `group_into_series` exactly: series sorted by label set,
/// samples time-sorted, and series with identical label sets (e.g. the same labels
/// appearing in both tables, or raw keys that collapse to the same label set after
/// empty-value attributes are dropped) merged together.
fn rows_to_series(gauge_rows: &[MetricSample], sum_rows: &[MetricSample]) -> Vec<TimeSeries> {
    let mut series: Vec<TimeSeries> = Vec::new();

    for rows in [gauge_rows, sum_rows] {
        let mut prev_key: Option<(&str, &str, &[(String, String)])> = None;
        for s in rows {
            let key = (
                s.metric_name.as_str(),
                s.service_name.as_str(),
                s.attributes.as_slice(),
            );
            if prev_key != Some(key) {
                series.push(TimeSeries {
                    labels: types::build_label_set(&s.metric_name, &s.service_name, &s.attributes),
                    samples: Vec::new(),
                });
                prev_key = Some(key);
            }
            series
                .last_mut()
                .expect("series pushed above")
                .samples
                .push((s.ts_ms as f64 / 1000.0, s.value));
        }
    }

    // Merge duplicate label sets (rare) and restore the label-sorted output order the
    // previous BTreeMap-based grouping produced.
    series.sort_by(|a, b| a.labels.cmp(&b.labels));
    let mut merged: Vec<TimeSeries> = Vec::with_capacity(series.len());
    for ts in series {
        match merged.last_mut() {
            Some(last) if last.labels == ts.labels => {
                last.samples.extend(ts.samples);
                last.samples
                    .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            }
            _ => merged.push(ts),
        }
    }
    merged
}

/// Snap raw series to step timestamps, picking the latest sample within tolerance.
fn step_align_series(
    series: Vec<TimeSeries>,
    step_timestamps: &[f64],
    lookback: f64,
) -> Vec<TimeSeries> {
    let half_step = if step_timestamps.len() >= 2 {
        (step_timestamps[1] - step_timestamps[0]) / 2.0
    } else {
        // For instant queries (single timestamp), use the full lookback window
        lookback.max(5.0)
    };

    // Two-pointer merge: samples are time-sorted and steps ascending, so a single
    // forward pass replaces the old O(steps × samples) per-step reverse scan. For
    // each step we want the latest sample with ts in [t - half_step, t + half_step].
    series
        .into_iter()
        .map(|ts| {
            let mut samples: Vec<(f64, f64)> = Vec::with_capacity(step_timestamps.len());
            let mut i = 0usize; // first sample index not yet known to be <= t + half_step
            for &t in step_timestamps {
                while i < ts.samples.len() && ts.samples[i].0 <= t + half_step {
                    i += 1;
                }
                if i > 0 {
                    let (st, v) = ts.samples[i - 1];
                    if st >= t - half_step {
                        samples.push((t, v));
                    }
                }
            }
            TimeSeries {
                labels: ts.labels,
                samples,
            }
        })
        .collect()
}

// ═══════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════

/// Evaluate a range function at each step timestamp over a sliding window.
fn evaluate_range_at_steps(
    raw_series: &[TimeSeries],
    func: types::RangeFunc,
    range_secs: f64,
    step_timestamps: &[f64],
    param: Option<f64>,
) -> Vec<TimeSeries> {
    raw_series
        .iter()
        .map(|ts| {
            let samples: Vec<(f64, f64)> = step_timestamps
                .iter()
                .filter_map(|&t| {
                    let window: Vec<(f64, f64)> = ts
                        .samples
                        .iter()
                        .filter(|(st, _)| *st >= t - range_secs && *st <= t)
                        .copied()
                        .collect();

                    compute::evaluate_range_func(func, &window, param).map(|v| (t, v))
                })
                .collect();

            TimeSeries {
                labels: ts.labels.clone(),
                samples,
            }
        })
        .collect()
}

/// Extract a number literal from an Expr node.
fn extract_number_literal(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::NumberLiteral(n) => Some(n.val),
        Expr::Unary(u) => extract_number_literal(&u.expr).map(|v| -v),
        _ => None,
    }
}

/// Extract the maximum range/lookback duration from an expression tree.
pub fn extract_lookback(expr: &Expr) -> f64 {
    match expr {
        Expr::MatrixSelector(ms) => ms.range.as_secs_f64(),
        Expr::Call(call) => {
            call.args.args.iter().map(|a| extract_lookback(a)).fold(0.0_f64, f64::max)
        }
        Expr::Aggregate(agg) => extract_lookback(&agg.expr),
        Expr::Binary(bin) => {
            extract_lookback(&bin.lhs).max(extract_lookback(&bin.rhs))
        }
        Expr::Unary(u) => extract_lookback(&u.expr),
        Expr::Paren(p) => extract_lookback(&p.expr),
        _ => 300.0, // default 5m
    }
}

fn generate_steps(start: f64, end: f64, step: f64) -> Vec<f64> {
    let mut timestamps = Vec::new();
    let mut t = start;
    while t <= end {
        timestamps.push(t);
        t += step;
    }
    timestamps
}

/// Walk a promql-parser Expr tree and extract all metric names from VectorSelectors.
pub fn extract_metrics_from_expr(expr: &Expr) -> Vec<String> {
    let mut names = Vec::new();
    collect_metrics(expr, &mut names);
    names.sort();
    names.dedup();
    names
}

fn collect_metrics(expr: &Expr, names: &mut Vec<String>) {
    match expr {
        Expr::VectorSelector(vs) => {
            if let Some(name) = &vs.name {
                if !name.is_empty() {
                    names.push(name.clone());
                }
            }
        }
        Expr::MatrixSelector(ms) => {
            if let Some(name) = &ms.vs.name {
                if !name.is_empty() {
                    names.push(name.clone());
                }
            }
        }
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_metrics(arg, names);
            }
        }
        Expr::Aggregate(agg) => {
            collect_metrics(&agg.expr, names);
            if let Some(param) = &agg.param {
                collect_metrics(param, names);
            }
        }
        Expr::Binary(bin) => {
            collect_metrics(&bin.lhs, names);
            collect_metrics(&bin.rhs, names);
        }
        Expr::Unary(u) => collect_metrics(&u.expr, names),
        Expr::Paren(p) => collect_metrics(&p.expr, names),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference implementation of step alignment — the exact pre-optimization
    /// per-step reverse scan — used to pin the two-pointer rewrite's behavior.
    fn step_align_reference(
        series: Vec<TimeSeries>,
        step_timestamps: &[f64],
        lookback: f64,
    ) -> Vec<TimeSeries> {
        let half_step = if step_timestamps.len() >= 2 {
            (step_timestamps[1] - step_timestamps[0]) / 2.0
        } else {
            lookback.max(5.0)
        };
        series
            .into_iter()
            .map(|ts| {
                let samples: Vec<(f64, f64)> = step_timestamps
                    .iter()
                    .filter_map(|&t| {
                        ts.samples
                            .iter()
                            .rev()
                            .find(|(st, _)| *st <= t + half_step && *st >= t - half_step)
                            .map(|(_, v)| (t, *v))
                    })
                    .collect();
                TimeSeries { labels: ts.labels, samples }
            })
            .collect()
    }

    fn series_with(samples: Vec<(f64, f64)>) -> Vec<TimeSeries> {
        vec![TimeSeries { labels: BTreeMap::new(), samples }]
    }

    fn assert_align_matches_reference(samples: Vec<(f64, f64)>, steps: &[f64], lookback: f64) {
        let got = step_align_series(series_with(samples.clone()), steps, lookback);
        let want = step_align_reference(series_with(samples), steps, lookback);
        assert_eq!(got.len(), want.len());
        for (g, w) in got.iter().zip(want.iter()) {
            assert_eq!(g.samples, w.samples);
        }
    }

    #[test]
    fn step_align_matches_reference_on_regular_grid() {
        // 15s scrape on a 30s step grid.
        let samples: Vec<(f64, f64)> = (0..40).map(|i| (i as f64 * 15.0, i as f64)).collect();
        let steps: Vec<f64> = (0..20).map(|i| i as f64 * 30.0).collect();
        assert_align_matches_reference(samples, &steps, 30.0);
    }

    #[test]
    fn step_align_matches_reference_on_sparse_and_gappy_data() {
        // Irregular timestamps with gaps larger than the step.
        let samples = vec![
            (3.0, 1.0),
            (14.5, 2.0),
            (15.0, 2.5),
            (61.0, 3.0),
            (200.0, 4.0),
            (201.0, 5.0),
        ];
        let steps: Vec<f64> = (0..30).map(|i| i as f64 * 10.0).collect();
        assert_align_matches_reference(samples, &steps, 10.0);
    }

    #[test]
    fn step_align_matches_reference_for_instant_query_single_step() {
        // Single step timestamp → lookback window semantics.
        let samples = vec![(100.0, 1.0), (250.0, 2.0), (290.0, 3.0)];
        let steps = vec![300.0];
        assert_align_matches_reference(samples.clone(), &steps, 300.0);
        // And one where the only samples are outside the window.
        let far = vec![(1.0, 9.0)];
        assert_align_matches_reference(far, &steps, 5.0);
    }

    #[test]
    fn step_align_matches_reference_at_window_boundaries() {
        // Samples landing exactly on t ± half_step (half_step = 5 here).
        let samples = vec![(5.0, 1.0), (15.0, 2.0), (25.0, 3.0)];
        let steps = vec![0.0, 10.0, 20.0, 30.0];
        assert_align_matches_reference(samples, &steps, 10.0);
    }

    #[test]
    fn step_align_empty_samples_yields_empty() {
        let steps = vec![0.0, 10.0];
        let got = step_align_series(series_with(vec![]), &steps, 10.0);
        assert!(got[0].samples.is_empty());
    }

    // ── rows_to_series ──

    fn sample(metric: &str, service: &str, attrs: &[(&str, &str)], ts_ms: i64, value: f64) -> MetricSample {
        MetricSample {
            metric_name: metric.to_string(),
            service_name: service.to_string(),
            attributes: attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ts_ms,
            value,
        }
    }

    /// rows_to_series must produce exactly what the old per-row group_into_series
    /// produced (label-sorted series, time-sorted samples, duplicates merged).
    fn assert_matches_group_into_series(gauge: Vec<MetricSample>, sum: Vec<MetricSample>) {
        let got = rows_to_series(&gauge, &sum);
        let all: Vec<(BTreeMap<String, String>, f64, f64)> = gauge
            .iter()
            .chain(sum.iter())
            .map(|s| {
                (
                    types::build_label_set(&s.metric_name, &s.service_name, &s.attributes),
                    s.ts_ms as f64 / 1000.0,
                    s.value,
                )
            })
            .collect();
        let want = types::group_into_series(all);
        assert_eq!(got.len(), want.len(), "series count mismatch");
        for (g, w) in got.iter().zip(want.iter()) {
            assert_eq!(g.labels, w.labels);
            assert_eq!(g.samples, w.samples);
        }
    }

    #[test]
    fn rows_to_series_groups_ordered_rows_per_series() {
        // Rows ordered by (metric, service, attrs, time) as the SQL guarantees.
        let gauge = vec![
            sample("cpu", "api", &[("host", "a")], 1_000, 1.0),
            sample("cpu", "api", &[("host", "a")], 2_000, 2.0),
            sample("cpu", "api", &[("host", "b")], 1_000, 3.0),
            sample("mem", "api", &[], 1_000, 4.0),
        ];
        assert_matches_group_into_series(gauge, vec![]);
    }

    #[test]
    fn rows_to_series_merges_same_labels_across_tables() {
        // Same label set in both tables must merge into one time-sorted series.
        let gauge = vec![
            sample("up", "api", &[], 2_000, 1.0),
            sample("up", "api", &[], 4_000, 1.0),
        ];
        let sum = vec![
            sample("up", "api", &[], 1_000, 0.0),
            sample("up", "api", &[], 3_000, 1.0),
        ];
        assert_matches_group_into_series(gauge, sum);
    }

    #[test]
    fn rows_to_series_merges_keys_that_collapse_to_same_label_set() {
        // Empty-valued attributes are dropped by build_label_set, so distinct raw
        // keys can collapse to the same label set and must be merged.
        let gauge = vec![
            sample("up", "api", &[("dc", "")], 2_000, 1.0),
            sample("up", "api", &[], 1_000, 0.0),
        ];
        assert_matches_group_into_series(gauge, vec![]);
    }

    #[test]
    fn rows_to_series_empty_input() {
        assert!(rows_to_series(&[], &[]).is_empty());
    }
}
