use std::collections::BTreeMap;
use std::pin::Pin;
use std::future::Future;
use clickhouse::Client;
use promql_parser::parser::{self, Expr};

use super::types::TimeSeries;
use super::{aggregate, binary, compute, scalar, sql, translate, types};
use crate::models::metrics::MetricSample;

// ═══════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════

/// Evaluate an instant query (single point in time).
pub async fn evaluate_instant_query(
    ch: &Client,
    query: &str,
    eval_time: f64,
    lookback: f64,
) -> Result<Vec<TimeSeries>, String> {
    let expr = parser::parse(query).map_err(|e| format!("{e}"))?;
    let step_timestamps = vec![eval_time];
    evaluate(&expr, ch, eval_time - lookback, eval_time, &step_timestamps).await
}

/// Evaluate a range query (multiple points across a time range).
pub async fn evaluate_range_query(
    ch: &Client,
    query: &str,
    start: f64,
    end: f64,
    step: f64,
) -> Result<Vec<TimeSeries>, String> {
    let expr = parser::parse(query).map_err(|e| format!("{e}"))?;
    let lookback = extract_lookback(&expr);
    let step_timestamps = generate_steps(start, end, step);
    evaluate(&expr, ch, start - lookback, end, &step_timestamps).await
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
) -> Pin<Box<dyn Future<Output = Result<Vec<TimeSeries>, String>> + Send + 'a>> {
    Box::pin(async move {
    match expr {
        Expr::VectorSelector(vs) => {
            query_clickhouse(ch, vs, query_start, query_end, step_timestamps, true).await
        }

        Expr::MatrixSelector(ms) => {
            // MatrixSelector wraps a VectorSelector with a range duration.
            // We query the full range needed and keep ALL raw samples
            // so that range functions (rate, increase, etc.) have enough data.
            let range_secs = ms.range.as_secs_f64();
            let adjusted_start = query_start - range_secs;
            query_clickhouse(ch, &ms.vs, adjusted_start, query_end, step_timestamps, false).await
        }

        Expr::Call(call) => {
            let func_name = call.func.name;

            // Check if it's a range function
            if let Some(range_func) = translate::to_range_func(func_name) {
                return evaluate_range_call(
                    &call.args.args, range_func, func_name, ch, query_start, query_end, step_timestamps,
                )
                .await;
            }

            // Check if it's a scalar function
            if let Some(scalar_func) = translate::to_scalar_func(func_name) {
                return evaluate_scalar_call(
                    &call.args.args, scalar_func, func_name, ch, query_start, query_end, step_timestamps,
                )
                .await;
            }

            Err(format!("unsupported function: {func_name}"))
        }

        Expr::Aggregate(agg) => {
            let op = translate::to_agg_op(agg.op)?;
            let (by_labels, without) = translate::extract_label_modifier(&agg.modifier);

            let inner = evaluate(&agg.expr, ch, query_start, query_end, step_timestamps).await?;

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
                evaluate(&bin.lhs, ch, query_start, query_end, step_timestamps),
                evaluate(&bin.rhs, ch, query_start, query_end, step_timestamps),
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
            let mut inner = evaluate(&unary.expr, ch, query_start, query_end, step_timestamps).await?;
            // Negate all values
            for ts in &mut inner {
                for sample in &mut ts.samples {
                    sample.1 = -sample.1;
                }
            }
            Ok(inner)
        }

        Expr::Paren(paren) => {
            evaluate(&paren.expr, ch, query_start, query_end, step_timestamps).await
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

    let inner_series = evaluate(inner_expr, ch, query_start, query_end, step_timestamps).await?;
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
) -> Result<Vec<TimeSeries>, String> {
    let mut where_parts = vec![
        format!("TimeUnix >= toDateTime64({}, 9)", start_secs as i64),
        format!("TimeUnix <= toDateTime64({}, 9)", end_secs as i64),
    ];

    // Extract metric name from matchers
    if let Some(name) = &vs.name {
        if !name.is_empty() {
            where_parts.push(format!(
                "MetricName = '{}'",
                name.replace('\'', "\\'")
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

    let gauge_sql = format!(
        "SELECT MetricName, ServiceName, Attributes, \
         toInt64(toUnixTimestamp64Milli(TimeUnix)) AS ts_ms, Value \
         FROM otel_metrics_gauge \
         WHERE {where_clause} \
         ORDER BY TimeUnix"
    );
    let sum_sql = format!(
        "SELECT MetricName, ServiceName, Attributes, \
         toInt64(toUnixTimestamp64Milli(TimeUnix)) AS ts_ms, Value \
         FROM otel_metrics_sum \
         WHERE {where_clause} \
         ORDER BY TimeUnix"
    );

    let (gauge_res, sum_res) = tokio::join!(
        ch.query(&gauge_sql).fetch_all::<MetricSample>(),
        ch.query(&sum_sql).fetch_all::<MetricSample>(),
    );

    let gauge_rows = gauge_res.unwrap_or_default();
    let sum_rows = sum_res.unwrap_or_default();

    let all_samples: Vec<(BTreeMap<String, String>, f64, f64)> = gauge_rows
        .iter()
        .chain(sum_rows.iter())
        .map(|s| {
            let labels = types::build_label_set(&s.metric_name, &s.service_name, &s.attributes);
            let ts_secs = s.ts_ms as f64 / 1000.0;
            (labels, ts_secs, s.value)
        })
        .collect();

    let series = types::group_into_series(all_samples);

    if align {
        // Step-align for VectorSelector (instant vectors)
        let lookback = end_secs - start_secs;
        Ok(step_align_series(series, step_timestamps, lookback))
    } else {
        // Return all raw samples for MatrixSelector (range vectors)
        Ok(series)
    }
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
