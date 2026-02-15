use axum::{
    Form, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use std::collections::BTreeMap;

use crate::AppState;
use crate::models::metrics::*;
use crate::promql::{self, PromExpr, RangeFunc, TimeSeries};

// ═══ Prometheus-compatible API endpoints ═══

// ── /api/v1/query — instant query ──

#[derive(Debug, Deserialize)]
pub struct InstantQueryParams {
    pub query: String,
    pub time: Option<String>,
}

pub async fn prom_query(
    State(state): State<AppState>,
    Query(params): Query<InstantQueryParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    prom_query_inner(state, params).await
}

pub async fn prom_query_post(
    State(state): State<AppState>,
    Form(params): Form<InstantQueryParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    prom_query_inner(state, params).await
}

async fn prom_query_inner(
    state: AppState,
    params: InstantQueryParams,
) -> Result<Json<PromResponse<VectorData>>, (StatusCode, String)> {
    let expr = promql::parse(&params.query).map_err(|e| {
        (StatusCode::BAD_REQUEST, format!("PromQL parse error: {e}"))
    })?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let eval_time = params
        .time
        .as_ref()
        .and_then(|t| t.parse::<f64>().ok())
        .unwrap_or(now);

    // For instant queries, look back 5 minutes for data
    let lookback = 300.0;
    let start = eval_time - lookback;

    let series = evaluate_expr(&state, &expr, start, eval_time, lookback).await?;

    // Return the latest value from each series
    let result: Vec<VectorResult> = series
        .iter()
        .filter_map(|ts| {
            ts.samples.last().map(|(t, v)| VectorResult {
                metric: ts.labels.clone(),
                value: (*t, format_value(*v)),
            })
        })
        .collect();

    Ok(Json(PromResponse {
        status: "success",
        data: VectorData {
            result_type: "vector",
            result,
        },
    }))
}

// ── /api/v1/query_range — range query ──

#[derive(Debug, Deserialize)]
pub struct RangeQueryParams {
    pub query: String,
    pub start: String,
    pub end: String,
    pub step: Option<String>,
}

pub async fn prom_query_range(
    State(state): State<AppState>,
    Query(params): Query<RangeQueryParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    prom_query_range_inner(state, params).await
}

pub async fn prom_query_range_post(
    State(state): State<AppState>,
    Form(params): Form<RangeQueryParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    prom_query_range_inner(state, params).await
}

async fn prom_query_range_inner(
    state: AppState,
    params: RangeQueryParams,
) -> Result<Json<PromResponse<MatrixData>>, (StatusCode, String)> {
    let expr = promql::parse(&params.query).map_err(|e| {
        (StatusCode::BAD_REQUEST, format!("PromQL parse error: {e}"))
    })?;

    let start = parse_timestamp(&params.start)?;
    let end = parse_timestamp(&params.end)?;
    let step = params
        .step
        .as_ref()
        .and_then(|s| parse_step(s).ok())
        .unwrap_or(15.0);

    // Determine lookback window for range functions
    let lookback = match &expr {
        PromExpr::RangeFunction { range_secs, .. } => *range_secs,
        PromExpr::Aggregation { inner, .. } => match inner.as_ref() {
            PromExpr::RangeFunction { range_secs, .. } => *range_secs,
            _ => 300.0,
        },
        _ => 300.0,
    };

    let series = evaluate_expr(&state, &expr, start - lookback, end, lookback).await?;

    // Align to step grid
    let step_timestamps = generate_steps(start, end, step);

    let result: Vec<MatrixResult> = match &expr {
        PromExpr::Aggregation { op, by_labels, without, inner } => {
            // For aggregations over range functions, we need step-aligned evaluation
            let inner_series = match inner.as_ref() {
                PromExpr::RangeFunction { func, range_secs, .. } => {
                    // Compute rate/irate/increase at each step
                    evaluate_range_at_steps(&series, *func, *range_secs, &step_timestamps)
                }
                _ => series,
            };

            let aggregated = promql::aggregate_series(
                inner_series,
                *op,
                by_labels,
                *without,
                &step_timestamps,
            );

            aggregated
                .into_iter()
                .map(|ts| MatrixResult {
                    metric: ts.labels,
                    values: ts.samples.iter().map(|(t, v)| (*t, format_value(*v))).collect(),
                })
                .collect()
        }
        PromExpr::RangeFunction { func, range_secs, .. } => {
            let rated = evaluate_range_at_steps(&series, *func, *range_secs, &step_timestamps);
            rated
                .into_iter()
                .map(|ts| MatrixResult {
                    metric: ts.labels,
                    values: ts.samples.iter().map(|(t, v)| (*t, format_value(*v))).collect(),
                })
                .collect()
        }
        PromExpr::Selector(_) => {
            // Raw samples aligned to steps
            series
                .into_iter()
                .map(|ts| {
                    let values: Vec<(f64, String)> = step_timestamps
                        .iter()
                        .filter_map(|&t| {
                            // Find the latest sample at or before this step
                            ts.samples
                                .iter()
                                .rev()
                                .find(|(st, _)| *st <= t + step / 2.0)
                                .map(|(_, v)| (t, format_value(*v)))
                        })
                        .collect();
                    MatrixResult {
                        metric: ts.labels,
                        values,
                    }
                })
                .collect()
        }
    };

    Ok(Json(PromResponse {
        status: "success",
        data: MatrixData {
            result_type: "matrix",
            result,
        },
    }))
}

// ── /api/v1/series — series discovery ──

#[derive(Debug, Deserialize)]
pub struct SeriesParams {
    #[serde(rename = "match[]")]
    pub match_exprs: Option<Vec<String>>,
    pub start: Option<String>,
    pub end: Option<String>,
}

pub async fn prom_series(
    State(state): State<AppState>,
    Query(params): Query<SeriesParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    prom_series_inner(state, params).await
}

pub async fn prom_series_post(
    State(state): State<AppState>,
    Form(params): Form<SeriesParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    prom_series_inner(state, params).await
}

async fn prom_series_inner(
    state: AppState,
    params: SeriesParams,
) -> Result<Json<PromResponse<Vec<BTreeMap<String, String>>>>, (StatusCode, String)> {
    let match_exprs = params.match_exprs.unwrap_or_default();
    if match_exprs.is_empty() {
        return Ok(Json(PromResponse {
            status: "success",
            data: Vec::<BTreeMap<String, String>>::new(),
        }));
    }

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let start_secs = params.start.as_ref().and_then(|s| s.parse::<f64>().ok()).unwrap_or(now_secs - 3600.0);
    let end_secs = params.end.as_ref().and_then(|s| s.parse::<f64>().ok()).unwrap_or(now_secs);

    let mut all_series = Vec::new();

    for expr_str in &match_exprs {
        let selector = match promql::parse(expr_str) {
            Ok(PromExpr::Selector(s)) => s,
            _ => continue,
        };

        let mut where_parts = vec![
            format!(
                "TimeUnix >= toDateTime64({}, 9)",
                start_secs as i64
            ),
            format!(
                "TimeUnix <= toDateTime64({}, 9)",
                end_secs as i64
            ),
        ];
        if !selector.name.is_empty() {
            where_parts.push(format!("MetricName = '{}'", selector.name.replace('\'', "\\'")));
        }
        where_parts.extend(promql::matchers_to_sql(&selector.matchers));

        let where_clause = where_parts.join(" AND ");

        // Query both gauge and sum tables
        for table in &["otel_metrics_gauge", "otel_metrics_sum"] {
            let sql = format!(
                "SELECT DISTINCT MetricName, ServiceName, Attributes \
                 FROM {table} \
                 WHERE {where_clause} \
                 LIMIT 1000"
            );

            let rows: Vec<SeriesRow> = state
                .ch
                .query(&sql)
                .fetch_all()
                .await
                .unwrap_or_default();

            for row in rows {
                let labels = promql::build_label_set(
                    &row.metric_name,
                    &row.service_name,
                    &row.attributes,
                );
                all_series.push(labels);
            }
        }
    }

    // Deduplicate
    all_series.sort();
    all_series.dedup();

    Ok(Json(PromResponse {
        status: "success",
        data: all_series,
    }))
}

// ── /api/v1/labels — label names ──

pub async fn prom_labels(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Return well-known labels plus discovered attribute keys
    let mut labels = vec![
        "__name__".to_string(),
        "service_name".to_string(),
        "job".to_string(),
    ];

    // Discover attribute keys from gauge and sum tables
    for table in &["otel_metrics_gauge", "otel_metrics_sum"] {
        let sql = format!(
            "SELECT DISTINCT arrayJoin(mapKeys(Attributes)) AS name \
             FROM {table} \
             WHERE TimeUnix >= now() - INTERVAL 1 HOUR \
             ORDER BY name \
             LIMIT 200"
        );

        let rows: Vec<LabelNameRow> = state
            .ch
            .query(&sql)
            .fetch_all()
            .await
            .unwrap_or_default();

        for row in rows {
            if !row.name.is_empty() && !labels.contains(&row.name) {
                labels.push(row.name);
            }
        }
    }

    labels.sort();
    labels.dedup();

    Ok(Json(PromResponse {
        status: "success",
        data: labels,
    }))
}

// ── /api/v1/label/{name}/values — label values ──

pub async fn prom_label_values(
    State(state): State<AppState>,
    Path(label_name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut values = Vec::new();

    for table in &["otel_metrics_gauge", "otel_metrics_sum"] {
        let sql = match label_name.as_str() {
            "__name__" => format!(
                "SELECT DISTINCT MetricName AS value FROM {table} \
                 WHERE TimeUnix >= now() - INTERVAL 1 HOUR \
                 ORDER BY value LIMIT 500"
            ),
            "service_name" | "job" => format!(
                "SELECT DISTINCT ServiceName AS value FROM {table} \
                 WHERE TimeUnix >= now() - INTERVAL 1 HOUR \
                 ORDER BY value LIMIT 500"
            ),
            _ => {
                let escaped = label_name.replace('\'', "\\'");
                format!(
                    "SELECT DISTINCT Attributes['{escaped}'] AS value FROM {table} \
                     WHERE TimeUnix >= now() - INTERVAL 1 HOUR \
                       AND value != '' \
                     ORDER BY value LIMIT 500"
                )
            }
        };

        let rows: Vec<LabelValueRow> = state
            .ch
            .query(&sql)
            .fetch_all()
            .await
            .unwrap_or_default();

        for row in rows {
            if !row.value.is_empty() && !values.contains(&row.value) {
                values.push(row.value);
            }
        }
    }

    values.sort();
    values.dedup();

    Ok(Json(PromResponse {
        status: "success",
        data: values,
    }))
}

// ═══ Internal evaluation helpers ═══

/// Evaluate a PromQL expression against ClickHouse, returning raw TimeSeries.
async fn evaluate_expr(
    state: &AppState,
    expr: &PromExpr,
    start_secs: f64,
    end_secs: f64,
    _lookback: f64,
) -> Result<Vec<TimeSeries>, (StatusCode, String)> {
    let selector = extract_selector(expr);

    let mut where_parts = vec![
        format!(
            "TimeUnix >= toDateTime64({}, 9)",
            start_secs as i64
        ),
        format!(
            "TimeUnix <= toDateTime64({}, 9)",
            end_secs as i64
        ),
    ];

    if !selector.name.is_empty() {
        where_parts.push(format!(
            "MetricName = '{}'",
            selector.name.replace('\'', "\\'")
        ));
    }
    where_parts.extend(promql::matchers_to_sql(&selector.matchers));

    let where_clause = where_parts.join(" AND ");

    // Query both gauge and sum tables in parallel
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
        state.ch.query(&gauge_sql).fetch_all::<MetricSample>(),
        state.ch.query(&sum_sql).fetch_all::<MetricSample>(),
    );

    let gauge_rows = gauge_res.unwrap_or_default();
    let sum_rows = sum_res.unwrap_or_default();

    // Merge and group into series
    let all_samples: Vec<(BTreeMap<String, String>, f64, f64)> = gauge_rows
        .iter()
        .chain(sum_rows.iter())
        .map(|s| {
            let labels = promql::build_label_set(
                &s.metric_name,
                &s.service_name,
                &s.attributes,
            );
            let ts_secs = s.ts_ms as f64 / 1000.0;
            (labels, ts_secs, s.value)
        })
        .collect();

    Ok(promql::group_into_series(all_samples))
}

/// Extract the innermost MetricSelector from any expression.
fn extract_selector(expr: &PromExpr) -> &promql::MetricSelector {
    match expr {
        PromExpr::Selector(s) => s,
        PromExpr::RangeFunction { selector, .. } => selector,
        PromExpr::Aggregation { inner, .. } => extract_selector(inner),
    }
}

/// Evaluate a range function (rate/irate/increase) at each step timestamp.
fn evaluate_range_at_steps(
    raw_series: &[TimeSeries],
    func: RangeFunc,
    range_secs: f64,
    step_timestamps: &[f64],
) -> Vec<TimeSeries> {
    raw_series
        .iter()
        .map(|ts| {
            let samples: Vec<(f64, f64)> = step_timestamps
                .iter()
                .filter_map(|&t| {
                    // Gather samples in [t - range_secs, t]
                    let window: Vec<(f64, f64)> = ts
                        .samples
                        .iter()
                        .filter(|(st, _)| *st >= t - range_secs && *st <= t)
                        .copied()
                        .collect();

                    let value = match func {
                        RangeFunc::Rate => promql::compute_rate(&window),
                        RangeFunc::Irate => promql::compute_irate(&window),
                        RangeFunc::Increase => promql::compute_increase(&window),
                    };

                    value.map(|v| (t, v))
                })
                .collect();

            TimeSeries {
                labels: ts.labels.clone(),
                samples,
            }
        })
        .collect()
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

fn parse_timestamp(s: &str) -> Result<f64, (StatusCode, String)> {
    s.parse::<f64>().map_err(|_| {
        // Try ISO 8601
        chrono::DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.timestamp() as f64)
            .map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("invalid timestamp '{s}': {e}"),
                )
            })
    }).or_else(|r| r)
}

fn parse_step(s: &str) -> Result<f64, String> {
    // Try plain number (seconds)
    if let Ok(n) = s.parse::<f64>() {
        return Ok(n);
    }
    // Try duration string
    let mut total = 0.0;
    let mut num = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
        } else {
            let n: f64 = num.parse().unwrap_or(0.0);
            num.clear();
            total += match c {
                's' => n,
                'm' => n * 60.0,
                'h' => n * 3600.0,
                _ => 0.0,
            };
        }
    }
    if total > 0.0 {
        Ok(total)
    } else {
        Err("invalid step".to_string())
    }
}

fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        format!("{v}")
    }
}
