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
use crate::promql;

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
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let eval_time = params
        .time
        .as_ref()
        .and_then(|t| t.parse::<f64>().ok())
        .unwrap_or(now);

    let series = promql::evaluate_instant_query(&state.ch, &params.query, eval_time, 300.0)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("PromQL error: {e}")))?;

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

    // Only track usage if the query actually returned data
    if !result.is_empty() {
        let metric_names = crate::usage_tracker::extract_metrics_from_query(&params.query);
        for name in metric_names {
            state.usage.track(crate::usage_tracker::UsageEvent {
                signal_name: name,
                signal_type: "metric".to_string(),
                source: "prom_api".to_string(),
            });
        }
    }

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
    let start = parse_timestamp(&params.start)?;
    let end = parse_timestamp(&params.end)?;
    let step = params
        .step
        .as_ref()
        .and_then(|s| parse_step(s).ok())
        .unwrap_or(15.0);

    let series = promql::evaluate_range_query(&state.ch, &params.query, start, end, step)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("PromQL error: {e}")))?;

    let result: Vec<MatrixResult> = series
        .into_iter()
        .map(|ts| MatrixResult {
            metric: ts.labels,
            values: ts.samples.iter().map(|(t, v)| (*t, format_value(*v))).collect(),
        })
        .collect();

    // Only track usage if the query actually returned data
    if !result.is_empty() {
        let metric_names = crate::usage_tracker::extract_metrics_from_query(&params.query);
        for name in metric_names {
            state.usage.track(crate::usage_tracker::UsageEvent {
                signal_name: name,
                signal_type: "metric".to_string(),
                source: "prom_api".to_string(),
            });
        }
    }

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
        // Parse with promql-parser and extract the VectorSelector
        let expr = match promql_parser::parser::parse(expr_str) {
            Ok(e) => e,
            Err(_) => continue,
        };

        let vs = match &expr {
            promql_parser::parser::Expr::VectorSelector(vs) => vs,
            _ => continue,
        };

        let mut where_parts = vec![
            format!("TimeUnix >= toDateTime64({}, 9)", start_secs as i64),
            format!("TimeUnix <= toDateTime64({}, 9)", end_secs as i64),
        ];
        if let Some(name) = &vs.name {
            if !name.is_empty() {
                where_parts.push(format!("MetricName = '{}'", name.replace('\'', "\\'")));
            }
        }
        // Add matchers (skip __name__ since we handle it via vs.name)
        let non_name_matchers: Vec<_> = vs.matchers.matchers.iter()
            .filter(|m| m.name != "__name__")
            .cloned()
            .collect();
        where_parts.extend(promql::matchers_to_sql(&non_name_matchers));

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

#[derive(Debug, Deserialize)]
pub struct LabelsParams {
    #[serde(rename = "match[]")]
    pub match_expr: Option<String>,
}

pub async fn prom_labels(
    State(state): State<AppState>,
    Query(params): Query<LabelsParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Return well-known labels plus discovered attribute keys
    let mut labels = vec![
        "__name__".to_string(),
        "service_name".to_string(),
        "job".to_string(),
    ];

    // Build optional metric filter — parse Prometheus match expression
    // e.g. {__name__="http_requests_total"} → MetricName = 'http_requests_total'
    // e.g. {__name__=~"http_.*"} → MetricName LIKE 'http_%'
    let metric_filter = params.match_expr.as_ref().and_then(|m| {
        let trimmed = m.trim().trim_start_matches('{').trim_end_matches('}');
        // Exact match: __name__="value"
        if let Some(val) = trimmed.strip_prefix("__name__=\"").and_then(|s| s.strip_suffix('"')) {
            let escaped = val.replace('\'', "\\'");
            return Some(format!("AND MetricName = '{escaped}'"));
        }
        // Regex match: __name__=~"value"
        if let Some(val) = trimmed.strip_prefix("__name__=~\"").and_then(|s| s.strip_suffix('"')) {
            let like = val.replace(".*", "%").replace('.', "_").replace('\'', "\\'");
            return Some(format!("AND MetricName LIKE '{like}'"));
        }
        // Fallback: treat as literal metric name
        if !trimmed.is_empty() {
            let escaped = trimmed.replace('\'', "\\'");
            return Some(format!("AND MetricName = '{escaped}'"));
        }
        None
    });

    // Discover attribute keys from gauge and sum tables
    for table in &["otel_metrics_gauge", "otel_metrics_sum"] {
        let sql = format!(
            "SELECT DISTINCT arrayJoin(mapKeys(Attributes)) AS name \
             FROM {table} \
             WHERE TimeUnix >= now() - INTERVAL 1 HOUR \
             {filter} \
             ORDER BY name \
             LIMIT 200",
            filter = metric_filter.as_deref().unwrap_or("")
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
    Query(params): Query<LabelsParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut values = Vec::new();

    let metric_filter = params.match_expr.as_ref().and_then(|m| {
        let trimmed = m.trim().trim_start_matches('{').trim_end_matches('}');
        if let Some(val) = trimmed.strip_prefix("__name__=\"").and_then(|s| s.strip_suffix('"')) {
            let escaped = val.replace('\'', "\\'");
            return Some(format!("AND MetricName = '{escaped}'"));
        }
        if let Some(val) = trimmed.strip_prefix("__name__=~\"").and_then(|s| s.strip_suffix('"')) {
            let like = val.replace(".*", "%").replace('.', "_").replace('\'', "\\'");
            return Some(format!("AND MetricName LIKE '{like}'"));
        }
        if !trimmed.is_empty() {
            let escaped = trimmed.replace('\'', "\\'");
            return Some(format!("AND MetricName = '{escaped}'"));
        }
        None
    });
    let filter = metric_filter.as_deref().unwrap_or("");

    for table in &["otel_metrics_gauge", "otel_metrics_sum"] {
        let sql = match label_name.as_str() {
            "__name__" => format!(
                "SELECT DISTINCT MetricName AS value FROM {table} \
                 WHERE TimeUnix >= now() - INTERVAL 1 HOUR \
                 {filter} \
                 ORDER BY value LIMIT 500"
            ),
            "service_name" | "job" => format!(
                "SELECT DISTINCT ServiceName AS value FROM {table} \
                 WHERE TimeUnix >= now() - INTERVAL 1 HOUR \
                 {filter} \
                 ORDER BY value LIMIT 500"
            ),
            _ => {
                let escaped = label_name.replace('\'', "\\'");
                format!(
                    "SELECT DISTINCT Attributes['{escaped}'] AS value FROM {table} \
                     WHERE TimeUnix >= now() - INTERVAL 1 HOUR \
                       AND value != '' \
                     {filter} \
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

// ═══ Helpers ═══

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
