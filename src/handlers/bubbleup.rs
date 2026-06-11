use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Extension,
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::TenantContext;
use crate::models::query::{Filter, FilterOp};
use crate::query_builder::{format_value, format_array_value, resolve_field};

// ── Request types ──

#[derive(Debug, Deserialize)]
pub struct BubbleUpRequest {
    /// The selected anomalous time window.
    pub selection: TimeWindow,
    /// The baseline comparison window (usually the full visible range minus the selection).
    pub baseline: TimeWindow,
    /// Which signal to analyze: "spans" or "logs".
    pub signal: String,
    /// Optional filters to apply to both windows (same as explore filters).
    #[serde(default)]
    pub filters: Vec<Filter>,
    /// Max number of values to return per dimension (default 10).
    pub top_k: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct TimeWindow {
    pub from: String,
    pub to: String,
}

// ── Response types ──

#[derive(Debug, Serialize)]
pub struct BubbleUpResponse {
    pub dimensions: Vec<DimensionComparison>,
    pub selection_count: u64,
    pub baseline_count: u64,
}

#[derive(Debug, Serialize)]
pub struct DimensionComparison {
    pub name: String,
    pub values: Vec<ValueComparison>,
}

#[derive(Debug, Serialize)]
pub struct ValueComparison {
    pub value: String,
    pub selection_count: u64,
    pub baseline_count: u64,
    pub selection_pct: f64,
    pub baseline_pct: f64,
    pub lift: f64,
}

// ── ClickHouse row types ──

#[derive(Debug, clickhouse::Row, Deserialize)]
struct DimensionRow {
    dim_idx: u8,
    value: String,
    sel_count: u64,
    base_count: u64,
}

#[derive(Debug, clickhouse::Row, Deserialize)]
struct TotalRow {
    selection_count: u64,
    baseline_count: u64,
}

// ── Signal configuration ──

struct SignalConfig {
    table: &'static str,
    timestamp_col: &'static str,
    dimensions: Vec<&'static str>,
}

/// Maps a ClickHouse column name to a human-friendly display name.
fn friendly_dimension_name(col: &str) -> &str {
    match col {
        "service_name" | "ServiceName" => "Service",
        "span_name" => "Operation",
        "http_method" => "Method",
        "http_path" => "Path",
        "http_status_code" => "Status Code",
        "status" => "Span Status",
        "kind" => "Span Kind",
        "SeverityText" => "Severity",
        "ScopeName" => "Scope",
        "mat_k8s_namespace" => "K8s Namespace",
        "mat_k8s_deployment" => "K8s Deployment",
        _ => col,
    }
}

fn signal_config(signal: &str) -> Result<SignalConfig, (StatusCode, String)> {
    match signal {
        "spans" => Ok(SignalConfig {
            table: "spans",
            timestamp_col: "timestamp",
            dimensions: vec![
                "service_name",
                "span_name",
                "http_method",
                "http_path",
                "http_status_code",
                "status",
                "kind",
            ],
        }),
        "logs" => Ok(SignalConfig {
            table: "logs",
            timestamp_col: "Timestamp",
            dimensions: vec![
                "ServiceName",
                "SeverityText",
                "ScopeName",
                "mat_k8s_namespace",
                "mat_k8s_deployment",
            ],
        }),
        _ => Err((
            StatusCode::BAD_REQUEST,
            format!("unsupported signal type: {signal}. Expected \"spans\" or \"logs\""),
        )),
    }
}

// ── Filter clause builder ──

/// Build additional filter SQL conditions that apply to both windows.
fn build_filter_conditions(filters: &[Filter], signal: &str) -> String {
    if filters.is_empty() {
        return String::new();
    }

    let mut parts = Vec::with_capacity(filters.len());
    for filter in filters {
        let field = if signal == "logs" {
            resolve_log_filter_field(&filter.field)
        } else {
            resolve_field(&filter.field)
        };
        let condition = match &filter.op {
            FilterOp::Eq => format!("{field} = {}", format_value(&filter.value)),
            FilterOp::Ne => format!("{field} != {}", format_value(&filter.value)),
            FilterOp::Gt => format!("{field} > {}", format_value(&filter.value)),
            FilterOp::Gte => format!("{field} >= {}", format_value(&filter.value)),
            FilterOp::Lt => format!("{field} < {}", format_value(&filter.value)),
            FilterOp::Lte => format!("{field} <= {}", format_value(&filter.value)),
            FilterOp::Like => format!("{field} LIKE {}", format_value(&filter.value)),
            FilterOp::NotLike => format!("{field} NOT LIKE {}", format_value(&filter.value)),
            FilterOp::In => format!("{field} IN {}", format_array_value(&filter.value)),
            FilterOp::NotIn => format!("{field} NOT IN {}", format_array_value(&filter.value)),
        };
        // Prefixed with AND so it appends cleanly after "WHERE TRUE"
        parts.push(format!("AND {condition}"));
    }

    parts.join(" ")
}

/// Resolve a log filter field to its ClickHouse column (mirrors logs handler).
fn resolve_log_filter_field(field: &str) -> String {
    match field {
        "service_name" | "ServiceName" => "ServiceName".to_string(),
        "severity" | "severity_text" | "SeverityText" => "SeverityText".to_string(),
        "severity_number" | "SeverityNumber" => "SeverityNumber".to_string(),
        "body" | "Body" => "Body".to_string(),
        "trace_id" | "TraceId" => "TraceId".to_string(),
        "span_id" | "SpanId" => "SpanId".to_string(),
        "scope_name" | "ScopeName" => "ScopeName".to_string(),
        _ => {
            if let Some(attr) = field.strip_prefix("resource.") {
                match attr {
                    "k8s.namespace.name" => "mat_k8s_namespace".to_string(),
                    "k8s.pod.name" => "mat_k8s_pod".to_string(),
                    "k8s.container.name" => "mat_k8s_container".to_string(),
                    "k8s.deployment.name" => "mat_k8s_deployment".to_string(),
                    "deployment.environment" => "mat_environment".to_string(),
                    _ => format!("ResourceAttributes['{attr}']"),
                }
            } else if let Some(attr) = field.strip_prefix("log.") {
                format!("LogAttributes['{attr}']")
            } else {
                field.to_string()
            }
        }
    }
}

// ── Handler ──

/// BubbleUp comparison analysis: compare the distribution of every dimension
/// between a selection window and a baseline window, returning which values
/// are statistically over-represented in the selection.
pub async fn bubbleup(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<BubbleUpRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let start = std::time::Instant::now();
    let tenant_id = &tenant.tenant_id;
    let escaped_tenant = crate::query_builder::escape_string_literal(&tenant_id);
    let top_k = req.top_k.unwrap_or(10).min(50);

    let config = signal_config(&req.signal)?;
    let table = config.table;
    let ts_col = config.timestamp_col;
    let dimensions = &config.dimensions;

    // Compute the earliest and latest timestamps spanning both windows to
    // minimize the ClickHouse scan range.
    let sel_from = crate::query_builder::escape_string_literal(&req.selection.from);
    let sel_to = crate::query_builder::escape_string_literal(&req.selection.to);
    let base_from = crate::query_builder::escape_string_literal(&req.baseline.from);
    let base_to = crate::query_builder::escape_string_literal(&req.baseline.to);

    let earliest = if sel_from < base_from { &sel_from } else { &base_from };
    let latest = if sel_to > base_to { &sel_to } else { &base_to };

    let additional_filters = build_filter_conditions(&req.filters, &req.signal);

    // ── Total counts query ──
    // PREWHERE on tenant_id + time range: ClickHouse reads only those compact columns
    // first, discards non-matching rows, then loads the remaining columns — reducing I/O
    // significantly for multi-tenant tables.
    let totals_sql = format!(
        "SELECT \
            countIf({ts_col} >= parseDateTimeBestEffort('{sel_from}') \
                AND {ts_col} <= parseDateTimeBestEffort('{sel_to}')) AS selection_count, \
            countIf({ts_col} >= parseDateTimeBestEffort('{base_from}') \
                AND {ts_col} <= parseDateTimeBestEffort('{base_to}')) AS baseline_count \
         FROM {table} \
         PREWHERE tenant_id = '{escaped_tenant}' \
           AND {ts_col} >= parseDateTimeBestEffort('{earliest}') \
           AND {ts_col} <= parseDateTimeBestEffort('{latest}') \
         WHERE TRUE {additional_filters}"
    );

    // ── Dimension query ──
    // One GROUPING SETS query replaces the previous per-dimension parallel queries:
    // the (identical) filtered window is scanned ONCE instead of once per dimension
    // (7-8×). grouping(col) = 0 identifies which set a result row belongs to, and
    // `LIMIT {top_k} BY dim_idx` (after ORDER BY dim_idx, sel_count DESC) reproduces
    // each per-dimension `ORDER BY sel_count DESC LIMIT {top_k}` exactly — validated
    // against the per-dimension queries on live ClickHouse 26.1 (identical rows; only
    // tie order among equal sel_counts differs, which the old parallel queries didn't
    // guarantee either).
    let dim_idx_expr = {
        let mut args: Vec<String> = dimensions
            .iter()
            .take(dimensions.len() - 1)
            .enumerate()
            .map(|(i, dim)| format!("grouping({dim}) = 0, {i}"))
            .collect();
        args.push(format!("{}", dimensions.len() - 1));
        format!("multiIf({})", args.join(", "))
    };
    let value_expr = {
        let mut args: Vec<String> = dimensions
            .iter()
            .take(dimensions.len() - 1)
            .map(|dim| format!("grouping({dim}) = 0, toString({dim})"))
            .collect();
        args.push(format!("toString({})", dimensions.last().expect("non-empty dimensions")));
        format!("multiIf({})", args.join(", "))
    };
    let grouping_sets = dimensions
        .iter()
        .map(|dim| format!("({dim})"))
        .collect::<Vec<_>>()
        .join(", ");

    let dimensions_sql = format!(
        "SELECT \
            toUInt8({dim_idx_expr}) AS dim_idx, \
            {value_expr} AS value, \
            countIf({ts_col} >= parseDateTimeBestEffort('{sel_from}') \
                AND {ts_col} <= parseDateTimeBestEffort('{sel_to}')) AS sel_count, \
            countIf({ts_col} >= parseDateTimeBestEffort('{base_from}') \
                AND {ts_col} <= parseDateTimeBestEffort('{base_to}')) AS base_count \
         FROM {table} \
         PREWHERE tenant_id = '{escaped_tenant}' \
           AND {ts_col} >= parseDateTimeBestEffort('{earliest}') \
           AND {ts_col} <= parseDateTimeBestEffort('{latest}') \
         WHERE TRUE {additional_filters} \
         GROUP BY GROUPING SETS ({grouping_sets}) \
         HAVING sel_count > 0 OR base_count > 0 \
         ORDER BY dim_idx ASC, sel_count DESC \
         LIMIT {top_k} BY dim_idx"
    );

    // Execute totals + dimensions queries in parallel.
    let (totals_result, dimension_result) = tokio::join!(
        crate::tenant_query(&state.ch, &totals_sql, tenant_id).fetch_one::<TotalRow>(),
        crate::tenant_query(&state.ch, &dimensions_sql, tenant_id).fetch_all::<DimensionRow>(),
    );

    let totals = totals_result.map_err(|e| {
        tracing::error!(error = %e, signal = %req.signal, handler = "bubbleup", "totals query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, format!("totals query failed: {e}"))
    })?;

    let selection_count = totals.selection_count;
    let baseline_count = totals.baseline_count;

    let dimension_rows = dimension_result.map_err(|e| {
        tracing::error!(error = %e, signal = %req.signal, handler = "bubbleup", "dimensions query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, format!("dimensions query failed: {e}"))
    })?;

    // Bucket rows back into per-dimension lists (rows arrive ordered by dim_idx).
    let mut per_dim_rows: Vec<Vec<DimensionRow>> = (0..dimensions.len()).map(|_| Vec::new()).collect();
    for row in dimension_rows {
        let idx = row.dim_idx as usize;
        if idx < per_dim_rows.len() {
            per_dim_rows[idx].push(row);
        }
    }

    // ── Compute percentages and lift for each dimension ──
    let mut dim_comparisons = Vec::with_capacity(dimensions.len());
    for (i, rows) in per_dim_rows.into_iter().enumerate() {
        let dim_name = dimensions[i];

        let mut values: Vec<ValueComparison> = rows
            .into_iter()
            .map(|row| {
                let sel_pct = if selection_count > 0 {
                    (row.sel_count as f64 / selection_count as f64) * 100.0
                } else {
                    0.0
                };
                let base_pct = if baseline_count > 0 {
                    (row.base_count as f64 / baseline_count as f64) * 100.0
                } else {
                    0.0
                };
                let lift = sel_pct / base_pct.max(0.01);

                ValueComparison {
                    value: row.value,
                    selection_count: row.sel_count,
                    baseline_count: row.base_count,
                    selection_pct: (sel_pct * 100.0).round() / 100.0,
                    baseline_pct: (base_pct * 100.0).round() / 100.0,
                    lift: (lift * 100.0).round() / 100.0,
                }
            })
            .collect();

        // Sort by lift descending (most over-represented first).
        values.sort_by(|a, b| b.lift.partial_cmp(&a.lift).unwrap_or(std::cmp::Ordering::Equal));

        dim_comparisons.push(DimensionComparison {
            name: friendly_dimension_name(dim_name).to_string(),
            values,
        });
    }

    tracing::info!(
        signal = %req.signal,
        tenant_id = %tenant_id,
        query = "bubbleup",
        dimensions = dimensions.len(),
        selection_count = selection_count,
        baseline_count = baseline_count,
        duration_ms = start.elapsed().as_millis() as u64,
        "bubbleup analysis completed"
    );

    Ok(Json(BubbleUpResponse {
        dimensions: dim_comparisons,
        selection_count,
        baseline_count,
    }))
}
