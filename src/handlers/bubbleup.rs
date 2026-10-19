use axum::{Extension, Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::TenantContext;
use crate::models::query::{Filter, FilterOp};
use crate::query_builder::{format_array_value, format_value, resolve_field};

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
    /// Optional free-text search (same syntax as the explore search box),
    /// applied to both windows so the cohorts match the explore result set.
    #[serde(default)]
    pub search: Option<String>,
    /// Max number of values to return per dimension (default 10).
    pub top_k: Option<u32>,
    /// Optional span-duration bounds defining the selected cohort. These make
    /// a latency brush compare the dots the user actually enclosed instead of
    /// every span that happened during the same time interval.
    #[serde(default)]
    pub selection_min_duration_ns: Option<u64>,
    #[serde(default)]
    pub selection_max_duration_ns: Option<u64>,
    /// Restrict the selected span cohort to errors / HTTP 5xx responses.
    #[serde(default)]
    pub selection_errors_only: bool,
    /// Remove the selected cohort from the baseline so lift is not diluted by
    /// comparing a selection against a population that contains itself.
    #[serde(default)]
    pub exclude_selection_from_baseline: bool,
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

/// Build the free-text search condition shared by both windows, using the same
/// span/log search compilers as the explore query so the cohorts cover exactly
/// the spans or logs the user is looking at.
fn build_search_condition(search: Option<&str>, signal: &str) -> Option<String> {
    let term = search.map(str::trim).filter(|t| !t.is_empty())?;
    let sql = if signal == "logs" {
        crate::query_builder::build_log_search_sql(term)
    } else {
        crate::query_builder::build_span_search_sql(term)
    }?;
    Some(format!("AND ({sql})"))
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
                    "deployment.environment" | "deployment.environment.name" => {
                        // Existing parts may have been materialized before Rush recognized
                        // deployment.environment.name.
                        "if(notEmpty(mat_environment), mat_environment, ResourceAttributes['deployment.environment.name'])".to_string()
                    }
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

fn comparison_conditions(
    req: &BubbleUpRequest,
    ts_col: &str,
    sel_from: &str,
    sel_to: &str,
    base_from: &str,
    base_to: &str,
) -> Result<(String, String), (StatusCode, String)> {
    if let (Some(min), Some(max)) = (req.selection_min_duration_ns, req.selection_max_duration_ns) {
        if min > max {
            return Err((
                StatusCode::BAD_REQUEST,
                "selection minimum duration must not exceed maximum duration".into(),
            ));
        }
    }
    if req.signal != "spans"
        && (req.selection_min_duration_ns.is_some()
            || req.selection_max_duration_ns.is_some()
            || req.selection_errors_only)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "duration/error cohort filters are supported only for spans".into(),
        ));
    }

    let mut cohort_parts = Vec::new();
    if let Some(min) = req.selection_min_duration_ns {
        cohort_parts.push(format!("duration_ns >= {min}"));
    }
    if let Some(max) = req.selection_max_duration_ns {
        cohort_parts.push(format!("duration_ns <= {max}"));
    }
    if req.selection_errors_only {
        cohort_parts.push(crate::query_builder::ERROR_SPAN.to_string());
    }
    let cohort_suffix = if cohort_parts.is_empty() {
        String::new()
    } else {
        format!(" AND {}", cohort_parts.join(" AND "))
    };
    let selection = format!(
        "{ts_col} >= parseDateTime64BestEffort('{sel_from}') AND {ts_col} <= parseDateTime64BestEffort('{sel_to}'){cohort_suffix}"
    );
    let baseline_time = format!(
        "{ts_col} >= parseDateTime64BestEffort('{base_from}') AND {ts_col} <= parseDateTime64BestEffort('{base_to}')"
    );
    let baseline = if req.exclude_selection_from_baseline {
        format!("{baseline_time} AND NOT ({selection})")
    } else {
        baseline_time
    };
    Ok((selection, baseline))
}

/// Build the FROM-scope clause shared by the selection (cohort A) and baseline
/// (cohort B): tenant + spanning time range + the explore filters and free-text
/// search. Both cohorts are `countIf`s over this one scan, so anything that
/// narrows the explore result set must be applied here.
fn build_scan_clause(
    req: &BubbleUpRequest,
    escaped_tenant: &str,
    ts_col: &str,
    earliest: &str,
    latest: &str,
) -> String {
    let mut additional_filters = build_filter_conditions(&req.filters, &req.signal);
    let search_condition = build_search_condition(req.search.as_deref(), &req.signal);
    if let Some(cond) = &search_condition {
        if !additional_filters.is_empty() {
            additional_filters.push(' ');
        }
        additional_filters.push_str(cond);
    }

    // A user `Body LIKE` filter (logs signal) or a free-text search is backed by a
    // text/skip index, which an explicit PREWHERE on tenant/time would defeat (CH reads
    // the whole index instead of pruning granules). When such a predicate is present,
    // fold the scope into a single WHERE and let `optimize_move_to_prewhere` re-derive
    // the prewhere. Pure-PK filters keep the explicit PREWHERE (the efficient path).
    let uses_index_predicate = search_condition.is_some()
        || req
            .filters
            .iter()
            .any(|f| matches!(f.op, FilterOp::Like | FilterOp::NotLike));
    if uses_index_predicate {
        format!(
            "WHERE tenant_id = '{escaped_tenant}' \
               AND {ts_col} >= parseDateTime64BestEffort('{earliest}') \
               AND {ts_col} <= parseDateTime64BestEffort('{latest}') {additional_filters}"
        )
    } else {
        format!(
            "PREWHERE tenant_id = '{escaped_tenant}' \
               AND {ts_col} >= parseDateTime64BestEffort('{earliest}') \
               AND {ts_col} <= parseDateTime64BestEffort('{latest}') \
             WHERE TRUE {additional_filters}"
        )
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

    let earliest = if sel_from < base_from {
        &sel_from
    } else {
        &base_from
    };
    let latest = if sel_to > base_to { &sel_to } else { &base_to };

    let (selection_condition, baseline_condition) =
        comparison_conditions(&req, ts_col, &sel_from, &sel_to, &base_from, &base_to)?;

    let scan_clause = build_scan_clause(&req, &escaped_tenant, ts_col, earliest, latest);

    // ── Total counts query ──
    let totals_sql = format!(
        "SELECT \
            countIf({selection_condition}) AS selection_count, \
            countIf({baseline_condition}) AS baseline_count \
         FROM {table} \
         {scan_clause}"
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
        args.push(format!(
            "toString({})",
            dimensions.last().expect("non-empty dimensions")
        ));
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
            countIf({selection_condition}) AS sel_count, \
            countIf({baseline_condition}) AS base_count \
         FROM {table} \
         {scan_clause} \
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
        crate::api_error::internal_legacy("bubbleup.totals", e)
    })?;

    let selection_count = totals.selection_count;
    let baseline_count = totals.baseline_count;

    let dimension_rows = dimension_result.map_err(|e| {
        tracing::error!(error = %e, signal = %req.signal, handler = "bubbleup", "dimensions query failed");
        crate::api_error::internal_legacy("bubbleup.dimensions", e)
    })?;

    // Bucket rows back into per-dimension lists (rows arrive ordered by dim_idx).
    let mut per_dim_rows: Vec<Vec<DimensionRow>> =
        (0..dimensions.len()).map(|_| Vec::new()).collect();
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
        values.sort_by(|a, b| {
            b.lift
                .partial_cmp(&a.lift)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_environment_dimension_accepts_current_and_legacy_otel_names() {
        let current = resolve_log_filter_field("resource.deployment.environment.name");
        let legacy = resolve_log_filter_field("resource.deployment.environment");

        assert_eq!(current, legacy);
        assert!(current.contains("mat_environment"));
        assert!(current.contains("deployment.environment.name"));
    }

    fn request(signal: &str) -> BubbleUpRequest {
        BubbleUpRequest {
            selection: TimeWindow {
                from: "2026-01-01T00:05:00Z".into(),
                to: "2026-01-01T00:06:00Z".into(),
            },
            baseline: TimeWindow {
                from: "2026-01-01T00:00:00Z".into(),
                to: "2026-01-01T00:10:00Z".into(),
            },
            signal: signal.into(),
            filters: vec![],
            search: None,
            top_k: None,
            selection_min_duration_ns: None,
            selection_max_duration_ns: None,
            selection_errors_only: false,
            exclude_selection_from_baseline: false,
        }
    }

    #[test]
    fn span_cohort_keeps_duration_and_error_bounds_and_excludes_it_from_baseline() {
        let mut req = request("spans");
        req.selection_min_duration_ns = Some(100_000_000);
        req.selection_max_duration_ns = Some(2_000_000_000);
        req.selection_errors_only = true;
        req.exclude_selection_from_baseline = true;
        let (selection, baseline) = comparison_conditions(
            &req,
            "timestamp",
            "sel-from",
            "sel-to",
            "base-from",
            "base-to",
        )
        .unwrap();
        assert!(selection.contains("duration_ns >= 100000000"));
        assert!(selection.contains("duration_ns <= 2000000000"));
        assert!(selection.contains(crate::query_builder::ERROR_SPAN));
        assert!(selection.contains("parseDateTime64BestEffort('sel-from')"));
        assert!(baseline.contains("AND NOT (timestamp >="));
        assert!(baseline.contains(&selection));
    }

    #[test]
    fn legacy_comparison_keeps_full_baseline_when_exclusion_is_off() {
        let req = request("logs");
        let (_, baseline) = comparison_conditions(
            &req,
            "Timestamp",
            "sel-from",
            "sel-to",
            "base-from",
            "base-to",
        )
        .unwrap();
        assert!(!baseline.contains("NOT"));
        assert!(baseline.contains("base-from"));
    }

    // Regression: a free-text explore search must scope BOTH cohorts. Previously
    // `search` was not part of the request, so "Slowest 5%" vs "Everything else"
    // ran over every span in the time range instead of the matching spans.
    #[test]
    fn span_search_scopes_shared_scan_for_both_cohorts() {
        let mut req: BubbleUpRequest = serde_json::from_value(serde_json::json!({
            "selection": { "from": "2026-01-01T00:00:00Z", "to": "2026-01-01T01:00:00Z" },
            "baseline": { "from": "2026-01-01T00:00:00Z", "to": "2026-01-01T01:00:00Z" },
            "signal": "spans",
            "filters": [{ "field": "service_name", "op": "=", "value": "payments" }],
            "search": "charge",
            "selection_min_duration_ns": 50_000_000u64,
            "exclude_selection_from_baseline": true,
        }))
        .unwrap();
        assert_eq!(req.search.as_deref(), Some("charge"));

        let scan = build_scan_clause(&req, "default", "timestamp", "a", "b");
        let search_sql = crate::query_builder::build_span_search_sql("charge").unwrap();
        assert!(scan.contains(&format!("AND ({search_sql})")), "{scan}");
        assert!(scan.contains("service_name = 'payments'"), "{scan}");
        // Text-index predicate: scope must be folded into WHERE, not PREWHERE.
        assert!(scan.starts_with("WHERE tenant_id = 'default'"), "{scan}");
        assert!(!scan.contains("PREWHERE"), "{scan}");

        // Blank search leaves the scan unchanged (and keeps the PREWHERE path).
        req.search = Some("   ".into());
        let scan = build_scan_clause(&req, "default", "timestamp", "a", "b");
        assert!(scan.starts_with("PREWHERE"), "{scan}");
        assert!(!scan.contains(&search_sql), "{scan}");
    }

    #[test]
    fn log_search_uses_log_search_compiler() {
        let mut req = request("logs");
        req.search = Some("timeout".into());
        let scan = build_scan_clause(&req, "default", "Timestamp", "a", "b");
        let log_sql = crate::query_builder::build_log_search_sql("timeout").unwrap();
        assert!(scan.contains(&format!("AND ({log_sql})")), "{scan}");
        assert!(!scan.contains("PREWHERE"), "{scan}");
    }

    #[test]
    fn search_is_optional_in_request_body() {
        let req: BubbleUpRequest = serde_json::from_value(serde_json::json!({
            "selection": { "from": "a", "to": "b" },
            "baseline": { "from": "a", "to": "b" },
            "signal": "spans",
        }))
        .unwrap();
        assert!(req.search.is_none());
        let scan = build_scan_clause(&req, "default", "timestamp", "a", "b");
        assert!(scan.starts_with("PREWHERE"), "{scan}");
    }

    #[test]
    fn rejects_reversed_duration_bounds() {
        let mut req = request("spans");
        req.selection_min_duration_ns = Some(20);
        req.selection_max_duration_ns = Some(10);
        assert!(comparison_conditions(&req, "timestamp", "a", "b", "c", "d").is_err());
    }
}
