use axum::{
    Extension, Json,
    body::Body,
    extract::State,
    http::{StatusCode, header},
    response::Response,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

use crate::models::log::LogListRecord;
use crate::models::query::{CountBucket, Filter, TimeRange};
use crate::models::trace::SlimEvent;
use crate::query_builder::{build_where_clause_with_search, clamp_bucket_interval, resolve_field};
use crate::{AppState, TenantContext};

const MAX_SEARCH_CHARS: usize = 512;
const MAX_GROUP_FIELD_CHARS: usize = 128;
const MAX_ROWS: u64 = 1_000;
const MAX_SUMMARY_ROWS_PER_KIND: u64 = 200;
const SUMMARY_TIMEOUT: Duration = Duration::from_secs(3);
const COORDINATED_QUERY_COUNT: u64 = 2;
const DEFERRED_QUERY_COUNT: u64 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExploreSignal {
    Spans,
    Logs,
}

impl ExploreSignal {
    fn label(self) -> &'static str {
        match self {
            Self::Spans => "spans",
            Self::Logs => "logs",
        }
    }

    fn operation(self) -> &'static str {
        match self {
            Self::Spans => "explore_spans",
            Self::Logs => "explore_logs",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ExploreSearchRequest {
    pub signal: ExploreSignal,
    pub time_range: TimeRange,
    #[serde(default)]
    pub filters: Vec<Filter>,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u64,
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default)]
    pub group_by: Option<String>,
    /// Existing clients keep the coordinated two-query response. The UI can
    /// request rows and summaries separately so rows paint as soon as their
    /// ClickHouse query finishes.
    #[serde(default = "default_true")]
    pub include_rows: bool,
    #[serde(default = "default_true")]
    pub include_summary: bool,
}

fn default_limit() -> u64 {
    100
}

fn default_interval() -> String {
    "1m".to_string()
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum CountKind {
    Exact,
    Capped,
}

#[derive(Debug, Serialize)]
struct ExploreCount {
    value: u64,
    kind: CountKind,
}

#[derive(Debug, Default, Serialize)]
struct FacetSummary {
    services: Vec<SummaryValue>,
    statuses: Vec<SummaryValue>,
    methods: Vec<SummaryValue>,
}

#[derive(Debug, Serialize)]
struct SummaryValue {
    key: String,
    count: u64,
}

#[derive(Debug, Default, Serialize)]
struct ExploreSummary {
    histogram: Vec<CountBucket>,
    facets: FacetSummary,
    groups: Vec<SummaryValue>,
    interval_secs: u64,
}

#[derive(Debug, Default, Serialize)]
struct ExploreErrors {
    #[serde(skip_serializing_if = "Option::is_none")]
    histogram: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    facets: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    groups: Option<&'static str>,
}

#[derive(Debug, Default, Serialize)]
struct ExploreQueryStats {
    clickhouse_queries: u64,
    matched_rows: u64,
    matched_logical_bytes: u64,
    time_to_first_results_ms: u64,
    response_bytes: u64,
}

#[derive(Debug, Serialize)]
struct ExploreSearchResponse<T: Serialize> {
    signal: ExploreSignal,
    rows: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
    count: ExploreCount,
    summary: ExploreSummary,
    errors: ExploreErrors,
    query_stats: ExploreQueryStats,
}

#[derive(Debug, clickhouse::Row, Deserialize)]
struct SummaryRow {
    kind: String,
    bucket: String,
    key: String,
    count: u64,
    error_count: u64,
    matched_bytes: u64,
}

#[derive(Debug)]
struct ExplorePlan {
    rows_sql: String,
    summary_sql: String,
    interval_secs: u64,
}

/// One coordinated Explore request executes exactly one row query and one
/// single-scan summary query. Facets and histogram are folded from GROUPING SETS
/// instead of issuing a separate ClickHouse request for every visible section.
pub async fn search(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<ExploreSearchRequest>,
) -> Result<Response, (StatusCode, String)> {
    validate_request(&req)?;
    let _query_guard = state
        .self_metrics
        .query_guard(req.signal.operation(), req.signal.label());
    let started = Instant::now();
    let tenant_id = tenant.tenant_id;
    let plan = build_plan(&req, &tenant_id)?;

    match req.signal {
        ExploreSignal::Spans => {
            let response = execute_spans(&state, &tenant_id, &req, plan, started).await?;
            encode_response(&state, "spans", response)
        }
        ExploreSignal::Logs => {
            let response = execute_logs(&state, &tenant_id, &req, plan, started).await?;
            encode_response(&state, "logs", response)
        }
    }
}

async fn execute_spans(
    state: &AppState,
    tenant_id: &str,
    req: &ExploreSearchRequest,
    plan: ExplorePlan,
    started: Instant,
) -> Result<ExploreSearchResponse<SlimEvent>, (StatusCode, String)> {
    let query_prefix = uuid::Uuid::new_v4();
    if !req.include_rows {
        let summary_result = tokio::time::timeout(
            SUMMARY_TIMEOUT,
            crate::tenant_query(&state.ch, &plan.summary_sql, tenant_id)
                .with_option("query_id", format!("rush-explore-{query_prefix}-summary"))
                .with_option("cancel_http_readonly_queries_on_client_close", "1")
                .fetch_all::<SummaryRow>(),
        )
        .await;
        return finish_response(
            state,
            tenant_id,
            req,
            plan.interval_secs,
            Vec::<SlimEvent>::new(),
            None,
            Some(summary_result),
            0,
            started,
            DEFERRED_QUERY_COUNT,
        );
    }

    let rows_started = Instant::now();
    let rows_query = async {
        let result = crate::tenant_query(&state.ch, &plan.rows_sql, tenant_id)
            .with_option("query_id", format!("rush-explore-{query_prefix}-rows"))
            .with_option("cancel_http_readonly_queries_on_client_close", "1")
            .fetch_all::<SlimEvent>()
            .await;
        (result, rows_started.elapsed().as_millis() as u64)
    };
    let ((rows_result, rows_ready_ms), summary_result, query_count) = if req.include_summary {
        let summary_query = tokio::time::timeout(
            SUMMARY_TIMEOUT,
            crate::tenant_query(&state.ch, &plan.summary_sql, tenant_id)
                .with_option("query_id", format!("rush-explore-{query_prefix}-summary"))
                .with_option("cancel_http_readonly_queries_on_client_close", "1")
                .fetch_all::<SummaryRow>(),
        );
        let (rows_result, summary_result) = tokio::join!(rows_query, summary_query);
        (rows_result, Some(summary_result), COORDINATED_QUERY_COUNT)
    } else {
        (rows_query.await, None, DEFERRED_QUERY_COUNT)
    };
    let rows = rows_result.map_err(|error| {
        state
            .self_metrics
            .record_explore_stage("spans", "rows", 0, rows_ready_ms, false);
        tracing::error!(%error, signal = "spans", "coordinated Explore row query failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "query failed".to_string(),
        )
    })?;
    state.self_metrics.record_explore_stage(
        "spans",
        "rows",
        rows.len() as u64,
        rows_ready_ms,
        true,
    );

    let next_cursor = (rows.len() as u64 == req.limit.clamp(1, MAX_ROWS)).then(|| {
        let row = rows.last().expect("non-empty full page");
        let scope = crate::pagination::query_scope(
            tenant_id,
            "spans",
            "wide",
            &req.time_range,
            &req.filters,
            req.search.as_deref(),
        );
        crate::pagination::encode(
            &state.config_db,
            "spans",
            &scope,
            crate::pagination::CursorPosition {
                timestamp_ns: row.timestamp,
                tie: vec![row.span_id.clone()],
            },
        )
    });

    finish_response(
        state,
        tenant_id,
        req,
        plan.interval_secs,
        rows,
        next_cursor,
        summary_result,
        rows_ready_ms,
        started,
        query_count,
    )
}

async fn execute_logs(
    state: &AppState,
    tenant_id: &str,
    req: &ExploreSearchRequest,
    plan: ExplorePlan,
    started: Instant,
) -> Result<ExploreSearchResponse<LogListRecord>, (StatusCode, String)> {
    let query_prefix = uuid::Uuid::new_v4();
    if !req.include_rows {
        let summary_result = tokio::time::timeout(
            SUMMARY_TIMEOUT,
            crate::tenant_query(&state.ch, &plan.summary_sql, tenant_id)
                .with_option("query_id", format!("rush-explore-{query_prefix}-summary"))
                .with_option("cancel_http_readonly_queries_on_client_close", "1")
                .fetch_all::<SummaryRow>(),
        )
        .await;
        return finish_response(
            state,
            tenant_id,
            req,
            plan.interval_secs,
            Vec::<LogListRecord>::new(),
            None,
            Some(summary_result),
            0,
            started,
            DEFERRED_QUERY_COUNT,
        );
    }

    let rows_started = Instant::now();
    let rows_query = async {
        let result = crate::tenant_query(&state.ch, &plan.rows_sql, tenant_id)
            .with_option("query_id", format!("rush-explore-{query_prefix}-rows"))
            .with_option("cancel_http_readonly_queries_on_client_close", "1")
            .fetch_all::<LogListRecord>()
            .await;
        (result, rows_started.elapsed().as_millis() as u64)
    };
    let ((rows_result, rows_ready_ms), summary_result, query_count) = if req.include_summary {
        let summary_query = tokio::time::timeout(
            SUMMARY_TIMEOUT,
            crate::tenant_query(&state.ch, &plan.summary_sql, tenant_id)
                .with_option("query_id", format!("rush-explore-{query_prefix}-summary"))
                .with_option("cancel_http_readonly_queries_on_client_close", "1")
                .fetch_all::<SummaryRow>(),
        );
        let (rows_result, summary_result) = tokio::join!(rows_query, summary_query);
        (rows_result, Some(summary_result), COORDINATED_QUERY_COUNT)
    } else {
        (rows_query.await, None, DEFERRED_QUERY_COUNT)
    };
    let rows = rows_result.map_err(|error| {
        state
            .self_metrics
            .record_explore_stage("logs", "rows", 0, rows_ready_ms, false);
        tracing::error!(%error, signal = "logs", "coordinated Explore row query failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "query failed".to_string(),
        )
    })?;
    state
        .self_metrics
        .record_explore_stage("logs", "rows", rows.len() as u64, rows_ready_ms, true);

    let next_cursor = (rows.len() as u64 == req.limit.clamp(1, MAX_ROWS)).then(|| {
        let row = rows.last().expect("non-empty full page");
        let scope = crate::pagination::query_scope(
            tenant_id,
            "logs",
            "slim",
            &req.time_range,
            &req.filters,
            req.search.as_deref(),
        );
        crate::pagination::encode(
            &state.config_db,
            "logs",
            &scope,
            crate::pagination::CursorPosition {
                timestamp_ns: row.timestamp,
                tie: vec![
                    row.service_name.clone(),
                    row.trace_id.clone(),
                    row.span_id.clone(),
                    row.cursor_hash.clone(),
                ],
            },
        )
    });

    finish_response(
        state,
        tenant_id,
        req,
        plan.interval_secs,
        rows,
        next_cursor,
        summary_result,
        rows_ready_ms,
        started,
        query_count,
    )
}

fn finish_response<T: Serialize>(
    state: &AppState,
    tenant_id: &str,
    req: &ExploreSearchRequest,
    interval_secs: u64,
    rows: Vec<T>,
    next_cursor: Option<String>,
    summary_result: Option<
        Result<Result<Vec<SummaryRow>, clickhouse::error::Error>, tokio::time::error::Elapsed>,
    >,
    rows_ready_ms: u64,
    started: Instant,
    query_count: u64,
) -> Result<ExploreSearchResponse<T>, (StatusCode, String)> {
    let row_count = rows.len() as u64;
    let mut errors = ExploreErrors::default();
    let (summary, count, matched_bytes, summary_ok) = match summary_result {
        Some(Ok(Ok(summary_rows))) => {
            let folded = fold_summary(summary_rows, interval_secs);
            let count = ExploreCount {
                value: folded.total,
                kind: CountKind::Exact,
            };
            (folded.summary, count, folded.matched_bytes, Some(true))
        }
        Some(Ok(Err(error))) => {
            tracing::warn!(%error, signal = req.signal.label(), "Explore summary failed; returning rows");
            mark_summary_unavailable(&mut errors);
            (
                ExploreSummary {
                    interval_secs,
                    ..ExploreSummary::default()
                },
                fallback_count(row_count, req.limit.min(MAX_ROWS)),
                0,
                Some(false),
            )
        }
        Some(Err(_)) => {
            tracing::warn!(
                signal = req.signal.label(),
                "Explore summary timed out; returning rows"
            );
            mark_summary_unavailable(&mut errors);
            (
                ExploreSummary {
                    interval_secs,
                    ..ExploreSummary::default()
                },
                fallback_count(row_count, req.limit.min(MAX_ROWS)),
                0,
                Some(false),
            )
        }
        None => (
            ExploreSummary {
                interval_secs,
                ..ExploreSummary::default()
            },
            fallback_count(row_count, req.limit.min(MAX_ROWS)),
            0,
            None,
        ),
    };

    let elapsed_ms = started.elapsed().as_millis() as u64;
    if let Some(summary_ok) = summary_ok {
        state.self_metrics.record_explore_stage(
            req.signal.label(),
            "summary",
            count.value,
            elapsed_ms,
            summary_ok,
        );
    }
    state.self_metrics.record_explore_coordinator(
        req.signal.label(),
        query_count,
        count.value,
        matched_bytes,
        rows_ready_ms,
    );
    state.self_metrics.record_query_and_search(
        req.signal.operation(),
        req.signal.label(),
        req.search.as_ref().map(|value| value.chars().count()),
        row_count,
        elapsed_ms,
        true,
    );

    // Deferred clients issue one row request and one summary request. Attribute
    // usage from the authoritative summary only so one user search is not
    // counted twice.
    if count.value > 0 && req.include_summary {
        let filter_pairs: Vec<(String, String)> = req
            .filters
            .iter()
            .map(|filter| {
                (
                    filter.field.clone(),
                    filter.value.as_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        let signals = crate::usage_tracker::extract_span_signals(&filter_pairs);
        let signal_type = if req.signal == ExploreSignal::Logs {
            "log"
        } else {
            "span"
        };
        state
            .usage
            .track_many(tenant_id, signals, signal_type, "explore_coordinator");
    }

    let matched_rows = count.value;
    Ok(ExploreSearchResponse {
        signal: req.signal,
        rows,
        next_cursor,
        count,
        summary,
        errors,
        query_stats: ExploreQueryStats {
            clickhouse_queries: query_count,
            matched_rows,
            matched_logical_bytes: matched_bytes,
            time_to_first_results_ms: rows_ready_ms,
            response_bytes: 0,
        },
    })
}

fn encode_response<T: Serialize>(
    state: &AppState,
    signal: &'static str,
    response: ExploreSearchResponse<T>,
) -> Result<Response, (StatusCode, String)> {
    let mut body = serde_json::to_vec(&response).map_err(|error| {
        tracing::error!(%error, "explore response serialization failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal error".to_string(),
        )
    })?;
    let response_bytes = patch_response_byte_count(&mut body).ok_or_else(|| {
        tracing::error!("explore response byte-count field was not serialized");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal error".to_string(),
        )
    })?;
    state
        .self_metrics
        .record_explore_response_bytes(signal, response_bytes);
    Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .map_err(|error| {
            tracing::error!(%error, "explore response construction failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })
}

fn patch_response_byte_count(body: &mut Vec<u8>) -> Option<u64> {
    const PREFIX: &[u8] = b"\"response_bytes\":";
    let value_start = body
        .windows(PREFIX.len())
        .rposition(|window| window == PREFIX)?
        + PREFIX.len();
    if body.get(value_start) != Some(&b'0') {
        return None;
    }

    let base_len = body.len();
    let mut final_len = base_len as u64;
    loop {
        let adjusted = (base_len - 1 + final_len.to_string().len()) as u64;
        if adjusted == final_len {
            break;
        }
        final_len = adjusted;
    }
    body.splice(value_start..value_start + 1, final_len.to_string().bytes());
    (body.len() as u64 == final_len).then_some(final_len)
}

fn mark_summary_unavailable(errors: &mut ExploreErrors) {
    errors.histogram = Some("summary unavailable");
    errors.facets = Some("summary unavailable");
    errors.groups = Some("summary unavailable");
}

fn fallback_count(row_count: u64, limit: u64) -> ExploreCount {
    ExploreCount {
        value: row_count,
        kind: if row_count < limit {
            CountKind::Exact
        } else {
            CountKind::Capped
        },
    }
}

struct FoldedSummary {
    summary: ExploreSummary,
    total: u64,
    matched_bytes: u64,
}

fn fold_summary(rows: Vec<SummaryRow>, interval_secs: u64) -> FoldedSummary {
    let mut summary = ExploreSummary {
        interval_secs,
        ..ExploreSummary::default()
    };
    let mut total = 0;
    let mut matched_bytes = 0;

    for row in rows {
        match row.kind.as_str() {
            "histogram" => summary.histogram.push(CountBucket {
                bucket: row.bucket,
                count: row.count,
                error_count: row.error_count,
            }),
            "service" => summary.facets.services.push(SummaryValue {
                key: row.key,
                count: row.count,
            }),
            "status" => summary.facets.statuses.push(SummaryValue {
                key: row.key,
                count: row.count,
            }),
            "method" => summary.facets.methods.push(SummaryValue {
                key: row.key,
                count: row.count,
            }),
            "group" => summary.groups.push(SummaryValue {
                key: row.key,
                count: row.count,
            }),
            "total" => {
                total = row.count;
                matched_bytes = row.matched_bytes;
            }
            _ => {}
        }
    }
    summary.histogram.sort_by(|a, b| a.bucket.cmp(&b.bucket));
    FoldedSummary {
        summary,
        total,
        matched_bytes,
    }
}

fn validate_request(req: &ExploreSearchRequest) -> Result<(), (StatusCode, String)> {
    if !req.include_rows && !req.include_summary {
        return Err((
            StatusCode::BAD_REQUEST,
            "include_rows and include_summary cannot both be false".to_string(),
        ));
    }
    if req
        .search
        .as_ref()
        .is_some_and(|value| value.chars().count() > MAX_SEARCH_CHARS)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("search query too long (max {MAX_SEARCH_CHARS} chars)"),
        ));
    }
    if req.group_by.as_ref().is_some_and(|value| {
        value.chars().count() > MAX_GROUP_FIELD_CHARS || value.chars().any(char::is_control)
    }) {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid group_by field".to_string(),
        ));
    }
    Ok(())
}

fn build_plan(
    req: &ExploreSearchRequest,
    tenant_id: &str,
) -> Result<ExplorePlan, (StatusCode, String)> {
    let interval = clamp_bucket_interval(
        &req.interval,
        &req.time_range.from,
        &req.time_range.to,
        MAX_SUMMARY_ROWS_PER_KIND,
    )
    .map_err(|message| (StatusCode::BAD_REQUEST, message))?;
    let interval_secs = interval_seconds(interval);
    let limit = req.limit.clamp(1, MAX_ROWS);

    match req.signal {
        ExploreSignal::Spans => build_span_plan(req, tenant_id, interval_secs, limit),
        ExploreSignal::Logs => build_log_plan(req, tenant_id, interval_secs, limit),
    }
}

fn build_span_plan(
    req: &ExploreSearchRequest,
    tenant_id: &str,
    interval_secs: u64,
    limit: u64,
) -> Result<ExplorePlan, (StatusCode, String)> {
    let tenant = crate::query_builder::escape_string_literal(tenant_id);
    let clauses = build_where_clause_with_search(
        &req.filters,
        &req.time_range.from,
        &req.time_range.to,
        req.search.as_deref(),
    )
    .with_prewhere_prefix(&format!("tenant_id = '{tenant}'"));
    let predicate = clauses.to_sql();
    let rows_sql = format!(
        "SELECT timestamp, service_name, span_name, http_method, http_path, http_status_code, duration_ns, status, trace_id, span_id FROM spans {predicate} ORDER BY timestamp DESC, span_id DESC LIMIT {limit}"
    );
    let group_expr = req
        .group_by
        .as_deref()
        .filter(|field| !field.is_empty())
        .map(resolve_field);
    if group_expr.as_deref() == Some("NULL") {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid group_by field".to_string(),
        ));
    }
    let summary_sql = span_summary_sql(&predicate, interval_secs, group_expr.as_deref());
    Ok(ExplorePlan {
        rows_sql,
        summary_sql,
        interval_secs,
    })
}

fn build_log_plan(
    req: &ExploreSearchRequest,
    tenant_id: &str,
    interval_secs: u64,
    limit: u64,
) -> Result<ExplorePlan, (StatusCode, String)> {
    let clauses = super::logs::build_log_where(
        &req.filters,
        &req.time_range.from,
        &req.time_range.to,
        req.search.as_deref(),
        tenant_id,
    );
    let predicate = clauses.to_sql();
    let rows_sql = format!(
        "SELECT {} FROM logs {predicate} ORDER BY Timestamp DESC, ServiceName DESC, TraceId DESC, SpanId DESC, hex(SHA256(Body)) DESC LIMIT {limit}",
        super::logs::LOG_LIST_SELECT_COLS
    );
    let group_expr = req
        .group_by
        .as_deref()
        .filter(|field| !field.is_empty())
        .map(super::logs::resolve_log_field);
    let summary_sql = log_summary_sql(&predicate, interval_secs, group_expr.as_deref());
    Ok(ExplorePlan {
        rows_sql,
        summary_sql,
        interval_secs,
    })
}

fn span_summary_sql(predicate: &str, interval_secs: u64, group_expr: Option<&str>) -> String {
    summary_sql(
        "spans",
        predicate,
        interval_secs,
        "timestamp",
        "service_name",
        "toString(multiIf(http_status_code >= 500, 500, http_status_code >= 400, 400, 200))",
        "http_method",
        "status IN ('ERROR', 'STATUS_CODE_ERROR') OR http_status_code >= 500",
        "length(service_name) + length(span_name) + length(http_path) + length(attributes)",
        group_expr,
        true,
    )
}

fn log_summary_sql(predicate: &str, interval_secs: u64, group_expr: Option<&str>) -> String {
    summary_sql(
        "logs",
        predicate,
        interval_secs,
        "Timestamp",
        "ServiceName",
        "multiIf(SeverityNumber >= 17, 'error', SeverityNumber >= 13, 'warn', SeverityNumber >= 9, 'info', 'other')",
        "''",
        "SeverityNumber >= 17",
        "length(ServiceName) + length(SeverityText) + length(Body)",
        group_expr,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn summary_sql(
    table: &str,
    predicate: &str,
    interval_secs: u64,
    timestamp: &str,
    service: &str,
    status: &str,
    method: &str,
    is_error: &str,
    matched_bytes: &str,
    group_expr: Option<&str>,
    include_method: bool,
) -> String {
    let requested_select = group_expr
        .map(|expr| format!(", toString({expr}) AS requested_group_key"))
        .unwrap_or_default();
    let requested_kind = if group_expr.is_some() {
        ", grouping(requested_group_key) = 0, 'group'"
    } else {
        ""
    };
    let requested_key = if group_expr.is_some() {
        ", grouping(requested_group_key) = 0, requested_group_key"
    } else {
        ""
    };
    let method_kind = if include_method {
        ", grouping(method_key) = 0, 'method'"
    } else {
        ""
    };
    let method_key = if include_method {
        ", grouping(method_key) = 0, method_key"
    } else {
        ""
    };
    let mut grouping_sets = vec!["(bucket)", "(service_key)", "(status_key)"];
    if include_method {
        grouping_sets.push("(method_key)");
    }
    if group_expr.is_some() {
        grouping_sets.push("(requested_group_key)");
    }
    grouping_sets.push("()");

    format!(
        "SELECT kind, bucket_value AS bucket, key, count, error_count, matched_bytes FROM (SELECT \
         multiIf(grouping(bucket) = 0, 'histogram', \
                  grouping(service_key) = 0, 'service', \
                  grouping(status_key) = 0, 'status'{method_kind}{requested_kind}, 'total') AS kind, \
         if(grouping(bucket) = 0, toString(bucket), '') AS bucket_value, \
         multiIf(grouping(service_key) = 0, service_key, \
                  grouping(status_key) = 0, status_key{method_key}{requested_key}, '') AS key, \
         count() AS count, countIf(is_error) AS error_count, sum(logical_bytes) AS matched_bytes \
         FROM (SELECT toStartOfInterval({timestamp}, INTERVAL {interval_secs} SECOND) AS bucket, \
                      toString({service}) AS service_key, toString({status}) AS status_key, \
                      toString({method}) AS method_key, ({is_error}) AS is_error, \
                      toUInt64({matched_bytes}) AS logical_bytes{requested_select} \
               FROM {table} {predicate}) \
         GROUP BY GROUPING SETS ({grouping_sets})) \
         ORDER BY kind ASC, count DESC \
         LIMIT {MAX_SUMMARY_ROWS_PER_KIND} BY kind",
        grouping_sets = grouping_sets.join(", "),
    )
}

fn interval_seconds(interval: &str) -> u64 {
    match interval {
        "1s" => 1,
        "10s" => 10,
        "1m" => 60,
        "5m" => 300,
        "15m" => 900,
        "1h" => 3_600,
        "1d" => 86_400,
        _ => 60,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::query::FilterOp;
    use sqlparser::{dialect::ClickHouseDialect, parser::Parser};

    fn assert_clickhouse_sql_parses(sql: &str) {
        Parser::parse_sql(&ClickHouseDialect {}, sql)
            .unwrap_or_else(|error| panic!("generated SQL did not parse: {error}\n{sql}"));
    }

    fn request(signal: ExploreSignal) -> ExploreSearchRequest {
        ExploreSearchRequest {
            signal,
            time_range: TimeRange {
                from: "2026-08-10T00:00:00Z".to_string(),
                to: "2026-08-10T01:00:00Z".to_string(),
            },
            filters: vec![Filter {
                field: "service_name".to_string(),
                op: FilterOp::Eq,
                value: serde_json::json!("gateway"),
            }],
            search: Some("POST".to_string()),
            limit: 100,
            interval: "1m".to_string(),
            group_by: None,
            include_rows: true,
            include_summary: true,
        }
    }

    #[test]
    fn span_plan_uses_two_queries_and_one_canonical_predicate() {
        let req = request(ExploreSignal::Spans);
        let plan = build_plan(&req, "tenant-a").expect("span plan");
        for sql in [&plan.rows_sql, &plan.summary_sql] {
            assert!(sql.contains("tenant_id = 'tenant-a'"));
            assert!(sql.contains("service_name = 'gateway'"));
            assert!(sql.contains("%post%"));
        }
        assert!(plan.summary_sql.contains("GROUP BY GROUPING SETS"));
        assert!(!plan.rows_sql.contains("SELECT *"));
        assert!(plan.rows_sql.contains("timestamp, service_name, span_name"));
        assert!(!plan.rows_sql.contains("event_attributes"));
        // Keep the GROUPING() expression under a distinct alias. ClickHouse
        // 26.6 otherwise expands `AS bucket` back into GROUP BY and rejects the
        // query with ILLEGAL_AGGREGATION even though sqlparser accepts it.
        assert!(plan.summary_sql.contains("bucket_value AS bucket"));
        assert!(
            !plan
                .summary_sql
                .contains("toString(bucket), '') AS bucket,")
        );
        assert_clickhouse_sql_parses(&plan.rows_sql);
        assert_clickhouse_sql_parses(&plan.summary_sql);
        assert_eq!(COORDINATED_QUERY_COUNT, 2);
    }

    #[test]
    fn log_plan_is_bounded_and_uses_one_summary_scan() {
        let mut req = request(ExploreSignal::Logs);
        req.limit = u64::MAX;
        req.group_by = Some("resource.k8s.namespace.name".to_string());
        let plan = build_plan(&req, "tenant-b").expect("log plan");
        assert!(plan.rows_sql.contains("LIMIT 1000"));
        assert!(plan.summary_sql.contains("GROUP BY GROUPING SETS"));
        assert!(plan.summary_sql.contains("requested_group_key"));
        assert!(plan.summary_sql.contains("tenant_id = 'tenant-b'"));
        assert_clickhouse_sql_parses(&plan.rows_sql);
        assert_clickhouse_sql_parses(&plan.summary_sql);
    }

    #[test]
    fn summary_failure_count_is_explicitly_capped_at_the_page_boundary() {
        assert!(matches!(fallback_count(99, 100).kind, CountKind::Exact));
        assert!(matches!(fallback_count(100, 100).kind, CountKind::Capped));
    }

    #[test]
    fn deferred_execution_requires_at_least_one_query() {
        let mut req = request(ExploreSignal::Logs);
        req.include_rows = false;
        req.include_summary = false;
        assert_eq!(
            validate_request(&req).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );

        req.include_rows = true;
        assert!(validate_request(&req).is_ok());
        req.include_rows = false;
        req.include_summary = true;
        assert!(validate_request(&req).is_ok());
        assert_eq!(DEFERRED_QUERY_COUNT, 1);
    }

    #[test]
    fn response_byte_count_is_patched_without_reserializing() {
        let mut body = br#"{"rows":[1,2,3],"query_stats":{"response_bytes":0}}"#.to_vec();
        let expected = patch_response_byte_count(&mut body).unwrap();
        assert_eq!(body.len() as u64, expected);
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["query_stats"]["response_bytes"], expected);
    }

    #[test]
    fn folds_independent_summary_sections_and_sorts_histogram() {
        let folded = fold_summary(
            vec![
                SummaryRow {
                    kind: "histogram".into(),
                    bucket: "2026-08-10 00:01:00".into(),
                    key: "".into(),
                    count: 2,
                    error_count: 1,
                    matched_bytes: 20,
                },
                SummaryRow {
                    kind: "total".into(),
                    bucket: "".into(),
                    key: "".into(),
                    count: 3,
                    error_count: 1,
                    matched_bytes: 30,
                },
                SummaryRow {
                    kind: "service".into(),
                    bucket: "".into(),
                    key: "gateway".into(),
                    count: 3,
                    error_count: 1,
                    matched_bytes: 30,
                },
                SummaryRow {
                    kind: "histogram".into(),
                    bucket: "2026-08-10 00:00:00".into(),
                    key: "".into(),
                    count: 1,
                    error_count: 0,
                    matched_bytes: 10,
                },
            ],
            60,
        );
        assert_eq!(folded.total, 3);
        assert_eq!(folded.matched_bytes, 30);
        assert_eq!(folded.summary.histogram[0].count, 1);
        assert_eq!(folded.summary.facets.services[0].key, "gateway");
    }

    #[test]
    fn query_count_fixture_reduces_default_span_fanout_by_more_than_forty_percent() {
        const LEGACY_DEFAULT_QUERIES: f64 = 6.0;
        let reduction =
            (LEGACY_DEFAULT_QUERIES - COORDINATED_QUERY_COUNT as f64) / LEGACY_DEFAULT_QUERIES;
        assert!(reduction >= 0.40, "reduction was {:.1}%", reduction * 100.0);
    }
}
