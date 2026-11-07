//! Optional, bounded explanations for empty structured searches.
//! Tenant and time predicates are never relaxed by the diagnostic probes.
use axum::{Extension, Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::models::query::{CountRow, Filter, FilterOp, TimeRange};
use crate::query_builder::{build_where_clause_with_search, escape_string_literal, resolve_field};
use crate::{AppState, TenantContext};

const MAX_FILTERS: usize = 8;
const COUNT_CAP: u64 = 10_000;
const DEADLINE: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Signal {
    Spans,
    Logs,
    Metrics,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnoseRequest {
    pub signal: Signal,
    pub time_range: TimeRange,
    #[serde(default)]
    pub filters: Vec<Filter>,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub promql: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Diagnosis {
    pub code: &'static str,
    pub message: String,
    /// False means the deadline or scan budget prevented a full explanation.
    pub complete: bool,
    pub checked_filters: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_index: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<String>,
}

impl Diagnosis {
    fn new(code: &'static str, message: impl Into<String>, complete: bool) -> Self {
        Self {
            code,
            message: message.into(),
            complete,
            checked_filters: 0,
            filter_index: None,
            suggestions: vec![],
        }
    }
}

fn validate(req: &DiagnoseRequest) -> Result<(), (StatusCode, String)> {
    if req.signal == Signal::Metrics {
        metric_names(req)?;
    } else if req.promql.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "PromQL requires the metrics signal".into(),
        ));
    }
    if req.filters.len() > MAX_FILTERS || req.search.as_ref().is_some_and(|s| s.len() > 512) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Diagnostics accept at most 8 filters and 512 search bytes".into(),
        ));
    }
    let from = chrono::DateTime::parse_from_rfc3339(&req.time_range.from);
    let to = chrono::DateTime::parse_from_rfc3339(&req.time_range.to);
    if !matches!((from, to), (Ok(from), Ok(to)) if from <= to) {
        return Err((
            StatusCode::BAD_REQUEST,
            "A valid ordered RFC3339 time range is required".into(),
        ));
    }
    for filter in &req.filters {
        super::log_views::validate_field(&filter.field)?;
        if serde_json::to_vec(&filter.value).map_or(true, |value| value.len() > 1024) {
            return Err((
                StatusCode::BAD_REQUEST,
                "Diagnostic filter value is too large".into(),
            ));
        }
    }
    Ok(())
}

fn clauses(
    req: &DiagnoseRequest,
    tenant: &str,
    filters: &[Filter],
    search: Option<&str>,
) -> Result<String, (StatusCode, String)> {
    if req.signal == Signal::Logs {
        return super::logs::build_log_where(
            filters,
            &req.time_range.from,
            &req.time_range.to,
            search,
            tenant,
        )
        .map(|c| c.to_sql());
    }
    Ok(
        build_where_clause_with_search(filters, &req.time_range.from, &req.time_range.to, search)
            .with_prewhere_prefix(&format!("tenant_id = '{}'", escape_string_literal(tenant)))
            .to_sql(),
    )
}

fn count_sql(
    req: &DiagnoseRequest,
    tenant: &str,
    filters: &[Filter],
    search: Option<&str>,
) -> Result<String, (StatusCode, String)> {
    let table = if req.signal == Signal::Logs {
        "logs"
    } else {
        "spans"
    };
    Ok(format!(
        "SELECT count() AS count FROM (SELECT 1 FROM {table} {} LIMIT {COUNT_CAP})",
        clauses(req, tenant, filters, search)?
    ))
}

async fn count(
    ch: &clickhouse::Client,
    tenant: &str,
    sql: &str,
) -> Result<u64, clickhouse::error::Error> {
    bounded_read(ch, sql, tenant)
        .with_setting("max_execution_time", "1")
        .fetch_one::<CountRow>()
        .await
        .map(|row| row.count)
}

fn bounded_read(ch: &clickhouse::Client, sql: &str, tenant: &str) -> clickhouse::query::Query {
    let budget = crate::query_governor::active_budget()
        .or_else(|| crate::query_governor::global().map(|g| g.config().background));
    let rows = budget
        .as_ref()
        .map_or(1_000_000, |b| b.max_rows_to_read.min(1_000_000));
    let bytes = budget
        .as_ref()
        .map_or(67_108_864, |b| b.max_bytes_to_read.min(67_108_864));
    crate::tenant_query(ch, sql, tenant)
        .with_setting("max_rows_to_read", rows.to_string())
        .with_setting("max_bytes_to_read", bytes.to_string())
        .with_setting("read_overflow_mode", "throw")
        .with_setting("cancel_http_readonly_queries_on_client_close", "1")
}

fn count_label(count: u64) -> String {
    if count >= COUNT_CAP {
        format!("at least {COUNT_CAP}")
    } else {
        count.to_string()
    }
}

fn metric_names(req: &DiagnoseRequest) -> Result<Vec<String>, (StatusCode, String)> {
    let text = req
        .promql
        .as_deref()
        .filter(|text| !text.trim().is_empty() && text.len() <= 4096)
        .ok_or((
            StatusCode::BAD_REQUEST,
            "Metrics diagnostics require up to 4096 bytes of PromQL".into(),
        ))?;
    let expr = promql_parser::parser::parse(text).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "The PromQL expression is invalid".into(),
        )
    })?;
    let names = crate::promql::extract_metrics_from_expr(&expr);
    if names.len() > 8 || !req.filters.is_empty() || req.search.is_some() {
        return Err((StatusCode::BAD_REQUEST, "Metrics diagnostics accept up to 8 named selectors, without structured filters or search".into()));
    }
    Ok(names)
}

fn metric_probe_sql(req: &DiagnoseRequest, tenant: &str, name: &str) -> String {
    let tenant = escape_string_literal(tenant);
    let name = escape_string_literal(name);
    let from = crate::query_builder::sanitize_datetime(&req.time_range.from);
    let to = crate::query_builder::sanitize_datetime(&req.time_range.to);
    let predicate = format!(
        "tenant_id = '{tenant}' AND MetricName = '{name}' AND TimeUnix >= parseDateTimeBestEffort('{from}') AND TimeUnix <= parseDateTimeBestEffort('{to}')"
    );
    format!(
        "SELECT count() AS count FROM (SELECT 1 FROM (SELECT 1 FROM metrics_gauge PREWHERE {predicate} LIMIT 1) UNION ALL SELECT 1 FROM (SELECT 1 FROM metrics_sum PREWHERE {predicate} LIMIT 1))"
    )
}

async fn explain_metrics(
    ch: &clickhouse::Client,
    tenant: &str,
    req: &DiagnoseRequest,
) -> Result<Diagnosis, String> {
    let names = metric_names(req).map_err(|(_, message)| message)?;
    if names.is_empty() {
        return Ok(Diagnosis::new(
            "selector_discovery",
            "This expression has no explicit metric names. Use metric discovery to check the selector, label names and time range.",
            false,
        ));
    }
    let mut missing = Vec::new();
    for name in names {
        if count(ch, tenant, &metric_probe_sql(req, tenant, &name))
            .await
            .map_err(|error| error.to_string())?
            == 0
        {
            missing.push(name);
        }
    }
    if !missing.is_empty() {
        return Ok(Diagnosis::new(
            "missing_metrics",
            format!(
                "No samples were found for {} in this time range, before applying label matchers or functions. Check the metric spelling or widen the range.",
                missing.join(", ")
            ),
            true,
        ));
    }
    Ok(Diagnosis::new(
        "metric_samples_exist",
        "The named metrics have samples in this range. Check label matchers and vector joins. Rate functions need at least two samples per series inside the range selector; histogram_quantile needs bucket series with an le label.",
        true,
    ))
}

async fn explain(
    ch: &clickhouse::Client,
    tenant: &str,
    req: &DiagnoseRequest,
) -> Result<Diagnosis, String> {
    if req.signal == Signal::Metrics {
        return explain_metrics(ch, tenant, req).await;
    }
    let probe = |filters: &[Filter], search: Option<&str>| {
        count_sql(req, tenant, filters, search).map_err(|(_, message)| message)
    };
    let original = count(ch, tenant, &probe(&req.filters, req.search.as_deref())?)
        .await
        .map_err(|e| e.to_string())?;
    if original > 0 {
        return Ok(Diagnosis::new(
            "has_matches",
            format!(
                "This query matches {} records. Check the panel aggregation or display settings.",
                count_label(original)
            ),
            true,
        ));
    }
    let total = count(ch, tenant, &probe(&[], None)?)
        .await
        .map_err(|e| e.to_string())?;
    if total == 0 {
        return Ok(Diagnosis::new(
            "no_signal_data",
            "No records for this signal were found in the selected time range. Widen the range or check telemetry delivery.",
            true,
        ));
    }
    let mut best = None;
    for index in 0..req.filters.len() {
        let remaining: Vec<_> = req
            .filters
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != index)
            .map(|(_, f)| f.clone())
            .collect();
        let recovered = count(ch, tenant, &probe(&remaining, req.search.as_deref())?)
            .await
            .map_err(|e| e.to_string())?;
        if recovered > best.map_or(0, |(_, count)| count) {
            best = Some((index, recovered));
        }
    }
    if let Some((index, recovered)) = best {
        let filter = &req.filters[index];
        if matches!(filter.op, FilterOp::Eq) {
            let alternate = match &filter.value {
                serde_json::Value::Number(number) => {
                    Some(serde_json::Value::String(number.to_string()))
                }
                serde_json::Value::String(text) => text
                    .parse::<serde_json::Number>()
                    .ok()
                    .map(serde_json::Value::Number),
                _ => None,
            };
            if let Some(alternate) = alternate {
                let mut changed = req.filters.clone();
                changed[index].value = alternate.clone();
                if let Ok(sql) = probe(&changed, req.search.as_deref()) {
                    if let Ok(matches) = count(ch, tenant, &sql).await {
                        if matches > 0 {
                            let form = if alternate.is_string() {
                                "quoted text"
                            } else {
                                "a number without quotes"
                            };
                            let mut result = Diagnosis::new(
                                "value_type_mismatch",
                                format!(
                                    "The {} filter matches {} records when its value is written as {form}.",
                                    filter.field,
                                    count_label(matches)
                                ),
                                true,
                            );
                            result.filter_index = Some(index);
                            result.checked_filters = req.filters.len();
                            return Ok(result);
                        }
                    }
                }
            }
        }
        let mut diagnosis = Diagnosis::new(
            "filter_excludes_data",
            format!(
                "No records match this combination. Removing the {} filter matches {} records in the same time range.",
                filter.field,
                count_label(recovered)
            ),
            true,
        );
        diagnosis.checked_filters = req.filters.len();
        diagnosis.filter_index = Some(index);
        // Only suggest actual service names, with every other filter retained.
        if matches!(filter.op, FilterOp::Eq)
            && matches!(filter.field.as_str(), "service_name" | "ServiceName")
        {
            let remaining: Vec<_> = req
                .filters
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != index)
                .map(|(_, f)| f.clone())
                .collect();
            let field = if req.signal == Signal::Logs {
                super::logs::resolve_log_field(&filter.field)
            } else {
                resolve_field(&filter.field)
            };
            let table = if req.signal == Signal::Logs {
                "logs"
            } else {
                "spans"
            };
            let sql = format!(
                "SELECT DISTINCT value FROM (SELECT toString({field}) AS value FROM {table} {} LIMIT 10000) WHERE value != '' AND length(value) <= 128 ORDER BY value LIMIT 30",
                clauses(req, tenant, &remaining, req.search.as_deref()).map_err(|(_, m)| m)?
            );
            #[derive(Deserialize, clickhouse::Row)]
            struct ValueRow {
                value: String,
            }
            if let Ok(rows) = bounded_read(ch, &sql, tenant)
                .with_setting("max_execution_time", "0.5")
                .fetch_all::<ValueRow>()
                .await
            {
                let selected = filter.value.as_str().unwrap_or_default().to_lowercase();
                diagnosis.suggestions = rows
                    .into_iter()
                    .map(|r| r.value)
                    .filter(|value| similar(&selected, &value.to_lowercase()))
                    .take(3)
                    .collect();
            }
        }
        return Ok(diagnosis);
    }
    if req.search.as_ref().is_some_and(|s| !s.trim().is_empty()) {
        let recovered = count(ch, tenant, &probe(&req.filters, None)?)
            .await
            .map_err(|e| e.to_string())?;
        if recovered > 0 {
            return Ok(Diagnosis::new(
                "search_excludes_data",
                format!(
                    "The filters match {} records without the search text. Check the spelling or try a shorter search.",
                    count_label(recovered)
                ),
                true,
            ));
        }
    }
    let mut diagnosis = Diagnosis::new(
        "filter_combination",
        "Records exist in this range, but these filters together match none. Try changing more than one filter or widening the range.",
        true,
    );
    diagnosis.checked_filters = req.filters.len();
    Ok(diagnosis)
}

fn similar(selected: &str, candidate: &str) -> bool {
    if selected.is_empty() {
        return false;
    }
    selected.contains(candidate)
        || candidate.contains(selected)
        || edit_distance(selected, candidate) <= 2
}

fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<_> = b.chars().collect();
    let mut prev: Vec<_> = (0..=b.len()).collect();
    for (i, x) in a.chars().enumerate() {
        let mut next = vec![i + 1];
        for (j, y) in b.iter().enumerate() {
            next.push(
                (prev[j + 1] + 1)
                    .min(next[j] + 1)
                    .min(prev[j] + usize::from(x != *y)),
            );
        }
        prev = next;
    }
    prev[b.len()]
}

pub async fn diagnose(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<DiagnoseRequest>,
) -> Result<Json<Diagnosis>, (StatusCode, String)> {
    validate(&req)?;
    // Build first so invalid filters are rejected, not presented as a timeout.
    if req.signal != Signal::Metrics {
        count_sql(&req, &tenant.tenant_id, &req.filters, req.search.as_deref())?;
    }
    let result = tokio::time::timeout(DEADLINE, explain(&state.ch, &tenant.tenant_id, &req)).await;
    let diagnosis = match result {
        Ok(Ok(diagnosis)) => diagnosis,
        other => {
            tracing::debug!(?other, "query diagnosis incomplete");
            Diagnosis::new(
                "incomplete",
                "The diagnostic check could not finish within its query budget. Try a shorter time range.",
                false,
            )
        }
    };
    Ok(Json(diagnosis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reports_a_value_type_correction_only_after_a_successful_probe() {
        use clickhouse::test::{Mock, handlers};
        let mock = Mock::new();
        let client = clickhouse::Client::default()
            .with_url(mock.url())
            .with_validation(false);
        let mut req = request(Signal::Spans);
        req.filters[0] = Filter {
            field: "attributes.code".into(),
            op: FilterOp::Eq,
            value: 500.into(),
        };
        for value in [0_u64, 10, 10, 3] {
            mock.add(handlers::provide([value]));
        }
        let result = explain(&client, "one", &req).await.unwrap();
        assert_eq!(result.code, "value_type_mismatch");
        assert!(result.message.contains("quoted text"));
    }

    #[tokio::test]
    async fn metric_diagnosis_distinguishes_missing_names_from_function_guidance() {
        use clickhouse::test::{Mock, handlers};
        let mock = Mock::new();
        let client = clickhouse::Client::default()
            .with_url(mock.url())
            .with_validation(false);
        let mut req = request(Signal::Metrics);
        req.filters.clear();
        req.promql = Some("rate(cpu_usage[1m])".into());
        assert!(validate(&req).is_ok());
        mock.add(handlers::provide([0_u64]));
        assert_eq!(
            explain(&client, "one", &req).await.unwrap().code,
            "missing_metrics"
        );
        mock.add(handlers::provide([1_u64]));
        assert_eq!(
            explain(&client, "one", &req).await.unwrap().code,
            "metric_samples_exist"
        );
        let sql = metric_probe_sql(&req, "tenant'one", "cpu_usage");
        assert_eq!(sql.matches("tenant_id = 'tenant''one'").count(), 2);
        assert_eq!(sql.matches("MetricName = 'cpu_usage'").count(), 2);
        req.promql = Some("sum(".into());
        assert!(validate(&req).is_err());
    }

    #[tokio::test]
    async fn explains_empty_range_and_never_guesses_after_a_failed_probe() {
        use clickhouse::test::{Mock, handlers};
        let mock = Mock::new();
        let client = clickhouse::Client::default()
            .with_url(mock.url())
            .with_validation(false);
        mock.add(handlers::provide([0_u64]));
        mock.add(handlers::provide([0_u64]));
        let result = explain(&client, "one", &request(Signal::Logs))
            .await
            .unwrap();
        assert_eq!(result.code, "no_signal_data");
        mock.add(handlers::failure(StatusCode::SERVICE_UNAVAILABLE));
        assert!(
            explain(&client, "one", &request(Signal::Logs))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn identifies_filter_and_suggests_only_observed_similar_services() {
        use clickhouse::test::{Mock, handlers};
        let mock = Mock::new();
        let client = clickhouse::Client::default()
            .with_url(mock.url())
            .with_validation(false);
        for value in [0_u64, 120, 50] {
            mock.add(handlers::provide([value]));
        }
        mock.add(handlers::provide([
            "payment".to_string(),
            "checkout".to_string(),
        ]));
        let result = explain(&client, "one", &request(Signal::Spans))
            .await
            .unwrap();
        assert_eq!(result.code, "filter_excludes_data");
        assert_eq!(result.filter_index, Some(0));
        assert_eq!(result.suggestions, vec!["payment"]);
        assert!(result.message.contains("50 records"));
    }

    #[tokio::test]
    async fn identifies_search_text_and_reports_existing_matches() {
        use clickhouse::test::{Mock, handlers};
        let mock = Mock::new();
        let client = clickhouse::Client::default()
            .with_url(mock.url())
            .with_validation(false);
        let mut req = request(Signal::Logs);
        req.filters.clear();
        req.search = Some("typo".into());
        for value in [0_u64, 42, 42] {
            mock.add(handlers::provide([value]));
        }
        assert_eq!(
            explain(&client, "one", &req).await.unwrap().code,
            "search_excludes_data"
        );
        mock.add(handlers::provide([5_u64]));
        assert_eq!(
            explain(&client, "one", &req).await.unwrap().code,
            "has_matches"
        );
    }
    fn request(signal: Signal) -> DiagnoseRequest {
        DiagnoseRequest {
            signal,
            time_range: TimeRange {
                from: "2026-11-01T00:00:00Z".into(),
                to: "2026-11-01T01:00:00Z".into(),
            },
            filters: vec![Filter {
                field: "service_name".into(),
                op: FilterOp::Eq,
                value: "payments".into(),
            }],
            search: None,
            promql: None,
        }
    }
    #[test]
    fn relaxed_probes_keep_tenant_and_time_for_both_signals() {
        for signal in [Signal::Spans, Signal::Logs] {
            let req = request(signal);
            for filters in [&req.filters[..], &[]] {
                let sql = count_sql(&req, "tenant'one", filters, None).unwrap();
                assert!(sql.contains("tenant_id = 'tenant''one'"));
                assert!(sql.contains("2026-11-01T00:00:00Z"));
                assert!(sql.contains("2026-11-01T01:00:00Z"));
                assert!(sql.contains("LIMIT 10000"));
            }
        }
    }
    #[test]
    fn rejects_oversized_or_invalid_diagnostics() {
        let mut req = request(Signal::Spans);
        req.filters = vec![req.filters[0].clone(); 9];
        assert!(validate(&req).is_err());
        req.filters.clear();
        req.time_range.from = "x' OR 1=1".into();
        assert!(validate(&req).is_err());
        req = request(Signal::Logs);
        req.time_range.to = "2020-01-01T00:00:00Z".into();
        assert!(validate(&req).is_err());
    }
    #[test]
    fn service_suggestions_require_a_close_observed_name() {
        assert!(similar("payment", "payments"));
        assert!(similar("paymnts", "payments"));
        assert!(!similar("checkout", "payments"));
        assert!(!similar("", "payments"));
        assert_eq!(count_label(COUNT_CAP), "at least 10000");
    }
}
