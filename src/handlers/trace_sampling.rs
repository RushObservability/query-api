//! Saved dry-run drafts and read-only evaluation on a bounded sample of stored traces.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use axum::{
    Extension, Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::handlers::{
    log_views::ensure_requested_tenant, telemetry_quality::read_bounded, users::require_admin,
};
use crate::query_builder::{ERROR_SPAN, escape_string_literal};
use crate::{AppState, TenantContext};

pub const MAX_BODY_BYTES: usize = 16 * 1024;
const MAX_RULES: usize = 12;
const MAX_OPTION_SPANS: usize = 10_000;
const MAX_OPTION_ROWS: usize = 2_000;
const MAX_DISCOVERY_ROWS: usize = 25_000;
const MAX_TRACES: usize = 512;
const MAX_SPANS_PER_TRACE: usize = 128;
const MAX_ROUTE_ROWS: usize = 20;
const RARE_ROUTE_THRESHOLD: usize = 10;
const SETTLE_SECONDS: i64 = 120;
const EMPTY_PARENT: &str = "0000000000000000";
const IS_ROOT: &str = "spans.parent_span_id IN ('', '0000000000000000')";
const IS_SERVER: &str = "kind IN ('SPAN_KIND_SERVER', 'SERVER')";
const HTTP_METHOD: &str = "substringUTF8(if(empty(http_method), JSONExtractString(attributes, 'http.request.method'), http_method), 1, 17)";
const ROUTE_TEMPLATE: &str = "substringUTF8(JSONExtractString(attributes, 'http.route'), 1, 161)";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewRequest {
    window_minutes: u32,
    #[serde(default = "default_baseline_percent")]
    baseline_percent: u8,
    #[serde(default = "default_slow_threshold_ms")]
    slow_threshold_ms: u64,
    #[serde(default)]
    keep_errors: bool,
    #[serde(default)]
    keep_slow: bool,
    #[serde(default)]
    keep_rare_routes: bool,
    #[serde(default)]
    rules: Vec<Rule>,
}

fn default_baseline_percent() -> u8 {
    100
}

fn default_slow_threshold_ms() -> u64 {
    750
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Rule {
    #[serde(skip_serializing_if = "Option::is_none")]
    service: Option<String>,
    scope: SpanScope,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_status: Option<HttpStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    route: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    minimum_root_duration_ms: Option<u64>,
    #[serde(default)]
    require_rare_route: bool,
    keep_percent: u8,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum SpanScope {
    AnySpan,
    ServerSpan,
    RootSpan,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
enum HttpStatus {
    Exact(u16),
    Class(u16),
}

impl From<HttpStatus> for String {
    fn from(status: HttpStatus) -> Self {
        match status {
            HttpStatus::Exact(code) => code.to_string(),
            HttpStatus::Class(class) => format!("{class}xx"),
        }
    }
}

impl TryFrom<String> for HttpStatus {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_bytes() {
            [first @ b'1'..=b'5', b'x' | b'X', b'x' | b'X'] => {
                Ok(Self::Class(u16::from(first - b'0')))
            }
            [b'1'..=b'5', b'0'..=b'9', b'0'..=b'9'] => {
                Ok(Self::Exact(value.parse().expect("three ASCII digits")))
            }
            _ => Err("Use an HTTP code such as 200 or a class such as 2xx."),
        }
    }
}

impl Rule {
    fn validate(&self) -> Result<(), &'static str> {
        if self
            .service
            .as_deref()
            .is_some_and(|service| !valid_service(service))
        {
            return Err("Enter a service name of up to 96 bytes.");
        }
        if self
            .method
            .as_deref()
            .is_some_and(|method| method.is_empty() || !valid_method(method))
        {
            return Err("Enter an HTTP method of up to 16 letters.");
        }
        if self
            .route
            .as_deref()
            .is_some_and(|route| !valid_route(route) || route.trim() != route)
        {
            return Err(
                "Enter a recorded route template starting with /, without a query string or fragment (up to 160 bytes).",
            );
        }
        if self.keep_percent > 100 {
            return Err("Retention must be between 0% and 100%.");
        }
        if self
            .minimum_root_duration_ms
            .is_some_and(|duration| !(1..=3_600_000).contains(&duration))
        {
            return Err("Minimum root span duration must be between 1 ms and one hour.");
        }
        Ok(())
    }

    fn matches(&self, trace: &CompleteTrace<'_>, rare_route: bool) -> bool {
        if self.require_rare_route && !rare_route
            || self
                .minimum_root_duration_ms
                .is_some_and(|duration| trace.root.duration_ns < duration * 1_000_000)
        {
            return false;
        }
        let matches_span = |span: &Span| {
            self.service
                .as_ref()
                .is_none_or(|service| service == &span.service)
                && self.http_status.as_ref().is_none_or(|status| match status {
                    HttpStatus::Exact(code) => span.http_status == *code,
                    HttpStatus::Class(class) => span.http_status / 100 == *class,
                })
                && self
                    .method
                    .as_ref()
                    .is_none_or(|method| method.eq_ignore_ascii_case(&span.method))
                && self
                    .route
                    .as_ref()
                    .is_none_or(|route| route == span.route.trim())
        };
        match self.scope {
            SpanScope::RootSpan => matches_span(trace.root),
            SpanScope::AnySpan => trace.spans.iter().any(|span| matches_span(span)),
            SpanScope::ServerSpan => trace
                .spans
                .iter()
                .any(|span| span.server != 0 && matches_span(span)),
        }
    }
}

impl PreviewRequest {
    fn validate(&self) -> Result<(), (StatusCode, String)> {
        let invalid = |message: &str| (StatusCode::BAD_REQUEST, message.to_string());
        validate_window(self.window_minutes)?;
        if self.baseline_percent > 100 {
            return Err(invalid("Baseline retention must be between 0% and 100%."));
        }
        if !(1..=3_600_000).contains(&self.slow_threshold_ms) {
            return Err(invalid(
                "Slow trace threshold must be between 1 ms and one hour.",
            ));
        }
        validate_rules(&self.rules)
    }
}

fn validate_rules(rules: &[Rule]) -> Result<(), (StatusCode, String)> {
    if rules.len() > MAX_RULES {
        return Err((
            StatusCode::BAD_REQUEST,
            "A draft supports up to 12 custom rules.".into(),
        ));
    }
    for (index, rule) in rules.iter().enumerate() {
        rule.validate().map_err(|message| {
            (
                StatusCode::BAD_REQUEST,
                format!("Rule {}: {message}", index + 1),
            )
        })?;
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SavedDraft {
    window_minutes: u32,
    rules: Vec<Rule>,
}

impl Default for SavedDraft {
    fn default() -> Self {
        Self {
            window_minutes: 60,
            rules: Vec::new(),
        }
    }
}

async fn draft_setting_key(
    state: &AppState,
    tenant: &TenantContext,
    headers: &HeaderMap,
) -> Result<String, (StatusCode, String)> {
    ensure_requested_tenant(headers, &tenant.tenant_id)?;
    let id = state
        .config_db
        .get_tenant_id_by_name(&tenant.tenant_id)
        .await
        .map_err(|error| crate::api_error::internal_legacy("trace_sampling.draft_tenant", error))?
        .ok_or((
            StatusCode::FORBIDDEN,
            "Requested tenant is not available".into(),
        ))?;
    Ok(format!("trace_sampling_draft:{id}"))
}

pub async fn get_draft(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    let key = draft_setting_key(&state, &tenant, &headers).await?;
    let encoded =
        state.config_db.get_setting(&key).await.map_err(|error| {
            crate::api_error::internal_legacy("trace_sampling.draft_read", error)
        })?;
    let draft = encoded
        .as_deref()
        .map(serde_json::from_str::<SavedDraft>)
        .transpose()
        .map_err(|error| crate::api_error::internal_legacy("trace_sampling.draft_decode", error))?
        .unwrap_or_default();
    Ok(([(header::CACHE_CONTROL, "private, no-store")], Json(draft)))
}

pub async fn put_draft(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(draft): Json<SavedDraft>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let caller = require_admin(&state, &headers).await?;
    let key = draft_setting_key(&state, &tenant, &headers).await?;
    validate_window(draft.window_minutes)?;
    validate_rules(&draft.rules)?;
    let encoded = serde_json::to_string(&draft)
        .map_err(|error| crate::api_error::internal_legacy("trace_sampling.draft_encode", error))?;
    state
        .config_db
        .set_setting(&key, &encoded)
        .await
        .map_err(|error| crate::api_error::internal_legacy("trace_sampling.draft_save", error))?;
    state.audit.log(
        crate::audit::AuditEvent::new("trace_sampling_draft.update", "user")
            .actor(caller.0, caller.1)
            .tenant(tenant.tenant_id.clone())
            .resource("trace_sampling_draft", &tenant.tenant_id)
            .outcome("success")
            .changes(serde_json::json!({"rule_count": draft.rules.len(), "window_minutes": draft.window_minutes}).to_string())
            .context(crate::audit::actor_context_from_headers(&headers)),
    ).await;
    Ok(([(header::CACHE_CONTROL, "private, no-store")], Json(draft)))
}

fn validate_window(minutes: u32) -> Result<(), (StatusCode, String)> {
    let message = if !matches!(minutes, 15 | 60) {
        "Choose a 15-minute or one-hour sample window."
    } else if crate::query_governor::active_budget().is_some_and(|budget| {
        u64::from(minutes) * 60 + 2 * SETTLE_SECONDS as u64 > budget.max_time_range_secs
    }) {
        "The sample and its settling period exceed the configured query time range."
    } else {
        return Ok(());
    };
    Err((StatusCode::BAD_REQUEST, message.into()))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
struct Retention {
    total: usize,
    kept: usize,
}

impl Retention {
    fn record(&mut self, kept: bool) {
        self.total += 1;
        self.kept += usize::from(kept);
    }
}

#[derive(Debug, Default, PartialEq, Eq, Serialize)]
struct Coverage {
    scanned_spans: usize,
    observed_minutes: usize,
    sampled_traces: usize,
    sample_limit: usize,
    discovery_capped: bool,
    trace_sample_capped: bool,
    incomplete_traces: usize,
    boundary_traces: usize,
    span_capped_traces: usize,
    traces_without_route: usize,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize)]
struct Cohorts {
    errors: Retention,
    slow: Retention,
    rare_routes: Retention,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct RouteResult {
    service: String,
    method: String,
    route: String,
    retention: Retention,
}

#[derive(Debug, Serialize)]
pub struct PreviewResponse {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    window_minutes: u32,
    settling_seconds: i64,
    coverage: Coverage,
    retention: Retention,
    rule_retention: Vec<Retention>,
    fallback_retention: Retention,
    cohorts: Cohorts,
    rare_route_threshold: usize,
    rare_route_count: usize,
    rare_routes_zero_kept: usize,
    rare_routes: Vec<RouteResult>,
}

#[derive(Row, Deserialize)]
struct Candidate {
    trace_id: String,
    minute: i64,
}

#[derive(Debug, Row, Deserialize, PartialEq, Eq)]
struct Span {
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    start_ns: i64,
    duration_ns: u64,
    error: u8,
    server: u8,
    http_status: u16,
    service: String,
    method: String,
    route: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptionsQuery {
    window_minutes: u32,
}

#[derive(Debug, Row, Deserialize, Serialize)]
pub struct OptionRow {
    service: String,
    method: String,
    route: String,
    http_status: u16,
    root: u8,
    server: u8,
}

#[derive(Debug, Copy, Clone)]
struct Window {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    from_ns: i128,
    to_ns: i128,
}

impl Window {
    fn new(minutes: u32, now: DateTime<Utc>) -> Self {
        // Whole minutes make the coverage denominator precise. Let recent
        // spans settle before sampling, then look beyond both window edges.
        let end = now.timestamp().div_euclid(60) * 60 - SETTLE_SECONDS;
        let start = end - i64::from(minutes) * 60;
        Self {
            from: DateTime::from_timestamp(start, 0).expect("valid sample start"),
            to: DateTime::from_timestamp(end, 0).expect("valid sample end"),
            from_ns: i128::from(start) * 1_000_000_000,
            to_ns: i128::from(end) * 1_000_000_000,
        }
    }
}

#[derive(Clone, Copy)]
struct Limits {
    discovery_rows: usize,
    traces: usize,
    spans_per_trace: usize,
}

impl Limits {
    fn from_budget() -> Self {
        let max_rows = crate::query_governor::active_budget()
            .map_or(u64::MAX, |budget| budget.max_result_rows.max(1))
            as usize;
        let spans_per_trace = (MAX_SPANS_PER_TRACE + 1).min(max_rows);
        Self {
            discovery_rows: (MAX_DISCOVERY_ROWS + 1).min(max_rows),
            traces: MAX_TRACES.min(max_rows / spans_per_trace),
            spans_per_trace,
        }
    }
}

pub async fn preview(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Json(policy): Json<PreviewRequest>,
) -> Result<Json<PreviewResponse>, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    policy.validate()?;
    let _query = state
        .self_metrics
        .query_guard("trace_sampling.preview", "spans");
    let window = Window::new(policy.window_minutes, Utc::now());
    // The preview receives only the tenant-scoped SELECT client. No policy
    // object or decision is stored or made available to the ingest pipeline.
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        sample(
            &state.ch,
            &tenant.tenant_id,
            &policy,
            window,
            Limits::from_budget(),
        ),
    )
    .await
    .map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Trace sampling preview timed out.".into(),
        )
    })?
    .map_err(|error| crate::api_error::internal_legacy("trace_sampling.preview", error))?;
    Ok(Json(response))
}

pub async fn options(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    headers: HeaderMap,
    Query(params): Query<OptionsQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    require_admin(&state, &headers).await?;
    validate_window(params.window_minutes)?;
    let _query = state
        .self_metrics
        .query_guard("trace_sampling.options", "spans");
    let window = Window::new(params.window_minutes, Utc::now());
    let limit = crate::query_governor::active_budget().map_or(MAX_OPTION_ROWS, |budget| {
        MAX_OPTION_ROWS.min(budget.max_result_rows as usize)
    });
    let sql = options_sql(&tenant.tenant_id, window, limit);
    let mut rows = tokio::time::timeout(
        Duration::from_secs(3),
        read_bounded::<OptionRow>(&state.ch, &sql, &tenant.tenant_id),
    )
    .await
    .map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Trace sampling suggestions timed out.".into(),
        )
    })?
    .map_err(|error| crate::api_error::internal_legacy("trace_sampling.options", error))?;
    rows.retain_mut(|row| {
        if !valid_service(&row.service) {
            return false;
        }
        if valid_method(&row.method) {
            row.method.make_ascii_uppercase();
        } else {
            row.method.clear();
        }
        let route = row.route.trim();
        row.route = if valid_route(route) {
            route.to_owned()
        } else {
            String::new()
        };
        if !(100..600).contains(&row.http_status) {
            row.http_status = 0;
        }
        true
    });
    Ok(([(header::CACHE_CONTROL, "private, no-store")], Json(rows)))
}

fn options_sql(tenant: &str, window: Window, limit: usize) -> String {
    let tenant = escape_string_literal(tenant);
    let from = window.from.timestamp();
    let to = window.to.timestamp();
    // Aggregate combinations from the same stored span. Never suggest raw URLs.
    format!(
        "SELECT service, method, route, http_status, root, server
         FROM (
             SELECT substringUTF8(service_name, 1, 97) AS service, {HTTP_METHOD} AS method,
                    {ROUTE_TEMPLATE} AS route, http_status_code AS http_status,
                    toUInt8({IS_ROOT}) AS root, toUInt8({IS_SERVER}) AS server
             FROM spans
             PREWHERE tenant_id = '{tenant}'
              AND timestamp >= fromUnixTimestamp64Milli({from} * 1000, 'UTC')
              AND timestamp < fromUnixTimestamp64Milli({to} * 1000, 'UTC')
             ORDER BY timestamp DESC LIMIT {MAX_OPTION_SPANS}
         )
         GROUP BY service, method, route, http_status, root, server
         ORDER BY count() DESC, service, method, route, http_status, root, server LIMIT {limit}"
    )
}

fn discovery_sql(tenant: &str, window: Window, limit: usize) -> String {
    let tenant = escape_string_literal(tenant);
    let from = window.from.timestamp();
    let to = window.to.timestamp();
    format!(
        "SELECT if(length(spans.trace_id) = 32, spans.trace_id, '') AS trace_id,
                intDiv(toUnixTimestamp64Milli(timestamp), 60000) AS minute
         FROM spans
         PREWHERE tenant_id = '{tenant}'
          AND timestamp >= fromUnixTimestamp64Milli({from} * 1000, 'UTC')
          AND timestamp < fromUnixTimestamp64Milli({to} * 1000, 'UTC')
         ORDER BY timestamp DESC LIMIT {limit}"
    )
}

fn lookup_sql(
    tenant: &str,
    window: Window,
    ids: &[String],
    limit: usize,
    rules: &[Rule],
) -> String {
    let tenant = escape_string_literal(tenant);
    let from = window.from.timestamp() - SETTLE_SECONDS;
    let to = window.to.timestamp() + SETTLE_SECONDS;
    let ids = ids
        .iter()
        .map(|id| format!("'{}'", escape_string_literal(id)))
        .collect::<Vec<_>>()
        .join(",");
    let all_services = u8::from(rules.iter().any(|rule| rule.service.is_some()));
    let all_methods = u8::from(rules.iter().any(|rule| rule.method.is_some()));
    let all_routes = u8::from(rules.iter().any(|rule| rule.route.is_some()));
    // Use recorded route templates. http_path can contain raw URLs or IDs.
    format!(
        "SELECT trace_id, substring(spans.span_id, 1, 17) AS span_id,
                substring(spans.parent_span_id, 1, 17) AS parent_span_id,
                toUnixTimestamp64Nano(timestamp) AS start_ns, duration_ns, toUInt8({ERROR_SPAN}) AS error,
                toUInt8({IS_SERVER}) AS server, http_status_code AS http_status,
                if({IS_ROOT} OR {all_services}, substringUTF8(service_name, 1, 97), '') AS service,
                if({IS_ROOT} OR {all_methods}, {HTTP_METHOD}, '') AS method,
                if({IS_ROOT} OR {all_routes}, {ROUTE_TEMPLATE}, '') AS route
         FROM spans
         WHERE tenant_id = '{tenant}'
           AND timestamp >= fromUnixTimestamp64Milli({from} * 1000, 'UTC')
           AND timestamp < fromUnixTimestamp64Milli({to} * 1000, 'UTC')
           AND trace_id IN ({ids})
         LIMIT {limit} BY trace_id"
    )
}

async fn sample(
    ch: &Client,
    tenant: &str,
    policy: &PreviewRequest,
    window: Window,
    limits: Limits,
) -> Result<PreviewResponse, clickhouse::error::Error> {
    let mut observed = read_bounded::<Candidate>(
        ch,
        &discovery_sql(tenant, window, limits.discovery_rows),
        tenant,
    )
    .await?;
    let discovery_capped = observed.len() == limits.discovery_rows;
    if discovery_capped && observed.len() > 1 {
        observed.pop();
    }
    let observed_minutes = observed
        .iter()
        .map(|row| row.minute)
        .collect::<BTreeSet<_>>()
        .len();
    let scanned_spans = observed.len();
    let mut ids: Vec<_> = observed
        .into_iter()
        .map(|row| row.trace_id)
        .filter(|id| valid_id(id, 32))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let trace_sample_capped = ids.len() > limits.traces;
    if trace_sample_capped {
        // Separate hash domains for sample selection and baseline retention.
        ids.sort_by_cached_key(|id| (hash(b"rush-preview-sample-v1", id), id.clone()));
        ids.truncate(limits.traces);
    }
    let mut coverage = Coverage {
        scanned_spans,
        observed_minutes,
        sampled_traces: ids.len(),
        sample_limit: limits.traces,
        discovery_capped,
        trace_sample_capped,
        ..Coverage::default()
    };
    let spans = if ids.is_empty() {
        Vec::new()
    } else {
        read_bounded::<Span>(
            ch,
            &lookup_sql(tenant, window, &ids, limits.spans_per_trace, &policy.rules),
            tenant,
        )
        .await?
    };
    let mut by_trace: HashMap<&str, Vec<&Span>> = HashMap::new();
    for span in &spans {
        by_trace.entry(&span.trace_id).or_default().push(span);
    }
    let mut eligible = Vec::new();
    for id in &ids {
        let trace = by_trace.get(id.as_str()).map_or(&[][..], Vec::as_slice);
        if trace.len() >= limits.spans_per_trace {
            coverage.span_capped_traces += 1;
        } else {
            match complete_trace(trace, window) {
                Ok(trace) => eligible.push((id.as_str(), trace)),
                Err(Exclusion::Incomplete) => coverage.incomplete_traces += 1,
                Err(Exclusion::Boundary) => coverage.boundary_traces += 1,
            }
        }
    }
    Ok(evaluate(policy, window, coverage, &eligible))
}

fn valid_id(id: &str, width: usize) -> bool {
    id.len() == width
        && id.bytes().all(|byte| byte.is_ascii_hexdigit())
        && id.bytes().any(|byte| byte != b'0')
}

fn hash(domain: &[u8], id: &str) -> u64 {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(id.as_bytes());
    let digest = hash.finalize();
    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 returns 32 bytes"))
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Copy, Clone)]
struct Route<'a> {
    service: &'a str,
    method: &'a str,
    template: &'a str,
}

fn valid_route(template: &str) -> bool {
    template.starts_with('/')
        && template.len() <= 160
        && !template.contains(['?', '#'])
        && !template.chars().any(char::is_control)
}

fn valid_service(service: &str) -> bool {
    !service.is_empty()
        && service.trim() == service
        && service.len() <= 96
        && !service.chars().any(char::is_control)
}

fn valid_method(method: &str) -> bool {
    method.len() <= 16 && method.bytes().all(|byte| byte.is_ascii_alphabetic())
}

fn route_template(root: &Span) -> Option<Route<'_>> {
    let template = root.route.trim();
    let service = root.service.trim();
    let method = root.method.trim();
    if valid_route(template)
        && !service.is_empty()
        && service.len() <= 96
        && !service.chars().any(char::is_control)
        && valid_method(method)
    {
        Some(Route {
            service,
            method,
            template,
        })
    } else {
        None
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Exclusion {
    Boundary,
    Incomplete,
}

#[derive(Debug)]
struct CompleteTrace<'a> {
    error: bool,
    root: &'a Span,
    spans: Vec<&'a Span>,
    route: Option<Route<'a>>,
}

fn complete_trace<'a>(rows: &[&'a Span], window: Window) -> Result<CompleteTrace<'a>, Exclusion> {
    let mut unique = HashMap::new();
    for &span in rows {
        if !valid_id(&span.span_id, 16) {
            return Err(Exclusion::Incomplete);
        }
        if unique
            .insert(span.span_id.as_str(), span)
            .is_some_and(|previous| previous != span)
        {
            return Err(Exclusion::Incomplete);
        }
    }
    if unique.values().any(|span| {
        let start = i128::from(span.start_ns);
        start < window.from_ns
            || start >= window.to_ns
            || start + i128::from(span.duration_ns) > window.to_ns
    }) {
        return Err(Exclusion::Boundary);
    }
    let mut root = None;
    let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut error = false;
    for span in unique.values() {
        error |= span.error != 0;
        if span.parent_span_id.is_empty() || span.parent_span_id == EMPTY_PARENT {
            if root.replace(*span).is_some() {
                return Err(Exclusion::Incomplete);
            }
        } else {
            children
                .entry(&span.parent_span_id)
                .or_default()
                .push(&span.span_id);
        }
    }
    let root = root.ok_or(Exclusion::Incomplete)?;
    let mut visited = 0;
    let mut stack = vec![root.span_id.as_str()];
    // Every non-root span has one parent, so a disconnected cycle is never
    // visited from the root. It is detected by the size check.
    while let Some(id) = stack.pop() {
        visited += 1;
        if let Some(next) = children.remove(id) {
            stack.extend(next);
        }
    }
    if visited != unique.len() {
        return Err(Exclusion::Incomplete);
    }
    Ok(CompleteTrace {
        error,
        root,
        spans: unique.into_values().collect(),
        route: route_template(root),
    })
}

fn evaluate(
    policy: &PreviewRequest,
    window: Window,
    mut coverage: Coverage,
    eligible: &[(&str, CompleteTrace<'_>)],
) -> PreviewResponse {
    let mut route_counts = BTreeMap::new();
    for (_, trace) in eligible {
        if let Some(route) = trace.route {
            *route_counts.entry(route).or_insert(0) += 1;
        } else {
            coverage.traces_without_route += 1;
        }
    }
    let mut retention = Retention::default();
    let mut rule_retention = vec![Retention::default(); policy.rules.len()];
    let mut fallback_retention = Retention::default();
    let mut cohorts = Cohorts::default();
    let mut rare_routes: BTreeMap<Route<'_>, Retention> = BTreeMap::new();
    let slow_ns = policy.slow_threshold_ms * 1_000_000;
    for (id, trace) in eligible {
        let slow = trace.root.duration_ns >= slow_ns;
        let rare = trace
            .route
            .filter(|route| route_counts[route] < RARE_ROUTE_THRESHOLD);
        let bucket = hash(b"rush-preview-baseline-v1", id) % 100;
        let kept = if let Some(index) = policy
            .rules
            .iter()
            .position(|rule| rule.matches(trace, rare.is_some()))
        {
            let kept = bucket < u64::from(policy.rules[index].keep_percent);
            rule_retention[index].record(kept);
            kept
        } else {
            let kept = policy.keep_errors && trace.error
                || policy.keep_slow && slow
                || policy.keep_rare_routes && rare.is_some()
                || bucket < u64::from(policy.baseline_percent);
            fallback_retention.record(kept);
            kept
        };
        retention.record(kept);
        if trace.error {
            cohorts.errors.record(kept);
        }
        if slow {
            cohorts.slow.record(kept);
        }
        if let Some(route) = rare {
            cohorts.rare_routes.record(kept);
            rare_routes.entry(route).or_default().record(kept);
        }
    }
    let rare_route_count = rare_routes.len();
    let rare_routes_zero_kept = rare_routes
        .values()
        .filter(|result| result.kept == 0)
        .count();
    let mut rare_routes: Vec<_> = rare_routes.into_iter().collect();
    // Least represented first. Stable tie order follows (service, method, route).
    rare_routes.sort_by(|(_, a), (_, b)| (a.kept * b.total).cmp(&(b.kept * a.total)));
    let rare_routes = rare_routes
        .into_iter()
        .take(MAX_ROUTE_ROWS)
        .map(|(route, retention)| RouteResult {
            service: route.service.to_string(),
            method: route.method.to_string(),
            route: route.template.to_string(),
            retention,
        })
        .collect();
    PreviewResponse {
        from: window.from,
        to: window.to,
        window_minutes: policy.window_minutes,
        settling_seconds: SETTLE_SECONDS,
        coverage,
        retention,
        rule_retention,
        fallback_retention,
        cohorts,
        rare_route_threshold: RARE_ROUTE_THRESHOLD,
        rare_route_count,
        rare_routes_zero_kept,
        rare_routes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window() -> Window {
        Window::new(15, DateTime::from_timestamp(1_702_123_401, 0).unwrap())
    }

    fn span(id: u8, parent: u8) -> Span {
        Span {
            trace_id: format!("{:032x}", 1),
            span_id: format!("{id:016x}"),
            parent_span_id: format!("{parent:016x}"),
            start_ns: window().from_ns as i64,
            duration_ns: 750_000_000,
            error: 0,
            server: 1,
            http_status: 200,
            service: "checkout".into(),
            method: "GET".into(),
            route: "/orders/:id".into(),
        }
    }

    fn keep_none() -> PreviewRequest {
        PreviewRequest {
            window_minutes: 15,
            baseline_percent: 0,
            slow_threshold_ms: 750,
            keep_errors: false,
            keep_slow: false,
            keep_rare_routes: false,
            rules: vec![],
        }
    }

    #[test]
    fn complete_trace_requires_one_connected_tree() {
        let root = span(1, 0);
        let child = Span {
            error: 1,
            ..span(2, 1)
        };
        let grandchild = span(3, 2);
        let copy = Span { ..span(3, 2) };
        let result = complete_trace(&[&child, &root, &grandchild, &copy], window()).unwrap();
        assert!(result.error);
        assert_eq!(result.root.duration_ns, root.duration_ns);
        assert_eq!(result.route.unwrap().template, "/orders/:id");

        let cycle = span(3, 3);
        let missing_parent = span(3, 7);
        let second_root = span(3, 0);
        let conflicting_span = span(2, 3);
        for invalid in [cycle, missing_parent, second_root, conflicting_span] {
            assert_eq!(
                complete_trace(&[&root, &child, &invalid], window()).unwrap_err(),
                Exclusion::Incomplete
            );
        }
        assert_eq!(
            complete_trace(&[&child, &grandchild], window()).unwrap_err(),
            Exclusion::Incomplete
        );
    }

    #[test]
    fn boundary_check_includes_span_duration() {
        let root = span(1, 0);
        let right_edge = Span {
            start_ns: window().to_ns as i64 - 1,
            ..span(2, 1)
        };
        let left_edge = Span {
            start_ns: window().from_ns as i64 - 1,
            ..span(2, 1)
        };
        for edge in [&left_edge, &right_edge] {
            assert_eq!(
                complete_trace(&[&root, edge], window()).unwrap_err(),
                Exclusion::Boundary
            );
        }
    }

    #[test]
    fn evaluates_overlapping_cohorts_and_rare_templates() {
        let route = Route {
            service: "checkout",
            method: "GET",
            template: "/orders/:id",
        };
        let common = Route {
            template: "/status",
            ..route
        };
        let patch = Route {
            method: "PATCH",
            ..common
        };
        let slow = span(1, 0);
        let fast = Span {
            duration_ns: 1,
            ..span(1, 0)
        };
        let trace = |error, root, route| CompleteTrace {
            error,
            root,
            spans: vec![root],
            route,
        };
        let mut eligible = vec![
            ("1", trace(true, &slow, Some(route))),
            ("2", trace(false, &fast, Some(route))),
            ("3", trace(false, &fast, None)),
            ("4", trace(false, &fast, Some(patch))),
        ];
        for id in ["5", "6", "7", "8", "9", "a", "b", "c", "d", "e"] {
            eligible.push((id, trace(false, &fast, Some(common))));
        }

        let none = evaluate(&keep_none(), window(), Coverage::default(), &eligible);
        assert_eq!(none.retention, Retention { kept: 0, total: 14 });
        assert_eq!(none.rare_routes_zero_kept, 2);
        assert_eq!(none.coverage.traces_without_route, 1);
        assert_eq!(none.cohorts.rare_routes, Retention { kept: 0, total: 3 });
        assert_eq!(none.rare_routes[1].method, "PATCH");

        let rules = PreviewRequest {
            keep_errors: true,
            keep_slow: true,
            keep_rare_routes: true,
            ..keep_none()
        };
        let result = evaluate(&rules, window(), Coverage::default(), &eligible);
        assert_eq!(result.retention, Retention { kept: 3, total: 14 });
        assert_eq!(result.cohorts.errors, Retention { kept: 1, total: 1 });
        assert_eq!(result.cohorts.slow, Retention { kept: 1, total: 1 });
        assert_eq!(result.rare_routes_zero_kept, 0);

        let baseline = PreviewRequest {
            baseline_percent: 50,
            ..keep_none()
        };
        let stable = evaluate(&baseline, window(), Coverage::default(), &eligible).retention;
        eligible.reverse();
        assert_eq!(
            evaluate(&baseline, window(), Coverage::default(), &eligible).retention,
            stable
        );
        let all = PreviewRequest {
            baseline_percent: 100,
            ..keep_none()
        };
        assert_eq!(
            evaluate(&all, window(), Coverage::default(), &eligible)
                .retention
                .kept,
            14
        );
    }

    #[test]
    fn custom_rules_use_first_match_and_match_all_fields_on_one_span() {
        let gateway_ok = Span {
            service: "gateway".into(),
            http_status: 204,
            ..span(1, 0)
        };
        let gateway_failure = Span {
            service: "gateway".into(),
            http_status: 503,
            error: 1,
            ..span(1, 0)
        };
        let gateway_client = Span {
            service: "gateway".into(),
            server: 0,
            ..span(2, 1)
        };
        let payments = Span {
            service: "payments".into(),
            ..span(2, 1)
        };
        let other_ok = Span {
            service: "other".into(),
            ..span(2, 1)
        };
        let eligible = [
            (
                "1",
                complete_trace(&[&gateway_ok, &payments], window()).unwrap(),
            ),
            ("2", complete_trace(&[&gateway_ok], window()).unwrap()),
            (
                "3",
                complete_trace(&[&gateway_failure, &other_ok], window()).unwrap(),
            ),
            (
                "4",
                complete_trace(&[&gateway_failure, &gateway_client], window()).unwrap(),
            ),
        ];
        let mut policy = PreviewRequest {
            keep_errors: true,
            keep_slow: true,
            rules: vec![
                serde_json::from_value(serde_json::json!({
                    "service": "payments", "scope": "any_span", "keep_percent": 100
                }))
                .unwrap(),
                serde_json::from_value(serde_json::json!({
                    "service": "gateway", "scope": "server_span", "keep_percent": 0,
                    "http_status": "2xx", "method": "get", "route": "/orders/:id"
                }))
                .unwrap(),
            ],
            ..keep_none()
        };
        assert!(policy.validate().is_ok());
        let preview = evaluate(&policy, window(), Coverage::default(), &eligible);
        assert_eq!(preview.retention, Retention { kept: 3, total: 4 });
        assert_eq!(
            preview.rule_retention,
            [
                Retention { kept: 1, total: 1 },
                Retention { kept: 0, total: 1 }
            ]
        );
        assert_eq!(preview.fallback_retention, Retention { kept: 2, total: 2 });
        policy.rules.swap(0, 1);
        let preview = evaluate(&policy, window(), Coverage::default(), &eligible);
        assert_eq!(
            preview.rule_retention,
            [Retention { kept: 0, total: 2 }, Retention::default()]
        );

        policy.rules[0].http_status = Some(HttpStatus::Exact(200));
        assert!(!policy.rules[0].matches(&eligible[1].1, false));
        policy.rules[0].http_status = Some(HttpStatus::Exact(204));
        assert!(policy.rules[0].matches(&eligible[1].1, false));
        policy.rules[1].scope = SpanScope::RootSpan;
        assert!(!policy.rules[1].matches(&eligible[0].1, false));
    }

    #[test]
    fn ordered_rules_handle_http_only_slow_rare_and_catch_all_conditions() {
        let failed = Span {
            http_status: 503,
            duration_ns: 100_000_000,
            ..span(1, 0)
        };
        let slow = Span {
            duration_ns: 500_000_000,
            ..span(1, 0)
        };
        let rare = Span {
            duration_ns: 100_000_000,
            ..span(1, 0)
        };
        let other = Span {
            duration_ns: 100_000_000,
            route: String::new(),
            ..span(1, 0)
        };
        let eligible = [(&failed, "1"), (&slow, "2"), (&rare, "3"), (&other, "4")]
            .map(|(root, id)| (id, complete_trace(&[root], window()).unwrap()));
        let mut policy: PreviewRequest = serde_json::from_value(serde_json::json!({
            "window_minutes": 15,
            "rules": [
                {"scope":"any_span", "http_status":"5xx", "keep_percent":100},
                {"scope":"any_span", "minimum_root_duration_ms":500, "keep_percent":100},
                {"scope":"any_span", "require_rare_route":true, "keep_percent":100},
                {"scope":"any_span", "keep_percent":0}
            ]
        }))
        .unwrap();
        assert!(policy.validate().is_ok());
        let result = evaluate(&policy, window(), Coverage::default(), &eligible);
        assert_eq!(
            result.rule_retention,
            [Retention { kept: 1, total: 1 }; 3]
                .into_iter()
                .chain([Retention { kept: 0, total: 1 }])
                .collect::<Vec<_>>()
        );
        assert_eq!(result.fallback_retention, Retention::default());

        policy.rules.pop();
        let result = evaluate(&policy, window(), Coverage::default(), &eligible);
        assert_eq!(result.fallback_retention, Retention { kept: 1, total: 1 });
        assert_eq!(result.retention, Retention { kept: 4, total: 4 });

        policy.rules[1].minimum_root_duration_ms = Some(0);
        assert!(policy.validate().is_err());
    }

    #[test]
    fn preview_policy_validates_bounds_and_rejects_extra_fields() {
        assert!(keep_none().validate().is_ok());
        let invalid = PreviewRequest {
            baseline_percent: 101,
            ..keep_none()
        };
        assert_eq!(invalid.validate().unwrap_err().0, StatusCode::BAD_REQUEST);
        let invalid = PreviewRequest {
            slow_threshold_ms: 0,
            ..keep_none()
        };
        assert_eq!(invalid.validate().unwrap_err().0, StatusCode::BAD_REQUEST);
        let invalid = PreviewRequest {
            window_minutes: 120,
            ..keep_none()
        };
        assert_eq!(invalid.validate().unwrap_err().0, StatusCode::BAD_REQUEST);
        assert!(
            serde_json::from_value::<PreviewRequest>(serde_json::json!({
                "window_minutes": 15, "baseline_percent": 0, "slow_threshold_ms": 750,
                "keep_errors": false, "keep_slow": false, "keep_rare_routes": false,
                "apply": true
            }))
            .is_err()
        );
        let raw_url = Span {
            route: "/orders?session=secret".into(),
            ..span(1, 0)
        };
        assert!(route_template(&raw_url).is_none());
        assert!(HttpStatus::try_from("6xx".to_owned()).is_err());
        assert!(HttpStatus::try_from("20*".to_owned()).is_err());
    }
}
