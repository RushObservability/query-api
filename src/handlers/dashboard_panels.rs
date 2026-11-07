//! Read-only, tenant-scoped data for dashboard pattern and scatter panels.
use axum::{Extension, Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};

use crate::models::query::{Filter, TimeRange};
use crate::query_builder::{
    ERROR_SPAN, build_where_clause_with_search, escape_string_literal, resolve_field,
};
use crate::{AppState, TenantContext};

type ApiError = (StatusCode, String);
fn bad(message: &str) -> ApiError {
    (StatusCode::BAD_REQUEST, message.into())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatternRequest {
    pub time_range: TimeRange,
    #[serde(default)]
    pub filters: Vec<Filter>,
    #[serde(default = "pattern_limit")]
    pub limit: u64,
}
fn pattern_limit() -> u64 {
    10
}

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct PatternRow {
    pub service: String,
    pub severity: String,
    pub template: String,
    pub example: String,
    pub trace_id: String,
    pub last_seen: String,
    pub count: u64,
    pub buckets: Vec<(i64, u64)>,
    pub total_count: u64,
}

fn validate(range: &TimeRange, filters: &[Filter]) -> Result<(), ApiError> {
    if chrono::DateTime::parse_from_rfc3339(&range.from).is_err()
        || chrono::DateTime::parse_from_rfc3339(&range.to).is_err()
    {
        return Err(bad("Panel time ranges must use RFC3339 timestamps"));
    }
    let max_range = crate::query_governor::active_budget()
        .map_or(30 * 86400, |b| b.max_time_range_secs.min(30 * 86400));
    crate::query_governor::ValidatedTimeRange::parse(&range.from, &range.to, max_range).map_err(
        |_| bad("An ordered time range within the query budget is required (maximum 30 days)"),
    )?;
    if filters.len() > 20 {
        return Err(bad("At most 20 filters are allowed"));
    }
    for filter in filters {
        super::log_views::validate_field(&filter.field)?;
        if filter.field == "tenant_id"
            || serde_json::to_vec(&filter.value).map_or(true, |v| v.len() > 2048)
        {
            return Err(bad("Invalid panel filter"));
        }
    }
    Ok(())
}

fn pattern_sql(req: &PatternRequest, tenant: &str) -> Result<String, ApiError> {
    validate(&req.time_range, &req.filters)?;
    if !(1..=50).contains(&req.limit) {
        return Err(bad("Pattern limit must be between 1 and 50"));
    }
    let clauses = super::logs::build_log_where(
        &req.filters,
        &req.time_range.from,
        &req.time_range.to,
        None,
        tenant,
    )?
    .to_sql();
    let range = crate::query_governor::ValidatedTimeRange::parse(
        &req.time_range.from,
        &req.time_range.to,
        30 * 86400,
    )
    .map_err(|_| bad("Invalid time range"))?;
    // At most ~60 sparkline buckets per pattern, including for a month-long window.
    let step = ((range.to - range.from).num_seconds().max(1) / 60).max(1);
    let template = super::services::LOG_BODY_TEMPLATE;
    Ok(format!(
        "SELECT service, severity, template, argMax(sample_body, last_timestamp) AS example, \
         argMax(sample_trace, last_timestamp) AS trace_id, toString(max(last_timestamp)) AS last_seen, \
         sum(events) AS count, arraySort(groupArray((bucket, events))) AS buckets, \
         sum(sum(events)) OVER () AS total_count FROM (\
         SELECT ServiceName AS service, SeverityText AS severity, {template} AS template, \
         argMax(leftUTF8(Body, 2048), Timestamp) AS sample_body, argMax(TraceId, Timestamp) AS sample_trace, \
         max(Timestamp) AS last_timestamp, toInt64(intDiv(toUnixTimestamp(Timestamp), {step}) * {step}) AS bucket, \
         count() AS events FROM logs {clauses} GROUP BY service, severity, template, bucket) \
         GROUP BY service, severity, template ORDER BY count DESC, service, severity, template LIMIT {}",
        req.limit
    ))
}

fn bounded_read(ch: &clickhouse::Client, sql: &str, tenant: &str) -> clickhouse::query::Query {
    let budget = crate::query_governor::active_budget()
        .or_else(|| crate::query_governor::global().map(|g| g.config().background));
    let rows = budget
        .as_ref()
        .map_or(10_000_000, |b| b.max_rows_to_read.min(10_000_000));
    let bytes = budget
        .as_ref()
        .map_or(536_870_912, |b| b.max_bytes_to_read.min(536_870_912));
    crate::tenant_query(ch, sql, tenant)
        .with_setting("max_rows_to_read", rows.to_string())
        .with_setting("max_bytes_to_read", bytes.to_string())
        .with_setting("read_overflow_mode", "throw")
}

pub async fn patterns(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<PatternRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let sql = pattern_sql(&req, &tenant.tenant_id)?;
    let rows = bounded_read(&state.ch, &sql, &tenant.tenant_id)
        .fetch_all::<PatternRow>()
        .await
        .map_err(|e| crate::api_error::internal_legacy("dashboard.patterns", e))?;
    let total = rows.first().map_or(0, |r| r.total_count);
    Ok(Json(serde_json::json!({"patterns": rows, "total": total})))
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ScatterSource {
    Spans,
    Logs,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScatterRequest {
    pub source: ScatterSource,
    pub time_range: TimeRange,
    #[serde(default)]
    pub filters: Vec<Filter>,
    pub x_field: String,
    pub y_field: String,
    #[serde(default = "scatter_limit")]
    pub limit: u64,
}
fn scatter_limit() -> u64 {
    500
}

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct ScatterPoint {
    pub x: f64,
    pub y: f64,
    pub timestamp: String,
    pub trace_id: String,
    pub span_id: String,
    pub service: String,
    pub label: String,
    pub error: u8,
}

fn numeric_field(source: &ScatterSource, field: &str) -> Result<String, ApiError> {
    super::log_views::validate_field(field)?;
    if field == "tenant_id" {
        return Err(bad("tenant_id is not a numeric axis"));
    }
    let expr = match source {
        ScatterSource::Logs => super::logs::resolve_log_field(field),
        ScatterSource::Spans => match field {
            "duration_ms" => "duration_ns / 1000000.0".into(),
            "duration_ns" | "http_status_code" => field.into(),
            value if value.starts_with("attributes.") => {
                let key = &value[11..];
                // Both OTLP string-valued attributes and native JSON numbers.
                let raw = format!(
                    "JSONExtractRaw(attributes, '{}')",
                    escape_string_literal(key)
                );
                format!(
                    "ifNull(toFloat64OrNull({raw}), toFloat64OrNull({}))",
                    resolve_field(value)
                )
            }
            _ => {
                return Err(bad(
                    "Span axes accept duration_ms, duration_ns, http_status_code, or attributes.<name>",
                ));
            }
        },
    };
    Ok(format!("toFloat64OrNull(toString({expr}))"))
}

fn scatter_sql(req: &ScatterRequest, tenant: &str) -> Result<String, ApiError> {
    validate(&req.time_range, &req.filters)?;
    if !(1..=2000).contains(&req.limit) {
        return Err(bad("Scatter limit must be between 1 and 2000"));
    }
    let x = numeric_field(&req.source, &req.x_field)?;
    let y = numeric_field(&req.source, &req.y_field)?;
    let (table, time, trace, span, service, label, error, clauses) =
        if req.source == ScatterSource::Logs {
            (
                "logs",
                "Timestamp",
                "TraceId",
                "SpanId",
                "ServiceName",
                "leftUTF8(Body, 160)",
                "SeverityNumber >= 17",
                super::logs::build_log_where(
                    &req.filters,
                    &req.time_range.from,
                    &req.time_range.to,
                    None,
                    tenant,
                )?,
            )
        } else {
            (
                "spans",
                "timestamp",
                "trace_id",
                "span_id",
                "service_name",
                "span_name",
                ERROR_SPAN,
                build_where_clause_with_search(
                    &req.filters,
                    &req.time_range.from,
                    &req.time_range.to,
                    None,
                )
                .with_prewhere_prefix(&format!("tenant_id = '{}'", escape_string_literal(tenant))),
            )
        };
    // Limit+1 makes truncation explicit. Both coordinates always belong to one row.
    Ok(format!(
        "SELECT assumeNotNull(raw_x) AS x, assumeNotNull(raw_y) AS y, event_time AS timestamp, trace_id, span_id, service, label, error \
        FROM (SELECT {x} AS raw_x, {y} AS raw_y, toString({time}) AS event_time, {trace} AS trace_id, {span} AS span_id, \
        {service} AS service, {label} AS label, toUInt8({error}) AS error FROM {table} {}) \
        WHERE isNotNull(raw_x) AND isNotNull(raw_y) AND isFinite(raw_x) AND isFinite(raw_y) \
        ORDER BY timestamp DESC, trace_id, span_id LIMIT {}",
        clauses.to_sql(),
        req.limit + 1
    ))
}

pub async fn scatter(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    Json(req): Json<ScatterRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let sql = scatter_sql(&req, &tenant.tenant_id)?;
    let mut points = bounded_read(&state.ch, &sql, &tenant.tenant_id)
        .fetch_all::<ScatterPoint>()
        .await
        .map_err(|e| crate::api_error::internal_legacy("dashboard.scatter", e))?;
    let truncated = points.len() > req.limit as usize;
    points.truncate(req.limit as usize);
    Ok(Json(
        serde_json::json!({"points": points, "truncated": truncated}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn range() -> TimeRange {
        TimeRange {
            from: "2026-11-07T13:00:00Z".into(),
            to: "2026-11-07T14:00:00Z".into(),
        }
    }
    #[test]
    fn patterns_are_scoped_and_total_precedes_top_n() {
        let req = PatternRequest {
            time_range: range(),
            filters: vec![],
            limit: 10,
        };
        let sql = pattern_sql(&req, "tenant'one?").unwrap();
        assert!(sql.contains("tenant_id = 'tenant''one\\x3F'"));
        assert!(sql.contains("TimestampDate >="));
        assert!(sql.contains("sum(sum(events)) OVER () AS total_count"));
        assert!(sql.contains("intDiv(toUnixTimestamp(Timestamp), 60)"));
        assert!(sql.contains("leftUTF8(Body, 2048)"));
        assert!(sql.ends_with("LIMIT 10"));
    }
    #[test]
    fn scatter_uses_same_row_finite_pairs_and_explicit_truncation() {
        let req = ScatterRequest {
            source: ScatterSource::Spans,
            time_range: range(),
            filters: vec![],
            x_field: "attributes.http.request.body.size".into(),
            y_field: "duration_ms".into(),
            limit: 500,
        };
        let sql = scatter_sql(&req, "tenant-two").unwrap();
        assert!(sql.contains("tenant_id = 'tenant-two'"));
        assert!(sql.contains("duration_ns / 1000000.0"));
        assert!(sql.contains("JSONExtractRaw(attributes, 'http.request.body.size')"));
        assert!(sql.contains("isFinite(raw_x) AND isFinite(raw_y)"));
        assert!(sql.ends_with("LIMIT 501"));
    }
    #[test]
    fn validates_range_fields_filters_and_limits() {
        let mut req = ScatterRequest {
            source: ScatterSource::Logs,
            time_range: range(),
            filters: vec![],
            x_field: "body.bytes".into(),
            y_field: "log.latency".into(),
            limit: 2000,
        };
        assert!(
            scatter_sql(&req, "default")
                .unwrap()
                .contains("JSON_VALUE(Body")
        );
        req.limit = 2001;
        assert!(scatter_sql(&req, "default").is_err());
        req.limit = 10;
        req.time_range.from = "invalid".into();
        assert!(scatter_sql(&req, "default").is_err());
        assert!(numeric_field(&ScatterSource::Spans, "duration_ns); DROP TABLE spans").is_err());
        assert!(numeric_field(&ScatterSource::Spans, "not_a_column").is_err());
        assert!(numeric_field(&ScatterSource::Logs, "tenant_id").is_err());
    }
    #[tokio::test]
    async fn pattern_wire_format_preserves_counts_and_buckets() {
        use clickhouse::test::{Mock, handlers};
        let mock = Mock::new();
        let client = clickhouse::Client::default()
            .with_url(mock.url())
            .with_validation(false);
        mock.add(handlers::provide([PatternRow {
            service: "payments".into(),
            severity: "ERROR".into(),
            template: "retry N".into(),
            example: "retry 3".into(),
            trace_id: "abc".into(),
            last_seen: "2026-11-07 13:10:00".into(),
            count: 3,
            buckets: vec![(1794057000, 3)],
            total_count: 20,
        }]));
        let rows = bounded_read(&client, "SELECT * FROM logs", "one")
            .fetch_all::<PatternRow>()
            .await
            .unwrap();
        assert_eq!(rows[0].total_count, 20);
        assert_eq!(rows[0].buckets[0].1, 3);
    }

    /// Uses only inline synthetic rows. Never reads or changes stored telemetry.
    #[test]
    #[ignore = "requires RUSH_TEST_CLICKHOUSE_CONTAINER and Docker"]
    fn clickhouse_executes_pattern_and_scatter_sql() {
        fn execute(sql: String) -> Vec<serde_json::Value> {
            let container =
                std::env::var("RUSH_TEST_CLICKHOUSE_CONTAINER").expect("test container");
            // CTE-backed inline rows do not support PREWHERE. Preserve every
            // predicate but move it to WHERE for this synthetic-only test.
            let sql = sql.replace(" PREWHERE ", " WHERE ");
            let output = std::process::Command::new("docker")
                .args([
                    "exec",
                    &container,
                    "clickhouse-client",
                    "--output_format_json_quote_64bit_integers=0",
                    "--query",
                    &format!("{sql} FORMAT JSONEachRow"),
                ])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        }
        let logs = "WITH logs AS (SELECT * FROM values(\
            'tenant_id String, Timestamp DateTime64(9), TimestampDate Date, ServiceName String, SeverityText String, Body String, TraceId String', \
            ('one','2026-11-07 13:10:00','2026-11-07','payments','WARN','retry 3','a'), \
            ('one','2026-11-07 13:11:00','2026-11-07','payments','WARN','retry 4','b'), \
            ('one','2026-11-07 13:12:00','2026-11-07','payments','INFO','ok','c'), \
            ('two','2026-11-07 13:10:00','2026-11-07','private','WARN','retry 5','d')))";
        let req = PatternRequest {
            time_range: range(),
            filters: vec![],
            limit: 1,
        };
        let rows = execute(format!("{logs} {}", pattern_sql(&req, "one").unwrap()));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["count"].as_u64(), Some(2));
        assert_eq!(rows[0]["total_count"].as_u64(), Some(3));
        assert_eq!(rows[0]["example"], "retry 4");

        let spans = "WITH spans AS (SELECT * FROM values(\
            'tenant_id String, timestamp DateTime64(9), duration_ns UInt64, attributes String, trace_id String, span_id String, service_name String, span_name String, status String, http_status_code UInt16', \
            ('one','2026-11-07 13:10:00',0,'{\"bytes\":0}','a','b','payments','charge','OK',200), \
            ('one','2026-11-07 13:11:00',3000000,'{\"bytes\":\"12.5\"}','c','d','payments','charge','ERROR',500), \
            ('one','2026-11-07 13:12:00',4000000,'{}','e','f','payments','charge','OK',200), \
            ('two','2026-11-07 13:13:00',5000000,'{\"bytes\":100}','g','h','private','charge','OK',200)))";
        let req = ScatterRequest {
            source: ScatterSource::Spans,
            time_range: range(),
            filters: vec![],
            x_field: "attributes.bytes".into(),
            y_field: "duration_ms".into(),
            limit: 10,
        };
        let rows = execute(format!("{spans} {}", scatter_sql(&req, "one").unwrap()));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["x"].as_f64(), Some(12.5));
        assert_eq!(rows[0]["y"].as_f64(), Some(3.0));
        assert_eq!(rows[1]["x"].as_f64(), Some(0.0));
        assert_eq!(rows[1]["y"].as_f64(), Some(0.0));
        assert!(rows.iter().all(|row| row["service"] == "payments"));

        let logs = "WITH logs AS (SELECT 'one' AS tenant_id, \
            parseDateTime64BestEffort('2026-11-07 13:10:00', 9) AS Timestamp, toDate(Timestamp) AS TimestampDate, \
            'payments' AS ServiceName, '{\"bytes\":0}' AS Body, 'a' AS TraceId, 'b' AS SpanId, \
            toUInt8(17) AS SeverityNumber, map('latency','7') AS LogAttributes)";
        let req = ScatterRequest {
            source: ScatterSource::Logs,
            time_range: range(),
            filters: vec![],
            x_field: "body.bytes".into(),
            y_field: "log.latency".into(),
            limit: 10,
        };
        let rows = execute(format!("{logs} {}", scatter_sql(&req, "one").unwrap()));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["x"].as_f64(), Some(0.0));
        assert_eq!(rows[0]["y"].as_f64(), Some(7.0));
        assert_eq!(rows[0]["error"], 1);
    }
}
