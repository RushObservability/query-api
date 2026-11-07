//! Versioned query boundary. Existing handlers remain the only query executors.
use axum::{
    Extension, Json,
    extract::{Query, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{sync::LazyLock, time::Instant};

use super::{dashboard_panels, explore, logs, metrics, query};
use crate::{
    AppState, TenantContext,
    api_error::ApiError,
    models::query::{Filter, FilterOp, TimeRange},
    query_governor::ValidatedTimeRange,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    version: String,
    signal: String,
    operation: String,
    time_range: TimeRange,
    #[serde(default)]
    filters: Vec<Filter>,
    #[serde(default)]
    search: Option<String>,
    #[serde(default = "empty_options")]
    options: Value,
}

fn empty_options() -> Value {
    json!({})
}
fn invalid() -> ApiError {
    ApiError::public(
        StatusCode::BAD_REQUEST,
        "invalid_query",
        "query does not match the v1 contract",
    )
}
fn decode<T: DeserializeOwned>(body: Value) -> Result<T, ApiError> {
    serde_json::from_value(body).map_err(|_| invalid())
}

static REGISTRY: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../contracts/query-v1.json")).expect("query registry")
});

fn unix_seconds(value: &str) -> String {
    (chrono::DateTime::parse_from_rfc3339(value)
        .expect("validated time")
        .timestamp_millis() as f64
        / 1000.0)
        .to_string()
}

/// The same registry ships in each client repository. No checkout-relative runtime dependency.
pub async fn schema() -> Json<Value> {
    Json(
        serde_json::from_str(include_str!("../../contracts/query-v1.schema.json"))
            .expect("query schema"),
    )
}

impl Request {
    fn legacy_body(&self) -> Result<Value, ApiError> {
        if self.version != "v1" {
            return Err(ApiError::public(
                StatusCode::BAD_REQUEST,
                "unsupported_query_version",
                "supported query contract version is v1",
            ));
        }
        let route = REGISTRY["routes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| {
                r["operation"] == self.operation
                    && (r["signal"] == self.signal
                        || r["signal"] == "dynamic"
                            && matches!(self.signal.as_str(), "logs" | "spans"))
            })
            .ok_or_else(invalid)?;
        let options = self.options.as_object().ok_or_else(invalid)?;
        if options
            .keys()
            .any(|k| !route["options"].as_array().unwrap().iter().any(|v| v == k))
        {
            return Err(invalid());
        }
        let budget =
            crate::query_governor::active_budget().map_or(30 * 86400, |b| b.max_time_range_secs);
        // All callers send absolute RFC3339. Relative lookbacks are resolved exactly once by clients.
        if chrono::DateTime::parse_from_rfc3339(&self.time_range.from).is_err()
            || chrono::DateTime::parse_from_rfc3339(&self.time_range.to).is_err()
        {
            return Err(invalid());
        }
        ValidatedTimeRange::parse(&self.time_range.from, &self.time_range.to, budget)
            .map_err(|_| invalid())?;
        if self.filters.len() > REGISTRY["max_filters"].as_u64().unwrap() as usize
            || self
                .search
                .as_ref()
                .is_some_and(|s| s.len() > REGISTRY["max_search_bytes"].as_u64().unwrap() as usize)
        {
            return Err(invalid());
        }
        for filter in &self.filters {
            if filter.field.is_empty()
                || filter.field.len() > 128
                || filter.field.trim() != filter.field
                || filter.field == "tenant_id"
                || filter.field.chars().any(char::is_control)
            {
                return Err(invalid());
            }
            let scalar = |v: &Value| v.is_string() || v.is_number() || v.is_boolean();
            let valid = match filter.op {
                FilterOp::In | FilterOp::NotIn => filter
                    .value
                    .as_array()
                    .is_some_and(|a| !a.is_empty() && a.len() <= 100 && a.iter().all(scalar)),
                _ => scalar(&filter.value),
            };
            if !valid || serde_json::to_vec(&filter.value).map_or(true, |v| v.len() > 8192) {
                return Err(invalid());
            }
        }
        if self.signal == "metrics"
            && (!self.filters.is_empty() || self.search.as_ref().is_some_and(|s| !s.is_empty()))
        {
            // PromQL owns its selectors. Never silently ignore a structured filter.
            return Err(invalid());
        }
        if matches!(self.operation.as_str(), "patterns" | "scatter")
            && self.search.as_ref().is_some_and(|s| !s.is_empty())
        {
            return Err(invalid());
        }
        if let Some(limit) = options.get("limit") {
            let max = match self.operation.as_str() {
                "patterns" => 50,
                "scatter" => 2000,
                _ => 1000,
            };
            if !limit.as_u64().is_some_and(|v| v > 0 && v <= max) {
                return Err(invalid());
            }
        }
        if options
            .get("offset")
            .is_some_and(|v| v.as_u64().is_none_or(|v| v > 10_000))
        {
            return Err(invalid());
        }
        if options
            .get("cursor")
            .is_some_and(|v| !v.is_null() && (!v.is_string() || v.as_str().unwrap().len() > 8192))
        {
            return Err(invalid());
        }
        if options
            .get("columns")
            .is_some_and(|v| !v.is_null() && !matches!(v.as_str(), Some("list" | "wide")))
        {
            return Err(invalid());
        }
        if options.get("interval").is_some_and(|interval| {
            !matches!(
                interval.as_str(),
                Some("1s" | "10s" | "1m" | "5m" | "15m" | "1h" | "1d")
            )
        }) {
            return Err(invalid());
        }
        if self.signal == "metrics" {
            if !options
                .get("query")
                .and_then(Value::as_str)
                .is_some_and(|q| !q.trim().is_empty() && q.len() <= 16384)
            {
                return Err(invalid());
            }
            if self.operation == "instant" && self.time_range.from != self.time_range.to {
                return Err(invalid());
            }
            if options
                .get("step")
                .is_some_and(|v| !v.as_str().is_some_and(|s| !s.is_empty() && s.len() <= 32))
            {
                return Err(invalid());
            }
        }
        let mut body = self.options.clone();
        body["time_range"] = serde_json::to_value(&self.time_range).unwrap();
        body["filters"] = serde_json::to_value(&self.filters).unwrap();
        if !matches!(self.operation.as_str(), "patterns" | "scatter") {
            body["search"] = json!(self.search);
        }
        if self.operation == "explore" {
            body["signal"] = json!(self.signal);
        }
        if self.operation == "scatter" {
            body["source"] = json!(self.signal);
        }
        Ok(body)
    }
}

/// Authentication, signal policy, admission and time budgets run on this route before dispatch.
pub async fn execute(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantContext>,
    payload: Result<Json<Request>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(req) = payload.map_err(|_| invalid())?;
    let body = req.legacy_body()?;
    let started = Instant::now();
    // Keep dispatch explicit. None of these handlers accepts SQL or mutates resources.
    let result = match (req.signal.as_str(), req.operation.as_str()) {
        ("spans", "search") => {
            query::execute_query(State(state), Extension(tenant), Json(decode(body)?))
                .await
                .map(IntoResponse::into_response)
        }
        ("logs", "search") => {
            logs::query_logs(State(state), Extension(tenant), Json(decode(body)?))
                .await
                .map(IntoResponse::into_response)
        }
        ("logs" | "spans", "explore") => {
            explore::search(State(state), Extension(tenant), Json(decode(body)?)).await
        }
        ("spans", "count") => {
            query::count_query(State(state), Extension(tenant), Json(decode(body)?))
                .await
                .map(IntoResponse::into_response)
        }
        ("logs", "count") => logs::count_logs(State(state), Extension(tenant), Json(decode(body)?))
            .await
            .map(IntoResponse::into_response),
        ("spans", "group") => {
            query::group_query(State(state), Extension(tenant), Json(decode(body)?))
                .await
                .map(IntoResponse::into_response)
        }
        ("logs", "group") => logs::group_logs(State(state), Extension(tenant), Json(decode(body)?))
            .await
            .map(IntoResponse::into_response),
        ("spans", "timeseries") => {
            query::timeseries_query(State(state), Extension(tenant), Json(decode(body)?))
                .await
                .map(IntoResponse::into_response)
        }
        ("logs", "histogram") => {
            logs::log_histogram(State(state), Extension(tenant), Json(decode(body)?))
                .await
                .map(IntoResponse::into_response)
        }
        ("logs", "patterns") => {
            dashboard_panels::patterns(State(state), Extension(tenant), Json(decode(body)?))
                .await
                .map(IntoResponse::into_response)
        }
        ("logs" | "spans", "scatter") => {
            dashboard_panels::scatter(State(state), Extension(tenant), Json(decode(body)?))
                .await
                .map(IntoResponse::into_response)
        }
        ("metrics", "range") => metrics::prom_query_range(
            State(state),
            Extension(tenant),
            Query(metrics::RangeQueryParams {
                query: req.options["query"].as_str().unwrap().into(),
                start: unix_seconds(&req.time_range.from),
                end: unix_seconds(&req.time_range.to),
                step: req
                    .options
                    .get("step")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            }),
        )
        .await
        .map(IntoResponse::into_response),
        ("metrics", "instant") => metrics::prom_query(
            State(state),
            Extension(tenant),
            Query(metrics::InstantQueryParams {
                query: req.options["query"].as_str().unwrap().into(),
                time: Some(unix_seconds(&req.time_range.to)),
            }),
        )
        .await
        .map(IntoResponse::into_response),
        _ => return Err(invalid()),
    };
    let response = result.map_err(|(status, cause)| {
        if status.is_server_error() {
            ApiError::internal_with_status(status, "query.execute", cause)
        } else {
            ApiError::public(
                status,
                "invalid_query",
                "query could not be executed; check its fields and options",
            )
        }
    })?;
    wrap_response(&req, response, started).await
}

async fn wrap_response(
    req: &Request,
    response: Response,
    started: Instant,
) -> Result<Response, ApiError> {
    let (mut parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, 32 * 1024 * 1024)
        .await
        .map_err(|e| ApiError::internal("query.response", e))?;
    let data: Value =
        serde_json::from_slice(&bytes).map_err(|e| ApiError::internal("query.response", e))?;
    let partial = data
        .get("errors")
        .and_then(Value::as_object)
        .is_some_and(|e| !e.is_empty());
    let warnings: Vec<&str> = if partial {
        vec!["Some query summaries are unavailable. Rows may still be complete."]
    } else {
        vec![]
    };
    // Preserve cursors, exact/capped counts and source-specific row/series shapes inside data.
    let envelope = json!({"version":"v1", "signal":req.signal, "operation":req.operation, "data":data,
        "meta":{"request_id":crate::api_error::current_request_id(), "time_range":req.time_range,
        "elapsed_ms":started.elapsed().as_millis() as u64, "partial":partial, "warnings":warnings}});
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    parts.headers.remove(axum::http::header::CONTENT_ENCODING);
    let mut response = Json(envelope).into_response();
    response.headers_mut().extend(parts.headers);
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn base() -> Value {
        json!({"version":"v1","signal":"logs","operation":"search",
        "time_range":{"from":"2026-01-01T00:00:00Z","to":"2026-01-01T01:00:00Z"},"filters":[],"options":{}})
    }

    #[test]
    fn client_fixtures_are_accepted() {
        let cases: Value =
            serde_json::from_str(include_str!("../../contracts/query-v1.fixtures.json")).unwrap();
        for c in cases.as_array().unwrap() {
            let request: Request = serde_json::from_value(c["request"].clone()).unwrap();
            request.legacy_body().unwrap();
        }
    }
    #[test]
    fn every_registered_operation_builds_its_existing_handler_request() {
        for route in REGISTRY["routes"].as_array().unwrap() {
            let signals = if route["signal"] == "dynamic" {
                vec!["logs", "spans"]
            } else {
                vec![route["signal"].as_str().unwrap()]
            };
            for signal in signals {
                let mut value = base();
                value["signal"] = json!(signal);
                value["operation"] = route["operation"].clone();
                value["options"] = match route["operation"].as_str().unwrap() {
                    "group" => json!({"group_by":["service_name"]}),
                    "scatter" => json!({"x_field":"attributes.bytes","y_field":"duration_ms"}),
                    "range" | "instant" => json!({"query":"up"}),
                    _ => json!({}),
                };
                if route["operation"] == "instant" {
                    value["time_range"]["to"] = value["time_range"]["from"].clone();
                }
                let req: Request = serde_json::from_value(value).unwrap();
                let body = req.legacy_body().unwrap();
                match (signal, req.operation.as_str()) {
                    ("spans", "search" | "group") | ("logs", "group") => {
                        decode::<crate::models::query::QueryRequest>(body).unwrap();
                    }
                    ("logs", "search") => {
                        decode::<logs::LogQueryRequest>(body).unwrap();
                    }
                    (_, "count") => {
                        decode::<crate::models::query::CountQueryRequest>(body).unwrap();
                    }
                    (_, "timeseries") => {
                        decode::<crate::models::query::TimeseriesRequest>(body).unwrap();
                    }
                    (_, "explore") => {
                        decode::<explore::ExploreSearchRequest>(body).unwrap();
                    }
                    (_, "histogram") => {
                        decode::<logs::LogHistogramRequest>(body).unwrap();
                    }
                    (_, "patterns") => {
                        decode::<dashboard_panels::PatternRequest>(body).unwrap();
                    }
                    (_, "scatter") => {
                        decode::<dashboard_panels::ScatterRequest>(body).unwrap();
                    }
                    ("metrics", _) => {}
                    _ => panic!("uncovered operation"),
                }
            }
        }
        assert_eq!(unix_seconds("2026-01-01T00:00:00.250Z"), "1767225600.25");
    }
    #[test]
    fn rejects_invalid_versions_signals_options_and_scoping() {
        for (key, value) in [
            ("version", json!("v2")),
            ("signal", json!("audit")),
            ("operation", json!("delete")),
            ("options", json!({"tenant_id":"other"})),
            ("options", json!({"limit":0})),
            ("options", json!({"limit":1001})),
            ("options", json!({"offset":10001})),
            (
                "filters",
                json!([{"field":"tenant_id","op":"=","value":"other"}]),
            ),
            ("filters", json!([{"field":"x","op":"IN","value":"wrong"}])),
            ("search", json!("x".repeat(513))),
        ] {
            let mut body = base();
            body[key] = value;
            assert!(
                serde_json::from_value::<Request>(body)
                    .unwrap()
                    .legacy_body()
                    .is_err(),
                "{key}"
            );
        }
        let mut body = base();
        body["tenant_id"] = json!("other");
        assert!(serde_json::from_value::<Request>(body).is_err());
    }
    #[test]
    fn rejects_ignored_metric_filters_and_bad_time_ranges() {
        for range in [
            json!({"from":"yesterday","to":"now"}),
            json!({"from":"2026-01-02T00:00:00Z","to":"2026-01-01T00:00:00Z"}),
        ] {
            let mut body = base();
            body["time_range"] = range;
            assert!(
                serde_json::from_value::<Request>(body)
                    .unwrap()
                    .legacy_body()
                    .is_err()
            );
        }
        let mut body = base();
        body["signal"] = json!("metrics");
        body["operation"] = json!("range");
        body["options"] = json!({"query":"up"});
        body["search"] = json!("ignored");
        assert!(
            serde_json::from_value::<Request>(body)
                .unwrap()
                .legacy_body()
                .is_err()
        );
    }
    #[tokio::test]
    async fn preserves_partial_results_and_pagination() {
        let req: Request = serde_json::from_value(base()).unwrap();
        let response = wrap_response(&req, Json(json!({"rows":[],"next_cursor":"opaque","count":{"value":1000,"kind":"capped"},"errors":{"histogram":"unavailable"}})).into_response(), Instant::now()).await.unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["data"]["next_cursor"], "opaque");
        assert_eq!(value["data"]["count"]["kind"], "capped");
        assert_eq!(value["meta"]["partial"], true);
        assert_eq!(value["meta"]["warnings"].as_array().unwrap().len(), 1);
    }
}
