//! Generated from query-api/contracts/client.rs. Run query-api/scripts/sync-query-contract.py.
//! Transport adapter only. Validation, authorization and query execution belong to the API.
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

pub const ENDPOINT: &str = "/api/v1/query/execute";

pub fn adapt(path: &str, body: &Value) -> Option<Value> {
    let registry: Value = serde_json::from_str(include_str!("../contracts/query-v1.json")).ok()?;
    let route = registry["routes"]
        .as_array()?
        .iter()
        .find(|r| r["path"] == path)?;
    let mut options = body.as_object()?.clone();
    let time_range = options.remove("time_range")?;
    let filters = options.remove("filters").unwrap_or_else(|| json!([]));
    let search = options.remove("search").unwrap_or(Value::Null);
    let signal = if route["signal"] == "dynamic" {
        options.remove(if route["operation"] == "scatter" {
            "source"
        } else {
            "signal"
        })?
    } else {
        route["signal"].clone()
    };
    Some(
        json!({"version":"v1", "signal":signal, "operation":route["operation"],
        "time_range":time_range, "filters":filters, "search":search, "options":options}),
    )
}

#[allow(dead_code)] // The tail-only CLI does not issue PromQL queries.
pub fn prom_request(path: &str, params: &[(&str, String)]) -> Option<Value> {
    if !matches!(path, "/prom/api/v1/query" | "/prom/api/v1/query_range") {
        return None;
    }
    let get = |key: &str| {
        params
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.clone())
    };
    // Resolve "now" once, so instant queries have an explicit, reproducible evaluation time.
    let end = get(if path.ends_with("query_range") {
        "end"
    } else {
        "time"
    })
    .unwrap_or_else(|| (chrono::Utc::now().timestamp_millis() as f64 / 1000.0).to_string());
    let start = get("start").unwrap_or_else(|| end.clone());
    let timestamp = |s: &str| {
        let seconds = s.parse::<f64>().ok()?;
        if !seconds.is_finite() {
            return None;
        }
        chrono::DateTime::from_timestamp_millis((seconds * 1000.0) as i64)
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    };
    let mut options = json!({"query": get("query")?});
    if let Some(step) = get("step") {
        options["step"] = json!(step);
    }
    Some(json!({"version":"v1", "signal":"metrics",
        "operation":if path.ends_with("query_range") { "range" } else { "instant" },
        "time_range":{"from":timestamp(&start)?, "to":timestamp(&end)?},
        "filters":[], "search":null, "options":options}))
}

/// Reject a wrong version or a response for a different query before decoding data.
pub fn decode<T: DeserializeOwned>(bytes: &[u8], request: &Value) -> Result<T, serde_json::Error> {
    let envelope: Value = serde_json::from_slice(bytes)?;
    for key in ["version", "signal", "operation"] {
        if envelope.get(key) != request.get(key) {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "query response contract mismatch",
            ));
        }
    }
    let data = envelope.get("data").cloned().ok_or_else(|| {
        <serde_json::Error as serde::de::Error>::custom("query response is missing data")
    })?;
    serde_json::from_value(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_fixtures() {
        let cases: Value =
            serde_json::from_str(include_str!("../contracts/query-v1.fixtures.json")).unwrap();
        for c in cases.as_array().unwrap() {
            let actual = if let Some(params) = c.get("params") {
                let pairs: Vec<_> = params
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str().unwrap().to_string()))
                    .collect();
                prom_request(c["path"].as_str().unwrap(), &pairs)
            } else {
                adapt(c["path"].as_str().unwrap(), &c["body"])
            };
            assert_eq!(actual, Some(c["request"].clone()), "{}", c["path"]);
            let response = json!({"version":"v1", "signal":c["request"]["signal"],
                "operation":c["request"]["operation"], "data":{"rows":[]}, "meta":{}});
            assert_eq!(
                decode::<Value>(&serde_json::to_vec(&response).unwrap(), &c["request"]).unwrap(),
                json!({"rows":[]})
            );
        }
    }

    #[test]
    fn rejects_wrong_envelopes_and_leaves_mutations_alone() {
        assert!(adapt("/api/v1/dashboards", &json!({})).is_none());
        let request = json!({"version":"v1", "signal":"logs", "operation":"search"});
        assert!(decode::<Value>(br#"{"version":"v2","data":{}}"#, &request).is_err());
        assert!(decode::<Value>(br#"{"rows":[]}"#, &request).is_err());
    }
}
