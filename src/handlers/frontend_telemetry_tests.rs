use super::*;
use serde_json::json;
fn payload(events: Value) -> RumIngestPayload {
    serde_json::from_value(json!({"meta":{"app_name":"rush-ui","app_version":"0.1.13","environment":"production","session_id":"sensitive-session","user_id":"sensitive-user","page_url":"https://rush.example/logs?secret=sensitive-query","page_path":"/logs/sensitive-id?secret=sensitive-query","view_name":"sensitive-title","referrer":"sensitive-referrer"},"events":events})).unwrap()
}
#[test]
fn privacy_is_enforced_without_losing_bounded_health_signals() {
    let input = payload(json!([
        {"event_type":"custom","event_name":"frontend_api_request","timestamp":123,"trace_id":"sensitive-trace","span_id":"sensitive-span","vital_name":"sensitive-vital","attributes":json!({"operation":"query","method":"GET","outcome":"decode_error","count":2,"duration_ms_sum":12.5,"status_class":"2xx","query":"sensitive-query","password":"sensitive-password","route":"sensitive-route"}).to_string()},
        {"event_type":"pageview","event_name":"sensitive-title","trace_id":"sensitive-trace","span_id":"sensitive-span"},
        {"event_type":"error","error_message":"sensitive-password","error_stack":"sensitive-stack","error_type":"sensitive-type"},
        {"event_type":"custom","event_name":"arbitrary-log-relay","attributes":"sensitive-payload"},
        {"event_type":"resource","event_name":"https://sensitive-url/"}
    ]));
    let output = sanitize_payload(input).unwrap();
    assert_eq!(output.events.len(), 3);
    assert_eq!(output.meta.page_path, "/logs");
    let encoded = serde_json::to_string(&output).unwrap();
    assert!(!encoded.contains("sensitive"));
    assert!(!encoded.contains("arbitrary-log-relay"));
    assert_eq!(output.events[0].timestamp, None);
    let attrs: Value = serde_json::from_str(&output.events[0].attributes).unwrap();
    assert_eq!(attrs["operation"], "query");
    assert_eq!(attrs["count"].as_f64(), Some(2.0));
    assert_eq!(attrs["duration_ms_sum"].as_f64(), Some(12.5));
    assert_eq!(attrs["route"], "other");
    assert_eq!(output.events[1].event_name, "pageview");
    assert!(
        output
            .events
            .iter()
            .all(|e| e.trace_id.is_empty() && e.span_id.is_empty())
    );
    assert_eq!(output.events[2].error_message, "Client error");
    assert_eq!(output.events[2].error_type, "Error");
}
#[test]
fn real_sdk_fixture_preserves_the_allowed_event_and_attribute_contract() {
    let mut payloads: Vec<RumIngestPayload> = serde_json::from_str(include_str!(
        "../../tests/fixtures/frontend-health-sdk.json"
    ))
    .unwrap();
    payloads.push(
        serde_json::from_str(include_str!(
            "../../tests/fixtures/frontend-health-all-events.json"
        ))
        .unwrap(),
    );
    let mut accepted = Vec::new();
    for original in payloads {
        let events = original
            .events
            .iter()
            .map(|event| {
                (
                    event.event_type.clone(),
                    event.event_name.clone(),
                    event.attributes.clone(),
                )
            })
            .collect::<Vec<_>>();
        let sanitized = sanitize_payload(original).unwrap();
        assert_eq!(sanitized.events.len(), events.len());
        for (event, (kind, name, attrs)) in sanitized.events.iter().zip(events) {
            assert_eq!(event.event_type, kind);
            if kind == "custom" {
                assert_eq!(event.event_name, name);
                let before: Value = serde_json::from_str(&attrs).unwrap();
                let after: Value = serde_json::from_str(&event.attributes).unwrap();
                for (key, value) in before.as_object().unwrap() {
                    let retained = &after[key];
                    if value.is_number() {
                        assert_eq!(retained.as_f64(), value.as_f64(), "{name}.{key}");
                    } else {
                        assert_eq!(retained, value, "{name}.{key}");
                    }
                }
            }
        }
        accepted.push(sanitized);
    }
    if let Ok(path) = std::env::var("RUSH_FRONTEND_SANITIZED_FIXTURE_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&accepted).unwrap()).unwrap();
    }
}
#[test]
fn identity_checks_accept_only_a_validated_same_tenant_session() {
    let identity = RequestIdentity {
        tenant_id: "tenant-a".into(),
        authenticated: true,
        actor_id: "user-1".into(),
        actor_name: "viewer".into(),
        actor_type: "user".into(),
        credential_type: "session".into(),
    };
    assert!(session_identity(&identity, "tenant-a"));
    assert!(!session_identity(&identity, "tenant-b"));
    for kind in ["anonymous", "query_key", "ingest_key", "internal_sre"] {
        let mut changed = identity.clone();
        changed.credential_type = kind.into();
        assert!(!session_identity(&changed, "tenant-a"));
    }
    let mut changed = identity.clone();
    changed.authenticated = false;
    assert!(!session_identity(&changed, "tenant-a"));
    changed = identity;
    changed.actor_id.clear();
    assert!(!session_identity(&changed, "tenant-a"));
}
#[test]
fn user_and_tenant_limits_are_independent_and_reset() {
    let limiter = dashmap::DashMap::new();
    let now = Instant::now();
    for _ in 0..USER_REQUESTS_PER_MINUTE {
        consume_rate_limits(&limiter, "u1", "t1", now).unwrap();
    }
    assert_eq!(consume_rate_limits(&limiter, "u1", "t1", now), Err(60));
    consume_rate_limits(&limiter, "u2", "t1", now).unwrap();
    consume_rate_limits(&limiter, "u1", "t1", now + Duration::from_secs(60)).unwrap();
    let limiter = dashmap::DashMap::new();
    for index in 0..TENANT_REQUESTS_PER_MINUTE {
        consume_rate_limits(&limiter, &format!("u{index}"), "t1", now).unwrap();
    }
    assert!(consume_rate_limits(&limiter, "another", "t1", now).is_err());
    consume_rate_limits(&limiter, "another", "t2", now).unwrap();
}
#[test]
fn payload_and_attribute_values_remain_bounded() {
    let event = json!({"event_type":"custom","event_name":"frontend_started","duration_ms":-1,"attributes":json!({"startup_duration_ms":1e30,"count":-1,"method":"sensitive-url","arbitrary":"sensitive-token"}).to_string()});
    let result = sanitize_payload(payload(json!([event.clone()]))).unwrap();
    let attrs: Value = serde_json::from_str(&result.events[0].attributes).unwrap();
    assert_eq!(attrs["startup_duration_ms"].as_f64(), Some(MAX_COUNTER));
    assert!(attrs.get("count").is_none());
    assert_eq!(attrs["method"], "other");
    assert!(attrs.get("arbitrary").is_none());
    let too_many = Value::Array(vec![event; MAX_EVENTS + 1]);
    assert_eq!(
        sanitize_payload(payload(too_many)).unwrap_err().0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let mut invalid = payload(json!([]));
    invalid.meta.app_name = "secret?token=value".into();
    assert_eq!(
        sanitize_payload(invalid).unwrap_err().0,
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn namespace_and_build_metadata_are_preserved_with_bounded_application_identity() {
    let mut input = payload(json!([]));
    input.meta.app_name = "canary".into();
    input.meta.app_version = "1.2.3+abc123".into();
    let output = sanitize_payload(input).unwrap();
    assert_eq!(output.meta.app_name, "rush-ui:canary");
    assert_eq!(output.meta.app_version, "1.2.3+abc123");
    assert_eq!(output.meta.environment, "production");
}
