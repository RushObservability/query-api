//! PagerDuty Events API v2. Credentials belong in the event body, not a REST API header.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub fn endpoint(config: &Value) -> Result<&'static str, String> {
    match config.get("region") {
        None => Ok("https://events.pagerduty.com/v2/enqueue"),
        Some(Value::String(region)) if region == "us" => {
            Ok("https://events.pagerduty.com/v2/enqueue")
        }
        Some(Value::String(region)) if region == "eu" => {
            Ok("https://events.eu.pagerduty.com/v2/enqueue")
        }
        _ => Err("PagerDuty region must be 'us' or 'eu'".into()),
    }
}

fn severity(config: &Value) -> &Value {
    config
        .get("severity")
        .or_else(|| {
            config
                .get("severity_mapping")
                .and_then(|m| m.get("critical"))
        })
        .unwrap_or(&Value::Null)
}

pub fn validate_config(config: &Value) -> Result<(), String> {
    let key = config
        .get("routing_key")
        .and_then(Value::as_str)
        .unwrap_or("");
    if key.is_empty() || !key.bytes().all(|c| c.is_ascii_graphic()) {
        return Err(
            "PagerDuty requires an Events API v2 integration key without whitespace".into(),
        );
    }
    endpoint(config)?;
    if !severity(config).is_null()
        && !matches!(
            severity(config).as_str(),
            Some("critical" | "error" | "warning" | "info")
        )
    {
        return Err("PagerDuty severity must be critical, error, warning, or info".into());
    }
    Ok(())
}

pub fn payload(
    channel: &crate::models::alert::NotificationChannel,
    config: &Value,
    alert_id: &str,
    alert_name: &str,
    state: &str,
    message: &str,
    value: f64,
    threshold: f64,
    signal: &str,
    runbook_url: &str,
) -> Value {
    // Hash a structured identity to bound the key length and avoid delimiter collisions.
    // Names and values can change without leaving an unresolvable alert behind.
    let identity = if alert_id.is_empty() {
        json!(["test", channel.tenant_id, channel.id])
    } else {
        json!(["alert", channel.tenant_id, signal, alert_id])
    };
    let dedup_key = format!("rush:{:x}", Sha256::digest(identity.to_string().as_bytes()));
    let resolved = state.eq_ignore_ascii_case("ok") || state.eq_ignore_ascii_case("resolved");
    let mut event = json!({
        "event_action": if resolved { "resolve" } else { "trigger" },
        "dedup_key": dedup_key,
    });
    if !resolved {
        // Bound the UTF-8 byte length without splitting a character.
        let summary = if message.trim().is_empty() {
            alert_name
        } else {
            message
        };
        let summary = if summary.trim().is_empty() {
            "Rush alert"
        } else {
            summary
        };
        let mut end = summary.len().min(1024);
        while !summary.is_char_boundary(end) {
            end -= 1;
        }
        event["payload"] = json!({
            "summary": &summary[..end],
            "source": "rush-observability",
            "severity": severity(config).as_str().unwrap_or("critical"),
            "custom_details": {
                "alert_id": alert_id,
                "alert_name": alert_name,
                "tenant_id": channel.tenant_id,
                "signal_type": signal,
                "message": message,
                "value": value,
                "threshold": threshold,
                "runbook_url": runbook_url,
                "test": state.eq_ignore_ascii_case("test"),
            }
        });
    }
    event
}

/// The manual notify endpoint receives Rush's generic anomaly payload.
pub fn manual_payload(
    channel: &crate::models::alert::NotificationChannel,
    config: &Value,
    data: &Value,
) -> Value {
    let name = data["monitor"].as_str().unwrap_or("Rush notification");
    let identity = json!([data["monitor"], data["metric"]]).to_string();
    payload(
        channel,
        config,
        &identity,
        name,
        "FIRING",
        data["text"].as_str().unwrap_or(name),
        data["value"].as_f64().unwrap_or(0.0),
        data["expected"].as_f64().unwrap_or(0.0),
        "anomaly",
        "",
    )
}

fn check_status(status: reqwest::StatusCode) -> Result<(), String> {
    // Events API v2 accepts deliveries asynchronously with 202.
    if status == reqwest::StatusCode::ACCEPTED {
        Ok(())
    } else {
        // Never include the response body, which could echo the routing key.
        Err(format!(
            "PagerDuty rejected the notification (HTTP {})",
            status.as_u16()
        ))
    }
}

pub async fn send(config: &Value, event: &Value) -> Result<(), String> {
    validate_config(config)?;
    let mut body = event.clone();
    body.as_object_mut()
        .ok_or("PagerDuty event must be an object")?
        .insert("routing_key".into(), config["routing_key"].clone());
    let response = super::delivery::request(reqwest::Method::POST, endpoint(config)?, true)
        .await?
        .json(&body)
        .send()
        .await
        .map_err(|_| "PagerDuty delivery failed; check connectivity and try again".to_string())?;
    check_status(response.status())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel() -> crate::models::alert::NotificationChannel {
        crate::models::alert::NotificationChannel {
            id: "channel-1".into(),
            tenant_id: "tenant-a".into(),
            name: "PagerDuty".into(),
            channel_type: "pagerduty".into(),
            config: "{}".into(),
            enabled: true,
            created_at: "".into(),
        }
    }

    #[test]
    fn validates_keys_regions_and_severity_with_legacy_defaults() {
        assert!(validate_config(&json!({"routing_key": "saved-key"})).is_ok());
        assert_eq!(
            endpoint(&json!({})).unwrap(),
            "https://events.pagerduty.com/v2/enqueue"
        );
        assert_eq!(
            endpoint(&json!({"region": "eu"})).unwrap(),
            "https://events.eu.pagerduty.com/v2/enqueue"
        );
        for config in [
            json!({}),
            json!({"routing_key": " "}),
            json!({"routing_key": "bad\nkey"}),
            json!({"routing_key": "key", "region": "https://evil.example"}),
            json!({"routing_key": "key", "region": 1}),
            json!({"routing_key": "key", "severity": "Critical"}),
        ] {
            assert!(validate_config(&config).is_err());
        }
        for severity in ["critical", "error", "warning", "info"] {
            assert!(validate_config(&json!({"routing_key": "key", "severity": severity})).is_ok());
        }
    }

    #[test]
    fn trigger_and_recovery_share_identity_not_names_or_tenants() {
        let mut channel = channel();
        let config = json!({"severity": "warning"});
        let fire = payload(
            &channel,
            &config,
            "rule-1",
            "Before",
            "FIRING",
            "High CPU",
            99.0,
            90.0,
            "metrics",
            "https://example.com/runbook",
        );
        assert_eq!(fire["event_action"], "trigger");
        assert_eq!(fire["payload"]["severity"], "warning");
        assert_eq!(fire["payload"]["custom_details"]["value"], 99.0);
        for state in ["ok", "OK", "resolved", "RESOLVED"] {
            let resolved = payload(
                &channel,
                &config,
                "rule-1",
                "After",
                state,
                "Recovered",
                10.0,
                90.0,
                "metrics",
                "",
            );
            assert_eq!(fire["dedup_key"], resolved["dedup_key"]);
            assert_eq!(resolved["event_action"], "resolve");
            assert!(resolved.get("payload").is_none());
        }
        let test = payload(
            &channel,
            &config,
            "",
            "Test Alert",
            "TEST",
            "Test",
            0.0,
            0.0,
            "metrics",
            "",
        );
        assert_ne!(fire["dedup_key"], test["dedup_key"]);
        assert_eq!(test["payload"]["custom_details"]["test"], true);
        channel.tenant_id = "tenant-b".into();
        let other = payload(
            &channel, &config, "rule-1", "Before", "FIRING", "High CPU", 99.0, 90.0, "metrics", "",
        );
        assert_ne!(fire["dedup_key"], other["dedup_key"]);
    }

    #[test]
    fn groups_stay_distinct_and_long_unicode_summaries_are_bounded() {
        let channel = channel();
        let make = |group: &str| {
            payload(
                &channel,
                &json!({}),
                &super::super::rootly::monitor_alert_id("m1", group),
                "Long",
                "FIRING",
                &"🦀".repeat(1000),
                0.0,
                0.0,
                "monitors",
                "",
            )
        };
        let a = make("host-a");
        assert_ne!(a["dedup_key"], make("host-b")["dedup_key"]);
        assert!(a["dedup_key"].as_str().unwrap().len() < 255);
        assert_eq!(a["payload"]["summary"].as_str().unwrap().len(), 1024);
        assert_eq!(a["payload"]["severity"], "critical");
    }

    #[test]
    fn rejected_requests_are_failures() {
        assert!(check_status(reqwest::StatusCode::ACCEPTED).is_ok());
        for code in [200, 302, 400, 401, 403, 429, 500] {
            assert!(check_status(reqwest::StatusCode::from_u16(code).unwrap()).is_err());
        }
    }
}
