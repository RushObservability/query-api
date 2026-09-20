//! Rootly Generic Webhook Alert Source (not the account REST API).
//! https://docs.rootly.com/integrations/generic-webhook-alert-source/generic-webhook-alert-source

use serde_json::{Value, json};

pub const WEBHOOK_URL: &str = "https://webhooks.rootly.com/webhooks/incoming/generic_webhooks";

pub fn monitor_alert_id(monitor_id: &str, group_key: &str) -> String {
    if group_key.is_empty() {
        monitor_id.to_string()
    } else {
        format!("{monitor_id}:group:{group_key}")
    }
}

pub fn validate_config(config: &Value) -> Result<(), String> {
    let raw = config.get("url").and_then(Value::as_str).unwrap_or("");
    let url = url::Url::parse(raw).map_err(|_| "Rootly requires a valid webhook URL")?;
    if url.scheme() != "https"
        || url.host_str() != Some("webhooks.rootly.com")
        || url.port_or_known_default() != Some(443)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("Use the HTTPS webhook URL from Rootly without query parameters. Enter the bearer secret separately.".into());
    }
    let path = url.path().trim_end_matches('/');
    let base = "/webhooks/incoming/generic_webhooks";
    let notify = path.strip_prefix(&format!("{base}/notify/"));
    let valid_notify = notify.is_some_and(|suffix| {
        let parts: Vec<_> = suffix.split('/').collect();
        parts.len() == 2
            && matches!(parts[0], "service" | "group" | "team" | "escalationPolicy")
            && !parts[1].is_empty()
            && parts[1]
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    });
    if path != base && !valid_notify {
        return Err(
            "Use Rootly's Generic Webhook URL, optionally including /notify/<type>/<id>.".into(),
        );
    }
    let token = config.get("token").and_then(Value::as_str).unwrap_or("");
    if token.trim().is_empty() || token != token.trim() {
        return Err("Rootly requires the Generic Webhook source's bearer secret".into());
    }
    auth_header(token)?;
    Ok(())
}

fn auth_header(token: &str) -> Result<reqwest::header::HeaderValue, String> {
    let mut header = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| "Rootly bearer secret contains invalid characters".to_string())?;
    header.set_sensitive(true);
    Ok(header)
}

pub fn payload(
    channel: &crate::models::alert::NotificationChannel,
    alert_id: &str,
    alert_name: &str,
    state: &str,
    message: &str,
    value: f64,
    threshold: f64,
    signal: &str,
    runbook_url: &str,
) -> Value {
    // Test deliveries cannot collide with a real rule, even one named "Test Alert".
    let external_id = if alert_id.is_empty() {
        format!("rush:test:{}:{}", channel.tenant_id, channel.id)
    } else {
        format!("rush:{}:{signal}:{alert_id}", channel.tenant_id)
    };
    json!({
        "title": alert_name,
        "description": message,
        "external_id": external_id,
        "state": if state.eq_ignore_ascii_case("ok") || state.eq_ignore_ascii_case("resolved") { "resolved" } else { "triggered" },
        "source": "rush-observability",
        "tenant_id": channel.tenant_id,
        "signal_type": signal,
        "value": value,
        "threshold": threshold,
        "runbook_url": runbook_url,
        "test": state.eq_ignore_ascii_case("test"),
    })
}

fn check_status(status: reqwest::StatusCode) -> Result<(), String> {
    if status.is_success() {
        Ok(())
    } else {
        // Do not echo remote bodies or request URLs into notification/audit logs.
        Err(format!(
            "Rootly rejected the notification (HTTP {})",
            status.as_u16()
        ))
    }
}

pub async fn send(config: &Value, payload: &Value) -> Result<(), String> {
    validate_config(config)?;
    let url = config["url"].as_str().unwrap_or(WEBHOOK_URL);
    let token = config["token"].as_str().unwrap_or_default();
    let response = super::delivery::request(reqwest::Method::POST, url, true)
        .await?
        .header(reqwest::header::AUTHORIZATION, auth_header(token)?)
        .json(payload)
        .send()
        .await
        .map_err(|_| "Rootly delivery failed; check connectivity and try again".to_string())?;
    check_status(response.status())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monitor_groups_have_independent_identities() {
        assert_ne!(
            monitor_alert_id("monitor-1", "host-a"),
            monitor_alert_id("monitor-1", "host-b")
        );
        assert_ne!(
            monitor_alert_id("monitor-1", "host-a"),
            monitor_alert_id("monitor-1", "")
        );
        assert_eq!(
            monitor_alert_id("monitor-1", "host-a"),
            monitor_alert_id("monitor-1", "host-a")
        );
    }

    #[test]
    fn allows_documented_endpoints_and_requires_a_secret() {
        assert!(validate_config(&json!({"url": WEBHOOK_URL, "token": "source-secret"})).is_ok());
        assert!(validate_config(&json!({"url": format!("{WEBHOOK_URL}/notify/escalationPolicy/abc-123"), "token": "secret"})).is_ok());
        for token in ["", "   ", "bad\r\nAuthorization: injection"] {
            assert!(validate_config(&json!({"url": WEBHOOK_URL, "token": token})).is_err());
        }
        for url in [
            "http://webhooks.rootly.com/webhooks/incoming/generic_webhooks",
            "https://webhooks.rootly.com.evil.example/webhooks/incoming/generic_webhooks",
            "https://127.0.0.1/",
            "https://api.rootly.com/v1/alerts",
            "https://webhooks.rootly.com/webhooks/incoming/generic_webhooks/notify/service",
        ] {
            assert!(validate_config(&json!({"url": url, "token": "secret"})).is_err());
        }
        assert!(
            validate_config(
                &json!({"url": format!("{WEBHOOK_URL}?secret=secret"), "token": "secret"})
            )
            .is_err()
        );
    }

    #[test]
    fn bearer_header_is_sensitive_and_rejections_are_not_success() {
        let header = auth_header("source-secret").unwrap();
        assert_eq!(header.to_str().unwrap(), "Bearer source-secret");
        assert!(header.is_sensitive());
        for code in [200, 202, 204] {
            assert!(check_status(reqwest::StatusCode::from_u16(code).unwrap()).is_ok());
        }
        for code in [302, 400, 401, 403, 429, 500] {
            assert!(check_status(reqwest::StatusCode::from_u16(code).unwrap()).is_err());
        }
    }

    #[test]
    fn recovery_uses_same_identity_and_renames_do_not_create_new_alerts() {
        let mut channel = crate::models::alert::NotificationChannel {
            id: "channel-1".into(),
            tenant_id: "tenant-a".into(),
            name: "Rootly".into(),
            channel_type: "rootly".into(),
            config: "{}".into(),
            enabled: true,
            created_at: "".into(),
        };
        let fire = payload(
            &channel,
            "rule-1",
            "High latency",
            "FIRING",
            "Latency exceeded",
            99.0,
            90.0,
            "metrics",
            "",
        );
        let recover = payload(
            &channel,
            "rule-1",
            "Renamed",
            "ok",
            "Recovered",
            20.0,
            90.0,
            "metrics",
            "",
        );
        assert_eq!(fire["external_id"], recover["external_id"]);
        assert_eq!(fire["state"], "triggered");
        assert_eq!(recover["state"], "resolved");
        assert_eq!(fire["title"], "High latency");
        let test = payload(&channel, "", "Test Alert", "TEST", "Test", 0.0, 0.0, "", "");
        assert_ne!(test["external_id"], fire["external_id"]);
        channel.tenant_id = "tenant-b".into();
        let other = payload(
            &channel,
            "rule-1",
            "High latency",
            "FIRING",
            "Test",
            0.0,
            0.0,
            "metrics",
            "",
        );
        assert_ne!(other["external_id"], fire["external_id"]);
    }
}
