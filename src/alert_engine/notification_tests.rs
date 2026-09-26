//! Wire-level regression tests for every channel. All destinations are local mocks.
//! Run with: cargo test --lib alert_engine::notification_tests

use super::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, StatusCode},
    response::IntoResponse,
};
use serde_json::{Value, json};
use std::{sync::Mutex, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    task::JoinHandle,
};

#[derive(Clone)]
pub(super) struct MockTransport {
    pub client: reqwest::Client,
    pub url: String,
    pub destinations: Arc<Mutex<Vec<(String, bool)>>>,
}

tokio::task_local! {
    pub(super) static HTTP_MOCK: MockTransport;
}

#[derive(Debug)]
struct Captured {
    method: axum::http::Method,
    headers: axum::http::HeaderMap,
    body: Value,
}

#[derive(Clone)]
struct Reply {
    status: StatusCode,
    body: &'static str,
    delay: Duration,
    captured: Arc<Mutex<Vec<Captured>>>,
}

async fn capture(State(reply): State<Reply>, request: Request<Body>) -> impl IntoResponse {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 1024 * 1024).await.unwrap();
    reply.captured.lock().unwrap().push(Captured {
        method: parts.method,
        headers: parts.headers,
        body: serde_json::from_slice(&body).unwrap(),
    });
    tokio::time::sleep(reply.delay).await;
    // A redirect target pointing back here makes accidental redirect following visible.
    (reply.status, [("location", "/redirected")], reply.body)
}

struct MockHttp {
    transport: MockTransport,
    captured: Arc<Mutex<Vec<Captured>>>,
    server: JoinHandle<()>,
}

impl MockHttp {
    async fn new(status: u16, body: &'static str, delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let captured = Arc::new(Mutex::new(vec![]));
        let reply = Reply {
            status: StatusCode::from_u16(status).unwrap(),
            body,
            delay,
            captured: captured.clone(),
        };
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().fallback(capture).with_state(reply))
                .await
                .unwrap();
        });
        Self {
            transport: MockTransport {
                client: reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(Duration::from_secs(1))
                    .build()
                    .unwrap(),
                url,
                destinations: Arc::new(Mutex::new(vec![])),
            },
            captured,
            server,
        }
    }

    async fn send(&self, kind: &str, config: Value, state: &str) -> Result<(), String> {
        HTTP_MOCK
            .scope(self.transport.clone(), notify(kind, config, state, None))
            .await
    }
}

impl Drop for MockHttp {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn smtp_config() -> SmtpConfig {
    SmtpConfig {
        host: None,
        port: 25,
        user: None,
        pass: None,
        from: "rush@example.invalid".into(),
    }
}

async fn notify(
    kind: &str,
    config: Value,
    state: &str,
    smtp: Option<AsyncSmtpTransport<Tokio1Executor>>,
) -> Result<(), String> {
    let channel = crate::models::alert::NotificationChannel {
        id: "channel-1".into(),
        tenant_id: "tenant-1".into(),
        name: "On-call".into(),
        channel_type: kind.into(),
        config: config.to_string(),
        enabled: true,
        created_at: "".into(),
    };
    send_channel_notification(
        &channel,
        "CPU above threshold",
        "High CPU",
        state,
        95.5,
        90.0,
        "metrics",
        ">",
        "Investigate CPU usage",
        "rule-1",
        "https://example.invalid/runbook",
        &reqwest::Client::new(),
        &smtp_config(),
        &smtp,
    )
    .await
}

fn channels() -> Vec<(&'static str, Value, &'static str, bool)> {
    vec![
        (
            "slack",
            json!({"webhook_url": "https://hooks.slack.com/services/test-secret"}),
            "https://hooks.slack.com/services/test-secret",
            false,
        ),
        (
            "slack_app",
            json!({"token": "test-secret", "channel": "C01234", "username": "Rush On-call"}),
            "https://slack.com/api/chat.postMessage",
            true,
        ),
        (
            "discord",
            json!({"webhook_url": "https://discord.com/api/webhooks/test-secret"}),
            "https://discord.com/api/webhooks/test-secret",
            false,
        ),
        (
            "webhook",
            json!({"url": "https://example.invalid/test-secret", "method": "PUT", "headers": {"Authorization": "Bearer test-secret", "X-Rush": "test"}}),
            "https://example.invalid/test-secret",
            false,
        ),
        (
            "alertmanager",
            json!({"url": "https://example.invalid/alertmanager/", "labels": {"team": "platform", "severity": "warning"}}),
            "https://example.invalid/alertmanager/api/v2/alerts",
            false,
        ),
        (
            "rootly",
            json!({"url": rootly::WEBHOOK_URL, "token": "test-secret"}),
            rootly::WEBHOOK_URL,
            true,
        ),
        (
            "pagerduty",
            json!({"routing_key": "test-secret", "region": "eu", "severity": "warning"}),
            "https://events.eu.pagerduty.com/v2/enqueue",
            true,
        ),
    ]
}

#[tokio::test]
async fn every_http_channel_sends_its_documented_payload_and_credentials() {
    for (kind, config, destination, strict) in channels() {
        let mock = MockHttp::new(
            if kind == "pagerduty" {
                202
            } else if kind == "discord" {
                204
            } else {
                200
            },
            "{\"ok\":true}",
            Duration::ZERO,
        )
        .await;
        mock.send(kind, config, "FIRING")
            .await
            .unwrap_or_else(|e| panic!("{kind}: {e}"));
        assert_eq!(
            *mock.transport.destinations.lock().unwrap(),
            [(destination.to_string(), strict)]
        );
        let requests = mock.captured.lock().unwrap();
        assert_eq!(requests.len(), 1, "{kind}");
        let request = &requests[0];
        assert_eq!(
            request.method,
            if kind == "webhook" { "PUT" } else { "POST" }
        );
        assert_eq!(request.headers["content-type"], "application/json");
        let body = &request.body;
        match kind {
            "slack" | "slack_app" => {
                let attachment = &body["attachments"][0];
                assert_eq!(attachment["color"], "#E53E3E");
                assert_eq!(attachment["text"], "Investigate CPU usage");
                assert_eq!(attachment["fields"][1]["value"], "95.50");
                let actions = attachment["actions"].as_array().unwrap();
                assert!(actions.iter().all(|a| a["text"].is_string()));
                assert!(
                    actions
                        .iter()
                        .any(|a| a["url"] == "https://example.invalid/runbook")
                );
                if kind == "slack_app" {
                    assert_eq!(request.headers["authorization"], "Bearer test-secret");
                    assert_eq!(body["channel"], "C01234");
                    assert_eq!(body["username"], "Rush On-call");
                }
            }
            "webhook" => {
                assert_eq!(request.headers["authorization"], "Bearer test-secret");
                assert_eq!(request.headers["x-rush"], "test");
                assert_eq!(
                    *body,
                    json!({"alert": "High CPU", "state": "FIRING", "value": 95.5, "threshold": 90.0, "message": "CPU above threshold"})
                );
            }
            "discord" => {
                assert_eq!(body["embeds"][0]["color"], 0xED4245);
                assert_eq!(body["embeds"][0]["description"], "CPU above threshold");
                assert_eq!(body["embeds"][0]["fields"][0]["value"], "95.5");
            }
            "alertmanager" => {
                assert_eq!(
                    body[0]["labels"],
                    json!({"alertname": "High CPU", "severity": "warning", "team": "platform"})
                );
                assert_eq!(body[0]["annotations"]["summary"], "CPU above threshold");
                assert!(body[0].get("endsAt").is_none());
            }
            "rootly" => {
                assert_eq!(request.headers["authorization"], "Bearer test-secret");
                assert_eq!(body["state"], "triggered");
                assert_eq!(body["external_id"], "rush:tenant-1:metrics:rule-1");
                assert_eq!(body["value"], 95.5);
                assert!(body.get("token").is_none());
            }
            "pagerduty" => {
                assert_eq!(body["routing_key"], "test-secret");
                assert_eq!(body["event_action"], "trigger");
                assert_eq!(body["payload"]["source"], "rush-observability");
                assert_eq!(body["payload"]["severity"], "warning");
                assert_eq!(body["payload"]["custom_details"]["value"], 95.5);
                assert!(!request.headers.contains_key("authorization"));
            }
            _ => unreachable!(),
        }
        if kind != "pagerduty" {
            assert!(!body.to_string().contains("test-secret"));
        }
    }
}

#[tokio::test]
async fn every_http_channel_reports_rejection_without_echoing_secrets_or_following_redirects() {
    for code in [302, 400, 401, 403, 429, 500] {
        for (kind, config, _, _) in channels() {
            let mock = MockHttp::new(code, "test-secret", Duration::ZERO).await;
            let error = mock.send(kind, config, "FIRING").await.expect_err(kind);
            assert!(error.contains(&code.to_string()), "{kind}: {error}");
            assert!(!error.contains("test-secret"), "{kind}");
            assert_eq!(
                mock.captured.lock().unwrap().len(),
                1,
                "redirect followed: {kind}"
            );
        }
    }
}

#[tokio::test]
async fn every_http_channel_reports_timeouts_without_leaking_credentials() {
    for (kind, config, _, _) in channels() {
        let mock = MockHttp::new(202, "{\"ok\":true}", Duration::from_secs(2)).await;
        let error = mock.send(kind, config, "FIRING").await.expect_err(kind);
        assert!(error.contains("delivery failed"), "{kind}: {error}");
        assert!(!error.contains("test-secret"));
    }
}

#[tokio::test]
async fn every_http_channel_reports_connection_failures() {
    let mut mock = MockHttp::new(200, "{}", Duration::ZERO).await;
    mock.server.abort();
    let _ = (&mut mock.server).await;
    for (kind, config, _, _) in channels() {
        let error = mock.send(kind, config, "FIRING").await.expect_err(kind);
        assert!(error.contains("delivery failed"), "{kind}: {error}");
        assert!(!error.contains("test-secret"));
    }
    assert!(mock.captured.lock().unwrap().is_empty());
}

#[tokio::test]
async fn recovery_payloads_and_identity_are_preserved_for_every_http_channel() {
    for (kind, config, _, _) in channels() {
        let mock = MockHttp::new(
            if kind == "pagerduty" { 202 } else { 200 },
            "{\"ok\":true}",
            Duration::ZERO,
        )
        .await;
        mock.send(kind, config.clone(), "FIRING").await.unwrap();
        for state in ["RESOLVED", "ok", "resolved", "OK"] {
            mock.send(kind, config.clone(), state).await.unwrap();
        }
        let requests = mock.captured.lock().unwrap();
        let fired = &requests[0].body;
        for request in &requests[1..] {
            let body = &request.body;
            match kind {
                "slack" | "slack_app" => assert_eq!(body["attachments"][0]["color"], "#38A169"),
                "discord" => assert_eq!(body["embeds"][0]["color"], 0x57F287),
                "webhook" => assert!(matches!(
                    body["state"].as_str(),
                    Some("RESOLVED" | "ok" | "resolved" | "OK")
                )),
                "alertmanager" => {
                    assert_eq!(body[0]["labels"], fired[0]["labels"]);
                    let ends =
                        chrono::DateTime::parse_from_rfc3339(body[0]["endsAt"].as_str().unwrap())
                            .unwrap();
                    assert!(ends <= chrono::Utc::now());
                    assert!(body[0].get("status").is_none());
                }
                "rootly" => {
                    assert_eq!(body["state"], "resolved");
                    assert_eq!(body["external_id"], fired["external_id"]);
                }
                "pagerduty" => {
                    assert_eq!(body["event_action"], "resolve");
                    assert_eq!(body["dedup_key"], fired["dedup_key"]);
                }
                _ => unreachable!(),
            }
        }
    }
}

#[tokio::test]
async fn slack_app_checks_application_errors_and_malformed_success_responses() {
    for body in [
        "{\"ok\":false,\"error\":\"test-secret\"}",
        "{}",
        "{\"ok\":\"true\"}",
        "not-json",
    ] {
        let mock = MockHttp::new(200, body, Duration::ZERO).await;
        let error = mock
            .send(
                "slack_app",
                json!({"token": "test-secret", "channel": "C1"}),
                "TEST",
            )
            .await
            .unwrap_err();
        assert!(!error.contains("test-secret"));
    }
}

#[tokio::test]
async fn legacy_config_aliases_and_defaults_keep_working() {
    let mock = MockHttp::new(200, "{\"ok\":true}", Duration::ZERO).await;
    mock.send(
        "slack",
        json!({"url": "https://example.invalid/legacy"}),
        "TEST",
    )
    .await
    .unwrap();
    mock.send(
        "webhook",
        json!({"url": "https://example.invalid/hook"}),
        "TEST",
    )
    .await
    .unwrap();
    mock.send(
        "slack_app",
        json!({"token": "test-secret", "channel": "C1"}),
        "TEST",
    )
    .await
    .unwrap();
    let requests = mock.captured.lock().unwrap();
    assert_eq!(requests[0].body["attachments"][0]["color"], "#888888");
    assert_eq!(requests[0].body["attachments"][0]["fields"], json!([]));
    assert_eq!(requests[1].method, "POST");
    assert_eq!(requests[2].body["username"], "Rush Alerts");
}

#[tokio::test]
async fn missing_config_and_unknown_channels_fail_before_delivery() {
    let mock = MockHttp::new(200, "{}", Duration::ZERO).await;
    for kind in [
        "slack",
        "slack_app",
        "discord",
        "webhook",
        "alertmanager",
        "rootly",
        "pagerduty",
        "email",
        "unknown",
    ] {
        assert!(mock.send(kind, json!({}), "TEST").await.is_err(), "{kind}");
    }
    assert!(mock.transport.destinations.lock().unwrap().is_empty());
    assert!(mock.captured.lock().unwrap().is_empty());
}

struct MockSmtp {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    messages: Arc<Mutex<Vec<String>>>,
    recipients: Arc<Mutex<Vec<String>>>,
    server: JoinHandle<()>,
}

impl MockSmtp {
    async fn new(reject: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let transport = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous("127.0.0.1")
            .port(port)
            .timeout(Some(Duration::from_secs(2)))
            .build();
        let messages = Arc::new(Mutex::new(vec![]));
        let recipients = Arc::new(Mutex::new(vec![]));
        let captured_messages = messages.clone();
        let captured_recipients = recipients.clone();
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let captured_messages = captured_messages.clone();
                let captured_recipients = captured_recipients.clone();
                connections.spawn(async move {
                    let (reader, mut writer) = stream.into_split();
                    writer
                        .write_all(b"220 localhost test SMTP\r\n")
                        .await
                        .unwrap();
                    let mut lines = BufReader::new(reader).lines();
                    let mut data = false;
                    let mut message = String::new();
                    while let Some(line) = lines.next_line().await.unwrap() {
                        let reply = if data {
                            if line == "." {
                                data = false;
                                captured_messages
                                    .lock()
                                    .unwrap()
                                    .push(std::mem::take(&mut message));
                                "250 queued\r\n"
                            } else {
                                message.push_str(&line);
                                message.push('\n');
                                continue;
                            }
                        } else if line.starts_with("EHLO") || line.starts_with("HELO") {
                            "250 localhost\r\n"
                        } else if line.starts_with("RCPT TO:") {
                            captured_recipients.lock().unwrap().push(line);
                            if reject {
                                "550 recipient rejected\r\n"
                            } else {
                                "250 ok\r\n"
                            }
                        } else if line == "DATA" {
                            data = true;
                            "354 send message\r\n"
                        } else if line == "QUIT" {
                            writer.write_all(b"221 bye\r\n").await.unwrap();
                            break;
                        } else {
                            "250 ok\r\n"
                        };
                        if writer.write_all(reply.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        Self {
            transport,
            messages,
            recipients,
            server,
        }
    }
}

impl Drop for MockSmtp {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn email_sends_plain_text_to_each_recipient_and_supports_legacy_to() {
    let smtp = MockSmtp::new(false).await;
    for key in ["recipients", "to"] {
        notify(
            "email",
            json!({key: " alice@example.invalid, bob@example.invalid "}),
            "FIRING",
            Some(smtp.transport.clone()),
        )
        .await
        .unwrap();
    }
    let messages = smtp.messages.lock().unwrap();
    assert_eq!(messages.len(), 4);
    for message in messages.iter() {
        assert!(message.contains("From: rush@example.invalid"));
        assert!(message.contains("Subject: [Rush Alert] High CPU - FIRING"));
        assert!(message.contains("Content-Type: text/plain"));
        assert!(message.contains("CPU above threshold"));
    }
    assert_eq!(smtp.recipients.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn email_fails_for_missing_smtp_rejections_and_invalid_recipients() {
    assert!(
        notify(
            "email",
            json!({"recipients": "a@example.invalid"}),
            "TEST",
            None
        )
        .await
        .unwrap_err()
        .contains("SMTP not set up")
    );
    let smtp = MockSmtp::new(true).await;
    assert!(
        notify(
            "email",
            json!({"recipients": "a@example.invalid"}),
            "TEST",
            Some(smtp.transport.clone())
        )
        .await
        .is_err()
    );
    assert!(smtp.messages.lock().unwrap().is_empty());
    for recipients in ["", " , ", "a@example.invalid, not-an-address"] {
        assert!(
            notify(
                "email",
                json!({"recipients": recipients}),
                "TEST",
                Some(smtp.transport.clone())
            )
            .await
            .is_err()
        );
    }
    assert_eq!(smtp.recipients.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn webhook_fields_extend_and_override_the_generic_payload() {
    let http = MockHttp::new(200, "ok", Duration::ZERO).await;
    let channel = crate::models::alert::NotificationChannel {
        id: "channel-1".into(),
        tenant_id: "tenant-1".into(),
        name: "SLO hook".into(),
        channel_type: "webhook".into(),
        config: json!({"url": "https://example.invalid/hook"}).to_string(),
        enabled: true,
        created_at: "".into(),
    };
    let fields = json!({"slo": "Checkout availability", "state": "breaching", "error_count": 42.0});
    HTTP_MOCK
        .scope(
            http.transport.clone(),
            send_channel_notification_with_fields(
                &channel,
                "SLO 'Checkout availability': BREACHING",
                "Checkout availability",
                "alert",
                -12.5,
                0.0,
                "slo",
                "",
                "",
                "slo-1",
                "",
                Some(&fields),
                &reqwest::Client::new(),
                &smtp_config(),
                &None,
            ),
        )
        .await
        .unwrap();
    let body = http.captured.lock().unwrap()[0].body.clone();
    assert_eq!(body["alert"], "Checkout availability");
    assert_eq!(body["value"], -12.5);
    assert_eq!(body["slo"], "Checkout availability");
    assert_eq!(body["error_count"], 42.0);
    assert_eq!(
        body["state"], "breaching",
        "source fields win, so old receivers keep working"
    );
}
