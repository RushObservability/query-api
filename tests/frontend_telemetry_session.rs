//! Exercises the real binary, session cookies and production middleware against a
//! disposable ClickHouse supplied through RUSH_TEST_CLICKHOUSE_URL. Never use a
//! development/production database: the API bootstraps its test admin and schema.
use anyhow::{Context, Result};
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
struct ApiProcess {
    child: Child,
    root: PathBuf,
}
impl Drop for ApiProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
async fn login(client: &Client, base: &str, user: &str, password: &str) -> Result<String> {
    let response = client
        .post(format!("{base}/api/v1/auth/login"))
        .header(header::ORIGIN, base)
        .json(&json!({"username":user,"password":password}))
        .send()
        .await?;
    anyhow::ensure!(
        response.status() == StatusCode::OK,
        "test login failed: {}",
        response.status()
    );
    Ok(response
        .headers()
        .get(header::SET_COOKIE)
        .context("session cookie")?
        .to_str()?
        .split(';')
        .next()
        .context("cookie value")?
        .into())
}
#[tokio::test]
#[ignore = "requires an empty disposable ClickHouse; starts the real API with isolated test-only secrets"]
async fn session_cookie_delivery_preserves_general_ingest_isolation() -> Result<()> {
    let clickhouse = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let target = url::Url::parse(&clickhouse)?;
    anyhow::ensure!(
        target.scheme() == "http"
            && target.host_str().is_some_and(|host| host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())),
        "test ClickHouse must be loopback HTTP"
    );
    let probe = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let tables = probe.post(&clickhouse).body("SELECT count() FROM system.tables WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')").send().await?.error_for_status()?.text().await?;
    anyhow::ensure!(
        tables.trim() == "0",
        "refusing to bootstrap a nonempty ClickHouse; run the disposable fixture script"
    );

    let root = std::env::temp_dir().join(format!("rush-frontend-auth-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root)?;
    let socket = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = socket.local_addr()?.port();
    drop(socket);
    let base = format!("http://127.0.0.1:{port}");
    let log = std::fs::File::create(root.join("api.log"))?;
    let password = format!("Example-Widget-{}!", uuid::Uuid::new_v4());
    let mut command = Command::new(env!("CARGO_BIN_EXE_rush-api"));
    command
        .current_dir(&root)
        .env_clear()
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log));
    for (key, value) in [
        ("RUSH_ENVIRONMENT", "test"),
        ("RUSH_INSECURE_COOKIES", "true"),
        ("RUSH_ALLOW_INSECURE_TENANT_READS", "true"),
        ("RUSH_SSO_REPLAY_STORE", "local"),
        ("RUSH_INGEST_BATCH_ROWS", "1"),
        ("RUSH_CLICKHOUSE_ASYNC_INSERT", "false"),
        ("RUSH_RUN_REPLAYER", "false"),
        ("RUSH_RUN_SLO_ENGINE", "false"),
        ("RUSH_RUN_ANOMALY_ENGINE", "false"),
        ("RUSH_RUN_MONITOR_ENGINE", "false"),
        ("RUSH_RUN_SIEM_ENGINE", "false"),
        ("RUSH_RUN_TELEMETRY_QUALITY_ENGINE", "false"),
        ("INITIAL_ADMIN_PASSWORD", password.as_str()),
        ("RUST_LOG", "warn"),
    ] {
        command.env(key, value);
    }
    for key in [
        "RUSH_API_KEY_SECRET",
        "RUSH_SESSION_HMAC_SECRET",
        "RUSH_AUDIT_HMAC_SECRET",
        "RUSH_CONFIG_ENCRYPTION_KEY",
        "RUSH_SSO_TRANSACTION_SECRET",
    ] {
        command.env(key, uuid::Uuid::new_v4().to_string());
    }
    command
        .env("CLICKHOUSE_URL", &clickhouse)
        .env("RUSH_PORT", port.to_string())
        .env("RUSH_BASE_URL", &base)
        .env("TMPDIR", &root);
    let mut api = ApiProcess {
        child: command.spawn()?,
        root,
    };
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Some(status) = api.child.try_wait()? {
            anyhow::bail!(
                "isolated API exited {status}: {}",
                std::fs::read_to_string(api.root.join("api.log"))?
            );
        }
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            break;
        }
        anyhow::ensure!(Instant::now() < deadline, "isolated API startup timeout");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let cookie = login(&client, &base, "admin", &password).await?;
    let payload = json!({"meta":{"app_name":"rush-ui","app_version":"1.2.3","environment":"test","page_path":"/logs/private-id?token=private-query","page_url":"https://private-host/logs?token=private-query","session_id":"private-session","user_id":"private-user"},"events":[{"event_type":"custom","event_name":"frontend_api_request","attributes":json!({"operation":"query","method":"POST","outcome":"success","count":1,"duration_ms_sum":42,"query":"private-query"}).to_string()}]});
    let request = |path: &str| {
        client
            .post(format!("{base}{path}"))
            .header(header::ORIGIN, &base)
            .header(header::COOKIE, &cookie)
    };
    for content_type in ["application/json", "text/plain;charset=UTF-8"] {
        let response = request("/api/v1/frontend/telemetry")
            .header(header::CONTENT_TYPE, content_type)
            .body(payload.to_string())
            .send()
            .await?;
        anyhow::ensure!(
            response.status() == StatusCode::OK,
            "frontend health request failed: {}",
            response.status()
        );
        assert_eq!(response.json::<Value>().await?["accepted"], 1);
    }
    assert_eq!(
        request("/api/v1/rum/ingest")
            .json(&payload)
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    for forged in ["", "rush_session=forged-session"] {
        assert_eq!(
            client
                .post(format!("{base}/api/v1/frontend/telemetry"))
                .header(header::ORIGIN, &base)
                .header(header::COOKIE, forged)
                .header("x-rush-credential-type", "session")
                .json(&payload)
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    for content_type in ["application/json", "text/plain;charset=UTF-8"] {
        for origin in [Some("https://attacker.invalid"), None] {
            let mut forged = client
                .post(format!("{base}/api/v1/frontend/telemetry"))
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, content_type)
                .body(payload.to_string());
            if let Some(origin) = origin {
                forged = forged.header(header::ORIGIN, origin);
            }
            assert_eq!(forged.send().await?.status(), StatusCode::FORBIDDEN);
        }
    }
    let other = request("/api/v1/tenants")
        .json(&json!({"name":"frontend-other"}))
        .send()
        .await?;
    anyhow::ensure!(other.status().is_success(), "create isolated tenant failed");
    assert_eq!(
        request("/api/v1/frontend/telemetry")
            .header("x-rush-tenant", "frontend-other")
            .json(&payload)
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request("/api/v1/frontend/telemetry")
            .header(header::CONTENT_TYPE, "application/json")
            .body("x".repeat(64 * 1024 + 1))
            .send()
            .await?
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    anyhow::ensure!(
        client
            .put(format!("{base}/api/v1/groups/viewers/tenants"))
            .header(header::ORIGIN, &base)
            .header(header::COOKIE, &cookie)
            .json(&json!({"tenant_ids":["default"]}))
            .send()
            .await?
            .status()
            .is_success(),
        "grant isolated viewer default-tenant access failed"
    );
    let viewer_password = format!("Example-Viewer-{}!", uuid::Uuid::new_v4());
    let viewer = request("/api/v1/users")
        .json(&json!({"username":"frontendviewer", "password":viewer_password}))
        .send()
        .await?;
    anyhow::ensure!(
        viewer.status().is_success(),
        "create isolated viewer failed: {}",
        viewer.status()
    );
    let viewer_cookie = login(&client, &base, "frontendviewer", &viewer_password).await?;
    let viewer_request = || {
        client
            .post(format!("{base}/api/v1/frontend/telemetry"))
            .header(header::ORIGIN, &base)
            .header(header::COOKIE, &viewer_cookie)
            .json(&payload)
    };
    assert_eq!(viewer_request().send().await?.status(), StatusCode::OK);
    assert_eq!(
        viewer_request()
            .header("x-rush-tenant", "frontend-other")
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .post(format!("{base}/api/v1/rum/ingest"))
            .header(header::ORIGIN, &base)
            .header(header::COOKIE, &viewer_cookie)
            .json(&payload)
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    let rum_setting = |enabled: bool| {
        client
            .put(format!("{base}/api/v1/settings/rum"))
            .header(header::ORIGIN, &base)
            .header(header::COOKIE, &cookie)
            .json(&json!({"enabled":enabled}))
    };
    anyhow::ensure!(
        rum_setting(false).send().await?.status().is_success(),
        "disable isolated RUM failed"
    );
    assert_eq!(
        viewer_request().send().await?.status(),
        StatusCode::FORBIDDEN
    );
    anyhow::ensure!(
        rum_setting(true).send().await?.status().is_success(),
        "restore isolated RUM failed"
    );
    let rum_signal = |enabled: bool| {
        client
            .put(format!("{base}/api/v1/tenants/default/signals"))
            .header(header::ORIGIN, &base)
            .header(header::COOKIE, &cookie)
            .json(&json!({"rum":enabled}))
    };
    anyhow::ensure!(
        rum_signal(false).send().await?.status().is_success(),
        "disable isolated tenant RUM signal failed"
    );
    assert_eq!(viewer_request().send().await?.status(), StatusCode::OK);
    anyhow::ensure!(
        rum_signal(true).send().await?.status().is_success(),
        "restore isolated tenant RUM signal failed"
    );
    // Synchronous insertion lets this confirm actual sanitized delivery, not just HTTP acceptance.
    let rows=client.post(&clickhouse).body("SELECT tenant_id, AppName, AppVersion, Environment, PagePath, PageUrl, SessionId, UserId, EventName, Attributes FROM observability.rum FORMAT JSONEachRow").send().await?.error_for_status()?.text().await?;
    assert!(!rows.contains("private-"), "raw private data reached RUM");
    let rows = rows
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(rows.len(), 3);
    for row in rows {
        assert_eq!(row["tenant_id"], "default");
        assert_eq!(row["AppName"], "rush-ui");
        assert_eq!(row["AppVersion"], "1.2.3");
        assert_eq!(row["Environment"], "test");
        assert_eq!(row["PagePath"], "/logs");
        let attrs: Value = serde_json::from_str(row["Attributes"].as_str().unwrap())?;
        assert_eq!(attrs["count"], 1);
        assert_eq!(attrs["duration_ms_sum"], 42);
    }
    // Logging out revokes the exact cookie used by the next telemetry request.
    anyhow::ensure!(
        request("/api/v1/auth/logout")
            .send()
            .await?
            .status()
            .is_success(),
        "logout failed"
    );
    assert_eq!(
        request("/api/v1/frontend/telemetry")
            .json(&payload)
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}
