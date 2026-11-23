use std::sync::atomic::Ordering;

use clickhouse::Client;

use super::*;
use crate::handlers::rum::{RumPageRow, RumVitalRow};

mod benchmark;

fn request(from: i64, to: i64, filters: Vec<Filter>) -> RumQueryRequest {
    RumQueryRequest {
        time_range: crate::models::query::TimeRange {
            from: chrono::DateTime::from_timestamp(from, 0)
                .unwrap()
                .to_rfc3339(),
            to: chrono::DateTime::from_timestamp(to, 0)
                .unwrap()
                .to_rfc3339(),
        },
        filters,
        limit: 100,
        offset: 0,
    }
}

fn ready() -> RumRollups {
    let ready = RumRollups::default();
    ready.pages.store(true, Ordering::Release);
    ready.vitals.store(true, Ordering::Release);
    ready
}

fn filter(field: &str, op: FilterOp, value: serde_json::Value) -> Filter {
    Filter {
        field: field.into(),
        op,
        value,
    }
}

#[test]
fn only_complete_buckets_enter_rollups() {
    let base = 1_800_000_000;
    let req = request(base + 17, base + 8 * 3600 + 23, vec![]);
    assert_eq!(
        full_bucket_window(&req, Kind::Pages),
        Some((base + 300, base + 8 * 3600))
    );
    assert_eq!(full_bucket_window(&req, Kind::Vitals), None);
    let long = request(base + 17, base + 32 * 3600 + 23, vec![]);
    assert_eq!(
        full_bucket_window(&long, Kind::Vitals),
        Some((base + 3600, base + 32 * 3600))
    );
    let req = request(base, base + 6 * 3600, vec![]);
    assert_eq!(
        full_bucket_window(&req, Kind::Pages),
        Some((base, base + 6 * 3600))
    );
    assert!(full_bucket_window(&request(base, base + 3599, vec![]), Kind::Pages).is_none());
    assert!(full_bucket_window(&request(base + 1, base, vec![]), Kind::Pages).is_none());
}

#[test]
fn missing_readiness_and_unsupported_filters_keep_raw_queries() {
    let req = request(1_800_000_000, 1_800_086_400, vec![]);
    for kind in [Kind::Pages, Kind::Vitals] {
        assert!(!RumRollups::default().plan(kind, &req, "tenant").uses_rollup);
        assert!(ready().plan(kind, &req, "tenant").uses_rollup);
        for field in ["SessionId", "BrowserName", "ErrorMessage", "unknown"] {
            let req = request(
                1_800_000_000,
                1_800_086_400,
                vec![filter(field, FilterOp::Eq, serde_json::json!("x"))],
            );
            assert!(!ready().plan(kind, &req, "tenant").uses_rollup);
        }
    }
}

#[tokio::test]
async fn failed_rollup_retries_raw_once_and_remembers_the_failure() {
    use clickhouse::test::{Mock, handlers};

    let mock = Mock::new();
    let client = Client::default()
        .with_url(mock.url())
        .with_validation(false);
    let rollups = ready();
    let req = request(1_800_000_000, 1_800_086_400, vec![]);
    let (from, to) = full_bucket_window(&req, Kind::Pages).unwrap();
    let density = density_sql(Kind::Pages, "a", &filters(Kind::Pages, &req), from, to);
    rollups.remember_hint(Sha256::digest(density.as_bytes()).into(), true);
    assert!(
        rollups
            .select(Kind::Pages, &req, "a", &client)
            .await
            .uses_rollup
    );

    mock.add(handlers::failure(
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    ));
    mock.add(handlers::provide([RumPageRow {
        page_path: "/home".into(),
        views: 3,
        unique_sessions: 2,
        avg_load_ms: 150.0,
        error_count: 1,
    }]));
    let rows = rollups
        .fetch_all::<RumPageRow>(Kind::Pages, &req, "a", &client)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].views, 3);
    assert!(
        !rollups
            .select(Kind::Pages, &req, "a", &client)
            .await
            .uses_rollup
    );

    // A failing raw query is returned directly, without an unbounded retry.
    mock.add(handlers::failure(
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    ));
    assert!(
        rollups
            .fetch_all::<RumPageRow>(Kind::Pages, &req, "a", &client)
            .await
            .is_err()
    );
}

fn admin() -> anyhow::Result<Client> {
    Ok(Client::default()
        .with_url(std::env::var("RUSH_TEST_CLICKHOUSE_URL")?)
        .with_user(std::env::var("RUSH_TEST_CLICKHOUSE_USER").unwrap_or("default".into()))
        .with_password(std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default()))
}

fn raw_ddl(database: &str, table: &str) -> anyhow::Result<String> {
    Ok(crate::migrations::MIGRATIONS
        .iter()
        .find(|sql| sql.starts_with("CREATE TABLE IF NOT EXISTS observability.rum\n"))
        .ok_or_else(|| anyhow::anyhow!("missing RUM schema"))?
        .replace("observability.rum", &format!("{database}.{table}")))
}

async fn reader(admin: &Client, database: &str, user: &str) -> anyhow::Result<Client> {
    admin
        .query(&format!("CREATE USER {user} IDENTIFIED WITH no_password"))
        .execute()
        .await?;
    admin
        .query(&format!("GRANT SELECT ON {database}.rum TO {user}"))
        .execute()
        .await?;
    admin.query(&format!(
        "CREATE ROW POLICY isolation ON {database}.rum FOR SELECT \
         USING notEmpty(getSetting('rush_tenant_id')) AND tenant_id = getSetting('rush_tenant_id') TO {user}"
    )).execute().await?;
    Ok(Client::default()
        .with_url(std::env::var("RUSH_TEST_CLICKHOUSE_URL")?)
        .with_user(user)
        .with_database(database))
}

async fn fetch(
    client: &Client,
    kind: Kind,
    plan: &Plan,
    context: &str,
) -> anyhow::Result<serde_json::Value> {
    let query = plan
        .query(client, "a")
        .with_setting("rush_tenant_id", context)
        .with_setting("session_timezone", "Asia/Kolkata");
    Ok(match kind {
        Kind::Pages => {
            let mut rows = query.fetch_all::<RumPageRow>().await?;
            rows.sort_by(|a, b| a.page_path.cmp(&b.page_path));
            serde_json::to_value(rows)?
        }
        Kind::Vitals => serde_json::to_value(query.fetch_all::<RumVitalRow>().await?)?,
    })
}

async fn parity(client: &Client, req: &RumQueryRequest) -> anyhow::Result<()> {
    for kind in [Kind::Pages, Kind::Vitals] {
        let baseline = RumRollups::default().plan(kind, req, "a");
        let projected = ready().plan(kind, req, "a");
        let expected = fetch(client, kind, &baseline, "a").await?;
        let actual = fetch(client, kind, &projected, "a").await?;
        anyhow::ensure!(
            actual == expected,
            "{kind:?} results changed: {actual} != {expected}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires RUSH_TEST_CLICKHOUSE_URL, rush_ settings and database/user creation"]
async fn rollups_preserve_sessions_bounds_filters_and_retention() -> anyhow::Result<()> {
    let admin = admin()?;
    let id = uuid::Uuid::new_v4().simple().to_string();
    let database = format!("test_rum_rollups_{id}");
    let user = format!("test_rum_reader_{id}");
    admin
        .query(&format!("CREATE DATABASE {database}"))
        .execute()
        .await?;
    let result: anyhow::Result<()> = async {
        admin.query(&raw_ddl(&database, "rum")?).execute().await?;
        let base = chrono::Utc::now().timestamp() / 300 * 300 - 36 * 3600;
        // Boundaries, shared sessions across buckets, empty session IDs, varied
        // durations and all vital ratings. Neighbouring rows outside the exact
        // bounds must not enter through their aggregate bucket.
        admin.query(&format!(r#"INSERT INTO {database}.rum
            (tenant_id, Timestamp, AppName, Environment, PagePath, SessionId,
             EventType, DurationMs, VitalName, VitalValue, VitalRating, BrowserName)
            SELECT tenant, fromUnixTimestamp64Nano(({base} + offset) * 1000000000 + nanos),
                app, env, page, session, event, duration, vital, value, rating, browser
            FROM values('tenant String, offset Int64, nanos Int64, app String, env String,
                page String, session String, event String, duration Float64, vital String,
                value Float64, rating String, browser String',
              ('a',16,999999999,'shop','prod','/','outside-left','pageview',999,'LCP',999,'poor','Chrome'),
              ('a',17,0,'shop','prod','/','shared','pageview',100,'',0,'','Chrome'),
              ('a',299,999999999,'shop','prod','/','shared','web_vital',0,'LCP',10,'good','Chrome'),
              ('a',300,0,'shop','prod','/','shared','pageview',300,'',0,'','Chrome'),
              ('a',300,1,'shop','prod','/','interior','error',0,'',0,'','DeleteMe'),
              ('a',601,0,'shop','prod','/','interior','web_vital',0,'LCP',20,'needs-improvement','Chrome'),
              ('a',800,0,'shop','prod','/','','web_vital',0,'LCP',90,'poor','Chrome'),
              ('a',1000,0,'shop','prod','/empty','only-errors','error',0,'',0,'','Chrome'),
              ('a',1200,0,'shop','stage','/','stage','web_vital',0,'LCP',40,'other','Chrome'),
              ('a',1600,0,'shop','prod','/','inside-later','web_vital',0,'INP',5,'good','Chrome'),
              ('a',2000,0,'other','prod','/other','other-app','pageview',50,'',0,'','Firefox'),
              ('a',7200,0,'shop','prod','/','shared','web_vital',0,'LCP',50,'good','Chrome'),
              ('a',14000,0,'shop','prod','/','inside-later','web_vital',0,'INP',15,'good','Chrome'),
              ('a',115200,0,'shop','prod','/','shared','pageview',200,'',0,'','Chrome'),
              ('a',115223,0,'shop','prod','/','shared','web_vital',0,'LCP',30,'good','Chrome'),
              ('a',115223,1,'shop','prod','/','outside-right','pageview',999,'LCP',999,'poor','Chrome'),
              ('b',301,0,'shop','prod','/','other-tenant','web_vital',9999,'LCP',9999,'poor','Chrome')
            )"#)).execute().await?;
        let read = reader(&admin, &database, &user).await?;
        let req = request(base + 17, base + 115223, vec![filter("AppName", FilterOp::Eq, serde_json::json!("shop"))]);
        // A stale readiness bit is still safe: ClickHouse computes the interior
        // from raw when no projection exists or a part is not yet materialized.
        parity(&read, &req).await?;
        for kind in [Kind::Pages, Kind::Vitals] {
            storage::prepare(&admin, &database, kind).await?;
        }
        let mutations_before = admin.query("SELECT count() FROM system.mutations WHERE database = ? AND table = 'rum'")
            .bind(&database).fetch_one::<u64>().await?;
        for kind in [Kind::Pages, Kind::Vitals] {
            storage::prepare(&admin, &database, kind).await?;
        }
        let mutations_after = admin.query("SELECT count() FROM system.mutations WHERE database = ? AND table = 'rum'")
            .bind(&database).fetch_one::<u64>().await?;
        anyhow::ensure!(mutations_before == mutations_after, "idempotent startup rebuilt projections");
        parity(&read, &req).await?;
        let selector = ready();
        let sparse = selector.select(Kind::Vitals, &req, "a", &read).await;
        anyhow::ensure!(!sparse.uses_rollup, "sparse percentile reports should remain raw");
        let initial_vitals = fetch(&read, Kind::Vitals, &sparse, "a").await?;
        let pages = fetch(&read, Kind::Pages, &ready().plan(Kind::Pages, &req, "a"), "a").await?;
        let home = pages.as_array().unwrap().iter().find(|row| row["PagePath"] == "/").unwrap();
        anyhow::ensure!(home["views"] == 3 && home["unique_sessions"] == 5 && home["avg_load_ms"] == 200.0,
            "sessions or bounds were counted incorrectly: {home}");
        for kind in [Kind::Pages, Kind::Vitals] {
            let plan = ready().plan(kind, &req, "a");
            let explain = read.query(&format!("EXPLAIN indexes = 1 {}", plan.sql))
                .with_setting("rush_tenant_id", "a").fetch_all::<String>().await?;
            anyhow::ensure!(explain.iter().any(|line| line.contains(kind.name())), "{kind:?} rollup unused: {explain:?}");
            anyhow::ensure!(fetch(&read, kind, &plan, "b").await? == serde_json::json!([]), "cross-tenant state leaked");
            anyhow::ensure!(fetch(&read, kind, &plan, "").await? == serde_json::json!([]), "missing context leaked state");
        }
        for condition in [
            filter("environment", FilterOp::In, serde_json::json!(["prod", "stage"])),
            filter("Environment", FilterOp::Ne, serde_json::json!("stage")),
            filter("BrowserName", FilterOp::Eq, serde_json::json!("Chrome")),
            filter("AppName", FilterOp::Eq, serde_json::json!("quote'and?")),
            filter("PagePath", FilterOp::Like, serde_json::json!("/%")),
        ] {
            let mut filtered = request(base + 17, base + 115223, req.filters.clone());
            filtered.filters.push(condition);
            parity(&read, &filtered).await?;
        }
        parity(&read, &request(base, base + 21600, vec![])).await?;
        parity(&read, &request(base, base + 1800, vec![])).await?;
        parity(&read, &request(base + 172800, base + 259200, vec![])).await?;
        let mut fractional = request(base + 17, base + 115223, req.filters.clone());
        fractional.time_range.from = chrono::DateTime::from_timestamp(base + 17, 500_000_000).unwrap().to_rfc3339();
        fractional.time_range.to = chrono::DateTime::from_timestamp(base + 115223, 900_000_000).unwrap().to_rfc3339();
        parity(&read, &fractional).await?;
        let offset = chrono::FixedOffset::east_opt(19800).unwrap();
        let mut zoned = request(base + 17, base + 115223, req.filters.clone());
        zoned.time_range.from = chrono::DateTime::parse_from_rfc3339(&zoned.time_range.from)?.with_timezone(&offset).to_rfc3339();
        zoned.time_range.to = chrono::DateTime::parse_from_rfc3339(&zoned.time_range.to)?.with_timezone(&offset).to_rfc3339();
        parity(&read, &zoned).await?;
        // Late data lands in an old summary bucket without a separate-table race.
        admin.query(&format!("INSERT INTO {database}.rum (tenant_id, Timestamp, AppName, Environment, PagePath, SessionId, EventType, DurationMs) VALUES ('a', fromUnixTimestamp64Second({}), 'shop', 'prod', '/', 'late', 'pageview', 400)", base + 900)).execute().await?;
        admin.query(&format!("INSERT INTO {database}.rum (tenant_id, Timestamp, AppName, Environment, PagePath, SessionId, EventType, VitalName, VitalValue, VitalRating) VALUES ('a', fromUnixTimestamp64Second({}), 'shop', 'prod', '/', 'shared', 'web_vital', 'LCP', 120, 'poor')", base + 900)).execute().await?;
        let cached_hint = selector.select(Kind::Vitals, &req, "a", &read).await;
        let current_vitals = fetch(&read, Kind::Vitals, &cached_hint, "a").await?;
        anyhow::ensure!(current_vitals != initial_vitals, "cached planning hints must not cache result data");
        parity(&read, &req).await?;
        admin.query(&format!("OPTIMIZE TABLE {database}.rum FINAL")).execute().await?;
        parity(&read, &req).await?;
        admin.query(&format!("ALTER TABLE {database}.rum DELETE WHERE BrowserName = 'DeleteMe' SETTINGS mutations_sync = 1")).execute().await?;
        parity(&read, &req).await?;
        for kind in [Kind::Pages, Kind::Vitals] {
            let plan = ready().plan(kind, &req, "a");
            let explain = read.query(&format!("EXPLAIN indexes = 1 {}", plan.sql))
                .with_setting("rush_tenant_id", "a").fetch_all::<String>().await?;
            anyhow::ensure!(explain.iter().any(|line| line.contains(kind.name())), "retention removed the usable {kind:?} projection");
        }
        admin.query(&format!("TRUNCATE TABLE {database}.rum")).execute().await?;
        parity(&read, &req).await?;
        Ok(())
    }.await;
    let drop_db = admin
        .query(&format!("DROP DATABASE {database} SYNC"))
        .execute()
        .await;
    let drop_user = admin
        .query(&format!("DROP USER IF EXISTS {user}"))
        .execute()
        .await;
    result?;
    drop_db?;
    drop_user?;
    Ok(())
}
