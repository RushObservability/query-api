use super::*;

const NOW: i64 = 1_790_000_000_000;
const HOUR: i64 = 3_600_000;

fn config() -> ForecastConfig {
    ForecastConfig {
        metric_name: "system.filesystem.utilization".into(),
        resource: ForecastResource {
            service: "storage".into(),
            attributes: BTreeMap::from([
                ("mountpoint".into(), "/data".into()),
                ("state".into(), "used".into()),
            ]),
            resource_attributes: BTreeMap::from([("host.name".into(), "db-a".into())]),
        },
        input_unit: InputUnit::Ratio,
        horizon_secs: 86_400,
    }
}

#[test]
fn linear_forecast_projects_crossing_and_handles_no_data_and_recovery() {
    let mut cfg = config();
    let samples = || {
        (0..=24)
            .map(|i| GaugeSample {
                timestamp_ms: NOW - (24 - i) * HOUR,
                value: 0.66 + i as f64 * 0.005,
            })
            .collect::<Vec<_>>()
    };

    let result = forecast(&cfg, 89.0, NOW, samples());
    assert_eq!(result.crossing_at, Some(iso(NOW + 22 * HOUR)));
    assert!(result.reading().unwrap() >= 89.0);
    assert_eq!(result.history.len(), 25);

    cfg.horizon_secs = 6 * 3600;
    assert!(forecast(&cfg, 89.0, NOW, samples()).crossing_at.is_none());
    assert_eq!(
        forecast(&cfg, 78.0, NOW, samples()).crossing_at,
        Some(iso(NOW))
    );

    let mut falling = samples();
    for (i, sample) in falling.iter_mut().enumerate() {
        sample.value = 0.95 - i as f64 * 0.001;
    }
    let result = forecast(&cfg, 90.0, NOW, falling);
    assert!(result.projected_pct.unwrap() < result.current_pct.unwrap());
    assert_eq!(result.reading(), result.current_pct);

    let sparse = forecast(&cfg, 90.0, NOW, samples().into_iter().skip(20).collect());
    assert_eq!(sparse.no_data, Some(NoData::InsufficientHistory));
    assert!(sparse.reading().is_none() && sparse.projection.is_empty());

    let stale = forecast(&cfg, 90.0, NOW, samples().into_iter().take(24).collect());
    assert_eq!(stale.no_data, Some(NoData::Stale));

    let mut invalid = samples();
    invalid.last_mut().unwrap().value = 1.1;
    assert_eq!(
        forecast(&cfg, 90.0, NOW, invalid).no_data,
        Some(NoData::InvalidUnit)
    );
}

#[tokio::test]
#[ignore = "requires RUSH_TEST_CLICKHOUSE_URL and permission to create a disposable database"]
async fn exact_gauge_series_uses_real_timestamps_and_isolates_tenants() -> anyhow::Result<()> {
    let database = format!("test_forecast_{}", uuid::Uuid::new_v4().simple());
    let admin = Client::default()
        .with_url(std::env::var("RUSH_TEST_CLICKHOUSE_URL")?)
        .with_user(std::env::var("RUSH_TEST_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()))
        .with_password(std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default());
    admin
        .query(&format!("CREATE DATABASE {database}"))
        .execute()
        .await?;
    let result = verify_gauge_series(&admin.clone().with_database(&database)).await;
    let cleanup = admin
        .query(&format!("DROP DATABASE {database} SYNC"))
        .execute()
        .await;
    result?;
    cleanup?;
    Ok(())
}

async fn verify_gauge_series(client: &Client) -> anyhow::Result<()> {
    client.query(
        "CREATE TABLE metrics_gauge (
            tenant_id String, MetricName String, ServiceName String, TimeUnix DateTime64(9),
            Attributes Map(LowCardinality(String), String), ResourceAttributes Map(LowCardinality(String), String),
            Value Float64, Flags UInt32
        ) ENGINE = MergeTree ORDER BY (tenant_id, MetricName, ServiceName, TimeUnix)"
    ).execute().await?;
    let to = NOW + 30_000;
    client.query(
        "INSERT INTO metrics_gauge
        SELECT tenant, 'system.filesystem.utilization', 'storage', fromUnixTimestamp64Milli(? - toInt64(number) * ?),
               if(number % 2 = 0, map('state', 'used', 'mountpoint', '/data'), map('mountpoint', '/data', 'state', 'used')),
               map('host.name', host), if(host = 'db-a' AND tenant = 'alpha', 0.78 - number * 0.005, 0.99), 0
        FROM numbers(24) AS ticks CROSS JOIN (SELECT arrayJoin(['alpha', 'beta']) AS tenant) AS tenants
        CROSS JOIN (SELECT arrayJoin(['db-a', 'db-b']) AS host) AS hosts"
    ).bind(NOW).bind(HOUR).execute().await?;
    client.query(
        "INSERT INTO metrics_gauge SELECT 'alpha', 'system.filesystem.utilization', 'storage',
            fromUnixTimestamp64Milli(?), map('mountpoint', '/data', 'state', 'used'), map('host.name', 'db-a'), 0.99, 1"
    ).bind(to).execute().await?;
    let cfg = config();
    let choices = resources(client, "alpha", &cfg.metric_name, "DB-A", to).await?;
    assert_eq!(choices.resources, vec![cfg.resource.clone()]);
    let choices = resources(client, "alpha", &cfg.metric_name, "", to).await?;
    assert_eq!(choices.resources.len(), 2);

    let result = evaluate(client, "alpha", &cfg, 89.0, to).await?;
    assert_eq!(result.history.len(), 24);
    assert_eq!(result.history.last().unwrap().timestamp, iso(NOW));
    assert_eq!(result.current_pct, Some(78.0));
    assert_eq!(result.crossing_at, Some(iso(to + 22 * HOUR)));
    assert!(result.reading().unwrap() >= 89.0);

    let other_tenant = evaluate(client, "beta", &cfg, 89.0, to).await?;
    assert_eq!(other_tenant.current_pct, Some(99.0));
    assert_eq!(other_tenant.crossing_at, Some(iso(to)));
    let absent = evaluate(client, "unrelated", &cfg, 89.0, to).await?;
    assert!(absent.reading().is_none());
    Ok(())
}
