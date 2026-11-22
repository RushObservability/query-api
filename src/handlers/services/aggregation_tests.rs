use super::*;

#[tokio::test]
#[ignore = "requires RUSH_TEST_CLICKHOUSE_URL and permission to create a disposable database"]
async fn combined_service_aggregates_preserve_results() -> anyhow::Result<()> {
    let database = format!("test_service_aggregates_{}", uuid::Uuid::new_v4().simple());
    let admin = clickhouse::Client::default()
        .with_url(std::env::var("RUSH_TEST_CLICKHOUSE_URL")?)
        .with_user(std::env::var("RUSH_TEST_CLICKHOUSE_USER").unwrap_or("default".into()))
        .with_password(std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default());
    admin
        .query(&format!("CREATE DATABASE {database}"))
        .execute()
        .await?;
    let client = admin.clone().with_database(&database);
    let result = check_aggregates(&client).await;
    // Clean up even when a query or a result check fails.
    let cleanup = admin
        .query(&format!("DROP DATABASE {database} SYNC"))
        .execute()
        .await;
    result?;
    cleanup?;
    Ok(())
}

async fn check_aggregates(client: &clickhouse::Client) -> anyhow::Result<()> {
    client
        .query(
            "CREATE TABLE spans (
                tenant_id String, timestamp DateTime64(9), service_name String,
                kind String, duration_ns UInt64, attributes String
             ) ENGINE = MergeTree ORDER BY (tenant_id, timestamp, service_name)",
        )
        .execute()
        .await?;
    // Anchor buckets in the preceding hour so they remain inside the two-hour
    // query window regardless of the current minute. Negative offsets are old data.
    client
        .query(
            r#"INSERT INTO spans
               SELECT tenant_id,
                   toStartOfHour(now()) - INTERVAL 1 HOUR + toIntervalMinute(bucket_offset),
                   service_name, kind, duration_ms * 1000000, attributes
               FROM values(
                   'tenant_id String, service_name String, bucket_offset Int32, kind String, duration_ms UInt64, attributes String',
                   ('a', 'checkout', 0, 'SPAN_KIND_SERVER', 100, '{}'),
                   ('a', 'checkout', 0, 'SPAN_KIND_SERVER', 300, '{}'),
                   ('a', 'checkout', 0, 'SPAN_KIND_CLIENT', 50, '{"db.system":"postgresql"}'),
                   ('a', 'checkout', 0, 'SPAN_KIND_CLIENT', 150, '{"db.system.name":"postgresql"}'),
                   ('a', 'checkout', 0, 'SPAN_KIND_INTERNAL', 5000, '{}'),
                   ('a', 'checkout', 1, 'SPAN_KIND_SERVER', 100, '{}'),
                   ('a', 'checkout', 1, 'SPAN_KIND_CLIENT', 250, '{"db.operation.name":"SELECT"}'),
                   ('a', 'checkout', 1, 'SPAN_KIND_CLIENT', 500, '{"db.operation":"SELECT"}'),
                   ('a', 'checkout', 2, 'SPAN_KIND_SERVER', 80, '{}'),
                   ('a', 'checkout', 3, 'SPAN_KIND_CLIENT', 500, '{"db.statement":"SELECT 1"}'),
                   ('a', 'checkout', 4, 'SPAN_KIND_INTERNAL', 500, '{}'),
                   ('a', 'checkout', 5, 'SPAN_KIND_SERVER', 200, '{"db.query.summary":"SELECT"}'),
                   ('a', 'checkout', 6, 'SPAN_KIND_SERVER', 60, '{}'),
                   ('a', 'checkout', 6, 'SPAN_KIND_CLIENT', 1, '{"db.system":"postgresql"}'),
                   ('a', 'checkout', 6, 'SPAN_KIND_CLIENT', 1, '{"db.system.name":"postgresql"}'),
                   ('a', 'checkout', 6, 'SPAN_KIND_CLIENT', 1, '{"db.operation.name":"SELECT"}'),
                   ('a', 'checkout', 6, 'SPAN_KIND_CLIENT', 1, '{"db.operation":"SELECT"}'),
                   ('a', 'checkout', 6, 'SPAN_KIND_CLIENT', 1, '{"db.statement":"SELECT 1"}'),
                   ('a', 'checkout', 6, 'SPAN_KIND_CLIENT', 1, '{"db.query.summary":"SELECT"}'),
                   ('b', 'checkout', 0, 'SPAN_KIND_SERVER', 1000, '{}'),
                   ('b', 'checkout', 0, 'SPAN_KIND_CLIENT', 25000, '{"db.system":"postgresql"}'),
                   ('a', 'payments', 0, 'SPAN_KIND_SERVER', 1000, '{}'),
                   ('a', 'payments', 0, 'SPAN_KIND_CLIENT', 25000, '{"db.system":"postgresql"}'),
                   ('a', 'checkout', -3000, 'SPAN_KIND_SERVER', 1000, '{}'),
                   ('a', 'checkout', -3000, 'SPAN_KIND_CLIENT', 25000, '{"db.system":"postgresql"}')
               )"#,
        )
        .execute()
        .await?;

    let buckets = time_breakdown_query(
        client,
        &time_breakdown_timeseries_sql("a", "checkout", 120, 60),
        "a",
    )
    .fetch_all::<ServiceTimeBreakdownAggregateBucket>()
    .await?
    .into_iter()
    .map(ServiceTimeBreakdownBucket::from)
    .collect::<Vec<_>>();
    let actual = buckets
        .iter()
        .map(|bucket| {
            (
                bucket.request_count,
                bucket.wall_time_ms,
                bucket.application_time_ms,
                bucket.database_time_ms,
                bucket.database_calls,
            )
        })
        .collect::<Vec<_>>();
    // Cover multiple requests, parallel calls, no database calls, a server span
    // that is also a database span, and every supported database attribute.
    // Database-only and unrelated internal-only buckets must stay absent.
    anyhow::ensure!(
        actual
            == vec![
                (2, 200.0, 100.0, 100.0, 2),
                (1, 100.0, 0.0, 100.0, 2),
                (1, 80.0, 80.0, 0.0, 0),
                (1, 200.0, 0.0, 200.0, 1),
                (1, 60.0, 54.0, 6.0, 6),
            ],
        "unexpected time breakdown: {actual:?}"
    );
    anyhow::ensure!(
        buckets
            .windows(2)
            .all(|pair| pair[0].bucket < pair[1].bucket),
        "buckets must be ordered"
    );
    let empty = time_breakdown_query(
        client,
        &time_breakdown_timeseries_sql("missing", "checkout", 120, 60),
        "missing",
    )
    .fetch_all::<ServiceTimeBreakdownAggregateBucket>()
    .await?;
    anyhow::ensure!(empty.is_empty(), "empty windows must have no buckets");

    // Below the reservoir limit, the shared and independent percentile states
    // must agree. Also exercise GROUP BY and the empty aggregate's NaN values.
    for filter in ["service_name = 'checkout'", "0"] {
        for group_by in ["", "GROUP BY tenant_id, service_name"] {
            let suffix = format!(
                "FROM spans PREWHERE tenant_id = 'a' \
                 AND timestamp >= now() - INTERVAL 120 MINUTE \
                 WHERE ({filter}) AND kind = 'SPAN_KIND_SERVER' {group_by}"
            );
            let shared = client
                .query(&format!(
                    "SELECT {LATENCY_PERCENTILES_SQL}, count() AS total {suffix}"
                ))
                .fetch_all::<LatencyPercentilesRow>()
                .await?;
            let original = client
                .query(&format!(
                    "SELECT quantile(0.5)(duration_ns) / 1000000.0 AS p50_ms, \
                            quantile(0.95)(duration_ns) / 1000000.0 AS p95_ms, \
                            quantile(0.99)(duration_ns) / 1000000.0 AS p99_ms, \
                            count() AS total {suffix}"
                ))
                .fetch_all::<LatencyPercentilesRow>()
                .await?;
            anyhow::ensure!(shared.len() == original.len(), "row count changed");
            for (actual, expected) in shared.iter().zip(&original) {
                anyhow::ensure!(actual.total == expected.total, "span count changed");
                for (actual, expected) in [
                    (actual.p50_ms, expected.p50_ms),
                    (actual.p95_ms, expected.p95_ms),
                    (actual.p99_ms, expected.p99_ms),
                ] {
                    anyhow::ensure!(
                        actual == expected || (actual.is_nan() && expected.is_nan()),
                        "percentile changed: {actual} != {expected}"
                    );
                }
            }
        }
    }
    Ok(())
}
