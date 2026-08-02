//! Live ClickHouse regression for QAPI-SEC-01.
//!
//! Run only against a dedicated test ClickHouse:
//! `cargo test --test tenant_row_policy -- --ignored --nocapture`

use clickhouse::Client;

#[derive(clickhouse::Row, serde::Deserialize)]
struct TenantRows {
    tenants: Vec<String>,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    count: u64,
}

fn client(url: &str, database: &str, user: &str, password: &str) -> Client {
    Client::default()
        .with_url(url)
        .with_database(database)
        .with_user(user)
        .with_password(password)
}

#[tokio::test]
#[ignore = "requires a dedicated ClickHouse with QAPI-SEC-01 read/write users"]
async fn row_policy_blocks_cross_tenant_query_shapes() -> anyhow::Result<()> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let database = std::env::var("RUSH_TEST_CLICKHOUSE_DATABASE")
        .unwrap_or_else(|_| "observability".to_string());
    let write_user = std::env::var("RUSH_TEST_CLICKHOUSE_USER")?;
    let write_password = std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD")?;
    let read_user = std::env::var("RUSH_TEST_CLICKHOUSE_READ_USER")?;
    let read_password = std::env::var("RUSH_TEST_CLICKHOUSE_READ_PASSWORD")?;

    let admin = client(&url, &database, &write_user, &write_password);
    let read = client(&url, &database, &read_user, &read_password);
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let tenant_a = format!("policy_a_{suffix}");
    let tenant_b = format!("policy_b_{suffix}");

    admin
        .query(&format!(
            "INSERT INTO observability.tenant_usage \
             (bucket, tenant_id, signal, events_count, bytes_count) VALUES \
             (now(), '{tenant_a}', 'policy_probe', 1, 1), \
             (now(), '{tenant_b}', 'policy_probe', 1, 1)"
        ))
        .execute()
        .await?;

    rush_api::migrations::verify_row_policies(&admin, &read, &read_user).await?;
    rush_api::mark_row_policy_enforced();

    let query_shapes = [
        "SELECT groupArray(tenant_id) AS tenants FROM observability.tenant_usage WHERE signal = 'policy_probe'",
        "SELECT groupArray(tenant_id) AS tenants FROM (SELECT tenant_id FROM observability.tenant_usage WHERE signal = 'policy_probe' UNION ALL SELECT tenant_id FROM observability.tenant_usage WHERE signal = 'policy_probe')",
        "SELECT groupArray(a.tenant_id) AS tenants FROM observability.tenant_usage AS a INNER JOIN observability.tenant_usage AS b ON a.tenant_id = b.tenant_id WHERE a.signal = 'policy_probe' AND b.signal = 'policy_probe'",
        "WITH scoped AS (SELECT tenant_id FROM observability.tenant_usage WHERE signal = 'policy_probe') SELECT groupArray(tenant_id) AS tenants FROM scoped",
        "SELECT groupArray(t.tenant_id) AS tenants FROM (SELECT tenant_id FROM observability.tenant_usage WHERE signal = 'policy_probe') AS t",
    ];

    for sql in query_shapes {
        let row = rush_api::tenant_query(&read, sql, &tenant_a)
            .fetch_one::<TenantRows>()
            .await?;
        assert!(
            !row.tenants.is_empty(),
            "query returned no tenant-a sentinel: {sql}"
        );
        assert!(
            row.tenants.iter().all(|tenant| tenant == &tenant_a),
            "cross-tenant row escaped policy for query: {sql}"
        );
    }

    // QAPI-SEC-02 uses the same compiler and tenant read principal for preview
    // and scheduled evaluations. A sentinel with the same detection predicate
    // in another tenant must never contribute to the rule count.
    let detection_marker = format!("detection_probe_{suffix}");
    admin
        .query(&format!(
            "INSERT INTO observability.logs \
             (tenant_id, Timestamp, SeverityText, Body, ServiceName) VALUES \
             ('{tenant_a}', now64(9), 'ERROR', '{detection_marker}', 'policy-test'), \
             ('{tenant_b}', now64(9), 'ERROR', '{detection_marker}', 'policy-test')"
        ))
        .execute()
        .await?;

    let template = format!(
        "SELECT ServiceName FROM logs WHERE Body = '{detection_marker}' \
         AND Timestamp BETWEEN @window_start AND @window_end"
    );
    let compiled = rush_api::detection_query::compile_count_query(
        &template,
        &tenant_a,
        "2000-01-01 00:00:00",
        "2100-01-01 00:00:00",
    )?;
    let detection_count =
        rush_api::detection_query::execute_count(&read, &compiled, &tenant_a).await?;
    assert_eq!(
        detection_count, 1,
        "cross-tenant detection sentinel contributed to the match count"
    );

    // The strict notEmpty() policy denies all rows when the application omits
    // rush_tenant_id, even though the SQL itself contains no tenant predicate.
    let missing = read
        .query(
            "SELECT count() AS count FROM observability.tenant_usage \
             WHERE signal = 'policy_probe'",
        )
        .fetch_one::<CountRow>()
        .await?;
    assert_eq!(missing.count, 0);

    Ok(())
}
