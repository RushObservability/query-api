use super::*;

#[derive(Debug, PartialEq, clickhouse::Row, serde::Deserialize)]
struct Totals {
    count: u64,
    duration: u64,
    attribute_bytes: u64,
}

#[tokio::test]
#[ignore = "requires RUSH_TEST_CLICKHOUSE_URL with rush_ settings and permission to create a database/user"]
async fn fresh_and_upgraded_projections_preserve_tenant_reads() -> anyhow::Result<()> {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let database = format!("test_span_projection_{id}");
    let user = format!("test_span_reader_{id}");
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let admin = Client::default()
        .with_url(&url)
        .with_user(std::env::var("RUSH_TEST_CLICKHOUSE_USER").unwrap_or("default".into()))
        .with_password(std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default());
    admin
        .query(&format!("CREATE DATABASE {database}"))
        .execute()
        .await?;
    let result = async {
        let ddl = super::super::MIGRATIONS
            .iter()
            .find(|sql| sql.starts_with("CREATE TABLE IF NOT EXISTS observability.spans\n"))
            .ok_or_else(|| anyhow::anyhow!("missing spans migration"))?
            .replace("observability.spans", &format!("{database}.spans"));
        admin.query(&ddl).execute().await?;
        anyhow::ensure!(!drop_outdated_projection(&admin, &database).await?, "fresh definition must be retained");
        anyhow::ensure!(!add_projection(&admin, &database).await?, "fresh schema must contain the projection");
        anyhow::ensure!(!materialize_projection(&admin, &database).await?, "empty table needs no backfill");

        // Simulate an existing installation with stored parts but no projection.
        admin.query(&format!("ALTER TABLE {database}.spans DROP PROJECTION {PROJECTION_NAME}")).execute().await?;
        admin.query(&format!(
            "INSERT INTO {database}.spans (tenant_id, timestamp, service_name, duration_ns, attributes) \
             SELECT if(number % 2 = 0, 'a', 'b'), now64(9) - INTERVAL 1 HOUR, \
                 concat('svc-', toString(intDiv(number, 2) % 16)), number + 1, toString(number) \
             FROM numbers(50000)"
        )).execute().await?;
        // An earlier keys-only definition must not shadow the covering index.
        admin.query(&format!("ALTER TABLE {database}.spans ADD PROJECTION {PROJECTION_NAME} (SELECT tenant_id, service_name, timestamp, _part_offset ORDER BY (tenant_id, service_name, timestamp))")).execute().await?;
        anyhow::ensure!(materialize_projection(&admin, &database).await?, "prototype must contain stored projection parts");
        anyhow::ensure!(drop_outdated_projection(&admin, &database).await?, "obsolete definition must be replaced");
        anyhow::ensure!(add_projection(&admin, &database).await?, "legacy schema must be upgraded");
        anyhow::ensure!(!drop_outdated_projection(&admin, &database).await?, "replacement must be retained");
        anyhow::ensure!(!add_projection(&admin, &database).await?, "creation must be idempotent");
        anyhow::ensure!(materialize_projection(&admin, &database).await?, "old parts must be backfilled");
        anyhow::ensure!(!materialize_projection(&admin, &database).await?, "completed backfill must not repeat");

        admin.query(&format!("CREATE USER {user} IDENTIFIED WITH no_password")).execute().await?;
        admin.query(&format!("GRANT SELECT ON {database}.spans TO {user}")).execute().await?;
        admin.query(&format!(
            "CREATE ROW POLICY tenant_isolation ON {database}.spans FOR SELECT \
             USING notEmpty(getSetting('rush_tenant_id')) AND tenant_id = getSetting('rush_tenant_id') TO {user}"
        )).execute().await?;
        let read = Client::default().with_url(&url).with_user(&user).with_database(&database);
        let sql = "SELECT count() AS count, sum(duration_ns) AS duration, \
                          sum(length(attributes)) AS attribute_bytes \
                   FROM spans PREWHERE service_name = 'svc-0' AND timestamp > now() - INTERVAL 2 HOUR";
        for tenant in ["a", "b", "missing", ""] {
            let baseline = read.query(sql).with_setting("rush_tenant_id", tenant)
                .with_setting("optimize_use_projections", "0").fetch_one::<Totals>().await?;
            let projected = read.query(sql).with_setting("rush_tenant_id", tenant)
                .with_setting("optimize_use_projections", "1").fetch_one::<Totals>().await?;
            anyhow::ensure!(baseline == projected, "projection changed results for tenant {tenant:?}");
            let expected_count = if matches!(tenant, "a" | "b") { 1563 } else { 0 };
            anyhow::ensure!(projected.count == expected_count, "row policy changed: {projected:?}");
        }
        let plan = read.query(
            "EXPLAIN indexes = 1 SELECT count(), sum(duration_ns) FROM spans \
             PREWHERE tenant_id = 'a' AND service_name = 'svc-0'"
        ).with_setting("rush_tenant_id", "a").fetch_all::<String>().await?;
        anyhow::ensure!(plan.iter().any(|line| line.contains(PROJECTION_NAME)), "projection was not selected: {plan:?}");
        let leaked = read.query("SELECT count() FROM spans WHERE tenant_id = 'b'")
            .with_setting("rush_tenant_id", "a").fetch_one::<u64>().await?;
        anyhow::ensure!(leaked == 0, "cross-tenant rows leaked");

        // New inserts automatically maintain the projection. Queries needing
        // attributes can still fall back to the wider base table.
        admin.query(&format!(
            "INSERT INTO {database}.spans (tenant_id, timestamp, service_name, duration_ns, attributes) \
             SELECT 'a', now64(9), 'svc-0', 1, 'new' FROM numbers(100)"
        )).execute().await?;
        anyhow::ensure!(!materialize_projection(&admin, &database).await?, "new inserts must include projection parts");
        let after_insert = read.query(sql).with_setting("rush_tenant_id", "a").fetch_one::<Totals>().await?;
        anyhow::ensure!(after_insert.count == 1663, "new spans are missing");

        // The retention enforcer uses heavyweight DELETE mutations on spans.
        admin.query(&format!(
            "ALTER TABLE {database}.spans DELETE WHERE tenant_id = 'a' AND duration_ns = 1 SETTINGS mutations_sync = 1"
        )).execute().await?;
        let baseline = read.query(sql).with_setting("rush_tenant_id", "a")
            .with_setting("optimize_use_projections", "0").fetch_one::<Totals>().await?;
        let projected = read.query(sql).with_setting("rush_tenant_id", "a")
            .with_setting("optimize_use_projections", "1").fetch_one::<Totals>().await?;
        anyhow::ensure!(baseline == projected && projected.count == 1562, "deletion changed projection semantics: {projected:?}");
        Ok(())
    }.await;
    let drop_database = admin
        .query(&format!("DROP DATABASE {database} SYNC"))
        .execute()
        .await;
    let drop_user = admin
        .query(&format!("DROP USER IF EXISTS {user}"))
        .execute()
        .await;
    result?;
    drop_database?;
    drop_user?;
    Ok(())
}
