//! Run against a disposable, empty ClickHouse, never a development/production DB:
//! `RUSH_TEST_CLICKHOUSE_URL=... cargo test --test tenant_name_reuse -- --ignored`

use rush_api::clickhouse_config::ConfigDb;

#[tokio::test]
#[ignore = "requires a disposable ClickHouse"]
async fn deleted_tenant_names_stay_reserved() -> anyhow::Result<()> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let db = ConfigDb::open(&url, "default", "").await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let (id, name) = (format!("t-{suffix}"), format!("acme-{suffix}"));

    assert!(!db.tenant_name_ever_used(&name).await?);
    db.create_tenant(&id, &name, true, true).await?;
    assert!(db.delete_tenant(&id).await?);
    assert!(db.tenant_name_ever_used(&name).await?);
    assert!(db.tenant_name_ever_used(&name.to_uppercase()).await?);
    assert!(db.tenant_name_ever_used(&id).await?);
    Ok(())
}
