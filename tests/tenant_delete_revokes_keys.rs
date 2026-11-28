//! Run against a disposable, empty ClickHouse, never a development/production DB:
//! `RUSH_TEST_CLICKHOUSE_URL=... cargo test --test tenant_delete_revokes_keys -- --ignored`

use rush_api::clickhouse_config::ConfigDb;

#[tokio::test]
#[ignore = "requires a disposable ClickHouse"]
async fn tenant_api_key_ids_match_name_and_id_only() -> anyhow::Result<()> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let db = ConfigDb::open(&url, "default", "").await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let (tenant_id, name) = (format!("t-{suffix}"), format!("acme-{suffix}"));
    db.create_tenant(&tenant_id, &name, true, true).await?;

    let key = |id: String, tenant: String| {
        let db = &db;
        async move {
            db.create_api_key(
                &id,
                &id,
                &format!("hash-{id}"),
                "rush_",
                &tenant,
                "query",
                "viewer",
                false,
                &[],
                0,
                &[],
            )
            .await
            .map(|_| id)
        }
    };
    let by_name = key(format!("k1-{suffix}"), name.clone()).await?;
    let by_id = key(format!("k2-{suffix}"), tenant_id.clone()).await?;
    let other = key(format!("k3-{suffix}"), format!("other-{suffix}")).await?;

    let mut ids = db.tenant_api_key_ids(&tenant_id, &name).await?;
    ids.sort();
    assert_eq!(ids, vec![by_name.clone(), by_id.clone()]);

    for id in &ids {
        assert!(db.delete_api_key(id).await?);
    }
    assert!(db.tenant_api_key_ids(&tenant_id, &name).await?.is_empty());
    assert_eq!(
        db.tenant_api_key_ids(&format!("other-{suffix}"), "")
            .await?,
        vec![other]
    );
    Ok(())
}
