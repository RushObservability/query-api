//! Requires an isolated ClickHouse with Keeper; see docs/security/saml-hardening.md.
use rush_api::clickhouse_config::ConfigDb;

#[tokio::test]
#[ignore = "requires disposable ClickHouse with Keeper and test-only secrets"]
async fn keeper_claims_survive_api_restart_and_coordinate_replicas() -> anyhow::Result<()> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    assert!(
        rush_api::api_key_auth::production_mode(),
        "test must exercise production auto mode"
    );
    let suffix = uuid::Uuid::new_v4();
    let expiry = chrono::Utc::now().timestamp() + 600;
    let keys = [
        format!("saml-assertion:{suffix}"),
        format!("saml-request:{suffix}"),
        format!("saml-response:{suffix}"),
    ];
    let first = ConfigDb::open(&url, "default", "").await?;
    // Model a deployment with async insertion enabled. The claim operation
    // must override it so duplicate keys cannot share one acknowledged batch.
    let mut first = first;
    first.client = first.client.clone().with_option("async_insert", "1");
    for key in &keys {
        assert!(first.claim_sso_key_once(key, expiry).await?);
    }
    drop(first);

    // Reconstruct all application state, as a new query-api process would.
    let mut restarted = ConfigDb::open(&url, "default", "").await?;
    let mut replica = ConfigDb::open(&url, "default", "").await?;
    restarted.client = restarted.client.clone().with_option("async_insert", "1");
    replica.client = replica.client.clone().with_option("async_insert", "1");
    for key in &keys {
        assert!(!restarted.claim_sso_key_once(key, expiry).await?);
        assert!(!replica.claim_sso_key_once(key, expiry).await?);
    }
    for attempt in 0..8 {
        let race_key = format!("saml-assertion:race-{suffix}-{attempt}");
        let (a, b) = tokio::join!(
            restarted.claim_sso_key_once(&race_key, expiry),
            replica.claim_sso_key_once(&race_key, expiry)
        );
        assert_ne!(a?, b?, "exactly one replica may consume an assertion");
    }
    assert!(
        !restarted
            .claim_sso_key_once("already-expired", chrono::Utc::now().timestamp() - 1)
            .await?
    );
    Ok(())
}
