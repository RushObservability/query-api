//! Run against a schema with extra columns to verify old readers remain usable.
#[tokio::test]
#[ignore = "requires RUSH_TEST_CLICKHOUSE_URL"]
async fn explicit_span_projection_matches_expanded_schema() {
    use rush_api::models::trace::WideEvent;
    let client = clickhouse::Client::default()
        .with_validation(false)
        .with_url(std::env::var("RUSH_TEST_CLICKHOUSE_URL").unwrap())
        .with_database(
            std::env::var("RUSH_TEST_CLICKHOUSE_DATABASE").unwrap_or("observability".into()),
        )
        .with_user(std::env::var("RUSH_TEST_CLICKHOUSE_USER").unwrap_or("default".into()))
        .with_password(std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default());
    let tenant = std::env::var("RUSH_TEST_TENANT").unwrap_or("default".into());
    let rows = client
        .query("SELECT ?fields FROM spans WHERE tenant_id = ? ORDER BY timestamp DESC LIMIT 1")
        .bind(tenant)
        .fetch_all::<WideEvent>()
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the configured tenant must have a stored span to exercise decoding"
    );
    println!(
        "Read {} spans using {} explicit columns",
        rows.len(),
        <WideEvent as clickhouse::Row>::COLUMN_NAMES.len()
    );
}
