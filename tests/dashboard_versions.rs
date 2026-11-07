use rush_api::clickhouse_config::ConfigDb;
use rush_api::models::dashboard::DashboardDefaults;

#[tokio::test]
#[ignore = "requires a disposable ClickHouse and test-only config/session secrets"]
async fn restoring_a_version_brings_back_its_metadata_and_widgets() -> anyhow::Result<()> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let db = ConfigDb::open(&url, "default", "").await?;
    let dash = uuid::Uuid::new_v4().to_string();
    let (kept, added) = (
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
    );
    let defaults = DashboardDefaults::default();
    db.create_dashboard(
        &dash, "Original", "", "default", "u1", "tenant", "[]", "[]", &defaults,
    )
    .await?;
    db.create_widget(
        &kept,
        &dash,
        "Errors",
        "timeseries",
        "{}",
        "{\"x\":0}",
        "{}",
    )
    .await?;
    db.record_dashboard_version(&dash, "dashboard.create", "u1", "alice")
        .await?;

    db.update_dashboard(
        &dash, "Renamed", "", "tenant", "[]", "[]", "default", "u1", "admin", None,
    )
    .await?;
    db.create_widget(&added, &dash, "Latency", "timeseries", "{}", "{}", "{}")
        .await?;
    db.delete_widget(&kept, &dash).await?;
    db.record_dashboard_version(&dash, "dashboard.update", "u1", "alice")
        .await?;

    let versions = db.list_dashboard_versions(&dash, 100).await?;
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].reason, "dashboard.update", "newest first");
    let first = db
        .get_dashboard_version(&dash, &versions[1].id)
        .await?
        .unwrap();
    assert_eq!(first.snapshot.dashboard.name, "Original");
    assert!(db.get_dashboard_version(&dash, "missing").await?.is_none());

    db.update_dashboard(
        &dash,
        &first.snapshot.dashboard.name,
        "",
        "tenant",
        "[]",
        "[]",
        "default",
        "u1",
        "admin",
        Some(&first.snapshot.dashboard.defaults),
    )
    .await?;
    db.replace_dashboard_widgets(&dash, &first.snapshot.widgets)
        .await?;

    let dashboard = db.get_dashboard(&dash, "default", "u1").await?.unwrap();
    assert_eq!(dashboard.name, "Original");
    let widgets = db.list_widgets(&dash).await?;
    assert_eq!(widgets.len(), 1, "the widget added later is removed");
    assert_eq!(
        widgets[0].id, kept,
        "the deleted widget returns under its id"
    );
    assert_eq!(widgets[0].title, "Errors");
    assert_eq!(widgets[0].position, "{\"x\":0}");
    Ok(())
}
