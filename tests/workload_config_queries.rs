use rush_api::clickhouse_config::ConfigDb;
use rush_api::models::dashboard::DashboardDefaults;
use rush_api::models::notebook::Notebook;

#[tokio::test]
#[ignore = "requires a disposable ClickHouse and test-only config/session secrets"]
async fn the_newest_setting_save_wins_even_after_a_future_version() -> anyhow::Result<()> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let db = ConfigDb::open(&url, "default", "").await?;
    let key = format!("test-setting-{}", uuid::Uuid::new_v4());

    db.set_setting(&key, "first").await?;
    db.set_setting(&key, "second").await?;
    assert_eq!(db.get_setting(&key).await?.as_deref(), Some("second"));

    // A save from a replica whose clock ran an hour ahead.
    let now_micros = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000;
    let ahead = (now_micros + 3_600_000_000) as u64;
    db.client
        .query("INSERT INTO config_settings (key, value, version, is_deleted) VALUES (?, 'skewed', ?, 0)")
        .bind(&key)
        .bind(ahead)
        .execute()
        .await?;
    db.set_setting(&key, "latest").await?;
    assert_eq!(db.get_setting(&key).await?.as_deref(), Some("latest"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable ClickHouse and test-only config/session secrets"]
async fn readable_checks_follow_the_visibility_rules() -> anyhow::Result<()> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let db = ConfigDb::open(&url, "default", "").await?;
    let defaults = DashboardDefaults::default();
    let (private, shared, global) = (
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
    );
    for (id, visibility) in [
        (&private, "private"),
        (&shared, "tenant"),
        (&global, "global"),
    ] {
        db.create_dashboard(
            id, "Board", "", "tenant-a", "u1", visibility, "[]", "[]", &defaults,
        )
        .await?;
    }
    let notebook = |visibility: &str| Notebook {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: "tenant-a".into(),
        name: "Checkout 5xx".into(),
        description: String::new(),
        owner_id: "u1".into(),
        owner_name: "u1".into(),
        visibility: visibility.into(),
        status: "open".into(),
        incident_start: "2026-11-07T13:40:00Z".into(),
        incident_end: String::new(),
        tags: "[]".into(),
        created_at: "2026-11-07T13:41:00.000000Z".into(),
        updated_at: "2026-11-07T13:41:00.000000Z".into(),
    };
    let (own_nb, team_nb) = (notebook("private"), notebook("tenant"));
    db.write_notebook(&own_nb, false).await?;
    db.write_notebook(&team_nb, false).await?;

    // Expected access, as (tenant-a u1, tenant-a u2, tenant-b u1, tenant-b u2).
    // A private dashboard follows its owner across tenants; notebooks don't.
    let viewers = [
        ("tenant-a", "u1"),
        ("tenant-a", "u2"),
        ("tenant-b", "u1"),
        ("tenant-b", "u2"),
    ];
    let missing = "missing".to_string();
    for (id, expected) in [
        (&private, [true, false, true, false]),
        (&shared, [true, true, false, false]),
        (&global, [true, true, true, true]),
        (&missing, [false, false, false, false]),
    ] {
        for ((tenant, user), expected) in viewers.into_iter().zip(expected) {
            let readable = db.dashboard_readable(id, tenant, user).await?;
            assert_eq!(readable, expected, "dashboard {id} {tenant} {user}");
            assert_eq!(
                db.get_dashboard(id, tenant, user).await?.is_some(),
                expected,
                "get_dashboard {id} {tenant} {user}"
            );
        }
    }
    for (id, expected) in [
        (&own_nb.id, [true, false, false, false]),
        (&team_nb.id, [true, true, false, false]),
    ] {
        for ((tenant, user), expected) in viewers.into_iter().zip(expected) {
            let readable = db.notebook_readable(id, tenant, user).await?;
            assert_eq!(readable, expected, "notebook {id} {tenant} {user}");
            assert_eq!(
                db.get_notebook(id, tenant, user).await?.is_some(),
                expected,
                "get_notebook {id} {tenant} {user}"
            );
        }
    }
    Ok(())
}
