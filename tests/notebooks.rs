use rush_api::clickhouse_config::ConfigDb;
use rush_api::models::notebook::{Notebook, NotebookCell};

fn notebook(tenant: &str, owner: &str, visibility: &str) -> Notebook {
    Notebook {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: tenant.into(),
        name: "Checkout 5xx".into(),
        description: String::new(),
        owner_id: owner.into(),
        owner_name: owner.into(),
        visibility: visibility.into(),
        status: "open".into(),
        incident_start: "2026-11-07T13:40:00Z".into(),
        incident_end: String::new(),
        tags: "[]".into(),
        created_at: "2026-11-07T13:41:00.000000Z".into(),
        updated_at: "2026-11-07T13:41:00.000000Z".into(),
    }
}

fn cell(nb: &Notebook, title: &str, position: u32) -> NotebookCell {
    NotebookCell {
        id: uuid::Uuid::new_v4().to_string(),
        notebook_id: nb.id.clone(),
        tenant_id: nb.tenant_id.clone(),
        position,
        cell_type: "note".into(),
        title: title.into(),
        body: String::new(),
        query_config: "{}".into(),
        time: "{\"mode\":\"inherit\"}".into(),
        display_config: "{}".into(),
        author_id: nb.owner_id.clone(),
        author_name: nb.owner_name.clone(),
        created_at: nb.created_at.clone(),
        updated_at: nb.updated_at.clone(),
    }
}

/// The disposable ClickHouse from `RUSH_TEST_CLICKHOUSE_URL`, with an optional
/// `RUSH_TEST_CLICKHOUSE_PASSWORD` for the default user.
async fn open_db() -> anyhow::Result<ConfigDb> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let password = std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default();
    ConfigDb::open(&url, "default", &password).await
}

#[tokio::test]
#[ignore = "requires a disposable ClickHouse and test-only config/session secrets"]
async fn notebooks_stay_in_their_tenant_and_private_ones_stay_with_their_owner()
-> anyhow::Result<()> {
    let db = open_db().await?;
    let (tenant_a, tenant_b) = (
        format!("nb-a-{}", uuid::Uuid::new_v4()),
        format!("nb-b-{}", uuid::Uuid::new_v4()),
    );

    let shared = notebook(&tenant_a, "u1", "tenant");
    let private = notebook(&tenant_a, "u1", "private");
    db.write_notebook(&shared, false).await?;
    db.write_notebook(&private, false).await?;

    assert!(
        db.get_notebook(&shared.id, &tenant_a, "u2")
            .await?
            .is_some()
    );
    assert!(
        db.get_notebook(&shared.id, &tenant_b, "u1")
            .await?
            .is_none(),
        "another tenant can't read it, even with the owner's id"
    );
    assert!(
        db.get_notebook(&private.id, &tenant_a, "u1")
            .await?
            .is_some()
    );
    assert!(
        db.get_notebook(&private.id, &tenant_a, "u2")
            .await?
            .is_none()
    );

    let ids = |list: Vec<Notebook>| list.into_iter().map(|n| n.id).collect::<Vec<_>>();
    assert_eq!(
        ids(db.list_notebooks(&tenant_a, "u2").await?),
        vec![shared.id.clone()]
    );
    assert_eq!(db.list_notebooks(&tenant_a, "u1").await?.len(), 2);
    assert!(db.list_notebooks(&tenant_b, "u1").await?.is_empty());

    let mut renamed = shared.clone();
    renamed.name = "Renamed".into();
    renamed.updated_at = "2026-11-07T14:00:00.000000Z".into();
    db.write_notebook(&renamed, false).await?;
    let read = db.get_notebook(&shared.id, &tenant_a, "u2").await?.unwrap();
    assert_eq!(read.name, "Renamed");
    assert_eq!(
        read.updated_at, renamed.updated_at,
        "the stale-write check reads this back"
    );

    db.write_notebook(&renamed, true).await?;
    assert!(
        db.get_notebook(&shared.id, &tenant_a, "u1")
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable ClickHouse and test-only config/session secrets"]
async fn cells_come_back_in_position_order_without_deleted_ones() -> anyhow::Result<()> {
    let db = open_db().await?;
    let nb = notebook(&format!("nb-{}", uuid::Uuid::new_v4()), "u1", "tenant");
    db.write_notebook(&nb, false).await?;

    let (third, first, gone) = (
        cell(&nb, "third", 3000),
        cell(&nb, "first", 1000),
        cell(&nb, "gone", 2000),
    );
    for c in [&third, &first, &gone] {
        db.write_notebook_cell(c, false).await?;
    }
    db.write_notebook_cell(&gone, true).await?;

    let mut moved = third.clone();
    moved.position = 500;
    db.write_notebook_cell(&moved, false).await?;

    let titles: Vec<String> = db
        .list_notebook_cells(&nb.id, &nb.tenant_id)
        .await?
        .into_iter()
        .map(|c| c.title)
        .collect();
    assert_eq!(titles, vec!["third", "first"]);
    assert!(
        db.list_notebook_cells(&nb.id, "other-tenant")
            .await?
            .is_empty()
    );
    Ok(())
}
