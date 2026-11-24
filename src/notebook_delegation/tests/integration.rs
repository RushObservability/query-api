use super::*;

async fn session(
    db: &ConfigDb,
    name: &str,
    password: &str,
) -> anyhow::Result<(String, SessionUser, String)> {
    let auth = db
        .authenticate(name, password)
        .await?
        .ok_or_else(|| anyhow::anyhow!("test login failed"))?;
    let issued = db.create_session_at_version(&auth.0, auth.5).await?;
    let caller = db
        .get_session_user(&issued.token)
        .await
        .ok_or_else(|| anyhow::anyhow!("test session unavailable"))?;
    let mut headers = HeaderMap::new();
    let cookie = crate::handlers::auth::session_cookie(&issued.token, 900);
    headers.insert(
        axum::http::header::COOKIE,
        cookie.split(';').next().unwrap().parse()?,
    );
    let grant = issue(db, &headers, &caller).map_err(|e| anyhow::anyhow!("{}", e.1))?;
    Ok((issued.token, caller, grant))
}

fn headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(HEADER, token.parse().unwrap());
    headers.insert(
        crate::internal_auth::INTERNAL_TOKEN_HEADER,
        std::env::var("SRE_AGENT_INTERNAL_TOKEN")
            .expect("test internal token")
            .parse()
            .unwrap(),
    );
    headers
}

#[tokio::test]
#[ignore = "requires a disposable ClickHouse and test-only config/session secrets"]
async fn delegated_access_rechecks_sessions_roles_keys_and_private_ownership() -> anyhow::Result<()>
{
    let db_password = std::env::var("RUSH_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default();
    let db = ConfigDb::open(
        &std::env::var("RUSH_TEST_CLICKHOUSE_URL")?,
        "default",
        &db_password,
    )
    .await?;
    let suffix = uuid::Uuid::new_v4().to_string();
    let name = format!("notebook-writer-{suffix}");
    let password = uuid::Uuid::new_v4().to_string();
    let user = db.create_user(&name, &password, "Notebook Writer").await?;
    let group = db
        .create_group(
            &format!("notebook-writers-{suffix}"),
            "",
            "[]",
            "[\"write\"]",
        )
        .await?;
    db.set_user_groups(&user, std::slice::from_ref(&group))
        .await?;
    let (_, caller, token) = session(&db, &name, &password).await?;
    let delegated = headers(&token);
    let mut missing_internal = delegated.clone();
    missing_internal.remove(crate::internal_auth::INTERNAL_TOKEN_HEADER);
    assert!(
        authorize(&db, &missing_internal, "default", true)
            .await
            .is_err()
    );
    let (writer, identity) = authorize(&db, &delegated, "default", true)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e.1))?;
    assert_eq!(writer.0, caller.0);
    assert_eq!(identity.actor_id, user);
    assert_eq!(identity.actor_type, "user");
    assert!(
        authorize(&db, &delegated, "another-tenant", true)
            .await
            .is_err()
    );
    db.set_user_groups(&user, &[]).await?;
    assert!(
        authorize(&db, &delegated, "default", true).await.is_err(),
        "a role downgrade must stop writes"
    );
    db.set_user_groups(&user, std::slice::from_ref(&group))
        .await?;
    let (raw, _, token) = session(&db, &name, &password).await?;
    let delegated = headers(&token);
    assert!(authorize(&db, &delegated, "default", true).await.is_ok());
    db.delete_session(&raw).await?;
    assert!(
        authorize(&db, &delegated, "default", true).await.is_err(),
        "logout must stop a running turn's writes"
    );

    let viewer_name = format!("notebook-viewer-{suffix}");
    db.create_user(&viewer_name, &password, "Notebook Viewer")
        .await?;
    let (_, viewer, token) = session(&db, &viewer_name, &password).await?;
    let delegated = headers(&token);
    assert!(authorize(&db, &delegated, "default", false).await.is_ok());
    assert!(authorize(&db, &delegated, "default", true).await.is_err());
    let now = crate::models::notebook::stamp(time::OffsetDateTime::now_utc());
    let notebook = crate::models::notebook::Notebook {
        id: uuid::Uuid::new_v4().to_string(),
        tenant_id: "default".into(),
        name: "Private incident".into(),
        description: String::new(),
        owner_id: user.clone(),
        owner_name: name,
        visibility: "private".into(),
        status: "open".into(),
        incident_start: now.clone(),
        incident_end: String::new(),
        tags: "[]".into(),
        created_at: now.clone(),
        updated_at: now,
    };
    db.write_notebook(&notebook, false).await?;
    assert!(
        db.get_notebook(&notebook.id, "default", &viewer.0)
            .await?
            .is_none()
    );
    assert!(
        db.get_notebook(&notebook.id, "default", &user)
            .await?
            .is_some()
    );

    let id = uuid::Uuid::new_v4().to_string();
    let hash = format!("notebook-hash-{suffix}");
    db.create_api_key(
        &id,
        "Notebook agent",
        &hash,
        "test",
        "default",
        "query",
        "write",
        true,
        &[],
        0,
        &[],
    )
    .await?;
    let grant = db.resolve_agent_key_by_id(&id).await?.unwrap();
    let principal = crate::handlers::users::api_key_principal(&grant).unwrap();
    let key_token = crate::request_auth::scope(async {
        crate::request_auth::set_api_key_grant(grant);
        issue(&db, &HeaderMap::new(), &principal).unwrap()
    })
    .await;
    let delegated = headers(&key_token);
    let (_, identity) = authorize(&db, &delegated, "default", true)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e.1))?;
    assert_eq!(identity.actor_type, "api_key");
    assert_eq!(identity.actor_id, id);
    db.create_api_key(
        &id,
        "Notebook agent",
        &hash,
        "test",
        "default",
        "query",
        "viewer",
        true,
        &[],
        0,
        &[],
    )
    .await?;
    assert!(authorize(&db, &delegated, "default", false).await.is_ok());
    assert!(authorize(&db, &delegated, "default", true).await.is_err());
    db.create_api_key(
        &id,
        "Notebook agent",
        &hash,
        "test",
        "default",
        "query",
        "write",
        true,
        &[],
        0,
        &["127.0.0.1".into()],
    )
    .await?;
    assert!(
        authorize(&db, &delegated, "default", false).await.is_err(),
        "changing the source network policy invalidates existing access"
    );
    db.delete_api_key(&id).await?;
    assert!(authorize(&db, &delegated, "default", false).await.is_err());
    Ok(())
}
