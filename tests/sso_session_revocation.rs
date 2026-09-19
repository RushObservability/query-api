//! Run against a disposable, empty ClickHouse, never a development/production DB.
//! See docs/security/sso-session-revocation.md for the test command.

use rush_api::clickhouse_config::ConfigDb;

async fn provider(
    db: &ConfigDb,
    id: &str,
    protocol: &str,
    issuer: &str,
    cert: &str,
) -> anyhow::Result<()> {
    db.upsert_sso_provider(
        id,
        "Security test",
        protocol,
        true,
        "test-client",
        "test-secret",
        issuer,
        "openid profile email groups",
        "groups",
        "email",
        "given_name",
        "family_name",
        true,
        "",
        "",
        "https://idp.example.test/sso",
        cert,
        "rush-test",
    )
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable ClickHouse and test-only config/session secrets"]
async fn sso_config_changes_revoke_sessions_across_replicas() -> anyhow::Result<()> {
    let url = std::env::var("RUSH_TEST_CLICKHOUSE_URL")?;
    let db = ConfigDb::open(&url, "default", "").await?;
    let other_replica = ConfigDb::open(&url, "default", "").await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let admin_group = db
        .create_group(&format!("admin-{suffix}"), "", "[\"all\"]", "[\"admin\"]")
        .await?;
    let viewer_group = db
        .create_group(&format!("viewer-{suffix}"), "", "[\"all\"]", "[\"read\"]")
        .await?;

    let local_name = format!("local-{suffix}");
    let local_id = db
        .create_user(&local_name, "a-long-test-only-passphrase", "Local")
        .await?;
    let (_, _, _, _, _, user_version) = db
        .authenticate(&local_name, "a-long-test-only-passphrase")
        .await?
        .unwrap();
    let local = db
        .create_session_at_version(&local_id, user_version)
        .await?;

    for protocol in ["oidc", "saml"] {
        let id = format!("{protocol}-{suffix}");
        provider(&db, &id, protocol, "https://idp.example.test", "cert-a").await?;
        let user = db
            .create_sso_user(
                &format!("{protocol}-{suffix}@example.test"),
                "SSO",
                &format!("namespaced-{id}"),
                protocol,
                "default",
            )
            .await?;
        let revision = db.sso_security_revision(&id).await?;
        let initial = db
            .create_sso_session(&user, protocol, &id, &revision)
            .await?;
        assert!(
            other_replica
                .get_session_user(&initial.token)
                .await
                .is_some()
        );

        // Creating a mapping also changes authorization for existing users.
        let mapping = db
            .create_idp_group_mapping("engineers", &admin_group, &id)
            .await?;
        assert!(
            other_replica
                .get_session_user(&initial.token)
                .await
                .is_none()
        );
        assert!(
            db.create_sso_session(&user, protocol, &id, &revision)
                .await
                .is_err()
        );
        let groups = db.resolve_idp_groups(&["engineers".into()], &id).await?;
        db.update_user_groups_from_idp(&user, &groups).await?;
        assert!(
            other_replica
                .resolve_user_permissions(&user)
                .await?
                .1
                .contains(&"admin".into())
        );
        let revision = db.sso_security_revision(&id).await?;
        let privileged = db
            .create_sso_session(&user, protocol, &id, &revision)
            .await?;
        assert_eq!(
            other_replica
                .list_auth_sessions(Some(&user), &privileged.token)
                .await?
                .len(),
            1
        );
        assert_eq!(
            other_replica
                .get_session_user(&privileged.token)
                .await
                .unwrap()
                .4,
            "admin"
        );

        db.update_idp_group_mapping(&mapping, "engineers", &viewer_group)
            .await?;
        assert!(
            other_replica
                .get_session_user(&privileged.token)
                .await
                .is_none()
        );
        assert!(
            other_replica
                .list_auth_sessions(Some(&user), &privileged.token)
                .await?
                .is_empty()
        );
        assert!(
            other_replica
                .rotate_session_if_due(&privileged.token)
                .await?
                .is_none()
        );
        let groups = db.resolve_idp_groups(&["engineers".into()], &id).await?;
        db.update_user_groups_from_idp(&user, &groups).await?;
        // The other replica must not retain its previously cached admin grant.
        assert!(
            !other_replica
                .resolve_user_permissions(&user)
                .await?
                .1
                .contains(&"admin".into())
        );
        let revision = db.sso_security_revision(&id).await?;
        let viewer = db
            .create_sso_session(&user, protocol, &id, &revision)
            .await?;
        assert_eq!(
            other_replica
                .get_session_user(&viewer.token)
                .await
                .unwrap()
                .4,
            "viewer"
        );

        // Rotation keeps the original revision, including the grace bearer.
        db.client.query("ALTER TABLE config_sessions UPDATE last_seen_at = toString(now() - INTERVAL 10 MINUTE) WHERE token = ? SETTINGS mutations_sync = 2")
            .bind(db.session_request_key(&viewer.token)).execute().await?;
        let rotated = db
            .rotate_session_if_due(&viewer.token)
            .await?
            .expect("rotation is due");
        assert!(
            other_replica
                .get_session_user(&rotated.issued.token)
                .await
                .is_some()
        );
        db.delete_idp_group_mapping(&mapping).await?;
        assert_ne!(db.sso_security_revision(&id).await?, revision);
        assert!(
            other_replica
                .get_session_user(&viewer.token)
                .await
                .is_none()
        );
        assert!(
            other_replica
                .get_session_user(&rotated.issued.token)
                .await
                .is_none()
        );
        assert!(
            db.resolve_idp_groups(&["engineers".into()], &id)
                .await?
                .is_empty()
        );

        for (issuer, cert) in [
            ("https://replacement.example.test", "cert-a"),
            ("https://replacement.example.test", "cert-b"),
        ] {
            let revision = db.sso_security_revision(&id).await?;
            let session = db
                .create_sso_session(&user, protocol, &id, &revision)
                .await?;
            provider(&db, &id, protocol, issuer, cert).await?;
            assert!(
                other_replica
                    .get_session_user(&session.token)
                    .await
                    .is_none()
            );
            assert!(
                db.create_sso_session(&user, protocol, &id, &revision)
                    .await
                    .is_err()
            );
        }

        // An old binary's unversioned SSO sessions fail closed during upgrade.
        let revision = db.sso_security_revision(&id).await?;
        let legacy = db
            .create_sso_session(&user, protocol, &id, &revision)
            .await?;
        db.client.query("ALTER TABLE config_sessions UPDATE sso_revision = '' WHERE token = ? SETTINGS mutations_sync = 2")
            .bind(db.session_request_key(&legacy.token)).execute().await?;
        assert!(
            other_replica
                .get_session_user(&legacy.token)
                .await
                .is_none()
        );
        assert!(other_replica.get_session_user(&local.token).await.is_some());
    }
    Ok(())
}
