# SSO identity and session changes

SSO accounts must match the configured provider ID, issuer, and subject. A raw
subject from an older installation is no longer enough to claim an account.
Automatic migration could give a user from a replacement IdP the permissions of
an unrelated account with the same subject.

Before upgrading, keep a working local administrator account. Existing
namespaced SSO accounts and provider-specific pre-provisioned accounts continue
to work. For a legacy account, an administrator must verify the identity, remove
the old account, and pre-provision its replacement for the correct provider.
Record its group assignments first and restore only the intended access. Account
IDs change; review references to the old account before removing it. Do not
rewrite external IDs based on matching email addresses or raw subjects alone.

## When users must sign in again

- Editing a provider invalidates that provider's SSO sessions, even for a rename.
- Creating, changing, or deleting a group mapping invalidates its provider's sessions.
- Switching providers invalidates sessions issued under the previous active configuration.
- The first deployment invalidates existing SSO sessions without a stored revision.
- Local password sessions are unaffected by SSO configuration changes.

Each SSO callback records the shared configuration revision before reading trust
settings and group mappings. Session creation, validation, and rotation check
that revision. Deleting old session rows is not required for revocation, and
rotation cannot restore access. Provider and mapping audit events record the
invalidation without logging credentials.

After signing in again, users in automatic-admission modes receive the current
mapped groups or configured fallback. Deny mode continues to use groups assigned
by an administrator, not IdP group mappings. Changing a mapping does not remove
an explicitly assigned group in deny mode.

All API replicas must run the updated code before relying on these checks. An
older replica does not validate revisions. Permission lookups no longer use a
per-process cache, so another replica cannot reuse a previous login's grants.

## Regression test

Use a disposable ClickHouse server with an empty default database. The test
creates users, groups, providers, sessions, and mappings. Never point it at your
development or production database.

```sh
docker run --rm --name rush-sso-test -p 127.0.0.1:18123:8123 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:26.6.1.1193
```

In another terminal:

```sh
RUSH_TEST_CLICKHOUSE_URL=http://localhost:18123 \
RUSH_SSO_REPLAY_STORE=local \
RUSH_SESSION_HMAC_SECRET=test-only-session-key-not-for-deployment \
RUSH_CONFIG_ENCRYPTION_KEY=test-only-encryption-key-not-for-deployment \
cargo test --test sso_session_revocation -- --ignored
```

The security workflow runs this test against its own ClickHouse service on PRs.
It covers both OIDC and SAML session provenance, mapping changes, trust-setting
changes, stale callback revisions, bearer rotation, upgrades, and local-session
isolation. Separate unit tests cover identity lookup and revision fingerprints.
