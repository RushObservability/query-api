# SAML constraints and durable replay protection

SAML sign-in supports the Web Browser SSO bearer profile. Every audience
restriction must allow Rush's entity ID. Multiple audiences inside one
restriction are alternatives; separate restrictions must all pass.

The accepted subject confirmation must use the bearer method, match the ACS URL
and request ID, and have an unexpired `NotOnOrAfter`. A `NotBefore` attribute on
bearer confirmation data is rejected, as required by the browser profile.
Holder-of-key alone is not sufficient. A valid bearer alternative is accepted.

Only `AudienceRestriction` conditions are supported. Other conditions, including
custom conditions, `OneTimeUse`, and `ProxyRestriction`, are rejected rather than
ignored. Configure the IdP to issue the supported profile. The parser preserves
restriction boundaries, checks namespace and element placement, and rejects
condition type overrides.

References: [OASIS SAML profiles](https://docs.oasis-open.org/security/saml/v2.0/saml-profiles-2.0-os.pdf)
and [approved SAML errata](https://docs.oasis-open.org/security/saml/v2.0/sstc-saml-approved-errata-2.0.html).

## Production upgrade requirement

`RUSH_SSO_REPLAY_STORE=auto` now selects Keeper in production, even with one API
replica. `RUSH_ENVIRONMENT` defaults to production when absent. Explicit `local`
replay mode is rejected in production and with multiple replicas.

Before deploying, configure ClickHouse Keeper and `keeper_map_path_prefix`.
Keeper logs and snapshots must use persistent storage. API startup fails if it
cannot initialize the durable replay store; it does not fall back to memory.
This applies at startup even when SSO is not yet configured, since the same
claim store also protects setup links and session rotation.

All API replicas must share the same ClickHouse/Keeper claim store. KeeperMap
strict, synchronous inserts allow only one caller to consume a key. Replay
claims explicitly disable async insertion so two duplicate claims cannot be
combined into a batch that acknowledges both callers. Claims survive API
restarts and remain until expiry. Do not delete Keeper state during an upgrade.

Single-instance development can use:

```sh
RUSH_ENVIRONMENT=development RUSH_SSO_REPLAY_STORE=local make dev
```

This mode intentionally loses replay claims on restart and logs a warning.

## Test with disposable ClickHouse and Keeper

From the query-api repository:

```sh
docker run --rm --name rush-saml-test -p 127.0.0.1:18124:8123 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/tests/fixtures/clickhouse-keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
  clickhouse/clickhouse-server:26.6.1.1193
```

In another terminal:

```sh
RUSH_ENVIRONMENT=production RUSH_SSO_REPLAY_STORE=auto \
RUSH_TEST_CLICKHOUSE_URL=http://localhost:18124 \
RUSH_SESSION_HMAC_SECRET=test-only-session-key-not-for-deployment \
cargo test --test sso_replay_durability -- --ignored
```

The opt-in test reconstructs API state after consuming claims, checks that
replays remain blocked, and races two independent clients for one claim. Use
only a disposable database. Unit tests sign malformed assertions with a trusted
test key to prove that constraint checks reject them after signature validation.
