# Authorization hot-path performance

Query API validates a browser session in tenant middleware before dispatching a
handler. Protected handlers consume that already-validated identity through a
request-local scope. Nothing is retained after the response, so the next HTTP
request always rechecks the session row, current user version, enabled state,
revocation tombstone, SSO provider binding, and role.

The scope key is the existing HMAC-derived session storage key, never the raw
cookie. Invalid sessions are also reused only for the current request to avoid
repeating the same failed lookup in a handler on an open tenant.

API-key grants deliberately bypass the former 60-second process cache. Every
request resolves the current grant from ClickHouse, so deletion is effective on
the next request across replicas rather than after a cache grace period.

## Evidence

The deterministic `request_auth` fixture models tenant middleware, a protected
handler guard, and a second handler consumer. The old path performs three
session/user/role bundles. The request scope performs one, a 66.7% reduction.
Ordinary protected routes fall from two bundles to one, a 50% reduction.

The revocation-race fixture resolves a session in one request, changes the
backing result to revoked, and proves the next request rejects it. Invalid
sessions are cached only within one request.

## Metrics

`GET /metrics` exposes:

- `rush_auth_lookup_duration_ms{lookup}` — backend latency histogram. The
  self-ingested snapshot also emits `_p50`, `_p95`, and `_p99` series.
- `rush_auth_lookup_result_rows{lookup}` — logical rows returned by the bounded
  authorization lookup.
- `rush_auth_lookups_total{lookup,outcome}` — successful, not-found, and failed
  resolutions.
- `rush_auth_cache_total{lookup,outcome}` — request-scope hit/miss counts.

The fixed lookup values are `session`, `user`, `role`, `tenant_policy`,
`tenant_ingest_policy`, `user_permissions`, and `api_key_grant`. Unknown values
collapse to `other`. No metric label contains a tenant, user, key, token, path,
or raw error.

For physical ClickHouse `read_rows` and `read_bytes`, use `system.query_log` and
group by `normalized_query_hash` for the fixed queries against
`config_sessions`, `config_users`, `config_user_groups`, `config_tenants`, and
`config_api_keys`. Compare the same traffic fixture before and after rollout;
the request-local optimization changes execution count, not query shape.

## Operational checks

Watch session cache hits alongside session lookup count. A protected browser
request should normally produce one session miss and at least one hit. A rising
`not_found` rate indicates expired/revoked cookies or authentication failures;
an `error` rate indicates config-store trouble and should page before optimizing
latency.
