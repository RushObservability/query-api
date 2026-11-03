<div align="center">

# query-api

**The read and write path for [Rush](https://github.com/RushObservability).**

[![release](https://github.com/RushObservability/query-api/actions/workflows/release.yml/badge.svg)](https://github.com/RushObservability/query-api/actions/workflows/release.yml)
![license](https://img.shields.io/badge/license-BUSL--1.1-blue)

</div>

query-api is the Rust service between Rush components and ClickHouse. Collectors
and agents send telemetry to it, while the UI and SRE agent query through it.
Keeping direct database access inside this service lets deployments isolate
ClickHouse and enforce tenancy, authentication, retention, and backpressure in
one place.

The service does not need a separate ingester, message queue, or second
datastore. It uses axum for HTTP and the `clickhouse` crate for database access.
Its configuration plane also lives in ClickHouse in the `config_*` tables.

## What it does

### Ingest

- OpenTelemetry over OTLP/HTTP: `/v1/traces`, `/v1/logs`, `/v1/metrics`
- Datadog agent and `dd-trace` libraries: `/datadog/...` for msgpack traces and JSON logs and metrics
- Prometheus `remote_write`
- Vector log shipping and RUM beacons

Every write goes through the same path. If ClickHouse is down or overloaded, batches spill to a durable on-disk spool and replay on recovery; when the spool fills, callers get a `429` instead of silent data loss. An optional object-store (S3/MinIO) buffer makes that backlog survive a pod restart and be shared by replicas. In HA, API replicas produce into the shared prefix while exactly one dedicated drain worker replays it; running multiple replayers is rejected because the queue is at-least-once and has no distributed claim protocol. A metric firewall can drop or relabel series at ingest before they're ever stored.

### Query

Explore search, trace waterfalls, service maps, log filters, and the
Prometheus-compatible metrics API compile requests into ClickHouse SQL. Spans
land in `spans`, with raw OTLP in `spans_raw` and a materialized view flattening
it. Logs use `logs`, and metrics use the `metrics_*` tables.

### Control plane

The API manages tenants, users, SSO through SAML or OIDC, API keys, RBAC groups,
dashboards, alerts, SLOs, anomaly and SIEM detection rules, deploy markers, and
retention caps. It stores this state in ClickHouse `config_*` tables.

### Saved log views

`GET /api/v1/settings/log-views` returns shared views for the active tenant plus
the signed-in user's personal views in that tenant. Each returned view includes
`scope: "tenant" | "personal"`; the response also includes `tenant_id`.
Anonymous requests to deliberately public tenants receive only shared views.

`PUT /api/v1/settings/log-views?scope=tenant` replaces the shared collection and
requires an admin session. `?scope=personal` replaces only the signed-in user's
personal collection in that tenant, including for users with the viewer role.
Omitting `scope` preserves the existing shared-write behavior. Send only the
views from the chosen scope; never send the combined GET collection to PUT.

The tenant comes from the authorized `X-Rush-Tenant` context. Personal ownership
comes from the session, never a supplied user ID. A rejected explicit tenant
selection fails instead of silently saving under the session's default tenant.
Personal views are not included in another user's list, even for admins.
Existing stored views stay shared. Saves and deletions emit `log_view.update`
events with scope, IDs, and counts, without filter values. No new environment
variables or database migrations are needed.

```json
{
  "views": [{
    "id": "33333333-3333-4333-8333-333333333333",
    "name": "Flights",
    "filters": [{ "field": "type", "op": "=", "value": "event_data" }],
    "columns": [
      { "field": "timestamp", "label": "Time" },
      { "field": "log.airline", "label": "Airline" },
      { "field": "log.flight_number", "label": "Flight number" },
      { "field": "log.status", "label": "Status" }
    ]
  }]
}
```

Limits: 50 shared views per tenant and 50 personal views per user per tenant,
20 columns and 20 base filters per view. Base
filters accept string values with `=`, `!=`, `LIKE`, and `NOT LIKE`; all must
match. View IDs must be unique UUIDs within a collection. Names are unique within
each collection, so a personal copy can have the same name as a shared view.

The frontend combines base filters with the user's filters for every log
search. Views do not restrict access to the underlying logs. Log queries accept
`display_fields` on `/api/v1/explore/search` (signal `logs`) and
`/api/v1/logs` with `slim: true`. These return a `DisplayValues` map with
only the requested fields alongside the standard slim row. Fields support
builtins, `log.*`, `resource.*`, and scalar JSON paths such as
`body.flight.number`. Unprefixed attributes check log attributes first, then
resource attributes. The full record remains available through log detail.

## Quick start

ClickHouse in Docker, the API on your host with reload:

```bash
make dev      # ClickHouse in Docker + query-api on :8080
make watch    # same, but reloads on change
```

Or run everything in Docker, or just the database:

```bash
make up-full  # ClickHouse + query-api
make up       # ClickHouse only, then: make run
```

The bundled Compose file is local-development only. Its published ports are
bound to `127.0.0.1`, images use explicit versions, and ClickHouse uses a
nonempty but well-known development credential. Never expose that stack to
another machine. Use the Rush Helm chart with digest-pinned images and
orchestrator-managed secrets for production.

Migrations run on startup, creating any missing schema objects and materialized
views. Send collector traffic to port `8080` to start ingesting data.

## Configuration

| Variable | Default | |
|---|---|---|
| `CLICKHOUSE_URL` | `http://localhost:8123` | database endpoint |
| `CLICKHOUSE_DATABASE` | `observability` | created on first run |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / empty | migration, configuration, and write identity |
| `CLICKHOUSE_READ_USER` / `CLICKHOUSE_READ_PASSWORD` | _(required)_ | distinct SELECT-only identity protected by tenant row policies |
| `RUSH_ALLOW_INSECURE_TENANT_READS` | `false` | explicit single-tenant development override; never enable in production |
| `RUSH_ENVIRONMENT` | `production` | use `development`, `local`, or `test` only for deliberate non-production compatibility |
| `RUSH_BASE_URL` | _(required in production)_ | canonical HTTPS public origin used for OIDC/SAML callbacks and production CSRF target-origin validation; paths, credentials, queries, and fragments are rejected |
| `RUSH_TRUST_PROXY_HEADERS` | `false` | development-only opt-in for deriving a fallback CSRF/SSO origin from forwarded headers; honored only when the direct peer is in `RUSH_TRUSTED_PROXY_CIDRS`, while production always uses `RUSH_BASE_URL` |
| `RUSH_ALLOW_ANONYMOUS_DEFAULT` | `false` | insecure development-only override for anonymous access to the default tenant |
| `RUSH_API_KEY_SECRET` | _(empty)_ | HMAC key for API-key hashes; set it in production |
| `RUSH_BOOTSTRAP_INGEST_API_KEY` | _(unset)_ | optional 32+ byte ingest key registered idempotently for the default tenant at startup; intended for Helm-managed bootstrap |
| `RUSH_SESSION_HMAC_SECRET` | falls back to API-key, then audit HMAC secret | stable 32+ byte HMAC key for one-way browser-session token storage; changing it signs every user out |
| `RUSH_SSO_TRANSACTION_SECRET` | falls back to `RUSH_API_KEY_SECRET` | stable 32+ byte HMAC key for browser-bound OIDC/SAML login transactions |
| `RUSH_AUDIT_HMAC_SECRET` | _(required in production)_ | current 32+ byte audit-chain signing key |
| `RUSH_AUDIT_HMAC_KEY_ID` | `primary` | non-secret identifier written to new audit rows; change it with a planned signing-key rotation |
| `RUSH_AUDIT_HMAC_PREVIOUS_KEYS` | _(empty)_ | JSON object of prior key IDs to 32+ byte secrets, required to verify historical segments after rotation; use `legacy` for rows predating key IDs |
| `RUSH_AUDIT_SPOOL_DIR` | `./data/audit-spool` | fsynced ordered audit outbox; use persistent storage in production |
| `RUSH_AUDIT_SPOOL_MAX_BYTES` | `268435456` | maximum audit outbox bytes before readiness remains degraded and new audit events follow the documented fail-open policy |
| `RUSH_QUERY_API_REPLICAS` | `1` | positive replica count; values above one require a shared SSO replay store |
| `RUSH_SSO_REPLAY_STORE` | `auto` | `auto`, `local`, or `keeper`; `auto` selects KeeperMap for multiple replicas and refuses an unsafe local override |
| `RUSH_CONFIG_ENCRYPTION_KEY` | _(required when SSO or LLM provider secrets exist)_ | stable 32+ byte key for AES-256-GCM encryption of SSO and LLM provider secrets; rotating it requires re-encrypting stored values |
| `RUSH_LOGIN_RATE_LIMIT_SECRET` | falls back to SSO/API-key secret | stable 32+ byte HMAC key for privacy-preserving distributed login-limit identifiers |
| `RUSH_LOGIN_ACCOUNT_LIMIT_PER_MINUTE` | `10` | maximum login attempts against one normalized account per minute across replicas |
| `RUSH_LOGIN_IP_LIMIT_PER_MINUTE` | `50` | maximum login attempts from one resolved client address per minute across replicas |
| `RUSH_SESSION_IDLE_TIMEOUT_SECS` | `7200` | initial inactivity window for browser sessions; Settings → Users → Idle logout overrides it; must be at least 60 seconds and no longer than the absolute lifetime |
| `RUSH_SESSION_ABSOLUTE_TIMEOUT_SECS` | `86400` | hard browser-session lifetime; must be at least the idle timeout and no more than 31 days |
| `RUSH_SESSION_RENEWAL_INTERVAL_SECS` | `300` | minimum activity interval before the HttpOnly bearer is rotated; must be 30 seconds or more and less than the idle timeout |
| `RUSH_TRUSTED_PROXY_CIDRS` | _(empty)_ | comma-separated proxy networks allowed to supply `X-Forwarded-For`/`X-Real-IP`; other peers' forwarding headers are ignored |
| `RUSH_SSO_ONLY` | `false` | when `true` and an SSO provider is active, reject local sign-in except for the configured admin break-glass account |
| `RUSH_BREAK_GLASS_USERNAME` | `admin` | canonical username of the local admin retained for emergency access in SSO-only mode; other admins cannot reset its password |
| `INITIAL_ADMIN_PASSWORD` | _(required for a new database)_ | initial administrator seed supplied through a secret; it is never generated or written to application logs |
| `RUSH_ALLOWED_ORIGINS` | _(empty; cross-origin disabled)_ | Comma-separated exact HTTP(S) browser origins. Invalid, `null`, wildcard, credential-bearing, or path-bearing entries stop startup; production browser mutations must still match `RUSH_BASE_URL` |
| `RUSH_INGEST_MAX_COMPRESSED_BYTES` | `8388608` | maximum wire body accepted by an ingest endpoint before decoding |
| `RUSH_INGEST_MAX_DECOMPRESSED_BYTES` | `33554432` | maximum inflated bytes per ingest request, including cumulative nested CloudWatch records |
| `RUSH_INGEST_MAX_ENTITIES` | `200000` | maximum combined decoded records, points, attributes, labels, and container entities |
| `RUSH_INGEST_MAX_SERIES` / `RUSH_INGEST_MAX_SAMPLES` | `20000` / `200000` | Prometheus and Datadog series/point limits |
| `RUSH_INGEST_MAX_METADATA` | `10000` | Prometheus remote-write metadata-record limit |
| `RUSH_INGEST_MAX_LABELS_PER_SERIES` | `128` | maximum labels on one Prometheus series |
| `RUSH_INGEST_MAX_LABEL_NAME_BYTES` / `RUSH_INGEST_MAX_LABEL_VALUE_BYTES` | `256` / `4096` | UTF-8 byte limits for Prometheus label names and values |
| `RUSH_INGEST_BATCH_ROWS` / `RUSH_INGEST_BATCH_MS` | `5000` / `500` | flush an in-memory table batch when either limit is reached; set rows to `1` or milliseconds to `0` for synchronous writes |
| `RUSH_CLICKHOUSE_ASYNC_INSERT` | `true` | let ClickHouse buffer inserts server-side |
| `RUSH_CLICKHOUSE_WAIT_FOR_ASYNC_INSERT` | `false` | wait for the ClickHouse async-insert flush; enable for stronger delivery confirmation at higher latency |
| `RUSH_INGEST_DECODE_CONCURRENCY` | `4` | process-wide CPU-heavy ingest decode slots; excess requests receive retryable 429 responses |
| `RUSH_EXPORT_SYNC_MAX_ROWS` | `50000` | synchronous streaming ceiling; larger allowed exports become expiring jobs |
| `RUSH_EXPORT_MAX_BYTES` | `268435456` | hard byte cap for synchronous downloads and asynchronous export objects |
| `RUSH_EXPORT_JOB_TTL_SECONDS` | `3600` | lifetime for protected asynchronous export jobs and objects |
| `RUSH_EXPORT_DIR` | OS temp `rush-exports` directory | private export-object directory; never expose it directly |
| `RUSH_SPOOL_DIR` · `RUSH_SPOOL_MAX_BYTES` | `./data/spool` · 2 GiB | durable ingest spool |
| `RUSH_BUFFER_BACKEND` | `disk` | `disk` or shared `object_store` |
| `RUSH_BUFFER_REQUIRE_OBJECT_STORE` | `false` | refuse unsafe fallback to disk |
| `RUSH_EXPECTED_QUERY_API_REPLICAS` | `1` | deployment contract for HA buffering |
| `RUSH_RUN_REPLAYER` | `true` | set `false` on HA API replicas |
| `RUSH_DRAIN_WORKER_ONLY` | `false` | run one shared-buffer drain worker |
| `RUSH_LEADER_ELECTION` | `auto` | `auto`, `local`, or `keeper`; `auto` uses Keeper leases for production or more than one replica, and `local` refuses to start in either |
| `RUSH_LEADER_LEASE_TTL_SECS` | `15` | background-engine lease lifetime, 6 to 300 seconds; renewed every third of it |
| `RUSH_SHUTDOWN_TOKEN` | _(empty)_ | optional token for non-loopback shutdown callers |
| `RUSH_RUNTIME_METRICS_INTERVAL_SECS` | `15` | process/runtime metric sampling interval |
| `RUST_LOG` | _(unset)_ | tracing filter, for example `rush_api=info` |

## Part of Rush

query-api requires ClickHouse and is normally deployed with these Rush
components:

- [frontend](https://github.com/RushObservability/frontend), the web UI
- [sre-agent](https://github.com/RushObservability/sre-agent), the AI investigator
- [helm-charts](https://github.com/RushObservability/helm-charts), the deployment charts

## Building from source

```bash
git clone https://github.com/RushObservability/query-api
cd query-api
cargo build --release   # or: make build-release
cargo test              # or: make test
```

## License

[Business Source License 1.1](LICENSE).
# CPU profiling

Free, built-in CPU profiling is available under **Observe → Profiles**.
See [profiling setup](docs/profiling.md) for the collector configuration, synthetic
demo, supported alpha protocol, tenant controls, and retention.
