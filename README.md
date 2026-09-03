<div align="center">

# query-api

**The read and write path for [Rush](https://github.com/RushObservability).**

[![release](https://github.com/RushObservability/query-api/actions/workflows/release.yml/badge.svg)](https://github.com/RushObservability/query-api/actions/workflows/release.yml)
![license](https://img.shields.io/badge/license-BUSL--1.1-blue)

</div>

query-api is the Rust service that sits between everything and ClickHouse. Collectors and agents push telemetry to it; the UI and the SRE agent read through it. Nothing else touches the database — which keeps ClickHouse off the network and leaves one place to enforce tenancy, auth, retention, and backpressure.

No separate ingester, no message queue, no second datastore. axum on the front, the `clickhouse` crate on the back, and the config plane lives in ClickHouse too (the `config_*` tables). Most of the code here is SQL generation and authorization; ClickHouse does the heavy lifting.

## What it does

**Ingest — one writer, many wire formats.**

- OpenTelemetry over OTLP/HTTP — `/v1/traces`, `/v1/logs`, `/v1/metrics`
- Datadog agent and `dd-trace` libraries — `/datadog/...` (msgpack traces, JSON logs and metrics)
- Prometheus `remote_write`
- Vector log shipping and RUM beacons

Every write goes through the same path. If ClickHouse is down or overloaded, batches spill to a durable on-disk spool and replay on recovery; when the spool fills, callers get a `429` instead of silent data loss. An optional object-store (S3/MinIO) buffer makes that backlog survive a pod restart and be shared by replicas. In HA, API replicas produce into the shared prefix while exactly one dedicated drain worker replays it; running multiple replayers is rejected because the queue is at-least-once and has no distributed claim protocol. A metric firewall can drop or relabel series at ingest before they're ever stored.

**Query.** The Explore search, trace waterfall, service maps, log filters, and a Prometheus-compatible metrics API all compile to ClickHouse SQL in here. Spans land in `spans` (raw OTLP in `spans_raw`, flattened by a materialized view), logs in `logs`, metrics across the `metrics_*` tables.

**Control plane.** Tenants, users, SSO (SAML/OIDC), API keys, RBAC groups, dashboards, alerts, SLOs, anomaly and SIEM detection rules, deploy markers, retention caps — stored in ClickHouse `config_*` tables and driven over the API.

## Quick start

ClickHouse in Docker, the API on your host with reload:

```bash
make dev      # ClickHouse in Docker + query-api on :8080
make watch    # same, but reloads on change
```

If `../postgres-collector` or `../mysql-collector` is checked out locally,
`make dev` and `make watch` compile the available collectors, enable their
features, and point the API supervisor at the debug binaries. A checkout's
ignored `config.yaml` acts as bootstrap configuration until an API-managed
target exists. Set `LOCAL_COLLECTOR_DIR` or `LOCAL_MYSQL_COLLECTOR_DIR` when a
checkout lives elsewhere.

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

Migrations run on startup, so the schema and materialized views are created if they're missing — point a collector at `:8080` and data shows up.

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
| `RUSH_API_KEY_SECRET` | _(empty)_ | HMAC key for API-key hashes — set it in production |
| `RUSH_BOOTSTRAP_INGEST_API_KEY` | _(unset)_ | Optional 32+ byte ingest key registered idempotently for the default tenant at startup; intended for Helm-managed bootstrap |
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
| `RUSH_SESSION_IDLE_TIMEOUT_SECS` | `1800` | inactivity window for browser sessions; accepted range is 60 seconds through 31 days |
| `RUSH_SESSION_ABSOLUTE_TIMEOUT_SECS` | `86400` | hard browser-session lifetime; must be at least the idle timeout and no more than 31 days |
| `RUSH_SESSION_RENEWAL_INTERVAL_SECS` | `300` | minimum activity interval before the HttpOnly bearer is rotated; must be 30 seconds or more and less than the idle timeout |
| `KUBERNETES_ACCESS_CREDENTIAL_TTL_SECONDS` | `3600` | initial browser-approved kubectl credential lifetime; accepted range is 300 through 43200 seconds. The saved Kubernetes logging setting overrides it for new approvals; API keys are never accepted |
| `KUBERNETES_ACCESS_COLLECT_PRIVATE_IP` | `false` | keep private addresses from authenticated Rush CLI device enrichment; other reported device fields are informational and never used for authorization |
| `RUSH_TRUSTED_PROXY_CIDRS` | _(empty)_ | comma-separated proxy networks allowed to supply `X-Forwarded-For`/`X-Real-IP`; other peers' forwarding headers are ignored |
| `RUSH_SSO_ONLY` | `false` | when `true` and an SSO provider is active, reject local sign-in except for the configured admin break-glass account |
| `RUSH_BREAK_GLASS_USERNAME` | `admin` | canonical username of the local admin retained for emergency access in SSO-only mode; other admins cannot reset its password |
| `INITIAL_ADMIN_PASSWORD` | _(required for a new database)_ | initial administrator seed supplied through a secret; it is never generated or written to application logs |
| `RUSH_INTEGRATION_ENCRYPTION_KEY_ID` | `primary` | non-secret identifier stored with new encrypted integration DSNs |
| `RUSH_INTEGRATION_ENCRYPTION_KEY` | _(required for managed targets)_ | dedicated 32+ byte secret used to encrypt integration DSNs; it never falls back to the API-key secret |
| `RUSH_INTEGRATION_ENCRYPTION_PREVIOUS_KEYS` | _(empty)_ | JSON object of prior key IDs to 32+ byte secrets; retain the pre-key-ID secret as `legacy` during migration |
| `RUSH_COLLECTOR_MANAGER_ENABLED` | `false` | enable API-managed local collector supervision |
| `RUSH_POSTGRES_COLLECTOR_BIN` | `../postgres-collector/target/debug/postgres-collector` | managed PostgreSQL collector executable |
| `RUSH_POSTGRES_COLLECTOR_CONFIG` | _(empty)_ | optional bootstrap YAML when no API-managed target exists |
| `RUSH_MYSQL_COLLECTOR_BIN` | `../mysql-collector/target/debug/mysql-collector` | managed MySQL collector executable |
| `RUSH_MYSQL_COLLECTOR_CONFIG` | _(empty)_ | optional bootstrap YAML when no API-managed MySQL target exists |
| `RUSH_COLLECTOR_API_KEY` | _(empty)_ | tenant ingest key with `logs`, `metrics`, and `collector` signals for managed collectors |
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
| `RUSH_SHUTDOWN_TOKEN` | _(empty)_ | optional token for non-loopback shutdown callers |
| `RUSH_RUNTIME_METRICS_INTERVAL_SECS` | `15` | process/runtime metric sampling interval |
| `RUST_LOG` | — | e.g. `rush_api=info` |

See [Bounded data exports](docs/exports.md) for streaming JSON, asynchronous
jobs, tenant-protected downloads, cancellation, and export workload controls.

Startup fails unless the `rush_` ClickHouse custom-setting prefix, strict row
policies, grants, and the separate read principal all verify. Fresh tenants are
locked. Local Compose explicitly enables the tenant-read and anonymous-default
development overrides; `/healthz` reports both states as insecure.

`GET /metrics` exposes low-cardinality HTTP RED, ingest batch latency and
outcome, per-operation query concurrency/latency/result counts, usage-queue
health, process and Tokio runtime gauges, ingest spool state, and ClickHouse
health. ClickHouse metrics include active queries, merges/mutations, memory,
disk, insert/select counters, and recent query-log latency, read-volume,
result-volume, memory, and error aggregates. The endpoint is intended for an
internal Prometheus path and is not tenant data.

Coordinated Explore requests expose fixed-label
`rush_explore_clickhouse_queries_total`, query-duration/result-row histograms,
matched-row/logical-byte and response-byte histograms, and
`rush_explore_time_to_first_results_ms`. Physical ClickHouse `read_rows` and
`read_bytes` can be correlated in `system.query_log` using the emitted
`rush-explore-<request UUID>-{rows,summary}` query IDs. No tenant, filter, or
search text is used as a metric label.

Authorization hot-path metrics expose fixed-label
`rush_auth_lookup_duration_ms`, `rush_auth_lookup_result_rows`,
`rush_auth_lookups_total`, and `rush_auth_cache_total`. Histogram snapshots add
p50/p95/p99 series. Labels identify only the bounded lookup category and
outcome; user IDs, tenants, bearer fingerprints, routes, and grants are never
included. See [Authorization hot-path performance](docs/authorization-performance.md)
for queries and interpretation.

Ingest limit failures are exposed as
`rush_ingest_limit_rejections_total{source,reason}`. Both labels are fixed
allowlists. `reason` distinguishes `compressed_bytes`, `decompressed_bytes`,
`entity_count`, `decode_concurrency`, and `malformed`; it never includes tenant
names, payload data, or decoder errors.

### Browser sessions

Browser sessions have both an idle deadline and a hard absolute deadline.
Successful authenticated activity renews the idle deadline after the configured
renewal interval and replaces the opaque bearer in the HttpOnly cookie. Renewal
never moves the absolute deadline. Password changes, user disablement, logout,
and manual session revocation are visible immediately because session
authorization is checked against the current user version on every request.
Session rows contain only `hmac-sha256:v1` digests keyed by
`RUSH_SESSION_HMAC_SECRET`; the raw bearer exists only in the issuing response
and HttpOnly cookie. The first upgrade to keyed storage revokes legacy raw or
unkeyed-SHA256 session rows because their bearers cannot be safely converted,
so existing users sign in again once.

Administrators can review and revoke active sessions in **Settings → Users →
Active sessions**. The inventory shows the user, authentication method, last
renewal, and both deadlines; it never returns the session bearer or its storage
hash. Users can also list and revoke their own sessions through
`GET /api/v1/auth/sessions` and `DELETE /api/v1/auth/sessions/{id}`. Admin-wide
controls use the corresponding `/api/v1/auth/admin/sessions` routes. Inventory
reads and revocations are written to the tamper-evident audit log.

### Password policy

Bootstrap, user creation, administrator reset, and self-service password change
all use one server-side policy. New passwords must contain at least 12 Unicode
characters and at least one non-whitespace character, may be up to 1,024 UTF-8
bytes, and must not exactly match the bundled case-insensitive common-password
denylist. Spaces and Unicode are supported for long passphrases. Existing
shorter passwords continue to verify, but the policy applies the next time they
are changed. Rejected password values are never written to logs or audit data.

### Audit delivery and key rotation

Security-sensitive audit calls remain fail-open for the completed business
mutation, as required by the query-api audit contract. Before `log()` returns,
however, the event is serialized into the chain and fsynced to the local audit
outbox. ClickHouse delivery is ordered and retried every five seconds. An
unavailable database or exhausted/unwritable outbox sets `/readyz` to `503` and
exports `rush_audit_degraded`, `rush_audit_outbox_events`,
`rush_audit_outbox_bytes`, `rush_audit_outbox_max_bytes`, and
`rush_audit_write_failures_total`. Telemetry ingestion does not use this path.

For a planned key rotation, keep the old secret in
`RUSH_AUDIT_HMAC_PREVIOUS_KEYS`, set a new `RUSH_AUDIT_HMAC_SECRET`, and change
`RUSH_AUDIT_HMAC_KEY_ID`. The next row starts a new segment linked to the prior
segment's tail. Do not remove a previous key until the associated audit rows
have expired. Startup fails instead of resetting the chain if its ClickHouse
tail cannot be read or a required verification key is missing.

Alert on `rush_audit_degraded == 1` for any sustained period and warn before
capacity exhaustion with
`rush_audit_outbox_bytes / rush_audit_outbox_max_bytes > 0.8`. A degraded
instance is also removed from service by the default `/readyz` probe.

### HA ingest buffering

The local `disk` spool is pod-local. Keep `RUSH_EXPECTED_QUERY_API_REPLICAS=1`
for the default single-pod deployment. For more than one API replica, configure
the shared object-store backend and deploy exactly one drain worker:

```text
# query-api Deployment (all API replicas)
RUSH_BUFFER_BACKEND=object_store
RUSH_BUFFER_REQUIRE_OBJECT_STORE=true
RUSH_EXPECTED_QUERY_API_REPLICAS=3
RUSH_RUN_REPLAYER=false

# dedicated drain worker (one pod only)
RUSH_BUFFER_BACKEND=object_store
RUSH_BUFFER_REQUIRE_OBJECT_STORE=true
RUSH_EXPECTED_QUERY_API_REPLICAS=3
RUSH_DRAIN_WORKER_ONLY=true
RUSH_RUN_REPLAYER=true
```

The API starts only when the selected backend matches this contract; an
object-store initialization failure cannot silently fall back to a pod-local
spool in HA. The object-store queue provides at-least-once delivery. During a
ClickHouse outage, restore ClickHouse, keep the single drain worker running, and
watch `rush_ingest_spool_oldest_age_secs`, `rush_ingest_spool_segments`, and
`rush_ingest_spool_utilization_ratio` until the backlog returns to zero. Do not
scale drain workers horizontally until the queue has a distributed claim/lease
protocol.

### Kubernetes graceful shutdown

`POST /shutdown` is intended for a pod-local `preStop` hook. It immediately
marks `/readyz` unavailable and rejects new application requests, then the
process flushes in-memory batches and waits for the durable spool to reach zero
before exiting. The endpoint accepts loopback callers without authentication;
set `RUSH_SHUTDOWN_TOKEN` if a non-loopback management caller must trigger it.

The published query-api image includes `curl`, so a Deployment can use:

```yaml
lifecycle:
  preStop:
    exec:
      command:
        - /bin/sh
        - -c
        - >-
          exec /usr/bin/curl --fail --silent --show-error --max-time 5
          --request POST http://127.0.0.1:8080/shutdown
```

Give the pod enough `terminationGracePeriodSeconds` for the expected backlog.
If ClickHouse is unavailable, the process keeps retrying instead of claiming a
clean drain; Kubernetes will ultimately enforce the grace-period deadline.

### Tenant and ingest authentication

Query keys are for telemetry read APIs. Ingest keys are separately scoped
to one tenant, one or more of `logs`, `traces`, `metrics`, and `rum`, a requests-
per-minute limit, and optional source IP/CIDR restrictions. Session cookies,
query keys, and pre-migration `legacy` keys are rejected by ingest routes.

Create an ingest key in **Settings → API Keys**, copy it once, and send it as:

```text
Authorization: Bearer rush_ing_...
```

Datadog's `DD-API-KEY` and Firehose's
`X-Amz-Firehose-Access-Key` headers are also accepted, but the stored key must
still be an ingest key with the matching signal and tenant scopes. Source CIDRs
are evaluated against the direct network peer, so deployments behind a proxy or
ingress must allowlist that peer range.

Query and ingest authentication are independent per tenant. Turn off **Query
auth** to allow anonymous reads, **Ingest auth** to accept telemetry without a
key, or both for a fully open tenant. These explicit tenant choices are reported
by `/healthz` and mark `secure=false`, but do not make `/readyz` unhealthy.

Existing tenants without an explicit ingest policy inherit their existing query
authentication setting, so previously open tenants remain open for ingestion.
The global `RUSH_ALLOW_ANONYMOUS_DEFAULT` compatibility override still makes
production readiness unhealthy. Existing API keys migrate to `legacy`
query-only behavior; issue new ingest keys before upgrading secured collectors.

### Managed integrations

The default build is the community build and does not compile optional
collectors. Collector-enabled distributions select features at build time and
still gate each collector with the signed `RUSH_LICENSE_KEY` at runtime.

```bash
# Open-source API
make build

# Collector-enabled builds containing the process supervisor
FEATURES=postgres-collector make build
FEATURES=mysql-collector make build
FEATURES=postgres-collector,mysql-collector make build

# PostgreSQL-enabled container build; GITHUB_TOKEN is consumed as a BuildKit secret
GITHUB_TOKEN="$GITHUB_TOKEN" \
RUSH_POSTGRES_COLLECTOR_VERSION=v0.1.0 \
FEATURES=postgres-collector make docker

GITHUB_TOKEN="$GITHUB_TOKEN" \
RUSH_MYSQL_COLLECTOR_VERSION=v0.1.0 \
FEATURES=mysql-collector make docker
```

When enabled, set `RUSH_COLLECTOR_MANAGER_ENABLED=true`. The API stores
integration targets in its config plane, encrypts DSNs with the dedicated
`RUSH_INTEGRATION_ENCRYPTION_KEY`, and supervises the collector process. For
local development the Makefile supplies a development key. In production, use
a stable 32+ byte secret-manager value and set a distinct key ID. To rotate it,
move the former ID and secret into `RUSH_INTEGRATION_ENCRYPTION_PREVIOUS_KEYS`,
then install a new current ID and key. Keep a pre-key-ID secret under `legacy`
until all older targets have been saved again.

Targets are managed through the admin API:

```text
GET    /api/v1/integrations/registry
GET    /api/v1/integrations/postgresql/targets
POST   /api/v1/integrations/postgresql/targets
PUT    /api/v1/integrations/postgresql/targets/{id}
DELETE /api/v1/integrations/postgresql/targets/{id}

GET    /api/v1/integrations/mysql/targets
POST   /api/v1/integrations/mysql/targets
PUT    /api/v1/integrations/mysql/targets/{id}
DELETE /api/v1/integrations/mysql/targets/{id}
```

The target response never returns the DSN. Target changes are audit logged and
the collector is reconciled immediately, then periodically. Set
`RUSH_POSTGRES_COLLECTOR_BIN` when the collector binary is not at the local
development default, and set `RUSH_COLLECTOR_API_KEY` for locked tenants.
That key must be an ingest key with the `logs`, `metrics`, and `collector`
signals. Existing collector keys without `collector` cannot poll or complete
EXPLAIN jobs and should be replaced.
The private collector release repository is
`RushObservability/postgresql-collector` or `RushObservability/mysql-collector`.

Static config (retention defaults, storage tiering) lives in `rush.toml`, found via `RUSH_CONFIG`.

## Part of Rush

This service is useless on its own — it needs ClickHouse, and it's normally deployed alongside:

- [frontend](https://github.com/RushObservability/frontend) — the web UI
- [sre-agent](https://github.com/RushObservability/sre-agent) — the AI investigator
- [helm-charts](https://github.com/RushObservability/helm-charts) — how the whole thing gets deployed

## Building from source

```bash
git clone https://github.com/RushObservability/query-api
cd query-api
cargo build --release   # or: make release
cargo test              # or: make test
```

## License

[Business Source License 1.1](LICENSE).
