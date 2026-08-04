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

If `../postgres-collector` is checked out locally, `make dev` and `make watch`
automatically compile it, enable the PostgreSQL collector feature, and point
the API's collector supervisor at the debug binary. When that checkout also
contains `config.yaml` and no API-managed PostgreSQL target exists yet, the
supervisor uses the local file as a bootstrap configuration. API-managed
targets take precedence. Use `LOCAL_COLLECTOR_DIR=/path/to/postgresql-collector
make watch` for a different checkout location.

Or run everything in Docker, or just the database:

```bash
make up-full  # ClickHouse + query-api
make up       # ClickHouse only, then: make run
```

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
| `RUSH_BASE_URL` | _(required in production)_ | canonical HTTPS public origin used for OIDC/SAML callbacks; paths, credentials, queries, and fragments are rejected |
| `RUSH_TRUST_PROXY_HEADERS` | `false` | development-only opt-in for deriving a fallback scheme from `X-Forwarded-Proto`; production always uses `RUSH_BASE_URL` |
| `RUSH_ALLOW_ANONYMOUS_DEFAULT` | `false` | insecure development-only override for anonymous access to the default tenant |
| `RUSH_API_KEY_SECRET` | _(empty)_ | HMAC key for API-key hashes — set it in production |
| `RUSH_SSO_TRANSACTION_SECRET` | falls back to `RUSH_API_KEY_SECRET` | stable 32+ byte HMAC key for browser-bound OIDC/SAML login transactions |
| `RUSH_CONFIG_ENCRYPTION_KEY` | _(required when SSO secrets exist)_ | stable 32+ byte key for AES-256-GCM encryption of SSO client secrets; rotating it requires re-encrypting stored values |
| `RUSH_LOGIN_RATE_LIMIT_SECRET` | falls back to SSO/API-key secret | stable 32+ byte HMAC key for privacy-preserving distributed login-limit identifiers |
| `RUSH_LOGIN_ACCOUNT_LIMIT_PER_MINUTE` | `10` | maximum login attempts against one normalized account per minute across replicas |
| `RUSH_LOGIN_IP_LIMIT_PER_MINUTE` | `50` | maximum login attempts from one resolved client address per minute across replicas |
| `RUSH_TRUSTED_PROXY_CIDRS` | _(empty)_ | comma-separated proxy networks allowed to supply `X-Forwarded-For`/`X-Real-IP`; other peers' forwarding headers are ignored |
| `RUSH_INTEGRATION_ENCRYPTION_KEY` | _(required for managed targets)_ | stable key used to encrypt integration DSNs |
| `RUSH_COLLECTOR_MANAGER_ENABLED` | `false` | enable API-managed local collector supervision |
| `RUSH_POSTGRES_COLLECTOR_BIN` | `../postgres-collector/target/debug/postgres-collector` | managed PostgreSQL collector executable |
| `RUSH_POSTGRES_COLLECTOR_CONFIG` | _(empty)_ | optional bootstrap YAML when no API-managed target exists |
| `RUSH_COLLECTOR_API_KEY` | _(empty)_ | tenant-scoped API key for managed collector ingest |
| `RUSH_ALLOWED_ORIGINS` | _(same-origin)_ | CORS allowlist |
| `RUSH_SPOOL_DIR` · `RUSH_SPOOL_MAX_BYTES` | `./data/spool` · 2 GiB | durable ingest spool |
| `RUSH_BUFFER_BACKEND` | `disk` | `disk` or shared `object_store` |
| `RUSH_BUFFER_REQUIRE_OBJECT_STORE` | `false` | refuse unsafe fallback to disk |
| `RUSH_EXPECTED_QUERY_API_REPLICAS` | `1` | deployment contract for HA buffering |
| `RUSH_RUN_REPLAYER` | `true` | set `false` on HA API replicas |
| `RUSH_DRAIN_WORKER_ONLY` | `false` | run one shared-buffer drain worker |
| `RUSH_SHUTDOWN_TOKEN` | _(empty)_ | optional token for non-loopback shutdown callers |
| `RUSH_RUNTIME_METRICS_INTERVAL_SECS` | `15` | process/runtime metric sampling interval |
| `RUST_LOG` | — | e.g. `rush_api=info` |

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

# PostgreSQL-enabled build containing the collector supervisor
FEATURES=postgres-collector make build

# PostgreSQL-enabled container build; GITHUB_TOKEN is consumed as a BuildKit secret
GITHUB_TOKEN="$GITHUB_TOKEN" \
RUSH_POSTGRES_COLLECTOR_VERSION=v0.1.0 \
FEATURES=postgres-collector make docker
```

When enabled, set `RUSH_COLLECTOR_MANAGER_ENABLED=true`. The API stores
integration targets in its config plane, encrypts DSNs with
`RUSH_INTEGRATION_ENCRYPTION_KEY`, and supervises the collector process. For
local development the Makefile supplies a development encryption key; use a
stable secret-manager value in production.

Targets are managed through the admin API:

```text
GET    /api/v1/integrations/registry
GET    /api/v1/integrations/postgresql/targets
POST   /api/v1/integrations/postgresql/targets
PUT    /api/v1/integrations/postgresql/targets/{id}
DELETE /api/v1/integrations/postgresql/targets/{id}
```

The target response never returns the DSN. Target changes are audit logged and
the collector is reconciled immediately, then periodically. Set
`RUSH_POSTGRES_COLLECTOR_BIN` when the collector binary is not at the local
development default, and set `RUSH_COLLECTOR_API_KEY` for locked tenants.
The private collector release repository is
`RushObservability/postgresql-collector`.

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
