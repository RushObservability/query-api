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

Every write goes through the same path. If ClickHouse is down or overloaded, batches spill to a durable on-disk spool and replay on recovery; when the spool fills, callers get a `429` instead of silent data loss. An optional object-store (S3/MinIO) buffer makes that backlog survive a pod restart and drain from any replica. A metric firewall can drop or relabel series at ingest before they're ever stored.

**Query.** The Explore search, trace waterfall, service maps, log filters, and a Prometheus-compatible metrics API all compile to ClickHouse SQL in here. Spans land in `spans` (raw OTLP in `spans_raw`, flattened by a materialized view), logs in `logs`, metrics across the `metrics_*` tables.

**Control plane.** Tenants, users, SSO (SAML/OIDC), API keys, RBAC groups, dashboards, alerts, SLOs, anomaly and SIEM detection rules, deploy markers, retention caps — stored in ClickHouse `config_*` tables and driven over the API.

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

Migrations run on startup, so the schema and materialized views are created if they're missing — point a collector at `:8080` and data shows up.

## Configuration

| Variable | Default | |
|---|---|---|
| `CLICKHOUSE_URL` | `http://localhost:8123` | database endpoint |
| `CLICKHOUSE_DATABASE` | `observability` | created on first run |
| `RUSH_API_KEY_SECRET` | _(empty)_ | HMAC key for API-key hashes — set it in production |
| `RUSH_ALLOWED_ORIGINS` | _(same-origin)_ | CORS allowlist |
| `RUSH_SPOOL_DIR` · `RUSH_SPOOL_MAX_BYTES` | `./data/spool` · 2 GiB | durable ingest spool |
| `RUST_LOG` | — | e.g. `rush_api=info` |

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
