# Wide Query API

Observability backend that ingests and queries traces, logs, and metrics. Accepts data from OpenTelemetry collectors, Datadog agents, and direct API calls. Stores everything in ClickHouse.

## Quick Start

### Local development (recommended)

Runs query-api natively with ClickHouse in Docker:

```bash
make dev
```

This starts ClickHouse in Docker, waits for it to be healthy, then runs the query-api on `http://localhost:8080`.

For auto-reload on code changes:

```bash
make watch
```

### Everything in Docker

Runs both query-api and ClickHouse in Docker:

```bash
make up-full
```

### ClickHouse only

Start the database, then run the API yourself:

```bash
make up
make run
```

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make dev` | Start ClickHouse in Docker + run query-api locally |
| `make watch` | Same as dev but auto-reloads on code changes |
| `make up` | Start ClickHouse only |
| `make up-full` | Start ClickHouse + query-api in Docker |
| `make down` | Stop all Docker services |
| `make logs` | Tail Docker compose logs |
| `make build` | Build debug binary |
| `make release` | Build optimized release binary |
| `make test` | Run tests |
| `make lint` | Run clippy lints |
| `make fmt` | Format code |
| `make clean` | Remove build artifacts |
| `make clean-all` | Remove build artifacts + Docker volumes |

## Configuration

Environment variables (defaults in `.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_URL` | `http://localhost:8123` | ClickHouse HTTP endpoint |
| `CLICKHOUSE_DATABASE` | `observability` | Database name (auto-created) |
| `CLICKHOUSE_USER` | `default` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | _(empty)_ | ClickHouse password |
| `RUSH_CONFIG_DB` | `./rush_config.db` | SQLite config database path |
| `RUST_LOG` | — | Log level filter |

## Ingestion Endpoints

| Endpoint | Protocol | Source |
|----------|----------|--------|
| `PUT /datadog/v0.4/traces` | msgpack | dd-trace libraries |
| `POST /datadog/api/v0.2/traces` | protobuf | Datadog agent |
| `POST /v1/traces` | OTLP JSON | OpenTelemetry collectors |
| `POST /v1/logs` | OTLP JSON | OpenTelemetry collectors |
| `POST /v1/metrics` | OTLP JSON | OpenTelemetry collectors |
| `POST /datadog/api/v2/series` | JSON | Datadog agent metrics |
| `POST /datadog/api/v2/logs` | JSON | Datadog agent logs |
| `POST /api/v1/write` | Prometheus remote write | Prometheus |

## Query API

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/traces/:id` | Get trace by ID |
| `POST /api/v1/query` | Query spans |
| `POST /api/v1/query/count` | Count spans |
| `POST /api/v1/query/group` | Group-by aggregations |
| `POST /api/v1/query/timeseries` | Time-bucketed series |
| `POST /api/v1/logs` | Query logs |
| `POST /api/v1/logs/count` | Count logs |
| `GET /api/v1/suggest/:field` | Field value autocomplete |
| `GET /api/v1/services` | List services |
| `GET /api/v1/services/graph` | Service dependency graph |
| `GET /api/v1/usage` | Ingestion usage stats |

## Metrics (Prometheus-compatible)

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/prom/query` | PromQL instant query |
| `GET /api/v1/prom/query_range` | PromQL range query |
| `GET /api/v1/prom/series` | Series metadata |
| `GET /api/v1/prom/labels` | Label names |
| `GET /api/v1/prom/label/:name/values` | Label values |

## Management API

| Endpoint | Description |
|----------|-------------|
| `GET/POST /api/v1/dashboards` | Dashboard CRUD |
| `GET/POST /api/v1/alerts` | Alert CRUD |
| `GET/POST /api/v1/alert-channels` | Notification channels |
| `GET/POST /api/v1/slos` | SLO management |
| `GET/POST /api/v1/anomaly-rules` | Anomaly detection rules |
| `GET/POST /api/v1/deploys` | Deploy markers |
| `GET/POST /api/v1/settings/api-keys` | API key management |
| `POST /api/v1/rum/ingest` | Real User Monitoring |

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌────────────┐
│  DD Agent    │────▶│             │     │            │
│  OTEL Col.  │────▶│  query-api  │────▶│ ClickHouse │
│  dd-trace   │────▶│  :8080      │     │  :8123     │
│  Prometheus  │────▶│             │     │            │
└─────────────┘     └──────┬──────┘     └────────────┘
                           │
                    ┌──────┴──────┐
                    │   SQLite    │
                    │  config.db  │
                    └─────────────┘
```

- **ClickHouse** stores traces (`otel_traces`), logs (`otel_logs`), metrics, and materialized views (`wide_events`)
- **SQLite** stores dashboards, alerts, SLOs, API keys, and UI configuration
- **Migrations** run automatically on startup, creating all tables and views

## Project Structure

```
src/
├── main.rs                # HTTP server, routes, startup
├── lib.rs                 # Shared types (AppState)
├── config.rs              # Configuration loading (env + TOML)
├── migrations.rs          # ClickHouse schema migrations
├── anomaly_engine.rs      # Anomaly detection engine
├── handlers/
│   ├── traces.rs          # Trace query API
│   ├── dd_traces.rs       # Datadog trace ingestion (protobuf + msgpack)
│   ├── dd_metrics.rs      # Datadog metrics ingestion
│   ├── dd_logs.rs         # Datadog log ingestion
│   ├── dd_common.rs       # Shared DD helpers (auth, decompression)
│   ├── logs.rs            # Log query API
│   ├── metrics.rs         # PromQL-compatible metrics API
│   ├── remote_write.rs    # Prometheus remote write
│   ├── query.rs           # Generic span query engine
│   ├── rum.rs             # Real User Monitoring
│   ├── dashboards.rs      # Dashboard CRUD
│   ├── alerts.rs          # Alert rules + notification channels
│   ├── anomalies.rs       # Anomaly detection rules + events
│   ├── slos.rs            # SLO management
│   ├── services.rs        # Service catalog + dependency graph
│   ├── suggest.rs         # Field value autocomplete
│   ├── usage.rs           # Ingestion usage reporting
│   ├── deploys.rs         # Deploy markers
│   ├── settings.rs        # API key management
│   ├── stats.rs           # System stats
│   └── health.rs          # Health check
├── models/                # Request/response data structures
└── bin/
    └── anomaly_engine.rs  # Anomaly detection (separate binary)
```

## Binaries

- **wide-query-api** — main HTTP server
- **wide-anomaly-engine** — background anomaly detection (run separately with `make run-anomaly`)
