# Threat model: Rush query-api

## What this project does and where untrusted input enters

query-api is the server of Rush Observability, a multi-tenant observability platform built on ClickHouse. One Rust
binary (`rush-api`, `src/main.rs`) ingests telemetry, answers queries, serves the control plane for the web UI and
tools, runs the alert, SLO, anomaly, detection and retention engines, and proxies the SRE agent. `rush-anomaly-engine`
(`src/bin/anomaly_engine.rs`) can run anomaly evaluation separately.

It is meant to be reachable from the internet. Untrusted input enters through:

- **Ingest** (often unauthenticated per tenant, or with an ingest key): OTLP over HTTP (`/v1/*`, protobuf, optionally gzip),
  Datadog Agent traces, metrics and logs (`/datadog/*`, `/api/v2/logs`), Prometheus remote write
  (`/prom/api/v1/write`, snappy), CloudWatch through Firehose (`/cloudwatch/*`), JSON logs from Vector
  (`/api/v1/ingest/logs`), browser RUM and session replay (`/api/v1/rum/*`), CPU profiles, and collector traffic.
- **Queries** (session, query API key, or anonymous on tenants that allow it): Explore and the query builder
  (`src/query_builder.rs`, `src/handlers/explore.rs`), PromQL (`/prom/api/v1/*`), the Jaeger API (`/jaeger/*`),
  BubbleUp, exports, saved queries, and natural-language parsing through the LLM gateway.
- **The control plane** (session or API key, by role): sign-in and lockout, OIDC and SAML SSO (`src/handlers/sso.rs`),
  users, groups, tenants, API keys, settings, monitors, SLOs, anomaly and detection rules (detection rules compile a
  restricted SQL subset in `src/detection_query.rs`), dashboards, notification channels (webhooks, Slack, PagerDuty,
  Rootly, Alertmanager, email), LLM providers, custom SRE agent skills, and Kubernetes, Argo CD and Flux views.
- **Internal SRE agent routes** (`/api/v1/internal/*`), allowed only with `x-rush-internal-token` and limited by
  `internal_sre_route_allowed` in `src/main.rs`.
- **Every header**, including `X-Rush-Tenant`, `Origin`, `X-Forwarded-*` and `Authorization`.

### Identities and tenancy

- Credentials: a `__Host-` session cookie; `rush_ing_` ingest keys (signal allowlist, rate limit, source CIDRs);
  `rush_qry_` query keys with a `viewer` or `write` role and an optional agent grant (never admin); the internal SRE
  token. Keys are stored as HMAC-SHA256 digests (`src/api_key_auth.rs`, `src/clickhouse_config.rs`).
- A key's tenant always wins over `X-Rush-Tenant`. Users reach tenants through groups.
- Tenant isolation is enforced twice: query-api adds tenant predicates, and every telemetry read runs as a separate
  read-only ClickHouse user under per-tenant row policies keyed on the `rush_tenant_id` setting
  (`src/lib.rs` `row_policy_supported`, `tests/tenant_row_policy.rs`).
- Every state change writes an append-only, HMAC hash-chained audit event (`src/audit.rs`).

## Components that matter most / least

Most important, in order:

1. **Tenant isolation** for reads and writes, across every signal, query path, export, saved query, session,
   dashboard, alert and engine.
2. **ClickHouse query construction**: SQL injection through filters, field names, group-by, search, PromQL,
   detection rules, settings or identifiers.
3. **Authentication and authorization**: sessions, API key resolution and roles, the agent grant, SSO (SAML signature
   and assertion validation, OIDC state, nonce and replay), lockout, CSRF via the Origin check, and the anonymous
   per-tenant policies.
4. **Ingest parsers and decompression** that an unauthenticated sender can reach.
5. **SSRF** from notification channels, LLM provider URLs (`src/llm_gateway.rs` has an allowlist and private-address
   checks), GitHub and Kubernetes integrations.
6. **Secrets** in responses, logs and audit events (API keys, SSO client secrets, provider keys, the license key,
   HMAC secrets), and the integrity of the audit chain.

Less important: `perf/`, `examples/`, `scripts/` (CI checks), and the `docker-compose.yml` development stack.

## How to exercise it

- `cargo test --all-targets --all-features` runs the unit tests and the integration tests in `tests/`.
- `.oss-scanner/run-local.sh` starts ClickHouse and query-api in this image with no network, using the production read
  path (separate read user and row policies). It prints the URL (`http://127.0.0.1:8080`) and the admin login.
  - Sign in: `curl -c jar -H 'Origin: http://127.0.0.1:8080' -H 'Content-Type: application/json'
    -d '{"username":"admin","password":"rush-local-admin-password"}' http://127.0.0.1:8080/api/v1/auth/login`
  - Create tenants with `POST /api/v1/tenants` and keys with `POST /api/v1/api-keys` (send the `Origin` header and
    the cookie jar), then ingest and query as different tenants to test isolation.
  - Logs are in `/tmp/rush-local/api/rush-api.log`; stop with `.oss-scanner/run-local.sh --stop`.
- The `#[ignore]` tests in `tests/` need ClickHouse. With `run-local.sh` running, run them with
  `RUSH_TEST_CLICKHOUSE_URL=http://127.0.0.1:8123 RUSH_TEST_CLICKHOUSE_USER=default
  RUSH_TEST_CLICKHOUSE_PASSWORD=rush-local-clickhouse-password RUSH_TEST_CLICKHOUSE_READ_USER=rushquery
  RUSH_TEST_CLICKHOUSE_READ_PASSWORD=rush-local-clickhouse-read-password cargo test --all-features --test
  tenant_row_policy -- --ignored`. Its last assertion currently fails: a read that omits `rush_tenant_id` gets
  ClickHouse's `UNKNOWN_SETTING` error rather than zero rows. Either way the read is refused, so this is known and
  not a finding. The SSO and leader-lease tests also need ClickHouse Keeper, which this image does not run.
- `src/main.rs` builds the router; `requires_csrf_origin`, `should_reject_interactive_llm` and the tenant resolution
  middleware there are good starting points.

## How you rate severity

- **Critical:** reading or writing another tenant's telemetry or configuration; unauthenticated access to any query
  or control-plane data; authentication bypass, including forged SAML or OIDC sign-in; SQL injection into ClickHouse;
  remote code execution; a viewer or API key gaining admin.
- **High:** a query key doing more than its role or grant allows; SSRF to internal addresses; secrets disclosed in
  responses, logs or audit events; a crash or memory exhaustion that an unauthenticated ingest request can trigger;
  tampering with the audit chain without detection; stored content that reaches the UI as script (the frontend is a
  separate repository, but report the server side if it marks content as safe HTML).
- **Medium:** denial of service that needs an authenticated user or a query key; CSRF on a state-changing route;
  information leaks within a tenant across roles (a viewer reading admin-only settings).
- **Low:** issues that need an admin, unless they reach the host or another tenant; missing hardening without a
  demonstrated impact.

## Anything to leave alone

- Development-only switches, which query-api refuses outside development, local or test environments:
  `RUSH_ALLOW_INSECURE_TENANT_READS`, `RUSH_INSECURE_COOKIES`, `RUSH_LLM_ALLOW_INSECURE_PRIVATE`. `run-local.sh` sets
  `RUSH_ENVIRONMENT=development` and `RUSH_INSECURE_COOKIES=true` for plain HTTP; do not report those choices.
- The fixed passwords and secrets in `run-local.sh` and `docker-compose.yml`, which exist only for local use.
- Anonymous ingest or queries on a tenant whose admin turned authentication off; that is a supported setting.
- ClickHouse server vulnerabilities, unless query-api lets an attacker reach them.
