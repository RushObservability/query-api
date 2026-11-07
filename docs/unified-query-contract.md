# Unified query contract

`POST /api/v1/query/execute` is the query boundary for the frontend, CLI, MCP
server and SRE agent. Deploy query-api before the updated clients. There is no
automatic retry against legacy endpoints, so a failed query runs only once.
The existing endpoints, including the Prometheus-compatible GET/form APIs,
remain available to older clients and external tools.

```json
{
  "version": "v1",
  "signal": "logs",
  "operation": "search",
  "time_range": {
    "from": "2026-11-07T10:00:00.000Z",
    "to": "2026-11-07T11:00:00.000Z"
  },
  "filters": [{"field": "service_name", "op": "=", "value": "payments"}],
  "search": "timeout",
  "options": {"limit": 100, "slim": true}
}
```

Send the authenticated session/API key and `X-Rush-Tenant` as before. Tenant scope
comes exclusively from authentication and the tenant middleware, never the body.
Internal SRE reads retain their internal token and explicit investigation tenant.
This endpoint does not create or mutate resources or add telemetry queries to
the user-activity audit log.

The common fields mean the same thing in every client:

- `version` is required. Only `v1` is supported. Unknown top-level fields fail.
- `signal` is `logs`, `spans` or `metrics`. UI trace mode is a span query with
  its existing root-span filters, not a fourth telemetry signal.
- `time_range` contains absolute RFC3339 bounds. Clients resolve relative
  lookbacks once. Existing query-engine boundary semantics are unchanged.
- `filters` are ANDed. Operators are `=`, `!=`, `>`, `>=`, `<`, `<=`, `LIKE`,
  `NOT LIKE`, `IN` and `NOT IN`. Membership takes a nonempty scalar array of at
  most 100 values. Other operators take a scalar. There are at most 30 filters.
  Fields are at most 128 bytes, values at most 8192 encoded bytes. `tenant_id`
  is reserved. Attribute aliases and search syntax use the existing builders.
- `search` is optional, at most 512 UTF-8 bytes. Metrics put selectors inside
  PromQL. Nonempty search/filters on metrics are rejected, not ignored.
- `options` is an operation-specific object. Unknown options fail rather than
  silently changing a query. `contracts/query-v1.json` lists supported options.

| Signal | Operations |
| --- | --- |
| Logs | search, explore, count, group, histogram, patterns, scatter |
| Spans | search, explore, count, group, timeseries, scatter |
| Metrics | range, instant |

Search defaults to 100 rows, capped at 1000. Offset is capped at 10000; prefer
the returned opaque cursor. Pattern limit is at most 50 and scatter at most 2000.
Pattern/scatter operations support structured filters, not free-text search.
Their existing tighter validation still applies. Explore can request rows and
summaries independently. Time buckets use `1s`, `10s`, `1m`, `5m`, `15m`, `1h`
or `1d`; the existing server-side bucket cap can increase the interval.

For metrics, use `options.query` and, for a range, `options.step` as a positive
seconds/duration string. Instant queries use equal `from`/`to` bounds. The shared
adapters convert existing Unix-second client arguments to RFC3339.

## Results and errors

```json
{
  "version": "v1",
  "signal": "logs",
  "operation": "search",
  "data": {"rows": [], "total": 0, "has_more": false},
  "meta": {
    "request_id": "server-generated-id",
    "time_range": {"from": "2026-11-07T10:00:00Z", "to": "2026-11-07T11:00:00Z"},
    "elapsed_ms": 12,
    "partial": false,
    "warnings": []
  }
}
```

`data` preserves the operation's current result format. Search returns rows;
count returns buckets; range/instant retains the Prometheus response including
`resultType` and series. Exact/capped Explore counts, authenticated cursors,
histogram/facet errors and scatter's `truncated` flag remain intact. `partial`
means a summary failed, not that the returned rows were truncated. Truncation
and count precision remain explicit in `data`. Clients check version, signal
and operation before decoding it. Results over 32 MiB fail, not truncate.

Failures use HTTP status plus
`{"error":{"code":"invalid_query","message":"...","request_id":"..."}}`.
Version mismatches use `unsupported_query_version`. Auth, CSRF, workload and
JSON failures use the same envelope. Internal SQL, query text and backend
causes are never returned in error messages. `Retry-After`, request IDs and
existing admission/time/read budgets are preserved. No tenant/auth policy is
implemented separately inside this dispatcher.

## Updating the contract

The API owns the registry, generated JSON Schema and language adapters in
`contracts/`. `GET /api/v1/query/schema` returns the request schema. Change
the registry and adapter templates in query-api, not their generated copies.

```sh
# From repos/query-api. App repositories can still build independently.
rustfmt --edition 2024 contracts/client.rs
python3 scripts/sync-query-contract.py --workspace ..
python3 scripts/sync-query-contract.py --workspace .. --check
```

Commit the updated copies in each affected repository. Standalone API CI checks
that its schema is current. All three Rust clients and the frontend run the same
JSON fixtures for request serialization and response checks. The dispatcher
has validation and partial-result tests, and transport/browser tests exercise
tenant headers, credentials, cancellation and dashboard query sharing.

This contract covers telemetry search, aggregation and PromQL execution.
Trace detail, profiles, metadata discovery, integrations and resource mutations
keep their dedicated APIs. It is not a raw-SQL endpoint or a cross-signal join.
