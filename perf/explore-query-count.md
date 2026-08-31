# Explore coordinator benchmark

QFP-PERF-03 replaces the default Explore fan-out with a fixed two-query plan:

1. One bounded row query.
2. One `GROUPING SETS` summary scan that returns the exact count, histogram,
   service/status/method facets, and an optional requested group.

The previous default span interaction executed six ClickHouse queries: rows,
capped count, histogram, service facet, status facet, and method facet. The
coordinator executes two, a 66.7% reduction. A requested group remains inside
the same summary scan, so it does not add another ClickHouse request.

Run the deterministic contract fixture with:

```bash
cargo test handlers::explore::tests::query_count_fixture
```

Run the focused ClickHouse before/after benchmark with:

```bash
./perf/explore-benchmark.sh
```

The benchmark anchors a one-hour request window to the newest span for the
selected tenant, warms both plans, alternates their execution order, and runs
their component queries concurrently just as the browser interaction did. Set
`PERF03_TENANT`, `PERF03_RUNS`, `PERF03_WARMUPS`, or
`PERF03_WINDOW_MINUTES` to override its safe defaults. Connection settings use
the normal `CLICKHOUSE_*` environment variables.

The response includes `query_stats.clickhouse_queries`, matched rows, bounded
logical matched bytes, response bytes, and time to first results. Query IDs use
the `rush-explore-<request UUID>-{rows,summary}` form, allowing physical
`read_rows` and `read_bytes` to be correlated from ClickHouse `system.query_log`
without adding an instrumentation query to the user request.

## Acceptance result

The checked-in 2026-08-10 warm-cache comparison used ClickHouse 26.6.1.1193,
20 measured interactions per plan, and 333,250 spans from six services. The
coordinated plan improved:

| Measure | Legacy | Coordinated | Improvement |
|---|---:|---:|---:|
| ClickHouse queries / interaction | 6 | 2 | 66.7% |
| p95 first-results readiness | 93 ms | 66 ms | 29.0% |
| p95 full interaction | 93 ms | 81 ms | 12.9% |
| Average physical rows read | 1,391,388 | 362,767 | 73.9% |
| Average physical bytes read | 15,980,421 | 11,817,751 | 26.0% |

The raw machine-readable result is in
`perf/results/explore-20260810T233944Z59793.json`. QFP-PERF-01 will broaden
this focused comparison into the shared multi-tenant regression suite; it is
no longer required to establish PERF-03's acceptance result.
