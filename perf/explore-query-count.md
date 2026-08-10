# Explore coordinator query-count fixture

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

The response includes `query_stats.clickhouse_queries`, matched rows, bounded
logical matched bytes, response bytes, and time to first results. Query IDs use
the `rush-explore-<request UUID>-{rows,summary}` form, allowing physical
`read_rows` and `read_bytes` to be correlated from ClickHouse `system.query_log`
without adding an instrumentation query to the user request.

Production p95 and physical bytes-read comparisons require the deterministic
data fixture tracked by QFP-PERF-01. Until that fixture exists, this document
makes only the directly enforced query-count claim.
