# Query diagnostics

`POST /api/v1/explore/diagnose` explains missing data without changing a query.
It uses the same tenant authorization, workload admission and time-range limits
as Explore. Every probe retains the affected tenant and absolute time range.

```json
{
  "signal": "logs",
  "time_range": {"from": "2026-11-06T12:00:00Z", "to": "2026-11-06T13:00:00Z"},
  "filters": [{"field": "service_name", "op": "=", "value": "payment"}]
}
```

For `spans` and `logs`, the check distinguishes an empty range, an excluding
filter, an excluding search term, and a combination of filters. Numeric/text
corrections are reported only if the alternate value actually matches records.
Similar service names come from observations with all other filters retained.

For `metrics`, send `promql` instead of structured filters or search. Named
selectors are checked against both metric tables in the selected range. If the
names have samples, the response offers label, join, rate-window and histogram
guidance. It does not claim to have tested each possible function or label fix.

Responses contain `code`, `message`, `complete`, `checked_filters`, an optional
`filter_index`, and optional service-name `suggestions`. Counts are capped at
10,000 and are described as lower bounds when capped. At most eight filters or
named metric selectors are accepted. The complete check has a three-second
deadline; individual probes also have execution, row and byte limits. A timeout
or database failure returns `complete: false`, never a false claim of no data.

Call this endpoint only when requested by the user. It is a read-only POST and
must not invalidate existing analytics requests or be retried on every refresh.
