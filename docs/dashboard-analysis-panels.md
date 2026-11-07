# Dashboard analysis panels

Three built-in panel types are available to dashboard readers. Creating, editing,
cloning, importing, and deleting them uses the existing dashboard permissions,
version history, and audit events. No separate integration or license is needed.

## Log patterns

Choose **Log patterns**, select log filters, and set a limit of 1–50 patterns.
The panel groups messages by service, severity, and normalized body. UUIDs become
`UUID` and digit runs become `N`. This is deterministic normalization, not semantic
clustering. Other differences, including hostnames and paths, remain distinct.

Each row shows its event count, share of **all** matching events, and volume across
the dashboard window. Expand a row for the latest example. Examples are capped at
2,048 characters. The drilldown opens Explore with the example trace or service and
the same absolute time range. All severities are included unless filtered out.

`POST /api/v1/logs/patterns` accepts:

```json
{
  "time_range": {"from": "2026-11-07T13:00:00Z", "to": "2026-11-07T14:00:00Z"},
  "filters": [{"field": "service_name", "op": "=", "value": "payments"}],
  "limit": 10
}
```

The response contains `patterns` and `total`. Each pattern contains `service`,
`severity`, `template`, `example`, `trace_id`, `last_seen`, `count`, `buckets`, and
`total_count`. Buckets are `[unix_seconds, event_count]` pairs. `total_count` is
computed before the top-N limit, so displayed shares do not falsely sum to 100%.

## State timeline

Choose **State timeline** and write a PromQL expression returning discrete numeric
values, such as `up`. Each series becomes one row, up to 24 rows. Use Panel options
to map exact numeric values to names and semantic colors. Defaults are `0` =
Unavailable, `1` = Healthy, and `2` = Degraded. Replace these for your metric.

The panel uses the existing PromQL range API. Adjacent equal states merge, but a
sample is held for at most 1.5 query steps. Gaps, missing leading/trailing data,
and non-finite values stay gray. PromQL's own lookback behavior still applies to
the samples the server returns. Unmapped finite values show `Value <number>`.

The whole selected time range stays visible. Select or focus an interval to see
its start, end, and duration. State mappings are saved in `display_config.states`
as `{value, label, color}` objects. Supported colors are green, yellow, red, blue,
and purple. State values must be unique finite numbers.

## Scatter plot

Choose **Scatter plot**, select APM or Logs, and enter two numeric field names.
Both coordinates always come from the same event. The panel does not join
unrelated metric series. Missing, non-numeric, and non-finite pairs are excluded;
real zeroes are retained.

- APM fields: `duration_ms`, `duration_ns`, `http_status_code`, or `attributes.<name>`.
- Log fields: `log.<name>`, `resource.<name>`, `body.<json.path>`, or existing log fields.

For example, compare `attributes.http.request.body.size` with `duration_ms`.
Set axis labels and units under Panel options. Units label the values; they do
not convert them. `duration_ms` explicitly converts stored nanoseconds.

`POST /api/v1/query/scatter` accepts:

```json
{
  "source": "spans",
  "time_range": {"from": "2026-11-07T13:00:00Z", "to": "2026-11-07T14:00:00Z"},
  "filters": [],
  "x_field": "attributes.http.request.body.size",
  "y_field": "duration_ms",
  "limit": 500
}
```

The response contains `points` and `truncated`. Points include numeric `x` and `y`,
timestamp, service, label, trace/span IDs, and an error flag. The limit is 1–2,000.
This is the **latest N matching events**, not a random or representative sample.
The panel explicitly marks capped results. Click a point or use the arrow keys to
inspect it. Span points with a trace ID link to the full trace; log points open
Explore with the captured range.

## Query safeguards

Both new read endpoints use the caller's tenant, existing structured filters,
workload admission and deadlines, and ClickHouse row policies. They reject invalid
RFC3339 windows, windows above the active budget or 30 days, more than 20 filters,
and oversized filter values. Reads are capped at 10 million rows and 512 MiB or
the active workload's tighter limits. Budget exhaustion fails the panel instead
of returning silently partial aggregates. Reads do not emit mutation audit events.

No telemetry schema migration is required. Deploy the API before the frontend.
