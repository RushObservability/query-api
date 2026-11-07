# Dashboard drafts from investigations

`POST /api/v1/sessions/{id}/dashboard-draft` creates an unsaved dashboard proposal.
The caller needs agent access and write permission. Sessions are resolved within
the authenticated tenant. A session in another tenant returns 404.

```json
{"instructions": "Payments latency, errors and CPU", "time_range_minutes": 60}
```

The range may be 5 minutes to 7 days and instructions may contain at most 2048
bytes. The API discovers bounded service and metric catalogs in that range.
Only the investigation title and recent user questions accompany that catalog
to the configured model. Working memory, raw logs and tool results are excluded.

The tenant's enabled default model and provider use the same controlled LLM
gateway as investigations. Installations without configured tenant models use
the existing environment-configured query-parsing gateway. Generation performs
one model request. Gateway quotas, provider routing and outbound controls apply.

The response has `draft`, in the existing dashboard export format, and
`time_range`, the absolute discovery window. Drafts contain 1 to 6 panels using
observed signals and metric names. The server validates structured filters,
PromQL, supported aggregations and panel types. It assigns layout and private
visibility itself. Model-supplied SQL, tenant filters, arbitrary variables and
unknown fields are rejected.

The frontend previews every query against the returned absolute window. It
requires successful previews with data for all included panels. Users can edit
the name, exclude panels, or generate another proposal before explicitly saving
through the existing dashboard import endpoint. The saved dashboard uses the
chosen relative lookback so it remains useful after the investigation.

Generation emits `dashboard.draft_generate` with the session ID and panel count.
Saving emits the existing `dashboard.import` audit event. Neither event records
the prompt, model output, telemetry values or provider credentials.
