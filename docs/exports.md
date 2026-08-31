# Bounded data exports

Rush exports Explore logs and spans without accumulating result rows in the
query-api process. CSV and JSON both read one ClickHouse row at a time. JSON is
a valid streaming document whose final fields contain `count` and `truncated`.

## Request lifecycle

`POST /api/v1/logs/export` and `POST /api/v1/query/export` accept the existing
CSV or JSON request. The admin-managed `export_max_rows` setting remains the
absolute row cap.

- Requests at or below `RUSH_EXPORT_SYNC_MAX_ROWS` return a streamed download.
- Larger requests return `202 Accepted` with an opaque job id, `status_url`, and
  expiry. Poll the status URL until it returns `completed`, then GET its
  `download_url`.
- Job status and downloads are restricted to the job's tenant. Filesystem paths,
  filters, free-text searches, and query text are never present in status,
  application logs, or audit changes.
- `DELETE /api/v1/exports/{id}` cancels a queued/running job and deletes any
  partial object. Expired objects are removed by the background janitor.

The web UI handles the `202` workflow automatically.

## Limits and admission

| Control | Default | Purpose |
|---|---:|---|
| `export_max_rows` setting | 1,000 | Absolute per-export row cap; editable in Settings |
| `RUSH_EXPORT_SYNC_MAX_ROWS` | 50,000 | Rows allowed on one synchronous connection |
| `RUSH_EXPORT_MAX_BYTES` | 256 MiB | Hard payload/object byte cap; minimum 1 MiB |
| `RUSH_EXPORT_JOB_TTL_SECONDS` | 3,600 | Job/object lifetime; accepted range 60–86,400 seconds |
| `RUSH_EXPORT_DIR` | OS temp directory + `rush-exports` | Private job-object directory |

Export concurrency and ClickHouse budgets come from the `export` workload class
under **Settings → Query limits**. Its global and per-tenant semaphores cover the
entire streamed response body, asynchronous generation, and protected download.
The same class controls queue timeout, request timeout, time range, rows/bytes
read, result rows, ClickHouse memory/spill limits, and thread count.

The job directory is created with mode `0700` and objects with mode `0600` on
Unix. Mount `RUSH_EXPORT_DIR` on an appropriately encrypted, quota-controlled
volume if exports must survive pod filesystem pressure. Object access must stay
behind query-api; do not serve this directory directly from a web server.

Job metadata is process-local in this implementation. The Helm chart's default
single query-api replica is safe. If query-api is scaled horizontally, keep a
client on the job-owning pod with service/ingress affinity; a pod restart or
failover invalidates that pod's outstanding jobs even when the object directory
is persistent.

## Cancellation and observability

Dropping a synchronous HTTP response drops its admission permit and ClickHouse
cursor. ClickHouse also receives
`cancel_http_readonly_queries_on_client_close=1`. Asynchronous cancellation
drops its cursor between streamed chunks and removes the partial file.

Use these existing low-cardinality governor metrics with `workload="export"`:

- `rush_query_admission_inflight`
- `rush_query_admission_queue_depth`
- `rush_query_admission_total`
- `rush_query_requests_total`

Creation, completion, failure, cancellation, expiry, and protected downloads
write redacted audit events. Query and search contents are deliberately omitted.
