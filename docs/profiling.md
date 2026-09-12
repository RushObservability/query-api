# CPU profiling

Profiling is a free, built-in Rush signal in standard and licensed builds. Open
**Observe → Profiles** at `/profiles`. It is not an integration and has no
entitlement or license configuration.

This first release is a CPU profiling preview. You must run a profiler; existing
traces, metrics, and logs cannot supply call-stack samples retroactively.

## Try the UI with synthetic data

Update and restart query-api to run the new profile table migration, and run the
updated frontend. In Settings, create an **ingest** API key with the `profiles`
signal for your tenant. Query keys cannot ingest profiles.

From the query-api repository, run this in your own terminal:

```sh
export RUSH_PROFILE_ENDPOINT=http://localhost:8080
export RUSH_PROFILE_TENANT=default
read -r -s RUSH_PROFILE_API_KEY
export RUSH_PROFILE_API_KEY
cargo run --example profiles_demo
```

Enter the ingest key at the silent prompt. Open `/profiles`, choose
`rush-profiles-demo`, select version `v2`, and compare with `v1`. The sender emits
synthetic samples every ten seconds until Ctrl-C. These are deliberately named
demo data, not CPU measurements of the sender. Do not commit keys to files.

## Collect real CPU profiles

The included `examples/profiles/collector.yaml` uses the pprof receiver in
OpenTelemetry Collector contrib **0.151.0**. A Go application exposing a private
`/debug/pprof/profile` endpoint can use it. Do not expose pprof to the public
internet; the collector must be able to reach that endpoint.

For a local test workload, run `go run examples/profiles/pprof-demo.go` in a
separate terminal. It exposes a private pprof endpoint on port 6062 and performs
CPU work. Set the collector variables in another terminal:

```sh
export PPROF_ENDPOINT='http://host.docker.internal:6062/debug/pprof/profile?seconds=10'
export PROFILE_SERVICE=my-api
export PROFILE_VERSION=v1
export RUSH_PROFILE_ENDPOINT=http://host.docker.internal:8080
export RUSH_PROFILE_TENANT=default
read -r -s RUSH_PROFILE_API_KEY
export RUSH_PROFILE_API_KEY
docker run --rm --name rush-cpu-profiler \
  -e PPROF_ENDPOINT -e PROFILE_SERVICE -e PROFILE_VERSION \
  -e RUSH_PROFILE_ENDPOINT -e RUSH_PROFILE_TENANT -e RUSH_PROFILE_API_KEY \
  -v "$PWD/examples/profiles/collector.yaml:/etc/otelcol-contrib/config.yaml:ro" \
  otel/opentelemetry-collector-contrib:0.151.0 \
  --feature-gates=service.profilesSupport \
  --config=/etc/otelcol-contrib/config.yaml
```

This example uses Docker Desktop's host address. On Linux, use reachable service
addresses or explicitly configure host access. HTTPS is required outside trusted
local development. Keep this collector separate from the older collector in the
workspace's default development compose stack.

Do not downgrade this example to 0.148.0 through 0.150.0. Their profile scraper
controller drops the shared dictionary, so Rush rejects the resulting export.

An eBPF or runtime profiler can also send the same supported OTLP format. Rush
does not install an eBPF DaemonSet or obtain privileged host access for you.

## Protocol and data

- `POST /v1development/profiles` accepts OTLP/HTTP **protobuf**, with optional gzip.
- The wire schema is pinned to `opentelemetry-proto` **v1.10.0**,
  `profiles/v1development`. Older v1.9 sample field numbers are incompatible.
  This is an alpha protocol, not a promise of compatibility with arbitrary
  collector versions. OTLP JSON, gRPC, and raw pprof uploads are not accepted.
- `service.name` is required. `service.version` and `k8s.pod.name` enable filters.
- CPU nanoseconds are stored as `cpu`. CPU counts with a positive CPU nanosecond
  period are converted to nanoseconds and stored separately as `sampled_cpu`.
  Select the measurement in the UI. Some pprof exports contain both, and Rush
  deliberately does not add them together and double-count the same work.
- Samples with timestamps use those timestamps. Aggregated samples without
  timestamps are assigned the profile start time; a narrow window cannot split
  such a sample accurately.
- Frames arrive leaf-first and are stored root-first. Supplied function names
  and source lines are displayed. Unresolved addresses remain marked unresolved.
  This release does not upload debug artifacts or symbolize binaries server-side.
- Dictionaries without empty index-zero entries are accepted for compatibility
  with the pinned pprof translator. Rush validates the referenced indices.
- Resource metadata other than the three filter fields, scope metadata, and
  sample attributes are not retained. Explicit sample trace/span links are kept.
- Memory, allocation, off-CPU, original-payload profiles, and unsupported sample
  types reject the entire export with 400. Configure a CPU-only collection path.

The shared ingest size, decode-concurrency, and entity limits apply. Expanded
samples also have a byte budget and a 128-frame depth cap. A 429 means retry;
400/413 means correct or reduce the export. Accepted writes use the shared
ClickHouse buffer and replay path. Stable profile IDs support deduplication;
ID-less identical exports get a content-derived ID. Rebatched or reordered
ID-less exports are not guaranteed to deduplicate.

## Tenant controls and retention

Startup upgrades older `profile_samples` tables automatically, including when
running `make watch`. The upgrade adds `profile_type` with a `cpu` default for
existing rows, preserves their data and sorting key, and records `schema.migrate`
in the audit log. It runs before profile ingestion and queries start. Repeated
starts are safe; no manual SQL or database reset is needed.

`profiles` appears alongside the other tenant signal toggles and ingest-key
scopes. Disabling ingestion preserves existing data, accepts and drops new
samples, and records dropped counts, matching the other Rush signal toggles.
Tenant setting changes are audit logged. The telemetry ingest hot path is not.
Usage metering records profiles for operational visibility, not license billing.

Each query uses the authenticated tenant and ClickHouse row policies. The tenant
cannot be supplied in a profile payload to override authentication.

The default TTL is seven days. Configure it in `rush.toml`, then restart query-api:

```toml
[retention.defaults]
profiles_days = 7
```

Profile retention is separate from the existing UI log/APM/metric retention
settings in this preview. TTL cleanup happens during ClickHouse background
merges. There is no per-tenant profile retention override yet.

## Query and comparison behavior

- `GET /api/v1/profiles/series?from=<RFC3339>&to=<RFC3339>` lists available
  service/version/pod/measurement combinations for the tenant and time window.
- `GET /api/v1/profiles` uses the same bounds plus required `service`, optional
  `version`, `pod`, `profile_type`, `trace_id`, and `span_id`. `profile_type`
  defaults to `cpu`; a span filter requires a trace ID. Bounds are inclusive at
  the start and exclusive at the end. Queries are limited to 31 days and run
  through the shared query governor. More than 2,000 distinct stacks returns 422
  rather than a misleading partial flame graph.
- Flame width represents sampled CPU, not elapsed request time. The functions
  table distinguishes self CPU from inclusive CPU. Recursive calls do not count
  the same sample twice in a function's inclusive total.
- Comparisons use self-CPU share, expressed as percentage-point changes. Different
  traffic volumes and workload mixes can change these shares. This is not an
  automatic per-request regression detector. Empty baselines show unavailable.
- The trace and Explore views link to **related service profiles** around a span.
  Those samples can include concurrent requests. Exact attribution requires
  trace/span links in the samples and an explicit trace/span query filter.

## Verification

```sh
cargo test --lib --bins
cargo run --example profiles_demo
```

Frontend tests cover navigation without integrations, flame graph math, search,
comparison, empty/error states, and mobile overflow. Real collector compatibility
must be rechecked when changing the pinned alpha protocol.

The optional `accepts_real_collector_export` Rust test listens on port 18082 for
60 seconds. Point the collector at `http://host.docker.internal:18082` with
`PROFILE_SERVICE=profile-test` while the Go demo runs. Start the test with
`cargo test --lib accepts_real_collector_export -- --ignored --nocapture`.
It checks real CPU values and a `main.work` frame without using your running API.
