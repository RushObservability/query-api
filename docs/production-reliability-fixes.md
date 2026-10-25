# Query API production reliability fixes

The query API now bounds accepted ingest batches, isolates concurrent audit writers, waits for durable audit delivery, and checks session tenant grants on every selection.

## Ingest backpressure

Each table buffer has a row limit and a serialized byte limit. `RUSH_INGEST_BATCH_BYTES` defaults to 8 MiB per table. A request that would reach either limit first flushes the previously accepted rows. If that fails and the durable spool is full, the request receives backpressure without entering the buffer. An oversized request uses synchronous delivery. Requests arriving while that table is flushing receive backpressure immediately, so waiting request bodies cannot form another unbounded queue. Background and shutdown flushes retain previously acknowledged rows on failure or cancellation.

Batching still acknowledges small requests while their rows are in memory. A hard process crash can lose those rows. Set `RUSH_INGEST_BATCH_ROWS=1` or `RUSH_INGEST_BATCH_MS=0` for synchronous insert/spool acknowledgment. Transport failures after an ambiguous ClickHouse response can still cause at-least-once delivery.

## Audit storage and upgrade

New events include a signed `chain_id` unique to the API process lifetime. Sequence numbers are monotonic within that chain. Event identity remains `id`; consumers using sequence numbers as a key must use `(chain_id, seq)`. The list API orders events by timestamp and ID. Verification checks sequence continuity and hashes independently for each chain, and includes `first_broken_chain_id` when verification fails.

The schema migration adds `chain_id` with an empty default. Existing rows keep their original hashes and form the legacy chain. Old outbox JSON without this field is still readable. Startup recovers pending chains before writing a new chain. Processes sharing an outbox directory use an advisory file lock to coordinate delivery and file deletion. Use storage that supports Unix file locks, preserve the outbox directory, and retain previous HMAC keys during rotation.

For the first upgrade to this format, drain and stop all old API processes before starting the new version. Old binaries do not understand independent chains, so a mixed-version rollout or binary downgrade after new-chain events exist is unsupported. Later rollouts between versions that understand `chain_id` can overlap. The separate Helm ReadWriteOnce scheduling limitation still applies.

Old releases running more than one replica forked the legacy chain, because every process appended to it. If an old process's outbox still holds a legacy row that no longer fits the stored legacy chain, startup verifies its HMAC and moves it onto the new process's chain. The moved row keeps its `id`, timestamp, and event fields. A following `audit.outbox_rechain` event records each moved row's legacy sequence and hashes. The move is idempotent by `id`. Conflicts on writer chains still stop startup.

Audit inserts force `async_insert=0` and `wait_for_async_insert=1`, regardless of telemetry ingestion settings. An outbox entry is removed only after a successful synchronous insert or confirmation that the same chain and sequence already hold the same hash.

## Tenant selection

A valid session's saved default must be enabled and permitted by its group grants. If no tenant was explicitly selected and the saved default is no longer permitted, the API chooses an enabled permitted tenant. An explicit unauthorized, disabled, or reserved tenant returns 403; a failed authorization lookup returns 503. Tenant IDs and names resolve to the canonical tenant name.

Session discovery and tenant listing remain accessible through their own authenticated handlers. Admins can manage tenants even when all tenants are disabled, so they can restore access. Denied tenant requests emit `tenant.access_denied` audit events.

## Verification

The ordinary Rust suite includes regressions for concurrent rejected writes, byte limits, recovery, cancellation, independent audit chains, durable insert failures, file locking, and tenant selection.

The audit integration test creates and removes its own database on a disposable ClickHouse server. It exercises an existing-table migration, concurrent writers with separate and shared outboxes, legacy hash verification, and restart recovery:

```sh
RUSH_TEST_CLICKHOUSE_URL=http://127.0.0.1:8123 cargo test --lib audit_migration_concurrency_and_restart_recovery -- --ignored
```

Use an isolated test server. The test never needs a production database.
