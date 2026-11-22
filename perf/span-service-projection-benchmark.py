#!/usr/bin/env python3
"""Benchmark the actual span schema in a disposable database. No third-party packages.

Set RUSH_TEST_CLICKHOUSE_URL, RUSH_TEST_CLICKHOUSE_USER and
RUSH_TEST_CLICKHOUSE_PASSWORD. The user needs database/user creation and system
query-log access. The server must allow rush_ custom settings, as Rush does.
"""

import argparse
import base64
import json
import math
import os
from pathlib import Path
import re
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


PROJECTION = "idx_spans_by_service"


def query(sql, user=None, settings=None):
    params = {"wait_end_of_query": "1", **(settings or {})}
    url = os.environ["RUSH_TEST_CLICKHOUSE_URL"].rstrip("/")
    user = user or os.environ.get("RUSH_TEST_CLICKHOUSE_USER", "default")
    password = "" if user == READER else os.environ.get("RUSH_TEST_CLICKHOUSE_PASSWORD", "")
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    request = urllib.request.Request(
        f"{url}/?{urllib.parse.urlencode(params)}",
        data=sql.encode(),
        headers={"Authorization": f"Basic {auth}"},
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            body = response.read().decode()
            summary = json.loads(response.headers.get("X-ClickHouse-Summary", "{}"))
    except urllib.error.HTTPError as error:
        raise RuntimeError(error.read().decode()) from None
    return body, summary, (time.perf_counter() - started) * 1000


def scalar(sql):
    return int(query(sql)[0].strip())


def insert(table, rows, offset=0):
    return query(
        f"""INSERT INTO {DATABASE}.{table}
            (tenant_id, timestamp, trace_id, span_id, service_name, span_name, kind,
             status, duration_ns, http_method, http_path, http_status_code, attributes)
        SELECT concat('tenant-', toString(number % 4)),
            toDateTime64('{ANCHOR}', 9, 'UTC') - toIntervalSecond(number % 86400),
            hex(MD5(toString(intDiv(number, 3)))), substring(hex(MD5(toString(number))), 1, 16),
            concat('svc-', toString(intDiv(number, 4) % 64)),
            if(number % 3 = 0, 'GET /items/:id', 'db.select'),
            if(number % 3 = 0, 'SPAN_KIND_SERVER', 'SPAN_KIND_CLIENT'),
            if(number % 20 = 0, 'STATUS_CODE_ERROR', 'STATUS_CODE_OK'),
            1000000 + cityHash64(number) % 500000000,
            if(number % 7 = 0, 'POST', 'GET'), concat('/items/', toString(number % 1000)),
            if(number % 20 = 0, 500, 200),
            concat('{{"request.id":"', toString(number), '","db.system":"',
                if(number % 3 = 0, '', 'postgresql'), '","payload":"',
                repeat(hex(cityHash64(number)), 16), '"}}')
        FROM numbers({offset}, {rows})"""
    )[2]


def run(args):
    source = (Path(__file__).resolve().parents[1] / "src/migrations.rs").read_text()
    ddl = re.search(r'r"(CREATE TABLE IF NOT EXISTS observability\.spans\n.*?)",\n', source, re.S)
    if not ddl:
        raise RuntimeError("Could not find the production spans CREATE TABLE")
    fresh = ddl.group(1).replace("observability.spans", f"{DATABASE}.spans")
    projection = re.search(r"\s*PROJECTION idx_spans_by_service \(.*?\n    \),", fresh, re.S)
    if not projection:
        raise RuntimeError("Production schema is missing the span service projection")
    legacy = fresh[:projection.start()] + fresh[projection.end():]
    query(legacy)
    initial_insert_ms = insert("spans", args.rows)
    base_bytes = scalar(f"SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = '{DATABASE}' AND table = 'spans'")
    started = time.perf_counter()
    query(f"ALTER TABLE {DATABASE}.spans ADD {projection.group().strip().rstrip(',')}")
    query(f"ALTER TABLE {DATABASE}.spans MATERIALIZE PROJECTION {PROJECTION} SETTINGS mutations_sync = 1")
    backfill_ms = (time.perf_counter() - started) * 1000
    projection_bytes = scalar(f"SELECT sum(bytes_on_disk) FROM system.projection_parts WHERE active AND database = '{DATABASE}' AND table = 'spans' AND name = '{PROJECTION}'")

    query(f"CREATE USER {READER} IDENTIFIED WITH no_password")
    query(f"GRANT SELECT ON {DATABASE}.spans TO {READER}")
    query(f"CREATE ROW POLICY tenant_isolation ON {DATABASE}.spans FOR SELECT USING notEmpty(getSetting('rush_tenant_id')) AND tenant_id = getSetting('rush_tenant_id') TO {READER}")
    predicate = f"""FROM {DATABASE}.spans PREWHERE tenant_id = 'tenant-0'
        AND service_name = 'svc-0'
        AND timestamp >= toDateTime64('{ANCHOR}', 9, 'UTC') - INTERVAL 6 HOUR
        AND timestamp <= toDateTime64('{ANCHOR}', 9, 'UTC')"""
    quantiles = """(quantiles(0.5, 0.95, 0.99)(duration_ns) AS q)[1] / 1000000.0 AS p50_ms,
        q[2] / 1000000.0 AS p95_ms, q[3] / 1000000.0 AS p99_ms"""
    db = "(" + " OR ".join(f"JSONExtractString(attributes, '{key}') != ''" for key in [
        "db.system", "db.system.name", "db.operation.name", "db.operation", "db.statement", "db.query.summary"
    ]) + ")"
    cases = {
        "count": f"SELECT count() {predicate}",
        "latency_markers": f"SELECT {quantiles}, count() AS total {predicate}",
        "endpoints": f"""SELECT http_method, http_path, count() AS req,
            countIf(status = 'STATUS_CODE_ERROR' OR http_status_code >= 500) AS errors,
            {quantiles} {predicate} WHERE kind = 'SPAN_KIND_SERVER'
            GROUP BY http_method, http_path ORDER BY req DESC, http_method, http_path LIMIT 100""",
        "latency_histogram": f"""SELECT toInt32(floor(log2(greatest(duration_ns / 1000000.0, 0.001)))) AS exp,
            count() AS count {predicate} GROUP BY exp ORDER BY exp""",
        "time_breakdown": f"""WITH kind = 'SPAN_KIND_SERVER' AS is_server, {db} AS is_database
            SELECT toString(toStartOfInterval(timestamp, INTERVAL 60 SECOND)) AS bucket,
            countIf(is_server) AS request_count, avgIf(duration_ns, is_server) / 1000000.0 AS wall_time_ms,
            sumIf(duration_ns, is_database) / 1000000.0 AS database_call_time_ms,
            countIf(is_database) AS database_calls {predicate}
            WHERE is_server OR is_database GROUP BY bucket HAVING request_count > 0 ORDER BY bucket""",
    }
    if args.case:
        cases = {name: cases[name] for name in args.case}
    common = {"rush_tenant_id": "tenant-0", "max_threads": "2", "max_execution_time": "30",
              "use_query_cache": "0", "use_query_condition_cache": "0", "log_queries": "1"}
    report = {"rows": args.rows, "runs": args.runs, "cache": "warm", "tenant_row_policy": True,
              "time_breakdown_projection_planning": args.raw_breakdown_auto_projections,
              "clickhouse_version": query("SELECT version()")[0].strip(),
              "base_bytes": base_bytes, "projection_bytes": projection_bytes,
              "storage_overhead_pct": round(projection_bytes / base_bytes * 100, 2),
              "initial_insert_ms": round(initial_insert_ms, 2), "backfill_ms": round(backfill_ms, 2),
              "queries": {}}
    for name, sql in cases.items():
        samples = {"baseline": [], "projection": []}
        bodies = {}
        for iteration in range(args.runs + 1):
            # Alternate order and discard the first pair to warm both plans.
            modes = ["baseline", "projection"] if iteration % 2 == 0 else ["projection", "baseline"]
            for mode in modes:
                enabled = "1" if mode == "projection" else "0"
                # Match time_breakdown_query in services.rs. This query cannot
                # use the covering index and avoids its measured planning cost.
                if name == "time_breakdown" and not args.raw_breakdown_auto_projections:
                    enabled = "0"
                settings = {**common, "optimize_use_projections": enabled,
                            "optimize_use_projection_filtering": enabled,
                            "query_id": f"{SESSION}-{name}-{mode}-{iteration}"}
                body, summary, wall_ms = query(sql + " FORMAT JSONEachRow", READER, settings)
                bodies[mode] = body
                if iteration:
                    samples[mode].append({"wall_ms": wall_ms,
                        "server_ms": int(summary["elapsed_ns"]) / 1000000,
                        "read_rows": int(summary["read_rows"]), "read_bytes": int(summary["read_bytes"])})
        if name not in {"latency_markers", "endpoints"} and bodies["baseline"] != bodies["projection"]:
            raise RuntimeError(f"Projection changed results for {name}")
        report["queries"][name] = {}
        for mode, measurements in samples.items():
            elapsed = sorted(sample["server_ms"] for sample in measurements)
            report["queries"][name][mode] = {
                "median_ms": round(statistics.median(elapsed), 3),
                "p95_ms": round(elapsed[math.ceil(len(elapsed) * .95) - 1], 3),
                "read_rows": int(statistics.median(sample["read_rows"] for sample in measurements)),
                "read_bytes": int(statistics.median(sample["read_bytes"] for sample in measurements)),
            }

    # Keep insertion comparisons separate from the unchanged query fixture.
    for table, schema in [("insert_baseline", legacy), ("insert_projection", fresh)]:
        query(schema.replace(f"{DATABASE}.spans", f"{DATABASE}.{table}"))
    insert_times = {"baseline": [], "projection": []}
    for iteration in range(4):
        modes = ["baseline", "projection"] if iteration % 2 == 0 else ["projection", "baseline"]
        for mode in modes:
            elapsed = insert(f"insert_{mode}", args.insert_rows, iteration * args.insert_rows)
            if iteration:
                insert_times[mode].append(elapsed)
    report["insert_rows_per_batch"] = args.insert_rows
    report["insert_median_ms"] = {mode: round(statistics.median(values), 2) for mode, values in insert_times.items()}
    query("SYSTEM FLUSH LOGS")
    for name in cases:
        for mode in ("baseline", "projection"):
            metadata = query(f"""SELECT max(memory_usage) AS peak_memory_bytes,
                arrayDistinct(arrayFlatten(groupArray(projections))) AS used_projections
                FROM system.query_log WHERE type = 'QueryFinish'
                AND startsWith(query_id, '{SESSION}-{name}-{mode}-') FORMAT JSONEachRow""")[0]
            report["queries"][name][mode].update(json.loads(metadata))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=2000000)
    parser.add_argument("--runs", type=int, default=11)
    parser.add_argument("--insert-rows", type=int, default=100000)
    parser.add_argument("--case", action="append", choices=("count", "latency_markers", "endpoints", "latency_histogram", "time_breakdown"))
    parser.add_argument("--raw-breakdown-auto-projections", action="store_true",
                        help="bypass the API's projection-planning guard to measure its cost")
    args = parser.parse_args()
    if args.rows < 1000000 or args.runs < 3 or args.insert_rows < 1:
        parser.error("use at least 1000000 rows, 3 runs and 1 insert row")
    SESSION = "rush-span-projection-" + uuid.uuid4().hex
    DATABASE = "test_span_projection_" + uuid.uuid4().hex
    READER = "test_span_reader_" + uuid.uuid4().hex
    ANCHOR = query("SELECT toString(now64(9))")[0].strip()
    query(f"CREATE DATABASE {DATABASE}")
    try:
        print(json.dumps(run(args), indent=2))
    finally:
        try:
            query(f"DROP DATABASE {DATABASE} SYNC")
        finally:
            query(f"DROP USER IF EXISTS {READER}")
