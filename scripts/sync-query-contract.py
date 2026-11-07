#!/usr/bin/env python3
"""Generate the v1 JSON Schema; optionally sync standalone client contract copies.

  python3 scripts/sync-query-contract.py --check
  python3 scripts/sync-query-contract.py --workspace ..
  python3 scripts/sync-query-contract.py --workspace .. --check

No network, dependencies, or runtime paths to sibling repositories.
"""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def schema(registry):
    scalar = {"type": ["string", "number", "boolean"]}
    option_types = {
        "limit": {"type": "integer", "minimum": 1, "maximum": 2000},
        "offset": {"type": "integer", "minimum": 0, "maximum": 10000},
        "cursor": {"type": ["string", "null"], "maxLength": 8192},
        "columns": {"enum": ["list", "wide", None]},
        "slim": {"type": "boolean"},
        "display_fields": {"type": "array", "items": {"type": "string"}},
        "include_rows": {"type": "boolean"}, "include_summary": {"type": "boolean"},
        "include_heatmap": {"type": "boolean"},
        "interval": {"enum": ["1s", "10s", "1m", "5m", "15m", "1h", "1d"]},
        "aggregation": {"type": "string"},
        "group_by": {"oneOf": [{"type": ["string", "null"]}, {"type": "array", "items": {"type": "string"}}]},
        "x_field": {"type": "string"}, "y_field": {"type": "string"},
        "query": {"type": "string", "minLength": 1, "maxLength": 16384},
        "step": {"type": "string", "minLength": 1, "maxLength": 32},
    }
    variants = []
    for route in registry["routes"]:
        options = {k: dict(option_types[k]) for k in route["options"]}
        if "limit" in options:
            options["limit"]["maximum"] = {"patterns": 50, "scatter": 2000}.get(route["operation"], 1000)
        if "group_by" in options:
            options["group_by"] = {"type": ["string", "null"]} if route["operation"] in ("timeseries", "explore") else {"type":"array", "items":{"type":"string"}}
        option_schema = {"type": "object", "additionalProperties": False, "properties": options}
        if route["signal"] == "metrics": option_schema["required"] = ["query"]
        if route["operation"] == "scatter": option_schema["required"] = ["x_field", "y_field"]
        variant = {"properties": {
            "signal": {"enum": ["logs", "spans"]} if route["signal"] == "dynamic" else {"const": route["signal"]},
            "operation": {"const": route["operation"]}, "options": option_schema,
        }}
        if "required" in option_schema: variant["required"] = ["options"]
        variants.append(variant)
    return {"$schema": "https://json-schema.org/draft/2020-12/schema", "title": "Rush query v1",
        "type": "object", "additionalProperties": False,
        "required": ["version", "signal", "operation", "time_range"],
        "properties": {
            "version": {"const": "v1"}, "signal": {"enum": ["logs", "spans", "metrics"]},
            "operation": {"type": "string"},
            "time_range": {"type": "object", "required": ["from", "to"], "properties": {
                "from": {"type": "string", "format": "date-time"}, "to": {"type": "string", "format": "date-time"}}},
            "filters": {"type": "array", "maxItems": registry["max_filters"], "items": {
                "type": "object", "required": ["field", "op", "value"], "properties": {
                    "field": {"type": "string", "minLength": 1, "maxLength": 128, "not": {"const": "tenant_id"}},
                    "op": {"enum": ["=", "!=", ">", ">=", "<", "<=", "LIKE", "NOT LIKE", "IN", "NOT IN"]},
                    "value": {"oneOf": [scalar, {"type": "array", "minItems": 1, "maxItems": 100, "items": scalar}]}}}},
            "search": {"type": ["string", "null"], "maxLength": registry["max_search_bytes"]},
            "options": {"type": "object"},
        }, "oneOf": variants}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, help="directory containing app checkouts")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    registry = json.loads((ROOT / "contracts/query-v1.json").read_text())
    schema_bytes = (json.dumps(schema(registry), indent=2) + "\n").encode()
    outputs = {ROOT / "contracts/query-v1.schema.json": schema_bytes}
    if args.workspace:
        for repo in ("frontend", "rush-cli", "mcp-server", "sre-agent"):
            checkout = args.workspace.resolve() / repo
            if not (checkout / ".git").exists():
                parser.error(f"not a checkout: {checkout}")
            for name in ("query-v1.json", "query-v1.fixtures.json"):
                outputs[checkout / "contracts" / name] = (ROOT / "contracts" / name).read_bytes()
            target = "src/lib/queryContract.ts" if repo == "frontend" else "src/query_contract.rs"
            template = "client.ts" if repo == "frontend" else "client.rs"
            outputs[checkout / target] = (ROOT / "contracts" / template).read_bytes()
    stale = []
    for path, data in outputs.items():
        if path.exists() and path.read_bytes() == data: continue
        if args.check: stale.append(str(path))
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
    if stale:
        raise SystemExit("Stale generated contracts:\n" + "\n".join(stale))
    print(f"{'Checked' if args.check else 'Synced'} {len(outputs)} contract files")


if __name__ == "__main__":
    main()
