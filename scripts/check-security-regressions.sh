#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

failed=0
required_tests=(
  arbitrary_snappy_and_protobuf_never_panic_or_leak_decoder_details
  arbitrary_filter_text_never_panics_or_builds_unbounded_sql
  arbitrary_cursor_tokens_never_panic_or_escape_the_sql_literal
  arbitrary_webhook_style_payloads_never_panic_or_emit_unbounded_labels
  arbitrary_internal_causes_never_enter_public_error_json
  auth_required_ingestion_rejects_anonymous_and_query_credentials
  no_auth_ingestion_accepts_anonymous_across_every_ingest_family
  open_query_tenants_still_require_interactive_auth_for_llm_parsing
)
for test_name in "${required_tests[@]}"; do
  if ! rg -q --fixed-strings "$test_name" src; then
    echo "missing adversarial security regression: $test_name" >&2
    failed=1
  fi
done

# Every literal route must belong to a reviewed external surface. Exact public
# and ingest behavior is asserted by Rust matrix tests; this inventory catches
# a new top-level route that bypasses that review entirely.
while IFS= read -r path; do
  case "$path" in
    /api/v1/*|/api/v2/*|/auth/sso/*|/cloudwatch/firehose/*|/datadog/*|/healthz|/readyz|/metrics|/shutdown|/jaeger/api/*|/prom/api/*|/v1/logs|/v1/metrics|/v1/traces) ;;
    *) echo "unclassified externally reachable route: $path" >&2; failed=1 ;;
  esac
done < <(perl -0777 -ne 'while (/\.route\(\s*"([^"]+)"/g) { print "$1\n" }' src/main.rs | sort -u)

for sensitive_file in users groups tenants sso audit settings integrations; do
  if ! rg -q "require_(auth|write|admin)" "src/handlers/${sensitive_file}.rs" 2>/dev/null; then
    echo "sensitive handler family lacks an explicit authorization guard: $sensitive_file" >&2
    failed=1
  fi
done

if (( failed != 0 )); then
  exit 1
fi
echo 'adversarial parser and authorization regression policy passed'
