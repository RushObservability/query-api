#!/usr/bin/env bash
set -euo pipefail

failures=0

reject_pattern() {
  local description="$1"
  local pattern="$2"
  local matches
  if matches=$(rg -n -U "$pattern" src/handlers); then
    printf 'Unsafe public error pattern: %s\n%s\n' "$description" "$matches" >&2
    failures=1
  fi
}

# A 5xx response must never be built from an internal error formatter. Use
# api_error::internal_legacy while tuple-returning handlers are migrated, or
# return ApiError directly from new handlers.
reject_pattern \
  '5xx response contains error.to_string()' \
  'StatusCode::(?:INTERNAL_SERVER_ERROR|BAD_GATEWAY|SERVICE_UNAVAILABLE|GATEWAY_TIMEOUT),\s*(?:e|error)\.to_string\(\)'
reject_pattern \
  '5xx response formats an internal error value' \
  'StatusCode::(?:INTERNAL_SERVER_ERROR|BAD_GATEWAY|SERVICE_UNAVAILABLE|GATEWAY_TIMEOUT),\s*format!\([^\n]*\{(?:e|error|text|s)(?::[^}]*)?\}'
reject_pattern \
  'writer failure is returned verbatim' \
  'WriteError::Fatal\(s\)\s*=>\s*\(StatusCode::(?:INTERNAL_SERVER_ERROR|BAD_GATEWAY|SERVICE_UNAVAILABLE),\s*s\)'

if ! rg -q 'api_error::public_error_middleware' src/main.rs; then
  printf 'Query API router must install api_error::public_error_middleware\n' >&2
  failures=1
fi

if (( failures != 0 )); then
  exit 1
fi

printf 'Public error checks passed.\n'
