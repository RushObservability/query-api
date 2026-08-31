#!/usr/bin/env bash
set -euo pipefail

failures=0
llm_handlers=(
  src/handlers/parse_query.rs
  src/handlers/parse_promql.rs
  src/handlers/anomalies.rs
  src/handlers/settings.rs
)

if matches=$(rg -n 'OPENAI_(?:BASE_URL|API_KEY)|reqwest::Client' "${llm_handlers[@]}"); then
  printf 'LLM handlers must use AppState::llm_gateway, not provider configuration or clients:\n%s\n' "$matches" >&2
  failures=1
fi

for parser in src/handlers/parse_query.rs src/handlers/parse_promql.rs; do
  if ! rg -q 'require_auth\(&state, &headers\)' "$parser"; then
    printf '%s must require an interactive authenticated session\n' "$parser" >&2
    failures=1
  fi
done

if ! rg -q 'pub llm_gateway: llm_gateway::LlmGateway' src/lib.rs; then
  printf 'AppState must own the shared bounded LLM gateway\n' >&2
  failures=1
fi

if ! rg -q 'LlmGateway::from_env' src/main.rs; then
  printf 'The LLM gateway must be validated during startup\n' >&2
  failures=1
fi

if (( failures != 0 )); then
  exit 1
fi

printf 'LLM gateway architecture checks passed.\n'
