#!/usr/bin/env bash
set -euo pipefail

report="${1:-trivy-report.json}"
summary="${GITHUB_STEP_SUMMARY:?GITHUB_STEP_SUMMARY must be set}"

{
  echo '### Container vulnerability report'
  echo
  echo 'Trivy is advisory and does not block this release.'
  echo 'Scope: fixable HIGH and CRITICAL vulnerabilities in the pushed image digest.'
  echo
} >> "$summary"

# A scanner failure or malformed report is not a clean scan.
if [[ "${TRIVY_OUTCOME:-failure}" != success ]] || ! jq -e '
  type == "object" and .SchemaVersion == 2 and
  (.Results == null or (.Results | type == "array"))
' "$report" >/dev/null 2>&1; then
  echo '::warning title=Trivy scan unavailable::Trivy did not complete successfully. See the scan logs. The release will continue; vulnerability status is unknown.'
  echo '**Scan unavailable.** Vulnerability status is unknown. See the Trivy step logs.' >> "$summary"
  exit 0
fi

high="$(jq '[.Results[]?.Vulnerabilities[]? | select(.Severity == "HIGH")] | length' "$report")"
critical="$(jq '[.Results[]?.Vulnerabilities[]? | select(.Severity == "CRITICAL")] | length' "$report")"

{
  echo '| Severity | Findings |'
  echo '|---|---:|'
  echo "| Critical | $critical |"
  echo "| High | $high |"
  echo
  echo 'Download the Trivy JSON artifact from this run for package names, CVEs, installed versions, and fixes.'
  echo 'The SPDX SBOM is generated separately and attached to the release.'
} >> "$summary"

if (( high + critical > 0 )); then
  echo "::warning title=Trivy vulnerability findings::$critical critical and $high high fixable findings. See the job summary and Trivy JSON artifact. These findings do not block the release."
else
  echo 'No fixable HIGH or CRITICAL vulnerabilities found.' >> "$summary"
fi
