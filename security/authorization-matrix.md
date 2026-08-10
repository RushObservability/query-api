# Query API authorization regression matrix

This matrix records the outer authorization contract shared by every route.
Individual control-plane handlers add `require_auth`, `require_write`, or
`require_admin`; the regression gate checks those sensitive handler families.

| Surface | Anonymous/open tenant | Anonymous/locked tenant | Browser session | Query key | Ingest key |
|---|---|---|---|---|---|
| Query, coordinated Explore search, logs, traces, services, PromQL, Jaeger | Allow | Deny | Allow | Allow for bound tenant | Deny |
| Export | Allow only when query access is explicitly open | Deny | Allow and audit | Allow for bound tenant and audit | Deny |
| Telemetry ingest with `ingest_auth_required=false` | Allow | Route-specific | Deny as the wrong credential | Deny as the wrong credential | Allow |
| Telemetry ingest with `ingest_auth_required=true` | Deny | Deny | Deny as the wrong credential | Deny as the wrong credential | Allow for key tenant/signal/source |
| Natural-language parse | Deny | Deny | Allow subject to LLM quotas | Deny | Deny |
| User-owned session reads/revocation | Deny | Deny | Allow for the same user | Deny | Deny |
| Administrative reads and mutations | Deny | Deny | Admin only | Deny | Deny |
| Tenant switch | Select open tenant only | Deny | Admin: any enabled tenant; other roles: group grants only | Fixed to key tenant | Fixed to key tenant |
| Login and SSO bootstrap endpoints | Explicitly public with Origin/replay controls where state changes | Same | Same | Same | Same |
| CSP reports, health, readiness, metrics | Explicitly public and bounded | Same | Same | Same | Same |

Route inventory is extracted from `src/main.rs` by
`scripts/check-security-regressions.sh`. Any new route family must be added to
that gate and this matrix before CI accepts it.
