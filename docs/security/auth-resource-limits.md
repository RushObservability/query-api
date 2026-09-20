# Auth resource limits and cookie security

## OIDC responses

Discovery documents, JWKS responses, and token responses share a 1 MiB JSON
body limit. The API reads each body in chunks and stops before accumulating more
than the limit, even without Content-Length. A declared oversized body is
rejected before reading it. Outbound request timeouts still apply.

## Password work

Argon2 hashing and verification run on Tokio's blocking pool, not on async
request workers. Each API process permits at most four submitted/running
password jobs across login, password changes, user creation, and initial-admin
creation. Excess work is rejected immediately instead of queued without a bound.

Cancelling an HTTP request does not free the slot until its blocking job finishes.
Unknown, disabled, and SSO-only accounts still take the dummy-hash verification
path. Password length and policy checks remain in place.

When login cannot obtain a slot, it returns HTTP 503 with the existing temporary
authentication failure message. The failed-login audit event records
`password_capacity_exhausted`, without credentials or password hashes. This
resource limit does not replace IP or account login throttling.

## Secure cookies in production

Production startup rejects `RUSH_INSECURE_COOKIES=true` or `1`. An unset or
unrecognized `RUSH_ENVIRONMENT` counts as production. Use HTTPS and leave the
insecure-cookie override unset in production.

For local HTTP development, set both:

```sh
export RUSH_ENVIRONMENT=development
export RUSH_INSECURE_COOKIES=true
```

Local-session, SSO-transaction, and setup cookies use the same environment policy.
Secure mode keeps the existing Secure/HttpOnly attributes and host-prefixed
cookie names; it does not accept the plain development cookie name. Startup
validation runs before database initialization. Runtime cookie helpers also
default to secure mode if they encounter an invalid production configuration.

## Regression coverage

Unit tests cover fragmented JSON, the exact body limit, oversized streams that
never reach EOF, declared oversized bodies, invalid JSON, and stream errors.
Password-worker tests check off-thread execution, saturation, cancellation, and
permit release after a panic. Cookie tests cover production defaults, explicit
development exceptions, shared SSO policy, and startup validation ordering.

Run them with `cargo test --lib`. These tests need no external IdP or database.
