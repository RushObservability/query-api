# Logs and spans pagination

Interactive log and span clients should use the opaque `next_cursor` returned by
`POST /api/v1/logs`, `POST /api/v1/query`, or the initial Explore response. Send
that value unchanged as `cursor` on the next request.

The cursor is authenticated and bound to the tenant, signal, projection, time
range, filters, and text search. A modified cursor, or one reused with a changed
query, returns `400 invalid or expired pagination cursor`. Cursors deliberately
do not encode a page number and clients must not parse them.

Logs return `has_more` in addition to `next_cursor`. List requests should use
`slim: true`; fetch the wide record from `POST /api/v1/logs/detail` only when a
row is opened. `POST /api/v1/logs/context` returns a bounded stream around a
selected log without repeated client-side time-window probes.

## Offset compatibility window

`offset` remains accepted for older clients through **2026-11-01**, but it is
deprecated and capped at 10,000 rows. Cursor requests ignore `offset`. New
clients should stop when `next_cursor` is absent (or `has_more` is false for
logs), and restart from page one after changing tenant, time range, filters,
search text, or list projection.
