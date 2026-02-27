# R14 CP0 - Bugs and Errors Review

Date: 2026-02-27

## Findings

### P1
1. Docs/runtime route mismatch from prior round is resolved.
- Evidence: `src/api/http_server.rs:122`, `docs/README.md:130`.
- Status: `closed`.

2. `/api/v1/portfolio/active` fallback no longer leaks unknown portfolio IDs.
- Evidence: `src/api/handlers.rs:259`, `src/api/handlers/tests.rs:641`.
- Status: `closed`.

3. Candidate history restore no longer disagrees with runtime event math.
- Evidence: `src/infrastructure/db.rs:735`, `src/infrastructure/db.rs:742`, `src/domain/screener/tests.rs:837`.
- Status: `closed`.

### P2
1. Event-collapse query lacks dedicated composite index for `(symbol, exit_ts_ms)`.
- Evidence: `src/infrastructure/db.rs:735`, `src/infrastructure/db.rs:152`, `src/infrastructure/db.rs:153`.
- Status: `open`.
