# R6 — Duplication, Complexity, Overengineering Review

## Findings
- **P2** `DbWriter` duplicates queue/backpressure orchestration for trades and portfolio snapshots.
  - Refs:
    - `src/infrastructure/db.rs:679`
    - `src/infrastructure/db.rs:738`

- **P2** `assign_without_overlap` is over-generalized for current runtime usage (two candidate lists, but same list passed in production).
  - Refs:
    - `src/application/services/portfolio_runtime.rs:94`
    - `src/domain/screener/mod.rs:393`

- **P3** Per-request DB fallbacks in portfolio handlers duplicate startup hydration semantics.
  - Refs:
    - `src/runtime_setup.rs:181`
    - `src/api/handlers.rs:253`
    - `src/api/handlers.rs:324`

## Simplification Potential
- Extract shared enqueue helper in DB writer.
- Collapse portfolio assignment API to one pool + deterministic split policy.
- Move DB fallback from request path to startup/lazy-once hydration.
