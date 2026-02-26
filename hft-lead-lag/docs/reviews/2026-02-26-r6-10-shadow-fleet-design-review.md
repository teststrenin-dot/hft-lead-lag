# R6 — Shadow Fleet Design Review

## Findings
- **P0** Portfolio race invariant is broken in runtime assign path (portfolio B starvation).
  - Refs:
    - `src/domain/screener/mod.rs:393`
    - `src/application/services/portfolio_runtime.rs:105`
    - `src/application/services/portfolio_runtime.rs:115`

- **P1** Full-history objective is not restart-safe; candidate continuity is lost after restart.
  - Refs:
    - `src/domain/screener/mod.rs:127`
    - `src/runtime_setup.rs:181`
    - `src/domain/screener/mod.rs:408`

- **P2** Snapshot persistence can become stale under queue reordering without monotonic apply guard.
  - Refs:
    - `src/infrastructure/db.rs:637`
    - `src/infrastructure/db.rs:864`
    - `src/infrastructure/db.rs:936`

- **P2** Archive fail-safe keeps payload but may create unbounded quarantine and manual recovery burden.
  - Refs:
    - `src/trial_queue_io.rs:225`
    - `src/trial_queue_io.rs:228`

## Regression Statement
- No failing automated tests in current range, but shadow-fleet operability has material runtime invariants to fix before claiming production-grade behavior.
