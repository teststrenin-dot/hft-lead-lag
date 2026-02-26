# R6 — Screener Design Review

## Strengths
- Added explicit portfolio runtime representation and persistence integration.
- Added tests for rebalance cadence/no-overlap and restore paths.

## Findings
- **P1** Restart durability regression risk: restored portfolios can be overwritten by empty recomputation due to accumulator-only candidate source.
  - Refs:
    - `src/runtime_setup.rs:181`
    - `src/domain/screener/mod.rs:377`
    - `src/domain/screener/mod.rs:390`
    - `src/domain/screener/mod.rs:408`

- **P2** Hot-path overhead: full candidate rebuild happens before cadence check on each quote update.
  - Refs:
    - `src/domain/screener/quote_ingest.rs:85`
    - `src/domain/screener/quote_ingest.rs:106`
    - `src/domain/screener/mod.rs:377`
    - `src/domain/screener/mod.rs:384`

- **P2** Domain boundary erosion: screener domain depends on app-service and DB record types directly.
  - Refs:
    - `src/domain/screener/mod.rs:37`
    - `src/domain/screener/mod.rs:40`
    - `src/domain/screener/mod.rs:264`

## Verdict
- Request changes before considering screener design stable for long-run operability.
