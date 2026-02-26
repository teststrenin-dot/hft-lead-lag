# R6 — Commits Review

## Scope
- Range: `0ef23f1..a583e39`
- Focus: commit quality, atomicity, regression risk, coverage movement

## What Was Good
- `2005c77` and `a583e39` are focused bugfix commits with direct test reinforcement.
- `bfbae67` added wide test coverage across service, API, screener, and DB paths.

## What Was Weak
- `bfbae67` is oversized and mixes multiple concerns (portfolio runtime, DB schema, API, error-handling behavior).
- Missing commit-level perf guard around hot-path rebalance invocation.

## Findings
- **P2** Large multi-concern commit reduces bisectability and rollback precision.
  - Refs:
    - `src/application/services/portfolio_runtime.rs:1`
    - `src/infrastructure/db.rs:126`
    - `src/api/handlers.rs:490`
- **P2** No commit-level guard for per-tick candidate rebuild cost after rebalance integration.
  - Refs:
    - `src/domain/screener/quote_ingest.rs:85`
    - `src/domain/screener/mod.rs:377`

## Commit Quality Score
- `7.3 / 10`
