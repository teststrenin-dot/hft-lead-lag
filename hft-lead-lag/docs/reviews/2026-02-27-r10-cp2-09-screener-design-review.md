# R10 CP2 - Screener Design Review

Date: 2026-02-27

## Findings

### P1
1. Partial-book rows can appear "live" without true cross-exchange lag validity.
- Evidence: `src/domain/screener/quote_ingest.rs:42`, `src/domain/screener/catalog_cache.rs:117`, `src/api/handlers.rs:226`.
- Status: `open`.

2. Extreme accepted timestamp can poison monotonic acceptance for a side.
- Evidence: `src/domain/screener/clock_offset.rs:30`, `src/domain/screener/state.rs:86`.
- Status: `open`.

### P2
1. Cycle window fidelity leak via non-expiring open cycle state.
- Evidence: `src/domain/screener/cycle_tracker.rs:49`, `src/domain/screener/cycle_tracker.rs:79`.
- Status: `open`.

### P3
1. Sparse direct tests for cycle-tracker semantics and partial-book/fallback behavior.
- Evidence: `src/domain/screener/tests.rs:171`, `src/api/handlers/tests.rs:40`.
- Status: `open`.
