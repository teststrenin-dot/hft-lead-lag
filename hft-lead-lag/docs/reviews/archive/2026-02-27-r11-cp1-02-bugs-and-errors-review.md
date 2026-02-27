# R11 CP1 - Bugs and Errors Review

Date: 2026-02-27

## Findings

### P1
1. Missing strict per-side freshness gate after both sides initialized.
- Evidence: `src/domain/screener/quote_ingest.rs:49`, `src/domain/screener/quote_ingest.rs:56`, `src/domain/screener/state.rs:129`, `src/domain/screener/state.rs:152`, `src/domain/screener/state.rs:201`.
- Status: `open`.

2. Lag-window eviction logic assumes monotonic corrected timestamps.
- Evidence: `src/domain/screener/quote_ingest.rs:57`, `src/domain/screener/state.rs:134`, `src/domain/screener/state.rs:138`, `src/domain/screener/tests.rs:271`, `src/domain/screener/tests.rs:298`.
- Status: `open`.

### P2
1. `first_tick_ms` / `updated_at_ms` are anchored to corrected exchange time.
- Evidence: `src/domain/screener/quote_ingest.rs:42`, `src/domain/screener/quote_ingest.rs:50`, `src/domain/screener/quote_ingest.rs:56`, `src/domain/screener/mod.rs:834`.
- Status: `open`.

2. Drift fields are sticky when outliers are filtered (`None` path keeps previous values).
- Evidence: `src/domain/screener/utils.rs:34`, `src/domain/screener/utils.rs:36`, `src/domain/screener/state.rs:99`, `src/domain/screener/state.rs:115`.
- Status: `open`.

### P3
1. Event-loop drift metric uses processing-time `now_ms`, mixing queue delay and transport drift.
- Evidence: `src/event_loop_ingest.rs:33`, `src/main_tests.rs:1188`, `src/main_tests.rs:1202`.
- Status: `open`.
