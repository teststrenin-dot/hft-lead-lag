# R11 CP1 - Architecture and Design Review

Date: 2026-02-27

## Findings

### P1
1. Corrected-time regressions are accepted, but rolling window logic still assumes forward progression.
- Evidence: `src/domain/screener/tests.rs:271`, `src/domain/screener/quote_ingest.rs:57`, `src/domain/screener/state.rs:134`.
- Impact: inconsistent window semantics around offset shifts.
- Status: `open`.

### P2
1. Invalid timestamps are normalized to decision time and then used by offset learning.
- Evidence: `src/domain/screener/utils.rs:62`, `src/domain/screener/quote_ingest.rs:109`, `src/domain/screener/clock_offset.rs:25`.
- Status: `open`.

2. Hot-path offset correction uses global lock.
- Evidence: `src/domain/screener/mod.rs:165`, `src/domain/screener/mod.rs:319`, `src/event_loop_ingest.rs:30`.
- Status: `open`.

### P3
1. Unknown-exchange updates allocate symbol state before rejection.
- Evidence: `src/domain/screener/quote_ingest.rs:22`, `src/domain/screener/state.rs:122`.
- Status: `open`.
