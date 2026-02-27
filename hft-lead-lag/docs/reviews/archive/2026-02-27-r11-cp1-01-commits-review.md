# R11 CP1 - Commits Review

Date: 2026-02-27

## Findings

### P1
1. Business-time anchors moved to corrected exchange time, while corrected time can step backward during offset recalibration.
- Evidence: `src/domain/screener/quote_ingest.rs:42`, `src/domain/screener/quote_ingest.rs:56`, `src/domain/screener/clock_offset.rs:39`, `src/domain/screener/clock_offset.rs:46`, `src/domain/screener/tests.rs:271`.
- Impact: age/freshness gates can jump.
- Status: `open`.

### P2
1. CP1 uses global lock for per-exchange offset correction in hot ingest path.
- Evidence: `src/domain/screener/mod.rs:165`, `src/domain/screener/mod.rs:319`, `src/event_loop_runtime.rs:31`.
- Impact: contention/jitter risk under volume.
- Status: `open`.

2. Batch ingest order is nondeterministic (`HashMap` iteration).
- Evidence: `src/event_loop_ingest.rs:30`, `src/main_tests.rs:1259`.
- Impact: replay/debug reproducibility risk.
- Status: `open`.

### P3
1. Symbol state can be allocated before exchange label validation.
- Evidence: `src/domain/screener/quote_ingest.rs:22`, `src/domain/screener/state.rs:122`.
- Impact: catalog churn on malformed labels.
- Status: `open`.
