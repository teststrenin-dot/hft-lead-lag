# R11 CP1 - Preventive Architecture Review

Date: 2026-02-27

## Findings

### P1
1. Missing invariant: `first_tick_ms` / freshness anchors must stay monotonic in raw event-time domain, independent from correction recalibration.
- Evidence: `src/domain/screener/quote_ingest.rs:35`, `src/domain/screener/quote_ingest.rs:42`, `src/domain/screener/state.rs:83`.
- Status: `open`.

2. Missing invariant: rolling windows must be safe under corrected-time rollback.
- Evidence: `src/domain/screener/quote_ingest.rs:57`, `src/domain/screener/state.rs:134`, `src/domain/screener/state.rs:138`.
- Status: `open`.

### P2
1. Missing observability guardrail for invalid timestamp fallback into offset learning.
- Evidence: `src/domain/screener/utils.rs:61`, `src/domain/screener/utils.rs:64`, `src/domain/screener/utils.rs:145`.
- Status: `open`.

2. Coverage gap: no jump/noise/regime-shift tests for offset estimator.
- Evidence: `src/domain/screener/clock_offset.rs:98`.
- Status: `open`.

3. Coverage gap: step-back tests do not assert `first_tick_ms` / `updated_at_ms` invariants.
- Evidence: `src/domain/screener/tests.rs:271`, `src/domain/screener/tests.rs:308`, `src/domain/screener/tests.rs:713`.
- Status: `open`.
