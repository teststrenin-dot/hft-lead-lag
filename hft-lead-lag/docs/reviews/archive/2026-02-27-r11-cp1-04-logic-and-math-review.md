# R11 CP1 - Logic and Math Review

Date: 2026-02-27

## Findings

### P1
1. Time-window lag math is not rollback-safe when corrected timestamp steps backward.
- Evidence: `src/domain/screener/state.rs:134`, `src/domain/screener/state.rs:138`, `src/domain/screener/tests.rs:271`.
- Status: `open`.

### P2
1. Mixed clock domains (raw-event drift vs corrected-event lag/leader) can produce inconsistent telemetry narratives.
- Evidence: `src/domain/screener/quote_ingest.rs:24`, `src/domain/screener/quote_ingest.rs:57`, `src/domain/screener/utils.rs:73`, `src/domain/screener/state.rs:144`.
- Status: `open`.

2. Offset estimator has quantized correction updates (recompute interval + median selection behavior).
- Evidence: `src/domain/screener/clock_offset.rs:40`, `src/domain/screener/clock_offset.rs:55`, `src/domain/screener/clock_offset.rs:62`.
- Status: `open`.

### P3
1. Timestamp normalization heuristic can collapse replay/synthetic relative clocks into decision-time fallback.
- Evidence: `src/domain/screener/utils.rs:86`, `src/domain/screener/utils.rs:96`, `src/domain/screener/utils.rs:99`, `src/domain/screener/utils.rs:102`.
- Status: `open`.
