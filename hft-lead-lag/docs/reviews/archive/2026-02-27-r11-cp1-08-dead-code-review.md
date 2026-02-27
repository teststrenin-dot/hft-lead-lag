# R11 CP1 - Dead Code Review

Date: 2026-02-27

## Findings

### P3
1. `calculate_ws_drift_ms` appears unused in production flow.
- Evidence: `src/domain/screener/utils.rs:81`.
- Status: `open`.

2. Defensive `Rejected` branch after partial-book guard looks unreachable in current flow.
- Evidence: `src/domain/screener/quote_ingest.rs:49`, `src/domain/screener/quote_ingest.rs:62`.
- Status: `open`.
