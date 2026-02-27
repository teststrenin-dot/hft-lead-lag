# R10 CP2 - Commits Review

Date: 2026-02-27

## Findings

### P1
1. Recent CP2 changes combine corrected timestamps with strict per-side monotonic rejection.
- Evidence: `src/domain/screener/quote_ingest.rs:102`, `src/domain/screener/state.rs:83`, `src/domain/screener/state.rs:99`.
- Impact: offset recalibration backward can drop valid fresh quotes.
- Status: `open`.

### P2
1. `fleet_patch_gate` now serializes full ingest hot path.
- Evidence: `src/domain/screener/quote_ingest.rs:96`, `src/domain/screener/quote_ingest.rs:106`, `src/domain/screener/fleet_reload.rs:14`.
- Impact: throughput/jitter regression risk.
- Status: `open`.

### P3
1. Symbol state can be allocated before exchange validation.
- Evidence: `src/domain/screener/quote_ingest.rs:21`, `src/domain/screener/state.rs:114`.
- Impact: noise/churn entries for malformed exchange labels.
- Status: `open`.
