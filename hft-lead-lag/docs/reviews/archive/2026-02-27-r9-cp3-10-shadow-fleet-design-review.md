# R9 CP3 - Shadow Fleet Design Review

Date: 2026-02-27

## Findings

### P1
1. Candidate stats are biased by config-level event multiplicity.
- Evidence: `src/domain/screener/shadow_fleet.rs:468`, `src/domain/screener/shadow_fleet.rs:492`, `src/domain/screener/mod.rs:647`.
- Status: `open`.

2. Same-timestamp trade ordering tie-break by `config_id` can alter guard outcomes and downstream candidate eligibility.
- Evidence: `src/domain/screener/mod.rs:639`, `src/application/services/portfolio_runtime.rs:230`.
- Status: `open`.

### P2
1. Symbol canonicalization is not enforced at fleet->candidate boundary.
- Evidence: `src/domain/screener/quote_ingest.rs:21`, `src/domain/screener/mod.rs:648`.
- Status: `open`.

2. `run_id` is carried in fleet output but ignored in candidate accumulation.
- Evidence: `src/domain/screener/shadow_fleet.rs:340`, `src/domain/screener/mod.rs:296`, `src/domain/screener/mod.rs:647`.
- Status: `open`.

### P3
1. Missing tests for equal-timestamp mixed outcomes across configs at CP3 boundary.
- Evidence: `src/domain/screener/tests.rs:1107`.
- Status: `open`.
