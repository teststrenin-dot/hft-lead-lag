# R9 CP3 - Logic and Math Review

Date: 2026-02-27

## Findings

### P1
1. Candidate statistics are config-event weighted, not symbol-event weighted.
- Evidence: `src/domain/screener/shadow_fleet.rs:468`, `src/domain/screener/mod.rs:647`, `src/application/services/portfolio_runtime.rs:140`.
- Impact: symbols with more active configs can be overrepresented in ranking.
- Status: `open`.

2. Same-timestamp mixed trades are resolved by `config_id` tie-break before guard logic.
- Evidence: `src/domain/screener/mod.rs:639`, `src/application/services/portfolio_runtime.rs:230`.
- Impact: guard/cooldown outcomes can vary by config-id ordering, affecting eligibility.
- Status: `open`.

### P3
1. Deterministic tie-break exists, but no direct regression test for full tuple equality.
- Evidence: `src/application/services/portfolio_runtime.rs:172`, `src/application/services/portfolio_runtime_tests.rs:52`.
- Status: `open`.
