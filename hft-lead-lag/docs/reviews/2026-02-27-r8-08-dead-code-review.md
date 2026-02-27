# R8 - Dead Code Review

Date: 2026-02-27

## Findings

### P3
1. `replace_portfolio_paper_state_v1` appears runtime-unused in production flow (referenced in tests).
- Evidence: `src/infrastructure/db.rs:633`.
- Status: `open`.

2. `replace_portfolio_state_v1` and `replace_portfolio_guards_v1` appear test-oriented in current repo usage.
- Evidence: `src/infrastructure/db.rs:513`, `src/infrastructure/db.rs:607`.
- Status: `open`.

3. `restore_portfolio_runtime_v1_from_db_rows` wrapper appears test-oriented while production path uses `_with_paper` variant.
- Evidence: `src/domain/screener/mod.rs:420`.
- Status: `open`.

Note: this is repo-local reachability analysis. If these public APIs are consumed by external crates, they are not dead code.
