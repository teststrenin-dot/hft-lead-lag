# R14 CP0 - Logic and Math Review

Date: 2026-02-27

## Findings

### P1
1. Candidate restore math now matches live accumulation contract.
- Evidence: `src/domain/screener/mod.rs:668`, `src/domain/screener/mod.rs:686`, `src/infrastructure/db.rs:735`, `src/infrastructure/db.rs:742`.
- Status: `closed`.

2. Useful-winrate unit semantics are explicit on API boundary (ratio + percent).
- Evidence: `src/domain/screener/portfolio_runtime.rs:129`, `src/domain/screener/portfolio_runtime.rs:143`, `src/api/handlers.rs:153`, `src/api/handlers.rs:154`, `src/api/handlers/tests.rs:482`.
- Status: `closed`.

### P2
1. None.
