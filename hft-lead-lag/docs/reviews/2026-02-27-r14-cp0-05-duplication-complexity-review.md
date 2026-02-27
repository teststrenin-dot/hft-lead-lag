# R14 CP0 - Duplication and Complexity Review

Date: 2026-02-27

## Findings

### P3
1. Runtime API surface duplicated across domain and application namespace.
- Evidence: `src/domain/screener/portfolio_runtime.rs:1`, `src/application/services/portfolio_runtime.rs:1`, `src/application/services/mod.rs:7`.
- Status: `open`.

2. Portfolio runtime complexity moved, but not decomposed.
- Evidence: `src/domain/screener/portfolio_runtime.rs:1`.
- Status: `open`.

### Trend
1. Duplication around exit-reason constants reduced by typed enum centralization.
- Evidence: `src/domain/screener/shadow_trader.rs:30`, `src/domain/screener/shadow_trader.rs:38`, `src/infrastructure/db.rs:1299`.
- Status: `improved`.
