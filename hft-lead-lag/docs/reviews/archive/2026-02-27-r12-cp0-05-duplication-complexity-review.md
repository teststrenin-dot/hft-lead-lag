# R12 CP0 - Duplication, Redundancy, and Complexity Review

Date: 2026-02-27

## Findings

### P2
1. Portfolio ID normalization logic duplicated in startup and runtime engine paths.
- Evidence: `src/main.rs:112`, `src/main.rs:127`, `src/application/services/portfolio_runtime.rs:110`, `src/application/services/portfolio_runtime.rs:123`.
- Status: `open`.

2. Exchange identity modeled twice (`config::ExchangeId` and `domain::ExchangeId`) with manual mapping glue.
- Evidence: `src/config/mod.rs:53`, `src/domain/exchange.rs:11`, `src/application/strategies/mod.rs:82`.
- Status: `open`.

### P3
1. Fleet patch surface has overlapping entrypoints for one contract (`replace_*`, `try_apply_*`, `apply_*`).
- Evidence: `src/domain/screener/mod.rs:957`, `src/domain/screener/mod.rs:971`, `src/domain/screener/mod.rs:990`.
- Status: `open`.
