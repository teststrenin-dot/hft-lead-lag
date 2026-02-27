# R12 CP0 - Dead Code Review

Date: 2026-02-27

## Findings

### P1
1. Order execution contract surface appears unused in current runtime path (market-data path is active).
- Evidence: `src/domain/exchange.rs:97`, `src/domain/exchange.rs:116`, `src/infrastructure/exchanges/binance/mod.rs:398`, `src/infrastructure/exchanges/gate/mod.rs:261`, `src/lib.rs:79`.
- Status: `open`.

### P2
1. Order-model helper methods currently unused in-repo, consistent with inactive execution contract branch.
- Evidence: `src/domain/models.rs:14`, `src/domain/models.rs:64`, `src/domain/models.rs:82`, `src/domain/models.rs:125`, `src/domain/models.rs:129`.
- Status: `open`.

### P3
1. API compatibility re-exports look like stale boundary baggage for internal dependency paths.
- Evidence: `src/api/mod.rs:17`, `src/api/mod.rs:18`, `src/api/mod.rs:19`, `src/api/http_server.rs:13`.
- Status: `open`.
