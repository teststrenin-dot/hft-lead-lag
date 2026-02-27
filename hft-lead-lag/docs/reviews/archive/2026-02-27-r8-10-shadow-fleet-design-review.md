# R8 - Shadow Fleet Design Review

Date: 2026-02-27

## Findings

### P1
1. Cooldown/guard outcomes depend on fleet config traversal order during drained trade replay.
- Evidence: `src/domain/screener/shadow_fleet.rs:436`, `src/domain/screener/shadow_fleet.rs:468`, `src/domain/screener/mod.rs:632`, `src/application/services/portfolio_runtime.rs:230`.
- Status: `open`.

### P2
1. Stop-loss policy boundary depends on string value `"stop_loss"`.
- Evidence: `src/domain/screener/shadow_fleet.rs:206`, `src/domain/screener/mod.rs:636`, `src/domain/screener/shadow_trader.rs:350`.
- Status: `open`.

2. Ingest hot path performs inline CP4 state mutation per drained trade.
- Evidence: `src/domain/screener/quote_ingest.rs:101`, `src/domain/screener/quote_ingest.rs:118`, `src/domain/screener/mod.rs:582`, `src/domain/screener/mod.rs:615`.
- Status: `open`.

### P3
1. Assignment history cap behavior is not formalized as explicit CP4 contract.
- Evidence: `src/domain/screener/mod.rs:64`, `src/domain/screener/mod.rs:391`.
- Status: `open`.

## Strengths
1. Fleet-to-CP4 integration boundary via `FleetTrade` is explicit and testable.
2. Entry-time ownership fallback behavior is covered by targeted tests.
3. Rebalance cadence boundary protection is implemented and validated by tests.
