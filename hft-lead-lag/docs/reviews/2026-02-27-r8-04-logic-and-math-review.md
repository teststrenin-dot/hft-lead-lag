# R8 - Logic and Math Review

Date: 2026-02-27

## Findings

### P1
1. Guard/cooldown semantics are order-dependent on drained fleet trade sequence.
- Evidence: `src/domain/screener/shadow_fleet.rs:436`, `src/domain/screener/shadow_fleet.rs:468`, `src/domain/screener/mod.rs:632`, `src/application/services/portfolio_runtime.rs:230`.
- Impact: different cooldown outcomes for same market event set.
- Status: `open`.

### P2
1. Owner attribution may shift from entry owner to close-time active owner after history eviction.
- Evidence: `src/domain/screener/mod.rs:64`, `src/domain/screener/mod.rs:360`, `src/domain/screener/mod.rs:391`, `src/domain/screener/mod.rs:604`.
- Impact: distorted portfolio PnL/WR race metrics.
- Status: `open`.

2. Rebalance gate restoration from `updated_at_ms` mixes rebalance time with unrelated updates.
- Evidence: `src/domain/screener/mod.rs:478`, `src/domain/screener/mod.rs:614`, `src/domain/screener/mod.rs:666`.
- Impact: incorrect cadence behavior after restart.
- Status: `open`.

3. Stop-loss logic relies on string equality (`"stop_loss"`) at policy boundary.
- Evidence: `src/domain/screener/mod.rs:636`, `src/domain/screener/shadow_trader.rs:350`.
- Impact: silent drift if reason string changes.
- Status: `open`.
