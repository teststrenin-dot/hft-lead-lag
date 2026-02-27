# R8 - Preventive Architecture Review

Date: 2026-02-27

## Findings

### P1
1. Flush acknowledgment protocol is not safe against dropped commands.
- Evidence: `src/infrastructure/db.rs:886`, `src/infrastructure/db.rs:942`.
- Preventive gap: no invariant that `target_seq` must be enqueue-confirmed.
- Status: `open`.

2. Durability model for CP4 runtime and trade history is not atomic.
- Evidence: `src/domain/screener/mod.rs:621`, `src/domain/screener/mod.rs:643`, `src/infrastructure/db.rs:880`.
- Preventive gap: no transactional boundary or recovery reconciliation phase.
- Status: `open`.

### P2
1. Time-source assumptions are not guarded against skew/rollback.
- Evidence: `src/domain/screener/mod.rs:666`, `src/event_loop_runtime.rs:62`.
- Preventive gap: no monotonic scheduler anchor for rebalance cadence.
- Status: `open`.

2. Policy-critical stop-loss classification is stringly typed.
- Evidence: `src/domain/screener/mod.rs:636`, `src/domain/screener/shadow_trader.rs:350`.
- Preventive gap: no typed enum contract across modules.
- Status: `open`.

3. Attribution retention cap is fixed but not validated against max hold horizon.
- Evidence: `src/domain/screener/mod.rs:64`, `src/domain/screener/trader_config.rs:89`.
- Preventive gap: missing config invariant `history_horizon >= max_hold_horizon`.
- Status: `open`.
