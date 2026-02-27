# R8 - Bugs and Errors Review

Date: 2026-02-27

## Findings

### P1
1. `flush_all()` may wait forever after enqueue drop under saturation.
- Evidence: `src/infrastructure/db.rs:886`, `src/infrastructure/db.rs:889`, `src/infrastructure/db.rs:942`, `src/infrastructure/db.rs:1116`.
- Impact: runtime paths that call flush (trial apply, hot reload) can stall.
- Status: `open`.

2. Fleet config switch is not atomic across symbols.
- Evidence: `src/domain/screener/fleet_reload.rs:17`, `src/domain/screener/fleet_reload.rs:24`.
- Impact: mixed old/new runtime during one switch window.
- Status: `open`.

3. Snapshot/trades persistence boundary is non-atomic.
- Evidence: `src/domain/screener/mod.rs:621`, `src/domain/screener/mod.rs:628`, `src/domain/screener/mod.rs:643`, `src/infrastructure/db.rs:880`.
- Impact: inconsistent recovered state after crash/backpressure.
- Status: `open`.

### P2
1. Rebalance cadence can freeze after restart when restored timestamp is ahead of host clock.
- Evidence: `src/domain/screener/mod.rs:478`, `src/domain/screener/mod.rs:666`.
- Status: `open`.

2. `/api/v1/portfolio/active` DB fallback can return stale or foreign portfolio IDs.
- Evidence: `src/api/handlers.rs:272`, `src/api/handlers.rs:286`.
- Status: `open`.

3. Owner-at-entry attribution can degrade to owner-at-close for long holds due to bounded history.
- Evidence: `src/domain/screener/mod.rs:64`, `src/domain/screener/mod.rs:360`, `src/domain/screener/mod.rs:391`.
- Status: `open`.
