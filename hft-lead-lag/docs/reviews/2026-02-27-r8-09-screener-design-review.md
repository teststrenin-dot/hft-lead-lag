# R8 - Screener Design Review

Date: 2026-02-27

## Findings

### P1
1. Non-atomic fleet config switch causes mixed runtime window.
- Evidence: `src/domain/screener/fleet_reload.rs:17`, `src/domain/screener/fleet_reload.rs:24`, `src/domain/screener/quote_ingest.rs:59`.
- Status: `open`.

2. Owner attribution can drift to current active owner when assignment history no longer covers entry timestamp.
- Evidence: `src/domain/screener/mod.rs:64`, `src/domain/screener/mod.rs:360`, `src/domain/screener/mod.rs:391`, `src/domain/screener/tests.rs:1070`.
- Status: `open`.

3. Persistence split between snapshots and trades can restore contradictory state under crash/backpressure.
- Evidence: `src/domain/screener/mod.rs:621`, `src/domain/screener/mod.rs:628`, `src/domain/screener/mod.rs:643`, `src/runtime_setup.rs:233`.
- Status: `open`.

### P2
1. Rebalance scheduler can stall after restart when restored timestamp is ahead of current host clock.
- Evidence: `src/domain/screener/mod.rs:478`, `src/domain/screener/mod.rs:666`, `src/event_loop_runtime.rs:103`.
- Status: `open`.

## Strengths
1. Rebalance loop is decoupled from hot ingest path.
2. Dual cadence check avoids unnecessary expensive candidate recomputations.
3. Entry ownership and run-id ownership are explicitly represented and tested.
