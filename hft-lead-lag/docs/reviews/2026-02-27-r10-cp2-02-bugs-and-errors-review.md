# R10 CP2 - Bugs and Errors Review

Date: 2026-02-27

## Findings

### P1
1. Freshness guard blocks full lifecycle, including risk exits and timeout.
- Evidence: `src/domain/screener/shadow_trader.rs:248`, `src/domain/screener/shadow_trader.rs:259`, `src/domain/screener/shadow_trader.rs:263`.
- Status: `open`.

2. `run_id` can be rebound at close when entry run_id was `None`.
- Evidence: `src/domain/screener/shadow_fleet.rs:462`, `src/domain/screener/shadow_fleet.rs:466`, `src/domain/screener/shadow_trader.rs:427`.
- Status: `open`.

3. Partial-book symbols exposed as `ws_live` with synthetic `lag_ms=0` suppress fallback rows.
- Evidence: `src/domain/screener/quote_ingest.rs:42`, `src/domain/screener/catalog_cache.rs:117`, `src/api/handlers.rs:226`.
- Status: `open`.

### P2
1. Backward timestamps can produce negative `catchup_ms` and delay timeout behavior.
- Evidence: `src/domain/screener/shadow_trader.rs:338`, `src/domain/screener/shadow_trader.rs:552`.
- Status: `open`.

2. Cycle tracker open-cycle state is not expired by cleanup.
- Evidence: `src/domain/screener/cycle_tracker.rs:49`, `src/domain/screener/cycle_tracker.rs:79`.
- Status: `open`.
