# R10 CP2 - Shadow Trader/Fleet Design Review

Date: 2026-02-27

## Findings

### P1
1. `run_id` close-time rebinding is possible when entry run_id is `None`.
- Evidence: `src/domain/screener/shadow_fleet.rs:462`, `src/domain/screener/shadow_trader.rs:427`.
- Status: `open`.

2. Freshness gate blocks entire lifecycle including exits/timeouts.
- Evidence: `src/domain/screener/shadow_trader.rs:248`, `src/domain/screener/shadow_trader.rs:259`.
- Status: `open`.

### P2
1. Timeout can be masked by breakeven/trailing precedence.
- Evidence: `src/domain/screener/shadow_trader.rs:339`, `src/domain/screener/shadow_trader.rs:345`, `src/domain/screener/shadow_trader.rs:1202`.
- Status: `open`.

2. Short-side lifecycle symmetry is under-tested.
- Evidence: `src/domain/screener/shadow_trader.rs:833`, `src/domain/screener/shadow_trader.rs:903`, `src/domain/screener/shadow_trader.rs:948`.
- Status: `open`.

### P3
1. Docs/comments still mention `target` semantics while implementation is breakeven/trailing-first.
- Evidence: `src/domain/screener/shadow_trader.rs:5`, `src/domain/screener/shadow_trader.rs:332`.
- Status: `open`.
