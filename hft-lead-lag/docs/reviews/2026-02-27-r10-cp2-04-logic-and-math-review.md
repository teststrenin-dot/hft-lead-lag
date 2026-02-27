# R10 CP2 - Logic and Math Review

Date: 2026-02-27

## Findings

### P1
1. Baseline uses current tick in averaging window.
- Evidence: `src/domain/screener/state.rs:197`, `src/domain/screener/shadow_trader.rs:449`, `src/domain/screener/shadow_trader.rs:487`.
- Impact: signal dilution and min-sample off-by-one semantics.
- Status: `open`.

2. Strict monotonic gating + corrected timestamps can create false regression drops.
- Evidence: `src/domain/screener/quote_ingest.rs:102`, `src/domain/screener/state.rs:83`.
- Status: `open`.

### P2
1. Exit precedence can mask `timeout` as `breakeven`/`trailing_take`.
- Evidence: `src/domain/screener/shadow_trader.rs:339`, `src/domain/screener/shadow_trader.rs:345`.
- Status: `open`.

2. Metrics blend both direction trackers regardless of leading side.
- Evidence: `src/domain/screener/state.rs:160`, `src/domain/screener/state.rs:165`.
- Status: `open`.
