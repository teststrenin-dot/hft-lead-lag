# R7 — Logic and Math Review

Date: 2026-02-26

## Findings

### P1
1. Portfolio shortlist currently shared between A/B, not independent per portfolio.
- Evidence: `src/application/services/portfolio_runtime.rs:94`, `:123`, `:128`.
- Impact: mismatch with target business model for portfolio race.

2. Lead-lag signal lacks explicit freshness gate for local timestamps.
- Evidence: `src/application/services/lead_lag.rs:225`, `src/event_loop_core.rs:281`.
- Impact: stale secondary quote can still trigger signals.

### P2
1. `shadow_trader` direction selection is biased by branch order when both signals are valid.
- Evidence: `src/domain/screener/shadow_trader.rs:462`, `:471`.

2. `min_baseline_samples` checked against total samples, not samples in active window.
- Evidence: `src/domain/screener/shadow_trader.rs:424`, `:433`.

### P3
1. Stop-loss "in a row" semantics may be ambiguous (non-stop-loss losing exits do not reset streak).
- Evidence: `src/application/services/portfolio_runtime.rs:148`, `:154`.
