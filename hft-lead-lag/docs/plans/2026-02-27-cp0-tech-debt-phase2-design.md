# CP0 Tech Debt Phase 2 Design

Date: 2026-02-27
Scope: close remaining non-blocking debts after CP0 confirm (`r14-cp0`).

## Goals
1. Remove duplicate public surface for portfolio-runtime domain logic.
2. Add preventive DB index for candidate-restore event aggregation query.
3. Reduce cognitive load in `ScreenerStore` by extracting drained-trades preparation logic.

## Constraints
1. No behavior changes in trading/candidate math.
2. Keep API and DB contracts stable.
3. Changes must be regression-safe with existing tests.

## Solution
1. **Boundary cleanup**
- Stop exporting portfolio runtime through `application::services`.
- Use `domain::screener::portfolio_runtime` directly at call sites.
- Move portfolio-runtime unit tests from application module to domain module.

2. **Preventive DB hardening**
- Add `idx_trades_symbol_exit_ts` index on `trades(symbol, exit_ts_ms)`.
- Add test verifying index presence after `open_db()`.

3. **Cognitive-load reduction**
- Extract drained-trades preparation (sorting, active-run filtering, candidate-event collapse) into `domain/screener/drained_trades.rs`.
- Keep `handle_drained_fleet_trades()` orchestration but delegate heavy preprocessing.

## Verification
1. Targeted tests for new index and drained-trades helpers.
2. Existing regression tests around candidate restore/collapse and API fallback.
3. Full `cargo test` pass.
