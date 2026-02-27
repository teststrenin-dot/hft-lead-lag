# HFT-CP3 Evidence — Updated-Only Event Processing

Date: 2026-02-28
Checkpoint: `HFT-CP3`
Scope: prove runtime processing is update-driven, not universe-driven

## 1) Runtime path shape
1. Pending signal scheduling uses `PendingSymbolSet` (`SymbolId` bitset), not tree/string structures.
2. Signal checks consume only pending ids via `pop_first()` with per-tick budget (`SIGNAL_CHECK_BUDGET_PER_TICK`).
3. Strategy update queue carries `(ExchangeSide, SymbolId, BookTicker)` and flushes directly into strategy apply.
4. Runtime no longer performs latest-cache lookup clone in strategy flush path.

## 2) Test proof
Executed with `cargo test -q` (pass: `312`, fail: `0`, ignored: `2`).

Key tests:
1. `handle_signal_tick_checks_only_pending_symbols`
   - Verifies only pending ids are checked.
2. `handle_signal_tick_respects_budget_and_keeps_backlog`
   - Verifies bounded per-tick work and backlog carryover.
3. `handle_signal_tick_scales_with_updates_not_universe_size`
   - Universe = `5000` symbols, pending updates = `2`; checked count = `2`.
4. `strategy_update_queue_flushes_tickers_without_latest_cache_lookup`
   - Verifies strategy updates are applied from queue-carried tickers directly.

## 3) CP3 exit assessment
`HFT-CP3` exit gate: CPU/work scales with update rate, not full universe traversal.

Assessment:
1. Data path is now pending-id driven (bitset).
2. Signal checks are bounded and update-scoped.
3. Strategy feed path avoids runtime cache-lookup cloning.
4. CP3 is complete; remaining hot-path work moves to `HFT-CP4` (parse/copy minimization).
