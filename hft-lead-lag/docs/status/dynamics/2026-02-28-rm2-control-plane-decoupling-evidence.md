# HFT-RM2 Evidence — Control-Plane Decoupling

Date: 2026-02-28
Scope: enforce control-plane ingest boundary and prevent hot-path side effects

## 1) Contract summary
1. Runtime ingest does not apply direct screener side-effects in production mode.
2. Ingest emits bounded `ControlUpdate` handoff.
3. Overflow lane keeps latest update per `(symbol, exchange)` to avoid cross-exchange overwrite.

## 2) Code evidence
1. `src/event_loop_core.rs`
   - direct ingest fallback is constrained to test builds only (`cfg!(test)` guard).
2. `src/event_loop_control.rs`
   - bounded `ControlUpdate` queue with latest overflow lane.
   - overflow key is `(symbol, exchange)`, not symbol-only.
3. `src/main.rs`
   - mixed mode wires control-plane worker and runtime passes control plane handle.

## 3) Test evidence
Executed:

```bash
cargo test -q control_plane_worker_applies_update_and_emits_ws_event
cargo test -q control_plane_try_enqueue_overflow_lane_keeps_latest_by_symbol_and_counts_replacements
cargo test -q control_plane_overflow_lane_keeps_latest_per_symbol_and_exchange
```

Validated:
1. Control-plane worker applies updates and emits WS events.
2. Overflow replacement accounting increments drop counters as expected.
3. Overflow lane preserves per-exchange latest updates for same symbol under queue pressure.

## 4) Exit statement
`HFT-RM2` exit gate is met:
1. Runtime hot path uses control-plane boundary in production.
2. Compatibility direct path is test-only.
3. Overflow semantics are deterministic under multi-exchange contention.
