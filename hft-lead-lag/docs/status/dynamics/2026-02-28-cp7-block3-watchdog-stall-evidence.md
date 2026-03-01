# CP7 Block3 Evidence — Watchdog Stall Signals in `/health`

Date: 2026-02-28
Status: Completed
Scope: `HFT-CP7` block3 (engine/signal/execution stall watchdogs)

## What was added
1. `src/api/handlers/health_support.rs`
   - Added watchdog thresholds:
     - `ENGINE_STALL_THRESHOLD_MS = 5000`
     - `SIGNAL_LOOP_STALL_THRESHOLD_MS = 3000`
     - `EXECUTION_LOOP_STALL_THRESHOLD_MS = 3000`
   - Added stage-age helpers (`now_ns`, `stage_age_ms_from_ns`).
   - Added runtime watchdog issue signals in `/health`:
     - `engine_state_stall` when both feeds are healthy but engine `state_updated_ts` is stale.
     - `signal_loop_stall` when signal backlog is non-zero and `signal_decided_ts` is stale.
     - `execution_loop_stall` when execution backlog is non-zero and both enqueue/sent progress are stale.

2. `src/api/handlers/tests.rs`
   - Added regression test:
     - `health_reports_watchdog_stalls_for_engine_signal_and_execution`
   - Test seeds stale stage timestamps + non-zero backlog and verifies `/health` degrades with all three watchdog issues.

## Verification
Commands run:

```bash
cargo test -q health_reports_watchdog_stalls_for_engine_signal_and_execution -- --nocapture
cargo test -q health_ -- --nocapture
cargo test -q api::handlers::tests:: -- --nocapture
cargo check -q
```

Result:
1. New watchdog regression test passes.
2. All `health_*` tests pass.
3. Full API handler test module passes.
4. Workspace compiles cleanly.

## Exit-gate impact
1. CP7 now has active runtime watchdog signals for three hot-loop failure classes (`engine`, `signal`, `execution`).
2. Remaining CP7 scope is deterministic recovery/runbook and final alert-contract closure.
