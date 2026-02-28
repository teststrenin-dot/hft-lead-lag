# HFT-CP6 Evidence — Execution Fast Path

Date: 2026-02-28
Checkpoint: `HFT-CP6`
Status: `Completed`

## Scope delivered
1. Added execution fast-path module:
   - `src/event_loop_execution.rs`
   - bounded `OrderIntent` queue with `try_send` (non-blocking hot path).
   - dedicated async worker for send path.
2. Replaced CP0 proxy enqueue timestamp with real queue enqueue:
   - `src/event_loop_core.rs::handle_signal_tick` now builds `OrderIntent` and enqueues it.
3. Implemented strict send timeout + kill-switch behavior:
   - timeout is enforced per intent.
   - consecutive timeout streak activates kill-switch.
   - kill-switch blocks new intents and marks health as degraded.
4. Implemented execution SLA telemetry (`intent -> sent`) in health:
   - stage timestamp: `order_intent_sent_ts_ns`.
   - latency snapshot: `execution_intent_to_sent` (`samples/p50/p95/p99/max`).
   - backlog depth: `execution_intent_queue_depth`.
   - counters: sent, dropped, timeouts, kill-switch state.
5. Wired execution queue into runtime loop:
   - `src/event_loop_runtime.rs` context now carries execution queue handle.
   - `src/main.rs` spawns execution runtime before main event loop.

## TDD evidence
1. `RED`:
```bash
cargo test -q execution_queue_accepts_intents_and_tracks_queue_depth -- --nocapture
```
Result: failed (CP6 fields missing in `HealthState`).

2. `GREEN` (execution queue + worker + kill-switch tests):
```bash
cargo test -q execution_queue_accepts_intents_and_tracks_queue_depth -- --nocapture
cargo test -q execution_worker_reports_sent_intents_and_latency -- --nocapture
cargo test -q execution_kill_switch_activates_after_timeout_streak -- --nocapture
```
Result: all passed.

3. Health telemetry regression test:
```bash
cargo test -q health_reports_execution_fast_path_telemetry_and_kill_switch_issue -- --nocapture
```
Result: passed.

4. Signal loop integration regression:
```bash
cargo test -q handle_signal_tick_checks_only_pending_symbols -- --nocapture
```
Result: passed.

## Full verification
Commands:
```bash
cargo fmt -- --check
cargo check --all-targets
cargo test
```

Results:
1. `cargo fmt -- --check`: success.
2. `cargo check --all-targets`: success.
3. `cargo test`: success (`329 passed`, `0 failed`, `6 ignored`; doc-tests `1 passed`).

## Runtime knobs
1. `EXECUTION_INTENT_QUEUE_CAPACITY` (default: `2048`)
2. `EXECUTION_SEND_TIMEOUT_MS` (default: `25`)
3. `EXECUTION_KILL_SWITCH_TIMEOUT_STREAK` (default: `4`)
4. `EXECUTION_METRICS_FLUSH_INTERVAL_MS` (default: `1000`)
5. `EXECUTION_SIMULATED_SEND_DELAY_MS` (default: `0`, testing/diagnostics)

## CP6 exit assessment
1. Strategy thread is non-blocking on execution send path (`try_send` only).
2. Intent queue and async fire-and-track path are live.
3. Intent->sent SLA telemetry is measurable via `/health`.
4. Kill-switch behavior is explicit and observable.
5. CP6 is closed; next stage is CP7 operations hardening.
