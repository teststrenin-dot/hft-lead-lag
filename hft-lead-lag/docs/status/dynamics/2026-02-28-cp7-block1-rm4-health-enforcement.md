# HFT-CP7 Block 1 Evidence — RM4 Health Enforcement

Date: 2026-02-28
Scope: operationalize RM4 SLO contract in runtime `/health` output

## 1) Implemented behavior
1. `/health` now evaluates RM4 envelopes on each health window:
   - latency p99 bounds (`ingest/decision/end_to_end`)
   - backlog bounds (`binance/gate/signal/execution/control`)
   - per-window drop/timeout deltas (`execution_dropped_intents`, `execution_send_timeouts`, `control_dropped_updates`)
2. Runtime tracks consecutive RM4 breaches (`runtime_rm4_breach_streak`).
3. After `3` consecutive breached windows, runtime is marked non-HFT:
   - `issues` contains `hft_slo_degraded_non_hft`
   - `hft_mode_status` becomes `degraded_non_hft`
4. Before threshold is hit, window breaches are warnings:
   - `warnings` contains `hft_slo_window_breach`

## 2) Code evidence
1. `src/api/http_server.rs`
   - added RM4 tracking atomics in `HealthState`:
     - breach streak
     - degraded flag
     - last-eval counters for delta computation
2. `src/api/handlers/health_support.rs`
   - added RM4 threshold constants and breach evaluator
   - added per-window counter delta tracking
   - added streak/degradation transitions and response fields
3. `src/api/handlers.rs`
   - `HealthResponse` extended with:
     - `hft_mode_status`
     - `rm4_breach_streak`
     - `rm4_window_threshold`

## 3) Test evidence
Executed:

```bash
cargo test -q health_marks_hft_mode_degraded_after_three_consecutive_rm4_breaches
cargo test -q health_reports_execution_fast_path_telemetry_and_kill_switch_issue
cargo test -q health_returns_degraded_when_feed_is_stale
```

Validated:
1. first two RM4 breaches are warnings only;
2. third consecutive breach escalates to `degraded_non_hft` issue;
3. existing health degradation semantics remain intact.
