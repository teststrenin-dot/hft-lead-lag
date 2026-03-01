# CP7 Block4 — Deterministic Recovery Runbook v1

Date: 2026-02-28
Status: Active
Scope: `HFT-CP7` recovery/runbook layer (pre-live, no capital rebalance yet)

## 1) Purpose
Define a deterministic operator path when runtime health degrades, with explicit recovery checks and idempotent restore guarantees.

## 2) Failure Signals (source of truth: `/health`)
Treat runtime as degraded and enter recovery flow if any of the following appears in `issues`:
1. `hft_slo_degraded_non_hft`
2. `engine_state_stall`
3. `signal_loop_stall`
4. `execution_loop_stall`
5. `execution_kill_switch_active`
6. `binance_stale` / `gate_stale`

## 3) Recovery Procedure (deterministic)
1. Freeze observer actions:
   - Do not trigger additional trial orchestration during active recovery.
2. Capture pre-restart diagnostics:
   - Save one `/health` snapshot (`issues`, `warnings`, stage timestamps, latency/backlog, execution counters).
3. Perform process restart:
   - restart runtime process with the same `RUNTIME_PLANE_MODE` and config set.
4. Wait warm-up window:
   - require at least one fresh feed cycle and one health interval before pass/fail decision.
5. Validate post-restart invariants:
   - `/health.status == ok`
   - watchdog issues absent
   - feed freshness restored
   - no immediate re-entry into `degraded_non_hft`

## 4) Restore Idempotency Guarantees (code-level)
1. Portfolio snapshot sequence guard:
   - stale snapshot sequence is ignored in DB writer (`writer_ignores_stale_portfolio_snapshot_sequence`).
2. Duplicate drained trade protection:
   - duplicate natural-key fleet trade does not double-apply guard/paper state (`duplicate_drained_trade_natural_key_is_idempotent_for_guard_and_paper`).
3. Runtime restore path:
   - startup restore rebuilds runtime assignment/guard/paper state from persisted DB rows before normal loop.

## 5) Verification Commands
```bash
cargo test -q writer_ignores_stale_portfolio_snapshot_sequence -- --nocapture
cargo test -q duplicate_drained_trade_natural_key_is_idempotent_for_guard_and_paper -- --nocapture
scripts/ops/health_recovery_drill.sh --help
```

Expected:
1. both tests pass;
2. no stale snapshot overwrite;
3. no duplicate trade double-count in guard/paper accounting;
4. drill script CLI is available for operator validation loop.

## 6) Open Tail
1. Extend alert contract with explicit drift alarm thresholds and escalation policy.
