# RM5 Fix Review (r18)

Date: 2026-02-28
Scope: commit `eeaa9cd` (`feat(rm5): isolate control-plane thread and coalesce updates`)
Status: Findings present

## Findings

### P1 — Startup panic path on control-plane thread spawn
- File: `src/event_loop_control.rs`
- Lines: `219-240`
- Detail:
  - `std::thread::Builder::spawn(...).expect("failed to spawn control-plane worker thread")` converts a recoverable OS resource failure into process panic.
  - On constrained hosts this can hard-stop runtime at startup instead of surfacing controlled degraded state / actionable startup error.
- Risk:
  - Availability regression (`panic`) rather than controlled failure handling.

### P1 — Coalescing discards updates without observability counter
- File: `src/event_loop_control.rs`
- Lines: `132-135`, `173-177`
- Detail:
  - `pending.insert(key, update)` overwrites previous update for same `(symbol, exchange)` in coalescing window.
  - This intentional discard is not reflected in any health counter.
  - Existing `runtime_control_dropped_updates` only tracks overflow-lane replacement on `try_enqueue` full path.
- Risk:
  - `/health` underreports effective drop/coalesce pressure, making RM4-style quality gating less trustworthy under burst load.

### P2 — Silent fallback for invalid control-plane env config
- File: `src/event_loop_control.rs`
- Lines: `87-107`
- Detail:
  - `parse_env_usize` / `parse_env_u64` silently return `None` on invalid values.
  - No warning is emitted for malformed values of:
    - `CONTROL_UPDATE_QUEUE_CAPACITY`
    - `CONTROL_UPDATE_FLUSH_INTERVAL_MS`
    - `CONTROL_UPDATE_MAX_BATCH`
- Risk:
  - Misconfiguration can remain invisible in production and complicate incident diagnosis.

## What was good
1. Dedicated control-plane thread/runtime separation is correctly wired and test-covered.
2. Coalescing latest-by-key reduces screener churn and is directionally correct for 2-core budget.
3. Runtime-grid defaults were aligned with 2-core profile (`512`) in both code template and checked-in config.

## Test coverage gaps
1. No explicit regression test for max-batch-triggered flush behavior (`pending.len() >= max_batch`).
2. No test for health observability semantics under coalescing overwrite pressure.
