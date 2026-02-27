# HFT Checkpoint Readiness Breakdown

Date: 2026-02-28
Status: Active
Checkpoint set: `docs/status/core/2026-02-28-hft-rust-only-checkpoints.md`

## `HFT-CP0` Latency and Allocation Observatory
1. Done and not touched:
   - Drift sampling and log summary.
   - Drop counters exposed via health support.
2. Done but must be reworked:
   - Existing metrics are not stage-based and cannot explain internal p99 path.
3. Missing and required:
   - Stage timestamps: `recv_ws_frame_ts`, `parsed_ts`, `state_updated_ts`, `signal_decided_ts`, `order_intent_enqueued_ts`.
   - Internal latency histograms (`p50/p95/p99/max`) for ingest, decision, end-to-end.
   - Backlog depth counters for runtime channels.
   - Single endpoint/page for baseline and before/after comparisons.

## `HFT-CP1` SymbolId and Allocation Removal
1. Done and not touched:
   - Price representation in ticks.
2. Done but must be reworked:
   - `String` symbol conversion in hot loops.
   - `HashMap<String, ...>` in hot runtime segments.
3. Missing and required:
   - `SymbolId` + universe mapping.
   - Array-style state (`Vec`/indexed storage) for books and pending processing.

## `HFT-CP2` Lock-Free Strategy State
1. Done and not touched:
   - Runtime strategy/event loop wiring exists.
2. Done but must be reworked:
   - `Arc<RwLock<...>>`/`Arc<Mutex<...>>` in lead-lag strategy hot path.
3. Missing and required:
   - Single-owner strategy engine state with queue-fed updates.
   - Lock-free signal check path.

## `HFT-CP3` Event-Driven Updated-Only Processing
1. Done and not touched:
   - Pending-symbol scheduling mechanism exists.
2. Done but must be reworked:
   - `BTreeSet<String>` pending store.
   - Per-batch string sort/dedup and tick cloning.
3. Missing and required:
   - Bitset/ID-based updated-symbol flow (`SymbolId`-first path).
   - Processing proportional to update rate, not universe size.

## `HFT-CP4` Minimal-Copy WS Parse Path
1. Done and not touched:
   - Fast float parsing and tick conversion.
2. Done but must be reworked:
   - Generic extractors using repeated `format!`.
   - Byte copies for symbol extraction in hot parse path.
3. Missing and required:
   - Symbol mapping to `SymbolId` with minimal copy overhead.
   - Specialized hot parse path for required fields only.

## `HFT-CP5` Deterministic Replay Harness
1. Done and not touched:
   - General tests and reliability fixes from legacy track.
2. Done but must be reworked:
   - None (feature not implemented yet).
3. Missing and required:
   - Raw feed recorder.
   - Replay engine with deterministic decision/trade equivalence checks.
   - Replay benchmark for regression detection.

## `HFT-CP6` Execution Fast Path
1. Done and not touched:
   - Signal-producing runtime foundation.
2. Done but must be reworked:
   - Existing execution path is not SLA-modeled for `intent -> sent`.
3. Missing and required:
   - Explicit `OrderIntent` queue.
   - Non-blocking send path with strict timeout and kill-switch behavior.
   - Internal send SLA telemetry.

## `HFT-CP7` Production Operations Layer
1. Done and not touched:
   - Base health endpoint and saturation counters.
2. Done but must be reworked:
   - Ops coverage is incomplete for production deterministic recovery.
3. Missing and required:
   - Component watchdogs (`feed/engine/execution/dbwriter`).
   - Alert contract for drift, drops, backlog, engine stall.
   - Idempotent snapshot/restore runbook and recovery verification.
