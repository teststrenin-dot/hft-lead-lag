# HFT Checkpoint Readiness Breakdown

Date: 2026-02-28
Status: Active
Checkpoint set: `docs/status/core/2026-02-28-hft-rust-only-checkpoints.md`

## `HFT-CP0` Latency and Allocation Observatory
1. Done and not touched:
   - Stage timestamps in runtime health: `recv_ws_frame_ts`, `parsed_ts`, `state_updated_ts`, `signal_decided_ts`, `order_intent_enqueued_ts` (proxy until CP6 queue).
   - Internal latency histograms (`samples/p50/p95/p99/max`) for ingest, decision, end-to-end.
   - Runtime backlog depth counters (`binance`, `gate`, `signal`) exposed via health.
   - Drift sampling and log summary retained.
2. Done but must be reworked:
   - None.
3. Missing and required:
   - Optional: persist baseline snapshots for automated before/after diffing (not required for CP0 exit).

## `HFT-CP1` SymbolId and Allocation Removal
1. Done and not touched:
   - Price representation in ticks.
   - Runtime uses `Bytes` keys instead of `String` in latest maps.
   - `StrategySymbolIndex` introduced with stable `SymbolId`.
   - Signal backlog and stage timestamps switched to `SymbolId` (no symbol cloning in pending queue path).
   - Exchange batch processing now emits deduped `updated_strategy_symbol_ids`; downstream hot-path steps consume ids directly.
   - Per-exchange latest-book cache by `SymbolId` added and wired into stage timestamping + strategy book updates.
   - `updated_strategy_symbol_ids` are now derived directly from incoming ticker batch (removed intermediate `updated_symbols` allocation from runtime path).
   - Runtime `EventLoopState` no longer stores transitional `latest_* HashMap<Bytes, BookTicker>`; hot-path state uses per-exchange `SymbolId` caches.
   - Connector parse path now extracts string fields by borrowed slices (`&[u8]`) and reuses symbol bytes via cache; repeated `Bytes::copy_from_slice` on parsed symbol fields removed.
2. Done but must be reworked:
   - Strategy lookup path still calls `check_signal(&str)` and resolves symbol string by id.
3. Missing and required:
   - Extend `SymbolId` directly to connector output (emit id-indexed ticker updates instead of symbol bytes in runtime ingest boundary).
   - Remove remaining parser-side per-field pattern formatting overhead (`format!(\"\\\"{}\\\"\", field)`) from hot decode paths.

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
