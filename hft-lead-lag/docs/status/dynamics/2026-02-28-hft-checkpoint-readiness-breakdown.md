# HFT Checkpoint Readiness Breakdown

Date: 2026-02-28
Status: Active
Checkpoint set: `docs/status/dynamics/2026-02-28-hft-rust-only-checkpoints.md`

## `HFT-CP0` Latency and Allocation Observatory
1. Done and not touched:
   - Stage timestamps in runtime health: `recv_ws_frame_ts`, `parsed_ts`, `state_updated_ts`, `signal_decided_ts`, `order_intent_enqueued_ts`, `order_intent_sent_ts`.
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
   - Runtime batch boundary now materializes `SymbolId + ticker` updates in a single pass and writes symbol-id caches without secondary symbol lookups.
   - Hot WS message classification now uses byte-slice matching (`contains_bytes`) instead of per-message UTF-8 conversion.
   - Common extractor path now supports `*_by_pattern` APIs for `string/i64/bool`, enabling parser calls without per-invocation pattern `format!`.
   - Gate nested decode hot path (`data.p`, `data.s`) now uses static byte patterns; dynamic pattern formatting removed from runtime calls.
   - Connectors now attach `strategy_symbol_id` to `BookTicker` at source; runtime consumes id-indexed updates directly at ingest boundary.
   - Runtime strategy checks now execute by `SymbolId` in event loop (`check_signal(symbol_id)`), removing per-tick `id -> string` resolution in hot loop.
   - Canonical `symbol -> SymbolId` map builder extracted to domain helper and reused by connectors/runtime test index to eliminate id-construction drift.
   - Runtime startup now fails fast on `SymbolId` capacity overflow; silent truncation removed.
   - Runtime ingest path now restores latest-per-symbol batch dedupe before screener/ws update fanout.
2. Done but must be reworked:
   - None.
3. Missing and required:
   - None.
   - Optional cleanup: migrate/trim non-hot legacy parser wrappers that still rely on dynamic field-name formatting for compatibility callers.

## `HFT-CP2` Lock-Free Strategy State
1. Done and not touched:
   - Runtime strategy/event loop wiring exists.
   - `LeadLagStrategy` hot-path state is now single-owner (`HashMap` + direct fields), with no `Arc<RwLock<...>>`/`Arc<Mutex<...>>`.
   - Runtime strategy API switched to sync `&mut self` path; event loop feeds strategy updates/checks without async lock points.
   - Event loop now uses explicit strategy-update queue boundary (`enqueue_strategy_updates` -> `flush_strategy_updates`) between ingest and strategy apply.
   - Live p99 evidence captured and stored in `docs/status/dynamics/2026-02-28-cp2-lock-free-p99-evidence.md`.
2. Done but must be reworked:
   - None.
3. Missing and required:
   - None.

## `HFT-CP3` Event-Driven Updated-Only Processing
1. Done and not touched:
   - Pending-symbol scheduling mechanism exists.
   - Pending-symbol store is now `SymbolId` bitset-backed (`PendingSymbolSet`), replacing tree-based pending queue.
   - Strategy-update queue now carries `(ExchangeSide, SymbolId, BookTicker)` and flushes directly into strategy apply (removed runtime latest-cache lookup clone on this path).
   - Rate-proportional behavior proof is captured in `docs/status/dynamics/2026-02-28-cp3-updated-only-proof.md`.
2. Done but must be reworked:
   - None.
3. Missing and required:
   - None.

## `HFT-CP4` Minimal-Copy WS Parse Path
1. Done and not touched:
   - Fast float parsing and tick conversion.
   - Symbol cache now interns raw byte keys/values directly (`Vec<u8> -> Bytes`) without UTF-8 fallback conversion.
   - Runtime parse paths are pattern-based; dynamic wrapper APIs remain for compatibility but are not used on hot path.
   - Binance/Gate `parse_book_ticker_static` now assign `strategy_symbol_id` directly during parse (no post-parse attach step).
   - Fast numeric extractors now handle scientific notation (`e/E`) in numeric token paths.
   - Connector drain dedupe now keys by `strategy_symbol_id` when present, reducing symbol-byte hashing/cloning on the hot receive drain path.
   - Gate trade parse now reuses normalized symbol bytes directly (removed redundant `intern_bytes` pass on already-normalized symbol).
   - Gate parser now prefers `contract` over `s/c` fallback keys and is protected by regression test.
   - Synthetic parse benchmark harness baselines are recorded (debug/release).
   - Current CP4 evidence log is tracked in `docs/status/dynamics/2026-02-28-cp4-parse-path-evidence.md`.
2. Done but must be reworked:
   - None.
3. Missing and required:
   - None.
   - Optional debt only: migrate or remove compatibility-only dynamic extractor wrappers in `common.rs`.

## `HFT-CP5` Deterministic Replay Harness
1. Done and not touched:
   - General tests and reliability fixes from legacy track.
   - CP5 replay core: `src/infrastructure/replay/raw_feed.rs` adds:
     - JSONL raw-feed recorder (`seq`, `exchange`, `recv_ts_ns`, `payload_b64`).
     - Strict replay reader with sequence-order validation and payload decode validation.
     - Deterministic signal replay trace + equivalence report.
     - Contract tests for deterministic round-trip, invalid payload rejection, and replay determinism.
   - Evidence captured in `docs/status/dynamics/2026-02-28-cp5-block1-raw-feed-evidence.md`.
   - Runtime capture wiring:
     - `BinanceMarketData` and `GateMarketData` record incoming raw WS frames when recorder is configured.
     - `main` enables recording via `RAW_FEED_RECORD_PATH`.
   - Offline replay mode:
     - `main` runs deterministic replay check when `REPLAY_RAW_FEED_PATH` is set.
2. Done but must be reworked:
   - Recorder currently flushes per frame for reliability-first baseline; batching can be optimized later if needed.
3. Missing and required:
   - None.
   - Optional: add richer replay diff report for first divergence context.

## `HFT-CP6` Execution Fast Path
1. Done and not touched:
   - Signal-producing runtime foundation.
   - Bounded non-blocking `OrderIntent` queue (`try_send`) is wired from signal loop.
   - Async execution worker with strict timeout and kill-switch timeout-streak contract.
   - `/health` exposes execution queue depth, sent/dropped/timeout counters, kill-switch state, and intent->sent latency snapshot.
   - Evidence is captured in `docs/status/dynamics/2026-02-28-cp6-execution-fast-path-evidence.md`.
2. Done but must be reworked:
   - None.
3. Missing and required:
   - None.

## `HFT-CP7` Production Operations Layer
1. Done and not touched:
   - Base health endpoint and saturation counters.
2. Done but must be reworked:
   - Ops coverage is incomplete for production deterministic recovery.
3. Missing and required:
   - Component watchdogs (`feed/engine/execution/dbwriter`).
   - Alert contract for drift, drops, backlog, engine stall.
   - Idempotent snapshot/restore runbook and recovery verification.
