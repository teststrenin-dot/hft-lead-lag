# HFT Checkpoint Readiness Breakdown

Date: 2026-02-28
Status: Active
Checkpoint set: `docs/status/dynamics/2026-02-28-hft-rust-only-checkpoints.md`
Last sync: 2026-02-28 (CP7 block8 alert-hook script landed; dynamics scope aligned to observer-first UI + `scout+forward` control)

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
   - Post-review hardening:
     - recorder now updates `seq` only after successful write+flush inside one mutex-protected recorder state.
     - connector record path now handles and logs recorder IO errors (`io::Result<()>`) instead of silent drop.
     - replay reader rejects malformed JSON lines and out-of-order sequence with contextual `InvalidData`.
     - concurrent monotonic-sequence stress test is added for recorder safety.
2. Done but must be reworked:
   - None.
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
   - Post-review hardening:
     - queue-depth accounting now reserves before send and decrements on consume/failure, removing producer-consumer drift.
     - full queue now stores latest overflow intent per symbol and drains overflow lane in worker.
     - stale-intent max-age guard drops outdated intents before send path.
     - kill-switch recovers automatically after cooldown timer.
2. Done but must be reworked:
   - None.
3. Missing and required:
   - None.

## `HFT-CP7` Production Operations Layer
1. Done and not touched:
   - Base health endpoint and saturation counters.
   - RM4 health-window enforcement is now runtime-active:
     - per-window SLO breach evaluation in `/health`
     - consecutive breach streak tracking
     - `degraded_non_hft` escalation after 3 consecutive breached windows
   - Evidence captured in:
     - `docs/status/dynamics/2026-02-28-cp7-block1-rm4-health-enforcement.md`
     - `docs/status/dynamics/2026-02-28-rm4-hft-core-live-slo-validation.md`
     - `docs/status/dynamics/2026-02-28-cp7-block2-event-driven-signal-loop-evidence.md`
     - `docs/status/dynamics/2026-02-28-cp7-block3-watchdog-stall-evidence.md`
     - `docs/status/dynamics/2026-02-28-cp7-block4-recovery-runbook-v1.md`
     - `docs/status/dynamics/2026-02-28-cp7-block5-recovery-drill-automation-evidence.md`
     - `docs/status/dynamics/2026-02-28-cp7-block6-dbwriter-drift-alert-evidence.md`
     - `docs/status/dynamics/2026-02-28-cp7-block7-alert-level-escalation-contract.md`
     - `docs/status/dynamics/2026-02-28-cp7-block8-alert-hook-script-evidence.md`
   - Runtime watchdog issue signals are active in `/health` for:
     - `engine_state_stall`
     - `signal_loop_stall`
     - `execution_loop_stall`
   - Recovery runbook v1 defines deterministic restart/validation flow with idempotent restore checks.
   - Automated recovery drill script is now available:
     - `scripts/ops/health_recovery_drill.sh`
   - DB writer progress is observable in `/health` and stall is flagged via:
     - `db_writer_stall`
   - Drift alert signal is runtime-visible in `/health` via:
     - `drift_p99_high`
   - Operator severity is machine-readable via `/health.alert_level` (`ok|warn|critical`).
   - External alert-hook script is available:
     - `scripts/ops/health_alert_gate.sh`
2. Done but must be reworked:
   - Recovery drill is script-level; still needs orchestration integration in ops pipeline.
3. Missing and required:
   - Continuous scheduled execution of recovery drill in CI/ops loop.
   - Deploy and schedule alert hooks/drills under concrete policy (cron/systemd/CI).

## Observation / UI-Feedback Scope (active)
1. Done and not touched:
   - `mixed` mode provides portfolio/symbol race observation through API/UI.
   - UI runner control surface is constrained to `scout` + `forward`, with server-side rejection of other phases and forward prerequisite guard (`2026-02-28-observer-scout-forward-control-evidence.md`).
2. Done but must be reworked:
   - polling UI remains transitional; near-realtime observer stream is still open.
3. Missing and required:
   - avoid re-introducing broad trials/runner control endpoints in active contour.
   - keep observer plane isolated from `hft_core` hot path.

## `HFT-RM1` Plane mode split (`mixed` vs `hft_core`)
1. Done and not touched:
   - Runtime supports explicit plane mode via `RUNTIME_PLANE_MODE` (`mixed` / `hft_core`).
   - `hft_core` mode disables direct screener ingest in event loop and disables portfolio scheduler tick in runtime loop.
   - `hft_core` mode uses strategy-only subscriptions and does not start runtime-grid hot reload / NATR refresher / screener DB persistence.
   - `hft_core` server surface is health-only (`/health`): trial runner/trials orchestration routes are not exposed.
   - Mode parser contract is regression-tested in `src/main_tests.rs` (default/valid/invalid inputs).
   - Contract evidence is captured in `docs/status/dynamics/2026-02-28-rm1-plane-mode-contract-evidence.md`.
2. Done but must be reworked:
   - None.
3. Missing and required:
   - None for RM1 closure.

## `HFT-RM2` Screener decoupling from data-plane
1. Done and not touched:
   - `ScreenerStore` and portfolio runtime logic are functionally complete for paper analytics.
   - Bounded control-plane queue/worker is wired: ingest path enqueues `ControlUpdate`; worker applies `screener.update` and WS fanout outside strategy hot path.
   - Full queue policy is latest-by-symbol overflow lane (not hard drop-all).
   - `/health` exposes control-plane backlog and dropped-update counters.
   - Overflow-lane replacement behavior is covered by regression test in `src/event_loop_control.rs`.
   - RM2 evidence bundle is captured in `docs/status/dynamics/2026-02-28-rm2-control-plane-decoupling-evidence.md`.
   - Runtime direct-ingest compatibility path is constrained to test builds only (`cfg!(test)` guard in `src/event_loop_core.rs`).
2. Done but must be reworked:
   - None.
3. Missing and required:
   - None for RM2 closure.
   - CP7 may add continuous runtime assertion that production runs always carry non-null control-plane in `mixed` mode.

## `HFT-RM3` 2-core host budget guardrails
1. Done and not touched:
   - Runtime has explicit startup host caps for symbols and runtime-grid fanout:
     - `MAX_STRATEGY_SYMBOLS`
     - `MAX_SCREENER_SYMBOLS`
     - `MAX_RUNTIME_GRID_CONFIGS`
   - Frozen default profile is applied when env overrides are absent:
     - strategy symbols: `64`
     - screener symbols: `128`
     - runtime-grid configs: `512`
   - Cap application is logged with before/after counts and cap source (`env` or `2core_default`).
   - Evidence is captured in `docs/status/dynamics/2026-02-28-rm3-2core-cap-profile-evidence.md`.
2. Done but must be reworked:
   - None.
3. Missing and required:
   - None for RM3 closure.
   - CP7 can optionally add continuous capped-profile health-window auditing as ops automation.

## `HFT-RM4` Numeric HFT SLO freeze
1. Done and not touched:
   - `/health` already exposes latency and backlog metrics.
   - Core contracts now freeze numeric HFT envelopes and degradation rule:
     - `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
     - `docs/status/core/2026-02-27-operating-model-spec-v1.md`
   - Live `hft_core` window sampling evidence is captured in
     `docs/status/dynamics/2026-02-28-rm4-hft-core-live-slo-validation.md`.
2. Done but must be reworked:
   - None.
3. Missing and required:
   - CP7 block2+: continuous ops audit/alert pipeline around already-implemented RM4 auto-flagging (runtime check exists; runbook+automation remains).

## `HFT-RM5` Control-plane threaded isolation + coalesced apply
1. Done and not touched:
   - Control-plane worker now runs in dedicated OS thread with a current-thread Tokio runtime (`src/event_loop_control.rs`).
   - Worker batches updates by `(symbol, exchange)` and applies latest-only values per flush window.
   - Flush policy is bounded by interval and max-batch thresholds.
   - Runtime-grid default fanout is reduced to `512` in both default config template and checked-in runtime config.
   - Regression tests:
     - `control_plane_worker_coalesces_latest_update_within_flush_window`
     - `runtime_grid_config_default_matches_2core_profile`
   - Evidence bundle:
     - `docs/status/dynamics/2026-02-28-rm5-control-plane-threaded-coalescing-evidence.md`
2. Done but must be reworked:
   - None.
3. Missing and required:
   - Optional CP7 hardening: expose coalesce window/batch settings in `/health` for operator visibility.
