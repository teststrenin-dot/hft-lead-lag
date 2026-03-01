# Implementation Status — HFT Runtime Migration

Date: 2026-02-26
Last sync: 2026-02-28 (CP7 block8 external alert-hook script added)
Strategic anchor: `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
Roadmap anchor: `docs/status/core/2026-02-26-business-logic-roadmap.md`
Checkpoint set: `docs/status/dynamics/2026-02-28-hft-rust-only-checkpoints.md`

## 1) Executive status
1. Core Rust runtime is alive and producing signals/portfolio analytics.
2. Main open HFT gaps are ops hardening (`watchdog/recovery`) and observer-plane completion, not language/runtime ownership.
3. Project is now tracked against `HFT-CP0..HFT-CP7` runtime-hardening ladder.

## 2) Runtime planes (current)
| Plane | Current implementation | Status |
|---|---|---|
| Signal/Shadow runtime | Rust (`src/domain/screener/*`) | `Implemented` |
| Candidate math and ranking | Rust | `Implemented` |
| Portfolio race analytics | Rust + API/UI | `Implemented (analytics)` |
| Scout pre-stage (range discovery only) | Rust target (`scout` -> corridor artifact), legacy Python/Ray kept as transitional cold-path only | `In transition` |
| Execution fast path SLA layer | Rust queue/worker + health SLA telemetry | `Implemented` |

## 3) HFT checkpoint progress
| Checkpoint | Status | Evidence | Main gap |
|---|---|---|---|
| `HFT-CP0` Latency and Allocation Observatory | `Completed` | `/health` includes staged timestamps, ingest/decision/e2e latency stats, execution intent->sent latency stats, and runtime backlog depths | None |
| `HFT-CP1` SymbolId and Allocation Removal | `Completed` | Hot-path ingest now dedupes latest-per-symbol, runtime consumes connector-attached `strategy_symbol_id`, and connectors/runtime use canonical `symbol->id` builder with fail-fast capacity guard | None |
| `HFT-CP2` Lock-Free Strategy State | `Completed` | `LeadLagStrategy` migrated to single-owner state (no `RwLock/Mutex` in hot path); runtime strategy interface is `&mut self` sync; strategy updates pass through explicit event-loop queue boundary; p99 capture recorded | None (`docs/status/dynamics/2026-02-28-cp2-lock-free-p99-evidence.md`) |
| `HFT-CP3` Event-Driven Updated-Only Processing | `Completed` | Updated-symbol batch de-duplicates without sort; pending-signal queue uses `SymbolId` bitset (`PendingSymbolSet`); strategy-update queue carries tickers directly into strategy apply (no runtime cache-lookup clone in flush path) | None (`docs/status/dynamics/2026-02-28-cp3-updated-only-proof.md`) |
| `HFT-CP4` Minimal-Copy WS Parse Path | `Completed` | ticks and fast parse are in place; symbol cache interns raw bytes directly; runtime parse uses pattern-based extractors with early `strategy_symbol_id`; connector drain dedupe is `strategy_symbol_id`-first; Gate trade parse removes redundant re-interning; parser profile baselines and contract-priority regression test are captured in CP4 evidence doc | None |
| `HFT-CP5` Deterministic Replay Harness | `Completed` | `src/infrastructure/replay/raw_feed.rs` provides recorder/reader + deterministic signal replay equivalence checks; recorder sequence advances only on successful write+flush, reader rejects invalid JSON and out-of-order sequence, and concurrent monotonic-sequence behavior is test-covered; Binance/Gate ingest is wired to recorder and `main` supports offline replay mode (`REPLAY_RAW_FEED_PATH`) | None |
| `HFT-CP6` Execution Fast Path | `Completed` | `src/event_loop_execution.rs` provides bounded `OrderIntent` queue (`try_send`), async timeout-enforced send worker, queue-depth drift protection, full-queue latest-by-symbol overflow lane, stale-intent max-age drop guard, kill-switch cooldown auto-recovery, and execution latency/counter telemetry; signal loop enqueue is wired in `src/event_loop_core.rs` | None (`docs/status/dynamics/2026-02-28-cp6-execution-fast-path-evidence.md`) |
| `HFT-CP7` Production Operations Layer | `In progress` | `/health` enforces RM4 windows (`block1`), event-driven signal loop removes timer jitter (`block2`), watchdog stalls are active (`block3`), recovery runbook v1 is codified (`block4`), recovery drill script is shipped (`block5`), DB-writer/drift alert contracts are runtime-visible (`block6`), machine-readable `alert_level` is active (`block7`), and external alert-gate script is shipped (`block8`) (`docs/status/dynamics/2026-02-28-cp7-block1-rm4-health-enforcement.md`, `docs/status/dynamics/2026-02-28-cp7-block2-event-driven-signal-loop-evidence.md`, `docs/status/dynamics/2026-02-28-cp7-block3-watchdog-stall-evidence.md`, `docs/status/dynamics/2026-02-28-cp7-block4-recovery-runbook-v1.md`, `docs/status/dynamics/2026-02-28-cp7-block5-recovery-drill-automation-evidence.md`, `docs/status/dynamics/2026-02-28-cp7-block6-dbwriter-drift-alert-evidence.md`, `docs/status/dynamics/2026-02-28-cp7-block7-alert-level-escalation-contract.md`, `docs/status/dynamics/2026-02-28-cp7-block8-alert-hook-script-evidence.md`) | Scheduled policy integration (cron/systemd/CI) remains open |

## 3.1) Remediation progress (`HFT-RM*`)
| Remediation | Status | Evidence | Main gap |
|---|---|---|---|
| `HFT-RM1` Plane mode split (`mixed` vs `hft_core`) | `Completed` | `RUNTIME_PLANE_MODE` startup contract is wired and verified: `hft_core` disables runtime-grid helpers/NATR/persistence/scheduler and uses strategy-only subscriptions; `mixed` keeps control-plane path; dedicated mode parser tests are in `src/main_tests.rs` | None (`docs/status/dynamics/2026-02-28-rm1-plane-mode-contract-evidence.md`) |
| `HFT-RM2` Screener decoupling from data-plane | `Completed` | Bounded `ControlUpdate` handoff worker is runtime-wired; `/health` exposes control queue depth + dropped updates; runtime ingest enforces control-plane production path and constrains direct-ingest fallback to test builds only; overflow lane is keyed by `(symbol, exchange)` and regression-tested | None (`docs/status/dynamics/2026-02-28-rm2-control-plane-decoupling-evidence.md`) |
| `HFT-RM3` 2-core host budget guardrails | `Completed` | Startup now enforces frozen 2-core defaults (`64/128/512`) for `MAX_STRATEGY_SYMBOLS` / `MAX_SCREENER_SYMBOLS` / `MAX_RUNTIME_GRID_CONFIGS`; env overrides still work and cap source is logged on truncation | None (`docs/status/dynamics/2026-02-28-rm3-2core-cap-profile-evidence.md`) |
| `HFT-RM4` Numeric HFT SLO freeze | `Completed` | Core contracts now define numeric latency/backlog/drop envelopes and a `3`-window degradation rule tied to `/health` fields (`docs/status/core/2026-02-27-business-objective-economic-control-map.md`, `docs/status/core/2026-02-27-operating-model-spec-v1.md`) | Keep CP7 ops checks aligned with frozen contract |
| `HFT-RM5` Control-plane threaded isolation + coalesced apply | `Completed` | Control worker moved to dedicated OS thread/runtime; updates are coalesced latest-per `(symbol, exchange)` within flush windows before `screener.update`; runtime-grid default `max_configs` reduced to `512` with regression tests | Optional CP7 visibility polish only (`docs/status/dynamics/2026-02-28-rm5-control-plane-threaded-coalescing-evidence.md`) |

## 4) What is kept from legacy track
1. Existing strategy and screener business logic is reused as functional baseline.
2. Existing reliability hotfixes (dedupe/guard/reload hardening) remain valid.
3. Operator surface remains observer-first (`symbol race` + `portfolio race`) with minimal control (`scout` + `forward` start only).
4. Runner control path now enforces scoped phases (`scout`, `forward`) with forward prerequisite guard in command/start flow (`docs/status/dynamics/2026-02-28-observer-scout-forward-control-evidence.md`).

## 5) What is explicitly deprecated
1. String-heavy symbol handling in runtime hot loops.
2. Lock-based strategy state in the hottest path.
3. Any new feature that introduces Python into runtime data-plane.
4. Broad UI orchestration controls beyond observer scope and allowed starts (`scout`, guarded `forward`).

## 6) Top open gaps (priority)
1. `P1`: finish `HFT-CP7` ops hardening (scheduled policy integration for drills/hooks).
2. `P1`: finish observer-plane contract for near-realtime race feedback without contaminating hot path.
3. `P2`: optional `HFT-CP0` baseline snapshot automation for before/after regression diffs.

## 7) Tracking rule
For every status change:
1. Reference one `HFT-CP*` checkpoint.
2. Add concrete code evidence (path/function/test).
3. Mark legacy impact (if any).
