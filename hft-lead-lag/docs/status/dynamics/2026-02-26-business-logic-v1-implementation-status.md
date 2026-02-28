# Implementation Status — HFT Runtime Migration

Date: 2026-02-26
Last sync: 2026-02-28 (CP5/CP6 remediation hardening synced)
Strategic anchor: `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
Roadmap anchor: `docs/status/core/2026-02-26-business-logic-roadmap.md`
Checkpoint set: `docs/status/dynamics/2026-02-28-hft-rust-only-checkpoints.md`

## 1) Executive status
1. Core Rust runtime is alive and producing signals/portfolio analytics.
2. Main HFT gap is not language choice, it is runtime hot-path shape: allocations, lock usage, symbol modeling, and observability depth.
3. Project is now tracked against `HFT-CP0..HFT-CP7` runtime-hardening ladder.

## 2) Runtime planes (current)
| Plane | Current implementation | Status |
|---|---|---|
| Signal/Shadow runtime | Rust (`src/domain/screener/*`) | `Implemented` |
| Candidate math and ranking | Rust | `Implemented` |
| Portfolio race analytics | Rust + API/UI | `Implemented (analytics)` |
| Forward orchestration | Python/Ray (`ray_driver/*`) + Rust IPC | `Transitional (control/cold path)` |
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
| `HFT-CP7` Production Operations Layer | `Planned` | Health endpoint exists | Watchdog/recovery/alert contracts not complete |

## 4) What is kept from legacy track
1. Existing strategy and screener business logic is reused as functional baseline.
2. Existing reliability hotfixes (dedupe/guard/reload hardening) remain valid.
3. Existing operator pages remain and will consume new observability metrics from `HFT-CP0`.

## 5) What is explicitly deprecated
1. String-heavy symbol handling in runtime hot loops.
2. Lock-based strategy state in the hottest path.
3. Any new feature that introduces Python into runtime data-plane.

## 6) Top open gaps (priority)
1. `P1`: implement `HFT-CP7` ops hardening (watchdogs, deterministic recovery runbook, alert contracts).
2. `P2`: optional `HFT-CP0` baseline snapshot automation for before/after regression diffs.

## 7) Tracking rule
For every status change:
1. Reference one `HFT-CP*` checkpoint.
2. Add concrete code evidence (path/function/test).
3. Mark legacy impact (if any).
