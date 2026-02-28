# Implementation Status — HFT Runtime Migration

Date: 2026-02-26
Last sync: 2026-02-28 (CP5 closed: runtime raw capture + deterministic replay mode)
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
| Execution fast path SLA layer | Partial | `Planned` |

## 3) HFT checkpoint progress
| Checkpoint | Status | Evidence | Main gap |
|---|---|---|---|
| `HFT-CP0` Latency and Allocation Observatory | `Completed` | `/health` includes staged timestamps, ingest/decision/e2e latency stats, and runtime backlog depths | `order_intent_enqueued_ts` remains proxy-level until CP6 execution queue is live |
| `HFT-CP1` SymbolId and Allocation Removal | `Completed` | Hot-path ingest now dedupes latest-per-symbol, runtime consumes connector-attached `strategy_symbol_id`, and connectors/runtime use canonical `symbol->id` builder with fail-fast capacity guard | None |
| `HFT-CP2` Lock-Free Strategy State | `Completed` | `LeadLagStrategy` migrated to single-owner state (no `RwLock/Mutex` in hot path); runtime strategy interface is `&mut self` sync; strategy updates pass through explicit event-loop queue boundary; p99 capture recorded | None (`docs/status/dynamics/2026-02-28-cp2-lock-free-p99-evidence.md`) |
| `HFT-CP3` Event-Driven Updated-Only Processing | `Completed` | Updated-symbol batch de-duplicates without sort; pending-signal queue uses `SymbolId` bitset (`PendingSymbolSet`); strategy-update queue carries tickers directly into strategy apply (no runtime cache-lookup clone in flush path) | None (`docs/status/dynamics/2026-02-28-cp3-updated-only-proof.md`) |
| `HFT-CP4` Minimal-Copy WS Parse Path | `Completed` | ticks and fast parse are in place; symbol cache interns raw bytes directly; runtime parse uses pattern-based extractors with early `strategy_symbol_id`; connector drain dedupe is `strategy_symbol_id`-first; Gate trade parse removes redundant re-interning; parser profile baselines and contract-priority regression test are captured in CP4 evidence doc | None |
| `HFT-CP5` Deterministic Replay Harness | `Completed` | `src/infrastructure/replay/raw_feed.rs` provides recorder/reader + deterministic signal replay equivalence checks; Binance/Gate ingest is wired to recorder and `main` supports offline replay mode (`REPLAY_RAW_FEED_PATH`) | None |
| `HFT-CP6` Execution Fast Path | `Planned` | N/A | No explicit non-blocking `OrderIntent` queue SLA contract |
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
1. `P1`: implement `HFT-CP6` execution SLA contracts and `HFT-CP7` ops hardening.
2. `P2`: optional `HFT-CP0` baseline snapshot automation for before/after regression diffs.

## 7) Tracking rule
For every status change:
1. Reference one `HFT-CP*` checkpoint.
2. Add concrete code evidence (path/function/test).
3. Mark legacy impact (if any).
