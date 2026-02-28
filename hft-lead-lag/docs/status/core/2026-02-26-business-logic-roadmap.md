# Business Logic Roadmap — HFT Runtime Track

Date: 2026-02-26
Last sync: 2026-02-28 (CP4 parse-path and early symbol-id assignment updates)

## Canonical sources
1. `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
2. `docs/status/core/2026-02-27-operating-model-spec-v1.md`
3. `docs/status/core/2026-02-28-hft-rust-only-checkpoints.md`

## Locked direction
1. Keep business objective unchanged.
2. Build deterministic low-jitter runtime path first (CP0-CP4).
3. Add replay, execution SLA, and operations layers after runtime stabilization (CP5-CP7).
4. Keep Python/Ray out of runtime data-plane and off the trading host CPU budget (offline only).

## Checkpoint Status (current baseline)
| Checkpoint | Status | Notes |
|---|---|---|
| `HFT-CP0` Latency and Allocation Observatory | `Completed` | `/health` exposes staged timestamps + latency snapshots + backlog gauges; `order_intent_enqueued_ts` remains proxy-level until CP6 execution queue lands. |
| `HFT-CP1` SymbolId and Allocation Removal | `Completed` | Runtime/connector path is `SymbolId`-first with canonical id map builder, per-batch latest dedupe, and capacity fail-fast (no silent truncation). |
| `HFT-CP2` Lock-Free Strategy State | `Completed` | Lead-lag strategy moved to single-owner state and sync `&mut self` runtime path; explicit event-loop queue boundary added for strategy updates; p99 evidence captured (`2026-02-28-cp2-lock-free-p99-evidence.md`). |
| `HFT-CP3` Event-Driven Updated-Only Processing | `Completed` | Updated flow is `Bytes`-based with no string-sort path; pending signal store migrated to `SymbolId` bitset (`PendingSymbolSet`); strategy-update queue applies tickers directly without runtime cache-lookup clone; proof stored in `2026-02-28-cp3-updated-only-proof.md`. |
| `HFT-CP4` Minimal-Copy WS Parse Path | `In Progress` | Fast parsing exists; symbol cache interns raw bytes directly; runtime parse paths use pattern-based extractors with early `strategy_symbol_id` assignment in Binance/Gate; connector drain dedupe is `strategy_symbol_id`-first and Gate trade parse avoids redundant re-interning; remaining parse/copy hot spots still need cleanup. |
| `HFT-CP5` Deterministic Replay Harness | `Planned` | Full record/replay contract is not implemented. |
| `HFT-CP6` Execution Fast Path | `Planned` | Order intent queue + non-blocking send SLA are not formalized. |
| `HFT-CP7` Production Operations Layer | `Planned` | Watchdog/recovery/alert stack not closed. |

## Delivery sequence to production
1. `HFT-CP0` delivered; optional baseline snapshot automation remains as non-blocking enhancement.
2. `HFT-CP1` and `HFT-CP2` delivered (allocation and lock-jitter elimination).
3. `HFT-CP3` delivered (updated-only execution path).
4. Deliver `HFT-CP4` (minimal-copy parse path).
5. Deliver `HFT-CP5` (deterministic replay for bugs and perf regression).
6. Deliver `HFT-CP6` (execution fast path and intent->sent SLA).
7. Deliver `HFT-CP7` (operations hardening and deterministic recovery).

## Legacy continuity map
1. Legacy signal/validation/scoring remains reusable baseline.
2. Existing `/portfolio` and `/trials` UI are retained but are not proof of hot-path readiness.
3. Python orchestration remains transitional and must not re-enter runtime hot path.

## Exit criteria for re-baseline stage
1. All status docs reference `HFT-CP*` as primary checkpoint system.
2. Legacy checkpoints remain only as historical trace, not future planning axis.
3. Every new implementation task explicitly maps to one `HFT-CP*`.
