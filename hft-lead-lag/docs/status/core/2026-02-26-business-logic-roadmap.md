# Business Logic Roadmap — HFT Runtime Track

Date: 2026-02-26
Last sync: 2026-02-28 (`HFT-CP` re-baseline)

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
| `HFT-CP0` Latency and Allocation Observatory | `In Progress` | `/health` exposes staged timestamps + latency snapshots + backlog gauges; final operator polish pending. |
| `HFT-CP1` SymbolId and Allocation Removal | `Completed` | Runtime/connector path is `SymbolId`-first with canonical id map builder, per-batch latest dedupe, and capacity fail-fast (no silent truncation). |
| `HFT-CP2` Lock-Free Strategy State | `In Progress` | Lead-lag strategy moved to single-owner state and sync `&mut self` runtime path; explicit event-loop queue boundary added for strategy updates; p99 evidence remains. |
| `HFT-CP3` Event-Driven Updated-Only Processing | `In Progress` | Updated flow is `Bytes`-based and no longer sorts string batches; `SymbolId` bitset path remains. |
| `HFT-CP4` Minimal-Copy WS Parse Path | `Planned` | Fast parsing exists but symbol copy path remains in critical segments. |
| `HFT-CP5` Deterministic Replay Harness | `Planned` | Full record/replay contract is not implemented. |
| `HFT-CP6` Execution Fast Path | `Planned` | Order intent queue + non-blocking send SLA are not formalized. |
| `HFT-CP7` Production Operations Layer | `Planned` | Watchdog/recovery/alert stack not closed. |

## Delivery sequence to production
1. Close `HFT-CP0` and capture baseline p99/drop/backlog.
2. Deliver `HFT-CP1` and `HFT-CP2` (allocation and lock-jitter elimination).
3. Deliver `HFT-CP3` and `HFT-CP4` (updated-only execution + minimal-copy parse path).
4. Deliver `HFT-CP5` (deterministic replay for bugs and perf regression).
5. Deliver `HFT-CP6` (execution fast path and intent->sent SLA).
6. Deliver `HFT-CP7` (operations hardening and deterministic recovery).

## Legacy continuity map
1. Legacy signal/validation/scoring remains reusable baseline.
2. Existing `/portfolio` and `/trials` UI are retained but are not proof of hot-path readiness.
3. Python orchestration remains transitional and must not re-enter runtime hot path.

## Exit criteria for re-baseline stage
1. All status docs reference `HFT-CP*` as primary checkpoint system.
2. Legacy checkpoints remain only as historical trace, not future planning axis.
3. Every new implementation task explicitly maps to one `HFT-CP*`.
