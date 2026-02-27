# HFT Rust-Only Checkpoints v2

Date: 2026-02-28
Status: Active
Strategic anchor: `docs/status/core/2026-02-27-business-objective-economic-control-map.md`

## 1) Locked Architecture Invariant
1. Runtime hot path is Rust-only.
2. Python/Ray is allowed only for offline/cold tasks and must not consume runtime CPU budget on the trading host.
3. Control-plane and data-plane are separated by explicit contracts.

## 2) Checkpoint Ladder (to production)
| Checkpoint | Status | Scope | Exit gate |
|---|---|---|---|
| `HFT-CP0` Latency and Allocation Observatory | `Completed` | Stage timestamps, internal latency histograms, drop counters, backlog depth | One endpoint shows p50/p95/p99/max and drop/backlog metrics; baseline captured |
| `HFT-CP1` SymbolId and Allocation Removal | `Completed` | Replace `String`/`HashMap<String,...>` in hot path with `SymbolId` and array-style state | Hot-path profile no longer dominated by `String` allocation/sort/dedup |
| `HFT-CP2` Lock-Free Strategy State | `In Progress` | Remove `RwLock/Mutex` from strategy hot path, single-owner engine state, queue-fed updates | No `RwLock::write().await`/`Mutex::lock()` in hot strategy path; p99 tail stabilizes |
| `HFT-CP3` Event-Driven Updated-Only Processing | `Planned` | Process only updated symbols via bitset/updated-id set | CPU scales with update rate, not universe size |
| `HFT-CP4` Minimal-Copy WS Parse Path | `Planned` | Parse only required fields, map symbol to id early, avoid symbol copying in hot path | Parser is not dominant in profile, symbol-copy hot spots removed |
| `HFT-CP5` Deterministic Replay Harness | `Planned` | Raw feed recorder + replay mode + decision equivalence checks | Any bug/regression is reproducible on replay with deterministic outcomes |
| `HFT-CP6` Execution Fast Path | `Planned` | Non-blocking `OrderIntent` queue, async fire-and-track, strict send SLA, kill-switch | Strategy thread is non-blocking on network I/O; internal intent->sent SLA measured |
| `HFT-CP7` Production Operations Layer | `Planned` | Watchdogs, idempotent snapshot/restore, stall/drop/backlog alerting | Operational runbook/alerts are green and recovery is deterministic |

## 3) Legacy Mapping (for continuity)
1. Existing Rust signal/validation portfolio logic remains the functional base.
2. Legacy CP artifacts are historical context, not planning authority.
3. Python `ray_driver` is transitional for research/control tasks only.

## 4) Review Rule
Each `HFT-CP*` is reviewed as a separate large round:
1. Commits and changes.
2. Bugs and runtime failures.
3. Architecture and design.
4. Logic and math.
5. Duplication/overcomplexity.
6. Cognitive load and god objects.
7. Preventive architecture.
8. Dead code.
9. Screener design.
10. Shadow fleet design.
