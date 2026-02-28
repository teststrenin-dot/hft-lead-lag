# HFT Rust-Only Checkpoints v2

Date: 2026-02-28
Status: Active
Strategic anchor: `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
Last sync: 2026-02-28 (stud2 remediation closed; CP7 block1 started)

## 1) Locked Architecture Invariant
1. Runtime hot path is Rust-only.
2. Python/Ray is allowed only for offline/cold tasks and must not consume runtime CPU budget on the trading host.
3. Control-plane and data-plane are separated by explicit contracts.

## 2) Checkpoint Ladder (to production)
| Checkpoint | Status | Scope | Exit gate |
|---|---|---|---|
| `HFT-CP0` Latency and Allocation Observatory | `Completed` | Stage timestamps, internal latency histograms, drop counters, backlog depth | One endpoint shows p50/p95/p99/max and drop/backlog metrics; baseline captured |
| `HFT-CP1` SymbolId and Allocation Removal | `Completed` | Replace `String`/`HashMap<String,...>` in hot path with `SymbolId` and array-style state | Hot-path profile no longer dominated by `String` allocation/sort/dedup |
| `HFT-CP2` Lock-Free Strategy State | `Completed` | Remove `RwLock/Mutex` from strategy hot path, single-owner engine state, queue-fed updates | No `RwLock::write().await`/`Mutex::lock()` in hot strategy path; p99 tail evidence captured (`2026-02-28-cp2-lock-free-p99-evidence.md`) |
| `HFT-CP3` Event-Driven Updated-Only Processing | `Completed` | Process only updated symbols via bitset/updated-id set | Pending-symbol bitset path and queue-carried ticker strategy updates are live; proof captured (`2026-02-28-cp3-updated-only-proof.md`) |
| `HFT-CP4` Minimal-Copy WS Parse Path | `Completed` | Parse only required fields, map symbol to id early, avoid symbol copying in hot path | Symbol cache interns raw bytes; Binance/Gate parse paths assign `strategy_symbol_id` during parse; drain dedupe is `SymbolId`-first; profile baselines + parse-order regression proof in `2026-02-28-cp4-parse-path-evidence.md` |
| `HFT-CP5` Deterministic Replay Harness | `Completed` | Raw feed recorder + replay mode + decision equivalence checks | JSONL recorder + strict reader are wired into WS ingest via opt-in env; recorder sequence advances only after successful write+flush; reader rejects malformed JSON/out-of-order sequence; offline replay determinism runner validates stable signal traces (`2026-02-28-cp5-block1-raw-feed-evidence.md`) |
| `HFT-CP6` Execution Fast Path | `Completed` | Non-blocking `OrderIntent` queue, async fire-and-track, strict send SLA, kill-switch | Strategy thread uses bounded `try_send` queue; queue-depth accounting is race-safe; full queue keeps latest overflow intent per symbol; stale intent max-age guard and kill-switch cooldown recovery are active; `/health` exposes intent->sent SLA metrics (`2026-02-28-cp6-execution-fast-path-evidence.md`) |
| `HFT-CP7` Production Operations Layer | `In progress` | RM4 SLO contract is runtime-enforced in `/health` windows with breach-streak escalation | Watchdogs + idempotent snapshot/restore runbook + full alert contracts are green |

## 2.1) Remediation Track (from `docs/studies/stud2.md`)
| Remediation | Status | Scope | Exit gate |
|---|---|---|---|
| `HFT-RM1` Plane mode split (`mixed` vs `hft_core`) | `Completed` | Explicit runtime mode contract is active and test-covered | `hft_core` mode runs without control-plane helpers and `mixed` keeps bounded handoff path (`2026-02-28-rm1-plane-mode-contract-evidence.md`) |
| `HFT-RM2` Screener decoupling from data-plane | `Completed` | Event loop uses bounded control-update handoff with latest-by-symbol overflow and health telemetry | Compatibility direct-ingest path is constrained to test builds only; production runtime path is control-plane only |
| `HFT-RM3` 2-core host budget guardrails | `Completed` | Hard caps for symbol/config fanout on trading host (`runtime-grid`, subscriptions, screener workload) are always applied with frozen 2-core defaults and env override path | Production default cap profile is frozen and documented (`2026-02-28-rm3-2core-cap-profile-evidence.md`) |
| `HFT-RM4` Numeric HFT SLO freeze | `Completed` | Hard p99/backlog/drop envelopes and degradation rule are now frozen in core contracts | Core docs are the single source of numeric fail/pass criteria tied to `/health` metrics |

Notes:
1. `HFT-RM*` is a mandatory continuation layer after CP6, not a replacement of CP0-CP7.
2. CP7 closure depends on operational enforcement of RM4 contract.

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
