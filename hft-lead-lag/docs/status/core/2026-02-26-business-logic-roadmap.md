# Business Logic Roadmap — HFT Runtime Track

Date: 2026-02-26
Last sync: 2026-02-28 (RM1-RM4 closed; CP7 block1 enforcement started)

## Canonical sources
1. `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
2. `docs/status/core/2026-02-27-operating-model-spec-v1.md`
3. `docs/status/dynamics/2026-02-28-hft-rust-only-checkpoints.md`

## Locked direction
1. Keep business objective unchanged.
2. Build deterministic low-jitter runtime path first (CP0-CP4).
3. Add replay, execution SLA, and operations layers after runtime stabilization (CP5-CP7).
4. Keep Python/Ray out of runtime data-plane and off the trading host CPU budget (offline only).
5. Post-CP6 mandatory remediation (`HFT-RM*`): enforce hot-path/control-plane split and host-budget compliance on target hardware.

## End-to-End Business Process (current stage)
1. Shadow ingest:
   - Runtime collects quotes/trades and computes closed-trade symbol stats.
2. Eligibility gate:
   - Candidate enters portfolio pool only if V1 thresholds pass.
3. Competition:
   - Ranked candidates are distributed into portfolio shortlists without overlap.
   - Active symbols are selected from shortlists without overlap and with `0..4` cap.
4. Risk containment:
   - Stop-loss streak rules trigger symbol cooldown.
   - Positive trades reset streak counter.
5. Re-entry:
   - After cooldown symbol may return only through eligibility gate.
6. Rebalance:
   - Scheduler applies assignment update every `120_000 ms`.
7. Outputs:
   - Portfolio assignment snapshots and paper performance metrics are persisted and exposed in UI/API.

## Checkpoint Status (current baseline)
| Checkpoint | Status | Notes |
|---|---|---|
| `HFT-CP0` Latency and Allocation Observatory | `Completed` | `/health` exposes staged timestamps + latency snapshots + backlog gauges, including execution intent->sent SLA metrics. |
| `HFT-CP1` SymbolId and Allocation Removal | `Completed` | Runtime/connector path is `SymbolId`-first with canonical id map builder, per-batch latest dedupe, and capacity fail-fast (no silent truncation). |
| `HFT-CP2` Lock-Free Strategy State | `Completed` | Lead-lag strategy moved to single-owner state and sync `&mut self` runtime path; explicit event-loop queue boundary added for strategy updates; p99 evidence captured (`2026-02-28-cp2-lock-free-p99-evidence.md`). |
| `HFT-CP3` Event-Driven Updated-Only Processing | `Completed` | Updated flow is `Bytes`-based with no string-sort path; pending signal store migrated to `SymbolId` bitset (`PendingSymbolSet`); strategy-update queue applies tickers directly without runtime cache-lookup clone; proof stored in `2026-02-28-cp3-updated-only-proof.md`. |
| `HFT-CP4` Minimal-Copy WS Parse Path | `Completed` | Fast parsing exists; symbol cache interns raw bytes directly; runtime parse paths use pattern-based extractors with early `strategy_symbol_id` assignment in Binance/Gate; connector drain dedupe is `strategy_symbol_id`-first; Gate trade parse avoids redundant re-interning; profile baselines and parser-priority regression proof are captured in CP4 evidence doc. |
| `HFT-CP5` Deterministic Replay Harness | `Completed` | Raw-feed recorder is wired into WS ingest via opt-in env, recorder now advances `seq` only after successful write+flush, connector wiring surfaces recorder errors, replay reader validates invalid JSON/out-of-order sequence, and concurrent monotonic-sequence safety is test-covered. |
| `HFT-CP6` Execution Fast Path | `Completed` | Order intent queue uses non-blocking `try_send`, queue-depth drift race is fixed, full-queue path keeps latest intent per symbol, stale intents are dropped by max-age guard, timeout kill-switch now auto-recovers via cooldown, and intent->sent SLA telemetry remains formalized (`2026-02-28-cp6-execution-fast-path-evidence.md`). |
| `HFT-CP7` Production Operations Layer | `In progress` | RM4 health-window enforcement is now runtime-active (`2026-02-28-cp7-block1-rm4-health-enforcement.md`); watchdog/recovery stack remains open. |

## Remediation Track Status (`HFT-RM*`, derived from `stud2`)
| Remediation | Status | Notes |
|---|---|---|
| `HFT-RM1` Plane mode split (`mixed` vs `hft_core`) | `Completed` | Runtime mode split is startup-enforced and test-covered; `mixed` and `hft_core` contracts are documented (`docs/status/dynamics/2026-02-28-rm1-plane-mode-contract-evidence.md`). |
| `HFT-RM2` Screener decoupling from data-plane | `Completed` | Runtime ingest now enforces control-plane handoff as the production path; direct ingest fallback is constrained to test builds only, and overflow-lane replacement behavior is regression-tested. |
| `HFT-RM3` 2-core host budget guardrails | `Completed` | Runtime now enforces frozen 2-core defaults for strategy symbols, screener symbols, and runtime-grid configs; env remains override-only (`docs/status/dynamics/2026-02-28-rm3-2core-cap-profile-evidence.md`). |
| `HFT-RM4` Numeric HFT SLO freeze | `Completed` | Core docs now contain numeric latency/backlog/drop envelopes and explicit `degraded/non-HFT` fail rule tied to `/health` (`business-objective-economic-control-map.md`, `operating-model-spec-v1.md`). |

## Rule -> Code -> Test Matrix
1. Eligibility gate (`age>5`, `closed>5`, `useful_winrate>=0.30`, `avg_pnl>=0`):
   - Code: `src/domain/screener/portfolio_runtime.rs::eligible`
   - Tests: `src/domain/screener/portfolio_runtime_tests.rs::portfolio_runtime_eligible_requires_all_v1_thresholds`
2. Ranking tuple determinism:
   - Code: `src/domain/screener/portfolio_runtime.rs::rank_candidates`, `rank_tuple_cmp`
   - Tests: `src/domain/screener/portfolio_runtime_tests.rs::portfolio_runtime_ranking_uses_v1_tuple_priority`
3. Shortlist/active no-overlap and capacity (`shortlist<=5`, `active<=4`):
   - Code: `src/domain/screener/portfolio_runtime.rs::build_shortlists_no_overlap`, `assign_active_symbols_no_overlap`
   - Tests: `portfolio_runtime_assign_without_overlap_enforces_top5_and_max4`, `portfolio_runtime_assign_without_overlap_balances_identical_candidate_pool`
4. Dynamic portfolio count:
   - Code: `src/domain/screener/portfolio_runtime.rs::with_portfolio_ids`, `set_portfolio_ids`
   - Tests: `portfolio_runtime_with_portfolio_ids_supports_dynamic_count_and_independent_shortlists`
5. Hard reset/cooldown logic:
   - Code: `src/domain/screener/portfolio_runtime.rs::record_closed_trade`, constants `FAST_STREAK_WINDOW_MS`, `COOLDOWN_MS`
   - Tests: `portfolio_runtime_stop_loss_fast_trigger_at_5_within_2m`, `portfolio_runtime_stop_loss_persistent_trigger_on_6th_if_fast_missed`, `portfolio_runtime_stop_loss_streak_resets_on_positive_pnl`
6. Re-entry after cooldown:
   - Code: `src/domain/screener/portfolio_runtime.rs::can_reenter`
   - Tests: `portfolio_runtime_cooldown_blocks_and_reentry_requires_eligible_again`
7. Rebalance cadence (`120_000 ms`):
   - Code: `src/domain/screener/mod.rs::PORTFOLIO_REBALANCE_INTERVAL_MS`, `portfolio_scheduler_tick_v1`
   - Tests: integration path in `src/domain/screener/tests.rs` and assignment history assertions in screener tests.

## Acceptance Criteria (business stage "paper before live")
1. Two portfolios (or configured count) continuously produce deterministic shortlist/active snapshots.
2. Active symbols per portfolio never exceed four and can be zero.
3. Cooldown/guard behavior is deterministic and survives snapshot/restore.
4. UI endpoints show paper race status without requiring live capital rebalance.
5. Checkpoint evidence for CP0-CP4 is complete; CP5/CP6 include post-review remediation evidence.

## Delivery sequence to production
1. `HFT-CP0` delivered; optional baseline snapshot automation remains as non-blocking enhancement.
2. `HFT-CP1` and `HFT-CP2` delivered (allocation and lock-jitter elimination).
3. `HFT-CP3` delivered (updated-only execution path).
4. `HFT-CP4` delivered (minimal-copy parse path).
5. `HFT-CP5` delivered and hardened (deterministic replay for bugs/perf regression + strict recorder semantics).
6. `HFT-CP6` delivered and hardened (execution fast path, intent->sent SLA, overflow/stale/cooldown safeguards).
7. Deliver `HFT-CP7` (operations hardening and deterministic recovery).
8. Deliver `HFT-RM1` and `HFT-RM2` to complete hot-path/control-plane boundary.
9. Wire CP7 ops automation to enforce existing `HFT-RM4` SLO governance at runtime.

## Legacy continuity map
1. Legacy signal/validation/scoring remains reusable baseline.
2. Existing `/portfolio` and `/trials` UI are retained but are not proof of hot-path readiness.
3. Python orchestration remains transitional and must not re-enter runtime hot path.

## Exit criteria for re-baseline stage
1. All status docs reference `HFT-CP*` as primary checkpoint system.
2. Legacy checkpoints remain only as historical trace, not future planning axis.
3. Every new implementation task explicitly maps to one `HFT-CP*`.
