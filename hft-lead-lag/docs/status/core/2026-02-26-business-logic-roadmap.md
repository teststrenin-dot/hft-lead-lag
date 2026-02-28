# Business Logic Roadmap — HFT Runtime Track

Date: 2026-02-26
Last sync: 2026-02-28 (core business process formalized with rule traceability)

## Canonical sources
1. `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
2. `docs/status/core/2026-02-27-operating-model-spec-v1.md`
3. `docs/status/dynamics/2026-02-28-hft-rust-only-checkpoints.md`

## Locked direction
1. Keep business objective unchanged.
2. Build deterministic low-jitter runtime path first (CP0-CP4).
3. Add replay, execution SLA, and operations layers after runtime stabilization (CP5-CP7).
4. Keep Python/Ray out of runtime data-plane and off the trading host CPU budget (offline only).

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
| `HFT-CP5` Deterministic Replay Harness | `Completed` | Raw-feed recorder is wired into WS ingest via opt-in env, offline replay determinism check is available via `REPLAY_RAW_FEED_PATH`, and replay profile harness is in evidence docs. |
| `HFT-CP6` Execution Fast Path | `Completed` | Order intent queue, non-blocking `try_send` path, timeout/kill-switch contract, and intent->sent SLA telemetry are formalized (`2026-02-28-cp6-execution-fast-path-evidence.md`). |
| `HFT-CP7` Production Operations Layer | `Planned` | Watchdog/recovery/alert stack not closed. |

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
5. Checkpoint evidence for CP0-CP4 is complete; CP5 has at least recorder/reader baseline.

## Delivery sequence to production
1. `HFT-CP0` delivered; optional baseline snapshot automation remains as non-blocking enhancement.
2. `HFT-CP1` and `HFT-CP2` delivered (allocation and lock-jitter elimination).
3. `HFT-CP3` delivered (updated-only execution path).
4. `HFT-CP4` delivered (minimal-copy parse path).
5. `HFT-CP5` delivered (deterministic replay for bugs and perf regression).
6. `HFT-CP6` delivered (execution fast path and intent->sent SLA).
7. Deliver `HFT-CP7` (operations hardening and deterministic recovery).

## Legacy continuity map
1. Legacy signal/validation/scoring remains reusable baseline.
2. Existing `/portfolio` and `/trials` UI are retained but are not proof of hot-path readiness.
3. Python orchestration remains transitional and must not re-enter runtime hot path.

## Exit criteria for re-baseline stage
1. All status docs reference `HFT-CP*` as primary checkpoint system.
2. Legacy checkpoints remain only as historical trace, not future planning axis.
3. Every new implementation task explicitly maps to one `HFT-CP*`.
