# Business Objective and Economic Control Map v2

Date: 2026-02-27
Status: Active strategic anchor (`HFT-based-only`, Rust-only runtime stage)
Last sync: 2026-02-28 (`HFT-based-only` objective and observer-first UI feedback contract clarified)

## 1) Locked Business Objective
Maximize risk-adjusted return under constrained capital by continuously selecting robust alpha contexts (symbols/configs/portfolios) from shadow validation into paper/live execution, while strictly containing drawdown and operational failure risk, in an `HFT-based-only` architecture (event-driven, low-jitter, Rust hot path).

## 2) Economic Control Map
Control chain:
`Signal -> Validation -> Competition -> Risk -> Capital -> Feedback`

| Node | What it optimizes | Primary risk it reduces | Current implementation status |
|---|---|---|---|
| `Signal` | Detect exploitable lead/lag inefficiency | Wrong leader/lagger attribution, stale quotes | Implemented in Rust |
| `Validation` | Admit statistically acceptable candidates | Noise admission | Implemented in Rust |
| `Competition` | Allocate best contexts across portfolios/configs | Attention lock on weak contexts | Partial (portfolio side in Rust, forward engine transitional) |
| `Risk` | Contain degradation and operational failure | Loss streak damage, restart drift, duplication | Partial |
| `Capital` | Allocate money to strongest portfolios | Misallocation/overexposure | Planned |
| `Feedback` | Provide operator observation and control entrypoint | Blind operation, delayed incident response, incorrect manual control surface | Partial |

## 3) Architecture Law (mandatory)
1. Runtime hot path is Rust-only.
2. Python is not allowed in forward compute/data-plane target architecture.
3. Transitional Python/Ray components are migration artifacts and must be removed by checkpoint gate.
4. UI is observer-first and control-minimal:
   - Primary UI role: observe symbol race + portfolio race in near real time.
   - Allowed control from UI: start `scout`, start `forward` (only with valid non-empty scout artifact).
   - Other orchestration controls (`expand`, `promote`, broad runner management) are out of target scope.

## 4) Checkpoint binding (`HFT-CP`)
1. `HFT-CP0`: latency and allocation observability baseline.
2. `HFT-CP1`: `SymbolId`/array-state migration for hot path.
3. `HFT-CP2`: lock-free single-owner strategy state.
4. `HFT-CP3`: updated-symbol-only event processing.
5. `HFT-CP4`: minimal-copy parse and ingest path.
6. `HFT-CP5`: deterministic record/replay harness.
7. `HFT-CP6`: execution fast path and intent->sent SLA.
8. `HFT-CP7`: production operations and deterministic recovery.

## 5) Strategic KPI envelope (numeric gates)
1. `HFT core latency` (`hft_core` mode, target host):
   - `runtime_latency_us.end_to_end.p99 <= 2_000`
   - `runtime_latency_us.ingest.p99 <= 1_500`
   - `runtime_latency_us.decision.p99 <= 1_500`
2. `Backlog envelope` (`hft_core` mode):
   - `runtime_backlog_depth.binance_msg_queue_depth <= 64`
   - `runtime_backlog_depth.gate_msg_queue_depth <= 64`
   - `runtime_backlog_depth.signal_backlog_depth <= 128`
   - `runtime_backlog_depth.execution_intent_queue_depth <= 128`
   - `runtime_backlog_depth.control_update_queue_depth <= 256` (if control-plane enabled)
3. `Drop/timeout envelope`:
   - `execution_dropped_intents = 0` for stable baseline windows
   - `execution_send_timeouts = 0` for stable baseline windows
   - `control_dropped_updates = 0` for stable baseline windows
4. `Mode fail rule`:
   - Any envelope breach for 3 consecutive health windows marks run as `degraded/non-HFT`.
5. Candidate/portfolio quality metrics (`useful_winrate`, expectancy, turnover) remain business KPIs but do not override runtime safety envelopes.

## 6) Business Process Contract (V1, current implementation)
1. Shadow-first lifecycle:
   - Symbol starts in shadow statistics accumulation.
   - Portfolio participation is allowed only after passing eligibility gate.
2. Portfolio topology:
   - Default portfolios: `A`, `B` (configurable count).
   - Per-portfolio shortlist capacity: `5`.
   - Per-portfolio active capacity: `0..4`.
   - Shortlists are built without overlap across portfolios.
   - Active symbols are built without overlap across portfolios.
3. Eligibility gate (symbol-level):
   - `age_minutes_from_first_tick > 5`
   - `closed_trades > 5`
   - `useful_winrate = profitable_trades / closed_trades >= 0.30`
   - `avg_pnl_pct >= 0`
4. Candidate ranking tuple (descending):
   - `useful_winrate`
   - `pm_raw = profitable_trades - losing_trades`
   - `avg_pnl_pct`
   - `closed_trades`
   - `symbol` (lexicographic tiebreak)
5. Risk guard / hard reset:
   - Streak counts only stop-loss exits with non-positive pnl.
   - Positive pnl trade resets streak counter.
   - Fast trigger: `stop_loss_streak >= 5` within `120_000 ms`.
   - Persistent trigger: `stop_loss_streak >= 6` (even if fast window missed).
   - On trigger: symbol goes to cooldown for `300_000 ms`.
6. Re-entry rule:
   - During cooldown symbol is ineligible.
   - After cooldown symbol must pass full eligibility gate again.
7. Rebalance cadence:
   - Portfolio scheduler tick period: `120_000 ms`.
8. Stage scope:
   - Paper competition is active.
   - Live money rebalance/allocation across portfolios is not enabled yet.

## 6.1 Observation / UI-Feedback Contract (mandatory)
1. Purpose:
   - Operator must see portfolio race and symbol race as the main feedback loop for strategy quality and risk behavior.
2. Required observable entities:
   - Candidate ranking transitions (enter/leave, rank drift).
   - Portfolio shortlist and active allocation transitions.
   - Guard/cooldown/hard-reset transitions per symbol.
   - Paper performance deltas per portfolio.
3. Control surface:
   - UI may start `scout` and `forward` only.
   - `forward` start must be prevalidated by scout artifact dependency.
   - UI must not expose broad runtime orchestration controls in target `HFT-based-only` contour.
4. Mode boundary:
   - `mixed`: observer pages are enabled and fed from runtime snapshots.
   - `hft_core`: execution kernel remains minimal; observer integration must not contaminate hot path.

## 6.2 Scout Contract (mandatory)
1. Purpose of `scout`:
   - find only trade-bearing parameter corridors (ranges where trades exist).
2. Non-goals of `scout`:
   - no full optimization, no final ranking, no promotion logic.
3. Output of `scout`:
   - compact corridor artifact (small info volume) passed to race runtime.
4. Downstream ownership:
   - portfolio/symbol race consumes scout corridors and performs competition.

## 7) State Machine (business view)
1. `ShadowCollecting` -> `EligiblePool`:
   - Trigger: eligibility gate passes.
2. `EligiblePool` -> `Shortlisted`:
   - Trigger: ranking + no-overlap shortlist assignment.
3. `Shortlisted` -> `Active`:
   - Trigger: no-overlap active assignment with `<=4` cap.
4. `Active` -> `Cooldown`:
   - Trigger: hard-reset risk rule fires (fast or persistent).
5. `Cooldown` -> `ShadowCollecting`:
   - Trigger: cooldown expires; symbol returns to common candidate pool.
6. `Any` -> `ShadowCollecting`:
   - Trigger: rebalance drop, metric deterioration, or portfolio reassignment.

## 8) Definition of Done (current stage)
1. End-to-end paper flow works with configured portfolio count and visible assignments in UI.
2. Portfolio race obeys capacities and no-overlap constraints on every rebalance.
3. Guard/cooldown behavior is deterministic and reproducible from persisted trades.
4. Runtime hot path remains Rust-only and checkpoint-tracked (`HFT-CP*`).

## 9) Change policy
Any architecture or runtime change must update:
1. This control map.
2. `docs/status/core/2026-02-26-business-logic-roadmap.md`.
3. `docs/status/dynamics/2026-02-26-business-logic-v1-implementation-status.md`.
4. Evidence set (tests/runbook/metrics) for affected checkpoint.
