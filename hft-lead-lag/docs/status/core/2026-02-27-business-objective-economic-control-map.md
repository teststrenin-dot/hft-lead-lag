# Business Objective and Economic Control Map v1

Date: 2026-02-27
Status: Active strategic anchor for CP4+
Last implementation sync: `CP4.6-R1` (ASHA per-config + runtime auto-prune + review remediations)

## 1) Locked Business Objective
Maximize risk-adjusted return under constrained capital by continuously selecting robust alpha contexts (symbols/configs/portfolios) from shadow validation into paper execution, while strictly containing drawdown and operational failure risk.

v1 practical objective:
1. Prove deterministic end-to-end shadow -> validation -> portfolio race pipeline.
2. Reach stable paper portfolio behavior without silent state loss.
3. Prepare for capital rebalance/live gates, but do not enable live capital flow yet.

## 2) Economic Control Map
Control chain:
`Signal -> Validation -> Competition -> Risk -> Capital -> Feedback`

| Node | What it optimizes | Primary risk it reduces | Main runtime evidence | CP ownership | Current status |
|---|---|---|---|---|---|
| `Signal` | Detect exploitable lead/lag inefficiency | False leader/lagger attribution, stale quotes | `spread_bps`, direction, offset-corrected timestamps | `CP1-CP2` | `Done` |
| `Validation` | Admit only statistically acceptable candidates | Noise symbols entering portfolios | candidate history, eligibility gates | `CP3` | `Done` |
| `Competition` | Allocate best candidates across portfolios | Capital attention stuck on weak symbols | shortlist/active assignment, no-overlap, race metrics | `CP4` (+`CP4.1`) | `In Progress` |
| `Risk` | Contain degradation and regime damage | Unbounded loss streaks, delayed ejection | stop-loss streak, hard reset, cooldown, re-entry gate | `CP4-CP5` | `In Progress` |
| `Capital` | Allocate money to best portfolios | Misallocation and overexposure | portfolio equity/PnL and future rebalance policy | `CP7` | `Planned` |
| `Feedback` | Keep operator and runtime aligned | Blind operations, delayed incident response | health, read-model API/UI, recovery checks | `CP5-CP6` | `In Progress` |

## 3) Checkpoint Binding Rules
1. Every checkpoint task must declare target node(s) in the control map.
2. Every milestone must declare expected metric effect and failure mode if omitted.
3. Checkpoint exits are valid only if node-level risks are demonstrably reduced.

## 3.1) CP4 Runtime Note (latest)
1. `Competition`: `forward` now evaluates configs as independent ASHA trials (`1 config = 1 trial`), so winner/loser decisions are no longer batch-aggregated.
2. `Risk`: early-stopped ASHA trials are now auto-pruned from runtime via incremental patches, reducing runtime load during forward execution.
3. `Risk` remediation: incremental prune matching now uses full active fleet config set (not only symbol-attached fleets), preventing false rejects on untouched config removal.
4. `Feedback` hygiene: `forward` now clears active `run_id` lease on completion, preventing stale background execution after trial end.

Binding by checkpoint:
1. `CP0`: freeze contracts that expose node boundaries.
2. `CP1`: stabilize time domain and market-data correctness for `Signal`.
3. `CP2`: stabilize signal lifecycle and shadow execution quality for `Signal`.
4. `CP3`: stabilize eligibility and ranking correctness for `Validation`.
5. `CP4`: operationalize `Competition` + baseline `Risk` behavior in paper.
6. `CP5`: harden `Risk` and `Feedback` through restart/recovery guarantees.
7. `CP6`: harden `Feedback` with operator UX/telemetry/incident flows.
8. `CP7`: introduce `Capital` policy and staged go-live safety.

## 4) Strategic KPI Envelope (Tracking Baseline)
The following KPIs are mandatory for strategic tracking, even when some are not yet production gates:
1. Candidate admission quality (`useful_winrate`, `avg_pnl_pct`, `pm_raw` distribution).
2. Portfolio race quality (active symbol utilization, per-portfolio paper expectancy, turnover).
3. Risk containment quality (reset frequency, cooldown hit-rate, post-cooldown relapse).
4. State integrity (restart recovery correctness, silent-loss incidence = zero).
5. Capital readiness (allocation policy determinism and safety constraints coverage).

## 5) Change Policy
Any change that affects control-map semantics requires:
1. Doc update in this file.
2. CP mapping update in roadmap/status docs.
3. Evidence update (tests, runtime checks, or API contracts) before claiming completion.
