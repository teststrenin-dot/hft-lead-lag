# Business Objective and Economic Control Map v2

Date: 2026-02-27
Status: Active strategic anchor (Rust-only migration stage)
Last sync: 2026-02-28 (`HFT-CP` re-baseline)

## 1) Locked Business Objective
Maximize risk-adjusted return under constrained capital by continuously selecting robust alpha contexts (symbols/configs/portfolios) from shadow validation into paper/live execution, while strictly containing drawdown and operational failure risk.

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
| `Feedback` | Keep operator/system aligned and auditable | Blind operation, delayed incident response | Partial |

## 3) Architecture Law (mandatory)
1. Runtime hot path is Rust-only.
2. Python is not allowed in forward compute/data-plane target architecture.
3. Transitional Python/Ray components are migration artifacts and must be removed by checkpoint gate.

## 4) Checkpoint binding (`HFT-CP`)
1. `HFT-CP0`: latency and allocation observability baseline.
2. `HFT-CP1`: `SymbolId`/array-state migration for hot path.
3. `HFT-CP2`: lock-free single-owner strategy state.
4. `HFT-CP3`: updated-symbol-only event processing.
5. `HFT-CP4`: minimal-copy parse and ingest path.
6. `HFT-CP5`: deterministic record/replay harness.
7. `HFT-CP6`: execution fast path and intent->sent SLA.
8. `HFT-CP7`: production operations and deterministic recovery.

## 5) Strategic KPI envelope
1. Forward throughput and determinism on target host.
2. Candidate/portfolio quality metrics (`useful_winrate`, expectancy, turnover).
3. Risk containment metrics (resets/cooldowns/relapse).
4. State integrity metrics (silent-loss and silent-duplication incidence).
5. Capital readiness metrics (policy determinism, guard coverage).

## 6) Change policy
Any architecture or runtime change must update:
1. This control map.
2. `docs/status/core/2026-02-26-business-logic-roadmap.md`.
3. `docs/status/core/2026-02-26-business-logic-v1-implementation-status.md`.
4. Evidence set (tests/runbook/metrics) for affected checkpoint.
