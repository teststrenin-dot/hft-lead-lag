# Operating Model Spec v1

Date: 2026-02-27
Status: Canonical operating model for CP4+
Strategic anchor: `docs/status/core/2026-02-27-business-objective-economic-control-map.md`

## 1) Purpose
This document is the single executable description of the business flow:
`Signal -> Validation -> Competition -> Risk -> Capital -> Feedback`.

It binds business logic to runtime artifacts (code, state, API, tests) and removes semantic drift between docs and implementation.

## 2) Competition Semantics (Frozen for v1)
Competition model in v1 is **allocation-based**, not independent-portfolio overlap competition:
1. One global ranked candidate pool is built.
2. Shortlists are allocated round-robin without overlap across portfolios.
3. Active symbols are allocated without overlap across portfolios.

Runtime evidence:
- `src/domain/screener/portfolio_runtime.rs` (`assign_without_overlap`, `build_shortlists_no_overlap`, `assign_active_symbols_no_overlap`).

## 3) End-to-End Operating Flow
| Step | Node | Input | Transform | Output | State of record | Operator/API surface | Main failure mode | Acceptance gate |
|---|---|---|---|---|---|---|---|---|
| 1 | `Signal` | Exchange quotes + timestamps | Ingest + clock-offset normalization + freshness checks | Normalized symbol state | In-memory `SymbolState` | `/api/v1/screener`, `/screener` | stale/dirty quote stream | rows rebuild without invalid quote leakage |
| 2 | `Signal` | Normalized quote states | Lead-lag spread/drift computation | Signal context | In-memory runtime | `/api/v1/screener` | wrong leader attribution | deterministic signal math in tests |
| 3 | `Signal` | Signal context + fleet configs | Shadow entry/exit lifecycle | Closed shadow trades | Fleet pending trades | `/api/v1/shadow/:symbol`, `/api/v1/fleet/*` | exit logic drift | exit reasons and lifecycle tests green |
| 4 | `Feedback` | Drained shadow trades | Chronological sort + natural-key dedupe | Unique drained trade stream | Runtime + DB writer queue | `/health` (indirect), logs | duplicate replay inflates state | duplicate natural key does not double-apply runtime state |
| 5 | `Validation` | Unique drained trades | Candidate event collapse by `(symbol, exit_ts_ms)` | Candidate events | Candidate accumulators + `trades` table | `/api/v1/portfolio/candidates` | noisy duplicate candidate counting | event-level collapse contract holds |
| 6 | `Validation` | Candidate stats | Eligibility gates (`age>5m`, `closed>5`, `wr>=0.30`, `avg>=0`) | Eligible set | Runtime candidate stats | `/api/v1/portfolio/candidates` | under-sampled/noisy admissions | eligibility tests deterministic |
| 7 | `Validation` | Eligible set | Tuple ranking (`useful_wr`, `pm_raw`, `avg_pnl`, `closed`) | Ranked pool | Runtime ranking | `/api/v1/portfolio/candidates` | unstable ordering | tie-break behavior deterministic |
| 8 | `Competition` | Ranked pool + portfolio ids | Disjoint shortlist + disjoint active assignment | Portfolio assignment snapshot | `portfolio_state_v1` + runtime assignment history | `/api/v1/portfolio/active`, `/portfolio` | semantic mismatch in allocation | no-overlap allocation tests green |
| 9 | `Risk` | Closed trades + guard state | Stop-loss streak tracking + cooldown | Symbol-level reset/cooldown state | `portfolio_symbol_guard_v1` | `/api/v1/portfolio/guards`, `/portfolio` | reset/cooldown drift | guard transition tests green |
| 10 | `Risk` | Symbol stats + guard state | Re-entry gate (`cooldown + eligible`) | Re-enterable candidate filter | runtime guard + candidate state | `/api/v1/portfolio/active` (indirect) | immediate relapse churn | re-entry scenarios deterministic |
| 11 | `Competition` + `Feedback` | Assignment history + closed trades | Paper attribution to entry-owner (fallback active-owner) | Per-portfolio paper metrics | `portfolio_paper_state_v1` | `/api/v1/portfolio/performance`, `/portfolio` | wrong owner attribution / double count | attribution + idempotency tests green |
| 12 | `Feedback` | Runtime/DB snapshots | API read-model + operator UI | Operator-visible race state | API + HTML templates | `/portfolio`, `/trials`, `/fleet`, `/health` | blind operations | dashboard reflects latest snapshots |

## 4) Boundaries by Checkpoint
1. `CP4`: portfolio race analytics and paper attribution.
2. `CP5`: state integrity, restart/recovery, idempotency hardening.
3. `CP6`: operator telemetry/UX and incident handling.
4. `CP7`: capital allocation/rebalance/live gates.

## 5) Mandatory Change Rule
Any runtime behavior change touching this flow requires:
1. Update this file.
2. Update CP mapping in:
   - `docs/status/core/2026-02-26-business-logic-roadmap.md`
   - `docs/status/core/2026-02-26-business-logic-v1-implementation-status.md`
3. Add/adjust regression tests proving acceptance gate for changed step.
