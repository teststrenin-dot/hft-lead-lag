# Operating Model Spec v1 (Rust-Only Target)

Date: 2026-02-27
Status: Canonical operating model for `HFT-CP*`
Strategic anchor: `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
Last sync: 2026-02-28 (`HFT-based-only` purpose and UI feedback/control boundary clarified)

## 1) Purpose
Define one executable operating model that binds business flow to implementation checkpoints and enforces `HFT-based-only` behavior with Rust-only hot path.

## 2) Current vs target execution model
| Domain | Current | Target |
|---|---|---|
| Signal and shadow runtime | Rust | Rust |
| Candidate math and ranking | Rust | Rust |
| Portfolio assignment and guards | Rust | Rust |
| Runtime engine internals | String/HashMap/lock-heavy segments | `SymbolId` + array-state + updated-only event flow |
| Orchestration | Mixed (Rust API starts Python module) | Rust runtime hot path, Python only in cold/offline tooling |

## 3) Locked flow
`Signal -> Validation -> Competition -> Risk -> Capital -> Feedback`

### 3.1 Signal
1. Ingest and normalize market data.
2. Compute lead-lag context.
3. Produce shadow trades and events.

### 3.2 Validation
1. Build candidate events.
2. Apply eligibility gates.
3. Rank candidates deterministically.

### 3.3 Competition
1. Build disjoint shortlist/active allocation per portfolio.
2. Ensure runtime competition reads only updated symbols and uses deterministic state transitions.
3. `v1` semantics: this is a disjoint allocation policy (diversification), not overlap competition on one symbol.

### 3.4 Risk
1. Guard/cooldown/hard-reset logic.
2. Idempotent state updates and replay safety.

### 3.5 Capital
1. Allocation and rebalance policy.
2. Risk caps and kill switches.

### 3.6 Feedback
1. Observer-first UI for symbol race and portfolio race.
2. Health/alerts/recovery telemetry.
3. UI control scope is minimal: `scout` and `forward` start only.
4. Broad orchestration controls are not part of the target `HFT-based-only` feedback surface.

### 3.7 Scout (pre-race only)
1. `scout` exists only to locate trade-bearing parameter ranges.
2. `scout` output is a compact corridor artifact for race input.
3. `scout` is not a competition engine and not a final config selector.

## 4) Formal IO Contract (V1)
1. Input signals:
   - Normalized market events (`book_ticker`, `trade`) with exchange/local timestamps.
   - Closed-trade facts per symbol (`pnl_pct`, `is_stop_loss`, `ts_ms`).
2. Intermediate entities:
   - Candidate symbol stats (`age_minutes_from_first_tick`, `closed_trades`, `wins`, `losses`, `avg_pnl_pct`).
   - Guard state (`streak_count`, `first_streak_ts_ms`, `cooldown_until_ms`).
3. Output artifacts:
   - Portfolio assignment snapshot: per portfolio `shortlist` and `active_symbols`.
   - Paper state snapshot: equity, realized pnl, trades, wins/losses, last trade.
   - Health telemetry: latency, backlog, drift.

## 4.1 Mode-specific process boundary
1. `mixed` mode:
   - Full process is active: `Signal -> Validation -> Competition -> Risk -> Capital -> Feedback`.
   - Portfolio scheduler, persistence, and operator pages are enabled.
   - UI exposes race observation (symbol + portfolio), plus `scout` and `forward` start controls.
   - `forward` start is valid only when scout artifact prerequisites pass.
   - `scout` feeds only corridor candidates into race; race performs selection.
2. `hft_core` mode:
   - Runtime is reduced to low-jitter execution kernel: `Signal -> Execution -> Health`.
   - Portfolio scheduler, persistence, control-plane worker, and trial runner routes are disabled.
   - Runtime server surface is health-only; no race UI and no runner/trials orchestration endpoints.

## 5) Portfolio Runtime State Machine (operational)
1. `CandidateCollecting`:
   - Symbol accumulates history from first observed tick/trade.
2. `CandidateEligible`:
   - Entry condition: passes eligibility gate.
3. `Shortlisted`:
   - Entry condition: selected into portfolio shortlist (`<=5`) with no cross-portfolio overlap.
4. `Active`:
   - Entry condition: selected into active set (`<=4`) with no cross-portfolio overlap.
5. `Cooldown`:
   - Entry condition: risk hard-reset trigger (fast or persistent stop-loss streak).
   - Exit condition: cooldown timer elapsed and eligibility gate passes again.

## 6) Deterministic constraints
1. Ranking and assignment are deterministic for identical candidate set.
2. Tie-break order is explicit and stable (`symbol` as final key).
3. Rebalance cadence is fixed (`120_000 ms`).
4. Guard transition thresholds are fixed by constants in runtime.
5. Runtime mode boundaries (`scout`, `forward-only`, `promote`) are explicit and observable.

## 7) Numeric HFT SLO Contract (`hft_core` mode)
1. Latency envelope (`/health.runtime_latency_us`):
   - `end_to_end.p99 <= 2_000`
   - `ingest.p99 <= 1_500`
   - `decision.p99 <= 1_500`
2. Backlog envelope (`/health.runtime_backlog_depth`):
   - `binance_msg_queue_depth <= 64`
   - `gate_msg_queue_depth <= 64`
   - `signal_backlog_depth <= 128`
   - `execution_intent_queue_depth <= 128`
   - `control_update_queue_depth <= 256` (when control-plane is enabled)
3. Drop/timeout envelope:
   - `/health.execution_dropped_intents = 0` for stable baseline windows
   - `/health.execution_send_timeouts = 0` for stable baseline windows
   - `/health.control_dropped_updates = 0` for stable baseline windows
4. Degradation rule:
   - If any envelope is breached for `3` consecutive health windows, runtime status is `degraded/non-HFT` and cannot be accepted as HFT-quality run.

## 8) Checkpoint boundaries
1. `HFT-CP0`: latency and allocation observability.
2. `HFT-CP1`: `SymbolId` and allocation removal.
3. `HFT-CP2`: lock-free strategy state.
4. `HFT-CP3`: updated-only event loop.
5. `HFT-CP4`: minimal-copy parse path.
6. `HFT-CP5`: deterministic replay harness.
7. `HFT-CP6`: execution fast path.
8. `HFT-CP7`: operations and recovery layer.

## 9) Mandatory invariants
1. No Python in target runtime hot path.
2. Deterministic ranking and state transitions.
3. Idempotent trade/snapshot application.
4. Explicit, observable mode boundaries (`scout`, `forward-only`, `promote`).

## 10) Out of scope (current stage)
1. Real-money capital rebalance across portfolios.
2. Live execution allocation policy.
3. Cross-portfolio capital optimizer.

## 11) Change rule
Any behavior change touching this model requires:
1. Update this file.
2. Update roadmap and implementation status docs.
3. Attach test/metric evidence to a concrete `HFT-CP*`.
