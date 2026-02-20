# Sprint 006 — Dislocation Reversion Strategy + A/B Validation

**Window:** 4 рабочих дня  
**Primary objective:** добавить вторую стратегию (`dislocation_reversion`) и сравнить с текущей на live paper.

---

## 1) Scope

In-scope:

1. Реализация strategy branch B:
   - entry: percentile extremes (`P90`/`P10`),
   - exit: convergence to `P50`,
   - dwell filter (`>=50ms` initial default).
2. Strategy-specific telemetry.
3. A/B framework on identical symbols and windows.

Out-of-scope:

1. Real-money execution.
2. Full capital allocator for live positions.

---

## 2) Key risks addressed

1. Одностратегийная уязвимость к рыночным фазам.
2. Неподтвержденные гипотезы о percentile convergence.
3. Сложность сравнения без нормализованной телеметрии.

---

## 3) Phases

### Phase 0 — Strategy spec freeze

Deliverables:

1. Mathematical spec for B strategy (signal, thresholds, exits, guardrails).
2. Decision-window semantics for dwell-time.

Parameters v1:

1. `entry_upper_pct = 90`
2. `entry_lower_pct = 10`
3. `exit_pct = 50`
4. `min_dwell_ms = 50`

Exit criteria:

1. Spec document approved and versioned.

### Phase 1 — Strategy module implementation

Deliverables:

1. New runtime strategy module implementing `RuntimeStrategy`.
2. Unit tests for signal generation and exit transitions.

Touched paths:

1. `src/application/strategies/mod.rs`
2. `src/application/strategies/dislocation_reversion.rs` (new)
3. `src/config/mod.rs` (strategy-specific config block if needed)

Verification:

1. `cargo test` strategy module suite.

Exit criteria:

1. Branch B can run in runtime via config selection.

### Phase 2 — Shared analytics and guardrails

Deliverables:

1. Reuse existing fee/freshness/spread safeguards from branch A.
2. Explicit anti-noise rules for percentile spikes.

Touched paths:

1. `src/domain/screener/*` (if shared helpers needed)
2. `src/application/strategies/*`

Verification:

1. Regression tests ensure no guardrail regressions vs A.

Exit criteria:

1. B strategy behavior is bounded and deterministic.

### Phase 3 — A/B telemetry and reporting

Deliverables:

1. Strategy-tagged metrics in logs/API:
   - trades,
   - avg pnl,
   - stop_loss share,
   - MAE/MFE proxies,
   - dwell stats.
2. Comparable output schema for A and B.

Touched paths:

1. `src/api/handlers.rs`
2. `src/main.rs` (strategy-tag logging only)
3. `docs/` report template update

Verification:

1. A/B report generated from same sample window.

Exit criteria:

1. Clear apples-to-apples comparison available for decision.

### Phase 4 — 24-48h shadow evaluation

Deliverables:

1. Continuous A/B run window.
2. Summary report with per-symbol and portfolio-level comparison.

Promotion criteria:

1. B positive expectancy on >=3 symbols.
2. Lower downside concentration than A baseline.
3. No material increase in stop_loss severity.

Exit criteria:

1. Go/No-Go decision documented with evidence.

---

## 4) Definition of Done

1. `dislocation_reversion` fully runnable via config.
2. A/B comparison pipeline automated and reproducible.
3. Strategy choice can be justified by measurable evidence.

---

## 5) Rollback and safety

1. Fail-fast on unknown strategy remains enabled.
2. Runtime default remains `lead_lag_classic` until promotion gate passes.
3. Any instability in B path triggers immediate fallback to A only.
