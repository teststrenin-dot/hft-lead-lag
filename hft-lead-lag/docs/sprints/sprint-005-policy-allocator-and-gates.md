# Sprint 005 — Policy Allocator and Symbol Gates

**Window:** 4 рабочих дня  
**Primary objective:** перейти от "один статичный конфиг" к адаптивной policy-модели по символам.

## Execution Update — 2026-02-20

Status this cycle:

1. Phase 0 executed (scoring model freeze + thresholds versioned in code/docs).
2. Phase 1 started and executed (policy state model with rolling/decayed aggregates).
3. Phase 2+ not started yet (allocator decisions are still shadow diagnostics only).

Scoring model (frozen v1):

1. `score = 1.0 * avg_pnl_6h + 0.20 * win_rate_6h - 0.50 * stop_loss_share_6h`
2. `win_rate_6h` and `stop_loss_share_6h` are used as fractions `[0..1]` inside formula.

Gate thresholds (shadow mode, v1):

1. `min_trades_6h >= 5`
2. `rolling_expectancy_6h > 0`
3. `stop_loss_share_6h <= 55%`

Implemented in code:

1. Added decayed windows for `1h/6h/24h` policy metrics.
2. Added per-config policy state and snapshots:
   - score,
   - gate enabled/reason,
   - rolling metrics (trades/win-rate/stop-loss-share/avg-pnl).
3. Added `top_policy_configs(k)` helper (gate-filtered + score-sorted shortlist).
4. Added deterministic unit tests for:
   - min-trades gate,
   - positive profile enablement,
   - exponential decay behavior,
   - top-K shortlist filtering.

Current safety posture:

1. Policy currently runs in diagnostics/shadow-decision mode only.
2. Existing trading tick path behavior is unchanged by allocator decisions.

---

## 1) Scope

In-scope:

1. Rolling scorer для конфигов на символ.
2. Symbol-level gating (expectancy, stop-loss share, liquidity/freshness).
3. Regime-aware enable/disable логика на shadow path.
4. API/telemetry for policy decisions.

Out-of-scope:

1. Реальные live-ордера.
2. Новая стратегия B (это Sprint 006).

---

## 2) Key risks addressed

1. Концентрация edge на 1-2 символах.
2. Деградация при regime shift.
3. Накопление убытков на "плохих" символах без auto-gate.

---

## 3) Phases

### Phase 0 — Scoring model freeze

Deliverables:

1. Formal scoring formula and thresholds doc.
2. Window definitions: 1h, 6h, 24h.

Reference candidates:

1. `score = w1*avg_pnl_6h + w2*win_rate_6h - w3*stop_loss_share_6h`
2. EWMA decay for regime adaptation.

Exit criteria:

1. Formula approved and versioned in docs.

### Phase 1 — Policy state model

Deliverables:

1. Policy structs for per-symbol config stats.
2. Rolling aggregates update path from completed trades.

Touched paths:

1. `src/domain/screener/shadow_fleet.rs`
2. `src/domain/screener/shadow_trader.rs`
3. `src/domain/screener/mod.rs`

Verification:

1. Unit tests for rolling updates and decay behavior.

Exit criteria:

1. Deterministic policy state evolution in tests.

### Phase 2 — Gating and allocator decisions

Deliverables:

1. Symbol gating rules:
   - min trades threshold,
   - rolling expectancy > 0,
   - stop_loss share cap,
   - freshness/spread guard.
2. Top-K config shortlist per symbol with confidence threshold.

Touched paths:

1. `src/domain/screener/shadow_fleet.rs`
2. `src/domain/screener/trader_config.rs`

Verification:

1. Simulation tests on synthetic trade streams.
2. No decision oscillation under small noise.

Exit criteria:

1. Gate decisions reproducible and logged.

### Phase 3 — API and observability

Deliverables:

1. New endpoint(s) with policy and gate diagnostics.
2. Expose per-symbol rolling metrics and active config selection.

Touched paths:

1. `src/api/handlers.rs`
2. `src/api/http_server.rs`
3. `src/api/templates.rs` (optional minimal rendering)

Verification:

1. `curl` contracts for new endpoint payload.
2. Snapshot tests for response schema.

Exit criteria:

1. Operator can inspect "why symbol/config is enabled/disabled".

### Phase 4 — Guardrails and kill-switch

Deliverables:

1. Global kill switch on negative rolling portfolio expectancy.
2. Cooldown/rehabilitation window for disabled symbols.
3. Anti-churn hysteresis to avoid frequent toggles.

Verification:

1. Scenario tests: unstable regimes, rapid flips.

Exit criteria:

1. System avoids thrash and uncontrolled overtrading.

### Phase 5 — Sprint closeout and runtime checkpoint

Deliverables:

1. 24h paper checkpoint report with policy metrics.
2. Before/after concentration and downside metrics.

Target metrics:

1. Positive expectancy on >=3 symbols.
2. Lower loss concentration vs baseline snapshot.
3. Stable active-config set (limited churn).

Verification:

1. `cargo test`
2. `cargo clippy --all-targets -- -D warnings`
3. DB query evidence block in doc.

---

## 4) Definition of Done

1. Dynamic symbol/config gating works and is observable.
2. Rolling policy metrics are persisted or reconstructable.
3. Policy decisions demonstrably reduce downside concentration.

---

## 5) Rollback and safety

1. Policy allocator runs in shadow-decision mode first.
2. Hard fallback: revert to static best-config path per symbol.
3. Kill-switch defaults to conservative behavior on missing data.
