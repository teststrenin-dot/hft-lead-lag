# Profit Extraction Deep Dive (2026-02-20)

**Date:** 2026-02-20  
**Commit baseline:** `1719123`  
**Scope:** runtime data + code architecture + concrete path from paper alpha to robust extraction.

---

## 1) Verified data snapshot

Source: `data/optimizer.db` queried on 2026-02-20.

### Core totals

1. `configs`: 2304
2. `trades`: 23453
3. `distinct traded symbols`: 4
4. `total pnl_pct`: +561.142545
5. `avg pnl per trade`: +0.023926%
6. `win rate`: 46.4589%
7. `avg hold`: 4601.36 ms

### Symbol concentration (critical)

1. `ENSOUSDT`: 11228 trades (47.87%)
2. `RIVERUSDT`: 6789 trades (28.95%)
3. `NAORISUSDT`: 4572 trades (19.49%)
4. `VVVUSDT`: 864 trades (3.68%)

Trade-share HHI = `0.3524` (high concentration).  
This is not broad market edge; this is concentrated edge.

---

## 2) What currently makes money vs loses money

### By symbol

1. `RIVERUSDT`: +0.074969% avg/trade
2. `ENSOUSDT`: +0.061811% avg/trade
3. `VVVUSDT`: +0.044091% avg/trade
4. `NAORISUSDT`: -0.148717% avg/trade

Interpretation: one symbol (`NAORISUSDT`) materially drags portfolio expectancy.

### By exit reason

1. `trailing_take`: 39.76% of trades, +0.149304% avg
2. `timeout`: 24.88% of trades, +0.354485% avg
3. `stop_loss`: 31.89% of trades, -0.365394% avg
4. `breakeven`: 3.48% of trades, -0.204365% avg

Interpretation: PnL depends on keeping `stop_loss` frequency/size controlled.  
`timeout` is currently not a problem globally; it contributes positive expectancy.

---

## 3) Hyperparameter behavior in live paper

### `spike_threshold_bps` coverage

Only `30 bps` produced trades (`50/60/80` inactive in current regime).

### `baseline_window_ms`

1. `10000`: +0.069244% avg
2. `20000`: +0.023676% avg
3. `30000`: -0.020210% avg
4. `60000`: -0.012438% avg

Current best tendency is short baseline windows (10-20s), not long windows.

### `max_hold_ms`

1. `5000`: +0.049523% avg
2. `10000`: +0.011889% avg
3. `30000`: +0.009333% avg

Shorter hold is currently better.

---

## 4) Regime non-stationarity (why static config decays)

Daily split:

1. 2026-02-19 UTC: `13185` trades, avg `-0.036876%`, total `-486.2042`
2. 2026-02-20 UTC: `10268` trades, avg `+0.102001%`, total `+1047.3467`

Same strategy family flips from negative to positive across adjacent days.  
This confirms your core concern: static fixed config is fragile and regime-dependent.

---

## 5) Deep code-level constraints that cap robustness

1. Trade parser correctness risks in connectors.
   - `src/infrastructure/exchanges/binance/mod.rs:133`
   - `src/infrastructure/exchanges/gate/mod.rs:128`
   - `src/infrastructure/exchanges/gate/mod.rs:475`

2. Mixed time domains in lag/hold analytics.
   - `src/domain/screener/mod.rs:123`
   - `src/domain/screener/state.rs:101`
   - `src/domain/screener/shadow_trader.rs:206`

3. Runtime does O(symbols) strategy updates per tick.
   - `src/main.rs:313`
   - `src/main.rs:318`
   - `src/main.rs:330`

4. Config/runtime contract mismatch (some knobs exist but not fully drive runtime behavior).
   - `src/config/mod.rs:20`
   - `src/main.rs:22`
   - `src/application/strategies/mod.rs:54`

5. `clippy -D warnings` currently fails (quality gate red).

---

## 6) Practical extraction model (what to do next)

## 6.1 Policy layer above configs (must-have)

Instead of one fixed config per symbol, run a policy allocator:

1. Keep top-K configs per symbol (`K=3..5`) by rolling out-of-sample score.
2. Use decayed score:
   `score_t = alpha * recent_avg + (1-alpha) * score_{t-1}`.
3. Route shadow/live weight to highest score config only when confidence threshold is met.

## 6.2 Hard symbol gating

At symbol level, disable symbols that violate stability constraints:

1. min trades in recent window (e.g. 100 in 6h)
2. rolling expectancy > 0
3. stop_loss share under cap (example: <40%)
4. max drawdown proxy under cap

This would likely disable `NAORISUSDT` quickly in current snapshot.

## 6.3 Regime gates

Before signal execution:

1. `short_window_expectancy(symbol) > 0`
2. `time_above_extreme_ms` gate for dislocation entries (your idea)
3. quote freshness + spread gate

If regime gate fails, skip entry regardless of threshold trigger.

## 6.4 Second strategy branch (already enabled by architecture)

Runtime modularity is now in place (`RuntimeStrategy`, `StrategyKind`).
Use it for branch-B:

1. `lead_lag_classic` (current)
2. `dislocation_reversion`:
   - entry at `P90/P10`
   - exit at `P50`
   - dwell filter `>= 50ms` (temporary default)

Then compare both branches under identical OOS windows.

---

## 7) 72-hour execution plan

### Phase 1 (today)

1. Fix parser/time-domain P1 issues.
2. Bring `clippy -D warnings` to green.
3. Add reliable payload fixture tests for connectors.

### Phase 2 (next 24h)

1. Add policy scorer and symbol gating in shadow fleet path.
2. Track per-symbol rolling stats (1h, 6h, 24h).
3. Add API endpoint exposing stability metrics.

### Phase 3 (next 48h)

1. Implement dislocation reversion branch in strategy module.
2. Run A/B shadow comparison:
   - same symbols
   - same time windows
   - same fee assumptions

### Phase 4 (decision)

Promotion criteria:

1. positive expectancy on at least 3 symbols
2. lower downside concentration than baseline
3. stable rolling score for at least 24h

---

## 8) Bottom line

The system is now technically capable of multi-strategy runtime selection, but profit extraction is still regime-fragile.  
The main unlock is not a single new threshold; it is adaptive policy + symbol/regime gating on top of existing fleet data.

This is the step that converts paper statistics into robust operational alpha.
