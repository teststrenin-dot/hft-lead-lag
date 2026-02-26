# Project Math Model — HFT Lead-Lag

Date: 2026-02-26
Last sync: commits up to `ad041ca`
Scope: математика и формулы, реально используемые в runtime.

## 0) Units and Conventions
- `bps` (basis points): `1 bps = 0.01%`.
- Перевод относительного смещения в `bps`: `ratio * 10_000`.
- `pnl_pct` хранится в процентах (например, `0.15` = `+0.15%`).
- Временные домены:
  - exchange timestamps -> ms,
  - локальные ingress/decision timestamps -> ms.

Evidence:
- `src/application/services/lead_lag.rs`
- `src/domain/screener/utils.rs`
- `src/domain/screener/shadow_trader.rs`

## 1) Lead-Lag Signal Math
Формула спреда:
```text
spread_bps = ((leader_price - lagger_price) / lagger_price) * 10_000
```

Проверяются 2 направления:
```text
bid_ask_bps = spread(primary.bid, hedge.ask)
ask_bid_bps = spread(hedge.bid, primary.ask)
spread_bps = max(bid_ask_bps, ask_bid_bps)
```

Выбор торгового направления:
```text
direction = LONG_LAGGER,  if bid_ask_bps >= ask_bid_bps
direction = SHORT_LAGGER, otherwise
```

Гейт по лидерству primary (после offset-коррекции времени):
```text
signal is allowed only if primary_corrected_ts_ns >= hedge_corrected_ts_ns
```

Freshness-гейты по local-time:
```text
pair_skew_ns = |primary.local_ts_ns - hedge.local_ts_ns|
pair_skew_ns <= max_quote_skew_ms * 1_000_000

age_primary_ns = |now_ns - primary.local_ts_ns|
age_hedge_ns   = |now_ns - hedge.local_ts_ns|
age_primary_ns <= max_quote_age_ms * 1_000_000
age_hedge_ns   <= max_quote_age_ms * 1_000_000
```

Сигнал есть, если:
```text
spread_bps >= min_entry_spread_bps
```

Evidence:
- `src/application/services/lead_lag.rs::calculate_spread_bps`
- `src/application/services/lead_lag.rs::check_signal`
- `src/application/strategies/mod.rs::resolve_lead_lag_config`

## 2) Time Normalization, Drift, Percentiles
Нормализация timestamp к ms:
- sec -> `*1000`
- ms -> как есть
- us -> `/1000`
- ns -> `/1_000_000`

Exchange clock-offset correction (по каждой бирже отдельно):
```text
offset_sample = ingress_ts - exchange_ts
offset = median(last N offset_sample)
corrected_exchange_ts = exchange_ts + offset
```

Параметры:
- screener: `N=512` samples, recompute median every `64` updates, guard `|offset_sample_ms| <= 6h`;
- lead-lag strategy service: та же формула в ns-домене, `N=256`.

WebSocket drift:
```text
drift_ms = local_ts_ms - exchange_ts_ms
```
Outlier-guard:
```text
if |drift_ms| > 30_000 -> None
```

Percentile (linear interpolation):
```text
rank = (pct / 100) * (n - 1)
lo = floor(rank), hi = ceil(rank)
p = v[lo]*(1-frac) + v[hi]*frac
```

Evidence:
- `src/domain/screener/utils.rs`
- `src/domain/screener/clock_offset.rs`
- `src/application/services/lead_lag.rs`

## 3) Screener Lag and Cycle Metrics
Instant lag:
```text
instant_lag_ms = |binance.ts_ms - gate.ts_ms|
```

Публикуемый lag:
```text
lag_ms = p50(lag_samples in lag_window)
```

Leader side:
```text
leader = argmax(corrected_exchange_ts_ms)
```

Cycle divergence/convergence (через leader_mid):
```text
leader_mid = mid(fresher exchange)

binance_div_bps = ((binance.bid - gate.ask) / leader_mid) * 10_000
binance_conv_bps = ((binance.ask - gate.bid) / leader_mid) * 10_000
gate_div_bps = ((gate.bid - binance.ask) / leader_mid) * 10_000
gate_conv_bps = ((gate.ask - binance.bid) / leader_mid) * 10_000
```

Evidence:
- `src/domain/screener/state.rs::update_lag`
- `src/domain/screener/state.rs::update_cycles`

## 4) CycleTracker Math
Внутри окна:
- `p90_divergence`,
- `p50_convergence`.

Half-life sample:
```text
entry when divergence >= p90_divergence
exit when convergence <= p50_convergence
half_life_ms = exit_ts - entry_ts
```

Duration above P90:
```text
zone_duration_ms = zone_exit_ts - zone_entry_ts
```

Публикация:
```text
avg_half_life_ms = mean(half_life_samples)
avg_gt_p90_ms = mean(above_p90_samples)
```

Evidence:
- `src/domain/screener/cycle_tracker.rs`

## 5) ShadowTrader Math
### 5.1 Entry (baseline gap model)
Baseline по окну:
```text
baseline_ask_gap_bps = mean((binance_ask - gate_ask)/gate_ask * 10_000)
baseline_bid_gap_bps = mean((gate_bid - binance_bid)/gate_bid * 10_000)
```

Текущий сигнал:
```text
long_signal_bps = current_ask_gap_bps - baseline_ask_gap_bps
short_signal_bps = current_bid_gap_bps - baseline_bid_gap_bps
```

Entry trigger:
```text
signal_bps >= spike_threshold_bps
```

Spread filter:
```text
gate_spread_bps = ((gate.ask - gate.bid) / gate_mid) * 10_000
gate_spread_bps <= max_spread_bps
```

### 5.2 Exit and PnL
Unrealized (in bps):
```text
long_unrealized_bps = ((gate.bid - entry_price)/entry_price) * 10_000
short_unrealized_bps = ((entry_price - gate.ask)/entry_price) * 10_000
```

Breakeven activation:
```text
breakeven_threshold_bps = spike_bps * target_ratio
activate if unrealized_bps >= breakeven_threshold_bps
```

After breakeven:
- exit `breakeven`, если `unrealized_bps <= 0`;
- exit `trailing_take`, если `unrealized_bps <= peak_unrealized_bps * trailing_decay_ratio`;
- иначе `timeout` при превышении `max_hold_ms`.

Before breakeven:
- exit `stop_loss`, если `unrealized_bps <= -stop_loss_bps`;
- иначе `timeout` при `hold_ms >= max_hold_ms`.

Closed trade pnl:
```text
raw_return = (exit - entry) / entry    (sign by direction)
fees = 2 * taker_fee
pnl_pct = (raw_return - fees) * 100
```

Early stop churn:
```text
early_stop_churn = (exit_reason == stop_loss) and (hold_ms <= 500)
```

Session metrics:
```text
avg_trade_pct = session_total_pnl_pct / session_trades
win_rate_pct = session_wins / session_trades * 100
```

Evidence:
- `src/domain/screener/shadow_trader.rs`

## 6) ShadowFleet Policy Math
Экспоненциальное затухание окна:
```text
decay = exp(-dt_ms / horizon_ms)
state *= decay
```

Окна: `1h`, `6h`, `24h`.

Window metrics:
```text
avg_pnl_pct = pnl_sum_pct / trades
win_rate_pct = wins / trades * 100
stop_loss_share_pct = stop_loss_trades / trades * 100
```

Score (фаза-0 frozen):
```text
score =
  1.0 * (avg_pnl_6h / 100)
  + 0.20 * (win_rate_6h / 100)
  - 0.50 * (stop_loss_share_6h / 100)
```

Gate:
```text
trades_6h >= 5
avg_pnl_6h > 0
stop_loss_share_6h <= 55%
```

Prune:
```text
if session_trades >= 30 and avg_pnl_pct < -0.05 -> disable config
if session_trades == 0 and elapsed >= 10 min -> disable config
```

Evidence:
- `src/domain/screener/shadow_fleet.rs`

## 7) Portfolio Runtime Math
Useful winrate:
```text
useful_winrate = profitable_trades / closed_trades
```

PM raw:
```text
pm_raw = profitable_trades - losing_trades
```

Eligibility gate:
```text
age_minutes_from_first_tick > 5
closed_trades > 5
useful_winrate >= 0.30
avg_pnl_pct >= 0
```

Ranking tuple (descending):
1. `useful_winrate`
2. `pm_raw`
3. `avg_pnl_pct`
4. `closed_trades`
5. `symbol` (lexicographic tie-break)

Hard reset / cooldown:
```text
fast_trigger: stop_loss_streak >= 5 within 120_000 ms
persistent_trigger: stop_loss_streak >= 6
cooldown_until = ts_ms + 300_000
```

Scheduler cadence (portfolio rebalance):
```text
event_loop_tick_every = 120_000 ms
rebalance_allowed if now_ms - last_rebalance_ms >= 120_000
```

Portfolio cardinality (topology constraint):
```text
N_portfolios = len(configured_portfolio_ids), default = 2 (A,B)
max_shortlist_capacity = N_portfolios * 5
max_active_capacity = N_portfolios * 4
```

Evidence:
- `src/application/services/portfolio_runtime.rs`
- `src/domain/screener/mod.rs`
- `src/event_loop_runtime.rs`

## 8) Trial Axes Analytics Math
Bucketing:
```text
bucketed_value = round(value / step) * step
```
(`step=0` -> без бакетинга)

Per-bucket weighted average pnl:
```text
avg_pnl_pct = sum(row_avg_pnl * row_trades) / total_trades
```

Evidence:
- `src/api/handlers/trial_axes_support.rs`

## 9) Runtime Grid / Combinatorics
Общее число конфигов:
```text
total = |gaps| * |targets| * |stops| * |holds| * |spreads| * |trails| * |baselines|
```

Если `total > max_configs`, применяется downsample:
```text
stride = total / max_configs
idx_i = floor(i * stride)
```
плюс дедуп по `config_id`.

Evidence:
- `src/runtime_grid.rs`

## 10) DB-Level Aggregation Math
Portfolio candidate history из `trades`:
```text
closed_trades = COUNT(*)
profitable_trades = SUM(pnl_pct > 0)
losing_trades = SUM(pnl_pct < 0)
pnl_sum_pct = SUM(pnl_pct)
first_trade_ts_ms = MIN(entry_ts_ms)
```

Evidence:
- `src/infrastructure/db.rs::load_portfolio_candidate_history_v1`

## 11) End-to-End KPI Set (Current Runtime)
- `spread_bps`, `lag_ms`, `entry_half_life_ms`, `avg_gt_p90_ms`
- shadow:
  - `session_pnl_pct`,
  - `session_trades`,
  - `avg_trade_pct`,
  - `win_rate_pct`,
  - `avg_catchup_pct`,
  - `avg_catchup_lag_ms`
- portfolio:
  - `useful_winrate`,
  - `pm_raw`,
  - `avg_pnl_pct`,
  - cooldown guard states

Evidence:
- `src/domain/screener/mod.rs`
- `src/api/handlers.rs`
