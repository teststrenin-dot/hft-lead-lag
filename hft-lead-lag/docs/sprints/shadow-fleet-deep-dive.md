# Shadow Fleet Deep Dive (Current Runtime)

Полный deep dive текущей реализации fleet optimizer в `main`.

**Snapshot:** 2026-02-19  
**Commit:** `807178a`

---

## 1) Задача Shadow Fleet

Shadow Fleet запускает много paper-трейдеров на одном live потоке:

- общий `PriceSamples` (shared market history),
- отдельный state на каждый config,
- online оценка параметров в реальном времени,
- запись результатов в SQLite,
- ranking через API/UI.

Это **online shadow-evaluation**, а не оффлайн backtest.

---

## 2) Модульная карта

| Модуль | Роль |
|---|---|
| `domain/screener/shadow_fleet.rs` | grid generation, tick_all, pruning, trade draining |
| `domain/screener/shadow_trader.rs` | entry/exit state machine, pnl math |
| `domain/screener/trader_config.rs` | гиперпараметры + `config_id()` |
| `domain/screener/price_samples.rs` | ring buffer истории котировок (2 минуты retention) |
| `domain/screener/state.rs` | wiring symbol update -> shadow/fleet ticks |
| `infrastructure/db.rs` | schema/migrations/upsert/writer |
| `api/handlers.rs` | ranking endpoints |
| `api/http_server.rs` | route registration |

---

## 3) Grid (актуальный)

```text
gap_threshold_bps (spike_threshold_bps): [30, 50, 60, 80]           (4)
target_ratio:                           [0.3, 0.5, 0.7]             (3)
stop_loss_bps:                          [8, 15, 25, 40]              (4)
max_hold_ms:                            [5000, 10000, 30000]         (3)
max_spread_bps:                         [3, 5]                        (2)
trailing_decay_ratio:                   [0.3, 0.7]                    (2)
baseline_window_ms:                     [10000, 20000, 30000, 60000]  (4)

TOTAL: 4 * 3 * 4 * 3 * 2 * 2 * 4 = 2304
```

`baseline_window_ms` добавлен как отдельная ось гиперпараметров.

---

## 4) Entry логика: baseline-window gap

`detect_gap(ts_ms, binance, gate, samples)`:

1. Проверка `samples.len() >= min_baseline_samples`.
2. `cutoff = ts_ms - baseline_window_ms`.
3. Baseline считается только по samples `s.ts_ms >= cutoff`.
4. Сигнал = `current_gap - baseline_gap`.
5. Вход при `signal >= spike_threshold_bps`.

Ключевое отличие от старого поведения:

- раньше baseline фактически тянулся по всей истории retention (~2m),
- теперь baseline управляется гиперпараметром окна (10-60s в grid).

---

## 5) Exit логика (двухфазная)

### Фаза 1: до breakeven

- `stop_loss`: `unrealized_bps <= -stop_loss_bps`
- `timeout`: `hold_ms >= max_hold_ms`

### Активация breakeven

- если `unrealized_bps >= spike_bps * target_ratio`

### Фаза 2: после breakeven

- `breakeven stop`: `unrealized_bps <= 0`
- `trailing_take`: `unrealized_bps <= peak_unrealized_bps * trailing_decay_ratio`
- `timeout`

---

## 6) PnL формулы

- LONG raw return: `(exit_bid - entry_ask) / entry_ask`
- SHORT raw return: `(entry_bid - exit_ask) / entry_bid`
- `pnl_pct = (raw_return - 2*taker_fee) * 100`

`catchup_pct` хранится отдельно как pre-fee метрика.

---

## 7) Pruning (runtime)

В `ShadowFleet::tick_all()`:

1. **Negative expectancy prune**
   - `session_trades >= 30`
   - `avg_pnl_pct < -0.05`
   - config -> disabled

2. **Inactive prune**
   - `session_trades == 0`
   - `elapsed >= 10 min` с начала fleet для символа
   - config -> disabled

Отключённые конфиги не тикаются дальше.

---

## 8) Persistence: SQLite

Файл: `data/optimizer.db`

### `configs`

Содержит параметры, включая:

- `trailing_decay_ratio`
- `baseline_window_ms`
- `fill_delay_ms`, `cooldown_ms`, `warmup_ms`, `quote_freshness_ms`, `taker_fee`

### `trades`

Содержит фактические paper сделки:

- `config_id`, `symbol`, `direction`,
- `entry_ts_ms`, `exit_ts_ms`, `entry_price`, `exit_price`,
- `spike_bps`, `pnl_pct`, `exit_reason`, `gate_spread_at_entry_bps`.

### Надёжность

- WAL + `synchronous=NORMAL`
- batch writer flush каждые `5s`
- `INSERT OR IGNORE` (idempotent upsert path)
- миграции добавляют отсутствующие колонки, включая `baseline_window_ms`

---

## 9) API optimizer surface

### `GET /api/v1/fleet`

- ranking по expectancy (`total_pnl / total`)
- `HAVING total >= 10`
- top 50
- включает `baseline_window_ms`

### `GET /api/v1/fleet/ranked`

- composite ranking:
  - `sharpe = avg_pnl / stddev`
  - `profit_factor = gross_win / gross_loss`
  - `composite = sharpe * sqrt(trades) * min(profit_factor, 3.0)`
- `HAVING total >= 10`
- top 100
- включает `baseline_window_ms`

### `GET /api/v1/fleet/symbols`

- лучший config на символ (`ROW_NUMBER() OVER PARTITION BY symbol`)
- `HAVING total >= 5`
- включает `baseline_window_ms`

---

## 10) Live runtime observations (snapshot)

На текущем запуске:

- universe: 53 symbols
- single-shadow trades есть у 11 symbols
- fleet/symbols показывает 3 symbols с устойчивой статистикой
- в ranked-топе доминируют `baseline_window_ms = 20s и 60s`
- лучший config на snapshot: `gap=30`, `target=0.7`, `sl=40`, `hold=5s`, `spread=5`, `trailing=0.7`, `baseline=60s`

Практический вывод:

- baseline как гиперпараметр реально влияет на поведение fleet,
- слишком короткие окна дают больше шума и нестабильный expectancy,
- окно 60s в текущем запуске выглядит устойчивее.

---

## 11) Проверка после деплоя

```bash
# build/test
cargo test
cargo build --release

# health
curl -s http://localhost:5000/health

# fleet API
curl -s http://localhost:5000/api/v1/fleet | head
curl -s http://localhost:5000/api/v1/fleet/ranked | head
curl -s http://localhost:5000/api/v1/fleet/symbols | head
```

Ожидаемо:

- `configs` seeded на `2304`,
- trades растут,
- `baseline_window_ms` присутствует в payload.

---

*Last updated: 2026-02-19 (baseline-window rollout + full status sync)*
