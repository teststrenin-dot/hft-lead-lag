# Shadow Fleet Deep Dive

Супер-детальная документация по текущей реализации Shadow Fleet в `hft-lead-lag`.

---

## 1) Цель и роль в системе

Shadow Fleet — это слой массового paper-тестирования параметров стратегии.

- На каждый символ запускается не один, а **N shadow-trader инстансов**.
- Все инстансы получают одинаковые рыночные данные.
- Каждый инстанс торгует со своим `TraderConfig`.
- Результаты пишутся в SQLite и доступны через API ranking.

Это основа для fine-tune и выбора робастных конфигов.

---

## 2) Модульная карта

| Файл | Ответственность |
|---|---|
| `domain/screener/trader_config.rs` | набор параметров + `config_id()` |
| `domain/screener/price_samples.rs` | shared price history per symbol |
| `domain/screener/shadow_trader.rs` | single-trader state machine |
| `domain/screener/shadow_fleet.rs` | fleet orchestrator + grid generation |
| `domain/screener/mod.rs` | wiring: update -> shadow + fleet + db writer |
| `infrastructure/db.rs` | SQLite schema + async writer |
| `api/handlers.rs` | `/api/v1/fleet` ranking endpoint |
| `main.rs` | DB init, config seeding, writer spawn |

---

## 3) Parameter grid (1152 конфигурации)

```text
spike_threshold_bps: [20, 30, 40, 50]      (4)
spike_window_ms:     [300, 500, 1000]      (3)
target_ratio:        [0.3, 0.5, 0.7, 1.0]  (4)
stop_loss_bps:       [8, 10, 15, 20]       (4)
max_hold_ms:         [10000, 30000]        (2)
max_spread_bps:      [5, 10, 15]           (3)
TOTAL: 4*3*4*4*2*3 = 1152
```

Прочие параметры наследуются из `TraderConfig::default()`:
- trailing_stop_bps = 0.0
- fill_delay_ms = 6
- cooldown_ms = 3000
- warmup_ms = 30000
- quote_freshness_ms = 1000
- taker_fee = 0.0005

---

## 4) Data model

### 4.1 Price data

`PriceSample`:
- ts_ms
- gate_bid / gate_ask
- binance_bid / binance_ask

`PriceSamples`:
- `VecDeque<PriceSample>`
- retention: 2 minutes
- shared by all traders on symbol

### 4.2 Trade data

`ClosedTrade` (single trader):
- pnl_pct
- direction
- entry/exit ts, price
- spike_bps
- catchup_pct
- catchup_ms
- gate_spread_at_entry_bps

`FleetTrade`:
- config_id
- symbol
- trade: ClosedTrade

---

## 5) Execution lifecycle

### 5.1 Startup

`main.rs`:
1. создаётся `ScreenerStore` (внутри уже `fleet_configs = generate_grid()`).
2. открывается `data/optimizer.db`.
3. выполняется `upsert_configs()` (idempotent, `INSERT OR IGNORE`).
4. запускается `DbWriter` background task.
5. writer прикрепляется к `ScreenerStore`.

### 5.2 Runtime tick path

`ScreenerStore::update()`:
1. normalize timestamps + refresh drift.
2. append sample to `PriceSamples` + cleanup.
3. tick single `shadow` trader.
4. lazy-init `ShadowFleet` (один раз на символ).
5. `fleet.tick_all(...)` по shared samples.
6. `fleet.drain_trades()` -> `db_writer.send(...)`.

### 5.3 Single trader tick internals

`ShadowTrader::tick()`:
- freshness gate
- warmup gate
- `try_fill()`
- `try_exit()`
- `try_entry()`

Entry signal:
- LONG: move of Binance ask >= threshold in window
- SHORT: move of Binance bid >= threshold in window

Exit conditions:
- target
- stop_loss
- trailing_stop (if enabled)
- timeout

---

## 6) Persistence layer (SQLite)

DB file: `data/optimizer.db`

### 6.1 PRAGMA / mode
- `journal_mode=WAL`
- `synchronous=NORMAL`

### 6.2 Schema

`configs`:
- id (PK)
- spike_threshold_bps
- spike_window_ms
- target_ratio
- stop_loss_bps
- max_hold_ms
- max_spread_bps
- trailing_stop_bps
- fill_delay_ms
- cooldown_ms
- taker_fee

`trades`:
- id (PK autoincrement)
- config_id (FK -> configs.id)
- symbol
- direction
- entry_ts_ms / exit_ts_ms
- entry_price / exit_price
- spike_bps
- pnl_pct
- exit_reason
- gate_spread_at_entry_bps

Indexes:
- idx_trades_config
- idx_trades_symbol
- idx_trades_exit_ts

### 6.3 Writer mechanics

- channel: `tokio::mpsc<Vec<FleetTrade>>`, capacity = 10_000
- flush interval: 5 seconds
- batch transaction inserts
- on channel close: flush remaining buffer
- on overflow: warn and drop batch (explicit, не silent)

---

## 7) Fleet ranking API

Endpoint: `GET /api/v1/fleet`

### 7.1 SQL logic

- JOIN `trades` + `configs`
- aggregate by config
- filter `HAVING total >= 10`
- sort: `(wins / total) DESC`, then `total_pnl DESC`
- limit 50

### 7.2 Response fields

- config params (threshold/window/target/stop/hold/spread)
- `total_trades`
- `wins`
- `win_rate_pct`
- `total_pnl_pct`
- `avg_pnl_pct`
- `symbols_traded`

---

## 8) Runtime validation checklist

```bash
# health
curl -s http://localhost:5000/health

# fleet ranking
curl -s http://localhost:5000/api/v1/fleet | python3 -m json.tool | head -40

# db counts
python3 - <<'PY'
import sqlite3
c=sqlite3.connect('data/optimizer.db')
print('configs', c.execute('select count(*) from configs').fetchone()[0])
print('trades', c.execute('select count(*) from trades').fetchone()[0])
PY
```

Ожидаемо:
- `configs = 1152`
- `trades` растёт во времени
- fleet endpoint возвращает массив ranking-объектов

---

## 9) Что уже хорошо

1. Пайплайн завершён end-to-end (generate -> trade -> persist -> rank).
2. Shared samples минимизируют память на fleet.
3. DB writer вынесен из hot path.
4. Реальные runtime данные уже доступны для fine-tune.

---

## 10) Ограничения текущей версии

1. Ranking win-rate heavy; profitability/robustness пока вторичны.
2. Thompson Sampling policy loop ещё не включён в runtime управление.
3. Нет allocator-а капитала между конфигами (paper-mode only).
4. Нет сегментации по symbol cohorts внутри API.

---

## 11) Следующий шаг fine-tune

1. Ввести score:
   - win_rate
   - avg_pnl
   - profit_factor
   - min trades constraint
   - symbols_coverage constraint
2. Добавить robust endpoint (только конфиги прибыльные на >= M символах).
3. Добавить Thompson Sampling selection loop для live shadow prioritization.

---

*Last updated: 2026-02-19 (runtime with active fleet + sqlite persistence + /api/v1/fleet)*
