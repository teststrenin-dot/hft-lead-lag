# Shadow Fleet Deep Dive (Current Runtime)

Глубокий разбор текущей реализации fleet optimizer в `hft-lead-lag`.

---

## 1) Задача Shadow Fleet

Shadow Fleet запускает много paper-trader инстансов на одном символе:

- один и тот же поток котировок (`PriceSamples`) для всех конфигов;
- независимый state у каждого трейдера;
- сбор фактической статистики в SQLite;
- ranking лучших конфигов через HTTP API.

Это не backtest, а online shadow-evaluation на live рынке.

---

## 2) Карта модулей

| Модуль | Роль |
|---|---|
| `domain/screener/shadow_fleet.rs` | grid generation, tick_all, pruning, drain |
| `domain/screener/shadow_trader.rs` | entry/exit state machine и формулы |
| `domain/screener/trader_config.rs` | набор параметров и `config_id()` |
| `domain/screener/price_samples.rs` | 2-мин shared история bid/ask |
| `domain/screener/mod.rs` | wiring в `ScreenerStore::update()` |
| `infrastructure/db.rs` | schema, writer, flush |
| `api/handlers.rs` | ranking endpoints |
| `api/templates.rs` | `/fleet` UI |

---

## 3) Parameter grid (2880 configs)

```text
gap_threshold_bps (spike_threshold_bps): [50, 60, 70, 80, 100]     (5)
target_ratio (breakeven trigger):        [0.3, 0.4, 0.5, 0.7]      (4)
stop_loss_bps:                           [20, 30, 50, 80]           (4)
max_hold_ms:                             [5000, 10000, 20000, 30000](4)
max_spread_bps:                          [3, 5, 8]                  (3)
trailing_decay_ratio (trailing take):    [0.3, 0.5, 0.7]            (3)

TOTAL: 5 * 4 * 4 * 4 * 3 * 3 = 2880
```

> Двухфазная exit-модель: breakeven при spike×target_ratio → trailing take-profit.

### Почему в поле остался `spike_*` нейминг

Исторически стратегия была spike-based. Сейчас entry — baseline gap, но:

- имя поля `spike_threshold_bps` сохранено для совместимости,
- в UI колонка переименована в `Gap`,
- в ranking payload поле пока остаётся `spike_threshold_bps`.

---

## 4) Runtime lifecycle

### 4.1 Startup

1. Генерится grid (`generate_grid()`).
2. Открывается `data/optimizer.db`.
3. `upsert_configs()` seed-ит конфиги в таблицу `configs`.
4. Стартует async `DbWriter`.
5. `ScreenerStore` получает writer через `set_db_writer()`.

### 4.2 Tick path

`ScreenerStore::update()`:

1. обновляет symbol state и `PriceSamples`;
2. тикает single `shadow`;
3. lazy-init `ShadowFleet` для символа;
4. `fleet.tick_all(...)`;
5. `fleet.drain_trades()` -> `db_writer.send(...)`.

---

## 5) Вход/выход внутри одного конфигурационного трейдера

### Entry

`detect_gap()`:

- baseline по истории samples (~2 минуты, минимум 20 samples),
- signal = current gap - baseline gap,
- LONG/SHORT при signal >= threshold.

### Exit (двухфазная модель)

**Фаза 1 (до breakeven):** только stop_loss и timeout.

**Breakeven-активация:** `unrealized_bps >= spike_bps * target_ratio`.

**Фаза 2 (после breakeven):**
1. `breakeven` — стоп переносится на entry price (`unrealized <= 0`)
2. `trailing_take` — `unrealized <= peak * trailing_decay_ratio`
3. `timeout` — `max_hold_ms`

### PnL

`pnl_pct = (raw_return - 2*taker_fee) * 100`.

---

## 6) Авто-прунинг в fleet

Прунинг реализован прямо в `tick_all()`:

1. **Negative expectancy prune**
   - если `session_trades >= 30`
   - и `avg_pnl_pct < -0.05`
   - конфиг помечается disabled.

2. **Zero-trade inactivity prune**
   - если `session_trades == 0`
   - и прошло >= `10 минут` от первого тика symbol-fleet
   - конфиг отключается как неактивный.

Отключённые конфиги пропускаются при следующих тиках.  
`active()` показывает текущий живой размер флота.

---

## 7) Persistence: SQLite details

### 7.1 Schema

`configs`:
- `id`, `spike_threshold_bps`, `target_ratio`,
- `stop_loss_bps`, `max_hold_ms`, `max_spread_bps`,
- `trailing_decay_ratio`,
- `fill_delay_ms`, `cooldown_ms`, `warmup_ms`, `quote_freshness_ms`, `taker_fee`.

`trades`:
- `config_id`, `symbol`, `direction`,
- `entry_ts_ms`, `exit_ts_ms`,
- `entry_price`, `exit_price`,
- `spike_bps`, `pnl_pct`, `exit_reason`,
- `gate_spread_at_entry_bps`.

### 7.2 Reliability choices

- WAL + `synchronous=NORMAL`.
- Writer flush interval: `5s`.
- Channel capacity: `10_000`.
- `INSERT OR IGNORE` + unique natural key в trades.
- Flush error не очищает буфер (retry на следующем цикле).
- Channel overflow дропает batch с `warn!` (не silent).

### 7.3 Migration

При открытии БД выполняется idempotent migration:

- `ALTER TABLE configs ADD COLUMN trailing_decay_ratio REAL NOT NULL DEFAULT 0.5`.

---

## 8) API: optimizer surface

### 8.1 `GET /api/v1/fleet`

- агрегаты по config_id;
- фильтр `HAVING total >= 10`;
- сортировка по expectancy (`total_pnl / total DESC`);
- top 50.

Payload:
- параметры конфига + `trailing_decay_ratio`,
- `total_trades`, `wins`, `win_rate_pct`,
- `total_pnl_pct`, `avg_pnl_pct`, `symbols_traded`.

### 8.2 `GET /api/v1/fleet/symbols`

- CTE + `ROW_NUMBER() OVER (PARTITION BY symbol ...)`;
- лучший конфиг на символ (min 5 trades);
- сортировка также по expectancy.

---

## 9) Fleet UI (`/fleet`)

Страница показывает:

1. Summary cards (total trades, profitable configs/symbols, best avg pnl).
2. Global top configs таблицу.
3. Best config per symbol таблицу.

Колонки:

- Gap, Tgt, SL, Hold, Spread, Decay, Trades, Wins, WR%, PnL%, Avg%
- + `Syms` для глобальной таблицы.

---

## 10) Операционные заметки

### 10.1 Symbol universe

- `MIN_VOLUME_USD = 2.5M`
- universe = пересечение Binance/Gate символов
- strategy blacklist: BTCUSDT/ETHUSDT/SOLUSDT/DYDXUSDT

### 10.2 CPU profile

- Fleet стартует крупным (2880 конфигов/символ),
- затем уменьшается из-за двух уровней прунинга,
- это удерживает нагрузку в пределах 2 vCPU.

---

## 11) Чеклист верификации

```bash
# build/test
cargo build
cargo test

# health
curl -s http://localhost:5000/health

# fleet endpoints
curl -s http://localhost:5000/api/v1/fleet | head
curl -s http://localhost:5000/api/v1/fleet/symbols | head

# sqlite quick check
python3 - <<'PY'
import sqlite3
c = sqlite3.connect('data/optimizer.db')
print('configs=', c.execute('select count(*) from configs').fetchone()[0])
print('trades=', c.execute('select count(*) from trades').fetchone()[0])
PY
```

Ожидаемо:
- `configs = 2880`,
- trades растут,
- endpoint-ы возвращают данные.

---

## 12) Следующий уровень оптимизации

1. Multi-objective score (expectancy + profit factor + robustness).
2. Dynamic policy loop (Thompson/UCB) поверх уже собранной online-статистики.
3. Portfolio allocator между top-конфигами.
4. Интеграция OBI/ingress drift filters после стабилизации data ingress.

---

*Last updated: 2026-02-19 (after decay-grid + expectancy ranking + active pruning updates)*
