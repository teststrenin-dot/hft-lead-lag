# HFT Lead-Lag Documentation (Current)

Актуальная документация по состоянию кода и runtime в `main`.

---

## 1) Что читать в первую очередь

- `docs/README.md` (этот файл): карта системы, формулы, API, runbook.
- `docs/sprints/shadow-fleet-deep-dive.md`: deep dive по Shadow Fleet/optimizer.
- `docs/review-2026-02-19-deep-dive.md`: инженерный аудит и риски.
- `docs/studies/leadlag_classic+.md`: исследовательский материал и идеи (OBI/Kalman/hot-reload).
- `docs/review-shadow-trader.md`: архивный документ по старой реализации.

---

## 2) Серверный контекст (ground truth)

| Параметр | Значение |
|---|---|
| OS | Linux 5.15 (KVM VM) |
| CPU | 2 vCPU |
| RAM | 3.8 GiB (+ swap) |
| Runtime | Rust nightly 1.95 + Tokio |
| HTTP | `:5000` |
| WS | `:8181` |

---

## 3) Архитектура в одном потоке

1. `main.rs` получает символы по 24h объёму с Binance/Gate.
2. Берётся пересечение символов, применяется blacklist, поднимается runtime.
3. Каждый тик идёт в `ScreenerStore::update()`.
4. Для символа обновляется `PriceSamples` (2-мин история).
5. Тикается:
   - `shadow` (single paper trader),
   - `fleet` (много конфигов на тех же samples).
6. Fleet-трейды дренятся в `DbWriter` и батчами пишутся в SQLite.
7. API/UI читают state + агрегаты из `optimizer.db`.

---

## 4) Математика стратегии (актуальная)

### 4.1 Entry (gap-based lead-lag with baseline)

Используется baseline-нормализация расхождения Binance vs Gate по истории samples.

Для LONG:

- `baseline_ask_gap = mean((binance_ask - gate_ask) / gate_ask * 10_000)`
- `current_ask_gap = (binance_ask - gate_ask) / gate_ask * 10_000`
- `signal_long = current_ask_gap - baseline_ask_gap`
- вход если `signal_long >= spike_threshold_bps`

Для SHORT:

- `baseline_bid_gap = mean((gate_bid - binance_bid) / gate_bid * 10_000)`
- `current_bid_gap = (gate_bid - binance_bid) / gate_bid * 10_000`
- `signal_short = current_bid_gap - baseline_bid_gap`
- вход если `signal_short >= spike_threshold_bps`

> В коде имя поля историческое (`spike_threshold_bps`), но семантика сейчас именно **gap threshold**.

### 4.2 Exit

`unrealized_bps`:

- LONG: `((gate_bid - entry_price) / entry_price) * 10_000`
- SHORT: `((entry_price - gate_ask) / entry_price) * 10_000`

Условия выхода:

1. `target`: `unrealized_bps >= spike_bps * target_ratio`
2. `stop_loss`: `unrealized_bps <= -stop_loss_bps`
3. `trailing_stop` (если включён): падение ниже `peak_unrealized_bps * trailing_decay_ratio`
4. `timeout`: `hold_ms >= max_hold_ms`

### 4.3 PnL

- `raw_return` считается по bid/ask исполнения.
- Комиссия: `taker_fee * 2` (вход + выход).
- `pnl_pct = (raw_return - 2 * taker_fee) * 100`
- `catchup_pct = raw_return * 100` (до комиссий).

---

## 5) Screener: что означают ключевые столбцы

Все данные приходят из `ShadowStats` и `ScreenerRow`.

| Колонка | Формула/смысл |
|---|---|
| `Spikes` | Кол-во entry-сигналов за последние 2 минуты (`spike_timestamps`) |
| `PnL%` | Суммарный session PnL в процентах (`session_total_pnl_pct`) |
| `Trd` | Кол-во закрытых сделок за сессию (`session_trades`) |
| `Avg%` | `session_pnl_pct / session_trades` |
| `Win%` | `(session_wins / session_trades) * 100` |
| `Catch%` | Средний `catchup_pct` по rolling window закрытых сделок |
| `Lag ms` | Средний `catchup_ms` по rolling window закрытых сделок |

---

## 6) Shadow Fleet / Optimizer

### 6.1 Grid (текущий)

```text
gap_threshold_bps (spike_threshold_bps): [40, 50, 60, 70, 80, 100]  (6)
target_ratio:                            [0.3, 0.5, 0.7]            (3)
stop_loss_bps:                           [8, 10, 15, 20, 30]        (5)
max_hold_ms:                             [3000, 5000, 10000]         (3)
max_spread_bps:                          [3, 5, 8]                   (3)
trailing_decay_ratio:                    [0.3, 0.5, 0.7]             (3)
spike_window_ms:                         [500] (fixed for id/compat) (1)

TOTAL: 6 * 3 * 5 * 3 * 3 * 3 = 2430 configs
```

### 6.2 Авто-прунинг конфигов

В `ShadowFleet::tick_all()`:

1. **Negative expectancy prune**  
   Если `session_trades >= 30` и `avg_pnl_pct < -0.05`, конфиг отключается.

2. **Inactive prune**  
   Если за `10 минут` после старта symbol-fleet у конфига `0 трейдов`, он отключается.

Отключённые конфиги не тикаются дальше, но остаются в памяти/истории как pruned.

### 6.3 Ranking endpoints

- `GET /api/v1/fleet`
  - `GROUP BY config_id`
  - `HAVING total >= 10`
  - `ORDER BY total_pnl / total DESC` (**expectancy**, не win-rate)
  - `LIMIT 50`

- `GET /api/v1/fleet/symbols`
  - лучший конфиг на символ (`ROW_NUMBER() OVER (PARTITION BY symbol ...)`)
  - `HAVING total >= 5`
  - сортировка по expectancy.

---

## 7) БД и надежность записи

Файл: `data/optimizer.db`

### 7.1 Таблицы

- `configs`:
  - ключевые параметры стратегии, включая `trailing_decay_ratio`
- `trades`:
  - закрытые сделки (`config_id`, symbol, entry/exit, `pnl_pct`, `spike_bps`, reason)

### 7.2 Индексы/идемпотентность

- `idx_trades_config`
- `idx_trades_symbol`
- `idx_trades_exit_ts`
- `UNIQUE(config_id, symbol, entry_ts_ms, exit_ts_ms)` natural key
- `INSERT OR IGNORE` для idempotent persistence

### 7.3 Writer semantics

- `mpsc` capacity = `10_000`
- flush interval = `5s`
- WAL + `synchronous=NORMAL`
- flush error: буфер **не очищается** (повторная попытка позже)
- channel overflow: batch drop с warn-логом (осознанный fail-open)

---

## 8) Universe символов

В `main.rs`:

- фильтр объёма: `MIN_VOLUME_USD = 2_500_000`
- universe = пересечение Binance/Gate символов
- blacklist = env blacklist + strategy blacklist
- strategy blacklist (текущий): `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `DYDXUSDT`
- fallback при проблемах REST: `BTCUSDT`, `ETHUSDT`

---

## 9) HTTP/WS маршруты

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/health` | статус runtime и коннектов |
| GET | `/api/v1/symbols` | список символов + объёмы |
| GET | `/api/v1/screener` | таблица screener |
| GET | `/api/v1/shadow/:symbol` | debug single shadow |
| GET | `/api/v1/chart/:symbol` | исторические ряды + сделки |
| GET | `/api/v1/fleet` | глобальный ranking конфигов |
| GET | `/api/v1/fleet/symbols` | лучший конфиг на символ |
| GET | `/screener` | HTML screener page |
| GET | `/fleet` | HTML fleet optimizer page |
| WS | `ws://host:8181/ws` | live market ticks |

---

## 10) Runbook

```bash
# build/test
cargo build
cargo test

# release
cargo build --release
./target/release/hft-lead-lag
```

Быстрая проверка:

```bash
curl -s http://localhost:5000/health
curl -s http://localhost:5000/api/v1/fleet | head
curl -s http://localhost:5000/api/v1/fleet/symbols | head
```

---

## 11) Ограничения текущей версии

1. Paper trading, не real execution.
2. Авто-прунинг локальный (per symbol fleet), но нет полноценного online policy engine.
3. Нет portfolio allocator между конфигами.
4. Channel overflow в writer дропает batch (с логом).
5. OBI/Kalman/hot-reload описаны в studies, но ещё не интегрированы в runtime.

---

## 12) Источник правды

Если есть конфликт между документом и кодом:

1. `src/domain/screener/*`
2. `src/api/*`
3. `src/infrastructure/db.rs`
4. `src/main.rs`

Код имеет приоритет, docs синхронизируются с `main`.

---

*Last updated: 2026-02-19 (post fleet optimizer upgrades: expectancy ranking, decay grid, auto-pruning, zero-trade pruning)*
