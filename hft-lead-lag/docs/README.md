# HFT Lead-Lag Documentation (Current)

Актуальная документация по состоянию кода и production runtime.

---

## 1) Где что читать

- **`docs/README.md`** (этот файл) — актуальный overview, API, runtime, запуск.
- **`docs/shadow-fleet-deep-dive.md`** — супер-детальный deep-dive по Shadow Fleet (архитектура, математика, persistence, ranking).
- **`docs/review-2026-02-19-deep-dive.md`** — полный аудит проекта (качество, баги, слои, god objects, commits, серверная привязка).

Исторические task/sprint документы оставлены в `docs/` как архив и не являются source of truth.

---

## 2) Проверенный серверный контекст

| Параметр | Значение |
|---|---|
| OS | Linux 5.15 (KVM VM) |
| CPU | 2 vCPU Intel Xeon Skylake |
| RAM | 3.8 GiB + 9 GiB swap |
| Локация | Tokyo, Japan |
| Rust | nightly 1.95 |
| Runtime порты | HTTP 5000, WS 8181 |

---

## 3) Текущее состояние проекта

| Область | Статус | Комментарий |
|---|---|---|
| Binance/Gate WS коннекторы | ✅ | reconnect + replay + bounded channels |
| Screener runtime | ✅ | lag/drift/half-life + NATR enrichment |
| Shadow Trader (single) | ✅ | spike-follow на bid/ask, без mid |
| Shadow Fleet (multi-config) | ✅ | 1152 конфигурации на символ |
| SQLite persistence optimizer | ✅ | WAL + async batch writer |
| Fleet ranking API | ✅ | `GET /api/v1/fleet` |
| Реальные ордера | ⚠️ | executor-стабы, не подключены |

---

## 4) Архитектура (факт по коду)

```text
src/ (Rust): 39 files, 6079 LOC

api/
  http_server.rs     — router + health state
  handlers.rs        — HTTP handlers incl. /api/v1/fleet
  templates.rs       — screener UI + chart markers
  ws_server.rs       — ws broadcast

domain/screener/
  mod.rs             — ScreenerStore facade, update pipeline
  state.rs           — SymbolState + Quote + fleet field
  trader_config.rs   — all strategy params (Copy)
  price_samples.rs   — shared price ring (per symbol)
  shadow_trader.rs   — single-trader engine
  shadow_fleet.rs    — fleet grid + tick_all + drain
  cycle_tracker.rs   — divergence/convergence metrics

infrastructure/
  db.rs              — SQLite schema + writer
  exchanges/*        — Binance/Gate connectors
  enrichment.rs      — NATR enrichment
  rest/mod.rs        — REST clients
```

---

## 5) API endpoints (актуально)

| Метод | Путь | Что возвращает |
|---|---|---|
| GET | `/health` | `{status, binance, gate}`; 200/503 |
| GET | `/api/v1/symbols` | universe символов и 24h данные |
| GET | `/api/v1/screener` | таблица screener метрик |
| GET | `/screener` | HTML dashboard |
| GET | `/api/v1/shadow/:symbol` | debug single ShadowTrader |
| GET | `/api/v1/chart/:symbol` | chart series + trades |
| GET | `/api/v1/fleet` | top-50 config ranking по win-rate (min 10 trades) |

---

## 6) Shadow Trader (single)

Файл: `src/domain/screener/shadow_trader.rs`

### Вход
1. Котировки Binance/Gate свежие (`quote_freshness_ms`).
2. Прошли warmup/cooldown.
3. LONG: Binance ask move >= `spike_threshold_bps` за `spike_window_ms`.
4. SHORT: Binance bid move >= `spike_threshold_bps` за `spike_window_ms`.

### Выход
- `target`: unrealized_bps >= `spike_bps * target_ratio`
- `stop_loss`: unrealized_bps <= `-stop_loss_bps`
- `trailing_stop`: если включён (`trailing_stop_bps > 0`)
- `timeout`: hold >= `max_hold_ms`

### Дефолтные параметры
`TraderConfig::default()`:
- spike_threshold=30bps
- spike_window=500ms
- target_ratio=1.0
- stop_loss=10bps
- max_hold=30s
- fill_delay=6ms
- cooldown=3s
- warmup=30s
- quote_freshness=1s
- taker_fee=0.05% (round-trip 0.1%)

---

## 7) Shadow Fleet (в production)

Файлы:
- `src/domain/screener/shadow_fleet.rs`
- `src/infrastructure/db.rs`
- `src/domain/screener/mod.rs` (wire)
- `src/main.rs` (db init + seeding + writer spawn)

### 7.1 Parameter grid

```text
spike_threshold_bps: [20, 30, 40, 50]      (4)
spike_window_ms:     [300, 500, 1000]      (3)
target_ratio:        [0.3, 0.5, 0.7, 1.0]  (4)
stop_loss_bps:       [8, 10, 15, 20]       (4)
max_hold_ms:         [10000, 30000]        (2)
max_spread_bps:      [5, 10, 15]           (3)
TOTAL: 4*3*4*4*2*3 = 1152 configs
```

### 7.2 Data flow

1. `ScreenerStore::update()` получает бинанс/гейт quote.
2. Пишет `PriceSample` в shared `PriceSamples` (на символ).
3. Тикает single `shadow` и fleet (`tick_all`) на одних и тех же samples.
4. Fleet собирает новые `ClosedTrade` как `FleetTrade`.
5. `DbWriter` получает батчи через `mpsc`.
6. Фоновая задача flush в SQLite каждые 5 секунд.

### 7.3 SQLite

DB: `data/optimizer.db`

Таблицы:
- `configs` (параметры конфига)
- `trades` (каждая сделка с config_id)

Индексы:
- `idx_trades_config`
- `idx_trades_symbol`
- `idx_trades_exit_ts`

Режим:
- `WAL`
- `synchronous=NORMAL`

Writer:
- channel capacity: `10_000`
- flush interval: `5s`

### 7.4 Fleet ranking endpoint

`GET /api/v1/fleet`:
- `JOIN trades + configs`
- `GROUP BY config_id`
- `HAVING total >= 10`
- `ORDER BY wins/total DESC, total_pnl DESC`
- `LIMIT 50`

---

## 8) Runtime validation snapshot

Проверено на live runtime:
- `health`: `{"status":"ok","binance":true,"gate":true}`
- optimizer db: `configs=1152`, `trades` активно растёт
- fleet endpoint отдаёт непустой ranking
- release процесс стабильно запущен

---

## 9) Quality gates

```bash
cargo build
cargo test
```

Текущий результат:
- 0 warnings
- tests: 17 + 1 doctest (всего 18), все pass

---

## 10) Ключевые ограничения (честно)

1. Это paper trading, не реальное исполнение на бирже.
2. Ранжирование сейчас по win-rate; нужен второй фильтр по expectancy/profit factor.
3. /api/v1/fleet не делает Thompson Sampling online (это следующий шаг).
4. Нет portfolio-level risk allocation по флоту.

---

## 11) Последние важные коммиты

- `3eaf827` — декомпозиция shadow trader на модули config/samples/trader.
- `b093af5` — Shadow Fleet + SQLite + `/api/v1/fleet` + wiring.

---

*Last updated: 2026-02-19 (post Shadow Fleet Sprints 1-6)*
