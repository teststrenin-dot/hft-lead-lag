# HFT Lead-Lag Documentation

Актуальная документация проекта `hft-lead-lag`.

---

## 1) Текущее состояние

| Компонент | Статус |
|---|---|
| Exchange connectors (Binance/Gate) | ✅ Done |
| Runtime API (`/health`, `/api/v1/symbols`, `/api/v1/screener`) | ✅ Done |
| Runtime UI (`/screener`) | ✅ Done |
| Screener ingress drift метрики | ✅ Done |
| Startup timestamp drift hardening | ✅ Done |
| Shadow Trader (paper trading) | ✅ Done |
| Real-time chart (embedded in `/screener`) | ✅ Done |
| Order management | ⬜ Planned |
| Production hardening (reconnect/metrics/alerts) | ⬜ Planned |

---

## 2) Навигация по документации

```text
docs/
├── README.md
├── manifest/
│   └── MANIFESTO.md
├── backlog/
│   └── README.md
├── sprints/
│   ├── sprint-001-connectors.md
│   ├── sprint-002-orders.md
│   └── sprint-003-production.md
├── TASK-001-connectors.md
└── TASK-002-screener-leadlag-checkpoints.md
```

---

## 3) Быстрый старт

```bash
cd /root/turbo/hft-lead-lag

export BINANCE_API_KEY="..."
export BINANCE_API_SECRET="..."
export GATE_API_KEY="..."
export GATE_API_SECRET="..."

cargo run --quiet
```

Проверка:

```bash
curl http://127.0.0.1:5000/health
curl http://127.0.0.1:5000/api/v1/symbols
curl http://127.0.0.1:5000/api/v1/screener
```

- UI: `http://127.0.0.1:5000/screener`
- WS broadcast: `ws://127.0.0.1:8181/ws`

---

## 4) Основные runtime endpoint'ы

### `GET /health`
Возвращает статус HTTP сервиса (`{"status":"ok"}`).

### `GET /api/v1/symbols`
Возвращает символы Binance/Gate после volume-фильтра:
- `min_volume_usd = 1_000_000`
- `common_symbols` = пересечение символов двух бирж.
- исключены `BTCUSDT`, `ETHUSDT`, `SOLUSDT` (blacklisted).

### `GET /api/v1/screener`
Возвращает строки скринера:
- `symbol`
- `leader_exchange`
- `lag_ms`: Медиана (P50) абсолютной разницы timestamps за последние 5 минут.
- `ws_drift_ms`
- `ws_drift_binance_ms`
- `ws_drift_gate_ms`
- `ws_drift_ingress_binance_ms`
- `ws_drift_ingress_gate_ms`
- `entry_half_life_ms`
- `avg_gt_p90_ms`
- `gate_natr_30m_pct`
- Shadow trader fields: `shadow_position`, `shadow_pnl_per_hour_pct`, `shadow_trades`, `shadow_avg_trade_pct`, `shadow_win_rate_pct`
- `volume_24h_usd`: Объем торгов на Gate за 24ч.

### `GET /screener`
Веб-таблица поверх `/api/v1/screener` с встроенным real-time графиком.
- Polling таблицы: 1 раз в секунду.
- График: uPlot поверх WebSocket, 4 линии (Gate Bid/Ask, Binance Bid/Ask).
- Визуализация сделок: Trade Zones (зеленая/красная заливка).
- Сортировка колонок кликом по заголовку.

### `GET /api/v1/chart/:symbol`
JSON данные для инициализации графика:
- Downsampled premium timeseries.
- Текущие пороги P90/P10/P50.
- Список последних сделок для отрисовки зон.

### `GET /api/v1/shadow/:symbol`
Debug данные shadow trader: premium samples, cached thresholds, edge, trades, position.

---

## 5a) Shadow Trader

Paper trading модуль для валидации стратегии до реальных ордеров.

### Модель
- **Сигнал**: `premium_bps = (gate.mid − binance.mid) / binance.mid × 10000`
- **Вход**: premium пересекает замороженный P90 → SHORT Gate, P10 → LONG Gate
- **Выход**: premium возвращается к P50 (mean reversion)
- **Execution**: Gate bid/ask + L1 market impact, 10ms simulated delay
- **Fees**: Gate taker 0.05% × 2 = 10 bps round-trip
- **MIN_EDGE_BPS = 10**: вход только если |P90−P50| ≥ 10 bps (покрывает fees)

### Параметры
| Параметр | Значение | Описание |
|---|---|---|
| `WARMUP_MS` | 120000 | 2 мин прогрев перед торговлей |
| `COOLDOWN_MS` | 5000 | 5 сек пауза между сделками |
| `THRESHOLD_INTERVAL_MS` | 60000 | Пересчёт P90/P10/P50 раз в минуту |
| `QUOTE_FRESHNESS_MS` | 1000 | Макс. возраст котировки для расчёта premium |
| `EXECUTION_DELAY_MS` | 10 | Симулированная задержка order-to-fill |
| `MIN_EDGE_BPS` | 10.0 | Минимальный edge для входа (= 2 × fee) |

---

### `lag_ms`
Абсолютная разница между exchange timestamps Binance и Gate для текущих котировок.

### `ws_drift_ingress_binance_ms` / `ws_drift_ingress_gate_ms`
`local_receive_ts_ms - exchange_ts_ms` для соответствующей биржи, где `local_receive_ts_ms` фиксируется **в момент получения WS кадра** reader-задачей.

### `entry_half_life_ms`
Среднее время от входа в зону расхождения (`P90`) до схождения (`P50`) в окне 10 минут.

### `avg_gt_p90_ms`
Средняя длительность нахождения в зоне `>= P90` в окне 10 минут.

### `gate_natr_30m_pct`
NATR по Gate candles (`interval=30m`, `period=30`) в процентах.

---

## 6) Интерпретация расхождений

Если `lag_ms` высокий, а ingress drift низкий:
- это обычно не сетевой лаг процесса,
- это асинхронность обновлений между биржами и различие времени прихода последних тикеров.

Если ingress drift резко растет на многих символах:
- проверяйте нагрузку процесса и частоту polling/REST fallback,
- проверяйте состояние WS reader/consumer и backlog сообщений.

---

## 7) Проверка качества

```bash
cargo build
cargo test
```

Логи:
- `logs/runtime.log`
- `logs/test_connection_*.log`
- `logs/test_final_*.log`
- `logs/summary.log`

---

## 8) Что обновлено в текущей версии документации

- добавлен Shadow Trader: paper trading с gate-only execution,
- добавлен real-time chart `/chart` на uPlot с premium/thresholds/trade markers,
- добавлены API endpoints: `/api/v1/chart/:symbol`, `/api/v1/shadow/:symbol`, `/api/v1/chart-symbols`,
- screener таблица расширена shadow-метриками (position, pnl/hr, trades, avg trade, win rate),
- отражена фиксация `local_ts_ns` на ingress (WS receive time),
- отражен startup-drain накопленных сообщений,
- удалены чувствительные данные из документации.

---

*Last updated: 2026-02-18 (runtime drift hardening + docs refresh)*
