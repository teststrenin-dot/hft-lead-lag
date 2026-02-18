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
- `min_volume_usd = 10_000_000`
- `common_symbols` = пересечение символов двух бирж.

### `GET /api/v1/screener`
Возвращает строки скринера:
- `symbol`
- `leader_exchange`
- `lag_ms`
- `ws_drift_ms`
- `ws_drift_binance_ms`
- `ws_drift_gate_ms`
- `ws_drift_ingress_binance_ms`
- `ws_drift_ingress_gate_ms`
- `entry_half_life_ms`
- `avg_gt_p90_ms`
- `gate_natr_30m_pct`

### `GET /screener`
Веб-таблица поверх `/api/v1/screener` (polling 1 раз в секунду).

---

## 5) Важные определения метрик

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

- отражена фиксация `local_ts_ns` на ingress (WS receive time),
- отражен startup-drain накопленных сообщений,
- отражено поведение `/api/v1/screener` (fallback только при отсутствии live rows),
- отражен polling `/screener` раз в 1 секунду,
- удалены чувствительные данные из документации.

---

*Last updated: 2026-02-18 (runtime drift hardening + docs refresh)*
