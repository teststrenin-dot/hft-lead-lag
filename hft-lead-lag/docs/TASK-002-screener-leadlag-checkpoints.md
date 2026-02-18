# TASK-002: Web Screener + Lead-Lag Checkpoints

**Приоритет:** P0  
**Статус:** 🟡 Partially complete

- ✅ Web Screener (API + UI + runtime метрики) — реализован.
- 🟡 Полная торговая логика на percentile-триггерах (P90/P50 execution flow) — в backlog.

---

## Цель

Сделать проверяемый screener для lead-lag наблюдения и зафиксировать checkpoint-правила для runtime валидации.

---

## 1) Web Screener (реализовано)

### Endpoint'ы
- `GET /api/v1/screener` — JSON со строками метрик.
- `GET /screener` — веб-таблица поверх API.

### Текущие колонки UI
- `Coin` (`symbol`)
- `Leader` (`leader_exchange`)
- `Lag (ms)` (`lag_ms`)
- `WS drift ingress Binance (ms)` (`ws_drift_ingress_binance_ms`)
- `WS drift ingress Gate (ms)` (`ws_drift_ingress_gate_ms`)
- `Entry half-life (ms)` (`entry_half_life_ms`)
- `Avg >P90 time (ms)` (`avg_gt_p90_ms`)
- `Gate NATR 30m (%)` (`gate_natr_30m_pct`)

### Источники и фильтры
- Только `common symbols` (после пересечения Binance/Gate).
- Volume фильтр: `quote_volume >= 10_000_000 USD`.
- Период для half-life / avg>p90: скользящее окно `10m`.

### Актуальные runtime детали
- `local_ts_ns` для drift фиксируется на ingress WS-reader.
- Startup backlog очищается перед входом в основной loop.
- `/api/v1/screener` использует REST fallback только когда live rows отсутствуют.
- UI polling `/screener`: 1 запрос в секунду.

---

## 2) Checkpoints (актуальная версия)

1. **SCREENER-CP-01 / Поля строки**  
   В выдаче присутствуют поля:
   `symbol`, `leader_exchange`, `lag_ms`,
   `ws_drift_ingress_binance_ms`, `ws_drift_ingress_gate_ms`,
   `entry_half_life_ms`, `avg_gt_p90_ms`, `gate_natr_30m_pct`.

2. **SCREENER-CP-02 / Volume filter**  
   В runtime universe нет символов с `quote_volume < 10_000_000`.

3. **SCREENER-CP-03 / Стабильная выдача для UI**  
   API отдает детерминированно отсортированные строки по `symbol` (ASC).

4. **SCREENER-CP-04 / Валидность lag**  
   `lag_ms >= 0`, `leader_exchange ∈ {binance, gate}`.

5. **SCREENER-CP-05 / Drift semantics**  
   `ws_drift_ingress_*` рассчитывается как `ingress_receive_ts_ms - exchange_ts_ms`.

6. **SCREENER-CP-06 / Half-life semantics**  
   `entry_half_life_ms` = среднее по завершенным циклам `P90 -> P50` в окне 10 минут.

7. **SCREENER-CP-07 / P90-zone duration semantics**  
   `avg_gt_p90_ms` = средняя длительность нахождения в зоне `>= P90` в окне 10 минут.

8. **SCREENER-CP-08 / Gate NATR semantics**  
   `gate_natr_30m_pct` вычисляется из Gate candles (`interval=30m`, `period=30`) и неотрицателен.

---

## 3) Интерпретация метрик

- Высокий `lag_ms` при низком ingress drift не обязательно означает сетевую проблему процесса.
- Обычно это отражает разный темп обновлений котировок на биржах и момент "последнего свежего тика" в каждой стороне.

---

## 4) Что осталось по TASK-002

### Не завершено
- Полный execution-контур, где entry/exit решения стратегии строятся напрямую от rolling percentile триггеров (`P90/P50`) как торговый state machine.

### Текущее состояние стратегии
- Базовые lead-lag сигналы присутствуют,
- но execution-пайплайн percentile-based версии требует отдельной реализации.

---

*Updated: 2026-02-18*
