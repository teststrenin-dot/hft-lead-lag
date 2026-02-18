# TASK-002: Web Screener + Lead-Lag Checkpoints + Shadow Trader

**Приоритет:** P0  
**Статус:** 🟢 Complete (v1.5)

- ✅ Web Screener (API + UI + runtime метрики).
- ✅ Shadow Trader (paper trading engine).
- ✅ Real-time Chart (uPlot + WebSocket).
- ✅ Median Lag & Sortable Columns.

---

## Цель

Предоставить полный инструмент для анализа lead-lag арбитража между Binance и Gate.io, включая:
1. Скринер возможностей в реальном времени.
2. Paper-trading движок (Shadow Trader) для валидации PnL.
3. Визуализацию спредов и сделок на графике.

---

## 1) Web Screener & UI

### Endpoint'ы
- `GET /api/v1/screener` — JSON со строками метрик.
- `GET /api/v1/chart/:symbol` — данные для графика (спреды, сделки, P90/P50).
- `GET /api/v1/shadow/:symbol` — дебаг информация по Shadow Trader.
- `GET /screener` — веб-интерфейс (SPA).

### Колонки таблицы (Сортируемые)
| Колонка | Поле JSON | Описание |
|---|---|---|
| **Coin** | `symbol` | Тикер (напр. BTCUSDT) |
| **Leader** | `leader_exchange` | Кто ведет цену (Binance/Gate) |
| **Lag** | `lag_ms` | Медиана лага за 5 мин (P50 rolling) |
| **Drift BN** | `ws_drift_ingress_binance_ms` | Сетевая задержка Binance |
| **Drift GT** | `ws_drift_ingress_gate_ms` | Сетевая задержка Gate |
| **Vol 24h** | `volume_24h_usd` | Объем Gate за 24ч (млн $) |
| **Half-life** | `entry_half_life_ms` | Время возврата к P50 |
| **>P90** | `avg_gt_p90_ms` | Время в зоне расхождения |
| **NATR%** | `gate_natr_30m_pct` | Волатильность Gate (30m) |
| **Pos** | `shadow_position` | Позиция Shadow Trader |
| **PnL/hr%** | `shadow_pnl_per_hour_pct` | Доходность в час |
| **Trd** | `shadow_trades` | Кол-во сделок |
| **Avg%** | `shadow_avg_trade_pct` | Средний PnL сделки |
| **Win%** | `shadow_win_rate_pct` | Винрейт |

### Фильтры и Config
- **Volume Filter**: `MIN_VOLUME_USD = 1_000_000` (Gate 24h quote vol).
- **Blacklist**: Исключение `BTCUSDT`, `ETHUSDT`, `SOLUSDT` (через `config.toml`).
- **Universe**: Только `common symbols` (пересечение Binance/Gate).

---

## 2) Shadow Trader (Paper Trading)

Эмулятор торговли в реальном времени внутри скринера.

### Логика входа/выхода
- **Сигнал**: `gate_premium_bps = (gate - binance) / binance * 10000`.
- **Entry**: Вход, если премиум > P90 (SHORT) или < P10 (LONG).
  - **Edge Guard**: Вход только если `|P90 - P50| >= 10 bps` (покрытие комиссий).
  - **Warmup**: 2 минуты сбора статистики перед первым входом.
- **Exit**: Выход, когда премиум возвращается к P50 (mean reversion).
- **Execution**: Симуляция с задержкой 10ms.

### PnL и Комиссии
- **Fee**: 0.05% (taker) за сторону = 0.1% round-trip.
- **PnL**: Чистый % с учетом комиссий.
- **Market Impact**: Симуляция проскальзывания при нехватке ликвидности в стакане (L1).

---

## 3) Real-time Chart

График 4-х линий цен (Gate Bid/Ask, Binance Bid/Ask) + визуализация сделок.

- **Технология**: uPlot + WebSocket (`ws://host:8181/ws`).
- **Частота**: 15 FPS (throttled для снижения CPU/GC).
- **Trade Zones**:
  - 🟩 **Green Zone**: LONG сделка (от входа до выхода).
  - 🟥 **Red Zone**: SHORT сделка.
  - Пунктирные линии P90/P10 и сплошная P50 (из `pollTrades`).

---

## 4) Checkpoints (Runtime Validation)

1. **CP-01 / Lag Metric**
   `lag_ms` должен быть стабильным медианным значением (не мгновенным спайком).
   *Реализовано через rolling window 5 min P50.*

2. **CP-02 / Data Consistency**
   График должен совпадать с таблицей по состоянию позиции.
   *Реализовано через синхронизацию `/api/v1/screener` и `/api/v1/chart`.*

3. **CP-03 / Execution Logic**
   Сделки не должны открываться внутри спреда или без преимущества (edge < 10bps).
   *Реализовано через `MIN_EDGE_BPS` guard.*

4. **CP-04 / Performance**
   UI не должен фризить браузер.
   *Оптимизация: uPlot, 15fps limit, reuse arrays, no reactivity overhead.*

---

*Updated: 2026-02-18*
