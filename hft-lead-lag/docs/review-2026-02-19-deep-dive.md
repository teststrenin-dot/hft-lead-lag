# Deep-Dive Review: hft-lead-lag (Current)

**Дата:** 2026-02-19  
**Scope:** runtime + стратегия + fleet optimizer + persistence + API/docs consistency

---

## 1) Executive verdict

Текущее состояние: **рабочий HFT-oriented paper-trading stack с real-time optimizer контуром**.

- Архитектура: практичная для MVP, без лишнего распределённого оверхеда.
- Математика: корректная для gap-based гипотезы (baseline-normalized divergence).
- Fleet: рабочий end-to-end (generate -> trade -> persist -> rank -> UI).
- Главный прогресс: переход от старого spike-подхода к baseline gap и введение авто-прунинга.

---

## 2) Что изменилось относительно старого состояния

1. **Entry logic**
   - было: spike за окно на Binance;
   - стало: baseline-adjusted gap Binance vs Gate.

2. **Ranking objective**
   - было: win-rate heavy;
   - стало: expectancy (`total_pnl / total`) для `/api/v1/fleet`.

3. **Fleet space**
   - было: 810/1152 исторические сетки;
   - сейчас: 2430 конфигов (включая `trailing_decay_ratio`).

4. **Runtime pruning**
   - негативные конфиги отключаются после достаточной статистики;
   - нулевые (без сделок) отключаются после warmup времени.

5. **UI/API**
   - `/fleet` + `/api/v1/fleet/symbols` дают глобальный и per-symbol срез.

---

## 3) Техническая оценка по слоям

| Слой | Оценка | Комментарий |
|---|---|---|
| `domain/screener` | ✅ good | state machine читаемая, явные gates, формулы прозрачны |
| `infrastructure/db` | ✅ good | WAL, batch writer, dedupe, миграция колонки |
| `api` | ✅ good | полезные endpoint-ы + рабочий fleet dashboard |
| `main` wiring | ✅ good | fail-fast bind, symbol filtering, clear startup flow |
| Docs sync | ✅ updated | ключевые расхождения 1152/win-rate/spike-window закрыты |

---

## 4) Математическая корректность (коротко)

Корректно реализовано:

- baseline gap signal (long/short симметрия),
- entry по threshold в bps,
- exit target/SL/trailing/timeout,
- session-level PnL/trades/win-rate,
- fee-adjusted pnl с двухсторонней комиссией.

Риск модели (не баг):  
стратегия всё ещё чувствительна к regime shifts и не учитывает market microstructure фильтры типа OBI в production-логике.

---

## 5) Optimizer maturity

**Что уже production-готово для paper-loop:**

- генерация большого grid,
- параллельное shadow-исполнение на shared samples,
- persistence в SQLite,
- ranking endpoint-ы,
- runtime pruning для снижения бесполезной нагрузки.

**Что ещё не сделано (осознанно):**

1. online policy selection (Thompson/UCB),
2. portfolio allocator между конфигами,
3. robust score с profit factor / drawdown / symbol coverage.

---

## 6) Операционные риски

1. `DbWriter` при переполнении канала дропает batch (с warn) — это fail-open выбор.
2. Fleet адаптация пока rule-based pruning, не полноценный adaptive optimizer.
3. Нейминг `spike_*` частично устарел семантически (фактически используется gap threshold).
4. Реальные ордера не подключены (paper only).

---

## 7) Практический приоритет next

1. Ввести multi-factor ranking score (expectancy + PF + robustness).
2. Добавить robust endpoint с фильтром по symbol coverage.
3. Включить lightweight policy loop поверх уже существующих метрик.
4. Подключить OBI/ingress-drift фильтры после стабилизации data pipeline.

---

## 8) Финальный вывод

Проект готов к следующей фазе: **не переписывать**, а дорастить optimizer policy и risk allocation поверх уже рабочей базы.

---

*Last updated: 2026-02-19 (docs/code synchronized to current fleet runtime)*
