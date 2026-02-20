# Deal-Hunt + Hot Reload Design (Phase A)

**Date:** 2026-02-21  
**Status:** approved for implementation

## Goal
Собрать максимум сделок для первичного картирования пространства гиперпараметров, не допуская «ультраговно»-паттерна (быстрый stop-loss churn), и подготовить систему к частым коротким прогонам с поэтапным отбором конфигов.

## Product Decisions (фиксировано)
1. Фаза A (первичный отбор): целевая функция = количество сделок.
2. Первичное отсечение: в первую очередь удаляем конфиги без сделок.
3. Качество на фазе A почти не ограничиваем, кроме ultra_govno.
4. Ultra_govno формально:
   - `exit_reason = stop_loss`
   - `hold_ms <= 500`
5. Нужна накопительная статистика и смена конфигураций по накопленным данным.
6. Нужен search policy для диапазонов и шага (расширение/сужение).

## Architecture (Phase A)
1. **Data plane enrichment**
   - Добавить в `trades` поля контекста входа:
     - `gate_natr_30m_pct_at_entry`
     - `hold_ms`
     - `early_stop_churn` (bool/int)
   - Это минимальный baseline для анализа параметров.

2. **Runtime NATR snapshot**
   - Периодически обновлять NATR по символам через Gate REST.
   - Хранить последнее значение в `ScreenerStore`/`SymbolState`.
   - При открытии позиции фиксировать NATR в контексте сделки.

3. **Deal-hunt run cadence**
   - Короткий прогон: ~10 минут.
   - Бюджет конфигов: до 1500 на прогон.
   - Каждые 5-10 минут pruning по нулевым сделкам.
   - Новые конфиги добавляются за счет расширения/сужения диапазонов (следующий шаг).

4. **Hot reload preparation (Phase A scope)**
   - Добавить runtime-control конфиг для параметров прогона и NATR refresh.
   - Ввести структуру данных для generation/run метаданных.
   - Полный live hot-reload конфигов трейдеров — в следующем спринте (Phase B).

## Non-goals (Phase A)
1. Полная авто-оптимизация качества (PnL/risk-adjusted) на уровне production allocator.
2. Жесткие защитные risk guardrails кроме ultra_govno.
3. Финальный adaptive policy от NATR (только подготовка данных и контур).

## Risks and Controls
1. REST-нагрузка по NATR:
   - Ограничить batch на цикл.
   - Ограничить частоту обновления.
2. Неполные NATR-данные:
   - Писать `0.0` fallback, но логировать coverage.
3. Ложные pruning-решения на коротком окне:
   - На Phase A режем только zero-trade и отдельно маркируем ultra_govno.

## Success Criteria (Phase A)
1. Каждая сделка в `trades` содержит NATR+hold+early_stop_churn.
2. Есть runtime лог метрик coverage по NATR.
3. Есть документированный спринт-процесс коротких прогонов deal-hunt.
