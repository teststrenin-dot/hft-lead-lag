# Deep-Dive Review: hft-lead-lag (Updated after Shadow Fleet)

**Дата:** 2026-02-19  
**Формат:** deep-dive (code + runtime + commits + server constraints)  
**Статус:** актуализировано после Sprint 1-6 по Shadow Fleet

---

## 1) Executive verdict

Проект в текущем состоянии — **сильный HFT-oriented MVP foundation** с рабочим runtime и рабочим optimizer-пайплайном.

- **Инженерное качество:** good (после закрытия P0/P1 и декомпозиции монолитов)
- **Математика стратегии:** корректная для текущей гипотезы (spike-follow + bid/ask only)
- **Логика и state-machine:** консистентна, без пирамидинга, с контролируемыми exit rules
- **Архитектура:** достаточно компактна для MVP, без избыточного enterprise-overhead
- **Shadow Fleet:** внедрён корректно и производительно для 2 vCPU / 3.8 GiB

---

## 2) Серверная привязка (ground truth)

| Параметр | Значение |
|---|---|
| CPU | 2 vCPU |
| RAM | 3.8 GiB |
| Swap | 9 GiB |
| Runtime ports | 5000 / 8181 |
| Release process | live |
| Health | ok |

Почему это важно: все выводы ниже валидированы под реальный лимитный сервер, не под абстрактный high-end стенд.

---

## 3) Качество по категориям

| Категория | Оценка | Комментарий |
|---|---|---|
| Общая инженерия | ✅ Good | clear module boundaries, predictable runtime |
| Математика | ✅ Good | bps math корректна, win-rate/PnL считаются корректно |
| Логика | ✅ Good | entry/exit lifecycle последовательный, fill-delay симулируется |
| Баги | ✅ Good | критичные P0/P1 закрыты, warnings=0 |
| Слои | ✅ Good | domain/api/infra разделены | 
| Дублирование | ✅ Better | ключевые дубли убраны (включая parser/logic churn) |
| God objects | ✅ Fixed | screener/http_server декомпозированы, shadow разделён на 3 модуля |

---

## 4) Проверка Shadow Fleet (deep-dive)

### 4.1 Реализация

Внедрён полный цикл:
- `generate_grid()` => 1152 конфигов
- `ShadowFleet::tick_all()` на shared `PriceSamples`
- `drain_trades()` -> `DbWriter`
- SQLite WAL batch flush (5s)
- `/api/v1/fleet` ranking endpoint

### 4.2 Производительность

Для MVP на 2 vCPU решение рабочее:
- shared samples исключили N-копий истории для каждого трейдера
- batch persistence уносит I/O из hot path
- процесс стабилен при live потоке

### 4.3 Корректность метрик

Что корректно:
- `wins` = `pnl_pct > 0`
- `win_rate_pct` = `wins / total`
- сортировка ranking: win-rate, затем total_pnl

Ограничение:
- высокий win-rate может быть с отрицательным expectancy (видно на некоторых конфигурациях), поэтому для финального fine-tune нужен multi-metric ranking.

---

## 5) Баги / риски (актуально)

### Закрыто
- P0 security/reconnect/health/bounded-channels/fail-fast
- P1 decomposition/dead-code cleanup/layer cleanup
- shadow trade marker fixes, mid-price removal, stop-loss/fill-delay adjustments

### Оставшиеся риски (не критично для MVP)
1. Ranking сейчас win-rate heavy, нужен profitability-weighted score.
2. Нет online Thompson Sampling policy-loop (есть инфраструктурная база, нет loop управления arms).
3. Нет portfolio-level budget allocator на fleet config уровне.
4. Нет отдельного robust-segment ranking (by symbol cluster/tier).

---

## 6) Архитектура: избыточность vs MVP

Вердикт: **не избыточно для MVP**.

Почему:
- Нет тяжёлых оркестраторов, брокеров, distributed infra.
- SQLite выбран уместно (single-node, быстрый локальный write/read, WAL).
- Большая часть сложности — полезная и напрямую обслуживает целевую задачу fine-tune.

---

## 7) Commit trajectory (deep-dive summary)

- `3eaf827`: правильно устранён mini-god-object в shadow модуле, добавлены config/samples abstractions.
- `b093af5`: завершён вертикальный срез fleet (grid + storage + API + wiring) — это хороший production-style commit.

Итог по коммитам: trajectory улучшилась от feature-churn к более системной инженерии с закрытием циклов (implement + test + deploy + verify).

---

## 8) Финальный вывод

Проект в текущей точке:
- **готов к фазе fine-tuning экспериментов**, 
- имеет достаточную инфраструктуру для сбора статистики,
- имеет рабочий и проверенный shadow-fleet runtime на реальном сервере.

Для перехода к “полноценному заработку” нужен не rewrite, а эволюция scoring и policy (Thompson + robust objective), используя уже внедрённую базу.

---

*Last updated: 2026-02-19 (post Shadow Fleet sprints 1-6, commit b093af5)*
