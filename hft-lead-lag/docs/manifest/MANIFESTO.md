# HFT Lead-Lag Manifest (Current)

## Миссия

Построить практичный HFT-oriented lead-lag контур, который:

1. измеряет задержки и расхождения в реальном времени;
2. тестирует гипотезы через Shadow Fleet на live потоке;
3. быстро переводит статистику в решения по параметрам.

---

## Принципы

1. **MVP-first, no overengineering**  
   Сначала работоспособный контур, потом усложнение.

2. **Bid/Ask truth only**  
   Никаких "красивых" mid-price допущений для execution/PnL.

3. **Data beats opinion**  
   Все tuning-решения подтверждаются runtime-данными.

4. **Hot path stays cheap**  
   Shared samples, bounded queues, async persistence, pruning.

5. **Fail loudly, recover predictably**  
   Ошибки логируются явно; поведение системы предсказуемо.

6. **Docs must track code**  
   Документация синхронизируется с `main`, а не с историческими гипотезами.

---

## Текущий фокус

- Gap-based lead-lag с baseline нормализацией.
- Shadow Fleet exploration: **2430 configs**.
- Expectancy-first ranking + runtime pruning.
- Подготовка к следующему шагу: policy selection + robust scoring.

---

## Source of truth

- `docs/README.md`
- `docs/sprints/shadow-fleet-deep-dive.md`
- `docs/review-2026-02-19-deep-dive.md`

Архивные документы не должны использоваться как source of truth.

---

*Manifest v2.1 — synchronized with current fleet optimizer runtime*
