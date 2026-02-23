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
- Shadow Fleet exploration через runtime-grid hot-reload (`max_configs=1500`).
- Baseline timing как гиперпараметр (`baseline_window_ms`: 10s/20s/30s/60s).
- Expectancy + composite ranking + runtime pruning + health degradation signals.
- Следующий шаг: iterative hyperparam cycle и подготовка Ray/ASHA forward-testing.

---

## Source of truth

- `docs/README.md`
- `docs/sprints/sprint-008-deal-hunt-natr-db.md`
- `docs/plans/2026-02-21-iterative-hyperparam-methodology.md`

Архивные документы не должны использоваться как source of truth.

---

*Manifest v2.3 — synchronized with runtime-grid + deal-hunt Phase A state*
