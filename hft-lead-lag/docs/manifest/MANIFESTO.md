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
- Ray orchestration контур активен: `ray_driver` + `trial-batch.json`/`.trial-ack`.
- Trial analytics доступны через `/api/v1/trials*` и `/trials`.
- Следующий шаг: multi-trial ASHA (не `num_samples=1`) и контролируемый auto-promotion.

---

## Source of truth

- `docs/README.md`
- `docs/ray-asha-deep-dive.md`
- `src/main.rs`
- `ray_driver/*`

Архивные документы не должны использоваться как source of truth.

---

*Manifest v2.4 — synchronized with live Ray/ASHA integration state*
