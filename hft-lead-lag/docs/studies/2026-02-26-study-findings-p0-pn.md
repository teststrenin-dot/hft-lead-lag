# Study Findings (P0..Pn)

Дата: 2026-02-26  
Источники:
- `docs/studies/audit_status.md`
- `docs/studies/audit_status_add.md`
- `docs/studies/audit_status_gemini.md`

Ниже только те пункты, которые реально полезны для roadmap.  
Формат: `приоритет -> что нашли -> почему это важно -> confidence`.

## P0

1. Потеря направления в `lead_lag` сигнале (`max(bid_ask, ask_bid)` без явной ветки)
- Что нашли: сигнал хранит только `spread_bps`, но не хранит, какая ветка победила (`bid_ask` или `ask_bid`).
- Почему важно: без явного направления легко получить неконсистентные вход/выход и слабую диагностируемость.
- В коде: `src/application/services/lead_lag.rs` (`bid_ask_bps`, `ask_bid_bps`, `max`), `src/application/strategies/mod.rs` (context без branch id).
- Источник: `audit_status.md`, `audit_status_add.md`.
- Confidence: `high`.

2. Rebalance портфеля все еще зависит от тиков (event-driven), а не от отдельного scheduler
- Что нашли: gate 2 минуты есть, но вызов rebalance идет из quote-ingest пути.
- Почему важно: недетерминированный cadence на редком/всплесковом потоке.
- В коде: `src/domain/screener/quote_ingest.rs`, `src/domain/screener/mod.rs`.
- Источник: `audit_status.md`, `audit_status_add.md`, `audit_status_gemini.md`.
- Confidence: `high`.

## P1

1. Time-domain/offset риск был валидный, и уже закрыт
- Что нашли: старый риск сравнения кросс-биржевых `exchange_ts` подтверждался; добавлена офсет-коррекция по бирже.
- Почему важно: снимает системный bias лидера/лага при рассинхроне часов бирж.
- В коде: `src/domain/screener/clock_offset.rs`, `src/domain/screener/quote_ingest.rs`, `src/application/services/lead_lag.rs`.
- Источник: `audit_status.md`, `audit_status_add.md`.
- Confidence: `high`.
- Статус: `done`.

2. Недостаток диагностик по сигналу в runtime
- Что нашли: в live логах нет явных полей `edge_long/edge_short/winning_branch`.
- Почему важно: сложно отлаживать связь между “большим спредом” и фактическим PnL.
- Источник: `audit_status.md`, `audit_status_add.md`.
- Confidence: `medium-high`.

3. Eject учитывает только `stop_loss` streak (не все отрицательные исходы)
- Что нашли: убыточные `timeout/breakeven/trailing_take` не инкрементят guard streak.
- Почему важно: возможен “drip loss” сценарий без hard reset.
- В коде: `src/application/services/portfolio_runtime.rs` (`if !is_stop_loss { return false; }`).
- Источник: `audit_status.md`, `audit_status_gemini.md`.
- Confidence: `high` как факт, `medium` как “обязательно менять” (зависит от продуктового решения).

## P2

1. Малые выборки для eligibility/ranking (`closed_trades > 5`) шумные
- Что нашли: ранний отбор на малых `n` + сырые winrate/pm_raw.
- Почему важно: shortlist может ловить случайные всплески.
- В коде: `src/application/services/portfolio_runtime.rs`.
- Источник: `audit_status.md`, `audit_status_add.md`, `audit_status_gemini.md`.
- Confidence: `high` как статистический риск.

2. Portfolio history (all-time) и fleet decay windows (1h/6h/24h) живут в разных горизонтах
- Что нашли: fleet адаптивный, portfolio более инерционный.
- Почему важно: медленная реакция на regime shift в портфельном слое.
- Источник: `audit_status.md`, `audit_status_add.md`.
- Confidence: `medium-high`.

3. Fee-units guardrails нужны как observability, не как core-bug
- Что нашли: формула корректна, но полезен регулярный sanity-check `fees_bps`.
- Почему важно: защититься от конфиг-ошибок формата “процент vs доля”.
- Источник: `audit_status.md`.
- Confidence: `medium`.

## P3 / Low-Confidence (из Gemini, брать выборочно)

1. “Срочно только maker-taker, иначе стратегия математически невозможна”
- Оценка: слишком категорично для текущего этапа (у нас shadow/read-model и другая цель).
- Что полезно взять: отдельная проверка unit-экономики (`edge` vs `fees+slippage`) как KPI-гейт.
- Confidence: `low-medium` (в части категоричности), `medium` (в части идеи unit-economics гейта).

2. “Нужно срочно CPU pinning / DPDK / kernel bypass”
- Оценка: преждевременная оптимизация для текущего maturity.
- Что полезно взять: оставить как future perf-track после стабилизации логики.
- Confidence: `low` для текущей фазы.

3. “Обязательно t-digest/HDR вместо текущих процентилей”
- Оценка: имеет смысл только при подтвержденном CPU bottleneck на перцентилях.
- Confidence: `low-medium` как immediate action, `medium` как future option.

## Рекомендуемый ближайший порядок

1. `P0`: добавить явный `direction/winning_branch` в сигнал + runtime telemetry.
2. `P0`: вынести portfolio rebalance в отдельный scheduler с фиксированным 2m cadence.
3. `P1`: расширить guardrail-метрики (drift drops, branch-hit, fee sanity, loss-by-exit-reason).
4. `P2`: решить продуктово, нужен ли soft-eject по отрицательным timeout/breakeven сериям.
