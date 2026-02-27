# Delivery Playbook — Outcome First, Contract First

Date: 2026-02-26  
Scope: обязательный рабочий процесс для всех следующих изменений (CP4+ и дальше).
Strategic anchor: `docs/status/core/2026-02-27-business-objective-economic-control-map.md`

## 0) Strategic Pre-Check (mandatory)
Перед стартом любой вехи команда обязана зафиксировать:
1. Какой узел Economic Control Map изменяется (`Signal/Validation/Competition/Risk/Capital/Feedback`).
2. Какую бизнес-метрику это должно улучшить.
3. Какой риск снижается.
4. Что сломается, если этот шаг не сделать.

Без этого шага реализация не начинается.

## 1) Core Principle
Сначала фиксируем **результат и критерии готовности**, потом делаем **контракты и тестируемые вехи**, и только потом реализацию.

Запрещено:
- начинать код до явных контрактов и сценариев проверки;
- расширять scope в ходе вехи;
- смешивать архитектурный рефактор и новую бизнес-логику в одном шаге.

## 2) Delivery Order (mandatory)
1. Strategic pre-check (economic node + metric + risk + failure mode).
2. Outcome + Definition of Done.
3. Ограничения и quality gates (метрики, риск, стабильность).
4. Контракты модулей и payload-форматы.
5. Веха (минимально изолируемая) + Given/When/Then тесты.
6. Минимальная реализация до green.
7. Рефактор только после green.

## 3) Required Artifacts per Milestone
- `spec`: цель, scope, DoD, acceptance.
- `contracts`: DTO/events/interfaces.
- `tests`: unit + integration + scenario/property (где нужно).
- `report`: результат проверки вехи (что прошло/что осталось).

## 4) Current Project Mapping
### CP4 — Portfolio Race & Paper Runtime
Outcome:
- `1 portfolio = 1 execution loop` (paper mode), winner-promotion path, health per portfolio.

DoD:
- loop каждого портфеля изолирован;
- winner switch детерминирован и воспроизводим;
- API показывает execution state по каждому портфелю;
- e2e smoke green после рестарта/ошибок связи.

Contract-first artifacts:
- execution state contract (`portfolio_id`, `winner`, `loop_state`, `last_error`, `last_tick_ms`);
- winner selection contract (входные метрики, tie-break, versioned ruleset);
- restart/recovery contract (какое состояние обязательно переживает рестарт).

### CP7 — Capital Rebalance + Live
Outcome:
- управляемый ребаланс капитала + безопасный live rollout.

DoD:
- allocation/reallocation policy формально определена и детерминирована;
- risk guards (kill switch, daily stop, cap limits) в runtime обязательны;
- есть rollback/runbook на уровень портфеля и символа.

Contract-first artifacts:
- capital allocation contract;
- live safety contract;
- incident/recovery runbook contract.

## 5) Engineering Rules for This Repository
- Любой новый endpoint/DTO: сначала schema и контрактные тесты.
- Любая новая runtime-логика: сначала сценарии Given/When/Then.
- Любая миграция state: versioned format + migration test.
- Любая “оптимизация”: только после доказанной корректности.

## 6) Working Agreement
С этого момента все следующие шаги выполняются только в этом порядке:
`Strategic Pre-Check -> Outcome -> Contracts -> Tests -> Minimal Implementation -> Verification`.
