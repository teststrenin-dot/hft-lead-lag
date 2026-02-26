# Business Logic v1 — Roadmap

Date: 2026-02-26  
Last sync: commits up to `ad041ca`

Scope: дорожная карта реализации бизнес-логики от текущего состояния до финальных вех (`paper execution` -> `money rebalance` -> `live`).

## Checkpoint Status
| Checkpoint | Status | Notes |
|---|---|---|
| CP0 — Baseline Lock | `Done` | Shadow fleet + portfolio runtime + API/health baseline зафиксированы. |
| CP1 — Race-Ready Portfolios | `Done` | Dynamic portfolio ids, независимые shortlist, no-overlap active symbols, UI/API read-model закрыты. |
| CP2 — Promotion & Bot Runtime | `In Progress` | Ещё нет явной runtime-связки `portfolio -> dedicated execution loop` и auto-promote winner path. |
| CP3 — Capital Rebalance + Live | `Planned` | Денежный ребаланс и live execution контур в runtime ещё не включены. |

## Current Readiness
- Готовность к целевому состоянию **CP1**: `100%` (доставлено в код и тесты).
- Готовность к сквозной бизнес-логике до запуска live (CP2 + CP3): `~65%`.

## What Was Closed Since Previous Roadmap Revision
- Dynamic portfolio count через env-конфигурацию (`PORTFOLIO_IDS`) без перекомпиляции.
- Независимое формирование shortlist per portfolio.
- Сохранение правила no-overlap для активных символов.
- Адаптация backend API/UI read-model к динамическому числу портфелей.
- Стабилизационные фиксы по lead-lag свежести/clock offsets/portfolio fairness и health telemetry.

Evidence:
- `src/main.rs`
- `src/application/services/portfolio_runtime.rs`
- `src/domain/screener/mod.rs`
- `src/api/handlers.rs`
- `src/api/handlers/tests.rs`

## Remaining Work Queue
### P2 (next checkpoint work)
1. Явная runtime-связка `1 portfolio = 1 bot execution loop` (изолированный цикл исполнения и lifecycle по портфелю).
2. Winner promotion path: формальный выбор победителя гонки и автоматический перевод в execution path.
3. Health/restart policy per portfolio bot (а не только общий runtime health).

### P3 (final milestone work)
1. Money rebalance policy между портфелями (allocation/reallocation + risk limits).
2. Live-trading safety layer: kill-switches, лимиты и rollback/runbook.
3. Dynamic hyperparameters (v2): адаптация порогов к regime shift.

## Next Checkpoint Definition (CP2)
**Goal:** сделать портфельную гонку операционной, но всё ещё без live денег.

**Acceptance Criteria:**
1. Для каждого портфеля есть отдельный execution loop и отдельный health-state.
2. Winner selection воспроизводим и основан на формальном score/правилах.
3. Auto-promote winner не ломает shadow ingestion и историческую аналитику.
4. REST API возвращает состояние execution по каждому портфелю.

## Notes
- Этот документ отражает **delivery roadmap**.
- Процесс исполнения фиксирован в `docs/status/2026-02-26-delivery-contract-first-playbook.md` и обязателен для следующих вех.
- Детализация по правилам и текущему покрытию — в:
  - `docs/status/2026-02-26-business-logic-v1-implementation-status.md`
  - `docs/status/2026-02-26-project-math-model.md`
