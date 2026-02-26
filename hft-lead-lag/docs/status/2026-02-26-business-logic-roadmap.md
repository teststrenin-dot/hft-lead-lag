# Business Logic v1 — Roadmap

Date: 2026-02-26
Scope: бизнес-логика и дорожная карта реализации до 100%, разбитая на тестируемые вехи.

## Checkpoint 0 — Baseline Lock (done)
**Goal:** зафиксировать текущее рабочее ядро и прозрачный статус.

- Shadow fleet работает и копит статистику.
- Portfolio runtime работает (2m scheduler, cooldown/guards, API read-model).
- UI видит состояние портфелей через backend API.
- Математика и статус реализации зафиксированы в `docs/status`.

**Acceptance:**
- `/health` = `ok`, exchange feeds alive.
- `/api/v1/portfolio/active` отдаёт валидные состояния `A/B`.
- Документы статуса и математики соответствуют коду.

## Checkpoint 1 — Race-Ready Portfolios (major)
**Goal:** получить целевой промежуточный режим: теневой флот + соревнующиеся портфели, без денежного ребаланса.

- Теневой флот стабильно работает и формирует кандидатов.
- Портфели работают и видны в UI.
- Логика смены денежного баланса **не внедрена** (осознанно).
- Количество портфелей регулируется конфигом (не hardcoded `A/B`).
- Соревнование монет работает: независимые shortlist/ranking по портфелям, без overlap активных символов.

**Acceptance:**
- Число портфелей меняется конфигом без перекомпиляции.
- Каждый портфель имеет собственный shortlist и активный набор.
- UI отображает все портфели и их метрики.
- Система продолжает работать в shadow/paper без money rebalance.

## Estimate to Reach Checkpoint 1
- Оценка: `5–8 рабочих дней` (1 инженер, без новых scope-расширений).
- Текущая готовность: `~70–75%` относительно критериев `Checkpoint 1`.
- Основной оставшийся объём:
  - убрать hardcode `A/B` в runtime/API/tests;
  - сделать независимые shortlist/ranking per portfolio вместо общего shortlist.

Assumptions:
- не добавляются новые бизнес-требования в процессе;
- инфраструктура и текущий feed/runtime остаются стабильными;
- фокус только на достижении `Checkpoint 1` без включения live/money rebalance.

## Intermediate Milestones to Checkpoint 1
### CP1.1 — Dynamic Portfolio Count (backend contract)
- ETA: `1.5–2.5 дня`.
- Scope:
  - конфигурируемое количество портфелей (без hardcoded `A/B`);
  - адаптация runtime state и API-контрактов к динамическому набору портфелей;
  - совместимость с persistence snapshot.
- Exit criteria:
  - число портфелей меняется конфигом без перекомпиляции;
  - `/api/v1/portfolio/active` корректно возвращает весь список портфелей.

### CP1.2 — Independent Coin Race per Portfolio
- ETA: `2–3 дня`.
- Scope:
  - независимые shortlist/ranking для каждого портфеля;
  - сохранение правила no-overlap для активных символов.
- Exit criteria:
  - каждый портфель формирует свой shortlist;
  - активные символы не пересекаются между портфелями.

### CP1.3 — UI & Endpoint Adaptation
- ETA: `0.5–1 дня`.
- Scope:
  - UI отображает произвольное число портфелей;
  - выравнивание фронт/бэк контрактов по метрикам и состояниям.
- Exit criteria:
  - все портфели видны в UI;
  - shortlist/active/guards читаются без ручных фиксов.

### CP1.4 — Stabilization & Regression Sweep
- ETA: `1–1.5 дня`.
- Scope:
  - тесты, e2e smoke и перф-smoke после изменений CP1.1–CP1.3;
  - проверка scheduler/cooldown/restore после рестарта.
- Exit criteria:
  - критических регрессий нет;
  - pipeline стабилен в shadow/paper режиме.

## Checkpoint 2 — Promotion & Bot Runtime
**Goal:** довести гонку до операционного режима исполнения (всё ещё без live денег).

- Явная связка `portfolio -> bot runtime` (1 портфель = 1 execution loop).
- Winner selection и auto-promote путь из race-аналитики в execution mode.
- Health/restart policy на уровне каждого портфельного бота.

**Acceptance:**
- Победитель гонки выбирается формально и воспроизводимо.
- Переключение winner не ломает shadow ingestion и метрики.
- По каждому портфелю есть отдельный health/state в API.

## Checkpoint 3 — Capital Rebalance + Live (final milestone)
**Goal:** последняя веха — реальный денежный ребаланс и подключение live-торговли.

- Политика allocation/reallocation между портфелями.
- Лимиты риска и kill-switch для live execution.
- Контур live-исполнения поверх проверенного paper/shadow pipeline.

**Acceptance:**
- Детальная money-rebalance policy применима автоматически.
- Live включается только при пройденных safety-guards.
- Есть runbook rollback/disable до уровня портфеля и символа.

## Work Queue (from current status)
P1:
- Независимые shortlist per portfolio.
- Конфигурируемое количество портфелей.

P2:
- Явная runtime-связка `portfolio -> bot`.
- Winner promotion path (analytics -> execution).

P3:
- Dynamic hyperparameters (v2).
- Money rebalance + live hardening.

## Notes
- Этот документ — дорожная карта delivery; фактический прогресс должен синхронизироваться с `2026-02-26-business-logic-v1-implementation-status.md`.
