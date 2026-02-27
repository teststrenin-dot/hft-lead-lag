# Business Logic v1 — 8 Checkpoints Roadmap

Date: 2026-02-26  
Last sync: commits up to `a03aec4`

Scope: верхнеуровневая дорожная карта из 8 чекпоинтов.  
Принцип: ревью выполняется **по одному большому чекпоинту**, но с детальным разбором подпунктов.

## Strategic Anchor (Mandatory)
Canonical source:
- `docs/status/core/2026-02-27-business-objective-economic-control-map.md`

Locked objective:
- maximize risk-adjusted return under constrained capital through controlled path `Signal -> Validation -> Competition -> Risk -> Capital -> Feedback`.

Checkpoint ownership over economic nodes:
1. `CP1-CP2`: `Signal`
2. `CP3`: `Validation`
3. `CP4` (+`CP4.1`): `Competition`
4. `CP4-CP5`: `Risk`
5. `CP5-CP6`: `Feedback`
6. `CP7`: `Capital`

## Checkpoint Status
| Checkpoint | Status | Notes |
|---|---|---|
| `CP0` Контракт и границы системы | `Done` | Outcome/DoD/scope и contract-first подход зафиксированы. |
| `CP1` Рыночные данные и время | `Done` | Ingest, clock offsets, drift и базовые time-domain гарантии в коде и тестах. |
| `CP2` Lead-Lag и shadow execution | `Done` | Сигналы, shadow lifecycle, stop/exit-ветки и базовая устойчивость закрыты. |
| `CP3` Математика кандидатов | `Done` | Eligibility/ranking/pm_raw/useful_winrate в рабочем контуре. |
| `CP4` Портфельная гонка (paper runtime) | `In Progress` | Assignment/paper-state работает; `CP4.1` (operator UI `/portfolio`), `CP4.2` (startup command-gating hotfix), `CP4.3` (bounded forward hotfix), `CP4.4` (forward fairness + hard guardrails), `CP4.5` (`1 config = 1 ASHA trial`) и `CP4.6` (runtime auto-prune ранних ASHA-stop) реализованы, остаются winner-promotion и runtime-isolation задачи. |
| `CP5` Надёжность состояния | `In Progress` | Persistence/restore усилены, нужны доп. e2e-гарантии restart/recovery на уровне checkpoint. |
| `CP6` Контрольный слой и UI | `In Progress` | Базовые API/health/read-model есть; фокус на operational UX, алертах и incident-сценариях. |
| `CP7` Предзапуск к rebalance/live | `Planned` | Rebalance/live safety layer ещё не включены в runtime. |

## Checkpoint Breakdown (Subpoints)
### `CP0` Контракт и границы системы
1. Outcome и Definition of Done.
2. Ограничения scope (что явно не делаем в v1).
3. Контракты модулей и инварианты.

### `CP1` Рыночные данные и время
1. Подключение и нормализация котировок.
2. Exchange-time correction и drift-контроль.
3. Гарантии freshness/ordering.

### `CP2` Lead-Lag и shadow execution
1. Lead/lag signal correctness.
2. Shadow entry/exit lifecycle.
3. Stop-loss/timeout/trailing логика.

### `CP3` Математика кандидатов
1. Полная история сделок кандидатов.
2. Eligibility gates.
3. Ranking tuple и tie-break.

### `CP4` Портфельная гонка (paper runtime)
1. Shortlist/active assignment без overlap.
2. Атрибуция сделок и paper-money перфоманс по портфелям.
3. Winner logic и переход из аналитики в operational path.
4. `CP4.1` Операторский UI для гонки портфелей (active/candidates/performance/guards) как обязательный шаг тестируемости. `Done` (`/portfolio`).
5. `CP4.2` Hotfix управления запуском: runtime не исполняет stale `trial-batch`/`trial-control` и startup queue-пейлоады на старте без явного нового события. `Done`.
6. `CP4.3` Hotfix bounded-forward: ограничены `max_refs/max_configs`, чтобы forward не падал по OOM на рабочем сервере. `Done`.
7. `CP4.4` Hotfix forward fairness: `expand_around_references` при capped-forward распределяет конфиги round-robin по refs, а `max_refs/max_configs` дополнительно зажаты hard-cap guardrails в CLI/Runner/UI. `Done`.
8. `CP4.5` ASHA granularity refactor: forward запускает единый batch `run_id`, после чего Ray ведёт отдельный trial на каждый `config_id` (`1 config = 1 trial`) вместо агрегатного batch-trial. `Done`.
9. `CP4.6` Runtime auto-prune: при раннем `ASHA STOP` (до terminal rung) runtime получает batched incremental patch и удаляет stop-нутые config-ы из активного fleet, уменьшая нагрузку в ходе forward-run. `Done`.

### `CP5` Надёжность состояния
1. Persistence/restore runtime и paper-state.
2. Restart consistency и idempotency.
3. Поведение при частичных/устаревших snapshot.

### `CP6` Контрольный слой и UI (operational hardening)
1. API контракты (active/candidates/guards/performance).
2. Health/telemetry.
3. Operational UX и инцидентные сценарии на уже готовых read-model страницах.

### `CP7` Предзапуск к rebalance/live
1. Rebalance capital policy.
2. Safety gates (kill-switch, risk caps, rollback).
3. Readiness runbook и launch criteria.

## Review Workflow (One Round per Checkpoint)
Для каждого чекпоинта выполняется отдельный раунд ревью:
1. Коммиты и изменения в границах чекпоинта.
2. Баги и ошибки.
3. Архитектура и дизайн.
4. Логика и математика.
5. Дублирование, избыточность, переусложнение.
6. Когнитивная нагрузка и god objects.
7. Превентивная архитектура.
8. Dead code.
9. Отдельно дизайн screener.
10. Отдельно дизайн shadow fleet.

## Current Focus
Текущий рабочий фокус: `CP4` + `CP5` + `CP6` до состояния checkpoint-ready, затем переход к `CP7`.

## Backlog to Production (Checkpoint by Checkpoint)
Ниже полный расклад до прода: от текущего состояния до live.

### `CP0` Контракт и границы системы (`Done`, regression guard)
1. Freeze текущих контрактов API/runtime зафиксирован: `docs/status/dynamics/2026-02-27-cp0-contract-freeze-v2.md`.
2. Проверить, что новые изменения не расширяют scope без отдельного RFC.
3. Держать smoke-набор контрактных тестов как обязательный pre-merge gate.
Exit gate:
1. Нет drift между докой и runtime-контрактами.
2. Все контрактные тесты зелёные.

### `CP1` Рыночные данные и время (`Done`, regression guard)
1. Регулярно проверять exchange offset stability (алерт на большие скачки).
2. Валидировать freshness/order гарантии при рестартах и reconnect.
3. Поддерживать regression suite по time-domain edge cases.
Exit gate:
1. Нет деградации lead/lag метрик из-за тайм-домена.
2. Health сигнализирует о time/freshness проблемах до деградации стратегии.

### `CP2` Lead-Lag и shadow execution (`Done`, regression guard)
1. Стабилизировать сценарии long/short symmetry в интеграционных тестах.
2. Проверять корректность exit-reason и hold-time статистики.
3. Запретить изменения сигналов без пересчёта baseline отчётов.
Exit gate:
1. Все сценарные тесты signal->entry->exit зелёные.
2. Расхождение с baseline отчётами в допустимом коридоре.

### `CP3` Математика кандидатов (`Done`, regression guard)
1. Поддерживать корректность eligibility/ranking после новых фич.
2. Проверять отсутствие смещений в candidate history после миграций.
3. Держать детерминированность ранжирования при tie-break.
Exit gate:
1. Candidate math regression suite зелёный.
2. Результаты ранжирования воспроизводимы.

### `CP4` Портфельная гонка (paper runtime) (`In Progress`, active delivery)
1. Завершить winner-promotion path из аналитики в operational runtime.
2. Реализовать явную модель `1 portfolio = 1 bot runtime context` (paper).
3. Добавить контроль независимости портфелей (не мешают друг другу по lifecycle).
4. Закрыть e2e сценарии: накопление статистики -> отбор -> активная гонка.
5. `CP4.1` `Done`: добавлена UI-страница `/portfolio` на основе `/api/v1/portfolio/*`; оператор может тестировать гонку без ручных API-запросов.
6. `CP4.2` `Done` (`P0 hotfix`): startup watchers (`trial-batch`/`trial-control`) seed-ят текущий fingerprint, а stale queue-пейлоады архивируются; команды выполняются только после явного обновления/submit.
7. `CP4.3` `Done` (`P0 hotfix`): forward-run ограничен по `max_refs` и `max_configs` (CLI + runner defaults), чтобы исключить монолитный unbounded batch и OOM.
8. `CP4.4` `Done` (`P1 remediation`): capped-forward больше не вырождается в первый reference; добавлены round-robin распределение конфигов и hard-cap guardrails (`max_refs<=256`, `max_configs<=5000`) на всех входах.
9. `CP4.5` `Done` (`P1 architecture shift`): forward переведён на `1 config = 1 ASHA trial` через `grid_search(config_id)` на общем `run_id`.
10. `CP4.6` `Done` (`P1 performance shift`): ранние ASHA-stop автоматически prun-ятся из runtime (incremental patch), что снижает фактическое число активных конфигов во время прогона.
Exit gate:
1. В рантайме есть стабильный winner selection и promotion flow.
2. Портфели изолированы по execution-state.
3. Оператор видит гонку в UI (active/candidates/performance/guards) и может прогнать ручной smoke-test.
4. Review-раунд по `CP4` закрыт без open P0/P1.

### `CP5` Надёжность состояния (`In Progress`, active delivery)
1. Завершить restart/recovery сценарии для portfolio runtime + paper state.
2. Добавить проверку консистентности при out-of-order и частичных snapshot.
3. Укрепить idempotency для повторных применений snapshot/commands.
4. Добавить e2e тесты "restart under load" и "recovery after partial failure".
Exit gate:
1. После рестарта состояние полностью восстанавливается без потери бизнес-метрик.
2. Нет silent data loss по trade attribution/paper performance.
3. Review-раунд по `CP5` закрыт без open P0/P1.

### `CP6` Контрольный слой и UI (`In Progress`, active delivery)
1. Довести API read-model до полной связности по всем checkpoint-сущностям.
2. Доработать health/telemetry до operational сигнала (а не только debug).
3. Закрыть сценарные проверки UX: что оператор видит и что делает при инциденте.
Exit gate:
1. UI-операторка и алерты дают полный operational контур (без слепых зон).
2. API и UI контракты стабильны и покрыты тестами.
3. Review-раунд по `CP6` закрыт без open P0/P1.

### `CP7` Предзапуск к rebalance/live (`Planned`, final delivery to prod)
1. Ввести policy ребаланса денег между портфелями (allocation/reallocation).
2. Включить safety layer live: kill-switch, limits, circuit-breakers, rollback.
3. Реализовать staged rollout: shadow -> paper money -> constrained live.
4. Подготовить runbooks и incident playbooks для прод-эксплуатации.
5. Пройти финальный readiness review и go/no-go gate.
Exit gate (production ready):
1. Ребаланс и risk guards детерминированы и протестированы.
2. Есть подтверждённый rollback path и эксплуатационные runbooks.
3. Финальный checkpoint review (`CP7`) закрыт без open P0/P1.
4. Go-live approve.

## Delivery Sequence to Prod
1. Закрыть `CP4`.
2. Закрыть `CP5`.
3. Закрыть `CP6`.
4. Выполнить предпрод по `CP7` (paper money + safety + staged rollout).
5. Пройти финальный go/no-go ревью и вывести в live.

## Rule: One Checkpoint = One Full Review Round
До перехода к следующему чекпоинту текущий чекпоинт обязан пройти полный ревью-раунд по шаблону из раздела **Review Workflow**.

## Notes
Этот документ отражает только checkpoint-структуру и приоритеты.  
Детализация покрытия и математики:
- `docs/status/core/2026-02-26-business-logic-v1-implementation-status.md`
- `docs/status/dynamics/2026-02-26-project-math-model.md`
- `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
