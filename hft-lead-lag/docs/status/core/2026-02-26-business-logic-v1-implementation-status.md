# Business Logic v1 — Implementation Status Tracker

Date: 2026-02-26
Last sync: commits up to `ad041ca`
Source spec: `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md`
Strategic anchor: `docs/status/core/2026-02-27-business-objective-economic-control-map.md`

## Strategic Objective Snapshot
Locked objective for all CP4+ work:
1. Maximize risk-adjusted return under constrained capital.
2. Keep deterministic control flow `Signal -> Validation -> Competition -> Risk -> Capital -> Feedback`.
3. Reach checkpoint-ready paper operation before capital rebalance/live.

## Economic Node Coverage Snapshot
| Economic node | CP owner | Coverage now | Notes |
|---|---|---|---|
| `Signal` | `CP1-CP2` | `Implemented` | Lead-lag ingestion/signal lifecycle in runtime. |
| `Validation` | `CP3` | `Implemented` | Eligibility + ranking gates are active. |
| `Competition` | `CP4` (+`CP4.1`) | `Partial` | Runtime race exists; operator UI (`/portfolio`) реализован, остаётся winner-promotion path. |
| `Risk` | `CP4-CP5` | `Partial` | Guard/reset/cooldown active; full restart hardening still ongoing. |
| `Feedback` | `CP5-CP6` | `Partial` | API/health present; operational UX and incident paths are in progress. |
| `Capital` | `CP7` | `Planned` | Rebalance/live gates not enabled yet. |

## Status Legend
- `Implemented` — реализовано и используется в runtime.
- `Partial` — реализовано частично, есть зафиксированный gap/риск.
- `Planned` — зафиксировано как следующий шаг, в коде не реализовано.
- `Out of Scope (v1)` — намеренно не делаем в v1.

## 1) Objective
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Shadow fleet -> candidate gates -> portfolio transfer | `Implemented` | `src/domain/screener/quote_ingest.rs:93`, `src/domain/screener/mod.rs:331`, `src/runtime_setup.rs:181` | Пайплайн и восстановление candidate-history после рестарта в runtime реализованы. |

## 2) Operating Model
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Shadow fleet торгует и копит статистику | `Implemented` | `src/domain/screener/quote_ingest.rs:68`, `src/domain/screener/mod.rs:330` | Закрытые сделки попадают в накопители и guard-логику. |
| Монеты становятся кандидатами после gate | `Implemented` | `src/domain/screener/portfolio_runtime.rs:54`, `src/domain/screener/portfolio_runtime.rs:250` | `eligible()` + shortlist build. |
| Портфели строятся из eligible-кандидатов | `Implemented` | `src/domain/screener/portfolio_runtime.rs:94` | Assignment + no-overlap. |
| Гонка портфелей пока для аналитики | `Implemented` | `src/api/http_server.rs:145`, `src/api/handlers.rs:234` | API read-model есть, денежного ребаланса нет (по плану v2). |
| Operator UI для гонки портфелей (`/portfolio`) | `Implemented` | `src/api/http_server.rs:126`, `src/api/templates.rs:24`, `src/api/templates/portfolio.html:1` | Показывает active/candidates/performance/guards с автообновлением. |
| Startup command-gating для trial hot-reload (`CP4.2`) | `Implemented` | `src/runtime_hot_reload.rs:41`, `src/runtime_hot_reload.rs:272`, `src/runtime_hot_reload.rs:303` | Stale `trial-batch`/`trial-control` и startup queue-пейлоады больше не исполняются автоматически на старте; нужна явная новая команда (изменение файла/новый queue submit). |
| Bounded forward-run limits (`CP4.3`) | `Implemented` | `ray_driver/cli.py:176`, `ray_driver/cli.py:245`, `ray_driver/expand.py:17`, `src/api/runner/command.rs:154`, `src/api/templates/trials.html:129` | Forward теперь всегда ограничен по `max_refs/max_configs`; это устраняет unbounded batch/OOM-паттерн на малом сервере. |
| Forward fairness + hard caps (`CP4.4`) | `Implemented` | `ray_driver/expand.py:13`, `ray_driver/cli.py:12`, `ray_driver/tests/test_forward_limits.py:52`, `src/api/runner/command.rs:3`, `src/api/templates/trials.html:126` | При capped-forward конфиги заполняются round-robin по refs (без first-ref bias); лимиты дополнительно ограничены safe cap (`max_refs<=256`, `max_configs<=5000`) в CLI/Runner/UI. |
| ASHA trial granularity (`CP4.5`) | `Implemented` | `ray_driver/cli.py:213`, `ray_driver/trainable.py:11`, `ray_driver/config_store.py:78`, `ray_driver/tests/test_trainable.py:21` | Forward запускает единый runtime batch и затем оценивает каждый `config_id` отдельным Ray trial (`grid_search`), что убирает batch-агрегацию метрик внутри ASHA. |
| Runtime auto-prune during forward (`CP4.6`) | `Implemented` | `ray_driver/cli.py:19`, `ray_driver/cli.py:291`, `ray_driver/ipc.py:42`, `ray_driver/tests/test_forward_limits.py:247` | Ранние `ASHA STOP` автоматически собираются в batched incremental patch и удаляются из runtime active set, снижая нагрузку прямо в процессе forward-run. |
| CP4.6 review remediation (`CP4.6-R1`) | `Implemented` | `src/domain/screener/fleet_reload.rs:130`, `src/domain/screener/tests.rs:569`, `ray_driver/cli.py:383`, `ray_driver/tests/test_forward_limits.py:133` | Исправлены два P1 из review: incremental prune больше не отклоняется для “untouched” config_id, и forward гарантированно очищает `run_id` lease после завершения/ошибки. |

## 3) Portfolio Topology
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Динамический набор портфелей (`PORTFOLIO_IDS`, fallback на `A/B`) | `Implemented` | `src/main.rs:126`, `src/main.rs:180`, `src/domain/screener/portfolio_runtime.rs:57`, `src/domain/screener/mod.rs:321` | Количество и имена портфелей задаются через env; при пустом/некорректном вводе используется дефолт `A/B`. |
| 1 портфель = 1 бот | `Partial` | `src/domain/screener/portfolio_runtime.rs:10` | Логическая модель есть, явной runtime-привязки «бот-процесс на портфель» нет. |
| Размер портфеля 0..4 | `Implemented` | `src/domain/screener/portfolio_runtime.rs:5`, `src/domain/screener/portfolio_runtime.rs:156` | `MAX_ACTIVE_SYMBOLS=4`, допускается 0. |

## 4) Core Metric Definitions
| Item | Status | Evidence | Notes |
|---|---|---|---|
| `useful_winrate = profitable / total` | `Implemented` | `src/domain/screener/portfolio_runtime.rs:43` | `profitable` = `pnl > 0`. |
| Min useful winrate >= 30% | `Implemented` | `src/domain/screener/portfolio_runtime.rs:57` | Проверяется в `eligible`. |
| `pm_raw = profitable - losing` (`losing` = `pnl < 0`) | `Implemented` | `src/domain/screener/portfolio_runtime.rs:50`, `src/domain/screener/mod.rs:337` | Счётчики `profitable/losing` ведутся корректно. |
| Приоритет ранжирования: useful_winrate, pm_raw, avg_pnl_pct, closed_trades | `Implemented` | `src/domain/screener/portfolio_runtime.rs:75` | Реализован tuple comparator. |

## 5) Symbol Transfer Rules (Fleet -> Portfolio)
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Age > 5 min | `Implemented` | `src/domain/screener/portfolio_runtime.rs:55`, `src/domain/screener/mod.rs:418` | Возраст от `first_tick_ms`. |
| Closed trades > 5 | `Implemented` | `src/domain/screener/portfolio_runtime.rs:56` | В `eligible`. |
| Метрики по всему флоту | `Implemented` | `src/domain/screener/mod.rs:408` | Глобальная агрегация по всем символам в accumulators. |
| Полная история (без rolling window) | `Implemented` | `src/infrastructure/db.rs:731`, `src/domain/screener/mod.rs:666`, `src/runtime_setup.rs:236` | История агрегируется и восстанавливается event-level collapse-правилом `(symbol, exit_ts_ms)`; при активном `run_id` учитывается только текущий scope. |
| avg pnl >= 0 | `Implemented` | `src/domain/screener/portfolio_runtime.rs:58` | В `eligible`. |
| shortlist top-5 per portfolio | `Implemented` | `src/domain/screener/portfolio_runtime.rs:4`, `src/domain/screener/portfolio_runtime.rs:142`, `src/domain/screener/portfolio_runtime.rs:249` | Shortlist строятся независимо по портфелям через round-robin распределение глобального ranked pool; при дефиците символов shortlist заполняются частично. |
| Нет overlap между активными символами портфелей | `Implemented` | `src/domain/screener/portfolio_runtime.rs:143`, `src/domain/screener/portfolio_runtime.rs:287` | Активные символы раскладываются по портфелям без пересечений. |
| Конфликт по «лучшей» метрике | `Implemented` | `src/domain/screener/portfolio_runtime.rs:70`, `src/domain/screener/portfolio_runtime.rs:101` | Победитель определяется tuple-компаратором; при равном rank символы распределяются по портфелям через баланс по заполненности. |

## 6) Eject / Reset Rules
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Eject только по stop-loss streak trigger | `Implemented` | `src/domain/screener/portfolio_runtime.rs:196` | Негативные не-stop-loss не триггерят reset. |
| Streak per symbol, reset по прибыльной сделке | `Implemented` | `src/domain/screener/portfolio_runtime.rs:188`, `src/domain/screener/portfolio_runtime.rs:190` | `pnl > 0` обнуляет streak. |
| Hard reset symbol-level only | `Implemented` | `src/domain/screener/portfolio_runtime.rs:210` | Выставляется cooldown только для символа. |
| Trigger: 5 stop-loss <=2m или 6-й persistent | `Implemented` | `src/domain/screener/portfolio_runtime.rs:206`, `src/domain/screener/portfolio_runtime.rs:208` | Соответствует зафиксированным правилам v1. |

## 7) Re-Entry Rules
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Cooldown >= 5 min | `Implemented` | `src/domain/screener/portfolio_runtime.rs:7`, `src/domain/screener/portfolio_runtime.rs:211` | `COOLDOWN_MS=300_000`. |
| Возврат в общий котёл + повторная `eligible` проверка | `Implemented` | `src/domain/screener/portfolio_runtime.rs:220`, `src/domain/screener/portfolio_runtime.rs:253` | `can_reenter()` + фильтр shortlist. |

## 8) Rebalance Cadence
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Ребаланс каждые 2 минуты | `Implemented` | `src/event_loop_runtime.rs:8`, `src/event_loop_runtime.rs:106`, `src/domain/screener/mod.rs:468` | Вынесено в отдельный scheduler (`tokio::interval`) с внутренним cadence-gate в screener. |

## 9) Portfolio Race Policy
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Сейчас — статистика/аналитика | `Implemented` | `src/api/http_server.rs:145`, `src/api/handlers.rs:275` | API-метрики и state доступны. |
| В будущем — денежный ребаланс | `Planned` | `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md` | Не реализовано в текущем runtime. |

## 10) Scope Notes
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Dynamic hyperparameters policy | `Out of Scope (v1)` | `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md` | Намеренно отложено на v2. |

## Current Open Gaps (Priority)
1. `P2`: явная runtime-связка `1 portfolio = 1 bot process` пока не реализована.
2. `P2`: портфельная гонка пока аналитическая (нет money-rebalance/auto-promote winner path).
3. `P3`: dynamic hyperparameters policy для нормализации к режиму рынка отложена (v2).

## Recently Closed Hotfixes
1. `P0` (`CP4.2`): устранён автозапуск stale команд при старте runtime (`trial-batch`/`trial-control`).
2. `P0` (`CP4.3`): forward ограничен по масштабу (`max_refs/max_configs`) в CLI и Runner defaults для защиты от OOM.
3. `P1` (`CP4.4`): убран first-ref bias в capped-forward (round-robin across refs) и добавлены hard-cap guardrails на `max_refs/max_configs`.
4. `P1` (`CP4.5`): forward переведён на модель `1 config = 1 trial` в ASHA (пер-config метрики вместо batch-агрегата).
5. `P1` (`CP4.6`): ранние ASHA-stop теперь автоматически prun-ятся в runtime через incremental patch.
6. `P1` (`CP4.6-R1`): закрыты review-баги incremental prune matching и stale run lease cleanup.

## Что Нужно Сделать Для 100% Бизнес-Логики
1. Реализовать явный runtime слой `portfolio -> bot` (изолированный execution loop, health, restart policy по каждому портфелю).
2. Доделать переход «гонка -> действие»: winner selection и автоматический маршрут в execution mode (сейчас это только read-model/аналитика).
3. Добавить money-rebalance policy (allocation, лимиты риска, handoff между портфелями), сейчас это `Planned`.
4. Включить dynamic hyperparameters (adaptive thresholds/guards от распределений, а не от абсолютов), чтобы система была устойчива к regime shift.
5. Закрыть это e2e-проверкой в runbook: restart, cooldown-resets, winner-promotion, и replay на исторических данных с KPI-acceptance.

## Tracking Update Rule
- Обновлять этот файл после каждого раунда ревью и после каждого фикса `P0/P1`.
- Для изменения статуса пункта обязательно указывать код-референс в колонке `Evidence`.

## Business Logic Roadmap
- Чекпоинты реализации вынесены в отдельный документ:
  - `docs/status/core/2026-02-26-business-logic-roadmap.md`
