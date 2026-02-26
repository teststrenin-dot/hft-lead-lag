# Business Logic v1 — Implementation Status Tracker

Date: 2026-02-26
Source spec: `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md`

## Status Legend
- `Implemented` — реализовано и используется в runtime.
- `Partial` — реализовано частично, есть зафиксированный gap/риск.
- `Planned` — зафиксировано как следующий шаг, в коде не реализовано.
- `Out of Scope (v1)` — намеренно не делаем в v1.

## 1) Objective
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Shadow fleet -> candidate gates -> portfolio transfer | `Partial` | `src/domain/screener/quote_ingest.rs:90`, `src/domain/screener/mod.rs:376` | Пайплайн есть, но есть restart gap по накопленной истории кандидатов. |

## 2) Operating Model
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Shadow fleet торгует и копит статистику | `Implemented` | `src/domain/screener/quote_ingest.rs:68`, `src/domain/screener/mod.rs:330` | Закрытые сделки попадают в накопители и guard-логику. |
| Монеты становятся кандидатами после gate | `Implemented` | `src/application/services/portfolio_runtime.rs:54`, `src/application/services/portfolio_runtime.rs:250` | `eligible()` + shortlist build. |
| Портфели строятся из eligible-кандидатов | `Implemented` | `src/application/services/portfolio_runtime.rs:94` | Assignment + no-overlap. |
| Гонка портфелей пока для аналитики | `Implemented` | `src/api/http_server.rs:145`, `src/api/handlers.rs:234` | API read-model есть, денежного ребаланса нет (по плану v2). |

## 3) Portfolio Topology
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Ровно 2 портфеля (`A`, `B`) | `Implemented` | `src/application/services/portfolio_runtime.rs:10`, `src/domain/screener/mod.rs:276` | Слоты A/B всегда присутствуют. |
| 1 портфель = 1 бот | `Partial` | `src/application/services/portfolio_runtime.rs:10` | Логическая модель есть, явной runtime-привязки «бот-процесс на портфель» нет. |
| Размер портфеля 0..4 | `Implemented` | `src/application/services/portfolio_runtime.rs:5`, `src/application/services/portfolio_runtime.rs:156` | `MAX_ACTIVE_SYMBOLS=4`, допускается 0. |

## 4) Core Metric Definitions
| Item | Status | Evidence | Notes |
|---|---|---|---|
| `useful_winrate = profitable / total` | `Implemented` | `src/application/services/portfolio_runtime.rs:43` | `profitable` = `pnl > 0`. |
| Min useful winrate >= 30% | `Implemented` | `src/application/services/portfolio_runtime.rs:57` | Проверяется в `eligible`. |
| `pm_raw = profitable - losing` (`losing` = `pnl < 0`) | `Implemented` | `src/application/services/portfolio_runtime.rs:50`, `src/domain/screener/mod.rs:337` | Счётчики `profitable/losing` ведутся корректно. |
| Приоритет ранжирования: useful_winrate, pm_raw, avg_pnl_pct, closed_trades | `Implemented` | `src/application/services/portfolio_runtime.rs:75` | Реализован tuple comparator. |

## 5) Symbol Transfer Rules (Fleet -> Portfolio)
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Age > 5 min | `Implemented` | `src/application/services/portfolio_runtime.rs:55`, `src/domain/screener/mod.rs:418` | Возраст от `first_tick_ms`. |
| Closed trades > 5 | `Implemented` | `src/application/services/portfolio_runtime.rs:56` | В `eligible`. |
| Метрики по всему флоту | `Implemented` | `src/domain/screener/mod.rs:408` | Глобальная агрегация по всем символам в accumulators. |
| Полная история (без rolling window) | `Partial` | `src/domain/screener/mod.rs:127`, `src/runtime_setup.rs:181` | Полная история в рамках uptime, но не восстанавливается в `trade_accumulators` после рестарта. |
| avg pnl >= 0 | `Implemented` | `src/application/services/portfolio_runtime.rs:58` | В `eligible`. |
| shortlist top-5 per portfolio | `Implemented` | `src/application/services/portfolio_runtime.rs:4`, `src/application/services/portfolio_runtime.rs:255` | `SHORTLIST_SIZE=5`. |
| Нет overlap между активными символами портфелей | `Implemented` | `src/application/services/portfolio_runtime.rs:94` | Ownership map + split active sets. |
| Конфликт по «лучшей» метрике | `Implemented` | `src/application/services/portfolio_runtime.rs:118` | При строгом превосходстве — победитель по tuple; при полном равенстве добавлен балансирующий tie-break. |

## 6) Eject / Reset Rules
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Eject только по stop-loss streak trigger | `Implemented` | `src/application/services/portfolio_runtime.rs:196` | Негативные не-stop-loss не триггерят reset. |
| Streak per symbol, reset по прибыльной сделке | `Implemented` | `src/application/services/portfolio_runtime.rs:188`, `src/application/services/portfolio_runtime.rs:190` | `pnl > 0` обнуляет streak. |
| Hard reset symbol-level only | `Implemented` | `src/application/services/portfolio_runtime.rs:210` | Выставляется cooldown только для символа. |
| Trigger: 5 stop-loss <=2m или 6-й persistent | `Implemented` | `src/application/services/portfolio_runtime.rs:206`, `src/application/services/portfolio_runtime.rs:208` | Соответствует зафиксированным правилам v1. |

## 7) Re-Entry Rules
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Cooldown >= 5 min | `Implemented` | `src/application/services/portfolio_runtime.rs:7`, `src/application/services/portfolio_runtime.rs:211` | `COOLDOWN_MS=300_000`. |
| Возврат в общий котёл + повторная `eligible` проверка | `Implemented` | `src/application/services/portfolio_runtime.rs:220`, `src/application/services/portfolio_runtime.rs:253` | `can_reenter()` + фильтр shortlist. |

## 8) Rebalance Cadence
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Ребаланс каждые 2 минуты | `Partial` | `src/domain/screener/mod.rs:49`, `src/domain/screener/mod.rs:384`, `src/domain/screener/quote_ingest.rs:106` | Есть 2-минутный gate, но вызов event-driven на тиках, а не отдельный background таймер. |

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
1. `P1`: restart durability for candidate history (`trade_accumulators` restore path отсутствует).
2. `P1`: flush/snapshot ordering & durability semantics for portfolio snapshot writes.
3. `P2`: rebalance invocation model (event-driven hot path vs dedicated 2-minute loop).

## Tracking Update Rule
- Обновлять этот файл после каждого раунда ревью и после каждого фикса `P0/P1`.
- Для изменения статуса пункта обязательно указывать код-референс в колонке `Evidence`.
