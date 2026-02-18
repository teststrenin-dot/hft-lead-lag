# Product Backlog

## Приоритеты

| Priority | Label | Description |
|----------|-------|-------------|
| P0 | Critical | Блокирует релиз, критично для безопасности |
| P1 | High | Важно для следующего спринта |
| P2 | Medium | Важно, но может подождать |
| P3 | Low | Nice to have, оптимизации |

---

## Эпик: Exchange Connectivity

### P0 — Критично для MVP

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| CONN-001 | Binance WS подключение | Подключение к fapi.binance.com WS | ✅ Done |
| CONN-002 | Gate WS подключение | Подключение к fx-ws.gateio.ws WS | ✅ Done |
| CONN-003 | Binance book ticker | Парсинг @bookTicker stream | ✅ Done |
| CONN-004 | Gate book ticker | Парсинг futures.book_ticker | ✅ Done |
| CONN-005 | Binance trades | Парсинг @trade stream | ⬜ Todo |
| CONN-006 | Gate trades | Парсинг futures.trades | ⬜ Todo |
| CONN-007 | Gate auth | futures.login аутентификация | ✅ Done |
| CONN-008 | Reconnect logic | Auto-reconnect с exponential backoff | ⬜ Todo |

### P1 — Важно для следующего спринта

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| CONN-009 | Binance user data | Listen key + order updates | ⬜ Todo |
| CONN-010 | Gate order updates | Order status через WS | ⬜ Todo |
| CONN-011 | Rate limiting | Client-side rate limiter | ⬜ Todo |
| CONN-012 | Health checks | Connection monitoring | ⬜ Todo |

### P2 — Средний приоритет

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| CONN-013 | Bybit connector | Добавить Bybit Futures | ⬜ Todo |
| CONN-014 | OKX connector | Добавить OKX Futures | ⬜ Todo |
| CONN-015 | WebSocket compression | gzip для экономии трафика | ⬜ Todo |

---

## Эпик: Order Management

### P0 — Критично для MVP

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| ORD-001 | Binance place order | REST API для размещения | ⬜ Todo |
| ORD-002 | Gate place order | REST API для размещения | ⬜ Todo |
| ORD-003 | Binance cancel order | Отмена ордера | ⬜ Todo |
| ORD-004 | Gate cancel order | Отмена ордера | ⬜ Todo |
| ORD-005 | Order tracking | Отслеживание статуса | ⬜ Todo |

### P1 — Важно для следующего спринта

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| ORD-006 | WS order placement | Размещение через WebSocket | ⬜ Todo |
| ORD-007 | Batch cancel | Отмена всех ордеров по символу | ⬜ Todo |
| ORD-008 | Order validation | Pre-trade risk checks | ⬜ Todo |

### P2 — Средний приоритет

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| ORD-009 | IOC/FOK orders | Time-in-force поддержка | ⬜ Todo |
| ORD-010 | Stop orders | Stop-loss/take-profit | ⬜ Todo |

---

## Эпик: Lead-Lag Strategy

### P0 — Критично для MVP

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| STRAT-001 | Spread calculation | Расчет spread между биржами | ✅ Done |
| STRAT-002 | Signal generation | Генерация lead-lag сигналов | ✅ Done |
| STRAT-003 | Threshold config | Настройка trigger spread | ✅ Done |

### P1 — Важно для следующего спринта

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| STRAT-004 | Position management | Lifecycle management | ⬜ Todo |
| STRAT-005 | Exit logic | Закрытие позиций по target spread | ⬜ Todo |
| STRAT-006 | Multi-symbol | Параллельная торговля по символам | ⬜ Todo |

### P2 — Средний приоритет

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| STRAT-007 | Dynamic thresholds | Адаптивные пороги | ⬜ Todo |
| STRAT-008 | ML signal filter | ML-based signal quality | ⬜ Todo |

---

## Эпик: Risk Management

### P0 — Критично для MVP

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| RISK-001 | Position limits | Максимальная экспозиция | ✅ Done |
| RISK-002 | Daily loss limit | Лимит дневных потерь | ✅ Done |
| RISK-003 | Pre-trade check | Проверка перед ордером | ✅ Done |

### P1 — Важно для следующего спринта

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| RISK-004 | Real-time PnL | Мониторинг PnL в реальном времени | ⬜ Todo |
| RISK-005 | Circuit breaker | Auto-stop при аномалиях | ⬜ Todo |
| RISK-006 | Margin monitoring | Отслеживание маржи | ⬜ Todo |

### P2 — Средний приоритет

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| RISK-007 | Correlation risk | Лимиты на correlated symbols | ⬜ Todo |
| RISK-008 | VaR calculation | Value at Risk | ⬜ Todo |

---

## Эпик: Infrastructure

### P0 — Критично для MVP

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| INFRA-001 | Logging | Structured logging с tracing | ✅ Done |
| INFRA-002 | Config management | Загрузка из TOML + env | ✅ Done |
| INFRA-003 | Error handling | Типизированные ошибки | ✅ Done |

### P1 — Важно для следующего спринта

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| INFRA-004 | Metrics | Prometheus metrics | ⬜ Todo |
| INFRA-005 | Alerting | Alertmanager integration | ⬜ Todo |
| INFRA-006 | Data recording | Запись тиков в data lake | ⬜ Todo |

### P2 — Средний приоритет

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| INFRA-007 | Dashboard | Grafana dashboard | ⬜ Todo |
| INFRA-008 | Replay system | Replay historical data | ⬜ Todo |

---

## Эпик: Testing & Quality

### P0 — Критично для MVP

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| TEST-001 | Unit tests | Покрытие > 80% | ✅ Done (14 tests) |
| TEST-002 | Integration tests | Тесты с реальными WS | ⬜ Todo |

### P1 — Важно для следующего спринта

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| TEST-003 | Load tests | Нагрузочное тестирование | ⬜ Todo |
| TEST-004 | Chaos testing | Fault injection | ⬜ Todo |

### P2 — Средний приоритет

| ID | Задача | Описание | Статус |
|----|--------|----------|--------|
| TEST-005 | Benchmark suite | Performance benchmarks | ⬜ Todo |
| TEST-006 | CI/CD pipeline | GitHub Actions | ⬜ Todo |

---

## Бэклог идей (P3)

| ID | Идея | Описание |
|----|------|----------|
| IDEA-001 | Smart order routing | Маршрутизация по лучшей цене |
| IDEA-002 | Inventory management | Балансировка позиций |
| IDEA-003 | Cross-exchange arb | Треугольный арбитраж |
| IDEA-004 | Market making | Добавление ликвидности |
| IDEA-005 | Statistical arb | Коинтеграция пар |

---

## Метрики бэклога

- **Total items**: 50+
- **P0 items**: 18
- **Done**: 14
- **Ready for sprint**: 20+

---

*Last updated: 2026-02-18*
