# Sprint 2: Order Management

**Даты**: 2026-02-25 — 2026-03-04 (1 неделя)

**Цель**: Полноценное управление ордерами — размещение, отмена, отслеживание.

---

## Sprint Goals

1. ⬜ Размещение ордеров на Binance через REST
2. ⬜ Размещение ордеров на Gate через REST
3. ⬜ Отмена ордеров на обеих биржах
4. ⬜ Отслеживание статуса ордеров
5. ⬜ Базовый position tracking

---

## Задачи спринта

### P0 — Критично

| ID | Задача | Описание | Оценка | Статус |
|----|--------|----------|--------|--------|
| ORD-001 | Binance place order | REST API для размещения | 5 SP | ⬜ Todo |
| ORD-002 | Gate place order | REST API для размещения | 5 SP | ⬜ Todo |
| ORD-003 | Binance cancel order | Отмена ордера | 3 SP | ⬜ Todo |
| ORD-004 | Gate cancel order | Отмена ордера | 3 SP | ⬜ Todo |
| ORD-005 | Order tracking | Отслеживание статуса | 5 SP | ⬜ Todo |

### P1 — Важно

| ID | Задача | Описание | Оценка | Статус |
|----|--------|----------|--------|--------|
| ORD-006 | WS order placement | Размещение через WebSocket | 8 SP | ⬜ Todo |
| ORD-007 | Batch cancel | Отмена всех ордеров по символу | 3 SP | ⬜ Todo |
| RISK-004 | Real-time PnL | Мониторинг PnL в реальном времени | 5 SP | ⬜ Todo |
| RISK-005 | Circuit breaker | Auto-stop при аномалиях | 5 SP | ⬜ Todo |

### P2 — Средний приоритет

| ID | Задача | Описание | Оценка | Статус |
|----|--------|----------|--------|--------|
| ORD-008 | Order validation | Pre-trade risk checks | 3 SP | ⬜ Todo |
| CONN-009 | Binance user data | Listen key + order updates | 5 SP | ⬜ Todo |
| CONN-010 | Gate order updates | Order status через WS | 5 SP | ⬜ Todo |

---

## Definition of Done

- [ ] Код написан
- [ ] Тесты написаны и проходят
- [ ] Интеграционные тесты с testnet
- [ ] Документация обновлена
- [ ] Code review пройдено

---

## Зависимости

### Блокеры
- API ключи с правами на торговлю
- Доступ к Binance testnet
- Доступ к Gate testnet

### Внешние зависимости
- Binance API rate limits: 1200 requests/minute
- Gate API rate limits: 100 requests/second

---

## Риски

| Риск | Вероятность | Влияние | Митигация |
|------|-------------|---------|-----------|
| Ошибки в order logic | Средняя | Высокое | Тщательное тестирование на testnet |
| Rate limiting | Высокая | Среднее | Client-side rate limiter |
| Потеря соединений | Средняя | Высокое | Reconnect logic + retry |

---

## Метрики успеха

- **Order placement latency** < 100ms
- **Order cancellation latency** < 50ms
- **Test coverage** > 75%
- **Zero accidental trades** на production

---

## Plan по тестированию

### Unit Tests
- [ ] Тесты на валидацию ордеров
- [ ] Тесты на расчет комиссий
- [ ] Тесты на position tracking

### Integration Tests
- [ ] Размещение ордера на testnet
- [ ] Отмена ордера на testnet
- [ ] Проверка статуса ордера

### Load Tests
- [ ] 100 orders/second
- [ ] 1000 cancellations/second

---

*Sprint planned: 2026-02-18*
