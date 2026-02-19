# Multi-agent Review Report

**Date:** 2026-02-19  
**Repository:** `hft-lead-lag`  
**Method:** параллельное ревью несколькими сабагентами (commit bug review, architecture/design, math/stats, duplication, god objects)

---

## 1) Scope

Проверено:

- история коммитов и регрессионные риски,
- архитектура и дизайн модулей,
- математическая корректность метрик и порогов,
- дублирование кода/логики/конфигов,
- кандидаты на god objects.

---

## 2) Главные находки (P0/P1)

### P0

1. **WS reconnect path неполный (Binance/Gate):**
   после reconnect read-loop продолжает работу, но write-path не перекидывается полностью на новый сокет.
   - `src/infrastructure/exchanges/binance/mod.rs:213-216`
   - `src/infrastructure/exchanges/gate/mod.rs:245-282`

2. **Деструктивная legacy-миграция БД:**
   при обнаружении legacy-колонки выполняется `DROP TABLE trades/configs`.
   - `src/infrastructure/db.rs:73-87`

3. **Тихая потеря данных под нагрузкой:**
   `try_send` без обработки `Full` в market-data и drop batch в DB writer.
   - `src/infrastructure/exchanges/binance/mod.rs:170,173`
   - `src/infrastructure/exchanges/gate/mod.rs:210,213`
   - `src/infrastructure/db.rs:150-153`

### P1

4. **Подписка trade-stream Binance vs фильтр события:**
   подписка идет на `@aggTrade`, а `recv_trade` фильтрует `\"e\":\"trade\"`.
   - `src/infrastructure/exchanges/binance/mod.rs:96`
   - `src/infrastructure/exchanges/binance/mod.rs:388`

5. **Смешение слоев и высокая связанность:**
   `domain` использует инфраструктурный `DbWriter` напрямую.
   - `src/domain/screener/mod.rs:27,72-88,159-163`

6. **Перегруженный composition root:**
   `main.rs` совмещает bootstrap/runtime orchestration/event loop.
   - `src/main.rs`

---

## 3) Математика и статистика

Ключевые риски:

- **in-sample selection bias** (rank/selection и оценка на одном окне),
- **survivorship bias** в формировании universe,
- Sharpe считается на сделках без нормализации по времени,
- отсутствует измерение max drawdown в ранжировании.

Сильные стороны:

- консистентные единицы `bps/pct/ms`,
- защитные проверки от деления на ноль в критичных местах,
- фильтрация невалидных значений в части pipeline.

---

## 4) Дублирование и god objects

Основные дубли:

- зеркальная обработка Binance/Gate в `main.rs`,
- повторяющиеся SQL-агрегации в `src/api/handlers.rs`,
- дубли symbol normalization и hardcoded config/db-path значений.

Кандидаты на god objects/modules:

- `src/domain/screener/shadow_trader.rs`,
- `src/main.rs`,
- `src/infrastructure/exchanges/gate/mod.rs`,
- `src/api/handlers.rs`.

---

## 5) Что уже в хорошем состоянии

- ряд исторических проблем по shadow trader и миграциям уже закрыт,
- ключевые тесты проходят на HEAD (`cargo test --quiet`),
- основной paper-loop и fleet optimizer pipeline рабочие.

---

## 6) Рекомендуемый порядок фиксов

1. Починить reconnect state-machine (read/write owner + replay + ping/pong).
2. Убрать destructive migration path, заменить на versioned/backup migration.
3. Ввести backpressure/метрики потерь сообщений и DB batches.
4. Декомпозировать `main.rs`, `api/handlers.rs`, `shadow_trader.rs`.
5. Добавить OOS/walk-forward оценку и drawdown-метрики в ранжирование.
