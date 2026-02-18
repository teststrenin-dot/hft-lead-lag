# TASK-001: Exchange Connectors (Binance + Gate)

**Статус:** ✅ Completed  
**Спринт:** Sprint 1 + post-sprint hardening

---

## Цель

Построить WebSocket-first коннекторы для Binance Futures и Gate Futures:
- получение `book_ticker`/`trades`,
- единая модель данных,
- минимизация задержки и аллокаций.

---

## Реализовано

### 1) Подключение и подписки
- Binance: `wss://fstream.binance.com/ws`
- Gate: `wss://fx-ws.gateio.ws/v4/ws/usdt`
- Подписки на `book_ticker` и `trades`.

### 2) Нормализация данных
- fixed-point представление цен/объемов (`i64` ticks, 1e-8),
- нормализация символов (`BTC_USDT -> BTCUSDT`),
- общий `BookTicker`/`Trade` для стратегии и API слоя.

### 3) Timestamp pipeline (важно)
После post-sprint hardening:
- timestamp ingress фиксируется в reader-задаче сразу при получении WS кадра,
- по каналу передается `(payload, receive_ts_ns)`,
- `BookTicker::new`/`Trade::new` получают `local_ts_ns` извне,
- устранен искусственный drift, который возникал при timestamp в момент позднего парсинга.

### 4) Startup hardening
- перед основным event loop выполняется drain накопленных startup сообщений,
- это убирает искажение первых метрик после долгой фазы подписок.

---

## Файлы реализации

- `src/infrastructure/exchanges/binance/mod.rs`
- `src/infrastructure/exchanges/gate/mod.rs`
- `src/infrastructure/exchanges/common.rs`
- `src/domain/messages.rs`
- `src/main.rs` (startup drain)

---

## Runtime surface

- HTTP: `GET /health`, `GET /api/v1/symbols`, `GET /api/v1/screener`, `GET /screener`
- WS: `ws://127.0.0.1:8181/ws`

Volume filter в symbols/screener runtime: `1_000_000 USD`.

---

## Проверка

```bash
cargo build
cargo test
```

На текущем состоянии: тесты проходят (`14 passed` + doc-tests).

---

## Ограничения (не входило в TASK-001)

- полноценная order execution логика,
- продакшен reconnect/observability стек,
- расширенная интеграционная нагрузочная валидация.

---

*Updated: 2026-02-18*
