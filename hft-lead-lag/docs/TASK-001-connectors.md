# Task: HFT Lead-Lag Exchange Connectors

**Статус**: ✅ Completed (2026-02-18)

**Спринт**: Sprint 1

**Исполнители**: Development Team

---

## Краткое описание

Разработка WebSocket-first коннекторов для Binance Futures и Gate.io Futures для получения market data (book ticker, trades) с минимальной задержкой.

---

## Требования

### Функциональные
- [x] Подключение к Binance Futures WebSocket
- [x] Подключение к Gate.io Futures WebSocket  
- [x] Подписка на book ticker stream
- [x] Подписка на trade stream
- [x] Парсинг сообщений (zero-copy)
- [x] Аутентификация Gate.io (futures.login)

### Нефункциональные
- [x] Zero-allocation hot path
- [x] Symbol interning
- [x] Fixed-point arithmetic (i64 ticks)
- [x] Fast build (< 30s debug)
- [x] Test coverage > 60%

---

## Спецификация

### Binance Futures

**Endpoint**: `wss://fstream.binance.com/ws`

**Подписки**:
```json
{
  "method": "SUBSCRIBE",
  "params": ["btcusdt@bookTicker", "ethusdt@bookTicker"],
  "id": 1234567890
}
```

**Формат сообщения**:
```json
{
  "e": "bookTicker",
  "u": 400900217,
  "s": "BTCUSDT",
  "b": "50000.00",
  "B": "1.5",
  "a": "50000.50",
  "A": "2.0"
}
```

### Gate.io Futures

**Endpoint**: `wss://fx-ws.gateio.ws/v4/ws/usdt`

**Аутентификация**:
```json
{
  "time": 1234567890,
  "channel": "futures.login",
  "event": "api",
  "sign_method": "HMAC_SHA512",
  "key": "your_api_key",
  "sign": "hmac_signature"
}
```

**Подписки**:
```json
{
  "time": 1234567890,
  "channel": "futures.book_ticker",
  "event": "subscribe",
  "data": ["BTC_USD", "ETH_USD"]
}
```

---

## Реализация

### Файлы
- `src/infrastructure/exchanges/binance/mod.rs` — Binance connector
- `src/infrastructure/exchanges/gate/mod.rs` — Gate connector
- `src/infrastructure/exchanges/common.rs` — Общие утилиты (HMAC, парсинг)
- `src/domain/exchange.rs` — Traits (MarketDataStream, OrderExecutor)

### Ключевые решения

#### 1. Fixed-Point Arithmetic
```rust
pub type PriceTicks = i64;  // 1e-8 precision

pub fn ticks_to_decimal(ticks: PriceTicks) -> f64 {
    ticks as f64 / 100_000_000.0
}

pub fn decimal_to_ticks(decimal: f64) -> PriceTicks {
    (decimal * 100_000_000.0) as i64
}
```

#### 2. Symbol Interning
```rust
pub struct SymbolCache {
    cache: Arc<DashMap<String, Arc<str>>>,
}

// Одна аллокация на символ, дальше Arc clones
let symbol = cache.intern("BTCUSDT");
```

#### 3. Zero-Copy JSON Parsing
```rust
pub fn extract_json_string_field(json: &[u8], field: &str) -> Option<Bytes> {
    // Парсинг без аллокаций
    // Возвращает Bytes для zero-copy обработки
}
```

---

## Тесты

```bash
$ cargo test

running 14 tests
test infrastructure::exchanges::binance::tests::test_build_book_ticker_subscription ... ok
test infrastructure::exchanges::binance::tests::test_parse_book_ticker ... ok
test infrastructure::exchanges::gate::tests::test_build_auth_payload ... ok
test infrastructure::exchanges::common::tests::test_hmac_sha256 ... ok
test infrastructure::exchanges::common::tests::test_extract_json_string ... ok
test infrastructure::exchanges::common::tests::test_extract_json_i64 ... ok
...
test result: ok. 14 passed; 0 failed
```

---

## Метрики

| Метрика | Значение |
|---------|----------|
| LOC | ~1300 |
| Модули | 20+ |
| Тесты | 14 passing |
| Build time (debug) | ~15s |
| Test coverage | ~60% |

---

## Зависимости

### Крейты
- `tokio` — async runtime
- `tokio-tungstenite` — WebSocket
- `hmac` + `sha2` — криптография
- `bytes` — zero-copy buffers
- `serde_json` — JSON
- `fast-float` — быстрый парсинг чисел

### Внешние
- Binance Futures API
- Gate.io Futures API

---

## Ссылки

### Документация
- [Binance Futures API](https://binance-docs.github.io/apidocs/futures/en/)
- [Gate.io Futures WebSocket](https://www.gate.io/docs/developers/futures/ws/en/)

### Внутренние
- [docs/manifest/MANIFESTO.md](../manifest/MANIFESTO.md) — Архитектурные принципы
- [docs/backlog/README.md](../backlog/README.md) — Бэклог
- [docs/sprints/sprint-001-connectors.md](../sprints/sprint-001-connectors.md) — Sprint 1

### Код
- `src/infrastructure/exchanges/binance/mod.rs`
- `src/infrastructure/exchanges/gate/mod.rs`
- `src/domain/exchange.rs`

---

*Task completed: 2026-02-18*
