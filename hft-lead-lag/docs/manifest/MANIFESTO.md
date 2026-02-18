# HFT Lead-Lag Manifest

## Миссия
Создать надежную и измеримую lead-lag платформу между Binance Futures и Gate Futures с приоритетом на корректность данных и контролируемую задержку.

---

## Архитектурные принципы

1. **Separation of concerns**  
   `API -> Application -> Domain -> Infrastructure`.

2. **WebSocket-first market data**  
   REST используется только в cold-path / fallback сценариях.

3. **Deterministic data pipeline**  
   - fixed-point цены и объемы (`i64` ticks),
   - явная нормализация timestamp unit,
   - явная обработка ошибок.

4. **Latency observability over assumptions**  
   Метрики latency/lag считаются из фактических timestamp'ов, а не из предположений.

5. **Behavior-safe runtime defaults**  
   Fallback и polling не должны ломать hot-path и не должны перегружать runtime.

---

## Ключевые инженерные решения

### 1) Fixed-point hot path
- цены/объемы в ticks (1e-8),
- float только для отображения и агрегатов.

### 2) Symbol interning + легкий парсинг
- кэш символов,
- парсинг JSON без тяжелых аллокаций в горячем пути.

### 3) Ingress timestamping
- receive timestamp фиксируется в момент получения WS кадра reader-задачей,
- этот timestamp проходит до `BookTicker/Trade`,
- исключается ложный drift от позднего парсинга.

### 4) Startup backlog control
- перед основным event loop очищается startup backlog,
- первые метрики не искажаются историческими сообщениями.

### 5) Screener load-safety
- REST fallback для screener вызывается только при отсутствии live rows,
- UI polling ограничен до 1 сек.

---

## Технологии

- Rust 2021
- tokio
- tokio-tungstenite
- axum
- serde / serde_json
- bytes
- fast-float
- tracing + tracing-subscriber + tracing-appender

---

## Наблюдаемость и проверки

- Логи: `logs/runtime.log`
- Runtime API: `/health`, `/api/v1/symbols`, `/api/v1/screener`, `/screener`
- Broadcast WS: `ws://127.0.0.1:8181/ws`
- Базовая валидация: `cargo build`, `cargo test`

---

## Security policy (docs)

- Не хранить API ключи/секреты в документации и репозитории.
- Использовать только environment variables / secret manager.

---

## Связанные документы

- [docs/README.md](../README.md)
- [docs/TASK-001-connectors.md](../TASK-001-connectors.md)
- [docs/TASK-002-screener-leadlag-checkpoints.md](../TASK-002-screener-leadlag-checkpoints.md)
- [docs/backlog/README.md](../backlog/README.md)
- [docs/sprints/](../sprints/)

---

**Manifest v1.3**  
*Updated: 2026-02-18*
