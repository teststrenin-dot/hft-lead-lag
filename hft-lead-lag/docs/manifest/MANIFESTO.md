# HFT Lead-Lag Manifest

## Миссия
Построить высокочастотную торговую систему для арбитража lead-lag между Binance Futures и Gate.io Futures с минимальной задержкой и максимальной надёжностью.

---

## Архитектурные принципы

### 1. Strict Constraints
- **Нет god objects** — каждый класс/модуль имеет одну ответственность
- **Модульность** — детерминированная когнитивная нагрузка (каждый модуль < 500 LOC)
- **Явные зависимости** — никаких скрытых coupling

### 2. SOLID (оптимальный для HFT)
- **Single Responsibility** — разделение hot path и cold path
- **Open/Closed** — расширение через новые коннекторы, не модификацию
- **Liskov Substitution** — любой exchange реализует общий trait
- **Interface Segregation** — маленькие специализированные трейты
- **Dependency Inversion** — зависимость от абстракций, не реализаций

### 3. Слоистая архитектура
```
API → Application → Domain → Infrastructure
     (зависимости направлены внутрь)
```

### 4. WebSocket-First
- **Всё на WS**, кроме cold path (аутентификация, refresh токенов)
- **Zero-copy parsing** — минимизация аллокаций в hot path
- **Batch operations** — группировка запросов для обхода rate limits

### 5. Детерминизм
- **Fixed-point arithmetic** — никаких float в hot path
- **Предсказуемое поведение** — явная обработка всех ошибок
- **Измеримость** — метрики на каждом уровне

---

## Инструменты разработки

### MCP Sequential Thinking
**Статус**: ✅ Enabled

Используется для сложных многошаговых задач:
- Декомпозиция больших задач на подзадачи
- Пошаговое планирование с ревизией
- Ветвление альтернативных подходов
- Гипотезы и верификация

**Когда применять**:
- Архитектурные решения
- Debug сложных проблем
- Планирование спринтов
- Code review анализ

### Superpowers Skills
**Статус**: ✅ Installed (14 skills)

| Skill | Когда применяется |
|-------|------------------|
| `brainstorming` | Перед созданием новых фич, исследование идей |
| `dispatching-parallel-agents` | 2+ независимые задачи → параллельное выполнение |
| `executing-plans` | Есть written plan → выполнение с checkpoints |
| `finishing-a-development-branch` | Завершение фичи → merge/PR/cleanup |
| `receiving-code-review` | Получение feedback → анализ и implementation |
| `requesting-code-review` | Перед merge → verification |
| `subagent-driven-development` | Сложные задачи → делегирование сабагентам |
| `systematic-debugging` | Баги → систематическая отладка до фиксов |
| `test-driven-development` | Новая фича/багфикс → сначала тесты |
| `using-git-worktrees` | Feature work → изоляция в worktree |
| `using-superpowers` | Поиск и применение скиллов |
| `verification-before-completion` | Перед claim success → запуск verification |
| `writing-plans` | Multi-step task → written plan перед кодом |
| `writing-skills` | Создание новых скиллов |

### Subagents
**Статус**: ✅ Enabled

Используются для ускорения процессов:

**Когда делегировать сабагентам**:
- Поиск по кодовой базе (grep, glob)
- Чтение множественных файлов
- Исследование внешних ресурсов (web fetch)
- Параллельная реализация независимых модулей
- Code review и анализ паттернов

**Примеры**:
```
1. "Найди все HMAC реализации в проекте" → subagent search
2. "Изучи Binance API документацию" → subagent web fetch
3. "Реализуй коннекторы для Binance и Gate" → parallel subagents
4. "Проверь код перед merge" → subagent code review
```

---

## Технологический стек

### Ядро
- **Rust 2021** — безопасность памяти + производительность
- **Tokio** — async runtime с full features
- **tokio-tungstenite** — WebSocket клиент

### Криптография
- **hmac** + **sha2** — HMAC-SHA256 (Binance), HMAC-SHA512 (Gate)
- **hex** — encoding подписей

### Данные
- **bytes** — zero-copy buffers
- **serde_json** — JSON парсинг
- **fast-float** — быстрое парсинг чисел

### Наблюдаемость
- **tracing** + **tracing-subscriber** — структурированное логирование
- **tracing-appender** — централизованная запись runtime логов в `logs/runtime.log`
- **time** — тайминги и метрики

### Конфигурация
- **toml** + **serde** — декларативная конфигурация
- **Environment variables** — секреты и runtime настройки

---

## Структура проекта

```
hft-lead-lag/
├── Cargo.toml              # Fast-build: codegen-units=256
├── config/
│   └── config.toml         # Базовая конфигурация
├── docs/
│   ├── manifest/           # Этот документ + принципы
│   ├── backlog/            # Бэклог задач
│   ├── sprints/            # Спринты с задачами
│   └── TASK-*.md           # Спецификации задач
└── src/
    ├── domain/             # Domain layer (traits, entities)
    ├── application/        # Business logic (services, ports)
    ├── infrastructure/     # Exchange implementations
    ├── config/             # Configuration management
    └── api/                # External interfaces (HTTP, WS)
```

---

## Ключевые решения

### 1. Fixed-Point Arithmetic
```rust
pub type PriceTicks = i64;  // 1e-8 precision
pub fn ticks_to_decimal(ticks: PriceTicks) -> f64 {
    ticks as f64 / 100_000_000.0
}
```
**Почему:** Избегаем float сравнений и rounding errors в hot path.

### 2. Symbol Interning
```rust
pub struct SymbolCache {
    cache: Arc<DashMap<String, Arc<str>>>,
}
```
**Почему:** Одна аллокация на символ, дальше Arc clones.

### 3. Zero-Copy Message Parsing
```rust
fn parse_book_ticker(&self, data: &[u8]) -> Option<BookTicker> {
    let symbol = extract_json_string_field(data, "s")?;
    // ... парсинг без аллокаций
}
```
**Почему:** Hot path не должен триггерить GC.

### 4. Batched Subscriptions
```rust
const SUBSCRIPTION_BATCH_SIZE: usize = 50;  // Binance limit
```
**Почему:** Обход rate limits при подписке на 100+ символов.

---

## Метрики качества

### Производительность
- **Latency P99** < 1ms (от получения WS сообщения до сигнала)
- **Throughput** > 100,000 messages/second
- **Allocation rate** < 1 MB/s в steady state

### Надёжность
- **Uptime** > 99.9%
- **Reconnect time** < 1s после разрыва
- **Message loss** = 0 (гарантированная доставка)

### Код
- **Test coverage** > 80%
- **Module size** < 500 LOC
- **Build time** < 30s (debug), < 2min (release)

---

## API ключи (Dev)

### Binance Futures
```
ApiKey: TnczkCaMuCYSvkLBYbiRXkAPDXIexso3jdIKu3TBA8aSiRwGlOTnSspstBcdpZrp
ApiSecret: cYkg26J3WqiMyPZMKA87tgbPJmRo1ybghVyeh52s2JaLQrTNDolmAc6V66rAGPxj
```

### Gate.io Futures
```
ApiKey: f9dd727fd86d14c064971e59e0c88e3f
ApiSecret: 534d0d582a0fa23faf378cf2b0b68cc4c56212b47f1293b93fa335fdf326dfb1
```

**Внимание:** Это dev ключи для тестирования. Не использовать для production.

---

## Ссылки

### Документация бирж
- [Binance Futures API](https://binance-docs.github.io/apidocs/futures/en/)
- [Gate.io Futures WebSocket](https://www.gate.io/docs/developers/futures/ws/en/)

### Внутренние документы
- [docs/backlog/README.md](backlog/README.md) — Бэклог
- [docs/sprints/](sprints/) — Спринты
- [docs/TASK-001-connectors.md](TASK-001-connectors.md) — Спецификация коннекторов

---

## Version
**Manifest v1.1** | 2026-02-18

### Changelog
- **v1.2** — Добавлены runtime REST/WS checkpoint API и централизованный logging в `project/logs/`
- **v1.1** — Добавлены инструменты разработки (MCP Sequential Thinking, Superpowers Skills, Subagents)
- **v1.0** — Initial manifest
