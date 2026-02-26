# R5 — Architecture & Design Review

## What Improved
1. Чёткая декомпозиция runtime orchestration из `main.rs` в специализированные модули.
2. Разнесение handler/screener внутренних подсистем по отдельным файлам снизило связность по месту.
3. Архитектурная читаемость выросла: `main.rs` 1981 -> 248 LOC.

## Findings
- **P2** Layering leakage: доменный `ScreenerStore` напрямую держит `DbWriter` из infrastructure.
  - Это склеивает domain + infra и усложняет изоляцию/подмену persistence-контракта.
  - Refs:
    - `src/domain/screener/mod.rs:9`
    - `src/domain/screener/mod.rs:84-101`
    - `src/infrastructure/db.rs:410-415`

- **P2** API runner и Python driver остаются склеены через дублированный CLI contract.
  - Изменения флагов/аргументов требуют синхронных правок в 2 местах.
  - Refs:
    - `src/api/runner/command.rs:92-169`
    - `src/api/runner.rs:212-265`
    - `ray_driver/*`

- **P3** `runtime_hot_reload.rs` все еще объединяет несколько control-plane контуров в одном модуле.
  - Улучшение относительно `main.rs` есть, но концентрация ответственности сохраняется.
  - Refs:
    - `src/runtime_hot_reload.rs:257-427`

## Architecture Score
- **7/10**
