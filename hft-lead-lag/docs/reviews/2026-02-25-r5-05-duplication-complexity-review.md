# R5 — Duplication / Redundancy / Overcomplexity Review

## Findings
- **P3** Дублирование пересечения/сортировки символов между API и runtime helpers.
  - `get_symbols` повторяет логику, уже существующую в `runtime_symbols`.
  - Refs:
    - `src/api/handlers.rs:118-150`
    - `src/runtime_symbols.rs:86-132`

- **P3** Дублирование Gate NATR timeout/fetch-flow в setup и enrichment paths.
  - Refs:
    - `src/runtime_setup.rs:66-129`
    - `src/infrastructure/enrichment.rs:53-95`

- **P3** Повторяющиеся ветки `ExchangeSide` в event-loop core.
  - Refs:
    - `src/event_loop_core.rs:210-273`

- **P3** Дублирование в генерации grid-axis (f64/i64) с очень похожими guard/loop паттернами.
  - Refs:
    - `src/runtime_grid.rs:27-54`
    - `src/runtime_grid.rs:75-99`

## Simplification Potential
- Низкий риск, умеренный выигрыш в поддерживаемости.
- Приоритет: после закрытия P1/P2.
