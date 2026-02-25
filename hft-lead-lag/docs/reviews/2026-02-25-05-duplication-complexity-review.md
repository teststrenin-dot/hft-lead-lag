# Review: Дублирование, Избыточность, Переусложнение

## Findings

### P3

1. Дублированная логика фильтрации volume в REST-клиентах Binance/Gate.
   - Paths:
     - `src/infrastructure/rest/mod.rs:120`
     - `src/infrastructure/rest/mod.rs:257`

### P2

2. `build_runtime_grid` делает тяжелую комбинаторику в синхронном пути watcher.
   - Path:
     - `src/main.rs:432`
   - Риск: CPU spikes и задержки apply-path при расширении search-space.

## Verdict

Главная стоимость сопровождения — не локальные дубли, а концентрация тяжелой логики в runtime-critical путях `main.rs`.
