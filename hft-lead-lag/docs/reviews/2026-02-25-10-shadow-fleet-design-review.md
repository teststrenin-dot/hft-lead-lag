# Review: Дизайн Shadow Fleet

## Findings

### P1

1. Нет explicit failure-ack при reject/invalid patch — только таймаут на стороне драйвера.
   - Paths:
     - `ray_driver/ipc.py:46`
     - `src/main.rs:615`

### P2

2. Trial API не показывает patch-level операционные сигналы (scope/matched/unmatched/drained).
   - Path:
     - `src/api/handlers.rs:509`

3. Hash-based `config_id` без versioned контракта между Rust/Python уязвим к эволюции схемы.
   - Paths:
     - `src/domain/screener/trader_config.rs:60`
     - `src/main.rs:289`

## Verdict

Механика patch/apply рабочая и строгая, но UX эксплуатации и устойчивость контракта конфигов пока ниже желаемого уровня для production-scale orchestration.
