# Review: Dead Code

## Findings

### P3

1. `RiskManager`/`RiskLimits` определены, но не подключены в рабочий путь исполнения (использование в основном в тестах).
   - Path:
     - `src/application/services/risk.rs:1`

## Verdict

Dead code небольшой по объему, но концептуально вредный: создает ложное ощущение включенного risk-control слоя.
