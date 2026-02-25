# Review: Dead Code (Round 2)

## Findings

### P3

1. Нереализованный execution-контур через `OrderExecutor` и связные DTO (`OrderRequest/OrderResponse/Position`) сохраняется как неиспользуемая публичная поверхность.
   - Paths:
     - `src/domain/exchange.rs:103`
     - `src/domain/models.rs:33`

2. Конфиг-поля `trading`/`volume_filter` остаются декларативными и не используются runtime-путём.
   - Paths:
     - `src/config/mod.rs:20`
     - `src/config/mod.rs:116`
     - `config/config.toml:21`

## Verdict

После удаления `RiskManager` dead-code footprint снизился, но остаётся неиспользуемый execution/config слой, который вводит в заблуждение о реальных runtime-возможностях.
