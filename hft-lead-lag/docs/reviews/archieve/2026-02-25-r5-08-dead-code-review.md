# R5 — Dead Code Review

## Findings
- **P2** Public execution surface присутствует, но фактически не подключена в runtime flow.
  - `OrderExecutor`, `OrderRequest`, `OrderResponse`, `Position` экспортируются, но не используются активным runtime.
  - Refs:
    - `src/domain/exchange.rs:103-125`
    - `src/domain/models.rs:53-131`
    - `src/lib.rs:78-80`

- **P3** `config/config.toml` содержит `[trading]` секцию, которую `AppConfig` не десериализует.
  - Refs:
    - `config/config.toml:20-26`
    - `src/config/mod.rs:93-101`

- **P3** Архивный отчёт в `docs/reviews/archieve` содержит устаревшую ссылку на удаленный `risk.rs` модуль.
  - Refs:
    - `docs/reviews/archieve/2026-02-25-08-dead-code-review.md`
    - `src/application/services/mod.rs:1`

## Confidence
- `OrderExecutor` surface: high
- `[trading]` config drift: medium
- stale archived reference: medium
