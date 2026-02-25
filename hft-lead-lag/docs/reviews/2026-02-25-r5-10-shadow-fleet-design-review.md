# R5 — Shadow Fleet Design Review

## Findings
- **P1** DB/runtime divergence risk on rejected batch.
  - Конфиги upsert'ятся в DB до фактического patch apply, и при reject runtime/DB расходятся.
  - Refs:
    - `src/trial_batch_apply.rs:127-136`

- **P2** Policy scoring surface пока live-only (in-memory), without durable persistence.
  - После restart/history replay внешние автоматизации теряют scoring/gating контекст.
  - Refs:
    - `src/domain/screener/shadow_fleet.rs:513-533`
    - `src/domain/screener/policy_views.rs:1-44`
    - `src/api/handlers.rs:213-241`

- **P3** Gate flags (`gate_enabled`) не влияют на tick execution path; gating сейчас диагностический, не управляющий.
  - Refs:
    - `src/domain/screener/shadow_fleet.rs:423-496`
    - `src/domain/screener/shadow_fleet.rs:523-533`

## Regression Statement
- Явного crash/regression по тестам нет, но архитектурно policy surface пока не замыкается на устойчивый decision loop.
