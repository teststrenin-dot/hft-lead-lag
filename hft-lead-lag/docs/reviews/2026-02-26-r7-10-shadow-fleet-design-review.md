# R7 — Shadow Fleet Design Review

Date: 2026-02-26
Scope: `shadow_fleet`, `shadow_trader`, policy + lifecycle integration.

## Findings

### P1
1. No strict `TraderConfig` validation before runtime apply.
- Evidence: `src/domain/screener/trader_config.rs:12`, `src/trial_batch_protocol.rs:123`.

2. Duplicate config IDs in batch can cause double execution in-memory.
- Evidence: `src/trial_batch_protocol.rs:10`, `src/domain/screener/shadow_fleet.rs:374`.

3. `run_id` contamination risk at incremental patch boundaries.
- Evidence: `src/trial_batch_apply.rs:149`, `src/domain/screener/fleet_reload.rs:62`, `src/domain/screener/shadow_fleet.rs:463`.

### P2
1. Policy snapshot can include disabled/pruned configs as top results.
- Evidence: `src/domain/screener/shadow_fleet.rs:514`, `:523`.

2. Reset-path trade persistence not fully mirrored into in-memory portfolio path.
- Evidence: `src/domain/screener/fleet_reload.rs:69`, `:71`.

3. Entry direction bias and baseline-window gate mismatch.
- Evidence: `src/domain/screener/shadow_trader.rs:462`, `:471`, `:424`, `:433`.

### P3
1. Heavy policy endpoint computation without cache guardrails.
- Evidence: `src/api/handlers.rs:387`, `src/domain/screener/policy_views.rs:31`.
