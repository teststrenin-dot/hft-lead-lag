# R7 — Architecture and Design Review

Date: 2026-02-26

## Findings

### P1
1. Cross-layer coupling: domain depends on application/infrastructure types.
- Evidence: `src/domain/screener/mod.rs:40`, `src/domain/screener/mod.rs:43`.
- Impact: harder testing, higher coupling, slower refactors.
- Recommendation: introduce domain-level DTO/ports; map to DB/application at boundaries.

2. Trial apply transaction boundary is not strictly "durable then applied".
- Evidence: `src/trial_batch_apply.rs:128`, `src/runtime_hot_reload.rs:216`.
- Impact: possible memory/DB divergence around failures and restart boundaries.

### P2
1. API handlers still mix transport, SQL, and business aggregation.
- Evidence: `src/api/handlers.rs:17`, `src/api/handlers.rs:740`, `src/api/handlers.rs:846`.
- Recommendation: thin adapter handlers + dedicated query services.

2. Large modules remain central risk nodes.
- Evidence: `src/infrastructure/db.rs`, `src/api/handlers.rs`, `src/api/runner.rs`.

### P3
1. Boundary leakage through broad re-exports and utility placement.
- Evidence: `src/api/mod.rs:17`, `src/domain/screener/utils.rs:26`.
