# R12 CP0 - Preventive Architecture Review

Date: 2026-02-27

## Findings

### P1
1. Missing hard invariant for layer independence: domain code currently crosses into application/infrastructure pathways.
- Evidence: `src/domain/screener/mod.rs:41`, `src/domain/screener/mod.rs:45`, `src/domain/screener/mod.rs:163`, `src/domain/screener/mod.rs:633`, `src/domain/screener/mod.rs:742`, `docs/status/2026-02-26-business-logic-roadmap.md:25`.
- Status: `open`.

2. API DTO boundary is not version-isolated from domain DTO churn.
- Evidence: `src/api/mod.rs:17`, `src/api/mod.rs:19`, `src/api/handlers.rs:110`, `src/domain/screener/mod.rs:67`, `src/domain/screener/mod.rs:71`.
- Status: `open`.

### P2
1. Missing guardrail for `set_portfolio_ids_v1`: no uniqueness/non-empty contract validation at boundary.
- Evidence: `src/domain/screener/mod.rs:408`, `src/domain/screener/mod.rs:413`.
- Status: `open`.

2. Fallback API path can reintroduce DB IDs/states not aligned with active runtime IDs.
- Evidence: `src/api/handlers.rs:272`, `src/api/handlers.rs:277`, `src/api/handlers.rs:286`, `src/api/handlers.rs:394`.
- Status: `open`.

3. Capability boundary is weak: same HTTP surface mixes read APIs and process-control/subprocess actions.
- Evidence: `src/api/http_server.rs:28`, `src/api/http_server.rs:174`, `src/api/runner.rs:306`.
- Status: `open`.
