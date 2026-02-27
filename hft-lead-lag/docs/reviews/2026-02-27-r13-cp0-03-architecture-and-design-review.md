# R13 CP0 - Architecture and Design Review

Date: 2026-02-27

## Findings

### P1
1. API contract drift between docs and runtime routes.
- Evidence: `docs/README.md:127`, `docs/README.md:143`, `src/api/http_server.rs:127`, `src/api/http_server.rs:155`, `src/api/http_server.rs:166`.
- Status: `open`.

2. Layering contract violation: domain depends on application/infrastructure internals.
- Evidence: `src/lib.rs:13`, `src/lib.rs:38`, `src/domain/screener/mod.rs:41`, `src/domain/screener/mod.rs:45`, `src/domain/screener/mod.rs:163`.
- Status: `open`.

### P2
1. Dual config control planes mutate fleet state without explicit authority arbitration.
- Evidence: `docs/README.md:15`, `src/runtime_hot_reload.rs:299`, `src/runtime_hot_reload.rs:311`, `src/runtime_hot_reload.rs:356`, `src/runtime_hot_reload.rs:381`.
- Status: `open`.
