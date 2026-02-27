# R13 CP0 - Preventive Architecture Review

Date: 2026-02-27

## Findings

### P1
1. Portfolio read-model endpoints use mixed data sources with asymmetric fallback behavior.
- Evidence: `src/api/handlers.rs:257`, `src/api/handlers.rs:272`, `src/api/handlers.rs:330`, `src/api/handlers.rs:371`, `src/api/handlers.rs:394`.
- Status: `open`.

### P2
1. Domain boundary remains porous (`ScreenerStore` depends on app/infra services).
- Evidence: `src/domain/screener/mod.rs:41`, `src/domain/screener/mod.rs:45`, `src/domain/screener/mod.rs:742`, `src/domain/screener/mod.rs:802`.
- Status: `open`.

2. HTTP route contract fragmentation (many inline literals, weak centralization).
- Evidence: `src/api/http_server.rs:121`, `src/api/http_server.rs:188`.
- Status: `open`.

### P3
1. Transport DTO concerns embedded into domain row model.
- Evidence: `src/domain/screener/mod.rs:66`, `src/domain/screener/mod.rs:70`.
- Status: `open`.
