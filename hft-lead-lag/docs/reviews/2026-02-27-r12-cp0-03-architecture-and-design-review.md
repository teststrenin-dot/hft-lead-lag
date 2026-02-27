# R12 CP0 - Architecture and Design Review

Date: 2026-02-27

## Findings

### P1
1. Domain/Application boundary inversion: domain screener depends directly on application services.
- Evidence: `src/lib.rs:13`, `src/lib.rs:18`, `src/domain/screener/mod.rs:41`, `src/domain/screener/mod.rs:42`.
- Status: `open`.

2. API boundary leaks domain contracts and is used as internal dependency path.
- Evidence: `src/api/mod.rs:17`, `src/api/mod.rs:19`, `src/main.rs:6`, `src/runtime_setup.rs:2`, `src/api/http_server.rs:13`.
- Status: `open`.

### P2
1. Documented architecture contract drift: deterministic/module-size expectations diverge from 1000+ LOC boundary modules.
- Evidence: `src/lib.rs:37`, `src/lib.rs:38`, `src/domain/screener/mod.rs:1084`, `src/api/handlers.rs:1012`, `docs/status/2026-02-26-business-logic-roadmap.md:86`.
- Status: `open`.
