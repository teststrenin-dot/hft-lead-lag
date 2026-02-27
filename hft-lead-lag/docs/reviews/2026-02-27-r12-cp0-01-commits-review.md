# R12 CP0 - Commits Review

Date: 2026-02-27

## Findings

### P1
1. CP0 tracker docs are pinned to older sync points while boundary commits continued changing core surfaces.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:4`, `docs/status/2026-02-26-business-logic-v1-implementation-status.md:4`, `docs/status/2026-02-26-business-logic-roadmap.md:86`.
- Status: `open`.

### P2
1. New control-plane endpoints were added without explicit CP0 contract-freeze artifact/version bump.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:82`, `src/api/http_server.rs:166`, `src/api/http_server.rs:170`, `src/api/http_server.rs:174`, `src/api/http_server.rs:178`.
- Status: `open`.

2. CP0 requires contract-smoke gate, but runner-control handlers lack aligned handler-level contract tests.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:84`, `src/api/handlers.rs:972`, `src/api/handlers.rs:986`, `src/api/handlers.rs:1000`, `src/api/handlers/tests.rs:63`.
- Status: `open`.
