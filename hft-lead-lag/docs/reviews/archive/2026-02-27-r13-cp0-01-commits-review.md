# R13 CP0 - Commits Review

Date: 2026-02-27

## Findings

### P1
1. API contract drift: docs lag behind live runtime routes.
- Evidence: `docs/README.md:127`, `docs/README.md:143`, `src/api/http_server.rs:127`, `src/api/http_server.rs:180`.
- Status: `open`.

2. Status tracker evidence links are stale/non-verifiable for current code layout.
- Evidence: `docs/status/2026-02-26-business-logic-v1-implementation-status.md:24`, `docs/status/2026-02-26-business-logic-v1-implementation-status.md:75`, `src/api/http_server.rs:145`, `src/api/handlers.rs:234`, `src/api/handlers.rs:275`.
- Status: `open`.

### P2
1. CP0 freeze requires version-tagged contracts, but docs still point to moving `HEAD`.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:82`, `docs/README.md:6`.
- Status: `open`.

2. Declared execution-state contract fields are not represented by current portfolio API DTOs.
- Evidence: `docs/status/2026-02-26-delivery-contract-first-playbook.md:40`, `src/api/handlers.rs:114`, `src/api/handlers.rs:127`, `src/api/handlers.rs:166`.
- Status: `open`.
