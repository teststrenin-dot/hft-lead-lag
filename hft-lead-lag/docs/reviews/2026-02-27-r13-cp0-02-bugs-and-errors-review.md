# R13 CP0 - Bugs and Errors Review

Date: 2026-02-27

## Findings

### P1
1. Docs/runtime API drift violates CP0 contract boundary expectations.
- Evidence: `docs/README.md:127`, `docs/README.md:143`, `src/api/http_server.rs:127`, `src/api/http_server.rs:180`.
- Status: `open`.

2. DoD/scope conflict on portfolio topology (`exactly 2` vs dynamic `PORTFOLIO_IDS`).
- Evidence: `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md:19`, `docs/status/2026-02-26-business-logic-v1-implementation-status.md:29`, `src/main.rs:127`, `src/main.rs:181`.
- Status: `open`.

### P2
1. Declared execution-state fields are not observable/persisted in API/DB boundary.
- Evidence: `docs/status/2026-02-26-delivery-contract-first-playbook.md:40`, `src/api/handlers.rs:114`, `src/infrastructure/db.rs:125`.
- Status: `open`.

2. CP0 freeze/versioning contract is not enforceable from current docs.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:82`, `docs/README.md:6`, `docs/README.md:288`, `docs/status/2026-02-26-project-math-model.md:4`.
- Status: `open`.

3. `portfolio/active` DB fallback can reintroduce non-runtime portfolio IDs.
- Evidence: `src/api/handlers.rs:275`, `src/api/handlers.rs:285`, `docs/status/2026-02-26-business-logic-v1-implementation-status.md:29`.
- Status: `open`.
