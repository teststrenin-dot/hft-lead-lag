# R12 CP0 - Screener Design Review

Date: 2026-02-27

## Findings

### P1
1. Docs/API contract drift: documented operator endpoint surface does not fully match runtime routes.
- Evidence: `docs/README.md:125`, `docs/README.md:127`, `src/api/http_server.rs:127`, `src/api/http_server.rs:131`, `src/api/http_server.rs:135`, `src/api/http_server.rs:139`, `src/api/http_server.rs:148`, `src/api/http_server.rs:152`.
- Status: `open`.

2. Operator topology contract drift (`A/B` docs vs dynamic portfolio IDs) with DB fallback returning non-runtime IDs.
- Evidence: `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md:19`, `docs/runbooks/2026-02-26-shadow-drill-v1.md:51`, `docs/status/2026-02-26-business-logic-v1-implementation-status.md:29`, `src/application/services/portfolio_runtime.rs:110`, `src/api/handlers.rs:258`, `src/api/handlers.rs:285`.
- Status: `open`.

### P2
1. Execution-state contract expected in status/playbook is not exposed in screener/operator APIs.
- Evidence: `docs/status/2026-02-26-delivery-contract-first-playbook.md:36`, `docs/status/2026-02-26-delivery-contract-first-playbook.md:40`, `src/api/handlers.rs:115`, `src/api/http_server.rs:127`.
- Status: `open`.

2. UI surfaces are not aligned with declared operator model for portfolio/guards/performance.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:135`, `docs/status/2026-02-26-business-logic-roadmap.md:137`, `src/api/templates/screener.html:86`, `src/api/templates/fleet.html:73`, `src/api/templates/trials.html:399`.
- Status: `open`.

3. Metric unit drift: useful winrate shown as ratio in one API and percent in another.
- Evidence: `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md:27`, `src/api/handlers.rs:153`, `src/api/handlers.rs:317`, `src/application/services/portfolio_runtime.rs:129`, `src/api/handlers.rs:341`.
- Status: `open`.

### P3
1. CP0 version-tagged contract freeze is declared but no explicit versioned contract artifact exists.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:82`, `docs/status/2026-02-26-business-logic-roadmap.md:86`, `docs/README.md:5`.
- Status: `open`.
