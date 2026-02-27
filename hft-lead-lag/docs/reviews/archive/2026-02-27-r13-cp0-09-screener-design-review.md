# R13 CP0 - Screener Design Review

Date: 2026-02-27

## Findings

### P1
1. Portfolio identity contract inconsistent across docs/runtime and across endpoints.
- Evidence: `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md:19`, `docs/runbooks/2026-02-26-shadow-drill-v1.md:51`, `docs/status/2026-02-26-business-logic-v1-implementation-status.md:29`, `src/main.rs:127`, `src/main.rs:181`, `src/api/handlers.rs:258`, `src/api/handlers.rs:285`, `src/api/handlers.rs:335`.
- Status: `open`.

2. CP0 “no drift” contract is broken by docs vs live route surface mismatch.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:82`, `docs/status/2026-02-26-business-logic-roadmap.md:86`, `docs/README.md:125`, `src/api/http_server.rs:127`, `src/api/http_server.rs:131`, `src/api/http_server.rs:135`, `src/api/http_server.rs:139`, `src/api/http_server.rs:148`, `src/api/http_server.rs:166`, `src/api/http_server.rs:174`.
- Status: `open`.

### P2
1. Declared execution-state contract is not represented in screener/operator APIs.
- Evidence: `docs/status/2026-02-26-delivery-contract-first-playbook.md:36`, `docs/status/2026-02-26-delivery-contract-first-playbook.md:40`, `src/api/handlers.rs:114`, `src/api/handlers.rs:127`, `src/api/handlers.rs:166`.
- Status: `open`.

2. UI does not consume portfolio contracts expected by status docs.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:53`, `docs/status/2026-02-26-business-logic-roadmap.md:135`, `src/api/templates/screener.html:86`, `src/api/templates/fleet.html:73`, `src/api/templates/trials.html:399`, `src/api/templates/trials.html:431`.
- Status: `open`.

3. `useful_winrate` unit mismatch across endpoints (ratio vs percent).
- Evidence: `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md:25`, `src/api/handlers.rs:153`, `src/api/handlers.rs:317`, `src/api/handlers.rs:134`, `src/api/handlers.rs:341`, `src/application/services/portfolio_runtime.rs:129`.
- Status: `open`.

### P3
1. Versioned contract-freeze artifact is still not explicit.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:82`, `docs/README.md:5`, `docs/README.md:288`.
- Status: `open`.

2. Runbook API sanity checks omit `/api/v1/portfolio/performance`.
- Evidence: `docs/runbooks/2026-02-26-shadow-drill-v1.md:44`, `docs/runbooks/2026-02-26-shadow-drill-v1.md:46`, `src/api/http_server.rs:135`.
- Status: `open`.
