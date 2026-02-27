# R13 CP0 - Shadow Trader/Fleet Design Review

Date: 2026-02-27

## Findings

### P1
1. CP0 contract freeze is not actually pinned for shadow/fleet surfaces.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:81`, `docs/status/2026-02-26-business-logic-roadmap.md:82`, `docs/README.md:6`, `docs/README.md:127`, `src/api/http_server.rs:148`, `src/api/http_server.rs:153`.
- Status: `open`.

2. `exit_reason` is stringly-typed across trader/fleet/portfolio/analytics boundaries.
- Evidence: `src/domain/screener/shadow_trader.rs:338`, `src/domain/screener/shadow_fleet.rs:206`, `src/domain/screener/mod.rs:705`, `src/api/handlers.rs:863`, `ray_driver/ipc.py:137`, `docs/status/2026-02-26-project-math-model.md:216`.
- Status: `open`.

### P2
1. `run_id` attribution depends on positional coupling of separate deques.
- Evidence: `src/domain/screener/shadow_trader.rs:162`, `src/domain/screener/shadow_trader.rs:163`, `src/domain/screener/shadow_trader.rs:564`, `src/domain/screener/shadow_trader.rs:578`, `src/domain/screener/shadow_fleet.rs:457`, `src/domain/screener/shadow_fleet.rs:462`, `docs/README.md:25`.
- Status: `open`.

2. Symbol normalization contract differs across shadow/fleet endpoints.
- Evidence: `src/api/handlers.rs:421`, `src/api/handlers.rs:428`, `src/domain/screener/mod.rs:1048`, `src/domain/screener/mod.rs:1054`, `src/domain/screener/policy_views.rs:15`, `src/domain/screener/tests.rs:52`.
- Status: `open`.

### P3
1. Module boundary docs are stale/incomplete vs actual shadow/fleet module surface.
- Evidence: `src/domain/screener/mod.rs:3`, `src/domain/screener/mod.rs:7`, `src/domain/screener/mod.rs:12`, `src/domain/screener/mod.rs:18`.
- Status: `open`.
