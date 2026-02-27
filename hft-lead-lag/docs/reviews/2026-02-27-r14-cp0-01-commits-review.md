# R14 CP0 - Commits Review

Date: 2026-02-27
Range: `3c50138..16b7813`

## Findings

### P1
1. `closed` API contract drift fixed and centralized.
- Evidence: `src/api/http_server.rs:122`, `src/api/http_server.rs:194`, `docs/README.md:130`.
- Status: `closed`.

2. `closed` Topology drift fixed (`PORTFOLIO_IDS` dynamic, no hardcoded exactly-2 in target-state doc).
- Evidence: `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md:19`, `src/main.rs:184`.
- Status: `closed`.

3. `closed` Candidate-restore semantics aligned with live collapse behavior.
- Evidence: `src/infrastructure/db.rs:735`, `src/domain/screener/mod.rs:666`, `src/infrastructure/db.rs:1918`.
- Status: `closed`.

4. `closed` `exit_reason` moved to typed domain enum with boundary serialization.
- Evidence: `src/domain/screener/shadow_trader.rs:30`, `src/domain/screener/mod.rs:707`, `src/infrastructure/db.rs:1299`.
- Status: `closed`.

### P3
1. Application-layer shim keeps secondary import surface for domain portfolio runtime.
- Evidence: `src/application/services/portfolio_runtime.rs:1`, `src/application/services/mod.rs:7`.
- Status: `open`.
