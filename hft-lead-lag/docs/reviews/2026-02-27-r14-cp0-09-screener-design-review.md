# R14 CP0 - Screener Design Review

Date: 2026-02-27

## Findings

### P1
1. Candidate contract and topology contract are now synchronized between code and docs.
- Evidence: `src/api/handlers.rs:259`, `src/api/handlers/tests.rs:641`, `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md:19`, `docs/status/dynamics/2026-02-27-cp0-contract-freeze-v2.md:46`.
- Status: `closed`.

### P2
1. Screener runtime still aggregates many responsibilities in a single store boundary.
- Evidence: `src/domain/screener/mod.rs:158`, `src/domain/screener/mod.rs:642`.
- Status: `open`.

2. Event-level restore query lacks dedicated composite index for sustained growth.
- Evidence: `src/infrastructure/db.rs:735`, `src/infrastructure/db.rs:152`, `src/infrastructure/db.rs:153`.
- Status: `open`.
