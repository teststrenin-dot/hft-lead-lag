# R9 CP3 - Architecture and Design Review

Date: 2026-02-27

## Findings

### P1
1. Candidate age contract is architecturally ambiguous across lifecycle phases.
- Evidence: `src/domain/screener/mod.rs:806`, `src/infrastructure/db.rs:741`.
- Impact: CP3 gate semantics depend on process lifecycle, not only on market/trade facts.
- Status: `open`.

### P2
1. Candidate bootstrap path scales with entire historical trades table.
- Evidence: `src/infrastructure/db.rs:735`, `src/runtime_setup.rs:236`.
- Status: `open`.

2. CP3 data plane relies on shared `ScreenerStore` orchestration with weak local boundaries for candidate-only behavior.
- Evidence: `src/domain/screener/mod.rs:151`, `src/domain/screener/mod.rs:793`.
- Status: `open`.
