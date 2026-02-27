# R12 CP0 - Shadow Trader/Fleet Design Review

Date: 2026-02-27

## Findings

### P1
1. Shadow lifecycle semantics depend on string `exit_reason` contracts across modules.
- Evidence: `src/domain/screener/shadow_trader.rs:53`, `src/domain/screener/shadow_trader.rs:338`, `src/domain/screener/shadow_trader.rs:347`, `src/domain/screener/shadow_trader.rs:356`, `src/domain/screener/shadow_fleet.rs:206`, `src/domain/screener/mod.rs:705`, `docs/status/2026-02-26-project-math-model.md:198`, `docs/status/2026-02-26-project-math-model.md:216`.
- Status: `open`.

### P2
1. `run_id` trade attribution is based on implicit positional coupling of separate deques.
- Evidence: `src/domain/screener/shadow_trader.rs:201`, `src/domain/screener/shadow_trader.rs:205`, `src/domain/screener/shadow_trader.rs:548`, `src/domain/screener/shadow_trader.rs:564`, `src/domain/screener/shadow_fleet.rs:457`, `src/domain/screener/shadow_fleet.rs:462`.
- Status: `open`.

2. Fleet/trader integration contract is mostly code-only; docs do not formalize call-order/invariant boundary.
- Evidence: `docs/status/2026-02-26-project-math-model.md:5`, `docs/README.md:91`, `src/domain/screener/shadow_fleet.rs:424`, `src/domain/screener/shadow_fleet.rs:451`, `src/domain/screener/shadow_fleet.rs:456`.
- Status: `open`.

### P3
1. Module-level docs are already slightly stale (missing `shadow_fleet` in structure list), reducing discoverability of boundary contracts.
- Evidence: `src/domain/screener/mod.rs:3`, `src/domain/screener/mod.rs:7`, `src/domain/screener/mod.rs:18`.
- Status: `open`.
