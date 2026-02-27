# R9 CP3 - Screener Design Review

Date: 2026-02-27

## Findings

### P1
1. `age_minutes_from_first_tick` semantics are not stable across live and restore paths.
- Evidence: `src/domain/screener/mod.rs:793`, `src/domain/screener/mod.rs:802`, `src/domain/screener/mod.rs:806`, `src/domain/screener/mod.rs:520`.
- Status: `open`.

### P2
1. Candidate cleanup depends on traffic path (prune not guaranteed at candidate read boundary).
- Evidence: `src/api/handlers.rs:304`, `src/domain/screener/mod.rs:967`, `src/domain/screener/mod.rs:1012`.
- Status: `open`.

2. Non-atomic candidate snapshot across maps under concurrent mutation.
- Evidence: `src/domain/screener/mod.rs:152`, `src/domain/screener/mod.rs:153`, `src/domain/screener/mod.rs:799`.
- Status: `open`.

### P3
1. Deterministic endpoint contract is implicit and lightly tested.
- Evidence: `src/api/handlers.rs:308`, `src/domain/screener/tests.rs:681`.
- Status: `open`.
