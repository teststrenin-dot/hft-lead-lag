# R13 CP0 - Duplication, Redundancy, and Complexity Review

Date: 2026-02-27

## Findings

### P1
1. Stated complexity budget (`module < 500 LOC`) is violated by core runtime modules.
- Evidence: `src/lib.rs:37`, `src/infrastructure/db.rs:2345`, `src/domain/screener/mod.rs:1084`, `src/api/handlers.rs:1012`.
- Status: `open`.

### P2
1. Portfolio snapshot persistence logic duplicated across several DB functions.
- Evidence: `src/infrastructure/db.rs:513`, `src/infrastructure/db.rs:540`, `src/infrastructure/db.rs:607`, `src/infrastructure/db.rs:633`, `src/infrastructure/db.rs:1310`.
- Status: `open`.

2. Near-duplicate SQL pipelines for best-config-per-symbol in multiple endpoints.
- Evidence: `src/api/handlers.rs:596`, `src/api/handlers.rs:806`.
- Status: `open`.

3. DB writer enqueue/backpressure logic is deeply branched and multi-queue.
- Evidence: `src/infrastructure/db.rs:824`, `src/infrastructure/db.rs:880`, `src/infrastructure/db.rs:1084`.
- Status: `open`.
