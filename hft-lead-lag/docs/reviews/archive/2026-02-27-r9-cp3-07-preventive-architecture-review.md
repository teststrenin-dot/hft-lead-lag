# R9 CP3 - Preventive Architecture Review

Date: 2026-02-27

## Findings

### P1
1. Missing invariant that candidate pool must be partitioned (or explicitly marked global) by run context.
- Evidence: `src/domain/screener/mod.rs:296`, `src/domain/screener/mod.rs:647`, `src/infrastructure/db.rs:735`.
- Status: `open`.

2. Missing invariant for unified age basis source across live and restore paths.
- Evidence: `src/domain/screener/mod.rs:648`, `src/infrastructure/db.rs:741`.
- Status: `open`.

### P2
1. Startup restore lacks bounded-history strategy for candidate bootstrap.
- Evidence: `src/infrastructure/db.rs:735`, `src/runtime_setup.rs:236`.
- Status: `open`.

### P3
1. Unchecked narrowing from `i64` to `u32` in candidate history decode.
- Evidence: `src/infrastructure/db.rs:752`, `src/infrastructure/db.rs:753`, `src/infrastructure/db.rs:754`.
- Status: `open`.
