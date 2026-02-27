# R9 CP3 - Cognitive Load and God Objects Review

Date: 2026-02-27

## Findings

### P2
1. CP3 candidate pipeline is spread across ingest, store mutation, scheduler, DB restore, and API projection.
- Evidence: `src/domain/screener/quote_ingest.rs:106`, `src/domain/screener/mod.rs:631`, `src/domain/screener/mod.rs:793`, `src/runtime_setup.rs:239`, `src/api/handlers.rs:308`.
- Impact: high reasoning burden and easier regressions during local changes.
- Status: `open`.

### P3
1. API semantics are inconsistent for winrate units (`ratio` vs `pct`).
- Evidence: `src/api/handlers.rs:153`, `src/api/handlers.rs:317`, `src/api/handlers.rs:344`.
- Impact: operator cognitive load and interpretation mistakes.
- Status: `open`.
