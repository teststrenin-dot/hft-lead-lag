# R9 CP3 - Duplication, Redundancy, and Complexity Review

Date: 2026-02-27

## Findings

### P2
1. Portfolio snapshot write logic is duplicated in separate DB paths.
- Evidence: `src/infrastructure/db.rs:540`, `src/infrastructure/db.rs:1310`.
- Impact: drift risk in restore compatibility and maintenance overhead.
- Status: `open`.

### P3
1. Candidate endpoint recomputes metric projections after ranking on same primitives.
- Evidence: `src/api/handlers.rs:308`, `src/api/handlers.rs:317`.
- Impact: minor complexity/overhead.
- Status: `open`.
