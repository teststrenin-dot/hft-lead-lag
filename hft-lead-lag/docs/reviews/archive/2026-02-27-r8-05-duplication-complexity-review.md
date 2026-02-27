# R8 - Duplication, Redundancy, and Complexity Review

Date: 2026-02-27

## Findings

### P2
1. Duplicate SQL ranking patterns across portfolio endpoints.
- Evidence: `src/api/handlers.rs:595`, `src/api/handlers.rs:805`.
- Impact: drift risk when ranking logic changes.
- Status: `open`.

2. Repeated DB replace paths for state/guards/paper/snapshots share near-identical loops.
- Evidence: `src/infrastructure/db.rs:513`, `src/infrastructure/db.rs:540`, `src/infrastructure/db.rs:607`, `src/infrastructure/db.rs:633`.
- Impact: high maintenance cost and bug surface.
- Status: `open`.

3. DB writer has deep multi-queue pipeline with relay tasks.
- Evidence: `src/infrastructure/db.rs:23`, `src/infrastructure/db.rs:805`, `src/infrastructure/db.rs:1005`.
- Impact: difficult reasoning, harder failure-mode guarantees.
- Status: `open`.
