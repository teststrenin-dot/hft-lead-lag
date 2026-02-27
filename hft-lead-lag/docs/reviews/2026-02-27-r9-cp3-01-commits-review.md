# R9 CP3 - Commits Review

Date: 2026-02-27

## Findings

### P1
1. CP3 candidate history restore (`1be4ec5`) left semantic mismatch live vs restore for age basis.
- Evidence: `src/domain/screener/mod.rs:648`, `src/domain/screener/mod.rs:520`, `src/infrastructure/db.rs:741`.
- Impact: same trade set can cross eligibility gate differently after restart.
- Status: `open`.

2. CP3 flow persists `run_id`, but candidate math remains global.
- Evidence: `src/infrastructure/db.rs:1275`, `src/infrastructure/db.rs:735`, `src/domain/screener/mod.rs:647`.
- Impact: cross-run contamination of candidate ranking.
- Status: `open`.

### P2
1. Missing integration test on runtime setup boundary for candidate restore sequencing.
- Evidence: `src/runtime_setup.rs:236`, `src/runtime_setup.rs:239`.
- Status: `open`.

## Positive
1. Commit history around CP3 includes focused tests for eligibility/ranking and candidate restore pieces.
2. Tie-break comparator includes deterministic final key (symbol).
