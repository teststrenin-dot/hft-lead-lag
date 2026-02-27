# R8 - Commits Review

Date: 2026-02-27

## Findings

### P3
1. Commit scope discipline is inconsistent in part of history.
- Evidence: `a79a6ff` (mixed runtime changes and DB artifact files), `f72003b` (checkpoint in-flight mixed scope).
- Impact: worse `git bisect` and riskier selective rollback.
- Status: `open`.

## What Was Good
1. Multiple fixes were delivered with nearby targeted tests (paper attribution, persistence, read-model behavior).
2. Clear progression in CP4 area from bug reports to deterministic test coverage.

## Recommendation
- Keep commits atomic: one concern per commit (runtime, docs, artifacts separated).
- Never include mutable DB artifacts in functional commits.
