# R7 — Commits Review

Date: 2026-02-26
Analyzed range: full history with deep dive in recent review/refactor waves.

## What Went Well
- Strong hardening of DB/queue pipeline and recovery behavior.
- Large decomposition of `main.rs` into smaller modules reduced direct blast radius.
- Runtime safety and telemetry improvements were shipped incrementally.

## Findings

### P1
1. Commit-type mismatch: docs-labeled commit changed runtime behavior.
- Evidence: `e4f62a1`, `src/main.rs` (blacklist expansion in runtime path).
- Impact: high risk of review-gate bypass and release confusion.
- Recommendation: enforce commit-scope checks in CI (`docs/*` commits cannot touch `src/*`).

2. Feature commit followed by quick regression fixes in same area.
- Evidence: `bfbae67` followed by `d510c92`, `1be4ec5` in portfolio runtime flow.
- Impact: weak pre-merge scenario coverage for fairness/recovery.
- Recommendation: mandatory scenario suite for portfolio runtime before merge.

### P2
1. "broken" intermediate states reached mainline history.
- Evidence: `aca81f0` then revert/fix `0662daf`.
- Impact: poor bisectability and CI confidence.
- Recommendation: block `wip/broken` commits on protected branch.

2. Runtime artifacts landed in Git and later cleaned up.
- Evidence: `660d722`, `5867d54` (`config/.trial-ack`, large trial JSON churn).
- Impact: noisy diffs and accidental state commits.
- Recommendation: stronger ignore/pre-commit rules for runtime artifacts.

### P3
1. Mixed-scope checkpoint/chore commits reduced traceability.
- Evidence: `19bf570`, `f72003b`.
- Recommendation: constrain commit scope and size in branch policy.
