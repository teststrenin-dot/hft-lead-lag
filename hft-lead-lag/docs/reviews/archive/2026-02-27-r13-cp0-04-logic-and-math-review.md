# R13 CP0 - Logic and Math Review

Date: 2026-02-27

## Findings

### P1
1. Candidate-history definition drift: docs say global cumulative history, runtime scopes/resets by `run_id`.
- Evidence: `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md:45`, `docs/status/2026-02-26-business-logic-v1-implementation-status.md:46`, `src/domain/screener/mod.rs:301`, `src/domain/screener/mod.rs:656`, `src/trial_batch_apply.rs:164`.
- Status: `open`.

2. Live aggregation math and restore math are inconsistent.
- Evidence: `src/domain/screener/mod.rs:666`, `src/domain/screener/mod.rs:684`, `src/infrastructure/db.rs:737`, `src/domain/screener/mod.rs:523`.
- Status: `open`.

### P3
1. Checkpoint taxonomy is inconsistent across core docs (`CP2/CP3` semantics differ).
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:14`, `docs/status/2026-02-26-business-logic-roadmap.md:42`, `docs/status/2026-02-26-delivery-contract-first-playbook.md:29`, `docs/status/2026-02-26-delivery-contract-first-playbook.md:44`.
- Status: `open`.
