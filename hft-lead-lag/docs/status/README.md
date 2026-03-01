# Status Docs Index (`HFT-CP` Baseline)

Date: 2026-03-01
Status: active
Last sync: 2026-03-01 (status deep-dive refresh + archive cleanup)

## Purpose
Single entry point for current project status, checkpoint tracking, and operating constraints.

## Folder layout
1. `core/` — canonical business/process contracts (exactly 3 files).
2. `dynamics/` — active checkpoint status, readiness, evidence, and evolving math model.
3. `dynamics/archive/` — closed or superseded status artifacts kept for traceability.

## Canonical read order
1. `core/2026-02-27-business-objective-economic-control-map.md`
2. `core/2026-02-27-operating-model-spec-v1.md`
3. `core/2026-02-26-business-logic-roadmap.md`
4. `dynamics/2026-02-28-hft-rust-only-checkpoints.md`
5. `dynamics/2026-02-28-hft-checkpoint-readiness-breakdown.md`
6. `dynamics/2026-02-26-business-logic-v1-implementation-status.md`
7. `dynamics/2026-02-26-project-math-model.md`

## Active evidence bundles (used by checkpoints)
1. `dynamics/2026-02-28-cp2-lock-free-p99-evidence.md`
2. `dynamics/2026-02-28-cp3-updated-only-proof.md`
3. `dynamics/2026-02-28-cp4-parse-path-evidence.md`
4. `dynamics/2026-02-28-cp5-block1-raw-feed-evidence.md`
5. `dynamics/2026-02-28-cp6-execution-fast-path-evidence.md`
6. `dynamics/2026-02-28-rm1-plane-mode-contract-evidence.md`
7. `dynamics/2026-02-28-rm2-control-plane-decoupling-evidence.md`
8. `dynamics/2026-02-28-rm3-2core-cap-profile-evidence.md`
9. `dynamics/2026-02-28-rm5-control-plane-threaded-coalescing-evidence.md`
10. `dynamics/2026-02-28-cp7-block1-rm4-health-enforcement.md`
11. `dynamics/2026-02-28-rm4-hft-core-live-slo-validation.md`
12. `dynamics/2026-02-28-cp7-block2-event-driven-signal-loop-evidence.md`
13. `dynamics/2026-02-28-cp7-block3-watchdog-stall-evidence.md`
14. `dynamics/2026-02-28-cp7-block4-recovery-runbook-v1.md`
15. `dynamics/2026-02-28-cp7-block5-recovery-drill-automation-evidence.md`
16. `dynamics/2026-02-28-cp7-block6-dbwriter-drift-alert-evidence.md`
17. `dynamics/2026-02-28-cp7-block7-alert-level-escalation-contract.md`
18. `dynamics/2026-02-28-cp7-block8-alert-hook-script-evidence.md`
19. `dynamics/2026-02-28-observer-scout-forward-control-evidence.md`
20. `dynamics/2026-02-28-forward-rust-runtime-runner-evidence.md`
21. `dynamics/2026-02-28-forward-ui-live-race-lifecycle-evidence.md`
22. `dynamics/2026-02-28-forward-fresh-start-guardrails-evidence.md`
23. `dynamics/2026-02-28-e2e-forward-race-acceptance-evidence.md`
24. `dynamics/2026-02-26-delivery-contract-first-playbook.md`

## Archived dynamics docs
1. `dynamics/archive/2026-02-27-cp0-contract-freeze-v2.md`
2. `dynamics/archive/2026-02-27-cp6-1-forward-only-orchestration-spec.md`
3. `dynamics/archive/2026-02-28-e2e-forward-race-gap-list.md` (gap list closed; moved out of active set)

## Checkpoint system (`HFT-CP*`)
1. `HFT-CP0` Latency and Allocation Observatory
2. `HFT-CP1` SymbolId and Allocation Removal
3. `HFT-CP2` Lock-Free Strategy State
4. `HFT-CP3` Event-Driven Updated-Only Processing
5. `HFT-CP4` Minimal-Copy WS Parse Path
6. `HFT-CP5` Deterministic Replay Harness
7. `HFT-CP6` Execution Fast Path
8. `HFT-CP7` Production Operations Layer

## Rules
1. Rust-only hot path is mandatory target architecture.
2. `forward` runtime path on trading host is Rust-internal; Python/Ray is allowed only for offline/cold tasks (including transitional `scout`) and must not load trading-host data-plane CPU budget.
3. Any status update must reference concrete `HFT-CP*` and code/test evidence.
4. `project-math-model` stays in `dynamics` while thresholds/formulas are checkpoint-driven and still evolving; it moves to `core` only after final model freeze.
