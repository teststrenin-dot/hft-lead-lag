# Status Docs Index (`HFT-CP` Baseline)

Date: 2026-02-28
Status: active
Last sync: 2026-02-28 (CP6 execution fast path closed)

## Purpose
Single entry point for project status, checkpoint tracking, and operating constraints.

## Folder layout
1. `core/` — only canonical strategy docs (exactly 3 files).
2. `dynamics/` — checkpoints, implementation status, evidence, workflow, math model.

## Read order (canonical)
1. `core/2026-02-27-business-objective-economic-control-map.md`
2. `core/2026-02-27-operating-model-spec-v1.md`
3. `core/2026-02-26-business-logic-roadmap.md`
4. `dynamics/2026-02-28-hft-rust-only-checkpoints.md`
5. `dynamics/2026-02-28-hft-checkpoint-readiness-breakdown.md`
6. `dynamics/2026-02-26-business-logic-v1-implementation-status.md`
7. `dynamics/2026-02-28-cp2-lock-free-p99-evidence.md`
8. `dynamics/2026-02-28-cp3-updated-only-proof.md`
9. `dynamics/2026-02-28-cp4-parse-path-evidence.md`
10. `dynamics/2026-02-28-cp5-block1-raw-feed-evidence.md`
11. `dynamics/2026-02-28-cp6-execution-fast-path-evidence.md`
12. `dynamics/2026-02-26-delivery-contract-first-playbook.md`
13. `dynamics/2026-02-27-cp0-contract-freeze-v2.md`
14. `dynamics/2026-02-26-project-math-model.md`

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
2. Python/Ray is allowed only for offline/cold tasks and must not load the trading host CPU budget.
3. Any status update must reference concrete `HFT-CP*` and code/test evidence.
