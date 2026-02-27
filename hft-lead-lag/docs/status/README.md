# Status Docs Index (`HFT-CP` Baseline)

Date: 2026-02-28
Status: active

## Purpose
Single entry point for project status, checkpoint tracking, and operating constraints.

## Folder layout
1. `core/` — business objective, operating model, roadmap, implementation status, checkpoint ladder.
2. `dynamics/` — delivery workflow, contracts freeze, math model.

## Read order (canonical)
1. `core/2026-02-27-business-objective-economic-control-map.md`
2. `core/2026-02-28-hft-rust-only-checkpoints.md`
3. `core/2026-02-26-business-logic-roadmap.md`
4. `core/2026-02-26-business-logic-v1-implementation-status.md`
5. `core/2026-02-27-operating-model-spec-v1.md`
6. `dynamics/2026-02-26-delivery-contract-first-playbook.md`
7. `dynamics/2026-02-27-cp0-contract-freeze-v2.md`
8. `dynamics/2026-02-26-project-math-model.md`

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
