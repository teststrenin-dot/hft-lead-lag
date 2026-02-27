# Delivery Playbook — Outcome First, Contract First

Date: 2026-02-26
Scope: mandatory workflow for `HFT-CP*` delivery.
Strategic anchor: `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
Checkpoint set: `docs/status/core/2026-02-28-hft-rust-only-checkpoints.md`
Last delivery sync: 2026-02-28 (`HFT-CP` re-baseline)

## 0) Strategic pre-check (mandatory)
Before any implementation:
1. Declare target economic node(s).
2. Declare KPI impact.
3. Declare reduced risk.
4. Declare failure mode if skipped.

## 1) Architecture guardrail (mandatory)
1. Runtime hot path is Rust-only.
2. Python is prohibited in target data-plane.
3. Transitional legacy components must include removal gate.

## 2) Delivery order (mandatory)
1. Strategic pre-check.
2. Outcome + DoD.
3. Scope and quality gates.
4. Contracts and payload formats.
5. Failing tests/scenarios.
6. Minimal implementation to green.
7. Refactor only after green.

## 3) Required artifacts per checkpoint
1. Spec (goal/scope/DoD).
2. Contracts (interfaces/events/state).
3. Tests (unit/integration/scenario).
4. Verification report (pass/fail/open risks).

## 4) Current HFT mapping
1. `HFT-CP0`: latency/allocation observability.
2. `HFT-CP1`: `SymbolId` and allocation removal.
3. `HFT-CP2`: lock-free strategy state.
4. `HFT-CP3`: updated-only event processing.
5. `HFT-CP4`: minimal-copy parse path.
6. `HFT-CP5`: deterministic replay harness.
7. `HFT-CP6`: execution fast path.
8. `HFT-CP7`: production ops and recovery.

## 5) Working agreement
All work follows:
`Strategic Pre-Check -> Outcome -> Contracts -> Tests -> Minimal Implementation -> Verification`.
