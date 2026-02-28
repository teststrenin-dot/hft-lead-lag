# R16 - Preventive Architecture Review

Date: 2026-02-28

## Findings

### P1
1. CP5 strict reader + non-atomic recorder sequencing creates a preventive-architecture inversion: validation is strict, producer contract is weak.
- Evidence: `src/infrastructure/replay/raw_feed.rs:70`, `src/infrastructure/replay/raw_feed.rs:120`.

### P2
1. Missing explicit recovery state machine for execution kill-switch.
- Evidence: `src/event_loop_execution.rs:245`, `src/event_loop_execution.rs:247`.
- Preventive remediation: `Closed -> Tripped -> Probing -> Closed` with cooldown and operator reset.

2. Missing negative tests for replay reader invariant failures (invalid JSON/out-of-order sequence).
- Evidence: `src/infrastructure/replay/raw_feed.rs:348`, `src/infrastructure/replay/raw_feed.rs:371`, `src/infrastructure/replay/raw_feed.rs:395`.
