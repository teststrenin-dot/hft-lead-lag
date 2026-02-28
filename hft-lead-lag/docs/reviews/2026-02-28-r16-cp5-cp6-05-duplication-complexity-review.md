# R16 - Duplication, Complexity, Over-Engineering Review

Date: 2026-02-28

## Findings

### P2
1. Latency snapshot logic is duplicated across event-loop modules instead of shared utility.
- Evidence: `src/event_loop_core.rs:203`, `src/event_loop_execution.rs:99`.

### P3
1. CP6 carries full `StrategySignal` in `OrderIntent` while worker currently does not use most of it.
- Evidence: `src/event_loop_core.rs:695`, `src/event_loop_execution.rs:22`, `src/event_loop_execution.rs:294`.
- Impact: extra payload and cognitive noise in hot path.
