# R16 - Logic and Math Review

Date: 2026-02-28

## Findings

### P1
1. Execution logic under overload violates freshness priority (older decision can be sent while fresher one is dropped).
- Evidence: `src/event_loop_execution.rs:149`, `src/event_loop_execution.rs:219`.
- Business impact: degrades signal quality during bursts.

### P2
1. Queue depth math is non-atomic from producer/consumer perspective and can drift.
- Evidence: `src/event_loop_execution.rs:149`, `src/event_loop_execution.rs:210`.
- Impact: misleading health/SLA interpretation.

2. Replay determinism check compares traces but not runtime-parameter equivalence, so logical "determinism" can be false-positive across config drift.
- Evidence: `src/infrastructure/replay/raw_feed.rs:217`, `src/main.rs:220`.
