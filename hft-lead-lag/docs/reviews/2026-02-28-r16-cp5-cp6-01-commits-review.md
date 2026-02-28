# R16 - Commits Review

Date: 2026-02-28
Range: `201052c..06f9eca`

## Findings

### P1
1. CP5 recorder/replay contract is internally brittle: recorder writes can break the strict monotonic sequence invariant required by reader.
- Evidence: `src/infrastructure/replay/raw_feed.rs:70`, `src/infrastructure/replay/raw_feed.rs:77`, `src/infrastructure/replay/raw_feed.rs:120`.

2. CP6 execution queue policy under pressure favors stale intents over fresh ones.
- Evidence: `src/event_loop_execution.rs:149`, `src/event_loop_execution.rs:157`, `src/event_loop_execution.rs:219`.

### P2
1. CP6 telemetry depth metric has concurrency skew risk.
- Evidence: `src/event_loop_execution.rs:149`, `src/event_loop_execution.rs:151`, `src/event_loop_execution.rs:210`.

2. CP5 replay path does not enforce full runtime strategy-config parity during determinism check.
- Evidence: `src/infrastructure/replay/raw_feed.rs:217`, `src/main.rs:220`.

### P3
1. Commit stream includes significant docs-only moves and status churn; technical effect is low, but review/read overhead is high.
- Evidence: `f904fe4`, `d7c879c`, `a769383`.
