# R16 - Architecture and Design Review

Date: 2026-02-28

## Findings

### P1
1. Queue-full strategy is not HFT-safe: dropping newest intents while preserving older ones.
- Evidence: `src/event_loop_execution.rs:149`, `src/event_loop_execution.rs:157`, `src/event_loop_execution.rs:219`.
- Design gap: no latest-wins coalescing or max-intent-age control.

### P2
1. Replay determinism architecture currently validates logic on partial config reconstruction.
- Evidence: `src/infrastructure/replay/raw_feed.rs:217`, `src/main.rs:220`, `src/main.rs:235`.
- Design gap: replay should bind to full runtime strategy config snapshot/hash.

### P3
1. `HealthState` grows as a single telemetry god-object, increasing coupling and change risk.
- Evidence: `src/api/http_server.rs:36`, `src/event_loop_core.rs:203`, `src/event_loop_execution.rs:99`.
