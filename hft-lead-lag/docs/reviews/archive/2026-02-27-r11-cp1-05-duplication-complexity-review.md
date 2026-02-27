# R11 CP1 - Duplication, Redundancy, and Complexity Review

Date: 2026-02-27

## Findings

### P2
1. Portfolio trade-to-runtime update logic is duplicated in two paths.
- Evidence: `src/domain/screener/mod.rs:594`, `src/domain/screener/mod.rs:693`.
- Status: `open`.

### P3
1. Drift logic is split between event-loop metrics and screener time-domain utilities.
- Evidence: `src/event_loop_core.rs:24`, `src/domain/screener/utils.rs:86`.
- Status: `open`.
