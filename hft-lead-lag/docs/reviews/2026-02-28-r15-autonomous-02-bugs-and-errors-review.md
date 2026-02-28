# R15 Autonomous - Bugs and Errors Review

Date: 2026-02-28

## Findings

### P1
1. Pending signal scheduler is vulnerable to starvation pattern.
- Evidence: `src/event_loop_core.rs:63`, `src/event_loop_core.rs:87`, `src/event_loop_core.rs:667`.
- Why: `pop_first()` always drains smallest id first; budgeted loop can repeatedly consume same low-id subset.

### P2
1. Raw-byte symbol cache has no bound/validation, enabling unbounded key growth.
- Evidence: `src/domain/symbols.rs:16`, `src/domain/symbols.rs:43`, `src/domain/symbols.rs:49`.
- Why: every unique byte sequence is inserted.

2. Gate contract cache mirrors same unbounded pattern.
- Evidence: `src/domain/symbols.rs:17`, `src/domain/symbols.rs:54`, `src/domain/symbols.rs:62`.

### P3
1. No runtime crash regression detected in this range by current tests (`cargo test -q` passing), but starvation/memory risks require stress validation.
