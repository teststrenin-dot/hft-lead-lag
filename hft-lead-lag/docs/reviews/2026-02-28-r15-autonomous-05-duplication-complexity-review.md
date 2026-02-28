# R15 Autonomous - Duplication and Complexity Review

Date: 2026-02-28

## Findings

### P3
1. `PendingSymbolSet::pop_first` has duplicated scan/remove blocks (forward range + wrapped range).
- Evidence: `src/event_loop_core.rs:70`, `src/event_loop_core.rs:87`.
- Impact: higher cognitive and maintenance cost in hot utility.

2. Queue tuple includes unused `symbol_id` at consume site.
- Evidence: `src/event_loop_core.rs:245`, `src/event_loop_core.rs:650`.
- Impact: redundant data carried through queue path.

3. Multiple status docs repeat same checkpoint assertions; risk of drift remains despite current alignment.
- Evidence: `docs/status/core/2026-02-26-business-logic-roadmap.md:22`, `docs/status/dynamics/2026-02-26-business-logic-v1-implementation-status.md:28`, `docs/status/dynamics/2026-02-28-hft-checkpoint-readiness-breakdown.md:44`.
