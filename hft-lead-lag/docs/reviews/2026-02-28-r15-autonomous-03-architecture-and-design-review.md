# R15 Autonomous - Architecture and Design Review

Date: 2026-02-28

## Findings

### P1
1. Scheduler fairness model is weak for event-driven architecture under bursty skewed symbols.
- Evidence: `src/event_loop_core.rs:63`, `src/event_loop_core.rs:667`.
- Design gap: ordering policy favors low ids instead of fair rotation.

### P2
1. Event-loop queue payload still carries an unused field (`SymbolId`) in `(ExchangeSide, SymbolId, BookTicker)`.
- Evidence: `src/event_loop_core.rs:245`, `src/event_loop_core.rs:650`.
- Impact: unnecessary payload width in hot queue path.

2. Symbol cache now preserves raw bytes correctly, but lacks lifecycle controls (cap/eviction), so architecture is fast but not defensive under hostile/noisy feed.
- Evidence: `src/domain/symbols.rs:16`, `src/domain/symbols.rs:43`.

### P3
1. CP2 evidence doc marks checkpoint complete without strict comparative benchmark contract.
- Evidence: `docs/status/dynamics/2026-02-28-cp2-lock-free-p99-evidence.md:54`.
