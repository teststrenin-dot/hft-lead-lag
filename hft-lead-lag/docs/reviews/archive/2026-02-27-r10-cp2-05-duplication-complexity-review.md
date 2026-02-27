# R10 CP2 - Duplication, Redundancy, and Complexity Review

Date: 2026-02-27

## Findings

### P2
1. Event batch is decoded/traversed twice in ingest flow.
- Evidence: `src/event_loop_core.rs:219`, `src/event_loop_ingest.rs:12`, `src/event_loop_ingest.rs:71`.
- Impact: avoidable allocation/cpu overhead.
- Status: `open`.

### P3
1. Repeated quote-validation/spread math across layers increases drift risk.
- Evidence: `src/domain/screener/quote_ingest.rs:89`, `src/domain/screener/state.rs:77`, `src/domain/screener/shadow_trader.rs:403`.
- Status: `open`.

2. Baseline window configurable, sample retention fixed (silent truncation for larger windows).
- Evidence: `src/domain/screener/shadow_trader.rs:444`, `src/domain/screener/price_samples.rs:5`.
- Status: `open`.
