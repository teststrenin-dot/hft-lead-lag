# R12 CP0 - Logic and Math Review

Date: 2026-02-27

## Findings

### P1
1. Candidate-math contract mismatch: live runtime collapses `(symbol, ts_ms)` events while restore is per-trade count/sum.
- Evidence: `docs/status/2026-02-26-project-math-model.md:351`, `src/domain/screener/mod.rs:666`, `src/domain/screener/mod.rs:112`, `src/infrastructure/db.rs:737`.
- Status: `open`.

### P2
1. Run-scoping semantics drift: live accumulation is active-run filtered, restore query is unscoped all-run.
- Evidence: `src/domain/screener/mod.rs:656`, `src/domain/screener/mod.rs:301`, `src/infrastructure/db.rs:731`.
- Status: `open`.
