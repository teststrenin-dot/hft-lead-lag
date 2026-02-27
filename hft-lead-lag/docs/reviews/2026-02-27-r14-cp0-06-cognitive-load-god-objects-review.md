# R14 CP0 - Cognitive Load and God Objects Review

Date: 2026-02-27

## Findings

### P2
1. `ScreenerStore` remains a high-load orchestrator (state + runtime + persistence pipeline).
- Evidence: `src/domain/screener/mod.rs:158`, `src/domain/screener/mod.rs:161`, `src/domain/screener/mod.rs:165`, `src/domain/screener/mod.rs:642`.
- Status: `open`.

### P3
1. CP0 fixes reduced drift risk, but did not materially reduce total object fan-in for screener core.
- Evidence: `src/domain/screener/mod.rs`.
- Status: `open`.
