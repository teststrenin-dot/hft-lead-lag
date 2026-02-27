# R11 CP1 - Cognitive Load and God Objects Review

Date: 2026-02-27

## Findings

### P2
1. `ScreenerStore` remains high-coupling (ingest, clock correction, portfolio orchestration, patching, caching, DB dispatch).
- Evidence: `src/domain/screener/mod.rs:156`, `src/domain/screener/mod.rs:313`, `src/domain/screener/mod.rs:640`, `src/domain/screener/mod.rs:971`, `src/domain/screener/mod.rs:1024`.
- Status: `open`.

### P3
1. `update_symbol_state_and_drain_trades` combines multiple responsibilities in one routine.
- Evidence: `src/domain/screener/quote_ingest.rs:12`.
- Status: `open`.
