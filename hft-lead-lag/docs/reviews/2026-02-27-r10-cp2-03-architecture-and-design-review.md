# R10 CP2 - Architecture and Design Review

Date: 2026-02-27

## Findings

### P1
1. Ingest path is globally serialized via patch mutex.
- Evidence: `src/domain/screener/quote_ingest.rs:96`, `src/domain/screener/mod.rs:640`.
- Impact: CP2 path latency sensitivity and reduced parallelism.
- Status: `open`.

2. Dual shadow paths (`state.shadow` and `state.fleet`) advance together but feed different surfaces.
- Evidence: `src/domain/screener/state.rs:63`, `src/domain/screener/state.rs:205`, `src/domain/screener/quote_ingest.rs:65`.
- Impact: split-brain behavior/metrics risk.
- Status: `open`.

### P2
1. Baseline detection complexity scales with configs*samples*ticks.
- Evidence: `src/domain/screener/shadow_trader.rs:449`, `src/domain/screener/quote_ingest.rs:65`.
- Status: `open`.
