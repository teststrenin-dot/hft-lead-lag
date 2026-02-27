# R10 CP2 - Cognitive Load and God Objects Review

Date: 2026-02-27

## Findings

### P2
1. `ScreenerStore` remains high-coupling for CP2 ingest/runtime orchestration.
- Evidence: `src/domain/screener/mod.rs:156`, `src/domain/screener/mod.rs:640`, `src/domain/screener/quote_ingest.rs:12`.
- Status: `open`.

2. `ShadowTrader` carries multiple responsibilities in one module.
- Evidence: `src/domain/screener/shadow_trader.rs:158`, `src/domain/screener/shadow_trader.rs:578`, `src/domain/screener/shadow_trader.rs:678`.
- Status: `open`.

### P3
1. CP2 row semantics around `ws_live` vs cross-exchange-valid lag are not obvious to operators.
- Evidence: `src/domain/screener/catalog_cache.rs:117`, `src/api/handlers.rs:226`.
- Status: `open`.
