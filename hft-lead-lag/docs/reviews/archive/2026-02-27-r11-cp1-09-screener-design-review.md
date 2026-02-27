# R11 CP1 - Screener Design Review

Date: 2026-02-27

## Findings

### P1
1. Partial-book symbols are omitted from `ws_live`, while fallback is only used when all live rows are empty.
- Evidence: `src/domain/screener/quote_ingest.rs:49`, `src/domain/screener/catalog_cache.rs:117`, `src/api/handlers.rs:226`, `src/api/handlers.rs:249`.
- Status: `open`.

2. Side ordering can be poisoned by a future/extreme timestamp, causing prolonged valid-tick rejection on that side.
- Evidence: `src/infrastructure/exchanges/binance/mod.rs:165`, `src/infrastructure/exchanges/gate/mod.rs:556`, `src/domain/screener/utils.rs:86`, `src/domain/screener/state.rs:83`.
- Status: `open`.

### P2
1. `ws_live` semantics are ambiguous for one-sided staleness (both sides seen once != both fresh now).
- Evidence: `src/domain/screener/state.rs:130`, `src/domain/screener/state.rs:144`, `src/domain/screener/catalog_cache.rs:124`, `src/api/handlers/health_support.rs:59`.
- Status: `open`.

2. Same columns shift meaning between `ws_live` and `rest_fallback` without explicit schema distinction.
- Evidence: `src/domain/screener/state.rs:144`, `src/infrastructure/enrichment.rs:137`, `src/infrastructure/enrichment.rs:145`, `src/api/templates/screener.html:35`.
- Status: `open`.
