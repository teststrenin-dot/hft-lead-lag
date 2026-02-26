# R6 — Cognitive Load & God Objects Review

## Main Trend
- `ScreenerStore` keeps expanding as central orchestrator for quote ingest, fleet, portfolio runtime, persistence coupling, and cache invalidation.

## Current Hotspots
- **P2** `ScreenerStore` now carries portfolio state, guards, candidate stats accumulation, rebalance cadence, and snapshot emission.
  - Refs:
    - `src/domain/screener/mod.rs:125`
    - `src/domain/screener/mod.rs:249`
    - `src/domain/screener/mod.rs:376`
    - `src/domain/screener/mod.rs:408`

- **P2** Quote ingest path combines feed ingestion + fleet ticks + portfolio trade accounting + rebalance trigger in one control flow.
  - Refs:
    - `src/domain/screener/quote_ingest.rs:19`
    - `src/domain/screener/quote_ingest.rs:90`
    - `src/domain/screener/quote_ingest.rs:106`

## Snapshot Metrics
- Large concentration of responsibilities in `src/domain/screener/mod.rs` increased cognitive branching around runtime side effects.

## Recommendation
- Split portfolio coordinator from screener symbol-store.
- Keep quote-ingest minimal and side-effect boundaries explicit (ingest -> events -> portfolio coordinator).
