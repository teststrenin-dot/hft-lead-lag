# R5 — Screener Design Review

## Strengths
1. Catalog/cache path стал более целевым: `ArcSwap` snapshot + rebuild throttling снижает read-path нагрузку.
2. Fleet patch semantics стали прозрачнее: централизованный `FleetPatchPlan`, richer `FleetReloadReport`, отдельные policy views.

## Findings
- **P2** Snapshot throttling может отдавать stale rows до `ROWS_CACHE_MIN_REBUILD_INTERVAL_MS`, даже при dirty-состоянии.
  - Это скорее дизайн-трейд-офф, но его нужно явно отражать в operator expectations.
  - Refs:
    - `src/domain/screener/mod.rs:42`
    - `src/domain/screener/catalog_cache.rs:95-126`

## Verdict
- Дизайн screener улучшился и стал понятнее эксплуатационно, но есть компромисс по свежести read-model при агрессивном polling.
