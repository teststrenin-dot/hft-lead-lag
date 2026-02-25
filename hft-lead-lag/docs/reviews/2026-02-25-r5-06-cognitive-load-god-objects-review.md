# R5 — Cognitive Load & God Objects Review

## Main Trend
- Декомпозиция успешна: глобальная нагрузка на `main.rs` резко снижена.

## Current Hotspots
- **P2** `api/handlers.rs` остается крупным концентратором endpoint-логики, SQL-склейки и cache/health orchestration.
  - Refs: `src/api/handlers.rs:111-984`, `src/api/handlers.rs:40-86`

- **P2** `runtime_hot_reload.rs` держит 3 watcher/control loops + shared state orchestration.
  - Refs: `src/runtime_hot_reload.rs:257-427`

- **P3** `ScreenerStore` остается многоответственным объектом (state + cache + patch + policy views + writer integration).
  - Refs: `src/domain/screener/mod.rs:84-351`

## Snapshot Metrics
- `main.rs`: `1981 -> 248`
- `api/handlers.rs`: `1336 -> 996`
- `domain/screener/mod.rs`: `767 -> 362`
- New concentration: `infrastructure/db.rs` (`1203` LOC), `runtime_hot_reload.rs` (`428` LOC)

## Recommendation
- Следующая фаза: резать `handlers` и `runtime_hot_reload` на bounded slices, а не возвращаться к крупным sweep-рефакторам.
