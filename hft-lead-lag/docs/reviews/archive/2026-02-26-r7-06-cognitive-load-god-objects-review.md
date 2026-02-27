# R7 — Cognitive Load and God Objects Review

Date: 2026-02-26

## Findings

### P1
1. `infrastructure/db.rs` remains a major cognitive hotspot (schema+migrations+repo+writer+health).
- Evidence: `src/infrastructure/db.rs:154`, `:721`, `:762`, `:910`.

2. `api/handlers.rs` still overloaded with many concerns/endpoints.
- Evidence: `src/api/handlers.rs:47`, `:432`, `:740`, `:1174`.

3. `ScreenerStore` impl still aggregates many responsibilities.
- Evidence: `src/domain/screener/mod.rs:136`, `:293`, `:430`, `:586`.

### P2
1. `runtime_hot_reload.rs` mixes several watcher pipelines and reset semantics.
- Evidence: `src/runtime_hot_reload.rs:37`, `:279`, `:326`, `:377`.

2. `shadow_trader`/`shadow_fleet` mix runtime FSM, policy scoring, and view projection.
- Evidence: `src/domain/screener/shadow_trader.rs:417`, `:483`; `src/domain/screener/shadow_fleet.rs:424`.

## Recommended Decomposition Order
1. Split DB module by vertical concern with re-exports.
2. Split API handlers into feature modules and query helpers.
3. Split screener store impl by ingestion/runtime/read-model responsibilities.
