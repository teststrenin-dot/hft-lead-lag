# R7 — Screener Design Review

Date: 2026-02-26
Scope: `src/domain/screener/*` and runtime hooks.

## Findings

### P1
1. Hot-path complexity scales as `symbols × configs × samples`.
- Evidence: `src/domain/screener/shadow_fleet.rs:436`, `src/domain/screener/shadow_trader.rs:417`, `:433`.
- Impact: latency spikes/event-loop starvation under load.

2. Reset-path drained trades bypass normal in-memory portfolio accounting path.
- Evidence: `src/domain/screener/fleet_reload.rs:69`, `:73` vs `src/domain/screener/quote_ingest.rs:115`.

3. Config generation switch can be non-atomic relative to live ingest.
- Evidence: `src/domain/screener/fleet_reload.rs:35`, `:42`; `src/domain/screener/quote_ingest.rs:59`.

### P2
1. Config validation missing at design boundary.
- Evidence: `src/domain/screener/trader_config.rs:12`; usage in `shadow_trader.rs`.

2. Repeated percentile/sort work in critical loop.
- Evidence: `src/domain/screener/utils.rs:9`, `src/domain/screener/cycle_tracker.rs:28`, `:32`.

### P3
1. Deterministic testability limited by direct wall-clock calls in core path.
- Evidence: `src/domain/screener/utils.rs:26`, `src/domain/screener/quote_ingest.rs:93`.
