# R12 CP0 - Cognitive Load and God Objects Review

Date: 2026-02-27

## Findings

### P1
1. `ScreenerStore` is still a god object across ingest/time correction/portfolio/fleet/persistence/read-model concerns.
- Evidence: `src/domain/screener/mod.rs:156`, `src/domain/screener/mod.rs:313`, `src/domain/screener/mod.rs:640`, `src/domain/screener/mod.rs:769`, `src/domain/screener/mod.rs:957`, `src/domain/screener/mod.rs:1024`.
- Status: `open`.

### P2
1. `api/handlers.rs` is a second high-coupling god module across health/symbols/portfolio/fleet/trials.
- Evidence: `src/api/handlers.rs:17`, `src/api/handlers.rs:183`, `src/api/handlers.rs:502`, `src/api/handlers.rs:775`, `src/api/handlers.rs:986`, `src/api/handlers.rs:1012`.
- Status: `open`.

### P3
1. `runtime_setup.rs` mixes unrelated setup concerns (subscriptions, enrichment, restore, API boot).
- Evidence: `src/runtime_setup.rs:17`, `src/runtime_setup.rs:128`, `src/runtime_setup.rs:223`, `src/runtime_setup.rs:271`.
- Status: `open`.
