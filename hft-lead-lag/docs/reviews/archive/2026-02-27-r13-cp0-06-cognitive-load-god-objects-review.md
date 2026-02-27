# R13 CP0 - Cognitive Load and God Objects Review

Date: 2026-02-27

## Findings

### P1
1. `ScreenerStore` remains a god object (state, portfolio runtime, patching, persistence, caches).
- Evidence: `src/domain/screener/mod.rs:156`, `src/domain/screener/mod.rs:261`, `src/domain/screener/mod.rs:957`, `src/domain/screener/mod.rs:1024`.
- Status: `open`.

### P2
1. `api/handlers.rs` mixes DTOs, SQL-heavy reads, control-plane actions, and fallback behavior.
- Evidence: `src/api/handlers.rs:45`, `src/api/handlers.rs:183`, `src/api/handlers.rs:502`, `src/api/handlers.rs:986`.
- Status: `open`.

2. `main.rs` combines too many responsibilities for bootstrap/runtime orchestration.
- Evidence: `src/main.rs:18`, `src/main.rs:50`, `src/main.rs:137`, `src/main.rs:222`, `src/main.rs:283`.
- Status: `open`.
