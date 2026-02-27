# R8 - Cognitive Load and God Objects Review

Date: 2026-02-27

## Findings

### P2
1. `ScreenerStore` accumulates many responsibilities (ingest, runtime orchestration, persistence, caching, read-model access).
- Evidence: `src/domain/screener/mod.rs:151`, `src/domain/screener/mod.rs:568`, `src/domain/screener/mod.rs:660`, `src/domain/screener/mod.rs:848`, `src/domain/screener/mod.rs:915`.
- Impact: high coupling and costly change impact analysis.
- Status: `open`.

2. `handlers.rs` is oversized and mixes unrelated API concerns.
- Evidence: `src/api/handlers.rs:45`, `src/api/handlers.rs:183`, `src/api/handlers.rs:502`, `src/api/handlers.rs:774`, `src/api/handlers.rs:981`.
- Impact: high cognitive load and review overhead.
- Status: `open`.

3. Runtime behavior depends on implicit sequencing across modules (fleet drain -> policy update -> persistence enqueue).
- Evidence: `src/domain/screener/quote_ingest.rs:101`, `src/domain/screener/mod.rs:582`, `src/domain/screener/mod.rs:615`.
- Impact: fragile mental model and hidden regressions.
- Status: `open`.
