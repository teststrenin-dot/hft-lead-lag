# R8 - Architecture and Design Review

Date: 2026-02-27

## Findings

### P1
1. Async HTTP handlers execute synchronous SQLite work on Tokio runtime threads.
- Evidence: `src/api/handlers.rs:505`, `src/api/handlers.rs:646`, `src/api/handlers.rs:853`, `src/api/handlers.rs:957`.
- Impact: head-of-line blocking and latency spikes under load.
- Status: `open`.

2. Portfolio snapshot persistence uses heavy `DELETE + INSERT` full rewrites.
- Evidence: `src/domain/screener/mod.rs:624`, `src/domain/screener/mod.rs:633`, `src/infrastructure/db.rs:540`, `src/infrastructure/db.rs:547`.
- Impact: write amplification, lock contention in SQLite.
- Status: `open`.

### P2
1. Data-access logic is tightly coupled inside transport layer (`handlers.rs`).
- Evidence: `src/api/handlers.rs:507`, `src/api/handlers.rs:650`, `src/api/handlers.rs:857`.
- Impact: harder testing and slower safe evolution.
- Status: `open`.

2. Fleet runtime switch and CP4 state transitions are not transactional as one architectural unit.
- Evidence: `src/domain/screener/fleet_reload.rs:17`, `src/domain/screener/quote_ingest.rs:59`.
- Impact: temporary semantic split-brain during reload.
- Status: `open`.
