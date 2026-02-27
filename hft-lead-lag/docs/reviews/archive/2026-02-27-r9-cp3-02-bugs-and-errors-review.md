# R9 CP3 - Bugs and Errors Review

Date: 2026-02-27

## Findings

### P1
1. Candidate age source differs between live and restore paths.
- Live accumulation: close timestamp (`ts_ms`) contributes first-observed age basis.
- Restore: DB uses `MIN(entry_ts_ms)`.
- Evidence: `src/domain/screener/mod.rs:107`, `src/domain/screener/mod.rs:648`, `src/domain/screener/mod.rs:520`, `src/infrastructure/db.rs:741`, `src/application/services/portfolio_runtime.rs:141`.
- Impact: eligibility drift after restart.
- Status: `open`.

2. Candidate aggregation is not run-scoped.
- Evidence: `src/infrastructure/db.rs:735`, `src/domain/screener/mod.rs:647`, `src/api/handlers.rs:308`.
- Impact: trials/forward traffic can mix and distort CP3 candidate math.
- Status: `open`.

### P2
1. Startup candidate-history restore has unbounded `GROUP BY symbol` over full `trades` table.
- Evidence: `src/infrastructure/db.rs:735`, `src/runtime_setup.rs:236`.
- Impact: cold-start degradation as history grows.
- Status: `open`.

2. Candidate snapshot is assembled from independent maps without atomic snapshot boundary.
- Evidence: `src/domain/screener/mod.rs:152`, `src/domain/screener/mod.rs:153`, `src/domain/screener/mod.rs:799`.
- Impact: possible per-request inconsistency under concurrent prune/update.
- Status: `open`.
