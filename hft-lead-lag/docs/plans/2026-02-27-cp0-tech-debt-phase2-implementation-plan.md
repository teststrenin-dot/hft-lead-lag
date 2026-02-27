# CP0 Tech Debt Phase 2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close remaining CP0 non-blocking debts (boundary duplication, restore-query indexing, ScreenerStore cognitive-load hotspot) without behavior changes.

**Architecture:** Keep portfolio-runtime logic domain-owned, eliminate legacy application export path, add a focused DB index for event-collapse restore path, and extract drained-trades preprocessing into a helper module while preserving `ScreenerStore` orchestration.

**Tech Stack:** Rust (`axum`, `tokio`, `rusqlite`), SQLite schema migrations via startup `open_db()`, cargo test.

---

### Task 1: Add failing DB-index regression test

**Files:**
- Modify: `src/infrastructure/db.rs`
- Test: `src/infrastructure/db.rs` (existing tests module)

**Step 1: Write failing test**
- Add test asserting `idx_trades_symbol_exit_ts` exists in `sqlite_master`/`pragma_index_list` after `open_db()`.

**Step 2: Run targeted test (RED)**
- Run: `cargo test open_db_creates_candidate_restore_event_index -- --nocapture`
- Expected: FAIL (index missing).

**Step 3: Minimal implementation**
- Add `CREATE INDEX IF NOT EXISTS idx_trades_symbol_exit_ts ON trades(symbol, exit_ts_ms);` to schema.

**Step 4: Re-run targeted test (GREEN)**
- Run: `cargo test open_db_creates_candidate_restore_event_index -- --nocapture`
- Expected: PASS.

### Task 2: Remove application re-export surface for portfolio runtime

**Files:**
- Delete: `src/application/services/portfolio_runtime.rs`
- Modify: `src/application/services/mod.rs`
- Modify: `src/api/handlers.rs`
- Create: `src/domain/screener/portfolio_runtime_tests.rs`
- Modify: `src/domain/screener/mod.rs`
- Delete: `src/application/services/portfolio_runtime_tests.rs`

**Step 1: Write failing migration test signal**
- Keep tests compiling by moving runtime tests into domain module and removing app-module wiring.

**Step 2: Run targeted tests (RED/GREEN cycle)**
- Run: `cargo test portfolio_runtime_eligible_requires_all_v1_thresholds -- --nocapture`
- Expected RED during module migration, then GREEN after wiring updates.

**Step 3: Minimal implementation**
- Remove app exports for portfolio-runtime.
- Switch imports in API from `application::services` to `domain::screener::portfolio_runtime` for candidate math helpers.

**Step 4: Re-run targeted tests (GREEN)**
- Run: `cargo test portfolio_runtime_ -- --nocapture`
- Expected: PASS for moved runtime tests.

### Task 3: Extract drained-trades preprocessing helper

**Files:**
- Create: `src/domain/screener/drained_trades.rs`
- Modify: `src/domain/screener/mod.rs`
- Test: `src/domain/screener/drained_trades.rs` (unit tests)

**Step 1: Write helper tests first**
- Add tests for:
  - deterministic sort ordering,
  - active-run filtering,
  - candidate event collapse by `(symbol, exit_ts_ms)`.

**Step 2: Run targeted tests (RED)**
- Run: `cargo test drained_trades -- --nocapture`
- Expected: FAIL until helper functions implemented.

**Step 3: Minimal implementation**
- Implement helper functions and integrate into `handle_drained_fleet_trades()`.

**Step 4: Re-run targeted tests (GREEN)**
- Run: `cargo test drained_trades -- --nocapture`
- Expected: PASS.

### Task 4: Verify no regressions

**Files:**
- Verify existing tests only.

**Step 1: Run targeted regression set**
- `cargo test load_portfolio_candidate_history_v1_aggregates_trade_history -- --nocapture`
- `cargo test drained_trades_collapse_same_symbol_timestamp_for_candidate_math -- --nocapture`
- `cargo test portfolio_active_endpoint_falls_back_to_db_state_snapshot -- --nocapture`

**Step 2: Run full suite**
- `cargo test`

### Task 5: Commit and push

**Step 1: Commit**
- Message: `Close CP0 residual tech debt (index, boundaries, screener decomposition)`

**Step 2: Push**
- Push branch `main` to `origin/main`.
