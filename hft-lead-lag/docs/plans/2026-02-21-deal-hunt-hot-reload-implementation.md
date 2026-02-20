# Deal-Hunt + NATR Persistence Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Persist NATR/hold/churn context into trade storage and prepare runtime for short deal-hunt runs with periodic NATR refresh.

**Architecture:** Extend the domain trade model with entry-context fields, propagate through fleet/db writer, add DB migration-safe columns, and introduce a low-overhead runtime NATR refresher that updates symbol state used at entry time. Keep behavior backward compatible and fail-open on missing NATR data.

**Tech Stack:** Rust, Tokio, Axum, Rusqlite, existing Gate REST client.

---

### Task 1: DB schema extension for deal-hunt context

**Files:**
- Modify: `src/infrastructure/db.rs`

**Steps:**
1. Add columns to `trades` schema:
   - `gate_natr_30m_pct_at_entry REAL NOT NULL DEFAULT 0.0`
   - `hold_ms INTEGER NOT NULL DEFAULT 0`
   - `early_stop_churn INTEGER NOT NULL DEFAULT 0`
2. Add migration-safe `ALTER TABLE` attempts for older DBs.
3. Extend insert statement and parameter binding in `flush_trades`.
4. Add/extend tests asserting new columns exist and inserts work.

### Task 2: Domain model propagation of NATR/churn

**Files:**
- Modify: `src/domain/screener/shadow_trader.rs`
- Modify: `src/domain/screener/state.rs`
- Modify: `src/domain/screener/mod.rs`
- Modify: `src/domain/screener/shadow_fleet.rs`

**Steps:**
1. Add new fields to `ClosedTrade`:
   - `gate_natr_30m_pct_at_entry`
   - `hold_ms`
   - `early_stop_churn`
2. Fix ultra_govno threshold constant to `500ms`.
3. Pass NATR snapshot into `ShadowTrader::tick` and capture it at entry.
4. Compute `hold_ms` and `early_stop_churn` at exit.
5. Update all tests/fixtures that construct `ClosedTrade`.

### Task 3: Runtime NATR refresher for entry snapshots

**Files:**
- Modify: `src/main.rs`
- Modify: `src/domain/screener/mod.rs`
- Modify: `src/domain/screener/state.rs`

**Steps:**
1. Add `ScreenerStore::set_gate_natr_30m` to update symbol state.
2. Implement background task in `main.rs`:
   - periodic Gate NATR fetch,
   - bounded batch per cycle,
   - timeout per request,
   - write values into `ScreenerStore`.
3. Log refresh coverage and failures.

### Task 4: Sprint process docs for deal-hunt runs

**Files:**
- Create: `docs/sprints/sprint-008-deal-hunt-natr-db.md`

**Steps:**
1. Document run cadence:
   - run duration 10m,
   - max configs 1500,
   - prune zero-trade every 5-10m,
   - track ultra_govno share.
2. Document SQL checks and acceptance checks for freshly collected runs.

### Task 5: Verification

**Steps:**
1. Run: `cargo fmt --all`
2. Run: `cargo clippy --all-targets --all-features -- -D warnings`
3. Run: `cargo test --all --all-features`
4. Report verification and note any residual risks.
