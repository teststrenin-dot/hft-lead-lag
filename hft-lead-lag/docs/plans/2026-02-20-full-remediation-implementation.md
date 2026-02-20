# Full Remediation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement all high-priority remediation items from the comprehensive review: reliability, health observability, policy math freshness, and REST parsing correctness.

**Architecture:** Changes are split into independent tracks so they can be developed and validated separately: (1) policy-score math freshness in domain layer, (2) REST parsing correctness in infrastructure layer, (3) runtime health/backpressure visibility and safer DB writer behavior in runtime/API/infra integration. Each track is test-first and merged only after targeted verification and full-suite verification.

**Tech Stack:** Rust 2021, Tokio, Axum, DashMap, Rusqlite, cargo test/clippy.

---

### Task 1: Policy Decay-To-Now (Domain Math)

**Files:**
- Modify: `src/domain/screener/shadow_fleet.rs`
- Test: `src/domain/screener/shadow_fleet.rs`

**Step 1: Write failing tests**
- Add test that old policy observations decay in snapshot when no new trades arrive.

**Step 2: Run test to verify it fails**
Run: `cargo test policy_snapshot_decays_old_observations_without_new_trades -- --nocapture`
Expected: FAIL because current snapshot does not decay to now.

**Step 3: Implement minimal fix**
- Add `metrics_at(ts_ms)` in decayed window logic.
- Use current time in policy snapshot/scoring so stale configs decay naturally.

**Step 4: Run focused tests**
Run: `cargo test shadow_fleet::tests -- --nocapture`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add src/domain/screener/shadow_fleet.rs
git commit -m "fix(policy): decay fleet windows to current time in snapshots"
```

### Task 2: Binance REST lastPrice Parsing Correctness

**Files:**
- Modify: `src/infrastructure/rest/mod.rs`
- Test: `src/infrastructure/rest/mod.rs`

**Step 1: Write failing tests**
- Add parser-focused test that Binance ticker with `lastPrice` populates `last_price`.

**Step 2: Run test to verify it fails**
Run: `cargo test parse_binance_ticker_reads_last_price_field -- --nocapture`
Expected: FAIL because current code reads only `last`.

**Step 3: Implement minimal fix**
- Extract Binance ticker parser helper.
- Parse `lastPrice` and fallback to `last`.

**Step 4: Run focused tests**
Run: `cargo test infrastructure::rest::tests -- --nocapture`
Expected: PASS (excluding ignored live tests).

**Step 5: Commit**
Run:
```bash
git add src/infrastructure/rest/mod.rs
git commit -m "fix(rest): parse Binance lastPrice in ticker snapshots"
```

### Task 3: Health Degradation + Drop Telemetry

**Files:**
- Modify: `src/api/http_server.rs`
- Modify: `src/api/handlers.rs`
- Modify: `src/main.rs`
- Modify: `src/infrastructure/db.rs`
- Test: `src/api/handlers.rs`
- Test: `src/main.rs`

**Step 1: Write failing tests**
- Add health handler tests for stale tick timestamps and dropped-message counters.
- Add event-loop tests for health timestamp updates on successful data path.

**Step 2: Run tests to verify they fail**
Run:
- `cargo test health_ -- --nocapture`
- `cargo test event_loop_state_ -- --nocapture`
Expected: FAIL before implementation.

**Step 3: Implement minimal fix**
- Extend `HealthState` with per-exchange last-tick timestamps.
- Update event loop to mark live/degraded exchange health from data path outcomes.
- Expose dropped counters (`binance/gate/db`) and staleness in `/health`.
- Change `DbWriter::send` full-channel behavior from immediate drop to async retry path, and drop only on closed/no-runtime fallback.

**Step 4: Run focused tests**
Run:
- `cargo test api::handlers::tests -- --nocapture`
- `cargo test tests::event_loop_state_ -- --nocapture`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add src/api/http_server.rs src/api/handlers.rs src/main.rs src/infrastructure/db.rs
git commit -m "fix(reliability): degrade health on stale feeds and surface drop counters"
```

### Task 4: Final Verification Gate

**Files:**
- Modify: none (verification only)

**Step 1: Run full tests**
Run: `cargo test --all --all-features`
Expected: PASS.

**Step 2: Run lint gate**
Run: `cargo clippy --all-targets --all-features -- -D warnings`
Expected: PASS.

**Step 3: Inspect workspace status**
Run: `git status --short`
Expected: only intended files changed.

**Step 4: Commit verification evidence**
Run:
```bash
git log --oneline -n 5
```
Expected: remediation commits visible.
