# Master Remediation Program Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement a full, phased remediation across reliability, correctness, architecture, Screener/Fleet design tracks, and cleanup with strict verification gates.

**Architecture:** Work is split into waves with controlled batching: stabilize runtime first, then harden correctness and math, then perform structural decomposition. Screener and Shadow Fleet are executed as separate design+implementation tracks to avoid coupled regressions.

**Tech Stack:** Rust 2021, Tokio, Axum, DashMap, Rusqlite, tracing, cargo test, cargo clippy.

---

### Task 1: Event-Loop Hot Path De-Amplification (P0)

**Files:**
- Modify: `src/main.rs`
- Test: `src/main.rs`

**Step 1: Write failing tests**
- Add test that `process_exchange_batch` ingests only current batch symbols and does not re-emit all cached symbols each tick.
- Add test that repeated single-symbol updates do not inflate ticker_count as if whole map was reprocessed.

**Step 2: Run test to verify it fails**
Run: `cargo test process_exchange_batch_ -- --nocapture`
Expected: FAIL with current map-wide re-ingest behavior.

**Step 3: Implement minimal fix**
- Replace full-map ingest on every tick with batch-only ingest while still keeping latest snapshot map up to date.
- Preserve strategy update ordering and dedupe semantics.

**Step 4: Run focused tests**
Run: `cargo test process_exchange_batch_ ingest_latest_batch_ -- --nocapture`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add src/main.rs
git commit -m "fix(runtime): ingest only updated symbols per exchange batch"
```

### Task 2: Backpressure Hardening in Exchange Ingestion (P0)

**Files:**
- Modify: `src/infrastructure/exchanges/binance/mod.rs`
- Modify: `src/infrastructure/exchanges/gate/mod.rs`
- Test: `src/infrastructure/exchanges/binance/mod.rs`
- Test: `src/infrastructure/exchanges/gate/mod.rs`

**Step 1: Write failing tests**
- Add targeted tests for overflow policy helper behavior (drop/replace semantics at bounded channel pressure).

**Step 2: Run test to verify it fails**
Run: `cargo test dropped_messages -- --nocapture`
Expected: FAIL before helper policy extraction.

**Step 3: Implement minimal fix**
- Introduce explicit overflow-policy helper for message fan-in (oldest-first drain or bounded coalescing per symbol where feasible).
- Keep warning cadence controlled (power-of-two + periodic thresholds).

**Step 4: Run focused tests**
Run: `cargo test infrastructure::exchanges::binance::tests infrastructure::exchanges::gate::tests -- --nocapture`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add src/infrastructure/exchanges/binance/mod.rs src/infrastructure/exchanges/gate/mod.rs
git commit -m "fix(reliability): harden WS ingestion backpressure behavior"
```

### Task 3: Runtime Health/SLO Signal Consolidation (P0)

**Files:**
- Modify: `src/api/handlers.rs`
- Modify: `src/api/http_server.rs`
- Modify: `src/main.rs`
- Test: `src/api/handlers.rs`
- Test: `src/main.rs`

**Step 1: Write failing tests**
- Add tests for health status transitions under stale feed and sustained drop counters.
- Add tests for issue-classification stability.

**Step 2: Run test to verify it fails**
Run: `cargo test health_ exchange_side_ -- --nocapture`
Expected: FAIL before consolidation.

**Step 3: Implement minimal fix**
- Normalize health issue taxonomy.
- Ensure runtime marks state transitions consistently and idempotently.

**Step 4: Run focused tests**
Run: `cargo test api::handlers::tests tests::exchange_side_ -- --nocapture`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add src/api/handlers.rs src/api/http_server.rs src/main.rs
git commit -m "fix(health): consolidate degradation and issue taxonomy"
```

### Task 4: DB Schema Compatibility Guard (P0)

**Files:**
- Modify: `src/infrastructure/db.rs`
- Test: `src/infrastructure/db.rs`

**Step 1: Write failing tests**
- Add migration test reproducing startup failure when expected columns differ (e.g., strategy metadata drift).

**Step 2: Run test to verify it fails**
Run: `cargo test open_db -- --nocapture`
Expected: FAIL on reproduced legacy schema drift.

**Step 3: Implement minimal fix**
- Add explicit schema compatibility checks + safe migration path.
- Emit actionable error when unrecoverable mismatch detected.

**Step 4: Run focused tests**
Run: `cargo test infrastructure::db:: -- --nocapture`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add src/infrastructure/db.rs
git commit -m "fix(db): add schema compatibility guard for startup"
```

### Task 5: Math Invariants Regression Suite (P0/P1)

**Files:**
- Modify: `src/domain/screener/shadow_fleet.rs`
- Modify: `src/domain/screener/shadow_trader.rs`
- Modify: `src/domain/screener/utils.rs`
- Test: same files

**Step 1: Write failing tests**
- Add tests for time decay boundaries, expectancy edge thresholds, and timestamp normalization corner cases.

**Step 2: Run test to verify it fails**
Run: `cargo test shadow_fleet::tests shadow_trader::tests utils::tests -- --nocapture`
Expected: FAIL for missing invariants.

**Step 3: Implement minimal fix**
- Enforce invariant-safe calculations and tolerance policy for numeric drift.

**Step 4: Run focused tests**
Run: `cargo test domain::screener:: -- --nocapture`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add src/domain/screener/shadow_fleet.rs src/domain/screener/shadow_trader.rs src/domain/screener/utils.rs
git commit -m "test(math): lock screener/fleet invariants and edge cases"
```

### Task 6: Main Runtime Decomposition (P1)

**Files:**
- Create: `src/runtime/mod.rs`
- Create: `src/runtime/event_loop.rs`
- Create: `src/runtime/bootstrap.rs`
- Modify: `src/main.rs`
- Test: `src/main.rs`, `src/runtime/event_loop.rs`

**Step 1: Write failing tests**
- Add behavior-preserving tests for extracted runtime orchestrator functions.

**Step 2: Run test to verify it fails**
Run: `cargo test event_loop_state_ runtime_ -- --nocapture`
Expected: FAIL until extraction wiring is complete.

**Step 3: Implement minimal fix**
- Move orchestration helpers out of `main.rs` without changing externally observable behavior.

**Step 4: Run focused tests**
Run: `cargo test tests::event_loop_ -- --nocapture`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add src/main.rs src/runtime/mod.rs src/runtime/event_loop.rs src/runtime/bootstrap.rs
git commit -m "refactor(runtime): split main orchestration into runtime modules"
```

### Task 7: Screener Design Track Implementation Pack (separate)

**Files:**
- Create: `docs/plans/2026-02-20-screener-design.md`
- Modify: `src/domain/screener/mod.rs`
- Modify: `src/domain/screener/state.rs`
- Modify: `src/api/handlers.rs`
- Test: `src/domain/screener/mod.rs`, `src/domain/screener/state.rs`

**Step 1: Write failing tests**
- Add tests for screener state flow boundaries and row projection invariants.

**Step 2: Run test to verify it fails**
Run: `cargo test screener -- --nocapture`
Expected: FAIL before boundary cleanup.

**Step 3: Implement minimal fix**
- Separate update pipeline stages and reduce hidden side effects.

**Step 4: Run focused tests**
Run: `cargo test domain::screener:: api::handlers::tests::get_screener -- --nocapture`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add docs/plans/2026-02-20-screener-design.md src/domain/screener/mod.rs src/domain/screener/state.rs src/api/handlers.rs
git commit -m "refactor(screener): clarify state/update boundaries and projection invariants"
```

### Task 8: Shadow Fleet Design Track Implementation Pack (separate)

**Files:**
- Create: `docs/plans/2026-02-20-shadow-fleet-design.md`
- Modify: `src/domain/screener/trader_config.rs`
- Modify: `src/domain/screener/shadow_trader.rs`
- Modify: `src/domain/screener/shadow_fleet.rs`
- Modify: `src/infrastructure/db.rs`
- Test: same files

**Step 1: Write failing tests**
- Add tests for strategy-kind extensibility seam, policy gates, and config id stability.

**Step 2: Run test to verify it fails**
Run: `cargo test shadow_fleet shadow_trader trader_config -- --nocapture`
Expected: FAIL before seam implementation.

**Step 3: Implement minimal fix**
- Introduce low-coupling extension points without breaking current baseline behavior.

**Step 4: Run focused tests**
Run: `cargo test domain::screener::shadow_ -- --nocapture`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add docs/plans/2026-02-20-shadow-fleet-design.md src/domain/screener/trader_config.rs src/domain/screener/shadow_trader.rs src/domain/screener/shadow_fleet.rs src/infrastructure/db.rs
git commit -m "refactor(fleet): prepare extensible strategy seams with invariant safety"
```

### Task 9: Dead Code and Duplication Cleanup (P1/P2)

**Files:**
- Modify: `src/main.rs`
- Modify: `src/api/mod.rs`
- Modify: `src/infrastructure/mod.rs`
- Modify: `src/domain/screener/*` (targeted)
- Test: affected modules

**Step 1: Write failing tests**
- Add tests that capture behavior before removing duplicate helper paths.

**Step 2: Run test to verify it fails**
Run: `cargo test --all --all-features -- --nocapture`
Expected: At least one targeted fail before cleanup changes.

**Step 3: Implement minimal fix**
- Remove dead/duplicate code paths and centralize shared helper logic.

**Step 4: Run focused tests**
Run: `cargo test --all --all-features`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add src/main.rs src/api/mod.rs src/infrastructure/mod.rs src/domain/screener
git commit -m "refactor(cleanup): remove dead paths and consolidate duplicates"
```

### Task 10: Preventive Architecture Gates (P1/P2)

**Files:**
- Create: `scripts/verify-local.sh`
- Modify: `docs/README.md`
- Modify: `Cargo.toml` (if needed for lint/test profiles)

**Step 1: Write failing tests/checks**
- Add CI-like script that fails on lint/test/doc-reference drift.

**Step 2: Run checks to verify fail baseline**
Run: `bash scripts/verify-local.sh`
Expected: FAIL until all checks wired.

**Step 3: Implement minimal fix**
- Wire deterministic checks for tests, clippy, and documentation reference integrity.

**Step 4: Run checks to verify pass**
Run: `bash scripts/verify-local.sh`
Expected: PASS.

**Step 5: Commit**
Run:
```bash
git add scripts/verify-local.sh docs/README.md Cargo.toml
git commit -m "chore(quality): add local verification gate script"
```

### Task 11: Full Verification Gate

**Files:**
- Modify: none

**Step 1: Run full tests**
Run: `cargo test --all --all-features`
Expected: PASS.

**Step 2: Run strict clippy**
Run: `cargo clippy --all-targets --all-features -- -D warnings`
Expected: PASS.

**Step 3: Run format check (non-blocking if pre-existing drift outside scope)**
Run: `cargo fmt --all -- --check`
Expected: PASS or explicitly documented known pre-existing diffs.

**Step 4: Capture status**
Run: `git status --short`
Expected: only intended files changed.

### Task 12: Final Remediation Report

**Files:**
- Create: `docs/plans/2026-02-20-master-remediation-report.md`

**Step 1: Summarize completed tasks and evidence**
- Include test/lint outputs and runtime impact notes.

**Step 2: Capture residual risks**
- List known non-P0 risks and operational follow-ups.

**Step 3: Commit report**
Run:
```bash
git add docs/plans/2026-02-20-master-remediation-report.md
git commit -m "docs: add master remediation implementation report"
```
