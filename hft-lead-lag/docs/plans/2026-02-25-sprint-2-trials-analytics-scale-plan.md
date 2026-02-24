# Sprint 2 Trials Analytics Scale Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove heavy full-table analytics scans from `/api/v1/trials` and `/api/v1/trials/axes` by introducing incremental/materialized stats, preserving response correctness.

**Architecture:** Keep existing SQL endpoints as fallback, add materialized summary tables updated in DB writer flush path, and switch handlers to prefer precomputed stats. This avoids expensive group-by scans under frequent UI polling and keeps migration reversible.

**Tech Stack:** Rust (`rusqlite`, `axum`), existing DB writer (`src/infrastructure/db.rs`), API handlers (`src/api/handlers.rs`), Trials UI polling (`src/api/templates/trials.html`).

---

## Smell Guardrails (Mandatory)

1. No duplicate aggregation logic in multiple handlers; centralize query helpers.
2. No hidden SQL contract: explicit columns, indexes, and schema tests.
3. No hard switch without fallback: materialized path must fallback to legacy queries on empty/missing data.
4. No hot-path bloat: update summaries only in DB writer flush transaction (warm path), not quote-processing hot loop.
5. No unbounded response payload growth: add clear limits/pagination where relevant.

## Out of Scope (Sprint 2)

1. Portfolio clustering math.
2. Forward orchestration changes.
3. Frontend redesign beyond needed query params/refresh behavior.

---

### Task 1: Add Materialized Stats Schema (TDD)

**Files:**
- Modify: `src/infrastructure/db.rs`

**Step 1: Write failing schema tests**

Add tests:

```rust
#[test]
fn open_db_creates_trial_run_stats_tables() {}
```

Check tables:
1. `trial_run_stats`
2. `trial_axis_stats`

**Step 2: Run test (RED)**

Run: `cargo test --lib infrastructure::db::tests::open_db_creates_trial_run_stats_tables`
Expected: FAIL.

**Step 3: Implement schema + indexes**

Add tables/indexes in `SCHEMA`:
1. `trial_run_stats` keyed by `run_id`.
2. `trial_axis_stats` keyed by `(run_id, axis_name, axis_value_bucket)`.

**Step 4: Re-run test (GREEN)**

Run: same command.
Expected: PASS.

**Step 5: Commit**

```bash
git add src/infrastructure/db.rs
git commit -m "feat: add materialized trial run/axis stats tables"
```

---

### Task 2: Incremental Stats Upsert in DB Writer (TDD)

**Files:**
- Modify: `src/infrastructure/db.rs`

**Step 1: Write failing tests for stats upsert**

Add tests that:
1. Insert trade batch via writer-flush helper.
2. Assert `trial_run_stats` updated.
3. Assert `trial_axis_stats` updated for each axis bucket.

**Step 2: Run tests (RED)**

Run: `cargo test --lib infrastructure::db::tests::trial_stats`
Expected: FAIL.

**Step 3: Implement minimal upsert**

In flush path:
1. Build in-memory aggregates from `trades` batch.
2. Upsert into `trial_run_stats`.
3. Upsert into `trial_axis_stats` using joined config params.
4. Keep operation inside existing DB transaction.

**Step 4: Re-run tests (GREEN)**

Run: `cargo test --lib infrastructure::db::tests::trial_stats`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/infrastructure/db.rs
git commit -m "feat: update trial materialized stats during db flush"
```

---

### Task 3: Switch Handlers to Materialized-First with Fallback

**Files:**
- Modify: `src/api/handlers.rs`
- Modify: `src/api/http_server.rs`

**Step 1: Write failing handler tests**

Add tests for:
1. `get_trial_runs` returns data from materialized table when present.
2. `get_trial_axes` returns materialized buckets when present.
3. Fallback to legacy queries if materialized tables empty.

**Step 2: Run tests (RED)**

Run: `cargo test --lib api::handlers::tests`
Expected: FAIL.

**Step 3: Implement query helper**

In `handlers.rs`:
1. Add small helper functions:
   - `query_trial_runs_materialized(...)`
   - `query_trial_axes_materialized(...)`
2. Use materialized-first flow; fallback to current SQL on no rows.

**Step 4: Re-run tests (GREEN)**

Run: `cargo test --lib api::handlers::tests`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/api/handlers.rs src/api/http_server.rs
git commit -m "perf: use materialized trial stats with legacy fallback"
```

---

### Task 4: API Payload and UI Poll Safety

**Files:**
- Modify: `src/api/handlers.rs`
- Modify: `src/api/templates/trials.html`

**Step 1: Write failing tests for limits**

Add tests:
1. Trials endpoint enforces sane max rows.
2. Axis endpoint supports optional `run_id` and bounded output.

**Step 2: Run tests (RED)**

Run: `cargo test --lib api::handlers::tests`
Expected: FAIL.

**Step 3: Implement limits + query parameters**

1. Add optional `limit` (bounded) to trial runs endpoint.
2. Ensure UI requests bounded results.
3. Keep current UI rendering compatibility.

**Step 4: Verify UI behavior**

Manual smoke:
1. Open `/trials`.
2. Confirm tables load and refresh.
3. Confirm no JS errors in console.

**Step 5: Commit**

```bash
git add src/api/handlers.rs src/api/templates/trials.html
git commit -m "perf: bound trials responses and align ui polling payload"
```

---

### Task 5: Observability for Materialized Path

**Files:**
- Modify: `src/infrastructure/db.rs`
- Modify: `src/api/handlers.rs`

**Step 1: Add counters/log markers**

1. Materialized hit/miss counters.
2. Fallback invocation counter.

**Step 2: Add tests for fallback branch**

Test that empty materialized stats triggers fallback query path.

**Step 3: Verify**

Run: `cargo test --lib api::handlers::tests`
Expected: PASS.

**Step 4: Commit**

```bash
git add src/infrastructure/db.rs src/api/handlers.rs
git commit -m "chore: add materialized stats fallback observability"
```

---

### Task 6: Documentation and Rollback

**Files:**
- Modify: `docs/README.md`
- Modify: `docs/ray-asha-deep-dive.md`

**Step 1: Document materialized stats path**

Add section:
1. New tables and role.
2. Fallback semantics.
3. Operational checks.

**Step 2: Document rollback**

Rollback path:
1. Disable materialized read path with env/config switch (if introduced).
2. Revert to legacy SQL-only behavior safely.

**Step 3: Commit**

```bash
git add docs/README.md docs/ray-asha-deep-dive.md
git commit -m "docs: add trials materialized stats architecture and rollback"
```

---

### Task 7: Final Verification (Sprint 2 Gate)

**Step 1: Full checks**

Run:
1. `cargo check --all-targets`
2. `cargo build`
3. `cargo test --lib`
4. `pytest ray_driver/tests`

Expected: all pass.

**Step 2: Runtime smoke**

1. Start runtime and open `/trials`.
2. Confirm `trial runs` and `axes` load repeatedly without DB lock spikes.
3. Verify fallback still works when materialized rows are absent.

**Step 3: Push**

```bash
git push origin main
```

