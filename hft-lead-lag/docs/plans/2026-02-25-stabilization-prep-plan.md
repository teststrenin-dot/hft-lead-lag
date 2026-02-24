# Stabilization Prep Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the codebase ready for family/cluster portfolio changes by fixing expand cycle behavior, adding SQLite schema placeholders, and removing runtime-state files from git history.

**Architecture:** Keep runtime behavior unchanged except for expand-cycle seeding semantics. Extend existing SQLite migration path with new family/cluster/portfolio tables so future logic has a durable storage target. Treat `config/trial-*.json` and `.trial-ack` as runtime artifacts, not versioned source.

**Tech Stack:** Python (`ray_driver`), Rust (`rusqlite`), Git.

---

### Task 1: Expand Cycles Cumulative Seed (TDD)

**Files:**
- Modify: `ray_driver/tests/test_expand_cycles.py`
- Modify: `ray_driver/cli.py`

**Step 1: Write the failing test**

Add a test that runs `cmd_expand` with `cycles=2` and verifies the second cycle receives refs that include first-cycle alive configs.

**Step 2: Run test to verify it fails**

Run: `pytest ray_driver/tests/test_expand_cycles.py -q`
Expected: FAIL because refs do not change between cycles.

**Step 3: Write minimal implementation**

Update `cmd_expand` to merge first-cycle alive configs into refs for next cycles.

**Step 4: Run test to verify it passes**

Run: `pytest ray_driver/tests/test_expand_cycles.py -q`
Expected: PASS.

---

### Task 2: Add SQLite Tables for Family/Cluster State (TDD)

**Files:**
- Modify: `src/infrastructure/db.rs`

**Step 1: Write the failing test**

Add a unit test in `db.rs` that asserts the new tables exist:
- `config_families`
- `family_symbol_clusters`
- `portfolio_state`

**Step 2: Run test to verify it fails**

Run: `cargo test --lib infrastructure::db::tests::open_db_creates_family_cluster_tables`
Expected: FAIL because tables do not exist yet.

**Step 3: Write minimal implementation**

Add table definitions and supporting indexes to `SCHEMA`.

**Step 4: Run test to verify it passes**

Run: `cargo test --lib infrastructure::db::tests::open_db_creates_family_cluster_tables`
Expected: PASS.

---

### Task 3: Runtime Artifacts Not Tracked

**Files:**
- Modify: `hft-lead-lag/.gitignore`
- Remove tracked state: `hft-lead-lag/config/.trial-ack`, `hft-lead-lag/config/trial-batch.json`, `hft-lead-lag/config/trial-control.json`

**Step 1: Add ignore rules**

Ignore runtime-generated files under `config/`:
- `.trial-ack`
- `trial-batch.json`
- `trial-control.json`

**Step 2: Untrack existing files**

Run:
- `git rm --cached hft-lead-lag/config/.trial-ack`
- `git rm --cached hft-lead-lag/config/trial-batch.json`
- `git rm --cached hft-lead-lag/config/trial-control.json`

Expected: files stay on disk, removed from git index.

---

### Task 4: Verification

**Step 1: Python tests**

Run: `pytest ray_driver/tests`
Expected: all pass.

**Step 2: Rust library tests**

Run: `cargo test --lib`
Expected: all pass (except known ignored tests).

---

### Task 5: Commit

**Step 1: Commit all changes**

Run:
- `git add -A`
- `git commit -m "fix: stabilize expand cycles and add family-cluster DB scaffolding"`

**Step 2: Push**

Run: `git push origin main`

