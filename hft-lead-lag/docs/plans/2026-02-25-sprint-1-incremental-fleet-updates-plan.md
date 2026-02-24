# Sprint 1 Incremental Fleet Updates Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace global fleet reset with incremental, symbol-scoped fleet updates while preserving backward compatibility with existing `trial-batch` flow.

**Architecture:** Use side-by-side migration: keep current full-replace path as default, add a new incremental patch mode with explicit scope (`changed_config_ids`, optional symbol list). Move decision logic into small pure helpers to avoid growing `main.rs` hot-reload branch into a god-function.

**Tech Stack:** Rust (`axum`, `dashmap`, `arc-swap`, `rusqlite`), existing runtime loop in `src/main.rs`.

---

## Smell Guardrails (Mandatory)

1. No new god-function in `src/main.rs` (extract helper functions for parse/apply decisions).
2. No mixed concerns: batch-parse, patch-plan, and runtime-apply live in separate functions/modules.
3. No silent fallback: invalid incremental payloads emit explicit WARN and do not partially apply.
4. No hidden contract drift: `trial-batch` schema changes documented and covered by tests.
5. Keep backward compatibility: legacy payload (`run_id + configs`) remains valid.

## Out of Scope (Sprint 1)

1. Symbol clustering logic itself.
2. Family scoring or adaptive eject/revive rules.
3. New UI tabs for portfolios.

---

### Task 1: Add Incremental Trial-Batch Contract (Parse Layer)

**Files:**
- Modify: `src/main.rs`
- Modify: `src/main_tests.rs`

**Step 1: Write failing tests for new payload shape**

Add tests in `src/main_tests.rs`:

```rust
#[test]
fn load_trial_batch_parses_incremental_mode() {
    // mode=incremental, changed_config_ids present
}

#[test]
fn load_trial_batch_defaults_to_full_replace_when_mode_missing() {
    // backward compatibility
}
```

**Step 2: Run tests to verify failures**

Run: `cargo test --lib main_tests::load_trial_batch_parses_incremental_mode`
Expected: FAIL (fields absent in `TrialBatch`).

**Step 3: Implement minimal parser changes**

In `src/main.rs`:
1. Extend `TrialBatch` with:
   - `mode: Option<String>`
   - `changed_config_ids: Option<Vec<u64>>`
   - `symbols: Option<Vec<String>>`
2. Normalize mode into enum-like internal helper (`full_replace` default).

**Step 4: Re-run tests**

Run: `cargo test --lib main_tests::load_trial_batch_parses_incremental_mode`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/main.rs src/main_tests.rs
git commit -m "feat: add incremental trial-batch payload contract"
```

---

### Task 2: Introduce Patch Planning Helper (Pure Logic)

**Files:**
- Create: `src/domain/screener/fleet_patch.rs`
- Modify: `src/domain/screener/mod.rs`

**Step 1: Write failing tests for patch decision logic**

Add unit tests in new `fleet_patch.rs`:

```rust
#[test]
fn full_replace_marks_all_symbols_for_reset() {}

#[test]
fn incremental_only_resets_symbols_with_touched_configs() {}

#[test]
fn incremental_with_symbol_scope_limits_resets() {}
```

**Step 2: Run tests to verify failure**

Run: `cargo test --lib fleet_patch`
Expected: FAIL (module/functions missing).

**Step 3: Implement minimal helper**

Implement pure structs/functions:
1. `FleetPatchMode` (`FullReplace`, `Incremental`).
2. `FleetPatchPlan` with:
   - `mode`
   - `changed_config_ids`
   - `symbol_scope`
3. `should_reset_symbol(...)` pure function.

**Step 4: Re-run tests**

Run: `cargo test --lib fleet_patch`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/domain/screener/fleet_patch.rs src/domain/screener/mod.rs
git commit -m "refactor: extract fleet patch planning helper"
```

---

### Task 3: Add Incremental Apply Path in ScreenerStore

**Files:**
- Modify: `src/domain/screener/mod.rs`
- Modify: `src/domain/screener/state.rs`
- Modify: `src/domain/screener/shadow_fleet.rs`

**Step 1: Write failing tests**

Add tests around update behavior:
1. Full replace still resets all symbol fleets.
2. Incremental mode resets only affected symbols.
3. Unaffected symbols keep fleet state and do not drain trades.

**Step 2: Run tests (RED)**

Run: `cargo test --lib domain::screener`
Expected: FAIL with missing incremental apply API.

**Step 3: Implement minimal incremental API**

1. Add method:
   - `ScreenerStore::apply_fleet_patch(new_configs, plan) -> FleetReloadReport`
2. Keep `replace_fleet_configs` as wrapper over full replace for compatibility.
3. Add minimal `ShadowFleet` introspection helper required by planner:
   - e.g. `contains_any_config_ids(&HashSet<u64>)`.

**Step 4: Re-run tests (GREEN)**

Run: `cargo test --lib domain::screener`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/domain/screener/mod.rs src/domain/screener/state.rs src/domain/screener/shadow_fleet.rs
git commit -m "feat: apply fleet patches incrementally by symbol scope"
```

---

### Task 4: Wire Runtime Hot-Reload to Patch API

**Files:**
- Modify: `src/main.rs`
- Modify: `src/main_tests.rs`

**Step 1: Write failing integration-style tests**

Add tests for trial-batch apply branch:
1. `mode=full_replace` calls full behavior.
2. `mode=incremental` uses patch behavior and report fields are consistent.

**Step 2: Run tests (RED)**

Run: `cargo test --lib main_tests`
Expected: FAIL on old call path (`replace_fleet_configs` only).

**Step 3: Implement minimal runtime wiring**

In `src/main.rs` trial-batch branch:
1. Parse plan from payload.
2. Call `apply_fleet_patch`.
3. Preserve existing DB metadata and `.trial-ack`.

**Step 4: Re-run tests (GREEN)**

Run: `cargo test --lib main_tests`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/main.rs src/main_tests.rs
git commit -m "feat: wire incremental fleet patch into trial batch runtime path"
```

---

### Task 5: Documentation and Operations Guardrail

**Files:**
- Modify: `docs/README.md`
- Modify: `docs/ray-asha-deep-dive.md`

**Step 1: Document batch modes**

Add `trial-batch` contract section:
1. `full_replace` (default).
2. `incremental` (`changed_config_ids`, optional `symbols`).
3. Explicit safety behavior on invalid payload.

**Step 2: Add rollback procedure**

Document fallback command/path:
1. Remove `mode` from payload to force full replace.
2. Validate via `/api/v1/trials/runner/status`.

**Step 3: Commit**

```bash
git add docs/README.md docs/ray-asha-deep-dive.md
git commit -m "docs: document incremental fleet patch mode and rollback"
```

---

### Task 6: Final Verification (Sprint 1 Gate)

**Step 1: Compile and tests**

Run:
1. `cargo check --all-targets`
2. `cargo build`
3. `cargo test --lib`

Expected: all pass.

**Step 2: Regression smoke**

Run runtime + one `scout` cycle:
1. Ensure legacy payload still applies.
2. Ensure incremental payload path logs expected apply mode.

**Step 3: Push**

```bash
git push origin main
```

