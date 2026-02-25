# Incremental Fleet Patch Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Убрать логические дыры incremental patch-пути, чтобы `trial-batch` не мог молча примениться как no-op и не искажал run-аналитику.

**Architecture:** Вводим fail-closed preflight для incremental patch перед сменой `run_id`, переносим нормализацию symbol scope в domain-level план patch-а, расширяем apply-report диагностикой и усиливаем тесты на fail-path. Сохраняем backward compatibility по payload-формату (`mode` optional, `full_replace` по умолчанию).

**Tech Stack:** Rust (`axum`, `dashmap`, `arc-swap`, `rusqlite`), runtime watcher в `src/main.rs`, screener domain в `src/domain/screener/*`.

---

## Smell Guardrails (Mandatory)

1. Не добавлять новую god-function в `src/main.rs` — отдельные helper-функции для preflight/валидации.
2. Не смешивать parse/apply/observability в одном блоке.
3. Не допускать silent no-op для incremental patch при ошибочном `changed_config_ids`.
4. Не менять поведение legacy `full_replace` и payload без `mode`.
5. Любое новое инвариантное поведение фиксировать тестами до имплементации.

## Assumptions (Closed defaults because user cannot decide now)

1. `changed_config_ids` считаем валидным, если id найден хотя бы в одном из двух множеств:
   - текущий активный fleet (old),
   - входящий `batch.configs` (new).
2. Если `incremental` и `changed_config_ids` не матчится ни с old, ни с new, batch не применяется.
3. `run_id` переключается только после успешного patch apply.
4. Нормализация символов (`trim + uppercase`) должна быть в `FleetPatchPlan`, а не только в `main.rs`.

---

### Task 1: Fail-Closed Incremental Preflight (no-op prevention)

**Files:**
- Modify: `src/domain/screener/mod.rs`
- Modify: `src/domain/screener/shadow_fleet.rs`
- Modify: `src/main.rs`
- Test: `src/domain/screener/mod.rs` (module tests)

**Step 1: Write failing tests for id matching semantics**

Добавить тесты:

```rust
#[test]
fn incremental_matches_changed_ids_from_old_or_new_configs() {}

#[test]
fn incremental_rejects_when_changed_ids_match_nothing() {}
```

**Step 2: Run tests to verify RED**

Run: `cargo test --lib domain::screener::tests::incremental_`
Expected: FAIL (нет preflight/reject semantics).

**Step 3: Implement minimal preflight helper**

Добавить helper в `ScreenerStore`:

- собрать `old_ids` из активных fleet;
- собрать `new_ids` из `new_configs`;
- вычислить `matched_old`, `matched_new`, `matched_any`.

Инвариант:

- `incremental` + `matched_any == false` -> вернуть ошибку apply (без смены run).

**Step 4: Wire preflight into runtime apply path**

В `main.rs` trial-batch ветке:

- вызывать новый `try_apply_fleet_patch(...) -> Result<FleetReloadReport, FleetPatchApplyError>`;
- при ошибке логировать `warn!` и не делать `set_run_id`.

**Step 5: Run tests to verify GREEN**

Run: `cargo test --lib domain::screener::tests::incremental_`
Expected: PASS.

**Step 6: Commit**

```bash
git add src/domain/screener/mod.rs src/domain/screener/shadow_fleet.rs src/main.rs
git commit -m "fix: fail closed when incremental changed ids do not match old or new configs"
```

---

### Task 2: Safe Run-ID Transition (no analytic contamination)

**Files:**
- Modify: `src/main.rs`
- Modify: `src/main_tests.rs`

**Step 1: Write failing tests**

Добавить тесты:

```rust
#[test]
fn trial_batch_keeps_previous_run_when_incremental_apply_fails() {}

#[test]
fn trial_batch_switches_run_id_only_after_successful_apply() {}
```

**Step 2: Run tests to verify RED**

Run: `cargo test trial_batch_keeps_previous_run`
Expected: FAIL.

**Step 3: Implement minimal ordering fix**

В trial-batch ветке:

1. сначала `try_apply_fleet_patch`;
2. только после `Ok(report)` вызывать `set_run_id(Some(run_id))` и `upsert_trial_run_meta`;
3. при `Err` не закрывать предыдущий run и не переключать run_id.

**Step 4: Run tests to verify GREEN**

Run: `cargo test trial_batch_switches_run_id_only_after_successful_apply`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/main.rs src/main_tests.rs
git commit -m "fix: switch run_id only after successful fleet patch apply"
```

---

### Task 3: Symbol Scope Canonicalization at Domain Boundary

**Files:**
- Modify: `src/domain/screener/fleet_patch.rs`
- Modify: `src/main.rs`
- Test: `src/domain/screener/fleet_patch.rs` tests

**Step 1: Write failing tests**

Добавить тесты в `fleet_patch.rs`:

```rust
#[test]
fn plan_new_normalizes_symbol_scope_to_uppercase_trimmed() {}

#[test]
fn symbol_in_scope_is_case_insensitive_after_normalization() {}
```

**Step 2: Run tests to verify RED**

Run: `cargo test fleet_patch::tests::plan_new_normalizes_symbol_scope_to_uppercase_trimmed`
Expected: FAIL.

**Step 3: Implement minimal normalization move**

В `FleetPatchPlan::new`:

- нормализовать `symbol_scope` (`trim + uppercase`, dedup);
- удалить duplicate-normalization из `main.rs` (или сделать thin wrapper).

**Step 4: Run tests to verify GREEN**

Run: `cargo test fleet_patch`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/domain/screener/fleet_patch.rs src/main.rs
git commit -m "refactor: normalize incremental symbol scope inside fleet patch plan"
```

---

### Task 4: Patch Observability (unmatched scope and match stats)

**Files:**
- Modify: `src/domain/screener/mod.rs`
- Modify: `src/main.rs`
- Modify: `src/domain/screener/fleet_patch.rs`
- Test: `src/domain/screener/mod.rs` tests

**Step 1: Write failing tests**

Добавить тесты:

```rust
#[test]
fn incremental_report_exposes_unmatched_scope_symbols() {}

#[test]
fn incremental_report_exposes_changed_id_match_counts() {}
```

**Step 2: Run tests to verify RED**

Run: `cargo test --lib domain::screener::tests::incremental_report_`
Expected: FAIL.

**Step 3: Implement minimal diagnostics**

Расширить report:

- `matched_changed_ids_old: usize`
- `matched_changed_ids_new: usize`
- `scope_symbols_requested: usize`
- `scope_symbols_matched: usize`

В `main.rs` логировать эти значения в `trial-batch: applied ...`.

**Step 4: Run tests to verify GREEN**

Run: `cargo test --lib domain::screener::tests::incremental_report_`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/domain/screener/mod.rs src/main.rs src/domain/screener/fleet_patch.rs
git commit -m "feat: add incremental patch diagnostics for id and symbol scope matching"
```

---

### Task 5: Contract & Regression Tests for Invalid Incremental Payloads

**Files:**
- Modify: `src/main_tests.rs`
- Modify: `docs/README.md`
- Modify: `docs/ray-asha-deep-dive.md`

**Step 1: Write failing tests**

Добавить тесты:

```rust
#[test]
fn build_trial_batch_patch_plan_rejects_incremental_without_changed_ids() {}

#[test]
fn build_trial_batch_patch_plan_rejects_incremental_with_empty_changed_ids() {}

#[test]
fn build_trial_batch_patch_plan_rejects_incremental_with_empty_symbols_after_trim() {}
```

**Step 2: Run tests to verify RED**

Run: `cargo test build_trial_batch_patch_plan_rejects_incremental`
Expected: FAIL.

**Step 3: Implement minimal validation cleanup**

Убедиться, что ошибки стабильны и однозначны (без silent fallback).

**Step 4: Update docs**

Задокументировать:

- invalid incremental = reject;
- changed-id matching semantics (old/new);
- run-id switch happens only on successful apply.

**Step 5: Run tests to verify GREEN**

Run: `cargo test main_tests::`
Expected: PASS.

**Step 6: Commit**

```bash
git add src/main_tests.rs docs/README.md docs/ray-asha-deep-dive.md
git commit -m "test/docs: harden incremental payload contract and regression coverage"
```

---

### Final Verification Gate

Run:

1. `cargo check --all-targets`
2. `cargo build`
3. `cargo test --lib`
4. `cargo test main_tests::`

Expected: all pass, no new warnings.

Push:

```bash
git push origin main
```

