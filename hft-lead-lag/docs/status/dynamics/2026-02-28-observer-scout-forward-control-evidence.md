# Observer Scope Evidence — Scout + Forward UI Control

Date: 2026-02-28
Scope: keep observer-first UI while exposing only the two required runtime controls (`scout` and `forward`) aligned with the business contour.

## 1) Contract implemented
1. UI/operator control from runner surface is limited to two phases: `scout`, `forward`.
2. Non-target phases (`expand`, `promote`, unknown) are rejected server-side.
3. `forward` start is guarded by scout artifact prerequisites:
   - `data/scout-references.json` must exist.
   - File must be valid JSON array.
   - Array must be non-empty.

## 2) Code evidence
1. `src/api/runner/command.rs`
   - `runner_ui_config()` exposes only `scout` and `forward` defaults.
   - `build_trial_runner_command()` builds `scout` and `forward` only; rejects other phases.
2. `src/api/runner.rs`
   - `validate_phase_prerequisites()` enforces `forward` dependency on non-empty scout references.
   - Runner tests cover both command contract and prerequisite validation.
3. `src/api/templates/trials.html`
   - Tab-scoped allowlists are now explicit and minimal: `trials -> scout`, `forward -> forward`.
   - Bootstrap defaults include forward parameters.

## 3) Validation
Commands:
```bash
cargo check -q
cargo test -q api::runner::tests:: -- --nocapture
```

Observed:
1. Build passes.
2. Runner tests pass for:
   - forward command defaults/caps,
   - rejection of non-target phases,
   - forward prerequisite checks (missing/empty/valid scout references).

## 4) Result
Observer-plane control is now practical for the intended workflow:
1. UI can run `scout` and then run `forward` from the same surface.
2. Invalid `forward` starts are blocked early with explicit prerequisite errors.
3. Unneeded phases remain blocked, keeping cognitive load low.

## 5) Post-review hardening
1. Forward form fields are now force-synced from server config on load to avoid stale bootstrap defaults.
2. Runner start now prioritizes `409 conflict` when a job is already active (before forward prereq checks).
3. Forward prereq validation now requires typed scout rows (`config_id`, `trades`, `avg_pnl_pct`) and rejects empty/invalid lists.
