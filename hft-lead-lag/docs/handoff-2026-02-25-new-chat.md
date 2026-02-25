# Handoff Context (2026-02-25)

## Repo/Branch State

- Repo: `/root/turbo/hft-lead-lag`
- Branch: `main`
- Working tree: clean
- Latest commit: `3cee67f fix: harden incremental fleet patch apply semantics`

Recent commits:

1. `3cee67f` `fix: harden incremental fleet patch apply semantics`
2. `3e2acaa` `feat: add incremental trial-batch fleet patch mode`
3. `8dbfe8f` `docs: split remaining prod hardening into two smell-free sprints`

## What Was Just Finished

Incremental trial-batch path hardening is implemented and pushed:

- Fail-closed incremental apply semantics (`changed_config_ids` validation against old/new configs).
- `run_id` switches only after successful patch apply.
- Symbol scope normalization moved into `FleetPatchPlan`.
- Apply diagnostics added (`matched/unmatched changed ids`, scope match counters).
- Added guards for `new-only` changed ids requiring symbol scope.
- Updated docs for new contract behavior.

Key files touched:

- `src/main.rs`
- `src/main_tests.rs`
- `src/domain/screener/fleet_patch.rs`
- `src/domain/screener/mod.rs`
- `src/domain/screener/shadow_fleet.rs`
- `docs/README.md`
- `docs/ray-asha-deep-dive.md`

## Verification Status

All passed on latest state:

1. `cargo check --all-targets`
2. `cargo build`
3. `cargo test --lib`
4. `cargo test main_tests::`

## Current User Request In Progress

User requested a full docs refresh:

- “полностью обновляй доку, сноси в архив старую добавляй новую - /root/turbo/hft-lead-lag/docs. Сабагентами”

Context gathered via subagents:

- Current authoritative runtime/API/CLI contract inventory collected from code.
- Preliminary archive candidates identified (`docs/archive/2026-02-25/2026-02-23-*`).

Blocked only on archive scope choice from user:

1. Archive only legacy docs (minimal risk).
2. Archive almost all current docs and rebuild set.
3. Archive all docs and recreate from scratch.

## Next Step In New Chat

1. Confirm archive scope (1/2/3).
2. Execute docs migration with subagents:
   - create `docs/archive/2026-02-25/`,
   - move selected old docs,
   - rewrite canonical docs set,
   - run link/path sanity check,
   - commit + push.
