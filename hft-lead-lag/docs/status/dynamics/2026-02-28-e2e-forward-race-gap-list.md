# E2E Forward Race Gap List (to full business process)

Date: 2026-02-28
Scope: what is still required to reach full `scout -> forward -> symbol race + portfolio race in UI` workflow.

## Completed in this block
1. UI runner controls are scoped to `trials -> scout` and `forward -> forward`.
2. Backend runner supports `forward` and blocks non-target phases.
3. `forward` start now requires valid non-empty scout references artifact.
4. Conflict semantics are fixed (`409` active job has priority over prereq checks).
5. `forward` runtime start is Rust-native (internal runner job), without Python/Ray process spawn (`2026-02-28-forward-rust-runtime-runner-evidence.md`).
6. Forward lifecycle markers are bound in UI (`active_run`, `runner`, `last_forward`, `ack`) with explicit meta updates (`2026-02-28-forward-ui-live-race-lifecycle-evidence.md`).
7. Forward tab reads live race state (portfolio allocation + candidate ranking + performance) from runtime APIs, not only historical run aggregates (`2026-02-28-forward-ui-live-race-lifecycle-evidence.md`).
8. Fresh start control is available with guardrails (`confirm=true`, conflict on active runner, run-id contract) and wired into Forward UI (`2026-02-28-forward-fresh-start-guardrails-evidence.md`).
9. End-to-end acceptance checks are automated for forward runner lifecycle and race API transitions (`2026-02-28-e2e-forward-race-acceptance-evidence.md`).

## Remaining required work (ordered)
None.

## Exit gate for this gap list
1. Operator can start `scout`, then `forward`, and see moving symbol/portfolio race in real time in UI.
2. Forward runtime path is Rust-only on trading host.
3. No dependency on Python/Ray for runtime business process execution.
