# E2E Forward Race Acceptance Evidence

Date: 2026-02-28
Scope: add executable acceptance checks for `scout refs -> forward runtime -> race API transitions`.

## Automated tests added
1. `forward_internal_runner_smoke_e2e_from_scout_refs_to_success` (`src/api/runner.rs`)
   - seeds DB config + scout reference artifact,
   - starts internal Rust `forward` runner,
   - simulates trial ack handoff,
   - verifies job completion as `Success`.
2. `portfolio_api_exposes_assignment_transition_after_cooldown` (`src/api/handlers/tests.rs`)
   - verifies candidate/assignment transition path over API surface:
   - symbol active before cooldown trigger,
   - symbol removed from active allocation after cooldown + rebalance,
   - candidate API still exposes moving race symbols.

## Runtime path validated
1. `scout` artifact contract exists and is consumed by `forward` startup.
2. `forward` run lifecycle completes through Rust-internal control flow (no Python/Ray runtime process).
3. race transitions remain observable on API endpoints used by UI (`portfolio active` + `portfolio candidates`).

## Verification commands
1. `cargo test -q forward_internal_runner_smoke_e2e_from_scout_refs_to_success -- --nocapture`
2. `cargo test -q portfolio_api_exposes_assignment_transition_after_cooldown -- --nocapture`
3. `cargo test -q api::runner::tests:: -- --nocapture`
4. `cargo test -q api::handlers::tests:: -- --nocapture`
5. `cargo check -q`
6. `cargo build -q`
