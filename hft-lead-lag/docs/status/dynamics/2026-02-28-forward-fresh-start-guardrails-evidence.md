# Forward Fresh Start Guardrails Evidence

Date: 2026-02-28
Scope: add explicit safe reset for race state (`fresh start`) to support clean forward test restarts.

## Implemented
1. New API endpoint: `POST /api/v1/forward/fresh-start`.
2. Guardrails:
   - requires `{"confirm": true}`
   - rejects while runner job is active (`409 Conflict`)
   - validates `run_id` prefix (`forward-...`) when provided
3. Reset behavior:
   - full fleet reload with current configs (symbol-level runtime reset)
   - clears portfolio runtime snapshot/guards state (`restore_portfolio_runtime_v1_from_db_rows([], [])`)
   - switches screener run scope to a fresh run id (`forward-<ts>-fresh` by default)
4. `Forward` UI tab now has a `Fresh Start` button wired to this endpoint.

## Files
1. `src/api/handlers.rs`
2. `src/api/http_server.rs`
3. `src/api/handlers/tests.rs`
4. `src/api/templates/trials.html`

## Tests
1. `forward_fresh_start_requires_explicit_confirm`
2. `forward_fresh_start_resets_runtime_race_state`

## Verification
1. `cargo test -q forward_fresh_start_requires_explicit_confirm -- --nocapture`
2. `cargo test -q forward_fresh_start_resets_runtime_race_state -- --nocapture`
3. `cargo test -q api::handlers::tests:: -- --nocapture`
4. `cargo test -q api::runner::tests:: -- --nocapture`
5. `cargo check -q`
6. `cargo build -q`
