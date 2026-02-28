# Forward UI Live Race + Lifecycle Evidence

Date: 2026-02-28
Scope: close observer gaps for `forward` tab so operator can watch current race lifecycle and live portfolio/symbol competition without switching pages.

## Implemented
1. Forward lifecycle markers are explicit in `forward-meta`:
   - `active_run` from `/health.trial_active_run_id`
   - `runner` state (running/idle marker)
   - `last_forward` completion marker from latest forward job in runner history
   - `ack` status from `/health.trial_last_ack_status`
2. Forward run selector is bound to active run context:
   - active run option is tagged as `[ACTIVE]` when present in historical runs
   - selection fallback prefers active run, then latest historical run
3. Forward tab now includes live race sections:
   - `Live Portfolio Allocation (race)` table (from `/api/v1/portfolio/active`)
   - `Live Candidate Race` table (from `/api/v1/portfolio/candidates`)
   - summary cards include live active symbols, candidate count, equity and realized PnL (from `/api/v1/portfolio/performance`)
4. Refresh cadence for forward observer data reduced from 30s to 15s.

## Files
1. `src/api/templates/trials.html`

## Verification
1. `cargo test -q api::runner::tests:: -- --nocapture`
2. `cargo test -q api::handlers::tests:: -- --nocapture`
3. `cargo check -q`
4. `cargo build -q`

## Result
`Forward` tab is now an observer surface for both:
1. historical config/symbol performance by run, and
2. live portfolio/candidate race state tied to current lifecycle markers.
