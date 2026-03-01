# CP7 Block6 Evidence — DB Writer Stall Watchdog + Drift Alert Contract

Date: 2026-02-28
Status: Completed
Scope: close CP7 monitoring tail for DB writer progress and drift signalization

## What was implemented
1. `DbWriter` watchdog telemetry (global runtime counters):
   - `watchdog_enqueued_max_seq()`
   - `watchdog_observed_max_seq()`
   - `watchdog_last_progress_ms()`
   - progress is updated when producer enqueues seq and writer worker observes seq.
2. `/health` response now includes DB writer progress snapshot:
   - `db_writer_enqueued_seq`
   - `db_writer_observed_seq`
   - `db_writer_backlog_seq`
   - `db_writer_last_progress_age_ms`
3. `/health` issue contract extension:
   - `db_writer_stall` when backlog exists and writer progress age exceeds threshold.
4. Drift telemetry contract moved from logs to health payload:
   - `runtime_drift_ms` snapshot (`samples`, `avg`, `p50/p95/p99`, `abs_p99`, `abs_max`).
5. `/health` warning contract extension:
   - `drift_p99_high` when absolute drift p99 exceeds threshold.

## Files
1. `src/infrastructure/db.rs`
2. `src/event_loop_core.rs`
3. `src/api/http_server.rs`
4. `src/api/handlers.rs`
5. `src/api/handlers/health_support.rs`
6. `src/api/handlers/tests.rs`

## Verification
Commands run:

```bash
cargo test -q health_reports_db_writer_stall_when_backlog_progress_is_stale -- --nocapture
cargo test -q health_emits_drift_warning_when_p99_abs_is_high -- --nocapture
cargo test -q health_ -- --nocapture
cargo test -q api::handlers::tests:: -- --nocapture
cargo check -q
```

Result:
1. new watchdog/alert regression tests pass;
2. full health/API handler suites pass;
3. build is clean.

## Exit-gate impact
1. CP7 now has explicit DB-writer stall detection in runtime health contract.
2. Drift risk is no longer log-only; it is now visible as `/health` warning signal.
3. Remaining CP7 tail is operator escalation policy/runbook finalization (not raw telemetry availability).
