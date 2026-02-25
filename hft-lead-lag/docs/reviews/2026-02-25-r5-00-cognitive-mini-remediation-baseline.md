# R5 Baseline — Cognitive Load & Mini Remediation (2026-02-25)

## Scope
- Baseline snapshot recorded before launching full R5 review round.
- Commit window for upcoming round: `e1af2ee..HEAD`.

## Cognitive Load Baseline

### What improved
1. `src/main.rs` was decomposed from a monolithic orchestration file into dedicated modules:
   - `event_loop_core.rs`
   - `event_loop_ingest.rs`
   - `event_loop_runtime.rs`
   - `runtime_setup.rs`
   - `runtime_grid.rs`
   - `runtime_hot_reload.rs`
   - `runtime_symbols.rs`
   - `trial_batch_apply.rs`
   - `trial_batch_protocol.rs`
   - `trial_queue_io.rs`
   - `file_fingerprint.rs`
2. Tests were split out of large modules into dedicated test files for handlers and screener domains.

### Residual cognitive hotspots
1. `src/infrastructure/db.rs` remains large and multi-responsibility (schema migration + queueing + writer lifecycle + metrics + tests).
2. `src/api/handlers.rs` remains broad (many endpoint concerns in a single module).
3. `src/runtime_hot_reload.rs` now owns multiple control-plane loops in one file (runtime grid, trial batch, trial control), reducing local complexity in `main.rs` but concentrating operational orchestration.

## Mini Remediation Backlog (Pre-R5)

### Candidate M1 (P1)
**Topic:** DB writer flush barrier semantics.
- `DbWriter::flush_all()` currently sends `Flush` through the primary channel only.
- With overflow/retry/spillover/backpressure pipeline active, `flush_all()` is best-effort and not a strict "all queues drained" barrier.
- Impact: boundary operations that assume strict durability ordering (runtime-grid apply / trial batch apply) can observe post-flush late-arriving writes.
- Primary references:
  - `src/infrastructure/db.rs` (`flush_all`, queue fan-in/fan-out topology)
  - `src/runtime_hot_reload.rs` (`replace_fleet_configs` + `flush_db_writer`)
  - `src/trial_batch_apply.rs` (`set_run_id` + `flush_db_writer`)

### Candidate M2 (P2)
**Topic:** Trial-batch archive fail-safe behavior.
- Queue file archival currently removes source file when archive directory creation or rename fails.
- This prevents poison loops, but can cause payload loss under transient FS errors.
- Impact: at-most-once semantics with potential silent data loss in adverse IO conditions.
- Primary references:
  - `src/trial_queue_io.rs` (`archive_trial_batch_queue_file`)

## Notes
- This baseline is intentionally pre-review and pre-fix.
- P0 discoveries from full R5 review are allowed to be planned and fixed after reports are produced.
