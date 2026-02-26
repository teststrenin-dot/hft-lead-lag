# R7 — Preventive Architecture Review

Date: 2026-02-26

## Findings

### P1
1. Trial apply ack durability invariant is not strict.
- Evidence: `src/trial_batch_apply.rs:128`, `:135`, `:199`.
- Hardening: enforce "ack ok only if durable + applied" contract.

2. Fallback symbol logic may conflict with blacklist intent under REST failures.
- Evidence: `src/runtime_symbols.rs:47`, `:79`, `:147`, `src/main.rs:82`.

3. Dirty quote guards incomplete (`ask < bid`, timestamp regressions not filtered early).
- Evidence: `src/domain/screener/quote_ingest.rs:89`, `src/domain/screener/state.rs:70`.

### P2
1. Backpressure behavior can stall/react poorly in ingest hot path.
- Evidence: `src/infrastructure/db.rs:796`, `src/domain/screener/quote_ingest.rs:125`.

2. Health degradation does not always trigger runtime protective mode.
- Evidence: `src/api/handlers/health_support.rs:117`, `:139`.

3. Quarantine queue visibility is partial in health counters.
- Evidence: `src/trial_queue_io.rs:170`.

### P3
1. Several `.expect("... mutex poisoned")` paths prefer panic over degraded recovery.
- Evidence: `src/domain/screener/mod.rs:273`, `:299`; `src/application/services/lead_lag.rs:205`.
