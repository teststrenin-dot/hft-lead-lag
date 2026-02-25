# Preventive Architecture Review (R4)

## Observed Gaps
- No strict data-loss SLO/guardrail around queue saturation drops (`src/infrastructure/db.rs:453-491`).
- No robust multi-driver IPC isolation for trial batches (`src/main.rs:1031-1093`, `docs/ray-asha-deep-dive.md:286-293`).
- No bounded cardinality/TTL strategy for screener symbol catalog growth (`src/domain/screener/mod.rs:184-199`, `447-484`).
- Limited operator telemetry for trial-batch lifecycle (`src/api/handlers.rs:41-118`, `src/main.rs:931-1164`).

## Preventive Controls
1. Define saturation policy contract: allowed drop rate, alert thresholds, and escalation paths.
2. Introduce per-run namespacing/leases for trial-batch submissions and acks.
3. Add symbol lifecycle policy and row-build latency budget with metrics.
4. Expose trial-batch health signals (last ack ts, queue depth, latest run_id status).
