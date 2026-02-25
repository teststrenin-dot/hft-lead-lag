# Shadow Fleet Design Review (R4)

## Findings
- **P2** `src/domain/screener/shadow_fleet.rs:228-277`  
  Score formula uses mixed input normalization, reducing clarity and predictability of policy ranking.

- **P2** `src/main.rs:968-1219`  
  Shadow-fleet related runtime updates share control loop with trial-batch/runtime-grid orchestration, increasing coupling risk.

- **P3** `src/api/handlers.rs:224-236`, `src/domain/screener/mod.rs:44-69`  
  Policy score/gate is exposed per-symbol only; no fleet-level policy overview for operators.

- **P3** `src/api/handlers.rs:41-118`, `src/main.rs:931-1164`  
  Trial-batch health/ack telemetry is not surfaced in health endpoints, limiting operational visibility for shadow-fleet tuning loops.

- **P3** `src/main.rs:1061-1093`  
  Processed queue files are deleted immediately, which weakens postmortem/replay workflows for failed optimization submissions.

## Design Direction
- Normalize score inputs and define calibration tests.
- Decouple policy-producing flows from control-loop chokepoints.
- Add fleet-level policy observability and trial lifecycle telemetry.
