# R5 — Preventive Architecture Review

## Findings
- **P1** Flush barrier в DB writer не покрывает весь pipeline (primary + overflow/retry/spillover/backpressure).
  - Подрывает надежность boundary-операций при saturation.
  - Refs:
    - `src/infrastructure/db.rs:533-541`
    - `src/infrastructure/db.rs:565-616`
    - `src/trial_batch_apply.rs:149-151`
    - `src/runtime_hot_reload.rs:222-224`

- **P2** Archive error path удаляет batch payload вместо fail-safe retain/retry.
  - Refs:
    - `src/trial_queue_io.rs:220-231`
    - `src/trial_queue_io.rs:238-250`

## Preventive Readiness Score
- **5/10**
- Плюсы: bounded queues, saturation counters, health telemetry.
- Минусы: durable boundary semantics и IO-failure semantics пока неполные.
