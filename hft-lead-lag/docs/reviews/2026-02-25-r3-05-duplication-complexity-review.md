# Duplication, Redundancy & Complexity Review (R3)

## Findings
- **P2** `src/infrastructure/db.rs:130-209`  
  Startup migration block runs large batches of `ALTER TABLE ... ADD COLUMN` against schema that already contains those columns. Mostly dead compatibility path with repeated ignored errors (`let _ = ...`).

- **P2** `ray_driver/expand.py:13-63` and `ray_driver/promote.py:31-58`  
  Duplicated SQL/config extraction logic across scripts. Same query shape and mapping maintained in two places.

- **P3** `src/main.rs:1610-1656`  
  `GateSubscribeAttempt` abstraction is redundant: helper always returns `true`, so all enum branches collapse into the same behavior.

- **P3** `src/infrastructure/db.rs:765-778`  
  High tuple type complexity (clippy `type_complexity`) reduces maintainability and readability.

## Simplification Targets
- Replace repeated migration-ALTER sequence with checked/targeted migration steps.
- Extract shared `load_configs` helper for ray scripts.
- Remove or make meaningful `GateSubscribeAttempt` delay policy.
