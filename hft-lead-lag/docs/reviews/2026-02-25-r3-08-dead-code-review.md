# Dead Code Review (R3)

## Findings
- **P2** `src/domain/screener/shadow_fleet.rs:503-523`  
  `policy_snapshots` / `top_policy_configs` are implemented but not used by runtime decision path and not exposed via current handlers.

- **P2** `src/infrastructure/db.rs:130-209`  
  Startup migration sequence is effectively dead compatibility code for already-present columns; errors suppressed and behavior impact near-zero.

- **P3** `src/main.rs:1610-1656`  
  `GateSubscribeAttempt` + `should_delay_after_gate_subscribe_attempt` behave like dead branching (all outcomes delay).

## Decision Needed
- Either integrate these paths into observable/runtime behavior, or delete/reduce them to shrink maintenance surface.
