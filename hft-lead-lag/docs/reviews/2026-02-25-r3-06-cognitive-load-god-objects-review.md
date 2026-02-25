# Cognitive Load & God Objects Review (R3)

## Findings
- **P1** `src/main.rs:1996-2122`  
  `main` is overloaded with subsystem setup + runtime behavior decisions; difficult local reasoning, high onboarding cost.

- **P2** `src/domain/screener/mod.rs:151-441`  
  `ScreenerStore` aggregates too many responsibilities (state, fleet lifecycle, persistence bridge, API row materialization).

- **P2** `src/main.rs:1585-1595`  
  `handle_exchange_tick` exceeds argument complexity (`too_many_arguments` clippy), increasing callsite and change friction.

- **P3** `src/domain/screener/mod.rs:517-518`  
  Test setup pattern uses `Default` + reassignment (`field_reassign_with_default`) and adds avoidable noise.

## Low-Cost Reductions
- Introduce context structs for high-arg functions.
- Split store responsibilities into smaller cohesive structs.
- Keep test fixture construction explicit in initializer.
