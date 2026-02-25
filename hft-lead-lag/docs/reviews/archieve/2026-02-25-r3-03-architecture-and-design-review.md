# Architecture & Design Review (R3)

## Findings
- **P1** `src/main.rs:1996-2122`  
  Monolithic bootstrap/orchestration in `main` mixes config, exchange wiring, persistence, runtime-grid, API, and event-loop control. High coupling and poor testability.

- **P2** `src/api/handlers.rs:238-379`  
  Handlers embed SQL/statistics/business aggregation inline. API layer is tightly coupled to schema and calculation details.

- **P2** `src/domain/screener/mod.rs:151-441`  
  `ScreenerStore` is a god object: state cache + patch application + db writer bridge + fleet ticking + DTO projection.

- **P3** `src/infrastructure/db.rs:31-567`  
  Schema/migrations/helpers/async writer logic are combined in one large module, raising blast radius for small changes.

## Direction
- Split bootstrap into subsystem initializers.
- Extract repository/service layer from HTTP handlers.
- Decompose `ScreenerStore` by responsibilities (state store, patch coordinator, persistence bridge).
