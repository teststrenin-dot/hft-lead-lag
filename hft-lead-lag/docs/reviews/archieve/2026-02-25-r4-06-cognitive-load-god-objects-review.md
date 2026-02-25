# Cognitive Load & God Objects Review (R4)

## Findings
- **P2** `src/main.rs:1-2117`  
  `main.rs` remains a high-cognitive-load god module with mixed responsibilities (domain helpers, watchers, API boot, runtime loop, infra wiring).

- **P2** `src/main.rs:968-1219`  
  `spawn_runtime_grid_hot_reload` centralizes multiple state machines and mutable state, making local reasoning difficult.

- **P2** `src/domain/screener/mod.rs:151-484`  
  `ScreenerStore` still combines storage, fleet state transitions, persistence bridge, and API projection.

- **P3** `src/main.rs:1983-2117`  
  Bootstrap flow has long linear dependency chain with limited isolation points for targeted tests.

## Low-Cost Reductions
- Extract watcher components into dedicated modules/tasks.
- Introduce explicit control-plane state structs and interfaces.
