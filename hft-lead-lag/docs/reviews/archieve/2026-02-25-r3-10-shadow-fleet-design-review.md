# Shadow Fleet Design Review (R3)

## Findings
- **P2** `src/domain/screener/shadow_fleet.rs:228-293,503-523` and `src/api/handlers.rs:204-215`  
  Policy scoring/gating model exists but is not driving runtime behavior and is not externally observable via API.

- **P2** `src/domain/screener/shadow_fleet.rs:355-470`, `src/domain/screener/mod.rs:412-438`  
  `pending_trades` can grow unbounded when db writer is absent/unavailable because drain occurs only when writer is present.

- **P3** `src/domain/screener/shadow_trader.rs:222-255,423-458`  
  Baseline scanning is recomputed per trader per tick; shared computations are not hoisted, limiting scalability for large config grids.

## Recommendations
- Either wire policy gate into execution + expose snapshots, or remove policy layer.
- Make pending-trade buffer bounded and independent from writer presence.
- Extract shared baseline computation per symbol/tick and feed traders with precomputed signal context.
