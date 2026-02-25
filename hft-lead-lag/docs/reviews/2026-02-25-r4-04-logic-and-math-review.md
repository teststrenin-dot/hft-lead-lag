# Logic & Math Review (R4)

## Findings
- **P1 (logic contract)** `src/main.rs:1545-1568`, `src/application/services/lead_lag.rs:70-100`  
  Configurable primary/hedge exchange roles are not reflected in event routing logic.

- **P1 (latency logic)** `src/main.rs:1559-1578`, `src/main.rs:1976-1979`  
  Signal checks run sequentially for all symbols every 100ms tick, so decision latency degrades with universe size.

- **P2 (scoring math)** `src/domain/screener/shadow_fleet.rs:228-277`  
  Shadow-fleet score blends terms with inconsistent normalization (`avg_pnl_pct` vs ratio-normalized rate terms), weakening score interpretability.

- **P2 (resource logic)** `src/infrastructure/db.rs:453-491`  
  Saturation policy is bounded but lossy; system invariants should explicitly define acceptable loss envelope and alerting thresholds.

## Recommended Tests
- Config-routing test: flip primary/hedge and assert runtime feed mapping changes accordingly.
- Scale test: measure signal-loop lag as symbol count grows.
- Scoring invariants test: validate monotonicity/sensitivity of shadow-fleet score inputs.
