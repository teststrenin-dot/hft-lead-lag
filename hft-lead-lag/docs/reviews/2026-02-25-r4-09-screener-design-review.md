# Screener Design Review (R4)

## Findings
- **P1** `src/main.rs:1545-1568`, `src/application/services/lead_lag.rs:70-100`  
  Strategy config allows primary/hedge exchange choice, but runtime feed routing remains hardcoded Binance->primary and Gate->hedge.

- **P1** `src/main.rs:1559-1578`, `src/main.rs:1976-1979`  
  Signal evaluation is full-universe sequential polling at fixed interval, creating scaling pressure on decision latency.

- **P2** `src/api/handlers.rs:187-202`, `src/infrastructure/enrichment.rs:14-67`  
  `/api/v1/screener` performs synchronous enrichment calls in request path and creates REST clients per call for cache misses.

- **P2** `src/domain/screener/mod.rs:447-484`  
  Row materialization clones/sorts full dataset per request rather than reusing incremental/cached views.

## Design Direction
- Align runtime routing with strategy exchange-role config.
- Shift signal checks to updated-symbol driven or bounded-parallel model.
- Move enrichment to cached/background path and keep API response path deterministic.
