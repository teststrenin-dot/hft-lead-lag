# Bugs & Errors Review (R4)

## Findings
- **P1** `src/infrastructure/db.rs:453`, `src/infrastructure/db.rs:476`  
  Trade batches are dropped when both primary and overflow queues are saturated. This is explicit behavior now, but still a correctness risk for persisted trade history under burst load.

- **P1** `src/main.rs:1545-1568`, `src/application/services/lead_lag.rs:70-100`  
  Runtime routes Binance to primary and Gate to hedge unconditionally, while config exposes swappable `primary_exchange` / `hedge_exchange`. Config and runtime behavior can diverge.

- **P2** `src/domain/screener/mod.rs:184-199`, `src/domain/screener/mod.rs:447-484`  
  Symbol catalog grows and every `/api/v1/screener` request rebuilds/sorts full rows (`O(n log n)`), increasing CPU and latency on long-running sessions.

- **P3** `src/main.rs:1061-1093`  
  Queue file is deleted after processing (including malformed payloads), leaving no replay artifact for failed submissions.

## Repro/Test Gaps
- No load test that validates acceptable drop behavior/alerts when DB queues saturate.
- No integration test asserting primary/hedge routing matches configured exchange roles.
