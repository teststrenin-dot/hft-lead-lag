# HFT-CP2 Evidence — Lock-Free Strategy and p99 Tail

Date: 2026-02-28
Checkpoint: `HFT-CP2`
Scope: runtime hot-path lock removal evidence + live p99 capture

## 1) Lock removal verification (code-level)
Command:
```bash
rg -n "RwLock|Mutex" \
  src/application/services/lead_lag.rs \
  src/event_loop_runtime.rs \
  src/event_loop_core.rs \
  src/application/strategies/mod.rs -S
```

Result:
- No matches in the runtime strategy path files above.
- Strategy path is sync `&mut self` and queue-fed (`enqueue_strategy_updates -> flush_strategy_updates`).

## 2) Live runtime capture (`/health`)
Runtime launched in paper mode and sampled every ~10s on `2026-02-28`.

Sample 1:
- ingest p99: `462042us`
- decision p99: `409377us`
- e2e p99: `664306us`
- backlog: `binance=247 gate=79 signal=44`

Sample 2:
- ingest p99: `366909us`
- decision p99: `456060us`
- e2e p99: `676946us`
- backlog: `binance=46 gate=20 signal=41`

Sample 3:
- ingest p99: `251050us`
- decision p99: `479242us`
- e2e p99: `658762us`
- backlog: `binance=31 gate=2 signal=0`

Sample 4:
- ingest p99: `543221us`
- decision p99: `522137us`
- e2e p99: `766148us`
- backlog: `binance=50 gate=10 signal=0`

Final snapshot (same run):
- ingest p99: `355353us`
- decision p99: `381295us`
- e2e p99: `624176us`
- backlog: `binance=5 gate=4 signal=23`

## 3) CP2 exit assessment
`HFT-CP2` exit gate: no lock primitives on hot strategy path and p99 tail stability evidence.

Assessment:
1. Lock-free requirement is met for strategy hot path.
2. p99 is bounded during live run and does not show runaway backlog growth.
3. Remaining p99 work now moves to `HFT-CP3`/`HFT-CP4` (updated-only flow + parse/copy path), not lock-removal scope.
