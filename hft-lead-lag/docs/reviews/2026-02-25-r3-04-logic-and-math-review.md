# Logic & Math Review (R3)

## Findings
- **P1 (logic/resource)** `src/infrastructure/db.rs:368-441`  
  Deferred-overflow logic is mathematically unbounded in queued work under sustained saturation (`N sends -> N deferred senders`). No invariant limits outstanding backlog.

- **P2 (ordering logic)** `src/main.rs:542-575`  
  Comparator prefers `Some(timestamp)` over `None` unconditionally; this creates starvation for valid non-timestamp files.

- **P3 (consistency/readability)** `src/main.rs:990-993`  
  `map_or(true, …)` is equivalent to `is_none_or(...)`; low risk but current style obscures intent.

## Recommended Tests
- Saturation soak: verify bounded deferred objects (or explicit drop counter increase).
- Queue fairness: ensure non-timestamp queue file is eventually processed amid timestamped arrivals.
