# R6 — Bugs & Errors Review

## Scope
- Range: `0ef23f1..a583e39`
- Focus: correctness, state durability, ordering/race, failure modes

## Findings
- **P0** Portfolio B can be starved in live rebalance by tie handling + identical pools.
  - `maybe_rebalance_portfolios` passes same candidates to A/B.
  - Ownership transfer only on strict `Ordering::Less`.
  - With equal stats, ownership remains A and B can stay empty.
  - Refs:
    - `src/domain/screener/mod.rs:393`
    - `src/application/services/portfolio_runtime.rs:105`
    - `src/application/services/portfolio_runtime.rs:115`

- **P1** Restart persistence gap can collapse restored assignment to empty.
  - Startup restores assignment/guards, but not trade accumulators.
  - Next rebalance uses only in-memory accumulators and overwrites assignment snapshot.
  - Refs:
    - `src/runtime_setup.rs:181`
    - `src/domain/screener/mod.rs:264`
    - `src/domain/screener/mod.rs:377`
    - `src/domain/screener/mod.rs:390`
    - `src/domain/screener/mod.rs:408`

- **P1** Flush/snapshot ordering may acknowledge completion before snapshot durability.
  - Pending flushes are completed from `observed_max_seq` before snapshot writes run.
  - Refs:
    - `src/infrastructure/db.rs:841`
    - `src/infrastructure/db.rs:938`
    - `src/infrastructure/db.rs:944`

- **P2** Snapshot can regress to stale persisted state under queue saturation/out-of-order delivery.
  - Multi-queue async drain has no monotonic seq guard at snapshot apply.
  - Refs:
    - `src/infrastructure/db.rs:637`
    - `src/infrastructure/db.rs:864`
    - `src/infrastructure/db.rs:936`

- **P2** Archive fallback still has idempotency hole if stash rename also fails.
  - Failed stash leaves original `.json` queue payload for re-consume.
  - Refs:
    - `src/trial_queue_io.rs:228`
    - `src/trial_queue_io.rs:149`
    - `src/runtime_hot_reload.rs:148`

## Regression Status
- Functional tests green, but production-risk regressions remain for portfolio assignment durability and queue ordering behavior.
