# R6 — Preventive Architecture Review

## Findings
- **P1** Flush barrier semantics do not fully protect portfolio snapshot durability ordering.
  - Refs:
    - `src/infrastructure/db.rs:841`
    - `src/infrastructure/db.rs:938`
    - `src/infrastructure/db.rs:944`

- **P1** Restart safety gap for cumulative candidate history can wipe effective portfolio state.
  - Refs:
    - `src/runtime_setup.rs:181`
    - `src/domain/screener/mod.rs:377`
    - `src/domain/screener/mod.rs:408`

- **P2** Non-atomic state+guard persistence allows partial snapshot writes.
  - Refs:
    - `src/infrastructure/db.rs:504`
    - `src/infrastructure/db.rs:531`
    - `src/infrastructure/db.rs:944`

- **P2** Archive fallback preserves payload but lacks deterministic retry/quarantine path when stash rename fails.
  - Refs:
    - `src/trial_queue_io.rs:228`
    - `src/trial_queue_io.rs:149`

## Preventive Readiness Score
- `6.4 / 10`
