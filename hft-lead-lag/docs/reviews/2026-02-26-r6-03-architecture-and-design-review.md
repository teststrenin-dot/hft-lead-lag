# R6 — Architecture & Design Review

## What Improved
- Added explicit portfolio runtime model and runbooks/diagrams for operability.
- Added strict runtime mode validation path.

## Findings
- **P1** Restart architecture is only partially durable (assignment/guards durable, candidate history ephemeral).
  - Refs:
    - `src/runtime_setup.rs:181`
    - `src/domain/screener/mod.rs:377`
    - `src/domain/screener/mod.rs:408`

- **P2** Layering leakage: screener domain now orchestrates portfolio runtime and DB snapshot emission directly.
  - Refs:
    - `src/domain/screener/mod.rs:37`
    - `src/domain/screener/mod.rs:40`
    - `src/domain/screener/mod.rs:360`
    - `src/domain/screener/mod.rs:398`

- **P2** Snapshot persistence of two coupled tables is not atomic as a design unit.
  - Refs:
    - `src/infrastructure/db.rs:504`
    - `src/infrastructure/db.rs:531`
    - `src/infrastructure/db.rs:944`

## Architecture Score
- `6.8 / 10`
