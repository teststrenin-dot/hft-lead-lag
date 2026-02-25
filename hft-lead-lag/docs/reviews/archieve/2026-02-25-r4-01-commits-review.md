# Commits Review (R4)

## Reviewed Commits
- `3697b56` — fix: harden trial batch/ack pipeline and saturation handling
- `b7dc5cf` — fix: bound db saturation path and improve trial queue fairness
- `b25e917` — docs: add r3 review pack with dead-code and redundancy audit
- `e6d6535` — docs: archive v1 reviews and add r2 review pack
- `0edcb90` — docs: archive r2 review pack
- `b1f0487` — refactor: reduce review debt in db migrations, policy API, and ray config loading
- `6e09412` — fix: bound fleet pending queue when db writer is absent
- `3f519ed` — chore: clear remaining clippy smells in tick path

## Findings
- **P1** `src/infrastructure/db.rs:453`, `src/infrastructure/db.rs:476`  
  Saturation handling switched from unbounded defer to bounded drop. This removes OOM-style growth but introduces explicit trade-batch loss when both queues are full.
- **P2** `src/main.rs:968-1219`  
  Trial-batch/control/runtime-grid logic remains concentrated in one watcher task; recent commits improved local behavior but did not reduce orchestration coupling.
- **P3** `src/infrastructure/db.rs:24-209`  
  Migration safety improved via conditional column checks, but upgrade-path verification still lacks snapshot-style migration tests.

## Regression Status
- Tests and clippy are green on current `main`.
- No immediate functional break observed in reviewed commits.
- Remaining regression surface is mostly overload/operational (queue saturation and control-loop coupling).
