# R5 — Bugs & Errors Review

## Scope
- Range: `e1af2ee..HEAD`
- Focus: runtime correctness, concurrency, IO error paths, data loss/regression risks

## Findings
- **P1** DB flush barrier не гарантирует опустошение всех writer-очередей.
  - `flush_all()` отправляет `Flush` только в primary queue, while overflow/retry/spillover/backpressure идут отдельными каналами/тасками.
  - `flush_db_writer()` используется как boundary в trial/runtime apply.
  - Риск: подтверждение apply при фактически недофлашенных batches.
  - Refs:
    - `src/infrastructure/db.rs:533-541`
    - `src/infrastructure/db.rs:565-616`
    - `src/trial_batch_apply.rs:149-151`
    - `src/runtime_hot_reload.rs:222-224`

- **P1** Возможна рассинхронизация DB и runtime при reject patch.
  - В `apply_trial_batch()` upsert в DB выполняется до `try_apply_fleet_patch`.
  - Если patch отвергнут, runtime не применил, а `configs` в DB уже обновлены.
  - Refs:
    - `src/trial_batch_apply.rs:127-136`

- **P2** Потенциальная потеря queue payload на archive error.
  - При ошибке `create_dir_all`/`rename` исходный queue-файл удаляется.
  - Риск silent loss при transient FS проблемах.
  - Refs:
    - `src/trial_queue_io.rs:220-231`
    - `src/trial_queue_io.rs:238-250`

## Regression Status
- Functional regression по текущим автотестам не подтвержден (`cargo test`, `pytest`, `clippy` зелёные).
- Эксплуатационные регрессии остаются в saturation/IO failure сценариях.
