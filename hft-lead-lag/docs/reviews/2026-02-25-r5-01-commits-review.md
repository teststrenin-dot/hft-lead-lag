# R5 — Commits Review

## Scope
- Range: `e1af2ee..HEAD` (`bb8f791`)
- Commits: `53`
- Type mix: `refactor 29`, `fix 7`, `perf 6`, `feat 4`, `docs 5`, `chore 1`, `hft-lead-lag 1`

## What Was Good
1. Большой монолит `main.rs` декомпозирован серией атомарных коммитов в целевые модули (`event_loop_*`, `runtime_*`, `trial_*`), что заметно улучшило reviewability и локальность изменений.
2. Сообщения коммитов в целом дисциплинированы (`fix/refactor/perf`) и отражают intent.
3. Параллельно с изменениями добавлены/расширены тесты в критических местах (`main_tests.rs`, `api/handlers/tests.rs`, `domain/screener/tests.rs`, `ray_driver/tests`).

## What Was Weak
1. В ряде коммитов runtime-контракт и IPC-контракт менялись асимметрично (часть защит появилась в Rust без симметричного явного контракта в Python IPC payload).
2. Есть смысловые коммиты, где в одном шаге смешаны рефактор и behavioral-risk изменения (усложняет bisect и быстрый rollback).

## Findings
- **P2** Lease/takeover контракт между Rust trial-batch и Python driver остается хрупким.
  - `TrialBatch` поддерживает `allow_run_id_takeover`, но `FleetIPC.submit_batch()` его не отправляет.
  - Смягчение есть (`clear_active_run` в `scout/expand`), но в `trainable` cleanup отсутствует.
  - Риски: зависимость от корректного внешнего cleanup-path.
  - Refs:
    - `src/trial_batch_protocol.rs:20`
    - `src/trial_batch_apply.rs:86-116`
    - `ray_driver/ipc.py:57-79`
    - `ray_driver/scout.py:76-79`
    - `ray_driver/expand.py:75-79`
    - `ray_driver/trainable.py:18-52`

## Commit Quality Score
- **7/10**
- Причина: сильная декомпозиция и тестовая дисциплина, но есть остаточные контракто-сцепки между runtime и driver paths.
