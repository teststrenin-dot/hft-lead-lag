# R13 CP0 - Dead Code Review

Date: 2026-02-27

## Findings

### P3
1. `FleetIPC.batch_path` assigned but unused in current driver flow.
- Evidence: `ray_driver/ipc.py:51`, `ray_driver/ipc.py:73`.
- Status: `open`.

2. Legacy `.trial-ack` fallback path appears dormant for queue-based submission flow.
- Evidence: `ray_driver/ipc.py:66`, `ray_driver/ipc.py:74`, `src/trial_queue_io.rs:345`, `src/trial_queue_io.rs:361`.
- Status: `open`.

3. Legacy single-file trial batch watcher path appears dormant under current queue submission path.
- Evidence: `src/main.rs:228`, `src/runtime_hot_reload.rs:299`, `ray_driver/ipc.py:73`.
- Status: `open`.
