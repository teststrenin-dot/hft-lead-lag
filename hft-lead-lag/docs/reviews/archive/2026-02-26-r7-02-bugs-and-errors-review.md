# R7 — Bugs and Errors Review

Date: 2026-02-26

## Findings

### P0
1. Panic risk from `blocking_send` in async runtime path.
- Evidence: `src/infrastructure/db.rs:796`, `src/infrastructure/db.rs:802`.
- Impact: process/runtime crash under saturated queue/backpressure.
- Repro idea: saturate primary/overflow/retry/spillover queues and trigger enqueue in async loop.
- Fix: remove `blocking_send` from hot path, use non-blocking enqueue behavior.
- Status (this round): `fixed` with regression test `enqueue_command_backpressure_path_is_runtime_safe`.

### P1
1. Missing `TraderConfig` validation on batch ingress.
- Evidence: `src/trial_batch_protocol.rs:132`, `src/domain/screener/shadow_trader.rs:268`.
- Impact: overflow/wraparound or invalid trading behavior with extreme config values.

2. Unsanitized `submission_id` used as file path fragment.
- Evidence: `src/trial_queue_io.rs:297`, `src/trial_queue_io.rs:305`.
- Impact: path traversal/ack path corruption risk.

### P2
1. Candidate pool can contain stale/pruned symbols.
- Evidence: `src/domain/screener/catalog_cache.rs:55`, `src/domain/screener/mod.rs:488`.

2. NATR timeout path can overwrite with `0.0` and distort signal quality.
- Evidence: `src/runtime_setup.rs:94`, `src/runtime_setup.rs:95`.

3. Fingerprint deletion (`Some -> None`) not treated as change in watch flow.
- Evidence: `src/file_fingerprint.rs:38`, `src/runtime_hot_reload.rs:170`.

### P3
1. `trial_axes` swallows row decode errors (`filter_map(row.ok())`).
- Evidence: `src/api/handlers/trial_axes_support.rs:110`.
