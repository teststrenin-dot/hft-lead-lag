# Forward Rust Runtime Runner Evidence

Date: 2026-02-28
Scope: remove Python/Ray dependency from runtime `forward` start path in API runner.

## 1) Contract
1. `forward` from UI/API is executed by internal Rust runner job.
2. Runner no longer spawns `python3 -m ray_driver forward`.
3. Forward flow uses existing runtime trial-batch queue/ack/control contracts:
   - enqueue batch to `config/trial-batches`,
   - wait scoped ack from `config/trial-acks`,
   - run budget window,
   - clear active run through `config/trial-control.json`.

## 2) Code evidence
1. `src/api/runner.rs`
   - `spawn_forward_job(...)` implements internal forward lifecycle.
   - `forward_phase_options(...)` validates/clamps forward controls.
   - `load_scout_references(...)`, `select_reference_ids(...)`, `load_configs_for_reference_ids(...)` build runtime config set from scout artifact + DB.
   - `enqueue_trial_batch(...)`, `try_read_trial_ack(...)`, `write_trial_control_clear_run(...)` bridge to runtime hot-reload control-plane.
   - `start(...)` branches by phase and routes `forward` to internal job path.
2. `src/api/runner/command.rs`
   - phase contract remains `scout` + `forward`; other phases rejected.

## 3) Validation
Commands:
```bash
cargo test -q api::runner::tests:: -- --nocapture
cargo test -q api::handlers::tests:: -- --nocapture
cargo check -q
cargo build -q
```

Observed:
1. Runner tests pass with internal forward contract.
2. Handler tests remain green.
3. Project builds successfully.

## 4) Result
1. Runtime forward orchestration no longer depends on Python/Ray process execution.
2. Forward start remains guarded by scout artifact prerequisites.
3. Existing runtime queue/ack safety semantics are preserved.
