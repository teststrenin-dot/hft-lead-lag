# R12 CP0 - Bugs and Errors Review

Date: 2026-02-27

## Findings

### P1
1. Undocumented `run_id` lease can reject valid trial-batch transitions in runtime.
- Evidence: `docs/README.md:55`, `docs/ray-asha-deep-dive.md:78`, `src/trial_batch_apply.rs:86`, `src/trial_batch_apply.rs:98`, `src/trial_batch_apply.rs:110`, `ray_driver/trainable.py:26`.
- Status: `open`.

### P2
1. Docs publish WS endpoint as available by default, but runtime default disables chart WS server.
- Evidence: `docs/README.md:143`, `src/main.rs:89`, `src/runtime_setup.rs:295`.
- Status: `open`.

### P3
1. Scout config ceiling drift: docs mention `3000`, code uses `5000`.
- Evidence: `docs/README.md:225`, `docs/ray-asha-deep-dive.md:145`, `ray_driver/scout.py:11`.
- Status: `open`.

2. CP0 requires version-tagged freeze, while contract docs still track moving `main @ HEAD` snapshots.
- Evidence: `docs/status/2026-02-26-business-logic-roadmap.md:82`, `docs/README.md:6`, `docs/ray-asha-deep-dive.md:6`.
- Status: `open`.
