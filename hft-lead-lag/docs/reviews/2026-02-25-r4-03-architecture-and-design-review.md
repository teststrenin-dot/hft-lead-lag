# Architecture & Design Review (R4)

## Findings
- **P1** `src/main.rs:968-1219`  
  One hot-reload task owns runtime-grid refresh, trial-control command handling, trial-batch file processing, and queue processing. This creates contention and high blast radius for changes.

- **P1** `src/main.rs:1031-1093`, `docs/ray-asha-deep-dive.md:286-293`  
  Trial-batch IPC still behaves as singleton control-plane path (single ack semantics, weak namespacing for concurrent drivers).

- **P2** `src/main.rs:1983-2117`  
  `main` is still monolithic bootstrap + runtime orchestration, coupling infrastructure wiring with control behavior.

- **P2** `src/api/runner.rs:583-760`  
  HTTP runner is tightly coupled to Python CLI argument shape, with no protocol/version contract layer.

## Direction
- Split control-plane loops (runtime-grid watcher vs trial-batch/control pipeline).
- Introduce a stable runner contract (typed RPC/manifest) between Rust API and Python driver.
