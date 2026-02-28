# HFT-RM5 Evidence — Control-Plane Thread Isolation and Coalesced Apply

Date: 2026-02-28
Status: Completed
Scope: isolate control-plane execution from hot runtime workers and reduce per-tick screener pressure under burst updates

## Implemented changes
1. Control-plane worker runs in a dedicated OS thread with its own current-thread Tokio runtime.
2. Control updates are coalesced by `(symbol, exchange)` before apply:
   - latest update wins inside a flush window.
   - worker flushes on interval and on max-batch threshold.
3. Runtime-grid default config fanout was reduced from `1500` to `512` on default profile.

## Code evidence
1. Dedicated thread + coalesced loop:
   - `src/event_loop_control.rs`
   - `spawn_control_plane_worker_with_config`
   - `run_control_plane_worker_loop`
2. Coalescing regression test:
   - `src/event_loop_control.rs`
   - `control_plane_worker_coalesces_latest_update_within_flush_window`
3. Runtime-grid default cap update:
   - `src/runtime_grid.rs` (`RuntimeGridConfig::default`, default TOML template)
   - `config/runtime-grid.toml` (`max_configs = 512`)
4. Default-cap regression test:
   - `src/main_tests.rs`
   - `runtime_grid_config_default_matches_2core_profile`

## Verification commands
```bash
cargo test -q control_plane_worker_coalesces_latest_update_within_flush_window
cargo test -q control_plane_worker_
cargo test -q runtime_grid_config_default_matches_2core_profile
```

## Outcome
1. Control-plane work no longer depends on Tokio worker scheduling of the main runtime loop.
2. Repeated updates for the same symbol/exchange in burst windows are collapsed before `screener.update`.
3. Default config fanout matches 2-core profile and avoids accidental high-load startup defaults.
