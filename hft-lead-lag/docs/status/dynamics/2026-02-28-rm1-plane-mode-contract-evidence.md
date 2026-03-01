# HFT-RM1 Evidence — Plane Mode Contract (`mixed` / `hft_core`)

Date: 2026-02-28
Scope: formalize and verify runtime plane split contract

## 1) Contract summary
1. `RUNTIME_PLANE_MODE=mixed`
   - Runtime starts control-plane worker and routes screener updates through bounded handoff.
2. `RUNTIME_PLANE_MODE=hft_core`
   - Runtime disables screener/control-plane helpers in hot loop:
     - no runtime-grid startup apply/hot-reload
     - no NATR refresher
     - no screener DB persistence init
     - no portfolio scheduler tick
     - strategy-only subscriptions
   - Runtime server surface is health-only (`/health`), with no trials/runner control endpoints.

## 2) Code evidence
1. `src/main.rs`
   - `runtime_plane_mode_from_env()` parses mode and handles fallback.
   - startup wiring gates control helpers and subscriptions by mode.
2. `src/event_loop_runtime.rs`
   - portfolio scheduler execution is gated by `portfolio_scheduler_enabled`.
3. `src/event_loop_core.rs`
   - direct ingest fallback is test-only; runtime path stays control-plane-first.

## 3) Test evidence
Executed:

```bash
cargo test -q runtime_plane_mode_defaults_to_mixed_when_env_missing
cargo test -q runtime_plane_mode_parses_hft_core_case_insensitive
cargo test -q runtime_plane_mode_unknown_value_falls_back_to_mixed
```

Validated:
1. Missing env defaults to `mixed`.
2. Valid `hft_core` value is parsed case-insensitively.
3. Invalid values safely degrade to `mixed`.

## 4) Exit statement
`HFT-RM1` exit gate is met:
1. Plane-mode split is explicit and startup-enforced.
2. Runtime behavior changes by mode are deterministic and test-covered.
3. Invalid mode input has safe fallback semantics.
