# HFT-RM3 Evidence — 2-Core Default Cap Profile

Date: 2026-02-28
Scope: freeze and enforce default host-budget caps for 2-core runtime

## 1) Frozen default profile (runtime startup)
1. `MAX_STRATEGY_SYMBOLS` default cap: `64`
2. `MAX_SCREENER_SYMBOLS` default cap: `128`
3. `MAX_RUNTIME_GRID_CONFIGS` default cap: `512`

All three caps remain overrideable by env vars when explicit operator tuning is needed.

## 2) Code evidence
1. `src/main.rs`
   - constants:
     - `DEFAULT_MAX_STRATEGY_SYMBOLS_2CORE`
     - `DEFAULT_MAX_SCREENER_SYMBOLS_2CORE`
     - `DEFAULT_MAX_RUNTIME_GRID_CONFIGS_2CORE`
   - cap application:
     - `apply_symbol_cap(...)` uses env override or frozen default
     - `apply_runtime_grid_config_cap(...)` uses env override or frozen default
   - startup wiring:
     - strategy/screener symbol lists and runtime-grid configs are capped before runtime usage

## 3) Test evidence
Executed:

```bash
cargo test -q apply_symbol_cap_uses_2core_default_when_env_missing
cargo test -q apply_runtime_grid_config_cap_uses_2core_default_when_env_missing
cargo test -q apply_symbol_cap_
cargo test -q apply_runtime_grid_config_cap_
```

Coverage:
1. Default profile applies when env vars are absent.
2. Env override still works and takes precedence over defaults.

## 4) Exit statement
`HFT-RM3` exit gate is met:
1. Host caps are explicit and always enforced at startup.
2. Production-safe 2-core default profile is frozen and documented.
3. Operator override path remains explicit and bounded by env.
