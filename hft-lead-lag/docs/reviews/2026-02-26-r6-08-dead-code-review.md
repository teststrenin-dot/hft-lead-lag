# R6 — Dead Code Review

## Findings
- **P3** `PortfolioEngineV1::new()` appears production-unused (test convenience only).
  - Refs:
    - `src/application/services/portfolio_runtime.rs:90`
    - `src/application/services/portfolio_runtime_tests.rs:66`

- **P3** `TradingMode` currently has single variant and no behavioral branching beyond logging/validation.
  - Refs:
    - `src/config/mod.rs:95`
    - `src/main.rs:114`

## Confidence
- Medium: no clippy dead-code warnings, but semantic-unused surface exists and adds maintenance overhead.
