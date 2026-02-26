# R6 — Logic & Math Review

## Scope
- Range: `0ef23f1..a583e39`
- Focus: ranking math, tie-breaks, eligibility and runtime logic invariants

## Findings
- **P0** Tie-break logic + same candidate inputs violates 2-portfolio objective.
  - Current ownership rule only replaces on strict better, not equal.
  - In runtime usage both portfolios receive same ranked stats.
  - This mathematically biases all ties to first iteration owner (A).
  - Refs:
    - `src/domain/screener/mod.rs:393`
    - `src/application/services/portfolio_runtime.rs:105`
    - `src/application/services/portfolio_runtime.rs:115`

- **P2** Rebalance cadence logic is data-driven by quote flow, not strict time-loop.
  - Can skip expected “every 2 minutes” behavior during no-tick periods.
  - Refs:
    - `src/domain/screener/quote_ingest.rs:85`
    - `src/domain/screener/quote_ingest.rs:106`
    - `src/domain/screener/mod.rs:384`

## Notes
- Eligibility formula and rank tuple implementation are otherwise coherent with v1 spec.
