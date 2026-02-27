# R7 — Duplication, Redundancy, Over-Complexity Review

Date: 2026-02-26

## Findings

### P1
1. High duplication in exchange WS workers (binance/gate).
- Evidence: `src/infrastructure/exchanges/binance/mod.rs`, `src/infrastructure/exchanges/gate/mod.rs`.
- Recommendation: shared WS worker skeleton with exchange-specific adapters.

2. DB writer queue pipeline complexity is high (many bridge channels/tasks).
- Evidence: `src/infrastructure/db.rs:683`, `:721`, `:923`.

### P2
1. Duplicated SQL endpoint patterns (`trial_runs` vs `forward_runs`).
- Evidence: `src/api/handlers.rs:740`, `:846`.

2. Duplicated "best config per symbol" handlers.
- Evidence: `src/api/handlers.rs:519`, `:953`.

3. Template/route duplication around fleet/trials pages.
- Evidence: `src/api/templates.rs:5`, `:12`.

### P3
1. Similar crypto signer wrappers and nested JSON extractors can be collapsed.
- Evidence: `src/infrastructure/exchanges/common.rs`, `src/infrastructure/exchanges/gate/mod.rs`.
