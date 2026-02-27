# R11 CP1 - Shadow Trader/Fleet Design Review

Date: 2026-02-27

## Findings

### P1
1. Corrected-time discontinuities can freeze or accelerate shadow lifecycle timers (fill/hold/timeout).
- Evidence: `src/domain/screener/tests.rs:271`, `src/domain/screener/tests.rs:298`, `src/domain/screener/quote_ingest.rs:109`, `src/domain/screener/state.rs:213`, `src/domain/screener/shadow_trader.rs:286`, `src/domain/screener/shadow_trader.rs:344`, `src/domain/screener/shadow_trader.rs:368`.
- Status: `open`.

2. Invalid timestamp fallback can bias global offset estimator and indirectly shift shadow behavior.
- Evidence: `src/domain/screener/utils.rs:61`, `src/domain/screener/utils.rs:145`, `src/domain/screener/mod.rs:165`, `src/domain/screener/mod.rs:313`, `src/domain/screener/clock_offset.rs:25`.
- Status: `open`.

### P2
1. Gate freshness remains strict prerequisite for fills/exits; correction shifts can freeze lifecycle when gate marked stale.
- Evidence: `src/domain/screener/shadow_trader.rs:249`, `src/domain/screener/shadow_trader.rs:251`, `src/domain/screener/shadow_trader.rs:263`, `src/domain/screener/tests.rs:340`.
- Status: `open`.

2. Corrected-time regressions can suppress entries by reducing baseline-ready samples.
- Evidence: `src/domain/screener/state.rs:205`, `src/domain/screener/shadow_trader.rs:455`, `src/domain/screener/shadow_trader.rs:471`, `src/domain/screener/tests.rs:271`.
- Status: `open`.

### P3
1. Drift monitoring is mostly observational and may not pre-alert control plane before shadow degradation.
- Evidence: `src/event_loop_core.rs:24`, `src/event_loop_core.rs:316`, `src/api/http_server.rs:36`, `docs/status/2026-02-26-business-logic-roadmap.md:89`.
- Status: `open`.
