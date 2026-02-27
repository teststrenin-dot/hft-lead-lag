# R10 CP2 - Preventive Architecture Review

Date: 2026-02-27

## Findings

### P1
1. Missing invariant that stale opposite-side quotes must not freeze risk exits.
- Evidence: `src/domain/screener/shadow_trader.rs:248`, `src/domain/screener/shadow_trader.rs:263`.
- Status: `open`.

2. Missing invariant for strict entry-time run binding when entry run_id is absent.
- Evidence: `src/domain/screener/shadow_fleet.rs:462`, `src/domain/screener/shadow_trader.rs:427`.
- Status: `open`.

### P2
1. Missing guardrail for corrected timestamp step-back behavior.
- Evidence: `src/domain/screener/clock_offset.rs:46`, `src/domain/screener/state.rs:83`.
- Status: `open`.

2. No explicit test contract for cycle-tracker cleanup/open-cycle expiry.
- Evidence: `src/domain/screener/cycle_tracker.rs:79`.
- Status: `open`.
