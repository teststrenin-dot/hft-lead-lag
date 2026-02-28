# R15 Autonomous Remediation

Date: 2026-02-28  
Applies to findings from:
- `2026-02-28-r15-autonomous-*.md`
- `2026-02-28-r15-autonomous-deep-dive-review.md`

## Closed
1. `P1` Pending-scheduler starvation risk.
- Fix: `PendingSymbolSet` switched from min-id drain behavior to cursor-based fair draining.
- Evidence: `src/event_loop_core.rs`.
- Test: `pending_symbol_set_uses_fair_cursor_under_reinserts`.

2. `P2` Raw-byte symbol cache growth risk (no guardrails).
- Fix: cacheability validation + size caps for symbol and gate-contract caches.
- Evidence: `src/domain/symbols.rs`.
- Tests:
  - `test_symbol_bytes_preserves_non_utf8_payload`
  - `test_symbol_bytes_cache_is_capped`

3. `P3` Potential API break from test-only dynamic wrappers.
- Fix: restored non-test availability of `extract_json_*_field(...)` wrappers.
- Evidence: `src/infrastructure/exchanges/common.rs`.

## Still Open
1. `P2` CP2 performance evidence remains directional (no strict controlled before/after benchmark protocol in docs).
2. `P3` Queue payload still carries unused `symbol_id` in strategy-update queue tuple; low-priority cleanup.

## Verification
1. `cargo test -q`: pass (`226 + 88 + 1`, `0 failed`, `2 ignored`).
2. `cargo check --all-targets -q`: pass.
