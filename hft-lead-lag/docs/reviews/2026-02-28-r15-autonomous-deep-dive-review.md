# Deep Dive Review — Autonomous Changes (CP2 → CP4)

Date: 2026-02-28  
Range: `05f9697..1d4fe35`  
Scope: CP2 completion evidence, CP3 event-loop refactor, CP4 symbol/cache/parser hardening

## Findings (ordered by severity)

### P1 — Starvation risk in pending signal scheduler under sustained low-id churn
- Area: `PendingSymbolSet::pop_first` + budgeted loop in signal tick.
- Evidence:
  - `src/event_loop_core.rs`: `pop_first()` always returns the smallest pending `SymbolId`.
  - `src/event_loop_core.rs`: `handle_signal_tick()` processes up to `SIGNAL_CHECK_BUDGET_PER_TICK`.
- Why this matters:
  - With persistent updates on low `SymbolId` symbols and finite budget, higher ids can remain pending for long periods (or indefinitely in pathological load), reducing fairness and potentially suppressing valid signals for part of the universe.
- Recommendation:
  - Switch from strict min-id draining to rotating cursor / round-robin fairness for pending ids, while preserving bounded work per tick.

### P2 — Potential unbounded cache growth from raw symbol bytes after non-UTF8 preservation
- Area: `SymbolCache::intern_bytes`.
- Evidence:
  - `src/domain/symbols.rs`: `bytes_cache: DashMap<Vec<u8>, Bytes>`.
  - `src/domain/symbols.rs`: every new raw byte sequence is inserted (`symbol.to_vec()`), no bounding/eviction/validation.
- Why this matters:
  - CP4 improved correctness by preserving raw bytes, but it also allows unlimited unique keys from input stream payloads.
  - On malformed/noisy streams this can become a memory pressure vector.
- Recommendation:
  - Add safeguards: symbol length/canonical format validation and bounded cache policy (or reject unknown/non-whitelisted symbols before intern).

### P3 — Public API contraction in `common.rs` may break external crate consumers
- Area: dynamic field-name extractor wrappers made test-only.
- Evidence:
  - `src/infrastructure/exchanges/common.rs`: `extract_json_*_field(..., field: &str)` are now behind `#[cfg(test)]`.
- Why this matters:
  - Internal runtime is cleaner (good), but if any external binary/crate consumed these public helpers in non-test builds, it will now fail to compile.
- Recommendation:
  - If library compatibility matters, keep deprecated non-test shims (non-hot path) for one transition cycle.
  - If not needed, document this as intentional API break.

## What is good (confirmed)
1. CP2 lock-free migration is real in runtime hot path (`RwLock/Mutex` removed from strategy path).
2. CP3 materially improved runtime path:
   - Pending signal set is bitset-based.
   - Strategy queue carries tickers directly; cache-lookup clone in flush path removed.
3. CP4 moved runtime away from dynamic field-name formatting path (`format!`) by keeping wrappers test-only.
4. Verification is strong for this round:
   - `cargo test -q` passes (`313 passed, 0 failed, 2 ignored` in latest run).
   - `cargo check --all-targets` passes.

## Regression assessment
1. No hard functional regression detected by current automated tests.
2. Main residual risk is runtime fairness under load (P1) and cache growth guardrails (P2), both not yet covered by stress tests.

## Suggested next fixes (priority)
1. Implement fair pending scheduler in CP3 follow-up (P1).
2. Add bounded/validated symbol cache policy in CP4 follow-up (P2).
3. Decide and document API-compat stance for `common.rs` test-only wrappers (P3).
