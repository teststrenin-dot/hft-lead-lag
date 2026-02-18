# Shadow Trader & Screener Review Findings

**Date**: 2026-02-18
**Status**: Pending Fixes
**Reviewer**: Copilot

## 1. Critical Performance Issue: Lag Calculation
**Severity**: 🔴 Critical
**Location**: `src/api/screener.rs:140`

**Problem**:
The `lag_ms` (P50 median) calculation performs a full `O(n log n)` sort of the sliding window buffer on **every tick** (every update from WebSocket).
- With 50+ symbols updating frequently, this causes massive CPU usage (~150k sorts/sec).
- This blocks the Tokio executor and increases latency for all other tasks.

**Recommendation**:
- Throttle the recalculation of `lag_ms` to once per second (similar to `THRESHOLD_INTERVAL_MS`).
- The metric does not need 10ms resolution.

## 2. IO Blocking: Sequential NATR Fetches
**Severity**: 🟠 High
**Location**: `src/api/http_server.rs:281-304`

**Problem**:
The `enrich_gate_natr_30m` function iterates through symbols and awaits Gate REST API calls **sequentially** inside a loop.
- If the API takes 500ms, fetching 6 symbols blocks the screener thread for 3 seconds.
- This creates lag spikes in the UI and delays WebSocket processing if shared.

**Recommendation**:
- Use `futures::future::join_all` to execute all 6 requests in parallel.

## 3. Logic Flaw: Stale Signal Execution
**Severity**: 🟠 High
**Location**: `src/api/screener.rs:544-550`

**Problem**:
The Shadow Trader executes a pending signal after `EXECUTION_DELAY_MS` (10ms) **unconditionally**.
- If the price reverts (edge disappears) during those 10ms, the trade is still executed.
- This leads to entering positions with negative or zero edge in fast markets.

**Recommendation**:
- Add a "staleness check" right before execution: verify that `premium_bps` still satisfies the entry condition (or at least hasn't reverted past P50).

## 4. Mathematical Inaccuracy: Percentile Calculation
**Severity**: 🟡 Medium
**Location**: `src/api/screener.rs:809`

**Problem**:
The `percentile` helper uses `.round()` to find the index.
- This introduces a slight upward bias for P50 (median).
- In small sample sizes, this can shift the exit condition.

**Recommendation**:
- Use linear interpolation between the two nearest ranks for accurate P50.
