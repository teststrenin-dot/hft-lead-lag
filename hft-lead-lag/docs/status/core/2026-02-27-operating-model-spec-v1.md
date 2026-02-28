# Operating Model Spec v2 (Rust-Only Target)

Date: 2026-02-27
Status: Canonical operating model for `HFT-CP*`
Strategic anchor: `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
Last sync: 2026-02-28 (CP4 completed with parse-profile evidence + contract-priority regression guard)

## 1) Purpose
Define one executable operating model that binds business flow to implementation checkpoints and enforces Rust-only hot path.

## 2) Current vs target execution model
| Domain | Current | Target |
|---|---|---|
| Signal and shadow runtime | Rust | Rust |
| Candidate math and ranking | Rust | Rust |
| Portfolio assignment and guards | Rust | Rust |
| Runtime engine internals | String/HashMap/lock-heavy segments | `SymbolId` + array-state + updated-only event flow |
| Orchestration | Mixed (Rust API starts Python module) | Rust runtime hot path, Python only in cold/offline tooling |

## 3) Locked flow
`Signal -> Validation -> Competition -> Risk -> Capital -> Feedback`

### 3.1 Signal
1. Ingest and normalize market data.
2. Compute lead-lag context.
3. Produce shadow trades and events.

### 3.2 Validation
1. Build candidate events.
2. Apply eligibility gates.
3. Rank candidates deterministically.

### 3.3 Competition
1. Build disjoint shortlist/active allocation per portfolio.
2. Ensure runtime competition reads only updated symbols and uses deterministic state transitions.

### 3.4 Risk
1. Guard/cooldown/hard-reset logic.
2. Idempotent state updates and replay safety.

### 3.5 Capital
1. Allocation and rebalance policy.
2. Risk caps and kill switches.

### 3.6 Feedback
1. Operator-visible mode status and run artifacts.
2. Health/alerts/recovery telemetry.

## 4) Checkpoint boundaries
1. `HFT-CP0`: latency and allocation observability.
2. `HFT-CP1`: `SymbolId` and allocation removal.
3. `HFT-CP2`: lock-free strategy state.
4. `HFT-CP3`: updated-only event loop.
5. `HFT-CP4`: minimal-copy parse path.
6. `HFT-CP5`: deterministic replay harness.
7. `HFT-CP6`: execution fast path.
8. `HFT-CP7`: operations and recovery layer.

## 5) Mandatory invariants
1. No Python in target runtime hot path.
2. Deterministic ranking and state transitions.
3. Idempotent trade/snapshot application.
4. Explicit, observable mode boundaries (`scout`, `forward-only`, `promote`).

## 6) Change rule
Any behavior change touching this model requires:
1. Update this file.
2. Update roadmap and implementation status docs.
3. Attach test/metric evidence to a concrete `HFT-CP*`.
