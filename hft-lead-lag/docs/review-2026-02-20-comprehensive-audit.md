# Comprehensive Multi-Review (2026-02-20)

**Date:** 2026-02-20  
**Scope:** commits, bugs/errors, architecture/design, logic/math, duplication, cognitive load, preventive architecture, dead code, separate design review for Screener and Shadow Fleet.

---

## 1) Method and verification

Evidence collected from full source/docs review + command verification:

1. `cargo test` -> PASS (`65 passed`, `0 failed`)
2. `cargo clippy --all-targets -- -D warnings` -> FAIL (`15` lint-errors)
3. Full repo scan: `src/*`, `docs/*`, git history.

---

## 2) Severity-first findings

### P1 (must fix first)

1. Trade parsing likely wrong/incomplete for exchange payloads.
   - `src/infrastructure/exchanges/binance/mod.rs:133`
   - `src/infrastructure/exchanges/gate/mod.rs:128`
   - `src/infrastructure/exchanges/gate/mod.rs:475`

2. Mixed time domains (exchange clock vs local receive clock) inside lag/hold metrics.
   - `src/domain/screener/mod.rs:123`
   - `src/domain/screener/state.rs:101`
   - `src/domain/screener/shadow_trader.rs:206`
   - `src/domain/screener/shadow_trader.rs:423`

3. Runtime/event-loop cost is O(symbols) per tick for strategy updates.
   - `src/main.rs:313`
   - `src/main.rs:318`
   - `src/main.rs:330`

4. Config contract vs runtime behavior mismatch (settings exist but not fully honored).
   - `src/config/mod.rs:20`
   - `src/config/mod.rs:29`
   - `src/main.rs:22`
   - `src/main.rs:497`
   - `src/application/strategies/mod.rs:54`

5. Config identity incompleteness for fleet analytics:
   `min_baseline_samples` affects logic, but not included in `config_id`/DB schema.
   - `src/domain/screener/trader_config.rs:34`
   - `src/domain/screener/trader_config.rs:61`
   - `src/infrastructure/db.rs:27`

6. REST tests depend on live network endpoints (fragile in CI/offline).
   - `src/infrastructure/rest/mod.rs:389`
   - `src/infrastructure/rest/mod.rs:400`

### P2

7. Domain layer depends on infra writer directly (leaky layer boundary).
   - `src/domain/screener/mod.rs:27`
   - `src/domain/screener/mod.rs:159`

8. Baseline leakage in `detect_gap`: current sample enters baseline window before signal check.
   - `src/domain/screener/state.rs:155`
   - `src/domain/screener/shadow_trader.rs:364`

9. High cognitive load / god-object tendencies.
   - `src/main.rs:1`
   - `src/domain/screener/shadow_trader.rs:1`
   - `src/api/templates.rs:13`

10. Dead/underused paths.
    - `src/application/services/risk.rs:44`
    - `src/infrastructure/rest/mod.rs:16`
    - `src/infrastructure/exchanges/gate/mod.rs:49`

### P0 status

No confirmed active-runtime P0 found in current snapshot (test suite green).  
Primary risk band is P1 quality/robustness, not immediate crash-level faults.

---

## 3) Commits review

### What is good

1. Fast iteration and decomposition work are visible in recent history.
2. Runtime strategy modularity scaffold exists (`StrategyKind`, `build_runtime_strategy`, `RuntimeStrategy`).
3. Several regressions from earlier commits are already fixed in later commits.

### Main commit-level risks

1. High churn in `src/main.rs` + connectors increases regression probability.
2. History includes explicit broken WIP commit (`aca81f0`) before recovery.
3. Docs frequently lag behind runtime commit snapshots (`docs/README.md` hash drift risk).

---

## 4) Bugs and errors review

1. Parser-level correctness is the largest technical risk.
2. Time normalization policy is not strict enough for robust cross-exchange inference.
3. `clippy -D warnings` failing indicates baseline code-quality gate is currently red.

---

## 5) Architecture and design review

1. Positive: strategy runtime abstraction is a valid step toward multi-strategy operation.
   - `src/application/strategies/mod.rs:22`
2. Gap: domain/infrastructure boundary is still blurred (`DbWriter` in domain store).
3. Gap: runtime orchestration and business decisions are still heavily concentrated in `main`.

---

## 6) Logic and math review

1. Core bps math and fee handling are coherent.
2. Risk: signal baseline may self-dilute due to current-sample inclusion.
3. Risk: mixed clocks can bias lag and hold-time statistics.
4. Ranking math is useful but still sensitive to small sample and selection bias.

---

## 7) Duplication / overengineering review

1. Binance/Gate WS loops share near-identical reconnect/backpressure flows.
   - `src/infrastructure/exchanges/binance/mod.rs:162`
   - `src/infrastructure/exchanges/gate/mod.rs:207`
2. Fleet SQL ranking endpoints repeat aggregation logic variants.
   - `src/api/handlers.rs:166`
   - `src/api/handlers.rs:236`
   - `src/api/handlers.rs:316`
3. Several config knobs exist but are not end-to-end wired into runtime behavior.

---

## 8) Cognitive load and god objects review

1. `src/main.rs` is still a large multi-responsibility composition root.
2. `shadow_trader.rs` keeps many responsibilities (state machine, analytics DTOs, chart formatting, tests).
3. Large inline HTML/JS templates increase maintenance pressure in backend file.

---

## 9) Preventive architecture review

Required preventive controls:

1. Enforce CI quality gates (`cargo test`, `cargo clippy -D warnings`) on push.
2. Introduce strict clock-policy abstraction for metrics and decisions.
3. Separate domain ports from infra adapters for persistence and enrichment.
4. Add parser contract tests with recorded real payload fixtures.
5. Keep strategy module onboarding checklist as stable design contract.

---

## 10) Dead code review

1. `RiskManager` is only partially integrated into runtime decision flow.
2. `RestConfig` type is present but not effectively used by clients.
3. `is_authenticated` field in Gate connector has no operational impact.

---

## 11) Separate design review: Screener

### Strengths

1. `SymbolState` split keeps per-symbol data localized.
2. Screener rows and chart/debug DTO exposure are practical for ops.
3. Shared sample buffer is efficient for multi-trader usage.

### Weak points

1. `ScreenerStore::update` combines ingestion, metrics, fleet tick, and db drain.
   - `src/domain/screener/mod.rs:109`
2. Domain layer directly handles infra persistence dependency.
3. Clock policy and normalization are not explicit enough at boundary level.

---

## 12) Separate design review: Shadow Fleet

### Strengths

1. Shared `PriceSamples` + per-config trader state is good for memory/runtime tradeoff.
2. Grid generation and config hashing are deterministic.
3. Runtime pruning reduces long-run computation overhead.

### Weak points

1. Irreversible prune policy can hide recovery of regime-sensitive configs.
2. `ShadowTrader` file remains too broad (core logic + presentation + large tests).
3. Grid execution cost can be heavy for 2-core server at higher symbol coverage.

---

## 13) GitHub-ready backlog (from this review)

Suggested issue queue:

1. `P1: Fix Binance/Gate trade parser correctness and add fixture tests`
2. `P1: Introduce unified clock-policy for screener/fleet metrics`
3. `P1: Make runtime use config volume_filter/enable semantics consistently`
4. `P1: Include min_baseline_samples in config identity and DB schema`
5. `P1: Split network integration tests from unit tests (REST)`
6. `P2: Remove domain->infra coupling via persistence port`
7. `P2: Reduce O(N symbols) per-tick strategy updates`
8. `P2: Break down shadow_trader and main to lower cognitive load`

---

## 14) Notes

This document captures review output only.  
No strategy behavior changes are applied in this document commit by itself.
