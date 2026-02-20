# Sprint 007 — Pre-Live Reliability and Release Readiness

**Window:** 3 рабочих дня  
**Primary objective:** сделать архитектуру и эксплуатацию готовыми к controlled pre-live rollout.

---

## 1) Scope

In-scope:

1. Architecture seam hardening (domain vs infra boundaries).
2. Hot-path complexity/perf control.
3. CI quality gates and release checks.
4. Operational runbooks and rollback drills.

Out-of-scope:

1. Full production capital deployment.
2. Exchange-specific live order router overhaul.

---

## 2) Key risks addressed

1. Скрытая связанность и труднооткатываемые изменения.
2. Регрессии hot path latency при расширении функционала.
3. Недостаточная доказательная база перед pre-live.

---

## 3) Phases

### Phase 0 — Architecture boundary map

Deliverables:

1. Explicit dependency map: domain, application, infrastructure.
2. List of boundary violations and refactor order.

Primary target:

1. Remove direct `domain -> infrastructure` dependencies.

Exit criteria:

1. Refactor checklist frozen.

### Phase 1 — Boundary refactor

Deliverables:

1. Introduce ports/interfaces for persistence and enrichment where needed.
2. Move infra-specific types out of domain state structs.

Touched paths:

1. `src/domain/screener/mod.rs`
2. `src/infrastructure/db.rs`
3. `src/application/*`

Verification:

1. Compile/test pass.
2. No direct infra imports in domain modules for persistence path.

Exit criteria:

1. Layer boundaries are explicit and enforceable.

### Phase 2 — Hot-path performance pass

Deliverables:

1. Reduce O(symbols) updates on each tick to event-local updates where possible.
2. Add lightweight perf counters for update loop cost.

Touched paths:

1. `src/main.rs`
2. `src/application/strategies/*`
3. `src/domain/screener/*`

Verification:

1. Benchmark/trace snapshots before and after.
2. No increased message drop counters.

Exit criteria:

1. Throughput/latency profile improved or unchanged under load.

### Phase 3 — CI/CD quality gates

Deliverables:

1. Enforced checks on push/PR:
   - `cargo check --all-targets`
   - `cargo test`
   - `cargo clippy --all-targets -- -D warnings`
2. Optional split for live integration tests as opt-in stage.

Verification:

1. CI pipeline green on clean branch.

Exit criteria:

1. Regressions blocked automatically.

### Phase 4 — Ops runbooks and rollback drill

Deliverables:

1. Runbook for incident classes:
   - parser anomaly,
   - drift spike,
   - DB writer saturation,
   - policy thrash.
2. Rollback checklist with known-good commit references.
3. Pre-live go/no-go checklist template.

Verification:

1. Tabletop drill: one simulated incident and documented response.

Exit criteria:

1. Team can execute rollback and recovery without ad-hoc decisions.

### Phase 5 — Release packet

Deliverables:

1. Consolidated release readiness doc.
2. Final metrics packet from last 24-48h shadow run.

Go criteria:

1. Stable quality gates.
2. Stable rolling expectancy profile.
3. No unresolved P1 correctness issues.

---

## 4) Definition of Done

1. Architecture boundaries are enforceable and documented.
2. Hot path has controlled complexity and monitoring.
3. CI gates prevent quality regressions.
4. Pre-live ops packet is complete and actionable.

---

## 5) Rollback and safety

1. Feature flags/config switches retained for behavior rollback.
2. Every phase merged as small reversible commits.
3. No big-bang release of architectural and behavioral changes in one step.
