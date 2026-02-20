# Master Remediation Program Design

**Date:** 2026-02-20
**Status:** Approved for implementation

## Goal

Deliver a structured full-system remediation program that addresses runtime reliability, correctness/math integrity, architecture quality, and maintainability for both Screener and Shadow Fleet tracks.

## Scope

- Runtime reliability and throughput under production-like load.
- Logic/math correctness in signaling, policy windows, and persistence.
- Architecture redesign to reduce coupling and cognitive load.
- Separate design tracks for Screener and Shadow Fleet.
- Dead code and duplication cleanup with prevention guardrails.

## Operating Principles

- Reliability-first: all high-risk runtime failures are handled before deeper refactors.
- TDD-first for every behavior change and bugfix.
- Small, verifiable tasks with explicit rollback points.
- Incremental commits by concern (no mixed mega-commits).
- Evidence-based completion: no success claims without fresh test/lint/runtime proof.

## Execution Model

- Controlled batches (recommended): parallel where independent, serialized where shared-state risk exists.
- Subagent-style decomposition by concern:
  - Reliability stream
  - Correctness/math stream
  - Architecture stream
  - Screener design stream
  - Shadow Fleet design stream
- Integration gate after each wave:
  - focused tests
  - full test suite
  - clippy strict
  - runtime smoke/telemetry checks

## Program Waves

### Wave A: Runtime Stability (P0)

Objectives:
- Remove hot-path amplification causing queue overflow.
- Stabilize ingestion and event-loop pacing.
- Ensure health endpoint reflects actual degradation causes.

Exit criteria:
- Message drops sharply reduced in controlled load tests.
- No pathological backlog growth under expected symbol universe.
- Health contains actionable stale/disconnect/drop signals.

### Wave B: Correctness & Math (P0/P1)

Objectives:
- Lock down invariants for policy decay, score gating, and trade accounting.
- Ensure DB schema/runtime/API contracts stay aligned.
- Add regression tests for edge-case arithmetic and timestamp handling.

Exit criteria:
- Deterministic tests for all identified math edge cases.
- No schema drift startup failures on migrated DBs.

### Wave C: Architecture Split (P1)

Objectives:
- Decompose oversized modules (`main.rs`, heavy handler/runtime coupling).
- Introduce explicit boundaries: ingestion, orchestration, strategy feed, persistence pipeline.
- Reduce god-object behavior and local complexity.

Exit criteria:
- Smaller modules with clear ownership.
- Equivalent runtime behavior validated by regression suite.

### Wave D: Screener Design Track (separate)

Objectives:
- Validate Screener state model, update flow, and observability contract.
- Reduce hidden coupling between symbol state, shadow model, and API projections.

Exit criteria:
- Explicit Screener design doc + implemented guardrails.

### Wave E: Shadow Fleet Design Track (separate)

Objectives:
- Validate fleet policy model, scoring windows, and per-config lifecycle.
- Prepare path for pluggable strategy kinds without destabilizing engine core.

Exit criteria:
- Explicit Shadow Fleet design doc + staged implementation hooks.

### Wave F: Cleanup & Preventive Architecture (P1/P2)

Objectives:
- Remove dead code and duplicate logic.
- Add preventive gates (lint/test/runtime checks, docs consistency checks).

Exit criteria:
- Reduced duplicate paths and stale artifacts.
- CI-equivalent local verification script green.

### Wave G: Release Readiness

Objectives:
- End-to-end verification, risk register, operational checklist.

Exit criteria:
- Full verification green.
- Final remediation summary with residual known risks.

## Risk Management

Top risks:
- Shared file conflicts during parallel work on runtime/orchestration.
- Hidden behavior regressions from event-loop/performance fixes.
- Over-refactor before stabilizing observed production failure modes.

Mitigations:
- Controlled-batch execution with strict phase gates.
- Dedicated regression tests before and after structural changes.
- Keep behavior-preserving refactors separate from logic changes.

## Success Metrics

- Functional: all tests pass, clippy strict pass.
- Runtime: reduced dropped-message growth and stable status metrics.
- Quality: reduced LOC concentration in hotspot modules, reduced cyclomatic hotspots.
- Maintainability: clear module boundaries and reduced duplicated logic.

## Deliverables

- Master implementation plan with executable tasks.
- Code changes by wave.
- Updated docs for architecture/runbook/reliability.
- Verification evidence and final remediation report.
