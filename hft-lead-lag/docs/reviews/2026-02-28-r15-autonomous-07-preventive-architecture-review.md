# R15 Autonomous - Preventive Architecture Review

Date: 2026-02-28

## Findings

### P2
1. Missing preventive guardrails for symbol cache growth (cap/eviction/validation).
- Evidence: `src/domain/symbols.rs:16`, `src/domain/symbols.rs:49`.
- Preventive action: reject/normalize invalid symbol payloads before caching and enforce bounded cache size.

2. Missing starvation-focused stress test for scheduler under sustained skewed update distribution.
- Evidence: `src/main_tests.rs:1734` (coverage for count-scaling exists, starvation soak scenario missing).
- Preventive action: add long-running fairness stress case with adversarial low-id churn.

### P3
1. CP2 evidence should include fixed benchmark protocol (`before/after`, same symbol set, same load window) as checkpoint policy.
- Evidence: `docs/status/core/2026-02-28-cp2-lock-free-p99-evidence.md:21`.
