# Preventive Architecture Review (R3)

## Observed Gaps
- No explicit upper bound/backpressure contract for deferred DB enqueue path (`src/infrastructure/db.rs:368-441`).
- No fairness contract for mixed queue filename formats (`src/main.rs:542-575`).
- No eviction/TTL policy for screener symbol store growth (`src/domain/screener/mod.rs:385,444`).
- Policy diagnostics generated in shadow fleet but not surfaced/consumed operationally (`src/domain/screener/shadow_fleet.rs:503-523`, `src/api/handlers.rs:204-215`).

## Preventive Controls
1. Define bounded-queue invariants and enforce with metrics/alerts.
2. Define queue item ordering/fairness spec and add regression tests.
3. Add symbol lifecycle policy (TTL or max cardinality) plus histograms for row build latency.
4. Expose shadow policy snapshots via API/telemetry and wire them into runtime decisions or remove.
