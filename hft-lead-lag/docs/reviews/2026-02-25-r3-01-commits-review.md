# Commits Review (R3)

## Reviewed Commits
- `3697b56` — `fix: harden trial batch/ack pipeline and saturation handling`

## Findings
- **P1** `src/infrastructure/db.rs:368-441`  
  The saturation fix removed drops but replaced them with unbounded deferred senders (`schedule_deferred_overflow_send` per saturated enqueue). Under sustained pressure this can shift failure mode from controlled data loss to process resource exhaustion.
- **P2** `src/main.rs:542-575`  
  Queue ordering by embedded timestamp introduces starvation for non-conforming/manual queue files (`None` timestamps always sorted after timestamped files).

## Regression Status
- Functional regression tests currently green.
- Resource-behavior regression risk exists (P1) and should be fixed before larger load cycles.

## Suggested Next Commit Theme
- Bound deferred backlog (single bounded relay queue / explicit drop policy / backpressure policy).
- Adjust queue ordering to avoid starvation of legacy/manual items.
