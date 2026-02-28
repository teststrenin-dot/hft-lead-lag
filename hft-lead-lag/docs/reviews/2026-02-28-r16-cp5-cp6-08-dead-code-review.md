# R16 - Dead Code Review

Date: 2026-02-28

## Findings

### P3
1. Potentially unused helper surface in replay module.
- Evidence: `src/infrastructure/replay/raw_feed.rs:273` (`replay_signal_trace_from_file`), runtime path uses `verify_signal_replay_determinism_from_file` from `src/main.rs:235`.
- Recommendation: remove or route through this helper to avoid parallel APIs.

2. No additional dead-code hotspots were found in CP5/CP6 runtime path.
