# R16 - Bugs and Errors Review

Date: 2026-02-28

## Findings

### P1
1. Raw-feed files can become unreadable due to sequence reordering between concurrent writer calls.
- Evidence: `src/infrastructure/replay/raw_feed.rs:70`, `src/infrastructure/replay/raw_feed.rs:77`, `src/infrastructure/replay/raw_feed.rs:120`.
- Why: sequence id is allocated before mutex-protected write path; reader requires strict in-file monotonic order.

2. CP6 kill-switch has no recovery path after trip.
- Evidence: `src/event_loop_execution.rs:214`, `src/event_loop_execution.rs:245`, `src/event_loop_execution.rs:247`.
- Why: once active, new intents are dropped until restart.

### P2
1. Recorder swallows write/flush errors but still advances sequence; one transient I/O failure can invalidate later replay.
- Evidence: `src/infrastructure/replay/raw_feed.rs:80`, `src/infrastructure/replay/raw_feed.rs:83`, `src/infrastructure/replay/raw_feed.rs:84`.

2. Recorder does blocking mutex+flush in WS read loops.
- Evidence: `src/infrastructure/replay/raw_feed.rs:77`, `src/infrastructure/exchanges/binance/mod.rs:334`, `src/infrastructure/exchanges/gate/mod.rs:437`.

### P3
1. No crash regressions found in this range on current suite (`cargo test` green), but CP5/CP6 error-path behavior remains under-tested.
