# HFT-CP5 Evidence — Deterministic Replay Harness

Date: 2026-02-28
Checkpoint: `HFT-CP5`
Status: `Completed`
Last remediation sync: 2026-02-28 (`fix(cp5-cp6): harden replay recorder and execution queue semantics`)

## Scope delivered
1. Added `src/infrastructure/replay/raw_feed.rs`.
2. Implemented raw feed recorder in JSONL format:
   - fields: `seq`, `exchange`, `recv_ts_ns`, `payload_b64`.
3. Implemented strict replay reader:
   - validates monotonic sequence order.
   - validates base64 payload decoding.
4. Implemented runtime capture wiring:
   - Binance/Gate WS workers append raw frames to recorder when `RAW_FEED_RECORD_PATH` is set.
   - recording is opt-in and off by default.
5. Implemented replay mode + equivalence checks:
   - replay runner computes deterministic signal trace from recorded frames.
   - `REPLAY_RAW_FEED_PATH` launches offline replay determinism check from `main`.
6. Added profile harness:
   - ignored replay benchmark test for ns/frame tracking.
7. Added contract tests:
   - deterministic round-trip preserves order and payload bytes.
   - invalid payload encoding is rejected as `InvalidData`.
   - deterministic replay yields stable signal trace for same input.
8. Post-review hardening:
   - recorder state switched to single mutex-guarded `{next_seq, writer}` critical section.
   - `seq` now advances only after successful `to_writer + newline + flush` (no "phantom seq" on failed write).
   - `record(...)` now returns `io::Result<()>` so connector wiring can surface write failures.
   - replay reader now rejects invalid JSON lines and out-of-order sequence with contextual errors.
   - concurrent recorder test validates monotonic `seq` under multithreaded writes.

## TDD evidence
1. `RED`:
```bash
cargo test -q raw_feed::tests:: --manifest-path /root/turbo/hft-lead-lag/Cargo.toml
```
Result: `2 failed` on unimplemented reader path.

2. `GREEN`:
```bash
cargo test -q raw_feed::tests:: --manifest-path /root/turbo/hft-lead-lag/Cargo.toml
```
Result: `2 passed, 0 failed`.

3. Replay determinism tests:
```bash
cargo test -q replay_signal_trace_is_deterministic_for_same_input --manifest-path /root/turbo/hft-lead-lag/Cargo.toml
```
Result: `1 passed, 0 failed`.

4. Connector replay parse guards:
```bash
cargo test -q parse_book_ticker_for_replay_ignores_non_book_ticker_payload --manifest-path /root/turbo/hft-lead-lag/Cargo.toml
```
Result: `2 passed, 0 failed` (Binance + Gate).

5. Replay reader hardening tests:
```bash
cargo test -q replay_reader_rejects_invalid_json_line --manifest-path /root/turbo/hft-lead-lag/Cargo.toml
cargo test -q replay_reader_rejects_out_of_order_sequence --manifest-path /root/turbo/hft-lead-lag/Cargo.toml
cargo test -q concurrent_recording_keeps_monotonic_sequence --manifest-path /root/turbo/hft-lead-lag/Cargo.toml
```
Result: passed.

6. Replay benchmark harness (profiling only):
```bash
cargo test --release -q bench_replay_signal_trace_profile \
  --manifest-path /root/turbo/hft-lead-lag/Cargo.toml -- --ignored --nocapture --test-threads=1
```

## Full verification
Commands:
```bash
cargo check --all-targets --manifest-path /root/turbo/hft-lead-lag/Cargo.toml
cargo build --manifest-path /root/turbo/hft-lead-lag/Cargo.toml
cargo test --manifest-path /root/turbo/hft-lead-lag/Cargo.toml
```

Results:
1. `cargo check`: success.
2. `cargo build`: success.
3. `cargo test`: success (fresh counts from this stage recorded in commit output).

## Runtime usage
1. Raw recording:
```bash
export RAW_FEED_RECORD_PATH=data/replay/raw-feed.jsonl
cargo run
```
2. Offline deterministic replay check:
```bash
export REPLAY_RAW_FEED_PATH=data/replay/raw-feed.jsonl
export REPLAY_STRATEGY_SYMBOLS=BTCUSDT,ETHUSDT
export REPLAY_PRIMARY_EXCHANGE=binance
cargo run
```

## CP5 exit assessment
1. Recorder exists and is wired into live ingest path (opt-in).
2. Recorder semantics are hardened: sequence monotonicity and write/flush success are coupled.
3. Replay mode exists and verifies deterministic signal trace equivalence.
4. Reader hard-fails malformed JSON and sequence-order violations with contextual errors.
5. Benchmark harness exists for replay regression monitoring.
6. CP5 is closed and hardened; CP6 execution fast path is delivered.
