# HFT-CP5 Evidence — Block 1 (Raw Feed Recorder + Replay Reader)

Date: 2026-02-28
Checkpoint: `HFT-CP5`
Block: `1/3`

## Scope delivered
1. Added `src/infrastructure/replay/raw_feed.rs`.
2. Implemented raw feed recorder in JSONL format:
   - fields: `seq`, `exchange`, `recv_ts_ns`, `payload_b64`.
3. Implemented strict replay reader:
   - validates monotonic sequence order.
   - validates base64 payload decoding.
4. Added contract tests:
   - deterministic round-trip preserves order and payload bytes.
   - invalid payload encoding is rejected as `InvalidData`.

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
3. `cargo test`: success (`lib: 234 passed, 0 failed, 5 ignored`; `main: 88 passed, 0 failed`; docs: `1 passed`).

## Remaining for CP5
1. Wire runtime connector ingest (`recv_ts + raw frame`) into recorder.
2. Add replay execution path for deterministic decision/trade equivalence.
3. Add replay performance regression benchmark.

