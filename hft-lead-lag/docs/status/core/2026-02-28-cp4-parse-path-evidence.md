# HFT-CP4 Evidence — Parse/Copy Hot Path

Date: 2026-02-28
Checkpoint: `HFT-CP4`
Scope: connector parse path + receive-drain dedupe copy reduction

## 1) Code-level deltas
1. `BinanceMarketData::parse_book_ticker_static` and `GateMarketData::parse_book_ticker_static` assign `strategy_symbol_id` during parse.
2. Connector receive-drain dedupe is now `strategy_symbol_id`-first:
   - known symbols: dedupe by `SymbolId` (no symbol-byte key clone/hash on dedupe key).
   - unknown symbols: fallback dedupe by symbol bytes.
3. Gate trade parser removed redundant re-interning on normalized symbol:
   - `Trade::new(symbol, ...)` uses already-normalized `Bytes`.
4. Fast numeric token parsing supports scientific notation (`e/E`) in shared extractors and nested Gate price parsing.

## 2) Hot-path guard checks
Commands:
```bash
rg -n "String::from_utf8_lossy|from_utf8_lossy" src/infrastructure/exchanges -S
rg -n "extract_json_(string|bool|i64)_field\\(" src/infrastructure/exchanges -S
rg -n "extract_json_(string|bool|i64)_field_ref_by_pattern|extract_json_(bool|i64)_field_by_pattern" \
  src/infrastructure/exchanges/binance/mod.rs src/infrastructure/exchanges/gate/mod.rs -S
```

Result summary:
1. No `from_utf8_lossy` usage in connector hot parse code.
2. Non-pattern `extract_json_*_field(...)` wrappers exist only in `common.rs` compatibility surface/tests.
3. Runtime connector parse callsites use `*_by_pattern` / `*_ref_by_pattern` APIs.

## 3) Test evidence
Commands:
```bash
cargo test -q drain_dedupe
cargo test -q parse_book_ticker_sets_preconfigured_strategy_symbol_id
cargo test -q test_extract_json_i64_supports_scientific_notation
cargo check --all-targets
cargo build
cargo test
```

Results:
1. `drain_dedupe*` tests pass (strategy-id dedupe + unknown-symbol fallback).
2. Parse tests pass for early `strategy_symbol_id` assignment.
3. Scientific notation extractor test passes.
4. Full verification passes:
   - `lib`: `231 passed, 0 failed, 2 ignored`
   - `main`: `88 passed, 0 failed`
   - doc-tests: `1 passed`

## 4) CP4 exit assessment
Current state: `In Progress`.

Remaining for CP4 close:
1. Capture profile evidence for dominant remaining parse/copy hotspots under live-like runtime load.
2. Remove/contain any remaining hotspot copy points confirmed by that profile.
