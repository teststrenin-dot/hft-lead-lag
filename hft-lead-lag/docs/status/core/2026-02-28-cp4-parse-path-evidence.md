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
cargo test -q test_parse_book_ticker_prefers_contract_over_s_when_both_present
cargo test -q test_extract_json_i64_supports_scientific_notation
cargo check --all-targets
cargo build
cargo test
```

Results:
1. `drain_dedupe*` tests pass (strategy-id dedupe + unknown-symbol fallback).
2. Parse tests pass for early `strategy_symbol_id` assignment.
3. Scientific notation extractor test passes.
4. Contract-vs-symbol precedence regression test passes (`contract` takes priority when both keys are present).
5. Synthetic profiling harness passes for parser hot path:
   - debug: `common=5690ns`, `binance=9510ns`, `gate=11328ns` (per iteration).
   - release: `common=440ns`, `binance=617ns`, `gate=675ns` (per iteration).
6. Full verification passes:
   - `lib`: `232 passed, 0 failed, 5 ignored`
   - `main`: `88 passed, 0 failed`
   - doc-tests: `1 passed`

## 4) Parse-order optimization proof
1. New regression guard: Gate parser now prefers `contract` over `s` when both are present.
2. Red/green cycle evidence:
   - forced old order (`s` first) causes test failure on mismatched payload.
   - restored optimized order (`contract` first) makes the same test pass.

## 5) CP4 exit assessment
Current state: `Completed`.

Close rationale:
1. Parse path now runs on static pattern extraction with direct symbol-id assignment.
2. Receive-drain dedupe is `SymbolId`-first for known symbols, reducing key-copy pressure.
3. Profile harness baseline is captured in debug/release and can be reused for CP5+ regression checks.
4. Remaining non-pattern wrappers live in compatibility surface and are not used by connector hot path.
