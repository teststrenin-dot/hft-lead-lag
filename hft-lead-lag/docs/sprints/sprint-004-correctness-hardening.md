# Sprint 004 — Correctness Hardening Foundation

**Window:** 3 рабочих дня  
**Primary objective:** сделать pipeline данных надежным перед любым profit-tuning.

---

## 1) Scope

In-scope:

1. Parser correctness для Binance/Gate trade/book payloads.
2. Единая time-domain policy (exchange/local/ingress semantics).
3. Контрактная согласованность config -> strategy -> db.
4. Разделение unit/integration тестов для REST.
5. Green quality gates (`test`, `clippy -D warnings`).

Out-of-scope:

1. Новая торговая стратегия.
2. Политика аллокации капитала.
3. UI redesign.

---

## 2) Key risks addressed

1. Некорректная side/price/qty интерпретация сделок.
2. Искажения lag/hold/catchup из-за смешанных часов.
3. Неполная идентичность конфигов в БД/ранкинге.
4. Ложные падения CI из-за сетезависимых тестов.

---

## 3) Phases

### Phase 0 — Baseline and fixtures

Deliverables:

1. Зафиксированный baseline diagnostics документ в PR body/notes.
2. Набор raw payload fixtures (Binance/Gate) для trade/book.

Touched paths:

1. `tests/fixtures/exchanges/` (new)
2. `src/infrastructure/exchanges/*`

Verification:

1. `cargo test -q`

Exit criteria:

1. Fixtures покрывают минимум 2 валидных и 1 edge-case payload на тип сообщения.

### Phase 1 — Parser correctness fixes

Deliverables:

1. Исправлен parsing `is_buyer_maker` и nested numeric extraction.
2. Добавлены parser contract tests на fixtures.

Touched paths:

1. `src/infrastructure/exchanges/binance/mod.rs`
2. `src/infrastructure/exchanges/gate/mod.rs`
3. `src/infrastructure/exchanges/common.rs`
4. `src/infrastructure/exchanges/*tests*`

Verification:

1. `cargo test -- --nocapture`
2. Targeted parser tests pass with fixture assertions.

Exit criteria:

1. Парсер возвращает ожидаемые значения side/price/qty/timestamp по fixtures.

### Phase 2 — Unified clock policy

Deliverables:

1. Явная модель времени: `exchange_event_ts`, `local_ingress_ts`, `decision_ts`.
2. Refactor lag/hold calculations under one policy.
3. Regression tests на time normalization и lag windows.

Touched paths:

1. `src/domain/screener/utils.rs`
2. `src/domain/screener/state.rs`
3. `src/domain/screener/mod.rs`
4. `src/domain/screener/shadow_trader.rs`

Verification:

1. `cargo test` time-policy suite green.
2. Manual check: no negative or implausible lag spikes in sample run logs.

Exit criteria:

1. Все временные вычисления используют документированный time authority.

### Phase 3 — Config/DB contract parity

Deliverables:

1. `min_baseline_samples` включен в config identity (`config_id`) и persistence schema.
2. Safe DB migration for existing `optimizer.db`.
3. Документирована backward-compatibility policy.

Touched paths:

1. `src/domain/screener/trader_config.rs`
2. `src/infrastructure/db.rs`
3. `src/api/handlers.rs` (if rank payload impacted)

Verification:

1. Migration test on existing DB copy.
2. `cargo test` + rank endpoints sanity check.

Exit criteria:

1. Ranking and trades map to fully unique config semantics.

### Phase 4 — Quality gates and test split

Deliverables:

1. REST live tests moved to integration suite with opt-in flag.
2. Unit tests run offline deterministically.
3. `clippy -D warnings` issues resolved.

Touched paths:

1. `src/infrastructure/rest/mod.rs`
2. `tests/integration/rest_live.rs` (new)
3. CI-related docs/scripts (if present)

Verification:

1. `cargo test`
2. `cargo clippy --all-targets -- -D warnings`

Exit criteria:

1. Оба набора команд зеленые без внешней сети.

### Phase 5 — Sprint closeout

Deliverables:

1. Evidence report: before/after parser + time + quality metrics.
2. Updated docs map.

Verification:

1. `cargo check --all-targets`
2. `cargo build`
3. `cargo test`
4. `cargo clippy --all-targets -- -D warnings`

Exit criteria:

1. Sprint artifacts committed and linked from docs.

---

## 4) Definition of Done

1. Data pipeline correctness proven by fixture contracts.
2. Clock semantics unified and test-covered.
3. Config identity and DB schema parity restored.
4. Offline deterministic test path established.
5. Clippy gate fully green.

---

## 5) Rollback and safety

1. Parser fixes shipped behind small commits per exchange.
2. DB migration is additive + reversible backup naming.
3. Any regression in payload parsing triggers immediate rollback to previous commit tag.
