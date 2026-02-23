# HFT Lead-Lag Documentation (Current)

Актуальная документация по состоянию `main` (code-verified).

**Snapshot:** 2026-02-23  
**Branch/commit:** `main @ 5ee071c`  
**Mode:** paper trading + shadow fleet optimizer

---

## 1) Что обновлено в этой итерации

1. Runtime hot-reload grid активен через `config/runtime-grid.toml`:
   - startup apply + watch/apply loop в `src/main.rs` (`load_runtime_grid_generation`, `spawn_runtime_grid_hot_reload`);
   - атомарная замена fleet-конфигов через `ScreenerStore::replace_fleet_configs`.
2. Deal-hunt data contract доведён до прод-кода:
   - в `ClosedTrade` добавлены `gate_natr_30m_pct_at_entry`, `hold_ms`, `early_stop_churn` (`src/domain/screener/shadow_trader.rs`);
   - в SQLite `trades` добавлены те же поля + migration-safe `ALTER TABLE` (`src/infrastructure/db.rs`).
3. В рантайме работает батчевый Gate NATR refresher:
   - `spawn_gate_natr_refresher` + `refresh_gate_natr_batch` (`src/main.rs`);
   - запись snapshot в state через `ScreenerStore::set_gate_natr_30m`.
4. Health endpoint теперь учитывает staleness и drop counters:
   - stale feed detection (`binance_last_tick_age_ms`, `gate_last_tick_age_ms`);
   - drop counters (`binance_dropped_messages`, `gate_dropped_messages`, `db_dropped_batches`);
   - деградация до `503` при проблемах (`src/api/handlers.rs`).
5. Закрыты ключевые remediation-пункты:
   - decayed policy snapshot "to now" (`metrics_at` в `src/domain/screener/shadow_fleet.rs`);
   - корректный парсинг Binance `lastPrice` (`src/infrastructure/rest/mod.rs`);
   - retry-path в `DbWriter::send` при full-channel (`src/infrastructure/db.rs`).

---

## 2) Карта документации

- `docs/README.md` (этот файл): текущий статус, математика, runbook.
- `docs/manifest/MANIFESTO.md`: принципы и текущий фокус.
- `docs/sprints/sprint-008-deal-hunt-natr-db.md`: sprint-факт по deal-hunt/NATR data foundation.
- `docs/plans/2026-02-21-iterative-hyperparam-methodology.md`: активная методология итеративного поиска.
- `docs/plans/2026-02-23-ray-asha-forward-testing-context.md`: целевой контур Ray/ASHA (контекст, не код).
- `docs/studies/*.md`: исследовательские идеи, не source of truth.

---

## 3) Статус планов (что готово и что можно удалять)

Старые планы удалены из `docs/plans`:

1. `2026-02-20-master-remediation-design.md`
2. `2026-02-20-master-remediation-implementation.md`
3. `2026-02-20-full-remediation-implementation.md`
4. `2026-02-20-profit-extraction-brainstorm.md`
5. `2026-02-21-deal-hunt-hot-reload-design.md`
6. `2026-02-21-deal-hunt-hot-reload-implementation.md`

Активные планы:

- `docs/plans/2026-02-21-iterative-hyperparam-methodology.md`
- `docs/plans/2026-02-23-ray-asha-forward-testing-context.md`

История выполненной фазы:

- `docs/sprints/sprint-008-deal-hunt-natr-db.md`

---

## 4) Архитектура потока

1. `main.rs` собирает universe символов (пересечение Binance/Gate + volume filter + blacklist).
2. `ScreenerStore::update()` обновляет symbol state и `PriceSamples`.
3. На каждом тике работают:
   - single `shadow` trader
   - `ShadowFleet` (массив paper-конфигов на shared samples)
4. Закрытые fleet сделки дренятся в `DbWriter` и пишутся в `data/optimizer.db`.
5. HTTP/UI endpoints читают state и агрегаты из памяти + SQLite.

---

## 5) Математика (актуальная)

### 5.1 Entry: baseline-adjusted gap

Для LONG:

- `current_gap = (binance_ask - gate_ask) / gate_ask * 10_000`
- `baseline_gap = mean(current_gap over samples where ts >= now - baseline_window_ms)`
- `signal = current_gap - baseline_gap`
- вход если `signal >= spike_threshold_bps` (историческое имя поля)

Для SHORT:

- `current_gap = (gate_bid - binance_bid) / gate_bid * 10_000`
- `baseline_gap = mean(...)` по bid-gap
- вход если `signal >= spike_threshold_bps`

`min_baseline_samples` ограничивает ранний шум (default: 20).

### 5.2 Exit: breakeven + trailing

1. До breakeven: `stop_loss` и `timeout`.
2. Breakeven активируется при `unrealized_bps >= spike_bps * target_ratio`.
3. После breakeven:
   - exit на `unrealized <= 0` (возврат к цене входа),
   - или `unrealized <= peak_unrealized * trailing_decay_ratio`,
   - или timeout.

### 5.3 PnL

- `pnl_pct = (raw_return - 2*taker_fee) * 100`
- комиссия двусторонняя (вход+выход), bid/ask-aware.

---

## 6) Shadow Fleet grid (текущий)

```text
runtime-grid axes (`config/runtime-grid.toml`):
gap_threshold_bps:   30..80 step 10
target_ratio:        0.3..0.7 step 0.1
stop_loss_bps:       8..40 step 4
max_hold_ms:         5000..30000 step 5000
max_spread_bps:      3..5 step 1
trailing_decay_ratio:0.3..0.7 step 0.1
baseline_window_ms:  10000..60000 step 10000

raw combinations: 145800
runtime cap: max_configs = 1500 (downsample_configs)
```

Pruning:

- `session_trades >= 30 && avg_pnl_pct < -0.05` -> disable config
- `session_trades == 0` после `10m` -> disable as inactive

---

## 7) API surface

| Method | Path | Назначение |
|---|---|---|
| GET | `/health` | health/runtime status |
| GET | `/api/v1/symbols` | universe symbols |
| GET | `/api/v1/screener` | screener rows + shadow stats |
| GET | `/api/v1/shadow/:symbol` | per-symbol shadow debug |
| GET | `/api/v1/chart/:symbol` | chart + trades |
| GET | `/api/v1/fleet` | expectancy ranking |
| GET | `/api/v1/fleet/ranked` | composite ranking (sharpe/pf/composite) |
| GET | `/api/v1/fleet/symbols` | best config per symbol |
| GET | `/screener` | HTML page |
| GET | `/fleet` | HTML fleet page |
| WS | `ws://host:8181/ws` | live ticks |

---

## 8) Runbook

```bash
# build/test
cargo build
cargo test

# release run
cargo build --release
./target/release/hft-lead-lag

# quick checks
curl -s http://localhost:5000/health
curl -s http://localhost:5000/api/v1/fleet | head
curl -s http://localhost:5000/api/v1/fleet/ranked | head
curl -s http://localhost:5000/api/v1/fleet/symbols | head
```

---

## 9) Ограничения текущей версии

1. Это paper execution (real order routing ещё не интегрирован).
2. Universe coverage пока узкий: многие символы не проходят пороги сигналов.
3. Policy allocator между конфигами отсутствует (есть ranking, но нет auto-capital routing).
4. Ray Tune + ASHA контур пока только в плане (`docs/plans/2026-02-23-ray-asha-forward-testing-context.md`), в `src/` интеграции нет.
5. `dislocation_reversion` объявлен в config enum, но runtime-build пока поддерживает только `lead_lag_classic`.

---

## 10) Source of truth

При конфликте docs vs code приоритет у кода:

1. `src/domain/screener/*`
2. `src/infrastructure/db.rs`
3. `src/api/*`
4. `src/main.rs`

---

*Last updated: 2026-02-23 (old plans removed from docs/plans, docs index synced)*
