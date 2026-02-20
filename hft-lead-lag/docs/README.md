# HFT Lead-Lag Documentation (Current)

Актуальная документация по состоянию `main` и запущенного runtime.

**Snapshot:** 2026-02-19  
**Branch/commit:** `main @ 807178a`  
**Mode:** paper trading + shadow fleet optimizer

---

## 1) Что обновлено в этой итерации

1. Введён гиперпараметр `baseline_window_ms` в fleet/grid и БД.
2. `detect_gap()` теперь считает baseline только по окну `baseline_window_ms`, а не по всей 2-минутной истории.
3. Грид пересобран под диапазон тайминга **10s..60s**.
4. В API-выгрузки добавлен `baseline_window_ms`:
   - `/api/v1/fleet`
   - `/api/v1/fleet/ranked`
   - `/api/v1/fleet/symbols`

---

## 2) Карта документации

- `docs/README.md` (этот файл): текущий статус, математика, runbook.
- `docs/sprints/shadow-fleet-deep-dive.md`: полный deep dive по fleet/runtime/DB/API.
- `docs/sprints/strategy-runtime-modularity.md`: модульность runtime-стратегий и выбор через конфиг.
- `docs/review-2026-02-19-deep-dive.md`: инженерный статус проекта, риски, next actions.
- `docs/review-2026-02-19-multi-agent.md`: мультиагентное ревью (коммиты/архитектура/математика/дубли/god objects).
- `docs/review-2026-02-20-comprehensive-audit.md`: полный аудит (коммиты, баги, архитектура, математика, dead code, Screener/Shadow Fleet отдельно).
- `docs/manifest/MANIFESTO.md`: принципы и текущий фокус.
- `docs/review-shadow-trader.md`: архив (legacy, read-only).
- `docs/studies/*.md`: исследовательские идеи, не source of truth.

---

## 3) Runtime snapshot (живой сервер)

Метрики из текущего live paper запуска:

- `symbols_total`: **53**
- `symbols_with_trades` (single shadow): **11**
- `symbols_no_trades`: **42**
- `fleet ranked rows (>=10 trades/config)`: **100**
- `fleet/symbols` (best-by-symbol): **3 symbols**

Лучший конфиг по `/api/v1/fleet/ranked` на snapshot:

- `gap=30 bps`, `target=0.7`, `sl=40`, `hold=5s`, `spread=5`, `trailing=0.7`, `baseline=60s`
- `trades=35`, `win_rate=60%`, `avg_pnl=0.0199%`, `composite=0.592`

Распределение top-100 ranked по `baseline_window_ms`:

- `20s`: 72
- `60s`: 28

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
gap_threshold_bps (spike_threshold_bps): [30, 50, 60, 80]           (4)
target_ratio:                           [0.3, 0.5, 0.7]             (3)
stop_loss_bps:                          [8, 15, 25, 40]              (4)
max_hold_ms:                            [5000, 10000, 30000]         (3)
max_spread_bps:                         [3, 5]                        (2)
trailing_decay_ratio:                   [0.3, 0.7]                    (2)
baseline_window_ms:                     [10000, 20000, 30000, 60000]  (4)

TOTAL: 4 * 3 * 4 * 3 * 2 * 2 * 4 = 2304
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
4. Есть fail-open поведение в `DbWriter` при переполнении канала (batch drop с warn).

---

## 10) Source of truth

При конфликте docs vs code приоритет у кода:

1. `src/domain/screener/*`
2. `src/infrastructure/db.rs`
3. `src/api/*`
4. `src/main.rs`

---

*Last updated: 2026-02-20 (strategy runtime modularity + comprehensive audit docs added)*
