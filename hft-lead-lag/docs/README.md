# HFT Lead-Lag Documentation (Current)

Актуальная документация по состоянию `main` (code-verified).

**Snapshot:** 2026-02-24  
**Branch/commit:** `main @ HEAD`  
**Mode:** paper trading + shadow fleet optimizer + Ray/ASHA orchestration

---

## 1) Как это работает (без тумана, по шагам)

1. Rust runtime (`src/main.rs`) поднимает market data, shadow trader и `ShadowFleet`.
2. Fleet торгует в paper-режиме; закрытые сделки пишутся в `data/optimizer.db` через async `DbWriter`.
3. В фоне работает watcher `spawn_runtime_grid_hot_reload`:
   - следит за `config/runtime-grid.toml` (обычный grid hot-reload),
   - и за `config/trial-batch.json` (внешние батчи от Ray driver).
4. Когда Python driver пишет `config/trial-batch.json`, Rust:
   - парсит `{run_id, configs[]}` + patch-mode (`full_replace`/`incremental`),
   - валидирует incremental-контракт (`changed_config_ids`, optional `symbols`),
   - upsert-ит `configs` в SQLite,
   - применяет patch в fleet: полный reset или symbol-scoped reset,
   - flush-ит pending trades,
   - пишет `config/.trial-ack` с `run_id`, `config_count`, `drained_trades`.
5. После применения batch все новые fleet-сделки получают этот `run_id` и пишутся в `trades.run_id`.
6. Python driver читает агрегаты из `optimizer.db` и на их основе делает `scout/expand/forward/promote`.
7. Результаты смотришь в:
   - UI: `/trials`,
   - API: `/api/v1/trials`, `/api/v1/trials/:run_id`, `/api/v1/trials/axes`.

Итог: Ray driver не торгует сам. Он оркестрирует конфиги, а исполняет и считает сделки Rust runtime.

Роли в двух строках:

- `fleet` (Rust) = execution/measurement engine.
- `ray` (Python + ASHA) = optimizer/orchestrator.

---

## 2) Что добавлено по Ray + ASHA

1. Реальная интеграция driver-а: `ray_driver/*` (`scout`, `expand`, `forward`, `promote`).
2. File IPC контракт Rust <-> Python:
   - input: `config/trial-batch.json`,
   - ack: `config/.trial-ack`.
3. Трейды помечаются `run_id` и доступны для аналитики run-by-run:
   - `trades.run_id`,
   - API `/api/v1/trials*`,
   - HTML `/trials`.
4. Добавлен deep-dive документ по этому контуру:
   - `docs/ray-asha-deep-dive.md`.

### 2.1 Trial-Batch Contract Modes

`config/trial-batch.json` поддерживает два режима:

1. `full_replace`:
   - default, если `mode` отсутствует;
   - runtime сбрасывает fleet для всех символов.
2. `incremental`:
   - обязателен `changed_config_ids: [u64, ...]`;
   - optional `symbols: [\"BTCUSDT\", ...]` ограничивает reset только этой областью.

Safety behavior:

- некорректный `incremental` payload не применяется частично;
- runtime пишет `warn` и пропускает batch целиком.

---

## 3) Карта документации

- `docs/README.md` (этот файл): статус, архитектура, runbook.
- `docs/ray-asha-deep-dive.md`: полный разбор Ray/ASHA контура (IPC, runtime, API, ограничения).
- `docs/manifest/MANIFESTO.md`: принципы и текущий фокус.
- `docs/plans/2026-02-23-ray-asha-forward-testing-context.md`: ранний контекст/требования (исторический).
- `docs/plans/2026-02-23-ray-asha-fleet-integration.md`: детальный implementation plan (исторический).
- `docs/studies/*.md`: исследовательские заметки, не source of truth.

---

## 4) Runtime архитектура (текущая)

1. `main.rs` формирует universe символов и запускает event loop.
2. `ScreenerStore::update()` обновляет state и `PriceSamples`.
3. На каждом тике работают:
   - одиночный `shadow` trader,
   - `ShadowFleet` (массив paper-конфигов на shared samples).
4. Fleet trades пишутся в SQLite (`data/optimizer.db`) через `DbWriter` (WAL).
5. HTTP/UI слой (`src/api/*`) отдаёт screener/fleet/trials данные.

---

## 5) Ray driver lifecycle

```text
scout -> expand -> forward(ASHA) -> promote
```

1. `scout`
   - генерирует coarse configs из `ray_driver/bounds.py`,
   - отправляет batch в runtime,
   - ждёт `duration`,
   - сохраняет живые references в `data/scout-references.json`.
2. `expand`
   - берёт references,
   - строит соседние конфиги вокруг каждого reference (bounded expand),
   - запускает batch и возвращает alive subset.
3. `forward`
   - запускает Ray Tune + `ASHAScheduler` поверх `FleetTrial`,
   - `FleetTrial` периодически репортит метрики из SQLite:
     - `time_budget_s`, `total_trades`, `configs_with_trades`, `avg_pnl_pct`, `avg_win_rate_pct`.
4. `promote`
   - выбирает top configs из конкретного `run_id`,
   - экспортирует в `data/promoted-<run_id>.json`,
   - runtime-grid не переписывает автоматически (manual review expected).

---

## 6) API surface

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
| GET | `/api/v1/trials` | summary по `run_id` |
| GET | `/api/v1/trials/:run_id` | per-config метрики в run |
| GET | `/api/v1/trials/axes?run_id=...` | агрегаты по 7 осям |
| GET | `/screener` | HTML screener page |
| GET | `/fleet` | HTML fleet page |
| GET | `/trials` | HTML trials page |
| WS | `ws://host:8181/ws` | live ticks |

---

## 7) Runbook

```bash
# 0) build/test rust
cd /root/turbo/hft-lead-lag
cargo build
cargo test

# 1) run runtime (terminal A)
cargo build --release
./target/release/hft-lead-lag
```

```bash
# 2) python env + deps (terminal B)
cd /root/turbo/hft-lead-lag
python3 -m pip install -r ray_driver/requirements.txt

# 3) ray pipeline
python3 -m ray_driver scout --duration 600
python3 -m ray_driver expand --duration 600 --cycles 1
python3 -m ray_driver forward --max-budget 240 --grace-period 60 --report-interval 30

# 4) inspect runs
curl -s http://localhost:5000/api/v1/trials
curl -s http://localhost:5000/api/v1/trials/axes

# 5) promote from selected run
python3 -m ray_driver promote <run_id> --top-k 50 --min-trades 5 --min-pnl 0.0
```

Ключевые артефакты:

- `config/trial-batch.json` — batch на применение в runtime (`full_replace` или `incremental`).
- `config/.trial-ack` — подтверждение применения.
- `data/scout-references.json` — seed для expand/forward.
- `data/promoted-<run_id>.json` — кандидаты на ручной promotion.

Rollback к safe-path:

1. Убери `mode` из payload (или явно поставь `mode: "full_replace"`).
2. Отправь batch повторно.
3. Проверь статус runner-а: `GET /api/v1/trials/runner/status?tail=50`.
4. Убедись, что `.trial-ack` обновился с нужным `run_id`.

---

## 8) Первичные прогоны: `scout` для поиска reference-конфигов

Цель `scout`: не найти "финального победителя", а быстро найти живые зоны параметров, где есть сделки и приемлемый сигнал.

### 8.1 Preflight

```bash
cd /root/turbo/hft-lead-lag

# runtime health должен быть OK
curl -s http://localhost:5000/health
```

```bash
cd /root/turbo/hft-lead-lag

# deps для ray driver
python3 -m pip install -r ray_driver/requirements.txt
```

### 8.2 Первый scout-run

```bash
cd /root/turbo/hft-lead-lag
python3 -m ray_driver scout --duration 900
```

Что произойдет:

1. Driver сгенерирует coarse сетку (до `MAX_SCOUT_CONFIGS=3000`).
2. Отправит batch в runtime через `config/trial-batch.json`.
3. Дождется `.trial-ack`.
4. Подождет `duration`.
5. Сохранит references в `data/scout-references.json`.

### 8.3 Проверка результата scout

```bash
cd /root/turbo/hft-lead-lag
python3 - <<'PY'
import json
from pathlib import Path
p = Path("data/scout-references.json")
rows = json.loads(p.read_text()) if p.exists() else []
print("references:", len(rows))
rows = sorted(rows, key=lambda r: (r["trades"], r["avg_pnl_pct"]), reverse=True)
print("top10:")
for r in rows[:10]:
    print(r)
PY
```

Ориентиры для первичного цикла:

1. `references < 20`: обычно мало сигнала. Увеличивай `--duration` (например, 1800-3600) и запускай scout повторно.
2. `references` очень много, но у большинства `trades=1..2`: сигнал шумный. Перед `expand` отфильтруй `scout-references.json`.
3. Есть адекватный пул референсов: переходи в `expand`.

Важно: в текущей реализации `scout` сохраняет все конфиги с `trades >= 1` (CLI-параметра `min_trades` пока нет).

### 8.4 Переход к следующей фазе

```bash
cd /root/turbo/hft-lead-lag
python3 -m ray_driver expand --duration 900 --cycles 1
python3 -m ray_driver forward --max-budget 240 --grace-period 60 --report-interval 30
```

---

## 9) Ограничения текущей версии

1. Это paper execution (real order routing ещё не интегрирован).
2. `forward` сейчас запускается как `num_samples=1` (один Ray trial на batch конфигов), поэтому ASHA не делает полноценный multi-trial отбор.
3. `promote` сохраняет JSON и не применяет автоматически в `runtime-grid.toml`.
4. File IPC single-path (`config/trial-batch.json`) не рассчитан на несколько параллельных driver-процессов.
5. `dislocation_reversion` объявлен в config enum, но runtime-build пока поддерживает только `lead_lag_classic`.

---

## 10) Source of truth

При конфликте docs vs code приоритет у кода:

1. `ray_driver/*`
2. `src/main.rs`
3. `src/domain/screener/*`
4. `src/infrastructure/db.rs`
5. `src/api/*`

---

*Last updated: 2026-02-24 (incremental fleet patch contract documented)*
