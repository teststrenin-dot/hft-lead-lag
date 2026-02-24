# Ray + ASHA Deep Dive (Current)

Технический разбор фактической реализации Ray orchestration для Shadow Fleet.

**Snapshot:** 2026-02-24  
**Branch/commit:** `main @ HEAD`

---

## 1) Что это решает

Ray driver управляет поиском гиперпараметров, а Rust runtime исполняет paper-trading на live-потоке.

Разделение ответственности:

- `ray_driver/*`:
  - генерирует и отбирает конфиги (`scout/expand/forward/promote`);
  - пишет trial batch в runtime;
  - читает метрики из SQLite.
- `src/main.rs` + `ScreenerStore`:
  - принимает batch конфигов,
  - применяет в running fleet без перезапуска,
  - пишет трейды и агрегации в `data/optimizer.db`.

---

## 2) End-to-End поток

```text
python ray_driver  --(trial-batch.json)-->  rust runtime
rust runtime       --(.trial-ack)------->   python ray_driver
rust runtime       --(trades+configs)-->    optimizer.db
api/ui             <--(read-only)--------   optimizer.db
```

Последовательность:

1. Driver формирует `run_id` и набор конфигов.
2. `FleetIPC.submit_batch()` пишет `config/trial-batch.json` и ждёт `.trial-ack`.
3. Rust watcher замечает изменение файла, валидирует batch и применяет его.
4. Rust пишет `.trial-ack` с итогом применения.
5. Fleet торгует с новым набором конфигов; трейды получают `run_id`.
6. Driver читает агрегаты из SQLite для принятия решений.
7. API `/api/v1/trials*` и `/trials` показывают run-level статистику.

---

## 3) IPC контракт

### 3.1 Input: `config/trial-batch.json` (Python -> Rust)

```json
{
  "run_id": "expand-1700000000",
  "mode": "incremental",
  "changed_config_ids": [1234567890],
  "symbols": ["BTCUSDT"],
  "configs": [
    {
      "spike_threshold_bps": 50.0,
      "target_ratio": 0.5,
      "stop_loss_bps": 15.0,
      "max_hold_ms": 10000,
      "max_spread_bps": 3.0,
      "trailing_decay_ratio": 0.5,
      "baseline_window_ms": 20000,
      "fill_delay_ms": 6,
      "cooldown_ms": 3000,
      "warmup_ms": 30000,
      "quote_freshness_ms": 1000,
      "taker_fee": 0.0005,
      "min_baseline_samples": 20
    }
  ]
}
```

Валидация на Rust стороне:

- `run_id` не пустой;
- `configs` не пустой;
- JSON парсится в `TraderConfig`;
- `mode`:
  - `full_replace` (default, если поля нет),
  - `incremental` (строгая валидация);
- для `incremental` обязателен непустой `changed_config_ids`;
- `symbols` в `incremental` optional, но если поле передано, после нормализации список не может быть пустым;
- invalid incremental payload не применяется частично (batch skip + `warn`).

### 3.2 Ack: `config/.trial-ack` (Rust -> Python)

```json
{
  "run_id": "expand-1700000000",
  "applied_at_ms": 1700000000123,
  "config_count": 512,
  "drained_trades": 43
}
```

`drained_trades` показывает, сколько pending сделок было сброшено при переключении fleet-конфигов.

---

## 4) Rust runtime интеграция

Точка входа:

- `spawn_runtime_grid_hot_reload(..., trial_batch_path, ...)` в `src/main.rs`.

Ключевая логика:

1. На каждом цикле watcher проверяет `mtime` у `config/trial-batch.json`.
2. Если batch изменился:
   - `load_trial_batch()`,
   - `build_trial_batch_patch_plan()` (strict validation),
   - `upsert_runtime_configs()` в SQLite `configs`,
   - `screener.set_run_id(Some(run_id))`,
   - `screener.apply_fleet_patch(batch.configs, plan)`,
   - `screener.flush_db_writer().await`,
   - `write_trial_ack(...)`.
3. Ошибки парсинга/DB не падают процессом, а логируются (`warn!`).

Приоритет:

- Trial batch применяется сразу (без debounce).
- Runtime-grid (`config/runtime-grid.toml`) остаётся активным отдельным механизмом hot-reload.

Rollback / fallback path:

1. Перейти на `full_replace` (удалить `mode` или поставить `mode: "full_replace"`).
2. Переотправить batch.
3. Проверить `/api/v1/trials/runner/status?tail=50` и `config/.trial-ack`.

---

## 5) Ray driver фазы

### 5.1 `scout`

- `generate_scout_configs()` строит coarse набор по `AXES` (`ray_driver/bounds.py`).
- Лимит: `MAX_SCOUT_CONFIGS = 3000`.
- После прогона обновляет `data/scout-references.json` кумулятивно:
  - новые `scout`-метрики мерджатся с уже сохранёнными по `config_id`;
  - `trades` суммируется, `avg_pnl_pct` считается как trade-weighted среднее.

### 5.2 `expand`

- Читает references.
- Для каждого reference поднимает параметры из `configs` таблицы.
- Генерирует соседей `expand_around_references(..., n_steps=1)`.
- Ограничивает итоговый набор `max_configs=2000`.

### 5.3 `forward`

- Строит expanded configs.
- Запускает `tune.run(FleetTrial, scheduler=ASHAScheduler(...), num_samples=1)`.
- Метрика оптимизации: `avg_pnl_pct` (`mode="max"`).
- Временная ось ASHA: `time_budget_s`.

`FleetTrial.step()` каждые `report_interval_s` репортит:

- `time_budget_s`
- `total_trades`
- `configs_with_trades`
- `avg_pnl_pct`
- `avg_win_rate_pct`

### 5.4 `promote`

- Фильтрует конфиги по `min_trades` и `min_avg_pnl`.
- Сортирует по `avg_pnl_pct`.
- Экспортирует top-K в `data/promoted-<run_id>.json`.

---

## 6) Данные и метрики (SQLite)

### 6.1 Таблицы

- `configs`:
  - параметры стратегии + risk/runtime поля.
- `trades`:
  - `config_id`, `symbol`, entry/exit, `pnl_pct`, `exit_reason`,
  - `gate_natr_30m_pct_at_entry`, `hold_ms`, `early_stop_churn`,
  - `run_id` (ключевой тег для trial analytics).

### 6.2 Агрегации в driver

`FleetIPC.query_run_metrics(run_id)` считает по каждому `config_id`:

- `trades`
- `avg_pnl_pct`
- `win_rate_pct`
- `total_pnl_pct`
- `stop_loss_share_pct`

---

## 7) Trial API и UI

Endpoints:

- `GET /api/v1/trials`:
  - список run-ов (`run_id`, `config_count`, `total_trades`, `win_rate_pct`, `avg_pnl_pct`, `total_pnl_pct`, время первой/последней сделки).
- `GET /api/v1/trials/:run_id`:
  - метрики по каждому конфигу внутри run-а.
- `GET /api/v1/trials/axes?run_id=...`:
  - срез по 7 осям параметров (alive/dead зоны, weighted avg pnl).
- `GET /trials`:
  - HTML dashboard поверх этих API.

---

## 8) Операционный runbook

```bash
cd /root/turbo/hft-lead-lag

# terminal A: runtime
cargo build --release
./target/release/hft-lead-lag
```

```bash
cd /root/turbo/hft-lead-lag

# terminal B: ray driver
python3 -m pip install -r ray_driver/requirements.txt
python3 -m ray_driver scout --duration 600
python3 -m ray_driver expand --duration 600 --cycles 1
python3 -m ray_driver forward --max-budget 240 --grace-period 60 --report-interval 30
```

### 8.1 Первичный scout-playbook (поиск референсов)

`scout` используется как разведка search-space. Его задача: найти "живые" параметры, а не выдать финальный production-профиль.

Рекомендуемый порядок:

1. Первый прогон: `python3 -m ray_driver scout --duration 900`.
2. Проверь размер и качество референсов (`data/scout-references.json`).
3. Если референсов мало (`<20`) — увеличь duration и повтори scout.
4. Если референсов много, но слабо подтверждены (1-2 сделки), сделай фильтрацию перед `expand`.

Быстрая проверка файла референсов:

```bash
cd /root/turbo/hft-lead-lag
python3 - <<'PY'
import json
from pathlib import Path
p = Path("data/scout-references.json")
rows = json.loads(p.read_text()) if p.exists() else []
print("references:", len(rows))
rows = sorted(rows, key=lambda r: (r["trades"], r["avg_pnl_pct"]), reverse=True)
for r in rows[:10]:
    print(r)
PY
```

Техническая деталь текущей версии:

- `scout` сейчас сохраняет все конфиги с `trades >= 1` (порог не вынесен в CLI)
  и накапливает их кумулятивно между запусками.
- Поэтому "чистка" noise обычно делается на этапе анализа `scout-references.json` перед `expand`.

Проверка результатов:

```bash
curl -s http://localhost:5000/api/v1/trials
curl -s http://localhost:5000/api/v1/trials/axes
```

Promotion:

```bash
python3 -m ray_driver promote <run_id> --top-k 50 --min-trades 5 --min-pnl 0.0
```

---

## 9) Ограничения и риски

1. `forward` работает с `num_samples=1`, то есть один Ray trial на весь batch конфигов.
2. `promote` не применяет изменения в runtime автоматически, только экспортирует JSON.
3. IPC через один файл `config/trial-batch.json` не безопасен для нескольких одновременных driver-процессов.
4. При слабом потоке сигналов возможно много конфигов с `0 trades`, тогда выборка noisy.

---

## 10) Практические next steps

1. Разбить forward на несколько Ray trial-ов (шарды конфигов), чтобы ASHA реально ранжировал конкурентов.
2. Добавить controlled auto-promotion (через отдельный gate/checkpoint), а не ручной перенос JSON.
3. Добавить локи/namespace для IPC, если нужно параллелить несколько driver-сессий.
